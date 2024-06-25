#include <algorithm>
#include <cereal/types/optional.hpp>
#include <cereal/types/string.hpp>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <utility>
#include <vector>
#include <queue>
#include <mutex>
#include <thread>

#include <runtime.h>

#include "nu/dis_vector.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/farmhash.hpp"
#include "nu/utils/thread.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/spin_lock.hpp"

constexpr uint32_t kKeyLen = 20;
constexpr uint32_t kValLen = 2;
constexpr double kLoadFactor = 0.30;
constexpr uint32_t kNumProxys = 1;
constexpr uint32_t kProxyPort = 10086;

struct Key {
    char data[kKeyLen];

    bool operator==(const Key &o) const {
        return __builtin_memcmp(data, o.data, kKeyLen) == 0;
    }

    template <class Archive> void serialize(Archive &ar) {
        ar(cereal::binary_data(data, sizeof(data)));
    }
};

struct Val {
    
    char data[kValLen];

    Val() : data() {}

    Val(const char* src) : data() {
        std::memcpy(data, src, kValLen);
    }

    // 拷贝构造函数
    Val(const Val& other) {
        std::memcpy(data, other.data, kValLen);
    }

    // 拷贝赋值运算符
    Val& operator=(const Val& other) {
        if (this != &other) {
            std::memcpy(data, other.data, kValLen);
        }
        return *this;
    }
    
    template <class Archive> void serialize(Archive &ar) {
        ar(cereal::binary_data(data, sizeof(data)));
    }

    bool operator>(const Val &v) const {
        return std::memcmp(this->data, v.data, kValLen) > 0;
    }

    bool operator<(const Val &v) const {
        return std::memcmp(this->data, v.data, kValLen) < 0;
    }

    bool operator>=(const Val &v) const {
        return std::memcmp(this->data, v.data, kValLen) >= 0;
    }

    bool operator<=(const Val &v) const {
        return std::memcmp(this->data, v.data, kValLen) <= 0;
    }
};

void mySort(std::vector<Val> vec_to_sort, uint32_t arrays_num, std::vector<Val> arrays[])
{
    // std::cout << "start sort" << std::endl;
    std::sort(vec_to_sort.begin(), vec_to_sort.end());
    std::vector<uint32_t> sizes(arrays_num, 0); // 保存每个数组当前指针的位置
    for (uint32_t i = 0; i < arrays_num; i++) {
        sizes[i] = arrays[i].size();
        arrays[i].clear();
    }
    uint32_t pre_size = 0;
    for (uint32_t i = 0; i < arrays_num; i++) {
        uint32_t tmp_size = sizes[i];
        arrays[i].reserve(tmp_size);
        std::copy(vec_to_sort.begin() + pre_size, 
                vec_to_sort.begin() + pre_size + tmp_size, 
                arrays[i].begin());
    }
    // std::cout << "sort over" << std::endl;
}

using DSVector = nu::DistributedVector<Val>;

struct DisVector {
    std::vector<Val> shards_[1 << DSVector::kDefaultPowerNumShards];
    std::mutex mutexes[1 << DSVector::kDefaultPowerNumShards];
    std::mutex mutex_;

    DisVector() {}

    DisVector(DisVector &&o) {
        *this = std::move(o);
    }

    DisVector(const DisVector&o) {
        *this = o;
    }

    DisVector &operator=(const DisVector &o) {
        for(size_t i = 0; i < 1 << DSVector::kDefaultPowerNumShards; i++) {
            std::copy(o.shards_[i].begin(), o.shards_[i].end(), shards_[i].begin());
        }
        return *this;
    }

    DisVector &operator=(DisVector &&o) {
        for(size_t i = 0; i < 1 << DSVector::kDefaultPowerNumShards; i++) {
            shards_[i] = o.shards_[i];
        }
        return *this;
    }

    void sort(uint32_t shard_idx) {
        uint64_t size_ = shards_[shard_idx].size();
        for (size_t i = 0; i < size_ - 1; i++) {
            for (size_t j = 0; j < size_ - i - 1; j++) {
                if (shards_[shard_idx][j] > shards_[shard_idx][j + 1]) {
                    std::swap(shards_[shard_idx][j], shards_[shard_idx][j + 1]);
                }
            }
        }
    }

    std::optional<Val> get(uint64_t &&idx) {
        uint64_t shard_num = (1 << DSVector::kDefaultPowerNumShards);
        uint32_t shard_idx = idx % shard_num;
        mutexes[shard_idx].lock();
        if(idx < shards_[shard_idx].size()) {
            auto ret = std::make_optional(shards_[(idx /shard_num)]);
            mutexes[shard_idx].unlock();
        }
        mutexes[shard_idx].unlock();
        return std::nullopt;
    }

    void put(uint64_t &&idx, Val v) {
        // std::cout << "put " << idx << std::endl;
        // mutex_.lock();
        uint64_t shard_num = (1 << DSVector::kDefaultPowerNumShards);
        uint32_t shard_idx = idx % shard_num;
        mutexes[shard_idx].lock();
        // if(idx / shard_num < shards_[shard_idx].capacity()) {
        //     uint64_t new_capacity = 2 * shards_[shard_idx].capacity();
        //     shards_[shard_idx].resize(new_capacity);
        // }
        // shards_[shard_idx][idx / shard_num] = v;
        shards_[shard_idx].push_back(v);
        mutexes[shard_idx].unlock();
        // mutex_.unlock();
    }

    std::vector<Val> get_data_in_shard(uint32_t &&shard_idx) {
        mutexes[shard_idx].lock();
        sort(shard_idx);
        mutexes[shard_idx].unlock();
        return shards_[shard_idx];
    }

    void reload(std::vector<Val> vec_to_reload) {
        mutex_.lock();
        uint64_t shard_num = (1 << DSVector::kDefaultPowerNumShards);
        for(size_t i = 0; i < shard_num; i++) {
            shards_[i].clear();
        }
        for(size_t i = 0; i < vec_to_reload.size(); i++) {
            put(std::forward<uint64_t>(i), vec_to_reload[i]);
        }
        mutex_.unlock();
    }

    template <class Archive>
    void serialize(Archive &ar) {
        
    }
};


struct Req {
    uint64_t idx;
    uint32_t shard_id;
    bool shard_sort_finished;
    bool to_sort;
    bool end_of_req;
    bool waiting;
};

struct Resp {
    uint32_t latest_shard_ip;
    bool found;
    Val val;
    bool shard_sort_finished;
    bool all_data_sorted;
    bool ending;
};

constexpr static auto kFarmHashKeytoU64 = [](const Key &key) {
    return util::Hash64(key.data, kKeyLen);
};


// constexpr static size_t kNum = (1 << DSVector::kDefaultPowerNumShards) *
//                                     DSVector::kNumPerShard *
//                                     kLoadFactor; // 80530636
constexpr static size_t kNum = 100000;

void random_str(auto &dist, auto &mt, uint32_t len, char *buf) {
    for (uint32_t i = 0; i < len; i++) {
        buf[i] = dist(mt);
    }
}

void init(DisVector *vec) {
    std::vector<std::thread> threads;
    constexpr uint32_t kNumThreads = 400;
    auto num = kNum / kNumThreads;
    for (uint32_t i = 0; i < kNumThreads; i++) {
        threads.emplace_back([&, tid = i] {
            std::random_device rd;
            std::mt19937 mt(rd());
            std::uniform_int_distribution<int> dist('A', 'z');
            for (uint64_t j = i * num; j < (i + 1) * num; j++) {
                Val val;
                random_str(dist, mt, kValLen, val.data);
                vec->put(std::forward<uint64_t>(j), val);
            }
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }

    // std::random_device rd;
    // std::mt19937 mt(rd());
    // std::uniform_int_distribution<int> dist('A', 'z');
    // for (uint64_t i = 0; i < kNum; i++) {
    //     Val val;
    //     random_str(dist, mt, kValLen, val.data);
    //     vec->put(std::forward<uint64_t>(i), val);
    // }
}

static std::atomic<bool> flag = false;
uint32_t total_time = 0;
uint64_t get_shard_start_time = 0;
class Proxy {
    public:
        Proxy(DisVector vec) : vec_(std::move(vec)) {}

        void run_loop() {
            netaddr laddr = {.ip = 0, .port = kProxyPort};
            auto *queue = rt::TcpQueue::Listen(laddr, 128);
            rt::TcpConn *c;
            while ((c = queue->Accept())) {
                nu::Thread([&, c] { handle(c); }).detach();
            }
        }

        void handle(rt::TcpConn *c) {
            while (true) {
                Req req;
                BUG_ON(c->ReadFull(&req, sizeof(req)) <= 0);
                Resp resp;
                resp.latest_shard_ip = 0;
                if (req.end_of_req) {
                    resp.ending = true;
                    BUG_ON(c->WriteFull(&resp, sizeof(resp)) < 0);
                    break;
                } else if(req.waiting) {
                    if(!flag && got_shard_num >= (1 << DSVector::kDefaultPowerNumShards)) {
                        if(mutex_1.try_lock() && !flag) {
                            uint64_t start_time = microtime();
                            mySort(vec_to_sort, 1 << DSVector::kDefaultPowerNumShards, vecs_);
                            vec_.reload(vec_to_sort);
                            total_time += (microtime() - start_time);
                            std::cout << "total time = " << total_time << std::endl;
                            // std::sort(vec_to_sort.begin(), vec_to_sort.end());
                            // std::vector<Val> res = merge_sort(1 << DSVector::kDefaultPowerNumShards, vecs_);
                            // std::vector<Val> res = mergeSortedArrays(1 << DSVector::kDefaultPowerNumShards, vecs_);
                            flag = true;
                            resp.ending = true;
                            resp.all_data_sorted = true;
                            mutex_1.unlock();
                        } else {
                            bool is_local;
                            auto optional_v = vec_.get(std::forward<uint64_t>(req.idx));
                            resp.found = optional_v.has_value();
                            if (resp.found) {
                                resp.val = *optional_v;
                            }
                            resp.latest_shard_ip = 0;
                        }                   
                    }
                    BUG_ON(c->WriteFull(&resp, sizeof(resp)) < 0);
                    continue;
                }        
                // resp.reload_shard_finished = false;
                resp.all_data_sorted = false;
                resp.ending = false;
                bool is_local;
                if(req.shard_sort_finished) {
                    resp.shard_sort_finished = true;
                    if(req.to_sort) {
                        if(!flag && got_shard_num >= (1 << DSVector::kDefaultPowerNumShards)) {
                            if(mutex_1.try_lock() && !flag) {
                                uint64_t start_time = microtime();
                                mySort(vec_to_sort, 1 << DSVector::kDefaultPowerNumShards, vecs_);
                                vec_.reload(vec_to_sort);
                                total_time += (microtime() - start_time);
                                std::cout << "total time = " << total_time << std::endl;
                                // std::vector<Val> res = merge_sort(1 << DSVector::kDefaultPowerNumShards, vecs_);
                                // std::vector<Val> res = mergeSortedArrays(1 << DSVector::kDefaultPowerNumShards, vecs_);
                                resp.ending = true;
                                flag = true;
                                mutex_1.unlock();
                            }
                            resp.all_data_sorted = true;
                        } else {
                            resp.all_data_sorted = false;
                        } 
                    }          
                } else {
                    if(!get_shard_start_time) get_shard_start_time = microtime();
                    resp.shard_sort_finished = false;
                    // uint64_t start_time = microtime();
                    vecs_[req.shard_id] = vec_.get_data_in_shard(std::forward<uint32_t>(req.shard_id));
                    // total_time += (microtime() - start_time);
                    mutex_2.lock();
                    vec_to_sort.insert(vec_to_sort.end(), 
                                        vecs_[req.shard_id].begin(), 
                                        vecs_[req.shard_id].end());
                    got_shard_num++;
                    // std::cout << "got shard num " << got_shard_num << std::endl;
                    if(got_shard_num == (1 << DSVector::kDefaultPowerNumShards)) {
                        total_time += (microtime() - get_shard_start_time);
                        resp.shard_sort_finished = true;
                        std::cout << "all shard sorting finished" << std::endl;
                    }
                    mutex_2.unlock();
                    resp.latest_shard_ip = 0;
                }                
                // bool is_local;
                // vec_.sort_shard(std::forward<uint64_t>(req.shard_id), &is_local);
                // resp.found = true;
                // auto id = vec_.get_shard_proclet_id(req.shard_id);
                // if (is_local) {
                //     resp.latest_shard_ip = 0;
                // } else {
                //     resp.latest_shard_ip =
                //         nu::get_runtime()->rpc_client_mgr()->get_ip_by_proclet_id(id);
                // }

                // bool is_local;
                // auto optional_v = vec_.get(std::forward<uint64_t>(req.idx), &is_local);
                // resp.found = optional_v.has_value();
                // if (resp.found) {
                //     resp.val = *optional_v;
                // }
                BUG_ON(c->WriteFull(&resp, sizeof(resp)) < 0);
            }
        }

    private:
        DisVector vec_;
        std::mutex mutex_1; // for data_sort
        std::mutex mutex_2; // for get_shard
        std::atomic<uint32_t> got_shard_num;
        std::vector<Val> vec_to_sort;
        std::vector<Val> vecs_[1 << DSVector::kDefaultPowerNumShards];
};

void do_work() {
    DisVector vec;
    std::cout << "start initing..." << std::endl;
    init(&vec);
    std::cout << 
    "finish initing..." << std::endl;

    std::vector<nu::Future<void>> futures;
    nu::Proclet<Proxy> proxies[kNumProxys];
    for (uint32_t i = 0; i < kNumProxys; i++) {
        proxies[i] =
            nu::make_proclet<Proxy>(std::forward_as_tuple(vec), true);
        futures.emplace_back(proxies[i].run_async(&Proxy::run_loop));
    }
    futures.front().get();
}

int main(int argc, char **argv) {
    return nu::runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}