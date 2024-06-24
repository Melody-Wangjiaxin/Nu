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

    // // 移动赋值运算符
    // Val& operator=(Val&& other) noexcept {
    //     if (this != &other) {
    //         delete[] data;
    //         data = other.data;
    //         other.data = nullptr;
    //     }
    //     return *this;
    // }

    template <class Archive> void serialize(Archive &ar) {
        ar(cereal::binary_data(data, sizeof(data)));
    }

    bool operator>(const Val &v) const {
        // for(size_t i = 0; i < kValLen; i++) {
        //     if(data[i] != v.data[i]) return data[i] > v.data[i];
        // }
        // return false;
        return std::memcmp(this->data, v.data, kValLen) > 0;
    }

    bool operator<(const Val &v) const {
        // for(size_t i = 0; i < kValLen; i++) {
        //     if(data[i] != v.data[i]) return data[i] < v.data[i];
        // }
        // return false;
        return std::memcmp(this->data, v.data, kValLen) < 0;
    }

    bool operator>=(const Val &v) const {
        // for(size_t i = 0; i < kValLen; i++) {
        //     if(data[i] != v.data[i]) return data[i] >= v.data[i];
        // }
        // return true;
        return std::memcmp(this->data, v.data, kValLen) >= 0;
    }

    bool operator<=(const Val &v) const {
        // for(size_t i = 0; i < kValLen; i++) {
        //     if(data[i] != v.data[i]) return data[i] <= v.data[i];
        // }
        // return true;
        return std::memcmp(this->data, v.data, kValLen) <= 0;
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

using DSVector = nu::DistributedVector<Val>;

// constexpr static size_t kNum = (1 << DSVector::kDefaultPowerNumShards) *
//                                     DSVector::kNumPerShard *
//                                     kLoadFactor; // 80530636
constexpr static size_t kNum = 500000;

void random_str(auto &dist, auto &mt, uint32_t len, char *buf) {
    for (uint32_t i = 0; i < len; i++) {
        buf[i] = dist(mt);
    }
}

void init(DSVector *vec) {
    std::vector<nu::Thread> threads;
    constexpr uint32_t kNumThreads = 400;
    auto num = kNum / kNumThreads;  // 80530636 / 400 = 201326
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
}

struct Element {
    Val value;
    uint32_t arrayIndex;
    uint32_t elementIndex;
    bool operator>(const Element& other) const {
        return value > other.value;
    }
};

std::vector<Val> mergeSortedArrays(uint32_t arrays_num, std::vector<Val> arrays[]) {
    std::vector<Val> result;
    std::priority_queue<Element, std::vector<Element>, std::greater<Element> > minHeap;
    std::vector<Val> tmp[1 << DSVector::kDefaultPowerNumShards];

    // 将每个数组的第一个元素加入堆中
    for (size_t i = 0; i < arrays_num; ++i) {
        // tmp[i].reserve(arrays[i].size());
        // std::copy(arrays[i].begin(), arrays[i].end(), tmp[i].begin());
        // arrays[i].clear();
        // std::cout << "arrays[" << i << "].size()=" << arrays[i].size() << std::endl;
        if (!arrays[i].empty()) {
            minHeap.push(Element{arrays[i][0], i, 0});
        }
    }

    // 进行多路归并
    while (!minHeap.empty()) {
        Element current = minHeap.top();
        minHeap.pop();
        result.push_back(current.value);
        // 如果当前数组还有剩余元素，则将下一个元素加入堆中
        if (current.elementIndex + 1 < arrays[current.arrayIndex].size()) {
            minHeap.push(Element{arrays[current.arrayIndex][current.elementIndex + 1], 
                        current.arrayIndex, current.elementIndex + 1});
        }
    }

    for (size_t i = 0; i < arrays_num; ++i) {
        arrays[i].clear();
    }
    std::cout << "result size = " << result.size() << std::endl;
    for(Val i: result) {
        printf("%s ", i.data);
    }
    std::cout << std::endl;
    return result;
}

std::vector<Val> merge_sort(uint32_t arrays_num, std::vector<Val> arrays[]) {
    std::vector<Val> result;
    std::vector<uint32_t> indices(arrays_num, 0); // 保存每个数组当前指针的位置

    while (true) {
        Val min_value;
        memcpy(min_value.data, "{{", sizeof min_value.data);        
        int min_index = -1;
        // 找到所有数组中当前最小的元素
        for (uint32_t i = 0; i < arrays_num; i++) {
            if (indices[i] < arrays_num && arrays[i][indices[i]] < min_value) {
                min_value = arrays[i][indices[i]];
                min_index = i;
            }
        }
        // 如果没有找到最小值，说明所有数组都已合并完毕
        if (min_index == -1) {
            break;
        }
        // 将最小值加入结果，并移动对应数组的指针
        result.push_back(min_value);
        indices[min_index]++;
    }

    return result;
}

void mySort(std::vector<Val> vec_to_sort, uint32_t arrays_num, std::vector<Val> arrays[])
{
    std::cout << "start sort" << std::endl;
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
    std::cout << "sort over" << std::endl;
}

static std::atomic<bool> flag = false;
class Proxy {
    public:
        Proxy(DSVector vec) : vec_(std::move(vec)) {}

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
                    std::cout << "ending  break" << std::endl;
                    break;
                } else if(req.waiting) {
                    if(!flag && got_shard_num >= (1 << DSVector::kDefaultPowerNumShards)) {
                        if(mutex_1.try_lock() && !flag) {
                            mySort(vec_to_sort, 1 << DSVector::kDefaultPowerNumShards, vecs_);

                            std::cout << "start reload" << std::endl;
                            vec_.reload(vecs_);
                            std::cout << "reload over" << std::endl;
                            // std::sort(vec_to_sort.begin(), vec_to_sort.end());
                            // std::vector<Val> res = merge_sort(1 << DSVector::kDefaultPowerNumShards, vecs_);
                            // std::vector<Val> res = mergeSortedArrays(1 << DSVector::kDefaultPowerNumShards, vecs_);
                            
                            // std::cout << "start reload 2" << std::endl;
                            // vec_.reload(res);
                            // std::cout << "reload over 2" << std::endl;
                            
                            flag = true;
                            resp.ending = true;
                            resp.all_data_sorted = true;
                            mutex_1.unlock();
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
                                // std::cout << "start sort 1" << std::endl;
                                mySort(vec_to_sort, 1 << DSVector::kDefaultPowerNumShards, vecs_);
                                
                                std::cout << "start reload" << std::endl;
                                vec_.reload(vecs_);
                                std::cout << "reload over" << std::endl;
                                // std::vector<Val> res = merge_sort(1 << DSVector::kDefaultPowerNumShards, vecs_);
                                // std::vector<Val> res = mergeSortedArrays(1 << DSVector::kDefaultPowerNumShards, vecs_);
                                // std::cout << "sort over 1" << std::endl;
                                // std::cout << "start reload 1" << std::endl;
                                // vec_.reload(res);
                                // std::cout << "reload over 1" << std::endl;
                                resp.ending = true;
                                flag = true;
                                mutex_1.unlock();
                            }
                            resp.all_data_sorted = true;
                        } else {
                            resp.all_data_sorted = false;
                        } 
                    } 
                    // else {
                    //     if(req.to_reload_shard) {
                    //         std::cout << "start reload" << std::endl;
                    //         vec_.clear_all();
                    //         std::cout << "reload over" << std::endl;
                    //         resp.reload_shard_finished = true;
                    //         resp.latest_shard_ip = 0;
                    //         resp.ending = true;
                    //     }
                    // }          
                } else {
                    resp.shard_sort_finished = false;
                    vecs_[req.shard_id] = vec_.get_data_in_shard(std::forward<uint32_t>(req.shard_id), &is_local);
                    mutex_2.lock();
                    vec_to_sort.insert(vec_to_sort.end(), 
                                        vecs_[req.shard_id].begin(), 
                                        vecs_[req.shard_id].end());
                    got_shard_num++;
                    // std::cout << "got shard num " << got_shard_num << std::endl;
                    if(got_shard_num == (1 << DSVector::kDefaultPowerNumShards)) {
                        resp.shard_sort_finished = true;
                        std::cout << "all shard sorting finished" << std::endl;
                    }
                    mutex_2.unlock();
                    auto id = vec_.get_shard_proclet_id(req.shard_id);
                    if (is_local) {
                        resp.latest_shard_ip = 0;
                    } else {
                        resp.latest_shard_ip =
                            nu::get_runtime()->rpc_client_mgr()->get_ip_by_proclet_id(id);
                    }
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
        DSVector vec_;
        std::mutex mutex_1; // for data_sort
        std::mutex mutex_2; // for get_shard
        std::atomic<uint32_t> got_shard_num;
        std::vector<Val> vec_to_sort;
        std::vector<Val> vecs_[1 << DSVector::kDefaultPowerNumShards];
};

void do_work() {
    DSVector vec = nu::make_dis_vector<Val>();
    std::cout << "start initing..." << std::endl;
    init(&vec);
    std::cout << "finish initing..." << std::endl;

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
