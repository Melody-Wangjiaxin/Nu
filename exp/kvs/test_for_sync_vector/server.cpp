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
    bool to_clear_shard;
    bool end_of_req;
    bool waiting;
};

struct Resp {
    uint32_t latest_shard_ip;
    bool found;
    Val val;
    bool shard_sort_finished;
    bool all_data_sorted;
    bool clear_shard_finished;
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
                if (req.end_of_req) [[unlikely]] {
                    Resp resp;
                    resp.shard_sort_finished = true;
                    resp.ending = true;
                    BUG_ON(c->WriteFull(&resp, sizeof(resp)) < 0);
                    break;
                } else if(req.waiting) {
                    Resp resp;
                    resp.shard_sort_finished = true;
                    BUG_ON(c->WriteFull(&resp, sizeof(resp)) < 0);
                    continue;
                }
                Resp resp;
                resp.clear_shard_finished = false;
                resp.ending = false;
                bool is_local;
                if(req.shard_sort_finished) {
                    resp.shard_sort_finished = true;
                    if(req.to_sort) {
                        mutex_.lock();
                        std::cout << "start sort" << std::endl;
                        std::sort(vec_to_sort.begin(), vec_to_sort.end());
                        std::cout << "sort over" << std::endl;
                        mutex_.unlock();
                        resp.all_data_sorted = true;
                    } else {
                        if(req.to_clear_shard) {
                            std::cout << "start clear" << std::endl;
                            vec_.clear_all();
                            std::cout << "clear over" << std::endl;
                            resp.clear_shard_finished = true;
                            resp.latest_shard_ip = 0;
                            resp.ending = true;
                        }
                    }          
                } else {
                    resp.shard_sort_finished = false;
                    auto temp_vec = vec_.get_data_in_shard(std::forward<uint32_t>(req.shard_id), &is_local);
                    mutex_.lock();
                    vec_to_sort.insert(vec_to_sort.end(), temp_vec.begin(), temp_vec.end());
                    mutex_.unlock();
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
        std::mutex mutex_;
        std::vector<Val> vec_to_sort;
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
