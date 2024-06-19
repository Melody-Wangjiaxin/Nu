#pragma once

#include <memory>
#include <utility>
#include <vector>

extern "C" {
#include <runtime/net.h>
}

#include "nu/proclet.hpp"
#include "nu/utils/mutex.hpp"
#include "nu/utils/spin_lock.hpp"
#include "nu/utils/sync_vector.hpp"

namespace nu {

template <typename T>
class DistributedVector {
    public:
        constexpr static uint32_t kDefaultPowerNumShards = 13;
        constexpr static uint64_t kNumPerShard = 32768;

        using VectorShard = SyncVector<T, std::allocator<T>, Mutex>;

        DistributedVector(const DistributedVector &);
        DistributedVector &operator=(const DistributedVector &);
        DistributedVector(DistributedVector &&);
        DistributedVector &operator=(DistributedVector &&);
        DistributedVector();

        std::optional<T> get(uint64_t &&idx);
        std::optional<T> get(uint64_t &&idx, bool *is_local);
        template <typename T1>
        void put(uint64_t &&idx, T1 &&v);
        template <typename T1>
        void push_back(T1 &&v);
        bool remove(uint64_t &&idx);
        void sort();

        template <typename T1>
        Future<std::optional<T1>> get_async(uint64_t &&idx);
        template <typename T1>
        Future<void> put_async(uint64_t &&idx, T1 &&v);
        template <typename T1>
        Future<void> push_back_async(T1 &&v);
        Future<bool> remove_async(uint64_t &&idx);
        Future<void> sort_async();
        
        std::vector<T> get_all_data();
        static uint32_t get_shard_idx(uint64_t &&idx, uint32_t power_num_shards);
        ProcletID get_shard_proclet_id(uint32_t shard_id);

        template <class Archive>
        void serialize(Archive &ar);

        // For debugging and performance analysis.
        std::pair<std::optional<T>, uint32_t> get_with_ip(uint64_t &&idx);

    private:
        struct RefCnter {
            std::vector<Proclet<VectorShard> > shards;
        };

        friend class Test;
        uint32_t power_num_shards_;
        uint32_t num_shards_;
        Proclet<RefCnter> ref_cnter_;
        std::vector<WeakProclet<VectorShard> > shards_;

        uint32_t get_shard_idx(uint64_t idx);
        template <typename TT>
        friend DistributedVector<TT> make_dis_vector(
            uint32_t power_num_shards, bool pinned);
};

template <typename T>
DistributedVector<T> make_dis_vector(
    uint32_t power_num_shards = DistributedVector<T>::kDefaultPowerNumShards,
    bool pinned = false);

}  // namespace nu

#include "nu/impl/dis_vector.ipp"
