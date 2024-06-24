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

template <typename T, uint64_t NumZones = 32768>
class DistributedVector {
    public:
        constexpr static uint32_t kDefaultPowerNumShards = 13;
        constexpr static uint64_t kNumPerShard = NumZones;

        using VectorShard = SyncVector<NumZones, T, std::allocator<T>, Mutex>;

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
        void sort_shard(uint32_t &&idx);
        void sort_shard(uint32_t &&idx, bool *is_local);
        void clear_shard(uint32_t &&idx, bool *is_local);
        void clear_all();
        void reload(std::vector<T> data_[]);

        template <typename T1>
        Future<std::optional<T1>> get_async(uint64_t &&idx);
        template <typename T1>
        Future<void> put_async(uint64_t &&idx, T1 &&v);
        template <typename T1>
        Future<void> push_back_async(T1 &&v);
        Future<bool> remove_async(uint64_t &&idx);
        Future<void> sort_async();
        
        std::vector<T> get_all_data();
        std::vector<T> get_data_in_shard(uint32_t &&idx, bool *is_local);
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
        template <typename TT, uint64_t N>
        friend DistributedVector<TT, N> make_dis_vector(
            uint32_t power_num_shards, bool pinned);
        void bubble_sort(std::vector<T> &all_data);
};

template <typename T, uint64_t NumZones = 32768>
DistributedVector<T, NumZones> make_dis_vector(
    uint32_t power_num_shards = DistributedVector<T, NumZones>::kDefaultPowerNumShards,
    bool pinned = false);

}  // namespace nu

#include "nu/impl/dis_vector.ipp"
