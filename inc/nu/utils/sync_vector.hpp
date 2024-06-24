#pragma once

#include <sync.h>

#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "nu/utils/spin_lock.hpp"

namespace nu{

template <size_t NZones, typename T,
          typename Allocator = std::allocator<T>,
          typename Lock = SpinLock>
class SyncVector {
    public:
        SyncVector();
        ~SyncVector();
        SyncVector(const SyncVector &) noexcept;
        SyncVector &operator=(const SyncVector &) noexcept;
        SyncVector(SyncVector &&) noexcept;
        SyncVector &operator=(SyncVector &&) noexcept;

        T *get(uint64_t &&idx);
        std::optional<T> get_copy(uint64_t &&idx);
        template <typename T1>
        void put(uint64_t &&idx, T1 v);
        template <typename T1>
        void push_back(T1 v);
        bool remove(uint64_t &&idx);
        void sort();
        void clear();
        void reload(std::vector<T>& all_data);
                                
        std::vector<T> get_all_data();
        std::vector<T> get_all_sorted_data();
        
        template <class Archive>
        void save(Archive &ar) const;
        template <class Archive>
        void load(Archive &ar);

    private:
        void resize();
        void resize(uint64_t new_capacity);
        void quick_sort(int left, int right, T* data_);
        void bubble_sort(T* all_data);

        // std::unique_ptr<T[]> data_;
        struct Zone {
            T* data_;
            uint64_t capacity_per_zone_;
            uint64_t size_per_zone_;
            Lock lock;
        };
        Zone *zones_;
        uint64_t capacity_;
        uint64_t size_;
        Lock lock_;
        std::vector<T> all_sorted_data;

        using VectorAllocator =
            std::allocator_traits<Allocator>::template rebind_alloc<T>;
        using ZoneAllocator =
            std::allocator_traits<Allocator>::template rebind_alloc<Zone>;

};

}
#include "nu/impl/sync_vector.ipp"