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

template <typename T,
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
        void push_back(T1 v);
        bool remove(uint64_t &&idx);
        void sort();
        void quick_sort(int left, int right);
        void bubble_sort();
                                
        std::vector<T> get_all_data();
        
        template <class Archive>
        void save(Archive &ar) const;
        template <class Archive>
        void load(Archive &ar);

    private:
        void resize();
        std::unique_ptr<T[]> data_;
        uint64_t capacity_;
        uint64_t size_;
        Lock lock;

        using VectorAllocator =
            std::allocator_traits<Allocator>::template rebind_alloc<T>;
            
};

}
#include "nu/impl/sync_vector.ipp"