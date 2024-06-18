#include "nu/cereal.hpp"

namespace nu {

template <typename T, typename Allocator, typename Lock>
inline SyncVector<T, Allocator, Lock>::SyncVector(
                const SyncVector &o) noexcept : SyncVector() {
  SyncVector::operator=(o);
}          

template <typename Allocator, typename Lock>
inline SyncVector<T, Allocator, Lock>
    &SyncVector<T, Allocator, Lock>::operator=(
        const SyncVector &o) noexcept {
    VectorAllocator vecAllocator;
    capacity_ = o.capacity_;
    size_ = o.size_;
    data_ = vecAllocator(capacity_);
    for(size_t i = 0; i < size_; i++) {
        auto *d = reinterpret_cast<T *>(data_ + i);
        auto *o_d = reinterpret_cast<T *>(o.data_ + i);
        *d = *o_d;
    }
    vecAllocator.deallocate(o.data_, o.capacity_);
    o.capacity_ = 0;
    o.size_ = 0;
    return *this;
}


template <typename T, typename Allocator, typename Lock>
inline SyncVector<T, Allocator, Lock>::SyncVector(
                const SyncVector &o) noexcept : SyncVector() {
  SyncVector::operator=(o);
}          

template <typename T, typename Allocator, typename Lock>
inline SyncVector<T, Allocator, Lock>
    &SyncVector<T, Allocator, Lock>::operator=(
        const SyncVector &o) noexcept {
    VectorAllocator vecAllocator;
    capacity_ = o.capacity_;
    size_ = o.size_;
    data_ = vecAllocator.allocate(capacity_);
    std::copy(o.data_, o.data_ + o.size_, data_);
    // for(size_t i = 0; i < size_; i++) {
    //     auto *d = reinterpret_cast<T *>(data_ + i);
    //     auto *o_d = reinterpret_cast<T *>(o.data_ + i);
    //     *d = *o_d;
    // }
    vecAllocator.deallocate(o.data_, o.size_);
    o.capacity_ = 0;
    o.size_ = 0;
    return *this;
}

template <typename T, typename Allocator, typename Lock>
inline SyncVector<T, Allocator, Lock>::SyncVector(SyncVector &&o) noexcept
        : data_(o.data_), capacity_(o.capacity_), size_(o.size_) {
    o.data_ = nullptr;
    o.capacity_ = 0;
    o.size_ = 0;
}

template <typename T, typename Allocator, typename Lock>
inline SyncVector 
    &SyncVector<T, Allocator, Lock>::operator=(
        SyncVector && o) noexcept {
    data_ = o.data_;
    capacity_ = o.capacity_;
    size_ = o.size_;
    o.data_ = nullptr;
    o.capacity_ = 0;
    o.size_ = 0;
    return *this;
}

template <typename T, typename Allocator, typename Lock>
SyncVector<T, Allocator, Lock>::SyncVector() {
    VectorAllocator vecAllocator;
    capacity_ = 1;
    size_ = 0;
    data_ = vecAllocator.allocate(1);
}

template <typename T, typename Allocator, typename Lock>
SyncVector<T, Allocator, Lock>::~SyncVector() {
    VectorAllocator vecAllocator;
    if (data_) {
        vecAllocator.deallocate(data_, size_);
    }
    capacity_ = 0;
    size_ = 0;
}

template <typename T, typename Allocator, typename Lock>
void SyncVector<T, Allocator, Lock>::resize() {
    uint64_t new_capacity = capacity_ * 2;
    VectorAllocator vecAllocator;
    T* new_data = vecAllocator.allocate(new_capacity);
    std::move(data_, data_ + size_, new_data);
    vecAllocator.deallocate(data_, capacity_);
    data_ = new_data;
    capacity_ = new_capacity;
}

template <typename T, typename Allocator, typename Lock>
T *SyncVector<T, Allocator, Lock>::get(uint64_t &&idx) {
    lock.lock();
    if(idx < size_) {
        auto ret = reinterpret_cast<T *>(data_ + idx);
        lock.unlock();
        return ret;
    }
    lock.unlock();
    return nullptr;
}

template <typename T, typename Allocator, typename Lock>
std::optional<T> SyncVector<T, Allocator, Lock>::get_copy(uint64_t &&idx) {
    lock.lock();
    auto ret = nullptr;
    if(idx < size_) {
        auto ret = std::make_optional(data_[idx]);
        lock.unlock();
        return ret;
    }
    lock.unlock();
    return nullptr;
}

template <typename T, typename Allocator, typename Lock>
template <typename T1>
void SyncVector<T, Allocator, Lock>::push_back(T1 v) {
    lock.lock();
    if(size_ + 1 > capacity_) {
        resize();
    }
    data_[size_] = std::forward<T1>(v);
    size_++;
    lock.unlock();
}

template <typename T, typename Allocator, typename Lock>
bool SyncVector<T, Allocator, Lock>::remove(uint64_t &&idx) {
    lock.lock();
    if (idx >= size_) {
        lock.unlock();
        return false;
    }
    VectorAllocator vecAllocator;
    auto *d = reinterpret_cast<T *>(data_ + idx);
    std::destroy_at(d);
    vecAllocator.deallocate(d, 1);    
    std::move(data_ + idx + 1, data_ + size_, data_ + idx);
    --size_;
    lock.unlock();
    return true;
}

template <typename T, typename Allocator, typename Lock>
void SyncVector<T, Allocator, Lock>::sort() {
    lock.lock();
    bubble_sort();
    lock.unlock();
}

template <typename T, typename Allocator, typename Lock>
void SyncVector<T, Allocator, Lock>::quick_sort(int left, int right) {
    if (left >= right) {
        return;
    }
    T pivot = data_[left];
    int i = left, j = right;
    while (i < j) {
        while (i < j && data_[j] >= pivot) --j;
        if (i < j) data_[i++] = data_[j];
        while (i < j && data_[i] <= pivot) ++i;
        if (i < j) data_[j--] = data_[i];
    }
    data_[i] = pivot;
    quick_sort(left, i - 1);
    quick_sort(i + 1, right);
}

template <typename T, typename Allocator, typename Lock>
void SyncVector<T, Allocator, Lock>::bubble_sort() {
    for (size_t i = 0; i < size_ - 1; ++i) {
        for (size_t j = 0; j < size_ - i - 1; ++j) {
            if (data_[j] > data_[j + 1]) {
                std::swap(data_[j], data_[j + 1]);
            }
        }
    }
}

template <typename T, typename Allocator, typename Lock>
std::vector<T> SyncVector<T, Allocator, Lock>::get_all_data() {

}

template <typename T, typename Allocator, typename Lock>
template <class Archive>
void SyncVector<T, Allocator, Lock>::save(Archive &ar) const {
    std::vector<T*> new_data;
    ar(capacity_);
    ar(size_);
    for (size_t i = 0; i < size_; i++) {
        auto *d = reinterpret_cast<T*>(data_ + i);
        ar(*d);
    }
}

template <typename T, typename Allocator, typename Lock>
template <class Archive>
void SyncVector<T, Allocator, Lock>::load(Archive &ar) {
    VectorAllocator vecAllocator;
    ar(capacity_);
    ar(size_);
    data_ = vecAllocator.allocate(capacity_);
    for (size_t i = 0; i < size_; i++) {
        auto *d = reinterpret_cast<T*>(data_ + i);
        ar(*d);
    }
}

}