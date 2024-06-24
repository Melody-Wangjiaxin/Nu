#include "nu/cereal.hpp"

namespace nu {

template <size_t NZones, typename T, typename Allocator, typename Lock>
inline SyncVector<NZones, T, Allocator, Lock>::SyncVector(
                const SyncVector &o) noexcept : SyncVector() {
  SyncVector::operator=(o);
}          

template <size_t NZones, typename T, typename Allocator, typename Lock>
inline SyncVector<NZones, T, Allocator, Lock>
    &SyncVector<NZones, T, Allocator, Lock>::operator=(
        const SyncVector &o) noexcept {
    VectorAllocator vecAllocator;
    ZoneAllocator zone_allocator;
    capacity_ = o.capacity_;
    size_ = o.size_;

    zones_ = zone_allocator.allocate(NZones);
    uint64_t capacity_per_zone = (capacity_ + NZones - 1) / NZones;
    uint64_t size_per_zone = (size_ + NZones - 1) / NZones;
    for (size_t i = 0; i < NZones; i++) {
        zones_[i].capacity_per_zone_ = capacity_per_zone;
        zones_[i].size_per_zone_ = size_per_zone;
        auto &data_ = zones_[i].data_;
        auto &o_data = o.zones_[i].data_;
        data_ = vecAllocator.allocate(capacity_per_zone);
        std::copy(o_data, o_data + size_per_zone, data_);
        vecAllocator.deallocate(o_data, capacity_per_zone);
    }
    
    // data_ = vecAllocator.allocate(capacity_);
    // std::copy(o.data_, o.data_ + o.size_, data_);
    // vecAllocator.deallocate(o.data_, o.capacity_);
    o.capacity_ = 0;
    o.size_ = 0;
    return *this;
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
inline SyncVector<NZones, T, Allocator, Lock>::SyncVector(SyncVector &&o) noexcept
        : zones_(o.zones_), capacity_(o.capacity_), size_(o.size_), lock_() {
    o.zones_ = nullptr;
    o.capacity_ = 0;
    o.size_ = 0;
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
inline SyncVector<NZones, T, Allocator, Lock> 
    &SyncVector<NZones, T, Allocator, Lock>::operator=(
        SyncVector && o) noexcept {
    capacity_ = o.capacity_;
    size_ = o.size_;
    zones_ = o.zones_;
    
    o.capacity_ = 0;
    o.size_ = 0;
    o.zones_ = nullptr;
    return *this;
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
SyncVector<NZones, T, Allocator, Lock>::SyncVector() 
    : zones_(nullptr), capacity_(NZones), size_(0), lock_() {
    VectorAllocator vecAllocator;
    ZoneAllocator zone_allocator;
    zones_ = zone_allocator.allocate(NZones);
    for (size_t i = 0; i < NZones; i++) {
        zones_[i].capacity_per_zone_ = 1;
        zones_[i].size_per_zone_ = 0;
        auto &data_ = zones_[i].data_;
        data_ = vecAllocator.allocate(1);
    }
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
SyncVector<NZones, T, Allocator, Lock>::~SyncVector() {
    VectorAllocator vecAllocator;
    ZoneAllocator zone_allocator;
    for (size_t i = 0; i < NZones; i++) {
        auto &zone_ = zones_[i];
        std::destroy_at(&zone_);
    }
    zone_allocator.deallocate(zones_, NZones);
    capacity_ = 0;
    size_ = 0;
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
void SyncVector<NZones, T, Allocator, Lock>::clear() {
    VectorAllocator vecAllocator;
    for (size_t i = 0; i < NZones; i++) {
        auto &data_ = zones_[i].data_;
        vecAllocator.deallocate(data_, zones_[i].capacity_per_zone_);
        auto &zone = zones_[i];
        zone.capacity_per_zone_ = 1;
        zone.size_per_zone_ = 0;
    }
    capacity_ = NZones;
    size_ = 0;
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
void SyncVector<NZones, T, Allocator, Lock>::resize() {
    uint64_t new_capacity = capacity_ * 2;
    VectorAllocator vecAllocator;
    uint64_t capacity_per_zone = (capacity_ + NZones - 1) / NZones;
    uint64_t new_capacity_per_zone = (new_capacity + NZones - 1) / NZones;
    for (size_t i = 0; i < NZones; i++) {
        zones_[i].capacity_per_zone_ = new_capacity_per_zone;
        uint64_t size_per_zone = zones_[i].size_per_zone_;
        auto &data_ = zones_[i].data_;
        T* new_data = vecAllocator.allocate(new_capacity_per_zone);
        std::move(data_, data_ + size_per_zone, new_data);
        vecAllocator.deallocate(data_, capacity_per_zone);
        data_ = new_data;        
    }
    capacity_ = new_capacity;    
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
void SyncVector<NZones, T, Allocator, Lock>::resize(uint64_t new_capacity) {
    VectorAllocator vecAllocator;
    uint64_t capacity_per_zone = (capacity_ + NZones - 1) / NZones;
    uint64_t new_capacity_per_zone = (new_capacity + NZones - 1) / NZones;
    for (size_t i = 0; i < NZones; i++) {
        zones_[i].capacity_per_zone_ = new_capacity_per_zone;
        uint64_t size_per_zone = zones_[i].size_per_zone_;
        auto &data_ = zones_[i].data_;
        T* new_data = vecAllocator.allocate(new_capacity_per_zone);
        std::move(data_, data_ + size_per_zone, new_data);
        vecAllocator.deallocate(data_, capacity_per_zone);
        data_ = new_data;        
    }
    capacity_ = new_capacity;     
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
T *SyncVector<NZones, T, Allocator, Lock>::get(uint64_t &&idx) {
    lock_.lock();
    auto zone_idx = idx % NZones;
    auto &zone = zones_[zone_idx];
    auto &data_ = zone.data_;
    auto &lock = zone.lock;
    lock.lock();
    if(idx < size_) {
        auto ret = reinterpret_cast<T *>(data_ + (idx / NZones));
        lock.unlock();
        lock_.unlock();
        return ret;
    }
    lock.unlock();
    lock_.unlock();
    return nullptr;
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
std::optional<T> SyncVector<NZones, T, Allocator, Lock>::get_copy(uint64_t &&idx) {
    lock_.lock();
    auto zone_idx = idx % NZones;
    auto &zone = zones_[zone_idx];
    auto &data_ = zone.data_;
    auto &lock = zone.lock;
    lock.lock();
    if(idx < size_) {
        auto ret = std::make_optional(data_[(idx / NZones)]);
        lock.unlock();
        lock_.unlock();
        return ret;
    }
    lock.unlock();
    lock_.unlock();
    return std::nullopt;
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
template <typename T1>
void SyncVector<NZones, T, Allocator, Lock>::put(uint64_t &&idx, T1 v) {
    lock_.lock();
    auto zone_idx = idx % NZones;
    auto &zone = zones_[zone_idx];
    auto &data_ = zone.data_;
    auto &lock = zone.lock;
    lock.lock();
    if (idx >= capacity_) {
        for(size_t i = 0; i < NZones; i++) {
            if(i == zone_idx) continue;
            auto &tlock = zones_[i].lock;
            tlock.lock();
        }
        resize();
        for(size_t i = 0; i < NZones; i++) {
            if(i == zone_idx) continue;
            auto &tlock = zones_[i].lock;
            tlock.unlock();
        }
    }
    data_[(idx / NZones)] = v;
    zones_[zone_idx].size_per_zone_ = idx / NZones + 1;
    if (idx >= size_) {
        size_ = idx + 1;
    }
    lock.unlock();
    lock_.unlock();
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
template <typename T1>
void SyncVector<NZones, T, Allocator, Lock>::push_back(T1 v) {
    // auto &lock = lock_;
    // lock.lock();
    // if(size_ + 1 > capacity_) {
    //     // lock.unlock();
    //     resize();
    //     // lock.lock();
    // }
    // data_[size_] = std::move(v);
    // size_++;
    // lock.unlock();
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
bool SyncVector<NZones, T, Allocator, Lock>::remove(uint64_t &&idx) {
    // auto &lock = lock_;
    // lock.lock();
    // if (idx >= size_) {
    //     lock.unlock();
    //     return false;
    // }
    // // VectorAllocator vecAllocator;
    // // auto *d = reinterpret_cast<T *>(data_ + idx);
    // // std::destroy_at(d);
    // // vecAllocator.deallocate(d, 1);    
    // std::move(data_ + idx + 1, data_ + size_, data_ + idx);
    // --size_;
    // lock.unlock();
    return true;
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
void SyncVector<NZones, T, Allocator, Lock>::sort() {
    lock_.lock();
    VectorAllocator vecAllocator;
    T* all_data = vecAllocator.allocate(capacity_);
    uint64_t pre_size = 0;
    for(size_t i = 0; i < NZones; i++) {
        auto &zone = zones_[i];
        auto &data_ = zone.data_;
        auto &lock = zone.lock;
        uint64_t size_ = zone.size_per_zone_;
        std::copy(data_, data_ + size_, all_data + pre_size);
        pre_size += size_;
        lock.lock();
    }
    bubble_sort(all_data);
    pre_size = 0;
    for(size_t i = 0; i < NZones; i++) {
        auto &zone = zones_[i];
        auto &data_ = zone.data_;
        auto &lock = zone.lock;
        uint64_t size_ = zones_[i].size_per_zone_;
        std::copy(all_data + pre_size, all_data + pre_size + size_, data_);
        pre_size += size_;
        lock.unlock();
    }
    vecAllocator.deallocate(all_data, capacity_);
    lock_.unlock();
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
void SyncVector<NZones, T, Allocator, Lock>::quick_sort(int left, int right, T* data_) {
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

template <size_t NZones, typename T, typename Allocator, typename Lock>
void SyncVector<NZones, T, Allocator, Lock>::bubble_sort(T* all_data) {
    for (size_t i = 0; i < size_ - 1; i++) {
        for (size_t j = 0; j < size_ - i - 1; j++) {
            if (all_data[j] > all_data[j + 1]) {
                std::swap(all_data[j], all_data[j + 1]);
            }
        }
    }
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
std::vector<T> SyncVector<NZones, T, Allocator, Lock>::get_all_data() {
    lock_.lock();
    VectorAllocator vecAllocator;
    T* all_data = vecAllocator.allocate(capacity_);
    uint64_t pre_size = 0;
    for(size_t i = 0; i < NZones; i++) {
        auto &zone = zones_[i];
        auto &data_ = zone.data_;
        auto &lock = zone.lock;
        uint64_t size_ = zone.size_per_zone_;
        std::copy(data_, data_ + size_, all_data + pre_size);
        pre_size += size_;
    }

    std::vector<T> ret(all_data, all_data + pre_size);
    // clear();
    lock_.unlock();
    vecAllocator.deallocate(all_data, capacity_);
    return ret;
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
std::vector<T> SyncVector<NZones, T, Allocator, Lock>::get_all_sorted_data() {
    lock_.lock();
    VectorAllocator vecAllocator;
    T* all_data = vecAllocator.allocate(capacity_);
    uint64_t pre_size = 0;
    for(size_t i = 0; i < NZones; i++) {
        auto &zone = zones_[i];
        auto &data_ = zone.data_;
        auto &lock = zone.lock;
        lock.lock();
        uint64_t size_ = zone.size_per_zone_;
        std::copy(data_, data_ + size_, all_data + pre_size);
        lock.unlock();
        pre_size += size_;
    }
    
    for (size_t i = 0; i < pre_size - 1; i++) {
        for (size_t j = 0; j < pre_size - i - 1; j++) {
            if (all_data[j] > all_data[j + 1]) {
                std::swap(all_data[j], all_data[j + 1]);
            }
        }
    }

    std::vector<T> ret(all_data, all_data + pre_size);
    // clear();
    lock_.unlock();
    vecAllocator.deallocate(all_data, capacity_);
    return ret;
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
void SyncVector<NZones, T, Allocator, Lock>::reload(std::vector<T>& all_data)
{
    lock_.lock();
    uint32_t pre_size = 0;
    for(size_t i = 0; i < NZones; i++) {
        auto &zone = zones_[i];
        auto &data_ = zone.data_;
        auto &lock = zone.lock;
        lock.lock();
        uint64_t size_ = zones_[i].size_per_zone_;
        std::copy(all_data.begin() + pre_size, all_data.begin() + pre_size + size_, data_);
        pre_size += size_;
        lock.unlock();
    }
    lock_.unlock();
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
template <class Archive>
void SyncVector<NZones, T, Allocator, Lock>::save(Archive &ar) const {
    std::vector<T*> new_data;
    ar(capacity_);
    ar(size_);
    for(size_t i = 0; i < NZones; i++) {
        auto &zone = zones_[i];
        uint64_t size_per_zone = zone.size_per_zone_;
        auto &data_ = zone.data_;
        ar(zone.size_per_zone_, zone.capacity_per_zone_);
        for(size_t j = 0; j < size_per_zone; j++) {
            auto *d = reinterpret_cast<T*>(data_ + i);
            ar(*d);
        }
    }
}

template <size_t NZones, typename T, typename Allocator, typename Lock>
template <class Archive>
void SyncVector<NZones, T, Allocator, Lock>::load(Archive &ar) {
    VectorAllocator vecAllocator;
    ZoneAllocator zone_allocator;
    zones_ = zone_allocator.allocate(NZones);
    ar(capacity_);
    ar(size_);
    for(size_t i = 0; i < NZones; i++) {
        auto &zone = zones_[i];
        ar(zone.size_per_zone_, zone.capacity_per_zone_);
        auto &data_ = zone.data_;
        data_ = vecAllocator.allocate(zone.capacity_per_zone_);
        for(size_t j = 0; j < zone.size_per_zone; j++) {
            auto *d = reinterpret_cast<T*>(data_ + i);
            ar(*d);
        }
    }
}

}