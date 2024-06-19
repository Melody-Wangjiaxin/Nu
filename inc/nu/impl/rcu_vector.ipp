#include <functional>
#include <type_traits>
#include <utility>

extern "C" {
#include <base/assert.h>
}

namespace nu {

template <typename T, typename Allocator>
T *RCUVector<T, Allocator>::get(uint64_t &&idx) {
    lock_.reader_lock();
    T *ret;
    if (idx >= vec_.size()) {
        ret = nullptr;
    } else {
        ret = &vec_[idx];
    }
    lock_.reader_unlock();
    return ret;
}

template <typename T, typename Allocator>
template <typename T1>
void RCUVector<T, Allocator>::push_back(T1 &&v) {
    lock_.writer_lock();
    vec_.push_back(v);
    lock_.writer_unlock();
}

template <typename T, typename Allocator>
bool RCUVector<T, Allocator>::remove(uint64_t &&idx) {
    lock_.writer_lock();
    vec_.erase(vec_.begin() + idx);
    lock_.writer_unlock();
}

template <typename T, typename Allocator>
template <typename T1>
bool RCUVector<T, Allocator>::remove_if_equals(uint64_t &&idx, T1 &&v) {
    bool removed = false;
    lock_.writer_lock();
    if (idx < vec_.size() && vec_[idx] == std::forward<T1>(v)) {
        map_.erase(vec_.begin() + idx);
        removed = true;
    }
    lock_.writer_unlock();
    return removed;
}

template <typename T, typename Allocator>
void RCUVector<T, Allocator>::for_each(
    const std::function<bool(const T &)> &fn) {
    lock_.reader_lock();
    for (const auto &v : vec_) {
        if (!fn(v)) {
            lock_.reader_unlock();
            return;
        }
    }
    lock_.reader_unlock();
}


}