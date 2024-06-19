#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>

#include "nu/utils/cond_var.hpp"
#include "nu/utils/read_skewed_lock.hpp"

namespace nu {

template <typename T, typename Allocator = std::allocator<T> >
class RCUVector {
 public:
  constexpr static uint32_t kReaderWaitFastPathMaxUs = 20;

  T *get(uint64_t &&idx);
  template <typename T1>
  void push_back(T1 &&v);
  bool remove(uint64_t &&idx);
  template <typename T1>
  bool remove_if_equals(uint64_t &&idx, T1 &&v);
  void for_each(const std::function<bool(const T &)> &fn);

 private:
  ReadSkewedLock lock_;
  std::vector<T> vec_;
};
}  // namespace nu

#include "nu/impl/rcu_vector.ipp"
