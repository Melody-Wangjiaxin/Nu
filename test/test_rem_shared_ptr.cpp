#include <algorithm>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "nu/proclet.hpp"
#include "nu/rem_shared_ptr.hpp"
#include "nu/runtime.hpp"

using namespace nu;

class Obj {};

void do_work() {
  bool passed = true;

  std::vector<int> a{1, 2, 3, 4};
  std::vector<int> b{5, 6, 7, 8};

  auto proclet = make_proclet<Obj>();

  auto rem_shared_ptr_a_future = proclet.run_async(
      +[](Obj &_, std::vector<int> vec_a) {
        return make_rem_shared<std::vector<int>>(std::move(vec_a));
      },
      a);
  auto rem_shared_ptr_b_future = proclet.run_async(
      +[](Obj &_, std::vector<int> vec_b) {
        return make_rem_shared<std::vector<int>>(std::move(vec_b));
      },
      b);

  auto rem_shared_ptr_a = std::move(rem_shared_ptr_a_future.get());
  auto rem_shared_ptr_a_copy = rem_shared_ptr_a;
  auto rem_shared_ptr_b = std::move(rem_shared_ptr_b_future.get());
  auto rem_shared_ptr_b_copy = rem_shared_ptr_b;
  auto c = proclet.run(
      +[](Obj &_, RemSharedPtr<std::vector<int>> rem_shared_ptr_a,
          RemSharedPtr<std::vector<int>> rem_shared_ptr_b) {
        auto *raw_ptr_a = rem_shared_ptr_a.get();
        auto *raw_ptr_b = rem_shared_ptr_b.get();
        std::vector<int> c;
        for (size_t i = 0; i < raw_ptr_a->size(); i++) {
          c.push_back(raw_ptr_a->at(i) + raw_ptr_b->at(i));
        }
        return c;
      },
      std::move(rem_shared_ptr_a), std::move(rem_shared_ptr_b));

  passed &= !rem_shared_ptr_a;
  passed &= !rem_shared_ptr_b;
  passed &= rem_shared_ptr_a_copy;
  passed &= rem_shared_ptr_b_copy;

  for (size_t i = 0; i < a.size(); i++) {
    if (c[i] != a[i] + b[i]) {
      passed = false;
      break;
    }
  }
  if (a.size() != c.size()) {
    passed = false;
  }

  auto a_copy = *rem_shared_ptr_a_copy;
  for (size_t i = 0; i < a.size(); i++) {
    if (a_copy[i] != a[i]) {
      passed = false;
      break;
    }
  }
  if (a.size() != a_copy.size()) {
    passed = false;
  }

  auto b_copy = *rem_shared_ptr_b_copy;
  for (size_t i = 0; i < b.size(); i++) {
    if (b_copy[i] != b[i]) {
      passed = false;
      break;
    }
  }
  if (b.size() != b_copy.size()) {
    passed = false;
  }

  if (passed) {
    std::cout << "Passed" << std::endl;
  } else {
    std::cout << "Failed" << std::endl;
  }
}

int main(int argc, char **argv) {
  return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
