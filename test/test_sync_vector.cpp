#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "nu/runtime.hpp"
#include "nu/utils/farmhash.hpp"
#include "nu/utils/sync_vector.hpp"

using namespace nu;

constexpr size_t NZones = 262144;
constexpr double kLoadFactor = 0.25;
constexpr size_t kNum = 262144 * kLoadFactor;
constexpr uint32_t kKeyLen = 20;
constexpr uint32_t kValLen = 2;
constexpr auto kFarmHashStrtoU64 = [](const std::string &str) {
    return util::Hash64(str.c_str(), str.size());
};

using K = std::string;
using V = std::string;

std::random_device rd;
std::mt19937 mt(rd());
std::uniform_int_distribution<int> dist('A', 'z');

std::string random_str(uint32_t len) {
    std::string str = "";
    for (uint32_t i = 0; i < len; i++) {
        str += dist(mt);
    }
    return str;
}

void do_work() {
    auto vec_ptr = std::make_unique<SyncVector<NZones, V> >();
    std::vector<std::string> std_vec;

    std::cout << "Running " << __FILE__ "..." << std::endl;
    bool passed = true;

    for (uint64_t i = 0; i < kNum; i++) {
        std::string v = random_str(kValLen);
        std_vec.push_back(v);
        vec_ptr->put(std::forward<uint64_t>(i), v);
    }
    // std::cout << "Before sort..." << std::endl;
    for (uint64_t i = 0; i < std_vec.size(); i++) {
        auto optional = vec_ptr->get(std::forward<uint64_t>(i));
        auto &v = std_vec[i];
        if (!optional || v != *optional) {
            passed = false;
            goto done;
        }
    }

    std::sort(std_vec.begin(), std_vec.end());
    vec_ptr->sort();
    for (uint64_t i = 0; i < std_vec.size(); i++) {
        auto optional = vec_ptr->get(std::forward<uint64_t>(i));
        auto &v = std_vec[i];
        if (!optional || v != *optional) {
            passed = false;
            goto done;
        }
    }

done:
    if (passed) {
        std::cout << "Passed" << std::endl;
    } else {
        std::cout << "Failed" << std::endl;
    }
}

int main(int argc, char **argv) {
    return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
