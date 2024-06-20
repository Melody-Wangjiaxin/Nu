#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
}
#include <runtime.h>

#include "nu/dis_vector.hpp"
#include "nu/proclet.hpp"
#include "nu/runtime.hpp"
#include "nu/utils/farmhash.hpp"

using namespace nu;

constexpr size_t kNum = 100000;
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

bool run_test() {

    std::vector<std::string> std_vec;
    auto vec = make_dis_vector<std::string>(5);
    for (uint64_t i = 0; i < kNum; i++) {
        std::string v = random_str(kValLen);
        std_vec.push_back(v);
        vec.put(std::forward<uint64_t>(i), v);
    }
 
    auto vec2 = vec;
    for (uint64_t i = 0; i < std_vec.size(); i++) {
        auto optional = vec2.get(std::forward<uint64_t>(i));
        auto &v = std_vec[i];
        if (!optional || v != *optional) {
            return false;
        }
    }
    
    auto proclet = make_proclet<ErasedType>();
    if (!proclet.run(
            +[](ErasedType &,
                std::vector<std::string> std_vec,
                DistributedVector<std::string> vec) {
                for (uint64_t i = 0; i < std_vec.size(); i++) {
                    auto optional = vec.get(std::forward<uint64_t>(i));
                    auto &v = std_vec[i];
                    if (!optional || v != *optional) {
                        return false;
                    }
                }
                return true;
            },
            std_vec, vec2)) {
        return false;
    }
    
    auto vec3 = std::move(vec2);
    for (uint64_t i = 0; i < std_vec.size(); i++) {
        auto optional = vec3.get(std::forward<uint64_t>(i));
        auto &v = std_vec[i];
        if (!optional || v != *optional) {
            return false;
        }
    }

    bool flag = true;
    std::sort(std_vec.begin(), std_vec.end());
    vec3.sort();
    for (uint64_t i = 0; i < std_vec.size(); i++) {
        auto optional = vec3.get(std::forward<uint64_t>(i));
        auto &v = std_vec[i];
        std::cout << v << ' ' << *optional << std::endl;
        if (!optional || v != *optional) {
            flag = false;
        }
    }
    return flag;
}

void do_work() {
    if (run_test()) {
        std::cout << "Passed" << std::endl;
    } else {
        std::cout << "Failed" << std::endl;
    }
}

int main(int argc, char **argv) {
    return runtime_main_init(argc, argv, [](int, char **) { do_work(); });
}
