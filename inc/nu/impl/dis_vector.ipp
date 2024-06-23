#include "nu/commons.hpp"

namespace nu {

template <typename T, uint64_t NumZones>
inline DistributedVector<T, NumZones>::DistributedVector(const DistributedVector &o) {
  *this = o;
}

template <typename T, uint64_t NumZones>
inline DistributedVector<T, NumZones> &DistributedVector<T, NumZones>::operator=(
        const DistributedVector &o) {
  power_num_shards_ = o.power_num_shards_;
  num_shards_ = o.num_shards_;
  ref_cnter_ = o.ref_cnter_;
  shards_ = o.shards_;
  return *this;
}

template <typename T, uint64_t NumZones>
inline DistributedVector<T, NumZones>::DistributedVector(DistributedVector &&o) {
    *this = std::move(o);
}

template <typename T, uint64_t NumZones>
inline DistributedVector<T, NumZones> &DistributedVector<T, NumZones>::operator=(
        DistributedVector && o) {
    power_num_shards_ = o.power_num_shards_;
    num_shards_ = o.num_shards_;
    ref_cnter_ = std::move(o.ref_cnter_);
    shards_ = std::move(o.shards_);
    return *this;
}

template <typename T, uint64_t NumZones>
inline DistributedVector<T, NumZones>::DistributedVector() 
        : power_num_shards_(0), num_shards_(0) {}

template <typename T, uint64_t NumZones>
inline uint32_t DistributedVector<T, NumZones>::get_shard_idx(uint64_t idx) {
    // return idx / (std::numeric_limits<uint64_t>::max() >> power_num_shards_);
    return idx % (1 << power_num_shards_);
}

template <typename T, uint64_t NumZones>
inline uint32_t 
DistributedVector<T, NumZones>::get_shard_idx(
uint64_t &&idx, uint32_t power_num_shards) {
    return idx % (1 << power_num_shards);
    // return idx / (std::numeric_limits<uint64_t>::max() >> power_num_shards);
}
        
template <typename T, uint64_t NumZones>
inline ProcletID 
    DistributedVector<T, NumZones>::get_shard_proclet_id(uint32_t shard_id) {
    return shards_[shard_id].id_;
}
        
template <typename T, uint64_t NumZones>
inline std::optional<T> DistributedVector<T, NumZones>::get(uint64_t &&idx) {
    auto shard_idx = get_shard_idx(idx);
    auto &shard = shards_[shard_idx];
    return shard.__run(&VectorShard::template get_copy, idx);
}

template <typename T, uint64_t NumZones>
inline std::optional<T> DistributedVector<T, NumZones>::get(
                    uint64_t &&idx, bool *is_local) {
    auto shard_idx = get_shard_idx(idx);
    auto &shard = shards_[shard_idx];
    *is_local = shard.is_local();
    return shard.__run(&VectorShard::template get_copy, idx);
}

template <typename T, uint64_t NumZones>
inline std::pair<std::optional<T>, uint32_t> 
    DistributedVector<T, NumZones>::get_with_ip(uint64_t &&idx) {
    auto shard_idx = get_shard_idx(idx);
    auto &shard = shards_[shard_idx];
    return shard.__run(
        +[](VectorShard &shard, uint64_t idx) {
            return std::make_pair(shard.get_copy(std::move(idx)),
                                get_cfg_ip());
        },
        idx);
}

template <typename T, uint64_t NumZones>
template <typename T1>
void DistributedVector<T, NumZones>::put(uint64_t &&idx, T1 &&v) {
    auto shard_idx = get_shard_idx(idx);
    auto &shard = shards_[shard_idx];
    shard.__run(&VectorShard::template put<T>, idx, std::forward<T1>(v));
}

template <typename T, uint64_t NumZones>
template <typename T1>
inline void DistributedVector<T, NumZones>::push_back(T1 &&v) {
    // auto shard_idx = get_shard_idx(idx);
    // auto &shard = shards_[shard_idx];
    // shard.__run(&VectorShard::template push_back<T>, std::forward<T1>(v));
}

template <typename T, uint64_t NumZones>
inline bool DistributedVector<T, NumZones>::remove(uint64_t &&idx) {
    auto shard_idx = get_shard_idx(idx);
    auto &shard = shards_[shard_idx];
    shard.__run(&VectorShard::template remove, idx);
}

template <typename T, uint64_t NumZones>
inline void DistributedVector<T, NumZones>::sort() {
    std::vector<T> vec;
    std::vector<Future<std::vector<T> > > futures;
    for (uint32_t i = 0; i < num_shards_; i++) {
        futures.emplace_back(shards_[i].__run_async(
            +[](VectorShard &shard) { return shard.get_all_sorted_data(); }));
    }
    for (auto &future : futures) {
        auto &vec_shard = future.get();
        vec.insert(vec.end(), vec_shard.begin(), vec_shard.end());
    }

    uint64_t all_data_size = vec.size();

    bubble_sort(vec);

    for(size_t i = 0; i < all_data_size; i++) {
        put(std::forward<uint64_t>(i), std::move(vec[i]));
    }
    
}

template <typename T, uint64_t NumZones>
void DistributedVector<T, NumZones>::sort_shard(uint32_t &&idx)
{
    shards_[idx].__run(&VectorShard::template sort);
}

template <typename T, uint64_t NumZones>
void DistributedVector<T, NumZones>::sort_shard(
    uint32_t &&idx, bool* is_local)
{
    auto &shard = shards_[idx];
    *is_local = shard.is_local();
    shards_[idx].__run(&VectorShard::template sort);
}

template <typename T, uint64_t NumZones>
std::vector<T>
DistributedVector<T, NumZones>::get_data_in_shard(
    uint32_t &&idx, bool *is_local)
{
    auto &shard = shards_[idx];
    *is_local = shard.is_local();
    std::vector<T> ret = shard.__run(&VectorShard::template get_all_sorted_data);
    return ret;
}

template <typename T, uint64_t NumZones>
void DistributedVector<T, NumZones>::clear_shard(
    uint32_t &&idx, bool *is_local)
{
    auto &shard = shards_[idx];
    *is_local = shard.is_local();
    shard.__run(&VectorShard::template clear);
}

template <typename T, uint64_t NumZones>
void DistributedVector<T, NumZones>::clear_all()
{
    // std::vector<Future<void> > futures;
    // for (uint32_t i = 0; i < num_shards_; i++) {
    //     futures.emplace_back(shards_[i].__run_async(
    //         +[](VectorShard &shard) { return shard.clear(); }));
    // }
    // for (auto &future : futures) {
    //     future.get();
    // }  
    for (uint32_t i = 0; i < num_shards_; i++) {
        shards_[i].__run(&VectorShard::template clear);
    } 
}

template <typename T, uint64_t NumZones>
void DistributedVector<T, NumZones>::bubble_sort(std::vector<T> &all_data) {
    uint64_t size_ = all_data.size();
    for (size_t i = 0; i < size_ - 1; i++) {
        for (size_t j = 0; j < size_ - i - 1; j++) {
            if (all_data[j] > all_data[j + 1]) {
                std::swap(all_data[j], all_data[j + 1]);
            }
        }
    }
}

template <typename T, uint64_t NumZones>
template <typename T1>
inline Future<std::optional<T1>> DistributedVector<T, NumZones>::get_async(
                                uint64_t &&idx) {
    return nu::async([&, idx] { return get(std::move(idx)); });
}

template <typename T, uint64_t NumZones>
template <typename T1>
inline Future<void> DistributedVector<T, NumZones>::put_async(
                                uint64_t &&idx, T1 &&v) {
    return nu::async([&, idx, v] { return put(std::move(idx), std::move(v)); });
}

template <typename T, uint64_t NumZones>
template <typename T1>
inline Future<void> DistributedVector<T, NumZones>::push_back_async(T1 &&v) {
    // return nu::async([&, v] { return push_back(std::move(v)); });
}

template <typename T, uint64_t NumZones>
inline Future<bool> DistributedVector<T, NumZones>::remove_async(uint64_t &&idx) {
    return nu::async([&, idx] { return remove(std::move(idx)); });
}

template <typename T, uint64_t NumZones>
inline Future<void> DistributedVector<T, NumZones>::sort_async() {
    return nu::async([&] { return sort(); });
}

template <typename T, uint64_t NumZones>
inline std::vector<T> DistributedVector<T, NumZones>::get_all_data() {
    std::vector<T> vec;
    std::vector<Future<std::vector<T> > > futures;
    for (uint32_t i = 0; i < num_shards_; i++) {
        futures.emplace_back(shards_[i].__run_async(
            +[](VectorShard &shard) { return shard.get_all_data(); }));
    }
    for (auto &future : futures) {
        auto &vec_shard = future.get();
        vec.insert(vec.end(), vec_shard.begin(), vec_shard.end());
    }
    return vec;
}

template <typename T, uint64_t NumZones>
template <class Archive>
inline void DistributedVector<T, NumZones>::serialize(Archive &ar) {
    ar(power_num_shards_);
    ar(num_shards_);
    ar(ref_cnter_);
    ar(shards_);
}

template <typename T, uint64_t NumZones>
inline DistributedVector<T, NumZones> make_dis_vector(
    uint32_t power_num_shards, bool pinned) {
    using VectorType = DistributedVector<T, NumZones>;
    VectorType vec;
    vec.power_num_shards_ = power_num_shards;
    vec.num_shards_ = (1 << power_num_shards);
    vec.ref_cnter_ = make_proclet<typename VectorType::RefCnter>();
    vec.shards_ = vec.ref_cnter_.run(
        +[](VectorType::RefCnter &self, uint32_t num_shards, bool pinned) {
            std::vector<WeakProclet<typename VectorType::VectorShard> >
                weak_shards;
            for (uint32_t i = 0; i < num_shards; i++) {
                self.shards.emplace_back(
                    make_proclet<typename VectorType::VectorShard>(pinned));
                weak_shards.emplace_back(self.shards.back().get_weak());
            }
            return weak_shards;
        },
        vec.num_shards_, pinned);
    return vec;
}

}  // namespace nu
