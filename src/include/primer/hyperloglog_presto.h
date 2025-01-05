#pragma once

#include <sys/types.h>
#include <algorithm>
#include <bitset>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>  // NOLINT
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "primer/hyperloglog.h"

/** @brief Dense bucket size. */
#define DENSE_BUCKET_SIZE 4
/** @brief Overflow bucket size. */
#define OVERFLOW_BUCKET_SIZE 3

/** @brief Total bucket size. */
#define TOTAL_BUCKET_SIZE (DENSE_BUCKET_SIZE + OVERFLOW_BUCKET_SIZE)

namespace bustub {

template <typename KeyType>
class HyperLogLogPresto {
  /**
   * INSTRUCTIONS: Testing framework will use the GetDenseBucket and GetOverflow function,
   * hence SHOULD NOT be deleted. It's essential to use the dense_bucket_
   * data structure.
   */

  /** @brief Constant for HLL. */
  static constexpr double CONSTANT = 0.79402;

 public:
  std::bitset<BITSET_CAPACITY> bina_hash_;

  /** @brief Disabling default constructor. */
  HyperLogLogPresto() = delete;

  /** @brief Parameterized constructor. */
  explicit HyperLogLogPresto(int16_t n_leading_bits);

  /** @brief Returns the dense_bucket_ data structure. */
  auto GetDenseBucket() const -> std::vector<std::bitset<DENSE_BUCKET_SIZE>> { return dense_bucket_; }

  /** @brief Returns overflow bucket of a specific given index. */
  auto GetOverflowBucketofIndex(uint16_t idx) { return overflow_bucket_[idx]; }

  /** @brief Retusn the cardinality of the set. */
  auto GetCardinality() const -> uint64_t { return cardinality_; }

  /** @brief Element is added for HLL calculation. */
  auto AddElem(KeyType val) -> void;

  /** @brief Function to compute cardinality. */
  auto ComputeCardinality() -> void;

 private:
  /** @brief Calculate Hash.
   *
   * @param[in] val
   *
   * @returns hash value
   */
  inline auto CalculateHash(KeyType val) -> hash_t {
    Value val_obj;
    if constexpr (std::is_same<KeyType, std::string>::value) {
      val_obj = Value(VARCHAR, val);
      return bustub::HashUtil::HashValue(&val_obj);
    }
    if constexpr (std::is_same<KeyType, int64_t>::value) {
      return static_cast<hash_t>(val);
    }
    return 0;
  }

  void SplitAndStore(uint64_t zero_cnt, size_t bucket_idx) {
    // 将 hash_value 转换为 std::bitset
    std::bitset<64> bitset_hash(zero_cnt);  // 假设我们处理的是64位的哈希值
    int tmp = zero_cnt;
    // Step 1: 将右边连续零的数量 zero_cnt 分割成两部分

    int overflow_value = tmp >> DENSE_BUCKET_SIZE;
    int dense_value = zero_cnt;

    if (overflow_value != 0) {
      dense_value = zero_cnt - (overflow_value << 4);
      // std::cout << "zero_cnt: " << zero_cnt << "overflow: " << overflow_value << "dense: " << dense_value <<
      // std::endl;
    }

    // Step 2: 获取溢出部分和密集部分
    std::bitset<OVERFLOW_BUCKET_SIZE> overflow_bits(overflow_value);
    std::bitset<DENSE_BUCKET_SIZE> dense_bits(dense_value);
    // std::cout << "dense_bit: " << dense_bits << std::endl;
    // Step 3: 大于原来的元素才可以覆盖
    if (zero_cnt > (dense_bucket_[bucket_idx].to_ulong() + (overflow_bucket_[bucket_idx].to_ulong() << 4))) {
      dense_bucket_[bucket_idx] = dense_bits;
      if (overflow_bits.size() > 0) {
        overflow_bucket_[bucket_idx] = overflow_bits;
      }
    }

    // std::cout<<"overflow: "<<overflow_bits<<"   dense: "<<dense_bits<<std::endl;
  }

  /** @brief Structure holding dense buckets (or also known as registers). */
  std::vector<std::bitset<DENSE_BUCKET_SIZE>> dense_bucket_;

  /** @brief Structure holding overflow buckets. */
  std::unordered_map<uint16_t, std::bitset<OVERFLOW_BUCKET_SIZE>> overflow_bucket_;

  /** @brief Storing cardinality value */
  uint64_t cardinality_;

  // TODO(student) - can add more data structures as required
  size_t num_buckets_;
  // uint16_t nbits_; 唐完了
  int16_t nbits_;
};

}  // namespace bustub
