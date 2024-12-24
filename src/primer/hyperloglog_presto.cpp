#include "primer/hyperloglog_presto.h"
#include <bitset>
#include <cstddef>
#include <iostream>
#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0) {
  nbits_ = n_leading_bits;
  if (nbits_ < 0) {  //边界判断
    std::cout << "nbits小于0" << std::endl;
    return;
  }
  num_buckets_ = 1 << n_leading_bits;
  dense_bucket_.resize(num_buckets_, 0);
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  auto hash_value = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bina_hash(hash_value);
  bina_hash_ = bina_hash;
  int zero_cnt = 0;                                     //最低有效位的右侧有几个0
  for (int i = 0; i < BITSET_CAPACITY - nbits_; i++) {  //要不要保留最高位呢？要的
    if (!bina_hash[i]) {
      zero_cnt++;
    } else {
      break;
    }
  }

  std::string bin_str = bina_hash.to_string();
  std::string highest_b_bits = bin_str.substr(0, nbits_);
  // std::cout << "Highest " << nbits_ << " bits: " << highest_b_bits <<"cnt: "<<zero_cnt<< std::endl;
  size_t bucket_idx = 0;
  if (!highest_b_bits.empty()) {
    bucket_idx = std::stoi(highest_b_bits, nullptr, 2);  // 将提取的二进制字符串转换为十进制
  }

  SplitAndStore(zero_cnt, bucket_idx);
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */

  //边界判断
  if (nbits_ < 0) {
    std::cout << "nbits小于0" << std::endl;
    return;
  }

  double sum = 0.0;
  //打印register的元素
  std::cout << "buckets的元素为:";
  for (auto elem : dense_bucket_) {
    std::cout << elem << " ";
  }
  std::cout << std::endl;
  for (size_t i = 0; i < num_buckets_; i++) {
    uint8_t overflow_bucket_value = static_cast<uint8_t>(overflow_bucket_[i].to_ulong());
    auto bucket_value = static_cast<uint8_t>(dense_bucket_[i].to_ulong());

    // 拼接操作：将 overflow_bucket_value 左移 4 位，再与 bucket_value 进行按位或操作
    uint16_t merge = (overflow_bucket_value << 4) | bucket_value;

    sum += 1.0 / std::pow(2, merge);
  }
  cardinality_ = CONSTANT * num_buckets_ * num_buckets_ / sum;
  // cardinality_ = static_cast<uint64_t>(std::floor(alpha_mm * sum));
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
