#include "primer/hyperloglog.h"
#include <cmath>
#include <cstddef>
#include <iostream>

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  //边界判断
  nbits_ = n_bits;
  if (nbits_ < 0) {
    return;
  }
  num_registers_ = 1 << n_bits;
  registers_.resize(num_registers_, 0);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  return hash;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  size_t cnt = 1;
  for (int i = BITSET_CAPACITY - nbits_; i >= 0; i--) {
    if (!bset[i]) {
      cnt++;
    } else {
      break;
    }
  }
  if (nbits_ == 0) {  //尽管nbits_=0，最左侧的位依然要保留
    cnt--;
  }

  return cnt;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bina_hash = ComputeBinary(hash);
  bina_hash_ = bina_hash;
  // std::cout<<"val转化为binary为: "<<bina_hash<<std::endl;
  uint64_t leading_zeros = PositionOfLeftmostOne(bina_hash);
  std::string bin_str = bina_hash.to_string();
  std::string highest_b_bits = bin_str.substr(0, nbits_);
  // std::cout << "Highest " << nbits_ << " bits: " << highest_b_bits << std::endl;
  size_t register_index = 0;
  if (!highest_b_bits.empty()) {
    register_index = std::stoi(highest_b_bits, nullptr, 2);  // 将提取的二进制字符串转换为十进制
  }

  // 输出十进制值
  // std::cout << "Decimal value: " << register_index << std::endl;
  // std::cout<<"MSB: "<<leading_zeros<<std::endl;
  registers_[register_index] = std::max(registers_[register_index], leading_zeros);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */

  if (nbits_ < 0) {  //边界判断
    return;
  }

  double sum = 0.0;
  //打印register的元素
  // std::cout << "register的元素为:";
  // for (auto elem : registers_) {
  //   std::cout << elem << " ";
  // }
  // std::cout << std::endl;
  for (auto r : registers_) {
    // std::cout<<"-r:"<<-r<<" "<<std::pow(2,-r)<<std::endl;
    sum += 1.0 / std::pow(2, r);
  }

  cardinality_ = static_cast<size_t>(CONSTANT * num_registers_ * num_registers_ / sum);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
