//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <algorithm>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <utility>
#include "common/rwlatch.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  if (!frame_) {
    throw std::invalid_argument("This frame is null! ");
  }

  // // 使用共享锁来保证并发读
  // read_lock_ =std::shared_lock<std::shared_mutex>(frame_->rwlatch_);
  // 先获得rw锁，再获得bmp锁
  frame_->rwlatch_.lock();

  std::cout << "ReadPageGuard获得共享锁" << std::endl;
  std::cout << "frame_->rwlatch_ address: " << &(frame_->rwlatch_) << std::endl;

  // 在这里，我们假设一个有效的帧已被加载，接下来我们执行 pin 操作，确保该页面不会被替换。
  frame_->pin_count_++;
  replacer_->SetEvictable(frame_->frame_id_, false);
  is_valid_ = true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  page_id_ = that.page_id_;
  bpm_latch_ = std::move(that.bpm_latch_);
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  is_valid_ = that.is_valid_;
  that.is_valid_ = false;
  that.bpm_latch_ = nullptr;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    this->Drop();
    page_id_ = that.page_id_;
    bpm_latch_ = std::move(that.bpm_latch_);
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    is_valid_ = that.is_valid_;
    that.is_valid_ = false;
    that.bpm_latch_ = nullptr;
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
  }

  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() {
  // 锁住BPM的锁，防止其他线程并发修改资源
  // std::lock_guard<std::mutex> guard(*bpm_latch_);
  if (is_valid_) {
    frame_->rwlatch_.unlock();
    this->is_valid_ = false;
    frame_->pin_count_--;
    if ((frame_->pin_count_) == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
      std::cout << "frame: " << frame_->frame_id_ << "的pin_cnt为0" << std::endl;
    }
  }
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() {
  if (this->is_valid_) {
    std::cout << "ReadPageGuard释放共享锁" << std::endl;
  }
  Drop();
}

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  // write_lock_= std::unique_lock<std::shared_mutex>(frame_->rwlatch_);
  frame_->rwlatch_.lock();
  std::cout << "WritePageGuard获得独占锁" << std::endl;
  std::cout << "frame_->rwlatch_ address: " << &(frame_->rwlatch_) << std::endl;
  is_valid_ = true;
  frame_->pin_count_++;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  page_id_ = that.page_id_;
  bpm_latch_ = std::move(that.bpm_latch_);
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  is_valid_ = that.is_valid_;
  that.is_valid_ = false;
  that.bpm_latch_ = nullptr;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();
    page_id_ = that.page_id_;
    bpm_latch_ = std::move(that.bpm_latch_);
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    is_valid_ = that.is_valid_;
    that.is_valid_ = false;
    that.bpm_latch_ = nullptr;
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() {
  if (is_valid_) {
    frame_->rwlatch_.unlock();
    this->is_valid_ = false;
    frame_->pin_count_--;
    if ((frame_->pin_count_) == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() {
  if (this->is_valid_) {
    std::cout << "WritePageGuard释放独占锁" << std::endl;
  }
  Drop();
}

}  // namespace bustub
