//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * Also, make sure to read the documentation for `DeletePage`! You can assume that you will never run out of disk
 * space (via `DiskScheduler::IncreaseDiskSpace`), so this function _cannot_ fail.
 *
 * Once you have allocated the new page via the counter, make sure to call `DiskScheduler::IncreaseDiskSpace` so you
 * have enough space on disk!
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  std::scoped_lock latch(*bpm_latch_);
  page_id_t new_page_id = next_page_id_;
  next_page_id_.fetch_add(1);
  disk_scheduler_->IncreaseDiskSpace(next_page_id_);

  return new_page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * Ideally, we would want to ensure that all space on disk is used efficiently. That would mean the space that deleted
 * pages on disk used to occupy should somehow be made available to new pages allocated by `NewPage`.
 *
 * If you would like to attempt this, you are free to do so. However, for this implementation, you are allowed to
 * assume you will not run out of disk space and simply keep allocating disk space upwards in `NewPage`.
 *
 * For (nonexistent) style points, you can still call `DeallocatePage` in case you want to implement something slightly
 * more space-efficient in the future.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock latch(*bpm_latch_);  //
  // 查找页面在不在缓冲池

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  frame_id_t cur_frame_id = it->second;
  auto cur_frame_header = frames_[cur_frame_id];

  // 查看页面的pinCnt
  if (cur_frame_header->pin_count_ > 0) {
    return false;
  }

  // 将page从bp中移除
  page_table_.erase(page_id);
  free_frames_.push_back(cur_frame_id);

  // 如果页面被更改，则写回这个页面
  if (cur_frame_header->is_dirty_) {
    FlushPage(page_id);
  }

  // 通过磁盘调度器删除磁盘上的页面
  disk_scheduler_->DeallocatePage(page_id);

  // 将该帧的内存数据清空
  cur_frame_header->Reset();

  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  std::unique_lock latch(*bpm_latch_);
  auto it = page_table_.find(page_id);
  frame_id_t w_frame_id;
  // case 1: BPM有这个page
  if (it != page_table_.end()) {
    w_frame_id = it->second;
    replacer_->RecordAccess(w_frame_id);
    AcquireRWLock(w_frame_id);
    return std::make_optional(WritePageGuard(page_id, frames_[w_frame_id], replacer_, bpm_latch_));
  }

  // case 2: BPM没有这个page, 但是有空的frame
  if (!free_frames_.empty()) {
    w_frame_id = free_frames_.front();
    free_frames_.pop_front();
    replacer_->RecordAccess(w_frame_id);

    page_table_[page_id] = w_frame_id;
    std::cout << "checkWritePage中, ";
    PrintPGTBL();

    // 如果这个page还有数据，要把数据一起从磁盘读回来
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule(DiskRequest{false, frames_[w_frame_id]->data_.data(), page_id, std::move(promise1)});
    if (future1.get()) {
      std::cout << "checkWritePage中, 从磁盘读入page: " << page_id << "的数据: " << frames_[w_frame_id]->data_.data()
                << "成功" << std::endl;
    }
    AcquireRWLock(w_frame_id);
    return std::make_optional(WritePageGuard(page_id, frames_[w_frame_id], replacer_, bpm_latch_));
  }

  // case 3: BPM没有这个page, 也没新的frame
  auto evicted_frame = replacer_->Evict();
  if (evicted_frame.has_value()) {
    w_frame_id = evicted_frame.value();

    auto evicted_page_id = FindPage(w_frame_id).value();
    FlushPage(evicted_page_id);  // 将页面数据写回磁盘

    frames_[w_frame_id]->Reset();
    replacer_->RecordAccess(w_frame_id);
    // 在PGTBL中，先清空这个entry，再插入新的
    page_table_.erase(evicted_page_id);
    page_table_[page_id] = w_frame_id;
    std::cout << "checkWritePage中, ";
    PrintPGTBL();
    // 如果这个page还有数据，要把数据一起从磁盘读回来
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule(DiskRequest{false, frames_[w_frame_id]->data_.data(), page_id, std::move(promise1)});
    if (future1.get()) {
      std::cout << "checkWritePage中, 从磁盘读入page: " << page_id << "的数据: " << frames_[w_frame_id]->data_.data()
                << "成功" << std::endl;
    }
    AcquireRWLock(w_frame_id);
    return std::make_optional(WritePageGuard(page_id, frames_[w_frame_id], replacer_, bpm_latch_));
  }

  // 如果evict失败, 说明out of memory, 报错
  return std::nullopt;
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  std::unique_lock latch(*bpm_latch_);
  auto it = page_table_.find(page_id);
  frame_id_t r_frame_id;
  // case 1: BPM有这个page
  if (it != page_table_.end()) {
    r_frame_id = it->second;
    replacer_->RecordAccess(r_frame_id);
    AcquireRWLock(r_frame_id);
    return std::make_optional(ReadPageGuard(page_id, frames_[r_frame_id], replacer_, bpm_latch_));
  }

  // case 2: BPM没有这个page, 但是有空的frame
  if (!free_frames_.empty()) {
    r_frame_id = free_frames_.front();
    free_frames_.pop_front();
    replacer_->RecordAccess(r_frame_id);

    page_table_[page_id] = r_frame_id;
    std::cout << "checkReadPage中, ";
    PrintPGTBL();
    // 从磁盘读入这些数据
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule({false /*Read*/, frames_[r_frame_id]->data_.data(), page_id, std::move(promise1)});

    if (future1.get()) {
      std::cout << "从磁盘读入page: " << page_id << "的数据: " << frames_[r_frame_id]->data_.data() << "成功"
                << std::endl;
    }
    AcquireRWLock(r_frame_id);
    return std::make_optional(ReadPageGuard(page_id, frames_[r_frame_id], replacer_, bpm_latch_));
  }

  // case 3: BPM没有这个page, 也没新的frame
  auto evicted_frame = replacer_->Evict();
  if (evicted_frame.has_value()) {
    r_frame_id = evicted_frame.value();

    auto evicted_page_id = FindPage(r_frame_id).value();
    FlushPage(evicted_page_id);  // 将页面数据写回磁盘

    frames_[r_frame_id]->Reset();
    replacer_->RecordAccess(r_frame_id);

    page_table_.erase(evicted_page_id);
    page_table_[page_id] = r_frame_id;

    std::cout << "checkReadPage中, ";
    PrintPGTBL();

    // 从磁盘读入数据
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule({false /*Read*/, frames_[r_frame_id]->data_.data(), page_id, std::move(promise1)});

    if (future1.get()) {
      std::cout << "从磁盘读入page: " << page_id << "的数据: " << frames_[r_frame_id]->data_.data() << "成功"
                << std::endl;
    }
    AcquireRWLock(r_frame_id);
    return std::make_optional(ReadPageGuard(page_id, frames_[r_frame_id], replacer_, bpm_latch_));
  }

  // 如果evict失败, 说明out of memory, 报错
  return std::nullopt;
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt.value());
}

/**
 * @brief Flushes a page's data out to disk.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t w_frame_id = it->second;
  // 页面不是脏页就不要写了
  if (!frames_[w_frame_id]->is_dirty_) {
    return false;
  }
  // 写入磁盘应该是disk的事吧
  auto promise1 = disk_scheduler_->CreatePromise();
  auto future1 = promise1.get_future();
  auto data = frames_[w_frame_id]->data_.data();
  std::string data_copy(data);
  disk_scheduler_->Schedule({true /*Write*/, frames_[w_frame_id]->data_.data(), page_id, std::move(promise1)});
  if (future1.get()) {
    std::cout << "向page: " << page_id << " 的数据:" << data_copy << "写入磁盘成功" << std::endl;
  }
  frames_[w_frame_id]->is_dirty_ = false;
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  for (auto page : page_table_) {
    if (!FlushPage(page.first)) {
      BUSTUB_ASSERT("wrong when flushing all pages, at page id: {}.", page.first);
    }
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::scoped_lock lk(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }

  return frames_[it->second]->pin_count_;
}

// 取消对某一页的引用
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  // std::scoped_lock latch(*bpm_latch_);
  // 寻找对应的page, 进行pin_count--
  auto it = page_table_.find(page_id);
  if (it == page_table_.end() || frames_[it->second]->pin_count_ > 0) {
    return false;
  }
  if ((frames_[it->second]->pin_count_) == 0) {
    replacer_->SetEvictable(it->second, true);
  }

  return true;
}

/*
打印页表
*/
void BufferPoolManager::PrintPGTBL() {
  std::cout << "paga_table_内容为: ";
  for (auto const &elem : page_table_) {
    std::cout << "[" << elem.first << "," << elem.second << "]"
              << " ";
  }
  std::cout << std::endl;
}

/*
利用frame_id找到对应的page_id
*/
auto BufferPoolManager::FindPage(frame_id_t evicted_frame_id) -> std::optional<page_id_t> {
  page_id_t evicted_page_id = -1;
  for (auto const &elem : page_table_) {
    if (elem.second == evicted_frame_id) {
      evicted_page_id = elem.first;
      break;
    }
  }
  if (evicted_page_id != -1) {
    return evicted_page_id;
  }
  std::cout << "没找到对应的page" << std::endl;
  return std::nullopt;
}

/*
看能不能获得RW锁
*/
auto BufferPoolManager::AcquireRWLock(frame_id_t frame_id) -> bool {
  if (frames_[frame_id]->rwlatch_.try_lock()) {
    frames_[frame_id]->rwlatch_.unlock();
    return true;
  }
  bpm_latch_->unlock();
  return false;
}
}  // namespace bustub
