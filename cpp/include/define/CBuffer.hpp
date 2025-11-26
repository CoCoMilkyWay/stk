#pragma once
#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <span>

// ┌────────────────────────────┬──────────┬───────────┬
// │ Operation                  │ Small-64 │ Large-4096│
// ├────────────────────────────┼──────────┼───────────┼
// │ push_back                  │    0.000 │     0.002 │
// │ push_front                 │    0.000 │     0.001 │
// │ pop_back                   │    0.030 │     0.027 │
// │ pop_front                  │    0.032 │     0.026 │
// │ emplace_back               │    0.000 │     0.001 │
// │ emplace_front              │    0.000 │     0.001 │
// │ erase (single)             │    4.056 │     3.841 │
// │ erase_range                │    5.103 │     4.628 │
// │ insert (single)            │    4.847 │     4.630 │
// │ insert_range               │    4.551 │     4.033 │
// │ access (operator[])        │    4.961 │     5.094 │
// │ span (head, size=10)       │   33.143 │    30.983 │
// │ span (tail, size=10)       │   29.606 │    27.827 │
// │ span (subspan, size=10)    │   28.249 │    28.160 │
// └────────────────────────────┴──────────┴───────────┴
// (Time in milliseconds, all operations: 1M iterations)

// | Feature              | (A) Circular Index              | (B) Compacted Contiguous   | (C) Double Mirror          | (D) Chained Blocks                    |
// |--------------------- |-------------------------------- |--------------------------- |--------------------------- |-------------------------------------- |
// | Logical Continuity   | May wrap (two segments)         | Guaranteed (single ptr)    | Guaranteed (single ptr)    | May break across blocks               |
// | Space Efficiency     | N                               | N                          | 2N                         | >N (block overhead)                   |
// | push_back/front      | O(1)                            | Avg O(1), worst O(n)       | O(1)                       | O(1) amortized                        |
// | pop_back/front       | O(1)                            | Avg O(1), worst O(n)       | O(1)                       | O(1)                                  |
// | operator[] Access    | O(1) + modulo                   | O(1)                       | O(1) + no modulo           | O(1) + pointer chase                  |
// | span() Operations    | May return 2 segments           | Single contiguous ptr      | Single contiguous ptr      | Multiple segments                     |
// | Insert/Erase Mid     | O(n) elem-by-elem, fast         | O(n) memmove               | O(n) memmovex2 (mirror)    | O(1) amortized                        |
// | Memory Locality      | Moderate (modulo overhead)      | Good                       | Excellent (no modulo)      | Moderate (pointer jumps)              |
// | Cache Friendliness   | Good for sequential             | Excellent                  | Excellent (pre-fetching)   | Moderate                              |
// | Best For             | General queue, balanced perf    | Compact memory footprint   | Hot-path random access     | Large buffer, frequent mid-ops        |
// | Use Cases            | Lock-free queue, event buffer   | Time-series sliding window | HFT/DSP feature window     | Log streaming, data archive           |

// CBUFFER_OPTIMIZE_FOR_FULL:
// 1: e.g. for sliding window scenarios where the buffer stays at capacity
// 0: e.g. for event queue scenarios where the buffer is often not full
#define CBUFFER_OPTIMIZE_FOR_FULL 1

#if CBUFFER_OPTIMIZE_FOR_FULL
// Optimize for full buffer (sliding window use case)
#define CBUFFER_NOT_FULL_HINT [[unlikely]]  // Buffer has space (not at capacity)
#define CBUFFER_FULL_HINT [[likely]]        // Buffer is at capacity
#define CBUFFER_HAS_SPACE_HINT [[unlikely]] // Operation has space without overflow
#define CBUFFER_NO_WRAP_HINT [[unlikely]]   // Subspan is contiguous (no wrap to array start)
#define CBUFFER_EMPTY_HINT [[unlikely]]     // Buffer is empty
#else
// Optimize for filling buffer (event accumulation use case)
#define CBUFFER_NOT_FULL_HINT [[likely]]  // Buffer has space (not at capacity)
#define CBUFFER_FULL_HINT [[unlikely]]    // Buffer is at capacity
#define CBUFFER_HAS_SPACE_HINT [[likely]] // Operation has space without overflow
#define CBUFFER_NO_WRAP_HINT [[likely]]   // Subspan is contiguous (no wrap to array start)
#define CBUFFER_EMPTY_HINT [[unlikely]]   // Buffer is empty (still unlikely even in event queue)
#endif

template <typename T, size_t N>
class CBuffer {
  static_assert(N > 0, "Capacity must be positive");

public:
  // SplitSpan: represents potentially non-contiguous memory view for circular buffer
  struct SplitSpan {
    std::span<const T> head;
    std::span<const T> tail;

    size_t size() const noexcept {
      return head.size() + tail.size();
    }
  };

  struct MutableSplitSpan {
    std::span<T> head;
    std::span<T> tail;

    size_t size() const noexcept {
      return head.size() + tail.size();
    }
  };

  // Iterator types
  class iterator {
  private:
    CBuffer *buffer_;
    size_t pos_;

  public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = T *;
    using reference = T &;

    iterator(CBuffer *buf, size_t pos) : buffer_(buf), pos_(pos) {}

    reference operator*() const { return (*buffer_)[pos_]; }
    pointer operator->() const { return &(*buffer_)[pos_]; }

    iterator &operator++() {
      ++pos_;
      return *this;
    }
    iterator operator++(int) {
      iterator tmp = *this;
      ++pos_;
      return tmp;
    }
    iterator &operator--() {
      --pos_;
      return *this;
    }
    iterator operator--(int) {
      iterator tmp = *this;
      --pos_;
      return tmp;
    }

    iterator &operator+=(difference_type n) {
      pos_ += n;
      return *this;
    }
    iterator &operator-=(difference_type n) {
      pos_ -= n;
      return *this;
    }

    iterator operator+(difference_type n) const { return iterator(buffer_, pos_ + n); }
    iterator operator-(difference_type n) const { return iterator(buffer_, pos_ - n); }
    difference_type operator-(const iterator &other) const { return pos_ - other.pos_; }

    reference operator[](difference_type n) const { return (*buffer_)[pos_ + n]; }

    bool operator==(const iterator &other) const { return buffer_ == other.buffer_ && pos_ == other.pos_; }
    bool operator!=(const iterator &other) const { return !(*this == other); }
    bool operator<(const iterator &other) const { return buffer_ == other.buffer_ && pos_ < other.pos_; }
    bool operator>(const iterator &other) const { return buffer_ == other.buffer_ && pos_ > other.pos_; }
    bool operator<=(const iterator &other) const { return buffer_ == other.buffer_ && pos_ <= other.pos_; }
    bool operator>=(const iterator &other) const { return buffer_ == other.buffer_ && pos_ >= other.pos_; }
  };

  class const_iterator {
  private:
    const CBuffer *buffer_;
    size_t pos_;

  public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = const T *;
    using reference = const T &;

    const_iterator(const CBuffer *buf, size_t pos) : buffer_(buf), pos_(pos) {}

    reference operator*() const { return (*buffer_)[pos_]; }
    pointer operator->() const { return &(*buffer_)[pos_]; }

    const_iterator &operator++() {
      ++pos_;
      return *this;
    }
    const_iterator operator++(int) {
      const_iterator tmp = *this;
      ++pos_;
      return tmp;
    }
    const_iterator &operator--() {
      --pos_;
      return *this;
    }
    const_iterator operator--(int) {
      const_iterator tmp = *this;
      --pos_;
      return tmp;
    }

    const_iterator &operator+=(difference_type n) {
      pos_ += n;
      return *this;
    }
    const_iterator &operator-=(difference_type n) {
      pos_ -= n;
      return *this;
    }

    const_iterator operator+(difference_type n) const { return const_iterator(buffer_, pos_ + n); }
    const_iterator operator-(difference_type n) const { return const_iterator(buffer_, pos_ - n); }
    difference_type operator-(const const_iterator &other) const { return pos_ - other.pos_; }

    reference operator[](difference_type n) const { return (*buffer_)[pos_ + n]; }

    bool operator==(const const_iterator &other) const { return buffer_ == other.buffer_ && pos_ == other.pos_; }
    bool operator!=(const const_iterator &other) const { return !(*this == other); }
    bool operator<(const const_iterator &other) const { return buffer_ == other.buffer_ && pos_ < other.pos_; }
    bool operator>(const const_iterator &other) const { return buffer_ == other.buffer_ && pos_ > other.pos_; }
    bool operator<=(const const_iterator &other) const { return buffer_ == other.buffer_ && pos_ <= other.pos_; }
    bool operator>=(const const_iterator &other) const { return buffer_ == other.buffer_ && pos_ >= other.pos_; }
  };

  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

private:
  std::array<T, N> data_;
  size_t head_ = 0; // Start position
  size_t size_ = 0; // Number of elements

  // Convert logical index to physical array index
  [[nodiscard]] [[gnu::always_inline]] constexpr size_t to_physical_index(size_t logical_idx) const noexcept {
    const size_t result = head_ + logical_idx;
    return (result >= N) ? (result - N) : result;
  }

public:
  // ========== 1. Constructors ==========

  CBuffer() = default;

  explicit CBuffer(size_t initial_size) : size_(initial_size > N ? N : initial_size) {
    // Value-initialize elements to avoid undefined behavior for POD types
    for (size_t i = 0; i < size_; ++i) {
      data_[i] = T{};
    }
  }

  // ========== 2. Capacity Queries ==========

  [[gnu::always_inline]] size_t size() const noexcept {
    return size_;
  }

  [[gnu::always_inline]] constexpr size_t capacity() const noexcept {
    return N;
  }

  [[gnu::always_inline]] bool empty() const noexcept {
    return size_ == 0;
  }

  [[gnu::always_inline]] bool full() const noexcept {
    return size_ == N;
  }

  // ========== 3. Element Access ==========

  [[gnu::always_inline]] T &front() {
    return data_[head_];
  }

  [[gnu::always_inline]] const T &front() const {
    return data_[head_];
  }

  [[gnu::always_inline]] T &back() {
    return data_[to_physical_index(size_ - 1)];
  }

  [[gnu::always_inline]] const T &back() const {
    return data_[to_physical_index(size_ - 1)];
  }

  [[gnu::always_inline]] T &operator[](size_t index) noexcept {
    const size_t physical = head_ + index;
    return data_[(physical >= N) ? (physical - N) : physical];
  }

  [[gnu::always_inline]] const T &operator[](size_t index) const noexcept {
    const size_t physical = head_ + index;
    return data_[(physical >= N) ? (physical - N) : physical];
  }

  // ========== 4. Single Element Insertion ==========

  [[gnu::always_inline]] void push_front(const T &value) {
    head_ = (head_ == 0) ? (N - 1) : (head_ - 1);
    data_[head_] = value;
    if (size_ < N)
      CBUFFER_NOT_FULL_HINT {
        ++size_;
      }
  }

  [[gnu::always_inline]] void push_front(T &&value) {
    head_ = (head_ == 0) ? (N - 1) : (head_ - 1);
    data_[head_] = std::move(value);
    if (size_ < N)
      CBUFFER_NOT_FULL_HINT {
        ++size_;
      }
  }

  [[gnu::always_inline]] void push_back(const T &value) {
    if (size_ < N)
      CBUFFER_NOT_FULL_HINT {
        const size_t idx = head_ + size_;
        data_[(idx >= N) ? (idx - N) : idx] = value;
        ++size_;
      }
    else {
      data_[head_] = value;
      head_ = (head_ == N - 1) ? 0 : (head_ + 1);
    }
  }

  [[gnu::always_inline]] void push_back(T &&value) {
    if (size_ < N)
      CBUFFER_NOT_FULL_HINT {
        const size_t idx = head_ + size_;
        data_[(idx >= N) ? (idx - N) : idx] = std::move(value);
        ++size_;
      }
    else {
      data_[head_] = std::move(value);
      head_ = (head_ == N - 1) ? 0 : (head_ + 1);
    }
  }

  template <typename... Args>
  [[gnu::always_inline]] void emplace_front(Args &&...args) {
    head_ = (head_ == 0) ? (N - 1) : (head_ - 1);
    data_[head_] = T(std::forward<Args>(args)...);
    if (size_ < N)
      CBUFFER_NOT_FULL_HINT {
        ++size_;
      }
  }

  template <typename... Args>
  [[gnu::always_inline]] void emplace_back(Args &&...args) {
    if (size_ < N)
      CBUFFER_NOT_FULL_HINT {
        const size_t idx = head_ + size_;
        data_[(idx >= N) ? (idx - N) : idx] = T(std::forward<Args>(args)...);
        ++size_;
      }
    else {
      data_[head_] = T(std::forward<Args>(args)...);
      head_ = (head_ == N - 1) ? 0 : (head_ + 1);
    }
  }

  // ========== 5. Single Element Removal ==========

  [[gnu::always_inline]] void pop_front() noexcept {
    // Hot path: use assert only (zero cost in release)
    assert(size_ > 0 && "pop_front() on empty buffer");
    head_ = (head_ == N - 1) ? 0 : (head_ + 1);
    --size_;
  }

  [[gnu::always_inline]] void pop_back() noexcept {
    // Hot path: use assert only (zero cost in release)
    assert(size_ > 0 && "pop_back() on empty buffer");
    --size_;
  }

  // ========== 6. Batch Operations ==========

  [[gnu::always_inline]] void remove_front(size_t n) noexcept {
    // Hot path: assert + graceful clamp
    assert(n <= size_ && "remove_front(n) where n > size");
    if (n >= size_)
      CBUFFER_EMPTY_HINT {
        clear();
        return;
      }
    const size_t new_head = head_ + n;
    head_ = (new_head >= N) ? (new_head - N) : new_head;
    size_ -= n;
  }

  [[gnu::always_inline]] void remove_back(size_t n) noexcept {
    // Hot path: assert + graceful clamp
    assert(n <= size_ && "remove_back(n) where n > size");
    if (n >= size_)
      CBUFFER_EMPTY_HINT {
        clear();
        return;
      }
    size_ -= n;
  }

  // ========== 7. Range Insertion & Deletion ==========

  // NOTE: insert() overflow policy: when buffer is full, drop newest (tail) element first
  // by implicitly calling pop_back(). This keeps index semantics consistent between full
  // and non-full cases: index always refers to the same logical position relative to the
  // anchor point (index=0). This maintains sliding-window semantics where the head (oldest
  // data) is preserved and the tail is sacrificed. If you need different semantics, check
  // size() < capacity() before calling insert().

  void insert(size_t index, const T &value) {
    if (!(index <= size_)) [[unlikely]] {
      assert(false && "insert() index out of range");
    }

    // Full buffer: implicitly pop_back() to make space
    // This keeps index semantics consistent (index=0 anchor unchanged)
    if (size_ >= N)
      CBUFFER_FULL_HINT {
        --size_;
      }

    // Unified insertion logic: shift [index, size) right by 1
    size_t old_size = size_;
    ++size_;
    for (size_t i = old_size; i-- > index;) {
      (*this)[i + 1] = (*this)[i];
    }
    (*this)[index] = value;
  }

  void insert(size_t index, T &&value) {
    if (!(index <= size_)) [[unlikely]] {
      assert(false && "insert() index out of range");
    }

    // Full buffer: implicitly pop_back() to make space
    // This keeps index semantics consistent (index=0 anchor unchanged)
    if (size_ >= N)
      CBUFFER_FULL_HINT {
        --size_;
      }

    // Unified insertion logic: shift [index, size) right by 1
    size_t old_size = size_;
    ++size_;
    for (size_t i = old_size; i-- > index;) {
      (*this)[i + 1] = (*this)[i];
    }
    (*this)[index] = std::move(value);
  }

  template <typename InputIt>
  void insert_range(size_t index, InputIt first, InputIt last) {
    if (!(index <= size_)) [[unlikely]] {
      assert(false && "insert_range() index out of range");
    }

    size_t count = std::distance(first, last);
    if (count == 0) [[unlikely]]
      return;

    if (size_ + count <= N)
      CBUFFER_HAS_SPACE_HINT {
        // Completely fits: shift right and insert
        size_t old_size = size_;
        size_ += count;
        // Shift elements right by count positions
        for (size_t i = old_size; i-- > index;) {
          (*this)[i + count] = (*this)[i];
        }
        // Insert new elements
        size_t i = index;
        for (auto it = first; it != last; ++it, ++i) {
          (*this)[i] = *it;
        }
      }
    else {
      // Overflow: drop newest (tail) elements to make space
      // This keeps index semantics consistent (index=0 anchor unchanged)
      size_t excess = (size_ + count) - N;

      // Implicitly pop_back 'excess' elements
      if (excess > 0) {
        size_ -= excess;
      }

      // Now insert as usual (guaranteed to fit)
      size_t old_size = size_;
      size_t insert_count = (count <= N - size_) ? count : (N - size_);
      size_ += insert_count;

      // Shift right
      for (size_t i = old_size; i-- > index;) {
        (*this)[i + insert_count] = (*this)[i];
      }

      // Insert
      size_t i = index;
      auto it = first;
      for (size_t j = 0; j < insert_count && it != last; ++j, ++it, ++i) {
        (*this)[i] = *it;
      }
    }
  }

  void erase(size_t index) {
    if (index >= size_) [[unlikely]]
      return;
    // Dense-pack strategy: keep [0, index) unchanged (anchor at index=0)
    // Move suffix [index+1, size) left by 1 to fill the gap
    const size_t move_count = size_ - index - 1;
    for (size_t i = 0; i < move_count; ++i) {
      (*this)[index + i] = std::move((*this)[index + i + 1]);
    }
    --size_;
  }

  void erase_range(size_t start, size_t count) {
    if (count == 0) [[unlikely]]
      return;
    if (start >= size_) [[unlikely]]
      return;
    if (start + count > size_)
      count = size_ - start;
    // Dense-pack strategy: keep [0, start) unchanged (anchor at index=0)
    // Move suffix [start+count, size) left by count to fill the gap
    const size_t move_count = size_ - start - count;
    for (size_t i = 0; i < move_count; ++i) {
      (*this)[start + i] = std::move((*this)[start + i + count]);
    }
    size_ -= count;
  }

  // ========== 8. View Operations ==========

  [[gnu::always_inline]] SplitSpan subspan(size_t logical_start, size_t length) const noexcept {
    assert(logical_start + length <= size_ && "subspan(): range exceeds buffer size");

    const size_t physical_start = head_ + logical_start;
    size_t actual_start = (physical_start >= N) ? (physical_start - N) : physical_start;

    const size_t remaining_space = N - actual_start;

    // For small subspans or when starting near head_, no wrap is more likely
    // For full buffer spans in sliding window, wrap is expected (unless head_=0)
    if (length <= remaining_space)
      CBUFFER_NO_WRAP_HINT {
        return {std::span(data_.data() + actual_start, length), {}};
      }
    return {
        std::span(data_.data() + actual_start, remaining_space),
        std::span(data_.data(), length - remaining_space)};
  }

  [[gnu::always_inline]] MutableSplitSpan subspan(size_t logical_start, size_t length) noexcept {
    assert(logical_start + length <= size_ && "subspan(): range exceeds buffer size");

    const size_t physical_start = head_ + logical_start;
    size_t actual_start = (physical_start >= N) ? (physical_start - N) : physical_start;

    const size_t remaining_space = N - actual_start;

    // For small subspans or when starting near head_, no wrap is more likely
    // For full buffer spans in sliding window, wrap is expected (unless head_=0)
    if (length <= remaining_space)
      CBUFFER_NO_WRAP_HINT {
        return {std::span(data_.data() + actual_start, length), {}};
      }
    return {
        std::span(data_.data() + actual_start, remaining_space),
        std::span(data_.data(), length - remaining_space)};
  }

  [[gnu::always_inline]] SplitSpan span() const noexcept {
    if (size_ == 0)
      CBUFFER_EMPTY_HINT {
        return SplitSpan{};
      }
    return subspan(0, size_);
  }

  [[gnu::always_inline]] MutableSplitSpan span() noexcept {
    if (size_ == 0)
      CBUFFER_EMPTY_HINT {
        return MutableSplitSpan{};
      }
    return subspan(0, size_);
  }

  [[gnu::always_inline]] SplitSpan head(size_t n) const noexcept {
    if (n == 0) [[unlikely]] {
      return SplitSpan{};
    }
    return subspan(0, n);
  }

  [[gnu::always_inline]] MutableSplitSpan head(size_t n) noexcept {
    if (n == 0) [[unlikely]] {
      return MutableSplitSpan{};
    }
    return subspan(0, n);
  }

  [[gnu::always_inline]] SplitSpan tail(size_t n) const noexcept {
    if (n == 0) [[unlikely]] {
      return SplitSpan{};
    }
    return subspan(size_ - n, n);
  }

  [[gnu::always_inline]] MutableSplitSpan tail(size_t n) noexcept {
    if (n == 0) [[unlikely]] {
      return MutableSplitSpan{};
    }
    return subspan(size_ - n, n);
  }

  template <size_t M>
  std::array<T, M> to_array(size_t logical_start = 0) const {
    assert(logical_start + M <= size_ && "to_array(): length exceeds buffer size");
    auto split = subspan(logical_start, M);
    std::array<T, M> result;
    std::copy(split.head.begin(), split.head.end(), result.begin());
    std::copy(split.tail.begin(), split.tail.end(), result.begin() + split.head.size());
    return result;
  }

  // ========== 9. Iterators ==========

  iterator begin() {
    return iterator(this, 0);
  }

  iterator end() {
    return iterator(this, size_);
  }

  const_iterator begin() const {
    return const_iterator(this, 0);
  }

  const_iterator end() const {
    return const_iterator(this, size_);
  }

  reverse_iterator rbegin() {
    return reverse_iterator(end());
  }

  reverse_iterator rend() {
    return reverse_iterator(begin());
  }

  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(end());
  }

  const_reverse_iterator rend() const {
    return const_reverse_iterator(begin());
  }

  // ========== 10. Clear ==========

  [[gnu::always_inline]] void clear() noexcept {
    size_ = 0;
    head_ = 0;
  }
};
