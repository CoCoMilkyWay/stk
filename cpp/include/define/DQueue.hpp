#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <span>
#include <utility>

// DQueue<double, 100> dq;
// dq.push_back(1.0);
// dq.push_front(0.5);
// dq.pop_front();
// dq.pop_back();
// auto view = dq.span();
// for (double v : view.head) { /* ... */ }
// for (double v : view.tail) { /* ... */ }

template <typename T, size_t N>
class DQueue // Fixed-capacity double-ended queue on a circular buffer
{
  static_assert(N > 0, "Capacity must be positive");

private:
  std::array<T, N> data_{};
  size_t start_ = 0; // physical index of the logical front
  size_t size_ = 0;  // number of valid elements

  static constexpr size_t capacity_ = N;

  // Fast wrapping helpers to avoid modulo in hot paths
  static inline size_t wrap_add(size_t base, size_t add) noexcept {
    size_t idx = base + add;
    if (idx >= capacity_) {
      idx -= capacity_;
    }
    return idx;
  }

  static inline size_t wrap_inc(size_t idx) noexcept {
    return (idx + 1u < capacity_) ? (idx + 1u) : 0u;
  }

  static inline size_t wrap_dec(size_t idx) noexcept {
    return (idx == 0u) ? (capacity_ - 1u) : (idx - 1u);
  }

public:
  struct SplitSpan {
    std::span<const T> head;
    std::span<const T> tail;

    size_t size() const noexcept { return head.size() + tail.size(); }
  };

  // Push to back; if full, overwrite front (drop oldest)
  void push_back(const T &value) {
    if (size_ < capacity_) [[unlikely]] {
      data_[wrap_add(start_, size_)] = value;
      ++size_;
    } else {
      data_[start_] = value;
      start_ = wrap_inc(start_);
    }
  }

  void push_back(T &&value) {
    if (size_ < capacity_) [[unlikely]] {
      data_[wrap_add(start_, size_)] = std::move(value);
      ++size_;
    } else {
      data_[start_] = std::move(value);
      start_ = wrap_inc(start_);
    }
  }

  // Push to front; if full, overwrite back (drop newest)
  void push_front(const T &value) {
    if (size_ < capacity_) [[unlikely]] {
      start_ = wrap_dec(start_);
      data_[start_] = value;
      ++size_;
    } else {
      start_ = wrap_dec(start_);
      data_[start_] = value;
    }
  }

  void push_front(T &&value) {
    if (size_ < capacity_) [[unlikely]] {
      start_ = wrap_dec(start_);
      data_[start_] = std::move(value);
      ++size_;
    } else {
      start_ = wrap_dec(start_);
      data_[start_] = std::move(value);
    }
  }

  // Pop operations use assertions only (no error handling as requested)
  void pop_front() {
    assert(size_ > 0);
    start_ = wrap_inc(start_);
    --size_;
  }

  void pop_back() {
    assert(size_ > 0);
    --size_;
  }

  // Access
  T &front() {
    assert(size_ > 0);
    return data_[start_];
  }

  const T &front() const {
    assert(size_ > 0);
    return data_[start_];
  }

  T &back() {
    assert(size_ > 0);
    const size_t last_index = wrap_add(start_, size_ - 1);
    return data_[last_index];
  }

  const T &back() const {
    assert(size_ > 0);
    const size_t last_index = wrap_add(start_, size_ - 1);
    return data_[last_index];
  }

  // Views
  SplitSpan span() const noexcept { return subspan(0, size_); }

  SplitSpan last(size_t count) const {
    assert(count <= size_);
    return subspan(size_ - count, count);
  }

  SplitSpan subspan(size_t logical_start, size_t length) const {
    if (length == 0) [[unlikely]] {
      return SplitSpan{};
    }
    assert(logical_start + length <= size_);

    const size_t physical_start = wrap_add(start_, logical_start);
    const size_t contig_size = capacity_ - physical_start;

    if (length <= contig_size) [[likely]] {
      return {std::span<const T>(data_.data() + physical_start, length), {}};
    }
    return {
        std::span<const T>(data_.data() + physical_start, contig_size),
        std::span<const T>(data_.data(), length - contig_size)};
  }

  template <size_t M>
  std::array<T, M> to_array(size_t logical_start) const {
    auto split = subspan(logical_start, M);
    std::array<T, M> arr{};
    std::copy(split.head.begin(), split.head.end(), arr.begin());
    std::copy(split.tail.begin(), split.tail.end(), arr.begin() + split.head.size());
    return arr;
  }

  // State
  size_t size() const noexcept { return size_; }
  bool full() const noexcept { return size_ == capacity_; }
};
