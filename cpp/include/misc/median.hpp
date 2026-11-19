#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <vector>

// 累计中位: 逐个 push (O(log n)), 随时取中位 (偶数个取中间两个的均值). 双堆: 较小一半大顶堆 / 较大一半小顶堆, |lo| − |hi| ∈ {0, 1}.
// 业务无关.
namespace misc {

template <class T>
class RunMedian {
  std::vector<T> lo_, hi_;

public:
  void push(T x) {
    if (lo_.empty() || x <= lo_.front())
      lo_.push_back(x), std::ranges::push_heap(lo_);
    else
      hi_.push_back(x), std::ranges::push_heap(hi_, std::greater{});
    if (lo_.size() > hi_.size() + 1) {
      std::ranges::pop_heap(lo_);
      hi_.push_back(lo_.back()), lo_.pop_back();
      std::ranges::push_heap(hi_, std::greater{});
    } else if (hi_.size() > lo_.size()) {
      std::ranges::pop_heap(hi_, std::greater{});
      lo_.push_back(hi_.back()), hi_.pop_back();
      std::ranges::push_heap(lo_);
    }
  }
  bool empty() const { return lo_.empty(); }
  size_t size() const { return lo_.size() + hi_.size(); }
  T mid() const {
    assert(!lo_.empty());
    return lo_.size() > hi_.size() ? lo_.front() : (lo_.front() + hi_.front()) / T(2);
  }
};

} // namespace misc
