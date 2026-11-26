#pragma once

// LabelRows — 一日内的行集合 (位 = L_1min 行): 标签列的落盘行 (writer.rows) / 等待平仓的行; 两档标签类同用

#include "features/TimeIndex.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <iterator>

struct LabelRows {
  uint64_t w[(TRADE_MINUTES_PER_DAY + 63) / 64] = {};
  constexpr void set(size_t m) { w[m / 64] |= uint64_t{1} << (m % 64); }
  constexpr bool has(size_t m) const { return w[m / 64] >> (m % 64) & 1u; }
  constexpr bool operator==(const LabelRows &) const = default;
  bool any() const {
    for (uint64_t x : w)
      if (x)
        return true;
    return false;
  }
  // 行 < n 的子集
  LabelRows below(size_t n) const {
    LabelRows r;
    for (size_t i = 0; i < std::size(w); ++i) {
      const size_t lo = i * 64;
      r.w[i] = n >= lo + 64 ? w[i] : n > lo ? w[i] & ((uint64_t{1} << (n - lo)) - 1)
                                            : 0;
    }
    return r;
  }
  // 并入 s; 返回新增行数
  size_t merge(const LabelRows &s) {
    size_t n = 0;
    for (size_t i = 0; i < std::size(w); ++i) {
      n += static_cast<size_t>(std::popcount(s.w[i] & ~w[i]));
      w[i] |= s.w[i];
    }
    return n;
  }
  // 逐行回调
  template <class F>
  void each(F &&f) const {
    for (size_t i = 0; i < std::size(w); ++i)
      for (uint64_t x = w[i]; x; x &= x - 1)
        f(i * 64 + static_cast<size_t>(std::countr_zero(x)));
  }
  // 逐行回调并清空; 返回行数
  template <class F>
  size_t drain(F &&f) {
    size_t n = 0;
    for (size_t i = 0; i < std::size(w); ++i)
      for (; w[i]; w[i] &= w[i] - 1, ++n)
        f(i * 64 + static_cast<size_t>(std::countr_zero(w[i])));
    return n;
  }
};
