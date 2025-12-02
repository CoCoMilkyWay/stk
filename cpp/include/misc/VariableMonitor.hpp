#pragma once

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iostream>
#include <string>
#include <tuple>
#include <vector>

// VariableMonitor mon;
// mon.feed(daily_bar_count);
// mon.print("daily_bar_count");

class VariableMonitor {
public:
  explicit VariableMonitor(size_t reserve_n = 0) {
    if (reserve_n > 0)
      history_.reserve(reserve_n);
  }

  template <typename T>
  inline void feed(T value) {
    float v = static_cast<float>(value);
    history_.push_back(v);
    ++count_;
    sum_ += v;
    sumsq_ += static_cast<float>(v) * static_cast<float>(v);
  }

  inline size_t size() const { return count_; }

  inline float mean() const {
    assert(count_ > 0);
    return static_cast<float>(sum_ / static_cast<float>(count_));
  }

  inline float stddev() const {
    assert(count_ > 0);
    float m = sum_ / static_cast<float>(count_);
    float variance = (sumsq_ / static_cast<float>(count_)) - m * m;
    return static_cast<float>(std::sqrt(variance));
  }

  inline float p10() const { return percentile_(0.10); }
  inline float p90() const { return percentile_(0.90); }

  inline std::tuple<float, float, float, float> stats() const {
    float m = mean();
    float s = stddev();
    float q10 = p10();
    float q90 = p90();
    return {m, s, q10, q90};
  }

  inline void print(const std::string &label = "variable") const {
    auto [m, s, q10, q90] = stats();
    std::cout << label << " n=" << size() << " mean=" << m
              << " stddev=" << s << " p10=" << q10 << " p90=" << q90 << "\n";
  }

  inline void clear() {
    history_.clear();
    count_ = 0;
    sum_ = 0.0;
    sumsq_ = 0.0;
  }

private:
  inline float percentile_(float q) const {
    assert(count_ > 0);
    size_t idx = static_cast<size_t>(q * static_cast<float>(count_ - 1));
    std::vector<float> tmp = history_;
    std::nth_element(tmp.begin(), tmp.begin() + static_cast<std::ptrdiff_t>(idx), tmp.end());
    return tmp[idx];
  }

  std::vector<float> history_;
  size_t count_ = 0;
  float sum_ = 0.0;
  float sumsq_ = 0.0;
};