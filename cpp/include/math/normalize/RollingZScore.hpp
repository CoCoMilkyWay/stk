#pragma once
#include <cmath>

// include <iostream>
// include "RollingZScore.hpp"
//
// int  main() {
//    RollingZScore<float, 3> rz;
//    float data[] = {1, 2, 2, 3, 4, 6, 5, 4, 3, 2};
//
//    for (float x : data) {
//        float z = rz.update(x);
//        std::cout << z << "\n";
//    }
//

template <typename T, size_t N>
class RollingZScore {
public:
  explicit RollingZScore()
      : buf{}, idx(0), count(0), M2(0), mean(0), stddev(0), zs(0) {}

  // Update with new value, return z-score
  inline T update(T x) noexcept {

    T old = buf[idx];
    buf[idx] = x;
    idx = (idx + 1) % N;

    if (count < N) [[unlikely]] {
      count++;
      T delta = x - mean;
      mean += delta / count;
      M2 += delta * (x - mean);
    } else {
      T old_mean = mean;
      mean += (x - old) / N;
      M2 += (x - old) * (x - mean + old - old_mean);
    }

    T variance = (count > 1) ? M2 / (count - 1) : T(0.0);
    stddev = std::sqrt(variance);
    zs = (stddev > 1e-12) ? (x - mean) / stddev : T(0);
    return zs;
  }

  // Constant-time accessors
  inline T get_mean() const noexcept { return mean; }
  inline T get_stddev() const noexcept { return stddev; }
  inline T get_zscore() const noexcept { return zs; }

public:
private:
  T buf[N]; // circular buffer
  size_t idx;
  size_t count;
  T M2;
  T mean;
  T stddev;
  T zs;
};