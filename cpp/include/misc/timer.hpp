#pragma once

#include <chrono>
#include <iostream>
#include <string>

namespace misc {

class Timer {
public:
  Timer(const std::string &label = "")
      : label_(label), start_(std::chrono::high_resolution_clock::now()) {
    std::cout << "\n";
  }

  ~Timer() {
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<float> elapsed = end - start_;
    std::cout << label_ << " " << elapsed.count() * 1000 << "ms";
  }

private:
  std::string label_;
  std::chrono::time_point<std::chrono::high_resolution_clock> start_;
};

} // namespace misc
