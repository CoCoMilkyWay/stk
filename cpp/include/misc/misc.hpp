#pragma once

// System headers
#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>

namespace misc {


inline void print_progress(size_t current, size_t total) {
  size_t next = current++;
  size_t step = static_cast<size_t>(total * 0.1f);
  if ((next % step == 0 || next == total - 1)) [[unlikely]] {
    const int bar_width = 50;
    float progress = static_cast<float>(current) / total;
    int pos = static_cast<int>(bar_width * progress);

    std::cout << "\r[";
    for (int i = 0; i < bar_width; ++i) {
      if (i < pos)
        std::cout << "=";
      else if (i == pos)
        std::cout << ">";
      else
        std::cout << " ";
    }
    std::cout << "] " << std::setw(3) << static_cast<size_t>(progress * 100.0f)
              << "% (" << current << "/" << total << ")" << std::flush;
  }
}

class Timer {
public:
  Timer(const std::string &label = "")
      : label_(label), start_(std::chrono::high_resolution_clock::now()) {
    std::cout << "\n";
  }

  ~Timer() {
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<float> elapsed = end - start_;
    // std::cout << "\n[Timer] " << label_ << " Elapsed time: " << elapsed.count() * 1000 << " ms\n";
    std::cout << label_ << " " << elapsed.count() * 1000 << "ms";
  }

private:
  std::string label_;
  std::chrono::time_point<std::chrono::high_resolution_clock> start_;
};

} // namespace misc
