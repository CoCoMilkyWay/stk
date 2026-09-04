#pragma once

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace misc {

// Single-line progress bar with in-place update using \r
inline void print_progress(size_t current, size_t total, const std::string &message = "") {
  const int bar_width = 40;
  float progress = static_cast<float>(current) / total;
  int pos = static_cast<int>(bar_width * progress);

  // Build complete output string to avoid threading issues
  std::ostringstream output;
  output << "\r[";
  for (int i = 0; i < bar_width; ++i) {
    if (i < pos)
      output << "=";
    else if (i == pos)
      output << ">";
    else
      output << " ";
  }
  output << "] " << std::setw(3) << static_cast<size_t>(progress * 100.0f)
         << "% (" << current << "/" << total << ")";
  if (!message.empty()) {
    output << " " << message;
  }

  std::cout << output.str() << std::flush;
  if (current == total) {
    std::cout << "\n";
  }
}

} // namespace misc
