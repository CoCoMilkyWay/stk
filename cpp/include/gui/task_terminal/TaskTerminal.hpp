#pragma once
#include <string>
#include <vector>
#include <mutex>
#include "gui/util/Color.hpp"

// TaskTerminal - Scrolling log output (GUI window task)
// Stored in GuiState, accessed by all tasks for logging
class TaskTerminal {
public:
  struct Line {
    std::string text;
    Color color;
    
    Line(const std::string& t, const Color& c = Color::White()) 
      : text(t), color(c) {}
  };
  
private:
  std::vector<Line> lines_;
  std::mutex mutex_;
  bool auto_scroll_ = true;
  static constexpr int MAX_LINES = 1000;
  
public:
  void AddLine(const std::string& text, const Color& color = Color::White()) {
    std::lock_guard<std::mutex> lock(mutex_);
    lines_.emplace_back(text, color);
    
    // Keep buffer size under control
    if (lines_.size() > MAX_LINES) {
      lines_.erase(lines_.begin());
    }
  }
  
  void Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    lines_.clear();
  }
  
  // Thread-safe read access
  template<typename Func>
  void ReadLines(Func&& func) {
    std::lock_guard<std::mutex> lock(mutex_);
    func(lines_);
  }
  
  bool IsAutoScroll() const { return auto_scroll_; }
  void SetAutoScroll(bool enable) { auto_scroll_ = enable; }
};

