#pragma once
#include <string>
#include <vector>
#include <mutex>

// GUI-specific state (not business data)
struct GuiState {
  std::string status_msg = "";
  float status_timer = 0.0f;
  
  // Terminal buffer
  std::vector<std::string> terminal_lines;
  std::mutex terminal_mutex;
  bool terminal_auto_scroll = true;
  static constexpr int MAX_TERMINAL_LINES = 1000;
  
  void SetStatusMessage(const std::string &msg, float duration = 3.0f) {
    status_msg = msg;
    status_timer = duration;
  }
  
  void Update(float dt) {
    if (status_timer > 0.0f) {
      status_timer -= dt;
    }
  }
  
  bool HasActiveStatus() const {
    return status_timer > 0.0f;
  }
  
  void AddTerminalLog(const std::string &line) {
    std::lock_guard<std::mutex> lock(terminal_mutex);
    terminal_lines.push_back(line);
    
    // Keep buffer size under control
    if (terminal_lines.size() > MAX_TERMINAL_LINES) {
      terminal_lines.erase(terminal_lines.begin());
    }
  }
  
  void ClearTerminal() {
    std::lock_guard<std::mutex> lock(terminal_mutex);
    terminal_lines.clear();
  }
};

