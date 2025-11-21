#pragma once
#include <string>
#include <vector>
#include <mutex>

// Color struct for terminal text
struct TerminalColor {
  float r = 1.0f, g = 1.0f, b = 1.0f, a = 1.0f;  // Default: white
  
  TerminalColor() = default;
  TerminalColor(float r_, float g_, float b_, float a_ = 1.0f) : r(r_), g(g_), b(b_), a(a_) {}
  
  // Predefined colors
  static TerminalColor White() { return {1.0f, 1.0f, 1.0f, 1.0f}; }
  static TerminalColor Green() { return {0.0f, 1.0f, 0.0f, 1.0f}; }
  static TerminalColor Red() { return {1.0f, 0.0f, 0.0f, 1.0f}; }
  static TerminalColor Yellow() { return {1.0f, 1.0f, 0.0f, 1.0f}; }
  static TerminalColor Blue() { return {0.3f, 0.7f, 1.0f, 1.0f}; }
  static TerminalColor Gray() { return {0.5f, 0.5f, 0.5f, 1.0f}; }
};

// Terminal line with color
struct TerminalLine {
  std::string text;
  TerminalColor color;
  
  TerminalLine(const std::string& t, const TerminalColor& c = TerminalColor::White()) 
    : text(t), color(c) {}
};

// GUI-specific state (not business data)
struct GuiState {
  std::string status_msg = "";
  float status_timer = 0.0f;
  
  // Terminal buffer
  std::vector<TerminalLine> terminal_lines;
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
  
  void AddTerminalLog(const std::string &line, const TerminalColor &color = TerminalColor::White()) {
    std::lock_guard<std::mutex> lock(terminal_mutex);
    terminal_lines.emplace_back(line, color);
    
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

