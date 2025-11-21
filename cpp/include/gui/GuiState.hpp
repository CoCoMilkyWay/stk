#pragma once
#include <atomic>
#include <string>
#include <vector>
#include <mutex>
#include <memory>

class CoroManager;

// ============================================================================
// Color Definitions
// ============================================================================

struct Color {
  float r = 1.0f, g = 1.0f, b = 1.0f, a = 1.0f;
  
  Color() = default;
  Color(float r_, float g_, float b_, float a_ = 1.0f) : r(r_), g(g_), b(b_), a(a_) {}
  
  static Color White()  { return {1.0f, 1.0f, 1.0f, 1.0f}; }
  static Color Green()  { return {0.0f, 1.0f, 0.0f, 1.0f}; }
  static Color Red()    { return {1.0f, 0.0f, 0.0f, 1.0f}; }
  static Color Yellow() { return {1.0f, 1.0f, 0.0f, 1.0f}; }
  static Color Blue()   { return {0.3f, 0.7f, 1.0f, 1.0f}; }
  static Color Gray()   { return {0.5f, 0.5f, 0.5f, 1.0f}; }
};

// ============================================================================
// Status Bar - Temporary status messages
// ============================================================================

class StatusBar {
  std::string message_;
  float timer_ = 0.0f;
  
public:
  void Show(const std::string& msg, float duration = 3.0f);
  void Update(float dt);
  bool IsActive() const;
  const std::string& GetMessage() const;
};

// ============================================================================
// Terminal - Scrolling log output
// ============================================================================

class Terminal {
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
  void AddLine(const std::string& text, const Color& color = Color::White());
  void Clear();
  
  // Thread-safe read access
  template<typename Func>
  void ReadLines(Func&& func) {
    std::lock_guard<std::mutex> lock(mutex_);
    func(lines_);
  }
  
  bool IsAutoScroll() const { return auto_scroll_; }
  void SetAutoScroll(bool enable) { auto_scroll_ = enable; }
};

// ============================================================================
// Network Monitor - Network connectivity status
// ============================================================================

class NetworkMonitor {
public:
  enum class Status : int { Unknown, Good, Medium, Bad, Error };
  
private:
  std::atomic<Status> status_{Status::Unknown};
  std::atomic<int> ping_ms_{-1};  // Best ping (for color display)
  std::vector<std::unique_ptr<std::atomic<int>>> target_pings_;  // Individual target results
  mutable std::mutex mutex_;  // Protect vector operations
  
public:
  static NetworkMonitor& Instance();
  
  // Initialize with number of targets
  void Initialize(size_t num_targets);
  
  // Update overall status and best ping
  void SetStatus(Status status, int best_ping);
  
  // Update individual target result
  void SetTargetPing(size_t index, int ping_ms);
  
  // Get status
  Status GetStatus() const;
  int GetPingMs() const;
  
  // Get individual target results
  std::vector<int> GetTargetPings() const;
};

// ============================================================================
// GUI State - Main state container
// ============================================================================

struct GuiState {
  StatusBar status_bar;
  Terminal terminal;
  
  // Coroutine manager (encapsulated)
  CoroManager* coro_mgr = nullptr;
  
  void Update(float dt);
  CoroManager& Coro();
};
