#include "gui/GuiState.hpp"
#include "gui/coro/CoroManager.hpp"
#include <memory>

// Global singleton instances
static std::unique_ptr<CoroManager> g_coro_manager;

// ============================================================================
// StatusBar Implementation
// ============================================================================

void StatusBar::Show(const std::string& msg, float duration) {
  message_ = msg;
  timer_ = duration;
}

void StatusBar::Update(float dt) {
  if (timer_ > 0.0f) {
    timer_ -= dt;
  }
}

bool StatusBar::IsActive() const {
  return timer_ > 0.0f;
}

const std::string& StatusBar::GetMessage() const {
  return message_;
}

// ============================================================================
// Terminal Implementation
// ============================================================================

void Terminal::AddLine(const std::string& text, const Color& color) {
  std::lock_guard<std::mutex> lock(mutex_);
  lines_.emplace_back(text, color);
  
  // Keep buffer size under control
  if (lines_.size() > MAX_LINES) {
    lines_.erase(lines_.begin());
  }
}

void Terminal::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  lines_.clear();
}

// ============================================================================
// NetworkMonitor Implementation
// ============================================================================

NetworkMonitor& NetworkMonitor::Instance() {
  static NetworkMonitor instance;
  return instance;
}

void NetworkMonitor::Initialize(size_t num_targets) {
  std::lock_guard<std::mutex> lock(mutex_);
  target_pings_.clear();
  for (size_t i = 0; i < num_targets; ++i) {
    target_pings_.push_back(std::make_unique<std::atomic<int>>(-1));
  }
}

void NetworkMonitor::SetStatus(Status status, int best_ping) {
  status_.store(status);
  ping_ms_.store(best_ping);
}

void NetworkMonitor::SetTargetPing(size_t index, int ping_ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (index < target_pings_.size()) {
    target_pings_[index]->store(ping_ms);
  }
}

NetworkMonitor::Status NetworkMonitor::GetStatus() const {
  return status_.load();
}

int NetworkMonitor::GetPingMs() const {
  return ping_ms_.load();
}

std::vector<int> NetworkMonitor::GetTargetPings() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<int> results;
  for (const auto& ping : target_pings_) {
    results.push_back(ping->load());
  }
  return results;
}

// ============================================================================
// GuiState Implementation
// ============================================================================

void GuiState::Update(float dt) {
  // Initialize coroutine manager on first call
  if (!coro_mgr) {
    if (!g_coro_manager) {
      g_coro_manager = std::make_unique<CoroManager>();
    }
    coro_mgr = g_coro_manager.get();
  }
  
  // Process all ready coroutine events (non-blocking)
  if (coro_mgr) {
    coro_mgr->Poll();
  }
  
  // Update components
  status_bar.Update(dt);
}

CoroManager& GuiState::Coro() {
  // Ensure initialized
  if (!coro_mgr) {
    if (!g_coro_manager) {
      g_coro_manager = std::make_unique<CoroManager>();
    }
    coro_mgr = g_coro_manager.get();
  }
  return *coro_mgr;
}
