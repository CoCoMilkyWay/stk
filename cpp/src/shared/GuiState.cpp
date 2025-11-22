#include "shared/GuiState.hpp"
#include "gui/coro/CoroManager.hpp"
#include <memory>

// Global singleton for coroutine manager
static std::unique_ptr<CoroManager> g_coro_manager;

void GuiState::Update(float /*dt*/) {
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
}

CoroManager &GuiState::Coro() {
  // Ensure initialized
  if (!coro_mgr) {
    if (!g_coro_manager) {
      g_coro_manager = std::make_unique<CoroManager>();
    }
    coro_mgr = g_coro_manager.get();
  }
  return *coro_mgr;
}
