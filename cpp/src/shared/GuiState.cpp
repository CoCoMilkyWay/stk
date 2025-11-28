#include "shared/GuiState.hpp"

void GuiState::Update(float /*dt*/) {
  // Process all ready coroutine events (non-blocking)
  coro_mgr.Poll();
}

CoroManager &GuiState::Coro() {
  return coro_mgr;
}
