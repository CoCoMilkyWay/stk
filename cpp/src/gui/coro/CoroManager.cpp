#include "gui/coro/CoroManager.hpp"

// ============================================================================
// CoroutineHandle Implementation
// ============================================================================

CoroutineHandle::CoroutineHandle() 
  : cancel_signal_(std::make_shared<asio::cancellation_signal>()) {
}

CoroutineHandle::~CoroutineHandle() {
  Cancel();
}

void CoroutineHandle::Cancel() {
  if (cancel_signal_) {
    cancel_signal_->emit(asio::cancellation_type::terminal);
  }
}

// ============================================================================
// CoroManager Implementation
// ============================================================================

CoroManager::CoroManager() = default;

CoroManager::~CoroManager() = default;

void CoroManager::Poll() {
  io_ctx_.poll();
}

asio::io_context& CoroManager::GetIoContext() {
  return io_ctx_;
}

