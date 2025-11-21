#include "gui/GuiTask.hpp"
#include "gui/coro/CoroManager.hpp"

void TaskWithCoroutines::AddCoroutine(std::unique_ptr<CoroutineHandle> handle) {
  coroutines_.push_back(std::move(handle));
}

void TaskWithCoroutines::CancelAllCoroutines() {
  for (auto& coro : coroutines_) {
    if (coro) {
      coro->Cancel();
    }
  }
  coroutines_.clear();
}

TaskWithCoroutines::~TaskWithCoroutines() {
  CancelAllCoroutines();
}

