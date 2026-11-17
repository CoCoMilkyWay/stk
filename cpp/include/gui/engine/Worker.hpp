// Worker — 因子服务异步线程骨架 FactorWorker (Run / Search / Inspect / Portfolio 共用):
//   自包含值句柄 flow() + 统一中断收尾 settle(Rc).
#pragma once

#include "gui/engine/Load.hpp"
#include "gui/util/Service.hpp"

#include <mutex>
#include <string>

namespace GUI::Engine {

template <class D, class Req>
class FactorWorker : public Service<D, Req>, public Published {
public:
  std::string message; // 本轮说明 (UI 持 mutex 读)

protected:
  explicit FactorWorker(Prio prio) : prio_(prio) {}
  Progress &board() { return *this; }
  FactorFlow flow() { return {this->cancel_, board(), prio_}; }
  bool settle(Rc rc) {
    if (!aborted(rc))
      return true;
    if (rc == Rc::Stale) {
      Store::get().clear();
      std::lock_guard<std::mutex> lock(this->mutex);
      message = kStaleMsg;
    }
    return false;
  }

private:
  const Prio prio_;
};

} // namespace GUI::Engine
