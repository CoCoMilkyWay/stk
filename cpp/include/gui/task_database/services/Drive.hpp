#pragma once

#include "fundamental/Audit.hpp"
#include "fundamental/Sync.hpp"
#include "shared/Progress.hpp"

#include <atomic>
#include <functional>
#include <iostream>
#include <string>
#include <vector>

namespace GUI::Database {

// 逐项驱动的停因 (不叫 Stop: 与 Service::Stop 撞名)
enum class Outcome { Done,
                     Cancelled,
                     Gated };

// ============================================================================
// drive — 逐项驱动骨架: 同步 (sync::Task) 与核对 (audit::Unit) 同一份 (FundamentalService / AuditService 共用);
//   取消 / 闸门 / 计步 / 日志只在这里写一次
//   每项开跑前: cancel → Cancelled; gate 非空且为 false → Gated (跳过余下全部)
//   每项: say(label) → step(x) 返回一句结果 (日志) → board.publish(1)
//   计步分母由调用方 begin (修复 = 重拉 + 复核两段共用一个分母); 在飞的一项不可中断
// ============================================================================
template <class T, class Step>
Outcome drive(Progress &board, const std::atomic<bool> &cancel, const std::string &tag, const std::vector<T> &items,
              const std::function<void(const std::string &)> &say, Step &&step, const std::function<bool()> &gate = {}) {
  using fundamental::audit::label;
  using fundamental::sync::label;
  std::cout << "[" << tag << "] " << items.size() << " item(s)" << std::endl;
  Outcome stop = Outcome::Done;
  for (const T &x : items) {
    if (cancel.load(std::memory_order_relaxed)) {
      stop = Outcome::Cancelled;
      break;
    }
    if (gate && !gate()) {
      stop = Outcome::Gated;
      break;
    }
    const std::string l = label(x);
    say(l);
    std::cout << "[" << tag << "] " << l << " ... " << std::flush;
    std::cout << step(x) << std::endl;
    board.publish(1);
  }
  static constexpr const char *kStop[] = {"done", "cancelled", "gated"};
  std::cout << "[" << tag << "] " << kStop[static_cast<int>(stop)] << std::endl;
  return stop;
}

} // namespace GUI::Database
