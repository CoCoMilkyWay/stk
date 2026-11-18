// 通达信适配: 轮询原语 — 等客户端 (内存 / 界面 / shim) 到某状态; 适配里的等待一律经此
//   poll   每 gap 秒调 fn() 一次, 为真即回其值, timeout 秒内没等到回空 (调用方另有退路)
//   until  同上, 没等到 → Fail (what() 超时时才求值, 好报出当时的状态)
#pragma once

#include "live/Broker.hpp"

#include <chrono>
#include <functional>
#include <string>
#include <thread>
#include <utility>

namespace live::tdx {

using Clock = std::chrono::steady_clock;
using Secs = std::chrono::duration<double>;

template <class F>
auto poll(F &&fn, double timeout, double gap = 0.05) {
  const auto end = Clock::now() + Secs(timeout);
  while (true) {
    auto r = fn();
    if (r || Clock::now() >= end)
      return r;
    std::this_thread::sleep_for(Secs(gap));
  }
}

template <class F>
auto until(const std::function<std::string()> &what, F &&fn, double timeout, double gap = 0.05) {
  auto r = poll(std::forward<F>(fn), timeout, gap);
  if (!r)
    fail(strf("%gs 内未等到%s", timeout, what().c_str()));
  return r;
}

inline std::function<std::string()> said(std::string s) {
  return [s = std::move(s)] { return s; };
}

} // namespace live::tdx
