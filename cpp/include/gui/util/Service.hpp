#pragma once

#include "misc/profiler.hpp"
#include "shared/Progress.hpp"

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

namespace GUI {

// ============================================================================
// Service — GUI 服务的后台单线程骨架: 最新请求覆盖旧的 (全部 GUI 服务同一份)
// ============================================================================
//   GUI 线程:  Request(r)       覆盖挂起请求 + 取消在跑 (不起线程)
//              RequestCancel()  只中断在跑, 不丢挂起
//              Start() / Stop() 起 / 停线程 (幂等); 挂起请求跨 Stop 存活, 下次 Start 续算
//   worker:    摘最新请求 → board().begin(kStart) → run(req) → board().end(完成?)
//
// Derived 提供 (缺一编译错):
//   static constexpr const char *kName;   线程名 (profiler)
//   static constexpr Phase kStart;        起始阶段
//   Progress &board();                    进度面 (流式分析 = SharedData 里的目标; 因子服务 = 自身)
//   bool run(Req &req);                   false = 取消: 在抢任务处查 cancel_ 放弃
//   ~Derived() { Stop(); }                先停线程再拆成员 (worker 正用着它们)
// 生命周期二选一: 随对象 (ctor Start / dtor Stop, 因子服务) 或随任务 (UI 输入就绪 Start(data), 流式分析)
// worker 私有常驻件 (reader / 缓冲 / 线程池) 是 Derived 的成员, 只 worker 线程碰.
// ============================================================================
template <class Derived, class Req>
class Service {
public:
  Service() = default;
  ~Service() { assert(!thread_.joinable() && "Derived 析构须先 Stop: worker 还在用派生类成员"); }
  Service(const Service &) = delete;
  Service &operator=(const Service &) = delete;

  void Start() {
    if (thread_.joinable())
      return;
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread(&Service::loop, this);
  }
  void Stop() {
    if (!thread_.joinable())
      return;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stop_.store(true, std::memory_order_relaxed);
      cancel_.store(true, std::memory_order_relaxed);
    }
    cv_.notify_all();
    thread_.join();
  }

  void Request(Req req) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      pending_ = std::move(req);
      cancel_.store(true, std::memory_order_relaxed);
    }
    cv_.notify_all();
  }
  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }

  bool is_running() const { return thread_.joinable(); }

protected:
  std::atomic<bool> cancel_{false};

private:
  void loop() {
    TraceThread(Derived::kName);
    Derived &d = static_cast<Derived &>(*this);
    while (true) {
      std::optional<Req> req;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return stop_.load(std::memory_order_relaxed) || pending_.has_value(); });
        if (stop_.load(std::memory_order_relaxed))
          return;
        req.swap(pending_);
        cancel_.store(false, std::memory_order_relaxed); // 与摘取同临界区: 摘后到的新请求必能取消本轮
      }
      Progress &p = d.board();
      p.begin(Derived::kStart);
      p.end(d.run(*req));
    }
  }

  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::optional<Req> pending_;
  std::atomic<bool> stop_{false};
};

} // namespace GUI
