// StreamService — Features 页流式分析 (Dist / Transform / FeaturePreview) 的单 worker 线程编排骨架
//
// 线程模型 (对仗 shared/Analysis.hpp 的 StreamState 发布协议):
//   - GUI 线程: Derived::RequestCompute 解析参数快照成 Request (以 ReadScope 开头: 月份表 + universe
//     子轴 + 库目录, worker 不碰 config) → submit(): 覆盖挂起请求 + 取消在跑 + 唤醒
//   - worker 线程: 取最新请求 → FeatureRead → Derived::reset(target, req) → target.build() → end_build
//     (build 内部自己起一波常驻线程分批流式, 批末发布; 新请求/Cancel 置 cancel_, 抢任务处检查后放弃)
//   - UI 渲染持 target.mutex 读快照; 进度走原子, 免锁
//
// 生命周期 (三者同一约定): 进 Features 任务输入就绪即 Start; 切走 tab 只 RequestCancel
//   (内存与 worker 保留, 切回自动重算); 切出 Features 任务 Shutdown() = Stop + target.clear() 整体释放.
//   挂起的请求 (pending_) 跨 Stop/Start 存活, 重进自动续算.
//
// Derived 需提供:
//   using Request = ...;                            // 首字段 analysis::ReadScope scope
//   static Target &target(SharedData &);            // 目标状态 (Dist / Transform / FeaturePreview)
//   void reset(Target &, Request &);                // 调 target.reset_for_build(...)
#pragma once

#include "features/Backend/FeatureRead.hpp"
#include "misc/profiler.hpp"
#include "shared/Analysis.hpp"

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>

struct SharedData;

namespace GUI::Features {

template <class Derived, class Request>
class StreamService {
public:
  StreamService() = default;
  ~StreamService() { Stop(); }
  StreamService(const StreamService &) = delete;
  StreamService &operator=(const StreamService &) = delete;

  // Lifecycle (幂等)
  void Start(SharedData &data) {
    if (thread_.joinable())
      return;
    data_ = &data;
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread(&StreamService::worker_loop, this);
  }
  void Stop() {
    if (!thread_.joinable())
      return;
    {
      std::lock_guard<std::mutex> lock(req_mutex_);
      stop_.store(true, std::memory_order_relaxed);
      cancel_.store(true, std::memory_order_relaxed);
    }
    req_cv_.notify_all();
    thread_.join();
  }
  // 任务级回收: 停 worker + 释放全部构建内存 (切出 Features 任务时调; 切 tab 不回收)
  void Shutdown() {
    Stop(); // join 之后 clear 无竞争
    if (data_)
      Derived::target(*data_).clear();
  }

  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }
  bool is_running() const { return thread_.joinable(); }

protected:
  // GUI 线程: 最新请求覆盖旧的 + 放弃在跑
  void submit(Request req) {
    assert(!req.scope.months.empty() && "submit 前由 RequestCompute 过滤空区间");
    {
      std::lock_guard<std::mutex> lock(req_mutex_);
      pending_ = std::move(req);
      cancel_.store(true, std::memory_order_relaxed);
    }
    req_cv_.notify_all();
  }

private:
  void worker_loop() {
    TraceThread(Derived::kWorkerName);
    // FeatureRead 每请求现构造 (轻量, 只有目录 + 期望子轴); 常驻缓冲挂在 target 的 Runtime 里跨请求复用
    while (true) {
      Request req;
      {
        std::unique_lock<std::mutex> lock(req_mutex_);
        req_cv_.wait(lock, [&] { return stop_.load() || pending_.has_value(); });
        if (stop_.load())
          return;
        req = std::move(*pending_);
        pending_.reset();
        cancel_.store(false, std::memory_order_relaxed); // 与消费同临界区, 免竞争
      }
      auto &target = Derived::target(*data_);
      FeatureRead reader(req.scope.features_dir, req.scope.uni.size(), req.scope.uni.hash);
      static_cast<Derived *>(this)->reset(target, req);
      target.end_build(target.build(reader, cancel_));
    }
  }

  SharedData *data_ = nullptr;
  std::thread thread_;
  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<Request> pending_;
  std::atomic<bool> cancel_{false};
  std::atomic<bool> stop_{false};
};

} // namespace GUI::Features
