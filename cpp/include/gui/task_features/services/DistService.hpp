// DistService — Dist 的单 worker 线程编排
//
// 线程模型 (对仗 Dist.hpp 的发布协议):
//   - GUI 线程: RequestCompute 解析参数快照 (特征列 + valid 列 + 月份表) → 唤醒 worker
//   - worker 线程: 编排一次构建 (Dist::build 内部起一波常驻线程分批流式:
//     每批抢天入批平面 → 抢资产块扫描 → 批末发布); 新请求/Cancel 置 cancel_, 抢任务处检查后放弃在跑
//   - UI 渲染持 dist.mutex 读; 进度走原子, 免锁
//
// 生命周期: 进 Features 任务 Start + RequestPrewarm (worker 预热全部构建内存);
//           切走 Dist tab 只 RequestCancel (内存与 worker 保留, 切回自动重算);
//           切出 Features 任务 Shutdown() = Stop + dist.clear() 整体释放.
//           挂起的请求 (pending_) 跨 Stop/Start 存活, 重进自动续算.
#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

struct SharedData;

namespace GUI::Features {

class DistService {
public:
  explicit DistService(const std::string &features_dir);
  ~DistService();

  // Lifecycle (幂等)
  void Start(SharedData &data);
  void Stop();

  // 任务级回收: 停 worker + 释放全部构建内存 (切出 Features 任务时调; 切 tab 不回收)
  void Shutdown();

  // GUI 线程: 参数快照 + 取消在跑 (非 L1 选择静默忽略)
  void RequestCompute(SharedData &data);
  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }

  // GUI 线程: 进任务时预热 Dist 全部构建内存 (worker 线程执行, 不卡帧;
  // 已有真实请求在排队时预热多余, worker 侧自动丢弃)
  void RequestPrewarm(SharedData &data);

  bool is_running() const { return thread_.joinable(); }

private:
  struct Request {
    std::vector<size_t> columns;     // [值列 (+ valid 列)]
    std::vector<std::string> months; // "YYYYMM" 升序
  };

  void worker_loop();

  std::string features_dir_;
  SharedData *data_ = nullptr;
  std::thread thread_;

  struct Prewarm {
    size_t n_assets;
    size_t n_months;
  };

  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<Request> pending_;         // 最新请求覆盖旧的
  std::optional<Prewarm> pending_prewarm_; // 有真实请求时被丢弃 (reset/build 自会分配)
  std::atomic<bool> cancel_{false};
  std::atomic<bool> stop_{false};
};

} // namespace GUI::Features
