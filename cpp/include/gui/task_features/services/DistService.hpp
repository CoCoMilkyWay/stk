// DistService — Dist 的单 worker 线程编排
//
// 线程模型 (对仗 Dist.hpp 的发布协议):
//   - GUI 线程: RequestCompute 解析参数快照 (特征列 + valid 列 + 月份表) → 唤醒 worker
//   - worker 线程: 编排一次构建 (Dist::build 内部起一波常驻线程分批流式:
//     每批抢天入批平面 → 抢资产块扫描 → 批末发布); 新请求/Cancel 置 cancel_, 抢任务处检查后放弃在跑
//   - UI 渲染持 dist.mutex 读; 进度走原子, 免锁
//
// 生命周期: Tab 打开 Start(data) 起线程, Tab 关闭 Stop() 取消并 join;
//           挂起的请求 (pending_) 跨 Stop/Start 存活, 重进 Tab 自动续算.
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

  // GUI 线程: 参数快照 + 取消在跑 (非 L1 选择静默忽略)
  void RequestCompute(SharedData &data);
  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }

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

  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<Request> pending_; // 最新请求覆盖旧的
  std::atomic<bool> cancel_{false};
  std::atomic<bool> stop_{false};
};

} // namespace GUI::Features
