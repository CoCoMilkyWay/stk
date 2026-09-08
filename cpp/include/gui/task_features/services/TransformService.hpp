// TransformService — Transform 的单 worker 线程编排 (对仗 DistService)
//
// 线程模型:
//   - GUI 线程: RequestCompute 把 UI 参数 + 列选择 (特征列 / NeutralRank 上下文列 / _meta 门控列)
//     + 月份表 快照成 Request → 唤醒 worker; 新请求覆盖旧的并取消在跑
//   - worker 线程: 编排一次构建 (Transform::build 内部起一波常驻线程分批流式: IO → TS → CS → 统计 → 发布)
//   - UI 渲染持 transform.mutex 读; 进度走原子, 免锁
//
// 生命周期 (与 DistService 对仗): 进 Features 任务输入就绪即 Start; 选中特征/层变了即 RequestCompute
//           (无论当前在哪个 tab); 切走 Transform tab 只 RequestCancel (内存与 worker 保留, 切回自动重算);
//           切出 Features 任务 Shutdown() = Stop + transform.clear() 整体释放.
//           挂起的请求 (pending_) 跨 Stop/Start 存活, 重进自动续算.
#pragma once

#include "shared/Transform.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

struct SharedData;

namespace GUI::Features {

class TransformService {
public:
  explicit TransformService(const std::string &features_dir);
  ~TransformService();

  void Start(SharedData &data);
  void Stop();
  void Shutdown();

  // GUI 线程: 参数快照 + 取消在跑 (非 L1 / 无选择 静默忽略)
  void RequestCompute(SharedData &data, const Transform::Params &params);
  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }

  bool is_running() const { return thread_.joinable(); }

private:
  struct Request {
    Transform::Params params;
    std::vector<size_t> columns; // [特征 (+ mcap, ind_l1) (+ _meta)]
    bool has_valid = false;
    std::vector<std::string> months; // "YYYYMM" 升序
    std::vector<uint32_t> active;    // universe 资产下标 (GUI 线程解析 config, worker 不碰 config)
  };

  void worker_loop();

  std::string features_dir_;
  SharedData *data_ = nullptr;
  std::thread thread_;

  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<Request> pending_;
  std::atomic<bool> cancel_{false};
  std::atomic<bool> stop_{false};
};

} // namespace GUI::Features
