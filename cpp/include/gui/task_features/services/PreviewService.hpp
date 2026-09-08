// PreviewService — FeaturePreview 的单 worker 线程编排 (对仗 DistService)
//
// 线程模型:
//   - GUI 线程: RequestCompute 解析参数快照 (全 L1 非 META 特征列 + valid_type 表
//     + 月份表 + universe 名单) → 唤醒 worker; 不依赖特征选中
//   - worker 线程: 编排一次构建 (FeaturePreview::build 单线程轮训: 抽样日 × 特征块,
//     块末发布); 新请求/Cancel 置 cancel_, 块粒度检查后放弃在跑
//   - UI 渲染表格期间持 preview.mutex 读; 进度走原子, 免锁
//
// 生命周期: 进 Features 任务且输入就绪 Start + RequestCompute (自动构建);
//           universe / 日期区间变了 或 Compute 落了新库 → 再次 RequestCompute;
//           切出 Features 任务 Shutdown() = Stop + preview.clear() 整体释放.
#pragma once

#include "shared/AssetAxis.hpp" // UniverseAxis

#include "codec/L2_DataType.hpp" // L2::ValidType

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

struct SharedData;

namespace GUI::Features {

class PreviewService {
public:
  PreviewService();
  ~PreviewService();

  // Lifecycle (幂等)
  void Start(SharedData &data);
  void Stop();

  // 任务级回收: 停 worker + 释放全部构建内存 (切出 Features 任务时调)
  void Shutdown();

  // GUI 线程: 参数快照 + 取消在跑 (全 L1 非 META 特征, 不看选中)
  void RequestCompute(SharedData &data);
  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }

  bool is_running() const { return thread_.joinable(); }

private:
  struct Request {
    std::vector<size_t> feat_cols;          // 预览特征列 (metadata 下标, 升序)
    std::vector<L2::ValidType> valid_types; // 与 feat_cols 平行
    size_t meta_col = 0;                    // "_meta" 门控列下标
    size_t n_features = 0;                  // 该层特征总数 (cells 尺寸)
    std::vector<std::string> months;        // "YYYYMM" 升序
    UniverseAxis uni;                       // universe 子轴 (GUI 线程解析 config, worker 不碰 config)
    std::string features_dir;               // 该 universe 的特征库目录
  };

  void worker_loop();

  SharedData *data_ = nullptr;
  std::thread thread_;

  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<Request> pending_; // 最新请求覆盖旧的
  std::atomic<bool> cancel_{false};
  std::atomic<bool> stop_{false};
};

} // namespace GUI::Features
