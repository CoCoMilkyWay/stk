// OrderFlowService — OrderFlow 的单 worker 线程编排 (对仗 DistService)
//
// 线程模型 (发布协议见 shared/OrderFlow.hpp 头注释):
//   - GUI 线程: 每帧把期望态快照 (asset / anchor date / 选中特征) 交给 Request*;
//     变化检测与 gen 递增在 GUI 侧, 服务侧只管"照单执行最新一代" (新请求覆盖旧的)
//   - worker 线程: 两类工作交错 —
//       Depth 重放 (延迟敏感, 优先): decode .bin → LOB 逐笔重放 → 秒级快照 →
//         plot/heatmap 构建 → 背槽整体发布 (pending 未 ack 时等 GUI 翻面)
//       Kline 流式 (吞吐型): 逐日选列读 (OHLC + _meta + 特征) → 抽选中资产 →
//         追加 + 单调前缀发布; 每天之间轮询新请求, 随时被新代打断
//
// 生命周期: 进 OrderFlow tab Start (幂等); 切 tab 不停 (流式继续, 回来即全);
//           切出 Features 任务 Destroy → Stop (join).
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

struct SharedData;

namespace GUI::Features {

class OrderFlowService {
public:
  OrderFlowService();
  ~OrderFlowService();

  // Lifecycle (幂等)
  void Start(SharedData &data);
  void Stop();

  // GUI 线程: 期望态快照 (最新覆盖旧请求; gen 调用方递增自持, 与发布面配对)
  void RequestKline(uint32_t gen, size_t asset_idx, std::vector<int> feats, bool rescan_dates);
  void RequestDepth(uint32_t gen, std::string date, size_t asset_idx, std::vector<int> feats);

  bool is_running() const { return thread_.joinable(); }

private:
  struct KlineReq {
    uint32_t gen;
    size_t asset;
    std::vector<int> feats; // L1 字段下标
    bool rescan;            // 重扫日期列表 (特征重算后)
  };
  struct DepthReq {
    uint32_t gen;
    std::string date;
    size_t asset;
    std::vector<int> feats; // L0 字段下标
  };

  struct Impl; // worker 线程私有常驻资源 (reader / decoder / LOB / 缓冲)

  void worker_loop();
  void scan_dates(std::vector<std::string> &out) const;
  void kline_begin(const KlineReq &req);
  bool kline_step();                     // 装一天并发布; 返回是否还有下一天
  void depth_build(const DepthReq &req); // 重放 + 构建 + 背槽发布

  SharedData *data_ = nullptr;
  std::thread thread_;

  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<KlineReq> pending_kline_;
  std::optional<DepthReq> pending_depth_;
  std::atomic<bool> stop_{false};

  // ---- worker 线程私有 ----
  std::unique_ptr<Impl> impl_;
  KlineReq kline_cur_{};
  bool kline_active_ = false;
  size_t kline_next_day_ = 0;
  double kline_y_min_ = 0.0, kline_y_max_ = 0.0; // OHLC 运行范围
  std::vector<float> kline_feat_min_, kline_feat_max_;
};

} // namespace GUI::Features
