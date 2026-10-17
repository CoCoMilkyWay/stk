// OrderFlowService — OrderFlow 三路请求的编排外观: Kline / Depth / Pick 各一条 lane (后台线程) (gui/util/Service.hpp), 三路并行
//
// 线程模型 (发布协议见 shared/OrderFlow.hpp 头注释):
//   - GUI 线程: 每帧把期望态快照 (asset / anchor date / 选中特征) 交给 Request*;
//     变化检测与 gen 递增在 GUI 侧, 服务侧只管"照单执行最新一代" (新请求覆盖旧的, 在跑的被打断)
//   - 三个 worker 线程, 私有常驻资源 (reader / 缓冲) 首个请求时在 worker 线程建:
//       Depth 重放: decode 帧 → 订单簿逐笔重放 → 秒级快照 → plot/heatmap 构建 → 背槽整体发布
//       Pick 逐日状态: 锚点日的 L1 filter 列选读 → 全资产 PIT 状态 (ST / 在市 / 行业)
//         → 背槽整体发布 (供 GUI 资产筛选)
//       Kline 流式: 逐日选列读 (OHLC + ts_valid + 特征) → 抽选中资产 → 追加 + 单调前缀发布;
//         天与天之间可被新代打断
//     背槽 worker 写前等 GUI ack (限时睡轮询, 等待中可被新请求打断)
//
// 生命周期: 进 OrderFlow tab Start (幂等, 建三个 worker); 切 tab 不停 (流式继续, 回来即全);
//           切出 Features 任务 Destroy → Stop (析构 join). 一代服务绑一个票池库: 换池 / 库重算后整体重建 (TaskFeatures).
#pragma once

#include "misc/date.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Features {

class OrderFlowService {
public:
  explicit OrderFlowService(std::string pool); // pool = 票池库名 (Lob 档; 状态列另读 Bar 库)
  ~OrderFlowService();

  // Lifecycle (幂等)
  void Start(SharedData &data);
  void Stop();

  // GUI 线程: 期望态快照 (最新覆盖旧请求; gen 调用方递增自持, 与发布面配对)
  void RequestKline(uint32_t gen, size_t asset_idx, std::vector<int> feats, bool rescan_dates);
  void RequestDepth(uint32_t gen, misc::Ymd date, size_t asset_idx, int feat_level, std::vector<int> feats);
  void RequestPick(uint32_t gen, misc::Ymd date);

  bool is_running() const { return kline_ != nullptr; }
  const std::string &pool() const { return pool_; }

private:
  struct KlineReq {
    uint32_t gen;
    size_t asset;
    std::vector<int> feats; // L1 字段下标
    bool rescan;            // 重扫日期列表 (特征重算后)
  };
  struct DepthReq {
    uint32_t gen;
    misc::Ymd date;
    size_t asset;
    int feat_level;         // 特征所属层 (0=L0, 1=L1)
    std::vector<int> feats; // feat_level 层字段下标
  };
  struct PickReq {
    uint32_t gen;
    misc::Ymd date;
  };

  class KlineLane; // 逐日流式 + 前缀发布
  class DepthLane; // 重放 + 构建 + 背槽发布
  class PickLane;  // 逐日 filter 列 → 全资产状态 + 背槽发布

  std::string pool_;
  std::unique_ptr<KlineLane> kline_;
  std::unique_ptr<DepthLane> depth_;
  std::unique_ptr<PickLane> pick_;
};

} // namespace GUI::Features
