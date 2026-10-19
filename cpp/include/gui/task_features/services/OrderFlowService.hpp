// OrderFlowService — OrderFlow 三路请求的编排外观: Kline / Depth / Pick 各一条 lane (后台线程) (gui/util/Service.hpp), 三路并行
//
// 线程模型 (发布协议见 shared/OrderFlow.hpp 头注释):
//   - GUI 线程: 每帧把期望态快照 (asset / anchor date / 选中特征) 交给 Request*;
//     变化检测与 gen 递增在 GUI 侧, 服务侧只管"照单执行最新一代" (新请求覆盖旧的, 在跑的被打断)
//     Request* 在 GUI 线程解析读取范围 + 读计划 (与 StreamService::RequestCompute 同一个 analysis::read_request, 原生行), worker 不碰 config
//   - 三个 worker 线程, 私有常驻读端 (库集 reader 清单没变复用 / IO 暂存 / RowPlane), 三路同一请求 RowReq:
//       Depth 重放: decode 帧 → 订单簿逐笔重放 → 秒级快照 → plot/heatmap 构建 → 背槽整体发布
//       Pick 逐日状态: 锚点日的状态列 → 主库全资产 PIT 状态 (ST / 在市 / 行业)
//         → 背槽整体发布 (供 GUI 资产筛选)
//       Kline 流式: 逐日一次读计划 (OHLC + ts_valid + overlay, 逐源 (库, 层) 各读一次) → 追加 + 单调前缀发布;
//         天与天之间可被新代打断
//   读计划 = 固定列 [0, k0) (K 线 / VWAP / 状态, 按字段表 code 定列) + overlay [k0, n) (选中 ColRef, 任意库任意层), 同一条读路径:
//     原生行 (各列按自己的层画, 尾部对齐), 资产不在列所在库 / 该库缺这天 = 无点.
//     背槽 worker 写前等 GUI ack (限时睡轮询, 等待中可被新请求打断)
//
// 生命周期: 进 OrderFlow tab Start (幂等, 建三个 worker); 切 tab 不停 (流式继续, 回来即全);
//           切出 Features 任务 Destroy → Stop (析构 join). 一代服务绑一个主库 (当前档当前库: 日期轴 / K 线 / 候选轴;
//           逐笔重放按 orders 日文件, 与主库无关): 换主库 / 库重算后整体重建 + 重扫 (TaskFeatures).
#pragma once

#include "features/Storage/DayBatchPlane.hpp" // ReadPlan
#include "misc/date.hpp"
#include "shared/Analysis.hpp" // ReadScope

#include <cstdint>
#include <memory>
#include <vector>

struct SharedData;

namespace GUI::Features {

class OrderFlowService {
public:
  explicit OrderFlowService(LibId main); // main = 当前档当前库 (Selection::cur_lib())
  ~OrderFlowService();

  // Lifecycle (幂等)
  void Start(SharedData &data);
  void Stop();

  // GUI 线程: 期望态快照 (最新覆盖旧请求; gen 调用方递增自持, 与发布面配对). feats = overlay 列 (空 = 无 overlay).
  // 库集未建 → 不提交 (算完库后服务重建再来)
  void RequestKline(const SharedData &data, uint32_t gen, size_t asset_idx, const std::vector<ColRef> &feats, bool rescan_dates);
  void RequestDepth(const SharedData &data, uint32_t gen, misc::Ymd date, size_t asset_idx, const std::vector<ColRef> &feats);
  void RequestPick(const SharedData &data, uint32_t gen, misc::Ymd date);

  bool is_running() const { return kline_ != nullptr; }
  LibId main() const { return main_; }

private:
  // 三路同一请求: 代 + 读取范围 + 读计划 (固定列 [0, k0) + overlay [k0, n))
  struct RowReq {
    uint32_t gen = 0;
    misc::Ymd date = 0;        // Depth / Pick: 锚点日; Kline: 0 (整条主库日轴流式)
    analysis::ReadScope scope; // A 轴: Kline / Depth = {资产}, Pick = 主库轴
    ReadPlan plan;
    size_t k0 = 0;         // overlay 起点 (= 固定列数)
    uint32_t ts_price = 0; // 第 i 位 = overlay 线 i 是其层 ts_valid (展示幅值 = micro price)
    bool rescan = false;   // Kline: 重扫日期列表 (特征重算后)
  };
  // GUI 线程: 固定列 + overlay → 请求 (库集未建 → false, 不提交)
  bool make(const SharedData &data, RowReq &req, size_t level, std::vector<ColRef> fixed, const std::vector<ColRef> &feats,
            std::vector<uint32_t> assets) const;

  class KlineLane; // 逐日流式 + 前缀发布
  class DepthLane; // 重放 + 构建 + 背槽发布
  class PickLane;  // 逐日 filter 列 → 全资产状态 + 背槽发布

  LibId main_;
  std::unique_ptr<KlineLane> kline_;
  std::unique_ptr<DepthLane> depth_;
  std::unique_ptr<PickLane> pick_;
};

} // namespace GUI::Features
