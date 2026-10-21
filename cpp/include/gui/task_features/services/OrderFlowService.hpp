// OrderFlowService — OrderFlow 三路请求的编排外观: Min / Sec / Pick 各一条 lane (后台线程) (gui/util/Service.hpp), 三路并行
//
// 线程模型 (发布协议见 shared/OrderFlow.hpp 头注释):
//   - GUI 线程: 每帧把期望态键 (OrderFlow::MinKey / SecKey / PickKey) 交给 Request (三路同形重载);
//     变化检测与 gen 递增在 GUI 侧 (Want), 服务侧只管"照单执行最新一代" (新请求覆盖旧的, 在跑的被打断)
//     Request 在 GUI 线程解析读取范围 + 读计划 (与 StreamService::RequestCompute 同一个 analysis::read_request, 尾部行), worker 不碰 config
//   - 三个 worker 线程, 私有常驻读端 (池集 reader 清单没变复用 / 双缓冲 RowPlane 预读), 三路同一请求 RowReq:
//       Min 流式: 分批 (与 Dist / Transform / Corr 同一个 Prefetch::stream, 算本批同时预读下一批) → 追加 + 单调前缀发布;
//         批与批之间可被新代打断
//       Sec 重放: decode 帧 → 订单簿逐笔重放 → 秒序列 + 热力图 → overlay → 背槽整体发布
//       Pick 逐日状态: 锚点日承接的状态列 → 主池全资产 PIT 状态 (ST / 在市 / 行业) → 背槽整体发布 (供 GUI 资产筛选)
//   日轴 = 池集各库落盘日的并集 (analysis::DayAxis::Union: Min = all + overlay 各池, 不受回测区间限; 每天带各库落盘位);
//     Sec / Pick 只取锚点日 (analysis::plan_day). Min 底图 = all 池日 K (all 缺的日不画), overlay 各画各的
//   读计划 = 固定列 [0, k0) (路表定, 按字段表 code 定列) + overlay [k0, n) (选中 ColRef + 路表尾挂列, 任意池任意层), 同一条读路径:
//     尾部行 (各列按自己的层画, 行末对齐到网格行; 网格: Min = L_1min / Sec = L_sec, 细于网格的列不进 Min),
//     资产不在列所在池 / 该池缺这天 = 无点.
//     背槽 worker 写前等 GUI ack (限时睡轮询, 等待中可被新请求打断)
//
// 生命周期: 进 OrderFlow tab Start (幂等, 建三个 worker); 切 tab 不停 (流式继续, 回来即全);
//           切出 Features 任务 Destroy → Stop (析构 join). 库重算后整体重建 + 重扫 (TaskFeatures).
// 主池 (图1 VWAP / 候选轴) 随请求: GUI 按选中列定 (无选中 = all, 单池 = 该池); 主池未建 → 退回 all 并丢 overlay (三路同一条)
//   (逐笔重放按 orders 日文件, 与主池无关; 退回后无 VWAP / overlay)
#pragma once

#include "features/Storage/DayBatchPlane.hpp" // ReadPlan
#include "misc/date.hpp"
#include "shared/Analysis.hpp"  // ReadScope
#include "shared/OrderFlow.hpp" // MinKey / SecKey / PickKey

#include <cstdint>
#include <memory>
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

  // GUI 线程: 期望态键 (最新覆盖旧请求; gen 调用方 Want 自持, 与发布面配对). 池集未建 → 不提交 (算完池后服务重建再来)
  //   Min:  key.feats 只收 ≥ L_1min 的列; rescan = 重扫日轴 (特征重算后 / 池集变了; 事件, 不入键)
  //   Sec:  key.main = 主池 (VWAP 尾挂)
  //   Pick: key.main = 主池 (候选轴)
  void Request(const SharedData &data, uint32_t gen, const OrderFlow::MinKey &key, bool rescan);
  void Request(const SharedData &data, uint32_t gen, const OrderFlow::SecKey &key);
  void Request(const SharedData &data, uint32_t gen, const OrderFlow::PickKey &key);

  bool is_running() const { return min_ != nullptr; }

private:
  // 三路同一请求: 读取范围 (A 轴: Min / Sec = {资产}, Pick = 主池轴; 日轴 = 池集并集) + 代 + 读计划 (固定列 [0, k0) + overlay [k0, n))
  struct RowSpec {
    uint32_t gen = 0;
    misc::Ymd date = 0; // Sec / Pick: 锚点日; Min: 0 (整条日轴流式)
    ReadPlan plan;
    size_t k0 = 0;         // overlay 起点 (= 固定列数)
    uint32_t ts_price = 0; // 第 i 位 = overlay 线 i 是其层 ts_valid (展示幅值 = micro price)
    bool rescan = false;   // Min: 重扫日轴 (特征重算后 / 池集变了)
  };
  using RowReq = analysis::Request<RowSpec>;

  // 路表一行 (.cpp): 网格 / 承接 / 固定列 / overlay 尾挂列
  struct LaneDef;
  static const LaneDef kMinDef, kSecDef, kPickDef;
  // GUI 线程: 路表 + 主池 + 选中列 + A 轴 → 请求 (池集未建 → false, 不提交; 主池未建 → 退回 all 并丢 overlay)
  bool make(const SharedData &data, RowReq &req, const LaneDef &def, PoolId main, const std::vector<ColRef> &feats,
            std::vector<uint32_t> assets) const;

  class MinLane;  // 逐日流式 + 前缀发布
  class SecLane;  // 重放 + 构建 + 背槽发布
  class PickLane; // 逐日 filter 列 → 全资产状态 + 背槽发布

  std::unique_ptr<MinLane> min_;
  std::unique_ptr<SecLane> sec_;
  std::unique_ptr<PickLane> pick_;
};

} // namespace GUI::Features
