// fund — 日频 PIT 基本面 (qmt 估值/因子/filter 链路移植): Fund 算子的数据源 + 逐日推进方法
//
//   Pool    数据源. Phase 2 前构建一次: 扫 output/fundamental/ 月度 parquet → 全交易日历 D 轴 × AssetAxis A 轴的
//           原始网格 (close/股本/涨跌停/状态/两融, cutoff 已套, ffill 已做) + per-A 事件链 (ttm/资产负债/年报/分红/预告/行业).
//           网格只保留 [首回测日 − 15, 末] (trading_st 连续 15 日计数所需), 历史依赖全在事件链上; 只读, 全部 TS worker 共享.
//   Stream  per-资产日频状态机 (Fund 算子各持一个). advance_to(d) 沿 D 轴逐日推进到 d (回测日之间的非回测日也走,
//           状态才对; 回测日前只推事件指针, 不碰网格), 然后产出当日 Fund::kCount 列 (布局 Fund::Out, 缺失 = NaN,
//           fp16 存不下的极值也归 NaN). 总算力 = per-A 沿 D 轴一遍, 与预计算相同.
//
// PIT 口径 (与 qmt 完全一致):
//   cutoff=-1 (承认滞后, row D=T 取 T-1 可见): bar1d/shares/limit_price/industry/dividend/financial_ttm/balance/income/forecast
//   cutoff=0  (盘前可知): status(st/susp), margin_trading_detail
//   涨跌停价: cutoff=-1 后 row T 即 "T 当日适用涨跌停" (基于 T-1 close 推出), 直接配盘中实时价判触板.
//
// fast-math 契约: 输出行被 -ffast-math TU (Valuation 算子/CoreSequential) 盲算消费 — 只做算术 (NaN 硬件透传) 与
//   "NaN 恒 false" 语义的比较, 不做 isnan/isfinite. 本文件 .cpp 在 precise-math 列表里, 类型细节全在 .cpp.
#pragma once

#include <cassert>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace fund {

struct Data;  // 轴 / meta / 网格切片 / 事件链 (定义在 .cpp)
struct State; // Stream 内部状态 (定义在 .cpp)

class Pool {
public:
  Pool();
  ~Pool();
  Pool(const Pool &) = delete;
  Pool &operator=(const Pool &) = delete;

  // codes: AssetAxis 顺序的 "000001.SZ"; dates: 回测日 "YYYYMMDD" 升序 (决定网格切片起点, 必须全部命中交易日历).
  // 本地 parquet 缺失 → assert (Phase 2 前置条件: 基本面 sync 已完成).
  void build(const std::vector<std::string> &codes, const std::vector<std::string> &dates);

  bool built() const { return d_ != nullptr; }
  int date_index(const std::string &yyyymmdd) const; // D 轴下标; 不是交易日 → -1

  const Data &data() const {
    assert(d_ != nullptr && "fund::Pool 未 build");
    return *d_;
  }

private:
  std::unique_ptr<Data> d_;
};

class Stream {
public:
  Stream(const Pool &pool, std::size_t asset_id);
  ~Stream();
  Stream(const Stream &) = delete;
  Stream &operator=(const Stream &) = delete;

  // 推进到 D 轴日 d (必须单调不减, 首次可跨全部 warmup), 写 out[Fund::kCount]
  void advance_to(int d, float *out);

private:
  std::unique_ptr<State> s_;
};

} // namespace fund
