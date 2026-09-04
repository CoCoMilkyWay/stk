// FundamentalDaily — 日频 PIT 基本面预计算 (qmt 估值/因子/filter 链路移植), Fund 节点的数据源
//
// Phase 2 启动时构建一次: 扫 output/fundamental/ 月度 parquet, 沿全交易日历
// 回放 (状态机需 3 年 warmup), 只在回测日采样, 产出 [回测日 × AssetAxis × Fund::kCount]
// 纯 float 网格. worker 只读, 每 (date, asset) 一行普通输入数据, 缺失 = NaN
// (与其他特征输入完全同构, 无结构体/valid 位). 行布局 = Fund::Out (Operator/TS/Basic/Fund.hpp).
//
// PIT 口径 (与 qmt 完全一致):
//   cutoff=-1 (承认滞后, row D=T 取 T-1 可见): bar1d/shares/limit_price/
//     industry/dividend/financial_ttm/balance/income/forecast
//   cutoff=0  (盘前可知): status(st/susp), margin_trading_detail
//   涨跌停价: cutoff=-1 后 row T 即 "T 当日适用涨跌停" (基于 T-1 close 推出),
//     直接配盘中实时价判触板.
//
// fast-math 契约: 行数据被 -ffast-math TU (Valuation 算子/CoreSequential)
//   盲算消费 — 只做算术 (NaN 硬件透传) 与"NaN 恒 false"语义的比较, 不做
//   isnan/isfinite. 构建端本文件的 .cpp 在 precise-math 列表里.
#pragma once

#include "features/Operator/TS/Basic/Fund.hpp" // Fund::Out = 行布局
#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

class FundamentalDaily {
public:
  // codes: AssetAxis 顺序的 "000001.SZ"; dates: 回测日 "YYYYMMDD" 升序.
  // 本地 parquet 缺失 → assert (Phase 2 前置条件: 基本面 sync 已完成).
  void build(const std::vector<std::string> &codes,
             const std::vector<std::string> &dates);

  bool built() const { return !dates_.empty(); }
  std::size_t n_dates() const { return dates_.size(); }
  std::size_t n_assets() const { return n_a_; }

  const float *row(std::size_t date_idx, std::size_t asset_id) const {
    return grid_.data() + (date_idx * n_a_ + asset_id) * Fund::kCount;
  }

  // date 不在回测日集合 / 未 build → nullptr
  const float *find(const std::string &date, std::size_t asset_id) const {
    auto it = date_idx_.find(date);
    if (it == date_idx_.end())
      return nullptr;
    return row(it->second, asset_id);
  }

private:
  std::vector<std::string> dates_; // 回测日 "YYYYMMDD"
  std::unordered_map<std::string, std::size_t> date_idx_;
  std::size_t n_a_ = 0;
  std::vector<float> grid_; // [date × asset × Fund::kCount], 缺失 = NaN
};
