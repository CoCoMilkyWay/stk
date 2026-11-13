// Asset Information - 全市场个股基本面快照 (BigQuant + Tushare) + 交易日历
// 由 AssetInfo::build 从 output/data/fundamental/ parquet 构建 (Sync 每轮同步之后).
// 个股一律按全局 id (A 轴下标, fundamental/AssetAxis.hpp) 稠密存: 一对一 = vector, 一对多 = Csr.
// 一律原生类型: 日期 Ymd (0 = 无), 数值 float (NaN = 无效); 格式化只在 UI.
#pragma once

#include "define/Csr.hpp"
#include "misc/date.hpp"
#include "shared/AssetEvent.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <vector>

namespace fundamental {
class Catalog;
}
struct Progress;

// ============================================================================
// StockInfo — 一只股票的最新快照 (每字段一个出处)
// ============================================================================

struct StockInfo {
  static constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();

  // 静态字段 (cn_stock_basic_info + cn_stock_industry_component)
  bool known = false;     // cn_stock_basic_info 有此代码 (A 轴 append-only, 可含档案不再收录的代码)
  std::string name;       // 逐日 PIT 简称 (cn_stock_instruments), 退市股回落 basic_info
  misc::Ymd ipo_date = 0; // 0 = 未知
  misc::Ymd out_date = 0; // 0 = 在市
  std::string ind_code;
  std::string ind_name;

  // 日频字段 (cn_stock_real_bar1d / cn_stock_status 每股最新行)
  misc::Ymd update_date = 0;                       // 最新行 (含停牌日) = 数据水位
  float volume = kNaN, amount = kNaN, turn = kNaN; // 最新有效报价行; 全区间无成交 → NaN
  int st = 0;                                      // cn_stock_status.st_status 原值: 0=正常 1=ST 2=*ST
  bool suspended = false;                          // 最新状态行停牌

  // 估值快照 — 分子统一为总市值 mcap = close × total_shares (不复权真价).
  // mcap 单位 [亿元]; dy* 为年化股息率 [%]; 其余为倍数. 无效 → NaN (Table 显示 "-").
  float mcap = kNaN;
  float pe_ttm = kNaN;
  float pb_mrq = kNaN;
  float ps_ttm = kNaN;
  float pcf_ttm = kNaN;
  float dy1y = kNaN; // 近 1/3/5 年分红总额年化 / mcap; 上市不足窗长的按实际年数年化
  float dy3y = kNaN;
  float dy5y = kNaN;
};
using StockInfos = std::vector<StockInfo>; // [全局 id]

// 复权因子变点 (分红/拆分事件日)
struct FactorPoint {
  misc::Ymd date;
  float factor;
};

struct CalendarDay {
  misc::Ymd date;
  bool trading;
};

// ============================================================================
// AssetInfo
// ============================================================================

struct AssetInfo {
  std::vector<std::string> codes; // [全局 id] "000001.SZ" = 构建时的 A 轴 + 档案里新见的代码 (追加在尾, 换进时 Asset::materialize intern)
  StockInfos stocks;              // [全局 id]
  Csr<FactorPoint> factors;       // [全局 id] → 复权因子变点, date 升序
  AssetEvents events;             // [全局 id] → 个股历史事件 (shared/AssetEvent.hpp)
  std::vector<CalendarDay> days;  // 全日历 (含周末/节假日, 首末交易日之间逐日), date 升序
  Csr<std::uint32_t> suspended;   // [days 下标] → 当日全天停牌的全局 id (升序)
  misc::Ymd bar1m_end = 0;        // 末个 bar1m 分片日 (0 = 未同步); 缺日由同步保证不出现
  misc::Ymd quote_end = 0;        // 行情的基本面段末日 = bar1d 水位 (末个非空月分片; 0 行月也落文件) ∧ bar1m_end (0 = 未同步)
  misc::Ymd pit_end = 0;          // 基本面齐备的末个库日 (fund::last_day; 0 = 无)

  // 档案里没有 (!known) → nullptr
  const StockInfo *find(std::size_t id) const {
    assert(id < stocks.size() && "AssetInfo: id 不在 A 轴");
    const StockInfo &s = stocks[id];
    return s.known ? &s : nullptr;
  }

  // 本地 parquet → 全部字段 (实现 shared/AssetInfo.cpp). 返回 false = 本地数据缺失 (首跑需先联网同步).
  // 进度: p.begin(Running, 分片数) + 逐分片计步; stage(说明) = 阶段切换
  bool build(const fundamental::Catalog &cat, Progress &p, const std::function<void(std::string)> &stage);
};
