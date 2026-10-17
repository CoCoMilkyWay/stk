#pragma once

// =============================================================================
// bar1m 日截面 (output/fundamental/bar1m/YYYY-MM/YYYY-MM-DD.parquet) → 稠密 [A][255] 分钟 K: All 档 TS 的输入
// =============================================================================
//   bar1m: 每标的每日 241 根 (09:25 撮合 + 09:31..11:30 + 13:01..15:00), 时间戳 = bar 结束时刻, 价格不复权.
//   时间戳 → L1 行: 09:25 → 行 10 (撮合 bar 本身), 其余 hh:mm → 分钟 hh:mm−1 的行 (09:31 → 15, 11:30 → 134,
//   13:01 → 135, 15:00 → 254). 14:58 / 14:59 两根 (收盘竞价期, volume 0) 照填, 无成交.
//   无 bar 的行全 0 (volume 0 = 无成交; CoreBar 携带日内最近成交价).
//   代码 → 资产轴下标 (AssetAxis); 轴外 / 下标 ≥ A 的代码跳过. 分片缺失 (数据尾部未同步) → present = false, 全部资产零事件日.
// 本 TU (Bar1mDay.cpp) precise-math: parquet 缺失值是 NaN.
// =============================================================================

#include "features/TimeIndex.hpp"
#include "misc/date.hpp"
#include <cstddef>
#include <cstdint>
#include <vector>

namespace fundamental {
class Catalog;
}

struct Bar1m {
  float open = 0.0f, high = 0.0f, low = 0.0f, close = 0.0f; // [元] 不复权
  float amount = 0.0f;                                      // [元]
  uint32_t volume = 0;                                      // [股]; 0 = 本分钟无成交
};

struct Bar1mDay {
  misc::Ymd date = 0;
  bool present = false;      // 当日分片存在
  size_t A = 0;              // 资产轴前缀长度 (= All 档张量 A)
  std::vector<Bar1m> rows;   // [A][TRADE_MINUTES_PER_DAY]
  std::vector<uint8_t> has;  // [A] 本资产当日有任一 bar
  size_t n_assets = 0;       // has 计数 (日志)
  size_t n_skipped_rows = 0; // 轴外代码的行数 (日志)

  // 资产 a 当日 255 行; 无 bar → nullptr (零事件日)
  const Bar1m *asset(size_t a) const { return has[a] ? rows.data() + a * TRADE_MINUTES_PER_DAY : nullptr; }

  // 解码 d 的分片 (缓冲跨日复用, 稳态零分配)
  void load(const fundamental::Catalog &cat, misc::Ymd d, size_t num_assets);
};
