#pragma once

#include "fundamental/Spec.hpp"

#include <arrow/table.h>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

// ============================================================================
// Store — 本地数据集 output/data/fundamental/{YYYY-MM, _meta}/<name>.parquet + YYYY-MM/<name>/YYYY-MM-DD.parquet (Daily)
//
// 分片键 Ymd at 与 disk::Key::date 同构 (整数序 = 时间序):
//   Meta : 0
//   Month: m * 100 (YYYYMM00)
//   Daily: d       (YYYYMMDD)
//
// Catalog: 一次目录遍历的本地清单 (表名 → 分片, at 升序). sync / audit / 构建 / GUI Sync 页全只读它.
// Round  : 一轮的本地视图 = 清单 + 时刻 + 日历 (同一时刻各取一次); sync / audit / 构建共用.
// read / put: 本地唯一读点 / 唯一写点 (路径出自 disk 布局表).
//
// slots: 一张表全部槽位在 Round 视角下的状态 —— "冻结" 只在这里定义一次, sync 与 audit 严格互补:
//   horizon = 当前时刻已完整的最晚数据日 (now.hour >= avail_hour ? today : 昨天)
//   Month 关月 (月末 < today − lookback):
//     Missing  无文件                                   → Replace 整月
//     Stale    写盘日 < 月末 + lookback (月中写的半月片)  → Replace 整月 (服务端回填兜底)
//     Frozen   否则                                     → 不动 (audit 只核这类)
//   Month 开放月 (月末仍在 lookback 窗口内, 含当月):
//     Missing  无文件                                   → Replace [月初, min(月末, horizon)]
//     Open     有文件                                   → Append [mark+1, min(月末, horizon)] (0 行片 → Replace; Snap::Day 有行 → 不动)
//   Meta:
//     Meta·All (vd 空): 无文件 Missing / synced < horizon Stale → Replace; 否则 Frozen → 不动
//     Meta·Day (快照):  无文件 Missing → Replace [start, trade_hz]; 有文件 Open → Replace [mark+1, trade_hz] (已到水位 → 不动)
//   Daily (槽位 = Round::days 里 [start, horizon] 的交易日):
//     Missing  无文件                                   → Replace 该日
//     Open     有文件, 开放月                            → 不动 (avail_hour=24, 落盘即整日)
//     Frozen   有文件, 关月                              → 不动 (audit 核对)
// ============================================================================
namespace fundamental {

constexpr Ymd month_at(Ym m) { return m * 100; }

// 一个时刻: 三字段取自同一次时钟读数 (本地时区)
struct Now {
  std::filesystem::file_time_type at{}; // 与 Shard::mtime 同钟 (dedup)
  Ymd today = 0;                        // 写盘日 / 关月 / horizon
  int hour = 0;                         // [0, 23] (horizon)
};
Now now();

// 盘上一个分片
struct Shard {
  Ymd at = 0; // 0 = _meta, YYYYMM00 = Month, YYYYMMDD = Daily
  std::filesystem::path path;
  std::uint64_t bytes = 0;
  std::filesystem::file_time_type mtime{};

  constexpr Ym ym() const { return misc::ym_of(at); }
  constexpr Ymd day() const { return at % 100 ? at : 0; }
};

class Catalog {
public:
  Catalog(); // 列举 output/data/fundamental 一次 (目录缺失 → assert)

  // name 的全部分片, at 升序; 无 → 空
  const std::vector<Shard> &shards(std::string_view name) const;
  // 按分片键查 (Meta: 0, Month: month_at(m), Daily: d); 无 → nullptr
  const Shard *find(std::string_view name, Ymd at) const;
  const std::map<std::string, std::vector<Shard>, std::less<>> &all() const { return by_name_; }

private:
  std::map<std::string, std::vector<Shard>, std::less<>> by_name_;
};

// all_trading_days 的 CN 交易日 (≤ today, 升序) —— D 轴唯一来源; 无 CN 行 → assert
std::vector<Ymd> calendar(const Catalog &cat, Ymd today);

// 一轮的本地视图: 清单 + 时刻 + 日历, 同一时刻各取一次
//   days: calendar(cat, now.today); 日历未入盘 (首轮同步前) → 空
//   Slot / Unit 指向 cat ⇒ 不可拷贝, 须活到最后一个 run 结束
struct Round {
  Round();
  Round(const Round &) = delete;
  Round &operator=(const Round &) = delete;

  Catalog cat;
  Now now;
  std::vector<Ymd> days;
};

enum class State { Missing,
                   Stale,
                   Open,
                   Frozen };

// 一个分片槽 = 分片键 × 整段窗口 × 盘上分片 × 状态
struct Slot {
  Ymd at = 0;
  Window w;
  const Shard *sh = nullptr; // 指向 Catalog; nullptr = Missing
  State st = State::Missing;

  constexpr Ym ym() const { return misc::ym_of(at); }
};

// s 的全部槽位 (at 升序): Meta = {0}; Month = [ym(kSyncStart), ym(today)]; Daily = [kSyncStart, horizon] 交易日
std::vector<Slot> slots(const Spec &s, const Round &r);

// slots 中 Frozen 的 Month / Daily 槽 (at 升序; Meta → assert) —— audit 与本地清点的唯一来源
std::vector<Slot> frozen(const Spec &s, const Round &r);

// 水位 = 分片内 max(vd); 0 行 / 全 null → 0. 只读 footer 列统计 (vd 空 → assert)
Ymd mark(const Spec &s, const Shard &sh);

// 本地已齐到的最晚数据日 (此日及以前的行都已入盘; 0 = 无) —— 库规划 (fund::last_day) 的输入
Ymd covered(const Spec &s, const Catalog &cat, bool dense);

// 本地唯一读点: 只解 cols (空 = 全列), dict 里的列以字典落地
std::shared_ptr<arrow::Table> read(const Shard &sh, const std::vector<std::string> &cols = {},
                                   const std::vector<std::string> &dict = {});

// 本地唯一写点: 按 t.how 落 t.at 分片 (见 Write)
void put(const Task &t, const std::shared_ptr<arrow::Table> &tbl);

} // namespace fundamental
