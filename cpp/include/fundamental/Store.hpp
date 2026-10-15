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
// Store — 本地数据集 output/fundamental/{YYYY-MM, _meta}/<name>.parquet
//
// Catalog: 一次目录遍历的本地清单 (表名 → 分片). plan / audit / 构建 / Overview 全只读它.
//
// state: 分片在 now 视角下的去留 —— "冻结" 只在这里定义一次. 时间只经 Now 传入 ⇒ state / plan / frozen 是纯函数.
//   horizon = 当前时刻已完整的最晚数据日 (now.hour >= avail_hour ? today : 昨天)
//   Month 关月 (月末 < today − lookback):
//     Missing  无文件                                        → 整月拉, 覆盖
//     Stale    写盘日 < 月末 + lookback (月中写的半月文件)       → 整月拉, 覆盖 (服务端回填的唯一兜底)
//     Frozen   否则                                          → 不动 (audit 只核这类)
//   Month 开放月 (月末仍在 lookback 窗口内, 含当月):
//     Missing  无文件                                        → 整段 [月初, min(月末, horizon)] 拉, 覆盖
//     Open     有文件                                        → 水位增量: W = 文件内 max(vd),
//                                                             只拉 [W+1, min(月末, horizon)] append (0 行文件 → 整段覆盖);
//                                                             snap 且 W > 0 → 月初快照已入盘, 不动
//   Meta:
//     Missing  无文件 / Stale 不新鲜 → 整刷; Frozen 新鲜 → 不动
//     新鲜 = vd 空: 写盘日 == today; 否则: 文件内 max(vd) ≥ horizon
//   dedup: 已存在的分片 mtime 距 now.at < PIPELINE_DEDUP_WINDOW_SECONDS → 本轮不动 (外层节流).
//
// 配额按返回 cell 数计 ⇒ 增量稳态下每表每天只为新增的一天数据付费一次;
// 月内漏掉的服务端回填由关月整月重拉兜回 (完整性月内降一级, 关月恢复).
// 日期一律 Ymd; 写盘日按本地时区 (与 today 同口径).
// ============================================================================
namespace fundamental {

// 一个时刻: 三字段取自同一次时钟读数 (本地时区)
struct Now {
  std::filesystem::file_time_type at{}; // 与 Shard::mtime 同钟 (dedup)
  Ymd today = 0;                        // 写盘日 / 关月 / horizon
  int hour = 0;                         // [0, 23] (horizon)
};
Now now();

std::filesystem::path root();
// name 的分片路径: ym > 0 → YYYY-MM/<name>.parquet; ym = 0 → _meta/<name>.parquet
std::filesystem::path path(std::string_view name, Ym ym);

// 盘上一个分片
struct Shard {
  Ym ym = 0; // 0 = _meta 单文件
  std::filesystem::path path;
  std::uint64_t bytes = 0;
  std::filesystem::file_time_type mtime{};
};

// 一次拉取任务 = 表 + 分片 + 窗口 + 落盘方式 (Shard 的对偶: 盘上有什么 ↔ 要拉什么)
struct Task {
  const Spec *spec = nullptr; // 指向 SPECS 的一行
  Ym ym = 0;                  // 0 = _meta
  Window w;
  bool append = false; // true = 水位增量 (w.lo = W+1) 追加; false = 整段覆盖
};

class Catalog {
public:
  Catalog(); // 遍历 output/fundamental 一次 (目录缺失 → assert)

  // name 的全部分片, ym 升序 (_meta 片 ym = 0 排最前); 无 → 空
  const std::vector<Shard> &shards(std::string_view name) const;
  // Month: m 那一片; Meta: m = 0; 无 → nullptr
  const Shard *find(std::string_view name, Ym m) const;
  // 含目录里不在 SPECS 的历史遗留表
  const std::map<std::string, std::vector<Shard>, std::less<>> &all() const { return by_name_; }

private:
  std::map<std::string, std::vector<Shard>, std::less<>> by_name_;
};

enum class State { Missing,
                   Stale,
                   Open,
                   Frozen };

// sh = 该分片 (nullptr = 盘上没有); Month: m = 分片月, Meta: m = 0
State state(const Spec &s, const Shard *sh, Ym m, const Now &now);

// s 的全部待拉任务 (区间 [PIPELINE_START_DATE, today], ym 升序); 空 ⇒ 本轮无事可做
std::vector<Task> plan(const Spec &s, const Catalog &cat, const Now &now);

// s 在 now 视角下已冻结的月 (ym 升序) —— audit 与本地清点的唯一来源. 只看 now.today (关月判定与时刻无关)
std::vector<Ym> frozen(const Spec &s, const Catalog &cat, const Now &now);

// 落盘一次拉取结果: t.append → 追加, 否则覆盖 (单文件 tmp+rename 原子; 0 行也落文件 = 拉过)
void put(const Task &t, const std::shared_ptr<arrow::Table> &tbl);

} // namespace fundamental
