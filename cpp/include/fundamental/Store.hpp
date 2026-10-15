#pragma once

#include "fundamental/Spec.hpp"

#include <arrow/table.h>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

// ============================================================================
// Store — 本地数据集 output/fundamental/{YYYY-MM, _meta}/<name>.parquet
//
// Catalog: 一次目录遍历的本地清单 (表名 → 分片). sync / audit / 构建 / Overview 全只读它.
// read / put: 本地唯一读点 / 唯一写点 (路径只在 Store 内拼).
//
// ledger: 一张表全部槽位在 now 视角下的状态 —— "冻结" 只在这里定义一次, 每槽只判一次;
//   todo: 一个槽的动作 (右列) —— "拉什么 / 怎么写" 只在这里定义一次.
//   sync::plan = ledger 过 dedup 后逐槽 todo; frozen = ledger 的过滤. 时间只经 Now 传入 ⇒ 两者是纯函数.
//   horizon = 当前时刻已完整的最晚数据日 (now.hour >= avail_hour ? today : 昨天)
//   Month 关月 (月末 < today − lookback):
//     Missing  无文件                                        → 整月拉, 覆盖
//     Stale    写盘日 < 月末 + lookback (月中写的半月文件)       → 整月拉, 覆盖 (服务端回填的唯一兜底)
//     Frozen   否则                                          → 不动 (audit 只核这类)
//   Month 开放月 (月末仍在 lookback 窗口内, 含当月):
//     Missing  无文件                                        → 整段 [月初, min(月末, horizon)] 拉, 覆盖
//     Open     有文件                                        → 水位增量: W = mark (文件内 max(vd)),
//                                                             只拉 [W+1, min(月末, horizon)] append (0 行文件 → 整段覆盖);
//                                                             Snap::Day 且 W > 0 → 月初快照已入盘, 不动
//   Meta:
//     Missing  无文件 / Stale 不新鲜 → 整刷; Frozen 新鲜 → 不动
//     新鲜 = vd 空: 写盘日 == today; 否则: mark ≥ horizon
//   (右列动作 = todo; dedup 节流在 sync::plan)
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

// 盘上一个分片
struct Shard {
  Ym ym = 0; // 0 = _meta 单文件
  std::filesystem::path path;
  std::uint64_t bytes = 0;
  std::filesystem::file_time_type mtime{};
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

// 一个分片槽 = 槽位 × 盘上分片 × 状态 × 整段窗口 (Shard 是盘上有什么, Slot 是该有什么)
//   w: Meta = [start, horizon] (vd 空 → {0, 0}); Month = [max(月初, start), 关月 ? 月末 : min(月末, horizon)]
struct Slot {
  Ym ym = 0;                 // 0 = _meta
  const Shard *sh = nullptr; // 指向 Catalog; nullptr = 盘上没有 (Missing)
  State st = State::Missing;
  Window w;
};

// s 的全部槽位: Meta = {0}; Month = [ym(PIPELINE_START_DATE), ym(today)] 升序
std::vector<Slot> ledger(const Spec &s, const Catalog &cat, const Now &now);

// ledger 中 Frozen 的 Month 槽 (ym 升序) —— audit 与本地清点的唯一来源. 只看 now.today (关月判定与时刻无关)
std::vector<Slot> frozen(const Spec &s, const Catalog &cat, const Now &now);

// 槽 sl 的动作 (见文件头右列); 无事 (Frozen / 已到水位 / Snap::Day 快照已入盘) → nullopt.
//   Open 槽在这里读水位 (唯一一处), 其余零 IO
std::optional<Task> todo(const Spec &s, const Slot &sl);

// 水位 = 分片内 max(vd); 0 行 / 全 null → 0. 只读 footer 列统计 (vd 空 → assert)
Ymd mark(const Spec &s, const Shard &sh);

// 本地唯一读点: 只解 cols (空 = 全列), dict 里的列以字典落地 (见 misc::pq::read_table)
std::shared_ptr<arrow::Table> read(const Shard &sh, const std::vector<std::string> &cols = {},
                                   const std::vector<std::string> &dict = {});

// 本地唯一写点: 按 t.how 落 t.ym 分片 (见 Write)
//   Splice: 分片须已存在; tbl 的 vd 须全在 t.days 里, 追加在保留段之后;
//           列按名合并 (某侧全 null 的 NA 列 / 缺列提升为另一侧类型)
void put(const Task &t, const std::shared_ptr<arrow::Table> &tbl);

} // namespace fundamental
