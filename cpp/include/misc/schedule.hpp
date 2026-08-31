#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

// ============================================================================
// 统一月度调度器 — bigquant / tushare 共用唯一入口.
//
// 数据集唯一落地形态 = data/YYYY-MM/<name>.parquet (0 行月也落 0 行文件).
//
// avail_hour (每表一个变量, 唯一的业务输入): day X 的数据在 X 日 avail_hour
// 点后视为完整入库. 0 = 排程提前入库随时可拉 (all_trading_days); 9/10 = 真盘前;
// 20 = 盘后批 (17~20 统一记 20); 24 = 全天涓流, 次日才完整 (tushare 公告).
// horizon = 当前时刻已完整的最晚数据日 (now_hour >= avail_hour ? today : 昨天).
//
// 按月两轴判定:
//
//   关月 (月末 < today - lookback_days): 不变 —
//     parquet 存在 ∧ 写盘日 ≥ 月末 + lookback → skip (冻结; 0 行 = 拉过为空)
//     否则 → fetch 整月 [max(m01, start), m末] 覆盖 (吃回填/修订, 唯一完整性兜底)
//
//   开放月 (月末仍在 lookback 窗口内, 含当月): 水位增量 —
//     mtime 距今 < dedup_seconds → skip (外层节流)
//     水位 W = 文件内 max(vd) (拉过的行严格可信, 只增不改)
//     W + 1 > min(m末, horizon) → skip (已到水位, 连查询都不发)
//     否则 → 只拉 vd ∈ [W+1, min(m末, horizon)], append 到月文件
//     (文件缺失 / 0 行 → 整段 [max(m01, start), min(m末, horizon)] 覆盖)
//
// 配额按返回 cell 数计 ⇒ 增量稳态下每表每天只为新增的一天数据付费一次;
// 月内漏掉的服务端回填由关月整月重拉兜回 (完整性月内降一级, 关月恢复).
// ============================================================================
namespace misc {

struct FetchMonth {
  std::string ym;       // "YYYY-MM" (data/ 子目录名)
  std::string start;    // YYYYMMDD 闭区间 (SQL 窗口下界; MonthFirst 子查询用整月)
  std::string end;      // YYYYMMDD 闭区间 = min(m末, horizon)
  std::string inc_from; // 空 = 整段拉 + 覆盖写; 非空 = 只拉 vd >= inc_from + append
};

std::vector<FetchMonth> plan_months(std::string_view name,
                                    std::string_view vd_col, int avail_hour,
                                    std::string_view start_date,
                                    std::string_view today, int lookback_days,
                                    int dedup_seconds);

// _meta 单文件表 (Static / Snapshot) 的新鲜度判定 (fresh → skip):
//   mtime 距今 < dedup_seconds → fresh (外层节流)
//   vd_col 非空 (Snapshot): 文件内 max(vd) ≥ horizon(avail_hour) → fresh
//   vd_col 空   (Static):   写盘日 == today → fresh (无水位可言, 日级整刷)
bool meta_fresh(const std::filesystem::path &p, std::string_view vd_col,
                int avail_hour, std::string_view today, int dedup_seconds);

} // namespace misc
