#pragma once

#include "api/bigquant/dai.hpp"
#include "fundamental/Store.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace bigquant {

// ============================================================================
// audit — 远端 vs 本地已冻结月分片的一致性核对 (只读, 不改盘)
//   抓关月冻结之后服务端的晚到回填 (如分红一个月后批量入库) 与静默改历史.
//
// 指纹 = 每组 (月 / 日) 一行; 服务端 SQL GROUP BY 与本地 arrow 逐行累加同口径,
//   全部转十进制字符串逐格比 (求和一律落整数, 与行序无关, 逐位确定):
//     n             COUNT(*)
//     每列 nn       COUNT(c)
//     每列 s        double/float: Σ round(c·1e6) (仅有限值)
//                   整数 / bool:  Σ c
//                   timestamp:    Σ epoch_ms(c)
//                   string:       Σ strlen(c)  (等长改写测不出)
//   列清单 / 类型取服务端 LIMIT 0 schema (0 cell); 本地分片缺列 = 全 null.
//   配额按返回 cell 计: 一单元 ≈ 12 行 × (1 + 2·列数), 与服务端扫描量无关.
//
// 只核 fundamental::frozen 的月 (State::Frozen): 开放月 / 待重拉月本地本就落后.
// Meta (_meta 日级整刷) 不核. 窗口谓词与 fetch 同一个 bigquant::where.
// ============================================================================

//   Rows    : 只比 n — 补录 / 删除, 最便宜, 先全扫一轮
//   Content : n + 每列 nn / s — 改值; n 不一致的月 Rows 轮已报, 这里跳过
enum class AuditPass { Rows,
                       Content };

// 一单元 = 一表 × 一年 × 一轮 = 一次月级查询 (+ 不一致月的日级下钻)
struct AuditUnit {
  const fundamental::Spec *spec = nullptr;
  int year = 0;
  AuditPass pass = AuditPass::Rows;
  std::vector<fundamental::Ym> months; // 该年已冻结的月, 升序非空
  fundamental::Ymd from = 0, to = 0;   // SQL 窗口闭区间 (= months 首月初 ∨ start .. 末月末)
};

// 一个不一致的指纹格 (十进制原值, 看差的量级: 舍入级 vs 真改)
struct AuditCell {
  std::string column;
  bool sum = false; // false = nn, true = s
  std::string local, remote;
};

struct AuditDiff {
  std::string table;
  std::string group; // "YYYY-MM-DD" (日级下钻); 下钻对不上日时回落 "YYYY-MM"
  AuditPass pass = AuditPass::Rows;
  std::int64_t local_n = 0;
  std::int64_t remote_n = 0;
  std::string detail;           // Content: 不一致的 "列(nn|s)" 逗号连接 (截断); Rows 为空
  std::vector<AuditCell> cells; // Content: 全部不一致格 (不截断); Rows 为空
};

// SPECS 里的 DAI·Month 表, 两轮 (Rows 全部在前, Content 在后). 轮内表间轮转 —— 表序与各表年序均按
// seed 打乱, 相邻单元几乎总是不同表, 问题尽早暴露而不是盯着一张表扫完.
std::vector<AuditUnit> audit_plan(const fundamental::Catalog &cat, fundamental::Ymd today, unsigned seed);

// 跑一单元: 月级比对 → 不一致的月按日下钻 (每月再一次查询). 返回差异 (空 = 一致).
std::vector<AuditDiff> audit_unit(DaiClient &client, const AuditUnit &u);

// "cn_stock_dividend 2019 rows" (进度展示)
std::string audit_label(const AuditUnit &u);

} // namespace bigquant
