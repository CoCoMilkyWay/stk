#pragma once

#include "api/bigquant/client.hpp"
#include "fundamental/Store.hpp"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

// ============================================================================
// audit — 核对侧 (与 sync 逐项对仗: Unit ↔ Task, plan ↔ plan, run ↔ run, label ↔ label)
//   调用方循环: for u ∈ plan(cat, now, seed): run(client, u)
//   修复: for t ∈ repair(s, frozen, groups): sync::run(client, t) —— audit → sync 的唯一桥 (定位键 → Task)
//
// 远端 vs 本地已冻结月分片的一致性核对 (只读, 不改盘)
//   抓关月冻结之后服务端的晚到回填 (如分红一个月后批量入库) 与静默改历史.
//
// 指纹 = 每组一行, 每格一个整数; 服务端 SQL GROUP BY 与本地 arrow 逐行累加同口径,
//   全部转十进制字符串逐格比 (求和一律落整数, 与行序无关, 逐位确定):
//     n             COUNT(*)
//     列 c 的 nn    COUNT(c)
//     列 c 的 s     double/float: Σ round(c·1e6) (仅有限值)
//                   整数 / bool:  Σ c
//                   timestamp:    Σ epoch_ms(c)
//                   string:       Σ strlen(c)  (等长改写测不出)
//     折叠格        多列的 nn / s 按 (列号 + 1) 加权求和成一格 (只定位不诊断)
//   列清单 / 类型取服务端 LIMIT 0 schema (0 cell); 本地分片缺列 = 全 null.
//   配额按返回 cell 计 (组数 × (1 + 格数)), 与服务端扫描量无关.
//
// 定位 = 同一指纹逐级收窄 (Grain: Month → Day → Asset, 组键见 Spec.hpp):
//   月级: 全部格, 一单元一次查询 (第一遍扫描)
//   不一致的组 → 下一级只查它的子组 (key_filter 父键) 与它不一致的列:
//     子级格 = 逐列 nn / s (可诊断) 或折叠格 (3 格), 取代价更低且低于 "直接按父组修复" 的那种; 都不值 → 停在父组
//     一侧 0 行 / 远端无资产键列 (Asset) → 停在父组
//   子组全部不一致 / 一个都对不上 → 父组即最精确
//   定位结果 (组键彼此不相交) = 修复键: 定位到哪些行, repair 就只拉哪些行
//
// 只核 frozen 的月 (State::Frozen): 开放月 / 待重拉月本地本就落后.
// Daily: 冻结日按所属月成组, 与 Month 同一份月级指纹; 日文件无 Splice ⇒ 定位最细到 Day, 不下钻 Asset.
// Meta (_meta 日级整刷) 不核. SQL 与 fetch 同一个 select (Remote.hpp); 指纹列 = 远端 schema 去掉 Spec::drop.
// 远端指纹 (SQL GROUP BY) 与本地指纹 (arrow 逐行累加) 在 .cpp 里相邻成对.
// ============================================================================
namespace fundamental::audit {

// 参与核对的表: DAI 的 Month / Daily
inline bool audited(const Spec &s) { return from<Bq>(s) && s.shape != Shape::Meta; }

// 一单元 = 一表 × 一年 = 一次月级查询 (+ 不一致月的逐级定位)
struct Unit {
  const Spec *spec = nullptr;
  int year = 0;
  std::vector<Slot> slots; // 该年已冻结的槽 (指向 Catalog), 升序非空: Month = 月槽, Daily = 日槽; 各槽 w = 该槽 SQL 窗口
  Window w;                // 整单元 SQL 窗口闭区间 (= 首槽 w.lo .. 末槽 w.hi)
};

struct Diff {
  // 一个不一致的指纹格 (十进制原值, 看差的量级: 舍入级 vs 真改)
  struct Cell {
    std::string column; // 列名; 折叠格 = "Σ<列数>列"
    bool sum = false;   // false = nn, true = s
    std::string local, remote;
  };
  std::string table;
  std::string group; // 定位到的组键 (Month / Day / Asset 级, 见 Spec.hpp)
  std::int64_t local_n = 0;
  std::int64_t remote_n = 0;
  std::string detail;      // 不一致的 "列(nn|s)" 逗号连接 (截断); 只差 n 为空
  std::vector<Cell> cells; // 全部不一致格 (不截断, 不含 n)
};

// SPECS 里 audited 的表, 表间轮转 —— 表序与各表年序均按 seed 打乱, 相邻单元几乎总是不同表,
// 问题尽早暴露而不是盯着一张表扫完.
// keep: 只核 keep(spec, ym) 为真的冻结月 (空 = 全部); 单元仍按 表 × 年 切,
//   年内被滤掉的月夹在 w 里只多一组远端指纹行, 不参与比对.
// 单元里的槽指向 cat: cat 须活到最后一个 run 结束.
using Keep = std::function<bool(const Spec &, Ym)>;
std::vector<Unit> plan(const Catalog &cat, const Now &now, unsigned seed, const Keep &keep = {});

// 跑一单元: 月级比对 → 不一致的月逐级定位 (每级每月一次查询). 返回差异 (空 = 一致).
std::vector<Diff> run(bigquant::Client &client, const Unit &u);

// "cn_stock_dividend 2019" (进度展示)
std::string label(const Unit &u);

// 差异组键 (Diff::group) → 修复任务, (ym, 日) 升序:
//   Month: 每差异月一条 —— 月内有 Month 键 → Replace 整月 (槽窗口); 否则 Splice 这些 Day / Asset 键 (槽窗口)
//   Daily: 每差异日一条 Replace —— Day 键 → 该日; Month 键 → 该月全部冻结日
//   frozen = s 的 frozen() 槽; Month 的差异月须全在其中 (assert)
std::vector<Task> repair(const Spec &s, const std::vector<Slot> &frozen, const std::vector<std::string> &groups);

} // namespace fundamental::audit
