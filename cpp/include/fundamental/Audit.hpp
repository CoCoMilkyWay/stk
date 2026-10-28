#pragma once

#include "fundamental/Store.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <string>
#include <vector>

// ============================================================================
// audit — 核对侧 (与 sync 逐项对仗: Unit ↔ Task, plan ↔ plan, run ↔ run, label ↔ label, Book ↔ Store)
//   调用方循环: for u ∈ plan<Src>(round): book.put(u, run<Src>(client, u))
//   修复: for t ∈ repair(s, round, groups): sync::run(client, t) —— audit → sync 的唯一桥 (差异键 → Task)
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
//   月级: 全部格, 远端一单元一次查询 (第一遍扫描); 本地逐月读盘一次
//   不一致的组 → 下一级只查它的子组 (key_filter 父键) 与它不一致的列:
//     子级格 = 逐列 nn / s (可诊断) 或折叠格 (3 格), 取代价更低且低于 "直接按父组修复" 的那种; 都不值 → 停在父组
//     一侧 0 行 / 下钻到 finest(s) 之下 / 远端无资产键列 (Asset) → 停在父组
//   子组全部不一致 / 一个都对不上 → 父组即最精确
//   定位结果 (组键彼此不相交) = 修复键: 定位到哪些行, repair 就只拉哪些行
//
// 两侧同形 (.cpp 里 Remote ↔ Local 相邻成对, 同一接口 fps(cells, g, parents)):
//   远端: 每级一条 SQL GROUP BY
//   本地: 月读盘一次 → 逐日基格 (n + 逐列 nn / s); Month / Day 级与折叠格都由它线性导出, 零再读;
//         只有 Asset 级再读一次, 且只解所查列. 清点 (Inv) 是逐日基格的副产品
//
// 只核 frozen 的槽 (State::Frozen): 开放月 / 待重拉月本地本就落后.
// Daily: 冻结日按所属月成组, 与 Month 同一份月级指纹; finest = Day (日文件无 Splice), 不下钻 Asset.
// Meta (_meta 日级整刷) 不核. SQL 与 fetch 同一个 select (Remote.hpp); 指纹列 = 远端 schema 去掉 Spec::drop.
// 列型 (Real / Int / Ts / Str) 一型一行: 远端 SQL 求和式与本地逐值整数相邻成对.
// ============================================================================
namespace fundamental::audit {

// 参与核对的表: 精确源的 Month / Daily
inline bool audited(const Spec &s) {
  return exact(s) && (s.shape == Shape::Month || s.shape == Shape::Daily);
}

// 一单元 = 一表 × 一年 = 一次月级查询 (+ 不一致月的逐级定位)
struct Unit {
  const Spec *spec = nullptr;
  int year = 0;
  std::vector<Slot> slots; // 该年已冻结的槽 (指向 Catalog), 升序非空: Month = 月槽, Daily = 日槽; 各槽 w = 该槽 SQL 窗口
  Window w;                // 整单元 SQL 窗口闭区间 (= 首槽 w.lo .. 末槽 w.hi)
};

// 一月的本地清点 (覆盖率分母 + 流量): 本地逐日基格的副产品 (Daily: 该月各日分片累加)
struct Inv {
  std::int64_t rows = 0;
  std::int64_t days = 0; // 出现过的 vd 天数
  std::int64_t cols = 0; // 分片列数 (Daily: 各日最大)
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

// 一单元的核对结论 (与 sync::run 的行数对仗): 单元内每月的清点 + 差异 (空 = 一致)
struct Result {
  std::map<Ym, Inv> inv;
  std::vector<Diff> diffs;
};

// SPECS 里来自 Src 且 audited 的表, 表间轮转 —— 表序与各表年序均按 r.now 打乱, 相邻单元几乎总是不同表,
// 问题尽早暴露而不是盯着一张表扫完.
// keep: 只核 keep(spec, 槽) 为真的冻结槽 (空 = 全部; 须按月同判, 同月的槽一起留或一起滤);
//   单元仍按 表 × 年 切, 年内被滤掉的月夹在 w 里只多一组远端指纹行, 不参与比对.
// 单元里的槽指向 r.cat: r 须活到最后一个 run 结束.
template <class Src>
std::vector<Unit> plan(const Round &r, const Keep &keep = {});

// 跑一单元: 月级比对 → 不一致的月逐级定位 (每级每月一次查询)
template <class Src>
Result run(typename Src::Client &client, const Unit &u);

// "cn_stock_dividend 2019" (进度展示)
std::string label(const Unit &u);

// 差异组键 (Diff::group) → 修复任务, 槽序 ((ym, 日) 升序) —— 槽即组, Month / Daily 同一条规则:
//   差异键归入与之相交的冻结槽 (槽键是其前缀 = 在槽内; 其是槽键前缀 = 盖住槽);
//   槽内有不细于槽级的键 → Replace 整槽; 否则 Splice 这些键 (槽窗口). 差异键须全落在冻结槽里 (assert)
std::vector<Task> repair(const Spec &s, const Round &r, const std::vector<std::string> &groups);

// ============================================================================
// Book — 核对侧的本地账本 (与 Store 对仗: Store 存数据, Book 存核对结论)
//   output/fundamental/_audit/<表>.tsv: 每月只留最近一次核对的结论; put 后整写该表一个文件 (tmp + rename)
//   结论有效 ⇔ 记下的版本 = 当前该月冻结槽的版本 (各槽 mtime 之和):
//     重拉 / 修复 / 增删日 ⇒ 版本变 ⇒ 自动作废, 无需手动遗忘; renative 还原 mtime ⇒ 换编码不作废
//   单线程使用 (AuditService worker 独占)
// ============================================================================
class Book {
public:
  // 一月的结论
  struct Entry {
    Inv inv;
    std::string at;        // 核对时刻 "YYYY-MM-DD HH:MM"
    std::uint64_t ver = 0; // 核对时该月冻结槽的版本
  };
  // 一个不一致格 (只差 n: column / cell / local / remote 为 "-"; 原值留给人看)
  struct Row {
    std::string group; // 定位组键 (Month / Day / Asset 级); 前 7 位 = 所属月, 前 10 位 = 所属日
    std::int64_t local_n = 0;
    std::int64_t remote_n = 0;
    std::string column = "-";
    std::string cell = "-"; // "nn" / "s"
    std::string local = "-";
    std::string remote = "-";
  };
  // 一表的结论: 月 "YYYY-MM" → Entry; 差异格的所属月必在 months 里
  struct Sheet {
    std::map<std::string, Entry> months;
    std::vector<Row> rows;
  };

  // output/fundamental/_audit/ (不放 output/log: 特征计算开跑会整删 log_dir)
  static std::filesystem::path dir();

  // 读 dir 下全部 <表>.tsv (.tmp 残留不读); 不参与核对的表 / 格式不符 (旧格式等) 的文件直接删, 该表视为未核
  Book();

  // 唯一写点 (与 Store::put 对仗): 覆盖 u 各月的结论 (at = 核对时刻) + 整写该表文件
  void put(const Unit &u, const Result &res, const std::string &at);

  // s 在 r 视角下仍有效的结论 (版本与盘上一致的冻结月 + 其差异格)
  Sheet sheet(const Spec &s, const Round &r) const;

private:
  std::map<std::string, Sheet, std::less<>> sheets_; // 表名 → 全部结论 (含已作废的)
};

} // namespace fundamental::audit
