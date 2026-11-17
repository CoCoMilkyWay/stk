#pragma once

#include "fundamental/Store.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <string>
#include <vector>

// ============================================================================
// audit — 核侧 (与 sync 逐项对仗: Unit ↔ Task, plan ↔ plan, run ↔ run, label ↔ label, Book ↔ Store)
//   plan(s, round, keep)       : 冻结槽按年成单元 → Unit 列表
//   run(clients, unit)         : 远端指纹 ↔ 本地指纹 → Result (清点 + 差异)
//   repair(s, round, groups)   : 差异组键 → 命中冻结槽的 Replace 任务 (直交 sync)
//
// 远端 vs 本地已冻结分片的一致性核对 (只读, 不改盘):
//   抓关月冻结之后服务端的晚到回填与静默改历史.
//
// 指纹 = 每组一行, 每格一个整数; 服务端 SQL GROUP BY 与本地 arrow 逐行累加同口径,
//   全部转十进制字符串逐格比 (求和一律落整数, 与行序无关, 逐位确定):
//     n             COUNT(*)
//     列 c 的 nn    COUNT(c)
//     列 c 的 s     double/float: Σ round(c·1e6) (仅有限值)
//                   整数 / bool:  Σ c
//                   timestamp:    Σ epoch_ms(c)
//                   string:       Σ strlen(c)
//   列清单 / 类型取远端 LIMIT 0 schema (同轮 Clients 按表缓存一次); 本地分片缺列 = 全 null.
//   配额 1 cell / 查询 (见 Remote.hpp), 与组数 / 格数无关.
//
// 定位 = 月级 → 日级两层对偶 (Grain: Month → Day):
//   月级: 全部格, 远端一单元一次查询与本地各月并行读盘重叠; 清点 (Inv) 随逐日基格一并得出
//   不一致的月 (两侧皆非空): 只就差异列查该月的日级指纹 (远端一次查询, 本地由逐日基格内存线性导出, 零再读);
//     部分日不一致 → 定位到这些 Day; 全月各日皆差 → 保留 Month.
// ============================================================================
namespace fundamental {

struct Clients;

namespace audit {

using fundamental::audited;

// 一单元 = 一表 × 一年 = 一次月级查询 (+ 不一致月的日级定位)
struct Unit {
  const Spec *spec = nullptr;
  int year = 0;
  std::vector<Slot> slots; // 该年已冻结的槽 (指向 Catalog), at 升序非空
  Window w;                // 整单元 SQL 窗口闭区间 (= 首槽 w.lo .. 末槽 w.hi)
};

// 一月的本地清点 (覆盖率分母 + 流量): 本地逐日基格的副产品
struct Inv {
  std::int64_t rows = 0;
  std::int64_t days = 0;   // 出现过的 vd 天数
  std::int64_t cols = 0;   // 分片列数 (Daily: 各日最大)
  std::int64_t assets = 0; // Daily: 各日资产数最大; 其余 0
};

struct Diff {
  struct Cell {
    std::string column;
    bool sum = false; // false = nn, true = s
    std::string local, remote;
  };
  std::string group; // Month ("YYYY-MM") 或 Day ("YYYY-MM-DD")
  std::int64_t local_n = 0;
  std::int64_t remote_n = 0;
  std::vector<Cell> cells; // 不一致的列格 (不含 n)
};

// 一单元的核对结论: 单元内每月的清点 + 差异 (空 = 一致)
struct Result {
  std::map<Ym, Inv> inv;
  std::vector<Diff> diffs;
};

// 冻结槽的过滤 (须按月同判, 同月的槽一起留或一起滤)
using Keep = std::function<bool(const Slot &)>;

std::vector<Unit> plan(const Spec &s, const Round &r, const Keep &keep);

Result run(Clients &c, const Unit &u);

// "cn_stock_dividend 2019"
std::string label(const Unit &u);

// 差异组键 → 命中冻结槽的整槽 Replace 任务 (Month 一差异月一条; Daily 一差异日一条)
std::vector<Task> repair(const Spec &s, const Round &r, const std::vector<std::string> &groups);

// ============================================================================
// Book — 核侧的本地账本 (与 Store 对仗: Store 存数据分片, Book 存核对结论)
//   output/data/fundamental/_audit/<表>.tsv: 每月只留最近一次核对结论; put 后原子整写
//   结论有效 ⇔ 记下的版本 = 当前该月冻结槽的版本 (各槽 mtime 之和): 分片重写即自动作废
// ============================================================================
class Book {
public:
  struct Entry {
    Inv inv;
    std::string at;        // 核对时刻 "YYYY-MM-DD HH:MM"
    std::uint64_t ver = 0; // 核对时该月冻结槽的版本
  };
  struct Row {
    std::string group; // "YYYY-MM" 或 "YYYY-MM-DD"
    std::int64_t local_n = 0;
    std::int64_t remote_n = 0;
    std::string column = "-";
    std::string cell = "-"; // "nn" / "s"
    std::string local = "-";
    std::string remote = "-";
  };
  struct Sheet {
    std::map<std::string, Entry> months;
    std::vector<Row> rows;
  };

  static std::filesystem::path dir();

  Book();

  void put(const Unit &u, const Result &res, const std::string &at);

  Sheet sheet(const Spec &s, const Round &r) const;

private:
  std::map<std::string, Sheet, std::less<>> sheets_;
};

} // namespace audit
} // namespace fundamental
