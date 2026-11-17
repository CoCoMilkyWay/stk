// fundamental — 日频基本面数据集: 一张表 = SPECS 一行, 两个数据源同一类型
//
//   Spec   元描述 (本文件): 落盘形态 Shape × 快照 Snap × 数据源 src, 写读两侧共用
//          因果安全 (vd / drop) 在 Spec 层; 传输细节 (表名 / 键列 / 参数 / 列型) 在 src 层
//   Task   一次抓取 (本文件): 表 × 分片键 at × 窗口 × 写法 —— Remote::fetch 与 Store::put 的共同输入
//   Store  本地: 清单 Catalog + 轮视图 Round + 分片槽 slots / frozen + 唯一读点 read / 唯一写点 put (Store.hpp)
//   Remote 远端: Clients + fetch(clients, task) → Table, 两源同骨架 (Remote.hpp)
//   sync   写侧: plan(s, r) → Task 列表, run(clients, task) = put(fetch) (Sync.hpp)
//   audit  核侧: plan(s, r, keep) → Unit 列表, run(clients, unit) → Result, repair → Task 列表, Book (Audit.hpp)
//   Scan   读侧: split → fold → merge; Month / Meta / 最新日 / Daily 同一 Rows 视图 (Scan.hpp)
//
// 依赖单向: fundamental → api/{bigquant, tushare}::Client (纯传输, 不知 Spec)
#pragma once

#include "misc/date.hpp"

#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace fundamental {

using misc::Ym;
using misc::Ymd;

// 同步参数 (sync::plan / Store 分片槽共用)
//   kSyncStart: 同步起点 (A 股财报电子化从 2015 起逐渐完整). 与 Config::start_date (回测日历起点)
//     语义不同, 不合并 —— 基本面历史为特征表全周期服务, 不随回测日历收窄
//   kLookbackDays: 月末仍在该窗口内的月视为开放月 (兜服务端回填修订)
//   kDedupSeconds: parquet mtime 距今 < 该值则本轮跳过 (连跑零网络)
inline constexpr Ymd kSyncStart = 20150101;
inline constexpr int kLookbackDays = 7;
inline constexpr int kDedupSeconds = 60 * 60;

// 落盘形态 (分片键 Ymd at 与 disk::Key::date 同构: 0 = Meta, YYYYMM00 = Month, YYYYMMDD = Daily)
//   Month: output/data/fundamental/YYYY-MM/<name>.parquet, 行按 vd 归月 (0 行月也落文件 = 拉过)
//   Meta : output/data/fundamental/_meta/<name>.parquet 单文件整刷
//   Daily: output/data/fundamental/YYYY-MM/<name>/YYYY-MM-DD.parquet, 一交易日一文件 (vd 为分钟时间戳, 单日即大表)
enum class Shape { Month,
                   Meta,
                   Daily };

// 每个分片窗口内取哪些行 (以 vd 为准)
//   All: 全部
//   Day: 只留一天 —— Month 取最早 (MIN, 月初快照), Meta 取最晚 (MAX, 最新交易日快照); 方向由 Shape 定
//
// SPECS 静态初始化期逐行断言:
//   vd 空 ⇒ Meta·All;  Bq{Partition} ⇒ vd == "date" (服务端分区列)
//   Tu{Range|Day} ⇒ Month·All;  Tu{Concept} ⇒ Meta·Day
//   Daily ⇒ Bq{Partition}·All·avail_hour=24 (日文件落盘即整日, 只拉到昨天才保证整日完整)
enum class Snap { All,
                  Day };

// BigQuant 窗口谓词的裁剪方式
//   Where    : 纯 SQL WHERE
//   Partition: vd 是分区列 → 另附 filters 服务端裁剪
enum class Prune { Where,
                   Partition };

// Tushare 接口抓取模式
//   Range  : 区间接口, 窗口 1 次调用 {start_date, end_date}
//   Day    : 逐日接口, 窗口内逐日各 1 次 {param: YYYYMMDD}
//   Concept: 东财概念板块快照 (dc_index 名单 × dc_member 并发拉取成分)
enum class TuMode { Range,
                    Day,
                    Concept };

// BigQuant DAI. 远端表名 (FROM 子句) = table, 空 = Spec::name
struct Bq {
  static constexpr std::string_view key = "instrument"; // 资产键列
  static constexpr bool exact = true;                   // SQL GROUP BY 指纹 ⇒ 参与 audit
  Prune prune;
  std::string table = {};
};

// Tushare HTTP
//   num → 声明为 double 的列, 其余一律 string (全 null 列无从推断, 不声明会跨月漂移)
struct Tu {
  static constexpr std::string_view key = "ts_code"; // 资产键列
  static constexpr bool exact = false;
  TuMode mode = TuMode::Range;
  std::string api = {};
  std::string param = {};
  std::vector<std::string> num = {};
};

struct Spec {
  std::string name; // 本地表名 = parquet 文件名 (Daily: 目录名)
  std::string vd;   // 因果安全可见日列; 空 = 无日期维度 (仅 Meta·All)
  Shape shape;
  Snap snap;
  int avail_hour; // day X 数据于 X 日该小时后完整:
                  //   0 = 排程提前入库; 9/10 = 真盘前; 20 = 盘后批; 24 = 全天涓流, 次日才完整
  std::variant<Bq, Tu> src;
  std::string desc;                   // 表内容一句话 (GUI Database → Sync)
  std::vector<std::string> drop = {}; // 不落盘的列: vd 之后才回填的未来信息 (两源同一步剥离)
};

// 资产键列 (两源同为 "000001.SZ" 形)
inline std::string_view key_col(const Spec &s) {
  return std::visit([](const auto &src) { return src.key; }, s.src);
}

// 数据源是否支持精确 SQL 指纹核对
inline bool exact(const Spec &s) {
  return std::visit([](const auto &src) { return src.exact; }, s.src);
}

// 参与远端指纹核对的表: 精确源的 Month / Daily
inline bool audited(const Spec &s) {
  return exact(s) && (s.shape == Shape::Month || s.shape == Shape::Daily);
}

template <class Src>
bool from(const Spec &s) { return std::holds_alternative<Src>(s.src); }

// 一次抓取的 vd 闭区间 [lo, hi]; {0, 0} = 无日期维度 (vd 空)
struct Window {
  Ymd lo = 0, hi = 0;
};

// 组键粒度 (与 Month / Daily 分片槽一一对偶):
//   Month "YYYY-MM" ⊃ Day "YYYY-MM-DD" —— 父键 = 子键前缀
//   远端 SQL 表达式 = Remote::group_expr, 本地 = group_key, 两侧逐字同形
enum class Grain { Month,
                   Day };

// 分片槽对应的组粒度: Month 形 → Month, Daily 形 → Day (Meta → assert)
Grain slot_grain(const Spec &s);

// 日 d 在 g 级的组键 (d ≤ 0 → assert)
std::string group_key(Grain g, Ymd d);
// 组键 → 粒度 / 所属月; 形不符 → assert
Grain grain_of(std::string_view key);
Ym key_ym(std::string_view key);

// 落盘方式 (单文件 tmp+rename 原子; Month 0 行也落文件 = 拉过)
//   Replace: 覆盖整个分片 (Meta·Day 增量探测 0 行时保留现存快照并刷新 mtime)
//   Append : 追加在末尾 (Month·All 开放月水位增量; 0 行时刷新 mtime)
enum class Write { Replace,
                   Append };

// 一次抓取 = 表 × 分片键 × 窗口 × 写法; sync::run = put(t, fetch(clients, t))
//   at: 0 = _meta, m * 100 = Month (YYYYMM00), d = Daily (YYYYMMDD)
//   Append: 仅 Month·All 开放月, w.lo = 水位 + 1
//   Daily : 一日一任务, at = d, w = [d, d], 恒 Replace
struct Task {
  const Spec *spec = nullptr;
  Ymd at = 0;
  Window w;
  Write how = Write::Replace;
};

extern const std::vector<Spec> SPECS;

// 按表名查; 不在 SPECS → assert
const Spec &spec(std::string_view name);

} // namespace fundamental
