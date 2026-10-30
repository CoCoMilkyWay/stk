// fundamental — 日频基本面数据集: 一张表 = SPECS 一行, 两个数据源同一类型
//
//   Spec   元描述 (本文件): 落盘形态 Shape × 快照 Snap × 数据源 src, 写读两侧共用
//          因果安全 (vd / drop) 在 Spec 层; 传输细节 (表名 / 键列 / 参数 / 列型) 在 src 层
//   Task   一次抓取 (本文件): 表 × 分片 × 窗口 × 写法 —— Remote::fetch 与 Store::put 的共同输入
//   Store  本地: 清单 + 槽位账本 (状态 ledger / 动作 todo) + 唯一读点 read / 唯一写点 put (Store.hpp)
//   Remote 远端: Task → Query → Table, 两源同骨架 (Remote.hpp)
//   sync   写侧: plan → run = put(t, fetch(client, t)) (Sync.hpp)
//   audit  核对: plan → run (远端指纹 ↔ 本地指纹, 仅精确源) → repair (差异 → Task); 结论入 Book (Audit.hpp)
//   Scan   读侧: split → fold → merge; meta / snapshot 同一 Rows 视图 (Scan.hpp)
//
// 依赖单向: fundamental → api/{bigquant, tushare}::Client (纯传输, 不知 Spec)
// 分派键唯一 = 数据源 Src (Bq / Tu); 客户端类型由 Src::Client 给出, 能力由 Src::exact 给出
#pragma once

#include "misc/date.hpp"

#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace bigquant {
class Client;
}
namespace tushare {
class Client;
}

namespace fundamental {

using misc::Ym;
using misc::Ymd;

// 落盘形态
//   Month: output/fundamental/YYYY-MM/<name>.parquet, 行按 vd 归月 (0 行月也落文件 = 拉过)
//   Meta : output/fundamental/_meta/<name>.parquet 单文件整刷
//   Daily: output/fundamental/YYYY-MM/<name>/YYYY-MM-DD.parquet, 一交易日一文件 (vd 为分钟时间戳, 单日即大表)
enum class Shape { Month,
                   Meta,
                   Daily };

// 每个分片窗口内取哪些行 (以 vd 为准)
//   All: 全部
//   Day: 只留一天 —— Month 取最早 (MIN, 月初快照), Meta 取最晚 (MAX, 真盘前快照); 方向由 Shape 定
//
// Month / Meta × Snap 四种皆合法; 另有 (SPECS 静态初始化期逐行断言):
//   vd 空 ⇒ Meta·All;  Tu ⇒ Month·All;  Bq{Partition} ⇒ vd == "date" (服务端分区列)
//   Daily ⇒ Bq{Partition}·All·avail_hour=24 (日文件落盘即冻结, 只拉到昨天才保证整日完整)
enum class Snap { All,
                  Day };

// BigQuant 窗口谓词的裁剪方式
//   Where    : 纯 SQL WHERE
//   Partition: vd 是分区列 → 另附 filters 服务端裁剪
enum class Prune { Where,
                   Partition };

// BigQuant DAI. 远端表名 (FROM 子句) = table, 空 = Spec::name
struct Bq {
  using Client = bigquant::Client;
  static constexpr std::string_view key = "instrument"; // 资产键列
  static constexpr bool exact = true;                   // 按组键精确取数 (SQL GROUP BY / 组键谓词) ⇒ 可核对 / 可 Splice
  Prune prune;
  std::string table = {};
};

// Tushare HTTP
//   api        → 接口名 (api_name)
//   day_params 空 → 区间接口, 窗口 1 次调用 {start_date, end_date}
//              非空 → 逐日接口, 窗口内逐日 × 每参数各 1 次
//   num        → 声明为 double 的列, 其余一律 string (全 null 列无从推断, 不声明会跨月漂移)
struct Tu {
  using Client = tushare::Client;
  static constexpr std::string_view key = "ts_code"; // 资产键列
  static constexpr bool exact = false;               // 只有接口参数, 无组键谓词 ⇒ 不核对 / 不 Splice
  std::string api;
  std::vector<std::string> day_params;
  std::vector<std::string> num;
};

struct Spec {
  std::string name; // 本地表名 = parquet 文件名 (Daily: 目录名)
  std::string vd;   // 因果安全可见日列; 空 = 无日期维度 (仅 Meta·All)
  Shape shape;
  Snap snap;
  int avail_hour; // day X 数据于 X 日该小时后完整:
                  //   0 = 排程提前入库; 9/10 = 真盘前; 20 = 盘后批; 24 = 全天涓流, 次日才完整
  std::variant<Bq, Tu> src;
  std::string desc;                   // 表内容一句话 (GUI Database → Overview)
  std::vector<std::string> drop = {}; // 不落盘的列: vd 之后才回填的未来信息 (两源同一步剥离)
};

// 资产键列 (两源同为 "000001.SZ" 形)
inline std::string_view key_col(const Spec &s) {
  return std::visit([](const auto &src) { return src.key; }, s.src);
}

// 数据源能否按组键精确取数 (见 Bq / Tu::exact)
inline bool exact(const Spec &s) {
  return std::visit([](const auto &src) { return src.exact; }, s.src);
}

template <class Src>
bool from(const Spec &s) { return std::holds_alternative<Src>(s.src); }

// 一次抓取的 vd 闭区间 [lo, hi]; {0, 0} = 无日期维度 (vd 空)
struct Window {
  Ymd lo = 0, hi = 0;
};

// 组键粒度 (audit 逐级定位 ↔ Splice 替换键, 同一键形):
//   Month "YYYY-MM" ⊃ Day "YYYY-MM-DD" ⊃ Asset "YYYY-MM-DD|<资产键>" —— 父键 = 子键前缀
//   远端 SQL 表达式 = Remote::group_expr, 本地 = group_key, 两侧逐字同形
enum class Grain { Month,
                   Day,
                   Asset };

// 槽即组: 一个分片槽 = 槽级的一个组 —— Month 形 = Month 级, Daily 形 = Day 级 (Meta 无组 → assert)
Grain slot_grain(const Spec &s);
// 可单独重写的最细组 (audit 下钻的底): 精确源的 Month 形 Splice 到 Asset; 否则 (Daily 日文件只整日 Replace) = 槽级
Grain finest(const Spec &s);

// 行 (vd = d, 资产键 = asset) 在 g 级的组键; Asset 级 asset 空 → assert
std::string group_key(Grain g, Ymd d, std::string_view asset = {});
// 组键 → 粒度 / 所属日 (Month 键 → 0) / 所属月; 形不符 → assert
Grain grain_of(std::string_view key);
Ymd key_day(std::string_view key);
Ym key_ym(std::string_view key);

// 落盘方式 (单文件 tmp+rename 原子; 0 行也落文件 = 拉过)
//   Replace: 覆盖整个分片
//   Append : 追加在末尾 (水位增量)
//   Splice : 分片里组键 ∈ keys 的行换成新行, 其余行原样 (精确修补)
enum class Write { Replace,
                   Append,
                   Splice };

// 一次抓取 = 表 × 分片 × 窗口 × 写法; sync::run = put(t, fetch(client, t))
//   Append: w.lo 已抬到水位 + 1; Snap::Day 表无 Append (快照须看整窗)
//   Splice: keys 升序去重非空, Day / Asset 级, 同属 ym 分片; w = 槽整窗 (Snap::Day 的 MIN 子查询照看整窗); 仅 exact 源
//   Shape::Daily: 一日一任务, w = [该日, 该日], 只 Replace (整日文件)
struct Task {
  const Spec *spec = nullptr; // 指向 SPECS 的一行
  Ym ym = 0;                  // 0 = _meta
  Window w;
  Write how = Write::Replace;
  std::vector<std::string> keys = {}; // Splice: 替换键
};

extern const std::vector<Spec> SPECS;

// 按表名查; 不在 SPECS → assert
const Spec &spec(std::string_view name);

} // namespace fundamental
