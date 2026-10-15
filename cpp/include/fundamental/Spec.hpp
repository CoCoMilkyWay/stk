// fundamental — 日频基本面数据集: 一张表 = SPECS 一行, 两个数据源同一类型
//
//   Spec   元描述 (本文件): 落盘形态 Shape × 快照 Snap × 数据源 src, 写读两侧共用
//          因果安全 (vd / drop) 在 Spec 层; 传输细节 (表名 / 键列 / 参数 / 列型) 在 src 层
//   Store  本地: 清单 + 槽位账本 + 唯一读点 read / 唯一写点 put (Store.hpp)
//   Remote 远端: Spec × Window → Query → Table, 两源同骨架 (Remote.hpp)
//   sync   写侧: plan → run (fetch → put) (Sync.hpp)
//   audit  核对: plan → run (远端指纹 ↔ 本地指纹, 仅 DAI) (Audit.hpp)
//   Scan   读侧: split → fold → merge (Scan.hpp)
//
// 依赖单向: fundamental → api/{bigquant, tushare}::Client (纯传输, 不知 Spec)
// 分派键唯一 = 数据源 Src (Bq / Tu); 客户端类型由 Src::Client 给出
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
enum class Shape { Month,
                   Meta };

// 每个分片窗口内取哪些行 (以 vd 为准)
//   All: 全部
//   Day: 只留一天 —— Month 取最早 (MIN, 月初快照), Meta 取最晚 (MAX, 真盘前快照); 方向由 Shape 定
//
// Shape × Snap 四种皆合法; 另有 (SPECS 静态初始化期逐行断言):
//   vd 空 ⇒ Meta·All;  Tu ⇒ Month·All;  Bq{Partition} ⇒ vd == "date" (服务端分区列)
enum class Snap { All,
                  Day };

// BigQuant 窗口谓词的裁剪方式
//   Where    : 纯 SQL WHERE
//   Partition: vd 是分区列 → 另附 filters 服务端裁剪
enum class Prune { Where,
                   Partition };

// BigQuant DAI. 远端表名 = Spec::name (FROM 子句)
struct Bq {
  using Client = bigquant::Client;
  static constexpr std::string_view key = "instrument"; // 资产键列
  Prune prune;
};

// Tushare HTTP
//   api        → 接口名 (api_name)
//   day_params 空 → 区间接口, 窗口 1 次调用 {start_date, end_date}
//              非空 → 逐日接口, 窗口内逐日 × 每参数各 1 次
//   num        → 声明为 double 的列, 其余一律 string (全 null 列无从推断, 不声明会跨月漂移)
struct Tu {
  using Client = tushare::Client;
  static constexpr std::string_view key = "ts_code"; // 资产键列
  std::string api;
  std::vector<std::string> day_params;
  std::vector<std::string> num;
};

struct Spec {
  std::string name; // 表名 = parquet 文件名
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

template <class Src>
bool from(const Spec &s) { return std::holds_alternative<Src>(s.src); }

// 一次抓取的 vd 闭区间 [lo, hi]; {0, 0} = 无日期维度 (vd 空)
struct Window {
  Ymd lo = 0, hi = 0;
};

extern const std::vector<Spec> SPECS;

// 按表名查; 不在 SPECS → assert
const Spec &spec(std::string_view name);

} // namespace fundamental
