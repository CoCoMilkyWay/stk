// fundamental — 日频基本面数据集: 一张表 = SPECS 一行, 两个数据源同一类型
//
//   Spec   元描述 (本文件): 落盘形态 Shape × 取行方式 Pick × 数据源 src, 写读两侧共用
//   Store  本地清单 + 分片状态 + 拉取计划 (Store.hpp)
//   Sync   写侧: plan → fetch → put (Sync.hpp)
//   Scan   读侧: split → fold → merge (Scan.hpp)
#pragma once

#include "misc/date.hpp"

#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace fundamental {

using misc::Ym;
using misc::Ymd;

// 落盘形态
//   Month: output/fundamental/YYYY-MM/<name>.parquet, 行按 vd 归月 (0 行月也落文件 = 拉过)
//   Meta : output/fundamental/_meta/<name>.parquet 单文件整刷
enum class Shape { Month,
                   Meta };

// 每个分片窗口内取哪些行 (以 vd 为准)
//   All  : 全部
//   First: 最早一天 (月初快照, Month 专用)
//   Last : 最晚一天 (真盘前快照, Meta 专用) —— 与 First 对仗 (MIN vs MAX)
enum class Pick { All,
                  First,
                  Last };

// BigQuant DAI. partition: vd 是分区列 → filters 服务端裁剪; 否则纯 SQL WHERE
struct Dai {
  bool partition;
};

// Tushare HTTP
//   day_params 空 → 区间接口, 窗口 1 次调用 {start_date, end_date}
//              非空 → 逐日接口, 窗口内逐日 × 每参数各 1 次
//   drop_fields → 不落盘的列 (vd 之后才回填的未来信息)
//   num_fields  → 声明为 double 的列, 其余一律 string (全 null 列无从推断, 不声明会跨月漂移)
struct Tu {
  std::string api;
  std::vector<std::string> day_params;
  std::vector<std::string> drop_fields;
  std::vector<std::string> num_fields;
};

struct Spec {
  std::string name; // 表名 = parquet 文件名 (= DAI FROM 子句)
  std::string vd;   // 因果安全可见日列; 空 = 无日期维度 (仅 Meta·All)
  Shape shape;
  Pick pick;
  int avail_hour; // day X 数据于 X 日该小时后完整:
                  //   0 = 排程提前入库; 9/10 = 真盘前; 20 = 盘后批; 24 = 全天涓流, 次日才完整
  std::variant<Dai, Tu> src;
  std::string desc; // 表内容一句话 (GUI Database → Overview)
};

// 资产键列: DAI = instrument, Tushare = ts_code (同为 "000001.SZ")
inline std::string_view key_col(const Spec &s) {
  return std::holds_alternative<Dai>(s.src) ? "instrument" : "ts_code";
}

template <class Src>
bool from(const Spec &s) { return std::holds_alternative<Src>(s.src); }

// 一次抓取的 vd 闭区间 [lo, hi]; inc != 0 = 增量下界 (只取 vd >= inc, append 落盘)
struct Window {
  Ymd lo = 0, hi = 0, inc = 0;
};

extern const std::vector<Spec> SPECS;

// 按表名查; 不在 SPECS → assert
const Spec &spec(std::string_view name);

} // namespace fundamental
