#pragma once

#include "features/MetaFlag.hpp"              // fmeta::ColumnSet
#include "features/Storage/FeatureLevels.hpp" // TierId / ColRef
#include "features/Storage/FeatureLib.hpp"    // FeatureLib
#include "features/Storage/FeaturePlane.hpp"  // SegPlan / ReadPlan / Readers
#include "misc/date.hpp"

#include <cstdint>
#include <vector>

class FeatureRead;
struct Config;
struct Feature;

// ============================================================================
// 读请求 — GUI 各读端 (Features 流式分析 / OrderFlow / 因子引擎 / K 线) 的请求构建
// ============================================================================
//   读请求 = 主库 × 日轴 × A 轴 × 行映射 × 列 (ReadSpec) → 读取范围 (ReadScope) + 读计划 (ReadPlan)
//   → 段序 (SegPlan) → 载入平面 (features/Storage/FeaturePlane.hpp)
//   GUI 线程从库清单解析, worker 只消费 (不碰 config)
// ============================================================================
namespace reader {

// ---- 读取范围: 库集 = 主库 + 各列所在库 (多池 / 跨档一视同仁; 资产不在列所在库 / 该库这天没落盘 = 无源), 余库空 ----
inline constexpr LibId kAutoMain = UINT8_MAX; // 主库 = 列所在库里池轴最短者 (子池优先: all 的列旁读, 池外格不算)
// 日轴: 段序 / 承接日取哪些库的落盘日
enum class DayAxis : uint8_t { Main,    // 主库日期轴首尾 × 主库 (流式分析: 主库日 = 样本日, 余库只旁读)
                               Union }; // 各库日期轴首尾 × 库集并集 (OrderFlow: 各库自画自的, 叠加)
struct ReadScope {
  std::vector<misc::Ym> months;     // 升序 (日轴区间覆盖的月); 库集任一未建 → 空
  LibId main = kAllBar;             // 主库: A 轴 (池轴) / K 线等主库列
  LibMask days = 0;                 // 日轴库集 (DayAxis 定): 这些库落盘日的并集
  PoolAxis axis;                    // A 轴 (平面资产维, 槽位 → 全局 id): 主库轴, 或指定资产 (OrderFlow 单资产)
  PerLib<FeatureLib> libs;          // 档 / 池轴 / 目录, 与写端同一清单
  PerLib<std::vector<uint32_t>> to; // A 轴 a → 该库轴下标 (kAbsent = 不在该库轴); 与 A 轴同轴的库空

  bool has(LibId l) const { return !libs[l].dir.empty(); } // 在库集内
  const FeatureLib &lib() const { return libs[main]; }
  PerLib<size_t> sizes() const;                                 // 各库的池轴长 (源暂存按源库开)
  Readers readers(const PerLib<const FeatureRead *> &fr) const; // fr: 库集各库 reader (LibReaders::ptr)
};
// 库集 = main + cols 所在库 (main = kAutoMain → 见上); assets 空 = A 轴取主库轴, 否则 = 这些全局 id (升序去重)
ReadScope read_scope(const Config &cfg, LibId main, const std::vector<ColRef> &cols, std::vector<uint32_t> assets = {},
                     DayAxis days = DayAxis::Main);
// A 轴 + 各库映射 (s.libs / s.main 已定; assets 空 = 主库轴, 主库同轴不映射)
void map_axis(ReadScope &s, std::vector<uint32_t> assets);
// 库 lib 落了第 d 天 (按库清单日期轴 + 末层日文件, 与写端 commit 标记同一判据)
bool landed(const FeatureLib &lib, misc::Ymd d);
// 池轴 → 全局轴映射必须升序去重非空 (PoolAxis::ids)
void check_axis(const std::vector<uint32_t> &global_ids);

// ---- 请求 (Service → 目标): 读取范围 + 目标自己的 Spec ----
template <class Spec>
struct Request {
  ReadScope scope;
  Spec spec;
};

// GUI 线程: 该档该层元数据 → 列描述 (vt 查元数据; 任一列需门控才追加该层 ts_valid)
fmeta::ColumnSet column_set(const Feature &feature, TierId tier, size_t level, std::vector<size_t> cols);

// 日轴 (升序): scope.days 各库落盘日 (按库清单日期轴) 的并集 ∩ months + 每日所属月下标 + 每日落了盘的日轴库
// (余库的位由 plan_segs 只对入选的日补判)
struct DateList {
  std::vector<misc::Ymd> dates;
  std::vector<uint16_t> month;
  std::vector<LibMask> has;
};
DateList enumerate_dates(const ReadScope &scope);

// 行映射: 平面行怎么由源行得到 (gather / scatter 对仗)
enum class Rows : uint8_t { AsOf,   // gather: 网格行 t ← 源行 level_asof(源列, 网格层, t)   (TsPlane / CsPlane)
                            Tail }; // scatter: 源行 r → 网格行 (行末秒 − 1 所在; 盘后 = 收盘之后), 原生行  (RowPlane)
// GUI 线程: 一组 ColRef → 读计划. 同 (库, 层, 盘后) 的列并成一个源 (column_set 定门控). carry = 开盘承接前日末行 (AsOf 必须)
ReadPlan read_plan(const Feature &feature, size_t level, Rows rows, bool carry, std::vector<ColRef> cols);

// 读请求 = 主库 × 日轴 × A 轴 × 行映射 × 列
struct ReadSpec {
  LibId main = kAutoMain;
  DayAxis days = DayAxis::Main;
  size_t level = L_1min;
  Rows rows = Rows::AsOf;
  bool carry = true; // Tail 可关 (前一日的点已在图上)
  std::vector<ColRef> cols;
  std::vector<uint32_t> assets; // 空 = 主库轴
};
// 统一请求构建 (= read_scope + read_plan): 库集未建 → false (请求方放弃)
bool read_request(const Config &cfg, const Feature &feature, ReadSpec rs, ReadScope &scope, ReadPlan &plan);

// ---- 段序 (全部读端唯一一份): 日轴 (enumerate_dates) 按 seg_days(level) 连续成段, 再排段 ----
//   Shuffle:    固定种子洗牌取前 limit 段 —— 无偏, 首批即覆盖全区间 (Distribution / Preview / Correlation*)
//   Sequential: 升序取前 limit 段 —— 因果 (Transform 的 TOD 轮廓跨段 / OrderFlow K 线)
enum class DayOrder : uint8_t { Shuffle,
                                Sequential };
SegPlan plan_segs(const ReadScope &scope, size_t level, DayOrder order, size_t limit); // limit 0 = 全部
// 单日段序 (OrderFlow Depth / Pick): 与 plan_segs 同口径, 只判这一天 + 日轴上前一日 (不枚举整轴); 单日不分月
SegPlan plan_day(const ReadScope &scope, misc::Ymd date);

// [0, n) 固定种子洗牌 (抽样段序 / 绘制子集: 无偏且跨构建稳定)
std::vector<uint32_t> shuffled_order(size_t n);

} // namespace reader
