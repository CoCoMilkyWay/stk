#pragma once

// ============================================================================
// FEATURE LEVELS — 特征存储的"字段表无关"稳定层 (从 FeatureStoreConfig.hpp 拆出)
// ============================================================================
// 读端 (FeatureRead / GUI / shared) 只需要这里的结构体 + 运行时层表访问, 不 include
// 字段表 (FieldsGenerated) —— 增删改特征不触发它们重编, 只重编 FeatureMeta.cpp + 写端.
//   level_info(tier, lvl)   运行时层表 (定义在 src/features/FeatureMeta.cpp, 指向编译期 <tier>::LEVELS)
//   编译期 <tier>::LEVELS[] (写端定址折叠用) 在 FeatureStoreConfig.hpp, 仅写端 include.
// 层号 / 行数 / 层名 (文件名) 与档无关 (L_sec 秒 / L_min 分钟 / L_10min 10 分钟 / L_day 日, 全档同一套), 是这里的常量; 字段数 / 字段表 / 指纹才随档.
// 日文件格式 / 落盘编码在 FeatureDay.hpp.
// ============================================================================

#include "codec/L2_DataType.hpp"       // L2::ValidType (FieldInfo.valid)
#include "features/FeaturesDefine.hpp" // FeatureDataType / ALL_TIERS
#include "features/Method/TS.hpp"      // ts::TfId (FieldInfo.ts_tf)
#include "features/TimeIndex.hpp"      // L_SEC_ROWS / L_MIN_ROWS / L_10MIN_ROWS / L_DAY_ROWS
#include <cassert>
#include <compare>
#include <cstddef>
#include <cstdint>

// 存储类型: _Float16 (内存/磁盘减半, F16C/AVX-512 FP16/NEON 硬件转换, ±65504, ~3.3 位有效数字)
using feature_storage_t = _Float16;

// 每列 {code, valid, kind, ts_tf} (由字段表 SRC 列推出); 列下标 = 存储列
struct FieldInfo {
  const char *code;
  L2::ValidType valid;
  FeatureDataType kind;
  ts::TfId ts_tf; // TS 行落盘前套用的元素变换 (改变落盘值 → 进指纹); 非 TS 行 None
};

// 档: 数据源 (见 FeaturesDefine.hpp ALL_TIERS)
#define TIER_ID_ONE(T, ns) T,
enum class TierId : uint8_t { ALL_TIERS(TIER_ID_ONE) };
#undef TIER_ID_ONE
#define TIER_COUNT_ONE(...) +1
inline constexpr size_t TIER_COUNT = 0 ALL_TIERS(TIER_COUNT_ONE);
#undef TIER_COUNT_ONE
inline constexpr const char *tier_name(TierId t) {
#define TIER_NAME_ONE(T, ns) \
  if (t == TierId::T)        \
    return #ns;
  ALL_TIERS(TIER_NAME_ONE)
#undef TIER_NAME_ONE
  return "?";
}

// 层表项: 运行时按 (档, 层) 取 rows / field_count / fingerprint; 档缺的层 fields == nullptr (见 has_level)
struct LevelInfo {
  const char *level_name;  // "L_sec" / "L_min" / "L_10min" / "L_day": 文件名 features_<name>.bin
  size_t rows;             // T
  size_t field_count;      // F
  const FieldInfo *fields; // [field_count]
  uint64_t fingerprint;    // 字段表指纹
  const char *psd;         // 该层特征的推荐频谱 (GUI 元数据)
};

// 语义层数 (FeatureLevel: L_sec 秒 / L_min 分钟 / L_10min 10 分钟 / L_day 日, 见 FeaturesDefine.hpp), 与档无关; 档可以缺层
inline constexpr size_t LEVEL_COUNT = 4;
inline constexpr const char *level_name(size_t lvl) {
  assert(lvl < LEVEL_COUNT);
  return lvl == L_sec ? "L_sec" : lvl == L_min ? "L_min"
                              : lvl == L_10min ? "L_10min"
                                               : "L_day";
}
// 一网格行的时间单位 (UI 周期 / 滞后轴标注)
inline constexpr const char *level_unit(size_t lvl) {
  assert(lvl < LEVEL_COUNT);
  return lvl == L_sec ? "s" : lvl == L_min ? "min"
                          : lvl == L_10min ? "10min"
                                           : "d";
}
inline constexpr size_t level_rows(size_t lvl) {
  assert(lvl < LEVEL_COUNT);
  return lvl == L_sec ? L_SEC_ROWS : lvl == L_min ? L_MIN_ROWS
                                 : lvl == L_10min ? L_10MIN_ROWS
                                                  : L_DAY_ROWS;
}

// 运行时层表访问 (定义在 src/features/FeatureMeta.cpp — 唯一依赖字段表的元数据 TU)
bool has_level(TierId tier, size_t lvl);
const LevelInfo &level_info(TierId tier, size_t lvl); // 档缺该层 → assert
size_t last_level(TierId tier);                       // 档内最后一层 (落盘层序末位 = 本日 commit 标记)

// 有效行数 = 落盘行数 - 1: 末行是哨兵 (label lookahead 的落点, 不是真实时间).
// rows 用于缓冲区 / stride; 消费端迭代时间轴一律用本函数, 不要用 rows.
inline constexpr size_t level_valid_rows(size_t lvl) {
  assert(lvl < LEVEL_COUNT);
  return level_rows(lvl) - 1;
}

// ---- 行时间代数 (与档无关): 行 t 覆盖交易秒 [., level_end_sec) ----
// 节点 emit 全序 = (行末秒, 层): 同刻细层先写 (close_minute → close_10min → end_day, 见 CoreTs.hpp); L_day = 收盘一行
inline constexpr size_t level_end_sec(size_t lvl, size_t t) {
  assert(t < level_valid_rows(lvl));
  return lvl == L_sec ? t + 1 : lvl == L_min ? L_min_to_L_sec(t) + 60
                            : lvl == L_10min ? L_min_to_L_sec(L_10min_last_min(t)) + 60
                                             : TRADE_SECONDS_PER_DAY;
}
// 交易秒 s 所在行
inline constexpr size_t level_row_of_sec(size_t lvl, size_t s) {
  assert(lvl < LEVEL_COUNT && s < TRADE_SECONDS_PER_DAY);
  return lvl == L_sec ? s : lvl == L_min ? L_sec_to_L_min(s)
                        : lvl == L_10min ? L_min_to_L_10min(L_sec_to_L_min(s))
                                         : 0;
}
// as-of 投影: dst 层行 t emit 时刻, src 层已 emit 的最新一行 (同日); -1 = 当日尚无 → 前一交易日末行.
// src 不粗于 dst → 含 dst 末秒的那行 (同刻细层先写, 已可见); 更粗 → 其前一行 (同刻粗层后写 / 尚未收)
inline constexpr ptrdiff_t level_asof(size_t src, size_t dst, size_t t) {
  const ptrdiff_t r = static_cast<ptrdiff_t>(level_row_of_sec(src, level_end_sec(dst, t) - 1));
  return src <= dst ? r : r - 1;
}

// 一列特征的全局身份 (档 × 层 × 存储列): GUI 跨层跨档选择 / 分析请求的列单位
struct ColRef {
  TierId tier = TierId::Lob;
  uint8_t level = L_min;
  uint16_t col = 0;
  auto operator<=>(const ColRef &) const = default;
};
