#pragma once

// ============================================================================
// FEATURE LEVELS — 特征存储的"字段表无关"稳定层 (从 FeatureStoreConfig.hpp 拆出)
// ============================================================================
// 读端 (FeatureRead / GUI / shared) 只需要这里的结构体 + 运行时层表访问, 不 include
// 字段表 (FieldsGenerated) —— 增删改特征不触发它们重编, 只重编 FeatureMeta.cpp + 写端.
//   level_info(tier, lvl)   运行时层表 (定义在 src/features/FeatureMeta.cpp, 指向编译期 <tier>::LEVELS)
//   编译期 <tier>::LEVELS[] (写端定址折叠用) 在 FeatureStoreConfig.hpp, 仅写端 include.
// 层号 / 行数 / 层名 (文件名) 与档无关 (L_sec 秒 / L_1min 分钟 / L_5min .. L_60min 分钟箱 / L_1day 日, 全档同一套), 是这里的常量; 字段数 / 字段表 / 指纹才随档.
// 日文件格式 / 落盘编码在 FeatureDay.hpp.
// ============================================================================

#include "codec/L2_DataType.hpp"       // L2::ValidType (FieldInfo.valid)
#include "features/FeaturesDefine.hpp" // FeatureDataType / ALL_TIERS
#include "features/Method/TS.hpp"      // ts::TfId (FieldInfo.ts_tf)
#include "features/TimeIndex.hpp"      // L_SEC_ROWS / L_1MIN_ROWS / bin_rows / L_1DAY_ROWS
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <ranges>
#include <tuple>
#include <utility>
#include <vector>

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
#define TIER_ID_LIST(T, ns) TierId::T,
inline constexpr std::array<TierId, TIER_COUNT> TIER_IDS{ALL_TIERS(TIER_ID_LIST)};
#undef TIER_ID_LIST

// 按档下标的数组: 一切 "本档 + 旁读档" 成对的东西 (池 / reader / 轴映射 / 按档配置) 都用它, 不设具名槽
template <class X>
struct PerTier {
  std::array<X, TIER_COUNT> v{};
  constexpr X &operator[](TierId t) { return v[static_cast<size_t>(t)]; }
  constexpr const X &operator[](TierId t) const { return v[static_cast<size_t>(t)]; }
  bool operator==(const PerTier &) const = default;
};

// 池: 池表 feature_pools() 下标 (shared/FeatureLib.hpp; [0] = all, 其余 = 池). 档 = 池的属性 (字段表按档, 数据按池)
using PoolId = uint8_t;
inline constexpr PoolId kAllPool = 0;
size_t pool_count();           // 池表长 (由 Bar 字段表池列定; 定义在 FeatureMeta.cpp)
TierId pool_tier(PoolId pool); // 池的档
// 池表全部下标 (与 TIER_IDS 对仗, 运行时长)
inline auto pool_ids() { return std::views::iota(PoolId{0}, static_cast<PoolId>(pool_count())); }

// 按池下标的数组 (与 PerTier 对仗): 一切 "主池 + 列所在池" 成组的东西 (库清单 / reader / 轴映射) 都用它
template <class X>
struct PerPool {
  std::vector<X> v = std::vector<X>(pool_count());
  X &operator[](PoolId l) {
    assert(l < v.size());
    return v[l];
  }
  const X &operator[](PoolId l) const {
    assert(l < v.size());
    return v[l];
  }
  bool operator==(const PerPool &) const = default;
};
// 池集位 (位 = PoolId; PerPool<bool> 的紧凑形): 日轴每天哪些池落了盘 / 日轴取哪些池
using PoolMask = uint32_t;
inline constexpr PoolMask pool_bit(PoolId l) {
  assert(l < 32 && "池表超出 PoolMask 位宽");
  return PoolMask{1} << l;
}

// 档间旁读 (TIER_SIDES 的运行时版): main 档可旁读 side 档 (写端截面; 读端不受它约束, 列读自己的池)
namespace tier_of {
#define TIER_OF_ONE(T, ns) inline constexpr TierId ns = TierId::T;
ALL_TIERS(TIER_OF_ONE)
#undef TIER_OF_ONE
} // namespace tier_of
inline constexpr bool tier_side(TierId main, TierId side) {
#define TIER_SIDE_RT(ns, s)                      \
  if (main == tier_of::ns && side == tier_of::s) \
    return true;
  TIER_SIDES(TIER_SIDE_RT)
#undef TIER_SIDE_RT
  return false;
}

// 层表项: 运行时按 (档, 层) 取 rows / field_count / fingerprint; 档缺的层 fields == nullptr (见 has_level)
struct LevelInfo {
  const char *level_name;  // "L_sec" / "L_1min" / "L_5min" / .. / "L_1day": 文件名 features_<name>.bin
  size_t rows;             // T
  size_t field_count;      // F
  const FieldInfo *fields; // [field_count]
  uint64_t fingerprint;    // 字段表指纹
  const char *psd;         // 该层特征的推荐频谱 (GUI 元数据)
};

// 语义层数 (FeatureLevel: L_sec 秒 / L_1min 分钟 / L_5min .. L_60min 分钟箱 / L_1day 日, 见 FeaturesDefine.hpp), 与档无关; 档可以缺层
inline constexpr size_t LEVEL_COUNT = L_1day + 1;
// 分钟箱层的箱长 (分钟)
inline constexpr size_t level_bin_minutes(size_t lvl) {
  assert(is_bin_level(lvl));
  return BIN_MINUTES[lvl - L_5min];
}
inline constexpr const char *level_name(size_t lvl) {
  assert(lvl < LEVEL_COUNT);
  constexpr const char *kNames[LEVEL_COUNT] = {"L_sec", "L_1min", "L_5min", "L_15min", "L_30min", "L_60min", "L_1day"};
  return kNames[lvl];
}
// 一网格行的时间单位 (UI 周期 / 滞后轴标注)
inline constexpr const char *level_unit(size_t lvl) {
  assert(lvl < LEVEL_COUNT);
  constexpr const char *kUnits[LEVEL_COUNT] = {"s", "min", "5min", "15min", "30min", "60min", "d"};
  return kUnits[lvl];
}
inline constexpr size_t level_rows(size_t lvl) {
  assert(lvl < LEVEL_COUNT);
  return lvl == L_sec ? L_SEC_ROWS : lvl == L_1min ? L_1MIN_ROWS
                                 : lvl == L_1day   ? L_1DAY_ROWS
                                                   : bin_rows(level_bin_minutes(lvl));
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
// 节点 emit 全序 = (行末秒, 层): 同刻细层先写 (for_each_close 级联, CoreTs / CoreCs 同一路径); L_1day = 收盘一行
inline constexpr size_t level_end_sec(size_t lvl, size_t t) {
  assert(t < level_valid_rows(lvl));
  return lvl == L_sec ? t + 1 : lvl == L_1min ? L_1min_to_L_sec(t) + 60
                            : lvl == L_1day   ? TRADE_SECONDS_PER_DAY
                                              : L_1min_to_L_sec(bin_last_min(level_bin_minutes(lvl), t)) + 60;
}
// 交易秒 s 所在行
inline constexpr size_t level_row_of_sec(size_t lvl, size_t s) {
  assert(lvl < LEVEL_COUNT && s < TRADE_SECONDS_PER_DAY);
  return lvl == L_sec ? s : lvl == L_1min ? L_sec_to_L_1min(s)
                        : lvl == L_1day   ? 0
                                          : L_1min_to_bin(level_bin_minutes(lvl), L_sec_to_L_1min(s));
}
// as-of 投影: dst 层行 t emit 时刻, src 层已 emit 的最新一行 (同日); -1 = 当日尚无 → 前一交易日末行.
// src 不粗于 dst → 含 dst 末秒的那行 (同刻细层先写, 已可见); 更粗 → 其前一行 (同刻粗层后写 / 尚未收)
inline constexpr ptrdiff_t level_asof(size_t src, size_t dst, size_t t) {
  const ptrdiff_t r = static_cast<ptrdiff_t>(level_row_of_sec(src, level_end_sec(dst, t) - 1));
  return src <= dst ? r : r - 1;
}

// 收行 (哪层在哪刻收行的唯一判据): 层 lvl 在交易秒 s 末收行 ⇔ s 所在行的末秒 = s
inline constexpr bool level_closes(size_t lvl, size_t s) { return level_end_sec(lvl, level_row_of_sec(lvl, s)) == s + 1; }
static_assert(level_closes(L_sec, 0) && !level_closes(L_1min, 58) && level_closes(L_1min, 59) && !level_closes(L_5min, 59) &&
                  level_closes(L_5min, OPEN_BIN_MINUTES * 60 - 1) && !level_closes(L_1day, TRADE_SECONDS_PER_DAY - 2) &&
                  level_closes(L_1day, TRADE_SECONDS_PER_DAY - 1) && level_closes(L_60min, TRADE_SECONDS_PER_DAY - 1),
              "收行: 每秒收 L_sec, 分钟末秒收 L_1min, 箱末分钟末秒收箱层, 收盘末秒全层收行");

// 交易秒 s 末收行的层 (层号 ≥ From), 细 → 粗逐层 f.template operator()<LVL>(该层行)
template <size_t From, class F>
[[gnu::always_inline]] inline void for_each_close(size_t s, F &&f) {
  assert(s < TRADE_SECONDS_PER_DAY);
  [&]<size_t... L>(std::index_sequence<L...>) {
    ((level_closes(From + L, s) ? f.template operator()<From + L>(level_row_of_sec(From + L, s)) : void()), ...);
  }(std::make_index_sequence<LEVEL_COUNT - From>{});
}

// 一列特征的全局身份 (池 × 层 × 存储列; 档 = 池的档, 列下标 = 该 (档, 层) 字段表行号): GUI 跨池跨层选择 / 分析请求的列单位.
// 序 = (档, 池, 层, 列): 同池相邻, Lob 档在前 (picked 首个 = primary)
struct ColRef {
  PoolId pool = kAllPool;
  uint8_t level = L_1min;
  uint16_t col = 0;
  TierId tier() const { return pool_tier(pool); }
  bool operator==(const ColRef &) const = default;
  auto operator<=>(const ColRef &o) const { return std::tuple(tier(), pool, level, col) <=> std::tuple(o.tier(), o.pool, o.level, o.col); }
};
