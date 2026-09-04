#pragma once

#include "../DataDefine.hpp"
#include "../FeaturesDefine.hpp"
#include "../Misc/CSMethods.hpp"
#include "features/NodesGenerated.hpp" // CMake 从算子文件汇总: NODES(N) / LEVEL_*_FIELDS(X) / DEPTH_FIELDS(X)
#include <array>
#include <cstddef>
#include <cstdint>

// ============================================================================
// FEATURE STORE CONFIGURATION - AUTO-GENERATED
// ============================================================================
// Fully macro-driven, generic (no hardcoded level/field count):
// - Flat [T][F_total][A] layout (no structs, optimal cache/SIMD)
// - Variable-width fields (width=1 or 30 for LOB)
// - Compile-time offset/width calculation
// - Extensible: add new level by adding 3 macros (see instructions below)
// ============================================================================

// ============================================================================
// STORAGE TYPE
// ============================================================================
// _Float16: Native compiler support (Clang/GCC)
// - 50% memory/disk vs float32
// - Hardware accelerated (F16C, AVX-512 FP16, ARM NEON)
// - Sufficient precision: ±65504, ~3.3 decimal digits
// - Auto conversion: float <-> _Float16
using feature_storage_t = _Float16;

// ============================================================================
// LEVEL METADATA
// ============================================================================

// Level count
#define COUNT_LEVEL(level_name, level_num, fields) +1
constexpr size_t LEVEL_COUNT = 0 ALL_LEVELS(COUNT_LEVEL);

// Level indices: L0_INDEX, L1_INDEX, L2_INDEX, ...
#define GENERATE_LEVEL_INDEX(level_name, level_num, fields) \
  constexpr size_t level_name##_INDEX = level_num;
ALL_LEVELS(GENERATE_LEVEL_INDEX)

// Level capacities (from LEVEL_CONFIGS)
constexpr size_t MAX_ROWS_PER_LEVEL[LEVEL_COUNT] = {
    LEVEL_CONFIGS[0].max_capacity(),
    LEVEL_CONFIGS[1].max_capacity(),
};

// ============================================================================
// FIELD METADATA - PART 1: Auxiliary Macros (Per-Level)
// ============================================================================

// Field format: X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, name_en, name_cn, description, formula, SRC)

// Count auxiliary (level-agnostic)
#define COUNT_FIELD(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) +1

// Width extractors (per-level)
#define GENERATE_FIELD_WIDTH_L0(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) width,
#define GENERATE_FIELD_WIDTH_L1(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) width,

// Index extractors (per-level)
#define GENERATE_FIELD_INDEX_L0(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) code,
#define GENERATE_FIELD_INDEX_L1(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) code,

// Type metadata extractors (per-level)
#define GENERATE_FIELD_TYPE_META_L0(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) \
  {L0_FieldOffset::code, FeatureDataType::dtype},
#define GENERATE_FIELD_TYPE_META_L1(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) \
  {L1_FieldOffset::code, FeatureDataType::dtype},

// ============================================================================
// FIELD METADATA - PART 2: Compile-Time Utilities
// ============================================================================

// Array sum (for total width calculation)
constexpr size_t array_sum(const size_t *arr, size_t count) {
  size_t sum = 0;
  for (size_t i = 0; i < count; ++i)
    sum += arr[i];
  return sum;
}

// Cumulative offsets from widths
template <size_t N>
constexpr auto generate_offsets(const size_t (&widths)[N]) {
  std::array<size_t, N> offsets{};
  size_t acc = 0;
  for (size_t i = 0; i < N; ++i) {
    offsets[i] = acc;
    acc += widths[i];
  }
  return offsets;
}

// ============================================================================
// FIELD METADATA - PART 3: Generated Constants/Arrays (Generic)
// ============================================================================

// 1. Field counts: L0_FIELD_COUNT, L1_FIELD_COUNT, ...
#define GENERATE_FIELD_COUNT(level_name, level_num, fields) \
  constexpr size_t level_name##_FIELD_COUNT = 0 fields(COUNT_FIELD);
ALL_LEVELS(GENERATE_FIELD_COUNT)

// 2. Field widths: L0_FIELD_WIDTHS[], L1_FIELD_WIDTHS[], ...
#define GENERATE_FIELD_WIDTHS(level_name, level_num, fields) \
  inline constexpr size_t level_name##_FIELD_WIDTHS[] = {    \
      fields(GENERATE_FIELD_WIDTH_##level_name)};
ALL_LEVELS(GENERATE_FIELD_WIDTHS)

// 3. Field offsets: L0_FIELD_OFFSETS[], L1_FIELD_OFFSETS[], ...
#define GENERATE_FIELD_OFFSETS(level_name, level_num, fields) \
  inline constexpr auto level_name##_FIELD_OFFSETS = generate_offsets(level_name##_FIELD_WIDTHS);
ALL_LEVELS(GENERATE_FIELD_OFFSETS)

// 4. Total widths: L0_TOTAL_WIDTH, L1_TOTAL_WIDTH, ...
#define GENERATE_TOTAL_WIDTH(level_name, level_num, fields) \
  constexpr size_t level_name##_TOTAL_WIDTH = array_sum(level_name##_FIELD_WIDTHS, level_name##_FIELD_COUNT);
ALL_LEVELS(GENERATE_TOTAL_WIDTH)

// 5. Field index enums: L0_FieldOffset::*, L1_FieldOffset::*, ...
//    (Use L*_FIELD_OFFSETS[idx] to get actual offset in flat array)
//    (Use L*_FIELD_WIDTHS[idx] to get field width)
#define GENERATE_FIELD_INDEX_ENUM(level_name, level_num, fields) \
  namespace level_name##_FieldOffset {                           \
    enum : size_t {                                              \
      fields(GENERATE_FIELD_INDEX_##level_name)                  \
    };                                                           \
  }
ALL_LEVELS(GENERATE_FIELD_INDEX_ENUM)

// 6. Field type metadata: L0_FIELD_TYPES[], L1_FIELD_TYPES[], ...
struct FieldTypeMeta {
  size_t offset; // field index, not actual offset (use L*_FIELD_OFFSETS[offset])
  FeatureDataType type;
};

#define GENERATE_FIELD_TYPE_METAS(level_name, level_num, fields) \
  inline constexpr FieldTypeMeta level_name##_FIELD_TYPES[] = {  \
      fields(GENERATE_FIELD_TYPE_META_##level_name)};
ALL_LEVELS(GENERATE_FIELD_TYPE_METAS)

// 7. Total widths array: FIELDS_PER_LEVEL[lvl] = F_total for [T][F][A] indexing
#define GENERATE_FIELDS_PER_LEVEL_ENTRY(level_name, level_num, fields) level_name##_TOTAL_WIDTH,
constexpr size_t FIELDS_PER_LEVEL[LEVEL_COUNT] = {
    ALL_LEVELS(GENERATE_FIELDS_PER_LEVEL_ENTRY)};

// ============================================================================
// FIELD COUNT BY DATA TYPE (TS, CS, LB, META)
// ============================================================================
// 按 data_type 统计字段宽度 (用于确定输出buffer大小)

// Helper: count width if data_type matches
#define COUNT_WIDTH_IF_TS(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) \
  +(FeatureDataType::dtype == FeatureDataType::TS ? (width) : 0)
#define COUNT_WIDTH_IF_CS(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) \
  +(FeatureDataType::dtype == FeatureDataType::CS ? (width) : 0)
#define COUNT_WIDTH_IF_LB(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) \
  +(FeatureDataType::dtype == FeatureDataType::LB ? (width) : 0)
#define COUNT_WIDTH_IF_META(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) \
  +(FeatureDataType::dtype == FeatureDataType::META ? (width) : 0)

// 8. Type widths per level: L0_TS_WIDTH, L0_CS_WIDTH, ..., L2_META_WIDTH
#define GENERATE_TYPE_WIDTHS(level_name, level_num, fields)             \
  constexpr size_t level_name##_TS_WIDTH = 0 fields(COUNT_WIDTH_IF_TS); \
  constexpr size_t level_name##_CS_WIDTH = 0 fields(COUNT_WIDTH_IF_CS); \
  constexpr size_t level_name##_LB_WIDTH = 0 fields(COUNT_WIDTH_IF_LB); \
  constexpr size_t level_name##_META_WIDTH = 0 fields(COUNT_WIDTH_IF_META);
ALL_LEVELS(GENERATE_TYPE_WIDTHS)

// ============================================================================
// SRC ↔ data_type 一致性 (OP/FUND→TS, CS→CS, LABEL→LB, META→META)
// ============================================================================
#define SRC_KIND_OP(...) FeatureDataType::TS
#define SRC_KIND_FUND(...) FeatureDataType::TS
#define SRC_KIND_CS(...) FeatureDataType::CS
#define SRC_KIND_LABEL FeatureDataType::LB
#define SRC_KIND_META FeatureDataType::META
#define CHECK_SRC_FIELD(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) \
  static_assert(SRC_KIND_##src == FeatureDataType::dtype, "SRC/data_type mismatch: " #code);      \
  static_assert(SRC_KIND_##src != FeatureDataType::TS || (width) == 1, "OP/FUND field must be width 1: " #code);
#define GENERATE_CHECK_SRC(level_name, level_num, fields) fields(CHECK_SRC_FIELD)
ALL_LEVELS(GENERATE_CHECK_SRC)
DEPTH_FIELDS(CHECK_SRC_FIELD)

// ============================================================================
// 字段层 ↔ 节点 flush 域一致性: OP 列所在层必须等于节点的落盘层 (onMinute→L1, 其余→L0);
// FUND 只能在 L1; CS/LABEL/META 不查 (-1). 错层 = 写回读到别的层的节点, 编译期拦住.
// ============================================================================
namespace node_flush_level {
#define NODE_FLUSH_LEVEL(name, type, args, ct, ft) constexpr int name = (Trigger::ft == Trigger::onMinute) ? 1 : 0;
NODES(NODE_FLUSH_LEVEL)
#undef NODE_FLUSH_LEVEL
} // namespace node_flush_level
#define SRC_LEVEL_OP(node, ...) node_flush_level::node
#define SRC_LEVEL_FUND(...) 1
#define SRC_LEVEL_CS(...) (-1)
#define SRC_LEVEL_LABEL (-1)
#define SRC_LEVEL_META (-1)
#define CHECK_LEVEL_FIELD(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) \
  static_assert(SRC_LEVEL_##src < 0 || SRC_LEVEL_##src == kLevel, "field level != node flush level: " #code);
#define GENERATE_CHECK_LEVEL(level_name, level_num, fields) \
  namespace level_name##_level_check {                      \
    constexpr int kLevel = level_num;                       \
    fields(CHECK_LEVEL_FIELD)                               \
  }
ALL_LEVELS(GENERATE_CHECK_LEVEL)
namespace depth_level_check {
constexpr int kLevel = -2; // DEPTH 层不允许 OP/FUND 列
DEPTH_FIELDS(CHECK_LEVEL_FIELD)
} // namespace depth_level_check

// ============================================================================
// CROSS-SECTIONAL TABLES (由字段表 CS(src_lvl, src, tf, m) 列生成): L0_CS_DEFS[], L1_CS_DEFS[]
// ============================================================================
// 每行 = 源列 + 元素变换 + 截面方法 → 目标列; *_Crosssection 按表 gather → cs::apply → scatter.
// 数组末尾带一个哨兵 (dst == SIZE_MAX), 允许某层没有 CS 字段; 用 L*_CS_COUNT 迭代.

struct CSFeatureDef {
  std::uint8_t src_lvl; // 源列所在层 (0 = L0 取分钟起始秒, 1 = L1)
  cs::Transform tf;     // 元素变换
  cs::Method method;    // 截面方法
  std::size_t src;      // 源列 (field index, 用 L*_FIELD_OFFSETS[src] 取 offset)
  std::size_t dst;      // 目标列 (本层 field index)
};

#define CS_ENTRY_CS(src_lvl, src, tf, m) src_lvl, cs::Transform::tf, cs::Method::m, L##src_lvl##_FieldOffset::src
#define CS_ROW_CS(code, src) {CS_ENTRY_##src, _FO::code},
#define CS_ROW_TS(code, src)
#define CS_ROW_LB(code, src)
#define CS_ROW_SH(code, src)
#define CS_ROW_META(code, src)
#define CS_ROW_FIELD(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) CS_ROW_##dtype(code, src)

#define GENERATE_CS_TABLE(level_name, level_num, fields)                                    \
  namespace level_name##_cs_detail {                                                        \
    namespace _FO = level_name##_FieldOffset;                                               \
    inline constexpr CSFeatureDef DEFS[] = {fields(CS_ROW_FIELD){0, {}, {}, 0, SIZE_MAX}};  \
  }                                                                                         \
  inline constexpr const CSFeatureDef *level_name##_CS_DEFS = level_name##_cs_detail::DEFS; \
  constexpr size_t level_name##_CS_COUNT = sizeof(level_name##_cs_detail::DEFS) / sizeof(CSFeatureDef) - 1;
ALL_LEVELS(GENERATE_CS_TABLE)

// ============================================================================
// FIELD TABLE FINGERPRINT (写入文件头, 读取时比对; 表改了旧文件立刻报错而非静默错位)
// ============================================================================
// FNV-1a 64 over "code:width:dtype;" 逐字段拼接 (顺序敏感)

constexpr uint64_t fnv1a_str(uint64_t h, const char *s) {
  for (; *s; ++s)
    h = (h ^ static_cast<uint8_t>(*s)) * 0x100000001b3ULL;
  return h;
}
constexpr uint64_t fnv1a_u64(uint64_t h, uint64_t v) {
  for (int i = 0; i < 8; ++i)
    h = (h ^ ((v >> (8 * i)) & 0xff)) * 0x100000001b3ULL;
  return h;
}

#define FINGERPRINT_FIELD(code, width, vtype, dtype, c1, c2, norm, psd, en, cn, desc, formula, src) \
  h = fnv1a_u64(fnv1a_u64(fnv1a_str(h, #code), (width)), static_cast<uint64_t>(FeatureDataType::dtype));

#define GENERATE_FINGERPRINT(level_name, level_num, fields) \
  constexpr uint64_t level_name##_FINGERPRINT = [] {        \
    uint64_t h = 0xcbf29ce484222325ULL;                     \
    fields(FINGERPRINT_FIELD) return h;                     \
  }();
ALL_LEVELS(GENERATE_FINGERPRINT)

#define GENERATE_FINGERPRINT_ENTRY(level_name, level_num, fields) level_name##_FINGERPRINT,
constexpr uint64_t LEVEL_FINGERPRINTS[LEVEL_COUNT] = {ALL_LEVELS(GENERATE_FINGERPRINT_ENTRY)};

constexpr uint64_t DEPTH_FINGERPRINT = [] {
  uint64_t h = 0xcbf29ce484222325ULL;
  DEPTH_FIELDS(FINGERPRINT_FIELD)
  return h;
}();

// 特征文件头: size_t × {T, F, A, axis_hash, table_fingerprint}
constexpr size_t FEATURE_FILE_HEADER_WORDS = 5;

// ============================================================================
// DEPTH METADATA (separate from levels)
// ============================================================================

// Depth field count
constexpr size_t DEPTH_FIELD_COUNT = 0 DEPTH_FIELDS(COUNT_FIELD);

// Depth field widths
inline constexpr size_t DEPTH_FIELD_WIDTHS[] = {DEPTH_FIELDS(GENERATE_FIELD_WIDTH_L0)};

// Depth field offsets
inline constexpr auto DEPTH_FIELD_OFFSETS = generate_offsets(DEPTH_FIELD_WIDTHS);

// Depth total width
constexpr size_t DEPTH_TOTAL_WIDTH = array_sum(DEPTH_FIELD_WIDTHS, DEPTH_FIELD_COUNT);

// Depth field enum
namespace DepthFieldOffset {
enum : size_t {
  DEPTH_FIELDS(GENERATE_FIELD_INDEX_L0)
};
}

// Depth 张量 T 维: 分钟频 (与 L1 同 T)。分钟内多次盘口更新覆盖同一行,
// 行终值 = 该分钟最后一次快照 (分钟末盘口)。
constexpr size_t DEPTH_ROWS = MAX_ROWS_PER_LEVEL[1];

// ============================================================================
// STORAGE LAYOUT (No Structs)
// ============================================================================
// Flat [T][F_total][A] layout for optimal cache/SIMD performance
// Access: data[t * F_total * A + f_offset * A + a]
//   where f_offset = L*_FIELD_OFFSETS[field_idx]
//         F_total  = FIELDS_PER_LEVEL[level]
// ============================================================================

// ============================================================================
// ACCESS UTILITIES - Runtime Field Lookup
// ============================================================================

// Pointer arrays for runtime level indexing
#define GENERATE_WIDTHS_PTR(level_name, level_num, fields) level_name##_FIELD_WIDTHS,
#define GENERATE_OFFSETS_PTR(level_name, level_num, fields) level_name##_FIELD_OFFSETS.data(),

inline constexpr const size_t *FIELD_WIDTHS_PTRS[LEVEL_COUNT] = {
    ALL_LEVELS(GENERATE_WIDTHS_PTR)};

inline constexpr const size_t *FIELD_OFFSETS_PTRS[LEVEL_COUNT] = {
    ALL_LEVELS(GENERATE_OFFSETS_PTR)};

// Runtime accessors
inline constexpr size_t get_field_offset(size_t level_idx, size_t field_idx) {
  return FIELD_OFFSETS_PTRS[level_idx][field_idx];
}

inline constexpr size_t get_field_width(size_t level_idx, size_t field_idx) {
  return FIELD_WIDTHS_PTRS[level_idx][field_idx];
}

// Usage example:
//   feature_storage_t* base = ...;  // level data
//   size_t t = ..., a = ..., field_idx = L0_FieldOffset::tick_ret_z;
//   size_t f_offset = get_field_offset(0, field_idx);  // or L0_FIELD_OFFSETS[field_idx]
//   size_t f_width = get_field_width(0, field_idx);    // or L0_FIELD_WIDTHS[field_idx]
//   size_t F = FIELDS_PER_LEVEL[0];  // L0_TOTAL_WIDTH
//   size_t A = num_assets;
//   // Single value:  base[t * F * A + f_offset * A + a]
//   // Full width:    &base[t * F * A + f_offset * A + a] through [f_offset + f_width - 1]
