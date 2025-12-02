#pragma once

#include "../FeaturesDefine.hpp"
#include <array>
#include <cstddef>

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
    LEVEL_CONFIGS[2].max_capacity(),
};

// ============================================================================
// FIELD METADATA - PART 1: Auxiliary Macros (Per-Level)
// ============================================================================
// HOW TO ADD NEW LEVEL (e.g., L3):
// 1. FeaturesDefine.hpp: Add LEVEL_3_FIELDS + update ALL_LEVELS
// 2. Below: Copy-paste L2 macros, rename L2→L3 (3 macros total)
// 3. Done! All arrays/enums auto-generated
// ============================================================================

// Field format: X(code, width, data_type, cat_l1, cat_l2, norm_method, name_en, name_cn, formula, description)

// Count auxiliary (level-agnostic)
#define COUNT_FIELD(code, width, dtype, c1, c2, norm, en, cn, formula, desc) +1

// Width extractors (per-level)
#define GENERATE_FIELD_WIDTH_L0(code, width, dtype, c1, c2, norm, en, cn, formula, desc) width,
#define GENERATE_FIELD_WIDTH_L1(code, width, dtype, c1, c2, norm, en, cn, formula, desc) width,
#define GENERATE_FIELD_WIDTH_L2(code, width, dtype, c1, c2, norm, en, cn, formula, desc) width,

// Index extractors (per-level)
#define GENERATE_FIELD_INDEX_L0(code, width, dtype, c1, c2, norm, en, cn, formula, desc) code,
#define GENERATE_FIELD_INDEX_L1(code, width, dtype, c1, c2, norm, en, cn, formula, desc) code,
#define GENERATE_FIELD_INDEX_L2(code, width, dtype, c1, c2, norm, en, cn, formula, desc) code,

// Type metadata extractors (per-level)
#define GENERATE_FIELD_TYPE_META_L0(code, width, dtype, c1, c2, norm, en, cn, formula, desc) \
  {L0_FieldOffset::code, FeatureDataType::dtype},
#define GENERATE_FIELD_TYPE_META_L1(code, width, dtype, c1, c2, norm, en, cn, formula, desc) \
  {L1_FieldOffset::code, FeatureDataType::dtype},
#define GENERATE_FIELD_TYPE_META_L2(code, width, dtype, c1, c2, norm, en, cn, formula, desc) \
  {L2_FieldOffset::code, FeatureDataType::dtype},

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

// 1. Field counts: L0_FIELD_COUNT, L1_FIELD_COUNT, L2_FIELD_COUNT, ...
#define GENERATE_FIELD_COUNT(level_name, level_num, fields) \
  constexpr size_t level_name##_FIELD_COUNT = 0 fields(COUNT_FIELD);
ALL_LEVELS(GENERATE_FIELD_COUNT)

// 2. Field widths: L0_FIELD_WIDTHS[], L1_FIELD_WIDTHS[], L2_FIELD_WIDTHS[], ...
#define GENERATE_FIELD_WIDTHS(level_name, level_num, fields) \
  inline constexpr size_t level_name##_FIELD_WIDTHS[] = {    \
      fields(GENERATE_FIELD_WIDTH_##level_name)};
ALL_LEVELS(GENERATE_FIELD_WIDTHS)

// 3. Field offsets: L0_FIELD_OFFSETS[], L1_FIELD_OFFSETS[], L2_FIELD_OFFSETS[], ...
#define GENERATE_FIELD_OFFSETS(level_name, level_num, fields) \
  inline constexpr auto level_name##_FIELD_OFFSETS = generate_offsets(level_name##_FIELD_WIDTHS);
ALL_LEVELS(GENERATE_FIELD_OFFSETS)

// 4. Total widths: L0_TOTAL_WIDTH, L1_TOTAL_WIDTH, L2_TOTAL_WIDTH, ...
#define GENERATE_TOTAL_WIDTH(level_name, level_num, fields) \
  constexpr size_t level_name##_TOTAL_WIDTH = array_sum(level_name##_FIELD_WIDTHS, level_name##_FIELD_COUNT);
ALL_LEVELS(GENERATE_TOTAL_WIDTH)

// 5. Field index enums: L0_FieldOffset::*, L1_FieldOffset::*, L2_FieldOffset::*, ...
//    (Use L*_FIELD_OFFSETS[idx] to get actual offset in flat array)
//    (Use L*_FIELD_WIDTHS[idx] to get field width)
#define GENERATE_FIELD_INDEX_ENUM(level_name, level_num, fields) \
  namespace level_name##_FieldOffset {                           \
    enum : size_t {                                              \
      fields(GENERATE_FIELD_INDEX_##level_name)                  \
    };                                                           \
  }
ALL_LEVELS(GENERATE_FIELD_INDEX_ENUM)

// 6. Field type metadata: L0_FIELD_TYPES[], L1_FIELD_TYPES[], L2_FIELD_TYPES[], ...
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

// ============================================================================
// FEATURE TYPE RANGES - Optimized TS/CS/LB/SH/META Access
// ============================================================================

struct FeatureRange {
  size_t start;
  size_t end;
  size_t count() const { return end - start; }
};

// Compile-time range computation
template <size_t Level, const FieldTypeMeta *types, size_t count>
constexpr FeatureRange compute_feature_range(FeatureDataType type) {
  size_t start = count;
  size_t end = 0;
  for (size_t i = 0; i < count; ++i) {
    if (types[i].type == type) {
      if (types[i].offset < start)
        start = types[i].offset;
      if (types[i].offset >= end)
        end = types[i].offset + 1;
    }
  }
  return {start, end};
}

// Template specializations (generic)
template <size_t Level>
constexpr FeatureRange get_feature_range(FeatureDataType type);

#define GENERATE_FEATURE_RANGE_SPECIALIZATION(level_name, level_num, fields)                      \
  template <>                                                                                     \
  constexpr FeatureRange get_feature_range<level_num>(FeatureDataType type) {                     \
    return compute_feature_range<level_num, level_name##_FIELD_TYPES,                             \
                                 sizeof(level_name##_FIELD_TYPES) / sizeof(FieldTypeMeta)>(type); \
  }
ALL_LEVELS(GENERATE_FEATURE_RANGE_SPECIALIZATION)

// Per-level range constants: L*_TS_RANGE, L*_CS_RANGE, ...
#define GENERATE_RANGE_CONSTANTS(level_name, level_num, fields)                             \
  constexpr auto level_name##_TS_RANGE = get_feature_range<level_num>(FeatureDataType::TS); \
  constexpr auto level_name##_CS_RANGE = get_feature_range<level_num>(FeatureDataType::CS); \
  constexpr auto level_name##_LB_RANGE = get_feature_range<level_num>(FeatureDataType::LB); \
  constexpr auto level_name##_SH_RANGE = get_feature_range<level_num>(FeatureDataType::SH); \
  constexpr auto level_name##_META_RANGE = get_feature_range<level_num>(FeatureDataType::META);
ALL_LEVELS(GENERATE_RANGE_CONSTANTS)

// ============================================================================
// TIME INDEX CONVERSION
// ============================================================================

inline size_t time_to_index(size_t level_idx, uint8_t hour, uint8_t minute,
                            uint8_t second, uint8_t millisecond) {
  const LevelTimeConfig &cfg = LEVEL_CONFIGS[level_idx];

  switch (cfg.unit) {
  case TimeUnit::MILLISECOND: {
    const size_t ms = time_to_trading_milliseconds(hour, minute, second, millisecond);
    return ms / cfg.interval;
  }
  case TimeUnit::SECOND: {
    const size_t sec = time_to_trading_seconds(hour, minute, second);
    return sec / cfg.interval;
  }
  case TimeUnit::MINUTE: {
    const size_t sec = time_to_trading_seconds(hour, minute, second);
    return (sec / 60) / cfg.interval;
  }
  case TimeUnit::HOUR: {
    const size_t sec = time_to_trading_seconds(hour, minute, second);
    return (sec / 3600) / cfg.interval;
  }
  }
  return 0;
}
