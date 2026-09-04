#pragma once

#include "../DataDefine.hpp"
#include "../FeaturesDefine.hpp"
#include "../Misc/CSMethods.hpp"
#include "features/NodesGenerated.hpp" // CMake 从算子文件汇总: NODES(N) / LEVEL_*_FIELDS(X) / DEPTH_FIELDS(X)
#include <array>
#include <cstddef>
#include <cstdint>

// ============================================================================
// FEATURE STORE CONFIGURATION — 全部由字段表 (FIELDS_*, CMake 汇总) 编译期展开
// ============================================================================
//   布局: 平铺 [T][F_total][A], data[(t * F_total + f_offset) * A + a], 变宽字段 (1 或 LOB_DEPTH)
//   每层 (L0/L1, 由 ALL_LEVELS 注册) 生成: L*_FIELD_COUNT / L*_FIELD_WIDTHS / L*_FIELD_OFFSETS /
//   L*_TOTAL_WIDTH / L*_FieldOffset::<code> / L*_CS_DEFS / L*_FINGERPRINT; DEPTH 同一套不带层.
//   字段格式: X(code, width, valid_type, cat_l1, cat_l2, norm_method, psd, name_en, name_cn, desc, formula, SRC)
// ============================================================================

// 存储类型: _Float16 (内存/磁盘减半, F16C/AVX-512 FP16/NEON 硬件转换, ±65504, ~3.3 位有效数字)
using feature_storage_t = _Float16;

// ============================================================================
// LEVELS
// ============================================================================
#define COUNT_LEVEL(level_name, level_num, fields) +1
constexpr size_t LEVEL_COUNT = 0 ALL_LEVELS(COUNT_LEVEL);

#define GENERATE_LEVEL_ROWS(level_name, level_num, fields) level_name##_ROWS,
constexpr size_t MAX_ROWS_PER_LEVEL[LEVEL_COUNT] = {ALL_LEVELS(GENERATE_LEVEL_ROWS)};

// ============================================================================
// 字段表逐行抽取宏 (层无关)
// ============================================================================
#define FIELD_COUNT_ONE(code, width, vtype, c1, c2, norm, psd, en, cn, desc, formula, src) +1
#define FIELD_WIDTH_ONE(code, width, vtype, c1, c2, norm, psd, en, cn, desc, formula, src) width,
#define FIELD_CODE_ONE(code, width, vtype, c1, c2, norm, psd, en, cn, desc, formula, src) code,

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
template <size_t N>
constexpr size_t array_sum(const size_t (&arr)[N]) {
  size_t sum = 0;
  for (size_t i = 0; i < N; ++i)
    sum += arr[i];
  return sum;
}

// ============================================================================
// 每层: 字段数 / 宽度 / 偏移 / 总宽 / 字段下标枚举
// ============================================================================
#define GENERATE_LEVEL_FIELDS(level_name, level_num, fields)                                        \
  constexpr size_t level_name##_FIELD_COUNT = 0 fields(FIELD_COUNT_ONE);                            \
  inline constexpr size_t level_name##_FIELD_WIDTHS[] = {fields(FIELD_WIDTH_ONE)};                  \
  inline constexpr auto level_name##_FIELD_OFFSETS = generate_offsets(level_name##_FIELD_WIDTHS);   \
  constexpr size_t level_name##_TOTAL_WIDTH = array_sum(level_name##_FIELD_WIDTHS);                 \
  namespace level_name##_FieldOffset {                                                              \
    enum : size_t { fields(FIELD_CODE_ONE) }; /* 字段下标 (非偏移); 偏移用 L*_FIELD_OFFSETS[idx] */ \
  }
ALL_LEVELS(GENERATE_LEVEL_FIELDS)

// FIELDS_PER_LEVEL[lvl] = F_total, 供运行时 [T][F][A] 索引
#define GENERATE_FIELDS_PER_LEVEL_ENTRY(level_name, level_num, fields) level_name##_TOTAL_WIDTH,
constexpr size_t FIELDS_PER_LEVEL[LEVEL_COUNT] = {ALL_LEVELS(GENERATE_FIELDS_PER_LEVEL_ENTRY)};

#define GENERATE_OFFSETS_PTR(level_name, level_num, fields) level_name##_FIELD_OFFSETS.data(),
inline constexpr const size_t *FIELD_OFFSETS_PTRS[LEVEL_COUNT] = {ALL_LEVELS(GENERATE_OFFSETS_PTR)};
inline constexpr size_t get_field_offset(size_t level_idx, size_t field_idx) {
  return FIELD_OFFSETS_PTRS[level_idx][field_idx];
}

// ============================================================================
// DEPTH (盘口快照张量, 不在 ALL_LEVELS 里; T 维分钟频, 与 L1 同 T, 分钟内多次更新覆盖同一行)
// ============================================================================
constexpr size_t DEPTH_FIELD_COUNT = 0 DEPTH_FIELDS(FIELD_COUNT_ONE);
inline constexpr size_t DEPTH_FIELD_WIDTHS[] = {DEPTH_FIELDS(FIELD_WIDTH_ONE)};
inline constexpr auto DEPTH_FIELD_OFFSETS = generate_offsets(DEPTH_FIELD_WIDTHS);
constexpr size_t DEPTH_TOTAL_WIDTH = array_sum(DEPTH_FIELD_WIDTHS);
namespace DepthFieldOffset {
enum : size_t { DEPTH_FIELDS(FIELD_CODE_ONE) };
}
constexpr size_t DEPTH_ROWS = MAX_ROWS_PER_LEVEL[1];

// ============================================================================
// 编译期一致性检查
//   1. OP/FUND 列 width 必须为 1 (TS 写回按标量)
//   2. OP 列所在层 == 节点 flush 域 (onMinute→L1, 其余→L0); FUND 只能在 L1; DEPTH 层不允许 OP/FUND
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
#define CHECK_FIELD_ONE(code, width, vtype, c1, c2, norm, psd, en, cn, desc, formula, src)                       \
  static_assert(SRC_KIND_##src != FeatureDataType::TS || (width) == 1, "OP/FUND field must be width 1: " #code); \
  static_assert(SRC_LEVEL_##src < 0 || SRC_LEVEL_##src == kLevel, "field level != node flush level: " #code);
#define GENERATE_CHECK_LEVEL(level_name, level_num, fields) \
  namespace level_name##_level_check {                      \
    constexpr int kLevel = level_num;                       \
    fields(CHECK_FIELD_ONE)                                 \
  }
ALL_LEVELS(GENERATE_CHECK_LEVEL)
namespace depth_level_check {
constexpr int kLevel = -2;
DEPTH_FIELDS(CHECK_FIELD_ONE)
} // namespace depth_level_check

// ============================================================================
// 截面表 (由字段表 CS(src_lvl, src, tf, m) 列生成): L*_CS_DEFS[] / L*_CS_COUNT
//   每行 = 源列 + 元素变换 + 截面方法 → 目标列; CoreCrosssection 按表 gather → cs::apply → scatter.
//   数组末尾带哨兵 (dst == SIZE_MAX), 允许某层没有 CS 字段.
// ============================================================================
struct CSFeatureDef {
  std::uint8_t src_lvl; // 源列所在层 (0 = L0 取分钟起始秒, 1 = L1)
  cs::Transform tf;     // 元素变换
  cs::Method method;    // 截面方法
  std::size_t src;      // 源列 (field index, 用 L*_FIELD_OFFSETS[src] 取 offset)
  std::size_t dst;      // 目标列 (本层 field index)
};

#define CS_ROW_CS(code, src_lvl, s, tf, m) {src_lvl, cs::Transform::tf, cs::Method::m, L##src_lvl##_FieldOffset::s, FO::code},
#define CS_ROW_OP(code, ...)
#define CS_ROW_FUND(code, ...)
#define CS_ROW_LABEL(code)
#define CS_ROW_META(code)
#define CS_ROW_ONE(code, width, vtype, c1, c2, norm, psd, en, cn, desc, formula, src) SRC_DISPATCH(CS_ROW, code, src)

#define GENERATE_CS_TABLE(level_name, level_num, fields)                                    \
  namespace level_name##_cs_detail {                                                        \
    namespace FO = level_name##_FieldOffset;                                                \
    inline constexpr CSFeatureDef DEFS[] = {fields(CS_ROW_ONE){0, {}, {}, 0, SIZE_MAX}};    \
  }                                                                                         \
  inline constexpr const CSFeatureDef *level_name##_CS_DEFS = level_name##_cs_detail::DEFS; \
  constexpr size_t level_name##_CS_COUNT = sizeof(level_name##_cs_detail::DEFS) / sizeof(CSFeatureDef) - 1;
ALL_LEVELS(GENERATE_CS_TABLE)

// ============================================================================
// 字段表指纹 (写入文件头, 读取时比对; 表改了旧文件立刻报错而非静默错位)
//   FNV-1a 64 over "code:width:kind" 逐字段拼接 (顺序敏感)
// ============================================================================
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
#define FINGERPRINT_ONE(code, width, vtype, c1, c2, norm, psd, en, cn, desc, formula, src) \
  h = fnv1a_u64(fnv1a_u64(fnv1a_str(h, #code), (width)), static_cast<uint64_t>(SRC_KIND_##src));

#define GENERATE_FINGERPRINT(level_name, level_num, fields) \
  constexpr uint64_t level_name##_FINGERPRINT = [] {        \
    uint64_t h = 0xcbf29ce484222325ULL;                     \
    fields(FINGERPRINT_ONE) return h;                       \
  }();
ALL_LEVELS(GENERATE_FINGERPRINT)
#define GENERATE_FINGERPRINT_ENTRY(level_name, level_num, fields) level_name##_FINGERPRINT,
constexpr uint64_t LEVEL_FINGERPRINTS[LEVEL_COUNT] = {ALL_LEVELS(GENERATE_FINGERPRINT_ENTRY)};
constexpr uint64_t DEPTH_FINGERPRINT = [] {
  uint64_t h = 0xcbf29ce484222325ULL;
  DEPTH_FIELDS(FINGERPRINT_ONE)
  return h;
}();

// 特征文件头: size_t × {T, F, A, axis_hash, table_fingerprint}
constexpr size_t FEATURE_FILE_HEADER_WORDS = 5;
