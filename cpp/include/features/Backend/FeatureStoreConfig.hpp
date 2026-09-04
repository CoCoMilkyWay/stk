#pragma once

#include "features/FeaturesDefine.hpp"
#include "features/Misc/CSMethods.hpp"
#include "features/NodesGenerated.hpp" // CMake 从算子文件汇总: NODES(N) / L0_FIELDS(X) / L1_FIELDS(X) / DEPTH_FIELDS(X)
#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <string>
#include <string_view>

// ============================================================================
// FEATURE STORE CONFIGURATION — 全部由字段表 (FIELDS_*, CMake 汇总) + ALL_LEVELS 编译期展开
// ============================================================================
//   布局: 每层平铺 [T][F_total][A], data[(t * F_total + offset) * A + a], 变宽字段 (1 或 LOB_DEPTH)
//   每层 <LVL> ∈ ALL_LEVELS 生成:
//     <LVL>_FIELD_INFO[]     每列 {code, width, valid, kind} (宽 / 有效性 / 类型均由 SRC 列推出)
//     <LVL>_FIELD_OFFSETS[]  列下标 → 行内偏移
//     <LVL>_Field::<code>    列下标枚举 (非偏移)
//     <LVL>_CS_DEFS[]        截面表
//     <LVL>_FINGERPRINT      字段表指纹 (落文件头)
//   汇总成 LEVELS[lvl] (LevelInfo), 运行时按层下标取 rows / width / offsets / fingerprint / 文件布局.
//   字段格式: X(code, cat_l1, cat_l2, norm_method, name_en, name_cn, desc, formula, SRC)
// ============================================================================

// 存储类型: _Float16 (内存/磁盘减半, F16C/AVX-512 FP16/NEON 硬件转换, ±65504, ~3.3 位有效数字)
using feature_storage_t = _Float16;

// ============================================================================
// 节点 flush 域 (OP 列的层 / 有效性标志由此推出)
// ============================================================================
namespace node_flush {
#define NODE_FLUSH_ONE(name, type, args, ...) constexpr Trigger name = NODE_FLUSH(__VA_ARGS__);
NODES(NODE_FLUSH_ONE)
#undef NODE_FLUSH_ONE
} // namespace node_flush

// ============================================================================
// 字段: 每层一张 FieldInfo 表 + 偏移 + 下标枚举
// ============================================================================
struct FieldInfo {
  const char *code;
  size_t width;
  L2::ValidType valid;
  FeatureDataType kind;
};

#define FIELD_INFO_ONE(code, c1, c2, norm, en, cn, desc, formula, src) {#code, SRC_WIDTH_##src, SRC_VALID_##src, SRC_KIND_##src},
#define FIELD_CODE_ONE(code, c1, c2, norm, en, cn, desc, formula, src) code,

template <size_t N>
constexpr auto field_offsets(const FieldInfo (&f)[N]) {
  std::array<size_t, N> offsets{};
  size_t acc = 0;
  for (size_t i = 0; i < N; ++i) {
    offsets[i] = acc;
    acc += f[i].width;
  }
  return offsets;
}
template <size_t N>
constexpr size_t total_width(const FieldInfo (&f)[N]) {
  size_t w = 0;
  for (size_t i = 0; i < N; ++i)
    w += f[i].width;
  return w;
}
// 某类型 (LB 等) 列的首下标 / 个数 (标签回填按此定位, 不依赖列名)
template <size_t N>
constexpr size_t first_of_kind(const FieldInfo (&f)[N], FeatureDataType k) {
  for (size_t i = 0; i < N; ++i)
    if (f[i].kind == k)
      return i;
  return N;
}
template <size_t N>
constexpr size_t count_of_kind(const FieldInfo (&f)[N], FeatureDataType k) {
  size_t c = 0;
  for (size_t i = 0; i < N; ++i)
    c += f[i].kind == k;
  return c;
}
// 同类型列必须连续 (标签组按下标区间写)
template <size_t N>
constexpr bool kind_contiguous(const FieldInfo (&f)[N], FeatureDataType k) {
  const size_t b = first_of_kind(f, k), c = count_of_kind(f, k);
  for (size_t i = b; i < b + c; ++i)
    if (f[i].kind != k)
      return false;
  return true;
}

#define GENERATE_LEVEL_FIELDS(name, num, fields, rows, psd, columnar)                                \
  inline constexpr FieldInfo name##_FIELD_INFO[] = {fields(FIELD_INFO_ONE)};                         \
  constexpr size_t name##_FIELD_COUNT = std::size(name##_FIELD_INFO);                                \
  inline constexpr auto name##_FIELD_OFFSETS = field_offsets(name##_FIELD_INFO);                     \
  constexpr size_t name##_TOTAL_WIDTH = total_width(name##_FIELD_INFO);                              \
  namespace name##_Field {                                                                           \
    enum : size_t { fields(FIELD_CODE_ONE) }; /* 列下标 (非偏移); 偏移用 <LVL>_FIELD_OFFSETS[idx] */ \
  }
ALL_LEVELS(GENERATE_LEVEL_FIELDS)

// ============================================================================
// 编译期一致性检查: 列所在层 == 来源允许的层
//   OP → 节点 flush 域 (onMinute→L1, 其余→L0); FUND → L1; CS/LABEL → L0/L1; META(w) → DEPTH; FLAG 任意
// ============================================================================
#define SRC_LEVEL_OP(node, ...) level_of(node_flush::node)
#define SRC_LEVEL_FUND(...) 1
#define SRC_LEVEL_CS(...) (kLevel == 2 ? -1 : kLevel)
#define SRC_LEVEL_LABEL (kLevel == 2 ? -1 : kLevel)
#define SRC_LEVEL_FLAG kLevel
#define SRC_LEVEL_META(w) 2
#define CHECK_FIELD_ONE(code, c1, c2, norm, en, cn, desc, formula, src) \
  static_assert(SRC_LEVEL_##src == kLevel, "field level != source level: " #code);
#define GENERATE_CHECK_LEVEL(name, num, fields, rows, psd, columnar) \
  namespace name##_level_check {                                     \
    constexpr int kLevel = num;                                      \
    fields(CHECK_FIELD_ONE)                                          \
  }
ALL_LEVELS(GENERATE_CHECK_LEVEL)

// ============================================================================
// 截面表 (由字段表 CS(src_lvl, src, tf, m) 列生成): <LVL>_CS_DEFS[] / <LVL>_CS_COUNT
//   每行 = 源列 + 元素变换 + 截面方法 → 目标列; CoreCrosssection 按表 gather → cs::apply → scatter.
//   数组末尾带哨兵 (dst == SIZE_MAX), 允许某层没有 CS 字段.
// ============================================================================
struct CSFeatureDef {
  std::uint8_t src_lvl; // 源列所在层 (0 = L0 取分钟起始秒, 1 = L1)
  cs::Transform tf;     // 元素变换
  cs::Method method;    // 截面方法
  std::size_t src;      // 源列 (field index, 用 <LVL>_FIELD_OFFSETS[src] 取 offset)
  std::size_t dst;      // 目标列 (本层 field index)
};

#define CS_ROW_CS(code, src_lvl, s, tf, m) {src_lvl, cs::Transform::tf, cs::Method::m, L##src_lvl##_Field::s, FO::code},
#define CS_ROW_OP(code, ...)
#define CS_ROW_FUND(code, ...)
#define CS_ROW_LABEL(code)
#define CS_ROW_FLAG(code)
#define CS_ROW_META(code, w)
#define CS_ROW_ONE(code, c1, c2, norm, en, cn, desc, formula, src) SRC_DISPATCH(CS_ROW, code, src)

#define GENERATE_CS_TABLE(name, num, fields, rows, psd, columnar)                        \
  namespace name##_cs_detail {                                                           \
    namespace FO = name##_Field;                                                         \
    inline constexpr CSFeatureDef DEFS[] = {fields(CS_ROW_ONE){0, {}, {}, 0, SIZE_MAX}}; \
  }                                                                                      \
  inline constexpr const CSFeatureDef *name##_CS_DEFS = name##_cs_detail::DEFS;          \
  constexpr size_t name##_CS_COUNT = std::size(name##_cs_detail::DEFS) - 1;
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
template <size_t N>
constexpr uint64_t table_fingerprint(const FieldInfo (&f)[N]) {
  uint64_t h = 0xcbf29ce484222325ULL;
  for (size_t i = 0; i < N; ++i)
    h = fnv1a_u64(fnv1a_u64(fnv1a_str(h, f[i].code), f[i].width), static_cast<uint64_t>(f[i].kind));
  return h;
}
#define GENERATE_FINGERPRINT(name, num, fields, rows, psd, columnar) \
  constexpr uint64_t name##_FINGERPRINT = table_fingerprint(name##_FIELD_INFO);
ALL_LEVELS(GENERATE_FINGERPRINT)

// ============================================================================
// 层表: LEVELS[lvl] — 运行时按层下标索引的一切
// ============================================================================
struct LevelInfo {
  const char *level_name;  // "L0" / "L1" / "DEPTH": 文件名 features_<name>[_f<i>].zst
  size_t rows;             // T
  size_t width;            // F_total
  size_t field_count;      // 列数 (≤ width)
  const FieldInfo *fields; // [field_count]
  const size_t *offsets;   // [field_count] 列下标 → 行内偏移
  uint64_t fingerprint;    // 字段表指纹
  const char *psd;         // 该层特征的推荐频谱 (GUI 元数据)
  bool columnar;           // true: 每列一个文件 (按列选读); false: 整层一个文件
};
#define LEVEL_INFO_ONE(name, num, fields, rows, psd, columnar) \
  {#name, rows, name##_TOTAL_WIDTH, name##_FIELD_COUNT, name##_FIELD_INFO, name##_FIELD_OFFSETS.data(), name##_FINGERPRINT, psd, columnar},
inline constexpr LevelInfo LEVELS[] = {ALL_LEVELS(LEVEL_INFO_ONE)};
constexpr size_t LEVEL_COUNT = std::size(LEVELS);
#define CHECK_LEVEL_INDEX(name, num, fields, rows, psd, columnar) \
  static_assert(std::string_view(LEVELS[num].level_name) == #name, "ALL_LEVELS index must equal position");
ALL_LEVELS(CHECK_LEVEL_INDEX)
static_assert(LEVELS[2].rows == LEVELS[1].rows, "DEPTH shares the L1 minute axis");

// 特征文件: <base>/YYYY/MM/DD/features_<LVL>.zst (整层) 或 features_<LVL>_f<i>.zst (逐列)
// 头 = size_t × {T, F, A, axis_hash, table_fingerprint}
constexpr size_t FEATURE_FILE_HEADER_WORDS = 5;
inline std::string feature_day_dir(const std::string &base, const std::string &date) {
  return base + "/" + date.substr(0, 4) + "/" + date.substr(4, 2) + "/" + date.substr(6, 2);
}
inline std::string feature_file(const std::string &day_dir, size_t lvl) {
  return day_dir + "/features_" + LEVELS[lvl].level_name + ".zst";
}
inline std::string feature_column_file(const std::string &day_dir, size_t lvl, size_t col) {
  return day_dir + "/features_" + LEVELS[lvl].level_name + "_f" + std::to_string(col) + ".zst";
}
