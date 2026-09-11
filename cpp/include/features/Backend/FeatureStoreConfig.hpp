#pragma once

#include "FeatureLevels.hpp" // 稳定层: feature_storage_t / FieldInfo / LevelInfo / 文件布局 / 编码选型
#include "features/FeaturesDefine.hpp"
#include "features/FieldsGenerated.hpp" // CMake 从算子文件汇总 (宏文本, 不含算子 #include): NODES(N) / L0_FIELDS(X) / L1_FIELDS(X)
#include "features/Method/TS.hpp"       // OP 行 Tf (FieldInfo.ts_tf → 指纹; RowWriter 落盘套用)
#include "features/TimeIndex.hpp"       // ALL_LEVELS 的 rows 参数 (L0_ROWS / L1_ROWS) 在此展开
#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <string_view>

// ============================================================================
// FEATURE STORE CONFIGURATION — 全部由字段表 (FIELDS_*, CMake 汇总) + ALL_LEVELS 编译期展开
// ============================================================================
// 写端专用 (FeatureStore / Core* / FeatureMeta.cpp): 编译期表保住常量定址折叠.
// 读端/GUI 走 FeatureLevels.hpp 的 level_info(lvl) (运行时), 不依赖本文件 ——
// 增删改特征只重编写端 + FeatureMeta.cpp.
//   布局: 每层平铺 [T][F_total][A], data[(t * F_total + offset) * A + a]
//   每层 <LVL> ∈ ALL_LEVELS 生成:
//     <LVL>_FIELD_INFO[]     每列 {code, width, valid, kind} (宽 / 有效性 / 类型均由 SRC 列推出)
//     <LVL>_FIELD_OFFSETS[]  列下标 → 行内偏移
//     <LVL>_Field::<code>    列下标枚举 (非偏移)
//     <LVL>_FINGERPRINT      字段表指纹 (落文件头)
//   汇总成 LEVELS[lvl] (LevelInfo), 编译期/运行时按层下标取 rows / width / offsets / fingerprint.
//   字段格式: X(code, cat_l1, cat_l2, name_en, name_cn, desc, formula, SRC)
// ============================================================================

// ============================================================================
// 节点 flush 域 (OP 列的层 / 有效性标志由此推出)
// ============================================================================
namespace node_flush {
#define NODE_FLUSH_ONE(name, type, args, ...) constexpr Trigger name = NODE_FLUSH(__VA_ARGS__);
NODES(NODE_FLUSH_ONE)
#undef NODE_FLUSH_ONE
} // namespace node_flush

// ============================================================================
// 字段: 每层一张 FieldInfo 表 + 偏移 + 下标枚举 (FieldInfo 结构体在 FeatureLevels.hpp)
// ============================================================================
#define FIELD_INFO_ONE(code, c1, c2, en, cn, desc, formula, src) {#code, SRC_WIDTH_##src, SRC_VALID_##src, SRC_KIND_##src, SRC_TS_TF_##src},
#define FIELD_CODE_ONE(code, c1, c2, en, cn, desc, formula, src) code,

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

#define GENERATE_LEVEL_FIELDS(name, num, fields, rows, psd, columnar, xor_delta)                     \
  inline constexpr FieldInfo name##_FIELD_INFO[] = {fields(FIELD_INFO_ONE)};                         \
  constexpr size_t name##_FIELD_COUNT = std::size(name##_FIELD_INFO);                                \
  inline constexpr auto name##_FIELD_OFFSETS = field_offsets(name##_FIELD_INFO);                     \
  constexpr size_t name##_TOTAL_WIDTH = total_width(name##_FIELD_INFO);                              \
  namespace name##_Field {                                                                           \
    enum : size_t { fields(FIELD_CODE_ONE) }; /* 列下标 (非偏移); 偏移用 <LVL>_FIELD_OFFSETS[idx] */ \
  }
ALL_LEVELS(GENERATE_LEVEL_FIELDS)

// ============================================================================
// 每个字段的"来源" (GUI 特征依赖解析用)
//   OP(node[,port]) → 节点名 (如 "Ci_5") + 口名 (单口节点 ""); CS(lvl,src,...) → 源字段 code; 其余 → ""
//   口名配合 node_deps::TABLE 的 "Up.port" 项: 下游只引用某一口时, 依赖只算该口的字段
// ============================================================================
struct FieldSource {
  const char *code;   // 字段 code
  const char *source; // 节点名 (OP) / 源字段 code (CS) / 空 (LABEL/FLAG)
  const char *port;   // 口名 (OP 多口) / 空
};
#define SRCSRC_OP(code, node, ...) #node
#define SRCSRC_CS(code, lvl, src, ...) #src
#define SRCSRC_LABEL(code) ""
#define SRCSRC_FLAG(code) ""
#define SRCPORT_OP_3(node, tf, m) ""
#define SRCPORT_OP_4(node, port, tf, m) #port
#define SRCPORT_OP(code, ...) SRC_OP_PICK(__VA_ARGS__, SRCPORT_OP_4, SRCPORT_OP_3, , )(__VA_ARGS__)
#define SRCPORT_CS(...) ""
#define SRCPORT_LABEL(code) ""
#define SRCPORT_FLAG(code) ""
#define FIELD_SOURCE_ONE(code, c1, c2, en, cn, desc, formula, src) {#code, SRC_DISPATCH(SRCSRC, code, src), SRC_DISPATCH(SRCPORT, code, src)},
#define GENERATE_LEVEL_SOURCES(name, num, fields, rows, psd, columnar, xor_delta) \
  inline constexpr FieldSource name##_FIELD_SOURCE[] = {fields(FIELD_SOURCE_ONE)};
ALL_LEVELS(GENERATE_LEVEL_SOURCES)

// ============================================================================
// 编译期一致性检查: 列所在层 == 来源允许的层
//   OP → 节点 flush 域 (onMinute→L1, 其余→L0); CS/LABEL → L0/L1; FLAG 任意
// ============================================================================
#define SRC_LEVEL_OP(node, ...) level_of(node_flush::node)
#define SRC_LEVEL_CS(...) kLevel
#define SRC_LEVEL_LABEL kLevel
#define SRC_LEVEL_FLAG kLevel
#define CHECK_FIELD_ONE(code, c1, c2, en, cn, desc, formula, src) \
  static_assert(SRC_LEVEL_##src == kLevel, "field level != source level: " #code);
#define GENERATE_CHECK_LEVEL(name, num, fields, rows, psd, columnar, xor_delta) \
  namespace name##_level_check {                                                \
    constexpr int kLevel = num;                                                 \
    fields(CHECK_FIELD_ONE)                                                     \
  }
ALL_LEVELS(GENERATE_CHECK_LEVEL)

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
  for (size_t i = 0; i < N; ++i) {
    h = fnv1a_u64(fnv1a_u64(fnv1a_str(h, f[i].code), f[i].width), static_cast<uint64_t>(f[i].kind));
    if (f[i].ts_tf != ts::TfId::None) // Tf 改变落盘值; None 不折入, 旧文件 (无 Tf 时代) 指纹不变
      h = fnv1a_u64(h, static_cast<uint64_t>(f[i].ts_tf));
  }
  return h;
}
#define GENERATE_FINGERPRINT(name, num, fields, rows, psd, columnar, xor_delta) \
  constexpr uint64_t name##_FINGERPRINT = table_fingerprint(name##_FIELD_INFO);
ALL_LEVELS(GENERATE_FINGERPRINT)

// ============================================================================
// 层表: LEVELS[lvl] — 编译期版本 (写端定址折叠; LevelInfo 结构体在 FeatureLevels.hpp)
// 运行时入口 level_info(lvl) 在 FeatureMeta.cpp 定义, 指向本表.
// ============================================================================
#define LEVEL_INFO_ONE(name, num, fields, rows, psd, columnar, xor_delta) \
  {#name, rows, name##_TOTAL_WIDTH, name##_FIELD_COUNT, name##_FIELD_INFO, name##_FIELD_OFFSETS.data(), name##_FINGERPRINT, psd, columnar, xor_delta},
inline constexpr LevelInfo LEVELS[] = {ALL_LEVELS(LEVEL_INFO_ONE)};
static_assert(std::size(LEVELS) == LEVEL_COUNT, "LEVELS 与 ALL_LEVELS 项数一致");
#define CHECK_LEVEL_INDEX(name, num, fields, rows, psd, columnar, xor_delta) \
  static_assert(std::string_view(LEVELS[num].level_name) == #name, "ALL_LEVELS index must equal position");
ALL_LEVELS(CHECK_LEVEL_INDEX)

static_assert(LEVELS[0].rows - 1 == TRADE_SECONDS_PER_DAY && LEVELS[1].rows - 1 == TRADE_MINUTES_PER_DAY,
              "每层落盘 T 必须是 有效行数 + 1 哨兵行");
