#pragma once

// 落盘编码两实现 (同 API 对仗), 选型见文件末尾 FeatureCodec
#include "SparseCodec.hpp"
#include "ZstdCodec.hpp" // IWYU pragma: keep
#include "features/FeaturesDefine.hpp"
#include "features/NodesGenerated.hpp" // CMake 从算子文件汇总: NODES(N) / L0_FIELDS(X) / L1_FIELDS(X) / DEPTH_FIELDS(X)
#include "features/TimeIndex.hpp"      // ALL_LEVELS 的 rows 参数 (L0_ROWS / L1_ROWS) 在此展开
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
//   OP(node[,port]) → 节点名 (如 "Ci_5"); CS(lvl,src,...) → 源字段 code; 其余 → ""
// ============================================================================
struct FieldSource {
  const char *code;   // 字段 code
  const char *source; // 节点名 (OP) / 源字段 code (CS) / 空 (LABEL/FLAG/META)
};
#define SRCSRC_OP(code, node, ...) #node
#define SRCSRC_CS(code, lvl, src, ...) #src
#define SRCSRC_LABEL(code) ""
#define SRCSRC_FLAG(code) ""
#define SRCSRC_META(code, w) ""
#define FIELD_SOURCE_ONE(code, c1, c2, norm, en, cn, desc, formula, src) {#code, SRC_DISPATCH(SRCSRC, code, src)},
#define GENERATE_LEVEL_SOURCES(name, num, fields, rows, psd, columnar, xor_delta) \
  inline constexpr FieldSource name##_FIELD_SOURCE[] = {fields(FIELD_SOURCE_ONE)};
ALL_LEVELS(GENERATE_LEVEL_SOURCES)

// ============================================================================
// 编译期一致性检查: 列所在层 == 来源允许的层
//   OP → 节点 flush 域 (onMinute→L1, 其余→L0); CS/LABEL → L0/L1; META(w) → DEPTH; FLAG 任意
// ============================================================================
#define SRC_LEVEL_OP(node, ...) level_of(node_flush::node)
#define SRC_LEVEL_CS(...) (kLevel == 2 ? -1 : kLevel)
#define SRC_LEVEL_LABEL (kLevel == 2 ? -1 : kLevel)
#define SRC_LEVEL_FLAG kLevel
#define SRC_LEVEL_META(w) 2
#define CHECK_FIELD_ONE(code, c1, c2, norm, en, cn, desc, formula, src) \
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
  for (size_t i = 0; i < N; ++i)
    h = fnv1a_u64(fnv1a_u64(fnv1a_str(h, f[i].code), f[i].width), static_cast<uint64_t>(f[i].kind));
  return h;
}
#define GENERATE_FINGERPRINT(name, num, fields, rows, psd, columnar, xor_delta) \
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
  bool xor_delta;          // true: 落盘前沿 T 轴 XOR 差分 (无损预变换, 提升 zstd 收益, 见 ALL_LEVELS)
};
#define LEVEL_INFO_ONE(name, num, fields, rows, psd, columnar, xor_delta) \
  {#name, rows, name##_TOTAL_WIDTH, name##_FIELD_COUNT, name##_FIELD_INFO, name##_FIELD_OFFSETS.data(), name##_FINGERPRINT, psd, columnar, xor_delta},
inline constexpr LevelInfo LEVELS[] = {ALL_LEVELS(LEVEL_INFO_ONE)};
constexpr size_t LEVEL_COUNT = std::size(LEVELS);
#define CHECK_LEVEL_INDEX(name, num, fields, rows, psd, columnar, xor_delta) \
  static_assert(std::string_view(LEVELS[num].level_name) == #name, "ALL_LEVELS index must equal position");
ALL_LEVELS(CHECK_LEVEL_INDEX)
static_assert(LEVELS[2].rows == LEVELS[1].rows, "DEPTH shares the L1 minute axis");

// 有效行数 = 落盘行数 - 1: 末行是哨兵 (label lookahead 的落点, 不是真实时间).
// rows 用于缓冲区 / stride; 消费端迭代时间轴一律用本函数, 不要用 rows.
inline constexpr size_t level_valid_rows(size_t lvl) { return LEVELS[lvl].rows - 1; }
static_assert(level_valid_rows(0) == TRADE_SECONDS_PER_DAY && level_valid_rows(1) == TRADE_MINUTES_PER_DAY &&
                  level_valid_rows(2) == TRADE_MINUTES_PER_DAY,
              "每层落盘 T 必须是 有效行数 + 1 哨兵行");

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

// ============================================================================
// T 轴 XOR 差分 (LEVELS[lvl].xor_delta 层的落盘预变换, 无损, 就地)
//   编码 (写端, 压缩前): row[t] ^= row[t-1], t 从高往低; row = 文件内一个时间行
//   (整层文件 F_total×A, 逐列文件 1×A). 解码 (读端, 解压后): 前缀 XOR, t 从低往高.
//   fp16 按位当 uint16 处理, 往返恒等 (含 NaN/Inf 位型).
// ============================================================================
static_assert(sizeof(feature_storage_t) == sizeof(uint16_t));
inline void xor_delta_encode(feature_storage_t *data, size_t T, size_t row_elems) {
  auto *u = reinterpret_cast<uint16_t *>(data);
  for (size_t t = T - 1; t >= 1; --t)
    for (size_t i = 0; i < row_elems; ++i)
      u[t * row_elems + i] ^= u[(t - 1) * row_elems + i];
}
inline void xor_delta_decode(feature_storage_t *data, size_t T, size_t row_elems) {
  auto *u = reinterpret_cast<uint16_t *>(data);
  for (size_t t = 1; t < T; ++t)
    for (size_t i = 0; i < row_elems; ++i)
      u[t * row_elems + i] ^= u[(t - 1) * row_elems + i];
}

// ============================================================================
// 落盘编码选型: 两种实现同一 API (bound / encode / decode), 载荷不落自述信息,
// 读端按此别名解码 —— 换选型 = 换别名 + 重算特征库 (解码断言会拦住旧文件)
//   SparseCodec  位图+非零字面: 全天 ~45%, 内存带宽级, IO 单核无压力 (默认)
//   ZstdCodec    zstd 熵压缩:   全天 ~29%, 单核 ~185 MB/s 顶不住落盘节奏
//   CODEC_ENABLED = false: 裸写直读 (写读两端 if constexpr 免掉中转拷贝)
// ============================================================================
using FeatureCodec = SparseCodec;
inline constexpr bool CODEC_ENABLED = true;
