#pragma once

#include "FeatureLevels.hpp" // 稳定层: feature_storage_t / FieldInfo / LevelInfo / TierId
// FieldsGenerated: CMake 从节点文件汇总 (宏文本, 不含节点 #include): <Tier>_NODES(N) / <Tier>_L*_FIELDS(X); 经 ALL_TIERS 拼名使用
#include "features/Bar/FieldsGenerated.hpp" // IWYU pragma: keep
#include "features/FeaturesDefine.hpp"
#include "features/Lob/FieldsGenerated.hpp" // IWYU pragma: keep
#include "features/Method/TS.hpp"           // TS 行 Tf (FieldInfo.ts_tf → 指纹; RowWriter 落盘套用)
#include "features/TimeIndex.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <span>
#include <string_view>

// ============================================================================
// FEATURE STORE CONFIGURATION — 全部由字段表 (FIELDS_*, CMake 汇总) + TIER_LEVELS 编译期展开, 每档一个命名空间
// ============================================================================
// 写端专用 (FeatureStore / Core* / FeatureMeta.cpp): 编译期表保住常量定址折叠.
// 读端/GUI 走 FeatureLevels.hpp 的 level_info(tier, lvl) (运行时), 不依赖本文件 ——
// 增删改特征只重编写端 + FeatureMeta.cpp.
//   布局: 每层平铺 [T][F][A], data[(t * F + f) * A + a]
//   TIER_LEVELS 每格 (档 <tier>, 层 <LVL>) 生成 (消费者统一签名 GEN(T, ns, LVL, index, fields, psd), 自己进 ns):
//     <tier>::<LVL>_FIELD_INFO[]     每列 {code, valid, kind, ts_tf} (有效性 / 类型均由 SRC 列推出)
//     <tier>::<LVL>_Field::<code>    列下标枚举
//     <tier>::<LVL>_FINGERPRINT      字段表指纹 (落文件头)
//   汇总成 <tier>::LEVELS[lvl] (LevelInfo, 按语义层号; 档缺的层 fields == nullptr), 编译期/运行时按层取 rows / field_count / fingerprint;
//   <tier>::Tier = 写端基建 (FeatureStore / CoreCs / fstore) 的档模板参数; TierSide<T>::type = 旁读档 (TIER_SIDES).
//   字段格式: X(code, cat_l1, cat_l2, name_en, name_cn, desc, formula, SRC)
// ============================================================================

// ============================================================================
// 节点 flush 域 (L_sec 的 TS 列有效性标志由此推出; SRC_VALID_TS 在档命名空间内按名查到本档的)
// ============================================================================
#define NODE_FLUSH_ONE(name, type, args, ...) constexpr Trigger name = NODE_FLUSH(__VA_ARGS__);
#define TIER_NODE_FLUSH(T, ns) \
  namespace ns::node_flush {   \
  T##_NODES(NODE_FLUSH_ONE)    \
  }
ALL_TIERS(TIER_NODE_FLUSH)
#undef TIER_NODE_FLUSH
#undef NODE_FLUSH_ONE

// ============================================================================
// 字段: 每层一张 FieldInfo 表 + 下标枚举 (FieldInfo 结构体在 FeatureLevels.hpp)
// ============================================================================
#define FIELD_INFO_ONE(code, c1, c2, en, cn, desc, formula, src) {#code, SRC_VALID_##src, SRC_KIND_##src, SRC_TS_TF_##src},
#define FIELD_CODE_ONE(code, c1, c2, en, cn, desc, formula, src) code,

// 某类型 (LB 等) 列的首下标 / 个数 (标签回填按此定位, 不依赖列名)
constexpr size_t first_of_kind(std::span<const FieldInfo> f, FeatureDataType k) {
  for (size_t i = 0; i < f.size(); ++i)
    if (f[i].kind == k)
      return i;
  return f.size();
}
constexpr size_t count_of_kind(std::span<const FieldInfo> f, FeatureDataType k) {
  size_t c = 0;
  for (const FieldInfo &x : f)
    c += x.kind == k;
  return c;
}
// 同类型列必须连续 (标签组按下标区间写)
constexpr bool kind_contiguous(std::span<const FieldInfo> f, FeatureDataType k) {
  const size_t b = first_of_kind(f, k), c = count_of_kind(f, k);
  for (size_t i = b; i < b + c; ++i)
    if (f[i].kind != k)
      return false;
  return true;
}

#define GENERATE_LEVEL_FIELDS(T, ns, name, num, fields, psd)                  \
  namespace ns::name##_info_detail {                                          \
    constexpr size_t kLevel = num; /* SRC_VALID_TS 按层推有效性 */            \
    inline constexpr FieldInfo TABLE[] = {fields(FIELD_INFO_ONE)};            \
  }                                                                           \
  namespace ns {                                                              \
  inline constexpr const auto &name##_FIELD_INFO = name##_info_detail::TABLE; \
  constexpr size_t name##_FIELD_COUNT = std::size(name##_FIELD_INFO);         \
  namespace name##_Field {                                                    \
    enum : size_t { fields(FIELD_CODE_ONE) };                                 \
  }                                                                           \
  }
TIER_LEVELS(GENERATE_LEVEL_FIELDS)

// 档缺的层不许有列 (CMake 对每档都生成全部层的表, 未登记的层应为空)
#define FIELD_COUNT_ONE(...) +1
#define CELL_FIELD_COUNT(T, ns, name, num, fields, psd) +(TierId::T == tier ? ns::name##_FIELD_COUNT : 0)
constexpr size_t registered_field_count(TierId tier) { return 0 TIER_LEVELS(CELL_FIELD_COUNT); }
#define CHECK_TIER_FIELDS(T, ns)                                                                                          \
  static_assert(0 T##_L_sec_FIELDS(FIELD_COUNT_ONE) T##_L_1min_FIELDS(FIELD_COUNT_ONE) T##_L_5min_FIELDS(FIELD_COUNT_ONE) \
                        T##_L_15min_FIELDS(FIELD_COUNT_ONE) T##_L_30min_FIELDS(FIELD_COUNT_ONE)                           \
                            T##_L_60min_FIELDS(FIELD_COUNT_ONE) T##_L_1day_FIELDS(FIELD_COUNT_ONE) ==                     \
                    registered_field_count(TierId::T),                                                                    \
                "档 " #T " 有未在 TIER_LEVELS 登记的层的 FIELDS_ 行");
ALL_TIERS(CHECK_TIER_FIELDS)
#undef CHECK_TIER_FIELDS
#undef CELL_FIELD_COUNT
#undef FIELD_COUNT_ONE

// ============================================================================
// 每个字段的"来源" (GUI 特征依赖解析用)
//   TS(node[,port]) → 节点名 (如 "Book") + 口名 (单口节点 ""); CS(ns,lvl,src,...) → 源字段 code (TS / CS 列名跨档唯一, 旁读也不加档名); 其余 → ""
//   口名配合 node_deps::TABLE 的 "Up.port" 项: 下游只引用某一口时, 依赖只算该口的字段
// ============================================================================
struct FieldSource {
  const char *code;   // 字段 code
  const char *source; // 节点名 (TS) / 源字段 code (CS) / 空 (LABEL/FLAG)
  const char *port;   // 口名 (TS 多口) / 空
};
#define SRCSRC_TS(code, node, ...) #node
#define SRCSRC_CS(code, ns, lvl, src, ...) #src
#define SRCSRC_LABEL(code) ""
#define SRCSRC_FLAG(code) ""
#define SRCPORT_TS_3(node, tf, m) ""
#define SRCPORT_TS_4(node, port, tf, m) #port
#define SRCPORT_TS(code, ...) SRC_NODE_PICK(__VA_ARGS__, SRCPORT_TS_4, SRCPORT_TS_3, , )(__VA_ARGS__)
#define SRCPORT_CS(...) ""
#define SRCPORT_LABEL(code) ""
#define SRCPORT_FLAG(code) ""
#define FIELD_SOURCE_ONE(code, c1, c2, en, cn, desc, formula, src) {#code, SRC_DISPATCH(SRCSRC, code, src), SRC_DISPATCH(SRCPORT, code, src)},
#define GENERATE_LEVEL_SOURCES(T, ns, name, num, fields, psd)                      \
  namespace ns {                                                                   \
  inline constexpr FieldSource name##_FIELD_SOURCE[] = {fields(FIELD_SOURCE_ONE)}; \
  }
TIER_LEVELS(GENERATE_LEVEL_SOURCES)

// ============================================================================
// 编译期一致性检查: 列所在层 == 来源允许的层
//   CS 源层任意 (CoreCs 按 level_asof(源层, 本层, t) 投影; 跨日只许 TS / 元列, CoreCs 编译期查); TS 任意 (节拍取 Series 最新值, 见 Level);
//   LABEL / FLAG 任意 —— 目前全部放行, 留作挂点
// ============================================================================
#define SRC_LEVEL_TS(...) kLevel
#define SRC_LEVEL_CS(...) kLevel
#define SRC_LEVEL_LABEL kLevel
#define SRC_LEVEL_FLAG kLevel
#define CHECK_FIELD_ONE(code, c1, c2, en, cn, desc, formula, src) \
  static_assert(SRC_LEVEL_##src == kLevel, "field level != source level: " #code);
#define GENERATE_CHECK_LEVEL(T, ns, name, num, fields, psd) \
  namespace ns::name##_level_check {                        \
    constexpr size_t kLevel = num;                          \
    fields(CHECK_FIELD_ONE)                                 \
  }
TIER_LEVELS(GENERATE_CHECK_LEVEL)

// ============================================================================
// 字段表指纹 (写入文件头, 读取时比对; 表改了旧文件立刻报错而非静默错位)
//   FNV-1a 64 over "code:1:kind" 逐字段拼接 (顺序敏感; 1 = 已废的列宽, 恒折入以保旧库指纹)
//   LB 列另折入 kLabelSemver: 标签列名 / 宽度不变但口径变 (LabelLob 的 entry / exit 定义) 时 +1, 旧库读取即判废而非静默错义
//   L_1day 表另折入 kDaySemver: 日行口径变 (列名不变) 时 +1, 同理
// ============================================================================
constexpr uint64_t kLabelSemver = 4; // 1: 分钟型 exit = 成交 VWAP; 2: exit = 行 m+n 的 entry 中间价 (entry / exit 同基准); 3: 价格 × 当日复权比 (开盘型跨除权);
                                     // 4: 可成交进标签 (建仓非两向 → NaN, 平仓顺延; 见 LabelHold)
constexpr uint64_t kDaySemver = 1;   // 1: 行 D = 次日盘前态 (Fund / PoolKey onSettle, 见 Fund.hpp 逐表对齐)
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
    h = fnv1a_u64(fnv1a_u64(fnv1a_str(h, f[i].code), 1), static_cast<uint64_t>(f[i].kind));
    if (f[i].ts_tf != ts::TfId::None) // Tf 改变落盘值; None 不折入, 旧文件 (无 Tf 时代) 指纹不变
      h = fnv1a_u64(h, static_cast<uint64_t>(f[i].ts_tf));
    if (f[i].kind == FeatureDataType::LB)
      h = fnv1a_u64(h, kLabelSemver);
  }
  return h;
}
#define GENERATE_FINGERPRINT(T, ns, name, num, fields, psd)                                                           \
  namespace ns {                                                                                                      \
  constexpr uint64_t name##_FINGERPRINT = num == L_1day ? fnv1a_u64(table_fingerprint(name##_FIELD_INFO), kDaySemver) \
                                                        : table_fingerprint(name##_FIELD_INFO);                       \
  }
TIER_LEVELS(GENERATE_FINGERPRINT)

// ============================================================================
// 层表: <tier>::LEVELS[lvl] — 编译期版本 (写端定址折叠; LevelInfo 结构体在 FeatureLevels.hpp)
//   TIER_LEVELS 平铺成格表, 每档按语义层号筛出定长 LEVEL_COUNT; 档缺的层 = {层名, 行数, 0 列, fields = nullptr}
// 运行时入口 level_info(tier, lvl) 在 FeatureMeta.cpp 定义, 指向本表.
// ============================================================================
struct LevelCell {
  TierId tier;
  size_t index;
  LevelInfo info;
};
#define LEVEL_CELL_ONE(T, ns, name, num, fields, psd) \
  LevelCell{TierId::T, num, LevelInfo{#name, level_rows(num), ns::name##_FIELD_COUNT, ns::name##_FIELD_INFO, ns::name##_FINGERPRINT, psd}},
inline constexpr LevelCell LEVEL_CELLS[] = {TIER_LEVELS(LEVEL_CELL_ONE)};
#undef LEVEL_CELL_ONE

constexpr bool level_cells_valid() {
  for (size_t i = 0; i < std::size(LEVEL_CELLS); ++i) {
    const LevelCell &c = LEVEL_CELLS[i];
    if (c.index >= LEVEL_COUNT || std::string_view(c.info.level_name) != level_name(c.index))
      return false;
    for (size_t j = 0; j < i; ++j)
      if (LEVEL_CELLS[j].tier == c.tier && LEVEL_CELLS[j].index == c.index)
        return false;
  }
  return true;
}
static_assert(level_cells_valid(), "TIER_LEVELS: 层名须与层号一致 (FeatureLevels.hpp level_name), 同档层号不许重复");

constexpr std::array<LevelInfo, LEVEL_COUNT> tier_levels(TierId tier) {
  std::array<LevelInfo, LEVEL_COUNT> l{};
  for (size_t i = 0; i < LEVEL_COUNT; ++i)
    l[i] = LevelInfo{level_name(i), level_rows(i), 0, nullptr, 0, ""};
  for (const LevelCell &c : LEVEL_CELLS)
    if (c.tier == tier)
      l[c.index] = c.info;
  return l;
}
// 档内首 / 末层 (首层 = 截面节拍层; 末层 = 落盘层序末位, 本日 commit 标记)
constexpr size_t first_present(const std::array<LevelInfo, LEVEL_COUNT> &l) {
  for (size_t i = 0; i < LEVEL_COUNT; ++i)
    if (l[i].fields)
      return i;
  return LEVEL_COUNT;
}
constexpr size_t last_present(const std::array<LevelInfo, LEVEL_COUNT> &l) {
  for (size_t i = LEVEL_COUNT; i-- > 0;)
    if (l[i].fields)
      return i;
  return LEVEL_COUNT;
}

#define GENERATE_TIER(T, ns)                                                           \
  namespace ns {                                                                       \
  inline constexpr std::array<LevelInfo, LEVEL_COUNT> LEVELS = tier_levels(TierId::T); \
  /* 写端基建 (FeatureStore / CoreCs / fstore) 的档模板参数 */                         \
  struct Tier {                                                                        \
    static constexpr TierId kId = TierId::T;                                           \
    static constexpr const std::array<LevelInfo, LEVEL_COUNT> &LEVELS = ns::LEVELS;    \
    static constexpr bool has(size_t lvl) { return LEVELS[lvl].fields != nullptr; }    \
    static constexpr size_t kFirstLevel = first_present(ns::LEVELS);                   \
    static constexpr size_t kLastLevel = last_present(ns::LEVELS);                     \
  };                                                                                   \
  static_assert(Tier::kFirstLevel < LEVEL_COUNT, "档 " #T " 未登记任何层");            \
  }
ALL_TIERS(GENERATE_TIER)
#undef GENERATE_TIER

// 旁读档 (TIER_SIDES): TierSide<T>::type = T 的截面可读 (同层) 的档; 无 = void
template <class T>
struct TierSide {
  using type = void;
};
#define TIER_SIDE_ONE(ns, side) \
  template <>                   \
  struct TierSide<ns::Tier> {   \
    using type = side::Tier;    \
  };
TIER_SIDES(TIER_SIDE_ONE)
#undef TIER_SIDE_ONE

// 档 T 的 lvl 层里 code 列的下标 (编译期按名定址, 供档模板化的基建取元列 / 方法源列); 找不到 = field_count
template <class T>
constexpr size_t field_of(size_t lvl, std::string_view code) {
  const LevelInfo &L = T::LEVELS[lvl];
  for (size_t i = 0; i < L.field_count; ++i)
    if (code == L.fields[i].code)
      return i;
  return L.field_count;
}

static_assert(level_rows(L_sec) - 1 == TRADE_SECONDS_PER_DAY && level_rows(L_1min) - 1 == TRADE_MINUTES_PER_DAY &&
                  level_rows(L_5min) - 1 == bins_per_day(5) && level_rows(L_60min) - 1 == bins_per_day(60) && level_rows(L_1day) - 1 == 1,
              "每层落盘 T 必须是 有效行数 + 1 哨兵行");
