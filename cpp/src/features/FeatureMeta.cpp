// ============================================================================
// FeatureMeta — 字段表元数据的唯一数据 TU
// ============================================================================
// 全项目只有这个 TU (加上写端 worker) include 生成的字段表 (FieldsGenerated, 经
// FeatureStoreConfig) 与 FeatureCategoriesGenerated —— GUI/shared 消费者只 include
// 稳定头 (shared/Feature.hpp / features/Storage/FeatureLevels.hpp), 增删改特征时
// 它们不重编, 只重编本文件 + 写端, 再重链.
//   level_info(tier, lvl)      FeatureLevels.hpp 声明的运行时层表入口
//   feature_meta::features     每档每层 FeatureMetadata 表 (编译期展开, 运行时只读)
//   feature_meta::categories_* 每档分类表 (FeatureCategoriesGenerated)
//   Feature::Metadata::init_from_compile_time  元数据拷贝 + 依赖解析 (node_deps)
//   两档 TS / CS 列名全局唯一 (编译期断言): 因子层按列名拼两档, 重名即歧义
// ============================================================================

#include "features/Bar/FeatureCategoriesGenerated.hpp"
#include "features/Lob/FeatureCategoriesGenerated.hpp"
#include "features/Storage/FeatureStoreConfig.hpp" // 编译期 <tier>::LEVELS / 字段表 / FieldSource / node_deps
#include "shared/Config.hpp"                       // Config::kAllTierDir (池表 [0])
#include "shared/Feature.hpp"
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ============================================================================
// 运行时层表入口 (FeatureLevels.hpp 声明)
// ============================================================================
namespace {
#define TIER_LEVELS_PTR(T, ns) &ns::LEVELS,
constexpr const std::array<LevelInfo, LEVEL_COUNT> *TIER_LEVEL_TABLE[TIER_COUNT] = {ALL_TIERS(TIER_LEVELS_PTR)};
#undef TIER_LEVELS_PTR
#define TIER_LAST_LEVEL(T, ns) ns::Tier::kLastLevel,
constexpr size_t TIER_LAST[TIER_COUNT] = {ALL_TIERS(TIER_LAST_LEVEL)};
#undef TIER_LAST_LEVEL
} // namespace

bool has_level(TierId tier, size_t lvl) {
  assert(static_cast<size_t>(tier) < TIER_COUNT && lvl < LEVEL_COUNT);
  return (*TIER_LEVEL_TABLE[static_cast<size_t>(tier)])[lvl].fields != nullptr;
}

const LevelInfo &level_info(TierId tier, size_t lvl) {
  assert(has_level(tier, lvl));
  return (*TIER_LEVEL_TABLE[static_cast<size_t>(tier)])[lvl];
}

size_t last_level(TierId tier) {
  assert(static_cast<size_t>(tier) < TIER_COUNT);
  return TIER_LAST[static_cast<size_t>(tier)];
}

// ============================================================================
// 两档 TS / CS 列名唯一 (META 元列 / LB 标签列各档同名同语义, 豁免)
// ============================================================================
// 两档全部列按名 hash 排序后查同 hash 段 (两两比较 O(Fa·Fb·len) 会撞 constexpr 步数上限)
namespace {
using TierLevels = std::array<LevelInfo, LEVEL_COUNT>;
struct TierCode {
  uint64_t h;
  uint8_t tier;
  bool ts_cs; // TS / CS 列 (与对档任意同名列冲突); 其余 (META / LB / FLAG) 只与对档 TS / CS 同名才算
  const char *code;
};
constexpr size_t field_total(const TierLevels &L) {
  size_t n = 0;
  for (const LevelInfo &l : L)
    n += l.fields ? l.field_count : 0;
  return n;
}
constexpr void collect(const TierLevels &L, uint8_t tier, TierCode *out, size_t &n) {
  for (const LevelInfo &l : L)
    for (size_t i = 0; l.fields && i < l.field_count; ++i) {
      const FeatureDataType k = l.fields[i].kind;
      out[n++] = {fnv1a_str(0xcbf29ce484222325ULL, l.fields[i].code), tier, k == FeatureDataType::TS || k == FeatureDataType::CS, l.fields[i].code};
    }
}
constexpr bool tiers_unique() {
#define TIER_FIELD_TOTAL(T, ns) +field_total(ns::LEVELS)
  constexpr size_t N = 0 ALL_TIERS(TIER_FIELD_TOTAL);
#undef TIER_FIELD_TOTAL
  std::array<TierCode, N> v{};
  size_t n = 0;
#define TIER_COLLECT(T, ns) collect(ns::LEVELS, static_cast<uint8_t>(TierId::T), v.data(), n);
  ALL_TIERS(TIER_COLLECT)
#undef TIER_COLLECT
  std::sort(v.begin(), v.end(), [](const TierCode &a, const TierCode &b) { return a.h < b.h; });
  for (size_t i = 0; i < N;) {
    size_t j = i;
    while (j < N && v[j].h == v[i].h)
      ++j;
    for (size_t p = i; p < j; ++p)
      for (size_t q = p + 1; q < j; ++q)
        if (v[p].tier != v[q].tier && (v[p].ts_cs || v[q].ts_cs) && std::string_view(v[p].code) == v[q].code)
          return false;
    i = j;
  }
  return true;
}
static_assert(tiers_unique(), "跨档 TS / CS 列名重复 (因子层按名拼接两档, 须全局唯一; Lob 档 K 线用 lob_* 前缀)");
} // namespace

// ============================================================================
// Compile-time Metadata Generation: 每档每层一张表 (字段表行 + 层信息)
// ============================================================================
// 节点文件取自 <Tier>_<LVL>_FILES (与字段表同一展开, CAT1 位 = 文件路径, 行序一致; 行数不等即模板推导失败)
#define GENERATE_METADATA(code, cat_l1, cat_l2, name_en, name_cn, description, formula, src)                                \
  {#code, SRC_VALID_##src, SRC_KIND_##src, cat_l1, nullptr, #cat_l2, SRC_TS_TF_##src, SRC_TS_METHOD_##src, SRC_CS_TF_##src, \
   SRC_CS_METHOD_##src, formula, name_en, name_cn, description, kLevel},
#define GENERATE_FILE(code, file, ...) file,
template <size_t N>
constexpr std::array<FeatureMetadata, N> with_files(const FeatureMetadata (&base)[N], const char *const (&files)[N]) {
  std::array<FeatureMetadata, N> out{};
  for (size_t i = 0; i < N; ++i) {
    out[i] = base[i];
    out[i].file = files[i];
  }
  return out;
}
#define GENERATE_METADATA_TABLE(T, ns, name, num, fields, psd)                 \
  namespace ns::name##_meta_detail {                                           \
    constexpr uint8_t kLevel = num;                                            \
    constexpr FeatureMetadata BASE[] = {fields(GENERATE_METADATA)};            \
    constexpr const char *const FILES[] = {T##_##name##_FILES(GENERATE_FILE)}; \
    constexpr auto TABLE = with_files(BASE, FILES);                            \
  }
TIER_LEVELS(GENERATE_METADATA_TABLE)

// 格表 (与 LEVEL_CELLS 同序) → 每档 META[lvl] / SOURCES[lvl] (档缺的层为空 span) + 依赖表 / 分类表
struct MetaCell {
  TierId tier;
  size_t index;
  std::span<const FeatureMetadata> meta;
  std::span<const FieldSource> sources;
};
#define META_CELL_ONE(T, ns, name, num, fields, psd) MetaCell{TierId::T, num, ns::name##_meta_detail::TABLE, ns::name##_FIELD_SOURCE},
constexpr MetaCell META_CELLS[] = {TIER_LEVELS(META_CELL_ONE)};
#undef META_CELL_ONE
template <class V>
constexpr std::array<V, LEVEL_COUNT> tier_spans(TierId tier, V MetaCell::*field) {
  std::array<V, LEVEL_COUNT> out{};
  for (const MetaCell &c : META_CELLS)
    if (c.tier == tier)
      out[c.index] = c.*field;
  return out;
}
#define GENERATE_TIER_META(T, ns)                                                                                      \
  namespace ns {                                                                                                       \
  constexpr std::array<std::span<const FeatureMetadata>, LEVEL_COUNT> META = tier_spans(TierId::T, &MetaCell::meta);   \
  constexpr std::array<std::span<const FieldSource>, LEVEL_COUNT> SOURCES = tier_spans(TierId::T, &MetaCell::sources); \
  constexpr std::span<const char *const> CAT1{FeatureCategoryL1_ALL.data(), FeatureCategoryL1_ALL.size()};             \
  constexpr std::span<const char *const> CAT2{FeatureCategoryL2_ALL.data(), FeatureCategoryL2_ALL.size()};             \
  }
ALL_TIERS(GENERATE_TIER_META)

namespace {
struct TierMeta {
  const std::array<std::span<const FeatureMetadata>, LEVEL_COUNT> *meta;
  const std::array<std::span<const FieldSource>, LEVEL_COUNT> *sources;
  std::span<const char *const> cat1, cat2;
};
#define TIER_META_ONE(T, ns) {&ns::META, &ns::SOURCES, ns::CAT1, ns::CAT2},
const TierMeta TIER_META[TIER_COUNT] = {ALL_TIERS(TIER_META_ONE)};
#undef TIER_META_ONE

const TierMeta &tier_meta(TierId tier) {
  assert(static_cast<size_t>(tier) < TIER_COUNT);
  return TIER_META[static_cast<size_t>(tier)];
}
} // namespace

#undef GENERATE_METADATA
#undef GENERATE_FILE
#undef GENERATE_METADATA_TABLE
#undef GENERATE_TIER_META

// ============================================================================
// 池表 (FeatureLib.hpp 声明): [0] = all (Bar 档); 其余 = Bar L_1day 的 Pool 类 CS 列 pool_<pool> ↔ Lob 档池 <pool>
// ============================================================================
namespace {
constexpr std::string_view kPoolPrefix = "pool_";
constexpr bool is_pool(const FeatureMetadata &f) {
  return f.data_type == FeatureDataType::CS && std::string_view(f.cat_l1) == "Pool" && std::string_view(f.code).starts_with(kPoolPrefix);
}
constexpr std::span<const FeatureMetadata> kBarDay = bar::L_1day_meta_detail::TABLE;
constexpr size_t kPoolCount = std::count_if(kBarDay.begin(), kBarDay.end(), is_pool);
static_assert(kPoolCount > 0, "Bar 字段表没有池列 (Node/Bar/CS/Pool 的 pool_*)");
constexpr std::array<PoolDef, 1 + kPoolCount> kPools = [] {
  std::array<PoolDef, 1 + kPoolCount> out{};
  out[0] = {Config::kAllTierDir, "全市场", TierId::Bar, SIZE_MAX};
  size_t n = 1;
  for (size_t i = 0; i < kBarDay.size(); ++i)
    if (is_pool(kBarDay[i]))
      out[n++] = {kBarDay[i].code + kPoolPrefix.size(), kBarDay[i].name_cn, TierId::Lob, i};
  return out;
}();
static_assert(kPools.size() <= UINT8_MAX && kPools[kAllPool].tier == TierId::Bar, "PoolId = uint8_t 池表下标, [kAllPool] = all");
static_assert(kPools.size() <= sizeof(PoolMask) * 8, "池表超出 PoolMask 位宽");
} // namespace

std::span<const PoolDef> feature_pools() { return kPools; }
size_t pool_count() { return kPools.size(); }
TierId pool_tier(PoolId pool) { return pool_def(pool).tier; }

const PoolDef &pool_def(PoolId pool) {
  assert(pool < kPools.size() && "池下标越界");
  return kPools[pool];
}
const PoolDef &pool_def(std::string_view name) { return kPools[pool_id(name)]; }
PoolId pool_id(std::string_view name) {
  for (size_t i = 0; i < kPools.size(); ++i)
    if (name == kPools[i].name)
      return static_cast<PoolId>(i);
  assert(false && "不是池名 (all / Bar 池 pool_<pool> 列)");
  return kAllPool;
}

namespace feature_meta {

std::span<const FeatureMetadata> features(TierId tier, size_t level) {
  assert(level < LEVEL_COUNT);
  return (*tier_meta(tier).meta)[level];
}

std::span<const char *const> categories_l1(TierId tier) { return tier_meta(tier).cat1; }

std::span<const char *const> categories_l2(TierId tier) { return tier_meta(tier).cat2; }

} // namespace feature_meta

// ============================================================================
// Feature::Metadata 初始化 (从编译期表拷贝 + 依赖解析)
// ============================================================================

void Feature::Metadata::init_from_compile_time(TierId t) {
  tier = t;
  const TierMeta &tm = tier_meta(t);
  // 1. Copy from constexpr arrays to runtime vectors (for filtering/sorting); 档缺的层为空
  for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
    features[lvl].assign((*tm.meta)[lvl].begin(), (*tm.meta)[lvl].end());

  // 2. 节点上游依赖表: node name → 直接依赖项 (来自 CMake 生成的 <tier>::node_deps::TABLE)
  //    项 = "Up" (整节点: Up.out() / Up.outs() / Up.member) 或 "Up.port" (单口: Up.out(Up.port))
  std::unordered_map<std::string, std::vector<std::string>> node_deps_map;
  auto load_deps = [&](const auto &table) {
    for (const auto &e : table)
      node_deps_map.emplace(e.node, std::vector<std::string>(e.deps, e.deps + e.count));
  };
#define TIER_DEPS_ONE(T, ns) \
  if (t == TierId::T)        \
    load_deps(ns::node_deps::TABLE);
  ALL_TIERS(TIER_DEPS_ONE)
#undef TIER_DEPS_ONE

  // 3. 节点 → 产出字段 (code, 口名) (跨所有层)
  //    FieldSource.source: TS → 节点名 (在 node_deps_map 中); CS → 源字段 code; "" → 无
  struct NodeField {
    std::string code, port;
  };
  std::unordered_map<std::string, std::vector<NodeField>> node_fields;
  auto add_node_fields = [&](std::span<const FieldSource> srcs) {
    for (size_t i = 0; i < srcs.size(); ++i) {
      std::string_view s = srcs[i].source;
      if (!s.empty() && node_deps_map.count(std::string(s)))
        node_fields[std::string(s)].push_back({srcs[i].code, srcs[i].port});
    }
  };
  for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
    add_node_fields((*tm.sources)[lvl]);

  // 4. 逐字段解析直接依赖 (字段 code, 分号分隔)
  auto resolve_level = [&](std::span<const FieldSource> srcs, std::vector<std::string> &out) {
    const size_t n = srcs.size();
    out.assign(n, {});
    for (size_t i = 0; i < n; ++i) {
      std::string_view s = srcs[i].source;
      if (s.empty())
        continue;
      std::string key(s);
      if (node_deps_map.count(key)) {
        // TS 字段: 依赖 = 上游节点产出的字段 code; 单口引用只取该口的字段
        std::unordered_set<std::string> seen;
        for (const auto &dep : node_deps_map[key]) {
          const size_t dot = dep.find('.');
          const std::string up = dep.substr(0, dot);
          const std::string port = dot == std::string::npos ? "" : dep.substr(dot + 1);
          auto it = node_fields.find(up);
          if (it == node_fields.end())
            continue;
          for (const auto &f : it->second) {
            if (!port.empty() && f.port != port)
              continue;
            if (seen.insert(f.code).second)
              out[i] += (out[i].empty() ? "" : ";") + f.code;
          }
        }
      } else {
        // CS 字段: 依赖 = 源字段 code
        out[i] = key;
      }
    }
  };
  for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
    resolve_level((*tm.sources)[lvl], deps[lvl]);
}
