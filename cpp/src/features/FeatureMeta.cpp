// ============================================================================
// FeatureMeta — 字段表元数据的唯一数据 TU
// ============================================================================
// 全项目只有这个 TU (加上写端 worker) include 生成的字段表 (FieldsGenerated, 经
// FeatureStoreConfig) 与 FeatureCategoriesGenerated —— GUI/shared 消费者只 include
// 稳定头 (shared/Feature.hpp / features/Backend/FeatureLevels.hpp), 增删改特征时
// 它们不重编, 只重编本文件 + 写端, 再重链.
//   level_info(lvl)            FeatureLevels.hpp 声明的运行时层表入口
//   feature_meta::features     每层 FeatureMetadata 表 (编译期展开, 运行时只读)
//   feature_meta::categories_* 分类表 (FeatureCategoriesGenerated)
//   Feature::Metadata::init_from_compile_time  元数据拷贝 + 依赖解析 (node_deps)
// ============================================================================

#include "features/Backend/FeatureStoreConfig.hpp" // 编译期 LEVELS / 字段表 / FieldSource / node_deps
#include "features/FeatureCategoriesGenerated.hpp"
#include "shared/Feature.hpp"

#include <cassert>
#include <iterator>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ============================================================================
// 运行时层表入口 (FeatureLevels.hpp 声明)
// ============================================================================
const LevelInfo &level_info(size_t lvl) {
  assert(lvl < LEVEL_COUNT);
  return LEVELS[lvl];
}

// ============================================================================
// Compile-time Metadata Generation: 每层一张表 (字段表行 + 层信息)
// ============================================================================
#define GENERATE_METADATA(code, cat_l1, cat_l2, name_en, name_cn, description, formula, src)                                        \
  {#code, SRC_WIDTH_##src, SRC_VALID_##src, SRC_KIND_##src, cat_l1, #cat_l2, SRC_TS_TF_##src, SRC_TS_METHOD_##src, SRC_CS_TF_##src, \
   SRC_CS_METHOD_##src, formula, name_en, name_cn, description, kLevel},
#define GENERATE_METADATA_TABLE(name, num, fields, rows, psd, columnar, xor_delta) \
  namespace name##_meta_detail {                                                   \
    constexpr uint8_t kLevel = num;                                                \
    constexpr FeatureMetadata TABLE[] = {fields(GENERATE_METADATA)};               \
  }
#define METADATA_TABLE_PTR(name, num, fields, rows, psd, columnar, xor_delta) name##_meta_detail::TABLE,
#define METADATA_TABLE_COUNT(name, num, fields, rows, psd, columnar, xor_delta) std::size(name##_meta_detail::TABLE),

namespace {
ALL_LEVELS(GENERATE_METADATA_TABLE)
constexpr const FeatureMetadata *META_TABLES[LEVEL_COUNT] = {ALL_LEVELS(METADATA_TABLE_PTR)};
constexpr size_t META_COUNTS[LEVEL_COUNT] = {ALL_LEVELS(METADATA_TABLE_COUNT)};
} // namespace

#undef GENERATE_METADATA
#undef GENERATE_METADATA_TABLE
#undef METADATA_TABLE_PTR
#undef METADATA_TABLE_COUNT

namespace feature_meta {

std::span<const FeatureMetadata> features(size_t level) {
  assert(level < LEVEL_COUNT);
  return {META_TABLES[level], META_COUNTS[level]};
}

std::span<const char *const> categories_l1() {
  return {FeatureCategoryL1_ALL.data(), FeatureCategoryL1_ALL.size()};
}

std::span<const char *const> categories_l2() {
  return {FeatureCategoryL2_ALL.data(), FeatureCategoryL2_ALL.size()};
}

} // namespace feature_meta

// ============================================================================
// Feature::Metadata 初始化 (从编译期表拷贝 + 依赖解析)
// ============================================================================

void Feature::Metadata::init_from_compile_time() {
  // 1. Copy from constexpr arrays to runtime vectors (for filtering/sorting)
  for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
    features[lvl].assign(META_TABLES[lvl], META_TABLES[lvl] + META_COUNTS[lvl]);

  // 2. 节点上游依赖表: node name → 直接依赖项 (来自 CMake 生成的 node_deps::TABLE)
  //    项 = "Up" (整节点: Up.out() / Up.outs() / Up.member) 或 "Up.port" (单口: Up.out(Up.port))
  std::unordered_map<std::string, std::vector<std::string>> node_deps_map;
  for (const auto &e : node_deps::TABLE)
    node_deps_map.emplace(e.node, std::vector<std::string>(e.deps, e.deps + e.count));

  // 3. 节点 → 产出字段 (code, 口名) (跨所有层)
  //    FieldSource.source: OP → 节点名 (在 node_deps_map 中); CS → 源字段 code; "" → 无
  struct NodeField {
    std::string code, port;
  };
  std::unordered_map<std::string, std::vector<NodeField>> node_fields;
  auto add_node_fields = [&](const FieldSource *srcs, size_t n) {
    for (size_t i = 0; i < n; ++i) {
      std::string_view s = srcs[i].source;
      if (!s.empty() && node_deps_map.count(std::string(s)))
        node_fields[std::string(s)].push_back({srcs[i].code, srcs[i].port});
    }
  };
  add_node_fields(L0_FIELD_SOURCE, std::size(L0_FIELD_SOURCE));
  add_node_fields(L1_FIELD_SOURCE, std::size(L1_FIELD_SOURCE));

  // 4. 逐字段解析直接依赖 (字段 code, 分号分隔)
  auto resolve_level = [&](const FieldSource *srcs, size_t n, std::vector<std::string> &out) {
    out.assign(n, {});
    for (size_t i = 0; i < n; ++i) {
      std::string_view s = srcs[i].source;
      if (s.empty())
        continue;
      std::string key(s);
      if (node_deps_map.count(key)) {
        // OP 字段: 依赖 = 上游节点产出的字段 code; 单口引用只取该口的字段
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
  resolve_level(L0_FIELD_SOURCE, std::size(L0_FIELD_SOURCE), deps[0]);
  resolve_level(L1_FIELD_SOURCE, std::size(L1_FIELD_SOURCE), deps[1]);
}
