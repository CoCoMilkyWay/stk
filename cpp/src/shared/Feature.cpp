#include "shared/Feature.hpp"

#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// ============================================================================
// Feature Constructor
// ============================================================================

Feature::Feature() {
  // Initialize metadata from compile-time arrays on construction
  metadata.init_from_compile_time();
}

// ============================================================================
// Feature::Metadata Implementation
// ============================================================================

void Feature::Metadata::init_from_compile_time() {
  // 1. Copy from constexpr arrays to runtime vectors (for filtering/sorting)
  for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
    features[lvl].assign(FeatureMetadataRegistry::FEATURES[lvl],
                         FeatureMetadataRegistry::FEATURES[lvl] + FeatureMetadataRegistry::COUNTS[lvl]);

  // 2. 节点上游依赖表: node name → 直接依赖的上游节点名 (来自 CMake 生成的 node_deps::TABLE)
  std::unordered_map<std::string, std::vector<std::string>> node_deps_map;
  for (const auto &e : node_deps::TABLE)
    node_deps_map.emplace(e.node, std::vector<std::string>(e.deps, e.deps + e.count));

  // 3. 节点 → 产出字段 code (跨所有层)
  //    FieldSource.source: OP → 节点名 (在 node_deps_map 中); CS → 源字段 code; "" → 无
  std::unordered_map<std::string, std::vector<std::string>> node_fields;
  auto add_node_fields = [&](const FieldSource *srcs, size_t n) {
    for (size_t i = 0; i < n; ++i) {
      std::string_view s = srcs[i].source;
      if (!s.empty() && node_deps_map.count(std::string(s)))
        node_fields[std::string(s)].push_back(srcs[i].code);
    }
  };
  add_node_fields(L0_FIELD_SOURCE, std::size(L0_FIELD_SOURCE));
  add_node_fields(L1_FIELD_SOURCE, std::size(L1_FIELD_SOURCE));
  add_node_fields(DEPTH_FIELD_SOURCE, std::size(DEPTH_FIELD_SOURCE));

  // 4. 逐字段解析直接依赖 (字段 code, 分号分隔)
  auto resolve_level = [&](const FieldSource *srcs, size_t n, std::vector<std::string> &out) {
    out.assign(n, {});
    for (size_t i = 0; i < n; ++i) {
      std::string_view s = srcs[i].source;
      if (s.empty())
        continue;
      std::string key(s);
      if (node_deps_map.count(key)) {
        // OP 字段: 依赖 = 上游节点产出的所有字段 code
        std::unordered_set<std::string> seen;
        for (const auto &up : node_deps_map[key]) {
          auto it = node_fields.find(up);
          if (it == node_fields.end())
            continue;
          for (const auto &fc : it->second) {
            if (seen.insert(fc).second)
              out[i] += (out[i].empty() ? "" : ";") + fc;
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
  resolve_level(DEPTH_FIELD_SOURCE, std::size(DEPTH_FIELD_SOURCE), deps[2]);
}

// ============================================================================
// Feature::Selection Implementation
// ============================================================================

void Feature::Selection::clear() {
  filter_data_type.clear();
  filter_cat_l1.clear();
  filter_cat_l2.clear();
  filter_norm_method.clear();
  primary_feature_idx = -1;
  secondary_features.clear();
}

// ============================================================================
// Feature Implementation
// ============================================================================

void Feature::clear() {
  selection.clear();
  // analysis留空,以后扩展
}
