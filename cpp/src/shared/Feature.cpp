#include "shared/Feature.hpp"

#include <cassert>
#include <cstring>
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

size_t Feature::Metadata::col_of(size_t level, const char *code) const {
  assert(level < LEVEL_COUNT);
  const auto &list = features[level];
  for (size_t i = 0; i < list.size(); ++i)
    if (std::strcmp(list[i].code, code) == 0)
      return i;
  assert(false && "字段表里找不到该列");
  return 0;
}

// ============================================================================
// Feature::Selection Implementation
// ============================================================================

void Feature::Selection::clear() {
  filter_data_type.clear();
  filter_cat_l1.clear();
  filter_cat_l2.clear();
  filter_ts_method.clear();
  filter_cs_method.clear();
  selected_features.clear();
}

// ============================================================================
// Feature Implementation
// ============================================================================

void Feature::clear() { selection.clear(); }
