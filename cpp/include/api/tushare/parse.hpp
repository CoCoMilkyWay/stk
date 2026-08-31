#pragma once

#include "yyjson/yyjson.h"

#include <arrow/table.h>

#include <memory>
#include <vector>

namespace tushare {

struct InterfaceSpec;

// ============================================================================
// 响应 JSON → arrow::Table (与 bigquant fetch 出口对齐, 下游统一走 parquet).
//
// 输入: 同一接口的 1..N 个响应 (range API 1 个 / per-day API 每日 1 个),
//       envelope = root.data.{fields, items}; fields 各响应必须一致 (assert).
// 列类型: spec.num_fields 声明的列 → double, 其余 → string (不按数据推断,
//         见 spec.hpp num_fields 注释; 保证同一表跨月 schema 恒定).
// spec.drop_fields 内的列剥离 (防未来信息泄漏), 其余行原样保留 (信任服务端).
// 0 行输入 → 0 行表 (schema 仍完整).
// ============================================================================
std::shared_ptr<arrow::Table>
docs_to_table(const std::vector<yyjson_doc *> &docs, const InterfaceSpec &spec);

} // namespace tushare
