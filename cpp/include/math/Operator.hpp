#pragma once

#include <cassert>
#include <cstddef>

namespace math {

// ============================================================================
// 参数元数据
// ============================================================================

struct ParamMeta {
  const char *name;
  float default_val;
  float min_val;
  float max_val;
};

// ============================================================================
// 算子定义 (静态, 编译期)
// ============================================================================
//
// 每个算子在自己文件里定义:
//   struct MyOp {
//     static constexpr ParamMeta meta[] = {...};
//     static constexpr OperatorDef def = {"名字", meta, N};
//   };
//
// ============================================================================

struct OperatorDef {
  const char *name = "";
  const ParamMeta *meta = nullptr;
  size_t param_count = 0;
};

// ============================================================================
// 算子实例 (运行时)
// ============================================================================
//
// 继承 OperatorDef，添加运行时参数存储
//
// 编译期优化:
//   compute(x, constant<3.0f>());       // 常量内联
//   compute(x, [&] { return op[0]; });  // 运行期动态
//
// ============================================================================

struct Operator : OperatorDef {
  float params[4] = {};

  // 从静态定义初始化
  void init(const OperatorDef &d) {
    name = d.name;
    meta = d.meta;
    param_count = d.param_count;
    reset();
  }

  // 重置为默认值
  void reset() {
    for (size_t i = 0; i < param_count && i < 4; ++i)
      params[i] = meta[i].default_val;
  }

  // 索引访问
  float &operator[](size_t i) {
    assert(i < 4);
    return params[i];
  }
  float operator[](size_t i) const {
    assert(i < 4);
    return params[i];
  }
};

// ============================================================================
// 编译期常量 (用于静态优化)
// ============================================================================

template <auto V>
constexpr auto constant() {
  return [] { return V; };
}

} // namespace math
