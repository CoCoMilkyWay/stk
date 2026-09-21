#pragma once

// =============================================================================
// ELEM 逐值算子 (无状态, 契约见 Kernel.hpp; 表见 OpTable.hpp OP_ELEM1/2/3)
// =============================================================================
//   NaN 自然传播; 仅"无定义"处显式产 NaN (零分母 / 非正取对数).
//   一元中 Log/Asinh/Tanh/Sqrt 与 ts::Tf (Method/TS.hpp) 同式, 这里是因子层的同名副本 (ts:: 版走 fast-math 落盘热路径, 不能互相 include).
// =============================================================================

#include "factor/stream/Kernel.hpp"

namespace factor {

// ---- 一元 ----
struct Abs {
  static float apply(float x, const Param &) { return std::fabs(x); }
};
struct Sign {
  static float apply(float x, const Param &) { return ok(x) ? (x > 0.f ? 1.f : (x < 0.f ? -1.f : 0.f)) : kNaN; }
};
struct Log {
  static float apply(float x, const Param &) { return std::copysign(std::log1p(std::fabs(x)), x); }
};
struct Asinh {
  static float apply(float x, const Param &) { return std::asinh(x); }
};
struct Tanh {
  static float apply(float x, const Param &) { return std::tanh(x); }
};
struct Sqrt {
  static float apply(float x, const Param &) { return std::copysign(std::sqrt(std::fabs(x)), x); }
};
struct Relu {
  static float apply(float x, const Param &) { return ok(x) ? std::max(0.f, x) : kNaN; }
};
struct Recip {
  static float apply(float x, const Param &) { return x != 0.f ? 1.f / x : kNaN; }
};
struct SignedPow {
  static float apply(float x, const Param &p) { return std::copysign(std::pow(std::fabs(x), p.k), x); }
};
struct Clip {
  static float apply(float x, const Param &p) { return ok(x) ? std::clamp(x, -p.k, p.k) : kNaN; }
};

// ---- 二元 ----
struct Add {
  static float apply(float x, float y, const Param &) { return x + y; }
};
struct Sub {
  static float apply(float x, float y, const Param &) { return x - y; }
};
struct Mul {
  static float apply(float x, float y, const Param &) { return x * y; }
};
struct Div {
  static float apply(float x, float y, const Param &) { return y != 0.f ? x / y : kNaN; }
};
struct Max {
  static float apply(float x, float y, const Param &) { return (ok(x) && ok(y)) ? std::max(x, y) : kNaN; }
};
struct Min {
  static float apply(float x, float y, const Param &) { return (ok(x) && ok(y)) ? std::min(x, y) : kNaN; }
};
struct Imb {
  static float apply(float x, float y, const Param &) {
    const float s = x + y;
    return s != 0.f ? (x - y) / s : kNaN;
  }
};
struct Share {
  static float apply(float x, float y, const Param &) {
    const float s = x + y;
    return s != 0.f ? x / s : kNaN;
  }
};
struct LogRatio {
  static float apply(float x, float y, const Param &) { return (x > 0.f && y > 0.f) ? std::log(x / y) : kNaN; }
};

// ---- 三元 ----
struct Where {
  static float apply(float x, float y, float z, const Param &) { return ok(x) ? (x > 0.f ? y : z) : kNaN; }
};

} // namespace factor
