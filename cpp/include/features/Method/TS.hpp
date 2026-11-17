#pragma once

// =============================================================================
// 时序归一化 (namespace ts): 与 Method/CS.hpp 对仗, 声明在字段表 SRC 列 TS(node, port, Tf)
// =============================================================================
//   元素预变换 Tf (TS 行末参; 无参, 元素级, 单调, 逐值纯函数 → 落盘时 RowWriter 直接套用):
//     None     x → x
//     Log      x → sign(x)·log1p(|x|)   (与 cs::Log 同式; 重尾量 (量/额) 压尾)
//     Asinh    x → asinh(x)             (≈ 小值线性 / 大值对数, 不需要 log1p 的保号技巧)
//     Tanh     x → tanh(x)              (有界 (-1,1))
//     Sqrt     x → sign(x)·sqrt(|x|)    (幂变换 α = 0.5 固定; Tf 无参)
//     【fast-math 契约】落盘热路径 TU 走 -ffast-math, 这里不做 isnan/isfinite: NaN 靠硬件算术透传 (与 Fund/Valuation 同约);
//     cs::Log 的 "非 finite → NaN" 在 precise TU 里, 此处 ±inf 原样传
//
//   消费方式:
//     编译期: fstore::RowWriter (FeatureStore.hpp) 按 TS(node, port, Tf) 展开 ts::Tf::apply(值) 写槽位
//     元数据: FieldInfo.ts_tf (FeatureTier, 进指纹) / FeatureMetadata.ts_tf (Feature.hpp, GUI 列)
// =============================================================================

#include <cmath>
#include <cstdint>

namespace ts {

// ---- 全集 (X-macro: enum + 名字表 由此展开; 加项 = 加一行 + 一个 struct) ----
#define TS_TFS(X)   \
  X(None, "无")     \
  X(Log, "对数")    \
  X(Asinh, "asinh") \
  X(Tanh, "tanh")   \
  X(Sqrt, "开方")

// ---- 元素预变换 (逐值; 落盘热路径内联) ----
struct None {
  static constexpr float apply(float x) { return x; }
};
struct Log {
  static inline float apply(float x) { return std::copysign(std::log1p(std::fabs(x)), x); }
};
struct Asinh {
  static inline float apply(float x) { return std::asinh(x); }
};
struct Tanh {
  static inline float apply(float x) { return std::tanh(x); }
};
struct Sqrt {
  static inline float apply(float x) { return std::copysign(std::sqrt(std::fabs(x)), x); }
};

// ---- 枚举 + 名字表 (元数据 / GUI) ----
#define TS_ENUM_ONE(name, label) name,
enum class TfId : uint8_t { TS_TFS(TS_ENUM_ONE) kCount };
#undef TS_ENUM_ONE

#define TS_NAME_ONE(name, label) label,
inline constexpr const char *TF_NAMES[] = {TS_TFS(TS_NAME_ONE)};
#undef TS_NAME_ONE

#define TS_TOKEN_ONE(name, label) #name,
inline constexpr const char *TF_TOKENS[] = {TS_TFS(TS_TOKEN_ONE)};
#undef TS_TOKEN_ONE

} // namespace ts
