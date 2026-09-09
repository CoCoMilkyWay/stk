#pragma once

// =============================================================================
// 时序归一化 (namespace ts): 与 Method/CS.hpp 对仗, 声明在字段表 SRC 列 OP(node[, port], Tf, Method)
// =============================================================================
//   元素预变换 Tf (OP 行倒数第二参; 无参, 元素级, 单调, 逐值纯函数 → 落盘时 RowWriter 直接套用):
//     None     x → x
//     Log      x → sign(x)·log1p(|x|)   (与 cs::Log 同式; 重尾量 (量/额) 压尾)
//     Asinh    x → asinh(x)             (≈ 小值线性 / 大值对数, 不需要 log1p 的保号技巧)
//     Tanh     x → tanh(x)              (有界 (-1,1))
//     Sqrt     x → sign(x)·sqrt(|x|)    (幂变换 α = 0.5 固定; Tf 无参)
//     【fast-math 契约】落盘热路径 TU 走 -ffast-math, 这里不做 isnan/isfinite: NaN 靠硬件算术透传 (与 Fund/Valuation 同约);
//     cs::Log 的 "非 finite → NaN" 在 precise TU 里, 此处 ±inf 原样传
//   时序方法 Method (OP 行末参; None 同时充当"关"). 单资产因果 expanding 统计, 只看历史:
//     Z / RobustZ / IqrZ / Rank / NormRank / Clip / Winsor
//     ★ 占位: 只有名字 (枚举 / 元数据 / GUI 列), 尚无实现, 未接入 CoreSequential; 落盘值 == Tf(节点输出).
//       接入时需要每资产每列一份 expanding 状态 (放 DAG 旁), 签名届时再定; 探索侧对应 math::normalize (Transform 的 ts_norm).
//
//   消费方式:
//     编译期: fstore::RowWriter (FeatureStore.hpp) 按 OP(..., Tf, Method) 展开 ts::Tf::apply(值) 写槽位
//     元数据: FieldInfo.ts_tf (FeatureStoreConfig, 进指纹) / FeatureMetadata.ts_tf + ts_method (Feature.hpp, GUI 两列)
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

#define TS_METHODS(X)     \
  X(None, "无")           \
  X(Z, "Z")               \
  X(RobustZ, "RobustZ")   \
  X(IqrZ, "IqrZ")         \
  X(Rank, "Rank")         \
  X(NormRank, "NormRank") \
  X(Clip, "Clip")         \
  X(Winsor, "Winsor")

// ---- 元素预变换 (逐值; 落盘热路径内联) ----
struct None { // Tf 与 Method 双身份: 恒等
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

// ---- 时序方法 (占位: 见文件头 ★) ----
struct Z {};        // expanding (x − mean) / sd
struct RobustZ {};  // expanding (x − median) / MAD
struct IqrZ {};     // expanding (x − Q2) / (Q3 − Q1)
struct Rank {};     // expanding pct_rank ∈ [0,1]
struct NormRank {}; // expanding pct_rank → Φ⁻¹
struct Clip {};     // expanding z → clip ±k
struct Winsor {};   // expanding 分位缩尾

// ---- 枚举 + 名字表 (元数据 / GUI) ----
#define TS_ENUM_ONE(name, label) name,
enum class TfId : uint8_t { TS_TFS(TS_ENUM_ONE) kCount };
enum class MethodId : uint8_t { TS_METHODS(TS_ENUM_ONE) kCount };
#undef TS_ENUM_ONE

#define TS_NAME_ONE(name, label) label,
inline constexpr const char *TF_NAMES[] = {TS_TFS(TS_NAME_ONE)};
inline constexpr const char *METHOD_NAMES[] = {TS_METHODS(TS_NAME_ONE)};
#undef TS_NAME_ONE

#define TS_TOKEN_ONE(name, label) #name,
inline constexpr const char *TF_TOKENS[] = {TS_TFS(TS_TOKEN_ONE)};
inline constexpr const char *METHOD_TOKENS[] = {TS_METHODS(TS_TOKEN_ONE)};
#undef TS_TOKEN_ONE

} // namespace ts
