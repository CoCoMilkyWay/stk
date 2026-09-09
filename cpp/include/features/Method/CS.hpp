#pragma once

// =============================================================================
// 截面方法 (namespace cs): 无状态 struct, dense 列原地变换. 契约见 DataDefine.hpp, 实现 src/features/Method/CS.cpp (precise-math)
// =============================================================================
//   元素预变换 Tf (CS 行第三参, 先于方法; 无参, 元素级, 单调):
//     None         x → x
//     Reciprocal   x → 1/x  (x==0 或非 finite → NaN; 估值比率 → 收益率口径: pe→ep, pb→bp, ps→sp, pcf→cp)
//     Log          x → sign(x)·log1p(|x|)  (重尾量 (量/额) 走 z 类方法前压尾; rank 类方法对单调变换不变, 无意义)
//   截面方法 Method (CS 行第四参; None 同时充当"关"):
//     Rank         pct_rank, 输出 ∈ [0,1] (并列均秩), 缺失 → 均值填充.  最干净的基线, 丢弃截面离散度
//     NormRank     rank → inverse normal, 输出 ~N(0,1), 缺失 → 0.  Φ⁻¹((rank+1)/(N+1)), Beasley-Springer-Moro 近似, 钳 ±6
//     WinsorRank   winsor_mad(k=3) → z → pct_rank → 均值填充, 输出 ∈ [0,1]                    (= qmt factor_pipeline)
//     NeutralRank  winsor_q(1%,99%) → 中性化 (行业 + log 市值 OLS 残差, FWL 等价: 行业组内 demean → 对 demean 后 log(mcap)
//                  标量回归取残差, 行业 0 独立组) → z → pct_rank → 均值填充, 输出 ∈ [0,1]     (= qmt neutral_pipeline)
//                  唯一需要上下文的方法 (kNeutral): Ctx 由 CoreCrosssection 每分钟准备一次, 全部 NeutralRank 行复用 (仅 L1)
//     Demean       x − mean, 缺失 → 0.  去市场分量但保留截面离散度的时变 (z 会把每分钟离散度抹平成 1)
//     Z            (x − mean)/sd (double 累加), 缺失 → 0.  保留量级信息 (全市场同向尖峰 rank 看不见)
//     WinsorZ      winsor_mad(k=3) → z, 缺失 → 0.  稳健 z (= WinsorRank 去掉末尾 pct_rank)
//
//   忠实性契约: WinsorRank / NeutralRank 与 qmt/cpp/src/feature/cs.cpp 逐步一致
//   (中位数取法 / z 的 double 累加 / pct_rank 并列均秩 / 均值填充), 保证 qmt 因子效果可复现.
//
//   两种消费方式, 同一套 struct:
//     编译期: CoreCrosssection 按字段表 CS(src, Tf, Method) 模板展开 (回测 / 实盘)
//     运行期: Transform 分析用 column_fn(TfId, MethodId) 取一次函数指针 (每个 (Tf, Method) 组合是一个
//             静态实例化的 run<Tf, Method>, 内循环全内联), 每列一次间接调用. 探索里验证的变换 == 落盘算子.
// =============================================================================

#include <cstddef>
#include <cstdint>

namespace cs {

// ---- 全集 (X-macro: enum + 名字表 + 分派表 由此展开; 加方法 = 加一行 + 一个 struct) ----
#define CS_TFS(X)       \
  X(None, "无")         \
  X(Reciprocal, "倒数") \
  X(Log, "对数")

#define CS_METHODS(X)           \
  X(None, "无")                 \
  X(Rank, "Rank")               \
  X(NormRank, "NormRank")       \
  X(WinsorRank, "WinsorRank")   \
  X(NeutralRank, "NeutralRank") \
  X(Demean, "Demean")           \
  X(Z, "Z")                     \
  X(WinsorZ, "WinsorZ")

// ---- 元素预变换 ----
struct None { // Tf 与 Method 双身份: 恒等
  static constexpr bool kNeutral = false;
  static void apply(float *, std::size_t) {}
};
struct Reciprocal {
  static void apply(float *y, std::size_t n);
};
struct Log {
  static void apply(float *y, std::size_t n);
};

// ---- 截面方法 ----
struct Rank {
  static constexpr bool kNeutral = false;
  static void apply(float *y, std::size_t n);
};

struct NormRank {
  static constexpr bool kNeutral = false;
  static void apply(float *y, std::size_t n);
};

struct WinsorRank {
  static constexpr bool kNeutral = false;
  static void apply(float *y, std::size_t n);
};

struct Demean {
  static constexpr bool kNeutral = false;
  static void apply(float *y, std::size_t n);
};

struct Z {
  static constexpr bool kNeutral = false;
  static void apply(float *y, std::size_t n);
};

struct WinsorZ {
  static constexpr bool kNeutral = false;
  static void apply(float *y, std::size_t n);
};

struct NeutralRank {
  static constexpr bool kNeutral = true;

  // 中性化上下文: 与 y 同下标的 dense 数组 (有效资产子集)
  struct Ctx {
    const float *logmc;    // log(总市值), ≤0 / 非 finite → NaN
    const float *industry; // SW2021 一级行业 ID (0 = 未知, 1..31)
  };

  static void prepare_logmc(float *mcap, std::size_t n); // mcap → log(mcap) 原地
  static void apply(float *y, std::size_t n, const Ctx &c);
};

// ---- 运行期分派 (Transform 分析) ----
#define CS_ENUM_ONE(name, label) name,
enum class TfId : uint8_t { CS_TFS(CS_ENUM_ONE) kCount };
enum class MethodId : uint8_t { CS_METHODS(CS_ENUM_ONE) kCount };
#undef CS_ENUM_ONE

#define CS_NAME_ONE(name, label) label,
inline constexpr const char *TF_NAMES[] = {CS_TFS(CS_NAME_ONE)};
inline constexpr const char *METHOD_NAMES[] = {CS_METHODS(CS_NAME_ONE)};
#undef CS_NAME_ONE

#define CS_TOKEN_ONE(name, label) #name,
inline constexpr const char *TF_TOKENS[] = {CS_TFS(CS_TOKEN_ONE)};
inline constexpr const char *METHOD_TOKENS[] = {CS_METHODS(CS_TOKEN_ONE)};
#undef CS_TOKEN_ONE

#define CS_NEUTRAL_ONE(name, label) name::kNeutral,
inline constexpr bool METHOD_NEUTRAL[] = {CS_METHODS(CS_NEUTRAL_ONE)};
#undef CS_NEUTRAL_ONE
constexpr bool method_neutral(MethodId m) { return METHOD_NEUTRAL[static_cast<std::size_t>(m)]; }

// 一列: dense 有效子集 y[n] 原地 → Tf::apply → Method::apply. ctx 只在 method_neutral(m) 时读 (否则可传 nullptr).
// 表 [TfId][MethodId] 由 CS.cpp 用 run<Tf, Method> 全组合实例化, 取一次指针, 逐列调用.
using ColumnFn = void (*)(float *y, std::size_t n, const NeutralRank::Ctx *ctx);
ColumnFn column_fn(TfId tf, MethodId m);

} // namespace cs

// NeutralRank 上下文源列 (L1 字段 code): CoreCrosssection 每分钟按 L1_Field::<token> gather 一次
#define NEUTRAL_RANK_MCAP mcap
#define NEUTRAL_RANK_INDUSTRY ind_l1
