#pragma once

// =============================================================================
// 因子算子语义契约 (三后端唯一共享件之一, 另一件是 factor/OpTable.hpp)
// =============================================================================
//   【分类】算子 = 核作用在支撑集 S(t,a) 上, 三个正交维度各取一值 (每维穷尽互斥):
//     T 窗 (enum T)    输出在 t 依赖的时间支撑: POINT 当前点 / EXPAND 段内 expanding / ROLL 最近 d 期 / EXPO 指数加权全历史
//     A 域 (enum A)    资产支撑: SELF 只看本资产 / ALL 同一时刻全截面 / GROUP 同一时刻组内 (整数组 id 列给组)
//     核类 (Kern)   统计核的代数类: MAP 逐元素变换 / SHIFT 下标平移 / MOMENT 可和分解 (矩族) /
//                   EXTREME 极值及 arg 族 (半群不可逆) / ORDER 序统计 / RECUR 递推
//   类型约束 (非归约核不消费集合, 部分格子空): MAP ⇒ POINT×SELF; SHIFT ⇒ SELF 纯滞后 (不看整窗);
//   RECUR ⇔ EXPO 一一绑定; 归约核 (MOMENT / EXTREME / ORDER) 与窗/域自由组合. 复合算子标主导 (最重) 一级.
//   现库只填积空间里的"十字": SELF×任意窗 (Ts 前缀) ∪ POINT×{ALL, GROUP} (Cs 前缀);
//   十字外 (如 EXPAND×GROUP 段内组均值) 是合法格子, 留待将来, 命名按计算顺序 TsCs<核> / CsTs<核>.
//   CPU / GPU 具体怎么算 (逐格融合 / scan / van Herk / 直方图 …) 是后端私事, 不进表不进分类.
//
//   【三后端】同名 struct, 三处**完全独立**实现, 不共享一行算法代码 (否则对拍空转):
//     factor::ts::<Name>       factor::cs::<Name>       实盘流式 (逐点因果, O(d) 状态)  TS/Stream.hpp  CS/Stream.hpp
//     factor::cpu::ts::<Name>  factor::cpu::cs::<Name>  挖掘 CPU (整张量向量化批算)     TS/Cpu.hpp     CS/Cpu.hpp
//     factor::gpu::ts::<Name>  factor::gpu::cs::<Name>  挖掘 GPU (CUDA, 向量化)         TS/Gpu.cuh     CS/Gpu.cuh
//   缺任一侧 → op_check 编译错 (分派由 OP_ALL 宏展开).
//   OpTable 的"T 窗"列是**载荷**: 流式 TS struct 带 kT, op_check static_assert 对表;
//   A 域 / 核类是纯语义列, 实现侧不带载荷 (后端怎么算不受表约束).
//
//   【数据布局】
//     数组一律 SoA 两平面: 值 float* + 有效位 uint8_t*, 行主序 [T][A] (t*A + a).
//     标量用 Val {v, m}. GPU 侧同 SoA, 不用 AoS.
//     段 = 一个交易日 = kSegLen 分钟. 段内位置 t_seg = t % kSegLen (块起点对齐段起点).
//     Expand 窗按段 reset (流式实现推满 kSegLen 自动归零); Roll 窗**跨段**不 reset (窗口单位是分钟, 与日界无关);
//     Expo 窗全程递推, 不 reset.
//     Roll 窗未满 (t < d−1) 一律输出无效; 例外: TsDelayRoll / TsDeltaRoll 看的是 d 期之前那一格,
//     实际跨 d+1 格, 故 t < d 才无效.
//
//   【数值 / 退化契约】全程 branchless, 任何后端都不产 NaN / inf:
//     1. 入口: 落盘用 NaN 表缺失 → Val{0, false}. 之后 v 恒为有限值.
//     2. 退化 = **精确全并列** spread(lo, hi) = hi > lo 为假 (Var/Std/Skew/Kurt/Z/Corr/Beta/Resid/序统计族).
//        比的是同一份 fp32 输入的极值, 无舍入 → 三后端逐位一致, 且与量级无关: 价格 50 元窗内一跳 0.01 有离散度;
//        常值窗哪怕滑窗累加器残留 1e-14 也判退化 (GPU 侧用"最近变动位置 > 窗内首个有效位置"等价实现, 无需减法).
//     3. 相消 den_ok(): 分母相对两侧量级 < kRelEps 为退化 (Imb/Share/Hhi/WMean 的 Σ ≈ 0 是数值无意义, 不是并列).
//        这是唯一保留阈值的判据; 恰落在阈值带内的输入三后端可能不一致, 造数刻意避开.
//     4. 逐点除法 (Div/Recip): 三后端同序 IEEE 运算, 直接判 y ≠ 0.
//     5. **没有分母钳位**: 掩码已保证分母非零; 绝对 eps 钳位只会在小量级输入上把正确值改错而掩码仍真.
//        有效位上的溢出 (inf) 由出口 mk() 转成无效 —— 溢出是数据属性, 不是 bug.
//     6. valid = false 时 v = 0, 消费端只看 m.
//     7. 累加: CPU 用 double; GPU 幂和用 double, 按段/按块分块, 禁全局长 cumsum.
//     8. 输出 float (挖掘侧量化到 fp16 前需 clamp 到 ±65504, 见 operator1.md).
//
//   【对拍】掩码逐位相等, 且有效位上 |Δ| ≤ atol + rtol·max(|a|,|b|).
//
//   【precise-math】本模块依赖受控浮点语义, 必须编进 -fno-fast-math TU (CMake PRECISE_MATH_FLAG).
// =============================================================================

#include <cassert>
#include <cmath>

namespace factor {

// ---- 运行期参数 (每个算子只读 OpTable 里声明的字段) ----
struct Param {
  int d = 0;      // 窗长 / 滞后 (期 = 分钟)
  float k = 0.f;  // 阈值 / 桶数 / EMA 系数 / 分位
  float k2 = 0.f; // 第二阈值 (TodMask 上界)
};

// ---- 常量 ----
inline constexpr float kRelEps = 1e-6f; // 相消退化阈值 (只给 den_ok)
inline constexpr int kSegLen = 240;     // 段 (交易日) 的分钟数
inline constexpr int kBuckets = 256;    // 序统计近似的直方图桶数
inline constexpr int kMaxGroup = 1024;  // 分组列 id 上限 (三后端同一上限; GPU 用作片上槽数)

// ---- 三维分类 (OpTable 三列; 列名 = 枚举名 = 行字段名, 全库统一叫 T / A; 流式 TS struct::kT 对表) ----
enum class T { POINT,
               EXPAND,
               ROLL,
               EXPO };
enum class A { SELF,
               ALL,
               GROUP };
enum class Kern { MAP,
                  SHIFT,
                  MOMENT,
                  EXTREME,
                  ORDER,
                  RECUR };

// ---- 值 + 有效位 ----
struct Val {
  float v = 0.f;
  bool m = false;
};

// 入口: 非有限 → 无效. 落盘 NaN 在此转成 {0, false}, 之后全链路无 NaN
inline Val in(float x) { return std::isfinite(x) ? Val{x, true} : Val{0.f, false}; }
// 出口: 无效或非有限 (溢出) → {0, false}
inline Val mk(double v, bool m) {
  const float f = static_cast<float>(v);
  const bool ok = m && std::isfinite(f);
  return Val{ok ? f : 0.f, ok};
}

// ---- 退化判据 (三后端必须用同一式) ----
// 全并列: lo/hi = 有效样本极值. 精确比较, 无阈值
inline bool spread(float lo, float hi) { return hi > lo; }
// 相消: den 相对于两侧量级 scale
inline bool den_ok(double den, double scale) { return std::fabs(den) > static_cast<double>(kRelEps) * (scale + 1e-30); }

// ---- 并列均秩的 pct rank: (avg_rank − 1)/(m − 1), m ≤ 1 → 0.5 ----
inline float pct_of(int less, int eq, int m) {
  if (m <= 1)
    return 0.5f;
  const double avg = static_cast<double>(less) + (static_cast<double>(eq) + 1.0) * 0.5;
  return static_cast<float>((avg - 1.0) / static_cast<double>(m - 1));
}

// ---- 标准正态分位 Φ⁻¹ (Wichura AS241 PPND16, 相对误差 < 1e-15) ----
//   仓库里 vendored boost 只是子集, 与系统 boost 混链会炸, 所以自带一份.
//   这是共享数学件 (与 pct_of 同性质), 不是算子算法; GPU 侧用 normcdfinvf, 两者差 ~1e-7, 对拍容差已放宽.
inline float probit(double p) {
  assert(p > 0.0 && p < 1.0);
  const double q = p - 0.5;
  if (std::fabs(q) <= 0.425) {
    const double r = 0.180625 - q * q;
    const double num = (((((((2.5090809287301226727e+3 * r + 3.3430575583588128105e+4) * r +
                             6.7265770927008700853e+4) *
                                r +
                            4.5921953931549871457e+4) *
                               r +
                           1.3731693765509461125e+4) *
                              r +
                          1.9715909503065514427e+3) *
                             r +
                         1.3314166789178437745e+2) *
                            r +
                        3.3871328727963666080);
    const double den = (((((((5.2264952788528545610e+3 * r + 2.8729085735721942674e+4) * r +
                             3.9307895800092710610e+4) *
                                r +
                            2.1213794301586595867e+4) *
                               r +
                           5.3941960214247511077e+3) *
                              r +
                          6.8718700749205790830e+2) *
                             r +
                         4.2313330701600911252e+1) *
                            r +
                        1.0);
    return static_cast<float>(q * num / den);
  }
  double r = std::sqrt(-std::log(q < 0 ? p : 1.0 - p));
  double v;
  if (r <= 5.0) {
    r -= 1.6;
    const double num = (((((((7.74545014278341407640e-4 * r + 2.27238449892691845833e-2) * r +
                             2.41780725177450611770e-1) *
                                r +
                            1.27045825245236838258) *
                               r +
                           3.64784832476320460504) *
                              r +
                          5.76949722146069140550) *
                             r +
                         4.63033784615654529590) *
                            r +
                        1.42343711074968357734);
    const double den = (((((((1.05075007164441684324e-9 * r + 5.47593808499534494600e-4) * r +
                             1.51986665636164571966e-2) *
                                r +
                            1.48103976427480074590e-1) *
                               r +
                           6.89767334985100004550e-1) *
                              r +
                          1.67638483018380384940) *
                             r +
                         2.05319162663775882187) *
                            r +
                        1.0);
    v = num / den;
  } else {
    r -= 5.0;
    const double num = (((((((2.01033439929228813265e-7 * r + 2.71155556874348757815e-5) * r +
                             1.24266094738807843860e-3) *
                                r +
                            2.65321895265761230930e-2) *
                               r +
                           2.96560571828504891230e-1) *
                              r +
                          1.78482653991729133580) *
                             r +
                         5.46378491116411436990) *
                            r +
                        6.65790464350110377720);
    const double den = (((((((2.04426310338993978564e-15 * r + 1.42151175831644588870e-7) * r +
                             1.84631831751005468180e-5) *
                                r +
                            7.86869131145613259100e-4) *
                               r +
                           1.48753612908506148525e-2) *
                              r +
                          1.36929880922735805310e-1) *
                             r +
                         5.99832206555887937690e-1) *
                            r +
                        1.0);
    v = num / den;
  }
  return static_cast<float>(q < 0 ? -v : v);
}

// ---- 分桶规则 (序统计族: 三后端共用同一规则, 故桶计数是整数, 对拍可严格逐位) ----
//   lo/hi = 样本集的 min/max. 全并列 (!spread) → 由各算子给中性值.
//   误差相对于"真序统计"是桶宽量级 (kBuckets 分辨率), 但三后端之间**没有**误差.
inline int bin_of(float x, float lo, float hi) {
  const int b = static_cast<int>((static_cast<double>(x) - lo) / (static_cast<double>(hi) - lo) * kBuckets);
  return b < 0 ? 0 : (b >= kBuckets ? kBuckets - 1 : b);
}
inline float bin_center(int b, float lo, float hi) {
  return static_cast<float>(lo + (static_cast<double>(hi) - lo) * (b + 0.5) / kBuckets);
}

} // namespace factor
