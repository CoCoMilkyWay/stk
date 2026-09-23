#pragma once

// =============================================================================
// Stat: 因子评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 无流式后端 —— 评估只在挖掘侧)
// =============================================================================
//   输入  因子平面 x [T][A] (SoA: float + 有效位) + H 组常驻标签 (每组一个持有期 h 分钟):
//           lv / sv  做多 / 做空吃单净收益 (features 层 LabelReturn, fp16 位, 与落盘同格式, 无精度损失)
//           m        标签有效位 (long / short 同一快照, 共用一张)
//           ry       预处理: 做多标签逐行截面 rank (r16), 与因子无关, 常驻期算一次 (prep_label)
//   输出  两级:
//     一级 Row[H][T]  每 (持有期, 时刻) 一行沿 A 轴归约的统计 (两后端各算, 对拍在此级)
//     二级 HoldStat   每持有期 沿 T 轴汇总成十几个标量 (summarize, 主机 double, 两后端共用同一份代码)
//
//   【rank 口径】精确并列均秩 (排序, 不用桶近似 —— 原始因子重尾时一个离群点会把整截面压进同一桶, IC 归零):
//     r16 = round(pct · kRankMax) ∈ [0, kRankMax], 全整数公式 (r16_of), 两后端逐位一致; kRankNone = 无效.
//     行有效样本 n < 2 或全并列 (spread 为假) → 整行 kRankNone.
//     rank(x) 只看 x 的有效集, rank(y) 只看 y 的有效集 (与因子无关才能常驻); 两者的联合集 J 上算统计.
//
//   【一级 Row (每 h, t)】J = {a : rx ≠ None ∧ ry ≠ None}
//     ic     Spearman = Pearson(r16_x, r16_y) over J. 五个和都是整数 (pearson_int), 两后端逐位一致.
//     grp[k] 第 k 组 (k = grp_of(r16_x), 等分 kGroups 组) 做多标签均值; 任一组空 → 行无效.
//     ls     多空 = grp[K−1] (最高组做多) + 最低组做空标签均值 —— 两边都是实盘可成交的净收益.
//     mkt    J 上做多标签均值 (二级回归 beta 用).
//     ok     标签侧有效: 非段末尾部 (见下) ∧ ic 可算 (n ≥ 2, 两侧方差 > 0) ∧ 各组非空.
//     ac     rank-AC: Pearson(r16_x(t), r16_x(t−h)) over 两行都有效的资产 —— lag = h 才对应"一个持有期换多少仓".
//     ok_ac  t ≥ h ∧ n ≥ 2 ∧ 两侧方差 > 0. 与标签无关, 段末尾部也算.
//     段末尾部: LabelReturn 的 exit 越过连续竞价末秒 (14:57, 段末 kCloseAuction 分钟) 就"持有到收盘",
//       持有期缩短 → 掩掉 t_seg ≥ kSegLen − h − kCloseAuction 的行 (tail_masked).
//     ok / ok_ac 为假时对应字段全 0 (与 Contract 的 valid=false ⇒ v=0 同约).
//
//   【二级 HoldStat (每 h)】只用 ok 行 (n) / ok_ac 行 (n_ac):
//     ic_mean / ic_std (ddof=1) / icir = mean/std / ic_pos = IC>0 占比 / ic_skew, ic_kurt (总体矩, 超额峰度)
//     ic_t = icir · √(n/h): 分钟行的标签持有期重叠 (相邻 h 行是同一段收益), 有效样本按 n/h 折算, 不然 t 虚高 √h 倍
//     ls_mean / ls_t (同上折算) / sharpe = mean/std · √(kDaysPerYear · kSegLen / h) (按持有期为一期年化)
//     beta   ls 对 mkt 的 OLS 斜率 (alpha 略: beta ≈ 0 时 alpha ≈ ls_mean)
//     grp[k] 各组均值的时间平均; mono = Spearman(组号, grp[k])
//     rank_ac = ac 的时间平均
//     n < 3 → 只填 n, 其余 0 (消费端先看 n)
//
//   【数值】整数和精确 (r16 ≤ 65534, A ≤ kMaxA → n·Σx² < 2^57, 有符号 64 位不溢); 浮点和 (标签值) 两后端
//   求和序不同, 走对拍容差. 全程 branchless 无 NaN: 标签 fp16 → float 是精确转换, 出口不产 inf.
//   【precise-math】依赖受控浮点, 编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Contract.hpp"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>

namespace factor::stat {

// ---- 常量 ----
inline constexpr int kGroups = 20;              // 分组数 (等分 pct rank)
inline constexpr int kMaxHold = 8;              // 同时评估的持有期数上限
inline constexpr int kMaxA = 5120;              // 资产轴上限 (GPU 一行一 block 的片上排序容量)
inline constexpr int kCloseAuction = 3;         // 段末收盘集合竞价分钟数 (14:57–15:00), 标签 exit 不能越过
inline constexpr int kDaysPerYear = 242;        // 年化用交易日数
inline constexpr uint16_t kRankNone = 0xFFFF;   // r16 无效
inline constexpr int kRankMax = 65534;          // r16 = round(pct · kRankMax) ∈ [0, kRankMax]
inline constexpr unsigned kNoKey = 0xFFFFFFFFu; // 排序键的"无效"哨兵 (有限 float 的保序键 ≤ 0xFF800000, 不冲突)

// ---- 参数: 持有期列表 (分钟). 每个 h 对应一组标签 ----
struct Holds {
  int n = 0;
  int h[kMaxHold] = {};
};

inline void assert_holds(const Holds &hd) {
  assert(hd.n >= 1 && hd.n <= kMaxHold);
  for (int i = 0; i < hd.n; ++i)
    assert(hd.h[i] >= 1 && hd.h[i] + kCloseAuction < kSegLen && "持有期须 ≥ 1 且尾部掩码不能吞掉整段");
}

// ---- 一级输出: 每 (持有期, 时刻) 一行; 布局 rows[h_idx * T + t] ----
struct Row {
  float ic = 0.f, mkt = 0.f, ls = 0.f, ac = 0.f;
  float grp[kGroups] = {};
  uint8_t ok = 0, ok_ac = 0;
};

// ---- 二级输出: 每持有期一份 ----
struct HoldStat {
  int hold = 0, n = 0, n_ac = 0;
  float ic_mean = 0.f, ic_std = 0.f, icir = 0.f, ic_t = 0.f, ic_pos = 0.f, ic_skew = 0.f, ic_kurt = 0.f;
  float ls_mean = 0.f, ls_t = 0.f, sharpe = 0.f, beta = 0.f, mono = 0.f, rank_ac = 0.f;
  float grp[kGroups] = {};
};

// ---- 共享整数公式 (GPU 侧有逐字一致的 __device__ 版) ----

// float → 保序无符号键 (a < b ⟺ ord(a) < ord(b)); ±0 先归一为 +0
inline unsigned ord(float f) {
  const float g = f + 0.f;
  unsigned u;
  static_assert(sizeof(u) == sizeof(g));
  std::memcpy(&u, &g, sizeof(u));
  return (u & 0x80000000u) ? ~u : (u | 0x80000000u);
}

// 并列均秩 r16: pct = (avg_rank − 1)/(n − 1), avg_rank = less + (eq + 1)/2, r16 = round(pct · kRankMax)
//   全整数: num = (2·less + eq − 1)·kRankMax, den = 2·(n − 1), r16 = ⌊(2·num + den) / (2·den)⌋
inline uint16_t r16_of(int less, int eq, int n) {
  assert(n >= 2 && eq >= 1 && less >= 0 && less + eq <= n);
  const long long num = (2LL * less + eq - 1) * kRankMax;
  const long long den = 2LL * (n - 1);
  return static_cast<uint16_t>((2 * num + den) / (2 * den));
}

// 组号: r16 等分 kGroups 组
inline int grp_of(unsigned r16) {
  const int g = static_cast<int>(r16 * kGroups / (kRankMax + 1));
  return g < kGroups - 1 ? g : kGroups - 1;
}

// 段末尾部 (标签持有到收盘, 持有期缩短) → 掩掉
inline bool tail_masked(int t, int h) { return t % kSegLen >= kSegLen - h - kCloseAuction; }

// 整数五和的 Pearson: vx = n·Σx² − (Σx)² 等全在 64 位整数内精确, 只有最后一步除法/开方是浮点 (IEEE 正确舍入,
// 两后端逐位一致). ok = n ≥ 2 ∧ vx > 0 ∧ vy > 0
inline float pearson_int(unsigned long long n, unsigned long long sx, unsigned long long sy, unsigned long long sxx,
                         unsigned long long syy, unsigned long long sxy, bool &ok) {
  if (n < 2) {
    ok = false;
    return 0.f;
  }
  const long long vx = static_cast<long long>(n * sxx) - static_cast<long long>(sx * sx);
  const long long vy = static_cast<long long>(n * syy) - static_cast<long long>(sy * sy);
  const long long cxy = static_cast<long long>(n * sxy) - static_cast<long long>(sx * sy);
  ok = vx > 0 && vy > 0;
  return ok ? static_cast<float>(static_cast<double>(cxy) / std::sqrt(static_cast<double>(vx) * static_cast<double>(vy))) : 0.f;
}

// ---- 二级汇总 (主机, double; 两后端共用) ----

// 组号 0..K−1 与 g[k] 的 Spearman (g 并列均秩); g 全并列 → 0
inline double mono_of(const double *g, int K) {
  double rk[kGroups];
  for (int i = 0; i < K; ++i) {
    int less = 0, eq = 0;
    for (int j = 0; j < K; ++j) {
      less += g[j] < g[i];
      eq += g[j] == g[i];
    }
    rk[i] = less + (eq + 1) * 0.5; // 1-based 均秩
  }
  const double mu = (K + 1) * 0.5; // 0..K−1 的秩均值 (1-based) = 组号均值 + 1
  double cxy = 0.0, vx = 0.0, vy = 0.0;
  for (int i = 0; i < K; ++i) {
    const double dx = (i + 1) - mu, dy = rk[i] - mu;
    cxy += dx * dy;
    vx += dx * dx;
    vy += dy * dy;
  }
  return vy > 0.0 ? cxy / std::sqrt(vx * vy) : 0.0;
}

// rows = 该持有期的 T 行 (rows + h_idx * T)
inline HoldStat summarize(const Row *r, int T, int hold) {
  assert(T >= 1 && hold >= 1);
  HoldStat s;
  s.hold = hold;
  double sic = 0.0, sls = 0.0, smk = 0.0, sac = 0.0, sg[kGroups] = {};
  int n = 0, npos = 0, nac = 0;
  for (int t = 0; t < T; ++t) {
    const Row &w = r[t];
    if (w.ok) {
      ++n;
      npos += w.ic > 0.f;
      sic += w.ic;
      sls += w.ls;
      smk += w.mkt;
      for (int k = 0; k < kGroups; ++k)
        sg[k] += w.grp[k];
    }
    if (w.ok_ac) {
      ++nac;
      sac += w.ac;
    }
  }
  s.n = n;
  s.n_ac = nac;
  if (nac >= 1)
    s.rank_ac = static_cast<float>(sac / nac);
  if (n < 3)
    return s;
  const double mic = sic / n, mls = sls / n, mmk = smk / n;
  double m2 = 0.0, m3 = 0.0, m4 = 0.0, vls = 0.0, vmk = 0.0, cov = 0.0;
  for (int t = 0; t < T; ++t) {
    const Row &w = r[t];
    if (!w.ok)
      continue;
    const double d = w.ic - mic, dl = w.ls - mls, dm = w.mkt - mmk;
    m2 += d * d;
    m3 += d * d * d;
    m4 += d * d * d * d;
    vls += dl * dl;
    vmk += dm * dm;
    cov += dl * dm;
  }
  const double n_eff = static_cast<double>(n) / hold; // 重叠持有期折算
  const double ic_sd = std::sqrt(m2 / (n - 1));
  const double ls_sd = std::sqrt(vls / (n - 1));
  s.ic_mean = static_cast<float>(mic);
  s.ic_std = static_cast<float>(ic_sd);
  s.ic_pos = static_cast<float>(static_cast<double>(npos) / n);
  if (ic_sd > 0.0) {
    s.icir = static_cast<float>(mic / ic_sd);
    s.ic_t = static_cast<float>(mic / ic_sd * std::sqrt(n_eff));
    const double v = m2 / n;
    s.ic_skew = static_cast<float>((m3 / n) / (v * std::sqrt(v)));
    s.ic_kurt = static_cast<float>((m4 / n) / (v * v) - 3.0);
  }
  s.ls_mean = static_cast<float>(mls);
  if (ls_sd > 0.0) {
    s.ls_t = static_cast<float>(mls / ls_sd * std::sqrt(n_eff));
    s.sharpe = static_cast<float>(mls / ls_sd * std::sqrt(static_cast<double>(kDaysPerYear) * kSegLen / hold));
  }
  if (vmk > 0.0)
    s.beta = static_cast<float>(cov / vmk);
  double g[kGroups];
  for (int k = 0; k < kGroups; ++k) {
    g[k] = sg[k] / n;
    s.grp[k] = static_cast<float>(g[k]);
  }
  s.mono = static_cast<float>(mono_of(g, kGroups));
  return s;
}

} // namespace factor::stat
