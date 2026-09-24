#pragma once

// =============================================================================
// Stat: 因子评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 无流式后端 —— 评估只在挖掘侧)
// =============================================================================
//   输入  因子平面 x [T][A] (SoA: float + 有效位) + 口径 Frame (见下) + H 组常驻标签 (每组一个持有期键 hold, 见下【持有期键】):
//           lv / sv  做多 / 做空吃单净收益 (features 层 LabelReturn, fp16 位, 与落盘同格式, 无精度损失)
//           m        标签有效位 (long / short 同一快照, 共用一张)
//           ry       预处理: 做多标签逐行截面 rank (r16), 与因子无关, 常驻期算一次 (prep_label)
//   输出  两级:
//     一级 Row[H][T]  每 (持有期, 时刻) 一行沿 A 轴归约的统计 (两后端各算, 对拍在此级)
//     二级 HoldStat   每持有期 沿 T 轴汇总成十几个标量 (summarize, 主机 double, 两后端共用同一份代码)
//
//   【口径 Frame】alpha 因子分两种坐标系, 由因子表达式的根算子决定 (Expr.hpp root_frame). 标签**统一超额语义**:
//     e = lv − mean_J lv (做空侧 sv − mean_J sv), 每 t 的共同分量 (市场水平 / 市场择时) 被扔掉, 评的都是"相对市场";
//     一套代码只在两处分叉:
//     CS  截面: 根是截面归一算子 (CsRank / CsNormRank / CsZ). 每 t 沿 A 轴 rank x (排序) → 20 组 = 截面分位组;
//         任一组空 → 行无效. 因子值 = 仓位 (每 t 居中 → 多空对冲, β 自然隔离; 超额对 rank 类统计是平移不变的).
//     TS  时序: 根是 TsRankRoll(d = kTsNormD = kTsNormDays 个交易日), 输出已是每票对自身滚动历史的分位 ∈ [0,1]
//         (整数天窗跨日 deseason). 不排序, 直接量化 r16 = round(x · kRankMax) (r16_quant) → 20 组 = 自身历史分位组;
//         评的是"个股处于自身高位时是否跑赢市场", β 由超额隔离, 样本间不再因共同分量相关;
//         组允许空 (那一刻没票在该分位), ls 空项取 0 (= long/flat).
//     两口径之后逐字共用: grp_of / IC / 超额 / ls / mkt / ac / 二级汇总.
//
//   【rank 口径】精确并列均秩 (排序, 不用桶近似 —— 原始因子重尾时一个离群点会把整截面压进同一桶, IC 归零):
//     r16 = round(pct · kRankMax) ∈ [0, kRankMax], 全整数公式 (r16_of), 两后端逐位一致; kRankNone = 无效.
//     行有效样本 n < 2 或全并列 (spread 为假) → 整行 kRankNone.
//     rank(x) 只看 x 的有效集, rank(y) 只看 y 的有效集 (与因子无关才能常驻); 两者的联合集 J 上算统计.
//     TS 的 r16_quant 走 double (float × 65534 在 double 内精确, 与 FMA 无关), 两后端逐位一致; x ∉ [0,1] 断言.
//
//   【一级 Row (每 h, t)】J = {a : rx ≠ None ∧ ry ≠ None}; e = 超额标签 (lv − mkt; 做空侧 sv − mean_J sv)
//     ic     Spearman = Pearson(r16_x, r16_y) over J. 五个和都是整数 (pearson_int), 两后端逐位一致.
//     grp[k] 第 k 组 (k = grp_of(r16_x), 等分 kGroups 组) 做多超额 e 的**和** (代数上 Σlv − cnt·mkt, 一遍 A 轴);
//            cnt[k] = 组样本数. 二级沿 t 池化成均值 (Σ和 / Σ计数): TS 每 t 各组成员数剧变, 必须池化;
//            CS 池化与"组均值再时间平均"只差按每 t 有效数加权.
//     ls     多空 = 最高组做多超额均值 + 最低组做空超额均值 —— 两边都是实盘可成交口径, 相对市场 (TS: 组空 → 该项 0, 即 long/flat).
//            无信号时 ≈ 0 (原值口径会 ≈ −市场往返成本).
//     mkt    J 上做多标签原值均值 (超额减它; 二级回归 beta 用).
//     ok     标签侧有效: 非段末尾部 (见下) ∧ ic 可算 (n ≥ 2, 两侧方差 > 0) ∧ (CS: 各组非空).
//     ac     rank-AC: Pearson(r16_x(t), r16_x(t−h)) over 两行都有效的资产 —— lag = h 才对应"一个持有期换多少仓".
//     ok_ac  t ≥ h ∧ n ≥ 2 ∧ 两侧方差 > 0. 与标签无关, 段末尾部也算.
//     段末尾部 (仅分钟档): LabelReturn 的 exit 越过连续竞价末秒 (14:57, 段末 kCloseAuction 分钟) 就"持有到收盘",
//       持有期缩短 → 掩掉 t_seg ≥ kSegLen − h − kCloseAuction 的行 (tail_masked). 日级档全段 exit 同一时刻, 无尾部.
//     ok / ok_ac 为假时对应字段全 0 (与 Contract 的 valid=false ⇒ v=0 同约).
//
//   【持有期键 hold】int, 与 features 层 LabelReturn 的列名同源 (lb_<side>_<name>_<amt>w):
//     分钟档 <n>m   hold = n (1 ≤ n, n + kCloseAuction < kSegLen), 持仓 n 分钟
//     收盘档 close  hold = kHoldClose, 持有到当日收盘
//     开盘档 t<N>   hold = hold_open(N), T+N 日开盘平仓
//     折算 / lag / 年化用的有效分钟 h = hold_minutes(hold): 分钟档 = n; close 与 T+1 = kSegLen (同日各行共一个 exit →
//     每日一个独立样本); T+N = N · kSegLen (相邻 N 日的窗口重叠).
//
//   【二级 HoldStat (每 hold)】只用 ok 行 (n) / ok_ac 行 (n_ac):
//     ic_mean / ic_std (ddof=1) / icir = mean/std / ic_pos = IC>0 占比 / ic_skew, ic_kurt (总体矩, 超额峰度)
//     ic_t = icir · √(n/h): 分钟行的标签持有期重叠 (相邻 h 行是同一段收益), 有效样本按 n/h 折算, 不然 t 虚高 √h 倍
//     ls_mean / ls_t (同上折算) / ls_pos = ls>0 占比 / sharpe = mean/std · √(kDaysPerYear · kSegLen / h) (按持有期为一期年化)
//     beta   ls 对 mkt 的 OLS 斜率 (alpha 略: beta ≈ 0 时 alpha ≈ ls_mean)
//     grp[k] 各组沿 t 池化均值 (Σ grp / Σ cnt; 全程无样本 → 0); mono = Spearman(组号, grp[k])
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
#include <string>
#include <string_view>

namespace factor::stat {

// ---- 常量 ----
inline constexpr int kGroups = 20;                     // 分组数 (等分 pct rank)
inline constexpr int kMaxHold = 12;                    // 同时评估的持有期数上限 (amt × hold 展平后的组数)
inline constexpr int kMaxA = 5120;                     // 资产轴上限 (GPU 一行一 block 的片上排序容量)
inline constexpr int kCloseAuction = 3;                // 段末收盘集合竞价分钟数 (14:57–15:00), 标签 exit 不能越过
inline constexpr int kDaysPerYear = 242;               // 年化用交易日数
inline constexpr uint16_t kRankNone = 0xFFFF;          // r16 无效
inline constexpr int kRankMax = 65534;                 // r16 = round(pct · kRankMax) ∈ [0, kRankMax]
inline constexpr unsigned kNoKey = 0xFFFFFFFFu;        // 排序键的"无效"哨兵 (有限 float 的保序键 ≤ 0xFF800000, 不冲突)
inline constexpr int kTsNormDays = 5;                  // TS 口径根 TsRankRoll 的窗长 (整交易日, 跨日 deseason)
inline constexpr int kTsNormD = kTsNormDays * kSegLen; // = 1275 分钟 (== Expr.hpp kMaxD, 在 Expr.hpp static_assert 对账)

// ---- 口径 (见文件头【口径 Frame】) ----
enum class Frame : uint8_t { CS,
                             TS };
inline constexpr const char *frame_name(Frame f) { return f == Frame::CS ? "CS" : "TS"; }

// ---- 持有期键 (见文件头【持有期键】) ----
inline constexpr int kHoldDayBase = 1000;                          // ≥ 此值为日级档
inline constexpr int kHoldClose = kHoldDayBase;                    // 持有到当日收盘
inline constexpr int kHoldOpenMax = 30;                            // T+N 的 N 上限 (键值域 / 断言用)
inline constexpr int hold_open(int n) { return kHoldDayBase + n; } // T+n 开盘平仓
inline constexpr bool hold_intraday(int hold) { return hold < kHoldDayBase; }
// 折算用有效分钟: 分钟档 = n; close / T+1 = 一段; T+N = N 段
inline constexpr int hold_minutes(int hold) {
  if (hold_intraday(hold))
    return hold;
  const int n = hold - kHoldDayBase;
  return kSegLen * (n < 1 ? 1 : n);
}
// 列名 token → 键: "<n>m" / "close" / "t<N>" (LabelReturn LABEL_GROUPS 的 name); 不识别 → 断言
inline int hold_from_name(std::string_view name) {
  assert(!name.empty());
  if (name == "close")
    return kHoldClose;
  const bool is_open = name.front() == 't';
  const std::string_view digits = is_open ? name.substr(1) : name.substr(0, name.size() - 1);
  assert(!digits.empty() && (is_open || name.back() == 'm') && "持有期 token 不合 <n>m / close / t<N>");
  int n = 0;
  for (const char c : digits) {
    assert(c >= '0' && c <= '9' && "持有期 token 数字部分非数字");
    n = n * 10 + (c - '0');
  }
  return is_open ? hold_open(n) : n;
}
// 键 → 显示名: "5m" / "close" / "T+3"
inline std::string hold_name(int hold) {
  if (hold_intraday(hold))
    return std::to_string(hold) + "m";
  if (hold == kHoldClose)
    return "close";
  return "T+" + std::to_string(hold - kHoldDayBase);
}

// ---- 参数: 持有期键列表. 每个 h 对应一组标签 ----
struct Holds {
  int n = 0;
  int h[kMaxHold] = {};
};

inline void assert_holds(const Holds &hd) {
  assert(hd.n >= 1 && hd.n <= kMaxHold);
  for (int i = 0; i < hd.n; ++i) {
    const int h = hd.h[i];
    if (hold_intraday(h))
      assert(h >= 1 && h + kCloseAuction < kSegLen && "分钟档持有期须 ≥ 1 且尾部掩码不能吞掉整段");
    else
      assert(h - kHoldDayBase >= 0 && h - kHoldDayBase <= kHoldOpenMax && "日级档键越界 (close = kHoldClose, T+N = hold_open(N))");
  }
}

// ---- 一级输出: 每 (持有期, 时刻) 一行; 布局 rows[h_idx * T + t] ----
struct Row {
  float ic = 0.f, mkt = 0.f, ls = 0.f, ac = 0.f;
  float grp[kGroups] = {};    // 组内做多标签 e 的和 (CS 原值 / TS 超额)
  uint16_t cnt[kGroups] = {}; // 组样本数 (A ≤ kMaxA < 65536)
  uint8_t ok = 0, ok_ac = 0;
};

// ---- 二级输出: 每持有期一份 ----
struct HoldStat {
  int hold = 0, n = 0, n_ac = 0;
  float ic_mean = 0.f, ic_std = 0.f, icir = 0.f, ic_t = 0.f, ic_pos = 0.f, ic_skew = 0.f, ic_kurt = 0.f;
  float ls_mean = 0.f, ls_t = 0.f, ls_pos = 0.f, sharpe = 0.f, beta = 0.f, mono = 0.f, rank_ac = 0.f;
  float grp[kGroups] = {}; // 池化组均值
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

// TS 口径: 分位 x ∈ [0,1] 直接量化 r16 = round(x · kRankMax). double 内 float × 65534 精确, 与 FMA / 求值序无关
inline uint16_t r16_quant(float x) {
  assert(x >= 0.f && x <= 1.f && "TS 口径的因子值须是分位 ∈ [0,1] (根 TsRankRoll)");
  return static_cast<uint16_t>(static_cast<double>(x) * kRankMax + 0.5);
}

// 组号: r16 等分 kGroups 组
inline int grp_of(unsigned r16) {
  const int g = static_cast<int>(r16 * kGroups / (kRankMax + 1));
  return g < kGroups - 1 ? g : kGroups - 1;
}

// 段末尾部 (分钟档标签持有到收盘, 持有期缩短) → 掩掉; 日级档无尾部
inline bool tail_masked(int t, int hold) { return hold_intraday(hold) && t % kSegLen >= kSegLen - hold - kCloseAuction; }

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

// 一行标签侧的收尾 (两后端逐字一致; GPU 侧有 __device__ 版): J 上整数五和 → ic; 组和 / 组计数 → grp / cnt / ls; mkt.
//   smk / ssv = J 上 Σ lv / Σ sv; gs[k] / gc[k] = 组 k 的 Σ lv / 计数; s0 = 组 0 的 Σ sv. 不 ok → r 标签侧字段保持 0
inline void finish_row(Frame f, unsigned long long n, unsigned long long sx, unsigned long long sy, unsigned long long sxx,
                       unsigned long long syy, unsigned long long sxy, double smk, double ssv, const double *gs, const int *gc, double s0,
                       Row &r) {
  bool icok = false;
  const float ic = pearson_int(n, sx, sy, sxx, syy, sxy, icok);
  bool gok = true;
  if (f == Frame::CS)
    for (int k = 0; k < kGroups; ++k)
      gok = gok && gc[k] >= 1;
  if (!(icok && gok))
    return;
  const double mkt = smk / static_cast<double>(n), mkt_s = ssv / static_cast<double>(n);
  r.ok = 1;
  r.ic = ic;
  r.mkt = static_cast<float>(mkt);
  for (int k = 0; k < kGroups; ++k) { // 超额和: Σ(lv − mkt) = Σlv − cnt·mkt
    r.cnt[k] = static_cast<uint16_t>(gc[k]);
    r.grp[k] = static_cast<float>(gs[k] - gc[k] * mkt);
  }
  const int kt = kGroups - 1;
  const double top = gc[kt] ? (gs[kt] - gc[kt] * mkt) / gc[kt] : 0.0;
  const double bot = gc[0] ? (s0 - gc[0] * mkt_s) / gc[0] : 0.0;
  r.ls = static_cast<float>(top + bot);
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

// rows = 该持有期的 T 行 (rows + h_idx * T); hold = 持有期键
inline HoldStat summarize(const Row *r, int T, int hold) {
  assert(T >= 1 && hold >= 1);
  const int h = hold_minutes(hold); // 折算 / 年化用有效分钟
  HoldStat s;
  s.hold = hold;
  double sic = 0.0, sls = 0.0, smk = 0.0, sac = 0.0, sg[kGroups] = {};
  long long sc[kGroups] = {};
  int n = 0, npos = 0, nlpos = 0, nac = 0;
  for (int t = 0; t < T; ++t) {
    const Row &w = r[t];
    if (w.ok) {
      ++n;
      npos += w.ic > 0.f;
      nlpos += w.ls > 0.f;
      sic += w.ic;
      sls += w.ls;
      smk += w.mkt;
      for (int k = 0; k < kGroups; ++k) {
        sg[k] += w.grp[k];
        sc[k] += w.cnt[k];
      }
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
  const double n_eff = static_cast<double>(n) / h; // 重叠持有期折算
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
  s.ls_pos = static_cast<float>(static_cast<double>(nlpos) / n);
  if (ls_sd > 0.0) {
    s.ls_t = static_cast<float>(mls / ls_sd * std::sqrt(n_eff));
    s.sharpe = static_cast<float>(mls / ls_sd * std::sqrt(static_cast<double>(kDaysPerYear) * kSegLen / h));
  }
  if (vmk > 0.0)
    s.beta = static_cast<float>(cov / vmk);
  double g[kGroups];
  for (int k = 0; k < kGroups; ++k) {
    g[k] = sc[k] > 0 ? sg[k] / static_cast<double>(sc[k]) : 0.0; // 沿 t 池化
    s.grp[k] = static_cast<float>(g[k]);
  }
  s.mono = static_cast<float>(mono_of(g, kGroups));
  return s;
}

} // namespace factor::stat
