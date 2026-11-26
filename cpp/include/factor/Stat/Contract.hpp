#pragma once

// =============================================================================
// Stat: 因子评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 无流式后端 —— 评估只在挖掘侧)
// =============================================================================
//   一条链, 每环一个头: rank (本头: r16) → pos (Pos.hpp: 仓位感知) → row (本头 Row, 行核 Host.hpp ⇄ Dev.cuh) ⇄ book (Book.hpp: 账集 / 换手)
//     → fold (Fold.hpp: Accum / Day / HoldStat) → roll (Roll.hpp). 持有期几何 / 协议表在 define/Label.hpp (Hold), 费率与成交侧在 define/Fee.hpp.
//
//   输入  因子平面 x [T][A] (SoA: float + 有效位) + 口径 Frame + 方向 Dir + H 组常驻标签 (每组一个持有期 HoldId):
//           lv       做多收益 (features LabelBar lb_<h>, 毛口径, 平仓顺延; fp16 位, 与落盘同格式). 冲击不进标签: 超额对每 t 去均值,
//                    成本进来只剩截面相对项 —— 消费端在换手账上扣. 分钟型 entry / exit 同为分钟 VWAP (行 m 的 exit = 行 m+h 的
//                    entry), 相邻行精确伸缩: 留仓成员逐期标签之和 = 连续持有收益, 换手的"留仓不付"才成立
//           nv       名义收益 (lb_nom_<h>, 只偏移型): 名义价点即平不顺延 —— 交接续持的估值 (Pos.hpp); 锚点型无此列 (空指针)
//           m        标签有效位
//           eo       建仓可成交码 kEoBuy | kEoSell (features lb_entry_ok; 各持有期共用一张)
//           ry       预处理: 做多标签逐行截面 rank (r16), 与因子无关, 常驻期算一次 (prep_label)
//
//   【只做多】一切组合 (组 / 尾部 / 多空两端 / 池篮子 / 单只) 都是做多账本: 收益取 lv (续持取名义值), 建仓要价点可买, 冲击取买入冲击.
//   【多空 LS】两端 End = 最高组 G20 (kTop) 与最低组 G1 (kBot), 各是一本做多账本; 多空 = ½ (G20 − G1) (资金 1 两本各半, 不做空).
//   【方向 Dir】方向是评估参数 (因子行的 sign), 不改写表达式, 在源头一次定: rank_x 出的 r16 已定向 (反向 = kRankMax − r16, 高 = 做多端),
//     其后 Row / Day / Accum / 尾部 / 换手全部只按"高端做多"读. rIC 精确变号; 组边界上 grp_of(kRankMax − r) 与 K−1−grp_of(r) 至多差一个 r16 取值.
//     Search 只正向评一遍, 由 oriented(Accum) 改读反向的二级 (定向打分只看 rIC, 精确).
//   输出  三级:
//     一级 Row[H][T]  每 (持有期, 时刻) 一行沿 A 轴归约的统计 (两后端各算, 对拍在此级); 只在要逐行时落
//     二级 Day[H][D]  每 (持有期, 统计日) 的 ok 行数 / Σ rIC / Σ 多空 (滚动窗 / 日净值 Sharpe / 分年的原料); 恒出
//     三级 HoldStat   每持有期沿 T 轴汇总 (Fold.hpp; 主机 double, 两后端共用同一份代码); 恒出
//
//   【口径 Frame】alpha 因子分两种坐标系, 由因子表达式的根算子决定 (Expr.hpp root_frame). 标签**统一超额语义**:
//     e = lv − mean_J lv, 每 t 的共同分量 (市场水平 / 市场择时) 被扔掉, 评的都是"相对市场";
//     CS  截面: 根是截面归一算子 (CsRank / CsNormRank / CsZ). 每 t 沿 A 轴 rank x (排序) → 20 组 = 截面分位组;
//     TS  时序: 根是 TsRankRoll(d = ts_norm_d(seg), 整数个交易日), 输出已是每资产对自身滚动历史的分位 ∈ [0,1];
//         不排序, 直接量化 r16 = round(x · kRankMax) (r16_quant) → 20 组 = 自身历史分位组.
//     两口径之后逐字共用: grp_of / rIC / 超额 / 持仓口径 / 尾部 / ls / mkt / rAC / 二级汇总. 组允许空 (空组收益 0).
//
//   【持仓口径】每组资金固定为 1, 满仓 cap = Labels::cap 只 (调用方给 n_full = 池内平均有效资产数 / kGroups, 与因子无关), 每只 1/cap:
//     真持有 n 只 → 持仓 min(n, cap) / cap, 超过 cap 满仓在 n 只里均分 → 组收益 = Σe / max(n, cap) (book_den); 未持有 → 0.
//
//   【rank 口径】精确并列均秩 (排序, 不用桶近似): r16 = round(pct · kRankMax) ∈ [0, kRankMax], 全整数公式 (r16_of), 两后端逐位一致;
//     kRankNone = 无效. 行有效样本 n < 2 或全并列 → 整行 kRankNone. rank(x) 只看 x 的有效集, rank(y) 只看 y 的有效集; 两者的联合集 J 上算统计.
//
//   【一级 Row (每 h, t)】J = {a : rx ≠ None ∧ ry ≠ None}; e = 超额标签 lv − mkt
//     ric    rank IC = Pearson(r16_x, r16_y) over J (整数五和 → pearson_int, 两后端逐位一致)
//     grp[k] 第 k 组 (k = grp_of(r16_x)) 的持仓收益 Σe / max(cnt, cap); cnt[k] = 组真持有只数 (Pos.hpp)
//     tail   两端组内更极端的一截 (【尾部】), 同口径 Σe / max(n, cap · tail_frac(j))
//     ls_row 多空 = ½ (grp[K−1] − grp[0]); mkt = J 上 lv 均值 (超额基准, 二级回归 beta 用)
//     ok     标签侧有效: 开仓行 (Hold::opens) ∧ ric 可算 (n ≥ 2, 两侧方差 > 0). rac = Pearson(r16_x(t), r16_x(t−h)) (lag = h 行), ok_rac 同理.
//     ok / ok_rac 为假时对应字段全 0. J / ric / mkt 与可成交无关; grp / cnt / tail 只收真持有的格.
//
//   【尾部】两端组 (顶 / 底) 内更极端的一截, 占该组 tail_frac(j) (= 整截面 2.5% / 1% / 0.25%), j 越大越极端; 阈值按 r16 定 (tail_thr),
//     与 grp_of 的组边界同口径. 尾部集合嵌套 (组 ⊃ 尾 0 ⊃ 尾 1 ⊃ 尾 2) = 成员层级 (Pos.hpp InRank), 一次链回走同出各级. 不进二级.
//
//   【分块流式】超大张量沿 T 切块 (Span): t0_abs 块首绝对行 (Hold::opens / nets 看绝对行); 统计行 [t_lo, T − t_back), 前带只给因子回看 /
//     rAC lag / 交接链 (≥ 回看 + hold_reach + kChainDays 日), 后带只给续持 (≥ hold_reach); 两者按整日对齐. 分块 ≡ 整张量, 跨块 Accum merge 续攒.
//   【视图】Plane (因子平面) · Batch (C 个同形候选; 交互 C = 1) · Labels (持有期 hd 与其标签 l[] 同处, 带满仓 cap) · Span.
//   【数值】整数和精确 (r16 ≤ 65534, A ≤ kMaxA); 浮点和两后端求和序不同, 走对拍容差. 全程 branchless 无 NaN.
//   【一份公式】契约公式标 FACTOR_HD (define/HostDev.hpp): CPU / GPU 两后端直接调用这一份, 后端只剩访存与并行.
//   【precise-math】依赖受控浮点, 编进 -fno-fast-math TU.
// =============================================================================

#include "define/Fee.hpp"
#include "define/Label.hpp"
#include "factor/Contract.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>

namespace factor::stat {

// ---- 常量 ----
inline constexpr int kGroups = 20;              // 分组数 (等分 pct rank)
inline constexpr int kMaxA = 8192;              // 资产轴上限 (GPU 一行一 block 的片上排序容量)
inline constexpr uint16_t kRankNone = 0xFFFF;   // r16 无效
inline constexpr int kRankMax = 65534;          // r16 = round(pct · kRankMax) ∈ [0, kRankMax]
inline constexpr unsigned kNoKey = 0xFFFFFFFFu; // 排序键的"无效"哨兵 (有限 float 的保序键 ≤ 0xFF800000, 不冲突)
static_assert(kLastAnchorRow + 5 == kSegMax, "末个可成交锚点 = 收盘集合竞价 (末 3 分钟) 前一分钟的起始");
// TS 口径根 TsRankRoll 的窗长 (行) = ≤ kMaxD 行的最大整交易日数
constexpr int ts_norm_d(int seg) { return kMaxD / seg * seg; }

// ---- 多空两端 / 方向 ----
enum End : int { kTop = 0,
                 kBot = 1 };
inline constexpr int kEnds = 2;
enum Dir : int { kFwd = 0,
                 kRev = 1 };
inline constexpr Dir dir_of(int sign) { return sign < 0 ? kRev : kFwd; } // 权重行的 sign (±1) → 方向
// 端 e 的组号 (G20 / G1)
FACTOR_HD inline constexpr int end_group(int e) { return e == kTop ? kGroups - 1 : 0; }
// 建仓可成交码 (features lb_entry_ok): 价点可买 | 可卖. 做多建仓要可买, 名义平仓要可卖
inline constexpr uint8_t kEoBuy = 1, kEoSell = 2;

// ---- 口径 ----
enum class Frame : uint8_t { CS,
                             TS };
inline constexpr const char *frame_name(Frame f) { return f == Frame::CS ? "CS" : "TS"; }

// ---- 持有期集合在层上 (几何见 define/Label.hpp Hold) ----
inline HoldSet holds_on(int seg) {
  HoldSet s = 0;
  for (int i = 0; i < kHoldCount; ++i)
    if (hold_ok(static_cast<HoldId>(i), geo_of(seg)))
      s |= hold_bit(static_cast<HoldId>(i));
  return s;
}
inline void assert_holds(const Holds &hd, int seg) {
  assert(hd.n >= 1 && hd.n <= kHoldCount);
  for (int i = 0; i < hd.n; ++i)
    assert(hold_ok(hd.k[i], geo_of(seg)) && "持有期在本层不合法 (分钟型须是行长整数倍且尾部不吞整段; 日层无 close)");
  (void)seg;
}
// 最长持有期的行数 (rAC 的 lag / 续持前看 / 前一批的链; 分块前后带都按它)
inline int hold_reach(const Holds &hd, int seg) {
  int m = 0;
  for (int i = 0; i < hd.n; ++i)
    m = std::max(m, hold_on(hd.k[i], geo_of(seg)).h);
  return m;
}

// ---- 输入: 一组标签 = [T][A] 平面的指针 (宿主或设备, 由后端定; 每个持有期一组, 与 Holds 同序) ----
struct Label {
  const uint16_t *lv = nullptr;        // 做多收益 fp16 位 (毛, 顺延)
  const uint16_t *nv = nullptr;        // 名义收益 fp16 位 (只偏移型; 锚点型空)
  const uint8_t *m = nullptr;          // 有效位
  const uint8_t *eo = nullptr;         // 建仓可成交码 (各持有期共用同一张)
  const uint16_t *ry = nullptr;        // 做多收益逐行 r16 (后端 prep_label 填; 与因子无关, 常驻期一次)
  FACTOR_HD Label at(size_t i) const { // 偏到第 i 格 (行首 = t·A)
    return {lv + i, nv ? nv + i : nullptr, m + i, eo + i, ry ? ry + i : nullptr};
  }
};
// 本格两侧标签 {lv, nv} (fp16 位 → float; nv 空 = 0)
template <class H2F>
FACTOR_HD inline void label_cell(const Label &lb, ptrdiff_t a, const H2F &h2f, float (&y)[2]) {
  y[0] = h2f(lb.lv[a]);
  y[1] = lb.nv ? h2f(lb.nv[a]) : 0.f;
}

// ---- 视图 ----
inline constexpr int kMaxBatch = 128; // 一批候选上限 (GPU 走 kernel 参数区: 128 × 16 B = 2 KB)
struct Plane {                        // 因子平面 [T][A]: 值 + 有效位
  const float *v = nullptr;
  const uint8_t *m = nullptr;
};
struct Batch { // C 个同形候选 (同池 g / 同口径 / 同方向 / 同标签集); 交互 = 1
  int C = 0;
  Plane x[kMaxBatch];
};
struct Labels { // l[i] ↔ hd.k[i]
  Holds hd;
  Label l[kHoldCount];
  double cap = 0.0; // 满仓一组只数 (【持仓口径】; 调用方给 n_full)
};
struct Span { // 块形 + 层几何 + 分块流式; 整张量 t0_abs = t_lo = t_back = 0
  int T = 0, A = 0;
  Geo g;
  int t0_abs = 0, t_lo = 0, t_back = 0;
  FACTOR_HD size_t n() const { return static_cast<size_t>(T) * A; }
  FACTOR_HD int t_hi() const { return T - t_back; }            // 统计行 [t_lo, t_hi)
  FACTOR_HD int days() const { return (t_hi() - t_lo) / g.S; } // 统计日数 (二级 Day 的条数)
};

inline void assert_span(const Span &s) {
  assert(s.T >= 1 && s.A >= 1 && s.A <= kMaxA && static_cast<long long>(s.T) * s.A < (1LL << 31));
  assert(is_seg(s.g.S) && geo_of(s.g.S).N == s.g.N && geo_of(s.g.S).M0 == s.g.M0 && "Span::g 须是 kGeos 之一");
  assert(s.t0_abs >= 0 && s.t_lo >= 0 && s.t_back >= 0 && s.t_lo < s.t_hi());
  assert(s.t_lo % s.g.S == 0 && (s.t_hi() - s.t_lo) % s.g.S == 0 && s.t0_abs % s.g.S == 0 && "统计行按整日对齐");
}
// prepped = ry 已由 prep_label 填 (评估入口); 假 = 只要标签 + 掩码 (宿主输入). seg = 因子所在层
inline void assert_labels(const Labels &L, int seg, bool prepped) {
  assert_holds(L.hd, seg);
  for (int i = 0; i < L.hd.n; ++i)
    assert(L.l[i].lv && (!hold_offset(L.hd.k[i]) || L.l[i].nv) && L.l[i].m && L.l[i].eo && (!prepped || L.l[i].ry) && "标签不全 / 未预处理 (prep_label)");
  assert(L.cap > 0.0 && "满仓 cap 未给 (Labels::cap = n_full)");
}

// ---- 尾部 (见文件头【尾部】) ----
inline constexpr int kTails = 3;
inline constexpr int kLevels = 1 + kTails; // 端组的成员层级: 0 = 整组, 1 + j = 尾部 j
// 尾部 j 占端组的比例 (j 越大越极端)
FACTOR_HD inline constexpr float tail_frac(int j) { return j == 0 ? 0.5f : j == 1 ? 0.2f
                                                                                  : 0.05f; }
// 层级 l 占端组的比例 (满仓 cap · level_frac)
FACTOR_HD inline constexpr float level_frac(int l) { return l == 0 ? 1.f : tail_frac(l - 1); }
// r16 阈值: 顶端 r ≥ ⌈(1 − q)(kRankMax + 1)⌉, 底端 r < ⌈q (kRankMax + 1)⌉, q = tail_frac / kGroups (与 grp_of 组边界同口径)
FACTOR_HD inline constexpr int tail_thr(int e, int j) {
  const double q = static_cast<double>(tail_frac(j)) / kGroups, span = kRankMax + 1.0, x = (e == kTop ? 1.0 - q : q) * span;
  const int i = static_cast<int>(x);
  return i < x ? i + 1 : i;
}
FACTOR_HD inline constexpr bool in_tail(int e, unsigned r, int j) { return e == kTop ? r >= static_cast<unsigned>(tail_thr(kTop, j)) : r < static_cast<unsigned>(tail_thr(kBot, j)); }

// ---- 一级输出: 每 (持有期, 时刻) 一行; 布局 rows[h_idx * T + t] ----
struct Row {
  float ric = 0.f, rac = 0.f;
  float mkt = 0.f;                     // J 上做多收益均值 (超额基准)
  float grp[kGroups] = {};             // 组的超额持仓收益 Σe / max(cnt, cap)
  uint16_t cnt[kGroups] = {};          // 组真持有只数 (A ≤ kMaxA < 65536)
  float tail[kEnds][kTails] = {};      // 两端尾部的超额持仓收益 Σe / max(n, cap · tail_frac)
  uint16_t tail_n[kEnds][kTails] = {}; // 两端尾部真持有只数
  uint8_t ok = 0, ok_rac = 0;
};
// 多空 = ½ (G20 − G1)
FACTOR_HD inline float ls_row(const Row &r) { return 0.5f * (r.grp[end_group(kTop)] - r.grp[end_group(kBot)]); }
// r16 改读成方向 d: 反向 = kRankMax − x (高 = 做多端), 无效不动
FACTOR_HD inline uint16_t oriented(uint16_t x, int d) { return x == kRankNone || d == kFwd ? x : static_cast<uint16_t>(kRankMax - x); }

// ---- 共享公式 (FACTOR_HD: 两后端同一份) ----

// float → 保序无符号键 (a < b ⟺ ord(a) < ord(b)); ±0 先归一为 +0
FACTOR_HD inline unsigned ord(float f) {
  const unsigned u = __builtin_bit_cast(unsigned, f + 0.f);
  return (u & 0x80000000u) ? ~u : (u | 0x80000000u);
}

// 并列均秩 r16: pct = (avg_rank − 1)/(n − 1), avg_rank = less + (eq + 1)/2, r16 = round(pct · kRankMax) (全整数)
FACTOR_HD inline uint16_t r16_of(int less, int eq, int n) {
  assert(n >= 2 && eq >= 1 && less >= 0 && less + eq <= n);
  const long long num = (2LL * less + eq - 1) * kRankMax;
  const long long den = 2LL * (n - 1);
  return static_cast<uint16_t>((2 * num + den) / (2 * den));
}

// TS 口径: 分位 x ∈ [0,1] 直接量化 r16 = round(x · kRankMax). double 内精确
FACTOR_HD inline uint16_t r16_quant(float x) {
  assert(x >= 0.f && x <= 1.f && "TS 口径的因子值须是分位 ∈ [0,1] (根 TsRankRoll)");
  return static_cast<uint16_t>(static_cast<double>(x) * kRankMax + 0.5);
}

// 组号: r16 等分 kGroups 组
FACTOR_HD inline int grp_of(unsigned r16) {
  const int g = static_cast<int>(r16 * kGroups / (kRankMax + 1));
  return g < kGroups - 1 ? g : kGroups - 1;
}

// 整数五和 + 计数 (rIC / rAC 的 Pearson 原料)
enum : int { kSn,
             kSx,
             kSy,
             kSxx,
             kSyy,
             kSxy,
             kSums };
FACTOR_HD inline void pair_add(unsigned long long *s, unsigned x, unsigned y, bool ok) {
  s[kSn] += ok;
  s[kSx] += ok ? x : 0u;
  s[kSy] += ok ? y : 0u;
  s[kSxx] += ok ? x * x : 0u;
  s[kSyy] += ok ? y * y : 0u;
  s[kSxy] += ok ? x * y : 0u;
}

// 整数五和的 Pearson: 方差 / 协方差全在 64 位整数内精确, 只有最后一步除法/开方是浮点 (两后端逐位一致). ok = n ≥ 2 ∧ vx > 0 ∧ vy > 0
FACTOR_HD inline float pearson_int(const unsigned long long *s, bool &ok) {
  const unsigned long long n = s[kSn];
  if (n < 2) {
    ok = false;
    return 0.f;
  }
  const long long vx = static_cast<long long>(n * s[kSxx]) - static_cast<long long>(s[kSx] * s[kSx]);
  const long long vy = static_cast<long long>(n * s[kSyy]) - static_cast<long long>(s[kSy] * s[kSy]);
  const long long cxy = static_cast<long long>(n * s[kSxy]) - static_cast<long long>(s[kSx] * s[kSy]);
  ok = vx > 0 && vy > 0;
  return ok ? static_cast<float>(static_cast<double>(cxy) / std::sqrt(static_cast<double>(vx) * static_cast<double>(vy))) : 0.f;
}

// 持仓分母 = max(只数, cap)
FACTOR_HD inline double book_den(double n, double cap) { return n > cap ? n : cap; }
// 一本的超额持仓收益 = (Σlv − 只数 · 基准) / 分母; 空 → 0
FACTOR_HD inline double book_excess(double sum, double n, double base, double cap) { return (sum - n * base) / book_den(n, cap); }
// 超额持仓收益 → 绝对持仓收益
inline double book_abs(double excess, double n, double base, double cap) { return excess + n * base / book_den(n, cap); }

// 一行标签侧的收尾 (两后端同一份): J 上整数五和 s → ric; 组和 gs / gc + 尾部和 ts / tc → grp / cnt / tail / tail_n; 基准 → mkt.
//   sm = J 上 Σlv; cap = 满仓. 不 ok → r 标签侧字段保持 0
FACTOR_HD inline void finish_row(const unsigned long long *s, double sm, const double *gs, const int *gc, const double (*ts)[kTails], const int (*tc)[kTails],
                                 double cap, Row &r) {
  bool icok = false;
  const float ric = pearson_int(s, icok);
  if (!icok)
    return;
  const double mkt = sm / static_cast<double>(s[kSn]);
  r.ok = 1;
  r.ric = ric;
  r.mkt = static_cast<float>(mkt);
  for (int k = 0; k < kGroups; ++k) {
    r.cnt[k] = static_cast<uint16_t>(gc[k]);
    r.grp[k] = static_cast<float>(book_excess(gs[k], gc[k], mkt, cap));
  }
  for (int e = 0; e < kEnds; ++e)
    for (int j = 0; j < kTails; ++j) {
      r.tail_n[e][j] = static_cast<uint16_t>(tc[e][j]);
      r.tail[e][j] = static_cast<float>(book_excess(ts[e][j], tc[e][j], mkt, cap * tail_frac(j)));
    }
}

} // namespace factor::stat
