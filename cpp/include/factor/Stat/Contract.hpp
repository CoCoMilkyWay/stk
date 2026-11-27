#pragma once

// =============================================================================
// Stat: 因子评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 无流式后端 —— 评估只在挖掘侧)
// =============================================================================
//   一条链, 每环一个头: rank (本头: r16) → pos (Pos.hpp: 仓位感知) → row (本头 Row, 行核 Host.hpp ⇄ Dev.cuh) ⇄ book (Book.hpp: 账集 / 换手)
//     → fold (Fold.hpp: Sum / Realize / HoldStat) → roll (Roll.hpp). 持有期几何 / 协议表在 define/Label.hpp (Hold), 费率与流水在 define/Fee.hpp.
//
//   输入  因子平面 x [T][A] (SoA: float + 有效位) + 评估口径 Mode (口径 Frame × 方向 Dir × 收益 Ret, 逐根) + H 组常驻标签 (每组一个持有期 HoldId):
//           lv       做多收益 (features LabelBar lb_<h>, 毛口径, 平仓顺延; fp16 位, 与落盘同格式). 冲击不进标签: 超额对每 t 去均值,
//                    成本进来只剩截面相对项 —— 消费端在换手账上扣. 分钟型 entry / exit 同为分钟 VWAP (行 m 的 exit = 行 m+h 的
//                    entry), 相邻行精确伸缩: 留仓成员逐期标签之和 = 连续持有收益, 换手的"留仓不付"才成立
//           nv       名义收益 (lb_nom_<h>, 只偏移型): 名义价点即平不顺延 —— 交接续持的估值 (Pos.hpp); 锚点型无此列 (空指针)
//           m        标签有效位
//           ry       预处理: 做多标签逐行截面 rank (r16), 与因子无关, 常驻期算一次 (prep_label)
//           mk       池基准 [T]: 逐行 池内全部有标签的格各自做多 (可买才建仓, 续持取名义值) 的等权收益 = 池均线的批收益 (Pos.hpp pool_mean),
//                    与因子无关, 装载时一次 (超额的基准)
//           cap      满仓 [T]: 逐行 标签有效格数 / kGroups (【持仓口径】), 与 mk 同时算
//         + 各持有期共用的 eo  建仓可成交码 kEoBuy | kEoSell (features lb_entry_ok)
//
//   【只做多】一切组合 (组 / 尾部 / 多空两端 / 池篮子 / 单只) 都是做多账本: 收益取 lv (续持取名义值), 建仓要价点可买, 冲击取买入冲击.
//   【多空 LS】两端 End = 最高组 G20 (kTop) 与最低组 G1 (kBot), 各是一本做多账本; 多空 = ½ (G20 − G1) (资金 1 两本各半, 不做空).
//   【方向 Dir】方向是评估参数 (因子行的 sign), 不改写表达式, 在源头一次定: rank_x 出的 r16 已定向 (反向 = kRankMax − r16, 高 = 做多端),
//     其后 Row / Sum / 尾部 / 换手全部只按"高端做多"读. rIC 精确变号; 组边界上 grp_of(kRankMax − r) 与 K−1−grp_of(r) 至多差一个 r16 取值.
//     Search 只正向评一遍, 由 oriented(逐日 Sum) 改读反向 (只用来定号; 写进 weights 后 Run 在源头按 sign 精确评).
//   输出  三级:
//     一级 Row[H][T]  每 (持有期, 时刻) 一行沿 A 轴归约的统计 (两后端各算, 对拍在此级); 只在要逐行时落
//     二级 Sum[H][D]  每 (持有期, 统计日) 一份 Row 的可合并和 (唯一累加单元); 恒出
//     三级 HoldStat   任一日段的 Sum 之和 finish (全期 / 分年 / 滚动窗 / 观察分段同一份; Fold.hpp, 主机 double)
//
//   【口径 Frame】因子分两种坐标系, 由因子表达式的根算子决定 (Expr.hpp root_frame):
//     CS  截面: 根是截面归一算子 (CsRank / CsNormRank / CsZ). 每 t 沿 A 轴 rank x (排序) → 20 组 = 截面分位组;
//     TS  时序: 根是 TsRankRoll(d = ts_norm_d(seg), 整数个交易日), 输出已是每资产对自身滚动历史的分位 ∈ [0,1];
//         不排序, 直接量化 r16 = round(x · kRankMax) (r16_quant) → 20 组 = 自身历史分位组.
//     两口径之后逐字共用: grp_of / rIC / 收益 / 持仓口径 / 尾部 / ls / mkt / rAC / 二级汇总. 组允许空 (空组收益 0).
//   【收益 Ret】组 / 尾部的持仓收益按因子的收益口径 (因子文件 type, 人定; Mode::r, 逐根给; 标签与之无关). 行核与 Row 只出绝对值,
//     口径只在折进二级 (Sum::add) 与逐行读法 (ret_as / grp_as / tail_as / ls_row) 选, 唯一换算式 ret_as:
//     Excess 超额: e = lv − mkt (mkt = 池基准 mk[t]), 每 t 的共同分量 (市场水平 / 市场择时) 被扔掉, 评的是"相对池";
//     Abs    绝对: e = lv, 留着市场分量, 评的是"能不能赚绝对收益" (择时).
//     rIC / rAC / mkt 与口径无关 (截面秩不受每 t 的平移影响; mkt 与因子无关, 各因子同一基准); 满仓组两口径只差同一个 mkt, LS / mono 近乎不变,
//     差别主要在 TS 口径 (组随时间不满).
//
//   【持仓口径】每组资金固定为 1, 满仓 cap = Label::cap[t] 只 (当行池内标签有效格数 / kGroups, 与因子无关), 每只 1/cap:
//     真持有 n 只 → 持仓 min(n, cap) / cap, 超过 cap 满仓在 n 只里均分 → 组收益 = Σe / max(n, cap) (book_den); 未持有 → 0.
//
//   【rank 口径】精确并列均秩 (排序, 不用桶近似): r16 = round(pct · kRankMax) ∈ [0, kRankMax], 全整数公式 (r16_of), 两后端逐位一致;
//     kRankNone = 无效. 行有效样本 n < 2 或全并列 → 整行 kRankNone. rank(x) 只看 x 的有效集, rank(y) 只看 y 的有效集; 两者的联合集 J 上算统计.
//
//   【一级 Row (每 h, t)】J = {a : rx ≠ None ∧ ry ≠ None}; e = 做多收益 (续持取名义值; 绝对口径)
//     ric    rank IC = Pearson(r16_x, r16_y) over J (整数五和 → pearson_int, 两后端逐位一致)
//     grp[k] 第 k 组 (k = grp_of(r16_x)) 的持仓收益 Σe / max(cnt, cap); cnt[k] = 组真持有只数 (Pos.hpp)
//     tail   两端组内更极端的一截 (【尾部】), 同式 Σe / max(n, cap · tail_frac(j))
//     ls_row 多空 = ½ (grp[K−1] − grp[0]) (按口径读); mkt = 池基准 mk[t] (超额基准, 二级回归 beta 用)
//     ok     标签侧有效: 开仓行 (Hold::opens) ∧ ric 可算 (n ≥ 2, 两侧方差 > 0). rac = Pearson(r16_x(t), r16_x(t−h)) (lag = h 行), ok_rac 同理.
//     ok / ok_rac 为假时对应字段全 0. J / ric 与可成交无关; grp / cnt / tail 只收真持有的格.
//
//   【尾部】两端组 (顶 / 底) 内更极端的一截, 占该组 tail_frac(j) (= 整截面 2.5% / 1% / 0.25%), j 越大越极端; 阈值按 r16 定 (tail_thr),
//     与 grp_of 的组边界同口径. 尾部集合嵌套 (组 ⊃ 尾 0 ⊃ 尾 1 ⊃ 尾 2) = 成员层级 (Pos.hpp InRank), 一次链回走同出各级.
//
//   【分块流式】超大张量沿 T 切块 (Span; 块首恒在段首, Hold 只看段内行): 统计行 [t_lo, T − t_back), 前带只给因子回看 /
//     rAC lag / 交接链 (≥ 回看 + hold_reach + kChainDays 日), 后带只给续持 / 平仓行冲击 (≥ hold_reach); 两者按整日对齐. 分块 ≡ 整张量 (逐日 Sum 按绝对日拼回).
//   【视图】Plane (因子平面) · Batch (C 个同形候选; 交互 C = 1) · Mode (评估口径, 逐根) ⇄ Labels (持有期 hd 与其标签 l[] + 共用的 eo 同处; 同块各根共用) · Span.
//     流程 (rank_x / accum / run / stat_eval) 收 Mode: 秩收口径与方向, 折进二级收收益口径, 行核不收口径.
//   【数值】整数和精确 (r16 ≤ 65534, A ≤ kMaxA); 浮点和两后端求和序不同, 走对拍容差. 全程 branchless 无 NaN.
//   【一份公式】契约公式标 FACTOR_HD (define/HostDev.hpp): CPU / GPU 两后端直接调用这一份, 后端只剩访存与并行.
//   【precise-math】依赖受控浮点, 编进 -fno-fast-math TU.
// =============================================================================

#include "define/Label.hpp"
#include "factor/Contract.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <vector>

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

// ---- 收益口径 (【收益 Ret】) ----
enum class Ret : uint8_t { Excess,
                           Abs };
inline constexpr const char *ret_token(Ret r) { return r == Ret::Excess ? "excess" : "absolute"; } // 因子文件 type / run.ret 的写法
inline constexpr const char *ret_name(Ret r) { return r == Ret::Excess ? "超额" : "绝对"; }        // 显示

// ---- 评估口径: 一个根怎么评 (口径 × 方向 × 收益口径; 与标签无关, 逐根给) ----
struct Mode {
  Frame f = Frame::CS;
  Dir d = kFwd;
  Ret r = Ret::Excess;
  bool operator==(const Mode &) const = default;
};

// ---- 持有期集合在层上 (几何见 define/Label.hpp Hold) ----
inline Holds holds_on(int seg) {
  Holds s;
  for (int i = 0; i < kHoldCount; ++i)
    if (hold_ok(static_cast<HoldId>(i), geo_of(seg)))
      s = s.with(static_cast<HoldId>(i));
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

// ---- 输入: 一个持有期的标签 = [T][A] 平面的指针 (宿主或设备, 由后端定); 建仓可成交码 eo 各持有期共用, 在 Labels ----
struct Label {
  const uint16_t *lv = nullptr;        // 做多收益 fp16 位 (毛, 顺延)
  const uint16_t *nv = nullptr;        // 名义收益 fp16 位 (只偏移型; 锚点型空)
  const uint8_t *m = nullptr;          // 有效位
  const uint16_t *ry = nullptr;        // 做多收益逐行 r16 (后端 prep_label 填; 与因子无关, 常驻期一次)
  const float *mk = nullptr;           // [T] 池基准 (逐行, 不随格偏; 装载时一次)
  const float *cap = nullptr;          // [T] 满仓只数 (逐行, 不随格偏; 与 mk 同时算)
  FACTOR_HD Label at(size_t i) const { // 格平面偏到第 i 格 (行首 = t·A); mk / cap 按行取, 不偏
    return {lv + i, nv ? nv + i : nullptr, m + i, ry ? ry + i : nullptr, mk, cap};
  }
};
// 一个持有期的宿主标签平面 (评估装载 / 选池原料带 / 对拍造数同一型): lv / nv (只偏移型) / m [T·A] + 逐行池属性 mk / cap [T] (Pos.hpp pool_mean)
struct LabelPlanes {
  std::vector<uint16_t> lv, nv;
  std::vector<uint8_t> m;
  std::vector<float> mk, cap;
  void assign(size_t T, size_t A, bool nom) {
    lv.assign(T * A, 0), m.assign(T * A, 0), mk.assign(T, 0.f), cap.assign(T, 0.f);
    nom ? nv.assign(T * A, 0) : nv.clear();
  }
  size_t A() const { return lv.size() / mk.size(); }
};
// 平面 p 自第 t0 行起的契约视图 (ry 是与 p 同形的整平面, 同偏; 空 = 未预处理)
inline Label label_of(const LabelPlanes &p, const uint16_t *ry = nullptr, size_t t0 = 0) {
  const size_t o = t0 * p.A();
  return {p.lv.data() + o, p.nv.empty() ? nullptr : p.nv.data() + o, p.m.data() + o, ry ? ry + o : nullptr, p.mk.data() + t0, p.cap.data() + t0};
}
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
struct Labels { // l[i] ↔ hd.k[i]; eo = 各持有期共用的建仓可成交码平面
  Holds hd;
  const uint8_t *eo = nullptr;
  Label l[kHoldCount];
};
struct Span { // 块形 + 层几何 + 分块流式; 整张量 t_lo = t_back = 0
  int T = 0, A = 0;
  Geo g;
  int t_lo = 0, t_back = 0;
  FACTOR_HD size_t n() const { return static_cast<size_t>(T) * A; }
  FACTOR_HD int t_hi() const { return T - t_back; }            // 统计行 [t_lo, t_hi)
  FACTOR_HD int days() const { return (t_hi() - t_lo) / g.S; } // 统计日数 (二级逐日 Sum 的条数)
};

inline void assert_span(const Span &s) {
  assert(s.T >= 1 && s.A >= 1 && s.A <= kMaxA && static_cast<long long>(s.T) * s.A < (1LL << 31));
  assert(is_seg(s.g.S) && geo_of(s.g.S).N == s.g.N && geo_of(s.g.S).M0 == s.g.M0 && "Span::g 须是 kGeos 之一");
  assert(s.t_lo >= 0 && s.t_back >= 0 && s.t_lo < s.t_hi());
  assert(s.T % s.g.S == 0 && s.t_lo % s.g.S == 0 && (s.t_hi() - s.t_lo) % s.g.S == 0 && "块与统计行按整日对齐");
}
// prepped = ry 已由 prep_label 填 (评估入口); 假 = 只要标签 + 掩码 (宿主输入). seg = 因子所在层
inline void assert_labels(const Labels &L, int seg, bool prepped) {
  assert_holds(L.hd, seg);
  assert(L.eo && "缺建仓可成交码");
  for (int i = 0; i < L.hd.n; ++i)
    assert(L.l[i].lv && (!hold_offset(L.hd.k[i]) || L.l[i].nv) && L.l[i].m && L.l[i].mk && L.l[i].cap && (!prepped || L.l[i].ry) && "标签不全 / 未预处理 (prep_label)");
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
  float mkt = 0.f;                     // 池基准 (超额基准; 本行 ok 才填)
  float cap = 0.f;                     // 满仓只数 (本行 ok 才填; 超额改读 ret_as 用)
  float grp[kGroups] = {};             // 组的持仓收益 Σe / max(cnt, cap) (绝对)
  uint16_t cnt[kGroups] = {};          // 组真持有只数 (A ≤ kMaxA < 65536)
  float tail[kEnds][kTails] = {};      // 两端尾部的持仓收益 Σe / max(n, cap · tail_frac) (绝对)
  uint16_t tail_n[kEnds][kTails] = {}; // 两端尾部真持有只数
  uint8_t ok = 0, ok_rac = 0;
};
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
// 一本 n 只 (满仓 cap) 的绝对持仓收益 v 按口径 r 读 (唯一换算式): 超额 = v − n · mkt / 分母, 绝对 = v
FACTOR_HD inline double ret_as(double v, double n, double mkt, double cap, Ret r) { return r == Ret::Abs ? v : v - n * mkt / book_den(n, cap); }
// 行 r 的第 k 组 / 端 e 尾部 j / 多空 ½ (G20 − G1) 按口径 ret 读 (不 ok 的行字段全 0, 读出 0)
FACTOR_HD inline double grp_as(const Row &r, int k, Ret ret) { return ret_as(r.grp[k], r.cnt[k], r.mkt, r.cap, ret); }
FACTOR_HD inline double tail_as(const Row &r, int e, int j, Ret ret) { return ret_as(r.tail[e][j], r.tail_n[e][j], r.mkt, r.cap * tail_frac(j), ret); }
FACTOR_HD inline double ls_row(const Row &r, Ret ret) { return 0.5 * (grp_as(r, end_group(kTop), ret) - grp_as(r, end_group(kBot), ret)); }

// 一行标签侧的收尾 (两后端同一份): J 上整数五和 s → ric; 组和 gs / gc + 尾部和 ts / tc → grp / cnt / tail / tail_n (绝对); 池基准 mkt / 满仓 cap 照录.
//   不 ok → r 标签侧字段保持 0
FACTOR_HD inline void finish_row(const unsigned long long *s, double mkt, const double *gs, const int *gc, const double (*ts)[kTails], const int (*tc)[kTails],
                                 double cap, Row &r) {
  bool icok = false;
  const float ric = pearson_int(s, icok);
  if (!icok)
    return;
  r.ok = 1;
  r.ric = ric;
  r.mkt = static_cast<float>(mkt);
  r.cap = static_cast<float>(cap);
  for (int k = 0; k < kGroups; ++k) {
    r.cnt[k] = static_cast<uint16_t>(gc[k]);
    r.grp[k] = static_cast<float>(gs[k] / book_den(gc[k], cap));
  }
  for (int e = 0; e < kEnds; ++e)
    for (int j = 0; j < kTails; ++j) {
      r.tail_n[e][j] = static_cast<uint16_t>(tc[e][j]);
      r.tail[e][j] = static_cast<float>(ts[e][j] / book_den(tc[e][j], cap * tail_frac(j)));
    }
}

} // namespace factor::stat
