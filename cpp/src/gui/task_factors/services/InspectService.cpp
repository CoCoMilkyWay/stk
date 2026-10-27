// InspectService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/InspectService.hpp"

#include "factor/Stat/Cpu.hpp"
#include "gui/task_factors/services/FactorsEngine.hpp" // eval_job (只 CPU) / Probes / LinesP / pool_line
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <vector>

namespace GUI::Factors {

namespace {

using factor::stat::kLegs;
using factor::stat::kLong;
using factor::stat::kShort;

// 尾部阈值 (r16): 多腿尾 = r16 ≥ thr[kLong], 空腿尾 = r16 < thr[kShort], 尾占整截面 q = kTailOfGroup / kGroups. 与 grp_of 同一条边界公式
// (r16 · kGroups / (kRankMax + 1) 取整): q = 1/kGroups 时两条阈值正好是 G20 / G1 的组边界
struct TailThr {
  int v[kLegs][kTails] = {};
};
TailThr tail_thresholds() {
  TailThr o;
  for (int j = 0; j < kTails; ++j) {
    const double q = static_cast<double>(kTailOfGroup[j]) / factor::stat::kGroups, span = factor::stat::kRankMax + 1;
    o.v[kLong][j] = static_cast<int>(std::ceil((1.0 - q) * span));
    o.v[kShort][j] = static_cast<int>(std::ceil(q * span));
    assert(o.v[kShort][j] >= 1 && o.v[kLong][j] <= factor::stat::kRankMax && o.v[kShort][j] < o.v[kLong][j]);
  }
  return o;
}
const TailThr kTailThr = tail_thresholds();
inline bool in_tail(int l, int r16, int thr) { return l == kLong ? r16 >= thr : r16 < thr; }
// 腿 l 尾部的分层侧 / 做多视角符号 = 所在端组的 grp_leg / grp_sign (与 Row::grp 同开关)
constexpr int tail_leg(int l) { return factor::stat::grp_leg(factor::stat::leg_group(factor::stat::kFwd, l)); }
constexpr double tail_sign(int l) { return factor::stat::grp_sign(factor::stat::leg_group(factor::stat::kFwd, l)); }
// 第 j 级尾部的腿码 (与契约 LegCode 同值, 给 cost_rows): 多腿尾 kLcLong / 空腿尾 kLcShort / 其余有秩 kLcMid / 无秩 kLcVoid
uint8_t tail_code(uint16_t r16, int j) {
  if (r16 == factor::stat::kRankNone)
    return factor::stat::kLcVoid;
  return in_tail(kLong, r16, kTailThr.v[kLong][j])     ? factor::stat::kLcLong
         : in_tail(kShort, r16, kTailThr.v[kShort][j]) ? factor::stat::kLcShort
                                                       : factor::stat::kLcMid;
}

// 一行的尾部分层 (TailExt 收尾; 只在 Row::ok 行有值, 其余全 0)
struct TailRow {
  float sum[kLegs][kTails] = {};    // 两腿尾部分层侧的做多视角超额持仓收益 ± Σe / max(cnt, cap_j) (cap_j = 满仓 · kTailOfGroup[j]; 与 Row::grp 同口径)
  uint16_t cnt[kLegs][kTails] = {}; // 两腿尾部分层侧真持有只数 (与 Row::cnt 同口径)
  float ls[kTails] = {};            // 尾部多空 = ls_of(两腿尾部, Row::mkt, cap_j) (与 ls_row 同式; 尾空 → 该腿 0)
  float ls_abs[kTails] = {};        // 同上绝对 (基准 0: 两腿本侧原值持仓收益平均)
};
// 净值线 i (InspectResult::nav 线序, 见 kNavLines) 在行 t 这一批的收益, 全取 Row / TailRow 现成值 (不 ok → 0): 分层超额 = grp / 尾部 sum;
//   分层绝对 = 超额 + 只数 · 分层侧基准 / 持仓分母 (组 cap, 尾 j cap · kTailOfGroup[j]; 做多视角同超额);
//   LS 超额 0 = ls_row(kFwd), 1 + j = TailRow::ls[j]; LS 绝对 0 = 正向两腿 leg_abs 平均, 1 + j = TailRow::ls_abs[j]
double nav_value(const factor::stat::Row &r, const TailRow &tr, int i, double cap) {
  if (!r.ok)
    return 0.0;
  if (i >= 2 * kNavLayers) {
    const bool absolute = i < 2 * kNavLayers + kNavLs;
    const int j = i - 2 * kNavLayers - (absolute ? 0 : kNavLs);
    using factor::stat::kFwd;
    if (absolute)
      return j == 0 ? 0.5 * (leg_abs(r, kFwd, kLong, cap) + leg_abs(r, kFwd, kShort, cap)) : tr.ls_abs[j - 1];
    return j == 0 ? factor::stat::ls_row(r, kFwd) : tr.ls[j - 1];
  }
  const bool absolute = i < kNavLayers;
  const int k = absolute ? i : i - kNavLayers;
  if (k < factor::stat::kGroups)
    return absolute ? group_abs(r, k, cap) : r.grp[k];
  const int l = (k - factor::stat::kGroups) / kTails, j = (k - factor::stat::kGroups) % kTails;
  const double sg = tail_sign(l);
  return absolute ? sg * factor::stat::book_abs(sg * tr.sum[l][j], tr.cnt[l][j], r.mkt[tail_leg(l)], cap * kTailOfGroup[j]) : tr.sum[l][j];
}
// 尾部分层 (TailRow, 见头文件) = Stat label_row 的逐格扩展 (Host.hpp Ext): 与标签侧同一遍 A 轴, J 同集 (x ∧ ry 有效);
// 尾 (l, j) 是一个组合, 成员 / 收益按契约【仓位感知】(同一尾部沿交接链): ls 腿只收本侧 l 真持有的格, 分层只收分层侧 tail_leg(l) 真持有的格
struct TailExt {
  const int (*thr)[kTails];
  double cap;                                   // 满仓 (Labels::cap); 尾 j 的满仓 = cap · kTailOfGroup[j]
  factor::stat::LegSum leg[kTails][kLegs] = {}; // 尾部 (本侧真持有只数, 本侧收益和): ls 用
  double slay[kLegs][kTails] = {};              // 尾部分层侧真持有的 Σ 收益: 分层用
  int nlay[kLegs][kTails] = {};                 // 同上只数
  void cell(const factor::cpu::stat::RxCell &cl) {
    const int x = *cl.code;
    for (int l = 0; l < kLegs; ++l)
      for (int j = 0; j < kTails; ++j) {
        const int th = thr[l][j];
        if (!in_tail(l, x, th))
          continue;
        const auto in = [l, th](uint16_t v) { return v != factor::stat::kRankNone && in_tail(l, v, th); };
        const int tl = tail_leg(l);
        const bool hs = cl.held(l, in), ht = tl == l ? hs : cl.held(tl, in);
        const bool cont = (hs || ht) && cl.continues(in);
        const float rs = hs ? cl.ret(l, cont) : 0.f;
        nlay[l][j] += ht;
        slay[l][j] += !ht ? 0.f : tl == l ? rs
                                          : cl.ret(tl, cont);
        leg[j][l].n += hs;
        leg[j][l].s += rs;
      }
  }
  // r = 本行 Row (基准 mkt[kLegs] 直接用); 只在 Row::ok 行有值, 其余全 0
  void finish(const factor::stat::Row &r, TailRow &o) const {
    o = TailRow{};
    if (!r.ok)
      return;
    const double mkt[kLegs] = {r.mkt[kLong], r.mkt[kShort]}, zero[kLegs] = {};
    for (int j = 0; j < kTails; ++j) {
      const double cj = cap * kTailOfGroup[j];
      for (int l = 0; l < kLegs; ++l) {
        o.cnt[l][j] = static_cast<uint16_t>(nlay[l][j]);
        o.sum[l][j] = static_cast<float>(tail_sign(l) * (slay[l][j] - nlay[l][j] * mkt[tail_leg(l)]) / factor::stat::book_den(nlay[l][j], cj));
      }
      o.ls[j] = static_cast<float>(factor::stat::ls_of(leg[j], mkt, cj));
      o.ls_abs[j] = static_cast<float>(factor::stat::ls_of(leg[j], zero, cj));
    }
  }
};
// 分层 / 尾部 / LS 净值线 [hd.n · kNavLines] (契约【净值】; 全部持有期一次 fold_book, stride = S → 日收益):
// 逐格扩展 TailExt, 行收尾出本行尾部 → 各线这一批的收益 nav_value
struct NavP : LinesP<kNavLines> {
  using Ext = TailExt;
  double cap = 0.0;
  Ext ext() const { return {kTailThr.v, cap}; }
  void open(const Ctx &c) {
    LinesP::open(c);
    cap = c.L.cap, stride = c.c.y.sh.seg;
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &e) {
    TailRow tr;
    e.finish(r, tr);
    for (int i = 0; i < kNavLines; ++i)
      at(hi, i, t) = static_cast<float>(nav_value(r, tr, i, cap));
  }
};
// 逐行 rIC 落地 [hd.n][rows_all] (非 ok / 预热行 = NaN): IC 时序
struct RicP : LandRowsP<float> {
  void open(const Ctx &c) { LandRowsP::open(c, std::numeric_limits<float>::quiet_NaN()); }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &) {
    if (r.ok)
      at(hi, t) = r.ric;
  }
};
// 各级尾部 LS 线两腿的换手量 acc[j][hi] (Stat 契约【换手成本】的 Δw, cost_rows 走一个固定段; 顶底线 = CostP 的段 0):
// 第 j 级尾部腿码 tail_code, 满仓 cap · kTailOfGroup[j] (与 TailRow 同口径); 块间续攒
struct TurnP : ProbeBase {
  static constexpr uint8_t need = kNeedRx;
  factor::stat::CostAcc acc[kTails][factor::stat::kMaxHold] = {};
  std::vector<uint8_t> lc; // 尾部腿码 [T][A], 逐级复用
  static double bytes(const Shape &sh, int) { return static_cast<double>(sh.n()); }
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    lc.resize(s.n());
    for (int j = 0; j < kTails; ++j) {
      c.ex.rows(s.T, [&](int t0, int t1, int) {
        for (size_t k = static_cast<size_t>(t0) * s.A, e = static_cast<size_t>(t1) * s.A; k < e; ++k)
          lc[k] = tail_code(v.rx[k], j);
      });
      const factor::stat::Cost flat{};
      factor::stat::CostAcc out[factor::stat::kMaxHold];
      factor::cpu::stat::cost_rows(c.ex, lc.data(), s, c.L, c.L.cap * kTailOfGroup[j], &flat, 1, *c.w, out);
      for (int hi = 0; hi < c.L.hd.n; ++hi)
        acc[j][hi].merge(out[hi]);
    }
  }
};
// 池内秩 rx 落地 [rows_all][A] (统计行; 右上资产轴 / K 线着色)
struct RxP : LandCellsP<uint16_t> {
  static constexpr uint8_t need = kNeedRx;
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    std::copy(v.rx + static_cast<size_t>(s.t_lo) * s.A, v.rx + s.n(), stat_at(s));
  }
};
// 不看池的秩 (g = xm) 落地 [rows_all][A] (统计行; 只给 K 线显示池外段, 评估不用)
struct RxAllP : LandCellsP<uint16_t> {
  static constexpr uint8_t need = kNeedMask | kNeedX;
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    const size_t o = static_cast<size_t>(s.t_lo) * s.A;
    const factor::stat::Plane x{v.x.v + o, v.x.m + o};
    factor::cpu::stat::rank_x(c.ex, x, x.m, v.f, factor::stat::Span{s.T - s.t_lo, s.A, s.g}, stat_at(s), *c.w);
  }
};
using InspectProbes = EvalProbes<RxP, RxAllP, RicP, NavP, TurnP>;

} // namespace

bool MakeInspectRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, const FactorRow &row, bool reload,
                        bool gpu, InspectRequest &req) {
  assert(row.error.empty() && !row.expr.empty() && "Inspect 只接有效 alpha 行");
  req = InspectRequest{};
  req.spec = InspectSpec{FactorRef{row.expr, row.frame}, row.key};
  req.reload = reload;
  return MakeFactorScope(data, std::move(ft), pool, gpu, req.scope);
}

InspectService::InspectService() { Start(); }
InspectService::~InspectService() { Stop(); }

bool InspectService::run(InspectRequest &req) {
  {
    std::lock_guard<std::mutex> lock(mutex); // 旧 result 不清: 页面继续整页画旧因子, 新结果发布时一次性覆盖 (不闪)
    message.clear();
  }
  publish();
  const bool ok = eval(req, flow());
  flow().drop_engines(); // 块跨请求留, 引擎不留
  return ok;
}

bool InspectService::eval(const InspectRequest &req, FactorFlow &F) {
  TraceN("InspectEvaluate");
  const FactorScope &sc = req.scope;
  const FactorRef &f = req.spec.f;
  if (req.reload)
    F.c.reset();

  // ---- 管线 eval_job (与 Run / Portfolio 同一份): 一个根, 探针 InspectProbes; 块钩子 = 池等权线 (随 Y 一代) ----
  //   Y = 全部持有期 + 全部金额段; X = 评估域内读过的全留 (只增), 缺的补读
  RootSet rs;
  rs.add(*sc.ft, f.expr, f.frame);
  JobOut o;
  o.job.spec.y = y_full(*sc.ft, sc.max_impact);
  o.job.retain = false;
  o.job.who = "Inspect";
  InspectProbes ps;
  ps.hd = o.job.spec.y.hd;
  ps.at<RxP>().dst = &pl_.rx;
  ps.at<RxAllP>().dst = &pl_.rx_all;
  ps.at<RicP>().dst = &pl_.ric;
  struct Hooks : HookBase {
    InspectService &W;
    FactorFlow &F;
    void block(const Block &c, size_t, double) {
      if (W.pool_gen_ == c.gen)
        return;
      W.pool_ = pool_line(F.ex, c.y), W.pool_gen_ = c.gen;
      const int seg = c.y.sh.seg, days = W.pool_.P / seg; // 逐行收益 (每批 1/h̄, 平仓行实现) 全期之和 = 算术总收益, 按日折年 (与期限结构 × span_per_year 同口径)
      assert(days >= 1 && W.pool_.P == days * seg && W.pool_.n == c.y.hd.n);
      for (int hi = 0; hi < W.pool_.n; ++hi) {
        const float *v = W.pool_.at(hi);
        double s = 0.0;
        for (int t = 0; t < W.pool_.P; ++t)
          s += v[t];
        W.pool_ann_[hi] = static_cast<float>(s / days * factor::stat::kDaysPerYear);
      }
    }
  };
  const char *backend = nullptr;
  const Rc rc = F.with_engine<InspectProbes>(sc.gpu, [&](auto &eng) {
    backend = std::remove_reference_t<decltype(eng)>::kName;
    return eval_job(eng, F, sc, rs.roots(), [&](int) -> InspectProbes & { return ps; }, o, Hooks{{}, *this, F});
  });

  // ---- 结果骨架 ----
  InspectResult res;
  res.key = inspect_key(sc.lv, sc.pool, req.spec.row, f.expr);
  res.row = req.spec.row;
  res.expr = f.expr;
  res.lv = sc.lv;
  res.frame = f.frame;
  const auto publish_result = [&] { // 新平面进结果, 旧结果的平面缓冲回收给 worker (下次原地写)
    std::swap(res.pl, pl_);
    {
      std::lock_guard<std::mutex> lock(mutex);
      std::swap(result, res);
      ++result_gen;
    }
    pl_ = std::move(res.pl);
    publish();
  };

  // ---- 评估域不成 / 叶值域越界 (与 Run 同一套 settle): 只发头部 + error ----
  if (!settle(rc, o.job.why, [&](const std::string &why) {
        res.error = why;
        pl_.clear(); // 无一级 / 无 rx
        publish_result();
      }))
    return false;
  if (rc != Rc::Ok)
    return true;
  const YPlanes &y = o.job.blocks.of(0).y;
  const StatScope scope = stat_scope(sc, o.job.dm, y, backend);
  res.dates = o.job.dm.dates;
  res.read_scope = o.job.dm.rs;
  res.pool_index = pool_;
  std::copy_n(pool_ann_, pool_.n, res.pool_ann);
  res.hd = y.hd;
  fill_eval(ps, scope, o.eval_ms(0), res);
  res.alpha_dist = std::move(ps.at<AlphaP>().d);
  std::copy_n(ps.at<IcPdfP>().pdf, res.hd.n, res.ic_agg);
  res.nav = std::move(ps.at<NavP>().out);
  for (int j = 0; j < kTails; ++j)
    std::copy_n(ps.at<TurnP>().acc[j], res.hd.n, res.ls_turn[j]);
  publish_result();
  return true;
}

} // namespace GUI::Factors
