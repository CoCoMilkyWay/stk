// InspectService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/InspectService.hpp"

#include "factor/Stat/Cpu.hpp"
#include "gui/task_factors/services/FactorsEngine.hpp" // eval_job (只 CPU)
#include "gui/task_factors/services/FactorsLoad.hpp"   // fold_exit / pool_line / stat_scope
#include "gui/task_factors/services/FactorsProbe.hpp"  // Probes / group_abs
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
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

// 净值线 i (InspectResult::nav 线序, 见 kNavLines) 在行 t 这一批的收益, 全取 Row / TailRow 现成值 (不 ok → 0): 分层超额 = grp / 尾部 sum;
//   分层绝对 = 超额 + 只数 · mkt[kLong] / 持仓分母 (组 cap, 尾 j cap · kTailOfGroup[j]); LS 0 = Row::ls, 1 + j = TailRow::ls[j]
double nav_value(const factor::stat::Row &r, const TailRow &tr, int i, double cap) {
  if (!r.ok)
    return 0.0;
  if (i >= 2 * kNavLayers) {
    const int j = i - 2 * kNavLayers;
    return j == 0 ? r.ls : tr.ls[j - 1];
  }
  const bool absolute = i < kNavLayers;
  const int k = absolute ? i : i - kNavLayers;
  if (k < factor::stat::kGroups)
    return absolute ? group_abs(r, k, cap) : r.grp[k];
  const int l = (k - factor::stat::kGroups) / kTails, j = (k - factor::stat::kGroups) % kTails;
  return absolute ? factor::stat::book_abs(tr.sum[l][j], tr.cnt[l][j], r.mkt[kLong], cap * kTailOfGroup[j]) : tr.sum[l][j];
}
// 尾部分层 (TailRow, 见头文件) = Stat label_row 的逐格扩展 (Host.hpp Ext): 与标签侧同一遍 A 轴, J 同集 (x ∧ ry 有效)
struct TailExt {
  const int (*thr)[kTails];
  double cap;                                   // 满仓 (Labels::cap); 尾 j 的满仓 = cap · kTailOfGroup[j]
  factor::stat::LegSum leg[kTails][kLegs] = {}; // 尾部 (样本数, 本腿标签和): ls 用
  double slv[kLegs][kTails] = {};               // 尾部 Σlv: 分层用
  void cell(unsigned x, bool ok, const float *y) {
    for (int l = 0; l < kLegs; ++l)
      for (int j = 0; j < kTails; ++j) {
        const bool in = ok && in_tail(l, static_cast<int>(x), thr[l][j]);
        leg[j][l].n += in;
        leg[j][l].s += in ? y[l] : 0.f;
        slv[l][j] += in ? y[kLong] : 0.f;
      }
  }
  // r = 本行 Row (基准 mkt[kLegs] 直接用); 只在 Row::ok 行有值, 其余全 0
  void finish(const factor::stat::Row &r, TailRow &o) const {
    o = TailRow{};
    if (!r.ok)
      return;
    const double mkt[kLegs] = {r.mkt[kLong], r.mkt[kShort]};
    for (int j = 0; j < kTails; ++j) {
      const double cj = cap * kTailOfGroup[j];
      for (int l = 0; l < kLegs; ++l) {
        o.cnt[l][j] = static_cast<uint16_t>(leg[j][l].n);
        o.sum[l][j] = static_cast<float>((slv[l][j] - leg[j][l].n * mkt[kLong]) / factor::stat::book_den(leg[j][l].n, cj));
      }
      o.ls[j] = static_cast<float>(factor::stat::ls_of(leg[j], mkt, cj));
    }
  }
};
// 尾部分层落地 [hd.n][rows_all] (与 RowsP 同布局): 逐格扩展 TailExt, 行收尾 finish
struct TailP : ProbeBase {
  static constexpr uint8_t need = kNeedRows;
  using Ext = TailExt;
  std::vector<TailRow> *dst = nullptr;
  int T = 0, t0 = 0;
  double cap = 0.0;
  static double bytes(const Shape &sh, int H) { return sizeof(TailRow) * static_cast<double>(H) * sh.T; }
  Ext ext() const { return {kTailThr.v, cap}; }
  void open(const Ctx &c) {
    const factor::stat::Span s = c.s();
    T = c.c.rows_all(), t0 = s.t0_abs, cap = c.L.cap;
    land_open(*dst, static_cast<size_t>(c.L.hd.n) * T, s);
  }
  void row(int hi, int t, const factor::stat::Row &r, const Ext &e) { e.finish(r, (*dst)[static_cast<size_t>(hi) * T + t0 + t]); }
};
// 池内秩 rx 落地 [rows_all][A] (统计行; 右上资产轴 / K 线着色)
struct RxP : ProbeBase {
  static constexpr uint8_t need = kNeedRx;
  std::vector<uint16_t> *dst = nullptr;
  static double bytes(const Shape &sh, int) { return 2.0 * static_cast<double>(sh.n()); }
  void open(const Ctx &c) { land_open(*dst, static_cast<size_t>(c.c.rows_all()) * c.s().A, c.s()); }
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    std::copy(v.rx + static_cast<size_t>(s.t_lo) * s.A, v.rx + s.n(), dst->data() + static_cast<size_t>(s.t0_abs + s.t_lo) * s.A);
  }
};
// 不看池的秩 (g = xm) 落地 [rows_all][A] (统计行; 只给 K 线显示池外段, 评估不用)
struct RxAllP : ProbeBase {
  static constexpr uint8_t need = kNeedMask;
  std::vector<uint16_t> *dst = nullptr;
  static double bytes(const Shape &sh, int) { return 2.0 * static_cast<double>(sh.n()); }
  void open(const Ctx &c) { land_open(*dst, static_cast<size_t>(c.c.rows_all()) * c.s().A, c.s()); }
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    const size_t o = static_cast<size_t>(s.t_lo) * s.A;
    const factor::stat::Plane x{v.x.v + o, v.x.m + o};
    factor::cpu::stat::rank_x(c.ex, x, x.m, v.f, factor::stat::Span{s.T - s.t_lo, s.A, s.g}, dst->data() + static_cast<size_t>(s.t0_abs) * s.A + o, *c.w);
  }
};
// 分层累计净值 (净值引擎 fold_exit, 契约【净值】; 每持有期一遍, 吃落地的 Row / TailRow) → 日收益 nav[H] = [kNavLines][days]
struct NavP : ProbeBase {
  static constexpr uint8_t need = 0;
  const std::vector<factor::stat::Row> *rows = nullptr;
  const std::vector<TailRow> *tails = nullptr;
  std::vector<BookLines> nav;
  void finish(const Ctx &c) {
    const int H = c.L.hd.n, T = c.c.rows_all(), seg = c.c.y.sh.seg;
    assert(rows->size() == static_cast<size_t>(H) * T && tails->size() == rows->size());
    nav.resize(static_cast<size_t>(H));
    for (int hi = 0; hi < H; ++hi) {
      const factor::stat::Row *r = rows->data() + static_cast<size_t>(hi) * T;
      const TailRow *tr = tails->data() + static_cast<size_t>(hi) * T;
      fold_exit(c.ex, c.L.hd.h[hi], seg, T, kNavLines, seg, [&](int i, int t) { return nav_value(r[t], tr[t], i, c.L.cap); }, nav[static_cast<size_t>(hi)]);
    }
  }
};
using InspectProbes = Probes<HoldP, CostP, ValidP, RankP, AlphaP, RxP, RxAllP, RowsP, TailP, NavP>;

} // namespace

// 计算 flow 的状态 (worker 私有, 跨请求存活; 分级见头文件). 作用域块 c: Y 常驻, X = 作用域内读过的特征 (只增)
struct InspectService::Flow : FactorFlow {
  Engine<Cpu> E;         // 毛标签秩 (Stat 会话, 作用域内一次) + 活槽 (HostPool 常驻)
  InspectPlanes pl;      // 一级平面, 发布时与 result.pl 交换
  BookLines pool;        // 池等权线 (pool_line), 随 Y 一代
  uint64_t pool_gen = 0; // pool 算时的 c.gen
  explicit Flow(const std::atomic<bool> &cancel) : E(ex, cancel, 1) {}
};

bool MakeInspectRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int seg, const FactorRow &row,
                        bool reload, InspectRequest &req) {
  assert(row.error.empty() && !row.expr.empty() && "Inspect 只接有效 alpha 行");
  req = InspectRequest{};
  req.f.ft = std::move(ft);
  req.f.expr = row.expr;
  req.f.frame = row.frame;
  req.row = row.key;
  req.reload = reload;
  return MakeFactorScope(data, pool, seg, /*gpu=*/false, req.f.scope);
}

InspectService::InspectService() { Start(); }
InspectService::~InspectService() { Stop(); }

bool InspectService::run(InspectRequest &req) {
  {
    std::lock_guard<std::mutex> lock(mutex); // 旧 result 不清: 页面继续整页画旧因子, 新结果发布时一次性覆盖 (不闪)
    message.clear();
  }
  publish();
  if (!flow_)
    flow_ = std::make_unique<Flow>(cancel_);
  return eval(req, *flow_);
}

bool InspectService::eval(const InspectRequest &req, Flow &F) {
  TraceN("InspectEvaluate");
  const FeatureTable &ft = *req.f.ft;
  const FactorScope &sc = req.f.scope;
  if (req.reload)
    F.c.reset();

  // ---- 管线 eval_job (与 Run / Portfolio 同一份): 一棵树, 探针 InspectProbes ----
  //   Y = 全部持有期 + 全部金额段; X = 作用域内读过的全留 (只增), 缺的补读
  const YSpec ys{holds_of(keys_of(ft.y.labels)), amts_of(keys_of(ft.y.costs)), static_cast<float>(sc.max_impact)};
  const factor::expr::Expr e = parse_job(req.f);
  InspectProbes ps;
  ps.at<RxP>().dst = &F.pl.rx;
  ps.at<RxAllP>().dst = &F.pl.rx_all;
  ps.at<RowsP>().dst = &F.pl.rows;
  ps.at<TailP>().dst = &F.pl.tails;
  ps.at<NavP>().rows = &F.pl.rows;
  ps.at<NavP>().tails = &F.pl.tails;
  JobOut o;
  const Rc rc = eval_job(F.E, F, ft, sc, {&e}, &req.f.frame, ys, /*retain=*/false, "Inspect", [&](int) -> InspectProbes & { return ps; }, cancel_, *this, o, [] {}, [](int) {});
  if (rc == Rc::Empty) {
    std::lock_guard<std::mutex> lock(mutex);
    message = o.empty;
    return true;
  }
  if (rc == Rc::Cancel)
    return false;
  if (rc == Rc::Stale)
    return stale_out(F);
  const YPlanes &y = F.c.y;
  if (F.pool_gen != F.c.gen) {
    F.pool = pool_line(F.ex, y);
    F.pool_gen = F.c.gen;
  }

  // ---- 结果骨架 ----
  InspectResult res;
  res.key = req.key();
  res.row = req.row;
  res.expr = req.f.expr;
  res.seg = sc.seg;
  res.frame = req.f.frame;
  res.scope = stat_scope(sc, o.es, y, Engine<Cpu>::kName);
  res.dates = o.es.dates;
  res.read_scope = o.es.rs;
  res.pool_index = F.pool;
  res.hd = y.hd;
  const auto publish_result = [&] { // 新平面进结果, 旧结果的平面缓冲回收给 Flow (下次原地写)
    std::swap(res.pl, F.pl);
    {
      std::lock_guard<std::mutex> lock(mutex);
      std::swap(result, res);
      ++result_gen;
    }
    F.pl = std::move(res.pl);
    publish();
  };

  // ---- 叶值域越界 (与 Run 同一套 plan_forest): 只发头部 + error ----
  if (rc == Rc::Broken) {
    res.error = o.why[0];
    F.pl.clear(); // 无一级 / 无 rx
    publish_result();
    return true;
  }
  res.eval_ms = o.eval_ms(0);
  fill_eval(ps, res);
  res.alpha_dist = std::move(ps.at<AlphaP>().d);
  res.nav = std::move(ps.at<NavP>().nav);
  publish_result();
  return true;
}

} // namespace GUI::Factors
