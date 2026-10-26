// InspectService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/InspectService.hpp"

#include "factor/Stat/Cpu.hpp"
#include "gui/task_factors/services/FactorsEngine.hpp" // FactorFlow / factor_pass (只 CPU)
#include "gui/task_factors/services/FactorsLoad.hpp"   // fold_exit
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
  double e, n, c;
  if (k < factor::stat::kGroups)
    e = r.grp[k], n = r.cnt[k], c = cap;
  else {
    const int l = (k - factor::stat::kGroups) / kTails, j = (k - factor::stat::kGroups) % kTails;
    e = tr.sum[l][j], n = tr.cnt[l][j], c = cap * kTailOfGroup[j];
  }
  return absolute ? factor::stat::book_abs(e, n, r.mkt[kLong], c) : e;
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
// accum_rows 的 sink: 一级 Row 与尾部同布局落地 [i·T + t]
struct TailSink {
  factor::stat::Row *rows;
  TailRow *tails;
  int T;
  const int (*thr)[kTails];
  double cap;
  using Ext = TailExt;
  Ext ext() const { return {thr, cap}; }
  void row(int i, int t, const factor::stat::Row &r, const Ext &e) const {
    const size_t k = static_cast<size_t>(i) * T + t;
    rows[k] = r;
    e.finish(r, tails[k]);
  }
};

} // namespace

// 计算 flow 的状态 (worker 私有, 跨请求存活; 分级见头文件). 作用域块 c: Y 常驻, X = 作用域内读过的特征 (只增)
struct InspectService::Flow : FactorFlow {
  // R / C / L 常驻缓冲
  InspectPlanes pl;        // 一级平面, 发布时与 result.pl 交换
  std::vector<uint8_t> lc; // 腿码 [T][A] (C 用)
  using FactorFlow::FactorFlow;
};

bool MakeInspectRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int seg, const FactorRow &row,
                        bool reload, InspectRequest &req) {
  assert(row.error.empty() && !row.expr.empty() && "Inspect 只接有效 alpha 行");
  req = InspectRequest{};
  req.f.ft = std::move(ft);
  req.f.expr = row.expr;
  req.f.frame = row.frame;
  req.file = row.key;
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

bool InspectService::stale_out(Flow &F) {
  F.c.reset(); // 库换了, 缓存的一切不可信
  std::lock_guard<std::mutex> lock(mutex);
  message = kStaleMsg;
  return false;
}

bool InspectService::eval(const InspectRequest &req, Flow &F) {
  TraceN("InspectEvaluate");
  const FeatureTable &ft = *req.f.ft;
  const FactorScope &sc = req.f.scope;
  const auto cancelled = [&] { return cancel_.load(std::memory_order_relaxed); };
  if (req.reload)
    F.c.reset();

  // ---- S / Y / X / R: 单因子前缀 (factor_pass, 与 Portfolio 同一份) ----
  //   Y = 全部持有期 + 全部金额段 + 池等权指数 (每持有期一条); X = 作用域内读过的全留 (只增), 缺的补读
  //   另占: rx_all 2 + lc 1 (/ 格); Row / TailRow [H] (/ 行); 收尾: 评估 + 收尾
  const YSpec ys{holds_of(keys_of(ft.y.labels)), amts_of(keys_of(ft.y.costs)), static_cast<float>(sc.max_impact), IndexKind::Pool};
  const double row_extra = ys.hd.n * static_cast<double>(sizeof(factor::stat::Row) + sizeof(TailRow));
  FactorPass p;
  const Pass pr = factor_pass(F, req.f, PassSpec{ys, /*retain=*/false, "Inspect", 3.0, row_extra, /*tail=*/2}, cancel_, *this, F.pl.rx, p);
  if (pr == Pass::Empty) {
    std::lock_guard<std::mutex> lock(mutex);
    message = p.why;
    return true;
  }
  if (pr == Pass::Cancel)
    return false;
  if (pr == Pass::Stale)
    return stale_out(F);
  const YPlanes &y = F.c.y;
  const std::vector<misc::Ymd> &dates = p.es.dates;
  const size_t n = y.sh.n();
  const int H = y.hd.n, T = y.sh.T, A = static_cast<int>(p.es.rs.axis.size());

  // ---- 结果骨架 ----
  InspectResult res;
  res.key = req.key();
  res.file = req.file;
  res.expr = req.f.expr;
  res.seg = sc.seg;
  res.frame = req.f.frame;
  res.scope.pool = sc.pool;
  res.scope.start_date = dates.front();
  res.scope.end_date = dates.back();
  res.scope.days = y.sh.days, res.scope.T = T, res.scope.A = A;
  res.scope.backend = Engine<Cpu>::kName;
  res.scope.n_full = y.n_full;
  res.dates = dates;
  res.read_scope = p.es.rs;
  assert(y.index.n == H && y.index.P == T && "Pool 档: 每持有期一条池整体等权");
  res.pool_index = y.index;
  res.hd = y.hd;
  const auto publish_result = [&] { // 新平面进结果, 旧结果的平面缓冲回收给 Flow (下次原地写)
    res.scope.time = now_string();
    std::swap(res.pl, F.pl);
    {
      std::lock_guard<std::mutex> lock(mutex);
      std::swap(result, res);
      ++result_gen;
    }
    F.pl = std::move(res.pl);
    publish();
  };

  // ---- 叶值域越界 (与 FactorsService 同一套): 只发头部 + error ----
  if (pr == Pass::Broken) {
    res.error = p.why;
    F.pl.clear(); // 无一级 / 无 rx
    publish_result();
    return true;
  }
  res.eval_ms = p.eval_ms;

  // ---- R: 池内秩 rx (前缀已出) + 不看池的秩 (g = xm) → rx_all (只给 K 线显示池外段) ----
  factor::cpu::stat::StatSession &ss = F.E.stat_session();
  InspectPlanes &pl = F.pl;
  const size_t nrow = static_cast<size_t>(H) * T;
  pl.rows.resize(nrow);
  pl.tails.resize(nrow);
  pl.rx_all.resize(n);
  F.lc.resize(n);
  const factor::stat::Span &s = ss.s;
  const factor::stat::Plane x{p.root->v.data(), p.root->m.data()};
  factor::cpu::stat::rank_x(F.ex, x, x.m, req.f.frame, s, pl.rx_all.data(), ss.w);
  const uint16_t *rx = pl.rx.data();
  // ---- C: 腿码 (换手成本用) ----
  factor::cpu::stat::leg_codes(F.ex, rx, s, F.lc.data());
  if (cancelled())
    return false;

  // ---- L: 评估 (只毛): 一级 Row[H][T] + 一级累加; 同遍尾部分层 (TailExt, Inspect 专属: 只在 Row::ok 行, 两腿组内更极端的 kTailOfGroup 截) ----
  factor::stat::Accum acc[factor::stat::kMaxHold];
  factor::cpu::stat::accum_rows(F.ex, s, rx, ss.L, ss.w, acc, TailSink{pl.rows.data(), pl.tails.data(), T, kTailThr.v, ss.L.cap});
  publish(1);
  if (cancelled())
    return false;

  // ---- N: 分层累计净值 (净值引擎 fold_exit, 契约【净值】; 每持有期一遍, 吃 L 的 Row / TailRow) → 日收益 nav[H] = [kNavLines][days] ----
  res.nav.resize(static_cast<size_t>(H));
  for (int hi = 0; hi < H; ++hi) {
    const factor::stat::Row *r = pl.rows.data() + static_cast<size_t>(hi) * T;
    const TailRow *tr = pl.tails.data() + static_cast<size_t>(hi) * T;
    fold_exit(F.ex, y.hd.h[hi], y.sh.seg, T, kNavLines, y.sh.seg, [&](int i, int t) { return nav_value(r[t], tr[t], i, ss.L.cap); }, res.nav[static_cast<size_t>(hi)]);
    if (cancelled())
      return false;
  }

  // ---- 收尾 (FactorsLoad finish_root, 与 Run 同一份): valid% / rank% / 二级 HoldStat[H] / alpha 分布 / 换手成本 (契约【换手成本】) ----
  finish_root(F.ex, y, RootRaw{p.root->m.data(), rx, F.lc.data(), acc, p.alpha}, ss.w, res, res.alpha_dist);
  publish(1);
  if (cancelled())
    return false;
  publish_result();
  return true;
}

} // namespace GUI::Factors
