// InspectService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/InspectService.hpp"

#include "factor/EvalCpu.hpp"
#include "factor/Stat/Cpu.hpp"
#include "gui/task_factors/services/FactorsLoad.hpp"
#include "misc/exec.hpp"
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
void tail_thresholds(int (*thr)[kTails]) {
  for (int j = 0; j < kTails; ++j) {
    const double q = static_cast<double>(kTailOfGroup[j]) / factor::stat::kGroups, span = factor::stat::kRankMax + 1;
    thr[kLong][j] = static_cast<int>(std::ceil((1.0 - q) * span));
    thr[kShort][j] = static_cast<int>(std::ceil(q * span));
    assert(thr[kShort][j] >= 1 && thr[kLong][j] <= factor::stat::kRankMax && thr[kShort][j] < thr[kLong][j]);
  }
}
inline bool in_tail(int l, int r16, int thr) { return l == kLong ? r16 >= thr : r16 < thr; }

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

// 计算 flow 的状态 (worker 私有, 跨请求存活; 分级见头文件)
struct InspectService::Flow {
  misc::exec::ForkJoin ex;
  LibReaders lr; // 已建的库 (清单没变的复用)
  // S 作用域
  std::string scope_key; // 层 | 封顶 | EvalScope::key; 空 = 未装载
  std::vector<misc::Ymd> dates;
  Loaded L; // planes / feat_codes = 作用域内读过的特征 (只增; 每次评估前把当前 Dag::feats 换到前排); labels (毛) / costs / cs 常驻
  // Y 毛标签 [H]: ry = prep_label 常驻, lab.L 指向 L.labels + ry
  LabelsHost lab;
  // X / R / C / L 常驻缓冲
  factor::cpu::HostPool hp;
  std::vector<factor::check::Arena> nsc;
  factor::cpu::stat::Workspace w;
  InspectPlanes pl;        // 一级平面, 发布时与 result.pl 交换
  std::vector<uint8_t> lc; // 腿码 [T][A] (C 用)

  Flow() : ex(misc::exec::hw()) {}
  void reset_scope() {
    scope_key.clear();
    dates.clear();
    L = Loaded{};
    lab = LabelsHost{};
  }
};

bool MakeInspectRequest(const SharedData &data, const std::string &pool, int seg, const FactorRow &row, bool reload, InspectRequest &req) {
  assert(row.error.empty() && !row.expr.empty() && "Inspect 只接有效 alpha 行");
  req = InspectRequest{};
  req.spec = InspectSpec{row.key, row.expr, row.frame, reload};
  return MakeFactorScope(data, pool, seg, /*gpu=*/false, req.scope);
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
    flow_ = std::make_unique<Flow>();
  return evaluate(req, *flow_);
}

bool InspectService::evaluate(const InspectRequest &req, Flow &F) {
  TraceN("InspectEvaluate");
  const FeatureTable &ft = feats();
  const FactorScope &sc = req.scope;
  const InspectSpec &sp = req.spec;
  factor::expr::Expr e;
  {
    std::string err;
    assert(sc.seg == ft.seg && "请求层与字段表层不符");
    const bool ok = factor::expr::parse(sp.expr, ft.lookup(), ft.seg, e, err);
    assert(ok && "Factors 表的规范串必可再解析");
    (void)ok;
  }
  const factor::Dag d = factor::build(e);
  const int N = static_cast<int>(d.nodes.size()), root = d.root();
  const int nt = F.ex.threads();

  const auto publish_message = [&](std::string msg) {
    std::lock_guard<std::mutex> lock(mutex);
    message = std::move(msg);
  };
  const auto cancelled = [&] { return cancel_.load(std::memory_order_relaxed); };

  // ---- S: 作用域 (标签 / 成本 / 门控 + 特征缓存) ----
  EvalScope es;
  {
    std::string why;
    if (!eval_scope(ft, sc, d.feats, es, why)) {
      publish_message(why);
      return true;
    }
  }
  open_libs(sc, F.lr, &cancel_);
  const std::vector<misc::Ymd> &dates = es.dates;
  const std::string ckey = std::to_string(sc.seg) + "|cap" + std::to_string(sc.max_impact) + "|" + es.key();
  const int days = static_cast<int>(dates.size()), A = static_cast<int>(es.rs.axis.size());
  const double cells = static_cast<double>(days) * ft.seg * A;
  begin(Phase::Loading, static_cast<size_t>(days));
  const auto stale_out = [&] {
    if (F.lr.stale()) { // 库换了, 缓存的一切不可信
      F.reset_scope();
      publish_message("特征库判废 (池轴/字段表与当前不符), 需重算特征");
    }
    return false;
  };
  if (sp.reload || F.scope_key != ckey || F.dates != dates) {
    F.reset_scope();
    // 全部持有期 + 全部金额段 + 池等权指数
    const LoadSpec spec{d.feats, keys_of(ft.labels), keys_of(ft.costs), static_cast<float>(sc.max_impact), /*index=*/true};
    { // 宿主峰值: 装载平面 (cell_bytes) + 活槽 + rx / rx_all / lc + 标签秩 + Row / TailRow [H][T]
      const double H = static_cast<double>(spec.holds.size());
      const double per = cell_bytes(spec) + 5.0 * d.n_slots + 5.0 + 2.0 * H;
      assert_fits("Inspect", cells * per + H * days * ft.seg * static_cast<double>(sizeof(factor::stat::Row) + sizeof(TailRow)), days, A, ft.seg);
    }
    if (!load_planes(ft, es, F.lr, 0, es.days.size(), spec, F.L, F.ex, cancel_, *this))
      return stale_out();
    F.scope_key = ckey;
    F.dates = dates;
  } else {
    std::vector<std::string> missing;
    for (const std::string &c : d.feats)
      if (std::find(F.L.feat_codes.begin(), F.L.feat_codes.end(), c) == F.L.feat_codes.end())
        missing.push_back(c);
    if (missing.empty()) {
      publish(static_cast<size_t>(days));
    } else {
      const LoadSpec spec{missing}; // 只补读特征平面
      assert_fits("Inspect", cells * cell_bytes(spec), days, A, ft.seg);
      Loaded tmp;
      if (!load_planes(ft, es, F.lr, 0, es.days.size(), spec, tmp, F.ex, cancel_, *this))
        return stale_out();
      for (size_t i = 0; i < missing.size(); ++i) {
        F.L.feat_codes.push_back(missing[i]);
        F.L.planes.push_back(std::move(tmp.planes[i]));
      }
    }
  }
  Loaded &L = F.L;
  // 当前 Dag::feats 换到前排 (平面下标 = feats 下标, check_leaf_domains / plane_of 都按它), 其余缓存特征跟在后面
  for (size_t i = 0; i < d.feats.size(); ++i) {
    const auto it = std::find(L.feat_codes.begin() + static_cast<long>(i), L.feat_codes.end(), d.feats[i]);
    assert(it != L.feat_codes.end());
    const size_t j = static_cast<size_t>(it - L.feat_codes.begin());
    std::swap(L.feat_codes[i], L.feat_codes[j]);
    std::swap(L.planes[i], L.planes[j]);
  }
  const size_t n = L.n();
  const int H = L.hd.n, T = L.T;
  F.w.ensure(A, H, static_cast<int>(L.amts.size()), nt);

  // ---- Y: 毛标签 [H] (作用域内一次) ----
  if (F.lab.ry.empty()) {
    prep_labels(F.ex, L, F.w, F.lab);
    if (cancelled())
      return false;
  }

  // ---- 结果骨架 ----
  InspectResult res;
  res.key = sp.key(sc);
  res.file = sp.file;
  res.expr = sp.expr;
  res.seg = sc.seg;
  res.frame = sp.frame;
  res.scope.pool = sc.pool;
  res.scope.start_date = dates.front();
  res.scope.end_date = dates.back();
  res.scope.days = L.days, res.scope.T = T, res.scope.A = A;
  res.scope.backend = "cpu";
  res.scope.n_full = F.lab.L.cap;
  res.dates = dates;
  res.read_scope = es.rs;
  res.pool_index = L.index;
  res.n_hold = H;
  res.hd = L.hd;
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

  // ---- 数据检查 (与 FactorsService 同一套) ----
  for (int i = 0; i < N; ++i) {
    check_leaf_domains(d, i, L, res.error);
    if (!res.error.empty()) {
      F.pl.clear(); // 无一级 / 无 rx
      publish_result();
      return true;
    }
  }

  // ---- X: Dag 后序顺序走, 每节点全核 ----
  begin(Phase::Running, static_cast<size_t>(d.n_ops + 2)); // 节点 + 评估 + 收尾
  const std::vector<const factor::check::Plane *> in = leaf_planes(L, d.feats.size());
  if (!factor::cpu::walk(F.ex, d, in, F.hp, F.nsc, T, A, L.seg, L.cs.m.data(), nullptr, [&](int i, const auto &, double ms) {
        if (d.nodes[static_cast<size_t>(i)].op >= 0) {
          res.eval_ms += ms;
          publish(1);
        }
        return !cancelled();
      }))
    return false;
  const int xi = d.nodes[static_cast<size_t>(root)].in[0];
  assert(xi >= 0 && "根是归一算子, 至少一元");
  const factor::check::Plane *rp = factor::cpu::plane_at(d, in, F.hp, root); // Stat 只看池内 (g 在算子内部)
  const factor::check::Plane *xp = factor::cpu::plane_at(d, in, F.hp, xi);   // 根输入: 根的槽先于输入释放分配, 此刻仍在

  // ---- R: rank_x ----
  InspectPlanes &pl = F.pl;
  const size_t nrow = static_cast<size_t>(H) * T;
  pl.rows.resize(nrow);
  pl.tails.resize(nrow);
  pl.rx.resize(n);
  pl.rx_all.resize(n);
  F.lc.resize(n);
  const factor::stat::Span s = L.span();
  const factor::stat::Plane x{rp->v.data(), rp->m.data()};
  // 先多跑一遍不看池的秩 (g = xm) → rx_all (只给 K 线显示池外段), 再跑评估用的池内秩 → rx
  factor::cpu::stat::rank_x(F.ex, x, x.m, sp.frame, s, pl.rx_all.data(), F.w);
  factor::cpu::stat::rank_x(F.ex, x, L.cs.m.data(), sp.frame, s, pl.rx.data(), F.w);
  const uint16_t *rx = pl.rx.data();
  // ---- C: 腿码 (换手成本用) ----
  factor::cpu::stat::leg_codes(F.ex, rx, s, F.lc.data());
  if (cancelled())
    return false;

  // ---- L: 评估 (只毛): 一级 Row[H][T] + 一级累加; 同遍尾部分层 (TailExt, Inspect 专属: 只在 Row::ok 行, 两腿组内更极端的 kTailOfGroup 截) ----
  int thr[kLegs][kTails];
  tail_thresholds(thr);
  factor::stat::Accum acc[factor::stat::kMaxHold];
  factor::cpu::stat::accum_rows(F.ex, s, rx, F.lab.L, F.w, acc, TailSink{pl.rows.data(), pl.tails.data(), T, thr, F.lab.L.cap});
  publish(1);
  if (cancelled())
    return false;

  // ---- 收尾 (FactorsLoad finish_root, 与 Run 同一份): valid% / rank% / 二级 HoldStat[H] / x 分布 / 换手成本 (契约【换手成本】) ----
  RootOut o;
  finish_root(F.ex, L, RootRaw{rp->m.data(), rx, F.lc.data(), acc, {xp->v.data(), xp->m.data()}}, F.w, o);
  res.valid_pct = o.valid_pct;
  res.rank_pct = o.rank_pct;
  res.hold.assign(o.hold, o.hold + H);
  res.costs = std::move(o.costs);
  res.xdist = std::move(o.xd);
  publish(1);
  if (cancelled())
    return false;
  publish_result();
  return true;
}

} // namespace GUI::Factors
