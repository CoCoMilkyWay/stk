// InspectService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/InspectService.hpp"

#include "factor/Stat/Cpu.hpp"
#include "gui/task_factors/services/FactorsEngine.hpp" // eval_job (只 CPU) / Probes / LinesP / pool_line
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <limits>
#include <vector>

namespace GUI::Factors {

namespace {

using factor::stat::kBot;
using factor::stat::kTop;

// 端 e 第 j 级尾部的绝对持仓收益 (超额 + 只数 · 基准 / 持仓分母, 同 group_abs)
double tail_abs(const factor::stat::Row &r, const TailRow &tr, int e, int j, double cap) {
  return factor::stat::book_abs(tr.sum[e][j], tr.cnt[e][j], r.mkt, cap * kTailOfGroup[j]);
}
// 净值线 i (InspectResult::nav 线序, 见 kNavLines) 在行 t 这一批的收益, 全取 Row / TailRow 现成值 (不 ok → 0): 分层超额 = grp / 尾部 sum;
//   分层绝对 = 超额 + 只数 · 基准 / 持仓分母 (组 cap, 尾 j cap · kTailOfGroup[j]);
//   LS = ½ (顶端 − 底端) 的超额 / 绝对: 0 = 两端组 (超额即 ls_row), 1 + j = 第 j 级尾部 (r 已定向)
double nav_value(const factor::stat::Row &r, const TailRow &tr, int i, double cap) {
  if (!r.ok)
    return 0.0;
  if (i >= 2 * kNavLayers) {
    const bool absolute = i < 2 * kNavLayers + kNavLs;
    const int j = i - 2 * kNavLayers - (absolute ? 0 : kNavLs);
    using factor::stat::end_group;
    if (j == 0)
      return absolute ? 0.5 * (group_abs(r, end_group(kTop), cap) - group_abs(r, end_group(kBot), cap)) : factor::stat::ls_row(r);
    return absolute ? 0.5 * (tail_abs(r, tr, kTop, j - 1, cap) - tail_abs(r, tr, kBot, j - 1, cap)) : 0.5 * (tr.sum[kTop][j - 1] - tr.sum[kBot][j - 1]);
  }
  const bool absolute = i < kNavLayers;
  const int k = absolute ? i : i - kNavLayers;
  if (k < factor::stat::kGroups)
    return absolute ? group_abs(r, k, cap) : r.grp[k];
  const int e = (k - factor::stat::kGroups) / kTails, j = (k - factor::stat::kGroups) % kTails;
  return absolute ? tail_abs(r, tr, e, j, cap) : tr.sum[e][j];
}
// 分层 / 尾部 / LS 净值线 [hd.n · kNavLines] (契约【净值】; 全部持有期一次 fold_book, stride = S → 日收益):
// 逐格扩展 TailExt, 行收尾出本行尾部 → 各线这一批的收益 nav_value
struct NavP : LinesP<kNavLines> {
  using Ext = TailExt;
  double cap = 0.0;
  int d = factor::stat::kFwd;
  Ext ext() const { return {kTailThr.v, cap, d}; }
  void open(const Ctx &c) {
    LinesP::open(c);
    cap = c.L.cap, stride = c.c.y.sh.seg, d = c.d;
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
// 各级尾部 LS 线两端的换手量 acc[j][hi] (Stat 契约【换手成本】的 Δw, cost_rows 走一个固定段; 顶底线 = CostP 的段 0):
// 第 j 级尾部端码 tail_code, 满仓 cap · kTailOfGroup[j] (与 TailRow 同口径); 块间续攒
struct TurnP : ProbeBase {
  static constexpr uint8_t need = kNeedRx;
  factor::stat::CostAcc acc[kTails][factor::stat::kMaxHold] = {};
  static double scratch(const Shape &sh, int) { return static_cast<double>(sh.n()); }
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    std::vector<uint8_t> ec(s.n()); // 尾部端码 [T][A], 逐级复用
    for (int j = 0; j < kTails; ++j) {
      c.ex.rows(s.T, [&](int t0, int t1, int) {
        for (size_t k = static_cast<size_t>(t0) * s.A, e = static_cast<size_t>(t1) * s.A; k < e; ++k)
          ec[k] = tail_code(v.rx[k], j, c.d);
      });
      const factor::stat::Cost flat{};
      factor::stat::CostAcc out[factor::stat::kMaxHold];
      factor::cpu::stat::cost_rows(c.ex, ec.data(), s, c.L, c.L.cap * kTailOfGroup[j], &flat, 1, *c.w, out);
      for (int hi = 0; hi < c.L.hd.n; ++hi)
        acc[j][hi].merge(out[hi]);
    }
  }
};
// 秩按所评方向显示: 反向 = kRankMax − r16 (高 = 做多端; 无效不动), 就地
void orient_rx(misc::exec::ForkJoin &ex, uint16_t *rx, int T, int A, int d) {
  if (d == factor::stat::kFwd)
    return;
  ex.rows(T, [&](int t0, int t1, int) {
    for (size_t k = static_cast<size_t>(t0) * A, e = static_cast<size_t>(t1) * A; k < e; ++k)
      rx[k] = rx[k] == factor::stat::kRankNone ? rx[k] : static_cast<uint16_t>(factor::stat::kRankMax - rx[k]);
  });
}
// 池内秩 rx 落地 [rows_all][A] (统计行; 右上资产轴 / K 线着色)
struct RxP : LandCellsP<uint16_t> {
  static constexpr uint8_t need = kNeedRx;
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    std::copy(v.rx + static_cast<size_t>(s.t_lo) * s.A, v.rx + static_cast<size_t>(s.t_hi()) * s.A, stat_at(s));
    orient_rx(c.ex, stat_at(s), s.t_hi() - s.t_lo, s.A, c.d);
  }
};
// 不看池的秩 (g = xm) 落地 [rows_all][A] (统计行; 只给 K 线显示池外段, 评估不用)
struct RxAllP : LandCellsP<uint16_t> {
  static constexpr uint8_t need = kNeedMask | kNeedX;
  void root(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    const size_t o = static_cast<size_t>(s.t_lo) * s.A;
    const factor::stat::Plane x{v.x.v + o, v.x.m + o};
    factor::cpu::stat::rank_x(c.ex, x, x.m, v.f, factor::stat::Span{s.t_hi() - s.t_lo, s.A, s.g}, stat_at(s), *c.w);
    orient_rx(c.ex, stat_at(s), s.t_hi() - s.t_lo, s.A, c.d);
  }
};
using InspectProbes = EvalProbes<RxP, RxAllP, RicP, NavP, TurnP>;

} // namespace

bool MakeInspectRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, const FactorRow &row, bool reload,
                        bool gpu, InspectRequest &req) {
  assert(row.error.empty() && !row.expr.empty() && "Inspect 只接有效 alpha 行");
  req = InspectRequest{};
  req.spec = InspectSpec{FactorRef{row.expr, row.frame, factor::stat::dir_of(row.sign)}, row.key};
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
  flow().end(Life::Worker);
  return ok;
}

bool InspectService::eval(const InspectRequest &req, FactorFlow &F) {
  TraceN("InspectEvaluate");
  const FactorScope &sc = req.scope;
  const FactorRef &f = req.spec.f;
  if (req.reload)
    F.reset();

  // ---- 管线 eval_job (与 Run / Portfolio 同一份): 一个根, 探针 InspectProbes; 块钩子 = 池等权线 (随 Y 一代) ----
  //   Y = 全部持有期 + 全部金额段; X = 评估域内读过的全留 (Life::Worker), 缺的补读
  RootSet rs;
  rs.add(*sc.ft, f.expr, f.frame, f.dir, keys_of<Holds>(sc.ft->y.labels));
  JobOut o;
  o.job.life = Life::Worker;
  o.job.who = "Inspect";
  InspectProbes ps;
  ps.at<RxP>().dst = &pl_.rx;
  ps.at<RxAllP>().dst = &pl_.rx_all;
  ps.at<RicP>().dst = &pl_.ric;
  struct Hooks : HookBase {
    static constexpr bool whole() { return true; } // 池等权线 / alpha 分布要整期
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
  const Rc rc = F.with_engine<InspectProbes>(sc.gpu, [&](auto &eng) {
    return eval_job(eng, F, sc, rs, [&](int) -> InspectProbes & { return ps; }, o, Hooks{{}, *this, F});
  });
  if (!settle(rc))
    return false;

  // ---- 结果骨架 ----
  InspectResult res;
  res.key = inspect_key(sc.lv, sc.pool, req.spec.row, f.expr, f.dir);
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

  // ---- 评估域不成 / 叶值域越界: 只发头部 + error ----
  if (rc != Rc::Ok) {
    res.error = o.job.why;
    pl_.clear(); // 无一级 / 无 rx
    publish_result();
    return true;
  }
  res.dates = o.job.dm.dates;
  res.read_scope = o.job.dm.rs;
  res.pool_index = pool_;
  std::copy_n(pool_ann_, pool_.n, res.pool_ann);
  res.hd = o.job.spec.y.hd;
  fill_eval(ps, o.scope(sc), o.eval_ms(0), res);
  res.alpha_dist = std::move(ps.at<AlphaP>().d);
  std::copy_n(ps.at<IcPdfP>().pdf, res.hd.n, res.ic_agg);
  res.nav = std::move(ps.at<NavP>().out);
  for (int j = 0; j < kTails; ++j)
    std::copy_n(ps.at<TurnP>().acc[j], res.hd.n, res.ls_turn[j]);
  publish_result();
  return true;
}

} // namespace GUI::Factors
