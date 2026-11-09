// InspectService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_alpha/services/InspectService.hpp"

#include "factor/Stat/Cpu.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "features/TimeIndex.hpp"
#include "gui/factors/Engine.hpp" // eval_job (只 CPU) / Probes / LinesP / pool_line
#include "misc/date.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

namespace GUI::Alpha {

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

// ---- 导出 (flow E) ----
// 做多端前 n 的一段在持: 行 [t_in, t_out) 在前 n 内, t_out = −1 = 期末未平 (按末行估值). 名次 1 起;
// rank_out = 出行名次 (0 = 出行无秩: 出池 / 无值); 未平 = 最近一个有秩行的名次
struct Leg {
  uint32_t a = 0; // A 轴下标
  int t_in = 0, t_out = -1;
  int rank_in = 0, rank_out = 0;
  float px_in = 0.f, px_out = 0.f, adj_in = 0.f, adj_out = 0.f;
};
// rx [T][A] (已定向: 高 = 做多端) 逐行取前 n, 相邻行之差即进出
std::vector<Leg> top_legs(const std::vector<uint16_t> &rx, int T, int A, int n) {
  using factor::stat::kRankNone;
  assert(n > 0 && rx.size() == static_cast<size_t>(T) * static_cast<size_t>(A));
  std::vector<Leg> legs;
  std::vector<int> open(static_cast<size_t>(A), -1); // 资产 → 在持段 (legs 下标)
  std::vector<uint8_t> in(static_cast<size_t>(A), 0);
  std::vector<uint32_t> cand;
  for (int t = 0; t < T; ++t) {
    const uint16_t *r = rx.data() + static_cast<size_t>(t) * static_cast<size_t>(A);
    const auto before = [r](uint32_t x, uint32_t y) { return r[x] > r[y] || (r[x] == r[y] && x < y); };
    cand.clear();
    for (uint32_t a = 0; a < static_cast<uint32_t>(A); ++a)
      if (r[a] != kRankNone)
        cand.push_back(a);
    if (cand.empty())
      continue;
    const size_t k = std::min(cand.size(), static_cast<size_t>(n));
    std::nth_element(cand.begin(), cand.begin() + static_cast<long>(k - 1), cand.end(), before);
    std::sort(cand.begin(), cand.begin() + static_cast<long>(k), before);
    for (size_t i = 0; i < k; ++i)
      in[cand[i]] = 1;
    for (uint32_t a = 0; a < static_cast<uint32_t>(A); ++a) {
      int &o = open[a];
      if (o < 0 || in[a])
        continue;
      Leg &l = legs[static_cast<size_t>(o)];
      l.t_out = t;
      l.rank_out = r[a] == kRankNone ? 0 : 1 + static_cast<int>(std::count_if(cand.begin(), cand.end(), [&](uint32_t b) { return before(b, a); }));
      o = -1;
    }
    for (size_t i = 0; i < k; ++i) {
      const uint32_t a = cand[i];
      const int rank = static_cast<int>(i + 1);
      in[a] = 0;
      if (open[a] >= 0) {
        legs[static_cast<size_t>(open[a])].rank_out = rank;
        continue;
      }
      open[a] = static_cast<int>(legs.size());
      legs.push_back({a, t, -1, rank, rank});
    }
  }
  return legs;
}
// 进出行所在日读标签库 (A 轴 = 其池轴): 因子层 close 本行 + 日频 adj_factor 当日适用值 (= 承接日 L_1day 行; 承接日未落盘 / 轴首 → 当日行). false = 库判废
bool fill_px(const Domain &dm, const FeatureTable &ft, std::vector<Leg> &legs) {
  const FeatureLib &lib = dm.rs.lib();
  const size_t lvl = factor_level(ft.lv).lvl, A = dm.rs.axis.size();
  const Col &px_col = ft.b.ohlc[3], &adj_col = ft.b.adj_factor;
  assert(px_col.level == lvl && adj_col.level == L_1day && lib.axis.size() == A);
  const std::vector<size_t> c_px{px_col.col}, c_adj{adj_col.col};
  FeatureRead reader(lib);
  FeatureRead::DayColumns px, adj;
  px.preallocate(A, lvl, 1);
  adj.preallocate(A, L_1day, 1);
  const LibMask main = lib_bit(dm.rs.main);
  const int seg = ft.seg, t_last = static_cast<int>(dm.dates.size()) * seg - 1;
  struct Ev {
    int t;
    size_t i;
    bool out;
  };
  std::vector<Ev> ev;
  for (size_t i = 0; i < legs.size(); ++i)
    ev.push_back({legs[i].t_in, i, false}), ev.push_back({legs[i].t_out < 0 ? t_last : legs[i].t_out, i, true});
  std::sort(ev.begin(), ev.end(), [](const Ev &x, const Ev &y) { return x.t < y.t; });
  for (size_t j = 0; j < ev.size();) {
    const size_t d = static_cast<size_t>(ev[j].t / seg);
    const Day &dy = dm.days[d];
    assert(dy.date == dm.dates[d] && (dy.has & main));
    reader.load_day_columns(dy.date, c_px, px);
    reader.load_day_columns(dy.prev != 0 && (dy.has_prev & main) ? dy.prev : dy.date, c_adj, adj);
    if (reader.stale())
      return false;
    for (; j < ev.size() && static_cast<size_t>(ev[j].t / seg) == d; ++j) {
      Leg &l = legs[ev[j].i];
      const float p = static_cast<float>(px.get(static_cast<size_t>(ev[j].t % seg), 0, l.a)), f = static_cast<float>(adj.get(0, 0, l.a));
      (ev[j].out ? l.px_out : l.px_in) = p;
      (ev[j].out ? l.adj_out : l.adj_in) = f;
    }
  }
  return true;
}
// CSV (时刻 = 行末 YYYY-MM-DD HH:MM; ret = 复权收益; 未平按末行估值):
//   Log  一段一行 (进行序), 未平 → out_rank 空, out = 末行
//   Snap 未平段一行 (名次序) = 期末名单, time / px = 末行
void write_legs(const InspectSpec &sp, std::vector<Leg> &legs, const std::vector<misc::Ymd> &dates, const PoolAxis &axis, const factor::Geo &g) {
  std::filesystem::create_directories(std::filesystem::path(sp.csv).parent_path());
  std::ofstream f(sp.csv);
  assert(f && "导出文件写不开");
  const auto when = [&](int t) {
    const ClockTime c = L_1min_to_Clock(static_cast<size_t>(g.minute(t % g.S)));
    const int m = c.hour * 60 + c.minute + 1;
    char b[32];
    std::snprintf(b, sizeof(b), "%s %02d:%02d", misc::dash(dates[static_cast<size_t>(t / g.S)]).c_str(), m / 60, m % 60);
    return std::string(b);
  };
  const auto ret_of = [](const Leg &l) { return static_cast<double>(l.px_out) * l.adj_out / (static_cast<double>(l.px_in) * l.adj_in) - 1.0; };
  const auto who = [&](const Leg &l) {
    const uint32_t gid = axis.global(l.a);
    assert(gid < sp.codes.size() && "资产不在全局资产轴上");
    return sp.codes[gid] + ',' + sp.names[gid];
  };
  const int t_last = static_cast<int>(dates.size()) * g.S - 1;
  const double w = 1.0 / sp.top_n;
  char b[192];
  if (sp.ex == InspectExport::Log) {
    f << "code,name,in_time,in_rank,in_px,out_time,out_rank,out_px,ret,w\n";
    for (const Leg &l : legs) {
      std::snprintf(b, sizeof(b), ",%s,%d,%.3f,%s,%s,%.3f,%.6f,%.6g\n", when(l.t_in).c_str(), l.rank_in, static_cast<double>(l.px_in),
                    when(l.t_out < 0 ? t_last : l.t_out).c_str(), l.t_out < 0 ? "" : std::to_string(l.rank_out).c_str(), static_cast<double>(l.px_out),
                    ret_of(l), w);
      f << who(l) << b;
    }
  } else {
    assert(std::all_of(legs.begin(), legs.end(), [](const Leg &l) { return l.t_out < 0; }));
    std::sort(legs.begin(), legs.end(), [](const Leg &x, const Leg &y) { return x.rank_out < y.rank_out; });
    const std::string now = when(t_last);
    f << "time,rank,code,name,px,in_time,in_rank,in_px,ret,w\n";
    for (const Leg &l : legs) {
      std::snprintf(b, sizeof(b), ",%.3f,%s,%d,%.3f,%.6f,%.6g\n", static_cast<double>(l.px_out), when(l.t_in).c_str(), l.rank_in,
                    static_cast<double>(l.px_in), ret_of(l), w);
      f << now << ',' << l.rank_out << ',' << who(l) << b;
    }
  }
  assert(f && "导出文件写入失败");
}

} // namespace

bool MakeInspectRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, const FactorRow &row, bool reload,
                        bool gpu, InspectExport ex, int top_n, InspectRequest &req) {
  assert(row.error.empty() && !row.expr.empty() && "Inspect 只接有效 alpha 行");
  req = InspectRequest{};
  req.spec.f = FactorRef{row.expr, row.frame, factor::stat::dir_of(row.sign)};
  req.spec.row = row.key;
  req.reload = reload;
  if (!MakeFactorScope(data, std::move(ft), pool, gpu, req.scope))
    return false;
  if (ex != InspectExport::None) {
    assert(top_n >= 1);
    req.spec.ex = ex;
    req.spec.top_n = top_n;
    req.spec.csv = (std::filesystem::path(data.config.export_dir) / pool / factor_level(req.scope.lv).name /
                    (row.name_en + "_w" + std::to_string(row.widx) + "_top" + std::to_string(top_n) + (ex == InspectExport::Log ? "_log" : "_snap") + ".csv"))
                       .string();
    for (const AssetItem &it : data.asset.items)
      req.spec.codes.push_back(it.code_ex()), req.spec.names.push_back(it.asset_name);
  }
  return true;
}

InspectService::InspectService() : FactorWorker(Prio::Interactive) { Start(); }
InspectService::~InspectService() { Stop(); }

bool InspectService::run(InspectRequest &req) {
  {
    std::lock_guard<std::mutex> lock(mutex); // 旧 result 不清: 页面继续整页画旧因子, 新结果发布时一次性覆盖 (不闪)
    message.clear();
  }
  publish();
  const bool ok = [&] {
    FactorFlow &F = flow();
    Lease l(F);
    return l && eval(req, F);
  }();
  flow().end();
  return ok;
}

bool InspectService::eval(const InspectRequest &req, FactorFlow &F) {
  TraceN("InspectEvaluate");
  const FactorScope &sc = req.scope;
  const FactorRef &f = req.spec.f;
  if (req.reload)
    F.S.clear();

  // ---- 管线 eval_job (与 Run / Portfolio 同一份): 一个根, 探针 InspectProbes; 块钩子 = 池等权线 (随 Y 的代) ----
  //   Y = 全部持有期 + 全部金额段; X = 本因子的叶 (平面经 Store 缓存, 缺的补读)
  RootSet rs;
  rs.add(*sc.ft, f.expr, f.frame, f.dir, keys_of<Holds>(sc.ft->y.labels));
  JobOut o;
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
      W.pool_ = pool_line(F.S.ex, c.y), W.pool_gen_ = c.gen;
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
  const Domain &dm = o.job.dm;
  const int seg = sc.ft->seg, A = static_cast<int>(dm.rs.axis.size());
  std::vector<Leg> legs;
  if (req.spec.ex != InspectExport::None) {
    legs = top_legs(pl_.rx, static_cast<int>(dm.dates.size()) * seg, A, req.spec.top_n);
    if (req.spec.ex == InspectExport::Snap)
      std::erase_if(legs, [](const Leg &l) { return l.t_out >= 0; });
  }
  publish_result();

  // ---- 导出 (flow E): 结果已发布, 读价不挡页面 ----
  if (req.spec.ex != InspectExport::None) {
    if (!fill_px(dm, *sc.ft, legs))
      return settle(Rc::Stale);
    write_legs(req.spec, legs, dm.dates, dm.rs.axis, factor_level(sc.lv).geo());
    {
      std::lock_guard<std::mutex> lock(mutex);
      message = std::string(req.spec.ex == InspectExport::Log ? "导出进出 " : "导出名单 ") + std::to_string(legs.size()) + " 行 → " + req.spec.csv;
    }
    publish();
  }
  return true;
}

} // namespace GUI::Alpha