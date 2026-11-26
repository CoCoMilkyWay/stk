// InspectService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/services/InspectService.hpp"

#include "factor/Stat/Cpu.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "features/TimeIndex.hpp"
#include "gui/engine/Engine.hpp"
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

namespace GUI::Engine {

namespace {

struct RicP : RowsP<RicP, float> {
  std::vector<float> *dst = nullptr;
  static double bytes(const Shape &sh, int H) { return sizeof(float) * static_cast<double>(H) * sh.T; }
  static float of(const factor::stat::Row &r) { return ric_of(r); }
  void init(const PassCtx &p) {
    dst->assign(static_cast<size_t>(p.hd.n) * p.rows_stat, std::numeric_limits<float>::quiet_NaN());
  }
  void fold(const Ctx &c, int hi, int u0, std::span<const float> g) {
    std::copy(g.begin(), g.end(), dst->begin() + static_cast<std::ptrdiff_t>(static_cast<size_t>(hi) * c.c.rows_stat() + u0));
  }
};

struct RxP : LandCellsP<uint16_t> {
  static constexpr uint8_t need = kNeedRx;
  void close(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    std::copy(v.rx + static_cast<size_t>(s.t_lo) * s.A, v.rx + static_cast<size_t>(s.t_hi()) * s.A, at(c)); // rx 已定向
  }
};

struct RxAllP : LandCellsP<uint16_t> {
  static constexpr uint8_t need = kNeedMask | kNeedX;
  void close(const Ctx &c, const RootView &v) {
    const factor::stat::Span s = c.s();
    const size_t o = static_cast<size_t>(s.t_lo) * s.A;
    const factor::stat::Plane x{v.x.v + o, v.x.m + o};
    factor::cpu::stat::rank_x(c.ex, x, x.m, v.f, c.d, factor::stat::Span{s.t_hi() - s.t_lo, s.A, s.g}, at(c), *c.w);
  }
};

using InspectProbes = EvalProbes<RxP, RxAllP, RicP, NavP>;

struct Leg {
  uint32_t a = 0;
  int t_in = 0, t_out = -1;
  int rank_in = 0, rank_out = 0;
  float px_in = 0.f, px_out = 0.f, adj_in = 0.f, adj_out = 0.f;
};

std::vector<Leg> top_legs(const std::vector<uint16_t> &rx, int T, int A, int n) {
  using factor::stat::kRankNone;
  assert(n > 0 && rx.size() == static_cast<size_t>(T) * static_cast<size_t>(A));
  std::vector<Leg> legs;
  std::vector<int> open(static_cast<size_t>(A), -1);
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
  const int seg = ft.seg, t_last = static_cast<int>(dm.stat_days()) * seg - 1;
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
    const Day &dy = dm.days[dm.warm + d];
    assert(dy.date == dm.dates[dm.warm + d] && (dy.has & main));
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

bool MakeInspectRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int years, const FactorRow &row, bool reload,
                        bool gpu, InspectExport ex, int top_n, InspectRequest &req) {
  assert(row.error.empty() && !row.expr.empty() && "Inspect 只接有效 alpha 行");
  req = InspectRequest{};
  req.spec.f = FactorRef{row.expr, row.frame, factor::stat::dir_of(row.sign)};
  req.spec.row = row.key;
  req.reload = reload;
  if (!MakeFactorScope(data, std::move(ft), pool, years, gpu, req.scope))
    return false;
  if (ex != InspectExport::None) {
    assert(top_n >= 1);
    req.spec.ex = ex;
    req.spec.top_n = top_n;
    req.spec.csv = disk::path(disk::RunExportFile, {.pool = pool,
                                                    .level = factor_level(req.scope.lv).name,
                                                    .name = row.name_en + "_w" + std::to_string(row.widx) + "_top" + std::to_string(top_n) +
                                                            (ex == InspectExport::Log ? "_log" : "_snap")})
                       .string();
    for (const AssetItem &it : data.asset.items)
      req.spec.codes.push_back(it.code_ex()), req.spec.names.push_back(it.asset_name);
  }
  return true;
}

Rc InspectFactor(const InspectRequest &req, FactorFlow &F, InspectCache &cache, InspectResult &res, std::string &export_msg,
                 const std::function<void(InspectResult &)> &on_ready) {
  TraceN("InspectEvaluate");
  const FactorScope &sc = req.scope;
  const FactorRef &f = req.spec.f;
  export_msg.clear();
  if (req.reload)
    F.S.clear();

  RootSet rs{.hd = Holds::of(sc.ft->y.holds)};
  rs.add(*sc.ft, f.expr, f.frame, f.dir);
  ForestOut o;
  o.job.who = "Inspect";
  std::vector<InspectProbes> ps(1);
  InspectProbes &p = ps[0];
  p.at<RxP>().dst = &cache.pl.rx;
  p.at<RxAllP>().dst = &cache.pl.rx_all;
  p.at<RicP>().dst = &cache.pl.ric;

  const Rc rc = with_backend<InspectProbes>(F, sc.gpu, [&](auto &eng) {
    return eval_forest(eng, F, sc, rs, std::span(ps), o, /*whole=*/true, NoOp{}, [&](const Block &c, size_t, double) {
      cache.pool.update(c.gen, [&](LedgerLines &b) { b = pool_line(F.S.ex, c); });
      assert(cache.pool.val.P == c.rows_stat() && cache.pool.val.n == c.y.hd.n);
    });
  });
  if (aborted(rc))
    return rc;

  res = InspectResult{};
  res.key = inspect_key(sc.lv, sc.pool, sc.years, req.spec.row, f.expr, f.dir);
  res.row = req.spec.row;
  res.expr = f.expr;
  res.lv = sc.lv;
  res.frame = f.frame;
  if (rc != Rc::Ok) {
    res.error = o.job.why;
    cache.pl.clear();
    if (on_ready)
      on_ready(res);
    return rc;
  }

  const Domain &dm = o.job.dm;
  const int seg = sc.ft->seg, A = static_cast<int>(dm.rs.axis.size()), days = static_cast<int>(dm.stat_days()), T = days * seg;
  res.dates = dm.stat_dates();
  res.read_scope = dm.rs;
  res.pool_index = cache.pool.val;
  for (int hi = 0; hi < res.pool_index.n; ++hi) {
    const float *v = res.pool_index.at(hi);
    double s = 0.0;
    for (int t = 0; t < res.pool_index.P; ++t)
      s += v[t];
    res.pool_ann[hi] = static_cast<float>(per_year(s, days));
  }
  res.hd = o.job.spec.y.hd;
  fill_eval(p, o.scope(sc), o.eval_ms(0), res);
  res.alpha_dist = std::move(p.at<AlphaP>().d);
  std::copy_n(p.at<IcPdfP>().pdf, res.hd.n, res.ic_agg);
  res.nav = std::move(p.at<NavP>().out);
  for (int j = 0; j < kTails; ++j)
    std::copy_n(p.at<TurnP>().tails[j], res.hd.n, res.ls_turn[j]);

  std::vector<Leg> legs;
  if (req.spec.ex != InspectExport::None) {
    legs = top_legs(cache.pl.rx, T, A, req.spec.top_n);
    if (req.spec.ex == InspectExport::Snap)
      std::erase_if(legs, [](const Leg &l) { return l.t_out >= 0; });
  }
  if (on_ready)
    on_ready(res);

  if (req.spec.ex != InspectExport::None) {
    if (!fill_px(dm, *sc.ft, legs)) {
      F.S.clear();
      return Rc::Stale;
    }
    write_legs(req.spec, legs, dm.stat_dates(), dm.rs.axis, factor_level(sc.lv).geo());
    export_msg = std::string(req.spec.ex == InspectExport::Log ? "导出进出 " : "导出名单 ") + std::to_string(legs.size()) + " 行 → " + req.spec.csv;
  }
  return Rc::Ok;
}

bool InspectService::run(InspectRequest &req) {
  {
    std::lock_guard<std::mutex> lock(mutex);
    message.clear();
  }
  publish();
  FactorFlow F = flow();
  Lease l(F);
  if (!l)
    return false;
  InspectResult res;
  std::string export_msg;
  const Rc rc = InspectFactor(req, F, cache_, res, export_msg, [&](InspectResult &ready) {
    std::swap(ready.pl, cache_.pl);
    {
      std::lock_guard<std::mutex> lock(mutex);
      std::swap(result, ready);
      ++result_gen;
    }
    cache_.pl = std::move(ready.pl);
    publish();
  });
  if (!settle(rc))
    return false;
  if (!export_msg.empty()) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      message = std::move(export_msg);
    }
    publish();
  }
  return true;
}

} // namespace GUI::Engine
