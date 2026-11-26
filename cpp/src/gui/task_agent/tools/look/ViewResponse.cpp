// view_response — 看响应: 输入 factor / side / split / from / to → ResponseRes (开仓前后各时点的累计超额 / 因子分位 / 与此前收益的相关 / 按开仓时段) → 原文; 展示见 ui/look/ViewResponse.cpp
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/look/Kernel.hpp"

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

namespace {

double cluster_se(const Mean &all, const std::vector<Mean> &by) {
  if (all.n < 2)
    return kNaN;
  const double m = all.mean();
  double v = 0.0;
  int nd = 0;
  for (const Mean &d : by)
    if (d.n)
      v += (d.sum - static_cast<double>(d.n) * m) * (d.sum - static_cast<double>(d.n) * m), ++nd;
  return nd >= 2 ? std::sqrt(v) / static_cast<double>(all.n) : kNaN;
}

struct RespPt {
  std::string name;
  bool close;
  int n;
};
std::vector<RespPt> points_of(int seg) {
  if (seg == 1)
    return {{"-5 日", false, -5}, {"-1 日", false, -1}, {"0 (开仓)", false, 0}, {"+1 日", false, 1}, {"+2 日", false, 2}, {"+3 日", false, 3}, {"+5 日", false, 5}};
  std::vector<RespPt> p{{"前 1 日同一行", false, -seg}};
  for (const int n : {-30, -10, -5, -1})
    if (-n < seg)
      p.push_back({std::to_string(n) + " 行", false, n});
  p.push_back({"0 (开仓)", false, 0});
  for (const int n : {1, 5, 10, 30})
    if (n < seg)
      p.push_back({"+" + std::to_string(n) + " 行", false, n});
  p.push_back({"当日收盘", true, 0});
  p.push_back({"T+1 收盘", true, 1});
  p.push_back({"T+2 收盘", true, 2});
  return p;
}

} // namespace

std::string ResponseRes::headline() const { return "因子 `" + label + "` · " + side + " · 开仓前后的累计超额"; }

std::vector<Sec> ResponseRes::secs() const {
  if (!events)
    return {{.lines = {"本端没有真持有过的格"}, .warn = true}};
  std::string lag = "开仓行因子分位与此前收益的截面秩相关:";
  for (size_t i = 0; i < lags.size(); ++i)
    lag += (i ? " · " : " ") + lags[i].first + " " + r3(lags[i].second);
  std::vector<Sec> out;
  Sec &x = out.emplace_back();
  x.lines = {"真持有段 " + str(events) + " 段" + (kept < events ? " (等距抽 " + str(kept) + " 段)" : ""), lag};
  x.plot = 0;
  x.tab.head = {"时点", "段数", "累计超额 bp", "± 标准误", "为正占比", "因子分位"};
  for (const Pt &p : pts)
    x.tab.rows.push_back({p.name, str(p.n), p.zero ? Num("0") : nbp(p.ex), p.zero ? "—" : fmt("%.1f", p.se * 1e4), p.zero ? "—" : pct(p.up), rk(p.rank)});
  if (tod) {
    Sec &t = out.emplace_back();
    t.title = "按开仓时段 (各时点的累计超额 bp)";
    t.tab.head = {"开仓时段", "段数"};
    for (const Pt &p : pts)
      t.tab.head.push_back(p.name);
    for (const Part &q : parts) {
      std::vector<Num> row{q.name, str(q.n)};
      for (size_t j = 0; j < pts.size(); ++j)
        row.push_back(pts[j].zero ? Num("0") : nbp(q.ex[j]));
      t.tab.rows.push_back(std::move(row));
    }
  }
  return out;
}

std::string ResponseRes::title() const {
  return "`" + label + "` · " + side + " · " + (!events ? std::string("本端没有真持有过的格") : str(events) + " 段 · " + pts.back().name + " 累计超额 " + bp(pts.back().ex) + "bp");
}

ToolOut ViewResponse(const Call &c, Bay &bay) {
  const ViewEnv &env = bay.env;
  Engine::FactorFlow &F = bay.flow;
  std::string err;
  const bool top = c.str("side") == "top";
  Window win;
  FactorSpec fac;
  fac.ends = true, fac.only_end = top ? st::kTop : st::kBot;
  if (!window_of(c, win, err) || !resolve_factor(c.str("factor"), env, fac, err))
    return Fail(std::move(err));

  const int seg = env.fsc.ft->seg;
  const bool tod = c.str("split") == "tod" && seg > 1;
  const std::vector<RespPt> pts = points_of(seg);
  const size_t P = pts.size();
  const std::vector<Slot> ps = tod ? parts_of(seg, env.open, env.clock) : std::vector<Slot>{};
  const std::vector<int> pr = tod ? part_of_row(ps, seg) : std::vector<int>{};

  std::vector<std::pair<std::string, int>> lags;
  if (seg == 1) {
    lags = {{"前 1 日", 1}, {"前 5 日", 5}, {"前 20 日", 20}};
  } else {
    for (const int w : {5, 30})
      if (w < seg)
        lags.push_back({"前 " + std::to_string(w) + " 行", w});
    lags.push_back({"前 1 日", seg});
  }
  std::vector<Mean> lag_rho(lags.size());

  struct EvRec {
    int d0, part;
    std::array<float, 16> v, rk;
  };
  assert(P <= 16);
  std::vector<EvRec> all_ev;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac, .bars = bit(Close) | bit(PreClose), .extra_front = seg == 1 ? 20 : 1, .extra_back = seg == 1 ? 5 : 2, .extra_work = sizeof(float)};
  const Engine::Rc rc = run_stream(env, F, cfg, dates, job, err, win, [&](const ChunkView &cv) {
    const int A = cv.blk.y.sh.A, n_stat = cv.t_hi - cv.t_lo, total_T = static_cast<int>(cv.dates.size()) * seg;
    const Px px(cv.blk, cv.bm, cfg.bars, env.open);
    const std::vector<float> e = block_excess_steps(F.S.ex, cv.blk, px, cv.blk.land0());

    // 段首事件 (段在日界处天然断开, 不跨块)
    std::vector<std::pair<int, int>> raw; // (a, t)
    for (int r = 0; r < n_stat; ++r)
      for (const HeldCell &hc : cv.cells[static_cast<size_t>(r)])
        raw.push_back({hc.a, hc.t});
    std::sort(raw.begin(), raw.end());
    std::vector<std::pair<int, int>> ev;
    for (size_t i = 0; i < raw.size(); ++i) {
      const int u = cv.u0 + (raw[i].second - cv.t_lo);
      const int u_prev = i > 0 ? cv.u0 + (raw[i - 1].second - cv.t_lo) : -2;
      if (i == 0 || raw[i - 1].first != raw[i].first || u_prev + 1 != u || u_prev / seg != u / seg)
        ev.push_back(raw[i]);
    }
    const size_t base_idx = all_ev.size();
    all_ev.resize(base_idx + ev.size());
    F.S.ex.rows(static_cast<int>(ev.size()), [&](int i0, int i1, int) {
      for (int i = i0; i < i1; ++i) {
        const auto [a, t] = ev[static_cast<size_t>(i)];
        const int u = cv.u0 + (t - cv.t_lo), d0 = u / seg;
        EvRec rec{.d0 = d0, .part = tod ? pr[static_cast<size_t>(u % seg)] : 0};
        rec.v.fill(NAN), rec.rk.fill(NAN);
        for (size_t j = 0; j < P; ++j) {
          const int u_to = pts[j].close ? (d0 + pts[j].n) * seg + seg - 1 : u + pts[j].n;
          if (u_to < 0 || u_to >= total_T)
            continue;
          const int t_to = t + (u_to - u);
          assert(t_to >= 0 && t_to < cv.blk.y.sh.T);
          rec.v[j] = static_cast<float>(cum_step(e, A, a, t, t_to));
          const size_t k_to = static_cast<size_t>(t_to) * static_cast<size_t>(A) + static_cast<size_t>(a);
          if (cv.blk.y.gate[k_to])
            rec.rk[j] = rank_of_r16(cv.rx[k_to]);
        }
        all_ev[base_idx + static_cast<size_t>(i)] = rec;
      }
    });

    // 开仓行因子分位与此前收益的截面秩相关
    const int nt = F.S.ex.threads();
    for (size_t li = 0; li < lags.size(); ++li) {
      const int w = lags[li].second, rstep = row_step((cv.w_rows() - w) / kCorrRows, seg);
      std::vector<Mean> tr(static_cast<size_t>(nt));
      F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
        std::vector<double> xs, ys;
        RankScratch rs;
        for (int r = r0; r < r1; ++r) {
          const int t = cv.t_lo + r, u = cv.u0 + r;
          if (u < w || (u - w) % rstep != 0)
            continue;
          const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
          xs.clear(), ys.clear();
          for (int a = 0; a < A; ++a)
            if (cv.blk.y.gate[base + static_cast<size_t>(a)] && cv.rx[base + static_cast<size_t>(a)] != st::kRankNone)
              xs.push_back(rank_of_r16(cv.rx[base + static_cast<size_t>(a)])), ys.push_back(cum_step(e, A, a, t, t - w));
          if (static_cast<int>(xs.size()) >= kMinCs)
            tr[static_cast<size_t>(tid)].add(rs.spearman(xs, ys));
        }
      });
      for (int tid = 0; tid < nt; ++tid)
        lag_rho[li].merge(tr[static_cast<size_t>(tid)]);
    }
  });

  if (rc != Engine::Rc::Ok)
    return Fail(std::move(err));

  auto res = std::make_shared<ResponseRes>();
  ResponseRes &R = *res;
  R.period = period_note(dates, env, win), R.label = fac.label, R.side = top ? "做多端 G20" : "最低组 G1", R.tod = tod;
  R.events = all_ev.size();
  if (all_ev.empty())
    return Done(std::move(res));

  // 按全局资产序再按时间排以保证等距抽样确定性
  const size_t n_ev = all_ev.size(), step = std::max<size_t>(1, n_ev / kEvents);
  const size_t D = dates.size();
  std::vector<Mean> ex(P), rank(P);
  std::vector<std::vector<Mean>> by_day(P, std::vector<Mean>(D));
  std::vector<std::vector<Mean>> by_part(ps.size(), std::vector<Mean>(P));
  std::vector<size_t> n_part(ps.size(), 0);
  size_t kept = 0;
  for (size_t i = 0; i < n_ev; i += step, ++kept) {
    const EvRec &ev = all_ev[i];
    if (tod)
      ++n_part[static_cast<size_t>(ev.part)];
    for (size_t j = 0; j < P; ++j) {
      if (std::isfinite(ev.v[j])) {
        ex[j].add(ev.v[j]), by_day[j][static_cast<size_t>(ev.d0)].add(ev.v[j]);
        if (tod)
          by_part[static_cast<size_t>(ev.part)][j].add(ev.v[j]);
      }
      if (std::isfinite(ev.rk[j]))
        rank[j].add(ev.rk[j]);
    }
  }
  R.kept = kept;
  for (size_t j = 0; j < P; ++j)
    R.pts.push_back({pts[j].name, !pts[j].close && pts[j].n == 0, ex[j].n, ex[j].mean(), cluster_se(ex[j], by_day[j]), ex[j].up(), rank[j].mean()});
  for (size_t i = 0; i < lags.size(); ++i)
    R.lags.push_back({lags[i].first, lag_rho[i].mean()});
  for (size_t q = 0; q < ps.size(); ++q) {
    if (!n_part[q])
      continue;
    ResponseRes::Part &pt = R.parts.emplace_back();
    pt.name = ps[q].name, pt.n = n_part[q];
    for (size_t j = 0; j < P; ++j)
      pt.ex.push_back(by_part[q][j].mean());
  }
  return Done(std::move(res));
}

json SampleViewResponse(Feed &p) {
  return p.window({{"factor", p.factor()}, {"side", p.pick(Tool::ViewResponse, "side")}, {"split", p.ctx.seg > 1 && p.coin(0.3) ? "tod" : "none"}});
}

} // namespace GUI::Agent
