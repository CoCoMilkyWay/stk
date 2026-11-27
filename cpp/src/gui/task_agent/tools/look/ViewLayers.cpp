// view_layers — 看分层: 输入 factor / split / cond / from / to → LayersRes (20 组 + 尾部 + 切分 + 条件三段) → 原文; 展示见 ui/look/ViewLayers.cpp
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/look/Kernel.hpp"

#include <map>

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

std::string LayersRes::headline() const { return "因子 `" + label + "` 的分层"; }

std::vector<Sec> LayersRes::secs() const {
  std::vector<Sec> out;
  Sec &x = out.emplace_back();
  double mid = 0.0, lo = INFINITY, hi = -INFINITY;
  for (int k = 5; k < 15; ++k)
    mid += all.grp[static_cast<size_t>(k)] / 10.0;
  for (const double h : held)
    lo = std::min(lo, h), hi = std::max(hi, h);
  const bool even = !(hi - lo > 0.1 * hi);
  x.lines = {"有统计行 " + std::to_string(all.ok) + " / " + std::to_string(all.rows) + " · mono " + r3(all.mono) + " · 每期多空 " + bp(all.ls) + "bp · rIC " + r3(all.ric) +
                 " (t " + fmt("%.1f", ric_t) + ", 正 " + pct(ric_up) + ")",
             "做多端 G20 " + bp(all.grp[kG20]) + "bp · 最低组 G1 " + bp(all.grp[kG1]) + "bp · 中段 G6–G15 均值 " + bp(mid) + "bp" +
                 (even ? " · 各组行均真持有 " + fmt("%.1f", lo) + "–" + fmt("%.1f", hi) + " 只" : std::string())};
  x.sparks = {"20 组每期超额 G1 → G20 " + spark(all.grp, bp) + " bp"};
  x.plot = 0;
  x.tab.head = {""};
  std::vector<Num> ex{"每期超额 bp"}, cnt{"行均真持有只数"};
  for (int k = 0; k < kGroups; ++k) {
    x.tab.head.push_back(gname(k));
    ex.push_back(nbp(all.grp[static_cast<size_t>(k)])), cnt.push_back(fmt("%.1f", held[static_cast<size_t>(k)]));
  }
  x.tab.rows.push_back(std::move(ex));
  if (!even)
    x.tab.rows.push_back(std::move(cnt));

  Sec &t = out.emplace_back();
  t.title = "两端尾部";
  t.tab.head = {"截取", "占截面", "G20 端 bp", "G20 行均只数", "G1 端 bp", "G1 行均只数", "多空 bp"};
  t.tab.rows.push_back({"整组", fmt("%g%%", 100.0 / kGroups), nbp(all.grp[kG20]), fmt("%.1f", held[kG20]), nbp(all.grp[kG1]), fmt("%.1f", held[kG1]), nbp(all.ls)});
  for (const Tail &a : tails)
    t.tab.rows.push_back({"组内最极端 " + fmt("%g%%", a.frac * 100.0), fmt("%g%%", a.frac * 100.0 / kGroups), nbp(a.top), fmt("%.1f", a.top_n), nbp(a.bot), fmt("%.1f", a.bot_n),
                          nbp((a.top - a.bot) / 2.0)});

  if (split != "none") {
    Sec &p = out.emplace_back();
    p.title = split == "tod" ? "按日内时段" : "按月";
    if (split == "tod" && seg == 1)
      p.lines = {"日层每天一行, 没有日内时段可切"}, p.warn = true;
    else
      p.tab = LayerTab(split == "tod" ? "时段" : "月份", splits), p.plot = 1;
  }

  if (!cond.empty()) {
    Sec &q = out.emplace_back();
    q.title = "按条件三等分 `" + cond + "`";
    if (!cond_err.empty())
      q.lines = {"条件没算成: " + cond_err}, q.warn = true;
    else if (!has_cond)
      q.lines = {"没有可用的行 (每行至少要 " + std::to_string(3 * kMinCs) + " 个在池且因子 / 标签 / 条件都有效的格)"}, q.warn = true;
    else
      q.lines = {"条件与因子的截面秩相关 " + r3(cond_rho) + " · 条件自身 rIC " + r3(cond_ric) + " · 抽 " + std::to_string(conds[0].rows) + " 行"}, q.tab = LayerTab("条件段", conds),
      q.plot = 2;
  }
  return out;
}

std::string LayersRes::title() const { return "`" + label + "` · mono " + r3(all.mono) + " · 多空 " + bp(all.ls) + "bp · rIC " + r3(all.ric); }

std::string LayersRes::gist() const {
  const Tail &t = tails.back();
  std::string g = "mono " + r3(all.mono) + " · 多空 " + bp(all.ls) + "bp · rIC " + r3(all.ric) + " · 组内最极端 " + fmt("%g%%", t.frac * 100.0) + ": G20 " + bp(t.top) +
                  " / G1 " + bp(t.bot) + "bp";
  if (!cond.empty() && cond_err.empty() && has_cond)
    g += "; 按 " + text::head(cond, kGistExpr) + " 低/中/高 mono " + r3(conds[1].mono) + "/" + r3(conds[2].mono) + "/" + r3(conds[3].mono) + " · 多空 " + bp(conds[1].ls) + "/" +
         bp(conds[2].ls) + "/" + bp(conds[3].ls) + "bp";
  return g;
}

ToolOut ViewLayers(const Call &c, Bay &bay) {
  const ViewEnv &env = bay.env;
  Engine::FactorFlow &F = bay.flow;
  std::string err;
  Window win;
  FactorSpec fac;
  fac.tails = true;
  if (!window_of(c, win, err) || !resolve_factor(c.str("factor"), env, fac, err))
    return Fail(std::move(err));
  fac.cells = true, fac.only_end = kAllGroups;
  const std::string split = c.str("split"), cond = c.str("cond");
  std::vector<FeatSpec> feats;
  if (!cond.empty()) {
    resolve_feats({cond}, env, feats);
    if (!feats[0].err.empty())
      return Fail("cond: " + feats[0].err);
  }
  const int seg = env.fsc.ft->seg;
  const std::vector<Slot> ps = parts_of(seg, env.open, env.clock);
  const std::vector<int> pr = part_of_row(ps, seg);

  st::Sum all;
  std::vector<st::Sum> by_tod(ps.size());
  std::map<int, st::Sum> by_month;
  std::array<st::Sum, 4> cond_b;
  Mean cond_rho, cond_ric;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac, .feats = feats.empty() ? nullptr : &feats};
  const Engine::Rc rc = run_stream(env, F, cfg, dates, job, err, win, [&](const ChunkView &cv) {
    const int n_stat = cv.t_hi - cv.t_lo;
    for (int r = 0; r < n_stat; ++r) {
      const int u = cv.u0 + r;
      all.add(cv.rows[static_cast<size_t>(r)], st::Ret::Excess);
      if (split == "tod" && seg > 1)
        by_tod[static_cast<size_t>(pr[static_cast<size_t>(u % seg)])].add(cv.rows[static_cast<size_t>(r)], st::Ret::Excess);
      else if (split == "month")
        by_month[cv.dates[static_cast<size_t>(u / seg)] / 100].add(cv.rows[static_cast<size_t>(r)], st::Ret::Excess);
    }
    if (cond.empty() || !cv.feat_pl[0])
      return;
    const int A = cv.blk.y.sh.A;
    const int rstep = row_step(static_cast<int>(static_cast<size_t>(cv.w_rows()) * static_cast<size_t>(A) / kCsCells), seg);
    const factor::check::Plane *cp = cv.feat_pl[0];
    const Engine::LabelPlanes &lb = *cv.blk.y.labels[0];
    const uint16_t *lv = lb.lv.data();
    const int nt = F.S.ex.threads();
    std::vector<std::array<st::Sum, 4>> tb(static_cast<size_t>(nt));
    std::vector<Mean> trho(static_cast<size_t>(nt)), tric(static_cast<size_t>(nt));

    F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
      std::vector<double> cval, fxv, ey, hy, rc_v, rf_v, ry_v;
      std::vector<uint8_t> buy;
      std::array<std::vector<double>, 3> px, py, ph;
      std::array<std::vector<uint8_t>, 3> pb;
      RankScratch rs;
      st::Row w;
      const size_t ut = static_cast<size_t>(tid);
      for (int r = r0; r < r1; ++r) {
        const int t = cv.t_lo + r, u = cv.u0 + r;
        if (u % rstep != 0 || !opens(u, env))
          continue;
        const float mkt = mkt_of(cv, t);
        if (!std::isfinite(mkt))
          continue;
        const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
        cval.clear(), fxv.clear(), ey.clear(), hy.clear(), buy.clear();
        for (int a = 0; a < A; ++a) {
          const size_t k = base + static_cast<size_t>(a);
          if (cv.blk.y.gate[k] && cv.rx[k] != st::kRankNone && lb.m[k] && cp->m[k] && std::isfinite(cp->v[k])) {
            float hr = 0.f;
            const bool h = held_cell(cv.cells[static_cast<size_t>(r)], a, hr);
            cval.push_back(cp->v[k]), fxv.push_back(rank_of_r16(cv.rx[k])), ey.push_back(static_cast<double>(h2f(lv[k])) - mkt);
            hy.push_back(h ? static_cast<double>(hr) : 0.0), buy.push_back(h ? 1 : 0);
          }
        }
        const size_t n = cval.size();
        if (n < 3 * static_cast<size_t>(kMinCs))
          continue;
        rs.rank_into(cval, rc_v), rs.rank_into(fxv, rf_v), rs.rank_into(ey, ry_v);
        trho[ut].add(pearson(rc_v, rf_v)), tric[ut].add(pearson(rc_v, ry_v));
        for (size_t j = 0; j < 3; ++j)
          px[j].clear(), py[j].clear(), ph[j].clear(), pb[j].clear();
        for (size_t i = 0; i < n; ++i) {
          const size_t j = std::min<size_t>(2, static_cast<size_t>(rc_v[i] * 3.0 / static_cast<double>(n)));
          px[j].push_back(fxv[i]), py[j].push_back(ey[i]), ph[j].push_back(hy[i]), pb[j].push_back(buy[i]);
        }
        if (rs.approx_ranked(rf_v, hy, ry_v, buy, mkt, w))
          tb[ut][0].add(w, st::Ret::Excess);
        for (size_t j = 0; j < 3; ++j)
          if (rs.approx_row(px[j], py[j], ph[j], pb[j], mkt, w))
            tb[ut][j + 1].add(w, st::Ret::Excess);
      }
    });
    for (int tid = 0; tid < nt; ++tid) {
      const size_t ut = static_cast<size_t>(tid);
      cond_rho.merge(trho[ut]), cond_ric.merge(tric[ut]);
      for (size_t j = 0; j < 4; ++j)
        cond_b[j].merge(tb[ut][j]);
    }
  });

  if (rc != Engine::Rc::Ok)
    return Fail(std::move(err));

  auto r = std::make_shared<LayersRes>();
  r->period = period_note(dates, env, win);
  r->label = fac.label, r->split = split, r->cond = cond, r->seg = seg;
  r->all = row_of("全期", all);
  r->ric_t = all.ric_t(), r->ric_up = all.ric_up();
  for (int k = 0; k < kGroups; ++k)
    r->held.push_back(all.held(k));
  for (int j = 0; j < kTails; ++j)
    r->tails.push_back({factor::stat::tail_frac(j), all.tail_of(0, j), all.tail_held(0, j), all.tail_of(1, j), all.tail_held(1, j)});
  if (split == "tod" && seg > 1)
    for (size_t i = 0; i < ps.size(); ++i)
      r->splits.push_back(row_of(ps[i].name, by_tod[i]));
  else if (split == "month")
    for (const auto &[m, b] : by_month)
      r->splits.push_back(row_of(misc::dash_ym(m), b));
  if (!cond.empty()) {
    r->cond_err = feats[0].err;
    r->has_cond = cond_b[0].ok() > 0;
    r->cond_rho = cond_rho.mean(), r->cond_ric = cond_ric.mean();
    const char *names[] = {"全体 (同口径)", "低 1/3", "中 1/3", "高 1/3"};
    for (size_t j = 0; j < 4; ++j)
      r->conds.push_back(row_of(names[j], cond_b[j]));
  }
  return Done(std::move(r));
}

json SampleViewLayers(Feed &p) {
  static const char *kSplits[] = {"none", "month", "tod"};
  return p.window({{"factor", p.factor()}, {"split", kSplits[p.below(p.ctx.seg > 1 ? 3 : 2)]}, {"cond", p.coin(0.3) ? p.leaf() : std::string()}});
}

} // namespace GUI::Agent
