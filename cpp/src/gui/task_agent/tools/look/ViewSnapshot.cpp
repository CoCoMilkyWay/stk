// view_snapshot — 看截面快照: 输入 factor / date / time / n / exprs → SnapshotRes (本行概况 + 截面秩相关 + 两端表) → 原文; 展示见 ui/look/ViewSnapshot.cpp
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/look/Kernel.hpp"

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

std::string SnapshotRes::headline() const { return "因子 `" + label + "` · " + when + " 那一行的截面 (" + level + ")"; }

std::vector<Sec> SnapshotRes::secs() const {
  std::string cs = "与因子分位的截面秩相关:";
  for (size_t i = 0; i < corr.size(); ++i)
    cs += (i ? " · " : " ") + corr[i].first + " " + r3(corr[i].second);
  Sec x{.lines = {"在池且有秩 " + str(n) + " 只 · " +
                  (row_ok ? "rIC " + r3(ric) + " · G20 超额 " + bp(g20) + "bp (真持有 " + std::to_string(c20) + " 只) · G1 超额 " + bp(g1) + "bp (" + std::to_string(c1) + " 只)"
                          : std::string("本行无统计 (建不了仓 / 标签无效)"))},
        .plot = 0};
  x.lines.insert(x.lines.end(), exprs.begin(), exprs.end());
  x.lines.push_back(cs);
  x.tab.head = {"端", "代码", "分位", "组", "真持有", "当日涨跌"};
  if (seg > 1)
    x.tab.head.push_back("本行涨跌"), x.tab.head.push_back("日内位置");
  x.tab.head.push_back("至此额 万"), x.tab.head.push_back("后续超额 bp"), x.tab.head.push_back("盘面");
  for (size_t i = 0; i < exprs.size(); ++i)
    x.tab.head.push_back("E" + str(i + 1));
  for (const Cell &c : cells) {
    if (c.mid)
      x.tab.mark = static_cast<int>(x.tab.rows.size());
    std::vector<Num> row{c.end, c.code, c.mid ? "50" : rk(c.rank), c.mid ? std::string() : gname(c.grp),
                         c.mid || !c.held ? Num("") : Num(c.held > 0 ? "多" : "空", c.held), nchg(c.chg)};
    if (seg > 1)
      row.push_back(nchg(c.step)), row.push_back(pos2(c.pos));
    row.push_back(fmt("%.0f", c.amt / 1e4)), row.push_back(nbp(c.ex)), row.push_back(c.tags);
    for (const double v : c.fv)
      row.push_back(num(v));
    x.tab.rows.push_back(std::move(row));
  }
  return {std::move(x)};
}

std::string SnapshotRes::title() const {
  return "`" + label + "` · " + when + " · " + str(n) + " 只 · " + (row_ok ? "rIC " + r3(ric) + " · G20 " + bp(g20) + " / G1 " + bp(g1) + "bp" : std::string("本行无统计"));
}

ToolOut ViewSnapshot(const Call &c, Bay &bay) {
  const ViewEnv &env = bay.env;
  Engine::FactorFlow &F = bay.flow;
  std::string err;
  if (!date_arg_ok(c, "date", err))
    return Fail(std::move(err));
  const int seg = env.fsc.ft->seg;
  const int k_row = row_at(env.clock, c.str("time"));
  if (seg > 1 && (c.str("time").empty() || k_row < 0))
    return Fail("time: 写成本层某一行的行末时刻 HH:MM (本层 " + env.clock.front() + ", " + env.clock[1] + " … " + env.clock.back() + ")");

  FactorSpec fac;
  fac.cells = true;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return Fail(std::move(err));
  std::vector<FeatSpec> feats;
  resolve_feats(c.strs("exprs"), env, feats);

  int target_d = -1;
  std::shared_ptr<SnapshotRes> res;
  std::string thin; // 本行有秩的太少时的原文
  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac, .feats = &feats, .bars = kAllBars, .extra = {.front = seg == 1 ? 1 : 0}};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &ds, size_t &lo, size_t &hi, std::string &why) {
        target_d = day_index(ds, c.str("date"), why);
        if (target_d < 0)
          return false;
        lo = static_cast<size_t>(target_d), hi = lo + 1;
        return true;
      },
      [&](const ChunkView &cv) {
        const int u_want = target_d * seg + k_row;
        if (u_want < cv.u0 || u_want >= cv.u0 + (cv.t_hi - cv.t_lo))
          return;
        const int r_idx = u_want - cv.u0, t = cv.t_lo + r_idx, t0 = t - k_row, A = cv.blk.y.sh.A;
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const Engine::LabelPlanes &lb = *cv.blk.y.labels[0];
        const uint16_t *lv = lb.lv.data();
        const float mkt = mkt_of(cv, t);

        struct Cell {
          int a = 0, grp = -1, held = 0;
          double rank = kNaN, amt = 0.0, step = kNaN, ex = kNaN;
          Mark m;
          std::vector<double> fv;
        };
        std::vector<int> held(static_cast<size_t>(A), 0);
        for (const HeldCell &hc : cv.cells[static_cast<size_t>(r_idx)])
          held[static_cast<size_t>(hc.a)] = hc.g == st::end_group(st::kTop) ? 1 : -1;

        std::vector<Cell> cs;
        for (int a = 0; a < A; ++a) {
          const size_t q = static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a);
          if (!cv.blk.y.gate[q] || cv.rx[q] == st::kRankNone)
            continue;
          Cell x{.a = a, .grp = grp_of_r16(cv.rx[q]), .held = held[static_cast<size_t>(a)], .rank = rank_of_r16(cv.rx[q])};
          for (int q_t = t0; q_t <= t; ++q_t)
            if (const double v = px(Amount, q_t, a); std::isfinite(v))
              x.amt += v;
          const double cl = px(Close, t, a), prev = k_row == 0 ? px.pre(t, a) : px(Close, t - 1, a);
          x.step = cl / prev - 1.0;
          x.ex = lb.m[q] && opens(u_want, env) ? static_cast<double>(h2f(lv[q])) - mkt : kNaN;
          x.m = mark_of(px, t, a);
          for (size_t i = 0; i < feats.size(); ++i)
            x.fv.push_back(cv.feat_pl[i] && cv.feat_pl[i]->m[q] ? static_cast<double>(cv.feat_pl[i]->v[q]) : kNaN);
          cs.push_back(std::move(x));
        }

        const std::string when = misc::dash(dates[static_cast<size_t>(target_d)]) + (seg > 1 ? " " + env.clock[static_cast<size_t>(k_row)] : "");
        if (cs.size() < static_cast<size_t>(kMinCs)) {
          thin = "## 因子 `" + fac.label + "` · " + when + " 那一行的截面 (" + env.level + "; 分位 / 组号已定向: G20 = 做多端)\n本行在池且因子有秩的只有 " + str(cs.size()) + " 只";
          return;
        }
        std::sort(cs.begin(), cs.end(), [](const Cell &x, const Cell &y) { return x.rank > y.rank; });
        res = std::make_shared<SnapshotRes>();
        SnapshotRes &R = *res;
        R.label = fac.label, R.when = when, R.level = env.level, R.seg = seg, R.n = cs.size();
        const st::Row &w = cv.rows[static_cast<size_t>(r_idx)];
        R.row_ok = w.ok;
        R.ric = w.ric, R.g20 = w.ok ? static_cast<float>(st::grp_as(w, kG20, st::Ret::Excess)) : 0.f, R.g1 = w.ok ? static_cast<float>(st::grp_as(w, kG1, st::Ret::Excess)) : 0.f, R.c20 = static_cast<int>(w.cnt[kG20]), R.c1 = static_cast<int>(w.cnt[kG1]);
        for (size_t i = 0; i < feats.size(); ++i)
          R.exprs.push_back(expr_line(i, feats[i]));

        RankScratch rs;
        std::vector<double> cu, cv_y;
        const auto corr_fn = [&](auto &&fn) {
          cu.clear(), cv_y.clear();
          for (const Cell &x : cs)
            if (const double y = fn(x); std::isfinite(y))
              cu.push_back(x.rank), cv_y.push_back(y);
          return cu.size() >= static_cast<size_t>(kMinCs) ? rs.spearman(cu, cv_y) : kNaN;
        };
        R.corr.push_back({"当日涨跌", corr_fn([](const Cell &x) { return x.m.chg; })});
        if (seg > 1)
          R.corr.push_back({"本行涨跌", corr_fn([](const Cell &x) { return x.step; })});
        R.corr.push_back({"日内位置", corr_fn([](const Cell &x) { return x.m.pos; })});
        R.corr.push_back({"当日至此成交额", corr_fn([](const Cell &x) { return x.amt; })});
        R.corr.push_back({"后续超额", corr_fn([](const Cell &x) { return x.ex; })});
        for (size_t i = 0; i < feats.size(); ++i)
          R.corr.push_back({"E" + str(i + 1), corr_fn([i](const Cell &x) { return x.fv[i]; })});

        const AssetAxis &ax = asset_axis();
        const PoolAxis &pa = job.dm.rs.axis;
        const auto add = [&](const char *end, const Cell &x) {
          R.cells.push_back({end, ax.code(pa.global(static_cast<size_t>(x.a))), tags_of(x.m.m), false, x.rank, x.grp, x.held, x.m.chg, x.step, x.m.pos, x.amt, x.ex, x.fv});
        };
        const size_t n = std::min(cs.size() / 2, static_cast<size_t>(c.num("n")));
        for (size_t i = 0; i < n; ++i)
          add("高", cs[i]);
        std::vector<double> chs, sts, pss, ams, exs;
        for (const Cell &x : cs)
          chs.push_back(x.m.chg), sts.push_back(x.step), pss.push_back(x.m.pos), ams.push_back(x.amt), exs.push_back(x.ex);
        SnapshotRes::Cell mid{.end = "池中位", .mid = true, .rank = 0.5, .chg = median_of(chs), .step = median_of(sts), .pos = median_of(pss), .amt = median_of(ams), .ex = median_of(exs)};
        for (size_t i = 0; i < feats.size(); ++i) {
          std::vector<double> fv;
          for (const Cell &x : cs)
            fv.push_back(x.fv[i]);
          mid.fv.push_back(median_of(fv));
        }
        R.cells.push_back(std::move(mid));
        for (size_t i = 0; i < n; ++i)
          add("低", cs[cs.size() - 1 - i]);
      });

  if (rc != Engine::Rc::Ok)
    return Fail(std::move(err));
  if (!res)
    return Fail(thin);
  res->period = period_note(dates, env);
  return Done(std::move(res));
}

json SampleViewSnapshot(Feed &p) {
  return {{"factor", p.factor()}, {"date", p.day()}, {"time", p.clock()}, {"n", 6 + p.below(8)}, {"exprs", p.leaves(p.coin(0.4) ? 1 : 0)}};
}

} // namespace GUI::Agent
