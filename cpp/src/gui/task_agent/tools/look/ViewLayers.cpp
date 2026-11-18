// view_layers — 看分层: 输入 factor / split / cond → LayersRes (20 组 + 尾部 + 切分 + 条件三段) → 原文; 展示见 ui/look/ViewLayers.cpp
#include "gui/task_agent/tools/look/Kernel.hpp"

#include <map>

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

std::string LayersRes::text() const {
  std::string s = period + "## 因子 `" + label + "` (组号已定向: G20 = 做多端)\n";
  s += "有统计行 " + std::to_string(all.ok) + " / " + std::to_string(all.rows) + " · mono " + r3(all.mono) + " · 每期多空 " + bp(all.ls) + "bp · rIC 均值 " + r3(all.ric) +
       " (t " + fmt("%.1f", ric_t) + ", 正 " + pct(ric_up) + ")\n";
  s += "20 组每期超额 G1 → G20: " + spark(all.grp, bp) + " bp\n\n";

  std::vector<std::string> g_head{""}, g_ex{"每期超额 bp"}, g_cnt{"行均真持有只数"};
  double lo = INFINITY, hi = -INFINITY;
  for (int k = 0; k < kGroups; ++k) {
    g_head.push_back(gname(k));
    g_ex.push_back(bp(all.grp[static_cast<size_t>(k)]));
    g_cnt.push_back(fmt("%.1f", held[static_cast<size_t>(k)]));
    lo = std::min(lo, held[static_cast<size_t>(k)]), hi = std::max(hi, held[static_cast<size_t>(k)]);
  }
  text::Table gtbl(g_head);
  gtbl.add(g_ex);
  if (hi - lo > 0.1 * hi)
    gtbl.add(g_cnt);
  s += gtbl.str();
  if (!(hi - lo > 0.1 * hi))
    s += "\n各组行均真持有 " + fmt("%.1f", lo) + "–" + fmt("%.1f", hi) + " 只\n";

  double mid = 0.0;
  for (int k = 5; k < 15; ++k)
    mid += all.grp[static_cast<size_t>(k)] / 10.0;
  s += "\n做多端 G20 " + bp(all.grp[kG20]) + "bp · 最低组 G1 " + bp(all.grp[kG1]) + "bp · 中段 G6–G15 均值 " + bp(mid) + "bp\n";

  text::Table ttbl({"截取", "占截面", "G20 端 bp", "G20 行均只数", "G1 端 bp", "G1 行均只数", "多空 bp"});
  ttbl.add({"整组", fmt("%g%%", 100.0 / kGroups), bp(all.grp[kG20]), fmt("%.1f", held[kG20]), bp(all.grp[kG1]), fmt("%.1f", held[kG1]), bp(all.ls)});
  for (const Tail &t : tails)
    ttbl.add({"组内最极端 " + fmt("%g%%", t.frac * 100.0), fmt("%g%%", t.frac * 100.0 / kGroups), bp(t.top), fmt("%.1f", t.top_n), bp(t.bot), fmt("%.1f", t.bot_n),
              bp((t.top - t.bot) / 2.0)});
  s += "\n两端组内按池内秩再截更极端的一截 (同一份真持有判据, 满仓按宽度折算, 每期超额口径同上):\n" + ttbl.str();

  if (split == "tod" && seg == 1) {
    s += "\n(日层每天一行, 没有日内时段可切)\n";
  } else if (split != "none") {
    text::Table stbl = layer_table(split == "tod" ? "时段" : "月份");
    for (const LayerRow &b : splits)
      add_layer_row(stbl, b);
    s += "\n" + stbl.str();
  }

  if (!cond.empty()) {
    s += "\n## 按条件三等分: `" + cond + "`\n";
    if (!cond_err.empty()) {
      s += "条件没算成: " + cond_err + "\n";
    } else if (!has_cond) {
      s += "没有可用的行 (每行至少要 " + std::to_string(3 * kMinCs) + " 个在池且因子 / 标签 / 条件都有效的格)\n";
    } else {
      s += "条件与因子的截面秩相关 " + r3(cond_rho) + " · 条件自身 rIC " + r3(cond_ric) + "\n";
      s += "每行按条件值把池子截面三等分 (条件离散时并列落同一段, 三段不均), 段内按因子分位重新分 20 组; 组超额 = 组内 (标签 − 本行在池均值) 等权均值, 建不了仓的格不剔, "
           "与上面的真持有口径略有出入, 只作这四行之间对照\n\n";
      text::Table ctbl = layer_table("条件段");
      for (const LayerRow &b : conds)
        add_layer_row(ctbl, b);
      s += ctbl.str();
    }
  }
  return s;
}

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
  FactorSpec fac;
  fac.tails = true;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return Fail(std::move(err));
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

  Layer all;
  std::vector<Layer> by_tod(ps.size());
  std::map<int, Layer> by_month;
  std::array<Layer, 4> cond_b;
  Mean cond_rho, cond_ric;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac, .feats = feats.empty() ? nullptr : &feats};
  const Engine::Rc rc = run_stream(env, F, cfg, dates, job, err, whole, [&](const ChunkView &cv) {
    const int n_stat = cv.t_hi - cv.t_lo;
    for (int r = 0; r < n_stat; ++r) {
      const int u = cv.u0 + r;
      all.add(cv.rows[static_cast<size_t>(r)], &cv.tails[static_cast<size_t>(r)]);
      if (split == "tod" && seg > 1)
        by_tod[static_cast<size_t>(pr[static_cast<size_t>(u % seg)])].add(cv.rows[static_cast<size_t>(r)]);
      else if (split == "month")
        by_month[cv.dates[static_cast<size_t>(u / seg)] / 100].add(cv.rows[static_cast<size_t>(r)]);
    }
    if (cond.empty() || !cv.feat_pl[0])
      return;
    const int A = cv.blk.y.sh.A, total_T = static_cast<int>(cv.dates.size()) * seg;
    const int rstep = std::max(1, static_cast<int>(static_cast<size_t>(total_T) * static_cast<size_t>(A) / kCsCells));
    const factor::check::Plane *cp = cv.feat_pl[0];
    const Engine::LegPlane &lb = *cv.blk.y.labels[0];
    const uint16_t *lv = lb.v[st::kLong].data();
    const int nt = F.S.ex.threads();
    std::vector<std::array<Layer, 4>> tb(static_cast<size_t>(nt));
    std::vector<Mean> trho(static_cast<size_t>(nt)), tric(static_cast<size_t>(nt));

    F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
      std::vector<double> cval, fxv, ey, rc_v, rf_v, ry_v;
      std::array<std::vector<double>, 3> px, py;
      RankScratch rs;
      st::Row w;
      const size_t ut = static_cast<size_t>(tid);
      for (int r = r0; r < r1; ++r) {
        const int t = cv.t_lo + r, u = cv.u0 + r;
        if (u % rstep != 0)
          continue;
        const float mkt = row_mkt(cv.blk, t);
        if (!std::isfinite(mkt))
          continue;
        const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
        cval.clear(), fxv.clear(), ey.clear();
        for (int a = 0; a < A; ++a) {
          const size_t k = base + static_cast<size_t>(a);
          if (cv.blk.y.gate[k] && cv.rx[k] != st::kRankNone && lb.m[k] && cp->m[k] && std::isfinite(cp->v[k]))
            cval.push_back(cp->v[k]), fxv.push_back(rank_of_r16(cv.rx[k])), ey.push_back(static_cast<double>(h2f(lv[k])) - mkt);
        }
        const size_t n = cval.size();
        if (n < 3 * static_cast<size_t>(kMinCs))
          continue;
        rs.rank_into(cval, rc_v), rs.rank_into(fxv, rf_v), rs.rank_into(ey, ry_v);
        trho[ut].add(pearson(rc_v, rf_v)), tric[ut].add(pearson(rc_v, ry_v));
        for (size_t j = 0; j < 3; ++j)
          px[j].clear(), py[j].clear();
        for (size_t i = 0; i < n; ++i) {
          const size_t j = std::min<size_t>(2, static_cast<size_t>(rc_v[i] * 3.0 / static_cast<double>(n)));
          px[j].push_back(fxv[i]), py[j].push_back(ey[i]);
        }
        if (rs.approx_ranked(rf_v, ey, ry_v, w))
          tb[ut][0].add(w);
        for (size_t j = 0; j < 3; ++j)
          if (rs.approx_row(px[j], py[j], w))
            tb[ut][j + 1].add(w);
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
  r->period = period_note(dates, env);
  r->label = fac.label, r->split = split, r->cond = cond, r->seg = seg;
  r->all = row_of("全期", all);
  r->ric_t = all.ric.t(), r->ric_up = all.ric.up();
  for (int k = 0; k < kGroups; ++k)
    r->held.push_back(all.held(k));
  for (int j = 0; j < kTails; ++j)
    r->tails.push_back({Engine::kTailOfGroup[j], all.tail(0, j), all.tail_held(0, j), all.tail(1, j), all.tail_held(1, j)});
  if (split == "tod" && seg > 1)
    for (size_t i = 0; i < ps.size(); ++i)
      r->splits.push_back(row_of(ps[i].name, by_tod[i]));
  else if (split == "month")
    for (const auto &[m, b] : by_month)
      r->splits.push_back(row_of(date_of(m * 100 + 1).substr(0, 7), b));
  if (!cond.empty()) {
    r->cond_err = feats[0].err;
    r->has_cond = cond_b[0].ok > 0;
    r->cond_rho = cond_rho.mean(), r->cond_ric = cond_ric.mean();
    const char *names[] = {"全体 (同口径)", "低 1/3", "中 1/3", "高 1/3"};
    for (size_t j = 0; j < 4; ++j)
      r->conds.push_back(row_of(names[j], cond_b[j]));
  }
  return Done(std::move(r));
}

std::string RepLayers(const std::string &expr, int sign, const ViewEnv &env, Engine::FactorFlow &F) {
  const Engine::FeatureTable &ft = *env.fsc.ft;
  FactorSpec fac{.e = ft.reparse(expr), .dir = st::dir_of(sign), .label = expr};
  std::string dummy;
  [[maybe_unused]] const bool ok_rf = fx::root_frame(fac.e, ft.seg, fac.frame, dummy);
  assert(ok_rf);
  const int seg = ft.seg;
  const std::vector<Slot> ps = parts_of(seg, env.open, env.clock);
  const std::vector<int> pr = part_of_row(ps, seg);
  Layer all;
  std::vector<Layer> bs(ps.size());
  std::vector<misc::Ymd> dates;
  Engine::Job job;
  std::string err;
  StreamCfg cfg{.fac = &fac};
  const Engine::Rc rc = run_stream(env, F, cfg, dates, job, err, whole, [&](const ChunkView &cv) {
    for (int r = 0; r < cv.t_hi - cv.t_lo; ++r) {
      all.add(cv.rows[static_cast<size_t>(r)]);
      if (seg > 1)
        bs[static_cast<size_t>(pr[static_cast<size_t>((cv.u0 + r) % seg)])].add(cv.rows[static_cast<size_t>(r)]);
    }
  });
  if (rc != Engine::Rc::Ok)
    return err.empty() ? std::string() : "- 代表点" + err + "\n";
  std::string s = "- 代表点的 20 组每期超额 G1 → G20: " + spark(all.grps(), bp) + " bp · mono " + r3(all.mono()) + " · 多空 " + bp(all.ls()) + "bp\n";
  if (seg > 1) {
    s += "- 代表点分时段 mono:";
    for (size_t j = 0; j < ps.size(); ++j)
      s += (j ? " · " : " ") + ps[j].name + " " + r3(bs[j].mono());
    s += "\n";
  }
  return s + "- 观察工具的 factor 写本候选名即看代表点\n";
}

json SampleViewLayers(Feed &p) {
  static const char *kSplits[] = {"none", "tod", "month"};
  return {{"factor", p.factor()}, {"split", kSplits[p.below(3)]}, {"cond", p.coin(0.3) ? p.leaf() : std::string()}};
}

} // namespace GUI::Agent
