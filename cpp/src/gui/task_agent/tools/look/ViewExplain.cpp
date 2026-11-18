// view_explain — 看成分: 输入 factor / exprs → ExplainRes (各基准的相关 / 自身 rIC / 只剥它后的 rIC + 全剥离对照) → 原文; 展示见 ui/look/ViewExplain.cpp
#include "gui/task_agent/tools/look/Kernel.hpp"

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

namespace {

std::vector<double> centered_ranks(const std::vector<double> &x) {
  std::vector<double> r = ranks_of(x);
  const double m = 0.5 * static_cast<double>(x.size() - 1);
  for (double &v : r)
    v -= m;
  return r;
}
double dot(const std::vector<double> &a, const std::vector<double> &b) { return std::inner_product(a.begin(), a.end(), b.begin(), 0.0); }

std::vector<double> resid_of(const std::vector<std::vector<double>> &X, const std::vector<double> &y) {
  const size_t m = X.size();
  std::vector<double> g(m * m), h(m), beta(m);
  double tr = 0.0;
  for (size_t i = 0; i < m; ++i) {
    for (size_t j = 0; j <= i; ++j)
      g[i * m + j] = g[j * m + i] = dot(X[i], X[j]);
    h[i] = dot(X[i], y), tr += g[i * m + i];
  }
  for (size_t i = 0; i < m; ++i)
    g[i * m + i] += 1e-9 * tr / static_cast<double>(m) + 1e-12;
  for (size_t p = 0; p < m; ++p)
    for (size_t i = p + 1; i < m; ++i) {
      const double f = g[i * m + p] / g[p * m + p];
      for (size_t j = p; j < m; ++j)
        g[i * m + j] -= f * g[p * m + j];
      h[i] -= f * h[p];
    }
  for (size_t p = m; p-- > 0;) {
    double s = h[p];
    for (size_t j = p + 1; j < m; ++j)
      s -= g[p * m + j] * beta[j];
    beta[p] = s / g[p * m + p];
  }
  std::vector<double> res = y;
  for (size_t j = 0; j < m; ++j)
    for (size_t i = 0; i < res.size(); ++i)
      res[i] -= beta[j] * X[j][i];
  return res;
}

} // namespace

std::string ExplainRes::head() const {
  std::string s = period + "## 因子 `" + label + "` 的成分\n";
  for (const std::string &e : exprs)
    s += e;
  return s;
}

std::string ExplainRes::text() const {
  std::string s = head();
  s += "抽 " + str(rows) + " 行, 每行取在池、标签有效且全部基准有值的格; 收益类基准是对数收益减同行在池均值 (跨日按昨收接); 相关与 rIC 都是逐行截面 Spearman 的均值\n";
  s += "因子 rIC (同口径) " + r3(fric) + "\n\n";
  text::Table btbl({"基准", "与因子的截面秩相关", "基准自身 rIC", "只剥它后的因子 rIC"});
  for (const Base &b : bases)
    btbl.add({b.name, r3(b.corr), r3(b.ric), r3(b.rest)});
  s += btbl.str();
  s += "\n全部基准一起剥 (每行把因子秩对全部基准秩做截面回归, 取残差): 平均 R² " + r3(r2) + " · 因子 rIC " + r3(fric) + " → 剩余部分 " + r3(rric) + "\n";
  s += "20 组按标签逐行近似分 (组超额 = 组内 (标签 − 本行在池均值) 等权均值, 建不了仓的格不剔), 与回测 / view_layers 的真持有口径略有出入, 只作两行之间对照\n\n";
  text::Table ltbl = layer_table("");
  add_layer_row(ltbl, orig);
  add_layer_row(ltbl, rest);
  return s + ltbl.str();
}

std::string ExplainRes::gist() const {
  return "rIC " + r3(fric) + " → 剥全部基准剩 " + r3(rric) + " (R² " + r3(r2) + ") · 剩余部分 mono " + r3(rest.mono) + " · 多空 " + bp(rest.ls) + "bp";
}

ToolOut ViewExplain(const Call &c, Bay &bay) {
  const ViewEnv &env = bay.env;
  Engine::FactorFlow &F = bay.flow;
  std::string err;
  FactorSpec fac;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return Fail(std::move(err));
  std::vector<FeatSpec> feats;
  resolve_feats(c.strs("exprs"), env, feats);

  const int seg = env.fsc.ft->seg;
  const int lag = seg > 1 ? 6 * seg : 21;
  std::vector<std::string> b_names;
  std::vector<int> ret_lags; // 内置近 w 行收益基准
  if (seg > 1) {
    for (const int w : {5, 30})
      if (w < seg)
        b_names.push_back("近 " + std::to_string(w) + " 行收益"), ret_lags.push_back(w);
    for (const char *nm : {"当日至今收益", "前 1 日收益", "前 5 日收益", "当日至今成交额", "当日至今振幅", "日内位置", "价格"})
      b_names.emplace_back(nm);
  } else {
    for (const int w : {1, 5, 20})
      b_names.push_back(w == 1 ? "当日收益" : "近 " + std::to_string(w) + " 日收益"), ret_lags.push_back(w);
    for (const char *nm : {"成交额", "振幅", "价格"})
      b_names.emplace_back(nm);
  }
  const size_t n_builtin = b_names.size();
  for (size_t i = 0; i < feats.size(); ++i)
    if (feats[i].err.empty())
      b_names.push_back("E" + str(i + 1));

  // 由于运行期可能有坏根特征, 在首块 plan_forest 后按实际有效特征定列
  Mean fric, r2, rric;
  std::vector<Mean> corr, bric, sric;
  Layer orig, rest;
  std::vector<size_t> active_feats;
  bool cols_fixed = false;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac,
                .feats = &feats,
                .bars = bit(Close) | bit(High) | bit(Low) | bit(Volume) | bit(Amount) | bit(PreClose),
                .extra_front = seg > 1 ? 7 : 22,
                .extra_work = sizeof(double)};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &ds, size_t &, size_t &, std::string &why) {
        if (static_cast<int>(ds.size()) * seg <= lag) {
          why = "## 因子 `" + fac.label + "` 的成分\n观察的日段太短: 基准要回看 " + std::to_string(lag) + " 行\n";
          return false;
        }
        return true;
      },
      [&](const ChunkView &cv) {
        if (!cols_fixed) {
          b_names.resize(n_builtin);
          for (size_t i = 0; i < feats.size(); ++i)
            if (cv.feat_pl[i]) {
              active_feats.push_back(i);
              b_names.push_back("E" + str(i + 1));
            }
          const size_t m = b_names.size();
          corr.resize(m), bric.resize(m), sric.resize(m);
          cols_fixed = true;
        }
        const size_t m = b_names.size();
        const int T = cv.blk.y.sh.T, A = cv.blk.y.sh.A, n_stat = cv.t_hi - cv.t_lo, total_T = static_cast<int>(cv.dates.size()) * seg;
        const int rstep = std::max(1, (total_T - lag) / kExplainRows);
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const std::vector<float> e = block_excess_steps(F.S.ex, cv.blk, px, cv.blk.land0());
        std::vector<double> cum(e.size());
        F.S.ex.rows(A, [&](int a0, int a1, int) {
          for (int a = a0; a < a1; ++a) {
            double acc = 0.0;
            for (int t = 0; t < T; ++t)
              cum[static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a)] = acc += e[static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a)];
          }
        });
        const auto ret = [&](int a, int t1, int t0) {
          return cum[static_cast<size_t>(t1) * static_cast<size_t>(A) + static_cast<size_t>(a)] - cum[static_cast<size_t>(t0) * static_cast<size_t>(A) + static_cast<size_t>(a)];
        };

        const Engine::LegPlane &lb = *cv.blk.y.labels[0];
        const uint16_t *lv = lb.v[st::kLong].data();
        const int nt = F.S.ex.threads();
        struct ThreadAcc {
          Mean fric, r2, rric;
          std::vector<Mean> corr, bric, sric;
          Layer orig, rest;
        };
        std::vector<ThreadAcc> ta(static_cast<size_t>(nt));
        for (ThreadAcc &x : ta)
          x.corr.resize(m), x.bric.resize(m), x.sric.resize(m);

        F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
          ThreadAcc &loc = ta[static_cast<size_t>(tid)];
          std::vector<double> fxv, ey, one(m);
          std::vector<std::vector<double>> bv(m), X(m);
          st::Row w;
          for (int r = r0; r < r1; ++r) {
            const int t = cv.t_lo + r, u = cv.u0 + r;
            if (u < lag || (u - lag) % rstep != 0)
              continue;
            const float mkt = row_mkt(cv.blk, t);
            if (!std::isfinite(mkt))
              continue;
            assert(t >= lag);
            const int t_day0 = t - t % seg;
            const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
            fxv.clear(), ey.clear();
            for (std::vector<double> &v : bv)
              v.clear();

            for (int a = 0; a < A; ++a) {
              const size_t k = base + static_cast<size_t>(a);
              if (!cv.blk.y.gate[k] || cv.rx[k] == st::kRankNone || !lb.m[k])
                continue;
              size_t col = 0;
              for (const int rw : ret_lags)
                one[col++] = ret(a, t, t - rw);
              if (seg > 1) {
                double damt = 0.0, dhi = -INFINITY, dlo = INFINITY;
                for (int q = t_day0; q <= t; ++q) {
                  if (const double av = px(Amount, q, a); std::isfinite(av))
                    damt += av;
                  if (px.traded(q, a))
                    dhi = std::fmax(dhi, px(High, q, a)), dlo = std::fmin(dlo, px(Low, q, a));
                }
                const double pre = px.pre(t, a), cl = px(Close, t, a);
                one[col++] = ret(a, t, t_day0 - 1);
                one[col++] = ret(a, t_day0 - 1, t_day0 - 1 - seg);
                one[col++] = ret(a, t_day0 - 1, t_day0 - 1 - 5 * seg);
                one[col++] = damt;
                one[col++] = dhi > dlo ? (dhi - dlo) / pre : 0.0;
                one[col++] = dhi > dlo ? (cl - dlo) / (dhi - dlo) : 0.5;
                one[col++] = cl;
              } else {
                one[col++] = px(Amount, t, a);
                one[col++] = (px(High, t, a) - px(Low, t, a)) / px.pre(t, a);
                one[col++] = px(Close, t, a);
              }
              bool ok = true;
              for (const size_t fi : active_feats) {
                const factor::check::Plane *fp = cv.feat_pl[fi];
                one[col++] = fp->m[k] ? static_cast<double>(fp->v[k]) : kNaN;
              }
              assert(col == m);
              for (size_t j = 0; j < m && ok; ++j)
                ok = std::isfinite(one[j]);
              if (!ok)
                continue;
              fxv.push_back(rank_of_r16(cv.rx[k]));
              ey.push_back(static_cast<double>(h2f(lv[k])) - mkt);
              for (size_t j = 0; j < m; ++j)
                bv[j].push_back(one[j]);
            }
            if (fxv.size() < static_cast<size_t>(kMinCs))
              continue;
            const std::vector<double> rf = centered_ranks(fxv), ry = ranks_of(ey);
            loc.fric.add(pearson(rf, ry));
            for (size_t j = 0; j < m; ++j) {
              X[j] = centered_ranks(bv[j]);
              loc.corr[j].add(pearson(rf, X[j])), loc.bric[j].add(pearson(X[j], ry));
              const double xx = dot(X[j], X[j]), beta = xx > 0.0 ? dot(X[j], rf) / xx : 0.0;
              std::vector<double> res = rf;
              for (size_t i = 0; i < res.size(); ++i)
                res[i] -= beta * X[j][i];
              loc.sric[j].add(pearson(ranks_of(res), ry));
            }
            const std::vector<double> res = resid_of(X, rf);
            const double tot = dot(rf, rf);
            if (tot > 0.0)
              loc.r2.add(1.0 - dot(res, res) / tot);
            loc.rric.add(pearson(ranks_of(res), ry));
            if (approx_row(fxv, ey, w))
              loc.orig.add(w);
            if (approx_row(res, ey, w))
              loc.rest.add(w);
          }
        });
        for (const ThreadAcc &loc : ta) {
          fric.merge(loc.fric), r2.merge(loc.r2), rric.merge(loc.rric);
          orig.merge(loc.orig), rest.merge(loc.rest);
          for (size_t j = 0; j < m; ++j)
            corr[j].merge(loc.corr[j]), bric[j].merge(loc.bric[j]), sric[j].merge(loc.sric[j]);
        }
      });

  if (rc != Engine::Rc::Ok)
    return Fail(std::move(err));

  auto res = std::make_shared<ExplainRes>();
  ExplainRes &R = *res;
  R.period = period_note(dates, env), R.label = fac.label;
  for (size_t i = 0; i < feats.size(); ++i)
    R.exprs.push_back(expr_line(i, feats[i], "没算成, 不进基准"));
  if (!fric.n)
    return Fail(R.head() + "没有可用的行 (每行至少要 " + std::to_string(kMinCs) + " 个在池、标签有效且全部基准有值的格)\n");
  R.rows = fric.n, R.fric = fric.mean(), R.r2 = r2.mean(), R.rric = rric.mean();
  for (size_t j = 0; j < b_names.size(); ++j)
    R.bases.push_back({b_names[j], corr[j].mean(), bric[j].mean(), sric[j].mean()});
  R.orig = row_of("原因子", orig), R.rest = row_of("剩余部分", rest);
  return Done(std::move(res));
}

json SampleViewExplain(Feed &p) {
  json xs = json::array();
  for (int i = 0, n = p.below(3); i < n; ++i)
    xs.push_back(p.leaf());
  return {{"factor", p.factor()}, {"exprs", xs}};
}

} // namespace GUI::Agent
