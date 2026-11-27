// view_feature — 看特征: 输入 exprs / from / to → FeatureRes (分布 / 方差来源 / 持续性 / 日内轮廓 / rIC / 两两相关) → 原文; 展示见 ui/look/ViewFeature.cpp
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/look/Kernel.hpp"

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

std::string FeatureRes::headline() const { return "特征 " + str(feats.size()) + " 个式子"; }

std::vector<Sec> FeatureRes::secs() const {
  std::vector<Sec> out;
  for (size_t i = 0; i < feats.size(); ++i) {
    const Feat &f = feats[i];
    Sec &x = out.emplace_back();
    x.title = "E" + str(i + 1) + " `" + f.raw + "`";
    if (!f.err.empty() || f.ok < 100) {
      x.lines = {!f.err.empty() ? "没算成: " + f.err : "池内有效格太少 (" + str(f.ok) + " / " + str(f.in) + ")"}, x.warn = true;
      continue;
    }
    x.lines.push_back("池内有效 " + pct(static_cast<double>(f.ok) / static_cast<double>(f.in)) + " (" + str(f.ok) + " 格) · 均值 " + num(f.mean) + " · 标准差 " + num(f.sd) +
                      (f.zero >= 0.05 ? " · 零值占 " + pct(f.zero) : std::string()) + (f.levels ? " · 离散: 只有 " + str(f.levels) + " 种取值" : std::string()));
    if (f.var)
      x.lines.push_back("方差来自: 资产间 " + pct(f.v_asset) + " · 日间 " + pct(f.v_day) + (seg > 1 ? " · 日内 " + pct(f.v_intra) : std::string()));
    std::string lag = "持续性:";
    for (size_t li = 0; li < lags.size(); ++li)
      lag += " " + lags[li] + " " + r3(f.lag[li]);
    x.lines.push_back(lag);
    x.lines.push_back("rIC " + r3(f.ric) + " · t " + fmt("%.1f", f.ric_t) + " · 正 rIC 行 " + pct(f.ric_up));
    if (seg > 1) {
      std::string part = "分时段 rIC:";
      for (size_t j = 0; j < parts.size(); ++j)
        part += " " + parts[j] + " " + r3(f.ric_part[j]);
      x.lines.push_back(part);
    }
    if (seg > 1 && f.slow) {
      x.lines.push_back("日内轮廓: 日内方差不到 " + fmt("%g%%", kIntraNone * 100.0) + " (日频慢变量), 不列");
    } else if (seg > 1) {
      std::string ticks;
      for (const int k : part_end)
        ticks += " " + clock[static_cast<size_t>(k)] + " " + num(f.prof[static_cast<size_t>(k)]);
      x.sparks.push_back("日内轮廓 " + spark(f.prof) + " ·" + ticks);
    } else {
      x.sparks.push_back("截面中位数逐日 " + spark(f.prof));
    }
    x.plot = static_cast<int>(i);
    x.tab.head = {"p1", "p5", "p25", "p50", "p75", "p95", "p99"};
    x.tab.rows.emplace_back();
    for (const double q : f.q)
      x.tab.rows.back().push_back(num(q));
  }
  if (good.size() >= 2) {
    Sec &x = out.emplace_back();
    x.title = "两两截面秩相关";
    x.tab.head = {""};
    for (const size_t idx : good)
      x.tab.head.push_back("E" + str(idx + 1));
    for (size_t i = 0; i < good.size(); ++i) {
      std::vector<Num> row{"E" + str(good[i] + 1)};
      for (size_t j = 0; j < good.size(); ++j)
        row.push_back(j == i ? Num("1") : nr3(corr[i * good.size() + j]));
      x.tab.rows.push_back(std::move(row));
    }
  }
  return out;
}

std::string FeatureRes::title() const {
  std::string s;
  for (const Feat &f : feats)
    s += (s.empty() ? "" : "; ") + text::head(f.raw, kGistExpr) + (!f.err.empty() ? std::string(" 没算成") : f.ok < 100 ? std::string(" 有效格太少")
                                                                                                                        : " rIC " + r3(f.ric));
  return s;
}

std::string FeatureRes::gist() const {
  std::string g;
  for (const size_t i : good)
    g += (g.empty() ? "" : "; ") + text::head(feats[i].raw, kGistExpr) + " rIC " + r3(feats[i].ric) + " · 隔 1 日自相关 " + r3(feats[i].day1);
  return g;
}

ToolOut ViewFeature(const Call &c, Bay &bay) {
  const ViewEnv &env = bay.env;
  Engine::FactorFlow &F = bay.flow;
  std::string err;
  Window win;
  if (!window_of(c, win, err))
    return Fail(std::move(err));
  std::vector<FeatSpec> feats;
  if (!resolve_feats(c.strs("exprs"), env, feats)) {
    for (size_t i = 0; i < feats.size(); ++i)
      err += (i ? "; " : "") + ("exprs[" + str(i) + "]: " + feats[i].err);
    return Fail(std::move(err));
  }
  const int seg = env.fsc.ft->seg;
  const std::vector<Slot> ps = parts_of(seg, env.open, env.clock);
  const std::vector<int> pr = part_of_row(ps, seg);
  const std::vector<std::pair<int, const char *>> lags =
      seg > 1 ? std::vector<std::pair<int, const char *>>{{1, "隔 1 行"}, {seg, "隔 1 日"}, {5 * seg, "隔 5 日"}}
              : std::vector<std::pair<int, const char *>>{{1, "隔 1 日"}, {5, "隔 5 日"}, {20, "隔 20 日"}};

  struct LagAcc {
    double xy = 0.0, x = 0.0, y = 0.0, xx = 0.0, yy = 0.0;
    size_t n = 0;
  };
  struct Acc {
    size_t in = 0, ok = 0;
    std::vector<double> smp;
    double lo = -INFINITY, hi = INFINITY;
    bool bounds_ready = false;
    double sx = 0.0, sxx = 0.0, sum_day_sq = 0.0;
    size_t n_val = 0;
    std::vector<double> sa;
    std::vector<size_t> na;
    std::array<std::vector<LagAcc>, 3> lag;
    std::vector<double> med;
    Mean ric;
    std::vector<Mean> ric_part;
  };
  const size_t F_n = feats.size();
  std::vector<Acc> accs(F_n);
  std::vector<Mean> pair_corr(F_n * F_n);

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.feats = &feats, .extra = {.front = seg > 1 ? 5 : 20}};
  const Engine::Rc rc = run_stream(env, F, cfg, dates, job, err, win, [&](const ChunkView &cv) {
    const int A = cv.blk.y.sh.A, n_stat = cv.t_hi - cv.t_lo, days_stat = n_stat / seg;
    const int total_T = static_cast<int>(cv.dates.size()) * seg;
    const size_t win_N = static_cast<size_t>(cv.w_rows()) * static_cast<size_t>(A);
    const size_t st_smp = std::max<size_t>(1, win_N / kSample);
    const int rstep = row_step(static_cast<int>(win_N / kCsCells), seg);
    const int cstep = row_step(cv.w_rows() / kCorrRows, seg);
    const Engine::LabelPlanes &lb = *cv.blk.y.labels[0];
    const uint16_t *lv = lb.lv.data();
    const int nt = F.S.ex.threads();

    for (size_t i = 0; i < F_n; ++i) {
      const factor::check::Plane *pl = cv.feat_pl[i];
      if (!pl)
        continue;
      Acc &ac = accs[i];
      if (ac.sa.empty()) {
        ac.sa.assign(static_cast<size_t>(A), 0.0);
        ac.na.assign(static_cast<size_t>(A), 0);
        for (auto &lg : ac.lag)
          lg.assign(static_cast<size_t>(A), {});
        ac.med.assign(static_cast<size_t>(total_T), kNaN);
        ac.ric_part.resize(ps.size());
      }
      // 1) 池内有效格计数与等距抽样 (首有值块即定 1%~99% 截尾界, 不漏算首块方差)
      const size_t base0 = static_cast<size_t>(cv.t_lo) * static_cast<size_t>(A), end0 = static_cast<size_t>(cv.t_hi) * static_cast<size_t>(A);
      const size_t ok_prev = ac.ok;
      for (size_t k = base0; k < end0; ++k)
        if (cv.blk.y.gate[k]) {
          ++ac.in;
          if (pl->m[k] && std::isfinite(pl->v[k]) && ac.ok++ % st_smp == 0)
            ac.smp.push_back(pl->v[k]);
        }
      if (ac.ok == ok_prev)
        continue;
      if (!ac.bounds_ready) {
        std::vector<double> tmp = ac.smp;
        if (tmp.size() < 20) {
          tmp.clear();
          for (size_t k = base0; k < end0 && tmp.size() < 200; ++k)
            if (cv.blk.y.gate[k] && pl->m[k] && std::isfinite(pl->v[k]))
              tmp.push_back(pl->v[k]);
        }
        if (tmp.size() >= 2) {
          std::sort(tmp.begin(), tmp.end());
          ac.lo = quant(tmp, 0.01), ac.hi = quant(tmp, 0.99);
        }
        if (ac.smp.size() >= 20 || tmp.size() >= 20)
          ac.bounds_ready = true;
      }

      // 2) 按资产条带全核并行, 条带内逐行扫 (行主序 [T][A]: 一行里条带连续): 单趟攒方差分解 (ANOVA 恒等式) + 三档滞后自相关
      std::vector<double> t_sx(static_cast<size_t>(nt), 0.0), t_sxx(static_cast<size_t>(nt), 0.0), t_dsq(static_cast<size_t>(nt), 0.0);
      std::vector<size_t> t_n(static_cast<size_t>(nt), 0);
      const auto clamp_v = [&](float v) { return std::clamp(static_cast<double>(v), ac.lo, ac.hi); };
      F.S.ex.rows(A, [&](int a0, int a1, int tid) {
        double lsx = 0.0, lsxx = 0.0, ldsq = 0.0;
        size_t ln = 0;
        const size_t W = static_cast<size_t>(a1 - a0);
        std::vector<double> ds(W);
        std::vector<int> dn(W);
        for (int d = 0; d < days_stat; ++d) {
          std::fill(ds.begin(), ds.end(), 0.0), std::fill(dn.begin(), dn.end(), 0);
          for (int r = 0; r < seg; ++r) {
            const int t = cv.t_lo + d * seg + r, u = cv.u0 + d * seg + r;
            const size_t row = static_cast<size_t>(t) * static_cast<size_t>(A);
            for (int a = a0; a < a1; ++a) {
              const size_t ua = static_cast<size_t>(a), j = static_cast<size_t>(a - a0), k1 = row + ua;
              if (!cv.blk.y.gate[k1] || !pl->m[k1] || !std::isfinite(pl->v[k1]))
                continue;
              const double x = clamp_v(pl->v[k1]);
              ac.sa[ua] += x, ++ac.na[ua];
              ds[j] += x, ++dn[j];
              lsx += x, lsxx += x * x, ++ln;
              for (size_t li = 0; li < lags.size(); ++li) {
                const int lag = lags[li].first;
                if (u < lag)
                  continue;
                const size_t k0 = static_cast<size_t>(t - lag) * static_cast<size_t>(A) + ua;
                if (!cv.blk.y.gate[k0] || !pl->m[k0] || !std::isfinite(pl->v[k0]))
                  continue;
                const double y = clamp_v(pl->v[k0]);
                LagAcc &la = ac.lag[li][ua];
                la.xy += x * y, la.x += x, la.y += y, la.xx += x * x, la.yy += y * y, ++la.n;
              }
            }
          }
          for (size_t j = 0; j < W; ++j)
            if (dn[j] > 0)
              ldsq += ds[j] * ds[j] / dn[j];
        }
        const size_t ut = static_cast<size_t>(tid);
        t_sx[ut] += lsx, t_sxx[ut] += lsxx, t_dsq[ut] += ldsq, t_n[ut] += ln;
      });
      for (int tid = 0; tid < nt; ++tid) {
        const size_t ut = static_cast<size_t>(tid);
        ac.sx += t_sx[ut], ac.sxx += t_sxx[ut], ac.sum_day_sq += t_dsq[ut], ac.n_val += t_n[ut];
      }

      // 3) 按统计行全核并行: 截面中位数 (原地 nth_element 复用缓冲) + 抽样行 rIC (零堆分配)
      std::vector<Mean> t_ric(static_cast<size_t>(nt));
      std::vector<std::vector<Mean>> t_part(static_cast<size_t>(nt), std::vector<Mean>(ps.size()));
      F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
        std::vector<double> cs, xa, ya;
        RankScratch rs;
        const size_t ut = static_cast<size_t>(tid);
        for (int r = r0; r < r1; ++r) {
          const int t = cv.t_lo + r, u = cv.u0 + r;
          const size_t base = static_cast<size_t>(t) * static_cast<size_t>(A);
          cs.clear();
          for (int a = 0; a < A; ++a)
            if (const size_t k = base + static_cast<size_t>(a); cv.blk.y.gate[k] && pl->m[k] && std::isfinite(pl->v[k]))
              cs.push_back(pl->v[k]);
          if (cs.size() >= 5) {
            const auto mid = cs.begin() + static_cast<std::ptrdiff_t>(cs.size() / 2);
            std::nth_element(cs.begin(), mid, cs.end());
            ac.med[static_cast<size_t>(u)] = *mid;
          }
          if (u % rstep != 0 || !opens(u, env))
            continue;
          xa.clear(), ya.clear();
          for (int a = 0; a < A; ++a)
            if (const size_t k = base + static_cast<size_t>(a); cv.blk.y.gate[k] && pl->m[k] && std::isfinite(pl->v[k]) && lb.m[k])
              xa.push_back(pl->v[k]), ya.push_back(h2f(lv[k]));
          if (xa.size() >= static_cast<size_t>(kMinCs)) {
            const double rho = rs.spearman(xa, ya);
            t_ric[ut].add(rho);
            t_part[ut][static_cast<size_t>(pr[static_cast<size_t>(u % seg)])].add(rho);
          }
        }
      });
      for (int tid = 0; tid < nt; ++tid) {
        ac.ric.merge(t_ric[static_cast<size_t>(tid)]);
        for (size_t j = 0; j < ps.size(); ++j)
          ac.ric_part[j].merge(t_part[static_cast<size_t>(tid)][j]);
      }
    }

    // 4) 两两截面相关 (抽样行全核并行, 零堆分配)
    if (F_n >= 2) {
      std::vector<std::vector<Mean>> t_corr(static_cast<size_t>(nt), std::vector<Mean>(F_n * F_n));
      F.S.ex.rows(n_stat, [&](int r0, int r1, int tid) {
        std::vector<double> xa, ya;
        RankScratch rs;
        std::vector<Mean> &mc = t_corr[static_cast<size_t>(tid)];
        for (int r = r0; r < r1; ++r) {
          const int u = cv.u0 + r;
          if (u % cstep != 0)
            continue;
          const size_t base = static_cast<size_t>(cv.t_lo + r) * static_cast<size_t>(A);
          for (size_t i = 0; i < F_n; ++i)
            for (size_t j = i + 1; j < F_n; ++j) {
              const factor::check::Plane *px = cv.feat_pl[i], *py = cv.feat_pl[j];
              if (!px || !py)
                continue;
              xa.clear(), ya.clear();
              for (int a = 0; a < A; ++a)
                if (const size_t k = base + static_cast<size_t>(a);
                    cv.blk.y.gate[k] && px->m[k] && py->m[k] && std::isfinite(px->v[k]) && std::isfinite(py->v[k]))
                  xa.push_back(px->v[k]), ya.push_back(py->v[k]);
              if (xa.size() >= static_cast<size_t>(kMinCs))
                mc[i * F_n + j].add(rs.spearman(xa, ya));
            }
        }
      });
      for (int tid = 0; tid < nt; ++tid)
        for (size_t idx = 0; idx < F_n * F_n; ++idx)
          pair_corr[idx].merge(t_corr[static_cast<size_t>(tid)][idx]);
    }
  });

  if (rc != Engine::Rc::Ok)
    return Fail(std::move(err));

  auto r = std::make_shared<FeatureRes>();
  r->period = period_note(dates, env, win);
  r->seg = seg;
  r->clock = env.clock;
  for (const Slot &p : ps)
    r->parts.push_back(p.name), r->part_end.push_back(p.k1 - 1);
  for (const auto &[lag, name] : lags)
    r->lags.emplace_back(name);
  const int d_lo = static_cast<int>(win.lo), d_hi = static_cast<int>(win.hi);
  for (size_t i = 0; i < F_n; ++i) {
    FeatureRes::Feat &f = r->feats.emplace_back();
    f.raw = feats[i].raw, f.err = feats[i].err;
    if (!f.err.empty())
      continue;
    Acc &ac = accs[i];
    f.in = ac.in, f.ok = ac.ok;
    if (ac.ok < 100)
      continue;
    r->good.push_back(i);
    std::sort(ac.smp.begin(), ac.smp.end());
    Mean mv;
    for (const double x : ac.smp)
      mv.add(x);
    f.mean = mv.mean();
    f.sd = mv.n > 1 ? std::sqrt(std::max(0.0, (mv.sq - static_cast<double>(mv.n) * mv.mean() * mv.mean()) / static_cast<double>(mv.n - 1))) : kNaN;
    const double qs[] = {0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99};
    for (size_t k = 0; k < f.q.size(); ++k)
      f.q[k] = quant(ac.smp, qs[k]);
    std::vector<double> u = ac.smp;
    u.erase(std::unique(u.begin(), u.end()), u.end());
    f.levels = u.size() <= 20 ? u.size() : 0;
    f.zero = static_cast<double>(std::count(ac.smp.begin(), ac.smp.end(), 0.0)) / static_cast<double>(ac.smp.size());
    f.hist = hist_of(ac.smp, f.h_lo, f.h_hi);

    const double mu = ac.sx / static_cast<double>(ac.n_val);
    const double tot = ac.sxx - static_cast<double>(ac.n_val) * mu * mu;
    double ssa = 0.0, sum_asset_sq = 0.0;
    for (size_t a = 0; a < ac.sa.size(); ++a)
      if (ac.na[a]) {
        const double ma = ac.sa[a] / static_cast<double>(ac.na[a]);
        ssa += static_cast<double>(ac.na[a]) * (ma - mu) * (ma - mu);
        sum_asset_sq += ac.sa[a] * ma;
      }
    const double ssd = std::max(0.0, ac.sum_day_sq - sum_asset_sq);
    const double intra = tot > 0.0 ? std::max(0.0, tot - ssa - ssd) / tot : 0.0;
    f.var = tot > 0.0;
    if (f.var)
      f.v_asset = ssa / tot, f.v_day = ssd / tot;
    f.v_intra = intra;

    for (size_t li = 0; li < lags.size(); ++li) {
      double xy = 0.0, xx = 0.0, yy = 0.0;
      for (size_t a = 0; a < ac.sa.size(); ++a) {
        const LagAcc &la = ac.lag[li][a];
        if (!ac.na[a] || !la.n)
          continue;
        const double ma = ac.sa[a] / static_cast<double>(ac.na[a]), n = static_cast<double>(la.n);
        xy += la.xy - ma * (la.x + la.y) + n * ma * ma;
        xx += la.xx - 2.0 * ma * la.x + n * ma * ma;
        yy += la.yy - 2.0 * ma * la.y + n * ma * ma;
      }
      f.lag[li] = xx > 0.0 && yy > 0.0 ? xy / std::sqrt(xx * yy) : kNaN;
      if (lags[li].first == seg)
        f.day1 = f.lag[li];
    }

    f.slow = seg > 1 && intra < kIntraNone;
    if (seg > 1 && !f.slow) {
      std::vector<double> col(static_cast<size_t>(d_hi - d_lo));
      f.prof.assign(static_cast<size_t>(seg), kNaN);
      for (int k = 0; k < seg; ++k) {
        for (int d = d_lo; d < d_hi; ++d)
          col[static_cast<size_t>(d - d_lo)] = ac.med[static_cast<size_t>(d * seg + k)];
        f.prof[static_cast<size_t>(k)] = median_of(col);
      }
    } else if (seg == 1) {
      f.prof.assign(ac.med.begin() + d_lo, ac.med.begin() + d_hi);
    }
    f.ric = ac.ric.mean(), f.ric_t = ac.ric.t(), f.ric_up = ac.ric.up();
    for (const Mean &m : ac.ric_part)
      f.ric_part.push_back(m.mean());
  }
  const size_t G = r->good.size();
  r->corr.assign(G * G, 1.0);
  for (size_t i = 0; i < G; ++i)
    for (size_t j = 0; j < G; ++j)
      if (i != j)
        r->corr[i * G + j] = pair_corr[std::min(r->good[i], r->good[j]) * F_n + std::max(r->good[i], r->good[j])].mean();
  return Done(std::move(r));
}

json SampleViewFeature(Feed &p) { return p.window({{"exprs", p.leaves(1 + p.below(3))}}); }

} // namespace GUI::Agent
