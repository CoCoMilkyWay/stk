// view_stock_days — 看个股多日: 输入 code / from / to / factor / exprs → StockDaysRes (逐日一行 + 日级火花线) → 原文; 展示见 ui/look/ViewStockDays.cpp
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/look/Kernel.hpp"

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

std::string StockDaysRes::headline() const { return code + " · " + days.front().date + " ~ " + days.back().date + " (" + std::to_string(days.size()) + " 日) · " + level; }

std::vector<Sec> StockDaysRes::secs() const {
  Sec x{.lines = exprs, .plot = 0};
  std::vector<double> sc, sg, sf;
  std::vector<std::vector<double>> se(n_feats);
  for (const Day &d : days) {
    sc.push_back(d.close), sg.push_back(d.chg), sf.push_back(d.rank);
    for (size_t j = 0; j < n_feats; ++j)
      se[j].push_back(j < d.fv.size() ? d.fv[j] : kNaN);
  }
  x.sparks = {"收盘价 " + spark(sc), "当日涨跌 " + spark(sg, chg)};
  if (has_fac)
    x.sparks.push_back("因子 `" + fac + "` 日均分位 " + spark(sf, rk));
  for (size_t j = 0; j < n_feats; ++j)
    x.sparks.push_back("E" + str(j + 1) + " 收盘值 " + spark(se[j]));

  x.tab.head = {"日期", "涨跌", "振幅", "成交额 万", "在池行"};
  if (has_fac)
    x.tab.head.push_back("因子日均分位"), x.tab.head.push_back("收盘分位"), x.tab.head.push_back("多/空行");
  x.tab.head.push_back("收盘开仓超额 bp");
  for (size_t j = 0; j < n_feats; ++j)
    x.tab.head.push_back("E" + str(j + 1) + " 收盘");
  for (size_t i = 0; i < days.size(); ++i) {
    const Day &d = days[i];
    std::vector<Num> row{d.date, nchg(d.chg), pct(d.amp), std::isfinite(d.amt) ? fmt("%.0f", d.amt / 1e4) : "—", std::to_string(d.in)};
    if (has_fac)
      row.push_back(rk(d.rank)), row.push_back(rk(d.rank_close)), row.push_back(std::to_string(d.up) + "/" + std::to_string(d.dn));
    row.push_back(nbp(d.ex));
    for (size_t j = 0; j < n_feats; ++j)
      row.push_back(num(se[j][i]));
    x.tab.rows.push_back(std::move(row));
  }
  return {std::move(x)};
}

std::string StockDaysRes::title() const {
  return code + " · " + days.front().date + " ~ " + days.back().date + " (" + std::to_string(days.size()) + " 日) · 收 " + num(days.front().close) + " → " + num(days.back().close);
}

ToolOut ViewStockDays(const Call &c, Bay &bay) {
  const ViewEnv &env = bay.env;
  Engine::FactorFlow &F = bay.flow;
  std::string err, code;
  uint32_t asset_id = 0;
  if (!resolve_code(c.str("code"), env, code, asset_id, err) || !date_arg_ok(c, "from", err) || !date_arg_ok(c, "to", err))
    return Fail(std::move(err));

  FactorSpec fac;
  const bool has_fac = !c.str("factor").empty();
  if (has_fac && !resolve_factor(c.str("factor"), env, fac, err))
    return Fail(std::move(err));
  std::vector<FeatSpec> feats;
  resolve_feats(c.strs("exprs"), env, feats);

  const int seg = env.fsc.ft->seg;
  int d0 = 0, d1 = 0, n_days = 0;
  struct DayRec {
    double close = kNaN, chg_d = kNaN, amp = kNaN, close_ex = kNaN, fac_mean = kNaN, fac_close = kNaN;
    Mean amean;
    int in = 0, up = 0, dn = 0;
    std::vector<double> fv;
  };
  std::vector<DayRec> recs;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = has_fac ? &fac : nullptr, .feats = &feats, .bars = bit(Close) | bit(High) | bit(Low) | bit(Volume) | bit(Amount) | bit(PreClose), .extra = {.front = seg == 1 ? 1 : 0}};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &ds, size_t &lo, size_t &hi, std::string &why) {
        const int D = static_cast<int>(ds.size());
        d1 = c.str("to").empty() ? D - 1 : day_index(ds, c.str("to"), why);
        if (d1 < 0)
          return false;
        d0 = c.str("from").empty() ? std::max(0, d1 - 19) : day_index(ds, c.str("from"), why);
        if (d0 < 0)
          return false;
        if (d0 > d1 || d1 - d0 + 1 > kStockDays) {
          why = "from ~ to 须是先后有序且至多 " + std::to_string(kStockDays) + " 个交易日 (这里是 " + std::to_string(d1 - d0 + 1) + " 个)";
          return false;
        }
        n_days = d1 - d0 + 1;
        recs.resize(static_cast<size_t>(n_days));
        lo = static_cast<size_t>(d0), hi = static_cast<size_t>(d1 + 1);
        return true;
      },
      [&](const ChunkView &cv) {
        const size_t col_a = job.dm.rs.axis.sub_of(asset_id);
        assert(col_a < static_cast<size_t>(cv.blk.y.sh.A));
        const int a = static_cast<int>(col_a), A = cv.blk.y.sh.A, days_stat = (cv.t_hi - cv.t_lo) / seg, chunk_d0 = cv.u0 / seg;
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const Engine::LabelPlanes &lb = *cv.blk.y.labels[0];
        const uint16_t *lv = lb.lv.data();

        for (int cd = 0; cd < days_stat; ++cd) {
          const int d = chunk_d0 + cd;
          if (d < d0 || d > d1)
            continue;
          const int t0 = cv.t_lo + cd * seg, tl = t0 + seg - 1;
          DayRec &rec = recs[static_cast<size_t>(d - d0)];
          double hi = -INFINITY, lo = INFINITY;
          std::vector<double> rks;
          for (int t = t0; t <= tl; ++t) {
            const size_t cell_k = static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a);
            if (px.traded(t, a))
              hi = std::fmax(hi, px(High, t, a)), lo = std::fmin(lo, px(Low, t, a));
            rec.amean.add(px(Amount, t, a));
            if (const double x = px(Close, t, a); std::isfinite(x))
              rec.close = x;
            rec.in += cv.blk.y.gate[cell_k];
            if (has_fac) {
              rks.push_back(rank_of_r16(cv.rx[cell_k]));
              const int g = grp_of_r16(cv.rx[cell_k]);
              rec.up += g == kG20, rec.dn += g == kG1;
            }
          }
          const double pre = px.pre(t0, a);
          rec.chg_d = rec.close / pre - 1.0;
          rec.amp = hi > lo ? (hi - lo) / pre : kNaN;
          const size_t kl = static_cast<size_t>(tl) * static_cast<size_t>(A) + static_cast<size_t>(a);
          const float mkt = mkt_of(cv, tl);
          rec.close_ex = lb.m[kl] && opens(d * seg + seg - 1, env) ? static_cast<double>(h2f(lv[kl])) - mkt : kNaN;
          if (has_fac)
            rec.fac_mean = mean_of(rks), rec.fac_close = rank_of_r16(cv.rx[kl]);
          for (size_t j = 0; j < feats.size(); ++j)
            rec.fv.push_back(cv.feat_pl[j] && cv.feat_pl[j]->m[kl] ? static_cast<double>(cv.feat_pl[j]->v[kl]) : kNaN);
        }
      });

  if (rc != Engine::Rc::Ok)
    return Fail(std::move(err));

  auto res = std::make_shared<StockDaysRes>();
  StockDaysRes &R = *res;
  R.period = period_note(dates, env), R.code = code, R.level = env.level, R.has_fac = has_fac, R.fac = fac.label, R.n_feats = feats.size();
  for (size_t i = 0; i < feats.size(); ++i)
    R.exprs.push_back(expr_line(i, feats[i]));
  for (int i = 0; i < n_days; ++i) {
    const DayRec &r = recs[static_cast<size_t>(i)];
    R.days.push_back({misc::dash(dates[static_cast<size_t>(d0 + i)]), r.close, r.chg_d, r.amp, r.amean.n ? r.amean.sum : kNaN, r.in, r.up, r.dn, r.fac_mean, r.fac_close,
                      r.close_ex, r.fv});
  }
  return Done(std::move(res));
}

json SampleViewStockDays(Feed &p) {
  const Spot s = p.spot();
  json j = {{"code", s.code}, {"factor", p.coin(0.6) ? p.factor() : std::string()}, {"exprs", p.leaves(p.coin(0.4) ? 1 : 0)}};
  if (p.coin(0.5))
    j["to"] = s.day;
  return j;
}

} // namespace GUI::Agent
