// view_stock_day — 看个股一天: 输入 code / date / factor / exprs / rows → StockDayRes (当日概况 + 逐行序列 + 分段表 + 逐行表) → 原文; 展示见 ui/look/ViewStockDay.cpp
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/tools/look/Kernel.hpp"

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

std::string StockDayRes::text() const {
  std::string s = period + "## " + code + " · " + date + " · " + level + " (每天 " + std::to_string(seg) + " 行)\n";
  const std::string open_str = first < 0 ? "当日无成交"
                                         : "首个有成交行 (" + clock[static_cast<size_t>(first)] + ") 收 " + num(close[static_cast<size_t>(first)]) + " (" +
                                               chg(close[static_cast<size_t>(first)] / pre - 1.0) + ")";
  s += "昨收 " + num(pre) + " · " + open_str + " · 高 " + num(hi) + " (" + chg(hi / pre - 1.0) + ") · 低 " + num(lo) + " (" + chg(lo / pre - 1.0) + ") · 收 " + num(last) + " (" +
       chg(last / pre - 1.0) + ") · 涨停 " + num(lim_up) + " / 跌停 " + num(lim_dn) + " · 成交额 " + wan(amt_sum, amt_n) + " 万 · 在池 " + std::to_string(in) + " / " +
       std::to_string(seg) + " 行\n";
  for (const std::string &e : exprs)
    s += e;
  s += "\n- 收盘价 " + spark(close) + "\n- 成交量 " + spark(vol) + "\n";
  if (has_fac)
    s += "- 因子 `" + fac + "` 分位 (高 = 做多端) " + spark(rank, rk) + "\n";
  for (size_t i = 0; i < fv.size(); ++i)
    s += "- E" + str(i + 1) + " " + spark(fv[i]) + "\n";

  std::vector<std::string> phs{"时段", "涨跌", "段内高", "段内低", "量占比"};
  if (has_fac)
    phs.push_back("因子分位"), phs.push_back("多/空行");
  phs.push_back("后续超额 bp");
  for (size_t i = 0; i < fv.size(); ++i)
    phs.push_back("E" + str(i + 1) + " 段末");
  text::Table ptbl(phs);
  for (const Part &p : parts) {
    std::vector<std::string> row{p.name, chg(p.chg), chg(p.hi), chg(p.lo), pct(p.vol)};
    if (has_fac)
      row.push_back(rk(p.rank)), row.push_back(std::to_string(p.up) + "/" + std::to_string(p.dn));
    row.push_back(bp(p.ex));
    for (const double v : p.end)
      row.push_back(num(v));
    ptbl.add(row);
  }
  s += "\n" + ptbl.str();

  if (!rows.empty()) {
    const auto whole_n = [](double x) { return std::isfinite(x) ? fmt("%.0f", x) : std::string("—"); };
    std::vector<std::string> rhs{"时刻", "收", "涨跌", "当日", "量", "额"};
    if (has_fac)
      rhs.push_back("因子分位"), rhs.push_back("组");
    rhs.push_back("后续超额 bp");
    for (size_t i = 0; i < fv.size(); ++i)
      rhs.push_back("E" + str(i + 1));
    text::Table rtbl(rhs);
    for (int k = r0; k < r1; ++k) {
      const size_t q = static_cast<size_t>(k);
      const double prev = k == 0 ? pre : close[q - 1];
      std::vector<std::string> row{clock[q], num(close[q]), chg(close[q] / prev - 1.0), chg(close[q] / pre - 1.0), whole_n(vol[q]),
                                   std::isfinite(amt[q]) ? fmt("%.1f", amt[q] / 1e4) : std::string("—")};
      if (has_fac)
        row.push_back(rk(rank[q])), row.push_back(grp[q] < 0 ? "—" : gname(grp[q]));
      row.push_back(bp(ex[q]));
      for (const std::vector<double> &f : fv)
        row.push_back(num(f[q]));
      rtbl.add(row);
    }
    s += "\n逐行 " + rows + " (涨跌 = 对上一行收盘; 当日 = 对昨收; 额 万元):\n" + rtbl.str();
  }
  return s;
}

ToolOut ViewStockDay(const Call &c, Bay &bay) {
  const ViewEnv &env = bay.env;
  Engine::FactorFlow &F = bay.flow;
  const int seg = env.fsc.ft->seg;
  if (seg == 1)
    return Fail("日层每天只有一行, 看个股用 view_stock_days");
  std::string err, code;
  uint32_t asset_id = 0;
  if (!resolve_code(c.str("code"), env, code, asset_id, err) || !date_arg_ok(c, "date", err))
    return Fail(std::move(err));
  int rk0 = 0, rk1 = 0;
  if (!c.str("rows").empty() && !rows_in("rows", c.str("rows"), env.clock, kStockRows, rk0, rk1, err))
    return Fail(std::move(err));

  FactorSpec fac;
  const bool has_fac = !c.str("factor").empty();
  if (has_fac && !resolve_factor(c.str("factor"), env, fac, err))
    return Fail(std::move(err));
  std::vector<FeatSpec> feats;
  resolve_feats(c.strs("exprs"), env, feats);

  int target_d = -1;
  auto res = std::make_shared<StockDayRes>();
  StockDayRes &R = *res;
  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = has_fac ? &fac : nullptr, .feats = &feats, .bars = kAllBars};
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
        const int u0 = target_d * seg;
        if (u0 < cv.u0 || u0 >= cv.u0 + (cv.t_hi - cv.t_lo))
          return;
        const size_t col_a = job.dm.rs.axis.sub_of(asset_id);
        assert(col_a < static_cast<size_t>(cv.blk.y.sh.A));
        const int a = static_cast<int>(col_a), A = cv.blk.y.sh.A, t0 = cv.t_lo + (u0 - cv.u0);
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const Engine::LegPlane &lb = *cv.blk.y.labels[0];
        const uint16_t *lv = lb.v[st::kLong].data();
        const size_t S = static_cast<size_t>(seg);
        R.close.assign(S, kNaN), R.high.assign(S, kNaN), R.low.assign(S, kNaN), R.vol.assign(S, kNaN), R.amt.assign(S, kNaN), R.ex.assign(S, kNaN), R.rank.assign(S, kNaN);
        R.grp.assign(S, -1);
        R.fv.assign(feats.size(), std::vector<double>(S, kNaN));

        R.pre = px.pre(t0, a);
        double hi = -INFINITY, lo = INFINITY, vsum = 0.0, last = kNaN;
        for (int k = 0; k < seg; ++k) {
          const int t = t0 + k;
          const size_t q = static_cast<size_t>(k), cell_k = static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a);
          R.close[q] = px(Close, t, a), R.high[q] = px(High, t, a), R.low[q] = px(Low, t, a), R.vol[q] = px(Volume, t, a), R.amt[q] = px(Amount, t, a);
          if (px.traded(t, a)) {
            hi = std::fmax(hi, R.high[q]), lo = std::fmin(lo, R.low[q]);
            R.first = R.first < 0 ? k : R.first;
          }
          vsum += std::isfinite(R.vol[q]) ? R.vol[q] : 0.0;
          if (std::isfinite(R.amt[q]))
            R.amt_sum += R.amt[q], ++R.amt_n;
          last = std::isfinite(R.close[q]) ? R.close[q] : last;
          R.in += cv.blk.y.gate[cell_k];
          const float mkt = row_mkt(cv.blk, t);
          R.ex[q] = lb.m[cell_k] ? static_cast<double>(h2f(lv[cell_k])) - mkt : kNaN;
          if (has_fac)
            R.rank[q] = rank_of_r16(cv.rx[cell_k]), R.grp[q] = grp_of_r16(cv.rx[cell_k]);
          for (size_t i = 0; i < feats.size(); ++i)
            if (cv.feat_pl[i] && cv.feat_pl[i]->m[cell_k])
              R.fv[i][q] = cv.feat_pl[i]->v[cell_k];
        }
        R.hi = hi, R.lo = lo, R.last = last, R.lim_up = px(LimUp, t0, a), R.lim_dn = px(LimDn, t0, a);

        for (const Slot &p : parts_of(seg, env.open, env.clock)) {
          double ph = -INFINITY, pl = INFINITY, pv = 0.0;
          std::vector<double> prks, pexs;
          StockDayRes::Part &part = R.parts.emplace_back();
          part.name = p.name;
          for (int k = p.k0; k < p.k1; ++k) {
            const size_t q = static_cast<size_t>(k);
            if (px.traded(t0 + k, a))
              ph = std::fmax(ph, R.high[q]), pl = std::fmin(pl, R.low[q]);
            pv += std::isfinite(R.vol[q]) ? R.vol[q] : 0.0;
            pexs.push_back(R.ex[q]);
            if (has_fac) {
              prks.push_back(R.rank[q]);
              part.up += R.grp[q] == kG20, part.dn += R.grp[q] == kG1;
            }
          }
          const double pfrom = p.k0 == 0 ? R.pre : R.close[static_cast<size_t>(p.k0 - 1)];
          part.chg = R.close[static_cast<size_t>(p.k1 - 1)] / pfrom - 1.0, part.hi = ph / R.pre - 1.0, part.lo = pl / R.pre - 1.0;
          part.vol = vsum > 0.0 ? pv / vsum : kNaN;
          part.rank = mean_of(prks), part.ex = mean_of(pexs);
          for (size_t i = 0; i < feats.size(); ++i)
            part.end.push_back(R.fv[i][static_cast<size_t>(p.k1 - 1)]);
        }
      });

  if (rc != Engine::Rc::Ok)
    return Fail(std::move(err));
  R.period = period_note(dates, env), R.code = code, R.date = date_of(dates[static_cast<size_t>(target_d)]), R.level = env.level, R.seg = seg, R.clock = env.clock;
  for (size_t i = 0; i < feats.size(); ++i)
    R.exprs.push_back(expr_line(i, feats[i]));
  R.has_fac = has_fac, R.fac = fac.label;
  R.rows = c.str("rows"), R.r0 = rk0, R.r1 = rk1;
  return Done(std::move(res));
}

json SampleViewStockDay(Feed &p) {
  json j = {{"code", p.code()}, {"date", p.day()}, {"factor", p.coin(0.6) ? p.factor() : std::string()}, {"exprs", json::array()}};
  if (p.coin(0.4))
    j["rows"] = "10:00-10:20";
  return j;
}

} // namespace GUI::Agent
