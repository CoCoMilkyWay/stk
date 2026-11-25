// view_trades — 看交割单: 输入 factor / side / sort / n / merge / from / to / tod → TradesRes (段收益分布 / 开仓时段 / 盘面标签 / 明细) → 原文; 展示见 ui/look/ViewTrades.cpp
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/look/Kernel.hpp"

#include <random>

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

std::string TradesRes::headline() const { return "因子 `" + label + "` · " + side + " · 合并 " + merge + win; }

std::string TradesRes::text() const {
  std::string s = text_head();
  if (!cells)
    return s + "本端没有真持有过的格\n";
  s += "真持有 " + str(cells) + " 格 (每行平均 " + fmt("%.1f", static_cast<double>(cells) / win_rows) + " 只) → " + str(segs) + " 段, 段均 " +
       fmt("%.1f", static_cast<double>(cells) / static_cast<double>(segs)) + " 行\n";
  s += "格超额均值 " + bp(cell_mean) + "bp (胜率 " + pct(cell_up) + ") · 段超额: 均值 " + bp(seg_mean) + " · 中位 " + bp(seg_med) + " · p10 " + bp(seg_p10) + " · p90 " +
       bp(seg_p90) + " · 胜率 " + pct(seg_up) + "\n";
  s += "集中度 (段贡献 = 段超额 × 行数): 最差 10% 的段占全部亏损 " + pct(worst) + " · 最好 10% 的段占全部盈利 " + pct(best) + "\n";
  if (seg > 1) {
    text::Table ptbl({"开仓时段", "段数", "占比", "段超额均值 bp", "胜率"});
    for (const Part &p : parts)
      ptbl.add({p.name, str(p.n), pct(p.share), bp(p.mean), pct(p.up)});
    s += "\n" + ptbl.str();
  }
  if (!tags.empty()) {
    text::Table ttbl({"开仓那行的盘面", "本端格占比", "池内格占比", "提升", "开仓带此标签的段超额 bp", "不带的 bp"});
    for (const Tag &t : tags)
      ttbl.add({t.name, pct(t.held), pct(t.base), t.base > 0.0 ? fmt("%.2f", t.held / t.base) : "—", bp(t.with), bp(t.without)});
    s += "\n" + ttbl.str();
  }
  text::Table dtbl({"#", "代码", "日期", "时刻", "行", "开仓分位", "段超额 bp", "开仓时当日涨跌", "开仓时日内位置", "盘面"});
  for (size_t q = 0; q < list.size(); ++q) {
    const Seg &x = list[q];
    dtbl.add({str(q + 1), x.code, x.date, x.time, std::to_string(x.rows), rk(x.rank), bp(x.ex), chg(x.chg), pos2(x.pos), x.tags});
  }
  s += "\n明细 (" + sort + (sort == "random" ? "" : "; 尾部个例, 多是个股事件或数据缺失, 不代表信号") + "):\n" + dtbl.str();
  return s;
}

std::string TradesRes::title() const {
  return "`" + label + "` · " + side + " · " + (!cells ? std::string("本端没有真持有过的格") : str(segs) + " 段 · 段超额均值 " + bp(seg_mean) + "bp · 胜率 " + pct(seg_up));
}

ToolOut ViewTrades(const Call &c, Bay &bay) {
  const ViewEnv &env = bay.env;
  Engine::FactorFlow &F = bay.flow;
  std::string err;
  if (!date_arg_ok(c, "from", err) || !date_arg_ok(c, "to", err))
    return Fail(std::move(err));
  const int seg = env.fsc.ft->seg;
  const std::string from = c.str("from"), to = c.str("to"), tod = c.str("tod");
  int k0 = 0, k1 = seg;
  if (!tod.empty()) {
    if (seg == 1)
      return Fail("tod: 日层每天只有一行, 没有日内时段可选");
    if (!rows_in("tod", tod, env.clock, 0, k0, k1, err))
      return Fail(std::move(err));
  }
  const bool top = c.str("side") == "top";
  FactorSpec fac;
  fac.ends = true, fac.only_end = top ? st::kTop : st::kBot;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return Fail(std::move(err));

  const int ymd0 = from.empty() ? 0 : ymd_of(from), ymd1 = to.empty() ? 99999999 : ymd_of(to);
  const std::string merge = c.str("merge"), sort = c.str("sort");
  const std::string side_cn = top ? "做多端 G20" : "最低组 G1";

  struct CellRec {
    int a, u;
    float ex, rank;
    Mark mk;
  };
  std::vector<CellRec> cs;
  std::array<size_t, kTagCount> base_tags{};
  size_t nb = 0;
  int win_rows = 0;
  std::mt19937_64 rng(7);

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac, .bars = bit(Close) | bit(High) | bit(Low) | bit(Volume) | bit(PreClose) | bit(LimUp) | bit(LimDn), .extra_front = seg == 1 ? 1 : 0};
  const Engine::Rc rc = run_stream(
      env, F, cfg, dates, job, err,
      [&](const std::vector<misc::Ymd> &ds, size_t &lo, size_t &hi, std::string &why) {
        size_t a = ds.size(), b = 0;
        for (size_t d = 0; d < ds.size(); ++d)
          if (ds[d] >= ymd0 && ds[d] <= ymd1)
            a = std::min(a, d), b = d + 1;
        if (a >= b || k0 >= k1) {
          std::string win = (!from.empty() || !to.empty() ? (from.empty() ? "…" : from) + " ~ " + (to.empty() ? "…" : to) + " " : "") +
                            (!tod.empty() ? "行末 " + tod + " " : "");
          why = "## 因子 `" + fac.label + "` · " + side_cn + " · 合并 " + merge + " · 只看 " + win + "的行 (0 行)\n窗口里没有行 (观察的日段 " + misc::dash(ds.front()) +
                " ~ " + misc::dash(ds.back()) + ")";
          return false;
        }
        lo = a, hi = b;
        win_rows = static_cast<int>(b - a) * (k1 - k0);
        return true;
      },
      [&](const ChunkView &cv) {
        const int A = cv.blk.y.sh.A, n_stat = cv.t_hi - cv.t_lo;
        const Px px(cv.blk, cv.bm, cfg.bars, env.open);
        const Engine::LegPlane &lb = *cv.blk.y.labels[0];

        std::vector<std::pair<int, HeldCell>> flat;
        for (int r = 0; r < n_stat; ++r) {
          const int u = cv.u0 + r, ymd = cv.dates[static_cast<size_t>(u / seg)], k = u % seg;
          if (ymd < ymd0 || ymd > ymd1 || k < k0 || k >= k1)
            continue;
          for (const HeldCell &hc : cv.cells[static_cast<size_t>(r)])
            flat.push_back({r, hc});
        }
        const size_t base_idx = cs.size();
        cs.resize(base_idx + flat.size());
        F.S.ex.rows(static_cast<int>(flat.size()), [&](int i0, int i1, int) {
          for (int i = i0; i < i1; ++i) {
            const auto &[r, hc] = flat[static_cast<size_t>(i)];
            const size_t k = static_cast<size_t>(hc.t) * static_cast<size_t>(A) + static_cast<size_t>(hc.a);
            cs[base_idx + static_cast<size_t>(i)] = {hc.a, cv.u0 + r, hc.ret - cv.rows[static_cast<size_t>(r)].mkt, rank_of_r16(cv.rx[k]), mark_of(px, hc.t, hc.a)};
          }
        });

        // 池内基准抽样 (按本块所占统计行比例抽)
        const size_t want_b = std::max<size_t>(1, kBase * static_cast<size_t>(n_stat) / std::max<size_t>(1, cv.dates.size() * static_cast<size_t>(seg)));
        std::uniform_int_distribution<int> pick_r(0, n_stat - 1), pick_a(0, A - 1);
        for (size_t got = 0, tries = 0; got < want_b && nb < kBase && tries < want_b * 20; ++tries) {
          const int r = pick_r(rng), u = cv.u0 + r, ymd = cv.dates[static_cast<size_t>(u / seg)], kr = u % seg;
          if (ymd < ymd0 || ymd > ymd1 || kr < k0 || kr >= k1 || !opens(u, env))
            continue;
          const int t = cv.t_lo + r, a = pick_a(rng);
          const size_t k = static_cast<size_t>(t) * static_cast<size_t>(A) + static_cast<size_t>(a);
          if (!cv.blk.y.gate[k] || !lb.m[k])
            continue;
          const Mark m = mark_of(px, t, a);
          ++got, ++nb;
          for (int j = 0; j < kTagCount; ++j)
            base_tags[static_cast<size_t>(j)] += m.m >> j & 1u;
        }
      });

  if (rc != Engine::Rc::Ok)
    return Fail(std::move(err));

  auto res = std::make_shared<TradesRes>();
  TradesRes &R = *res;
  R.period = period_note(dates, env);
  R.label = fac.label, R.side = side_cn, R.merge = merge, R.sort = sort, R.seg = seg, R.win_rows = win_rows;
  std::string win;
  if (!from.empty() || !to.empty())
    win += (from.empty() ? "…" : from) + " ~ " + (to.empty() ? "…" : to) + " ";
  if (!tod.empty())
    win += "行末 " + tod + " ";
  if (!win.empty())
    R.win = " · 只看 " + win + "的行 (" + std::to_string(win_rows) + " 行)";
  R.cells = cs.size();
  if (cs.empty())
    return Done(std::move(res));

  std::sort(cs.begin(), cs.end(), [](const CellRec &x, const CellRec &y) { return x.a != y.a ? x.a < y.a : x.u < y.u; });
  struct Trade {
    int a, u0, u1, n;
    double ex;
    float rank;
    Mark mk;
  };
  std::vector<Trade> ts;
  Mean cell_m;
  for (const CellRec &x : cs) {
    cell_m.add(x.ex);
    Trade *b = ts.empty() ? nullptr : &ts.back();
    const bool join = b && b->a == x.a && b->u1 / seg == x.u / seg && (merge == "day" || (merge == "run" && x.u == b->u1 + 1));
    if (join)
      b->ex = (b->ex * b->n + x.ex) / (b->n + 1), b->u1 = x.u, ++b->n;
    else
      ts.push_back({x.a, x.u, x.u, 1, x.ex, x.rank, x.mk});
  }

  const size_t M = ts.size();
  std::vector<double> ex(M), contrib(M);
  Mean seg_ex;
  double gain = 0.0, loss = 0.0;
  for (size_t i = 0; i < M; ++i) {
    ex[i] = ts[i].ex, seg_ex.add(ex[i]);
    contrib[i] = ts[i].ex * ts[i].n;
    (contrib[i] > 0 ? gain : loss) += contrib[i];
  }
  std::vector<double> sx = ex;
  std::sort(sx.begin(), sx.end());
  std::sort(contrib.begin(), contrib.end());
  const size_t tenth = std::max<size_t>(1, M / 10);
  double worst = 0.0, best = 0.0;
  for (size_t i = 0; i < tenth; ++i)
    worst += std::min(0.0, contrib[i]), best += std::max(0.0, contrib[M - 1 - i]);
  R.segs = M;
  R.cell_mean = cell_m.mean(), R.cell_up = cell_m.up();
  R.seg_mean = seg_ex.mean(), R.seg_med = quant(sx, 0.5), R.seg_p10 = quant(sx, 0.1), R.seg_p90 = quant(sx, 0.9), R.seg_up = seg_ex.up();
  R.worst = loss < 0.0 ? worst / loss : kNaN, R.best = gain > 0.0 ? best / gain : kNaN;
  R.hist = hist_of(sx, R.h_lo, R.h_hi);

  if (seg > 1) {
    const std::vector<Slot> ps = parts_of(seg, env.open, env.clock);
    const std::vector<int> pr = part_of_row(ps, seg);
    std::vector<Mean> by(ps.size());
    for (size_t i = 0; i < M; ++i)
      by[static_cast<size_t>(pr[static_cast<size_t>(ts[i].u0 % seg)])].add(ex[i]);
    for (size_t j = 0; j < ps.size(); ++j)
      if (by[j].n)
        R.parts.push_back({ps[j].name, by[j].n, static_cast<double>(by[j].n) / static_cast<double>(M), by[j].mean(), by[j].up()});
  }

  std::array<size_t, kTagCount> held{};
  size_t nh = 0;
  for (size_t i = 0, step = std::max<size_t>(1, cs.size() / kBase); i < cs.size(); i += step, ++nh)
    for (int j = 0; j < kTagCount; ++j)
      held[static_cast<size_t>(j)] += cs[i].mk.m >> j & 1u;
  for (int j = 0; j < kTagCount; ++j) {
    Mean with, without;
    for (size_t i = 0; i < M; ++i)
      (ts[i].mk.m >> j & 1u ? with : without).add(ex[i]);
    const double fs = static_cast<double>(held[static_cast<size_t>(j)]) / static_cast<double>(nh), fb = nb ? static_cast<double>(base_tags[static_cast<size_t>(j)]) / static_cast<double>(nb) : kNaN;
    if (fs < 0.01 && !(fb >= 0.01))
      continue;
    R.tags.push_back({kTags[j].cn, fs, fb, with.mean(), without.mean()});
  }

  std::vector<size_t> ix(M);
  std::iota(ix.begin(), ix.end(), size_t{0});
  if (sort == "random")
    std::shuffle(ix.begin(), ix.end(), std::mt19937_64(7));
  else
    std::stable_sort(ix.begin(), ix.end(), [&](size_t x, size_t y) { return sort == "worst" ? ex[x] < ex[y] : ex[x] > ex[y]; });
  ix.resize(std::min<size_t>(M, static_cast<size_t>(c.num("n"))));
  const AssetAxis &ax = asset_axis();
  const PoolAxis &pa = job.dm.rs.axis;
  for (const size_t i : ix) {
    const Trade &x = ts[i];
    const std::string &c0 = env.clock[static_cast<size_t>(x.u0 % seg)], &c1 = env.clock[static_cast<size_t>(x.u1 % seg)];
    R.list.push_back({ax.code(pa.global(static_cast<size_t>(x.a))), misc::dash(dates[static_cast<size_t>(x.u0 / seg)]), x.n > 1 ? c0 + "–" + c1 : c0, tags_of(x.mk.m), x.n,
                      x.rank, x.ex, x.mk.chg, x.mk.pos});
  }
  return Done(std::move(res));
}

json SampleViewTrades(Feed &p) {
  constexpr Tool T = Tool::ViewTrades;
  json j = {{"factor", p.factor()}, {"side", p.pick(T, "side")}, {"sort", p.pick(T, "sort")}, {"n", 6 + p.below(10)}, {"merge", p.pick(T, "merge")}};
  if (p.coin(0.3) && !p.ctx.days.empty()) {
    const auto [a, b] = p.span(20, 60);
    j["from"] = a, j["to"] = b;
  }
  if (p.coin(0.2) && p.ctx.seg > 1)
    j["tod"] = p.tod(std::max(1, (p.ctx.seg - p.ctx.open) / 4));
  return j;
}

} // namespace GUI::Agent
