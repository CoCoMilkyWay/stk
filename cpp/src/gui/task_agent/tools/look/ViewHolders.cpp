// view_holders — 看常客: 输入 factor / side / n → HoldersRes (集中度 + 资产表) → 原文; 展示见 ui/look/ViewHolders.cpp
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/look/Kernel.hpp"

namespace GUI::Agent {

using namespace look;
using json = nlohmann::ordered_json;

std::string HoldersRes::headline() const { return "因子 `" + label + "` · " + side + " 的常客"; }

std::string HoldersRes::text() const {
  std::string s = text_head();
  if (!total)
    return s + "本端没有行\n";
  s += "本端共 " + str(total) + " 行 · 落过本端的资产 " + str(any) + " / " + std::to_string(active) + " · 前 10 只占 " + pct(top10) + " · 凑满一半要 " + str(half) + " 只\n\n";
  text::Table tbl({"代码", "本端行", "在池行", "占其在池", "天数", "平均连续行", "本端持仓格", "持仓格均超额 bp"});
  for (const Row &w : rows)
    tbl.add({w.code, std::to_string(w.side), std::to_string(w.in), pct(w.share), std::to_string(w.days), fmt("%.1f", w.run), str(w.held), bp(w.ex)});
  return s + tbl.str();
}

std::string HoldersRes::title() const {
  return "`" + label + "` · " + side + " · " + (!total ? std::string("本端没有行") : "前 10 只占 " + pct(top10) + " · 凑满一半要 " + str(half) + " 只");
}

ToolOut ViewHolders(const Call &c, Bay &bay) {
  const ViewEnv &env = bay.env;
  Engine::FactorFlow &F = bay.flow;
  std::string err;
  const bool top = c.str("side") == "top";
  FactorSpec fac;
  fac.ends = true, fac.only_end = top ? st::kTop : st::kBot;
  if (!resolve_factor(c.str("factor"), env, fac, err))
    return Fail(std::move(err));

  const int g_want = top ? kG20 : kG1, seg = env.fsc.ft->seg;
  struct Who {
    int a = 0, in = 0, side = 0, days = 0, runs = 0;
    Mean ex;
  };
  std::vector<Who> ws;
  std::vector<int> last_day;

  std::vector<misc::Ymd> dates;
  Engine::Job job;
  StreamCfg cfg{.fac = &fac};
  const Engine::Rc rc = run_stream(env, F, cfg, dates, job, err, whole, [&](const ChunkView &cv) {
    const int A = cv.blk.y.sh.A, n_stat = cv.t_hi - cv.t_lo;
    if (ws.empty()) {
      ws.resize(static_cast<size_t>(A));
      for (int a = 0; a < A; ++a)
        ws[static_cast<size_t>(a)].a = a;
      last_day.assign(static_cast<size_t>(A), -1);
    }
    // 按资产条带并行, 条带内逐行扫 (行主序 [T][A]: 一行里条带连续)
    F.S.ex.rows(A, [&](int a0, int a1, int) {
      for (int r = 0; r < n_stat; ++r) {
        const int t = cv.t_lo + r, u = cv.u0 + r;
        const size_t row = static_cast<size_t>(t) * static_cast<size_t>(A);
        for (int a = a0; a < a1; ++a) {
          Who &w = ws[static_cast<size_t>(a)];
          int &ld = last_day[static_cast<size_t>(a)];
          const size_t k = row + static_cast<size_t>(a);
          w.in += cv.blk.y.gate[k];
          if (grp_of_r16(cv.rx[k]) != g_want)
            continue;
          ++w.side;
          if (u / seg != ld)
            ++w.days, ld = u / seg;
          if (u % seg == 0 || grp_of_r16(cv.rx[k - static_cast<size_t>(A)]) != g_want)
            ++w.runs;
        }
      }
    });
    for (int r = 0; r < n_stat; ++r) {
      const float mkt = cv.rows[static_cast<size_t>(r)].mkt;
      for (const HeldCell &hc : cv.cells[static_cast<size_t>(r)])
        ws[static_cast<size_t>(hc.a)].ex.add(static_cast<double>(hc.ret) - mkt);
    }
  });

  if (rc != Engine::Rc::Ok)
    return Fail(std::move(err));

  auto res = std::make_shared<HoldersRes>();
  HoldersRes &R = *res;
  R.period = period_note(dates, env), R.label = fac.label, R.side = top ? "做多端 G20" : "最低组 G1";
  for (const Who &w : ws)
    R.active += w.in > 0;
  std::sort(ws.begin(), ws.end(), [](const Who &x, const Who &y) { return x.side != y.side ? x.side > y.side : x.a < y.a; });
  for (const Who &w : ws)
    R.total += static_cast<size_t>(w.side), R.any += w.side > 0;
  if (!R.total)
    return Done(std::move(res));
  size_t acc = 0, top10 = 0;
  for (size_t i = 0; i < ws.size(); ++i) {
    if (i < 10)
      top10 += static_cast<size_t>(ws[i].side);
    if (acc * 2 < R.total)
      acc += static_cast<size_t>(ws[i].side), ++R.half;
  }
  R.top10 = static_cast<double>(top10) / static_cast<double>(R.total);
  const AssetAxis &ax = asset_axis();
  const PoolAxis &pa = job.dm.rs.axis;
  for (size_t i = 0; i < std::min<size_t>(ws.size(), static_cast<size_t>(c.num("n"))); ++i) {
    const Who &w = ws[i];
    if (!w.side)
      break;
    R.rows.push_back({ax.code(pa.global(static_cast<size_t>(w.a))), w.side, w.in, w.days, w.in ? static_cast<double>(w.side) / w.in : kNaN,
                      static_cast<double>(w.side) / w.runs, w.ex.n, w.ex.mean()});
  }
  return Done(std::move(res));
}

json SampleViewHolders(Feed &p) { return {{"factor", p.factor()}, {"side", p.pick(Tool::ViewHolders, "side")}, {"n", 10 + p.below(11)}}; }

} // namespace GUI::Agent
