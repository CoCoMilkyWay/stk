// eval_factor — 回测: 输入 name / name_cn / expr / hyp / note → TrialRes (落文件与否 + 结局 + 最佳权重 20 组 / 分年 + 参数曲面) → 原文; 展示见 ui/trial/EvalFactor.cpp
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Trial.hpp"

#include <algorithm>
#include <cmath>

namespace GUI::Agent {

using json = nlohmann::ordered_json;
using text::fmt;

namespace {

constexpr size_t kSurfBins = 6;
constexpr size_t kSurfSlots = 3;
constexpr size_t kSurfTop = 12;

std::string bp_spark(const std::vector<double> &v) {
  const auto [lo, hi] = std::minmax_element(v.begin(), v.end());
  return "`" + text::bars(v) + "`  [" + text::bp(*lo) + ", " + text::bp(*hi) + "] bp";
}

std::string param(double v) { return v == std::round(v) ? fmt("%.0f", v) : fmt("%.4g", v); }

std::string surface_text(const Surface &sf) {
  if (sf.slots.empty() || sf.pts.size() < 2)
    return "";
  const size_t n = sf.pts.size(), per = (n + kSurfBins - 1) / kSurfBins;
  const auto g = [](const Stat &st) { return std::max(st.mono, 0.0) * std::max(st.mono, 0.0) * st.ls * st.ric; };
  const auto better = [&](const Stat &x, const Stat &y) { return x.mono != y.mono ? x.mono > y.mono : g(x) > g(y); };
  if (sf.slots.size() > kSurfSlots) {
    std::vector<const Probe *> ps;
    for (const Probe &p : sf.pts)
      ps.push_back(&p);
    std::stable_sort(ps.begin(), ps.end(), [&](const Probe *a, const Probe *b) { return better(a->st, b->st); });
    ps.erase(std::unique(ps.begin(), ps.end(), [](const Probe *a, const Probe *b) { return a->v == b->v; }), ps.end());
    ps.resize(std::min(ps.size(), kSurfTop));
    std::vector<std::string> hs = sf.slots;
    hs.push_back("mono"), hs.push_back("每期毛多空 bp"), hs.push_back("rIC");
    text::Table tbl(hs);
    for (const Probe *p : ps) {
      std::vector<std::string> row;
      for (const double x : p->v)
        row.push_back(param(x));
      row.push_back(fmt("%.3f", p->st.mono)), row.push_back(fmt("%+.2f", p->st.ls * 1e4)), row.push_back(fmt("%.4f", p->st.ric));
      tbl.add(row);
    }
    return "- 参数曲面 (Search 评过 " + std::to_string(n) + " 个点; 参数多, 只列最好的 " + std::to_string(ps.size()) + " 个不同点: mono 最高, 同 mono 取目标高的):\n\n" + tbl.str();
  }
  std::string s = "- 参数曲面 (Search 评过 " + std::to_string(n) + " 个点; 每个参数按取值分段, 段内取最好的点: mono 最高, 同 mono 取目标高的):\n";
  for (size_t j = 0; j < sf.slots.size(); ++j) {
    std::vector<size_t> ix(n);
    for (size_t i = 0; i < n; ++i)
      ix[i] = i;
    const auto v = [&](size_t i) { return sf.pts[ix[i]].v[j]; };
    std::stable_sort(ix.begin(), ix.end(), [&](size_t a, size_t b) { return sf.pts[a].v[j] < sf.pts[b].v[j]; });
    text::Table tbl({sf.slots[j] + " 段", "最好的点", "mono", "每期毛多空 bp", "rIC"});
    for (size_t a = 0; a < n;) {
      size_t b = std::min(n, a + per);
      while (b < n && v(b) == v(b - 1))
        ++b;
      size_t best = a;
      for (size_t i = a; i < b; ++i)
        if (better(sf.pts[ix[i]].st, sf.pts[ix[best]].st))
          best = i;
      const Probe &p = sf.pts[ix[best]];
      std::string at;
      for (size_t q = 0; q < sf.slots.size(); ++q)
        at += (q ? ", " : "") + (sf.slots.size() > 1 ? sf.slots[q] + "=" : std::string()) + param(p.v[q]);
      tbl.add({v(a) == v(b - 1) ? param(v(a)) : param(v(a)) + "–" + param(v(b - 1)), at, fmt("%.3f", p.st.mono), fmt("%+.2f", p.st.ls * 1e4), fmt("%.4f", p.st.ric)});
      a = b;
    }
    s += "\n" + tbl.str();
  }
  return s;
}

std::string years_text(const std::vector<Year> &ys) {
  if (ys.empty())
    return "";
  const auto neg = std::count_if(ys.begin(), ys.end(), [](const Year &y) { return y.st.mono < 0.0; });
  text::Table tbl({"年", "行", "mono", "每期毛多空 bp", "rIC"});
  for (const Year &y : ys)
    tbl.add({std::to_string(y.year), std::to_string(y.n), fmt("%.3f", y.st.mono), fmt("%+.2f", y.st.ls * 1e4), fmt("%.4f", y.st.ric)});
  return "- 最佳权重分年 (" + std::to_string(ys.size()) + " 年, mono 为负 " + std::to_string(neg) + " 年; 只看是否处处成立, 不据单年挑参数):\n\n" + tbl.str() + "\n";
}

} // namespace

std::string TrialRes::text() const {
  const Staged &st = k.st;
  const Result &r = st.res;
  std::string s = "### " + st.cand.name + " (" + st.cand.name_cn + ")\n`" + st.cand.expr + "`\n";
  if (!st.cand.note.empty())
    s += "- 思路 (" + st.cand.hyp + "): " + st.cand.note + "\n";
  s += Detail(r) + GateLine(r, gate) + r.rep_text;
  if (!r.visible)
    return s;
  if (const Weight *w = r.best(); w && !w->grp.empty()) {
    s += "- 最佳权重的 20 组每期毛收益 (减组间均值) G1 → G20: " + bp_spark(w->grp) + "\n";
    s += years_text(w->years);
  }
  return s + surface_text(r.surf);
}

ToolOut TrialOut(const Trial &k, const Gate &g) {
  auto r = std::make_shared<TrialRes>();
  r->k = k, r->gate = g;
  return Done(std::move(r), k.st.ok() && k.st.res.verdict != Verdict::Broken);
}

std::string EvalMiss(const Call &c, const Board &b, int round) {
  const Hyp *h = b.find(c.str("hyp"));
  if (!h)
    return "假设板上没有 " + c.str("hyp") + ": 先 hypothesize, 用观察检验并 judge 为支持, 再挂变体";
  if (h->stance != Stance::Supported)
    return h->id + " 的判定是 " + def(h->stance).cn + ": 只有判为支持的假设能挂变体";
  if (h->pending && h->pending != round)
    return h->id + " 有第 " + std::to_string(h->pending) + " 轮回测过的变体还没判: 先 judge 它";
  if (h->evals >= kHypEvals)
    return h->id + " 的 " + std::to_string(kHypEvals) + " 个变体已用完: 回到观察与研报找还没解释的现象, 提新假设";
  return {};
}

json SampleEvalFactor(Feed &p) {
  const std::string leaf = p.leaf();
  const bool ts = std::any_of(p.ctx.ops.begin(), p.ctx.ops.end(), [](const Op &o) { return o.name == "TsMeanRoll"; });
  return {{"name", "bench_" + std::to_string(p.below(100000))},
          {"name_cn", "工作台试样"},
          {"expr", ts ? "CsRank(TsMeanRoll(" + leaf + ", d))" : "CsRank(" + leaf + ")"},
          {"hyp", p.hyp(true)},
          {"note", "工作台试样: 检验 " + leaf + " 的截面排序"}};
}

} // namespace GUI::Agent
