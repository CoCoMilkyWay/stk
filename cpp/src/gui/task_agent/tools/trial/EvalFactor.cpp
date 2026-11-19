// eval_factor — 回测: 三拍对仗 OpenEvalFactor (核假设落盘, GUI) → LabTask (Search + Run + 代表点分层, Lab) → CloseEvalFactor (入表封存, GUI)
//               → TrialRes (结局 + 最佳权重 20 组 / 分年 + 参数曲面) → 原文; 展示见 ui/trial/EvalFactor.cpp
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Bay.hpp"
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/tools/Trial.hpp"

#include "factor/Expr.hpp"
#include "factor/Search/Space.hpp"
#include "gui/engine/File.hpp"
#include "gui/engine/Load.hpp"
#include "gui/engine/services/RunService.hpp"
#include "gui/engine/services/SearchService.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <utility>

namespace GUI::Agent {

using json = nlohmann::ordered_json;
using text::fmt;

namespace {

namespace fx = factor::expr;
namespace space = factor::search::space;

constexpr size_t kSurfBins = 6;
constexpr size_t kSurfSlots = 3;
constexpr size_t kSurfTop = 12;

struct EvalYield final : Res {
  Surface surf;
  std::string rep;
  std::string text() const override { return {}; }
  void draw() const override {}
};

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

Surface surface_of(const Engine::FeatureTable &ft, const Engine::SearchResult &r) {
  Surface s;
  if (r.slots.empty())
    return s;
  const fx::Expr t = ft.reparse_at(r.tmpl, r.hold);
  const std::vector<space::Slot> slots = space::slots_of(t, ft.seg, r.n_full * factor::stat::kGroups);
  assert(slots.size() == r.slots.size());
  const bool need_parse = std::ranges::any_of(slots, space::needs_qtab);
  s.slots = r.slots;
  s.pts.reserve(r.cands.size());
  for (const Engine::SearchCand &c : r.cands) {
    if (!c.sc.ok)
      continue;
    Probe p{.st = {c.sc.mono, c.sc.ls, c.sc.ric}};
    p.v.reserve(slots.size());
    if (!need_parse && c.u.size() == slots.size()) {
      for (size_t j = 0; j < slots.size(); ++j) {
        const space::Slot &x = slots[j];
        const double raw = space::to_v(x, c.u[j]);
        p.v.push_back(x.key == space::Key::D ? raw : static_cast<double>(fx::kv_of(raw)));
      }
    } else {
      const fx::Expr e = ft.reparse(c.expr);
      assert(e.nodes.size() == t.nodes.size() && "具体式与模板同构");
      for (const space::Slot &x : slots) {
        const auto &q = e.nodes[static_cast<size_t>(x.node)].p;
        p.v.push_back(x.key == space::Key::D    ? static_cast<double>(q.d)
                      : x.key == space::Key::K  ? q.k
                      : x.key == space::Key::K2 ? q.k2
                                                : q.w[x.wi]);
      }
    }
    s.pts.push_back(std::move(p));
  }
  return s;
}

// 假设不成的原因 (空 = 可挂)
std::string eval_miss(const Call &c, const Board &b, int round) {
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

ToolOut run_eval(const Engine::FactorScope &fsc, const std::string &file, const std::string &tmpl, Bay &bay) {
  const Engine::FeatureTable &ft = *fsc.ft;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  std::string dummy;
  [[maybe_unused]] const bool ok_rf = fx::root_frame(ft.reparse_at(tmpl, bay.env.hold), ft.seg, frame, dummy);
  assert(ok_rf);
  const Engine::SearchSpec ssp{file, tmpl, frame, bay.env.hold, true};
  Engine::SearchResult sres;
  std::string why;
  const Engine::Rc src = Engine::SearchFactor({fsc, ssp}, bay.flow, sres, why);
  if (bay.cancelled())
    return {};
  if (src != Engine::Rc::Ok || !sres.wrote)
    return Fail(src == Engine::Rc::Stale ? Engine::kStaleMsg : why.empty() ? "Search 中止"
                                                                           : why);
  auto yield = std::make_shared<EvalYield>();
  yield->surf = surface_of(ft, sres);

  std::vector<Engine::FactorRow> frows;
  Engine::ScanFactorFile(fsc.dir, ssp.file, ft, frows);
  if (sres.n_weights > 0) {
    bay.step(LabStep::Run);
    std::vector<size_t> idx;
    for (size_t i = 0; i < frows.size(); ++i)
      if (frows[i].widx >= 0 && frows[i].error.empty() && !frows[i].expr.empty())
        idx.push_back(i);
    Engine::Lease l(bay.flow);
    const Engine::Rc rc = l ? Engine::EvalFactorFile(fsc, bay.flow, frows, idx) : Engine::Rc::Cancel;
    if (bay.cancelled())
      return {};
    if (rc != Engine::Rc::Ok)
      return {false, rc == Engine::Rc::Stale ? Engine::kStaleMsg : "Run 中止", {}, std::move(yield)};
    return {true, {}, {}, std::move(yield)};
  }
  if (!frows.empty()) {
    const auto mk = std::find_if(frows[0].searched.begin(), frows[0].searched.end(), [&](const Engine::SearchMark &m) { return m.hold == ssp.hold; });
    if (mk != frows[0].searched.end() && mk->top) {
      bay.step(LabStep::Look);
      Engine::FactorFlow vflow(bay.cancel, bay.flow.prog, Engine::Prio::Interactive);
      yield->rep = RepLayers(mk->top->expr, mk->top->sign, bay.env, vflow);
    }
  }
  return {true, {}, {}, std::move(yield)};
}

} // namespace

std::string TrialRes::text() const {
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

std::optional<LabTask> OpenEvalFactor(Act &a, Seat &s) {
  const Call &c = a.call;
  auto r = std::make_shared<TrialRes>();
  r->gate = s.gate;
  Cand cand{c.str("name"), c.str("name_cn"), c.str("expr"), c.str("hyp"), c.str("note")};
  if (std::string miss = eval_miss(c, s.st.board, s.me.round); !miss.empty()) {
    r->st.cand = std::move(cand);
    r->st.res = {.name = r->st.cand.name, .name_cn = r->st.cand.name_cn, .tmpl = r->st.cand.expr, .note = r->st.cand.note, .error = std::move(miss), .verdict = Verdict::Unstaged, .visible = false};
  } else {
    const Hyp *h = s.st.board.find(cand.hyp);
    assert(h);
    r->st = s.lab.stage(std::move(cand), h->title());
  }
  if (!r->st.ok()) {
    a.out = Done(std::move(r), false);
    return std::nullopt;
  }
  const Engine::FactorScope fsc = FactorScopeOf(s.lab, s.gpu);
  const std::string file = r->st.file, tmpl = r->st.res.tmpl;
  a.out.res = std::move(r);
  return LabTask([fsc, file, tmpl](Bay &bay) { return run_eval(fsc, file, tmpl, bay); });
}

void CloseEvalFactor(Act &a, ToolOut out, Seat &s) {
  auto r = std::static_pointer_cast<TrialRes>(a.out.res);
  assert(r);
  Surface surf;
  std::string rep;
  if (auto *y = static_cast<EvalYield *>(out.res.get()))
    surf = std::move(y->surf), rep = std::move(y->rep);
  r->st.res = s.lab.absorb(r->st.file, out.ok, out.text, std::move(surf), std::move(rep));
  const bool ok = r->st.res.verdict != Verdict::Broken;
  a.out = Done(std::move(r), ok);
}

// 模板 = 归一根 (窗口统计 (本层叶, 裸 d)); 随机拼到过 stage 的全部查验为止 (试满 kSampleTries 次仍不过就交末一个, 原因见回测结果)
json SampleEvalFactor(Feed &p) {
  constexpr int kSampleTries = 64;
  constexpr std::string_view kRoots[] = {"CsRank", "CsNormRank", "CsZ", "TsRankRoll"};
  constexpr std::string_view kWraps[] = {"TsMeanRoll", "TsStdRoll", "TsSumRoll", "TsDeltaRoll", "TsMaxRoll", "TsMinRoll", "TsSlopeRoll", "TsZRoll", "TsWmaRoll", "TsSkewRoll"};
  const auto open = [&](std::string_view n) { return std::ranges::any_of(p.ctx.ops, [&](const Op &o) { return o.name == n; }); };
  std::vector<std::string_view> roots, wraps{std::string_view()};
  for (const std::string_view r : kRoots)
    if (open(r))
      roots.push_back(r);
  for (const std::string_view w : kWraps)
    if (open(w))
      wraps.push_back(w);
  if (roots.empty())
    roots.push_back(kRoots[0]);
  std::vector<std::string> leaves = p.grid();
  if (leaves.empty())
    leaves.push_back(p.leaf());
  Cand c{.name_cn = "工作台试样", .hyp = p.hyp(true)};
  for (int i = 0; i < kSampleTries; ++i) {
    const std::string &x = p.one(leaves);
    const std::string_view w = p.one(wraps), r = p.one(roots);
    const bool ts = r == "TsRankRoll";
    c.name = "bench_" + std::to_string(p.below(1000000));
    c.expr = std::string(r) + "(" + (w.empty() ? x : std::string(w) + "(" + x + ", d)") + (ts ? ", d=" + std::to_string(p.ctx.ts_root_d) : "") + ")";
    c.note = "工作台试样: 检验 " + x + (w.empty() ? "" : " 的 " + std::string(w)) + " 的" + (ts ? "时序" : "截面") + "排序";
    if (p.lab.vet(c).empty())
      break;
  }
  return {{"name", c.name}, {"name_cn", c.name_cn}, {"expr", c.expr}, {"hyp", c.hyp}, {"note", c.note}};
}

} // namespace GUI::Agent
