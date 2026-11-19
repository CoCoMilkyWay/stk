// eval_factor — 回测: 三拍对仗 OpenEvalFactor (核假设落盘, GUI) → LabTask (Search + Run, Lab) → CloseEvalFactor (入表封存, GUI)
//               → TrialRes (结局 + 最佳权重 20 组 / 分年 + 参数扫描) → 原文; 展示见 ui/trial/EvalFactor.cpp
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Bay.hpp"
#include "gui/task_agent/tools/Trial.hpp"

#include "factor/Expr.hpp"
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

constexpr size_t kSweepBins = 6;
constexpr size_t kSweepSlots = 3;
constexpr size_t kSweepTop = 12;

struct EvalYield final : Res {
  Sweep sweep;
  std::string text() const override { return {}; }
  void draw() const override {}
};

std::string bp_spark(const std::vector<double> &v) {
  const auto [lo, hi] = std::minmax_element(v.begin(), v.end());
  return "`" + text::bars(v) + "`  [" + text::bp(*lo) + ", " + text::bp(*hi) + "] bp";
}

std::string param(double v) { return v == std::round(v) ? fmt("%.0f", v) : fmt("%.4g", v); }

// 参数扫描 (pts 已按中位 rIC 降序): 参数 ≤ kSweepSlots 个时每个参数按取值分段、段内取中位 rIC 最高的点; 多了改列最高的 kSweepTop 个点
std::string sweep_text(const Sweep &sw) {
  if (sw.slots.empty() || sw.pts.size() < 2)
    return "";
  const size_t n = sw.pts.size(), per = (n + kSweepBins - 1) / kSweepBins;
  const Roll &r0 = sw.pts.front().roll;
  const std::string head = "- 参数扫描 (Search 评过 " + std::to_string(n) + " 个点, 滚动窗 " + std::to_string(r0.win) + " 日; ";
  const auto row_of = [](const Point &p, std::vector<std::string> row) {
    row.push_back(p.sign < 0 ? "反" : "正");
    row.push_back(fmt("%.4f", p.roll.ric_med)), row.push_back(fmt("%.4f", p.roll.ric_mean)), row.push_back(fmt("%+.1f", p.roll.ann_med * 100.0));
    row.push_back(fmt("%.3f", p.st.mono)), row.push_back(fmt("%+.2f", p.st.ls * 1e4));
    return row;
  };
  const std::vector<std::string> tail = {"向", "中位 rIC", "平均 rIC", "年化毛多空 中位 %", "全期 mono", "全期每期毛多空 bp"};
  if (sw.slots.size() > kSweepSlots) {
    std::vector<std::string> hs = sw.slots;
    hs.insert(hs.end(), tail.begin(), tail.end());
    text::Table tbl(hs);
    for (size_t i = 0; i < std::min(n, kSweepTop); ++i) {
      std::vector<std::string> row;
      for (const double x : sw.pts[i].v)
        row.push_back(param(x));
      tbl.add(row_of(sw.pts[i], std::move(row)));
    }
    return head + "参数多, 只列中位 rIC 最高的 " + std::to_string(std::min(n, kSweepTop)) + " 个):\n\n" + tbl.str();
  }
  std::string s = head + "每个参数按取值分段, 段内取中位 rIC 最高的点):\n";
  for (size_t j = 0; j < sw.slots.size(); ++j) {
    std::vector<size_t> ix(n);
    for (size_t i = 0; i < n; ++i)
      ix[i] = i;
    const auto v = [&](size_t i) { return sw.pts[ix[i]].v[j]; };
    std::stable_sort(ix.begin(), ix.end(), [&](size_t a, size_t b) { return sw.pts[a].v[j] < sw.pts[b].v[j]; });
    std::vector<std::string> hs = {sw.slots[j] + " 段", "最好的点"};
    hs.insert(hs.end(), tail.begin(), tail.end());
    text::Table tbl(hs);
    for (size_t a = 0; a < n;) {
      size_t b = std::min(n, a + per);
      while (b < n && v(b) == v(b - 1))
        ++b;
      size_t best = a;
      for (size_t i = a; i < b; ++i)
        if (sw.pts[ix[i]].roll.ric_med > sw.pts[ix[best]].roll.ric_med)
          best = i;
      const Point &p = sw.pts[ix[best]];
      std::string at;
      for (size_t q = 0; q < sw.slots.size(); ++q)
        at += (q ? ", " : "") + (sw.slots.size() > 1 ? sw.slots[q] + "=" : std::string()) + param(p.v[q]);
      tbl.add(row_of(p, {v(a) == v(b - 1) ? param(v(a)) : param(v(a)) + "–" + param(v(b - 1)), at}));
      a = b;
    }
    s += "\n" + tbl.str();
  }
  return s;
}

std::string years_text(const std::vector<Year> &ys) {
  if (ys.empty())
    return "";
  const auto neg = std::count_if(ys.begin(), ys.end(), [](const Year &y) { return y.st.ric < 0.0; });
  text::Table tbl({"年", "行", "rIC", "mono", "每期毛多空 bp"});
  for (const Year &y : ys)
    tbl.add({std::to_string(y.year), std::to_string(y.n), fmt("%.4f", y.st.ric), fmt("%.3f", y.st.mono), fmt("%+.2f", y.st.ls * 1e4)});
  return "- 最佳权重分年 (" + std::to_string(ys.size()) + " 年, rIC 为负 " + std::to_string(neg) + " 年; 只看是否处处成立, 不据单年挑参数):\n\n" + tbl.str() + "\n";
}

Sweep sweep_of(const Engine::SearchResult &r) {
  Sweep s{.slots = r.slots};
  for (const misc::Ymd d : r.dates)
    s.dates.push_back(static_cast<int>(d));
  for (const Engine::SearchCand &c : r.cands) {
    if (!c.roll.ok())
      continue;
    const factor::search::Roll &x = c.roll;
    s.pts.push_back({c.v, c.sign, {x.win, x.n, x.ric_med, x.ric_mean, x.ann_med, x.ann_mean}, {c.mono, c.ls, c.ric}, c.line});
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
  yield->sweep = sweep_of(sres);
  if (sres.n_weights == 0)
    return {true, {}, {}, std::move(yield)};
  bay.step(LabStep::Run);
  std::vector<Engine::FactorRow> frows;
  Engine::ScanFactorFile(fsc.dir, ssp.file, ft, frows);
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

} // namespace

std::string TrialRes::text() const {
  const Result &r = st.res;
  std::string s = "### " + st.cand.name + " (" + st.cand.name_cn + ")\n`" + st.cand.expr + "`\n";
  if (!st.cand.note.empty())
    s += "- 思路 (" + st.cand.hyp + "): " + st.cand.note + "\n";
  s += Detail(r) + GateLine(r, gate);
  if (!r.visible)
    return s;
  if (const Weight *w = r.best(); w && !w->grp.empty()) {
    s += "- 最佳权重的 20 组每期毛收益 (减组间均值) G1 → G20: " + bp_spark(w->grp) + "\n";
    s += years_text(w->years);
  }
  return s + sweep_text(r.sweep);
}

std::optional<LabTask> OpenEvalFactor(Act &a, Seat &s) {
  const Call &c = a.call;
  auto r = std::make_shared<TrialRes>();
  r->gate = s.gate;
  Cand cand{c.str("name"), c.str("name_cn"), c.str("expr"), c.str("hyp"), c.str("note")};
  // 工作台 / 外部 agent 不查假设 (板上没有也照回测)
  if (std::string miss = s.free ? std::string() : eval_miss(c, s.st.board, s.me.round); !miss.empty()) {
    r->st.cand = std::move(cand);
    r->st.res = {.name = r->st.cand.name, .name_cn = r->st.cand.name_cn, .tmpl = r->st.cand.expr, .note = r->st.cand.note, .error = std::move(miss), .verdict = Verdict::Unstaged, .visible = false};
  } else {
    const Hyp *h = s.st.board.find(cand.hyp);
    assert(h || s.free);
    r->st = s.lab.stage(std::move(cand), h ? h->title() : std::string());
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
  Sweep sweep;
  if (auto *y = static_cast<EvalYield *>(out.res.get()))
    sweep = std::move(y->sweep);
  r->st.res = s.lab.absorb(r->st.file, out.ok, out.text, std::move(sweep));
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
