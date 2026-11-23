// eval_factor — 回测: 三拍对仗 OpenEvalFactor (核假设落盘, GUI) → LabTask (Search + Run, Lab) → CloseEvalFactor (入表封存, GUI)
//               → TrialRes (符号 + 各权重 + 门槛 + 分年 → 参数扫描 → 20 组; Run 顺带最佳权重的逐日序列只给展示) → 原文; 展示见 ui/trial/EvalFactor.cpp
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
  std::vector<Daily> days;
  std::string text() const override { return {}; }
  void draw() const override {}
};

std::string param(double v) { return v == std::round(v) ? fmt("%.0f", v) : fmt("%.4g", v); }

// 参数扫描 (pts 已按中位 rIC 降序): 参数 ≤ kSweepSlots 个时每个参数按取值分段, 每段给点数 / 段内中位 rIC 的中位与最高 (及最高的点); 多了改列最高的 kSweepTop 个点
std::string sweep_text(const Sweep &sw) {
  if (sw.slots.empty() || sw.pts.size() < 2)
    return "";
  const size_t n = sw.pts.size(), per = (n + kSweepBins - 1) / kSweepBins;
  std::string s = "- " + SweepHead(sw);
  if (sw.slots.size() > kSweepSlots) {
    std::vector<std::string> hs = sw.slots;
    hs.push_back("中位 rIC");
    text::Table tbl(hs);
    for (size_t i = 0; i < std::min(n, kSweepTop); ++i) {
      std::vector<std::string> row;
      for (const double x : sw.pts[i].v)
        row.push_back(param(x));
      row.push_back(fmt("%.4f", sw.pts[i].score));
      tbl.add(std::move(row));
    }
    return s + ", 参数多, 只列中位 rIC 最高的 " + std::to_string(std::min(n, kSweepTop)) + " 个:\n\n" + tbl.str();
  }
  s += ", 每个参数按取值分段:\n";
  for (size_t j = 0; j < sw.slots.size(); ++j) {
    std::vector<size_t> ix(n);
    for (size_t i = 0; i < n; ++i)
      ix[i] = i;
    const auto v = [&](size_t i) { return sw.pts[ix[i]].v[j]; };
    std::stable_sort(ix.begin(), ix.end(), [&](size_t a, size_t b) { return sw.pts[a].v[j] < sw.pts[b].v[j]; });
    text::Table tbl({sw.slots[j] + " 段", "点数", "中位 rIC 段内中位", "段内最高", "最高的点"});
    for (size_t a = 0; a < n;) {
      size_t b = std::min(n, a + per);
      while (b < n && v(b) == v(b - 1))
        ++b;
      std::vector<double> sc;
      size_t best = a;
      for (size_t i = a; i < b; ++i) {
        sc.push_back(sw.pts[ix[i]].score);
        if (sw.pts[ix[i]].score > sw.pts[ix[best]].score)
          best = i;
      }
      std::sort(sc.begin(), sc.end());
      const double mid = 0.5 * (sc[(sc.size() - 1) / 2] + sc[sc.size() / 2]);
      const Point &p = sw.pts[ix[best]];
      std::string at;
      for (size_t q = 0; q < sw.slots.size(); ++q)
        at += (q ? ", " : "") + (sw.slots.size() > 1 ? sw.slots[q] + "=" : std::string()) + param(p.v[q]);
      tbl.add({v(a) == v(b - 1) ? param(v(a)) : param(v(a)) + "–" + param(v(b - 1)), std::to_string(b - a), fmt("%.4f", mid), fmt("%.4f", p.score), at});
      a = b;
    }
    s += "\n" + tbl.str();
  }
  return s;
}

Sweep sweep_of(const Engine::SearchResult &r) {
  Sweep s{.slots = r.slots};
  for (const misc::Ymd d : r.dates)
    s.dates.push_back(static_cast<int>(d));
  int sign = 0; // 写进的权重的符号 (cands 按中位 rIC 降序, 首个有窗的即权重)
  for (const Engine::SearchCand &c : r.cands) {
    if (!c.roll.ok())
      continue;
    if (!sign)
      sign = c.sign;
    Point p{c.v, c.roll.ric_med, c.line};
    if (c.sign != sign) {
      p.score = -p.score;
      for (float &x : p.line)
        x = -x;
    }
    s.pts.push_back(std::move(p));
  }
  std::stable_sort(s.pts.begin(), s.pts.end(), [](const Point &a, const Point &b) { return a.score > b.score; });
  return s;
}

// Run 顺带的逐日序列 → 每个算出来的权重一份
std::vector<Daily> days_of(const std::vector<Engine::FactorRow> &rows, const std::vector<size_t> &idx, const Engine::DayLines &dl) {
  std::vector<Daily> out;
  for (size_t e = 0; e < idx.size(); ++e) {
    const Engine::DayLine &x = dl.rows[e];
    if (x.nav.n == 0)
      continue;
    assert(x.nav.n == Engine::kNavExcess && x.nav.P == static_cast<int>(dl.dates.size()) && x.cum.size() == dl.dates.size());
    Daily d{.expr = rows[idx[e]].expr, .sign = rows[idx[e]].sign, .dates = dl.dates, .cum = x.cum};
    for (int i = 0; i < Engine::kNavExcess; ++i)
      (i < Engine::kNavLayers ? d.lay : d.ls).emplace_back(x.nav.at(i), x.nav.at(i) + x.nav.P);
    d.turn.assign(std::begin(x.turn), std::end(x.turn));
    out.push_back(std::move(d));
  }
  return out;
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
  Engine::DayLines dl{.hold = bay.env.hold};
  const Engine::Rc rc = l ? Engine::EvalFactorFile(fsc, bay.flow, frows, idx, {}, {}, &dl) : Engine::Rc::Cancel;
  if (bay.cancelled())
    return {};
  if (rc != Engine::Rc::Ok)
    return {false, rc == Engine::Rc::Stale ? Engine::kStaleMsg : "Run 中止", {}, std::move(yield)};
  yield->days = days_of(frows, idx, dl);
  return {true, {}, {}, std::move(yield)};
}

} // namespace

std::string GroupsHead() { return "最佳权重 20 组每期毛收益 G1 → G20 (减组间均值, bp)"; }

std::string YearsHead(const std::vector<Year> &ys) {
  assert(!ys.empty());
  const auto neg = std::count_if(ys.begin(), ys.end(), [](const Year &y) { return y.ric < 0.0; });
  const Year &lo = *std::min_element(ys.begin(), ys.end(), [](const Year &a, const Year &b) { return a.ric < b.ric; });
  return "最佳权重分年 rIC (" + std::to_string(ys.size()) + " 年, 为负 " + std::to_string(neg) + " 年, 最差 " + std::to_string(lo.year) + ")";
}

std::string SweepHead(const Sweep &sw) { return "参数扫描 (Search 评过 " + std::to_string(sw.pts.size()) + " 个点)"; }

std::string TrialRes::text() const {
  const Result &r = st.res;
  std::string s = "### " + st.cand.name + " (" + st.cand.name_cn + ")\n`" + st.cand.expr + "`\n";
  if (!st.cand.note.empty())
    s += "- 思路 (" + st.cand.hyp + "): " + st.cand.note + "\n";
  s += Detail(r) + GateLine(r, gate);
  if (!r.visible)
    return s;
  const Weight *w = r.best();
  if (w && !w->years.empty()) {
    std::vector<double> ric;
    for (const Year &y : w->years)
      ric.push_back(y.ric);
    s += "- " + YearsHead(w->years) + ": " + text::spark(ric) + "\n";
  }
  if (const std::string sw = sweep_text(r.sweep); !sw.empty())
    s += sw + "\n";
  if (w && !w->grp.empty())
    s += "- " + GroupsHead() + ": " + text::spark(w->grp, text::bp) + "\n";
  return s;
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
  std::vector<Daily> days;
  if (auto *y = static_cast<EvalYield *>(out.res.get()))
    sweep = std::move(y->sweep), days = std::move(y->days);
  r->st.res = s.lab.absorb(r->st.file, out.ok, out.text, std::move(sweep), std::move(days));
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
