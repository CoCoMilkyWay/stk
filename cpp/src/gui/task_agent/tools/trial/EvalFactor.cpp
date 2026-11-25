// eval_factor — 回测: 核验落盘 (GUI) → Search + Run (Lab) → 入表封存 (GUI 收尾 Commit)
//               → TrialRes (最佳权重的具体式 (负号前置) + 频率 / 预测周期 + 各权重统计与门槛 + 分年 → 累计分层 (Run 顺带的最佳权重逐日序列) / 分年 rIC 两张正负柱图)
//               → 原文; 展示见 ui/trial/EvalFactor.cpp
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Bay.hpp"
#include "gui/task_agent/tools/Trial.hpp"

#include "factor/Expr.hpp"
#include "factor/Stat/Contract.hpp"
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

// 累计分层: 20 组各自超额日收益的对数累计 Σ ln(1 + r) (评估期全段; 与展示的分层图同口径)
std::string layers_text(const Daily &d) {
  std::vector<double> v;
  for (int k = 0; k < factor::stat::kGroups; ++k) {
    double c = 0.0;
    for (const float r : d.lay[static_cast<size_t>(k)])
      c += std::log1p(static_cast<double>(r));
    v.push_back(c);
  }
  return text::vbars("累计分层 G1 → G20 (各组超额的对数累计, 评估期全段)", v, text::chg);
}

// 分年 rIC: 年内 ok 行 rIC 均值 (与主行 rIC 同口径)
std::string years_ric_text(const std::vector<Year> &ys) {
  std::vector<double> v;
  for (const Year &y : ys)
    v.push_back(y.ric);
  return text::vbars("分年 rIC " + std::to_string(ys.front().year) + " → " + std::to_string(ys.back().year) + " (年内 rIC 均值)", v, [](double x) { return fmt("%+.3f", x); });
}

// Search 概况: 自由参数个数 / 算出滚动窗的点数 / 中位 rIC 最高 · 次高 · 各点中位 (最佳点是孤峰还是一片高地)
std::string sweep_line(const Sweep &sw) {
  const std::vector<Point> &ps = sw.pts;
  std::string s = sw.slots.empty() ? std::string("Search: 无自由参数, 只评 1 个点")
                                   : "Search: 自由参数 " + std::to_string(sw.slots.size()) + " 个, 算出滚动窗的点 " + std::to_string(ps.size()) + " 个";
  if (ps.size() >= 2)
    s += fmt(" · 中位 rIC 最高 %.3f", ps[0].score) + fmt(" / 次高 %.3f", ps[1].score) + fmt(" / 各点中位 %.3f", ps[ps.size() / 2].score);
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
    assert(x.nav.n == Engine::kNavExcess && x.nav.P == static_cast<int>(dl.dates.size()) && x.ric.size() == dl.dates.size());
    Daily d{.expr = rows[idx[e]].expr, .sign = rows[idx[e]].sign, .dates = dl.dates};
    double s = 0.0, s2 = 0.0;
    int m = 0;
    for (const float v : x.ric) {
      if (!std::isnan(v))
        s += v, s2 += static_cast<double>(v) * v, ++m;
      d.cum.push_back(static_cast<float>(s));
    }
    const double mean = m ? s / m : 0.0, var = m > 1 ? (s2 - m * mean * mean) / (m - 1) : 0.0;
    d.ir = var > 0.0 ? mean / std::sqrt(var) * std::sqrt(static_cast<double>(factor::stat::kDaysPerYear)) : 0.0;
    for (int i = 0; i < Engine::kNavExcess; ++i)
      (i < Engine::kNavLayers ? d.lay : d.ls).emplace_back(x.nav.at(i), x.nav.at(i) + x.nav.P);
    d.turn.assign(std::begin(x.turn), std::end(x.turn));
    out.push_back(std::move(d));
  }
  return out;
}

// GUI 线程收尾: 候选目录按这趟的结局重扫, 入表封存 (BROKEN 之外都算做成)
Commit absorb(bool ok, std::string err, Sweep sweep, std::vector<Daily> days) {
  return [ok, err = std::move(err), sweep = std::move(sweep), days = std::move(days)](ToolOut &o, Desk &desk) mutable {
    auto r = std::static_pointer_cast<TrialRes>(o.res);
    assert(r);
    r->st.res = desk.absorb(r->st.file, ok, err, std::move(sweep), std::move(days));
    const bool fine = r->st.res.verdict != Verdict::Broken;
    o = Done(std::move(r), fine);
  };
}

Commit run_eval(const Engine::FactorScope &fsc, const std::string &file, const std::string &tmpl, Bay &bay) {
  bay.step(LabStep::Search);
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
    return absorb(false, src == Engine::Rc::Stale ? Engine::kStaleMsg : why.empty() ? "Search 中止"
                                                                                    : why,
                  {}, {});
  Sweep sweep = sweep_of(sres);
  if (sres.n_weights == 0)
    return absorb(true, {}, std::move(sweep), {});
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
    return absorb(false, rc == Engine::Rc::Stale ? Engine::kStaleMsg : "Run 中止", std::move(sweep), {});
  return absorb(true, {}, std::move(sweep), days_of(frows, idx, dl));
}

} // namespace

const Weight *TrialRes::shown() const { return st.res.visible && st.res.verdict == Verdict::Weights ? st.res.best() : nullptr; }

std::string Formula(const Weight &w) { return (w.sign < 0 ? "-" : "") + w.expr; }

std::string ScopeLine(const TrialRes &k) { return "频率 " + k.level + " · 预测周期 " + k.hold; }

std::string WeightLine(const TrialRes &k, const Weight &w) {
  const Weight *best = k.shown();
  assert(best);
  const std::string head = &w == best ? std::string() : "另一权重 `" + Formula(w) + "`: ";
  if (!w.has)
    return head + "无该持有期统计";
  std::string s = head + fmt("rIC %.3f", w.ric) + fmt(" · 中位 rIC %.3f", w.score) + fmt(" · 每期毛多 %+.2fbp", w.lng * 1e4) + fmt(" · 毛空 %+.2fbp", w.sht * 1e4) +
                  fmt(" · 净多空 %+.2fbp", w.net * 1e4) + fmt(" · 成本 %.2fbp", w.cost * 1e4) + fmt(" (单边换手 买 %.0f%%", w.buy * 100) +
                  fmt(" / 卖 %.0f%%", w.sell * 100) + fmt(", × 固定冲击 %.0fbp)", w.impact * 1e4);
  if (&w == best)
    s += GateMiss(k.gate, k.st.res).empty() ? " · 达入库门槛" : fmt(" · 未达入库门槛 %.3f", k.gate.ric);
  return s;
}

std::string YearsLine(const std::vector<Year> &ys) {
  assert(!ys.empty());
  std::string s = "年份(中位rIC,年换手):";
  for (size_t i = 0; i < ys.size(); ++i)
    s += (i ? ", " : " ") + std::to_string(ys[i].year) + "(" + text::r3(ys[i].med) + fmt(",%.0f)", ys[i].turn);
  return s;
}

std::string GroupsHead() { return "最佳权重 20 组每期毛收益 G1 → G20 (减组间均值, bp)"; }

std::string SweepHead(const Sweep &sw) { return "参数扫描 (Search 评过 " + std::to_string(sw.pts.size()) + " 个点)"; }

std::string TrialRes::text() const {
  const Result &r = st.res;
  const Weight *w = shown();
  std::string s = "### " + st.cand.name + " (" + st.cand.name_cn + ")\n`" + (w ? Formula(*w) : st.cand.expr) + "`\n";
  if (!st.cand.note.empty())
    s += "- 思路: " + st.cand.note + "\n";
  if (!w)
    return s + Detail(r);
  s += "- " + ScopeLine(*this) + "\n";
  s += "- " + WeightLine(*this, *w) + "\n";
  for (const Weight &x : r.ws)
    if (&x != w)
      s += "- " + WeightLine(*this, x) + "\n";
  if (!w->years.empty())
    s += "- " + YearsLine(w->years) + "\n";
  if (r.sweep && !r.sweep->pts.empty())
    s += "- " + sweep_line(*r.sweep) + "\n";
  if (r.daily)
    s += layers_text(*r.daily);
  if (!w->years.empty())
    s += years_ric_text(w->years);
  return s;
}

std::string TrialRes::title() const { return st.cand.name + " " + st.cand.name_cn + " · " + Brief(st.res); }

std::string TrialRes::gist() const {
  return st.ok() ? st.cand.name + " `" + st.cand.expr + "` · " + text::head(st.cand.note, 60) + " → " + Brief(st.res) : std::string();
}

Effect TrialRes::effect() const { return {.staged = st.file, .verdict = st.res.verdict}; }

bool TrialRes::abort() {
  st.file.clear();
  st.res = Result{.name = st.cand.name,
                  .name_cn = st.cand.name_cn,
                  .tmpl = st.cand.expr,
                  .note = st.cand.note,
                  .error = "研究员取消了这次回测",
                  .verdict = Verdict::Unstaged,
                  .visible = false};
  return true;
}

json TrialRes::save() const { return {{"cand", st.cand}, {"file", st.file}, {"error", st.res.error}}; }

ResPtr TrialRes::load(const Call &, const json &j, Seat &s) {
  auto k = std::make_shared<TrialRes>();
  k->gate = s.gate;
  k->level = s.desk.ctx().level, k->hold = s.desk.ctx().hold;
  j.at("cand").get_to(k->st.cand);
  j.at("file").get_to(k->st.file);
  const std::vector<Result> &rs = s.desk.results();
  const auto it = k->st.ok() ? std::ranges::find(rs, k->st.file, &Result::file) : rs.end();
  k->st.res = it != rs.end() ? *it
              : k->st.ok()   ? Result{.error = "候选文件已不在", .verdict = Verdict::Pending}
                             : Result{.error = j.at("error").get<std::string>(), .verdict = Verdict::Unstaged};
  return k;
}

Prep EvalFactor(const Call &c, Seat &s) {
  auto r = std::make_shared<TrialRes>();
  r->gate = s.gate;
  r->level = s.desk.ctx().level, r->hold = s.desk.ctx().hold;
  r->st = s.desk.stage({c.str("name"), c.str("name_cn"), c.str("expr"), c.str("note")});
  if (!r->st.ok())
    return {Done(std::move(r), false), {}};
  const Engine::FactorScope fsc = s.desk.factor_scope(s.gpu);
  const std::string file = r->st.file, tmpl = r->st.res.tmpl;
  return {{.res = std::move(r)}, [fsc, file, tmpl](Bay &bay) { return run_eval(fsc, file, tmpl, bay); }};
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
  Cand c{.name_cn = "工作台试样"};
  for (int i = 0; i < kSampleTries; ++i) {
    const std::string &x = p.one(leaves);
    const std::string_view w = p.one(wraps), r = p.one(roots);
    const bool ts = r == "TsRankRoll";
    c.name = "bench_" + std::to_string(p.below(1000000));
    c.expr = std::string(r) + "(" + (w.empty() ? x : std::string(w) + "(" + x + ", d)") + (ts ? ", d=" + std::to_string(p.ctx.ts_root_d) : "") + ")";
    c.note = "工作台试样: 检验 " + x + (w.empty() ? "" : " 的 " + std::string(w)) + " 的" + (ts ? "时序" : "截面") + "排序";
    if (p.desk.vet(c).empty())
      break;
  }
  return {{"name", c.name}, {"name_cn", c.name_cn}, {"expr", c.expr}, {"note", c.note}};
}

} // namespace GUI::Agent
