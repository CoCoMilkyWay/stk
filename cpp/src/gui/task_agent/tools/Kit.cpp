// Kit — 见头文件
#include "gui/task_agent/tools/Kit.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Hyp.hpp"
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/tools/Misc.hpp"
#include "gui/task_agent/tools/Note.hpp"
#include "gui/task_agent/tools/Query.hpp"
#include "gui/task_agent/tools/Trial.hpp"

#include <cassert>
#include <cstdio>
#include <iterator>
#include <utility>
#include <variant>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

void Put(Act &a, ToolOut o) {
  a.ok = o.ok;
  a.out = std::move(o.out);
  a.gist = std::move(o.gist);
  a.res = std::move(o.res);
}

void Clip(std::string &s) {
  const size_t n = text::chars(s), cut = text::fwd(s, 0, static_cast<size_t>(kOutChars));
  if (cut == s.size())
    return;
  s.resize(cut);
  s += "\n…(截断: 共 " + std::to_string(n) + " 字, 只给前 " + std::to_string(kOutChars) + " 字; 缩小范围或分几次看)\n";
}

void RunSync(Act &a, Seat &s) {
  const Call &c = a.call;
  assert(def(c.tool).exec == Exec::Sync && "不是当场工具");
  switch (c.tool) {
  case Tool::SearchLeaves:
    return Put(a, SearchLeaves(c, s));
  case Tool::ListTried:
    return Put(a, ListTried(c, s));
  case Tool::ListLibrary:
    return Put(a, ListLibrary(c, s));
  case Tool::ListNotes:
    return Put(a, ListNotes(c, s));
  case Tool::SubmitFactor:
    return Put(a, SubmitFactor(c, s));
  case Tool::RequestFeature:
    return Put(a, RequestFeature(c, s));
  case Tool::RequestTool:
    return Put(a, RequestTool(c, s));
  case Tool::NoteInsight:
    return Put(a, NoteInsight(c, s));
  case Tool::ReportIssue:
    return Put(a, ReportIssue(c, s));
  case Tool::Hypothesize:
    return Put(a, Hypothesize(c, s));
  case Tool::Judge:
    return Put(a, Judge(c, s));
  case Tool::Finish:
    return Put(a, Finish(c, s));
  default:
    assert(false && "不是当场工具");
  }
}

bool SendLab(Act &a, Seat &s, bool gpu) {
  const Call &c = a.call;
  assert(def(c.tool).exec == Exec::Lab && "不是后台工具");
  if (c.tool != Tool::EvalFactor) {
    s.lab.send(c);
    return true;
  }
  Trial &k = a.extra.emplace<Trial>();
  Cand cand{c.str("name"), c.str("name_cn"), c.str("expr"), c.str("hyp"), c.str("note")};
  if (std::string miss = EvalMiss(c, s.st.board, s.me.round); !miss.empty()) {
    k.st.cand = std::move(cand);
    k.st.res = {.name = k.st.cand.name, .name_cn = k.st.cand.name_cn, .tmpl = k.st.cand.expr, .note = k.st.cand.note, .error = std::move(miss), .verdict = Verdict::Unstaged, .visible = false};
  } else {
    const Hyp *h = s.st.board.find(cand.hyp);
    assert(h);
    k.st = s.lab.stage(std::move(cand), h->title());
  }
  if (!k.st.ok()) {
    Put(a, TrialOut(k, s.gate));
    return false;
  }
  s.lab.send(EvalJob{k.st.file, gpu});
  return true;
}

void SettleLab(Act &a, LabOut out, const Gate &g) {
  assert(!std::holds_alternative<std::monostate>(out) && "取消的不收");
  if (Result *res = std::get_if<Result>(&out)) {
    Trial &k = std::get<Trial>(a.extra);
    k.st.res = std::move(*res);
    assert(k.st.res.file == k.st.file);
    return Put(a, TrialOut(k, g));
  }
  Put(a, std::move(std::get<ToolOut>(out)));
}

// =============================================================================
// 随机输入的取材
// =============================================================================
namespace {

std::string dash(int ymd) {
  char b[16];
  std::snprintf(b, sizeof(b), "%04d-%02d-%02d", ymd / 10000, ymd / 100 % 100, ymd % 100);
  return b;
}

} // namespace

int Feed::below(int n) {
  assert(n > 0);
  return std::uniform_int_distribution<int>(0, n - 1)(rng);
}
bool Feed::coin(double p) { return std::uniform_real_distribution<double>(0.0, 1.0)(rng) < p; }

std::string Feed::leaf() { return ctx.leaves.empty() ? std::string("bar.1d.close") : one(ctx.leaves).code; }

std::string Feed::factor() {
  std::vector<const Result *> ok;
  for (const Result &r : tried)
    if (r.visible && (r.best() || r.top))
      ok.push_back(&r);
  if (!ok.empty() && coin(0.8))
    return one(ok)->name;
  return "CsRank(" + leaf() + ")";
}

std::string Feed::day() { return ctx.days.empty() ? std::string() : dash(one(ctx.days)); }
std::string Feed::code() { return ctx.codes.empty() ? std::string("000001.SZ") : one(ctx.codes); }
std::string Feed::clock() {
  if (ctx.clock.size() <= 1)
    return {};
  return ctx.clock[static_cast<size_t>(ctx.open + below(static_cast<int>(ctx.clock.size()) - ctx.open))];
}

std::string Feed::hyp(bool supported) {
  const std::vector<Hyp> &hs = board.all();
  if (hs.empty())
    return "H1";
  if (supported)
    for (auto it = hs.rbegin(); it != hs.rend(); ++it)
      if (it->stance == Stance::Supported && it->evals < kHypEvals)
        return it->id;
  return one(hs).id;
}

std::vector<std::string> Feed::refs(int n) {
  std::vector<std::string> out;
  for (size_t i = timeline.size(); i-- > 0 && static_cast<int>(out.size()) < n;)
    for (size_t j = timeline[i].acts.size(); j-- > 0 && static_cast<int>(out.size()) < n;)
      if (const Act &a = timeline[i].acts[j]; a.span.ended() && a.ok)
        out.push_back(RefText({static_cast<int>(i) + 1, static_cast<int>(j) + 1}));
  return out;
}

json SampleArgs(Tool t, Feed &p) {
  using Fn = json (*)(Feed &);
  static constexpr Fn kSamples[] = {
      SampleViewFeature, SampleViewLayers, SampleViewTrades, SampleViewHolders, SampleViewResponse, SampleViewExplain, SampleViewSnapshot,
      SampleViewStockDay, SampleViewStockDays, SampleViewBook, SampleSearchLeaves, SampleSearchDocs, SampleSearchFactors, SampleReadDoc,
      SampleHypothesize, SampleJudge, SampleEvalFactor, SampleListTried, SampleListLibrary, SampleListNotes, SampleSubmitFactor,
      SampleRequestFeature, SampleRequestTool, SampleNoteInsight, SampleReportIssue, SampleFinish, SampleAskHuman};
  static_assert(std::size(kSamples) == kToolCount, "取材表与 Tool 一一对应");
  return kSamples[static_cast<size_t>(t)](p);
}

} // namespace GUI::Agent
