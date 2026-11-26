// Feed — 见头文件
#include "gui/task_agent/tools/Feed.hpp"
#include "gui/task_agent/tools/All.hpp"

#include "factor/Expr.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <cassert>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

namespace {

constexpr int kSpotTries = 64;    // 挑当日有逐笔的票: 随机试几次
constexpr double kSpotSeen = 0.7; // 工作台有 view_trades 明细时取它的段的概率

// 叶名 档.层.列 的层段是 grid
bool on_grid(const std::string &code, const std::string &grid) {
  const size_t p = code.find('.');
  return p != std::string::npos && code.compare(p + 1, grid.size() + 1, grid + ".") == 0;
}

} // namespace

int Feed::below(int n) {
  assert(n > 0);
  return std::uniform_int_distribution<int>(0, n - 1)(rng);
}
bool Feed::coin(double p) { return std::uniform_real_distribution<double>(0.0, 1.0)(rng) < p; }

std::string Feed::pick(Tool t, std::string_view key) {
  const ArgDefs as = doc(t).args;
  const auto a = std::ranges::find_if(as, [&](const ArgDef &x) { return key == x.key; });
  assert(a != as.end() && !a->choices.empty() && "参数表里须有这个带可选集的参数");
  const int e = a->empty_ok(), i = below(static_cast<int>(a->choices.size()) + e) - e;
  return i < 0 ? std::string() : std::string(a->choices[static_cast<size_t>(i)]);
}

std::string Feed::leaf() { return ctx.leaves.empty() ? std::string("bar.1d.close") : one(ctx.leaves).code; }

std::vector<std::string> Feed::leaves(int n) {
  std::vector<std::string> v;
  for (const Leaf &l : ctx.leaves)
    v.push_back(l.code);
  std::shuffle(v.begin(), v.end(), rng);
  v.resize(std::min(v.size(), static_cast<size_t>(n)));
  return v;
}

std::vector<std::string> Feed::grid() {
  std::vector<std::string> v;
  for (const Leaf &l : ctx.leaves)
    if (on_grid(l.code, ctx.grid))
      v.push_back(l.code);
  return v;
}

std::string Feed::factor() {
  std::vector<const Result *> ok;
  for (const Result &r : tried)
    if (r.visible && r.best())
      ok.push_back(&r);
  if (!ok.empty() && coin(0.8))
    return one(ok)->name;
  const std::vector<std::string> g = grid();
  const std::string x = g.empty() ? leaf() : one(g);
  std::vector<std::string> roots;
  for (const Op &o : ctx.ops)
    if (const int i = factor::expr::op_index(o.name); factor::expr::cs_root(i))
      roots.push_back(o.name + "(" + x + ")");
    else if (i == factor::expr::kTsRoot)
      roots.push_back("TsRankRoll(" + x + ", d=" + std::to_string(ctx.ts_root_d) + ")");
  return roots.empty() ? "CsRank(" + x + ")" : one(roots);
}

std::string Feed::day() { return ctx.days.empty() ? std::string() : misc::dash(one(ctx.days)); }

std::pair<std::string, std::string> Feed::span(int lo, int hi) {
  assert(lo >= 1 && lo <= hi);
  const int D = static_cast<int>(ctx.days.size());
  if (D == 0)
    return {};
  const int n = std::min(D, lo + below(hi - lo + 1)), d0 = below(D - n + 1);
  return {misc::dash(ctx.days[static_cast<size_t>(d0)]), misc::dash(ctx.days[static_cast<size_t>(d0 + n - 1)])};
}

json Feed::window(json j) {
  if (coin(0.3) && !ctx.days.empty()) {
    const auto [a, b] = span(20, 60);
    j["from"] = a, j["to"] = b;
  }
  return j;
}

std::string Feed::code() { return ctx.codes.empty() ? std::string("000001.SZ") : one(ctx.codes); }
std::string Feed::clock() {
  if (ctx.clock.size() <= 1)
    return {};
  return ctx.clock[static_cast<size_t>(ctx.open + below(static_cast<int>(ctx.clock.size()) - ctx.open))];
}

std::string Feed::tod(int cap, const std::string &around) {
  assert(cap >= 1);
  const int seg = static_cast<int>(ctx.clock.size()), lo = ctx.open;
  if (seg <= 1)
    return {};
  const int n = 1 + below(std::min(cap, seg - lo));
  int k0 = lo + below(seg - lo - n + 1);
  if (const auto it = std::find(ctx.clock.begin() + lo, ctx.clock.end(), around); it != ctx.clock.end())
    k0 = std::clamp(static_cast<int>(it - ctx.clock.begin()) - n / 2, lo, seg - n);
  return ctx.clock[static_cast<size_t>(k0)] + "-" + ctx.clock[static_cast<size_t>(k0 + n - 1)];
}

Spot Feed::spot() {
  std::vector<Spot> seen;
  for (const Step &sp : timeline)
    if (const TradesRes *t = sp.act ? As<Tool::ViewTrades>(*sp.act) : nullptr)
      for (const TradesRes::Seg &s : t->list)
        seen.push_back({s.code, s.date, ctx.clock.size() > 1 ? s.time.substr(0, 5) : std::string()});
  if (!seen.empty() && coin(kSpotSeen))
    return one(seen);
  const size_t D = ctx.days.size(), N = ctx.codes.size();
  assert(ctx.traded.size() == D * N);
  for (int i = 0; i < kSpotTries && D > 0 && N > 0; ++i) {
    const size_t d = static_cast<size_t>(below(static_cast<int>(D))), a = static_cast<size_t>(below(static_cast<int>(N)));
    if (ctx.traded[d * N + a])
      return {ctx.codes[a], misc::dash(ctx.days[d]), clock()};
  }
  return {code(), day(), clock()};
}

std::vector<std::string> Feed::refs(int n) {
  std::vector<std::string> out;
  for (size_t i = timeline.size(); i-- > 0 && static_cast<int>(out.size()) < n;)
    if (const std::optional<Act> &a = timeline[i].act; a && a->span.ended() && a->out.ok)
      out.push_back(StepTag(static_cast<int>(i) + 1));
  return out;
}

namespace {
using SampleFn = json (*)(Feed &);
#define AGENT_TOOL_SAMPLE(T, id, cn, F, C, keep, R) &Sample##T,
constexpr Per<Tool, SampleFn> kSamples{{{AGENT_TOOLS(AGENT_TOOL_SAMPLE)}}};
#undef AGENT_TOOL_SAMPLE
} // namespace

json SampleArgs(Tool t, Feed &p) { return kSamples[t](p); }

} // namespace GUI::Agent
