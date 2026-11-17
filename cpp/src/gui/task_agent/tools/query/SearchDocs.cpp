// search_docs — 查研报: 输入 query / scope / stage / approach / asset / freq / genre / n → DocsRes (分面 + 研报行) → 原文; 展示见 ui/query/SearchDocs.cpp
#include "gui/task_agent/Docs.hpp"
#include "gui/task_agent/Text.hpp"

#include <algorithm>
#include <cstdint>

namespace GUI::Agent {

using namespace docs;
using json = nlohmann::ordered_json;

std::string DocsRes::text() const {
  std::string out = "命中 " + std::to_string(hit) + " 篇 " + cond + "\n" + (loose ? "没有同时含全部 query 词的, 下列含其中任一词 (各条件单独命中: " + alone + ")\n" : "");
  if (!hit)
    return out + "各条件单独命中: " + alone + "\n去掉命中少的条件或换个词再查\n";
  out += "\n### 阶段 → 流派 (篇数; 一篇可落多处)\n";
  for (const Stage &s : tree)
    out += "- " + s.name + " " + std::to_string(s.n) + ": " + ranked_text(s.sub, SIZE_MAX) + "\n";
  out += "\n### 机构\n" + ranked_text(brokers, kTop) + "\n";
  std::string ys;
  for (const auto &[y, n] : years)
    ys += (ys.empty() ? "" : " · ") + y + " " + std::to_string(n);
  out += "\n### 年份\n" + ys + "\n\n### 价值 (high 可直接复用 / medium 有参考 / low 有水分)\n" + ranked_text(values, kTop) + "\n";
  out += "\n### 研报 (前 " + std::to_string(docs.size()) + " 篇; " + (full ? "命中处多的在前, 再按" : "按") + "价值高在前, 同价值新在前)\n";
  for (const Doc &d : docs) {
    out += "- `" + d.id + "` · 价值 " + d.value + " · 主阶段 " + d.primary + " · 流派 " + d.approaches;
    if (d.factors)
      out += " · 因子 " + std::to_string(d.factors);
    if (full)
      out += " · 命中 " + std::to_string(d.hits) + " 处";
    out += "\n";
    if (!d.finding.empty())
      out += "  结论: " + d.finding + "\n";
    for (const std::string &sn : d.snippets)
      out += "  " + sn + "\n";
  }
  return out;
}

ToolOut SearchDocs(const Call &c, const std::atomic<bool> &cancel) {
  const Lib &L = lib();
  const std::string scope = c.str("scope"), stage = c.str("stage"), ap = c.str("approach"), asset = c.str("asset"), freq = c.str("freq"), genre = c.str("genre");
  const std::vector<std::string> ws = text::words(c.str("query"));
  const bool full = scope == "text";
  if (!ap.empty() && !L.stage_of.count(ap))
    return Fail("没有流派 " + ap + " (写流派 code, 见不带 approach 时分面里的 阶段 → 流派)");
  if (full && ws.empty())
    return Fail("scope=text 须给 query");
  if (full && !L.ensure_all_texts(cancel))
    return {};
  const auto hay = [&](size_t i) -> const std::string & { return full ? L.text_at(i).low : L.docs[i].hay; };
  text::Filter f;
  if (!stage.empty())
    f.add("stage=" + stage, [&](size_t i) { return has(L.docs[i].stages, stage); });
  if (!ap.empty())
    f.add("approach=" + ap, [&](size_t i) { return has(L.docs[i].approaches, ap); });
  if (!asset.empty())
    f.add("asset=" + asset, [&](size_t i) { return has(L.docs[i].asset, asset); });
  if (!freq.empty())
    f.add("freq=" + freq, [&](size_t i) { return has(L.docs[i].freqs, freq); });
  if (!genre.empty())
    f.add("genre=" + genre, [&](size_t i) { return L.docs[i].genre == genre; });
  f.add(ws, "query 词 ", hay);
  bool loose = false;
  std::vector<size_t> hit = f.hits(L.docs.size(), loose);
  std::vector<int> hits(L.docs.size(), 0);
  for (const size_t i : hit)
    hits[i] = ws.empty() ? 1 : count_in(hay(i), ws);

  auto res = std::make_shared<DocsRes>();
  DocsRes &R = *res;
  R.cond = conditions(c), R.full = full, R.loose = loose, R.hit = hit.size();
  if (loose || hit.empty())
    R.alone = f.alone(L.docs.size());
  if (hit.empty())
    return Done(std::move(res));

  Count st, apn, broker, year, value;
  for (const size_t i : hit) {
    const Doc &d = L.docs[i];
    for (const std::string &s : d.stages)
      ++st[s];
    for (const std::string &a : d.approaches)
      ++apn[a];
    ++broker[d.broker], ++year[std::to_string(d.year)], ++value[d.value];
  }
  for (const docs::Stage &s : L.stages) {
    if (!st.count(s.code))
      continue;
    Count sub;
    for (const std::string &a : s.approaches)
      if (apn.count(a))
        sub[named(L, a)] = apn[a];
    R.tree.push_back({named(L, s.code), st[s.code], ranked(sub)});
  }
  R.brokers = ranked(broker), R.values = ranked(value);
  for (auto it = year.rbegin(); it != year.rend(); ++it)
    R.years.push_back(*it);

  std::stable_sort(hit.begin(), hit.end(), [&](size_t a, size_t b) {
    const Doc &x = L.docs[a], &y = L.docs[b];
    if (full && hits[a] != hits[b])
      return hits[a] > hits[b];
    if (value_rank(x.value) != value_rank(y.value))
      return value_rank(x.value) < value_rank(y.value);
    return x.id > y.id;
  });
  const size_t n = std::min(hit.size(), static_cast<size_t>(c.num("n")));
  for (size_t r = 0; r < n; ++r) {
    const size_t i = hit[r];
    const Doc &d = L.docs[i];
    DocsRes::Doc &x = R.docs.emplace_back();
    x.id = d.id, x.value = d.value, x.primary = d.primary, x.approaches = text::join(d.approaches, ", "), x.factors = d.factors.size(), x.hits = hits[i];
    if (!d.findings.empty())
      x.finding = text::head(text::flat(d.findings[0]), kBrief);
    if (full) {
      const docs::Text &t = L.text_at(i);
      x.snippets = snippets(t, *std::find_if(ws.begin(), ws.end(), [&](const std::string &w) { return text::has_word(t.low, w); }));
    }
  }
  return Done(std::move(res));
}

json SampleSearchDocs(Feed &p) {
  static const char *kStages[] = {"", "L1_factor", "L3_alpha", "L5_portfolio", "L7_execution"};
  static const char *kWords[] = {"", "订单流", "反转", "动量", "流动性", "高频", "分钟"};
  json j = {{"query", kWords[p.below(7)]}, {"stage", kStages[p.below(5)]}, {"n", 6 + p.below(10)}};
  if (p.coin(0.2) && !j["query"].get<std::string>().empty())
    j["scope"] = "text";
  return j;
}

} // namespace GUI::Agent
