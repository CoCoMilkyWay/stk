// search_factors — 查研报因子: 输入 query / family / period / horizon / approach / asset / sort / n → FactorsRes (分面 + 周期矩阵 + 因子行) → 原文; 展示见 ui/query/SearchFactors.cpp
#include "gui/task_agent/Docs.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Feed.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>

namespace GUI::Agent {

using namespace docs;
using json = nlohmann::ordered_json;

std::string FactorsRes::text() const {
  std::string out = "命中 " + std::to_string(hit) + " 个因子 (出自 " + std::to_string(docs) + " 篇) " + cond + "\n" +
                    (loose ? "没有同时含全部 query 词的, 下列含其中任一词 (各条件单独命中: " + alone + ")\n" : "");
  if (!hit)
    return out + "各条件单独命中: " + alone + "\n去掉命中少的条件或换个词再查\n";
  out += "\n### 类别\n" + ranked_text(families, SIZE_MAX) + "\n\n### 出处流派\n" + ranked_text(approaches, kTop) + "\n\n### 数据周期 × 预测期 (因子数)\n";
  std::vector<std::string> hs{"数据周期 \\ 预测期"};
  hs.insert(hs.end(), cols.begin(), cols.end());
  text::Table tbl(hs);
  for (size_t r = 0; r < rows.size(); ++r) {
    std::vector<std::string> cs{rows[r]};
    for (size_t h = 0; h < cols.size(); ++h)
      cs.push_back(cells[r * cols.size() + h] ? std::to_string(cells[r * cols.size() + h]) : "·");
    tbl.add(cs);
  }
  out += tbl.str();
  out += "\n### 因子 (前 " + std::to_string(list.size()) + " 个; 统计是原文在其股票池与预测期上的数, 只看机制)\n";
  for (const Row &f : list)
    out += "- **" + f.name + "** · " + f.family + " · " + f.direction + " · " + f.period + " → " + f.horizon + " · " + f.stats + "\n  出处 `" + f.doc + "`\n  公式: " + f.formula + "\n";
  return out;
}

std::string FactorsRes::title() const {
  return "命中 " + std::to_string(hit) + " 个因子 (出自 " + std::to_string(docs) + " 篇)" + (loose ? " (放宽)" : "") + " " + cond;
}

ToolOut SearchFactors(const Call &c, Bay &) {
  const Lib &L = lib();
  const std::string family = c.str("family"), period = c.str("period"), horizon = c.str("horizon"), ap = c.str("approach"), asset = c.str("asset"), sort = c.str("sort");
  const std::vector<std::string> ws = text::words(c.str("query"));
  if (!ap.empty() && (!L.stage_of.count(ap) || L.stage_of.at(ap) != "L1_factor"))
    return Fail("没有因子构造流派 " + ap + " (因子只出自 L1_factor 的流派, code 见 search_docs 分面里 L1_factor 那行)");
  const auto doc = [&](size_t i) -> const docs::Doc & { return L.docs[L.factors[i].doc]; };
  text::Filter f;
  if (!family.empty())
    f.add("family=" + family, [&](size_t i) { return L.factors[i].family == family; });
  if (!period.empty())
    f.add("period=" + period, [&](size_t i) { return has(L.factors[i].period, period); });
  if (!horizon.empty())
    f.add("horizon=" + horizon, [&](size_t i) { return L.factors[i].horizon == horizon; });
  if (!ap.empty())
    f.add("approach=" + ap, [&](size_t i) { return has(doc(i).l1, ap); });
  if (!asset.empty())
    f.add("asset=" + asset, [&](size_t i) { return has(doc(i).asset, asset); });
  f.add(ws, "query 词 ", [&](size_t i) -> const std::string & { return L.factors[i].hay; });
  bool loose = false;
  std::vector<size_t> hit = f.hits(L.factors.size(), loose);

  auto res = std::make_shared<FactorsRes>();
  FactorsRes &R = *res;
  Count docs_n, family_n, ap_n;
  for (const size_t i : hit) {
    const docs::Factor &fa = L.factors[i];
    ++docs_n[L.docs[fa.doc].id], ++family_n[fa.family];
    for (const std::string &a : L.docs[fa.doc].l1)
      ++ap_n[named(L, a)];
  }
  R.cond = conditions(c), R.loose = loose, R.hit = hit.size(), R.docs = docs_n.size();
  if (loose || hit.empty())
    R.alone = f.alone(L.factors.size());
  if (hit.empty())
    return Done(std::move(res));
  R.families = ranked(family_n), R.approaches = ranked(ap_n);

  int m[kPeriods.size()][kPeriods.size()] = {};
  std::array<bool, kPeriods.size()> row{}, col{};
  for (const size_t i : hit) {
    const docs::Factor &fa = L.factors[i];
    const size_t h = period_ix(fa.horizon);
    for (const std::string &p : fa.period) {
      const size_t r = period_ix(p);
      ++m[r][h], row[r] = col[h] = true;
    }
  }
  for (size_t h = 0; h < kPeriods.size(); ++h)
    if (col[h])
      R.cols.emplace_back(kPeriods[h]);
  for (size_t r = 0; r < kPeriods.size(); ++r) {
    if (!row[r])
      continue;
    R.rows.emplace_back(kPeriods[r]);
    for (size_t h = 0; h < kPeriods.size(); ++h)
      if (col[h])
        R.cells.push_back(m[r][h]);
  }

  const auto key = [&](const docs::Factor &fa) {
    const auto &s = fa.stats;
    return sort == "ic" ? std::fabs(std::isfinite(s[1]) ? s[1] : s[0]) : sort == "icir" ? std::fabs(s[2])
                                                                     : sort == "ls"     ? s[3]
                                                                                        : NAN;
  };
  std::stable_sort(hit.begin(), hit.end(), [&](size_t a, size_t b) {
    const double ka = key(L.factors[a]), kb = key(L.factors[b]);
    if (std::isfinite(ka) != std::isfinite(kb))
      return std::isfinite(ka);
    if (std::isfinite(ka) && ka != kb)
      return ka > kb;
    return L.docs[L.factors[a].doc].id > L.docs[L.factors[b].doc].id;
  });
  const size_t n = std::min(hit.size(), static_cast<size_t>(c.num("n")));
  for (size_t r = 0; r < n; ++r) {
    const docs::Factor &fa = L.factors[hit[r]];
    R.list.push_back({fa.name, fa.family, fa.direction, text::join(fa.period, "+"), fa.horizon, stats_of(fa.stats), L.docs[fa.doc].id,
                      text::head(text::flat(fa.formula), kBrief * 2)});
  }
  return Done(std::move(res));
}

json SampleSearchFactors(Feed &p) {
  static const char *kFamilies[] = {"", "momentum", "reversal", "liquidity", "microstructure", "volatility", "value"};
  static const char *kPeriods_[] = {"", "second", "intraday", "daily"};
  return {{"family", kFamilies[p.below(7)]}, {"period", kPeriods_[p.below(4)]}, {"sort", p.pick(Tool::SearchFactors, "sort")}, {"n", 6 + p.below(10)}};
}

} // namespace GUI::Agent
