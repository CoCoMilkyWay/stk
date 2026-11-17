// search_leaves — 查特征叶: 输入 query / n → LeavesRes (命中的组行) → 原文; 展示见 ui/query/SearchLeaves.cpp
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Query.hpp"

#include <string_view>
#include <unordered_map>

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string LeavesRes::text() const {
  if (!hit)
    return "(无命中; 各词单独命中: " + alone + "; 换个写法或少给几个词)\n";
  text::Table tbl({"叶", "中文名", "公式 (原量 x)", "说明", "落盘值"});
  for (const Row &r : rows)
    tbl.add({r.leaf, r.name_cn, r.formula, r.desc, r.value});
  return (loose ? "没有同时含全部词的叶, 下列含其中任一词 (各词单独命中: " + alone + "). " : std::string()) + "命中 " + std::to_string(hit) + " 个叶, 在 " +
         std::to_string(groups) + " 组里" + (groups > static_cast<size_t>(n) ? ", 列前 " + std::to_string(n) + " 组" : std::string()) +
         ". 同组的叶只差计量 (amt 额 / vol 量 / n 笔) 或阈值 / 窗口 (带数字的词), 列出整组; 公式与说明按组内首个命中的写, 其余照族卡换词\n\n" + tbl.str();
}

ToolOut SearchLeaves(const Call &c, Seat &s) {
  const std::vector<Leaf> &leaves = s.lab.ctx().leaves;
  const std::string query = c.str("query");
  const int n = c.num("n");
  text::Filter f;
  f.add(text::words(query), "", [&](size_t i) -> const std::string & { return leaves[i].hay; });
  bool loose = false;
  std::vector<const Leaf *> hits, all;
  for (const size_t i : f.hits(leaves.size(), loose))
    hits.push_back(&leaves[i]);
  for (const Leaf &l : leaves)
    all.push_back(&l);
  auto res = std::make_shared<LeavesRes>();
  LeavesRes &R = *res;
  R.query = query, R.n = n, R.hit = hits.size(), R.loose = loose;
  if (loose || hits.empty())
    R.alone = f.alone(leaves.size());
  if (hits.empty())
    return Done(std::move(res));
  const std::vector<std::vector<const Leaf *>> gs = KinGroups(hits);
  std::unordered_map<std::string_view, std::vector<const Leaf *>> kins;
  for (const std::vector<const Leaf *> &g : KinGroups(all))
    kins.emplace(g.front()->kin, g);
  R.groups = gs.size();
  for (size_t i = 0; i < gs.size() && i < static_cast<size_t>(n); ++i) {
    const Leaf &l = *gs[i].front();
    const std::vector<const Leaf *> &kin = kins.at(l.kin);
    const std::string name = kin.size() == 1 ? l.code : LeafWords(l.code).first + "." + KinName(kin) + " (" + std::to_string(kin.size()) + " 个; 下列按 " + LeafCol(l.code) + ")";
    R.rows.push_back({name, l.name_cn, l.formula, l.desc, l.tf.empty() ? "x" : l.tf_def});
  }
  return Done(std::move(res));
}

json SampleSearchLeaves(Feed &p) {
  std::string q = "close";
  if (!p.ctx.leaves.empty()) {
    const std::vector<std::string> ws = LeafWords(p.leaf()).second;
    q = p.one(ws);
  }
  return {{"query", q}, {"n", 10 + p.below(21)}};
}

} // namespace GUI::Agent
