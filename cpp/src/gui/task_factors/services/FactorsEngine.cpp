// FactorsEngine — 见头文件 (plan_forest). include 因子 CPU 后端头, 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsEngine.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>

namespace GUI::Factors {

namespace {

// 叶 p 在严格域 dom 上首个越界格的值 (空 = 有效格全落在域内; 各条带首个越界格取全局最小, 与顺序扫同一格)
std::string out_of_dom(misc::exec::ForkJoin &ex, const factor::check::Plane &p, factor::Dom dom) {
  const size_t n = p.v.size();
  std::vector<size_t> first(static_cast<size_t>(ex.threads()), n);
  ex.rows(static_cast<int>(n), [&](int i0, int i1, int tid) {
    for (size_t i = static_cast<size_t>(i0); i < static_cast<size_t>(i1); ++i)
      if (p.m[i] && !factor::expr::dom_holds(dom, p.v[i])) {
        first[static_cast<size_t>(tid)] = i;
        return;
      }
  });
  const size_t i = *std::min_element(first.begin(), first.end());
  if (i == n)
    return {};
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%g", static_cast<double>(p.v[i]));
  return buf;
}

} // namespace

// 算子节点的特征叶元: parse 时放行 (值域只能查数据), 这里按 OpTable in 列的严格域 (dom_strict, 即组 id 的 INT) 逐元查数据
// (有效格全部落在值域内): CS 算子内部 max_gid 断言炸不得由数据触发. 非严格域越界格由算子自身置无效, 不在此判.
// 融合节点只装逐点 TS 算子, 无严格域元
int plan_forest(misc::exec::ForkJoin &ex, const ScopeCache &c, const factor::Dag &d, std::vector<std::string> &why, std::vector<uint8_t> &need) {
  assert(!d.roots.empty() && "森林 (build_forest) 才有根表");
  const std::vector<const factor::check::Plane *> in = leaves(c, d.feats);
  const size_t N = d.nodes.size();
  std::vector<std::string> err(N);
  for (size_t i = 0; i < N; ++i) {
    const factor::DagNode &nd = d.nodes[i];
    if (nd.op < 0 || nd.fu.n > 0)
      continue;
    const factor::expr::OpInfo &o = factor::expr::kOps[nd.op];
    assert(nd.n_in == o.arity);
    for (int s = 0; s < o.arity && err[i].empty(); ++s) {
      const factor::DagNode &g = d.nodes[static_cast<size_t>(nd.in[s])];
      if (g.op >= 0 || !factor::dom_strict(o.in[s]))
        continue;
      const std::string &code = d.feats[static_cast<size_t>(g.feat)];
      const auto [it, fresh] = c.dom.try_emplace({code, o.in[s]});
      if (fresh)
        it->second = out_of_dom(ex, *in[static_cast<size_t>(g.feat)], o.in[s]);
      if (!it->second.empty())
        err[i] = std::string(o.name) + " 第 " + std::to_string(s + 1) + " 元要求 " + factor::dom_name(o.in[s]) + ", 特征 " + code + " 含越界值 " + it->second;
    }
  }
  why.assign(d.roots.size(), {});
  need.assign(N, 0);
  int good = 0;
  for (size_t k = 0; k < d.roots.size(); ++k) {
    const std::vector<int> sub = d.subtree(d.roots[k]);
    for (const int i : sub)
      if (!err[static_cast<size_t>(i)].empty()) {
        why[k] = err[static_cast<size_t>(i)];
        break;
      }
    if (!why[k].empty())
      continue;
    ++good;
    for (const int i : sub)
      need[static_cast<size_t>(i)] = 1;
  }
  return good;
}

} // namespace GUI::Factors
