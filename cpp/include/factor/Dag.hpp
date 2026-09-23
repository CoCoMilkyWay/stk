#pragma once

// =============================================================================
// 因子 DAG: 表达式树 → 去重的算子节点序列 + 缓冲槽计划 (CPU / GPU 两个 evaluator 共用同一份计划)
// =============================================================================
//   build:  Expr 各子树按规范串去重 (TsAdd(x, x) / 多处引用同一子式只算一次), 后序拓扑排 (根 = 末节点);
//           特征叶也是节点 (op = -1, feat = 输入平面下标), 算子输入统一指向节点.
//   plan:   活性分析 → 槽复用: 节点 i 的输出占一个槽, 最后一个消费者 i' 执行完即释放 (free_after[i']);
//           n_slots = 峰值同时存活的中间量数, 通常 2..3, 与树大小无关 → 内存 / 显存按槽数预分配, eval 期零分配.
//   evaluator 只按计划走: 取输入槽 → 跑算子写输出槽 → 释放 free_after; 参数搜索时改 nodes[].p 重跑即可
//   (以后做增量: 只重算改动节点的下游, 计划不变).
//   build_forest: 多个 Expr 合成一张 DAG (跨因子公共子式只算一次 = tensor share); roots[k] = 第 k 棵的根 (同规范串 → 同节点).
//           根节点算完要先被调用方消费 (Stat), 再释放: plan 把根视作"自己消费自己", 无后续消费者时 free_after[根] 含自己的槽.
//   本头只管结构与槽计划, 不含任何后端, 也不含并行策略 (Run 与 search 的并行方案不同, 各在自己的 evaluator 里).
// =============================================================================

#include "factor/Expr.hpp"

#include <algorithm>
#include <cassert>
#include <map>
#include <string>
#include <vector>

namespace factor {

struct DagNode {
  int op = -1;              // expr::kOps 下标; -1 = 特征叶
  int feat = -1;            // 叶: Dag::feats 下标 (输入平面)
  int in[3] = {-1, -1, -1}; // 输入节点下标 (前 arity 个有效)
  Param p;                  // 本节点参数
  int slot = -1;            // 算子节点的输出槽; 叶 = -1 (直接用输入平面)
};

struct Dag {
  std::vector<DagNode> nodes;               // 后序拓扑, 根 = 末节点
  std::vector<std::string> feats;           // 去重特征 code (首见序), 下标 = 输入平面下标
  std::vector<std::vector<int>> free_after; // free_after[i] = 节点 i 执行完 (含消费根) 可释放的槽
  std::vector<int> roots;                   // build_forest: roots[k] = 第 k 个 Expr 的根节点 (可重复); build: 空
  int n_slots = 0;
  int n_ops = 0;

  int root() const {
    assert(!nodes.empty());
    return static_cast<int>(nodes.size()) - 1;
  }
  bool is_root(int i) const {
    for (int r : roots)
      if (r == i)
        return true;
    return false;
  }
  // 节点 i 的输出在哪个槽 (叶 → -1, 由 evaluator 转成输入平面)
  int slot_of(int i) const { return nodes[static_cast<size_t>(i)].slot; }
};

namespace detail {

struct DagBuilder {
  const expr::Expr &e;
  Dag &d;
  std::map<std::string, int> &memo;     // 子树规范串 → 节点下标 (build_forest 跨 Expr 共用)
  std::map<std::string, int> &feat_idx; // 特征 code → feats 下标

  int visit(int ei) {
    const expr::Node &n = e.nodes[static_cast<size_t>(ei)];
    const std::string key = expr::to_string(e, ei);
    if (const auto it = memo.find(key); it != memo.end())
      return it->second;
    DagNode dn;
    if (n.op < 0) {
      auto fit = feat_idx.find(n.feat);
      if (fit == feat_idx.end()) {
        fit = feat_idx.emplace(n.feat, static_cast<int>(d.feats.size())).first;
        d.feats.push_back(n.feat);
      }
      dn.feat = fit->second;
    } else {
      dn.op = n.op;
      dn.p = n.p;
      const int ar = expr::kOps[n.op].arity;
      for (int a = 0; a < ar; ++a)
        dn.in[a] = visit(n.args[a]); // 先子后父 = 后序
      d.n_ops++;
    }
    const int id = static_cast<int>(d.nodes.size());
    d.nodes.push_back(dn);
    memo.emplace(key, id);
    return id;
  }
};

} // namespace detail

// 活性分析 + 槽分配 (build 末尾调; 改参数不改结构则不必重做)
inline void plan(Dag &d) {
  const int N = static_cast<int>(d.nodes.size());
  std::vector<int> last_use(static_cast<size_t>(N), -1);
  for (int i = 0; i < N; ++i) {
    const DagNode &n = d.nodes[static_cast<size_t>(i)];
    if (n.op < 0)
      continue;
    for (int a = 0; a < expr::kOps[n.op].arity; ++a)
      last_use[static_cast<size_t>(n.in[a])] = i;
  }
  for (int r : d.roots) // 根自己消费自己 (调用方在 free_after[r] 之前取走结果)
    last_use[static_cast<size_t>(r)] = std::max(last_use[static_cast<size_t>(r)], r);
  d.free_after.assign(static_cast<size_t>(N), {});
  std::vector<int> free_slots;
  d.n_slots = 0;
  for (int i = 0; i < N; ++i) {
    DagNode &n = d.nodes[static_cast<size_t>(i)];
    if (n.op < 0) {
      n.slot = -1;
      continue;
    }
    if (free_slots.empty()) {
      n.slot = d.n_slots++;
    } else {
      n.slot = free_slots.back();
      free_slots.pop_back();
    }
    // 输入里以 i 为最后消费者的算子节点: 释放其槽 (同一节点被 i 引用多次只释放一次)
    for (int a = 0; a < expr::kOps[n.op].arity; ++a) {
      const int j = n.in[a];
      const DagNode &src = d.nodes[static_cast<size_t>(j)];
      if (src.op < 0 || last_use[static_cast<size_t>(j)] != i)
        continue;
      bool dup = false;
      for (int b = 0; b < a; ++b)
        dup = dup || n.in[b] == j;
      if (dup)
        continue;
      free_slots.push_back(src.slot);
      d.free_after[static_cast<size_t>(i)].push_back(src.slot);
    }
    // 森林的根且无后续消费者: 调用方消费完即释放
    if (last_use[static_cast<size_t>(i)] == i && d.is_root(i)) {
      free_slots.push_back(n.slot);
      d.free_after[static_cast<size_t>(i)].push_back(n.slot);
    }
  }
  // 单树 (roots 空) 的根不在任何 free_after 里, 留给调用方取结果
}

inline Dag build(const expr::Expr &e) {
  assert(!e.nodes.empty());
  Dag d;
  std::map<std::string, int> memo, feat_idx;
  detail::DagBuilder b{e, d, memo, feat_idx};
  b.visit(0);
  plan(d);
  return d;
}

// 多 Expr 合一: 节点按各 Expr 的后序依次追加 (仍是拓扑序), 公共子式命中 memo 不重建
inline Dag build_forest(const std::vector<const expr::Expr *> &es) {
  assert(!es.empty());
  Dag d;
  std::map<std::string, int> memo, feat_idx;
  for (const expr::Expr *e : es) {
    assert(e && !e->nodes.empty());
    detail::DagBuilder b{*e, d, memo, feat_idx};
    d.roots.push_back(b.visit(0));
  }
  plan(d);
  return d;
}

} // namespace factor
