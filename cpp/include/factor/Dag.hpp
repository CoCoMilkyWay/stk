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
//           keep_roots = true: 根的槽永不回收 (每根独占一槽, 全部根同时存活到走完), 调用方一批一起消费 (Search GPU 批评估要 C 张根平面同在).
//   fuse:   逐点链融合 (build / build_forest 默认开): 非根、只有一个消费者且消费者也是逐点 (POINT × SELF) 的逐点节点
//           并进消费者, 一组 = 一个融合节点 (Contract Fuse 程序, 外部输入走 in[0..n_in)), 中间量不落平面.
//           被并的节点不再出现在 nodes / keys 里 —— 要按 Expr 子树找中间节点的调用方传 fuse = false.
//   本头只管结构与槽计划, 不含任何后端, 也不含并行策略 (Run 与 search 的并行方案不同, 各在自己的 evaluator 里).
// =============================================================================

#include "factor/Expr.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace factor {

struct DagNode {
  int op = -1;          // expr::kOps 下标; -1 = 特征叶; 融合节点 = 组末步的算子 (显示 / 归类用, 执行看 fu)
  int feat = -1;        // 叶: Dag::feats 下标 (输入平面)
  int n_in = 0;         // 输入数: 算子 = 元数, 融合 = 外部输入数
  int in[kFuseIn] = {}; // 输入节点下标 (前 n_in 个有效)
  Param p;              // 本节点参数 (融合节点不用, 各步参数在 fu 里)
  Fuse fu;              // 逐点链融合程序; fu.n = 0 = 普通算子节点
  int slot = -1;        // 算子节点的输出槽; 叶 = -1 (直接用输入平面)
};
static_assert(kFuseIn >= 3, "融合节点的输入槽要盖住普通算子的元数");

struct Dag {
  std::vector<DagNode> nodes;               // 后序拓扑, 根 = 末节点
  std::vector<std::string> keys;            // 节点子树规范串 (下标同 nodes): 外部按 Expr 子树 (expr::to_string(e, ei)) 找节点
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
  // 节点 r 的子树 (含 r, 升序; 拓扑序 ⇒ 逆扫一遍)
  std::vector<int> subtree(int r) const {
    assert(r >= 0 && r < static_cast<int>(nodes.size()));
    std::vector<uint8_t> in(nodes.size(), 0);
    in[static_cast<size_t>(r)] = 1;
    for (int i = r; i >= 0; --i)
      if (in[static_cast<size_t>(i)])
        for (int a = 0; a < nodes[static_cast<size_t>(i)].n_in; ++a)
          in[static_cast<size_t>(nodes[static_cast<size_t>(i)].in[a])] = 1;
    std::vector<int> out;
    for (int i = 0; i <= r; ++i)
      if (in[static_cast<size_t>(i)])
        out.push_back(i);
    return out;
  }
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
      dn.n_in = expr::kOps[n.op].arity;
      for (int a = 0; a < dn.n_in; ++a)
        dn.in[a] = visit(n.args[a]); // 先子后父 = 后序
      d.n_ops++;
    }
    const int id = static_cast<int>(d.nodes.size());
    d.nodes.push_back(dn);
    d.keys.push_back(key);
    memo.emplace(key, id);
    return id;
  }
};

inline bool pointwise(int op) {
  const expr::OpInfo &o = expr::kOps[op];
  return o.t == T::POINT && o.a == A::SELF;
}

// 逐点链融合 (文件头 fuse; plan 之前调). 组 = 一个未被并的逐点节点 r + 递归并进来的上游;
// 步数 > kFuseLen 或外部输入 > kFuseIn 的组整组不融
inline void fuse_points(Dag &d) {
  const int N = static_cast<int>(d.nodes.size());
  std::vector<int> uses(static_cast<size_t>(N), 0), user(static_cast<size_t>(N), -1);
  for (int i = 0; i < N; ++i) {
    const DagNode &n = d.nodes[static_cast<size_t>(i)];
    for (int a = 0; a < n.n_in; ++a) {
      const int j = n.in[a];
      bool dup = false;
      for (int b = 0; b < a; ++b)
        dup = dup || n.in[b] == j;
      if (!dup)
        ++uses[static_cast<size_t>(j)], user[static_cast<size_t>(j)] = i;
    }
  }
  std::vector<uint8_t> root(static_cast<size_t>(N), 0), inl(static_cast<size_t>(N), 0);
  if (d.roots.empty())
    root[static_cast<size_t>(N - 1)] = 1;
  for (int r : d.roots)
    root[static_cast<size_t>(r)] = 1;
  for (int j = 0; j < N; ++j) {
    const int op = d.nodes[static_cast<size_t>(j)].op;
    inl[static_cast<size_t>(j)] = op >= 0 && pointwise(op) && !root[static_cast<size_t>(j)] && uses[static_cast<size_t>(j)] == 1 &&
                                  pointwise(d.nodes[static_cast<size_t>(user[static_cast<size_t>(j)])].op);
  }
  std::vector<DagNode> out;
  std::vector<std::string> keys;
  std::vector<int> remap(static_cast<size_t>(N), -1);
  int n_ops = 0;
  for (int r = 0; r < N; ++r) {
    if (inl[static_cast<size_t>(r)])
      continue;
    DagNode n = d.nodes[static_cast<size_t>(r)];
    bool group = false;
    for (int a = 0; n.op >= 0 && a < n.n_in; ++a)
      group = group || inl[static_cast<size_t>(n.in[a])];
    if (group) {
      DagNode f;
      f.op = n.op;
      std::map<int, int> code; // 节点 → 实参编码 (≥ 0 外部输入, < 0 ~步号)
      bool ok = true;
      const auto emit = [&](auto &self, int j) -> int {
        if (const auto it = code.find(j); it != code.end())
          return it->second;
        int c = 0;
        if (j != r && !inl[static_cast<size_t>(j)]) {
          ok = ok && f.n_in < kFuseIn;
          if (!ok)
            return 0;
          f.in[f.n_in] = j;
          c = f.n_in++;
        } else {
          const DagNode &m = d.nodes[static_cast<size_t>(j)];
          FuseStep s;
          s.op = m.op;
          s.p = m.p;
          for (int a = 0; a < m.n_in; ++a)
            s.arg[a] = self(self, m.in[a]);
          ok = ok && f.fu.n < kFuseLen;
          if (!ok)
            return 0;
          f.fu.s[f.fu.n] = s;
          c = ~f.fu.n++;
        }
        code.emplace(j, c);
        return c;
      };
      emit(emit, r);
      if (ok) {
        n = f;
      } else { // 超限: 沿并入边撤掉整组成员 (全部落平面); 成员下标 < r 已被跳过, 故整趟重排 (每次重排并入数严格减少)
        std::vector<int> stk{r};
        while (!stk.empty()) {
          const DagNode &m = d.nodes[static_cast<size_t>(stk.back())];
          stk.pop_back();
          for (int a = 0; m.op >= 0 && a < m.n_in; ++a)
            if (const int k = m.in[a]; inl[static_cast<size_t>(k)])
              inl[static_cast<size_t>(k)] = 0, stk.push_back(k);
        }
        out.clear(), keys.clear(), n_ops = 0;
        std::fill(remap.begin(), remap.end(), -1);
        r = -1;
        continue;
      }
    }
    for (int a = 0; a < n.n_in; ++a) {
      n.in[a] = remap[static_cast<size_t>(n.in[a])];
      assert(n.in[a] >= 0 && "输入必先于消费者落位");
    }
    remap[static_cast<size_t>(r)] = static_cast<int>(out.size());
    out.push_back(n);
    keys.push_back(d.keys[static_cast<size_t>(r)]);
    n_ops += n.op >= 0;
  }
  for (int &r : d.roots)
    r = remap[static_cast<size_t>(r)];
  d.nodes = std::move(out);
  d.keys = std::move(keys);
  d.n_ops = n_ops;
}

} // namespace detail

// 活性分析 + 槽分配 (build 末尾调; 改参数不改结构则不必重做). keep_roots 见文件头 build_forest
inline void plan(Dag &d, bool keep_roots = false) {
  const int N = static_cast<int>(d.nodes.size());
  std::vector<int> last_use(static_cast<size_t>(N), -1);
  for (int i = 0; i < N; ++i) {
    const DagNode &n = d.nodes[static_cast<size_t>(i)];
    if (n.op < 0)
      continue;
    for (int a = 0; a < n.n_in; ++a)
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
    for (int a = 0; a < n.n_in; ++a) {
      const int j = n.in[a];
      const DagNode &src = d.nodes[static_cast<size_t>(j)];
      if (src.op < 0 || last_use[static_cast<size_t>(j)] != i || (keep_roots && d.is_root(j)))
        continue;
      bool dup = false;
      for (int b = 0; b < a; ++b)
        dup = dup || n.in[b] == j;
      if (dup)
        continue;
      free_slots.push_back(src.slot);
      d.free_after[static_cast<size_t>(i)].push_back(src.slot);
    }
    // 森林的根且无后续消费者: 调用方消费完即释放 (keep_roots: 不回收, 根平面活到走完)
    if (!keep_roots && last_use[static_cast<size_t>(i)] == i && d.is_root(i)) {
      free_slots.push_back(n.slot);
      d.free_after[static_cast<size_t>(i)].push_back(n.slot);
    }
  }
  // 单树 (roots 空) 的根不在任何 free_after 里, 留给调用方取结果
}

inline Dag build(const expr::Expr &e, bool fuse = true) {
  assert(!e.nodes.empty() && !e.is_template() && "模板 (含自由参数) 不能评估");
  Dag d;
  std::map<std::string, int> memo, feat_idx;
  detail::DagBuilder b{e, d, memo, feat_idx};
  b.visit(0);
  if (fuse)
    detail::fuse_points(d);
  plan(d);
  return d;
}

// 多 Expr 合一: 节点按各 Expr 的后序依次追加 (仍是拓扑序), 公共子式命中 memo 不重建
inline Dag build_forest(const std::vector<const expr::Expr *> &es, bool keep_roots = false, bool fuse = true) {
  assert(!es.empty());
  Dag d;
  std::map<std::string, int> memo, feat_idx;
  for (const expr::Expr *e : es) {
    assert(e && !e->nodes.empty() && !e->is_template());
    detail::DagBuilder b{*e, d, memo, feat_idx};
    d.roots.push_back(b.visit(0));
  }
  if (fuse)
    detail::fuse_points(d);
  plan(d, keep_roots);
  return d;
}

} // namespace factor
