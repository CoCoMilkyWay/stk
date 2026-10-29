#pragma once

// =============================================================================
// 因子 DAG 的 CPU evaluator: 按 Dag 的槽计划跑 factor::cpu::{ts,cs} 算子 (整张量批算)
// =============================================================================
//   HostPool      按 n_slots 预分配的宿主平面 (跨因子复用, 只 ensure 不清: 契约是后端写满每格)
//   kernel        算子下标 → {scratch, run} (OP_ALL 展开; Contract【调用形状】)
//   run_node      一个算子节点, 并行由 Exec 定 (misc/exec.hpp): Serial = 单线程 (搜索: 因子间并行由上层起线程);
//                 ForkJoin = 节点内切满核 (Run / Inspect: 共享 DAG 顺序走节点). 与 Dag 计划解耦.
//                 切法只看 A 域 (不认具体算子), 结果与单线程**逐位一致** (不是近似):
//                   TS (SELF)        资产列各自独立 (段界 / 窗口历史都在列内) → 按 kTileA 列切任务
//                   CS (ALL / GROUP) 行独立 → 按 t 切
//                   融合 (Dag fuse)  kFuseRows 行一个任务, 块内按 kTileA 列逐步跑逐点算子 (中间量只在块缓冲里, 驻 L1/L2),
//                                    外部输入直接读平面, 末步直接写输出
//                 每线程暂存 = check::Arena (算子 scratch 声明大小); sc.size() ≥ ex.threads(), 跨节点 / 跨因子复用
//   walk          顺序走一张 Dag (每节点 run_node), 逐节点回调 visit (计时 / 进度 / 取消 / 到根即评); 输入平面按 Dag::feats 下标给
//   eval          walk 到底, 返回根平面 (根是特征叶时直接返回该输入)
//   cs_gate       截面池掩码 [T][A] (特征库 cs_valid 列 = 当日在池, 整日常量, 见 features/MetaFlag.hpp), 原样递给 CS 节点
//                 (A 域 ALL / GROUP) 的算子 —— 池内统计 / 池外就近取值全在算子内部 (契约【截面池 g】); TS 节点不收.
//                 evaluator 不做任何二次门控 / 补值; 有 CS 节点时 cs_gate 必填
//   依赖受控浮点: 消费者 TU 编进 -fno-fast-math (CMake PRECISE_MATH_FLAG).
// =============================================================================

#include "factor/Check.hpp" // check::Plane / check::Arena (宿主 SoA 平面 / 每线程暂存)
#include "factor/Dag.hpp"

#include "factor/CS/Cpu.hpp" // IWYU pragma: keep
#include "factor/TS/Cpu.hpp" // IWYU pragma: keep

#include <algorithm>
#include <cassert>
#include <chrono>
#include <vector>

namespace factor::cpu {

struct Kernel {
  size_t (*scratch)(const Call &);
  void (*run)(const Call &, Range, void *);
};

// 算子分派表 (下标 = expr::kOps 下标): A 域 token 粘贴选命名空间 (SELF → ts, ALL / GROUP → cs)
inline const Kernel &kernel(int op) {
#define FACTOR_CPU_SELF(Name) Kernel{&factor::cpu::ts::Name::scratch, &factor::cpu::ts::Name::run}
#define FACTOR_CPU_ALL(Name) Kernel{&factor::cpu::cs::Name::scratch, &factor::cpu::cs::Name::run}
#define FACTOR_CPU_GROUP FACTOR_CPU_ALL
#define FACTOR_CPU_ROW(Name, c_name, ar, t, a, kern, kdom, in, out, opx, note) FACTOR_CPU_##a(Name),
  static constexpr Kernel tbl[] = {OP_ALL(FACTOR_CPU_ROW)};
#undef FACTOR_CPU_ROW
#undef FACTOR_CPU_GROUP
#undef FACTOR_CPU_ALL
#undef FACTOR_CPU_SELF
  static_assert(sizeof(tbl) / sizeof(tbl[0]) == static_cast<size_t>(expr::kOpCount));
  assert(op >= 0 && op < expr::kOpCount);
  return tbl[op];
}

inline constexpr int kFuseRows = 8; // 融合任务的行数 (块 = kFuseRows × kTileA 格, 每步 5 KB)

struct HostPool {
  std::vector<check::Plane> slots;
  // 幂等: 槽数 / 形状对得上零分配
  void prepare(int n_slots, size_t n) {
    if (slots.size() < static_cast<size_t>(n_slots))
      slots.resize(static_cast<size_t>(n_slots));
    for (check::Plane &p : slots)
      p.ensure(n);
  }
  size_t bytes() const {
    size_t b = 0;
    for (const check::Plane &p : slots)
      b += p.v.size() * sizeof(float) + p.m.size();
    return b;
  }
};

// 逐点链融合节点 (run_node 分出): 一个任务 = kFuseRows 行; 中间步结果写块缓冲 ([kFuseRows][A], 行距 A 与平面同),
// 外部输入 / 末步输出直接用平面 (偏到任务首行). 每步是普通逐点算子的 run (c.t0 = 任务首行的全局行号), 结果与不融合逐位同
template <class Exec>
inline void run_fused(Exec &ex, const DagNode &nd, const check::Plane *const in[], check::Plane &out, int T, int A, Geo geo,
                      std::vector<check::Arena> &sc) {
  const Fuse &f = nd.fu;
  assert(f.n >= 2 && f.n <= kFuseLen);
  const size_t cell = static_cast<size_t>(kFuseRows) * A;
  Bump need;
  for (int s = 0; s + 1 < f.n; ++s)
    need.take<float>(cell), need.take<uint8_t>(cell);
  ex.tasks((T + kFuseRows - 1) / kFuseRows, [&](int b, int tid) {
    const int t0 = b * kFuseRows;
    const size_t off = static_cast<size_t>(t0) * A;
    Bump bp{static_cast<char *>(sc[static_cast<size_t>(tid)].get(need.used))};
    Out buf[kFuseLen];
    for (int s = 0; s + 1 < f.n; ++s)
      buf[s] = {bp.take<float>(cell), bp.take<uint8_t>(cell)};
    buf[f.n - 1] = {out.v.data() + off, out.m.data() + off};
    Call cs[kFuseLen];
    for (int s = 0; s < f.n; ++s) {
      const FuseStep &st = f.s[s];
      Call &c = cs[s];
      In *const slot[3] = {&c.x, &c.y, &c.z};
      for (int a = 0; a < expr::kOps[st.op].arity; ++a) {
        const int q = st.arg[a];
        assert(q < nd.n_in && ~q < s && "实参只指外部输入或更早的步");
        *slot[a] = q >= 0 ? In{in[q]->v.data() + off, in[q]->m.data() + off} : In{buf[~q].v, buf[~q].m};
      }
      c.o = buf[s];
      c.T = std::min(kFuseRows, T - t0), c.A = A, c.t0 = t0, c.geo = geo, c.p = st.p;
      assert(factor::detail::pointwise(st.op) && kernel(st.op).scratch(c) == 0);
    }
    for (int a0 = 0; a0 < A; a0 += kTileA) {
      const Range rg{a0, std::min(A, a0 + kTileA)};
      for (int s = 0; s < f.n; ++s)
        kernel(f.s[s].op).run(cs[s], rg, nullptr);
    }
  });
}

// in[a] (a < nd.n_in) 输入平面, out 输出平面 (已 ensure); sc.size() ≥ ex.threads()
// cs_gate: 截面池掩码 [T][A] (文件头), 原样递给 CS 节点的算子; CS 节点必填, TS 节点不看. seg = 本层每段行数 (Contract【层几何】)
template <class Exec>
inline void run_node(Exec &ex, const DagNode &nd, const check::Plane *const in[kFuseIn], check::Plane &out, int T, int A, int seg,
                     std::vector<check::Arena> &sc, const uint8_t *cs_gate) {
  assert(nd.op >= 0 && sc.size() >= static_cast<size_t>(ex.threads()));
  const size_t n = static_cast<size_t>(T) * A;
  for (int a = 0; a < nd.n_in; ++a)
    assert(in[a] && in[a]->v.size() == n && in[a]->m.size() == n);
  assert(out.v.size() == n && out.m.size() == n);
  if (nd.fu.n > 0)
    return run_fused(ex, nd, in, out, T, A, geo_of(seg), sc);
  const expr::OpInfo &o = expr::kOps[nd.op];
  assert((o.a == A::SELF || cs_gate) && "CS 节点必须给截面池掩码 cs_gate");
  Call c;
  In *const slot[3] = {&c.x, &c.y, &c.z};
  for (int a = 0; a < o.arity; ++a)
    *slot[a] = {in[a]->v.data(), in[a]->m.data()};
  c.g = cs_gate;
  c.o = {out.v.data(), out.m.data()};
  c.T = T, c.A = A, c.geo = geo_of(seg), c.p = nd.p;
  const Kernel &k = kernel(nd.op);
  const size_t ws = k.scratch(c);
  if (o.a == A::SELF)
    ex.tasks((A + kTileA - 1) / kTileA, [&](int b, int tid) {
      k.run(c, Range{b * kTileA, std::min(A, (b + 1) * kTileA)}, sc[static_cast<size_t>(tid)].get(ws));
    });
  else
    ex.rows(T, [&](int t0, int t1, int tid) { k.run(c, Range{t0, t1}, sc[static_cast<size_t>(tid)].get(ws)); });
}

// 节点 i 的平面: 叶 = inputs[feat], 算子 = 池槽 (槽按 Dag 计划复用: 只在 i 写出后、被下一个占用者覆盖前有效)
inline const check::Plane *plane_at(const Dag &d, const std::vector<const check::Plane *> &inputs, const HostPool &hp, int i) {
  const DagNode &nd = d.nodes[static_cast<size_t>(i)];
  return nd.op < 0 ? inputs[static_cast<size_t>(nd.feat)] : &hp.slots[static_cast<size_t>(nd.slot)];
}

// 顺序走 Dag (拓扑序; need 非空 → 只走 need[i] 的节点), 算子节点 run_node. 每走过一个节点 i 调 visit(i, at, ms) → bool:
// at(j) = plane_at(j) (visit 内 i 及其输入仍在槽里), ms = 算子 wall (叶 0); visit 返回 false → 停, walk 返回 false.
// inputs[k] = 特征 feats[k] 的平面 ([T][A] SoA); cs_gate / seg 同 run_node; hp / sc 按 Dag 与 ex 补齐
template <class Exec, class Visit>
inline bool walk(Exec &ex, const Dag &d, const std::vector<const check::Plane *> &inputs, HostPool &hp, std::vector<check::Arena> &sc, int T, int A,
                 int seg, const uint8_t *cs_gate, const uint8_t *need, Visit &&visit) {
  assert(inputs.size() == d.feats.size());
  const size_t n = static_cast<size_t>(T) * A;
  for (const check::Plane *p : inputs)
    assert(p && p->v.size() == n && p->m.size() == n);
  hp.prepare(d.n_slots, n);
  if (sc.size() < static_cast<size_t>(ex.threads()))
    sc.resize(static_cast<size_t>(ex.threads()));
  const auto at = [&](int j) { return plane_at(d, inputs, hp, j); };
  for (int i = 0; i < static_cast<int>(d.nodes.size()); ++i) {
    if (need && !need[i])
      continue;
    const DagNode &nd = d.nodes[static_cast<size_t>(i)];
    double ms = 0.0;
    if (nd.op >= 0) {
      const check::Plane *in[kFuseIn] = {};
      for (int a = 0; a < nd.n_in; ++a)
        in[a] = at(nd.in[a]);
      const auto t0 = std::chrono::steady_clock::now();
      run_node(ex, nd, in, hp.slots[static_cast<size_t>(nd.slot)], T, A, seg, sc, cs_gate);
      ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
    }
    if (!visit(i, at, ms))
      return false;
  }
  return true;
}

// 整张单树 Dag 走完, 返回根平面 (生存期: 池槽到下次 eval / 输入平面归调用方). 进度 / 计时 / 取消要逐节点的调用方用 walk; 多根森林也只能用 walk 边走边消费
template <class Exec>
inline const check::Plane *eval(Exec &ex, const Dag &d, const std::vector<const check::Plane *> &inputs, HostPool &hp, std::vector<check::Arena> &sc,
                                int T, int A, int seg, const uint8_t *cs_gate) {
  assert(d.roots.empty() && "eval 只给单树; 森林用 walk 逐根消费");
  walk(ex, d, inputs, hp, sc, T, A, seg, cs_gate, nullptr, [](int, const auto &, double) { return true; });
  return plane_at(d, inputs, hp, d.root());
}

} // namespace factor::cpu
