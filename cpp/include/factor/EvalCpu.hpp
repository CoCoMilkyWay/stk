#pragma once

// =============================================================================
// 因子 DAG 的 CPU evaluator: 按 Dag 的槽计划跑 factor::cpu::{ts,cs} 算子 (整张量批算, 单线程; 因子间并行由调用方起线程)
// =============================================================================
//   Pool  按 n_slots 预分配的宿主平面 (跨因子复用, 只 ensure 不清: 契约是后端写满每格)
//   eval  输入平面按 Dag::feats 下标给; 返回根平面 (根是特征叶时直接返回该输入)
//   依赖受控浮点: 消费者 TU 编进 -fno-fast-math (CMake PRECISE_MATH_FLAG).
// =============================================================================

#include "factor/Check.hpp" // check::Plane (宿主 SoA 平面)
#include "factor/Dag.hpp"

#include "factor/CS/Cpu.hpp" // IWYU pragma: keep
#include "factor/TS/Cpu.hpp" // IWYU pragma: keep

#include <cassert>
#include <vector>

namespace factor::cpu {

using RunFn = void (*)(const float *, const uint8_t *, const float *, const uint8_t *, const float *, const uint8_t *, float *,
                       uint8_t *, int, int, const Param &);

// 算子分派表 (下标 = expr::kOps 下标): A 域 token 粘贴选命名空间 (SELF → ts, ALL / GROUP → cs)
inline RunFn run_fn(int op) {
#define FACTOR_CPU_SELF(Name) (&factor::cpu::ts::Name::run)
#define FACTOR_CPU_ALL(Name) (&factor::cpu::cs::Name::run)
#define FACTOR_CPU_GROUP FACTOR_CPU_ALL
#define FACTOR_CPU_ROW(Name, c_name, ar, t, a, kern, prm, operand, opx, note) FACTOR_CPU_##a(Name),
  static const RunFn tbl[] = {OP_ALL(FACTOR_CPU_ROW)};
#undef FACTOR_CPU_ROW
#undef FACTOR_CPU_GROUP
#undef FACTOR_CPU_ALL
#undef FACTOR_CPU_SELF
  static_assert(sizeof(tbl) / sizeof(tbl[0]) == static_cast<size_t>(expr::kOpCount));
  assert(op >= 0 && op < expr::kOpCount);
  return tbl[op];
}

struct Pool {
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

// inputs[k] = 特征 feats[k] 的平面 ([T][A] SoA). 返回根平面 (生存期: 池槽到下次 eval / 输入平面归调用方)
inline const check::Plane *eval(const Dag &d, const std::vector<const check::Plane *> &inputs, Pool &pool, int T, int A) {
  assert(inputs.size() == d.feats.size());
  const size_t n = static_cast<size_t>(T) * A;
  for (const check::Plane *p : inputs)
    assert(p && p->v.size() == n && p->m.size() == n);
  pool.prepare(d.n_slots, n);
  auto plane_of = [&](int node) -> const check::Plane * {
    const DagNode &nd = d.nodes[static_cast<size_t>(node)];
    return nd.op < 0 ? inputs[static_cast<size_t>(nd.feat)] : &pool.slots[static_cast<size_t>(nd.slot)];
  };
  for (size_t i = 0; i < d.nodes.size(); ++i) {
    const DagNode &nd = d.nodes[i];
    if (nd.op < 0)
      continue;
    const int ar = expr::kOps[nd.op].arity;
    const check::Plane *in[3] = {};
    for (int a = 0; a < ar; ++a)
      in[a] = plane_of(nd.in[a]);
    check::Plane &out = pool.slots[static_cast<size_t>(nd.slot)];
    run_fn(nd.op)(check::pv(in[0], ar >= 1), check::pm(in[0], ar >= 1), check::pv(in[1], ar >= 2), check::pm(in[1], ar >= 2),
                  check::pv(in[2], ar >= 3), check::pm(in[2], ar >= 3), out.v.data(), out.m.data(), T, A, nd.p);
  }
  return plane_of(d.root());
}

} // namespace factor::cpu
