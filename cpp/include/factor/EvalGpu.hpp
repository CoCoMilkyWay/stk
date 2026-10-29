#pragma once

// =============================================================================
// 因子 DAG 的 GPU evaluator (宿主侧, 不含 CUDA 头): 中间量常驻显存, 按 Dag 槽计划复用设备平面
// =============================================================================
//   DevPool  会话内按 n_slots 预分配的设备平面 (跨因子复用; 只增不减, 会话关闭前 release)
//   run_node 一个算子节点: 融合节点 (fu.n > 0) → run_fused, 其余 → run_dev (CS 收 cs_gate, TS 不收); 不认具体算子
//   walk     顺序走 Dag, 逐节点回调 visit (口径同 EvalCpu walk); 输入 = 已 upload 的设备平面 (按 Dag::feats 下标); seg = 段长
//   eval     walk 到底, 返回根平面 (设备侧, 交给 gpu::stat_eval / download)
//   cs_gate  截面池掩码 (cs_valid 的设备平面, 只用 m; 语义见 EvalCpu.hpp 文件头) 原样递给 CS 节点的算子; 含 CS 节点时必填
//   kernel_ms 累加各算子纯 kernel 时间
// =============================================================================

#include "factor/Dag.hpp"
#include "factor/GpuRun.hpp"

#include <cassert>
#include <cstdint>
#include <vector>

namespace factor::gpu {

struct DevPool {
  Session *s = nullptr;
  std::vector<DevPlane *> slots;
  void prepare(Session *sess, int n_slots) {
    assert(sess && (!s || s == sess) && "DevPool 绑定的会话变了, 先 release");
    s = sess;
    while (slots.size() < static_cast<size_t>(n_slots))
      slots.push_back(plane_new(s));
  }
  void release() {
    for (DevPlane *p : slots)
      plane_del(s, p);
    slots.clear();
    s = nullptr;
  }
};

// in[a] (a < nd.n_in) 输入平面; kernel_ms 口径同 run_dev
inline void run_node(Session *s, const DagNode &nd, const DevPlane *const in[kFuseIn], DevPlane *out, int T, int A, int seg,
                     const DevPlane *cs_gate, double *kernel_ms = nullptr) {
  assert(nd.op >= 0);
  for (int a = 0; a < nd.n_in; ++a)
    assert(in[a]);
  if (nd.fu.n > 0)
    return run_fused(s, nd.fu, in, nd.n_in, out, T, A, seg, kernel_ms);
  const expr::OpInfo &o = expr::kOps[nd.op];
  assert((o.a == A::SELF || cs_gate) && "CS 节点必须给截面池掩码 cs_gate");
  run_dev(s, nd.op, in[0], in[1], in[2], o.a == A::SELF ? nullptr : cs_gate, out, T, A, seg, nd.p, kernel_ms);
}

// 节点 i 的平面: 叶 = inputs[feat], 算子 = 池槽 (复用口径同 EvalCpu plane_at)
inline const DevPlane *plane_at(const Dag &d, const std::vector<const DevPlane *> &inputs, const DevPool &dp, int i) {
  const DagNode &nd = d.nodes[static_cast<size_t>(i)];
  return nd.op < 0 ? inputs[static_cast<size_t>(nd.feat)] : dp.slots[static_cast<size_t>(nd.slot)];
}

// 顺序走 Dag, 回调口径同 EvalCpu walk: visit(i, at, ms) → bool, ms = 算子纯 kernel (叶 0); dp 按 Dag 补齐
template <class Visit>
inline bool walk(Session *s, const Dag &d, const std::vector<const DevPlane *> &inputs, DevPool &dp, int T, int A, int seg, const DevPlane *cs_gate,
                 const uint8_t *need, Visit &&visit) {
  assert(inputs.size() == d.feats.size());
  for (const DevPlane *p : inputs)
    assert(p);
  dp.prepare(s, d.n_slots);
  const auto at = [&](int j) { return plane_at(d, inputs, dp, j); };
  for (int i = 0; i < static_cast<int>(d.nodes.size()); ++i) {
    if (need && !need[i])
      continue;
    const DagNode &nd = d.nodes[static_cast<size_t>(i)];
    double ms = 0.0;
    if (nd.op >= 0) {
      const DevPlane *in[kFuseIn] = {};
      for (int a = 0; a < nd.n_in; ++a)
        in[a] = at(nd.in[a]);
      run_node(s, nd, in, dp.slots[static_cast<size_t>(nd.slot)], T, A, seg, cs_gate, &ms);
    }
    if (!visit(i, at, ms))
      return false;
  }
  return true;
}

inline const DevPlane *eval(const Dag &d, const std::vector<const DevPlane *> &inputs, DevPool &dp, Session *s, int T, int A, int seg,
                            const DevPlane *cs_gate, double *kernel_ms = nullptr) {
  assert(d.roots.empty() && "eval 只给单树; 森林用 walk 逐根消费");
  double total = 0.0;
  walk(s, d, inputs, dp, T, A, seg, cs_gate, nullptr, [&](int, const auto &, double ms) {
    total += ms;
    return true;
  });
  if (kernel_ms)
    *kernel_ms = total;
  return plane_at(d, inputs, dp, d.root());
}

} // namespace factor::gpu
