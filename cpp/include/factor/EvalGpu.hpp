#pragma once

// =============================================================================
// 因子 DAG 的 GPU evaluator (宿主侧, 不含 CUDA 头): 中间量常驻显存, 按 Dag 槽计划复用设备平面
// =============================================================================
//   DevPool  会话内按 n_slots 预分配的设备平面 (跨因子复用; 只增不减, 会话关闭前 release)
//   run_node 一个算子节点: 融合节点 (fu.n > 0) → run_fused, 其余 → run_dev (CS 收 cs_gate, TS 不收); 不认具体算子
//   eval     输入 = 已 upload 的设备平面 (按 Dag::feats 下标); 返回根平面 (设备侧, 交给 gpu::stat_eval / download); seg = 段长
//   cs_gate  截面池掩码 (cs_valid 的设备平面, 只用 m; 语义见 EvalCpu.hpp 文件头) 原样递给 CS 节点的算子; 含 CS 节点时必填
//   kernel_ms 累加各算子纯 kernel 时间
// =============================================================================

#include "factor/Dag.hpp"
#include "factor/GpuRun.hpp"

#include <cassert>
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

inline const DevPlane *eval(const Dag &d, const std::vector<const DevPlane *> &inputs, DevPool &dp, Session *s, int T, int A, int seg,
                            const DevPlane *cs_gate, double *kernel_ms = nullptr) {
  assert(inputs.size() == d.feats.size());
  for (const DevPlane *p : inputs)
    assert(p);
  dp.prepare(s, d.n_slots);
  auto plane_of = [&](int node) -> const DevPlane * {
    const DagNode &nd = d.nodes[static_cast<size_t>(node)];
    return nd.op < 0 ? inputs[static_cast<size_t>(nd.feat)] : dp.slots[static_cast<size_t>(nd.slot)];
  };
  double total = 0.0;
  for (size_t i = 0; i < d.nodes.size(); ++i) {
    const DagNode &nd = d.nodes[i];
    if (nd.op < 0)
      continue;
    const DevPlane *in[kFuseIn] = {};
    for (int a = 0; a < nd.n_in; ++a)
      in[a] = plane_of(nd.in[a]);
    double ms = 0.0;
    run_node(s, nd, in, dp.slots[static_cast<size_t>(nd.slot)], T, A, seg, cs_gate, &ms);
    total += ms;
  }
  if (kernel_ms)
    *kernel_ms = total;
  return plane_of(d.root());
}

} // namespace factor::gpu
