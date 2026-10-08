#pragma once

// =============================================================================
// 因子 DAG 的 GPU evaluator (宿主侧, 不含 CUDA 头): 中间量常驻显存, 按 Dag 槽计划复用设备平面
// =============================================================================
//   DevPool  会话内按 n_slots 预分配的设备平面 (跨因子复用; 只增不减, 会话关闭前 release) + 门控暂存 (gated, 按需)
//   eval     输入 = 已 upload 的设备平面 (按 Dag::feats 下标); 返回根平面 (设备侧, 交给 gpu::stat_eval / download)
//   cs_gate  截面门控 (cs_valid 的设备平面, 语义见 EvalCpu.hpp): CS 节点的每个输入经 gpu::gate 进暂存 (v 拷 D2D + m ∧ gate.m)
//            再喂算子; 设备上拷 4n 字节是 μs 级, 叶 / 中间量不分开处理 (叶的 covers 前提由宿主侧装载后 assert, 两后端共用)
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
  DevPlane *gated[3] = {}; // 门控暂存 (CS 节点输入 / Stat 的 x), 首次用到才分配
  void prepare(Session *sess, int n_slots) {
    assert(sess && (!s || s == sess) && "DevPool 绑定的会话变了, 先 release");
    s = sess;
    while (slots.size() < static_cast<size_t>(n_slots))
      slots.push_back(plane_new(s));
  }
  // 输入 a 经 cs_gate 门控后的暂存平面 (内容到下次同槽 gate_in 前有效)
  const DevPlane *gate_in(int a, const DevPlane *x, const DevPlane *cs_gate) {
    assert(s && a >= 0 && a < 3 && x && cs_gate);
    if (!gated[a])
      gated[a] = plane_new(s);
    gate(s, x, cs_gate, gated[a]);
    return gated[a];
  }
  void release() {
    for (DevPlane *p : slots)
      plane_del(s, p);
    slots.clear();
    for (DevPlane *&p : gated)
      if (p)
        plane_del(s, p), p = nullptr;
    s = nullptr;
  }
};

inline const DevPlane *eval(const Dag &d, const std::vector<const DevPlane *> &inputs, DevPool &pool, Session *s, int T, int A,
                            double *kernel_ms = nullptr, const DevPlane *cs_gate = nullptr) {
  assert(inputs.size() == d.feats.size());
  for (const DevPlane *p : inputs)
    assert(p);
  pool.prepare(s, d.n_slots);
  auto plane_of = [&](int node) -> const DevPlane * {
    const DagNode &nd = d.nodes[static_cast<size_t>(node)];
    return nd.op < 0 ? inputs[static_cast<size_t>(nd.feat)] : pool.slots[static_cast<size_t>(nd.slot)];
  };
  double total = 0.0;
  for (size_t i = 0; i < d.nodes.size(); ++i) {
    const DagNode &nd = d.nodes[i];
    if (nd.op < 0)
      continue;
    const expr::OpInfo &o = expr::kOps[nd.op];
    const bool gated = cs_gate && o.a != A::SELF;
    const DevPlane *in[3] = {};
    for (int a = 0; a < o.arity; ++a) {
      in[a] = plane_of(nd.in[a]);
      if (gated)
        in[a] = pool.gate_in(a, in[a], cs_gate);
    }
    DevPlane *out = pool.slots[static_cast<size_t>(nd.slot)];
    double ms = 0.0;
    if (o.a == A::SELF)
      run_ts_dev(s, o.name, in[0], in[1], in[2], out, T, A, nd.p, &ms);
    else
      run_cs_dev(s, o.name, in[0], in[1], in[2], out, T, A, nd.p, &ms);
    total += ms;
  }
  if (kernel_ms)
    *kernel_ms = total;
  return plane_of(d.root());
}

} // namespace factor::gpu
