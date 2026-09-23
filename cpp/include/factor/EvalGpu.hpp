#pragma once

// =============================================================================
// 因子 DAG 的 GPU evaluator (宿主侧, 不含 CUDA 头): 中间量常驻显存, 按 Dag 槽计划复用设备平面
// =============================================================================
//   DevPool  会话内按 n_slots 预分配的设备平面 (跨因子复用; 只增不减, 会话关闭前 release)
//   eval     输入 = 已 upload 的设备平面 (按 Dag::feats 下标); 返回根平面 (设备侧, 交给 gpu::stat_eval / download)
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

inline const DevPlane *eval(const Dag &d, const std::vector<const DevPlane *> &inputs, DevPool &pool, Session *s, int T, int A,
                            double *kernel_ms = nullptr) {
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
    const DevPlane *in[3] = {};
    for (int a = 0; a < o.arity; ++a)
      in[a] = plane_of(nd.in[a]);
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
