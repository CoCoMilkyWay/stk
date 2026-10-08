#pragma once

// =============================================================================
// 因子 DAG 的 CPU evaluator: 按 Dag 的槽计划跑 factor::cpu::{ts,cs} 算子 (整张量批算)
// =============================================================================
//   Pool          按 n_slots 预分配的宿主平面 (跨因子复用, 只 ensure 不清: 契约是后端写满每格)
//   eval          单线程顺序跑一张 Dag; 输入平面按 Dag::feats 下标给; 返回根平面 (根是特征叶时直接返回该输入).
//                 因子间并行由调用方起线程 (search 的并行方案)
//   run_node_par  一个算子节点切满 threads 个线程 (Run 的并行方案: 共享 DAG 顺序走节点, 节点内并行). 与 Dag 计划解耦.
//                 切法按算子的 (T 窗, A 域) 选, 结果与单线程**逐位一致** (不是近似):
//                   CS (ALL / GROUP)      行独立 → 按 t 任意切
//                   Ts POINT / EXPAND     逐元素 / 段内递推 → 按 t 切且对齐 kSegLen (段界; TsTodMask 看 t % kSegLen)
//                   Ts ROLL / EXPO        沿 t 全程递推 (Roll 窗跨段, Ema 不 reset) → 按资产列切块:
//                                         gather 成紧凑 [T][w] 子平面 → 原算子跑 (A = w) → scatter 回去. 多两趟拷贝,
//                                         换来算子零改动 + 精确. 每线程一份块暂存 (ParScratch)
//   cs_gate       截面门控 [T][A] (特征库 cs_valid 列 = 当日在池, 整日常量, 见 features/MetaFlag.hpp). CS 节点 (A 域 ALL / GROUP)
//                 的输入掩码按来源处理, TS 节点不碰 (跨进出池的历史照常用):
//                   特征叶    直接拿 cs_gate 当掩码 (零成本). 前提 = 叶掩码 ⊇ cs_gate: CS 只在平稳可比的信息上算, 不吃 PIT 事件 /
//                             带缺失的列 (Fund 族 NaN) —— 调用方对每个喂 CS 的叶 assert 一次 (covers), 炸了就该 think again
//                   算子中间量 自身掩码 ∧ cs_gate (Roll 暖机 / 退化格是算子语义, 合法无效) → 写线程暂存, 不改共享槽
//                 算子本身零改动 (只看掩码). nullptr = 不门控
//   依赖受控浮点: 消费者 TU 编进 -fno-fast-math (CMake PRECISE_MATH_FLAG).
// =============================================================================

#include "factor/Check.hpp" // check::Plane (宿主 SoA 平面)
#include "factor/Dag.hpp"

#include "factor/CS/Cpu.hpp" // IWYU pragma: keep
#include "factor/TS/Cpu.hpp" // IWYU pragma: keep

#include <algorithm>
#include <cassert>
#include <cstring>
#include <thread>
#include <vector>

namespace factor::cpu {

using RunFn = void (*)(const float *, const uint8_t *, const float *, const uint8_t *, const float *, const uint8_t *, float *,
                       uint8_t *, int, int, const Param &);

// 算子分派表 (下标 = expr::kOps 下标): A 域 token 粘贴选命名空间 (SELF → ts, ALL / GROUP → cs)
inline RunFn run_fn(int op) {
#define FACTOR_CPU_SELF(Name) (&factor::cpu::ts::Name::run)
#define FACTOR_CPU_ALL(Name) (&factor::cpu::cs::Name::run)
#define FACTOR_CPU_GROUP FACTOR_CPU_ALL
#define FACTOR_CPU_ROW(Name, c_name, ar, t, a, kern, kdom, in, out, opx, note) FACTOR_CPU_##a(Name),
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

// ---- 截面门控 (文件头 cs_gate) ----

// dst[i] = m[i] ∧ gate[i], i ∈ [0, n)
inline void gate_mask(uint8_t *dst, const uint8_t *m, const uint8_t *gate, size_t n) {
  for (size_t i = 0; i < n; ++i)
    dst[i] = m[i] & gate[i];
}
// m ⊇ gate (gate 为真处 m 必真): 特征叶喂 CS 节点的前提, 调用方 assert 一次
inline bool covers(const uint8_t *m, const uint8_t *gate, size_t n) {
  for (size_t i = 0; i < n; ++i)
    if (gate[i] & ~m[i])
      return false;
  return true;
}
// 节点 nd 的输入 a 是否特征叶 (位 a); 给 run_node_par 的 leaf_bits
inline unsigned leaf_bits_of(const Dag &d, const DagNode &nd) {
  unsigned b = 0;
  for (int a = 0; nd.op >= 0 && a < expr::kOps[nd.op].arity; ++a)
    b |= (d.nodes[static_cast<size_t>(nd.in[a])].op < 0 ? 1u : 0u) << a;
  return b;
}

// A 块切法的每线程暂存: 0..2 输入块, 3 输出块 (紧凑 [T][w]); t 切法 CS 节点的门控掩码也放 m[0..2]
struct ParScratch {
  std::vector<float> v[4];
  std::vector<uint8_t> m[4];
};

// inputs[k] = 特征 feats[k] 的平面 ([T][A] SoA). 返回根平面 (生存期: 池槽到下次 eval / 输入平面归调用方)
// cs_gate: 截面门控掩码 [T][A] (文件头), nullptr = 不门控; 喂 CS 节点的叶须 covers(叶.m, cs_gate) (此处 assert)
inline const check::Plane *eval(const Dag &d, const std::vector<const check::Plane *> &inputs, Pool &pool, int T, int A,
                                const uint8_t *cs_gate = nullptr) {
  assert(inputs.size() == d.feats.size());
  const size_t n = static_cast<size_t>(T) * A;
  for (const check::Plane *p : inputs)
    assert(p && p->v.size() == n && p->m.size() == n);
  pool.prepare(d.n_slots, n);
  auto plane_of = [&](int node) -> const check::Plane * {
    const DagNode &nd = d.nodes[static_cast<size_t>(node)];
    return nd.op < 0 ? inputs[static_cast<size_t>(nd.feat)] : &pool.slots[static_cast<size_t>(nd.slot)];
  };
  ParScratch gm; // CS 节点中间量的门控掩码暂存
  for (size_t i = 0; i < d.nodes.size(); ++i) {
    const DagNode &nd = d.nodes[i];
    if (nd.op < 0)
      continue;
    const expr::OpInfo &o = expr::kOps[nd.op];
    const int ar = o.arity;
    const bool gated = cs_gate && o.a != A::SELF;
    const check::Plane *in[3] = {};
    const uint8_t *m[3] = {};
    for (int a = 0; a < ar; ++a) {
      in[a] = plane_of(nd.in[a]);
      m[a] = in[a]->m.data();
      if (!gated)
        continue;
      if (d.nodes[static_cast<size_t>(nd.in[a])].op < 0) {
        assert(covers(m[a], cs_gate, n) && "CS 算子的特征叶在 cs_valid 行上有缺失: CS 不吃 PIT 事件 / 带 NaN 的列 (Fund 族), 先包 TS 算子处理缺失");
        m[a] = cs_gate;
      } else {
        gm.m[a].resize(n);
        gate_mask(gm.m[a].data(), m[a], cs_gate, n);
        m[a] = gm.m[a].data();
      }
    }
    check::Plane &out = pool.slots[static_cast<size_t>(nd.slot)];
    run_fn(nd.op)(check::pv(in[0], ar >= 1), m[0], check::pv(in[1], ar >= 2), m[1], check::pv(in[2], ar >= 3), m[2], out.v.data(),
                  out.m.data(), T, A, nd.p);
  }
  return plane_of(d.root());
}

// ---- 节点内并行 ----

namespace detail {

// n_tasks 个任务静态条带分给 min(threads, n_tasks) 个线程: fn(task, tid)
template <class Fn>
inline void par_tasks(int n_tasks, int threads, Fn &&fn) {
  assert(threads >= 1);
  const int nt = std::min(threads, n_tasks);
  if (nt <= 1) {
    for (int i = 0; i < n_tasks; ++i)
      fn(i, 0);
    return;
  }
  std::vector<std::thread> th;
  th.reserve(static_cast<size_t>(nt));
  for (int tid = 0; tid < nt; ++tid)
    th.emplace_back([&fn, tid, nt, n_tasks] {
      for (int i = tid; i < n_tasks; i += nt)
        fn(i, tid);
    });
  for (std::thread &t : th)
    t.join();
}

} // namespace detail

// in[a] (a < arity) 输入平面, out 输出平面 (已 ensure); sc.size() ≥ threads
// cs_gate: 截面门控掩码 [T][A] (文件头), 只作用于 CS 节点; nullptr = 不门控.
// leaf_bits: 位 a = 输入 a 是特征叶 (leaf_bits_of) → 直接拿 cs_gate 当掩码; covers 由调用方装载后 assert 一次, 这里不重扫
inline void run_node_par(const DagNode &nd, const check::Plane *const in[3], check::Plane &out, int T, int A, int threads,
                         std::vector<ParScratch> &sc, const uint8_t *cs_gate = nullptr, unsigned leaf_bits = 0) {
  assert(nd.op >= 0 && threads >= 1 && sc.size() >= static_cast<size_t>(threads));
  const expr::OpInfo &o = expr::kOps[nd.op];
  const int ar = o.arity;
  for (int a = 0; a < ar; ++a)
    assert(in[a] && in[a]->v.size() == static_cast<size_t>(T) * A);
  assert(out.v.size() == static_cast<size_t>(T) * A);
  const RunFn fn = run_fn(nd.op);
  const auto V = [&](int a, size_t off) -> const float * { return a < ar ? in[a]->v.data() + off : nullptr; };
  const auto M = [&](int a, size_t off) -> const uint8_t * { return a < ar ? in[a]->m.data() + off : nullptr; };

  const bool a_split = o.a == A::SELF && (o.t == T::ROLL || o.t == T::EXPO);
  if (!a_split) {
    // 按 t 切: CS 单位 = 行; Ts POINT / EXPAND 单位 = 段 (EXPAND 段内递推; POINT 里 TsTodMask 看 t % kSegLen, 也得对齐段界)
    const int unit = o.a == A::SELF ? kSegLen : 1;
    assert(T % unit == 0 && "Ts 切块要求 T 是整段");
    const int units = T / unit;
    const int nchunk = std::min(threads, units);
    const int per = (units + nchunk - 1) / nchunk;
    const bool gated = cs_gate && o.a != A::SELF; // CS 节点: 叶 → cs_gate 本身; 中间量 → 块内掩码 ∧ 门控写线程暂存, 值不拷
    detail::par_tasks(nchunk, threads, [&](int c, int tid) {
      const int t0 = c * per * unit, t1 = std::min(T, (c + 1) * per * unit);
      if (t0 >= t1)
        return;
      const size_t off = static_cast<size_t>(t0) * A, len = static_cast<size_t>(t1 - t0) * A;
      const uint8_t *m[3] = {M(0, off), M(1, off), M(2, off)};
      if (gated) {
        ParScratch &s = sc[static_cast<size_t>(tid)];
        for (int a = 0; a < ar; ++a) {
          if (leaf_bits >> a & 1u) {
            m[a] = cs_gate + off;
            continue;
          }
          s.m[a].resize(len);
          gate_mask(s.m[a].data(), m[a], cs_gate + off, len);
          m[a] = s.m[a].data();
        }
      }
      fn(V(0, off), m[0], V(1, off), m[1], V(2, off), m[2], out.v.data() + off, out.m.data() + off, t1 - t0, A, nd.p);
    });
    return;
  }
  // 按资产列切块 (宽度凑 8 的倍数, 便于向量化)
  const int w = std::max(8, ((A + threads - 1) / threads + 7) / 8 * 8);
  const int nb = (A + w - 1) / w;
  detail::par_tasks(nb, threads, [&](int b, int tid) {
    const int a0 = b * w, wb = std::min(w, A - a0);
    ParScratch &s = sc[static_cast<size_t>(tid)];
    const size_t nblk = static_cast<size_t>(T) * wb;
    for (int a = 0; a < ar; ++a) {
      s.v[a].resize(nblk), s.m[a].resize(nblk);
      for (int t = 0; t < T; ++t) {
        const size_t src = static_cast<size_t>(t) * A + a0, dst = static_cast<size_t>(t) * wb;
        std::memcpy(s.v[a].data() + dst, in[a]->v.data() + src, static_cast<size_t>(wb) * sizeof(float));
        std::memcpy(s.m[a].data() + dst, in[a]->m.data() + src, static_cast<size_t>(wb));
      }
    }
    s.v[3].resize(nblk), s.m[3].resize(nblk);
    const auto BV = [&](int a) -> const float * { return a < ar ? s.v[a].data() : nullptr; };
    const auto BM = [&](int a) -> const uint8_t * { return a < ar ? s.m[a].data() : nullptr; };
    fn(BV(0), BM(0), BV(1), BM(1), BV(2), BM(2), s.v[3].data(), s.m[3].data(), T, wb, nd.p);
    for (int t = 0; t < T; ++t) {
      const size_t dst = static_cast<size_t>(t) * A + a0, src = static_cast<size_t>(t) * wb;
      std::memcpy(out.v.data() + dst, s.v[3].data() + src, static_cast<size_t>(wb) * sizeof(float));
      std::memcpy(out.m.data() + dst, s.m[3].data() + src, static_cast<size_t>(wb));
    }
  });
}

} // namespace factor::cpu
