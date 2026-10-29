// =============================================================================
// op_check: 因子算子三方对拍 (stream 实盘流式 / cpu 挖掘向量 / gpu 挖掘 CUDA, 全程 C++, 不经 Python)
// =============================================================================
//   用法  op_check [--op Name] [--seg S] [--T 765] [--A 32] [--seed 1] [-v]
//         每个用例在全部层几何 (Contract kGeos: S = 255 / 49 / 17 / 9 / 5 / 1) 上各跑一遍; --seg 只跑一种.
//         --T 是目标行数, 每层向上取整到整段数
//   stream 是语义锚 (实盘路径); cpu 整张量一次算出参考平面, stream 与 gpu 各自与它比
//   (cpu↔stream 紧容差互证, gpu↔cpu 松容差 —— 传递闭包覆盖三方).
//   三份实现互不可见 (各自只 include Contract.hpp), 所以这套对拍不会空转.
//   分派表由 OpTable.hpp 展开: 表里有名字而某个后端没有同名 struct → 此处编译错.
//
//   比较口径 (契约): 掩码必须**逐位相等**; 有效位上 |Δ| ≤ atol + rtol·max(|a|,|b|).
//   属性列也对拍: 流式 TS struct::kT 与 OpTable 的 T 窗列不符 → static_assert 错.
//   造数 / 配方 / 容差 / 比较 / 驱动在 factor/Check.hpp (与 GUI Factors→Operators 页共用同一口径);
//   本文件只剩 profile × d 全扫 + 汇总. 本 TU 依赖受控浮点, CMake 里整 target -fno-fast-math.
//   单算子之后一节 Fuse (逐点链融合: 随机表达式树, 融合 DAG 与不融合 DAG 在 cpu / gpu 上各自逐位相等): --op Fuse 单跑.
//   末尾另有一节 Stat (因子评估算子, factor/Stat; 不在 OpTable, 无流式后端, cpu ↔ gpu 两方 match): --op Stat 单跑.
//   再一节 Search (参数搜索评估算子, factor/Search; 参考 = Stat CPU, Search cpu / gpu 各与它比 + 分块 ≡ 整块): --op Search 单跑.
// =============================================================================

#include "factor/TS/Cpu.hpp"    // IWYU pragma: keep
#include "factor/TS/Stream.hpp" // IWYU pragma: keep

#include "factor/CS/Cpu.hpp"    // IWYU pragma: keep
#include "factor/CS/Stream.hpp" // IWYU pragma: keep

#include "factor/Check.hpp"
#include "factor/Contract.hpp"
#include "factor/Dag.hpp"
#include "factor/EvalCpu.hpp"
#include "factor/EvalGpu.hpp"
#include "factor/Expr.hpp" // kOps / op_index (算子下标 = GPU 分派下标; 元数 / in 域 / 参数列)
#include "factor/GpuRun.hpp"
#include "factor/OpTable.hpp"
#include "factor/Search/Check.hpp"
#include "factor/Stat/Check.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace {

using namespace factor::check;
using factor::Param;

// ---- 汇总 ----
struct Report {
  int cases = 0, stream_bad = 0, gpu_bad = 0;
  int ops = 0, ops_bad = 0;
  bool verbose = false;
  int seg_only = 0; // 0 = 全部层; 否则只跑段长 = seg_only 的层
  int T_target = 0; // 目标行数 (每层向上取整到整段数)
};

// 本层的张量行数: ≥ T_target 的最小整段数, 且盖住造数 profile 的段界 (S + 37) 与最长 d (S + 60)
int rows_of(const Report &rep, int S) {
  int T = (rep.T_target + S - 1) / S * S;
  while (T <= S + 60)
    T += S;
  return T;
}

// 按 rep.seg_only 筛层, 每层以段长 S 调 f(S)
template <class F>
void for_segs(const Report &rep, F &&f) {
  for (const factor::Geo &g : factor::kGeos)
    if (rep.seg_only == 0 || rep.seg_only == g.S)
      f(g.S);
}

void emit(Report &rep, const char *name, int S, const Param &p, const char *prof, const char *side, const Diff &d, bool &op_bad) {
  if (d.ok()) {
    if (rep.verbose)
      std::printf("  ok   %-14s S=%-3d %-6s d=%-3d k=%-5g %-8s 比较 %6d 点, max|Δ| = %.3g\n", name, S, side, p.d, p.k, prof, d.compared,
                  d.worst);
    return;
  }
  op_bad = true;
  std::printf("  FAIL %-14s S=%-3d %-6s d=%-3d k=%-5g %-8s 掩码不符 %d, 超差 %d / %d, max|Δ| = %.3g, 首处 idx %d\n", name, S, side, p.d,
              p.k, prof, d.mask_bad, d.val_bad, d.compared, d.worst, d.worst_at);
}

// 一个算子在一层 (段长 S) 上的全部用例: profile × d 扫描
//   d 扫 {1, 5, 20, S, S + 60}: S = 恰一段 (ROLL 窗界与段界重合), S + 60 > 段 (窗跨段, GPU 块界不对齐段界)
template <class Str, class C, bool IS_CS>
void check_seg(int op, int S, int A, unsigned seed, Report &rep, bool &op_bad) {
  const factor::expr::OpInfo &o = factor::expr::kOps[op];
  const char *name = o.name;
  const int T = rows_of(rep, S), AR = o.arity;
  const std::string nm = name;
  const Recipe rc = recipe_of(o.in);
  const bool sweep_d = std::strchr(o.params, 'd') != nullptr;
  const int ds[] = {1, 5, 20, S, S + 60};
  constexpr int kNd = static_cast<int>(sizeof(ds) / sizeof(ds[0]));

  for (int pi = 0; pi < kProfiles; ++pi) {
    const Profile pr = static_cast<Profile>(pi);
    std::mt19937 rng(seed + 1000u * pi);
    Plane x, y, z, gate; // 元数够到的槽位才造; CS 多一张池掩码
    Data d;
    d.T = T, d.A = A;
    if (AR >= 1)
      fill(x, rc.x, pr, T, A, S, rng), d.x = &x;
    if (AR >= 2)
      fill(y, rc.y, pr, T, A, S, rng), d.y = &y;
    if (AR >= 3)
      fill(z, rc.z, pr, T, A, S, rng), d.z = &z;
    if (IS_CS)
      fill_gate(gate, pr, T, A, S, rng), d.g = &gate;

    for (int di = 0; di < (sweep_d ? kNd : 1); ++di) {
      Param p;
      p.d = sweep_d ? ds[di] : 1;
      set_k(nm, p, S);

      Plane ref, got;
      if constexpr (IS_CS) {
        run_cpu_cs<C>(d, p, AR, S, ref);
        run_stream_cs<Str>(d, p, AR, S, got);
      } else {
        run_cpu_ts<C>(d, p, AR, S, ref);
        run_stream_ts<Str>(d, p, AR, S, got);
      }
      ++rep.cases;
      Diff ds_ = compare(ref, got, tol_of(nm, false, pr));
      if (!ds_.ok())
        ++rep.stream_bad;
      emit(rep, name, S, p, kProfName[pi], "stream", ds_, op_bad);

      if (factor::gpu::available()) {
        Plane g;
        g.resize(static_cast<size_t>(T) * A);
        const auto V = [](const Plane *q) -> const float * { return q ? q->v.data() : nullptr; };
        const auto M = [](const Plane *q) -> const uint8_t * { return q ? q->m.data() : nullptr; };
        // 一次性宿主指针版本 (内部临时会话)
        factor::gpu::run(op, V(d.x), M(d.x), V(d.y), M(d.y), V(d.z), M(d.z), M(d.g), g.v.data(), g.m.data(), T, A, S, p);
        Diff dg = compare(ref, g, tol_of(nm, true, pr));
        if (!dg.ok())
          ++rep.gpu_bad;
        emit(rep, name, S, p, kProfName[pi], "gpu", dg, op_bad);
      }
    }
  }
}

// 一个算子的全部用例: 层 × profile × d
template <class Str, class C, bool IS_CS>
void check(int op, int A, unsigned seed, Report &rep) {
  assert(op >= 0);
  ++rep.ops;
  bool op_bad = false;
  for_segs(rep, [&](int S) { check_seg<Str, C, IS_CS>(op, S, A, seed, rep, op_bad); });
  if (op_bad)
    ++rep.ops_bad;
  else if (!rep.verbose)
    std::printf("  ok   %-14s\n", factor::expr::kOps[op].name);
}

// 逐点链融合 (Dag fuse): 随机表达式树, 融合 DAG ≡ 不融合 DAG, cpu / gpu 各自逐位相等 (跨后端已由单算子节覆盖).
//   内部节点 70% 逐点 (POINT × SELF) / 20% 其余 TS / 10% CS ALL; 叶 = 三张特征平面 x0 / x1 / x2 (NORM / POS / SMALL);
//   15% 的子节点复用已生成的子树 (多消费者 → 不并, 打组边界). 层 × profile × kFuseCases 棵, 每棵至少一个融合节点
constexpr int kFuseCases = 8, kFuseDepth = 6;

struct GenNode {
  int op = -1, feat = -1;
  Param p;
  int kids[3] = {};
};

// 生成一棵子树进 pool (子先于父入池), 返回其下标
int gen_tree(std::vector<GenNode> &pool, std::mt19937 &rng, int depth, int S) {
  static const auto ops_of = [](int kind) {
    std::vector<int> r;
    for (int i = 0; i < factor::expr::kOpCount; ++i) {
      const factor::expr::OpInfo &o = factor::expr::kOps[i];
      const int k = factor::detail::pointwise(i) ? 0 : (o.a == factor::A::SELF ? 1 : (o.a == factor::A::ALL ? 2 : -1));
      if (k == kind)
        r.push_back(i);
    }
    assert(!r.empty());
    return r;
  };
  static const std::vector<int> pools[3] = {ops_of(0), ops_of(1), ops_of(2)};
  constexpr int kD[] = {1, 5, 20};
  std::uniform_real_distribution<float> ud(0.f, 1.f);
  GenNode g;
  if (depth == 0 || ud(rng) < 0.15f) {
    g.feat = static_cast<int>(rng() % 3);
  } else {
    const float u = ud(rng);
    const std::vector<int> &from = pools[u < 0.7f ? 0 : (u < 0.9f ? 1 : 2)];
    g.op = from[rng() % from.size()];
    const factor::expr::OpInfo &o = factor::expr::kOps[g.op];
    if (factor::expr::declares(o.params, "d"))
      g.p.d = kD[rng() % 3];
    set_k(o.name, g.p, S);
    for (int a = 0; a < o.arity; ++a)
      g.kids[a] = !pool.empty() && ud(rng) < 0.15f ? static_cast<int>(rng() % pool.size()) : gen_tree(pool, rng, depth - 1, S);
  }
  pool.push_back(g);
  return static_cast<int>(pool.size()) - 1;
}

// pool[g] 前序展开进 e (复用的子树展开成重复子式, 由 Dag 按规范串去重回共享节点)
int emit_expr(factor::expr::Expr &e, const std::vector<GenNode> &pool, int g) {
  const GenNode &q = pool[static_cast<size_t>(g)];
  const int i = static_cast<int>(e.nodes.size());
  factor::expr::Node n;
  n.op = q.op, n.p = q.p;
  if (q.op < 0)
    n.feat = "x" + std::to_string(q.feat);
  e.nodes.push_back(n);
  if (q.op >= 0) {
    ++e.n_ops;
    for (int a = 0; a < factor::expr::kOps[q.op].arity; ++a) {
      const int c = emit_expr(e, pool, q.kids[a]);
      e.nodes[static_cast<size_t>(i)].args[a] = c;
    }
  }
  return i;
}

void check_fuse(int A, unsigned seed, Report &rep) {
  namespace gpu = factor::gpu;
  ++rep.ops;
  bool op_bad = false;
  misc::exec::ForkJoin ex(misc::exec::hw());
  std::vector<Arena> sc(static_cast<size_t>(ex.threads()));
  factor::cpu::HostPool hp_f, hp_u;
  const Param p0;
  const Tol exact{0.0, 0.0};
  const auto fi = [](const std::string &code) { return code[1] - '0'; }; // 特征 code "x<k>" → 平面 k
  for_segs(rep, [&](int S) {
    const int T = rows_of(rep, S);
    const size_t n = static_cast<size_t>(T) * A;
    for (int pi = 0; pi < kProfiles; ++pi) {
      const Profile pr = static_cast<Profile>(pi);
      std::mt19937 rng(seed + 1000u * pi + static_cast<unsigned>(S));
      Plane feat[3], gate;
      const Gen gens[3] = {Gen::NORM, Gen::POS, Gen::SMALL};
      for (int k = 0; k < 3; ++k)
        fill(feat[k], gens[k], pr, T, A, S, rng);
      fill_gate(gate, pr, T, A, S, rng);
      gpu::Session *gs = nullptr;
      const gpu::DevPlane *dfeat[3] = {}, *dgate = nullptr;
      gpu::DevPool dp_f, dp_u;
      if (gpu::available()) {
        gs = gpu::session_open(n);
        for (int k = 0; k < 3; ++k)
          dfeat[k] = gpu::upload(gs, feat[k].v.data(), feat[k].m.data());
        dgate = gpu::upload_mask(gs, gate.m.data());
      }
      for (int c = 0; c < kFuseCases; ++c) {
        factor::expr::Expr e;
        factor::Dag df;
        int fused = 0, steps = 0;
        while (fused == 0) {
          std::vector<GenNode> pool;
          const int root = gen_tree(pool, rng, kFuseDepth, S);
          e = factor::expr::Expr{};
          emit_expr(e, pool, root);
          df = factor::build(e);
          for (const factor::DagNode &nd : df.nodes)
            if (nd.fu.n > 0)
              ++fused, steps += nd.fu.n;
        }
        const factor::Dag du = factor::build(e, /*fuse=*/false);
        char tag[32];
        std::snprintf(tag, sizeof(tag), "Fuse.%d", c);
        bool bad = false;

        const auto cpu_in = [&](const factor::Dag &d) {
          std::vector<const Plane *> in(d.feats.size());
          for (size_t k = 0; k < in.size(); ++k)
            in[k] = &feat[fi(d.feats[k])];
          return in;
        };
        const Plane &rf = *factor::cpu::eval(ex, df, cpu_in(df), hp_f, sc, T, A, S, gate.m.data());
        const Plane &ru = *factor::cpu::eval(ex, du, cpu_in(du), hp_u, sc, T, A, S, gate.m.data());
        ++rep.cases;
        const Diff dc = compare(ru, rf, exact);
        if (!dc.ok())
          ++rep.stream_bad, bad = true; // cpu 侧失败计在 stream 列 (同 Search 节)
        emit(rep, tag, S, p0, kProfName[pi], "cpu", dc, op_bad);

        if (gs) {
          const auto gpu_in = [&](const factor::Dag &d) {
            std::vector<const gpu::DevPlane *> in(d.feats.size());
            for (size_t k = 0; k < in.size(); ++k)
              in[k] = dfeat[fi(d.feats[k])];
            return in;
          };
          const gpu::DevPlane *of = gpu::eval(df, gpu_in(df), dp_f, gs, T, A, S, dgate);
          const gpu::DevPlane *ou = gpu::eval(du, gpu_in(du), dp_u, gs, T, A, S, dgate);
          Plane gf, gu;
          gf.resize(n), gu.resize(n);
          gpu::download(gs, of, gf.v.data(), gf.m.data());
          gpu::download(gs, ou, gu.v.data(), gu.m.data());
          const Diff dg = compare(gu, gf, exact);
          if (!dg.ok())
            ++rep.gpu_bad, bad = true;
          emit(rep, tag, S, p0, kProfName[pi], "gpu", dg, op_bad);
        }
        if (bad || rep.verbose)
          std::printf("       %s S=%-3d %-8s 融合 %d 节点 / %d 步 (DAG %zu → %zu 节点): %s\n", tag, S, kProfName[pi], fused, steps,
                      du.nodes.size(), df.nodes.size(), factor::expr::to_string(e).c_str());
      }
      if (gs) {
        dp_f.release(), dp_u.release();
        gpu::session_close(gs);
      }
    }
  });
  if (op_bad)
    ++rep.ops_bad;
  else if (!rep.verbose)
    std::printf("  ok   %-14s\n", "Fuse");
}

// Stat 评估算子 (不在 OpTable: 输出是 rows[H][T] + 每持有期标量, 无流式后端): cpu ↔ gpu 两方 match 即过.
//   层 × 两口径 (CS / TS) × profile 全扫 (无 d; 持有期取本层合法的); 无 GPU 时只跑 cpu 并打印二级汇总 (-v), 不算失败
void check_stat(int A, unsigned seed, Report &rep) {
  namespace sc = factor::stat::check; // 与 factor::check 同名的 Data / Tol 等, 全限定
  ++rep.ops;
  bool op_bad = false;
  const int threads = misc::exec::hw();
  for_segs(rep, [&](int S) {
    const int T = rows_of(rep, S);
    for (const factor::stat::Frame fr : {factor::stat::Frame::CS, factor::stat::Frame::TS}) {
      const char *fname = factor::stat::frame_name(fr);
      char tag_rows[32], tag_stat[32];
      std::snprintf(tag_rows, sizeof(tag_rows), "Stat.%s.rows", fname);
      std::snprintf(tag_stat, sizeof(tag_stat), "Stat.%s.stat", fname);
      for (int pi = 0; pi < kProfiles; ++pi) {
        const Profile pr = static_cast<Profile>(pi);
        std::mt19937 rng(seed + 1000u * pi);
        sc::Data d;
        sc::make(d, fr, pr, T, A, S, rng);
        sc::Result cpu;
        sc::run_cpu(d, cpu, threads);
        if (rep.verbose)
          for (const factor::stat::HoldStat &s : cpu.stat)
            std::printf("       Stat.%s cpu    S=%-3d h=%-3d %-8s n=%d/%d  rIC %+.4f sd %.4f IR %+.3f t %+.2f pos %.2f  LS %+.5f t %+.2f pos %.2f "
                        "Sharpe %+.2f beta %+.3f  mono %+.3f  rAC %+.3f sd %.4f IR %+.3f t %+.2f pos %.2f\n",
                        fname, S, s.hold, kProfName[pi], s.n, s.n_rac, s.ric.mean, s.ric.sd, s.ric.ir, s.ric.t, s.ric.pos, s.ls[factor::stat::kFwd].mean,
                        s.ls[factor::stat::kFwd].t, s.ls[factor::stat::kFwd].pos, factor::stat::sharpe_of(s, factor::stat::kFwd), s.beta[factor::stat::kFwd],
                        s.mono, s.rac.mean, s.rac.sd, s.rac.ir, s.rac.t, s.rac.pos);
        if (!factor::gpu::available())
          continue;
        sc::Result gpu;
        sc::run_gpu(d, gpu);
        const Tol tol = sc::tol_of(pr);
        const Diff dr = sc::compare_rows(cpu.rows.data(), gpu.rows.data(), cpu.rows.size(), tol);
        const Diff ds = sc::compare_stat(cpu.stat.data(), gpu.stat.data(), d.hd.n, tol);
        ++rep.cases;
        if (!dr.ok() || !ds.ok())
          ++rep.gpu_bad;
        Param p;
        emit(rep, tag_rows, S, p, kProfName[pi], "gpu", dr, op_bad);
        emit(rep, tag_stat, S, p, kProfName[pi], "gpu", ds, op_bad);
      }
    }
  });
  if (op_bad)
    ++rep.ops_bad;
  else if (!rep.verbose)
    std::printf("  ok   %-14s\n", "Stat");
}

// Search 评估算子 (factor/Search): 参考 = Stat CPU 对每张候选平面的 HoldStat; Search cpu / gpu 各与它比,
//   再比 分块 (两块带前后带, merge) ≡ 整块. 层 × 两口径 × profile 全扫; 无 GPU 时只跑 cpu 两项
void check_search(int A, unsigned seed, Report &rep) {
  namespace sk = factor::search::check;
  ++rep.ops;
  bool op_bad = false;
  const int threads = misc::exec::hw();
  const Param p;
  for_segs(rep, [&](int S) {
    const int T = rows_of(rep, S);
    for (const factor::stat::Frame fr : {factor::stat::Frame::CS, factor::stat::Frame::TS}) {
      const char *fname = factor::stat::frame_name(fr);
      char tag[32];
      for (int pi = 0; pi < kProfiles; ++pi) {
        const Profile pr = static_cast<Profile>(pi);
        std::mt19937 rng(seed + 1000u * pi);
        sk::Data d;
        sk::make(d, fr, pr, T, A, S, rng);
        const factor::stat::Holds &hd = d.base.hd;
        const Tol tol = factor::stat::check::tol_of(pr);
        const sk::Split sp = sk::split_of(T, hd, S);
        sk::Ref ref[sk::kCands];
        for (int c = 0; c < sk::kCands; ++c)
          sk::reference(d, c, threads, ref[c]);
        // 一个后端的两项: 整块 vs 参考; 两块 merge vs 整块
        const auto judge = [&](const char *side, const factor::search::Result *full, const factor::search::Result *c1,
                               const factor::search::Result *c2) {
          bool bad = false;
          for (int c = 0; c < sk::kCands; ++c) {
            std::snprintf(tag, sizeof(tag), "Search.%s.c%d", fname, c);
            const Diff df = sk::compare(ref[c], full[c], hd, S, tol);
            emit(rep, tag, S, p, kProfName[pi], side, df, op_bad);
            bad = bad || !df.ok();
            factor::search::Result m = c1[c];
            m.merge(c2[c], hd.n);
            sk::Ref rf;
            sk::to_ref(full[c], hd, S, rf);
            const Diff dm = sk::compare(rf, m, hd, S, tol);
            std::snprintf(tag, sizeof(tag), "Search.%s.c%d.chunk", fname, c);
            emit(rep, tag, S, p, kProfName[pi], side, dm, op_bad);
            bad = bad || !dm.ok();
          }
          return bad;
        };
        {
          factor::search::Result full[sk::kCands], c1[sk::kCands], c2[sk::kCands];
          sk::run_cpu(d, threads, 0, T, 0, 0, full);
          sk::run_cpu(d, threads, 0, sp.e1, 0, sp.e1 - sp.T1, c1);
          sk::run_cpu(d, threads, sp.s, T - sp.s, sp.t_lo, 0, c2);
          ++rep.cases;
          if (judge("cpu", full, c1, c2))
            ++rep.stream_bad; // cpu 侧失败计在 stream 列 (本节无流式后端)
          if (rep.verbose)
            for (int c = 0; c < sk::kCands; ++c)
              for (int hi = 0; hi < hd.n; ++hi) {
                const factor::search::Score s = factor::search::score_of(full[c], hd, S, hi);
                std::printf("       Search.%s cpu S=%-3d c%d %-8s h=%-4d ls %+.5f rIC %+.4f score %+.3g\n", fname, S, c, kProfName[pi], hd.k[hi],
                            s.ls, s.ric, s.score);
              }
        }
        if (!factor::gpu::available())
          continue;
        {
          const int H = hd.n;
          std::vector<factor::stat::Label> lab(static_cast<size_t>(H));
          const float *xv[sk::kCands];
          const uint8_t *xm[sk::kCands];
          const auto run = [&](int row0, int rows, int t_lo, int t_back, factor::search::Result *out) {
            const size_t off = static_cast<size_t>(row0) * A;
            for (int i = 0; i < H; ++i)
              lab[static_cast<size_t>(i)] = factor::stat::check::label_of(d.base.lab[static_cast<size_t>(i)], d.base.eo, nullptr, off);
            for (int c = 0; c < sk::kCands; ++c)
              xv[c] = d.cand[c].v.data() + off, xm[c] = d.cand[c].m.data() + off;
            factor::gpu::run_search(sk::kCands, xv, xm, d.base.g.m.data() + off, fr, rows, A, S, row0, t_lo, t_back, hd, d.base.cap, lab.data(), out);
          };
          factor::search::Result full[sk::kCands], c1[sk::kCands], c2[sk::kCands];
          run(0, T, 0, 0, full);
          run(0, sp.e1, 0, sp.e1 - sp.T1, c1);
          run(sp.s, T - sp.s, sp.t_lo, 0, c2);
          ++rep.cases;
          if (judge("gpu", full, c1, c2))
            ++rep.gpu_bad;
        }
      }
    }
  });
  if (op_bad)
    ++rep.ops_bad;
  else if (!rep.verbose)
    std::printf("  ok   %-14s\n", "Search");
}

} // namespace

int main(int argc, char **argv) {
  std::string only;
  int A = 32;
  unsigned seed = 1;
  Report rep;
  rep.T_target = 3 * factor::kSegMax;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "-v")
      rep.verbose = true;
    else if (a == "--op" && i + 1 < argc)
      only = argv[++i];
    else if (a == "--seg" && i + 1 < argc)
      rep.seg_only = std::atoi(argv[++i]);
    else if (a == "--T" && i + 1 < argc)
      rep.T_target = std::atoi(argv[++i]);
    else if (a == "--A" && i + 1 < argc)
      A = std::atoi(argv[++i]);
    else if (a == "--seed" && i + 1 < argc)
      seed = static_cast<unsigned>(std::atoi(argv[++i]));
    else {
      std::fprintf(stderr, "用法: op_check [--op Name] [--seg S] [--T %d] [--A %d] [--seed 1] [-v]\n", rep.T_target, A);
      return 2;
    }
  }
  assert((rep.seg_only == 0 || factor::is_seg(rep.seg_only)) && "--seg 须是 kGeos 的段长之一 (255 / 49 / 17 / 9 / 5 / 1)");
  assert(rep.T_target >= 1);

  std::printf("op_check  T≈%d 行 (每层取整到整段) A=%d seed=%u seg=%s  gpu=%s\n", rep.T_target, A, seed,
              rep.seg_only ? std::to_string(rep.seg_only).c_str() : "全部", factor::gpu::available() ? "on" : "off (未编译 CUDA 后端)");

  // 分派: 表里每一行展开成一个用例组, 按 A 域列 token 粘贴选后端命名空间 (SELF → ts, ALL/GROUP → cs);
  // 缺任一后端的同名 struct → 编译错; 流式 TS kT 与表的 T 窗列不符 → 编译错.
  // 枚举 factor::T / factor::A 必须全限定: 本函数局部 int T / int A 遮蔽了不限定名
#define CK_SELF(Name, t)                                                                        \
  static_assert(factor::ts::Name::kT == factor::T::t, #Name ": 流式 kT 与 OpTable T 窗列不符"); \
  if (only.empty() || only == #Name)                                                            \
    check<factor::ts::Name, factor::cpu::ts::Name, false>(factor::expr::op_index(#Name), A, seed, rep);
#define CK_ALL(Name, t)              \
  if (only.empty() || only == #Name) \
    check<factor::cs::Name, factor::cpu::cs::Name, true>(factor::expr::op_index(#Name), A, seed, rep);
#define CK_GROUP CK_ALL
#define CK(Name, c_name, ar, t, a, kern, kdom, in, out, op, note) CK_##a(Name, t)
  OP_ALL(CK)
#undef CK
#undef CK_GROUP
#undef CK_ALL
#undef CK_SELF
  if (only.empty() || only == "Fuse") // 逐点链融合: 随机树, 融合 ≡ 不融合
    check_fuse(A, seed, rep);
  if (only.empty() || only == "Stat") // 评估算子: 不在 OpTable, 单独一节
    check_stat(A, seed, rep);
  if (only.empty() || only == "Search") // 搜索评估算子: 参考 = Stat CPU
    check_search(A, seed, rep);

  std::printf("=== %d 算子 / %d 用例: stream 失败 %d, gpu 失败 %d; 算子级失败 %d ===\n", rep.ops,
              rep.cases, rep.stream_bad, rep.gpu_bad, rep.ops_bad);
  if (rep.ops == 0) {
    std::fprintf(stderr, "op_check: %s 不在 OpTable.hpp\n", only.c_str());
    return 2;
  }
  return rep.ops_bad == 0 ? 0 : 1;
}
