// =============================================================================
// op_check: 因子算子三方对拍 (stream 实盘流式 / cpu 挖掘向量 / gpu 挖掘 CUDA, 全程 C++, 不经 Python)
// =============================================================================
//   用法  op_check [--op Name] [--T 720] [--A 32] [--seed 1] [-v]
//   stream 是语义锚 (实盘路径); cpu 整张量一次算出参考平面, stream 与 gpu 各自与它比
//   (cpu↔stream 紧容差互证, gpu↔cpu 松容差 —— 传递闭包覆盖三方).
//   三份实现互不可见 (各自只 include Contract.hpp), 所以这套对拍不会空转.
//   分派表由 OpTable.hpp 展开: 表里有名字而某个后端没有同名 struct → 此处编译错.
//
//   比较口径 (契约): 掩码必须**逐位相等**; 有效位上 |Δ| ≤ atol + rtol·max(|a|,|b|).
//   属性列也对拍: 流式 TS struct::kT 与 OpTable 的 T 窗列不符 → static_assert 错.
//   造数 / 配方 / 容差 / 比较 / 驱动在 factor/Check.hpp (与 GUI Factors→Operators 页共用同一口径);
//   本文件只剩 profile × d 全扫 + 汇总. 本 TU 依赖受控浮点, CMake 里整 target -fno-fast-math.
//   末尾另有一节 Stat (因子评估算子, factor/Stat; 不在 OpTable, 无流式后端, cpu ↔ gpu 两方 match): --op Stat 单跑.
//   再一节 Search (参数搜索评估算子, factor/Search; 参考 = Stat CPU, Search cpu / gpu 各与它比 + 分块 ≡ 整块): --op Search 单跑.
// =============================================================================

#include "factor/TS/Cpu.hpp"    // IWYU pragma: keep
#include "factor/TS/Stream.hpp" // IWYU pragma: keep

#include "factor/CS/Cpu.hpp"    // IWYU pragma: keep
#include "factor/CS/Stream.hpp" // IWYU pragma: keep

#include "factor/Check.hpp"
#include "factor/Contract.hpp"
#include "factor/GpuRun.hpp"
#include "factor/OpTable.hpp"
#include "factor/Search/Check.hpp"
#include "factor/Stat/Check.hpp"

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace {

using namespace factor::check;
using factor::kSegLen;
using factor::Param;

// ---- 汇总 ----
struct Report {
  int cases = 0, stream_bad = 0, gpu_bad = 0;
  int ops = 0, ops_bad = 0;
  bool verbose = false;
};

void emit(Report &rep, const char *name, const Param &p, const char *prof, const char *side,
          const Diff &d, bool &op_bad) {
  if (d.ok()) {
    if (rep.verbose)
      std::printf("  ok   %-14s %-6s d=%-3d k=%-5g %-8s 比较 %6d 点, max|Δ| = %.3g\n", name, side, p.d,
                  p.k, prof, d.compared, d.worst);
    return;
  }
  op_bad = true;
  std::printf("  FAIL %-14s %-6s d=%-3d k=%-5g %-8s 掩码不符 %d, 超差 %d / %d, max|Δ| = %.3g, 首处 idx %d\n",
              name, side, p.d, p.k, prof, d.mask_bad, d.val_bad, d.compared, d.worst, d.worst_at);
}

// 一个算子的全部用例: profile × d 扫描
//   d 扫 {1, 5, 20, 240, 300}: 240 = 恰一段 (ROLL 窗界与段界重合), 300 > 段 (窗跨段, GPU 块界不对齐段界)
template <class S, class C, int AR, factor::T W, bool IS_CS>
void check(const char *name, const char *params, int T, int A, unsigned seed, Report &rep) {
  const std::string nm = name;
  const Recipe rc = recipe_of(nm);
  const bool sweep_d = std::strchr(params, 'd') != nullptr;
  const int ds[] = {1, 5, 20, kSegLen, kSegLen + 60};
  constexpr int kNd = static_cast<int>(sizeof(ds) / sizeof(ds[0]));

  ++rep.ops;
  bool op_bad = false;
  for (int pi = 0; pi < kProfiles; ++pi) {
    const Profile pr = static_cast<Profile>(pi);
    std::mt19937 rng(seed + 1000u * pi);
    Plane x, y, z, gate; // 元数够到的槽位才造; CS 多一张池掩码
    Data d;
    d.T = T, d.A = A;
    if (AR >= 1)
      fill(x, rc.x, pr, T, A, rng), d.x = &x;
    if (AR >= 2)
      fill(y, rc.y, pr, T, A, rng), d.y = &y;
    if (AR >= 3)
      fill(z, rc.z, pr, T, A, rng), d.z = &z;
    if constexpr (IS_CS)
      fill_gate(gate, pr, T, A, rng), d.g = &gate;

    for (int di = 0; di < (sweep_d ? kNd : 1); ++di) {
      Param p;
      p.d = sweep_d ? ds[di] : 1;
      set_k(nm, p);

      Plane ref, got;
      if constexpr (IS_CS) {
        run_cpu_cs<C>(d, p, AR, ref);
        run_stream_cs<S>(d, p, AR, got);
      } else {
        run_cpu<C>(d, p, AR, ref);
        run_stream_ts<S, AR, W>(d, p, got);
      }
      ++rep.cases;
      Diff ds_ = compare(ref, got, tol_of(nm, false, pr));
      if (!ds_.ok())
        ++rep.stream_bad;
      emit(rep, name, p, kProfName[pi], "stream", ds_, op_bad);

      if (factor::gpu::available()) {
        Plane g;
        g.resize(static_cast<size_t>(T) * A);
        // 一次性宿主指针版本 (内部临时会话); run_ts / run_cs 有会话重载, 不能取三目
        if constexpr (IS_CS)
          factor::gpu::run_cs(name, pv(d.x, AR >= 1), pm(d.x, AR >= 1), pv(d.y, AR >= 2), pm(d.y, AR >= 2),
                              pv(d.z, AR >= 3), pm(d.z, AR >= 3), gate.m.data(), g.v.data(), g.m.data(), T, A, p);
        else
          factor::gpu::run_ts(name, pv(d.x, AR >= 1), pm(d.x, AR >= 1), pv(d.y, AR >= 2), pm(d.y, AR >= 2),
                              pv(d.z, AR >= 3), pm(d.z, AR >= 3), g.v.data(), g.m.data(), T, A, p);
        Diff dg = compare(ref, g, tol_of(nm, true, pr));
        if (!dg.ok())
          ++rep.gpu_bad;
        emit(rep, name, p, kProfName[pi], "gpu", dg, op_bad);
      }
    }
  }
  if (op_bad)
    ++rep.ops_bad;
  else if (!rep.verbose)
    std::printf("  ok   %-14s\n", name);
}

// Stat 评估算子 (不在 OpTable: 输出是 rows[H][T] + 每持有期标量, 无流式后端): cpu ↔ gpu 两方 match 即过.
//   两口径 (CS / TS) × profile 全扫 (无 d); 无 GPU 时只跑 cpu 并打印二级汇总 (-v), 不算失败
void check_stat(int T, int A, unsigned seed, Report &rep) {
  namespace sc = factor::stat::check; // 与 factor::check 同名的 Data / Tol 等, 全限定
  ++rep.ops;
  bool op_bad = false;
  const int threads = sc::cpu_threads();
  for (const factor::stat::Frame fr : {factor::stat::Frame::CS, factor::stat::Frame::TS}) {
    const char *fname = factor::stat::frame_name(fr);
    char tag_rows[32], tag_stat[32];
    std::snprintf(tag_rows, sizeof(tag_rows), "Stat.%s.rows", fname);
    std::snprintf(tag_stat, sizeof(tag_stat), "Stat.%s.stat", fname);
    for (int pi = 0; pi < kProfiles; ++pi) {
      const Profile pr = static_cast<Profile>(pi);
      std::mt19937 rng(seed + 1000u * pi);
      sc::Data d;
      sc::make(d, fr, pr, T, A, rng);
      sc::Result cpu;
      sc::run_cpu(d, cpu, threads);
      if (rep.verbose)
        for (const factor::stat::HoldStat &s : cpu.stat)
          std::printf("       Stat.%s cpu    h=%-3d %-8s n=%d/%d  rIC %+.4f sd %.4f IR %+.3f t %+.2f pos %.2f  LS %+.5f t %+.2f pos %.2f "
                      "Sharpe %+.2f beta %+.3f  mono %+.3f  rAC %+.3f sd %.4f IR %+.3f t %+.2f pos %.2f\n",
                      fname, s.hold, kProfName[pi], s.n, s.n_rac, s.ric.mean, s.ric.sd, s.ric.ir, s.ric.t, s.ric.pos, s.ls_mean, s.ls_t, s.ls_pos,
                      s.sharpe, s.beta, s.mono, s.rac.mean, s.rac.sd, s.rac.ir, s.rac.t, s.rac.pos);
      if (!factor::gpu::available())
        continue;
      sc::Result gpu;
      gpu.rows.assign(cpu.rows.size(), factor::stat::Row{});
      std::vector<factor::stat::Label> lab(static_cast<size_t>(d.hd.n));
      for (int i = 0; i < d.hd.n; ++i)
        lab[static_cast<size_t>(i)] = {d.lab[static_cast<size_t>(i)].lv.data(), d.lab[static_cast<size_t>(i)].sv.data(),
                                       d.lab[static_cast<size_t>(i)].m.data()};
      factor::gpu::run_stat(d.x.v.data(), d.x.m.data(), d.g.m.data(), fr, T, A, d.hd, lab.data(), gpu.rows.data(), &gpu.prep_ms,
                            &gpu.eval_ms);
      sc::summarize_all(gpu, d);
      const Tol tol = sc::tol_of(pr);
      const Diff dr = sc::compare_rows(cpu.rows.data(), gpu.rows.data(), cpu.rows.size(), tol);
      const Diff ds = sc::compare_stat(cpu.stat.data(), gpu.stat.data(), d.hd.n, tol);
      ++rep.cases;
      if (!dr.ok() || !ds.ok())
        ++rep.gpu_bad;
      Param p;
      emit(rep, tag_rows, p, kProfName[pi], "gpu", dr, op_bad);
      emit(rep, tag_stat, p, kProfName[pi], "gpu", ds, op_bad);
    }
  }
  if (op_bad)
    ++rep.ops_bad;
  else if (!rep.verbose)
    std::printf("  ok   %-14s\n", "Stat");
}

// Search 评估算子 (factor/Search): 参考 = Stat CPU 对每张候选平面的 HoldStat + 朴素成本池化; Search cpu / gpu 各与它比,
//   再比 分块 (两块带 carry, merge) ≡ 整块. 两口径 × profile 全扫; 无 GPU 时只跑 cpu 两项
void check_search(int T, int A, unsigned seed, Report &rep) {
  namespace sk = factor::search::check;
  ++rep.ops;
  bool op_bad = false;
  const int threads = factor::stat::check::cpu_threads();
  const Param p;
  for (const factor::stat::Frame fr : {factor::stat::Frame::CS, factor::stat::Frame::TS}) {
    const char *fname = factor::stat::frame_name(fr);
    char tag[32];
    for (int pi = 0; pi < kProfiles; ++pi) {
      const Profile pr = static_cast<Profile>(pi);
      std::mt19937 rng(seed + 1000u * pi);
      sk::Data d;
      sk::make(d, fr, pr, T, A, rng);
      const factor::stat::Holds &hd = d.base.hd;
      const Tol tol = factor::stat::check::tol_of(pr);
      const sk::Split sp = sk::split_of(T, hd);
      sk::Ref ref[sk::kCands];
      for (int c = 0; c < sk::kCands; ++c)
        sk::reference(d, c, threads, ref[c]);
      // 一个后端的两项: 整块 vs 参考; 两块 merge vs 整块
      const auto judge = [&](const char *side, const factor::search::Result *full, const factor::search::Result *c1,
                             const factor::search::Result *c2) {
        bool bad = false;
        for (int c = 0; c < sk::kCands; ++c) {
          std::snprintf(tag, sizeof(tag), "Search.%s.c%d", fname, c);
          const Diff df = sk::compare(ref[c], full[c], hd, tol);
          emit(rep, tag, p, kProfName[pi], side, df, op_bad);
          bad = bad || !df.ok();
          factor::search::Result m = c1[c];
          m.merge(c2[c], hd.n);
          sk::Ref rf;
          sk::to_ref(full[c], hd, rf);
          const Diff dm = sk::compare(rf, m, hd, tol);
          std::snprintf(tag, sizeof(tag), "Search.%s.c%d.chunk", fname, c);
          emit(rep, tag, p, kProfName[pi], side, dm, op_bad);
          bad = bad || !dm.ok();
        }
        return bad;
      };
      {
        factor::search::Result full[sk::kCands], c1[sk::kCands], c2[sk::kCands];
        sk::run_cpu(d, threads, 0, T, 0, full);
        sk::run_cpu(d, threads, 0, sp.T1, 0, c1);
        sk::run_cpu(d, threads, sp.s, T - sp.s, sp.t_lo, c2);
        ++rep.cases;
        if (judge("cpu", full, c1, c2))
          ++rep.stream_bad; // cpu 侧失败计在 stream 列 (本节无流式后端)
        if (rep.verbose)
          for (int c = 0; c < sk::kCands; ++c) {
            const factor::search::Score s = factor::search::score_of(full[c], hd, factor::search::Objective{.sell_impact = 0.0005});
            const factor::search::CostAcc &ca = full[c].cost[s.best >= 0 ? s.best : 0];
            std::printf("       Search.%s cpu c%d %-8s best h=%-4d ls_net/yr %+.4f util %.2f rIC %+.4f score %+.3g  turn hi %.3f lo %.3f  buy %.5f (%lld) sell %.5f (%lld)\n",
                        fname, c, kProfName[pi], s.best >= 0 ? hd.h[s.best] : -1, s.ls_net, s.util, s.ric, s.score, ca.turn_hi(), ca.turn_lo(), ca.buy_mean(),
                        ca.e_hi, ca.sell_mean(), ca.e_lo);
          }
      }
      if (!factor::gpu::available())
        continue;
      {
        const int H = hd.n;
        std::vector<factor::stat::Label> lab(static_cast<size_t>(H));
        const float *xv[sk::kCands];
        const uint8_t *xm[sk::kCands];
        const auto run = [&](int row0, int rows, int t_lo, factor::search::Result *out) {
          const size_t off = static_cast<size_t>(row0) * A;
          for (int i = 0; i < H; ++i) {
            const factor::stat::check::LabelHost &L = d.base.lab[static_cast<size_t>(i)];
            lab[static_cast<size_t>(i)] = {L.lv.data() + off, L.sv.data() + off, L.m.data() + off};
          }
          for (int c = 0; c < sk::kCands; ++c)
            xv[c] = d.cand[c].v.data() + off, xm[c] = d.cand[c].m.data() + off;
          const factor::search::Cost cost{d.buy.data() + off, d.sell.data() + off, d.cm.data() + off};
          factor::gpu::run_search(sk::kCands, xv, xm, d.base.g.m.data() + off, fr, rows, A, row0, t_lo, hd, lab.data(), cost, out);
        };
        factor::search::Result full[sk::kCands], c1[sk::kCands], c2[sk::kCands];
        run(0, T, 0, full);
        run(0, sp.T1, 0, c1);
        run(sp.s, T - sp.s, sp.t_lo, c2);
        ++rep.cases;
        if (judge("gpu", full, c1, c2))
          ++rep.gpu_bad;
      }
    }
  }
  if (op_bad)
    ++rep.ops_bad;
  else if (!rep.verbose)
    std::printf("  ok   %-14s\n", "Search");
}

} // namespace

int main(int argc, char **argv) {
  std::string only;
  int T = 3 * kSegLen, A = 32;
  unsigned seed = 1;
  Report rep;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "-v")
      rep.verbose = true;
    else if (a == "--op" && i + 1 < argc)
      only = argv[++i];
    else if (a == "--T" && i + 1 < argc)
      T = std::atoi(argv[++i]);
    else if (a == "--A" && i + 1 < argc)
      A = std::atoi(argv[++i]);
    else if (a == "--seed" && i + 1 < argc)
      seed = static_cast<unsigned>(std::atoi(argv[++i]));
    else {
      std::fprintf(stderr, "用法: op_check [--op Name] [--T %d] [--A %d] [--seed 1] [-v]\n", T, A);
      return 2;
    }
  }
  assert(T % kSegLen == 0 && "T 必须是整段数 (段界对齐是 EXPAND 的前提)");

  std::printf("op_check  T=%d (%d 段) A=%d seed=%u  gpu=%s\n", T, T / kSegLen, A, seed,
              factor::gpu::available() ? "on" : "off (未编译 CUDA 后端)");

  // 分派: 表里每一行展开成一个用例组, 按 A 域列 token 粘贴选后端命名空间 (SELF → ts, ALL/GROUP → cs);
  // 缺任一后端的同名 struct → 编译错; 流式 TS kT 与表的 T 窗列不符 → 编译错.
  // 枚举 factor::T / factor::A 必须全限定: 本函数局部 int T / int A 遮蔽了不限定名
#define CK_SELF(Name, ar, t, prm)                                                               \
  static_assert(factor::ts::Name::kT == factor::T::t, #Name ": 流式 kT 与 OpTable T 窗列不符"); \
  if (only.empty() || only == #Name)                                                            \
    check<factor::ts::Name, factor::cpu::ts::Name, ar, factor::T::t, false>(#Name, prm, T, A, seed, rep);
#define CK_ALL(Name, ar, t, prm)     \
  if (only.empty() || only == #Name) \
    check<factor::cs::Name, factor::cpu::cs::Name, ar, factor::T::t, true>(#Name, prm, T, A, seed, rep);
#define CK_GROUP CK_ALL
#define CK(Name, c_name, ar, t, a, kern, kdom, in, out, op, note) CK_##a(Name, ar, t, factor::params_str(factor::T::t, factor::KDom::kdom))
  OP_ALL(CK)
#undef CK
#undef CK_GROUP
#undef CK_ALL
#undef CK_SELF
  if (only.empty() || only == "Stat") // 评估算子: 不在 OpTable, 单独一节
    check_stat(T, A, seed, rep);
  if (only.empty() || only == "Search") // 搜索评估算子: 参考 = Stat CPU
    check_search(T, A, seed, rep);

  std::printf("=== %d 算子 / %d 用例: stream 失败 %d, gpu 失败 %d; 算子级失败 %d ===\n", rep.ops,
              rep.cases, rep.stream_bad, rep.gpu_bad, rep.ops_bad);
  if (rep.ops == 0) {
    std::fprintf(stderr, "op_check: %s 不在 OpTable.hpp\n", only.c_str());
    return 2;
  }
  return rep.ops_bad == 0 ? 0 : 1;
}
