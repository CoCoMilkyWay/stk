// =============================================================================
// op_check: 因子算子三方对拍 (naive 参考 / stream 流式 / gpu 向量, 全程 C++, 不经 Python)
// =============================================================================
//   用法  op_check [--op Name] [--T 720] [--A 32] [--seed 1] [-v]
//   naive 是基准 (按定义直白算, double), stream 与 gpu 各自与它比.
//   三份实现互不可见 (各自只 include Contract.hpp), 所以这套对拍不会空转.
//   分派表由 OpTable.hpp 展开: 表里有名字而某个后端没有同名 struct → 此处编译错.
//
//   比较口径 (契约): 掩码必须**逐位相等**; 有效位上 |Δ| ≤ atol + rtol·max(|a|,|b|).
//   属性列也对拍: 流式 struct::kWin / GPU struct::kStrat 与 OpTable 不符 → static_assert 错.
//   本 TU 依赖受控浮点, CMake 里整 target -fno-fast-math.
// =============================================================================

#include "factor/TS/Naive.hpp"
#include "factor/TS/Stream.hpp"

#include "factor/CS/Naive.hpp"
#include "factor/CS/Stream.hpp"

#include "factor/Contract.hpp"
#include "factor/GpuRun.hpp"
#include "factor/OpTable.hpp"

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace {

using factor::kSegLen;
using factor::Param;
using factor::Val;
using factor::Win;

// ---- 造数 ----
//   每个算子按"输入配方"拿到合适的数据: 正值型算子给正值, 分组型给整数组 id, 其余给正态.
//   profile 再叠一层边界, 每个专打一类契约分支:
//     HOLES    15% 缺失 + 某资产整段缺失 (段内 n = 0) + 某时刻**整行缺失** (截面 n = 0)
//     CONSTCOL 整列常值 (全并列退化, 全程)
//     CONSTSEG 某资产只在第 1 段常值, 其余段正常 (滑窗跨段进出常值区: 打 GPU 的 Chg 精确追踪);
//              另一资产全程常值只在一格跳变 (窗含该格才非退化)
//     HEAVY    t(2.5) 重尾 (极值 / 缩尾 / 直方图桶宽)
//     TINY     量级 ×1e−4 (契约无绝对 eps: 小量级下掩码与值都不得变)
enum class Gen { NORM,
                 POS,
                 SMALL,
                 GROUP };
enum class Profile { PLAIN,
                     HOLES,
                     CONSTCOL,
                     CONSTSEG,
                     HEAVY,
                     TINY };
constexpr int kProfiles = 6;
constexpr const char *kProfName[kProfiles] = {"plain", "holes", "const", "cseg", "heavy", "tiny"};
constexpr float kTinyScale = 1e-4f;

struct Plane {
  std::vector<float> v;
  std::vector<uint8_t> m;
  void resize(size_t n) { v.assign(n, 0.f), m.assign(n, 1); }
};

struct Data {
  Plane x, y, z;
  int T = 0, A = 0;
};

void fill(Plane &p, Gen g, Profile pr, int T, int A, std::mt19937 &rng) {
  std::normal_distribution<float> nd(0.f, 1.f);
  std::student_t_distribution<float> td(2.5f); // 重尾
  std::uniform_real_distribution<float> ud(0.f, 1.f);
  p.resize(static_cast<size_t>(T) * A);
  for (int t = 0; t < T; ++t)
    for (int a = 0; a < A; ++a) {
      const size_t i = static_cast<size_t>(t) * A + a;
      float v = 0.f;
      switch (g) {
      case Gen::NORM:
        v = pr == Profile::HEAVY ? td(rng) : nd(rng);
        break;
      case Gen::POS: // 对数正态: 保证 > 0, 给 Entropy / Gini / TopK / LogRatio
        v = std::exp(nd(rng) * 0.5f) + 0.05f;
        break;
      case Gen::SMALL: // |x| 小: 保证 1 + x > 0, 给 TsProductRoll
        v = nd(rng) * 0.05f;
        break;
      case Gen::GROUP: // 整数组 id (行业), 每个资产固定不变
        v = static_cast<float>(a % 7);
        break;
      }
      if (g != Gen::GROUP) {
        // 常值 (NORM 的 0 号资产给 0: 打 Σ = 0 的相消分支; SMALL 保持 |x| 小, 免得 Π(1+x) 溢出)
        const float c = g == Gen::SMALL ? 0.02f : g == Gen::POS ? 1.5f
                                                                : (a == 0 ? 0.f : 1.5f);
        // 常值列: 第 0/1 号资产整列常值 → 全并列退化 (spread 为假)
        if (pr == Profile::CONSTCOL && a < 2)
          v = c;
        // 常值段: 0/1 号资产只在第 1 段常值; 2 号资产全程常值, 仅 t = kSegLen + 37 一格跳变
        if (pr == Profile::CONSTSEG) {
          if (a < 2 && t / kSegLen == 1)
            v = c;
          if (a == 2)
            v = t == kSegLen + 37 ? c + 1.f : c;
        }
        if (pr == Profile::TINY)
          v *= kTinyScale;
      }
      p.v[i] = v;
      p.m[i] = 1;
      // 空洞: 15% 缺失 + 2 号资产整段缺失 (段内 n = 0) + t = kSegLen + 5 整行缺失 (截面 n = 0)
      if (pr == Profile::HOLES && (ud(rng) < 0.15f || (t / kSegLen == 1 && a == 2) || t == kSegLen + 5))
        p.v[i] = 0.f, p.m[i] = 0;
    }
}

// 输入配方: 默认全 NORM, 只列出需要特殊数据的算子
struct Recipe {
  const char *name;
  Gen x, y, z;
};
constexpr Recipe kRecipes[] = {
    {"TsLogRatio", Gen::POS, Gen::POS, Gen::NORM},
    {"TsEntropyCum", Gen::POS, Gen::NORM, Gen::NORM},
    {"TsGiniCum", Gen::POS, Gen::NORM, Gen::NORM},
    {"TsTopKCum", Gen::POS, Gen::NORM, Gen::NORM},
    {"TsHhiCum", Gen::POS, Gen::NORM, Gen::NORM},
    {"TsWMeanCum", Gen::NORM, Gen::POS, Gen::NORM}, // 权为正, 否则 Σy 抵消
    {"TsWMeanRoll", Gen::NORM, Gen::POS, Gen::NORM},
    {"TsProductRoll", Gen::SMALL, Gen::NORM, Gen::NORM},
    {"CsGroupMean", Gen::NORM, Gen::GROUP, Gen::NORM},
    {"CsGroupRank", Gen::NORM, Gen::GROUP, Gen::NORM},
    {"CsGroupResid", Gen::NORM, Gen::NORM, Gen::GROUP},
};
Recipe recipe_of(const std::string &name) {
  for (const Recipe &r : kRecipes)
    if (name == r.name)
      return r;
  return {"", Gen::NORM, Gen::NORM, Gen::NORM};
}

// 阈值型参数: d 由下面的扫描给, 这里只定 k / k2
struct KParam {
  const char *name;
  float k, k2;
};
constexpr KParam kKParams[] = {
    {"TsSignedPow", 0.5f, 0.f},
    {"TsClip", 2.f, 0.f},
    {"TsTodMask", 30.f, 90.f},
    {"TsTopKCum", 3.f, 0.f},
    {"TsPeaksCum", 1.f, 0.f},
    {"TsCountGtCum", 0.f, 0.f},
    {"TsCorrLagCum", 2.f, 0.f},
    {"TsCountGtRoll", 0.f, 0.f},
    {"TsMeanEma", 0.2f, 0.f},
    {"CsQuantile", 0.25f, 0.f},
    {"CsWinsor", 0.05f, 0.f},
    {"CsBucket", 5.f, 0.f},
    {"CsCondRank", 4.f, 0.f},
};
void set_k(const std::string &name, Param &p) {
  p.k = 1.f, p.k2 = 0.f;
  for (const KParam &q : kKParams)
    if (name == q.name)
      p.k = q.k, p.k2 = q.k2;
}

// ---- 比较 ----
struct Tol {
  double atol, rtol;
};
// GPU 走 fp32 与完全不同的并行序, 容差比 CPU 两份宽; 个别算子再单独放宽.
// TINY profile 把 atol 一起按量级缩 (否则 atol 比数据还大, 比较空转); rtol 本来就无量纲.
Tol tol_of(const std::string &name, bool gpu, Profile pr) {
  Tol t{1e-3, 1e-3};
  if (!gpu)
    t = {1e-5, 1e-4};
  else if (name == "CsNormRank") // 两侧用不同的正态分位实现 (Wichura AS241 vs normcdfinvf)
    t = {1e-3, 1e-2};
  else if (name == "TsSkewCum" || name == "TsKurtCum" || name == "TsSkewRoll" || name == "TsKurtRoll")
    t = {1e-2, 1e-2}; // 三四阶矩在 fp32 上只剩两三位有效位, 这是已知代价
  if (pr == Profile::TINY)
    t.atol *= kTinyScale;
  return t;
}

struct Diff {
  int mask_bad = 0, val_bad = 0, compared = 0;
  double worst = 0.0;
  int worst_at = -1;
  bool ok() const { return mask_bad == 0 && val_bad == 0; }
};

Diff compare(const Plane &ref, const Plane &got, Tol tol) {
  Diff d;
  for (size_t i = 0; i < ref.v.size(); ++i) {
    if ((ref.m[i] != 0) != (got.m[i] != 0)) {
      if (d.mask_bad++ == 0)
        d.worst_at = static_cast<int>(i);
      continue;
    }
    if (!ref.m[i])
      continue;
    ++d.compared;
    const double a = ref.v[i], b = got.v[i], e = std::fabs(a - b);
    if (e > d.worst)
      d.worst = e;
    if (e > tol.atol + tol.rtol * std::fmax(std::fabs(a), std::fabs(b))) {
      if (d.val_bad++ == 0 && d.worst_at < 0)
        d.worst_at = static_cast<int>(i);
    }
  }
  return d;
}

// ---- 驱动: naive (整段一次) / stream (逐点推进) / gpu ----

const float *pv(const Plane &p, bool use) { return use ? p.v.data() : nullptr; }
const uint8_t *pm(const Plane &p, bool use) { return use ? p.m.data() : nullptr; }

template <class N>
void run_naive(const Data &d, const Param &p, int ar, Plane &o) {
  o.resize(static_cast<size_t>(d.T) * d.A);
  N::run(pv(d.x, ar >= 1), pm(d.x, ar >= 1), pv(d.y, ar >= 2), pm(d.y, ar >= 2), pv(d.z, ar >= 3),
         pm(d.z, ar >= 3), o.v.data(), o.m.data(), d.T, d.A, p);
}

// TS 流式: 逐资产建一个 kernel 沿 t 推进 (与实盘同路: EXPAND 推满 kSegLen 自动归零, 不手动 reset)
template <class S, int AR, Win W>
void run_stream_ts(const Data &d, const Param &p, Plane &o) {
  o.resize(static_cast<size_t>(d.T) * d.A);
  if constexpr (W == Win::POINT) {
    for (int t = 0; t < d.T; ++t)
      for (int a = 0; a < d.A; ++a) {
        const size_t i = static_cast<size_t>(t) * d.A + a;
        Val r;
        if constexpr (AR == 0)
          r = S::apply(t % kSegLen, p);
        else if constexpr (AR == 1)
          r = S::apply(Val{d.x.v[i], d.x.m[i] != 0}, p);
        else if constexpr (AR == 2)
          r = S::apply(Val{d.x.v[i], d.x.m[i] != 0}, Val{d.y.v[i], d.y.m[i] != 0}, p);
        else
          r = S::apply(Val{d.x.v[i], d.x.m[i] != 0}, Val{d.y.v[i], d.y.m[i] != 0},
                       Val{d.z.v[i], d.z.m[i] != 0}, p);
        o.v[i] = r.v, o.m[i] = r.m;
      }
  } else {
    for (int a = 0; a < d.A; ++a) {
      S op(p);
      for (int t = 0; t < d.T; ++t) {
        const size_t i = static_cast<size_t>(t) * d.A + a;
        Val r;
        if constexpr (AR == 1)
          r = op.push(Val{d.x.v[i], d.x.m[i] != 0});
        else
          r = op.push(Val{d.x.v[i], d.x.m[i] != 0}, Val{d.y.v[i], d.y.m[i] != 0});
        o.v[i] = r.v, o.m[i] = r.m;
      }
    }
  }
}

// CS 流式: 每个时刻一个截面
template <class S>
void run_stream_cs(const Data &d, const Param &p, int ar, Plane &o) {
  o.resize(static_cast<size_t>(d.T) * d.A);
  for (int t = 0; t < d.T; ++t) {
    const size_t b = static_cast<size_t>(t) * d.A;
    S::apply(ar >= 1 ? d.x.v.data() + b : nullptr, ar >= 1 ? d.x.m.data() + b : nullptr,
             ar >= 2 ? d.y.v.data() + b : nullptr, ar >= 2 ? d.y.m.data() + b : nullptr,
             ar >= 3 ? d.z.v.data() + b : nullptr, ar >= 3 ? d.z.m.data() + b : nullptr,
             o.v.data() + b, o.m.data() + b, d.A, p);
  }
}

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
template <class S, class N, int AR, Win W, bool IS_CS>
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
    Data d;
    d.T = T, d.A = A;
    fill(d.x, rc.x, pr, T, A, rng);
    fill(d.y, rc.y, pr, T, A, rng);
    fill(d.z, rc.z, pr, T, A, rng);

    for (int di = 0; di < (sweep_d ? kNd : 1); ++di) {
      Param p;
      p.d = sweep_d ? ds[di] : 1;
      set_k(nm, p);

      Plane ref, got;
      run_naive<N>(d, p, AR, ref);
      if constexpr (IS_CS)
        run_stream_cs<S>(d, p, AR, got);
      else
        run_stream_ts<S, AR, W>(d, p, got);
      ++rep.cases;
      Diff ds_ = compare(ref, got, tol_of(nm, false, pr));
      if (!ds_.ok())
        ++rep.stream_bad;
      emit(rep, name, p, kProfName[pi], "stream", ds_, op_bad);

      if (factor::gpu::available()) {
        Plane g;
        g.resize(static_cast<size_t>(T) * A);
        auto call = IS_CS ? factor::gpu::run_cs : factor::gpu::run_ts;
        call(name, pv(d.x, AR >= 1), pm(d.x, AR >= 1), pv(d.y, AR >= 2), pm(d.y, AR >= 2),
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

  // 分派: 表里每一行展开成一个用例组; 缺任一后端的同名 struct → 编译错; 流式 kWin 与表不符 → 编译错
#define CK_TS(Name, ar, win, prm, gpu, doc)                                                   \
  static_assert(factor::ts::Name::kWin == Win::win, #Name ": 流式 kWin 与 OpTable 窗列不符"); \
  if (only.empty() || only == #Name)                                                          \
    check<factor::ts::Name, factor::naive::ts::Name, ar, Win::win, false>(#Name, prm, T, A, seed, rep);
#define CK_CS(Name, ar, prm, gpu, doc) \
  if (only.empty() || only == #Name)   \
    check<factor::cs::Name, factor::naive::cs::Name, ar, Win::POINT, true>(#Name, prm, T, A, seed, rep);
  OP_TS(CK_TS)
  OP_CS(CK_CS)
#undef CK_TS
#undef CK_CS

  std::printf("=== %d 算子 / %d 用例: stream 失败 %d, gpu 失败 %d; 算子级失败 %d ===\n", rep.ops,
              rep.cases, rep.stream_bad, rep.gpu_bad, rep.ops_bad);
  if (rep.ops == 0) {
    std::fprintf(stderr, "op_check: %s 不在 OpTable.hpp\n", only.c_str());
    return 2;
  }
  return rep.ops_bad == 0 ? 0 : 1;
}
