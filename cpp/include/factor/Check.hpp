#pragma once

// =============================================================================
// 因子算子对拍的公共件: 造数 / 输入配方 / 容差 / 比较 / 三后端驱动
// =============================================================================
//   两个消费者共用同一份 (口径必须一致, 否则 GUI 里"过"而 op_check 里"挂"):
//     src/factor/op_check.cpp                     命令行三方对拍 (profile × d 全扫)
//     src/gui/task_factors/services/OperatorsService  GUI Operators 表 (单张量, 逐算子对拍 + 计时)
//   本头只依赖 Contract.hpp; 驱动是模板, 具体的 Stream / Cpu struct 由消费者 include 后实例化.
//   Arena (每线程暂存) 也在此, EvalCpu 共用.
//   依赖受控浮点: 消费者 TU 必须编进 -fno-fast-math (CMake PRECISE_MATH_FLAG).
// =============================================================================

#include "factor/Contract.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace factor::check {

// ---- 造数 ----
//   每个算子按"输入配方"拿到合适的数据: 配方由 OpTable in 列的 Dom 逐元推出 (gen_of), 不按算子名列.
//   profile 再叠一层边界, 每个专打一类契约分支 (退化分支现在产中性值而非无效, 对拍照样逐位比掩码 + 比值):
//     HOLES    15% 缺失 + 某资产整段缺失 (段内 n = 0 → 中性值) + 某时刻**整行缺失** (截面 n = 0 → 中性值)
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
inline constexpr int kProfiles = 6;
inline constexpr const char *kProfName[kProfiles] = {"plain", "holes", "const", "cseg", "heavy", "tiny"};
inline constexpr float kTinyScale = 1e-4f;

struct Plane {
  std::vector<float> v;
  std::vector<uint8_t> m;
  void resize(size_t n) { v.assign(n, 0.f), m.assign(n, 1); }
  // 只保证尺寸, 不清 (输出缓冲跨算子复用: 契约是后端写满每格, 清零 + 首次触页不该混进算子计时)
  void ensure(size_t n) {
    if (v.size() != n)
      resize(n);
  }
};

// 每线程暂存 (算子 scratch 声明的字节数): 只增不缩, 返回 64B 对齐首址 (Bump 的对齐以它为基)
struct Arena {
  std::vector<char> buf;
  void *get(size_t n) {
    if (buf.size() < n + 64)
      buf.resize(n + 64);
    const uintptr_t p = reinterpret_cast<uintptr_t>(buf.data());
    return reinterpret_cast<void *>((p + 63) & ~static_cast<uintptr_t>(63));
  }
};

// 输入张量: 非拥有 (三张平面由调用方持有 —— op_check 每 profile 造一次扫 d, GUI 一轮造一次全表复用);
// 元数不到的槽位为空. g = 截面池掩码 (只用 m; CS 算子与 Stat 必填, TS 不看; 契约【截面池 g】)
struct Data {
  const Plane *x = nullptr, *y = nullptr, *z = nullptr, *g = nullptr;
  int T = 0, A = 0;
};

namespace detail {
// 一段 [t0, t1) 的造数 (fill 的段级内核): 逐点逻辑与 profile 边界全在此; S = 段长 (profile 边界按段定位)
inline void fill_seg(Plane &p, Gen g, Profile pr, int t0, int t1, int A, int S, std::mt19937 &rng) {
  std::normal_distribution<float> nd(0.f, 1.f);
  std::student_t_distribution<float> td(2.5f); // 重尾
  std::uniform_real_distribution<float> ud(0.f, 1.f);
  for (int t = t0; t < t1; ++t)
    for (int a = 0; a < A; ++a) {
      const size_t i = static_cast<size_t>(t) * A + a;
      float v = 0.f;
      switch (g) {
      case Gen::NORM:
        v = pr == Profile::HEAVY ? td(rng) : nd(rng);
        break;
      case Gen::POS: // 对数正态: 保证 > 0, 给 Dom POS / NONNEG
        v = std::exp(nd(rng) * 0.5f) + 0.05f;
        break;
      case Gen::SMALL: // |x| 小: 保证 1 + x > 0, 给 Dom RET
        v = nd(rng) * 0.05f;
        break;
      case Gen::GROUP: // 整数组 id (行业), 给 Dom INT; 每个资产固定不变; a % 11 == 5 的给 −1 (未知行业 → 以全池为组)
        v = a % 11 == 5 ? -1.f : static_cast<float>(a % 7);
        break;
      }
      if (g != Gen::GROUP) {
        // 常值 (0 号资产给 0: NORM 打 Σ = 0 的相消分支, POS 打 LogRatio 的非正饱和 / Entropy 无正样本; SMALL 保持 |x| 小, 免得 Π(1+x) 溢出)
        const float c = g == Gen::SMALL ? 0.02f : (a == 0 ? 0.f : 1.5f);
        // 常值列: 第 0/1 号资产整列常值 → 全并列退化 (spread 为假)
        if (pr == Profile::CONSTCOL && a < 2)
          v = c;
        // 常值段: 0/1 号资产只在第 1 段常值; 2 号资产全程常值, 仅 t = S + 37 一格跳变
        if (pr == Profile::CONSTSEG) {
          if (a < 2 && t / S == 1)
            v = c;
          if (a == 2)
            v = t == S + 37 ? c + 1.f : c;
        }
        if (pr == Profile::TINY)
          v *= kTinyScale;
      }
      p.v[i] = v;
      p.m[i] = 1;
      // 空洞: 15% 缺失 + 2 号资产整段缺失 (段内 n = 0) + t = S + 5 整行缺失 (截面 n = 0)
      if (pr == Profile::HOLES && (ud(rng) < 0.15f || (t / S == 1 && a == 2) || t == S + 5))
        p.v[i] = 0.f, p.m[i] = 0;
    }
}
} // namespace detail

// 并行按段造数: 每段从主 rng 派生一个种子再独立抽样, 结果只由 (seed, 段序) 决定, 与线程数无关.
// 24000×1000 单线程 mt19937 + normal_distribution 要 ~1 s, 这是对拍里唯一算不上算子的大头. S = 段长 (Contract【层几何】)
inline void fill(Plane &p, Gen g, Profile pr, int T, int A, int S, std::mt19937 &rng) {
  assert(is_seg(S) && T % S == 0 && T > S + 37 && "T 必须是整段数 (造数按段并行, 段界也是 EXPAND 的前提) 且盖住 profile 边界");
  p.resize(static_cast<size_t>(T) * A);
  const int nseg = T / S;
  std::vector<uint32_t> seeds(static_cast<size_t>(nseg));
  for (uint32_t &s : seeds)
    s = rng();
  auto seg = [&](int s) {
    std::mt19937 r(seeds[static_cast<size_t>(s)]);
    detail::fill_seg(p, g, pr, s * S, (s + 1) * S, A, S, r);
  };
  misc::exec::ForkJoin ex(misc::exec::fit(static_cast<size_t>(nseg)));
  ex.tasks(nseg, [&](int s, int) { seg(s); });
}

// 截面池掩码: 每 (段, 资产) 一个常量 (真实池按日增删), 约 60% 在池; v 恒 0 不用.
//   HOLES 再叠: 3 号资产全程池外 (只做受体); t = S + 9 整行空池 (池内 n = 0 → 广播中性值 / 相对型用空池统计量);
//              第 1 段里组 id 6 (Gen::GROUP 的 a % 7 == 6) 整组池外 → 打 GROUP 族的全池回退
inline void fill_gate(Plane &p, Profile pr, int T, int A, int S, std::mt19937 &rng) {
  assert(is_seg(S) && T % S == 0 && T > S + 9);
  p.resize(static_cast<size_t>(T) * A);
  std::fill(p.v.begin(), p.v.end(), 0.f);
  std::uniform_real_distribution<float> ud(0.f, 1.f);
  const int nseg = T / S;
  std::vector<uint8_t> in(static_cast<size_t>(nseg) * A);
  for (int s = 0; s < nseg; ++s)
    for (int a = 0; a < A; ++a) {
      bool g = ud(rng) < 0.6f;
      if (pr == Profile::HOLES && (a == 3 || (s == 1 && a % 7 == 6)))
        g = false;
      in[static_cast<size_t>(s) * A + a] = g;
    }
  for (int t = 0; t < T; ++t) {
    const uint8_t *row = in.data() + static_cast<size_t>(t / S) * A;
    uint8_t *o = p.m.data() + static_cast<size_t>(t) * A;
    if (pr == Profile::HOLES && t == S + 9)
      std::fill(o, o + A, 0);
    else
      std::copy(row, row + A, o);
  }
}

// 输入配方: 逐元由 OpTable in 列的 Dom 推 (NONNEG 给正值: 权 / 量为正, 否则 Σy 抵消、x + y 近 0)
inline Gen gen_of(Dom d) {
  switch (d) {
  case Dom::REAL:
    return Gen::NORM;
  case Dom::NONNEG:
  case Dom::POS:
    return Gen::POS;
  case Dom::RET:
    return Gen::SMALL;
  case Dom::INT:
    return Gen::GROUP;
  case Dom::UNIT:
  case Dom::SIGNED:
  case Dom::BIN:
  case Dom::SIGN3:
  case Dom::BCAST:
    break;
  }
  assert(false && "该 Dom 作输入尚无造数: 在 gen_of 补一支");
  return Gen::NORM;
}
struct Recipe {
  Gen x, y, z;
};
inline Recipe recipe_of(const Dom (&in)[3]) { return {gen_of(in[0]), gen_of(in[1]), gen_of(in[2])}; }

// 窗长参数: 每算子一个"典型用法"的默认 d (期 = 行; Operators 表在 L_min 上即分钟), 给 GUI Operators 表用;
// op_check 不吃这份 (它全扫 d = {1, 5, 20, S, S + 60} 打边界)
struct DParam {
  const char *e_name;
  int d;
};
inline constexpr DParam kDParams[] = {
    {"TsDelayRoll", 5}, // 短滞后差分
    {"TsDeltaRoll", 5},
    {"TsSkewRoll", 60}, // 高阶矩 / 序统计 / 二元统计: 样本量要足
    {"TsKurtRoll", 60},
    {"TsRankRoll", 60},
    {"TsQuantileRoll", 60},
    {"TsMaxRoll", 60},
    {"TsMinRoll", 60},
    {"TsArgMaxRoll", 60},
    {"TsArgMinRoll", 60},
    {"TsCovRoll", 60},
    {"TsCorrRoll", 60},
    {"TsBetaRoll", 60},
    {"TsResidRoll", 60},
};
inline int default_d(const std::string &name) {
  for (const DParam &q : kDParams)
    if (name == q.e_name)
      return q.d;
  return 30; // 其余 ROLL 族 (均值/和/斜率/计数等): 半小时
}

// 阈值型参数: 这里只定 k / k2. TsTodMask 的段内区间按 L_min (S = kSegMax) 给, set_k 按段长 S 等比缩放 (保持 0 ≤ k < k2 ≤ S)
struct KParam {
  const char *e_name;
  float k, k2;
};
inline constexpr KParam kKParams[] = {
    {"TsClip", 2.f, 0.f},
    {"TsGt", 0.f, 0.f}, // 正态数据下约一半过阈
    {"TsTodMask", 30.f, 90.f},
    {"TsQuantileRoll", 0.25f, 0.f}, // 非 0.5: 打一般分位的取整边界
    {"TsMeanEma", 0.2f, 0.f},
    {"CsQuantile", 0.25f, 0.f},
    {"CsWinsor", 0.05f, 0.f},
    {"CsBucket", 5.f, 0.f},
};
inline void set_k(const std::string &name, Param &p, int S) {
  assert(is_seg(S));
  p.k = 1.f, p.k2 = 0.f;
  for (const KParam &q : kKParams)
    if (name == q.e_name)
      p.k = q.k, p.k2 = q.k2;
  if (name == "TsTodMask") {
    const int k = static_cast<int>(p.k) * S / kSegMax, k2 = static_cast<int>(p.k2) * S / kSegMax;
    p.k = static_cast<float>(k), p.k2 = static_cast<float>(k2 > k ? k2 : k + 1);
  }
}

// ---- 比较 ----
struct Tol {
  double atol, rtol;
};
// GPU 走 fp32 与完全不同的并行序, 容差比 cpu↔stream 宽; 个别算子再单独放宽.
// TINY profile 把 atol 一起按量级缩 (否则 atol 比数据还大, 比较空转); rtol 本来就无量纲.
inline Tol tol_of(const std::string &name, bool gpu, Profile pr) {
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

inline Diff compare(const Plane &ref, const Plane &got, Tol tol) {
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

// ---- 驱动: cpu (整张量一次) / stream (逐行推进) / gpu 由消费者经 GpuRun.hpp 调 ----
//   ar = 元数 (够到的槽位必填); S = 段长 (Contract【层几何】; CS 不看)

// 整平面的调用形状 (Contract【调用形状】); o 先 ensure
inline Call call_of(const Data &d, int ar, const Param &p, int S, Plane &o) {
  o.ensure(static_cast<size_t>(d.T) * d.A);
  Call c;
  const Plane *const pl[3] = {d.x, d.y, d.z};
  In *const slot[3] = {&c.x, &c.y, &c.z};
  for (int a = 0; a < ar; ++a) {
    assert(pl[a]);
    *slot[a] = {pl[a]->v.data(), pl[a]->m.data()};
  }
  c.g = d.g ? d.g->m.data() : nullptr;
  c.o = {o.v.data(), o.m.data()};
  c.T = d.T, c.A = d.A, c.geo = geo_of(S), c.p = p;
  return c;
}

// CPU: 单线程一次调满 (TS 全部列 / CS 全部行)
template <class C>
void run_cpu_ts(const Data &d, const Param &p, int ar, int S, Plane &o) {
  thread_local Arena ws;
  const Call c = call_of(d, ar, p, S, o);
  C::run(c, Range{0, d.A}, ws.get(C::scratch(c)));
}
template <class C>
void run_cpu_cs(const Data &d, const Param &p, int ar, int S, Plane &o) {
  thread_local Arena ws;
  const Call c = call_of(d, ar, p, S, o);
  assert(c.g);
  C::run(c, Range{0, d.T}, ws.get(C::scratch(c)));
}

// TS 流式: 每资产一个算子对象, 逐行把全部资产推一步 (与实盘同路: 段内位置 t % S, EXPAND 在 t_seg = 0 自动归零)
template <class Op>
void run_stream_ts(const Data &d, const Param &p, int ar, int S, Plane &o) {
  o.ensure(static_cast<size_t>(d.T) * d.A);
  const Plane *const pl[3] = {d.x, d.y, d.z};
  for (int a = 0; a < ar; ++a)
    assert(pl[a]);
  const auto val = [&](int s, size_t i) { return s < ar ? Val{pl[s]->v[i], pl[s]->m[i] != 0} : Val{}; };
  std::vector<Op> ops(static_cast<size_t>(d.A), Op(p));
  for (int t = 0; t < d.T; ++t)
    for (int a = 0; a < d.A; ++a) {
      const size_t i = static_cast<size_t>(t) * d.A + a;
      const Val r = ops[static_cast<size_t>(a)].step(val(0, i), val(1, i), val(2, i), t % S);
      o.v[i] = r.v, o.m[i] = r.m;
    }
}

// CS 流式: 一个算子对象逐行推全部截面
template <class Op>
void run_stream_cs(const Data &d, const Param &p, int ar, int S, Plane &o) {
  const Call c = call_of(d, ar, p, S, o);
  assert(c.g);
  Op op;
  for (int t = 0; t < d.T; ++t)
    op.step(c, t);
}

} // namespace factor::check
