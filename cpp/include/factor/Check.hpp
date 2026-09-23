#pragma once

// =============================================================================
// 因子算子对拍的公共件: 造数 / 输入配方 / 容差 / 比较 / 三后端驱动
// =============================================================================
//   两个消费者共用同一份 (口径必须一致, 否则 GUI 里"过"而 op_check 里"挂"):
//     src/factor/op_check.cpp                     命令行三方对拍 (profile × d 全扫)
//     src/gui/task_factors/services/OperatorsService  GUI Operators 表 (单张量, 逐算子对拍 + 计时)
//   本头只依赖 Contract.hpp; 驱动是模板, 具体的 Stream / Cpu struct 由消费者 include 后实例化.
//   依赖受控浮点: 消费者 TU 必须编进 -fno-fast-math (CMake PRECISE_MATH_FLAG).
// =============================================================================

#include "factor/Contract.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace factor::check {

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

// 输入张量: 非拥有 (三张平面由调用方持有 —— op_check 每 profile 造一次扫 d, GUI 一轮造一次全表复用);
// 元数不到的槽位为空
struct Data {
  const Plane *x = nullptr, *y = nullptr, *z = nullptr;
  int T = 0, A = 0;
};

namespace detail {
// 一段 [t0, t1) 的造数 (fill 的段级内核): 逐点逻辑与 profile 边界全在此
inline void fill_seg(Plane &p, Gen g, Profile pr, int t0, int t1, int A, std::mt19937 &rng) {
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
      case Gen::POS: // 对数正态: 保证 > 0, 给 Entropy / Hhi / LogRatio
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
} // namespace detail

// 并行按段造数: 每段从主 rng 派生一个种子再独立抽样, 结果只由 (seed, 段序) 决定, 与线程数无关.
// 24000×1000 单线程 mt19937 + normal_distribution 要 ~1 s, 这是对拍里唯一算不上算子的大头
inline void fill(Plane &p, Gen g, Profile pr, int T, int A, std::mt19937 &rng) {
  assert(T % kSegLen == 0 && "T 必须是整段数 (造数按段并行, 段界也是 EXPAND 的前提)");
  p.resize(static_cast<size_t>(T) * A);
  const int nseg = T / kSegLen;
  std::vector<uint32_t> seeds(static_cast<size_t>(nseg));
  for (uint32_t &s : seeds)
    s = rng();
  auto seg = [&](int s) {
    std::mt19937 r(seeds[static_cast<size_t>(s)]);
    detail::fill_seg(p, g, pr, s * kSegLen, (s + 1) * kSegLen, A, r);
  };
  const int threads = std::min<int>(nseg, static_cast<int>(std::max<unsigned>(1, std::thread::hardware_concurrency())));
  if (threads <= 1) {
    for (int s = 0; s < nseg; ++s)
      seg(s);
    return;
  }
  std::atomic<int> next{0};
  std::vector<std::thread> th;
  th.reserve(static_cast<size_t>(threads));
  for (int i = 0; i < threads; ++i)
    th.emplace_back([&] {
      for (int s; (s = next.fetch_add(1, std::memory_order_relaxed)) < nseg;)
        seg(s);
    });
  for (std::thread &t : th)
    t.join();
}

// 输入配方: 默认全 NORM, 只列出需要特殊数据的算子 (e_name 索引, 与 OpTable 行名一致)
struct Recipe {
  const char *e_name;
  Gen x, y, z;
};
inline constexpr Recipe kRecipes[] = {
    {"TsLogRatio", Gen::POS, Gen::POS, Gen::NORM},
    {"TsEntropyCum", Gen::POS, Gen::NORM, Gen::NORM},
    {"TsHhiCum", Gen::POS, Gen::NORM, Gen::NORM},
    {"TsWMeanCum", Gen::NORM, Gen::POS, Gen::NORM}, // 权为正, 否则 Σy 抵消
    {"TsWMeanRoll", Gen::NORM, Gen::POS, Gen::NORM},
    {"TsProductRoll", Gen::SMALL, Gen::NORM, Gen::NORM},
    {"CsGroupMean", Gen::NORM, Gen::GROUP, Gen::NORM},
    {"CsGroupRank", Gen::NORM, Gen::GROUP, Gen::NORM},
    {"CsGroupResid", Gen::NORM, Gen::NORM, Gen::GROUP},
};
inline Recipe recipe_of(const std::string &name) {
  for (const Recipe &r : kRecipes)
    if (name == r.e_name)
      return r;
  return {"", Gen::NORM, Gen::NORM, Gen::NORM};
}

// 窗长参数: 每算子一个"典型用法"的默认 d (期 = 分钟), 给 GUI Operators 表用;
// op_check 不吃这份 (它全扫 d = {1, 5, 20, 240, 300} 打边界)
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

// 阈值型参数: 这里只定 k / k2
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
inline void set_k(const std::string &name, Param &p) {
  p.k = 1.f, p.k2 = 0.f;
  for (const KParam &q : kKParams)
    if (name == q.e_name)
      p.k = q.k, p.k2 = q.k2;
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

// ---- 驱动: cpu (整张量一次) / stream (逐点推进) / gpu 由消费者经 GpuRun.hpp 调 ----

// 槽位取指针: use 为真时平面必须在 (元数够到的槽位调用方必填)
inline const float *pv(const Plane *p, bool use) {
  assert(!use || p);
  return use ? p->v.data() : nullptr;
}
inline const uint8_t *pm(const Plane *p, bool use) {
  assert(!use || p);
  return use ? p->m.data() : nullptr;
}

template <class C>
void run_cpu(const Data &d, const Param &p, int ar, Plane &o) {
  o.ensure(static_cast<size_t>(d.T) * d.A);
  C::run(pv(d.x, ar >= 1), pm(d.x, ar >= 1), pv(d.y, ar >= 2), pm(d.y, ar >= 2), pv(d.z, ar >= 3),
         pm(d.z, ar >= 3), o.v.data(), o.m.data(), d.T, d.A, p);
}

// TS 流式: 逐资产建一个 kernel 沿 t 推进 (与实盘同路: EXPAND 推满 kSegLen 自动归零, 不手动 reset)
template <class S, int AR, T W>
void run_stream_ts(const Data &d, const Param &p, Plane &o) {
  o.ensure(static_cast<size_t>(d.T) * d.A);
  assert(AR < 1 || d.x);
  assert(AR < 2 || d.y);
  assert(AR < 3 || d.z);
  if constexpr (W == T::POINT) {
    for (int t = 0; t < d.T; ++t)
      for (int a = 0; a < d.A; ++a) {
        const size_t i = static_cast<size_t>(t) * d.A + a;
        Val r;
        if constexpr (AR == 0)
          r = S::apply(t % kSegLen, p);
        else if constexpr (AR == 1)
          r = S::apply(Val{d.x->v[i], d.x->m[i] != 0}, p);
        else if constexpr (AR == 2)
          r = S::apply(Val{d.x->v[i], d.x->m[i] != 0}, Val{d.y->v[i], d.y->m[i] != 0}, p);
        else
          r = S::apply(Val{d.x->v[i], d.x->m[i] != 0}, Val{d.y->v[i], d.y->m[i] != 0},
                       Val{d.z->v[i], d.z->m[i] != 0}, p);
        o.v[i] = r.v, o.m[i] = r.m;
      }
  } else {
    for (int a = 0; a < d.A; ++a) {
      S op(p);
      for (int t = 0; t < d.T; ++t) {
        const size_t i = static_cast<size_t>(t) * d.A + a;
        Val r;
        if constexpr (AR == 1)
          r = op.push(Val{d.x->v[i], d.x->m[i] != 0});
        else
          r = op.push(Val{d.x->v[i], d.x->m[i] != 0}, Val{d.y->v[i], d.y->m[i] != 0});
        o.v[i] = r.v, o.m[i] = r.m;
      }
    }
  }
}

// CS 流式: 每个时刻一个截面
template <class S>
void run_stream_cs(const Data &d, const Param &p, int ar, Plane &o) {
  o.ensure(static_cast<size_t>(d.T) * d.A);
  const float *xv = pv(d.x, ar >= 1), *yv = pv(d.y, ar >= 2), *zv = pv(d.z, ar >= 3);
  const uint8_t *xm = pm(d.x, ar >= 1), *ym = pm(d.y, ar >= 2), *zm = pm(d.z, ar >= 3);
  for (int t = 0; t < d.T; ++t) {
    const size_t b = static_cast<size_t>(t) * d.A;
    S::apply(xv ? xv + b : nullptr, xm ? xm + b : nullptr, yv ? yv + b : nullptr, ym ? ym + b : nullptr,
             zv ? zv + b : nullptr, zm ? zm + b : nullptr, o.v.data() + b, o.m.data() + b, d.A, p);
  }
}

} // namespace factor::check
