#include "shared/Corr.hpp"
#include "features/Storage/DayBatchPlane.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "features/DataDefine.hpp" // kNaN (全库缺失值口径)
#include "features/TimeIndex.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <bit>
#include <cmath>

using namespace analysis;

size_t corr_slice_stride(size_t level) {
  const size_t vr = level_valid_rows(level);
  assert(vr % TRADE_MINUTES_PER_DAY == 0 && "层有效行数必须是分钟数的整数倍");
  return kCorrSliceMinutes * (vr / TRADE_MINUTES_PER_DAY);
}

namespace {

// ============================================================================
// 引擎配置: 三个目标只差这两项 (切片布局 × lag 数)
// ============================================================================
struct CorrPlan {
  enum class Slices : uint8_t { Strided, // 矩阵: 日内等距抽稀, 相位逐日轮转 (尾片可越界, 整片作废)
                                Window,  // lag 矩阵: 每日一段连续 kCorrLagWindow 行, 起点按天等距铺满日内
                                Full };  // 单对: 取满 (要分辨相邻期)
  Slices slices;
  size_t lags; // 1 = 只同期; kCorrLags = ±kCorrMaxLag

  int lag_at(size_t k) const { return lags == 1 ? 0 : CorrPair::lag_of(k); }

  size_t n_slices(size_t level) const {
    const size_t VR = level_valid_rows(level);
    switch (slices) {
    case Slices::Strided: {
      const size_t stride = corr_slice_stride(level);
      return (VR + stride - 1) / stride;
    }
    case Slices::Window:
      assert(VR >= kCorrLagWindow);
      return kCorrLagWindow;
    case Slices::Full:
      return VR;
    }
    return 0;
  }
  // 第 d 个抽样日 (共 n_days) 的 S 个切片行号
  void fill_slices(size_t level, size_t d, size_t n_days, size_t *out) const {
    const size_t VR = level_valid_rows(level), S = n_slices(level);
    switch (slices) {
    case Slices::Strided: {
      const size_t stride = corr_slice_stride(level), off = d % stride;
      for (size_t s = 0; s < S; ++s)
        out[s] = off + s * stride;
      return;
    }
    case Slices::Window: { // 抽样日本身已洗牌, 起点序无需再随机
      const size_t start = d * (VR - S + 1) / n_days;
      for (size_t s = 0; s < S; ++s)
        out[s] = start + s;
      return;
    }
    case Slices::Full:
      for (size_t s = 0; s < S; ++s)
        out[s] = s;
      return;
    }
  }
};
constexpr CorrPlan kMatrixPlan{CorrPlan::Slices::Strided, 1};
constexpr CorrPlan kLagPlan{CorrPlan::Slices::Window, kCorrLags};
constexpr CorrPlan kPairPlan{CorrPlan::Slices::Full, kCorrLags};

// lag 的切片对 (t, t + lag) 都落在 [0, S) 的 t 区间
std::pair<size_t, size_t> lag_range(int lag, size_t S) {
  const size_t t0 = lag < 0 ? static_cast<size_t>(-lag) : 0;
  const size_t t1 = lag > 0 ? S - static_cast<size_t>(lag) : S;
  return {t0, std::max(t0, t1)};
}

// 上三角 (i < j) 紧凑下标: 累加器按对存, 不浪费一半
inline size_t upper_index(size_t i, size_t j, size_t F) {
  assert(i < j && j < F);
  return i * F - i * (i + 1) / 2 + (j - i - 1);
}

// ============================================================================
// 累加量 (口径见 Corr.hpp 头注释) + pairwise-complete ρ
// ============================================================================
struct PairSums {
  double xy = 0, x = 0, y = 0, xx = 0, yy = 0, n = 0;
};

// mean_x = x/n, mean_y = y/n, 分子 = xy - x·y/n, 分母 = √((xx - x²/n)(yy - y²/n))
// 样本不足 / 任一侧无方差 → NaN (UI 画空格, 不画成 0)
float pair_rho(const PairSums &s) {
  if (s.n < static_cast<double>(kCorrMinPairN))
    return kNaN;
  const double vx = s.xx - s.x * s.x / s.n;
  const double vy = s.yy - s.y * s.y / s.n;
  if (!(vx > 0.0) || !(vy > 0.0))
    return kNaN;
  const double r = (s.xy - s.x * s.y / s.n) / std::sqrt(vx * vy);
  return static_cast<float>(std::clamp(r, -1.0, 1.0));
}

// ============================================================================
// xy 的寄存器分块微核: NI 行 i × NJ 行 j 的 v8 累加器常驻寄存器 (2×4 = 8 累加 + 6 载入 = 14 ymm),
// 内层 = 切片 t × 资产向量 v; 尾部 hsum 一次.
// 本 TU 是 -fno-fast-math (NaN 口径要精确), 这里输入全是有限值 (秩, 无效槽恒 0), 局部放开 FMA 融合
// ============================================================================
using v8 = float __attribute__((vector_size(32)));
constexpr size_t kLanes = 8;
constexpr size_t kTile = 8;    // 任务宏块: 8 × 8 特征
constexpr size_t kChunkV = 16; // 资产维分块 (v8 个数 = 128 floats): 宏块 × 切片 的工作集驻留 L2

inline float hsum(v8 v) {
  float s = 0.0f;
  for (size_t l = 0; l < kLanes; ++l)
    s += v[l];
  return s;
}

// xi / xj = 首切片首特征的块起点; row = 特征行步长, slice = 切片步长 (都按 v8 计)
template <size_t NI, size_t NJ>
void dot_block(const v8 *xi, const v8 *xj, size_t row, size_t slice, size_t n_t, size_t n_vec, float *out) {
#if defined(__clang__)
#pragma clang fp contract(fast)
#endif
  v8 acc[NI][NJ] = {};
  for (size_t t = 0; t < n_t; ++t, xi += slice, xj += slice)
    for (size_t v = 0; v < n_vec; ++v) {
      v8 a[NI], b[NJ];
      for (size_t ii = 0; ii < NI; ++ii)
        a[ii] = xi[ii * row + v];
      for (size_t jj = 0; jj < NJ; ++jj)
        b[jj] = xj[jj * row + v];
      for (size_t ii = 0; ii < NI; ++ii)
        for (size_t jj = 0; jj < NJ; ++jj)
          acc[ii][jj] += a[ii] * b[jj];
    }
  for (size_t ii = 0; ii < NI; ++ii)
    for (size_t jj = 0; jj < NJ; ++jj)
      out[ii * 4 + jj] = hsum(acc[ii][jj]);
}

using DotFn = void (*)(const v8 *, const v8 *, size_t, size_t, size_t, size_t, float *);
constexpr DotFn kDot[2][4] = {
    {dot_block<1, 1>, dot_block<1, 2>, dot_block<1, 3>, dot_block<1, 4>},
    {dot_block<2, 1>, dot_block<2, 2>, dot_block<2, 3>, dot_block<2, 4>},
};

// ============================================================================
// RankPlane — 一步 (天批) 的全部 (天, 切片, 特征) 行: 门控后原值 → 标准化中位秩 + 位掩码 + 掩码等价类
// ============================================================================
// 行号 g 与 CsPlane 一致: g = (j·S + s)·F + f
struct RankPlane {
  size_t rows = 0, A = 0, Av = 0, W = 0; // Av = 每行 v8 数 (尾部 pad 恒 0), W = 每行掩码字数
  std::vector<v8> R;                     // [rows][Av] 标准化中位秩 (Σ R = 0, Σ R² = k; 无效槽恒 0)
  std::vector<uint64_t> bits;            // [rows][W] 有效位
  std::vector<uint32_t> k;               // [rows] 有效资产数 (0 = 作废)
  std::vector<uint64_t> hash;            // [rows] 掩码指纹 (k > 0 才有意义)
  std::vector<uint32_t> cls;             // [rows] 掩码等价类 (同类 ⇔ 掩码逐位相同)
  struct Key {
    uint64_t h;
    uint32_t k, row;
  };
  std::vector<Key> order;     // classify 暂存
  std::vector<uint32_t> reps; // classify 暂存 (同指纹组内的代表行)

  void prepare(size_t rows_, size_t A_) {
    assert(A_ <= (size_t(1) << 16) && "排序键低 16 位存资产下标");
    rows = rows_;
    A = A_;
    Av = (A + kLanes - 1) / kLanes;
    W = (A + 63) / 64;
    R.resize(rows * Av); // rank_row 逐行全覆写
    bits.resize(rows * W);
    k.assign(rows, 0);
    hash.assign(rows, 0);
    cls.assign(rows, 0);
    order.reserve(rows);
    reps.reserve(rows);
  }
  float *Rf(size_t g) { return reinterpret_cast<float *>(R.data() + g * Av); }
  const float *Rf(size_t g) const { return reinterpret_cast<const float *>(R.data() + g * Av); }
  const uint64_t *bits_of(size_t g) const { return bits.data() + g * W; }

  // 单行秩变换: 有限值 → 中位秩 → 标准化 (在它自己的有效资产集上).
  // 并列必须取中位秩: flag 列大片 0/1, 用序号秩会凭空造出排序.
  // 有效资产不足 / 全并列无方差 → 作废 (k = 0, R 与掩码全 0). key / tmp = 调用方 scratch [A]
  void rank_row(size_t g, const feature_storage_t *x, uint32_t *key, uint32_t *tmp) {
    float *r = Rf(g);
    uint64_t *b = bits.data() + g * W;
    std::fill_n(r, Av * kLanes, 0.0f);
    std::fill_n(b, W, uint64_t{0});
    k[g] = 0;
    // 键 = f16 可排序位型 (高 16) | 资产下标 (低 16): 哨兵 (门控不过 / NaN) 与 ±inf 不进秩
    size_t n = 0;
    for (size_t a = 0; a < A; ++a) {
      uint16_t u = std::bit_cast<uint16_t>(x[a]);
      if ((u & 0x7C00u) == 0x7C00u)
        continue;
      if (u == 0x8000u)
        u = 0; // -0 归正: 位型与 +0 不同, 否则并列段被切开 (零在稀疏订单流列极常见)
      const uint32_t s = (u & 0x8000u) ? (~u & 0xFFFFu) : (u | 0x8000u);
      key[n++] = (s << 16) | static_cast<uint32_t>(a);
      b[a >> 6] |= uint64_t{1} << (a & 63);
    }
    if (n < kCorrMinAssets) {
      std::fill_n(b, W, uint64_t{0});
      return;
    }
    const uint32_t *sorted = radix_hi16(key, tmp, n);

    // 中位秩: 值相等的一段共享该段下标的平均值. 秩用 0 基, 均值恒为 (n-1)/2 (与并列无关)
    const double mean = 0.5 * static_cast<double>(n - 1);
    double ss = 0.0;
    for (size_t i = 0; i < n;) {
      size_t j = i + 1;
      while (j < n && (sorted[j] >> 16) == (sorted[i] >> 16))
        ++j;
      const double rr = 0.5 * static_cast<double>(i + j - 1) - mean;
      ss += static_cast<double>(j - i) * rr * rr;
      i = j;
    }
    if (!(ss > 0.0)) { // 该切片内全并列: ρ 无定义
      std::fill_n(b, W, uint64_t{0});
      return;
    }
    const double inv = std::sqrt(static_cast<double>(n) / ss); // Σ R² == n
    for (size_t i = 0; i < n;) {
      size_t j = i + 1;
      while (j < n && (sorted[j] >> 16) == (sorted[i] >> 16))
        ++j;
      const float rr = static_cast<float>((0.5 * static_cast<double>(i + j - 1) - mean) * inv);
      for (size_t q = i; q < j; ++q)
        r[sorted[q] & 0xFFFFu] = rr;
      i = j;
    }
    uint64_t h = n;
    for (size_t w = 0; w < W; ++w) {
      h = (h ^ b[w]) * 0x9E3779B97F4A7C15ull;
      h ^= h >> 29;
    }
    k[g] = static_cast<uint32_t>(n);
    hash[g] = h;
  }

  // 两趟 8 位 LSD 基数排序 (只排高 16 位, 稳定); 全落一个桶的趟是恒等, 跳过. 返回结果所在缓冲
  static const uint32_t *radix_hi16(uint32_t *key, uint32_t *tmp, size_t n) {
    for (const unsigned shift : {16u, 24u}) {
      uint32_t cnt[256] = {};
      for (size_t i = 0; i < n; ++i)
        ++cnt[(key[i] >> shift) & 0xFFu];
      if (cnt[(key[0] >> shift) & 0xFFu] == n)
        continue;
      uint32_t off = 0;
      for (uint32_t &c : cnt) {
        const uint32_t c0 = c;
        c = off;
        off += c0;
      }
      for (size_t i = 0; i < n; ++i)
        tmp[cnt[(key[i] >> shift) & 0xFFu]++] = key[i];
      std::swap(key, tmp);
    }
    return key;
  }

  // 掩码等价类 (单线程): 按 (指纹, k) 分组, 组内逐字比对定类 (指纹碰撞也不会错归)
  void classify(size_t n_rows) {
    TraceN("CorrClassify");
    order.clear();
    for (size_t g = 0; g < n_rows; ++g)
      if (k[g] > 0)
        order.push_back({hash[g], k[g], static_cast<uint32_t>(g)});
    std::sort(order.begin(), order.end(), [](const Key &a, const Key &b) {
      return a.h != b.h ? a.h < b.h : (a.k != b.k ? a.k < b.k : a.row < b.row);
    });
    uint32_t next = 0;
    for (size_t i = 0; i < order.size();) {
      size_t j = i + 1;
      while (j < order.size() && order[j].h == order[i].h && order[j].k == order[i].k)
        ++j;
      reps.clear();
      for (size_t q = i; q < j; ++q) {
        const uint32_t g = order[q].row;
        bool found = false;
        for (const uint32_t rep : reps)
          if (std::equal(bits_of(g), bits_of(g) + W, bits_of(rep))) {
            cls[g] = cls[rep];
            found = true;
            break;
          }
        if (!found) {
          cls[g] = next++;
          reps.push_back(g);
        }
      }
      i = j;
    }
  }

  // 一个 (行 gi, 行 gj) 切片对的边际和: 行恒等式 Σ_own R = 0, Σ_own R² = k →
  // 只需对掩码差集 (或交集, 取位少的一侧) 求稀疏和; 同类 (掩码相同) O(1)
  void side(PairSums &s, size_t gi, size_t gj) const {
    const uint32_t ki = k[gi], kj = k[gj];
    if (cls[gi] == cls[gj]) {
      s.xx += ki;
      s.yy += kj;
      s.n += ki;
      return;
    }
    const uint64_t *bi = bits_of(gi), *bj = bits_of(gj);
    uint32_t n_and = 0;
    for (size_t w = 0; w < W; ++w)
      n_and += static_cast<uint32_t>(std::popcount(bi[w] & bj[w]));
    s.n += n_and;
    cross(bi, bj, Rf(gi), ki, n_and, s.x, s.xx);
    cross(bj, bi, Rf(gj), kj, n_and, s.y, s.yy);
  }
  // Σ_{own ∩ other} (R, R²): 交集位少 → 直接累交集; 否则 = 全集 (0, k) − 差集
  void cross(const uint64_t *own, const uint64_t *other, const float *r, uint32_t k_own, uint32_t n_and,
             double &x, double &xx) const {
    const bool direct = 2 * n_and <= k_own;
    double sx = 0.0, sxx = 0.0;
    for (size_t w = 0; w < W; ++w)
      for (uint64_t m = own[w] & (direct ? other[w] : ~other[w]); m; m &= m - 1) {
        const double v = r[(w << 6) + static_cast<size_t>(std::countr_zero(m))];
        sx += v;
        sxx += v * v;
      }
    if (direct) {
      x += sx;
      xx += sxx;
    } else {
      x -= sx;
      xx += static_cast<double>(k_own) - sxx;
    }
  }
};

// ============================================================================
// Engine — 三个目标共用: 天批流水线 (IO 预读 → 秩 → 等价类 → 累加 → 发布)
// ============================================================================
// 两个常驻池: ex = 计算 (worker 线程 dispatch), Prefetch 自带 io 池 (预读线程 dispatch), 两者并发互不重入.
// 累加任务 = (8×8 特征宏块对 I ≤ J) × lag 组: 每个 (对, lag) 累加格只被一个任务写, 免 shard 免归约.
struct Engine {
  CorrPlan plan{};
  size_t F = 0, A = 0, S = 0, days = 0, L = 0, P = 0;
  Prefetch<CsPlane> pf;
  RankPlane rk;
  std::vector<PairSums> acc; // [P][L] 跨天累加
  std::vector<double> diag;  // [F] Σ 有效资产数 (矩阵对角 pair_n)
  std::vector<std::pair<uint32_t, uint32_t>> tile_pairs;
  size_t lag_group = 1; // 每任务 lag 数: 宏块对够多 → 全部 lag 一任务 (工作集复用), 否则一 lag 一任务 (单对)
  struct Scratch {
    std::vector<uint32_t> key, tmp; // [A] 排序键
  };
  std::vector<Scratch> scratch; // [ex 线程数]
  std::unique_ptr<misc::exec::ForkJoin> ex;

  const PairSums &sums(size_t i, size_t j, size_t k) const { return acc[upper_index(i, j, F) * L + k]; }

  void prepare(const CorrSpec &spec, size_t A_, const CorrPlan &p) {
    TraceN("CorrPrepare");
    plan = p;
    F = spec.n();
    A = A_;
    S = p.n_slices(spec.level);
    L = p.lags;
    P = F * (F - 1) / 2;
    assert(F >= 2 && A > 0);
    days = std::clamp<size_t>(kCorrStepCells / (S * F * A), 1, kDaysPerBatch);

    const size_t n_tiles = (F + kTile - 1) / kTile;
    tile_pairs.clear();
    for (uint32_t I = 0; I < n_tiles; ++I)
      for (uint32_t J = I; J < n_tiles; ++J)
        tile_pairs.emplace_back(I, J);
    const size_t n_threads = threads_for(std::max(days * S * F, tile_pairs.size() * L));
    misc::exec::ensure(ex, static_cast<int>(n_threads));
    lag_group = tile_pairs.size() >= 4 * n_threads ? L : 1;

    const size_t n_blk = (F + CsPlane::kBlockCols - 1) / CsPlane::kBlockCols;
    pf.prepare(threads_for(days * n_blk), A, spec.level, std::min(F, CsPlane::kBlockCols));
    for (CsPlane &pl : pf.buf)
      pl.prepare(A, spec.level, days, S, F);
    rk.prepare(days * S * F, A);
    acc.assign(P * L, PairSums{});
    diag.assign(F, 0.0);
    scratch.resize(n_threads);
    for (Scratch &sc : scratch) {
      sc.key.resize(A);
      sc.tmp.resize(A);
    }
  }

  // 抽样日 (洗牌取前 kCorrDays 个, 与 Dist / Preview 同一天序) 逐步流式; 每步末 publish_step() 写快照
  template <class Publish>
  bool run(FeatureRead &reader, const ReadScope &scope, const CorrSpec &spec, const CorrPlan &p,
           const std::atomic<bool> &cancel, Progress &board, Publish &&publish_step) {
    const DayPlan sample = plan_days(reader, scope.months, DayOrder::Shuffle, kCorrDays);
    const size_t n_days = sample.size();
    if (n_days == 0)
      return true; // 库为空: 直接 Done, 快照全空
    board.total.store(n_days, std::memory_order_release);
    prepare(spec, scope.lib.axis.size(), p);

    // 第 d0 步的 IO (预读线程或本线程跑; 只写传入的平面, 切片行号随之填)
    const auto load = [&](size_t d0) {
      return [&, d0](CsPlane &pl, misc::exec::ForkJoin &io, std::vector<IoSlot> &slots) {
        const size_t bd = std::min(days, n_days - d0);
        for (size_t j = 0; j < bd; ++j)
          plan.fill_slices(spec.level, d0 + j, n_days, pl.slice_t.data() + j * S);
        pl.load_batch(io, slots, reader, sample.dates.data() + d0, bd, spec.cols, cancel);
      };
    };
    pf.now(load(0));

    for (size_t d0 = 0; d0 < n_days; d0 += days) {
      const size_t bd = std::min(days, n_days - d0);
      if (d0 + days < n_days)
        pf.issue(load(d0 + days)); // issue → wait 之间不许 return
      rank(pf.front(), bd, cancel);
      accumulate(bd, cancel);
      pf.wait();
      if (cancel.load(std::memory_order_relaxed))
        return false; // 半步不发布
      publish_step();
      board.publish(bd);
      pf.flip();
    }
    return true;
  }

  // 相: 秩 (行一任务, 粒度细尾部不空转) → 等价类 + 对角账 (单线程)
  void rank(const CsPlane &pl, size_t bd, const std::atomic<bool> &cancel) {
    TraceN("CorrRank");
    const size_t rows = bd * S * F;
    run_phase(*ex, rows, cancel, [&](size_t g, size_t tid) {
      Scratch &sc = scratch[tid];
      rk.rank_row(g, pl.data.data() + g * A, sc.key.data(), sc.tmp.data());
    });
    if (cancel.load(std::memory_order_relaxed))
      return;
    rk.classify(rows);
    for (size_t g = 0; g < rows; ++g)
      diag[g % F] += rk.k[g];
  }

  // 相: 累加 ((宏块对, lag 组) 一任务)
  void accumulate(size_t bd, const std::atomic<bool> &cancel) {
    TraceN("CorrAccum");
    const size_t n_groups = (L + lag_group - 1) / lag_group;
    run_phase(*ex, tile_pairs.size() * n_groups, cancel, [&](size_t task, size_t) {
      const auto [I, J] = tile_pairs[task / n_groups];
      const size_t k0 = (task % n_groups) * lag_group;
      accumulate_tile(I, J, k0, std::min(k0 + lag_group, L), bd);
    });
  }

  // 宏块对 (I, J) × lag [k0, k1): 行 i ∈ I 在切片 t, 行 j ∈ J 在切片 t + lag, 只累 i < j
  void accumulate_tile(size_t I, size_t J, size_t k0, size_t k1, size_t bd) {
    const size_t i0 = I * kTile, i1 = std::min(i0 + kTile, F);
    const size_t j0 = J * kTile, j1 = std::min(j0 + kTile, F);
    const size_t row = rk.Av, slice = F * rk.Av;

    // ---- xy: 资产分块 → lag → 微块, 微核内扫切片 (宏块 × 切片 × 128 资产的工作集跨 lag 复用) ----
    for (size_t d = 0; d < bd; ++d)
      for (size_t c0 = 0; c0 < rk.Av; c0 += kChunkV) {
        const size_t nv = std::min(kChunkV, rk.Av - c0);
        for (size_t k = k0; k < k1; ++k) {
          const int lag = plan.lag_at(k);
          const auto [t0, t1] = lag_range(lag, S);
          if (t0 == t1)
            continue;
          const size_t ti = d * S + t0, tj = static_cast<size_t>(static_cast<ptrdiff_t>(ti) + lag);
          const v8 *base_i = rk.R.data() + ti * slice + c0;
          const v8 *base_j = rk.R.data() + tj * slice + c0;
          for (size_t ib = i0; ib < i1; ib += 2) {
            const size_t ni = std::min<size_t>(2, i1 - ib);
            for (size_t jb = j0; jb < j1; jb += 4) {
              const size_t nj = std::min<size_t>(4, j1 - jb);
              if (jb + nj <= ib + 1)
                continue; // 微块全在 j ≤ i 一侧
              float out[8];
              kDot[ni - 1][nj - 1](base_i + ib * row, base_j + jb * row, row, slice, t1 - t0, nv, out);
              for (size_t ii = 0; ii < ni; ++ii)
                for (size_t jj = 0; jj < nj; ++jj) {
                  const size_t i = ib + ii, j = jb + jj;
                  if (j > i)
                    acc[upper_index(i, j, F) * L + k].xy += out[ii * 4 + jj];
                }
            }
          }
        }
      }

    // ---- 边际和 x / y / xx / yy / n: 两行都有效的切片对才累 (作废行 R 恒 0, xy 已自然跳过) ----
    for (size_t d = 0; d < bd; ++d)
      for (size_t k = k0; k < k1; ++k) {
        const int lag = plan.lag_at(k);
        const auto [t0, t1] = lag_range(lag, S);
        for (size_t t = t0; t < t1; ++t) {
          const size_t gi = (d * S + t) * F;
          const size_t gj = static_cast<size_t>(static_cast<ptrdiff_t>(d * S + t) + lag) * F;
          for (size_t i = i0; i < i1; ++i) {
            if (!rk.k[gi + i])
              continue;
            for (size_t j = std::max(j0, i + 1); j < j1; ++j)
              if (rk.k[gj + j])
                rk.side(acc[upper_index(i, j, F) * L + k], gi + i, gj + j);
          }
        }
      }
  }
};

} // namespace

// ============================================================================
// Corr (矩阵: Strided × 同期)
// ============================================================================

struct Corr::Runtime {
  Engine eng;
  Snap pub; // 发布 staging (与快照 swap, 稳态零分配)
};

Corr::Corr() = default;
Corr::~Corr() = default;

void Corr::reset(ReadScope scope, Spec sp) {
  TraceN("CorrReset");
  const size_t F = sp.n();
  assert(F >= 2 && sp.level < LEVEL_COUNT && scope.lib.axis.size() > 0 && !scope.months.empty());
  assert(std::is_sorted(sp.cols.io.begin(), sp.cols.io.begin() + F));
  assert(std::adjacent_find(sp.cols.io.begin(), sp.cols.io.begin() + F) == sp.cols.io.begin() + F);
  std::lock_guard<std::mutex> lock(mutex);
  scope_ = std::move(scope);
  spec = std::move(sp);
  // 列集/层变了旧矩阵下标全错位, 必须整体清 (与 Dist/Transform "旧图留住" 相反)
  snap.rho.assign(F * F, kNaN);
  snap.pair_n.assign(F * F, 0);
  if (!rt_)
    rt_ = std::make_unique<Runtime>();
  publish();
}

bool Corr::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("CorrBuild");
  assert(rt_ && "reset 先于 build");
  Runtime &rt = *rt_;
  const size_t F = spec.n();
  rt.pub.rho.assign(F * F, kNaN);
  rt.pub.pair_n.assign(F * F, 0);
  const Engine &eng = rt.eng;
  return rt.eng.run(reader, scope_, spec, kMatrixPlan, cancel, *this, [&] {
    TraceN("CorrPublish");
    for (size_t i = 0; i < F; ++i) {
      rt.pub.rho[i * F + i] = 1.0f;
      rt.pub.pair_n[i * F + i] = static_cast<uint32_t>(eng.diag[i]); // 对角只记有效资产数 (UI 悬停显示)
      for (size_t j = i + 1; j < F; ++j) {
        const PairSums &s = eng.sums(i, j, 0);
        rt.pub.rho[i * F + j] = rt.pub.rho[j * F + i] = pair_rho(s);
        rt.pub.pair_n[i * F + j] = rt.pub.pair_n[j * F + i] = static_cast<uint32_t>(s.n);
      }
    }
    std::lock_guard<std::mutex> lock(mutex);
    std::swap(snap, rt.pub); // 尺寸恒相等, swap 回来的上一份成品下步整体覆写
  });
}

void Corr::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  snap = Snap{};
  spec = Spec{};
  rt_.reset(); // worker 已 join, 整体释放
  scope_ = ReadScope{};
  idle();
}

// ============================================================================
// CorrLag (全矩阵 lead-lag 峭点: Window × ±kCorrMaxLag)
// ============================================================================

struct CorrLag::Runtime {
  Engine eng;
  Snap pub;
};

CorrLag::CorrLag() = default;
CorrLag::~CorrLag() = default;

void CorrLag::reset(ReadScope scope, Spec sp) {
  TraceN("CorrLagReset");
  const size_t F = sp.n();
  assert(F >= 2 && sp.level < LEVEL_COUNT && scope.lib.axis.size() > 0 && !scope.months.empty());
  assert(std::is_sorted(sp.cols.io.begin(), sp.cols.io.begin() + F));
  std::lock_guard<std::mutex> lock(mutex);
  scope_ = std::move(scope);
  spec = std::move(sp);
  snap.lag_best.assign(F * F, 0);
  snap.rho_best.assign(F * F, kNaN);
  if (!rt_)
    rt_ = std::make_unique<Runtime>();
  publish();
}

bool CorrLag::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("CorrLagBuild");
  assert(rt_ && "reset 先于 build");
  Runtime &rt = *rt_;
  const size_t F = spec.n();
  rt.pub.lag_best.assign(F * F, 0);
  rt.pub.rho_best.assign(F * F, kNaN);
  const Engine &eng = rt.eng;
  return rt.eng.run(reader, scope_, spec, kLagPlan, cancel, *this, [&] {
    TraceN("CorrLagPublish");
    for (size_t i = 0; i < F; ++i) {
      rt.pub.lag_best[i * F + i] = 0;
      rt.pub.rho_best[i * F + i] = 1.0f;
      for (size_t j = i + 1; j < F; ++j) {
        // 每对取 |ρ| 最大的 lag. 按 |lag| 递增扫 (0, -1, +1, -2, +2, ...), 平局留先到的 = 更靠近 0:
        // 慢变列曲线平坦, 不该被浮点噪声指到两端
        int best_k = -1;
        float best_r = 0.0f;
        for (size_t step = 0; step <= kCorrMaxLag; ++step)
          for (int sgn = -1; sgn <= 1; sgn += 2) {
            if (step == 0 && sgn > 0)
              continue;
            const size_t k = static_cast<size_t>(static_cast<int>(kCorrMaxLag) + sgn * static_cast<int>(step));
            const float r = pair_rho(eng.sums(i, j, k));
            if (r == r && (best_k < 0 || std::fabs(r) > std::fabs(best_r))) {
              best_k = static_cast<int>(k);
              best_r = r;
            }
          }
        const int8_t lag = best_k >= 0 ? static_cast<int8_t>(CorrPair::lag_of(static_cast<size_t>(best_k))) : int8_t{0};
        rt.pub.lag_best[i * F + j] = lag;
        rt.pub.lag_best[j * F + i] = static_cast<int8_t>(-lag);
        rt.pub.rho_best[i * F + j] = rt.pub.rho_best[j * F + i] = best_k >= 0 ? best_r : kNaN;
      }
    }
    std::lock_guard<std::mutex> lock(mutex);
    std::swap(snap, rt.pub);
  });
}

void CorrLag::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  snap = Snap{};
  spec = Spec{};
  rt_.reset();
  scope_ = ReadScope{};
  idle();
}

// ============================================================================
// CorrPair (单对 lead-lag 曲线: Full × ±kCorrMaxLag; 列少 → 一步读多天, IO 按天并行)
// ============================================================================

struct CorrPair::Runtime {
  Engine eng;
  Snap pub;
};

CorrPair::CorrPair() = default;
CorrPair::~CorrPair() = default;

void CorrPair::reset(ReadScope scope, Spec sp) {
  assert(sp.n() == 2 && sp.col(0) != sp.col(1));
  assert(sp.level < LEVEL_COUNT && scope.lib.axis.size() > 0 && !scope.months.empty());
  std::lock_guard<std::mutex> lock(mutex);
  scope_ = std::move(scope);
  spec = std::move(sp);
  snap.rho.fill(kNaN);
  snap.pair_n.fill(0);
  if (!rt_)
    rt_ = std::make_unique<Runtime>();
  publish();
}

bool CorrPair::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("CorrPairBuild");
  assert(rt_ && "reset 先于 build");
  Runtime &rt = *rt_;
  const Engine &eng = rt.eng;
  return rt.eng.run(reader, scope_, spec, kPairPlan, cancel, *this, [&] {
    // ρ(lag) = corr(A(t), B(t + lag)), lag > 0 = A 领先 B (A = 列 0 = 上三角行)
    for (size_t k = 0; k < kCorrLags; ++k) {
      const PairSums &s = eng.sums(0, 1, k);
      rt.pub.rho[k] = pair_rho(s);
      rt.pub.pair_n[k] = static_cast<uint32_t>(s.n);
    }
    std::lock_guard<std::mutex> lock(mutex);
    std::swap(snap, rt.pub);
  });
}

void CorrPair::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  snap = Snap{};
  spec = Spec{};
  rt_.reset();
  scope_ = ReadScope{};
  idle();
}
