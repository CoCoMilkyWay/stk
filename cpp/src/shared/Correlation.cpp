#include "shared/Correlation.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "features/DataDefine.hpp" // kNaN (全库缺失值口径)
#include "features/MetaFlag.hpp"   // fmeta::valid
#include "features/TimeIndex.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <functional>
#include <random>
#include <thread>

using namespace analysis;

// 本 TU 编译在 -fno-fast-math 下 (NaN 口径要精确), 但那样浮点归约不许重结合 → 内积循环
// 完全不向量化, 慢一个数量级. 热点内积的输入全是有限值 (秩 / 掩码, 无效槽恒 0), 局部
// 放开重结合无风险 —— 放在内积所在复合语句开头
#if defined(__clang__)
#define CORR_FP_REASSOC _Pragma("clang fp reassociate(on)")
#else
#define CORR_FP_REASSOC
#endif

// 无效槽哨兵 (与 DayBatchPlane 同一个 f16 qNaN 位型)
static constexpr uint16_t kInvalidBits = 0x7E00u;
static const feature_storage_t kInvalid16 = std::bit_cast<feature_storage_t>(kInvalidBits);

size_t corr_slice_stride(size_t level) {
  const size_t vr = level_valid_rows(level);
  assert(vr % TRADE_MINUTES_PER_DAY == 0 && "层有效行数必须是分钟数的整数倍");
  return kCorrSliceMinutes * (vr / TRADE_MINUTES_PER_DAY);
}

// ============================================================================
// 公共小件
// ============================================================================

// 与矩阵一致的抽样日集合: 固定种子洗牌取前 kCorrDays 个.
// 洗牌而非"每隔 k 天取一天" —— 等间隔抽天会锁死星期几, 把星期效应变成系统偏差.
static std::vector<std::string> sample_days(const FeatureRead &reader,
                                            const std::vector<std::string> &months) {
  std::vector<std::string> dates = enumerate_dates(reader, months).dates;
  if (dates.empty())
    return dates;
  std::shuffle(dates.begin(), dates.end(), std::mt19937{0x5eed});
  dates.resize(std::min(kCorrDays, dates.size()));
  return dates;
}

// float → 可按 uint32 比大小的键 (全序, 输入保证有限)
static inline uint32_t sort_key(float v) {
  const uint32_t u = std::bit_cast<uint32_t>(v);
  return (u >> 31) ? ~u : (u | 0x80000000u);
}

// 单 (切片, 特征) 的秩变换: 有效值 → 中位秩 → 标准化 (均值 0 / 单位方差, 在它自己的有效
// 资产集上). 无效槽 R = M = R2 = 0. 标准化是"累加叉积 ≡ 逐切片算 ρ 再平均"的前提.
// 并列必须取中位秩: flag 列大片 0/1, 用序号秩会凭空造出排序.
// 返回 false = 该 (切片, 特征) 作废 (有效资产不足 / 全并列无方差), 调用方置 row_active = 0.
// key 是调用方提供的 scratch ([A] 个, 高 32 位值键 + 低 32 位资产下标)
static bool rank_slice(const feature_storage_t *raw, size_t A, uint64_t *key,
                       float *R, float *M, float *R2) {
  std::memset(R, 0, A * sizeof(float));
  std::memset(M, 0, A * sizeof(float));
  std::memset(R2, 0, A * sizeof(float));

  size_t k = 0;
  for (size_t a = 0; a < A; ++a) {
    float v = static_cast<float>(raw[a]);
    if (v != v) // 无效 (门控不过 / 真 NaN / ±inf) 在 IO 侧已折成哨兵 NaN
      continue;
    if (v == 0.0f)
      v = 0.0f; // -0.0 归正: 位型与 +0.0 不同, 否则并列段被切开 (零在稀疏订单流列极常见)
    key[k++] = (static_cast<uint64_t>(sort_key(v)) << 32) | static_cast<uint32_t>(a);
  }
  if (k < kCorrMinAssets)
    return false;
  std::sort(key, key + k);

  // 中位秩: 值相等的一段共享该段下标的平均值. 秩用 0 基, 故均值恒为 (k-1)/2
  // (与并列无关: 中位秩之和恒等于 0+1+...+(k-1)).
  const double mean = 0.5 * static_cast<double>(k - 1);
  double ss = 0.0;
  for (size_t i = 0; i < k;) {
    size_t j = i + 1;
    while (j < k && (key[j] >> 32) == (key[i] >> 32))
      ++j;
    const double r = 0.5 * static_cast<double>(i + j - 1) - mean; // 段中位秩, 已去均值
    ss += static_cast<double>(j - i) * r * r;
    for (size_t q = i; q < j; ++q)
      R[static_cast<uint32_t>(key[q])] = static_cast<float>(r);
    i = j;
  }
  if (!(ss > 0.0))
    return false; // 该切片内全并列: ρ 无定义

  const float inv = static_cast<float>(std::sqrt(static_cast<double>(k) / ss)); // Σ R² == k
  for (size_t q = 0; q < k; ++q) {
    const uint32_t a = static_cast<uint32_t>(key[q]);
    R[a] *= inv;
    M[a] = 1.0f;
    R2[a] = R[a] * R[a];
  }
  return true;
}

// pairwise-complete 相关 (累加量见 Correlation.hpp 头注释):
//   mean_x = sx/n, mean_y = sy/n, 分子 = sxy - sx·sy/n, 分母 = √((sxx - sx²/n)(syy - sy²/n))
// 样本不足 / 任一侧无方差 → NaN (UI 画空格, 不画成 0)
static float pair_rho(double sxy, double sx, double sy, double sxx, double syy, double n) {
  if (n < static_cast<double>(kCorrMinPairN))
    return kNaN;
  const double vx = sxx - sx * sx / n;
  const double vy = syy - sy * sy / n;
  if (!(vx > 0.0) || !(vy > 0.0))
    return kNaN;
  const double r = (sxy - sx * sy / n) / std::sqrt(vx * vy);
  return static_cast<float>(std::clamp(r, -1.0, 1.0));
}

// 一波线程跑 n_tasks 个任务 (抢任务). 相关矩阵按天分三相 (IO / 秩 / 累加), 每相一波:
// 整个构建也就 3 × kCorrDays 次起线程 (几十 ms), 换掉一套跨相栅栏状态机
static void parallel_for(size_t n_tasks, size_t n_threads, const std::atomic<bool> &cancel,
                         const std::function<void(size_t, size_t)> &fn) {
  if (n_tasks == 0)
    return;
  const size_t n = std::min(n_threads, n_tasks);
  std::atomic<size_t> next{0};
  std::vector<std::thread> threads;
  threads.reserve(n);
  for (size_t t = 0; t < n; ++t)
    threads.emplace_back([&, t] {
      for (;;) {
        const size_t i = next.fetch_add(1, std::memory_order_relaxed);
        if (i >= n_tasks || cancel.load(std::memory_order_relaxed))
          return;
        fn(i, t);
      }
    });
  for (auto &th : threads)
    th.join();
}

static size_t worker_threads() {
  return std::max<size_t>(1, std::thread::hardware_concurrency());
}

// ============================================================================
// SlicePlane — 一天的 S 个切片 × F 特征 × A 资产: 门控后原值 → 标准化中位秩
// (Correlation / CorrLag 共用; 切片在日内的行号由调用方给: 矩阵 = 等距抽稀, lag = 连续窗)
// ============================================================================

// IO 侧双缓冲: 算第 d 天 (秩 + 累加, 吃满全部核) 的同时, 后台线程把第 d+1 天读进另一份 ——
// 磁盘等待不再让 CPU 空转; IO 自己的门控循环也并行抢块.
struct SlicePlane {
  size_t F = 0, A = 0, S = 0, VR = 0, n_threads = 0;

  struct RawBuf {
    std::vector<size_t> slice_t;                  // [S] 切片行号 (>= VR = 越界, 整片作废)
    std::vector<feature_storage_t> raw;           // [S][F][A] 门控后的原值 (无效 = 哨兵 NaN)
    std::vector<float> gate;                      // [S][A] 当日 _meta 值 (逐特征按自己的 valid_type 判)
    std::vector<FeatureRead::DayColumns> staging; // [n_io] IO 暂存
    FeatureRead::DayColumns meta_day;
  };
  RawBuf bufs[2];
  size_t cur = 0; // 本天计算用的缓冲; 另一份给预读

  std::vector<float> R, M, R2;                 // [S][F][A] 标准化中位秩 / 掩码 / 秩平方 (bufs[cur] 的)
  std::vector<uint8_t> row_ok;                 // [S][F] 该 (切片, 特征) 是否参与
  std::vector<std::vector<uint64_t>> sort_buf; // [n_threads][A] 排序键

  void prepare(size_t F_, size_t A_, size_t S_, size_t level) {
    TraceN("CorrPlanePrepare");
    F = F_;
    A = A_;
    S = S_;
    VR = level_valid_rows(level);
    n_threads = worker_threads();
    cur = 0;

    for (RawBuf &b : bufs) {
      b.slice_t.assign(S, VR);
      b.raw.assign(S * F * A, kInvalid16);
      b.gate.assign(S * A, 0.0f);
      b.staging.resize(std::min(n_threads, (F + kCorrBlockCols - 1) / kCorrBlockCols));
      for (auto &st : b.staging)
        st.preallocate(A, level, kCorrBlockCols);
      b.meta_day.preallocate(A, level, 1);
    }
    R.assign(S * F * A, 0.0f);
    M.assign(S * F * A, 0.0f);
    R2.assign(S * F * A, 0.0f);
    row_ok.assign(S * F, 0);
    sort_buf.assign(n_threads, std::vector<uint64_t>(A));
  }

  RawBuf &next() { return bufs[cur ^ 1]; } // 预读目标 (调用方先填它的 slice_t)
  void flip() { cur ^= 1; }

  const float *Rs(size_t s) const { return R.data() + s * F * A; }
  const float *Ms(size_t s) const { return M.data() + s * F * A; }
  const float *R2s(size_t s) const { return R2.data() + s * F * A; }
  const uint8_t *ok(size_t s) const { return row_ok.data() + s * F; }

  // 相 1: IO (并行抢特征块) —— 逐块读列文件, 就地按 _meta 门控抽出 b.slice_t 指定的 S 个切片.
  // 只写 b 自己的内存, 可与另一缓冲上的 rank / 累加并发跑 (FeatureRead 并发只读)
  void load(RawBuf &b, FeatureRead &reader, const std::string &date,
            const std::vector<uint32_t> &cols, const std::vector<L2::ValidType> &vts,
            size_t meta_col, const std::atomic<bool> &cancel) {
    TraceN("CorrIO");
    assert(cols.size() == F && vts.size() == F && b.slice_t.size() == S);
    const std::vector<size_t> meta_cols{meta_col};
    reader.load_day_columns(date, meta_cols, b.meta_day);
    for (size_t s = 0; s < S; ++s) {
      const size_t t = b.slice_t[s];
      float *dst = b.gate.data() + s * A;
      if (t >= VR) { // 越界片: 门控值用不到, 值列那边整片作废
        std::fill_n(dst, A, 0.0f);
        continue;
      }
      const feature_storage_t *src = b.meta_day.data.data() + t * A;
      for (size_t a = 0; a < A; ++a)
        dst[a] = static_cast<float>(src[a]);
    }

    const size_t n_blocks = (F + kCorrBlockCols - 1) / kCorrBlockCols;
    parallel_for(n_blocks, b.staging.size(), cancel, [&](size_t blk, size_t tid) {
      const size_t f0 = blk * kCorrBlockCols, f1 = std::min(f0 + kCorrBlockCols, F);
      std::vector<size_t> cs;
      cs.reserve(f1 - f0);
      for (size_t f = f0; f < f1; ++f)
        cs.push_back(cols[f]);
      FeatureRead::DayColumns &st = b.staging[tid];
      reader.load_day_columns(date, cs, st);

      const size_t nb = f1 - f0;
      for (size_t i = 0; i < nb; ++i) {
        const L2::ValidType vt = vts[f0 + i];
        for (size_t s = 0; s < S; ++s) {
          const size_t t = b.slice_t[s];
          feature_storage_t *dst = b.raw.data() + (s * F + f0 + i) * A;
          if (t >= VR) {
            std::fill_n(dst, A, kInvalid16);
            continue;
          }
          const feature_storage_t *src = st.data.data() + (t * nb + i) * A;
          const float *g = b.gate.data() + s * A;
          for (size_t a = 0; a < A; ++a) {
            const float v = static_cast<float>(src[a]);
            // 门控不过 / NaN / ±inf 一律折成哨兵: 热循环一次 v != v 就跳过
            dst[a] = (fmeta::valid(g[a], vt) && std::isfinite(v)) ? src[a] : kInvalid16;
          }
        }
      }
    });
  }

  // 相 2: 对 bufs[cur] 做秩变换 (并行抢 (切片, 特征) 格; 粒度细, 尾部不空转)
  void rank(const std::atomic<bool> &cancel) {
    TraceN("CorrRank");
    const RawBuf &b = bufs[cur];
    parallel_for(S * F, n_threads, cancel, [&](size_t sf, size_t tid) {
      const size_t o = sf * A;
      row_ok[sf] = rank_slice(b.raw.data() + o, A, sort_buf[tid].data(), R.data() + o,
                              M.data() + o, R2.data() + o)
                       ? 1
                       : 0;
    });
  }
};

// ============================================================================
// Correlation::Runtime — worker 私有 (UI 不看)
// ============================================================================

struct Correlation::Runtime {
  SlicePlane plane;
  size_t stride = 0;

  // 跨天累加器 [F][F] (sx / sxx 非对称: [i][j] 是 "i 在 i∩j 上的和", 转置位是另一侧;
  // 对角线不写 —— ρ_ii 恒为 1, 只有 npair 的对角线有意义 = 有效资产数)
  std::vector<double> sxy, sx, sxx, npair;

  std::vector<float> pub_rho; // [F][F] 发布 staging (与快照 swap, 稳态零分配)
  std::vector<uint32_t> pub_n;

  void prepare(size_t F, size_t A, size_t level) {
    TraceN("CorrPrepare");
    stride = corr_slice_stride(level);
    const size_t VR = level_valid_rows(level);
    plane.prepare(F, A, (VR + stride - 1) / stride, level);

    sxy.assign(F * F, 0.0);
    sx.assign(F * F, 0.0);
    sxx.assign(F * F, 0.0);
    npair.assign(F * F, 0.0);
    pub_rho.assign(F * F, kNaN);
    pub_n.assign(F * F, 0);
  }
};

Correlation::Correlation() = default;
Correlation::~Correlation() = default;

// ============================================================================
// Reset
// ============================================================================

void Correlation::reset_for_build(size_t lvl, std::vector<uint32_t> col_list,
                                  std::vector<L2::ValidType> valid_types, size_t meta_col,
                                  std::vector<std::string> month_keys, size_t n_assets) {
  TraceN("CorrReset");
  assert(col_list.size() >= 2 && col_list.size() == valid_types.size());
  assert(std::is_sorted(col_list.begin(), col_list.end()));
  assert(std::adjacent_find(col_list.begin(), col_list.end()) == col_list.end());
  assert(lvl < LEVEL_COUNT && n_assets > 0 && !month_keys.empty());
  std::lock_guard<std::mutex> lock(mutex);

  const size_t F = col_list.size();
  level = lvl;
  cols = std::move(col_list);
  valid_types_ = std::move(valid_types);
  meta_col_ = meta_col;
  months_ = std::move(month_keys);
  A_ = n_assets;

  // 列集/层变了旧矩阵下标全错位, 必须整体清 (与 Dist/Transform "旧图留住" 相反)
  rho.assign(F * F, kNaN);
  pair_n.assign(F * F, 0);

  if (!rt_)
    rt_ = std::make_unique<Runtime>();

  begin_build();
}

// ============================================================================
// Build (逐抽样日: IO → 秩 → 累加, 日末发布; 首帧 = 一天的 IO + 计算)
// ============================================================================

bool Correlation::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("CorrBuild");
  assert(rt_ && A_ > 0 && cols.size() >= 2 && "reset_for_build 先于 build");
  Runtime &rt = *rt_;

  const std::vector<std::string> dates = sample_days(reader, months_);
  if (dates.empty())
    return true; // 库为空: 直接 Done, 热图全空格
  total.store(dates.size(), std::memory_order_release);

  const size_t F = cols.size();
  rt.prepare(F, A_, level);
  SlicePlane &pl = rt.plane;
  const size_t S = pl.S, A = pl.A, stride = rt.stride;

  // 相位逐日轮转: 跨天把 stride 个日内相位均匀覆盖, 消掉固定相位的残余偏置
  // (尾片可越界: S 向上取整 + 相位偏移, SlicePlane 按 >= VR 整片作废)
  const auto set_slices = [&](SlicePlane::RawBuf &b, size_t d) {
    const size_t off = d % stride;
    for (size_t s = 0; s < S; ++s)
      b.slice_t[s] = off + s * stride;
  };
  const auto load = [&](SlicePlane::RawBuf &b, size_t d) {
    pl.load(b, reader, dates[d], cols, valid_types_, meta_col_, cancel);
  };

  set_slices(pl.bufs[pl.cur], 0);
  load(pl.bufs[pl.cur], 0);

  for (size_t d = 0; d < dates.size(); ++d) {
    if (cancel.load(std::memory_order_relaxed))
      return false;

    // ---- 预读下一天 (后台线程, 与本天的秩 / 累加并发; 日末 join) ----
    std::thread prefetch;
    if (d + 1 < dates.size()) {
      set_slices(pl.next(), d + 1);
      prefetch = std::thread([&, d] { load(pl.next(), d + 1); });
    }

    // ---- 相 2: 秩变换 ----
    pl.rank(cancel);

    // ---- 相 3: 累加 (并行抢矩阵行; 行 i 只被一个线程写, 免 shard 免归约) ----
    // 只跑上三角 (j > i), 同一趟把转置位一并填上 (sx/sxx 非对称, 两侧都要).
    // 任务粒度 = 单行: 行 i 的工作量 ∝ F - i, 动态抢任务即可摊平
    {
      TraceN("CorrAccum");
      parallel_for(F, pl.n_threads, cancel, [&](size_t i, size_t /*tid*/) {
        for (size_t s = 0; s < S; ++s) {
          const uint8_t *ok = pl.ok(s);
          if (!ok[i])
            continue;
          const float *Rs = pl.Rs(s), *Ms = pl.Ms(s), *R2s = pl.R2s(s);
          const float *Ri = Rs + i * A, *Mi = Ms + i * A, *Ri2 = R2s + i * A;
          float ni = 0;
          for (size_t a = 0; a < A; ++a)
            ni += Mi[a];
          rt.npair[i * F + i] += ni; // 对角线只记有效资产数 (UI 悬停显示)
          for (size_t j = i + 1; j < F; ++j) {
            if (!ok[j])
              continue;
            const float *Rj = Rs + j * A, *Mj = Ms + j * A, *Rj2 = R2s + j * A;
            // 六个点积一趟扫完 (A 连续, 放开重结合后向量化): 交集上的 Σxy / Σx / Σy / Σx² / Σy² / n.
            // R 在无效槽恒为 0, 故 Σ R_i·m_j 天然只累交集
            float axy = 0, ax = 0, ay = 0, axx = 0, ayy = 0, an = 0;
            {
              CORR_FP_REASSOC
              for (size_t a = 0; a < A; ++a) {
                axy += Ri[a] * Rj[a];
                ax += Ri[a] * Mj[a];
                ay += Rj[a] * Mi[a];
                axx += Ri2[a] * Mj[a];
                ayy += Rj2[a] * Mi[a];
                an += Mi[a] * Mj[a];
              }
            }
            rt.sxy[i * F + j] += axy;
            rt.sx[i * F + j] += ax;
            rt.sx[j * F + i] += ay;
            rt.sxx[i * F + j] += axx;
            rt.sxx[j * F + i] += ayy;
            rt.npair[i * F + j] += an;
          }
        }
      });
    }

    if (prefetch.joinable())
      prefetch.join(); // 早于任何 return: 预读线程引用着本栈
    if (cancel.load(std::memory_order_relaxed))
      return false;

    // ---- 日末发布: 锁外算成品, 短锁整体拷贝 ----
    {
      TraceN("CorrPublish");
      for (size_t i = 0; i < F; ++i) {
        rt.pub_rho[i * F + i] = 1.0f;
        rt.pub_n[i * F + i] = static_cast<uint32_t>(rt.npair[i * F + i]);
        for (size_t j = i + 1; j < F; ++j) {
          const double n = rt.npair[i * F + j];
          const float r = pair_rho(rt.sxy[i * F + j], rt.sx[i * F + j], rt.sx[j * F + i],
                                   rt.sxx[i * F + j], rt.sxx[j * F + i], n);
          rt.pub_rho[i * F + j] = rt.pub_rho[j * F + i] = r;
          rt.pub_n[i * F + j] = rt.pub_n[j * F + i] = static_cast<uint32_t>(n);
        }
      }
      {
        std::lock_guard<std::mutex> lock(mutex);
        rho.swap(rt.pub_rho); // 尺寸恒相等 (reset 时对齐), swap 回来的就是上一份成品, 整体覆写
        pair_n.swap(rt.pub_n);
      }
      publish_progress(1);
    }
    pl.flip(); // 预读好的那份成为下一天的计算缓冲
  }

  return !cancel.load(std::memory_order_relaxed);
}

// ============================================================================
// Clear
// ============================================================================

void Correlation::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存
  cols = std::vector<uint32_t>{};
  rho = std::vector<float>{};
  pair_n = std::vector<uint32_t>{};
  rt_.reset(); // worker 已 join, 整体释放
  valid_types_.clear();
  months_.clear();
  A_ = 0;
  level = 0;
  reset_idle();
}

// ============================================================================
// CorrPair::Runtime — 单对 lead-lag (只读两列; 并行单位 = 天, 每线程私有缓冲 + 累加器, 末尾归约)
// ============================================================================
// 按天并行而不是按相位并行: 一天的 IO + 秩 + 21 个 lag 内积全在一个线程里跑完, 没有相间栅栏,
// 32 天扔给全部核抢 —— 单对只有两列, 每线程缓冲 ~VR×A×26B, 几 MB, 开得起.

struct CorrPair::Runtime {
  size_t A = 0, VR = 0;

  struct DayBuf {
    FeatureRead::DayColumns day;                                         // [T][3][A]: a / b / _meta
    std::vector<feature_storage_t> raw_a, raw_b;                         // [VR][A] 门控后原值
    std::vector<float> Ra, Ma, R2a, Rb, Mb, R2b;                         // [VR][A]
    std::vector<uint8_t> ok_a, ok_b;                                     // [VR]
    std::vector<uint64_t> key;                                           // [A]
    std::array<double, kCorrLags> sxy{}, sx{}, sy{}, sxx{}, syy{}, np{}; // 线程私有累加
  };
  std::vector<DayBuf> bufs; // [n_threads]

  void prepare(size_t A_, size_t level, size_t n_threads) {
    A = A_;
    VR = level_valid_rows(level);
    bufs.resize(n_threads);
    for (DayBuf &b : bufs) {
      b.day.preallocate(A, level, 3);
      b.raw_a.assign(VR * A, kInvalid16);
      b.raw_b.assign(VR * A, kInvalid16);
      b.Ra.assign(VR * A, 0.0f);
      b.Ma.assign(VR * A, 0.0f);
      b.R2a.assign(VR * A, 0.0f);
      b.Rb.assign(VR * A, 0.0f);
      b.Mb.assign(VR * A, 0.0f);
      b.R2b.assign(VR * A, 0.0f);
      b.ok_a.assign(VR, 0);
      b.ok_b.assign(VR, 0);
      b.key.assign(A, 0);
      b.sxy = {};
      b.sx = {};
      b.sy = {};
      b.sxx = {};
      b.syy = {};
      b.np = {};
    }
  }
};

CorrPair::CorrPair() = default;
CorrPair::~CorrPair() = default;

void CorrPair::reset_for_build(size_t lvl, uint32_t a, uint32_t b, L2::ValidType vt_a,
                               L2::ValidType vt_b, size_t meta_col,
                               std::vector<std::string> month_keys, size_t n_assets) {
  assert(a != b && lvl < LEVEL_COUNT && n_assets > 0 && !month_keys.empty());
  std::lock_guard<std::mutex> lock(mutex);
  level = lvl;
  col_a = a;
  col_b = b;
  vt_a_ = vt_a;
  vt_b_ = vt_b;
  meta_col_ = meta_col;
  months_ = std::move(month_keys);
  A_ = n_assets;
  rho.fill(kNaN);
  pair_n.fill(0);
  if (!rt_)
    rt_ = std::make_unique<Runtime>();
  begin_build();
}

bool CorrPair::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("CorrPairBuild");
  assert(rt_ && A_ > 0 && "reset_for_build 先于 build");
  Runtime &rt = *rt_;

  const std::vector<std::string> dates = sample_days(reader, months_); // 与矩阵同一套抽样日
  if (dates.empty())
    return true;
  total.store(dates.size(), std::memory_order_release);

  const size_t n_threads = std::min(worker_threads(), dates.size());
  rt.prepare(A_, level, n_threads);
  const size_t A = rt.A, VR = rt.VR;
  const std::vector<size_t> day_cols{col_a, col_b, meta_col_};

  // 全部天并行抢 (每线程一套缓冲 + 累加器); 悬停即触发, 请求换得很快 → 日内也查取消
  parallel_for(dates.size(), n_threads, cancel, [&](size_t d, size_t tid) {
    Runtime::DayBuf &b = rt.bufs[tid];
    reader.load_day_columns(dates[d], day_cols, b.day);

    // 门控 + 秩变换 (切片取满: lead-lag 要分辨相邻期, 不抽稀)
    for (size_t t = 0; t < VR; ++t) {
      if ((t & 15) == 0 && cancel.load(std::memory_order_relaxed))
        return;
      const feature_storage_t *sa = b.day.data.data() + (t * 3 + 0) * A;
      const feature_storage_t *sb = b.day.data.data() + (t * 3 + 1) * A;
      const feature_storage_t *sm = b.day.data.data() + (t * 3 + 2) * A;
      feature_storage_t *da = b.raw_a.data() + t * A, *db = b.raw_b.data() + t * A;
      for (size_t a = 0; a < A; ++a) {
        const float g = static_cast<float>(sm[a]);
        const float va = static_cast<float>(sa[a]), vb = static_cast<float>(sb[a]);
        da[a] = (fmeta::valid(g, vt_a_) && std::isfinite(va)) ? sa[a] : kInvalid16;
        db[a] = (fmeta::valid(g, vt_b_) && std::isfinite(vb)) ? sb[a] : kInvalid16;
      }
      b.ok_a[t] = rank_slice(b.raw_a.data() + t * A, A, b.key.data(), b.Ra.data() + t * A,
                             b.Ma.data() + t * A, b.R2a.data() + t * A)
                      ? 1
                      : 0;
      b.ok_b[t] = rank_slice(b.raw_b.data() + t * A, A, b.key.data(), b.Rb.data() + t * A,
                             b.Mb.data() + t * A, b.R2b.data() + t * A)
                      ? 1
                      : 0;
    }

    // 逐 lag 累加: ρ(lag) = corr(A(t), B(t + lag)), lag > 0 = A 领先 B
    for (size_t k = 0; k < kCorrLags; ++k) {
      if (cancel.load(std::memory_order_relaxed))
        return;
      const int lag = lag_of(k);
      const size_t t0 = lag < 0 ? static_cast<size_t>(-lag) : 0;
      const size_t t1 = lag < 0 ? VR : VR - static_cast<size_t>(lag);
      for (size_t t = t0; t < t1; ++t) {
        const size_t tb = static_cast<size_t>(static_cast<ptrdiff_t>(t) + lag);
        if (!b.ok_a[t] || !b.ok_b[tb])
          continue;
        const float *Ri = b.Ra.data() + t * A, *Mi = b.Ma.data() + t * A;
        const float *Ri2 = b.R2a.data() + t * A;
        const float *Rj = b.Rb.data() + tb * A, *Mj = b.Mb.data() + tb * A;
        const float *Rj2 = b.R2b.data() + tb * A;
        float axy = 0, ax = 0, ay = 0, axx = 0, ayy = 0, an = 0;
        {
          CORR_FP_REASSOC
          for (size_t a = 0; a < A; ++a) {
            axy += Ri[a] * Rj[a];
            ax += Ri[a] * Mj[a];
            ay += Rj[a] * Mi[a];
            axx += Ri2[a] * Mj[a];
            ayy += Rj2[a] * Mi[a];
            an += Mi[a] * Mj[a];
          }
        }
        b.sxy[k] += axy;
        b.sx[k] += ax;
        b.sy[k] += ay;
        b.sxx[k] += axx;
        b.syy[k] += ayy;
        b.np[k] += an;
      }
    }
    publish_progress(1); // 只推进度; 曲线末尾一次成型 (UI 也只在 Done 时画)
  });

  if (cancel.load(std::memory_order_relaxed))
    return false;

  // 归约 + 发布
  std::array<double, kCorrLags> sxy{}, sx{}, sy{}, sxx{}, syy{}, np{};
  for (const Runtime::DayBuf &b : rt.bufs)
    for (size_t k = 0; k < kCorrLags; ++k) {
      sxy[k] += b.sxy[k];
      sx[k] += b.sx[k];
      sy[k] += b.sy[k];
      sxx[k] += b.sxx[k];
      syy[k] += b.syy[k];
      np[k] += b.np[k];
    }
  {
    std::lock_guard<std::mutex> lock(mutex);
    for (size_t k = 0; k < kCorrLags; ++k) {
      rho[k] = pair_rho(sxy[k], sx[k], sy[k], sxx[k], syy[k], np[k]);
      pair_n[k] = static_cast<uint32_t>(np[k]);
    }
  }
  epoch.fetch_add(1, std::memory_order_release);
  return true;
}

void CorrPair::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  rt_.reset();
  col_a = col_b = UINT32_MAX;
  rho.fill(kNaN);
  pair_n.fill(0);
  months_.clear();
  A_ = 0;
  level = 0;
  reset_idle();
}

// ============================================================================
// CorrLag::Runtime — 全矩阵 lead-lag 峭点 (口径见 Correlation.hpp)
// ============================================================================

// 上三角 (i < j) 紧凑下标: 累加器按对存, 不浪费一半
static inline size_t upper_index(size_t i, size_t j, size_t F) {
  assert(i < j && j < F);
  return i * F - i * (i + 1) / 2 + (j - i - 1);
}

struct CorrLag::Runtime {
  SlicePlane plane; // S = kCorrLagWindow 个连续切片
  size_t P = 0;     // 对数 F(F-1)/2

  std::vector<double> sxy, np; // [P][kCorrLags]
  std::vector<int8_t> pub_lag; // [F][F] 发布 staging
  std::vector<float> pub_rho;

  void prepare(size_t F, size_t A, size_t level) {
    TraceN("CorrLagPrepare");
    assert(level_valid_rows(level) >= kCorrLagWindow);
    plane.prepare(F, A, kCorrLagWindow, level);
    P = F * (F - 1) / 2;
    sxy.assign(P * kCorrLags, 0.0);
    np.assign(P * kCorrLags, 0.0);
    pub_lag.assign(F * F, 0);
    pub_rho.assign(F * F, kNaN);
  }
};

CorrLag::CorrLag() = default;
CorrLag::~CorrLag() = default;

void CorrLag::reset_for_build(size_t lvl, std::vector<uint32_t> col_list,
                              std::vector<L2::ValidType> valid_types, size_t meta_col,
                              std::vector<std::string> month_keys, size_t n_assets) {
  TraceN("CorrLagReset");
  assert(col_list.size() >= 2 && col_list.size() == valid_types.size());
  assert(std::is_sorted(col_list.begin(), col_list.end()));
  assert(lvl < LEVEL_COUNT && n_assets > 0 && !month_keys.empty());
  std::lock_guard<std::mutex> lock(mutex);

  const size_t F = col_list.size();
  level = lvl;
  cols = std::move(col_list);
  valid_types_ = std::move(valid_types);
  meta_col_ = meta_col;
  months_ = std::move(month_keys);
  A_ = n_assets;
  lag_best.assign(F * F, 0);
  rho_best.assign(F * F, kNaN);
  if (!rt_)
    rt_ = std::make_unique<Runtime>();
  begin_build();
}

bool CorrLag::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("CorrLagBuild");
  assert(rt_ && A_ > 0 && cols.size() >= 2 && "reset_for_build 先于 build");
  Runtime &rt = *rt_;

  const std::vector<std::string> dates = sample_days(reader, months_); // 与矩阵同一套抽样日
  if (dates.empty())
    return true;
  total.store(dates.size(), std::memory_order_release);

  const size_t F = cols.size();
  rt.prepare(F, A_, level);
  SlicePlane &pl = rt.plane;
  const size_t W = pl.S, A = pl.A, VR = pl.VR;

  // 窗起点按天等距铺满日内 (抽样日本身已洗牌, 起点序无需再随机): 跨天覆盖全部日内位置
  const auto set_slices = [&](SlicePlane::RawBuf &b, size_t d) {
    const size_t start = d * (VR - W + 1) / dates.size();
    for (size_t s = 0; s < W; ++s)
      b.slice_t[s] = start + s;
  };
  const auto load = [&](SlicePlane::RawBuf &b, size_t d) {
    pl.load(b, reader, dates[d], cols, valid_types_, meta_col_, cancel);
  };
  set_slices(pl.bufs[pl.cur], 0);
  load(pl.bufs[pl.cur], 0);

  for (size_t d = 0; d < dates.size(); ++d) {
    if (cancel.load(std::memory_order_relaxed))
      return false;

    std::thread prefetch; // 预读下一天, 与本天计算并发 (同 Correlation::build)
    if (d + 1 < dates.size()) {
      set_slices(pl.next(), d + 1);
      prefetch = std::thread([&, d] { load(pl.next(), d + 1); });
    }

    pl.rank(cancel);

    // 累加 (并行抢行 i; 行 i 的全部 (i, j > i) × lag 只被一个线程写).
    // 每 lag 把窗内所有 (t, t + lag) 切片对都用上; 只累 Σ R_i R_j 与 Σ m_i m_j (见头注释)
    {
      TraceN("CorrLagAccum");
      parallel_for(F, pl.n_threads, cancel, [&](size_t i, size_t /*tid*/) {
        for (size_t k = 0; k < kCorrLags; ++k) {
          const int lag = CorrPair::lag_of(k);
          const size_t t0 = lag < 0 ? static_cast<size_t>(-lag) : 0;
          const size_t t1 = lag < 0 ? W : W - static_cast<size_t>(lag);
          for (size_t t = t0; t < t1; ++t) {
            if (!pl.ok(t)[i])
              continue;
            const size_t tb = static_cast<size_t>(static_cast<ptrdiff_t>(t) + lag);
            const float *Ri = pl.Rs(t) + i * A, *Mi = pl.Ms(t) + i * A;
            const uint8_t *okb = pl.ok(tb);
            const float *Rb = pl.Rs(tb), *Mb = pl.Ms(tb);
            for (size_t j = i + 1; j < F; ++j) {
              if (!okb[j])
                continue;
              const float *Rj = Rb + j * A, *Mj = Mb + j * A;
              float axy = 0, an = 0;
              {
                CORR_FP_REASSOC
                for (size_t a = 0; a < A; ++a) {
                  axy += Ri[a] * Rj[a];
                  an += Mi[a] * Mj[a];
                }
              }
              const size_t o = upper_index(i, j, F) * kCorrLags + k;
              rt.sxy[o] += axy;
              rt.np[o] += an;
            }
          }
        }
      });
    }

    if (prefetch.joinable())
      prefetch.join();
    if (cancel.load(std::memory_order_relaxed))
      return false;

    // 日末发布: 每对取 |ρ| 最大的 lag; 反对称填转置位
    {
      TraceN("CorrLagPublish");
      for (size_t i = 0; i < F; ++i) {
        rt.pub_lag[i * F + i] = 0;
        rt.pub_rho[i * F + i] = 1.0f;
        for (size_t j = i + 1; j < F; ++j) {
          const double *sxy = rt.sxy.data() + upper_index(i, j, F) * kCorrLags;
          const double *np = rt.np.data() + upper_index(i, j, F) * kCorrLags;
          int best_k = -1;
          double best_r = 0.0;
          // 按 |lag| 递增扫 (0, -1, +1, -2, +2, ...), 平局留先到的 = 更靠近 0:
          // 慢变列曲线平坦, 不该被浮点噪声指到两端
          for (size_t step = 0; step <= kCorrMaxLag; ++step) {
            for (int sgn = -1; sgn <= 1; sgn += 2) {
              if (step == 0 && sgn > 0)
                continue;
              const size_t k =
                  static_cast<size_t>(static_cast<int>(kCorrMaxLag) + sgn * static_cast<int>(step));
              if (np[k] < static_cast<double>(kCorrMinPairN))
                continue;
              const double r = std::clamp(sxy[k] / np[k], -1.0, 1.0);
              if (best_k < 0 || std::fabs(r) > std::fabs(best_r)) {
                best_k = static_cast<int>(k);
                best_r = r;
              }
            }
          }
          int8_t lag = 0;
          float r = kNaN;
          if (best_k >= 0) {
            lag = static_cast<int8_t>(CorrPair::lag_of(static_cast<size_t>(best_k)));
            r = static_cast<float>(best_r);
          }
          rt.pub_lag[i * F + j] = lag;
          rt.pub_lag[j * F + i] = static_cast<int8_t>(-lag);
          rt.pub_rho[i * F + j] = rt.pub_rho[j * F + i] = r;
        }
      }
      {
        std::lock_guard<std::mutex> lock(mutex);
        lag_best.swap(rt.pub_lag);
        rho_best.swap(rt.pub_rho);
      }
      publish_progress(1);
    }
    pl.flip();
  }

  return !cancel.load(std::memory_order_relaxed);
}

void CorrLag::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  cols = std::vector<uint32_t>{};
  lag_best = std::vector<int8_t>{};
  rho_best = std::vector<float>{};
  rt_.reset();
  valid_types_.clear();
  months_.clear();
  A_ = 0;
  level = 0;
  reset_idle();
}
