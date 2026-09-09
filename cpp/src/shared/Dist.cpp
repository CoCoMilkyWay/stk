#include "shared/Dist.hpp"
#include "features/Backend/DayBatchPlane.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "misc/date.hpp"
#include "misc/profiler.hpp"

#include <barrier>
#include <cmath>
#include <thread>

using namespace analysis;

namespace {

// ============================================================================
// W2 偏移 (发布侧派生): 均值校准的 Wasserstein-L2 偏移距离, 相对上一批末的
// 全局分位参考 —— 随批次推进逐批收敛, UI 只消费成品.
// ============================================================================

struct W2Ref {
  std::array<float, kW2Deciles> q{};
  float mean = 0.0f;
  bool valid = false;
};

// 分位查询: exportICDF 的 u 网格等距 → 直接定址 + 线性插值 (免二分)
float quantile_at(const KLLcache::LinePtr &icdf, double q) {
  assert(icdf.n >= 2);
  const double u0 = icdf.x[0], u1 = icdf.x[icdf.n - 1];
  if (q <= u0)
    return icdf.y[0];
  if (q >= u1)
    return icdf.y[icdf.n - 1];
  const double f = (q - u0) / (u1 - u0) * static_cast<double>(icdf.n - 1);
  const size_t lo = static_cast<size_t>(f);
  const double t = f - static_cast<double>(lo);
  return static_cast<float>(icdf.y[lo] + t * (icdf.y[lo + 1] - icdf.y[lo]));
}

float compute_w2(const KLLcache::LinePtr &icdf, float mean, const W2Ref &ref) {
  const float shift = mean - ref.mean;
  float sum_sq = 0.0f;
  for (int d = 0; d < kW2Deciles; ++d) {
    const float qi = quantile_at(icdf, 0.05 * (d + 1));
    const float diff = (qi - shift) - ref.q[d];
    sum_sq += diff * diff;
  }
  return std::sqrt(sum_sq / kW2Deciles);
}

// 聚合槽成线的最少样本 (小面板, 有形即画)
constexpr size_t kMinAggSamples = 10;

void fill_slots(std::vector<AggPdf> &snap, const std::vector<KLLcache> &slots) {
  assert(snap.size() == slots.size());
  for (size_t i = 0; i < slots.size(); ++i)
    snap[i].fill(slots[i], kMinAggSamples);
}

void merge_slots(std::vector<KLLcache> &dst, const std::vector<KLLcache> &src) {
  assert(dst.size() == src.size());
  for (size_t i = 0; i < dst.size(); ++i)
    dst[i].mergeWith(src[i]);
}

} // namespace

// ============================================================================
// Runtime: worker 私有状态 (UI 不看)
// ============================================================================

struct Dist::Runtime {
  struct DayGroup {
    uint32_t begin, end;
    uint16_t month;
    uint8_t weekday;
  }; // shard.agg_samples 按天切片 → months / by_weekday
  struct TodRun {
    uint32_t begin, end;
    uint8_t bin;
  }; // 同日内桶连续段 → by_tod

  // 每线程私有: 扫描缓冲 + 聚合槽副本. 重活全在锁外做完; 批末两两归约到 shard 0.
  // shard 0 的聚合槽即全程累加器 (归约后不清), 其余 shard 每批归约后清零.
  struct Shard {
    std::vector<float> samples;     // Phase 扫描: 单资产本批全量样本 → 该资产 sketch
    std::vector<float> agg_samples; // Phase 扫描: stride 抽样样本 → 聚合槽 (下面两表索引它)
    std::vector<DayGroup> day_groups;
    std::vector<TodRun> tod_runs;
    std::vector<KLLcache> months;     // [n_months]
    std::vector<KLLcache> by_weekday; // [7]
    std::vector<KLLcache> by_tod;     // [kTodBins]
    KLLcache global{kAggKllCapacity, kAggKllResolution};
    Integrity integrity;

    void merge_from(const Shard &o) {
      merge_slots(months, o.months);
      merge_slots(by_weekday, o.by_weekday);
      merge_slots(by_tod, o.by_tod);
      global.mergeWith(o.global);
      integrity.add(o.integrity);
    }
    void clear_agg() {
      clear_slots(months);
      clear_slots(by_weekday);
      clear_slots(by_tod);
      global.clear();
      integrity.clear();
    }
  };

  DayBatchPlane plane;                  // [A][批天][分钟] 资产主序批平面 (f16) + IO 暂存
  std::vector<KLLcache> asset_klls;     // [A] 每资产累积 sketch
  std::vector<AssetLine> lines_staging; // [A] 扫描线程各写各槽, 批末与 lines 交换
  std::vector<AggPdf> months_snap;      // [n_months] 发布 staging (锁外填, 短锁 swap)
  std::vector<AggPdf> weekday_snap;     // [7]
  std::vector<AggPdf> tod_snap;         // [kTodBins]
  W2Ref w2_ref;                         // 上一批末的全局分位参考 (completion 更新, 扫描只读)
  std::vector<Shard> shards;            // [n_threads]

  // 容量准备 (幂等, 尺寸对得上零分配), 返回线程数
  size_t prepare(size_t A, size_t n_months, size_t n_cols, size_t agg_stride) {
    TraceN("DistPrepare"); // 首帧账目: 批平面 + 全资产 sketch + n_threads × 聚合 sketch
    const auto [n_threads, n_io] = thread_layout(A);
    const size_t asset_stride = kDaysPerBatch * kVR;
    plane.prepare(A, kLevel, kDaysPerBatch, 1, n_io, n_cols);
    prepare_slots(asset_klls, A, kAssetKllCapacity, kAssetKllResolution);
    lines_staging.assign(A, AssetLine{});
    months_snap.assign(n_months, AggPdf{});
    weekday_snap.assign(7, AggPdf{});
    tod_snap.assign(kTodBins, AggPdf{});
    w2_ref = W2Ref{};
    shards.resize(n_threads);
    for (Shard &sh : shards) {
      sh.samples.reserve(asset_stride);
      sh.agg_samples.reserve(asset_stride / agg_stride + 1);
      sh.day_groups.reserve(kDaysPerBatch);
      sh.tod_runs.reserve(kDaysPerBatch * kTodBins); // 每天最多 kTodBins 段
      prepare_slots(sh.months, n_months, kAggKllCapacity, kAggKllResolution);
      prepare_slots(sh.by_weekday, 7, kAggKllCapacity, kAggKllResolution);
      prepare_slots(sh.by_tod, kTodBins, kAggKllCapacity, kAggKllResolution);
      sh.global.clear();
      sh.integrity.clear();
    }
    return n_threads;
  }
};

Dist::Dist() = default;
Dist::~Dist() = default;

// ============================================================================
// Reset
// ============================================================================

void Dist::reset_for_build(std::vector<size_t> cols, std::vector<std::string> month_keys,
                           std::vector<uint32_t> global_ids) {
  TraceN("DistReset");
  assert(!cols.empty() && cols.size() <= 2 && "Dist 只接受值列 + 可选 valid 列");
  check_axis(global_ids);
  std::lock_guard<std::mutex> lock(mutex);

  columns_ = std::move(cols);
  months_ = std::move(month_keys);
  const bool axis_changed = global_ids_ != global_ids;
  global_ids_ = std::move(global_ids);
  const size_t A = global_ids_.size(); // A 轴 = universe 子轴

  // 展示字段一律不清: 旧图留住, 首批 publish 整体覆盖. universe / 区间变了 (槽位含义已变) 才重建
  if (lines.size() != A || axis_changed)
    lines.assign(A, AssetLine{});
  init_lines(lines, global_ids_);
  if (months.size() != months_.size())
    months.assign(months_.size(), AggPdf{});
  if (by_weekday.size() != 7)
    by_weekday.assign(7, AggPdf{});
  if (by_tod.size() != kTodBins)
    by_tod.assign(kTodBins, AggPdf{});
  if (!rt_)
    rt_ = std::make_unique<Runtime>();

  agg_stride.store(1, std::memory_order_relaxed);
  begin_build();
}

// ============================================================================
// Build (分批流式: 每批 IO → 扫描 → 归约 → 发布, 首帧与总区间长度无关)
// ============================================================================

bool Dist::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("DistBuild");
  assert(rt_ && "reset_for_build 先于 build");
  Runtime &rt = *rt_;
  const size_t A = lines.size(); // universe 子轴大小 (扫描/抽样预算/分母全按它)
  const size_t VR = kVR;
  assert(level_valid_rows(kLevel) == VR);
  assert(A > 0 && A == global_ids_.size() && "reset_for_build 先于 build");
  const size_t n_cols = columns_.size();
  const bool has_valid = (n_cols > 1);
  const size_t n_months = months_.size();

  // 预计算: 分钟 → 日内桶 (L1)
  std::array<uint8_t, kVR> tod_lut{};
  for (size_t t = 0; t < VR; ++t)
    tod_lut[t] = static_cast<uint8_t>(tod_bin_of(t));

  // ==========================================================================
  // 日期枚举 + 自适应日抽样: 抽样天表 = 日期 → (星期, 月下标)
  // ==========================================================================
  DateList all = enumerate_dates(reader, months_);
  if (all.dates.empty())
    return true;

  // 日抽样: 总样本预算 (分批后平面只存一批, 内存不再约束天数; 预算只是总时长旋钮)
  size_t stride = (all.dates.size() * A * VR + kMaxTotalSamples - 1) / kMaxTotalSamples;
  if (stride % 5 == 0)
    ++stride; // 与交易周互质, 避免星期偏置

  std::vector<std::string> dates;
  std::vector<uint8_t> weekdays;
  std::vector<uint16_t> day_month;
  for (size_t i = 0; i < all.dates.size(); i += stride) {
    weekdays.push_back(static_cast<uint8_t>(misc::weekday_of(all.dates[i])));
    day_month.push_back(all.month[i]);
    dates.push_back(std::move(all.dates[i]));
  }
  const size_t n_sel = dates.size();
  total.store(n_sel, std::memory_order_release);

  // 聚合槽抽样 stride: 按总格子数 (有效样本的上界) 折到 kAggTargetSamples 量级.
  // 区间小 → stride=1, 全量进聚合槽, 小数据集下不会被抽到低于 kMinSamples.
  size_t agg_stride = std::max<size_t>(1, (n_sel * A * VR) / kAggTargetSamples);
  while (agg_stride > 1 && VR % agg_stride == 0)
    ++agg_stride; // 与日内分钟数互质: 整除会让每天固定落在同一批分钟上, 扭曲日内分布
  this->agg_stride.store(agg_stride, std::memory_order_release);

  const size_t n_threads = rt.prepare(A, n_months, n_cols, agg_stride);
  const size_t n_io = std::min(n_threads, kDaysPerBatch);
  rt.lines_staging = lines; // asset / draw 标记随之带过去

  // ==========================================================================
  // 批循环: 一波常驻线程, 每批三道栅栏 (IO 完成 → 扫描完成 → 归约完成).
  // 批末发布走归约栅栏的 completion (标准保证在所有线程到齐后、解除阻塞前
  // 由单线程执行 → stop/进度/抢任务原子对所有线程一致可见)
  // ==========================================================================
  std::atomic<size_t> next_day{0};
  std::atomic<size_t> next_block{0};
  bool stop = false;
  size_t pub_begin = 0;               // completion 私有推进 (每批恰好执行一次, 串行)
  Runtime::Shard &acc = rt.shards[0]; // 归约终点 = 全程累加器

  auto publish = [&]() noexcept {
    TraceN("Publish");
    next_day.store(0, std::memory_order_relaxed);
    next_block.store(0, std::memory_order_relaxed);
    if (cancel.load(std::memory_order_relaxed)) {
      stop = true; // 半批不发布
      return;
    }
    const size_t bd = std::min(kDaysPerBatch, n_sel - pub_begin);
    acc.integrity.n_nan += rt.plane.take_nan_seen();

    // 下一批的 W2 参考 = 本批后的全局分位 (滞后一批, 逐批收敛)
    const bool had_ref = rt.w2_ref.valid;
    rt.w2_ref = W2Ref{};
    if (acc.global.totalCount() >= kMinSamples) {
      const auto icdf = acc.global.exportICDF();
      for (int d = 0; d < kW2Deciles; ++d)
        rt.w2_ref.q[d] = quantile_at(icdf, 0.05 * (d + 1));
      rt.w2_ref.mean = static_cast<float>(acc.global.mean());
      rt.w2_ref.valid = true;
    }
    // 首个有参考的批: 本批扫描时还没参考 (w2 全 -1), 用刚建好的参考就地补算一次,
    // 否则单批区间 (天数 ≤ kDaysPerBatch) 永远没有散点. 一次性 A 次 exportICDF, 后续批走滞后路径
    if (!had_ref && rt.w2_ref.valid) {
      TraceN("W2Backfill");
      for (size_t a = 0; a < A; ++a) {
        AssetLine &ln = rt.lines_staging[a];
        if (ln.n_pts > 0)
          ln.w2 = compute_w2(rt.asset_klls[a].exportICDF(), ln.mean, rt.w2_ref);
      }
    }
    // 聚合槽成品 (锁外导出)
    fill_slots(rt.months_snap, acc.months);
    fill_slots(rt.weekday_snap, acc.by_weekday);
    fill_slots(rt.tod_snap, acc.by_tod);
    AggPdf global_snap;
    global_snap.fill(acc.global, kMinAggSamples);
    {
      std::lock_guard<std::mutex> lock(mutex); // 短锁: 整体换新, 中间没有空态
      lines.swap(rt.lines_staging);
      months.swap(rt.months_snap);
      by_weekday.swap(rt.weekday_snap);
      by_tod.swap(rt.tod_snap);
      global = global_snap;
      integrity = acc.integrity;
    }
    publish_progress(bd);
    pub_begin += kDaysPerBatch;
  };

  std::barrier io_done(static_cast<ptrdiff_t>(n_threads));
  std::barrier scan_done(static_cast<ptrdiff_t>(n_threads));
  std::barrier reduce_step(static_cast<ptrdiff_t>(n_threads));
  std::barrier reduce_done(static_cast<ptrdiff_t>(n_threads), publish);

  auto worker = [&](size_t tid) {
    Runtime::Shard &sh = rt.shards[tid];
    for (size_t b0 = 0; b0 < n_sel && !stop; b0 += kDaysPerBatch) {
      const size_t bd = std::min(kDaysPerBatch, n_sel - b0);

      // ------------------------------------------------------------------
      // Phase IO: 抢单天载入 → 转置进批平面 (天与天写不同段, 无重叠; 门控/NaN 分账在 plane 内)
      // L1 门控只有 DATA 语义 (_meta 非 0; 编码见 Meta.hpp)
      // ------------------------------------------------------------------
      if (tid < n_io) {
        for (;;) {
          const size_t j = next_day.fetch_add(1, std::memory_order_relaxed);
          if (j >= bd || cancel.load(std::memory_order_relaxed))
            break;
          rt.plane.load_day(reader, dates[b0 + j], columns_, has_valid, L2::ValidType::DATA, j, tid);
        }
      }
      io_done.arrive_and_wait();

      // ------------------------------------------------------------------
      // Phase 扫描: 抢 kAssetBlock 个资产一块 (a = 子轴下标), 全在锁外, 聚合进私有 shard
      // ------------------------------------------------------------------
      for (;;) {
        const size_t k0 = next_block.fetch_add(kAssetBlock, std::memory_order_relaxed);
        if (k0 >= A || cancel.load(std::memory_order_relaxed))
          break;
        const size_t k1 = std::min(k0 + kAssetBlock, A);
        TraceN("ScanBlock");

        for (size_t a = k0; a < k1; ++a) {
          sh.samples.clear();
          sh.agg_samples.clear();
          sh.day_groups.clear();
          sh.tod_runs.clear();

          int cur_bin = -1;       // 日内桶 run 跨天延续 (同桶连续样本即一段)
          uint32_t run_begin = 0; // 索引 agg_samples
          size_t agg_tick = 0;

          for (size_t i = 0; i < bd; ++i) {
            const feature_storage_t *p = rt.plane.series(0, a, i);
            const uint32_t day_begin = static_cast<uint32_t>(sh.agg_samples.size());

            for (size_t t = 0; t < VR; ++t) {
              const float v = static_cast<float>(p[t]);
              if (v != v) // 哨兵: valid 不过 或 真 NaN (已在 Phase IO 分账)
                continue;
              if (std::isinf(v)) {
                ++(v > 0.0f ? sh.integrity.n_pos_inf : sh.integrity.n_neg_inf);
                continue;
              }
              sh.integrity.add_finite(v);
              sh.samples.push_back(v); // 全量 → 该资产私有 sketch

              if (++agg_tick < agg_stride) // 聚合槽只吃 stride 抽样
                continue;
              agg_tick = 0;
              const int b = tod_lut[t];
              if (b != cur_bin) {
                if (cur_bin >= 0)
                  sh.tod_runs.push_back({run_begin, static_cast<uint32_t>(sh.agg_samples.size()),
                                         static_cast<uint8_t>(cur_bin)});
                cur_bin = b;
                run_begin = static_cast<uint32_t>(sh.agg_samples.size());
              }
              sh.agg_samples.push_back(v);
            }

            if (sh.agg_samples.size() > day_begin)
              sh.day_groups.push_back({day_begin, static_cast<uint32_t>(sh.agg_samples.size()),
                                       day_month[b0 + i], weekdays[b0 + i]});
          }
          if (cur_bin >= 0)
            sh.tod_runs.push_back({run_begin, static_cast<uint32_t>(sh.agg_samples.size()),
                                   static_cast<uint8_t>(cur_bin)});

          sh.integrity.n_total += bd * VR;

          // 聚合槽: 私有副本吃切片 (零拷贝)
          if (!sh.agg_samples.empty()) {
            const float *s = sh.agg_samples.data();
            sh.global.addBatch(s, sh.agg_samples.size());
            for (const auto &g : sh.day_groups) {
              sh.months[g.month].addBatch(s + g.begin, g.end - g.begin);
              sh.by_weekday[g.weekday].addBatch(s + g.begin, g.end - g.begin);
            }
            for (const auto &r : sh.tod_runs)
              sh.by_tod[r.bin].addBatch(s + r.begin, r.end - r.begin);
          }

          // 每资产: 累积 sketch + 导出整条线到 staging (槽位 == 资产下标).
          // asset_klls/lines_staging 是 worker 私有且每资产单线程 → 全程无锁
          KLLcache &kll = rt.asset_klls[a];
          if (!sh.samples.empty())
            kll.addBatch(sh.samples.data(), sh.samples.size());
          AssetLine &ln = rt.lines_staging[a];
          ln.fill(kll, kMinAssetSamples);
          ln.w2 = (ln.n_pts > 0 && rt.w2_ref.valid) ? compute_w2(kll.exportICDF(), ln.mean, rt.w2_ref) : -1.0f;
        }
      }
      scan_done.arrive_and_wait();

      // ------------------------------------------------------------------
      // Phase 归约: shard 两两并行归约到 shard 0 (log2 步, 每步一道栅栏), 全程锁外.
      // 归约后 shard 0 = 全程累加器 (不清), 其余清零供下批
      // ------------------------------------------------------------------
      for (size_t step = 1; step < n_threads; step <<= 1) {
        if ((tid & (2 * step - 1)) == 0 && tid + step < n_threads) {
          TraceN("Reduce");
          Runtime::Shard &src = rt.shards[tid + step];
          sh.merge_from(src);
          src.clear_agg();
        }
        reduce_step.arrive_and_wait();
      }
      reduce_done.arrive_and_wait(); // → publish() (单线程), 重置抢任务原子供下批
    }
  };

  {
    std::vector<std::thread> threads;
    threads.reserve(n_threads);
    for (size_t t = 0; t < n_threads; ++t)
      threads.emplace_back(worker, t);
    for (auto &th : threads)
      th.join();
  }

  return !cancel.load(std::memory_order_relaxed);
}

// ============================================================================
// Clear
// ============================================================================

void Dist::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存
  months = std::vector<AggPdf>{};
  by_weekday = std::vector<AggPdf>{};
  by_tod = std::vector<AggPdf>{};
  global = AggPdf{};
  lines = std::vector<AssetLine>{};
  integrity.clear();
  rt_.reset(); // worker 已 join, 整体释放
  columns_.clear();
  months_.clear();
  global_ids_ = std::vector<uint32_t>{};
  agg_stride.store(1, std::memory_order_relaxed);
  reset_idle();
}
