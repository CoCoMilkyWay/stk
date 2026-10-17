#include "shared/Dist.hpp"
#include "features/Storage/DayBatchPlane.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "misc/profiler.hpp"

#include <cmath>

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

  Prefetch<TsPlane> pf;      // [A][批天][分钟] 资产主序批平面 ×2 (f16) + io 池
  AssetSketches assets;      // [A] 每资产累积 sketch
  Snap pub;                  // 发布 staging (扫描线程各写各资产线, 批末导出聚合槽; 短锁与 snap swap)
  W2Ref w2_ref;              // 上一批末的全局分位参考 (批末发布更新, 扫描只读)
  std::vector<Shard> shards; // [n_threads] 按 tid
  std::unique_ptr<misc::exec::ForkJoin> ex;

  // 容量准备 (幂等, 尺寸对得上零分配), 返回计算执行器
  misc::exec::ForkJoin &prepare(size_t A, size_t n_months, size_t n_io_cols, size_t agg_stride) {
    TraceN("DistPrepare"); // 首帧账目: 批平面 + 全资产 sketch + n_threads × 聚合 sketch
    const size_t n_threads = threads_for(n_asset_blocks(A));
    const size_t asset_stride = kDaysPerBatch * kVR;
    pf.prepare(threads_for(kDaysPerBatch), A, kLevel, n_io_cols);
    for (TsPlane &p : pf.buf)
      p.prepare(A, kLevel, kDaysPerBatch, 1);
    assets.prepare(A);
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
    return misc::exec::ensure(ex, static_cast<int>(n_threads));
  }
};

Dist::Dist() = default;
Dist::~Dist() = default;

// ============================================================================
// Reset
// ============================================================================

void Dist::reset(ReadScope scope, Spec sp) {
  TraceN("DistReset");
  assert(sp.cols.n() == 1 && "Dist 只接受单值列 (+ 门控)");
  check_axis(scope.lib.uni.ids);
  std::lock_guard<std::mutex> lock(mutex);

  const bool axis_changed = scope_.lib.uni.ids != scope.lib.uni.ids;
  scope_ = std::move(scope);
  spec = std::move(sp);
  const size_t A = scope_.lib.uni.size(); // A 轴 = universe 子轴

  // 展示字段一律不清: 旧图留住, 首批 publish 整体覆盖. universe / 区间变了 (槽位含义已变) 才重建
  if (snap.lines.size() != A || axis_changed)
    snap.lines.assign(A, AssetLine{});
  init_lines(snap.lines, scope_.lib.uni.ids);
  if (snap.months.size() != scope_.months.size())
    snap.months.assign(scope_.months.size(), AggPdf{});
  if (snap.by_weekday.size() != 7)
    snap.by_weekday.assign(7, AggPdf{});
  if (snap.by_tod.size() != kTodBins)
    snap.by_tod.assign(kTodBins, AggPdf{});
  if (!rt_)
    rt_ = std::make_unique<Runtime>();
  publish();
}

// ============================================================================
// Build (分批流式: 每批 IO → 扫描 → 归约 → 发布, 首帧与总区间长度无关)
// ============================================================================

bool Dist::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("DistBuild");
  assert(rt_ && "reset 先于 build");
  Runtime &rt = *rt_;
  const size_t A = scope_.lib.uni.size(); // universe 子轴大小 (扫描/抽样预算/分母全按它)
  const size_t VR = kVR;
  assert(level_valid_rows(kLevel) == VR);
  assert(A > 0 && snap.lines.size() == A && "reset 先于 build");
  const fmeta::ColumnSet &cols = spec.cols;
  const size_t n_months = scope_.months.size();

  // 预计算: 分钟 → 日内桶 (L1)
  std::array<uint8_t, kVR> tod_lut{};
  for (size_t t = 0; t < VR; ++t)
    tod_lut[t] = static_cast<uint8_t>(tod_bin_of(t));

  // 天序: 洗牌后按总样本预算截断 (分批后平面只存一批, 内存不约束天数; 预算只是总时长旋钮)
  const DayPlan plan = plan_days(reader, scope_.months, DayOrder::Shuffle, std::max<size_t>(1, kMaxTotalSamples / (A * VR)));
  if (plan.size() == 0)
    return true;
  const size_t n_sel = plan.size();
  total.store(n_sel, std::memory_order_release);

  // 聚合槽抽样 stride: 按总格子数 (有效样本的上界) 折到 kAggTargetSamples 量级.
  // 区间小 → stride=1, 全量进聚合槽, 小数据集下不会被抽到低于 kMinSamples.
  size_t agg_stride = std::max<size_t>(1, (n_sel * A * VR) / kAggTargetSamples);
  while (agg_stride > 1 && VR % agg_stride == 0)
    ++agg_stride; // 与日内分钟数互质: 整除会让每天固定落在同一批分钟上, 扭曲日内分布

  misc::exec::ForkJoin &ex = rt.prepare(A, n_months, cols.io.size(), agg_stride);
  const size_t n_blocks = n_asset_blocks(A);
  rt.pub = snap; // 资产线的 asset / draw 标记 + 各槽尺寸随之带过去 (之后每批整体覆写)
  rt.pub.agg_stride = agg_stride;

  // ==========================================================================
  // 批循环: 下一批 IO 后台预读, 本批两相 (扫描 → 归约), 每相一次 dispatch (返回即栅栏);
  // 批末发布在本线程 (单线程, 天然串行)
  // ==========================================================================
  Runtime::Shard &acc = rt.shards[0]; // 归约终点 = 全程累加器

  auto publish_batch = [&](size_t bd) {
    TraceN("Publish");
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
        AssetLine &ln = rt.pub.lines[a];
        if (ln.n_pts > 0)
          ln.w2 = compute_w2(rt.assets.klls[a].exportICDF(), ln.mean, rt.w2_ref);
      }
    }
    // 聚合槽成品 (锁外导出)
    fill_slots(rt.pub.months, acc.months);
    fill_slots(rt.pub.by_weekday, acc.by_weekday);
    fill_slots(rt.pub.by_tod, acc.by_tod);
    rt.pub.global.fill(acc.global, kMinAggSamples);
    rt.pub.integrity = acc.integrity;
    {
      std::lock_guard<std::mutex> lock(mutex); // 短锁: 整体换新, 中间没有空态
      std::swap(snap, rt.pub);
    }
    publish(bd);
  };

  // 批 b0 的 IO (预读线程或本线程跑; 只写传入的平面)
  const auto load = [&](size_t b0) {
    return [&, b0](TsPlane &pl, misc::exec::ForkJoin &io, std::vector<IoSlot> &slots) {
      pl.load_batch(io, slots, reader, plan.dates.data() + b0, std::min(kDaysPerBatch, n_sel - b0), cols, cancel);
    };
  };
  rt.pf.now(load(0));

  for (size_t b0 = 0; b0 < n_sel; b0 += kDaysPerBatch) {
    const size_t bd = std::min(kDaysPerBatch, n_sel - b0);
    if (b0 + kDaysPerBatch < n_sel)
      rt.pf.issue(load(b0 + kDaysPerBatch)); // issue → wait 之间不许 return
    const TsPlane &pl = rt.pf.front();

    // ------------------------------------------------------------------
    // Phase 扫描: kAssetBlock 个资产一任务 (a = 子轴下标), 全在锁外, 聚合进本 tid 的 shard
    // ------------------------------------------------------------------
    run_phase(ex, n_blocks, cancel, [&](size_t blk, size_t tid) {
      Runtime::Shard &sh = rt.shards[tid];
      const auto [k0, k1] = asset_block(blk, A);
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
          const feature_storage_t *p = pl.series(0, a, i);
          const uint32_t day_begin = static_cast<uint32_t>(sh.agg_samples.size());

          for (size_t t = 0; t < VR; ++t) {
            if (!sh.integrity.add_cell(p[t])) // 哨兵 / ±inf: 按位分账后跳过
              continue;
            const float v = static_cast<float>(p[t]);
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
                                     plan.month[b0 + i], plan.weekday[b0 + i]});
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

        // 每资产: 累积 sketch + 导出整条线到 staging (槽位 == 资产下标; 每资产单线程 → 全程无锁)
        AssetLine &ln = rt.pub.lines[a];
        const KLLcache &kll = rt.assets.add(a, sh.samples.data(), sh.samples.size(), ln);
        ln.w2 = (ln.n_pts > 0 && rt.w2_ref.valid) ? compute_w2(kll.exportICDF(), ln.mean, rt.w2_ref) : -1.0f;
      }
    });

    // ------------------------------------------------------------------
    // Phase 归约: shard 两两并行归约到 shard 0, 全程锁外 (shard 0 = 全程累加器, 其余清零供下批)
    // ------------------------------------------------------------------
    reduce_tree(ex, rt.shards);

    rt.pf.wait();
    if (cancel.load(std::memory_order_relaxed))
      return false; // 半批不发布
    publish_batch(bd);
    rt.pf.flip();
  }
  return true;
}

// ============================================================================
// Clear
// ============================================================================

void Dist::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  snap = Snap{};
  spec = Spec{};
  rt_.reset(); // worker 已 join, 整体释放
  scope_ = ReadScope{};
  idle();
}
