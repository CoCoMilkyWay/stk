#include "shared/Transform.hpp"
#include "features/Storage/DayBatchPlane.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "math/stationary/ADF.hpp"
#include "math/stationary/KPSS.hpp"
#include "math/timeseries/AutoCorr.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <cmath>

using namespace analysis;

namespace {

// 相对 Nyquist 的归一化频率: 周期 (样本数) → f = 2 / period
float period_to_freq(float period) { return 2.0f / period; }

} // namespace

// ============================================================================
// Runtime: worker 私有状态 (UI 不看)
// ============================================================================

struct Transform::Runtime {
  static constexpr size_t kColBlock = 16; // CS 抢块粒度 (截面列数 = 批天 × VR)

  struct Plane {
    TsPlane main; // 主库 [特征][A][批天][VR]
    TsPlane side; // Bar 档旁读 [mcap, ind][A][批天][VR] (已按 uni.ids 收拢到主轴; NeutralRank 时)
    void clear() {
      main.clear();
      side.clear();
    }
  };
  Prefetch<Plane> pf;                            // 输入 f16 ×2 + io 池
  std::vector<IoSlot> side_slots;                // [io 线程数] All 全轴暂存 (NeutralRank 时)
  std::vector<float> out;                        // 输出 [A][批天][VR] float (NaN = 缺/预热)
  std::vector<math::stationary::TodProfile> tod; // [A] 跨天 TOD 轮廓 (season 开时)
  math::spectral::IIRCoeffs iir;                 // 带通系数 (一次设计)
  math::stationary::FFDWeights ffd;              // 分数差分权重 (一次算)
  cs::ColumnFn cs_fn = nullptr;

  AssetSketches assets;                      // [A] 每资产累积 sketch
  std::vector<StatLine> stats;               // [A] 全程累积 (每批拷进 pub.stat_lines; .asset 由 build 起手贴)
  Snap pub;                                  // 发布 staging (短锁与 snap swap)
  std::vector<float> series_raw, series_out; // 焦点资产全程序列 (首批发布时 swap 进 series, 之后经稳定指针续写)
  std::vector<float> tod_mean, tod_sd;       // [VR] 焦点资产 TOD 轮廓 staging (每批短锁拷进 series)

  // 每线程私有: 链中间量 + 批内累加器. 批末各 shard 并入 shard 0 (全程累加器, 不清), 其余清零
  struct Shard {
    std::vector<float> x, y, z;  // VR: 一天的稠密行 / 压实后的链中间量
    std::vector<uint16_t> idx;   // VR: 压实 → 行内下标
    std::vector<float> col;      // A: 截面列 dense 有效子集
    std::vector<uint32_t> col_a; // A: dense → 资产下标
    std::vector<float> logmc;    // A: NeutralRank 上下文
    std::vector<float> industry; // A
    std::vector<float> samples;  // 批天 × VR: 单资产本批全量有限输出 → sketch
    math::stationary::ADFWorkspace adf_ws;
    math::stationary::KPSSWorkspace kpss_ws;
    std::unique_ptr<DayPSD> day_psd; // 大对象 (FFT 工作区), 堆上
    math::timeseries::ACFWorkspace acf_ws;

    PsdAcc psd;
    std::array<double, kTfMaxLag + 1> acf_sum{}, pacf_sum{};
    uint64_t acf_n = 0;
    KLLcache global{kAggKllCapacity, kAggKllResolution};
    Integrity in, out;

    void merge_from(const Shard &o) {
      psd.merge_from(o.psd);
      for (size_t k = 0; k <= kTfMaxLag; ++k) {
        acf_sum[k] += o.acf_sum[k];
        pacf_sum[k] += o.pacf_sum[k];
      }
      acf_n += o.acf_n;
      global.mergeWith(o.global);
      in.add(o.in);
      out.add(o.out);
    }
    void clear_agg() {
      psd.clear();
      acf_sum.fill(0.0);
      pacf_sum.fill(0.0);
      acf_n = 0;
      global.clear();
      in.clear();
      out.clear();
    }
  };
  std::vector<Shard> shards; // [n_threads] 按 tid
  std::unique_ptr<misc::exec::ForkJoin> ex;

  // A = 库轴大小 (平面/out/sketch/统计/序列快照 尺寸; 线程数按扫描块数封顶), A_all = 全轴 (旁读暂存),
  // 返回计算执行器
  misc::exec::ForkJoin &prepare(size_t A, size_t A_all, const Spec &sp) {
    TraceN("TransformPrepare");
    const Params &p = sp.params;
    const size_t VR = kVR;
    const size_t n_threads = threads_for(n_asset_blocks(A));

    const size_t n_io = threads_for(kDaysPerBatch);
    pf.prepare(n_io, A, kLevel, sp.cols.io.size());
    for (Plane &pl : pf.buf) {
      pl.main.prepare(A, kLevel, kDaysPerBatch, sp.cols.n());
      if (p.cs_neutral())
        pl.side.prepare(A, kLevel, kDaysPerBatch, sp.side.n());
    }
    if (p.cs_neutral()) {
      side_slots.resize(n_io);
      for (IoSlot &s : side_slots)
        s.prepare(A_all, kLevel, sp.side.io.size());
    }
    out.resize(A * kDaysPerBatch * VR);

    if (p.season != Season::None) {
      tod.resize(A);
      for (auto &t : tod)
        t.reset(VR);
    } else {
      tod = std::vector<math::stationary::TodProfile>{};
    }
    if (p.bandpass) {
      assert(p.bp_lo_period < p.bp_hi_period);
      iir.compute(period_to_freq(p.bp_hi_period), period_to_freq(p.bp_lo_period), p.iir_order, p.iir_type);
    }
    if (p.stationary == Stationary::FracDiff)
      ffd.compute(p.st[0], static_cast<int>(p.st[1]));
    cs_fn = p.cs_enabled() ? cs::column_fn(p.cs_tf, p.cs_method) : nullptr;

    assets.prepare(A);
    stats.assign(A, StatLine{});

    shards.resize(n_threads);
    for (Shard &sh : shards) {
      sh.x.resize(VR);
      sh.y.resize(VR);
      sh.z.resize(VR);
      sh.idx.resize(VR);
      sh.col.resize(A);
      sh.col_a.resize(A);
      sh.logmc.resize(A);
      sh.industry.resize(A);
      sh.samples.reserve(kDaysPerBatch * VR);
      if (!sh.day_psd)
        sh.day_psd = std::make_unique<DayPSD>();
      sh.acf_ws.ensure(kTfMaxLag);
      sh.clear_agg();
    }
    return misc::exec::ensure(ex, static_cast<int>(n_threads));
  }
};

Transform::Transform() = default;
Transform::~Transform() = default;

// ============================================================================
// Reset
// ============================================================================

void Transform::reset(ReadScope scope, Spec sp) {
  TraceN("TransformReset");
  assert(sp.cols.n() == 1u);
  assert(sp.side.n() == (sp.params.cs_neutral() ? 2u : 0u));
  check_axis(scope.lib.axis.ids);
  assert(scope.bar.axis.size() > 0 && scope.lib.axis.ids.back() < scope.bar.axis.size());
  std::lock_guard<std::mutex> lock(mutex);

  const bool axis_changed = scope_.lib.axis.ids != scope.lib.axis.ids;
  scope_ = std::move(scope);
  spec = std::move(sp);
  const size_t A = scope_.lib.axis.size(); // A 轴 = 库轴
  assert(spec.focus < A && "序列焦点越界 (库轴下标)");

  // 展示字段一律不清: 旧图留住, 首批 publish 整体覆盖. 库轴变了 (槽位含义已变) 才重建
  if (snap.lines.size() != A || axis_changed)
    snap.lines.assign(A, AssetLine{});
  init_lines(snap.lines, scope_.lib.axis.ids);
  if (snap.stat_lines.size() != A || axis_changed)
    snap.stat_lines.assign(A, StatLine{});
  for (size_t a = 0; a < A; ++a)
    snap.stat_lines[a].asset = scope_.lib.axis.ids[a];
  if (!rt_)
    rt_ = std::make_unique<Runtime>();

  publish();
}

// ============================================================================
// Build
// ============================================================================

bool Transform::build(FeatureRead &reader, const FeatureRead &all, const std::atomic<bool> &cancel) {
  TraceN("TransformBuild");
  assert(rt_ && "reset 先于 build");
  Runtime &rt = *rt_;
  const Params &p = spec.params;
  const fmeta::ColumnSet &cols = spec.cols;
  const size_t A = scope_.lib.axis.size(); // 库轴大小
  const size_t VR = kVR;
  assert(level_valid_rows(kLevel) == VR);
  assert(A > 0 && snap.lines.size() == A && "reset 先于 build");
  const size_t stride = kDaysPerBatch * VR; // out 每资产步长 (按满批)

  // 天序: 升序 (TOD 轮廓跨天因果); max_days = 快速迭代档, 只算区间头 N 天
  const DayPlan plan = plan_days(reader, scope_.months, DayOrder::Sequential, p.max_days);
  if (plan.size() == 0)
    return true;
  const size_t n_days = plan.size();
  total.store(n_days, std::memory_order_release);

  misc::exec::ForkJoin &ex = rt.prepare(A, scope_.bar.axis.size(), spec);
  const size_t n_blocks = n_asset_blocks(A);
  rt.pub = snap;                 // 资产线 asset / draw 标记随之带过去 (之后每批整体覆写)
  for (size_t a = 0; a < A; ++a) // stats 每次 build 清零重累积, asset 得重贴
    rt.stats[a].asset = scope_.lib.axis.ids[a];

  // 焦点资产全程序列: 总天数此刻才知道, 就地分配 (NaN = 未到批, UI SkipNaN).
  // 写指针跨 swap 稳定: 首批发布把缓冲整体 swap 进 series, 之后经同一指针续写未发布段
  const float nan = std::nanf("");
  const size_t focus = spec.focus;
  rt.series_raw.assign(n_days * VR, nan);
  rt.series_out.assign(n_days * VR, nan);
  rt.tod_mean.assign(VR, nan);
  rt.tod_sd.assign(VR, nan);
  float *const series_raw = rt.series_raw.data();
  float *const series_out = rt.series_out.data();

  // 链末输出 → 每资产步长
  auto out_row = [&](size_t a, size_t j) -> float * { return rt.out.data() + a * stride + j * VR; };

  Runtime::Shard &acc = rt.shards[0]; // 归约终点 = 全程累加器

  auto publish_batch = [&](size_t b0, size_t bd) {
    TraceN("Publish");
    // 成品 (锁外算): 全局 PDF + 均值谱/相关 + 平稳性
    Snap &pub = rt.pub;
    pub.global.fill(acc.global, kMinAssetSamples);
    for (size_t k = 0; k < DayPSD::N_FREQS; ++k)
      pub.psd_mean[k] = static_cast<float>(acc.psd.mean(k));
    pub.psd_n = acc.psd.n;
    pub.acf_mean.fill(0.0f);
    pub.pacf_mean.fill(0.0f);
    if (acc.acf_n > 0)
      for (size_t k = 0; k <= kTfMaxLag; ++k) {
        pub.acf_mean[k] = static_cast<float>(acc.acf_sum[k] / static_cast<double>(acc.acf_n));
        pub.pacf_mean[k] = static_cast<float>(acc.pacf_sum[k] / static_cast<double>(acc.acf_n));
      }
    pub.acf_n = acc.acf_n;
    pub.stat_lines = rt.stats;
    pub.in = acc.in;
    pub.out = acc.out;
    {
      std::lock_guard<std::mutex> lock(mutex); // 短锁: 整体换新, 中间没有空态
      std::swap(snap, pub);
      if (b0 == 0) { // 首批: 全程缓冲换进来 (之前一直是旧图)
        series.asset = scope_.lib.axis.ids[focus];
        series.raw.swap(rt.series_raw);
        series.out.swap(rt.series_out);
      }
      series.n_days = b0 + bd;
      series.date_begin = plan.dates.front();
      series.date_end = plan.dates[b0 + bd - 1];
      series.tod_mean = rt.tod_mean; // [VR] 小件, 拷贝 (staging 下批续写)
      series.tod_sd = rt.tod_sd;
    }
    publish(bd);
  };

  const bool do_cs = rt.cs_fn != nullptr;
  const bool neutral = p.cs_neutral();

  // 批 b0 的 IO (预读线程或本线程跑; 只写传入的平面)
  const auto load = [&](size_t b0) {
    return [&, b0](Runtime::Plane &pl, misc::exec::ForkJoin &io, std::vector<IoSlot> &slots) {
      const size_t bd = std::min(kDaysPerBatch, n_days - b0);
      pl.main.load_batch(io, slots, reader, plan.dates.data() + b0, bd, cols, cancel);
      if (neutral)
        pl.side.load_batch(io, rt.side_slots, all, plan.dates.data() + b0, bd, spec.side, cancel, scope_.lib.axis.ids.data());
    };
  };
  rt.pf.now(load(0));

  // ==========================================================================
  // 批循环: 下一批 IO 后台预读, 本批四相 (TS → CS → 统计 → 归约), 每相一次 dispatch (返回即栅栏);
  // 批末发布在本线程 (单线程, 天然串行)
  // ==========================================================================
  for (size_t b0 = 0; b0 < n_days; b0 += kDaysPerBatch) {
    const size_t bd = std::min(kDaysPerBatch, n_days - b0);
    if (b0 + kDaysPerBatch < n_days)
      rt.pf.issue(load(b0 + kDaysPerBatch)); // issue → wait 之间不许 return
    const TsPlane &pl = rt.pf.front().main;
    const TsPlane &side = rt.pf.front().side;

    // ------------------------------------------------------------------
    // Phase TS: 每 (资产, 天) 一条因果链 → out_ (a = 库轴下标); kAssetBlock 个资产一任务
    // ------------------------------------------------------------------
    run_phase(ex, n_blocks, cancel, [&](size_t blk, size_t tid) {
      Runtime::Shard &sh = rt.shards[tid];
      const auto [k0, k1] = asset_block(blk, A);
      TraceN("TSBlock");
      for (size_t a = k0; a < k1; ++a) {
        for (size_t j = 0; j < bd; ++j) {
          const feature_storage_t *src = pl.series(0, a, j);
          float *x = sh.x.data();
          for (size_t t = 0; t < VR; ++t)
            x[t] = static_cast<float>(src[t]);
          // 1. season (跨天状态, 因果)
          if (p.season != Season::None)
            rt.tod[a].apply_day(x, x, p.season);
          // 压实: 缺样本剔除, 链在稠密序列上跑 (日内偶发缺分钟视为不存在, 不进滤波器)
          size_t n = 0;
          float *y = sh.y.data();
          uint16_t *idx = sh.idx.data();
          for (size_t t = 0; t < VR; ++t)
            if (x[t] == x[t]) {
              y[n] = x[t];
              idx[n] = static_cast<uint16_t>(t);
              ++n;
            }
          float *row = out_row(a, j);
          std::fill_n(row, VR, nan);
          if (n == 0)
            continue;
          // 2. stationary (天首从零起; 预热段 → NaN)
          float *z = sh.z.data();
          size_t warm = 0;
          switch (p.stationary) {
          case Stationary::None:
            std::copy_n(y, n, z);
            break;
          case Stationary::MADetrend:
            math::stationary::ma_detrend({y, n}, {z, n}, static_cast<int>(p.st[0]));
            break;
          case Stationary::IntDiff:
            math::stationary::int_diff(y, z, n, static_cast<int>(p.st[0]));
            warm = static_cast<size_t>(p.st[0]);
            break;
          case Stationary::FracDiff:
            math::stationary::frac_diff(y, z, n, rt.ffd);
            warm = rt.ffd.size() - 1;
            break;
          }
          if (warm >= n)
            continue;
          const size_t m = n - warm;
          float *zz = z + warm;
          const uint16_t *ii = idx + warm;
          // 3. bandpass (因果 IIR, 天首复位)
          if (p.bandpass)
            math::spectral::iir_filter_forward(zz, zz, m, rt.iir);
          // 4. ts_norm (expanding, 天内)
          float *w = y; // y 已用完, 复用作输出
          math::normalize::apply_ts({zz, m}, {w, m}, p.ts_norm, p.ts);
          for (size_t i = 0; i < m; ++i)
            row[ii[i]] = w[i];
        }
      }
    });

    // ------------------------------------------------------------------
    // Phase CS: 每 (天, 分钟) 一列 gather → column_fn → scatter; kColBlock 列一任务
    // ------------------------------------------------------------------
    if (do_cs) {
      const size_t n_col = bd * VR;
      run_phase(ex, (n_col + Runtime::kColBlock - 1) / Runtime::kColBlock, cancel, [&](size_t blk, size_t tid) {
        Runtime::Shard &sh = rt.shards[tid];
        const size_t c0 = blk * Runtime::kColBlock;
        const size_t c1 = std::min(c0 + Runtime::kColBlock, n_col);
        TraceN("CSBlock");
        for (size_t c = c0; c < c1; ++c) {
          const size_t j = c / VR, t = c % VR;
          float *base = rt.out.data() + j * VR + t; // + a * stride
          const size_t pstride = side.asset_stride();
          const feature_storage_t *mc = neutral ? side.column(0, j, t) : nullptr;
          const feature_storage_t *ind = neutral ? side.column(1, j, t) : nullptr;
          size_t n = 0;
          for (size_t a = 0; a < A; ++a) {
            const float v = base[a * stride];
            if (v != v)
              continue;
            sh.col[n] = v;
            sh.col_a[n] = static_cast<uint32_t>(a);
            if (neutral) {
              sh.logmc[n] = static_cast<float>(mc[a * pstride]);
              sh.industry[n] = static_cast<float>(ind[a * pstride]);
            }
            ++n;
          }
          if (n == 0)
            continue;
          cs::NeutralRank::Ctx ctx{sh.logmc.data(), sh.industry.data()};
          if (neutral)
            cs::NeutralRank::prepare_logmc(sh.logmc.data(), n);
          rt.cs_fn(sh.col.data(), n, neutral ? &ctx : nullptr);
          for (size_t i = 0; i < n; ++i)
            base[sh.col_a[i] * stride] = sh.col[i];
        }
      });
    }

    // ------------------------------------------------------------------
    // Phase 统计: 每资产 sketch + ADF/KPSS/ACF/PSD + 序列快照 (全资产, 槽位 == 库轴下标); kAssetBlock 个资产一任务
    // ------------------------------------------------------------------
    run_phase(ex, n_blocks, cancel, [&](size_t blk, size_t tid) {
      Runtime::Shard &sh = rt.shards[tid];
      const auto [k0, k1] = asset_block(blk, A);
      TraceN("StatBlock");
      for (size_t a = k0; a < k1; ++a) {
        sh.samples.clear();
        for (size_t j = 0; j < bd; ++j) {
          const float *row = out_row(a, j);
          const feature_storage_t *src = pl.series(0, a, j);
          for (size_t t = 0; t < VR; ++t) {
            sh.in.add_cell(src[t]);
            const float v = row[t];
            if (v != v) // 缺 / 预热 / 常数槽
              continue;
            if (std::isinf(v)) {
              ++(v > 0.0f ? sh.out.n_pos_inf : sh.out.n_neg_inf);
              continue;
            }
            sh.out.add_finite(v);
            sh.samples.push_back(v);
          }

          // 焦点资产: 序列快照写进全程缓冲 (天偏移 = 批首 + 批内天; 未发布段, 锁外写)
          if (a == focus) {
            float *snap_raw = series_raw + (b0 + j) * VR;
            float *snap_out = series_out + (b0 + j) * VR;
            for (size_t t = 0; t < VR; ++t)
              snap_raw[t] = static_cast<float>(src[t]);
            std::copy_n(row, VR, snap_out);
          }

          // 逐天 ADF / KPSS (压实有效输出)
          size_t n = 0;
          for (size_t t = 0; t < VR; ++t)
            if (row[t] == row[t])
              sh.y[n++] = row[t];
          if (n >= kTfMinStatSamples) {
            const auto adf = math::stationary::adf_test({sh.y.data(), n}, 4, sh.adf_ws);
            const auto kpss = math::stationary::kpss_test({sh.y.data(), n}, -1, sh.kpss_ws);
            if (adf.valid && kpss.valid) {
              StatLine &st = rt.stats[a];
              ++st.n_days;
              st.adf_pass += adf.pvalue < 0.05f;
              st.kpss_pass += kpss.pvalue > 0.05f;
              st.adf_sum += adf.statistic;
              st.kpss_sum += kpss.statistic;
            }
          }
          // 逐天 ACF / PACF (同一份压实输出; 样本够 4×lag 才算, 保证滞后数满)
          if (n >= 4 * kTfMaxLag) {
            const auto ac = math::timeseries::compute_acf_pacf({sh.y.data(), n}, static_cast<int>(kTfMaxLag), sh.acf_ws);
            if (ac.valid) {
              assert(ac.acf.size() == kTfMaxLag + 1 && ac.pacf.size() == kTfMaxLag + 1);
              for (size_t k = 0; k <= kTfMaxLag; ++k) {
                sh.acf_sum[k] += ac.acf[k];
                sh.pacf_sum[k] += ac.pacf[k];
              }
              ++sh.acf_n;
            }
          }
          // 逐天 PSD
          if (sh.day_psd->compute(row))
            sh.psd.add(*sh.day_psd);
        }
        sh.in.n_total += bd * VR;
        sh.out.n_total += bd * VR;
        if (a == focus && p.season != Season::None) {
          const auto &slots = rt.tod[a].slots;
          float *tm = rt.tod_mean.data();
          float *ts = rt.tod_sd.data();
          for (size_t t = 0; t < VR; ++t) {
            tm[t] = slots[t].n >= math::stationary::TodProfile::kMinDays ? slots[t].mean : nan;
            ts[t] = slots[t].n >= math::stationary::TodProfile::kMinDays ? slots[t].sd() : nan;
          }
        }

        // 每资产: 累积 sketch + 导出整条线到 staging (每资产单线程 → 无锁)
        rt.assets.add(a, sh.samples.data(), sh.samples.size(), rt.pub.lines[a]);
        if (!sh.samples.empty())
          sh.global.addBatch(sh.samples.data(), sh.samples.size());
      }
    });

    // ------------------------------------------------------------------
    // Phase 归约: shard 两两并行归约到 shard 0, 全程锁外 (shard 0 = 全程累加器, 其余清零供下批)
    // ------------------------------------------------------------------
    reduce_tree(ex, rt.shards);

    rt.pf.wait();
    if (cancel.load(std::memory_order_relaxed))
      return false; // 半批不发布
    publish_batch(b0, bd);
    rt.pf.flip();
  }
  return true;
}

// ============================================================================
// Clear
// ============================================================================

void Transform::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  snap = Snap{};
  spec = Spec{};
  series = SeriesSnap{};
  rt_.reset(); // worker 已 join, 整体释放
  scope_ = ReadScope{};
  idle();
}
