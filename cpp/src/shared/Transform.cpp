#include "shared/Transform.hpp"
#include "features/Backend/DayBatchPlane.hpp"
#include "math/stationary/ADF.hpp"
#include "math/stationary/KPSS.hpp"
#include "math/timeseries/AutoCorrelation.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <barrier>
#include <cmath>
#include <random>
#include <thread>

namespace {

// 槽数对得上就只 clear (KLL 保留 buffer 容量, 稳态零分配), 对不上才重建
void prepare_slots(std::vector<KLLcache> &slots, size_t n, size_t capacity, size_t resolution) {
  if (slots.size() == n) {
    for (auto &kll : slots)
      kll.clear();
    return;
  }
  slots.clear();
  slots.reserve(n);
  for (size_t i = 0; i < n; ++i)
    slots.emplace_back(capacity, resolution);
}

// 相对 Nyquist 的归一化频率: 周期 (样本数) → f = 2 / period
float period_to_freq(float period) { return 2.0f / period; }

} // namespace

// ============================================================================
// Runtime: worker 私有状态 (UI 不看)
// ============================================================================

struct Transform::Runtime {
  static constexpr size_t kAssetBlock = 64; // TS / 统计 抢块粒度
  static constexpr size_t kColBlock = 16;   // CS 抢块粒度 (截面列数 = 批天 × VR)

  DayBatchPlane plane;                           // 输入 [列][A][批天][VR] f16
  std::vector<float> out;                        // 输出 [A][批天][VR] float (NaN = 缺/预热)
  std::vector<math::stationary::TodProfile> tod; // [A] 跨天 TOD 轮廓 (season 开时)
  math::spectral::IIRCoeffs iir;                 // 带通系数 (一次设计)
  math::stationary::FFDWeights ffd;              // 分数差分权重 (一次算)
  cs::ColumnFn cs_fn = nullptr;

  std::vector<KLLcache> asset_klls;                           // [A] 每资产累积 sketch
  std::vector<AssetLine> lines_staging;                       // [A]
  std::vector<StatLine> stat_staging;                         // [A] 累积 (发布时整体拷贝; .asset 由 build 起手贴)
  SeriesSnap series_staging;                                  // 焦点资产全程序列 (build 起手按总天数分配, 逐批填)
  std::array<double, TfDayPSD::N_FREQS> psd_sum{};            // 全区间单日谱累加 (completion 单线程更新)
  std::array<double, kTfMaxLag + 1> acf_sum{}, pacf_sum{};    // 全区间单日 ACF/PACF 累加 (同上)
  uint64_t psd_n = 0, acf_n = 0;                              // 同上. 展示字段 (Transform::psd_n 等) 只在 publish 写,
  KLLcache total{kTfTotalKllCapacity, kTfTotalKllResolution}; // 重算期间旧图留住不闪空
  Integrity integrity;

  // 每线程私有
  struct Shard {
    std::vector<float> x, y, z;  // VR: 一天的稠密行 / 压实后的链中间量
    std::vector<uint16_t> idx;   // VR: 压实 → 行内下标
    std::vector<float> col;      // A: 截面列 dense 有效子集
    std::vector<uint32_t> col_a; // A: dense → 资产下标
    std::vector<float> logmc;    // A: NeutralRank 上下文
    std::vector<float> industry; // A
    std::vector<float> samples;  // 批天 × VR: 单资产本批全量有效输出 → sketch
    math::stationary::ADFWorkspace adf_ws;
    math::stationary::KPSSWorkspace kpss_ws;
    std::unique_ptr<TfDayPSD> psd; // 大对象 (FFT 工作区), 堆上
    std::array<double, TfDayPSD::N_FREQS> psd_sum{};
    uint64_t psd_n = 0;
    math::timeseries::ACFWorkspace acf_ws;
    std::array<double, kTfMaxLag + 1> acf_sum{}, pacf_sum{};
    uint64_t acf_n = 0;
    KLLcache total{kTfTotalKllCapacity, kTfTotalKllResolution};
    Integrity integrity;
  };
  std::vector<Shard> shards;

  // A = universe 子轴大小 (平面/out/sketch/统计/序列快照 尺寸; 线程数按扫描块数封顶)
  size_t prepare(size_t A, size_t n_cols, const Params &p) {
    TraceN("TransformPrepare");
    const size_t VR = kTfVR;
    const size_t n_blocks = (A + kAssetBlock - 1) / kAssetBlock;
    const size_t n_hw = std::max<size_t>(1, std::thread::hardware_concurrency());
    const size_t n_threads = std::min(n_hw, std::max(kTfDaysPerBatch, n_blocks));
    const size_t n_io = std::min(n_threads, kTfDaysPerBatch);

    plane.prepare(A, p.level, kTfDaysPerBatch, n_cols, n_io, n_cols + 1);
    out.resize(A * kTfDaysPerBatch * VR);

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

    prepare_slots(asset_klls, A, kTfKllCapacity, kTfKllResolution);
    lines_staging.assign(A, AssetLine{});
    stat_staging.assign(A, StatLine{});
    psd_sum.fill(0.0);
    acf_sum.fill(0.0);
    pacf_sum.fill(0.0);
    psd_n = 0;
    acf_n = 0;
    total.clear();
    integrity.clear();

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
      sh.samples.reserve(kTfDaysPerBatch * VR);
      if (!sh.psd)
        sh.psd = std::make_unique<TfDayPSD>();
      sh.psd_sum.fill(0.0);
      sh.psd_n = 0;
      sh.acf_ws.ensure(kTfMaxLag);
      sh.acf_sum.fill(0.0);
      sh.pacf_sum.fill(0.0);
      sh.acf_n = 0;
      sh.total.clear();
      sh.integrity.clear();
    }
    return n_threads;
  }
};

Transform::Transform() = default;
Transform::~Transform() = default;

// ============================================================================
// Reset
// ============================================================================

void Transform::reset_for_build(const Params &p, std::vector<size_t> cols, bool has_valid,
                                const std::vector<std::string> &month_keys,
                                std::vector<uint32_t> global_ids, uint32_t series_focus) {
  TraceN("TransformReset");
  assert(p.level == kTfLevel && "Transform 目前只在 L1 跑 (VR / PSD 模板按 L1 配)");
  assert(!global_ids.empty() && "universe 为空");
  assert(std::is_sorted(global_ids.begin(), global_ids.end()) &&
         std::adjacent_find(global_ids.begin(), global_ids.end()) == global_ids.end() &&
         "global_ids 必须升序去重 (UniverseAxis::ids)");
  assert(cols.size() == 1u + (p.cs_neutral() ? 2u : 0u) + (has_valid ? 1u : 0u));
  std::lock_guard<std::mutex> lock(mutex);

  params = p;
  columns_ = std::move(cols);
  has_valid_ = has_valid;
  months_ = month_keys;
  const bool axis_changed = global_ids_ != global_ids;
  global_ids_ = std::move(global_ids);
  const size_t n_assets = global_ids_.size(); // A 轴 = universe 子轴
  assert(series_focus < n_assets && "序列焦点越界 (子轴下标)");
  series_focus_ = series_focus;

  // 固定种子洗牌 (子轴下标): 前 kTfDrawAssets 个 = PDF 绘制子集 (纯 UI 顶点预算;
  // 统计/序列快照无子集, 恒为全资产)
  std::vector<uint32_t> order(n_assets);
  for (size_t a = 0; a < n_assets; ++a)
    order[a] = static_cast<uint32_t>(a);
  std::shuffle(order.begin(), order.end(), std::mt19937{0x5eed});
  const size_t n_draw = std::min(kTfDrawAssets, order.size());

  // 展示字段 (lines PDF / series / psd_mean / psd_n / acf_n / total / integrity / stat_lines) 一律不清:
  // 拖动调参时旧图留住, 首批 publish 整体覆盖, 不闪空. 累加器全在 Runtime (prepare 里重置).
  // universe 变了 (子轴大小或成员) 则整体重建 —— 槽位含义已变, 旧线是陈旧数据.
  if (lines.size() != n_assets || axis_changed)
    lines.assign(n_assets, AssetLine{});
  for (size_t a = 0; a < n_assets; ++a) {
    lines[a].asset = global_ids_[a];
    lines[a].draw = 0;
  }
  for (size_t i = 0; i < n_draw; ++i)
    lines[order[i]].draw = 1;
  // 槽位 == 子轴下标 (与 lines 对齐): 一律重贴 asset (全局轴下标), 旧累积由首批 publish 覆盖
  if (stat_lines.size() != n_assets || axis_changed)
    stat_lines.assign(n_assets, StatLine{});
  for (size_t a = 0; a < n_assets; ++a)
    stat_lines[a].asset = global_ids_[a];
  if (!rt_)
    rt_ = std::make_unique<Runtime>();

  days_loaded.store(0, std::memory_order_relaxed);
  days_total.store(0, std::memory_order_relaxed);
  epoch.fetch_add(1, std::memory_order_release);
  status.store(Status::Building, std::memory_order_release);
}

// ============================================================================
// Build
// ============================================================================

bool Transform::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("TransformBuild");
  assert(rt_ && "reset_for_build 先于 build");
  Runtime &rt = *rt_;
  const Params &p = params;
  const size_t A = lines.size(); // universe 子轴大小
  const size_t VR = kTfVR;
  assert(level_valid_rows(p.level) == VR);
  assert(A > 0 && A == global_ids_.size() && "reset_for_build 先于 build");
  const size_t n_cols = columns_.size() - (has_valid_ ? 1 : 0);
  const size_t stride = kTfDaysPerBatch * VR; // out_ 每资产步长 (按满批)

  // 日期枚举 (全区间, 不抽样, 从前往后)
  std::vector<std::string> dates;
  {
    TraceN("EnumDates");
    for (const std::string &key : months_) {
      auto ds = reader.list_dates(key.substr(0, 4), key.substr(4, 2));
      dates.insert(dates.end(), ds.begin(), ds.end());
    }
  }
  if (dates.empty())
    return true;
  if (p.max_days > 0 && dates.size() > p.max_days)
    dates.resize(p.max_days); // 快速迭代档: 只算区间头 N 天, 不往后算
  const size_t n_days = dates.size();
  days_total.store(n_days, std::memory_order_release);

  const size_t n_threads = rt.prepare(A, n_cols, p);
  const size_t n_io = std::min(n_threads, kTfDaysPerBatch);
  rt.lines_staging = lines;      // asset / draw 标记随之带过去
  for (size_t a = 0; a < A; ++a) // stat_staging 每次 build 清零重累积, asset 得重贴 (publish 整体覆盖 stat_lines)
    rt.stat_staging[a].asset = global_ids_[a];

  // 焦点资产全程序列快照: 总天数此刻才知道, 就地分配 (NaN = 未到批, UI SkipNaN)
  const size_t focus = series_focus_;
  {
    SeriesSnap &ss = rt.series_staging;
    ss.asset = global_ids_[focus];
    ss.n_days = 0;
    ss.raw.assign(n_days * VR, std::nanf(""));
    ss.out.assign(n_days * VR, std::nanf(""));
    ss.tod_mean.assign(VR, std::nanf(""));
    ss.tod_sd.assign(VR, std::nanf(""));
  }

  // 链末输出 → 每资产步长
  auto out_row = [&](size_t a, size_t j) -> float * { return rt.out.data() + a * stride + j * VR; };

  const float nan = std::nanf("");
  std::atomic<size_t> next_day{0};
  std::atomic<size_t> next_block{0};
  std::atomic<size_t> next_col{0};
  bool stop = false;
  size_t pub_begin = 0;

  auto publish = [&]() noexcept {
    TraceN("Publish");
    const size_t bd = std::min(kTfDaysPerBatch, n_days - pub_begin);
    // 各 shard 并入 Runtime 累加器 (锁外, completion 单线程)
    rt.integrity.n_in_nan += rt.plane.take_nan_seen();
    for (auto &sh : rt.shards) {
      for (size_t k = 0; k < TfDayPSD::N_FREQS; ++k)
        rt.psd_sum[k] += sh.psd_sum[k];
      rt.psd_n += sh.psd_n;
      sh.psd_sum.fill(0.0);
      sh.psd_n = 0;
      for (size_t k = 0; k <= kTfMaxLag; ++k) {
        rt.acf_sum[k] += sh.acf_sum[k];
        rt.pacf_sum[k] += sh.pacf_sum[k];
      }
      rt.acf_n += sh.acf_n;
      sh.acf_sum.fill(0.0);
      sh.pacf_sum.fill(0.0);
      sh.acf_n = 0;
      rt.integrity.n_total += sh.integrity.n_total;
      rt.integrity.n_in_valid += sh.integrity.n_in_valid;
      rt.integrity.n_out_valid += sh.integrity.n_out_valid;
      sh.integrity.clear();
      rt.total.mergeWith(sh.total);
      sh.total.clear();
    }
    // 短锁: 展示字段整体覆盖 (旧图 → 新图, 中间没有空态)
    {
      std::lock_guard<std::mutex> lock(mutex);
      lines.swap(rt.lines_staging);
      stat_lines = rt.stat_staging;
      // 序列快照累积在 staging (全程缓冲), 不能 swap 走 —— 整体拷贝 (单资产, ~MB 级)
      rt.series_staging.n_days = pub_begin + bd;
      rt.series_staging.date_begin = dates.front();
      rt.series_staging.date_end = dates[pub_begin + bd - 1];
      series = rt.series_staging;
      integrity = rt.integrity;
      psd_n = rt.psd_n;
      acf_n = rt.acf_n;
      if (psd_n > 0)
        for (size_t k = 0; k < TfDayPSD::N_FREQS; ++k)
          psd_mean[k] = static_cast<float>(rt.psd_sum[k] / static_cast<double>(psd_n));
      if (acf_n > 0)
        for (size_t k = 0; k <= kTfMaxLag; ++k) {
          acf_mean[k] = static_cast<float>(rt.acf_sum[k] / static_cast<double>(acf_n));
          pacf_mean[k] = static_cast<float>(rt.pacf_sum[k] / static_cast<double>(acf_n));
        }
      total.clear();
      total.mergeWith(rt.total);
    }
    days_loaded.fetch_add(bd, std::memory_order_release);
    epoch.fetch_add(1, std::memory_order_release);
    pub_begin += kTfDaysPerBatch;
    next_day.store(0, std::memory_order_relaxed);
    next_block.store(0, std::memory_order_relaxed);
    next_col.store(0, std::memory_order_relaxed);
    stop = cancel.load(std::memory_order_relaxed);
  };

  std::barrier io_done(static_cast<ptrdiff_t>(n_threads));
  std::barrier ts_done(static_cast<ptrdiff_t>(n_threads), [&]() noexcept { next_block.store(0, std::memory_order_relaxed); });
  std::barrier cs_done(static_cast<ptrdiff_t>(n_threads), [&]() noexcept { next_block.store(0, std::memory_order_relaxed); });
  std::barrier stats_done(static_cast<ptrdiff_t>(n_threads), publish);

  const bool do_cs = rt.cs_fn != nullptr;
  const bool neutral = p.cs_neutral();

  auto worker = [&](size_t tid) {
    Runtime::Shard &sh = rt.shards[tid];
    for (size_t b0 = 0; b0 < n_days && !stop; b0 += kTfDaysPerBatch) {
      const size_t bd = std::min(kTfDaysPerBatch, n_days - b0);

      // ------------------------------------------------------------------
      // Phase IO
      // ------------------------------------------------------------------
      if (tid < n_io) {
        for (;;) {
          const size_t j = next_day.fetch_add(1, std::memory_order_relaxed);
          if (j >= bd || cancel.load(std::memory_order_relaxed))
            break;
          rt.plane.load_day(reader, dates[b0 + j], columns_, has_valid_, L2::ValidType::DATA, j, tid);
        }
      }
      io_done.arrive_and_wait();

      // ------------------------------------------------------------------
      // Phase TS: 每 (资产, 天) 一条因果链 → out_ (a = 子轴下标)
      // ------------------------------------------------------------------
      for (;;) {
        const size_t k0 = next_block.fetch_add(Runtime::kAssetBlock, std::memory_order_relaxed);
        if (k0 >= A || cancel.load(std::memory_order_relaxed))
          break;
        const size_t k1 = std::min(k0 + Runtime::kAssetBlock, A);
        TraceN("TSBlock");
        for (size_t a = k0; a < k1; ++a) {
          for (size_t j = 0; j < bd; ++j) {
            const feature_storage_t *src = rt.plane.series(0, a, j);
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
      }
      ts_done.arrive_and_wait();

      // ------------------------------------------------------------------
      // Phase CS: 每 (天, 分钟) 一列 gather → column_fn → scatter
      // ------------------------------------------------------------------
      if (do_cs) {
        const size_t n_col = bd * VR;
        for (;;) {
          const size_t c0 = next_col.fetch_add(Runtime::kColBlock, std::memory_order_relaxed);
          if (c0 >= n_col || cancel.load(std::memory_order_relaxed))
            break;
          const size_t c1 = std::min(c0 + Runtime::kColBlock, n_col);
          TraceN("CSBlock");
          for (size_t c = c0; c < c1; ++c) {
            const size_t j = c / VR, t = c % VR;
            float *base = rt.out.data() + j * VR + t; // + a * stride
            const size_t pstride = rt.plane.asset_stride();
            const feature_storage_t *mc = neutral ? rt.plane.column(1, j, t) : nullptr;
            const feature_storage_t *ind = neutral ? rt.plane.column(2, j, t) : nullptr;
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
        }
      }
      cs_done.arrive_and_wait();

      // ------------------------------------------------------------------
      // Phase 统计: 每资产 sketch + ADF/KPSS/ACF/PSD + 序列快照 (全资产, 槽位 == 子轴下标)
      // ------------------------------------------------------------------
      for (;;) {
        const size_t k0 = next_block.fetch_add(Runtime::kAssetBlock, std::memory_order_relaxed);
        if (k0 >= A || cancel.load(std::memory_order_relaxed))
          break;
        const size_t k1 = std::min(k0 + Runtime::kAssetBlock, A);
        TraceN("StatBlock");
        for (size_t a = k0; a < k1; ++a) {
          sh.samples.clear();
          for (size_t j = 0; j < bd; ++j) {
            const float *row = out_row(a, j);
            const feature_storage_t *src = rt.plane.series(0, a, j);
            size_t n_in = 0;
            for (size_t t = 0; t < VR; ++t) {
              n_in += (src[t] == src[t]);
              const float v = row[t];
              if (v == v)
                sh.samples.push_back(v);
            }
            sh.integrity.n_in_valid += n_in;

            // 焦点资产: 序列快照写进全程缓冲 (天偏移 = 批首 + 批内天)
            if (a == focus) {
              float *snap_raw = rt.series_staging.raw.data() + (b0 + j) * VR;
              float *snap_out = rt.series_staging.out.data() + (b0 + j) * VR;
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
                StatLine &st = rt.stat_staging[a];
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
            if (sh.psd->compute(row)) {
              for (size_t k = 0; k < TfDayPSD::N_FREQS; ++k)
                sh.psd_sum[k] += sh.psd->power[k];
              ++sh.psd_n;
            }
          }
          sh.integrity.n_total += bd * VR;
          sh.integrity.n_out_valid += sh.samples.size();
          if (a == focus && p.season != Season::None) {
            const auto &slots = rt.tod[a].slots;
            float *tm = rt.series_staging.tod_mean.data();
            float *ts = rt.series_staging.tod_sd.data();
            for (size_t t = 0; t < VR; ++t) {
              tm[t] = slots[t].n >= math::stationary::TodProfile::kMinDays ? slots[t].mean : nan;
              ts[t] = slots[t].n >= math::stationary::TodProfile::kMinDays ? slots[t].sd() : nan;
            }
          }

          // 每资产 sketch → AssetLine (worker 私有, 每资产单线程 → 无锁)
          KLLcache &kll = rt.asset_klls[a];
          if (!sh.samples.empty()) {
            kll.addBatch(sh.samples.data(), sh.samples.size());
            sh.total.addBatch(sh.samples.data(), sh.samples.size());
          }
          AssetLine &ln = rt.lines_staging[a];
          ln.n = kll.totalCount();
          if (ln.n >= kTfMinAssetSamples) {
            const auto pdf = kll.exportPDF();
            assert(pdf.n <= ln.x.size());
            ln.n_pts = static_cast<uint32_t>(pdf.n);
            std::copy_n(pdf.x, pdf.n, ln.x.data());
            std::copy_n(pdf.y, pdf.n, ln.y.data());
            ln.mean = static_cast<float>(kll.mean());
            ln.var = static_cast<float>(kll.var());
            ln.skew = static_cast<float>(kll.skew());
            ln.kurt = static_cast<float>(kll.kurt());
          } else {
            ln.n_pts = 0;
          }
        }
      }
      stats_done.arrive_and_wait(); // → publish() (sh.total 批内累积, 批末并入 rt.total)
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

void Transform::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  lines = std::vector<AssetLine>{};
  global_ids_ = std::vector<uint32_t>{};
  stat_lines = std::vector<StatLine>{};
  series = SeriesSnap{};
  psd_mean.fill(0.0f);
  psd_n = 0;
  acf_mean.fill(0.0f);
  pacf_mean.fill(0.0f);
  acf_n = 0;
  total.clear();
  integrity.clear();
  rt_.reset(); // worker 已 join, 整体释放
  columns_.clear();
  months_.clear();
  days_loaded.store(0, std::memory_order_relaxed);
  days_total.store(0, std::memory_order_relaxed);
  epoch.fetch_add(1, std::memory_order_release);
  status.store(Status::Idle, std::memory_order_release);
}
