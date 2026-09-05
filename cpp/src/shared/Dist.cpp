#include "shared/Dist.hpp"
#include "features/TimeIndex.hpp"
#include "misc/profiler.hpp"

#include <barrier>
#include <bit>
#include <cmath>
#include <cstdio>
#include <numeric>
#include <random>
#include <thread>
#include <tuple>

// ============================================================================
// Helper: Date parsing
// ============================================================================

namespace {

// Parse "YYYYMMDD" -> (year, month, day)
std::tuple<uint16_t, uint8_t, uint8_t> parse_date(const std::string &date) {
  assert(date.size() == 8);
  uint16_t year = std::stoi(date.substr(0, 4));
  uint8_t month = std::stoi(date.substr(4, 2));
  uint8_t day = std::stoi(date.substr(6, 2));
  return {year, month, day};
}

// Zeller's congruence: weekday (Mon=0, Sun=6)
uint8_t calc_weekday(uint16_t y, uint8_t m, uint8_t d) {
  if (m < 3) {
    m += 12;
    y -= 1;
  }
  int q = d, M = m, K = y % 100, J = y / 100;
  int h = (q + (13 * (M + 1)) / 5 + K + K / 4 + J / 4 - 2 * J) % 7;
  return static_cast<uint8_t>((h + 5) % 7);
}

// "YYYY-MM-DD" / "YYYYMMDD" -> "YYYYMM"
std::string parse_month(const std::string &date) {
  std::string d;
  for (char c : date)
    if (c != '-')
      d += c;
  assert(d.size() >= 6);
  return d.substr(0, 6);
}

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

void clear_slots(std::vector<KLLcache> &slots) {
  for (auto &kll : slots)
    kll.clear();
}

// ============================================================================
// W2 偏移 (发布侧派生): 均值校准的 Wasserstein-L2 偏移距离, 相对上一批末的
// 全局分位参考 —— 随批次推进逐批收敛, UI 只消费成品.
// ============================================================================

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

// RefT = Dist::W2Ref (私有嵌套类型, 模板推导绕开命名)
template <class RefT>
float compute_w2(const KLLcache::LinePtr &icdf, float mean, const RefT &ref) {
  const float shift = mean - ref.mean;
  float sum_sq = 0.0f;
  for (int d = 0; d < kW2Deciles; ++d) {
    const float qi = quantile_at(icdf, 0.05 * (d + 1));
    const float diff = (qi - shift) - ref.q[d];
    sum_sq += diff * diff;
  }
  return std::sqrt(sum_sq / kW2Deciles);
}

} // namespace

std::vector<std::string> dist_enumerate_months(const std::string &start_date,
                                               const std::string &end_date) {
  std::vector<std::string> months;
  const std::string start_month = parse_month(start_date);
  const std::string end_month = parse_month(end_date);

  int y = std::stoi(start_month.substr(0, 4));
  int m = std::stoi(start_month.substr(4, 2));
  while (true) {
    char buf[8];
    snprintf(buf, sizeof(buf), "%04d%02d", y, m);
    std::string month_key = buf;
    if (month_key > end_month)
      break;
    months.push_back(std::move(month_key));
    if (++m > 12) {
      m = 1;
      ++y;
    }
  }
  return months;
}

// ============================================================================
// Reset
// ============================================================================

void Dist::reset_for_build(std::vector<size_t> cols, const std::vector<std::string> &month_keys,
                           size_t n_assets) {
  std::lock_guard<std::mutex> lock(mutex);

  columns = std::move(cols);
  assert(!columns.empty() && "至少要有值列");
  assert(n_assets > 0 && "资产轴为空, Distribution 无法构建");

  // 月聚合: 槽数随区间变, 数量对得上就复用 sketch 容量
  if (months.size() != month_keys.size()) {
    months.clear();
    months.resize(month_keys.size());
  } else {
    for (auto &slot : months)
      slot.kll.clear();
  }
  for (size_t i = 0; i < month_keys.size(); ++i)
    months[i].month = month_keys[i];

  prepare_slots(by_hour, 24, KLL_CAPACITY, KLL_RESOLUTION);
  prepare_slots(by_weekday, 7, KLL_CAPACITY, KLL_RESOLUTION);

  // 绘制子集: 固定种子洗牌取前 n_draw 个 → 无偏随机, 画面统计形态与全量等价
  const size_t n_draw = std::min(kDrawAssets, n_assets);
  std::vector<uint32_t> order(n_assets);
  std::iota(order.begin(), order.end(), uint32_t{0});
  std::shuffle(order.begin(), order.end(), std::mt19937{0x5eed});
  draw_pos_.assign(n_assets, -1);
  for (size_t i = 0; i < n_draw; ++i)
    draw_pos_[order[i]] = static_cast<int32_t>(i);

  prepare_slots(asset_klls_, n_draw, KLL_ASSET_CAPACITY, KLL_ASSET_RESOLUTION);
  lines.assign(n_draw, AssetLine{});
  lines_staging_.assign(n_draw, AssetLine{});
  w2_ref_ = W2Ref{};

  total.clear();
  integrity.clear();

  days_loaded.store(0, std::memory_order_relaxed);
  days_total.store(0, std::memory_order_relaxed);
  lines_epoch.store(0, std::memory_order_relaxed);
  agg_stride.store(1, std::memory_order_relaxed);
  status.store(Status::Building, std::memory_order_release);
}

// ============================================================================
// Build (分批流式: 每批 IO → 扫描 → 发布, 首帧与总区间长度无关)
// ============================================================================

bool Dist::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("DistBuild");

  const size_t A = draw_pos_.size();
  const size_t n_cols = columns.size();
  const bool has_valid = (n_cols > 1);
  const size_t VR = level_valid_rows(kDistLevel); // 分钟/日
  const size_t n_months = months.size();
  assert(A > 0 && "资产轴为空, Distribution 无法构建");
  assert(n_cols <= 2 && "Dist 只接受值列 + 可选 valid 列");

  // 预计算: 分钟 → 小时 (L1)
  std::vector<uint8_t> hour_lut(VR);
  for (size_t t = 0; t < VR; ++t) {
    const uint8_t hour = L1_to_Clock(t).hour;
    assert(hour < 24);
    hour_lut[t] = hour;
  }

  // ==========================================================================
  // 日期枚举 + 自适应日抽样: 抽样天表 = 日期 → (星期, 月下标)
  // ==========================================================================
  std::vector<std::string> all_dates;
  std::vector<size_t> all_month; // 日 → 月下标
  for (size_t m = 0; m < n_months; ++m) {
    const std::string &key = months[m].month; // reset 后不变, 免锁读
    auto ds = reader.list_dates(key.substr(0, 4), key.substr(4, 2));
    for (auto &d : ds) {
      all_dates.push_back(std::move(d));
      all_month.push_back(m);
    }
  }
  if (all_dates.empty())
    return true;

  // 日抽样: 总样本预算 (分批后平面只存一批, 内存不再约束天数; 预算只是总时长旋钮)
  size_t stride = (all_dates.size() * A * VR + kMaxTotalSamples - 1) / kMaxTotalSamples;
  if (stride % 5 == 0)
    ++stride; // 与交易周互质, 避免星期偏置

  std::vector<std::string> dates;
  std::vector<uint8_t> weekdays;
  std::vector<uint16_t> day_month;
  for (size_t i = 0; i < all_dates.size(); i += stride) {
    auto [y, m, dd] = parse_date(all_dates[i]);
    dates.push_back(std::move(all_dates[i]));
    weekdays.push_back(calc_weekday(y, m, dd));
    day_month.push_back(static_cast<uint16_t>(all_month[i]));
  }
  const size_t n_sel = dates.size();
  days_total.store(n_sel, std::memory_order_release);

  const auto invalid = std::bit_cast<feature_storage_t>(kInvalidBits);
  const size_t asset_stride = kDaysPerBatch * VR; // 批平面内每资产步长 (按满批)
  plane_.resize(A * asset_stride);

  // 聚合槽抽样 stride: 按总格子数 (有效样本的上界) 折到 kAggTargetSamples 量级.
  // 区间小 → stride=1, 全量进聚合槽, 小数据集下不会被抽到低于 kMinSamples.
  size_t agg_stride = std::max<size_t>(1, (n_sel * A * VR) / kAggTargetSamples);
  while (agg_stride > 1 && VR % agg_stride == 0)
    ++agg_stride; // 与日内分钟数互质: 整除会让每天固定落在同一批分钟上, 扭曲小时分布
  this->agg_stride.store(agg_stride, std::memory_order_release);

  // 线程私有状态 (跨请求复用; 槽数对得上只 clear).
  // 扫描块数封顶线程数; IO 每批最多 kDaysPerBatch 个任务, staging 只给前 n_io 个线程备
  const size_t n_blocks = (A + kAssetBlock - 1) / kAssetBlock;
  const size_t n_hw = std::max<size_t>(1, std::thread::hardware_concurrency());
  const size_t n_threads = std::min(n_hw, std::max(kDaysPerBatch, n_blocks));
  const size_t n_io = std::min(n_threads, kDaysPerBatch);
  shards_.resize(n_threads);
  for (size_t i = 0; i < n_threads; ++i) {
    Shard &sh = shards_[i];
    if (i < n_io)
      sh.staging.preallocate(A, kDistLevel, n_cols);
    sh.nan_seen = 0;
    sh.samples.reserve(asset_stride);
    sh.agg_samples.reserve(asset_stride / agg_stride + 1);
    sh.day_groups.reserve(kDaysPerBatch);
    sh.hour_runs.reserve(kDaysPerBatch * 8); // L1 一天只跨 5 个小时 (9/10/11/13/14)
    prepare_slots(sh.months, n_months, KLL_CAPACITY, KLL_RESOLUTION);
    prepare_slots(sh.by_hour, 24, KLL_CAPACITY, KLL_RESOLUTION);
    prepare_slots(sh.by_weekday, 7, KLL_CAPACITY, KLL_RESOLUTION);
    sh.total.clear();
    sh.integrity.clear();
  }

  // ==========================================================================
  // 批循环: 一波常驻线程, 每批两道栅栏 (IO 完成 → 扫描完成).
  // 批末发布走 scan_done 的 completion (标准保证在所有线程到齐后、解除阻塞前
  // 由单线程执行 → stop/进度/抢任务原子对所有线程一致可见)
  // ==========================================================================
  std::atomic<size_t> next_day{0};
  std::atomic<size_t> next_block{0};
  bool stop = false;
  size_t pub_begin = 0; // completion 私有推进 (每批恰好执行一次, 串行)

  auto publish = [&]() noexcept {
    const size_t bd = std::min(kDaysPerBatch, n_sel - pub_begin);
    {
      std::lock_guard<std::mutex> lock(mutex);
      lines.swap(lines_staging_); // 整批换新; UI 消费快照零重建
      for (Shard &sh : shards_) {
        integrity.n_nan += sh.nan_seen;
        sh.nan_seen = 0;
      }
      // 下一批的 W2 参考 = 本批后的全局分位 (滞后一批, 逐批收敛)
      w2_ref_ = W2Ref{};
      if (total.totalCount() >= kMinSamples) {
        const auto icdf = total.exportICDF();
        for (int d = 0; d < kW2Deciles; ++d)
          w2_ref_.q[d] = quantile_at(icdf, 0.05 * (d + 1));
        w2_ref_.mean = static_cast<float>(total.mean());
        w2_ref_.valid = true;
      }
    }
    days_loaded.fetch_add(bd, std::memory_order_release);
    lines_epoch.fetch_add(1, std::memory_order_release);
    pub_begin += kDaysPerBatch;
    next_day.store(0, std::memory_order_relaxed);
    next_block.store(0, std::memory_order_relaxed);
    stop = cancel.load(std::memory_order_relaxed);
  };

  std::barrier io_done(static_cast<ptrdiff_t>(n_threads));
  std::barrier scan_done(static_cast<ptrdiff_t>(n_threads), publish);

  auto worker = [&](size_t tid) {
    Shard &sh = shards_[tid];
    for (size_t b0 = 0; b0 < n_sel && !stop; b0 += kDaysPerBatch) {
      const size_t bd = std::min(kDaysPerBatch, n_sel - b0);

      // ------------------------------------------------------------------
      // Phase IO: 抢单天载入 [T][列][A] → 转置进批平面 (天与天写不同段, 无重叠)
      // ------------------------------------------------------------------
      if (tid < n_io) {
        for (;;) {
          const size_t j = next_day.fetch_add(1, std::memory_order_relaxed);
          if (j >= bd || cancel.load(std::memory_order_relaxed))
            break;
          {
            TraceN("LoadDay");
            reader.load_day_columns(dates[b0 + j], columns, sh.staging);
          }
          {
            TraceN("TransposeDay");
            // [T][列][A] → plane[a][j][t]; 64 资产一块, 块内写驻留在 64 条 cache line 上.
            // valid 不过 与 真 NaN 一并折叠成哨兵; 真 NaN 就地记账 (这里才看得见 valid 列)
            feature_storage_t *day_base = plane_.data() + j * VR;
            for (size_t a0 = 0; a0 < A; a0 += 64) {
              const size_t a1 = std::min(a0 + 64, A);
              for (size_t t = 0; t < VR; ++t) {
                const feature_storage_t *val = sh.staging.data.data() + (t * n_cols) * A;
                const feature_storage_t *valid = val + A; // valid 列紧随其后 (has_valid 才读)
                for (size_t a = a0; a < a1; ++a) {
                  const bool ok = !has_valid || static_cast<float>(valid[a]) > 0.5f;
                  const feature_storage_t raw = val[a];
                  const bool is_nan = ok && !(raw == raw);
                  sh.nan_seen += is_nan;
                  day_base[a * asset_stride + t] = (ok && !is_nan) ? raw : invalid;
                }
              }
            }
          }
        }
      }
      io_done.arrive_and_wait();

      // ------------------------------------------------------------------
      // Phase 扫描: 抢 kAssetBlock 个资产一块, 全在锁外; 块末短锁 merge 聚合槽
      // ------------------------------------------------------------------
      for (;;) {
        const size_t k0 = next_block.fetch_add(kAssetBlock, std::memory_order_relaxed);
        if (k0 >= A || cancel.load(std::memory_order_relaxed))
          break;
        const size_t k1 = std::min(k0 + kAssetBlock, A);

        for (size_t a = k0; a < k1; ++a) {
          const int32_t dpos = draw_pos_[a];

          sh.samples.clear();
          sh.agg_samples.clear();
          sh.day_groups.clear();
          sh.hour_runs.clear();

          const feature_storage_t *pa = plane_.data() + a * asset_stride;
          int cur_hour = -1;      // 小时 run 跨天延续 (同小时连续样本即一段)
          uint32_t run_begin = 0; // 索引 agg_samples
          size_t agg_tick = 0;

          for (size_t i = 0; i < bd; ++i) {
            const feature_storage_t *p = pa + i * VR;
            const uint32_t day_begin = static_cast<uint32_t>(sh.agg_samples.size());

            for (size_t t = 0; t < VR; ++t) {
              const float v = static_cast<float>(p[t]);
              if (v != v) // 哨兵: valid 不过 或 真 NaN (已在 Phase IO 分账)
                continue;
              if (std::isinf(v)) {
                if (v > 0.0f)
                  ++sh.integrity.n_pos_inf;
                else
                  ++sh.integrity.n_neg_inf;
                continue;
              }
              if (v == 0.0f)
                ++sh.integrity.n_zero;
              ++sh.integrity.n_valid;
              sh.integrity.val_min = std::min(sh.integrity.val_min, v);
              sh.integrity.val_max = std::max(sh.integrity.val_max, v);

              if (dpos >= 0)
                sh.samples.push_back(v); // 绘制子集吃全量 → asset_klls_

              if (++agg_tick < agg_stride) // 其余只走 stride 抽样 → 聚合槽
                continue;
              agg_tick = 0;
              const int h = hour_lut[t];
              if (h != cur_hour) {
                if (cur_hour >= 0)
                  sh.hour_runs.push_back({run_begin, static_cast<uint32_t>(sh.agg_samples.size()),
                                          static_cast<uint8_t>(cur_hour)});
                cur_hour = h;
                run_begin = static_cast<uint32_t>(sh.agg_samples.size());
              }
              sh.agg_samples.push_back(v);
            }

            if (sh.agg_samples.size() > day_begin)
              sh.day_groups.push_back({day_begin, static_cast<uint32_t>(sh.agg_samples.size()),
                                       day_month[b0 + i], weekdays[b0 + i]});
          }
          if (cur_hour >= 0)
            sh.hour_runs.push_back({run_begin, static_cast<uint32_t>(sh.agg_samples.size()),
                                    static_cast<uint8_t>(cur_hour)});

          sh.integrity.n_total += bd * VR;

          // 聚合槽: 私有副本吃切片 (零拷贝), 块末一次并入全局
          if (!sh.agg_samples.empty()) {
            const float *s = sh.agg_samples.data();
            sh.total.addBatch(s, sh.agg_samples.size());
            for (const DayGroup &g : sh.day_groups) {
              sh.months[g.month].addBatch(s + g.begin, g.end - g.begin);
              sh.by_weekday[g.weekday].addBatch(s + g.begin, g.end - g.begin);
            }
            for (const HourRun &r : sh.hour_runs)
              sh.by_hour[r.hour].addBatch(s + r.begin, r.end - r.begin);
          }

          // 绘制子集: 累积 sketch + 导出整条线到 staging.
          // asset_klls_/lines_staging_ 是 worker 私有且每资产单线程 → 全程无锁
          if (dpos >= 0) {
            KLLcache &kll = asset_klls_[dpos];
            if (!sh.samples.empty())
              kll.addBatch(sh.samples.data(), sh.samples.size());

            AssetLine &ln = lines_staging_[dpos];
            ln.asset = static_cast<uint32_t>(a);
            ln.n = kll.totalCount();
            if (ln.n >= kMinAssetSamples) {
              const auto pdf = kll.exportPDF();
              assert(pdf.n <= ln.x.size());
              ln.n_pts = static_cast<uint32_t>(pdf.n);
              std::copy_n(pdf.x, pdf.n, ln.x.data());
              std::copy_n(pdf.y, pdf.n, ln.y.data());
              ln.mean = static_cast<float>(kll.mean());
              ln.var = static_cast<float>(kll.var());
              ln.skew = static_cast<float>(kll.skew());
              ln.kurt = static_cast<float>(kll.kurt());
              ln.w2 = w2_ref_.valid ? compute_w2(kll.exportICDF(), ln.mean, w2_ref_) : -1.0f;
            } else {
              ln.n_pts = 0;
              ln.w2 = -1.0f;
            }
          }
        }

        // 块末: 私有聚合槽一次并入全局 (锁内只有 sketch 级 merge)
        {
          std::lock_guard<std::mutex> lock(mutex);
          total.mergeWith(sh.total);
          for (size_t m = 0; m < n_months; ++m)
            months[m].kll.mergeWith(sh.months[m]);
          for (size_t w = 0; w < 7; ++w)
            by_weekday[w].mergeWith(sh.by_weekday[w]);
          for (size_t h = 0; h < 24; ++h)
            by_hour[h].mergeWith(sh.by_hour[h]);
          integrity.add(sh.integrity);
        }
        sh.total.clear();
        clear_slots(sh.months);
        clear_slots(sh.by_weekday);
        clear_slots(sh.by_hour);
        sh.integrity.clear();
      }
      scan_done.arrive_and_wait(); // → publish() (单线程), 重置抢任务原子供下批
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
  columns.clear();
  draw_pos_.clear();
  months.clear();
  lines.clear();
  by_hour.clear();
  by_weekday.clear();
  total.clear();
  integrity.clear();
  w2_ref_ = W2Ref{};
  // worker 私有缓冲: clear() 只在 worker join 后调用, 直接释放.
  // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存.
  asset_klls_ = std::vector<KLLcache>{};
  lines_staging_ = std::vector<AssetLine>{};
  plane_ = std::vector<feature_storage_t>{};
  shards_ = std::vector<Shard>{};
  days_loaded.store(0, std::memory_order_relaxed);
  days_total.store(0, std::memory_order_relaxed);
  lines_epoch.store(0, std::memory_order_relaxed);
  agg_stride.store(1, std::memory_order_relaxed);
  status.store(Status::Idle, std::memory_order_release);
}
