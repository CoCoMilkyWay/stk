#include "shared/FeaturePreview.hpp"
#include "features/Backend/DayBatchPlane.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "features/DataDefine.hpp" // kPxEps (涨跌停比较容差, 全库统一)
#include "features/MetaFlag.hpp"   // fmeta::valid
#include "misc/profiler.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>

using namespace analysis;

// ============================================================================
// Runtime: worker 私有状态 (UI 不看; 单线程, 无 shard)
// ============================================================================

struct FeaturePreview::Runtime {
  std::vector<KLLcache> klls;                                 // [n_preview] 每特征累积 sketch
  std::vector<Integrity> integ;                               // [n_preview] 每特征累积账目
  std::vector<std::array<double, PvSegPSD::N_FREQS>> psd_sum; // [n_preview] 段谱累加
  std::vector<uint64_t> psd_n;                                // [n_preview] 参与谱平均的 (资产, 段) 数
  std::vector<uint32_t> cage_n, cage_miss;                    // [n_preview] price 笼账目 (受检/笼外) 累积
  std::vector<float> cage_dn, cage_up;                        // [段日][n_draw][VR] 段内逐日笼快照 (NaN = 当日无值)
  std::vector<uint32_t> asset_order;                          // [A] 固定种子洗牌 (段间旋转取片)
  std::vector<float> samples;                                 // 单 (特征, 日) 有效样本缓冲
  std::vector<float> seg_buf;                                 // [块列][n_draw][段长] 5 日拼接序列 (缺/门控填 0 值)
  PvSegPSD psd;                                               // FFT workspace (N = 2048)
  std::array<float, PvSegPSD::N_FREQS> pow_b{};               // 打包 FFT 的第二条功率谱出口
  DayBatchPlane plane;                                        // [块列+1][A][1][VR]

  // 容量准备 (幂等, 尺寸对得上零分配)
  void prepare(size_t n_pv, size_t A) {
    TraceN("PreviewPrepare");
    prepare_slots(klls, n_pv, kPvKllCapacity, kPvKllResolution);
    integ.assign(n_pv, Integrity{});
    psd_sum.assign(n_pv, {});
    psd_n.assign(n_pv, 0);
    cage_n.assign(n_pv, 0);
    cage_miss.assign(n_pv, 0);
    const size_t n_draw = std::min(kPvAssetsPerSeg, A);
    cage_dn.resize(kPvSegDays * n_draw * kVR);
    cage_up.resize(kPvSegDays * n_draw * kVR);
    seg_buf.resize(kPvBlockCols * n_draw * kPvSegDays * kVR); // 512 资产 × 8 列 × 1275 min ≈ 21 MB
    asset_order = shuffled_order(A);                          // 无偏, 段间旋转取片覆盖不同资产
    samples.reserve(kPvAssetsPerSeg * kVR);
    // plane 按块 prepare (块列数随尾块变)
  }
};

FeaturePreview::FeaturePreview() = default;
FeaturePreview::~FeaturePreview() = default;

// ============================================================================
// Reset
// ============================================================================

void FeaturePreview::reset_for_build(std::vector<size_t> feat_cols,
                                     std::vector<L2::ValidType> valid_types,
                                     size_t meta_col, size_t lim_dn_col, size_t lim_up_col,
                                     std::vector<std::string> month_keys,
                                     size_t n_features, size_t n_assets) {
  TraceN("PreviewReset");
  assert(!feat_cols.empty() && feat_cols.size() == valid_types.size());
  assert(std::is_sorted(feat_cols.begin(), feat_cols.end()) && feat_cols.back() < n_features);
  assert(meta_col < n_features && n_assets > 0 && !month_keys.empty());
  assert(lim_dn_col < n_features && lim_up_col < n_features);
  std::lock_guard<std::mutex> lock(mutex);

  feat_cols_ = std::move(feat_cols);
  valid_types_ = std::move(valid_types);
  meta_col_ = meta_col;
  lim_dn_col_ = lim_dn_col;
  lim_up_col_ = lim_up_col;
  months_ = std::move(month_keys);
  A_ = n_assets;

  // 预览是全表底图: 输入变了旧格子无参照意义, 清零 (尺寸对得上就地 fill, 零分配)
  if (cells.size() != n_features)
    cells.assign(n_features, Cell{});
  else
    std::fill(cells.begin(), cells.end(), Cell{});
  if (!rt_)
    rt_ = std::make_unique<Runtime>();

  begin_build();
}

// ============================================================================
// Build (段训: 段 = 连续 kPvSegDays 个抽样日, 段内按特征分块、块内逐日扫描,
//         凑齐一段做段 FFT, 块末发布; 首帧 = 一块的 IO + 扫描)
// ============================================================================

bool FeaturePreview::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("PreviewBuild");
  assert(rt_ && A_ > 0 && !feat_cols_.empty() && "reset_for_build 先于 build");
  Runtime &rt = *rt_;
  const size_t VR = kVR;
  assert(level_valid_rows(kLevel) == VR);
  constexpr size_t SEG = kPvSegDays * kVR; // 段长 (分钟)

  // 日期枚举 (升序) → 连续 kPvSegDays 日为一段, 锚点固定种子洗牌取前 kPvSegments 个
  // (段内连续保低频, 段间随机保无偏覆盖; 段可重叠, 库短时段数自然减)
  std::vector<std::string> dates = enumerate_dates(reader, months_).dates;
  if (dates.size() < kPvSegDays)
    return true; // 库太短拼不出一段: 直接 Done, 表格全 "—"
  std::vector<size_t> anchors(dates.size() - kPvSegDays + 1);
  std::iota(anchors.begin(), anchors.end(), size_t{0});
  std::shuffle(anchors.begin(), anchors.end(), std::mt19937{0x5eed});
  const size_t n_seg = std::min(kPvSegments, anchors.size());
  anchors.resize(n_seg);
  total.store(n_seg, std::memory_order_release);

  const size_t n_pv = feat_cols_.size();
  const size_t n_draw = std::min(kPvAssetsPerSeg, A_);
  rt.prepare(n_pv, A_);
  std::vector<size_t> cols;
  cols.reserve(kPvBlockCols + 1);

  for (size_t s = 0; s < n_seg; ++s) {
    const size_t a_off = (s * n_draw) % A_; // 洗牌序旋转取片

    // 段首: 笼两列 (lim_dn/lim_up) 逐日 IO → 抽样资产段内逐日笼快照 (price 判定融合进块扫描)
    {
      TraceN("PreviewCageIO");
      // 笼外扩容差 (存快照时做, 热循环保持严格比较): 贴板成交价与 lim 两列来源不同
      // (整数分换算 vs 解析/计算), 原始 float 差 ≤ kPxEps, 落 f16 后可差 1 ulp (≈ 值/1024);
      // 严格比较会把涨跌停分钟误判笼外 (一字板整天 close == lim_up). NaN 经算术照传
      constexpr float kF16Rel = 1.0f / 1024.0f; // f16 尾数 10 bit → 1 ulp ≤ |值|/1024
      for (size_t d = 0; d < kPvSegDays; ++d) {
        cols.assign({lim_dn_col_, lim_up_col_});
        rt.plane.prepare(A_, kLevel, 1, 2, 1, kPvBlockCols + 1);
        rt.plane.load_day(reader, dates[anchors[s] + d], cols, false, L2::ValidType::ALL, 0, 0);
        for (size_t k = 0; k < n_draw; ++k) {
          const size_t a = rt.asset_order[(a_off + k) % A_];
          const feature_storage_t *dn_p = rt.plane.series(0, a, 0);
          const feature_storage_t *up_p = rt.plane.series(1, a, 0);
          float *cd = &rt.cage_dn[(d * n_draw + k) * VR];
          float *cu = &rt.cage_up[(d * n_draw + k) * VR];
          for (size_t t = 0; t < VR; ++t) {
            const float dn = static_cast<float>(dn_p[t]), up = static_cast<float>(up_p[t]);
            cd[t] = dn - (std::fabs(dn) * kF16Rel + kPxEps);
            cu[t] = up + (std::fabs(up) * kF16Rel + kPxEps);
          }
        }
      }
    }

    for (size_t f0 = 0; f0 < n_pv; f0 += kPvBlockCols) {
      if (cancel.load(std::memory_order_relaxed))
        return false;
      const size_t f1 = std::min(f0 + kPvBlockCols, n_pv);
      const size_t nb = f1 - f0;

      // 段内逐日: 块 IO + 扫描 (账目/KLL/笼逐日累积, 序列写进段拼接缓冲)
      for (size_t d = 0; d < kPvSegDays; ++d) {
        // 块 IO: nb 值列 + _meta 作为末值列 (has_valid=false → 门控在扫描侧按
        // 各特征 valid_type 判; plane 只把真 NaN 折成哨兵, _meta 值原样保留)
        {
          TraceN("PreviewIO");
          cols.assign(feat_cols_.begin() + f0, feat_cols_.begin() + f1);
          cols.push_back(meta_col_);
          rt.plane.prepare(A_, kLevel, 1, nb + 1, 1, kPvBlockCols + 1);
          rt.plane.load_day(reader, dates[anchors[s] + d], cols, false, L2::ValidType::ALL, 0, 0);
        }

        // 块扫描: 每特征抽 n_draw 个资产
        TraceN("PreviewScan");
        for (size_t i = 0; i < nb; ++i) {
          const size_t slot = f0 + i;
          const L2::ValidType vt = valid_types_[slot];
          Integrity &it = rt.integ[slot];
          uint32_t cage_n = 0, cage_miss = 0; // 本 (特征, 日) price 笼账目
          rt.samples.clear();

          for (size_t k = 0; k < n_draw; ++k) {
            const size_t a = rt.asset_order[(a_off + k) % A_];
            const feature_storage_t *v = rt.plane.series(i, a, 0);
            const feature_storage_t *mv = rt.plane.series(nb, a, 0);
            it.n_total += VR;
            float *seg = &rt.seg_buf[(i * n_draw + k) * SEG + d * VR];
            const float *cd = &rt.cage_dn[(d * n_draw + k) * VR];
            const float *cu = &rt.cage_up[(d * n_draw + k) * VR];
            for (size_t t = 0; t < VR; ++t) {
              const float x = static_cast<float>(v[t]);
              // 账目与 Dist 同口径: 只记门控过的格子 (NaN = 哨兵, 真 NaN 已折叠; ±inf 单列);
              // 只有有限值进 sketch / PSD
              bool ok = false;
              if (fmeta::valid(static_cast<float>(mv[t]), vt)) {
                if (x != x) {
                  ++it.n_nan;
                } else if (std::isinf(x)) {
                  ++(x > 0.0f ? it.n_pos_inf : it.n_neg_inf);
                } else {
                  ok = true;
                  it.add_finite(x);
                  // price 笼: 对照该资产当日 [lim_dn, lim_up] (NaN 笼 = 当日无值, 不受检)
                  if (cd[t] == cd[t] && cu[t] == cu[t]) {
                    ++cage_n;
                    cage_miss += !(x >= cd[t] && x <= cu[t]);
                  }
                }
              }
              // NaN/门控/±inf 填真实 0 值进谱 (不做中性填补): 缺口跳变本身就是特征
              // 质量信号, 要暴露在高频端; 全 0 段 → total = 0 仍被跳过
              seg[t] = ok ? x : 0.0f;
              if (ok)
                rt.samples.push_back(x);
            }
          }
          if (!rt.samples.empty())
            rt.klls[slot].addBatch(rt.samples.data(), rt.samples.size());
          rt.cage_n[slot] += cage_n;
          rt.cage_miss[slot] += cage_miss;
        }
      }

      // 段谱: 每 (特征, 资产) 5 日拼接序列做 FFT (段级去均值; 不逐日去均值 ——
      // 那会杀掉跨日低频; 隔夜跳变留谱, TOD 模式成 255 min 谱线可见).
      // seg_buf 稠密 (缺/门控已填 0) → 资产两两打包一次复 FFT, 次数减半
      {
        TraceN("PreviewPSD");
        // Parseval 归一累加: 段谱除以自身总功率 (跳 DC) → 每 bin = 方差占比.
        // 特征量纲被除掉, 跨特征绝对可比 (白噪声参照 = 1/kPvPsdPts 全表恒定);
        // 每 (资产, 段) 一票, 高波动段不再以方差加权主导谱形.
        // 恒值段 total = 0, 谱无定义, 不计入 (绝对方差在 Range 列 sd, 不丢信息)
        const auto accum = [&rt](size_t slot, const float *pw) {
          double total = 0.0;
          for (size_t q = 1; q < PvSegPSD::N_FREQS; ++q)
            total += static_cast<double>(pw[q]);
          if (total <= 0.0)
            return;
          const double inv = 1.0 / total;
          auto &sum = rt.psd_sum[slot];
          for (size_t q = 1; q < PvSegPSD::N_FREQS; ++q)
            sum[q] += static_cast<double>(pw[q]) * inv;
          ++rt.psd_n[slot];
        };
        for (size_t i = 0; i < nb; ++i) {
          const size_t slot = f0 + i;
          const float *base = &rt.seg_buf[i * n_draw * SEG];
          size_t k = 0;
          for (; k + 1 < n_draw; k += 2) {
            rt.psd.compute_pair_dense(base + k * SEG, base + (k + 1) * SEG,
                                      rt.psd.power.data(), rt.pow_b.data());
            accum(slot, rt.psd.power.data());
            accum(slot, rt.pow_b.data());
          }
          if (k < n_draw && rt.psd.compute(base + k * SEG)) // 奇数尾巴走单条路径
            accum(slot, rt.psd.power.data());
        }
      }

      // 块末发布: 锁外导出成品, 短锁拷贝进 cells
      {
        TraceN("PreviewPublish");
        for (size_t i = 0; i < nb; ++i) {
          const size_t slot = f0 + i;
          Cell c;
          c.fill(rt.klls[slot], kPvMinSamples);
          c.integrity = rt.integ[slot];
          c.cage_n = rt.cage_n[slot];
          c.cage_miss = rt.cage_miss[slot];
          c.psd_n = static_cast<uint32_t>(rt.psd_n[slot]);
          if (c.psd_n > 0) {
            const double inv = 1.0 / static_cast<double>(rt.psd_n[slot]);
            for (size_t q = 1; q < PvSegPSD::N_FREQS; ++q) {
              const double p = rt.psd_sum[slot][q] * inv;
              c.psd[q - 1] = p > 1e-20 ? static_cast<float>(std::log10(p)) : -20.0f;
            }
          }
          std::lock_guard<std::mutex> lock(mutex);
          cells[feat_cols_[slot]] = c;
        }
      }
      epoch.fetch_add(1, std::memory_order_release);
    }

    publish_progress(1);
  }

  return !cancel.load(std::memory_order_relaxed);
}

// ============================================================================
// Clear
// ============================================================================

void FeaturePreview::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存
  cells = std::vector<Cell>{};
  rt_.reset(); // worker 已 join, 整体释放
  feat_cols_.clear();
  valid_types_.clear();
  months_.clear();
  A_ = 0;
  reset_idle();
}
