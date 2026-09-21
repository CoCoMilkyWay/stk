#include "shared/FeaturePreview.hpp"
#include "features/Backend/DayBatchPlane.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "features/DataDefine.hpp" // kPxEps (涨跌停比较容差, 全库统一)
#include "features/MetaFlag.hpp"   // fmeta::valid
#include "misc/profiler.hpp"

#include <algorithm>
#include <barrier>
#include <cmath>
#include <random>
#include <thread>

using namespace analysis;

// ============================================================================
// Runtime: worker 私有状态 (UI 不看)
// 并行面 = 特征块: 轮内各线程抢块 (IO + 扫描 + 发布全在块内), 每特征槽一轮只被一个线程写,
// 轮末栅栏隔开相邻轮 → 每特征累积器无锁无 shard, 也不需要归约
// ============================================================================

struct FeaturePreview::Runtime {
  // ---- 每特征累积 (跨轮) ----
  std::vector<KLLcache> klls;                               // [n_preview] 每特征累积 sketch
  std::vector<Integrity> integ;                             // [n_preview] 每特征累积账目
  std::vector<std::array<double, DayPSD::N_FREQS>> psd_sum; // [n_preview] 单日谱 (每 bin 占比) 累加
  std::vector<uint64_t> psd_n;                              // [n_preview] 参与谱平均的 (资产, 天) 数 (含恒值日的零票)
  std::vector<uint32_t> cage_n, cage_miss;                  // [n_preview] price 笼账目 (受检/笼外) 累积
  std::vector<uint8_t> int_only;                            // [n_preview] flag 探测: 有效值至今全整数 (粘性)
  // ---- 轮共享只读 (轮末栅栏 completion 里为下一轮载入, 线程扫描期只读) ----
  std::vector<float> cage_dn, cage_up; // [n_draw][VR] 当轮抽样资产的笼快照 (NaN = 当日无值)
  DayBatchPlane round_plane;           // [3][A][1][VR]: lim_dn / lim_up / _meta (轮内各块共用, 免每块重读 _meta)
  std::vector<uint32_t> asset_order;   // [A] 固定种子洗牌 (轮间旋转取片)
  // ---- 线程私有 ----
  struct Shard {
    DayBatchPlane plane;              // [块列][A][1][VR]
    std::vector<float> samples;       // 单 (特征, 轮) 有效样本缓冲
    std::array<float, kVR> day_buf{}; // 单 (资产, 日) 序列 (缺/门控 NaN → 谱前 ffill)
    std::unique_ptr<DayPSD> psd;      // FFT workspace (大对象, 堆上)
  };
  std::vector<Shard> shards; // [n_threads]

  // 容量准备 (幂等, 尺寸对得上零分配), 返回线程数
  size_t prepare(size_t n_pv, size_t A) {
    TraceN("PreviewPrepare");
    prepare_slots(klls, n_pv, kPvKllCapacity, kPvKllResolution);
    integ.assign(n_pv, Integrity{});
    psd_sum.assign(n_pv, {});
    psd_n.assign(n_pv, 0);
    cage_n.assign(n_pv, 0);
    cage_miss.assign(n_pv, 0);
    int_only.assign(n_pv, 1);
    const size_t n_draw = std::min(kPvAssetsPerRound, A);
    cage_dn.resize(n_draw * kVR);
    cage_up.resize(n_draw * kVR);
    round_plane.prepare(A, kLevel, 1, 3, 1, 3);
    asset_order = shuffled_order(A); // 无偏, 轮间旋转取片覆盖不同资产
    // 线程数 = min(硬件并发, 块数): 块是抢任务单位, 多于块数的线程整轮空转
    const size_t n_blocks = (n_pv + kPvBlockCols - 1) / kPvBlockCols;
    const size_t n_hw = std::max<size_t>(1, std::thread::hardware_concurrency());
    const size_t n_threads = std::min(n_hw, n_blocks);
    shards.resize(n_threads);
    for (Shard &sh : shards) {
      sh.samples.reserve(kPvAssetsPerRound * kVR);
      if (!sh.psd)
        sh.psd = std::make_unique<DayPSD>();
      // plane 按块 prepare (块列数随尾块变)
    }
    return n_threads;
  }
};

FeaturePreview::FeaturePreview() = default;
FeaturePreview::~FeaturePreview() = default;

// 日序列缺口前向填充. 填 0 的问题: 水平量 (价格 ~10 元) 固定分钟槽缺 → 一根 −值 尖刺, 能量比特征自身
// 波动大几个数量级, 平铺全谱盖掉谱形 (全表水平类特征长一个样). ffill 让缺口成平台 (零变动, 对谱中性),
// 真实跳变仍在; 日首缺口用首个有效值回填. 调用方保证至少一个有效值
static void ffill_day(float *s, size_t n) {
  size_t t = 0;
  while (s[t] != s[t]) {
    ++t;
    assert(t < n);
  }
  std::fill_n(s, t, s[t]);
  for (++t; t < n; ++t)
    if (s[t] != s[t])
      s[t] = s[t - 1];
}

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
// Build (轮训: 轮 = 抽样日, 轮内按特征分块, 块末发布; 首帧 = 一块的 IO + 扫描)
// ============================================================================

bool FeaturePreview::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("PreviewBuild");
  assert(rt_ && A_ > 0 && !feat_cols_.empty() && "reset_for_build 先于 build");
  Runtime &rt = *rt_;
  const size_t VR = kVR;
  assert(level_valid_rows(kLevel) == VR);

  // 日期枚举 → 固定种子洗牌取前 kPvRounds 个 (无偏覆盖全区间, 轮序即收敛序)
  std::vector<std::string> dates = enumerate_dates(reader, months_).dates;
  if (dates.empty())
    return true; // 库为空: 直接 Done, 表格全 "—"
  std::shuffle(dates.begin(), dates.end(), std::mt19937{0x5eed});
  const size_t n_rounds = std::min(kPvRounds, dates.size());
  dates.resize(n_rounds);
  total.store(n_rounds, std::memory_order_release);

  const size_t n_pv = feat_cols_.size();
  const size_t n_draw = std::min(kPvAssetsPerRound, A_);
  const size_t n_blocks = (n_pv + kPvBlockCols - 1) / kPvBlockCols;
  const size_t n_threads = rt.prepare(n_pv, A_);

  // 轮首共享 IO (单线程, 轮末栅栏 completion 里跑): lim_dn / lim_up / _meta 三列一次读 →
  // 抽样资产当日笼快照 (price 逐日判定融合进块扫描); _meta 留在 round_plane 供各块门控
  // (has_valid=false: plane 只把真 NaN 折成哨兵, _meta 值原样保留, 门控在扫描侧按各特征 valid_type 判)
  std::vector<size_t> round_cols{lim_dn_col_, lim_up_col_, meta_col_};
  auto load_round = [&](size_t r) {
    TraceN("PreviewRoundIO");
    rt.round_plane.load_day(reader, dates[r], round_cols, false, L2::ValidType::ALL, 0, 0);
    // 笼外扩容差 (存快照时做, 热循环保持严格比较): 贴板成交价与 lim 两列来源不同
    // (整数分换算 vs 解析/计算), 原始 float 差 ≤ kPxEps, 落 f16 后可差 1 ulp (≈ 值/1024);
    // 严格比较会把涨跌停分钟误判笼外 (一字板整天 close == lim_up). NaN 经算术照传
    constexpr float kF16Rel = 1.0f / 1024.0f; // f16 尾数 10 bit → 1 ulp ≤ |值|/1024
    const size_t a_off = (r * n_draw) % A_;   // 洗牌序旋转取片
    for (size_t k = 0; k < n_draw; ++k) {
      const size_t a = rt.asset_order[(a_off + k) % A_];
      const feature_storage_t *dn_p = rt.round_plane.series(0, a, 0);
      const feature_storage_t *up_p = rt.round_plane.series(1, a, 0);
      for (size_t t = 0; t < VR; ++t) {
        const float dn = static_cast<float>(dn_p[t]), up = static_cast<float>(up_p[t]);
        rt.cage_dn[k * VR + t] = dn - (std::fabs(dn) * kF16Rel + kPxEps);
        rt.cage_up[k * VR + t] = up + (std::fabs(up) * kF16Rel + kPxEps);
      }
    }
  };

  // ==========================================================================
  // 轮循环: 一波常驻线程, 轮内抢特征块 (块 = IO + 扫描 + 发布, 全在锁外, 只在拷 cells 时短锁),
  // 轮末一道栅栏. completion (单线程, 所有线程到齐后、解除阻塞前执行): 进度 / 重置抢块原子 /
  // 为下一轮载入共享列 → stop 与 round_plane 对所有线程一致可见
  // ==========================================================================
  std::atomic<size_t> next_block{0};
  bool stop = false;
  size_t round = 0; // completion 私有推进

  auto round_end = [&]() noexcept {
    next_block.store(0, std::memory_order_relaxed);
    if (cancel.load(std::memory_order_relaxed)) {
      stop = true;
      return;
    }
    publish_progress(1);
    if (++round < n_rounds)
      load_round(round);
  };
  std::barrier round_done(static_cast<ptrdiff_t>(n_threads), round_end);

  auto worker = [&](size_t tid) {
    Runtime::Shard &sh = rt.shards[tid];
    DayPSD &psd = *sh.psd;
    std::vector<size_t> cols;
    cols.reserve(kPvBlockCols);
    std::array<Cell, kPvBlockCols> block_cells; // 块成品 staging (锁外填, 短锁拷贝)

    for (size_t r = 0; r < n_rounds && !stop; ++r) {
      const size_t a_off = (r * n_draw) % A_;

      for (;;) {
        const size_t b = next_block.fetch_add(1, std::memory_order_relaxed);
        if (b >= n_blocks || cancel.load(std::memory_order_relaxed))
          break;
        const size_t f0 = b * kPvBlockCols;
        const size_t f1 = std::min(f0 + kPvBlockCols, n_pv);
        const size_t nb = f1 - f0;

        // 块 IO: nb 值列 (门控列在 round_plane)
        {
          TraceN("PreviewIO");
          cols.assign(feat_cols_.begin() + f0, feat_cols_.begin() + f1);
          sh.plane.prepare(A_, kLevel, 1, nb, 1, kPvBlockCols);
          sh.plane.load_day(reader, dates[r], cols, false, L2::ValidType::ALL, 0, 0);
        }

        // 块扫描: 每特征抽 n_draw 个资产
        TraceN("PreviewScan");
        for (size_t i = 0; i < nb; ++i) {
          const size_t slot = f0 + i;
          const L2::ValidType vt = valid_types_[slot];
          Integrity &it = rt.integ[slot];
          uint32_t cage_n = 0, cage_miss = 0; // 本 (特征, 轮) price 笼账目
          bool nonint = false;                // 本 (特征, 轮) 见过非整数有效值 (flag 探测)
          sh.samples.clear();

          for (size_t k = 0; k < n_draw; ++k) {
            const size_t a = rt.asset_order[(a_off + k) % A_];
            const feature_storage_t *v = sh.plane.series(i, a, 0);
            const feature_storage_t *mv = rt.round_plane.series(2, a, 0);
            it.n_total += VR;
            size_t n_ok = 0;
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
                  // flag: 整数判定必须在有限值分支内 (NaN/±inf 转 int 是 UB); f16 值域 ±65504,
                  // int32 截断安全; -0.0 == 0 → 整数. 无分支 OR 累积, 轮末一次并进粘性位
                  nonint |= (x != static_cast<float>(static_cast<int32_t>(x)));
                  // price 笼: 对照该资产当日 [lim_dn, lim_up] (NaN 笼 = 当日无值, 不受检)
                  const float dn = rt.cage_dn[k * VR + t], up = rt.cage_up[k * VR + t];
                  if (dn == dn && up == up) {
                    ++cage_n;
                    cage_miss += !(x >= dn && x <= up);
                  }
                }
              }
              sh.day_buf[t] = ok ? x : kNaN; // 缺/门控/±inf 记 NaN, 谱前 ffill_day (不填 0, 见其注释)
              n_ok += ok;
              if (ok)
                sh.samples.push_back(x);
            }

            // 单日谱 → Parseval 归一累加: 除以当日总功率 (k ∈ [1, N/2], 跳 DC) → 每 bin = 方差占比.
            // 特征量纲被除掉, 跨特征绝对可比 (白噪声参照 = 1/kPvPsdPts 全表恒定);
            // 每 (资产, 天) 一票, 高波动日不再以方差加权主导谱形.
            // 有一个有效值就算谱 (ffill 后稠密; 稀疏日 = 阶梯序列, 谱偏红是其真实形态, 不设最少样本门槛 ——
            // 门控只留少数分钟的特征否则整列无谱). 恒值日 (日频常量 / 停牌) total = 0 谱无定义, 按
            // "占比全 0" 投一票: 曲线整体下沉 log10(有动的天占比), 图上同时看到 "多久动一次" (高度) 与
            // "怎么动" (形状, 重心尺度无关); 全部恒值 → 全 -20 地板 (贴底平线). 绝对方差在 Range 列 sd
            if (n_ok > 0) {
              ffill_day(sh.day_buf.data(), VR);
              [[maybe_unused]] const bool done = psd.compute(sh.day_buf.data());
              assert(done && "ffill 后稠密, 有效数 = VR");
              double total = 0.0;
              for (size_t q = 1; q <= kPvPsdPts; ++q)
                total += static_cast<double>(psd.power[q]);
              if (total > 0.0) {
                const double inv = 1.0 / total;
                auto &sum = rt.psd_sum[slot];
                for (size_t q = 1; q <= kPvPsdPts; ++q)
                  sum[q] += static_cast<double>(psd.power[q]) * inv;
              }
              ++rt.psd_n[slot];
            }
          }
          if (!sh.samples.empty())
            rt.klls[slot].addBatch(sh.samples.data(), sh.samples.size());
          rt.cage_n[slot] += cage_n;
          rt.cage_miss[slot] += cage_miss;
          rt.int_only[slot] &= static_cast<uint8_t>(!nonint);
        }

        // 块末发布: 锁外导出成品, 一次短锁拷贝整块进 cells
        {
          TraceN("PreviewPublish");
          for (size_t i = 0; i < nb; ++i) {
            const size_t slot = f0 + i;
            Cell &c = block_cells[i];
            c = Cell{}; // staging 复用: psd_n == 0 时不填 psd, 必须清掉上一块的残值
            c.fill(rt.klls[slot], kPvMinSamples);
            c.integrity = rt.integ[slot];
            c.cage_n = rt.cage_n[slot];
            c.cage_miss = rt.cage_miss[slot];
            c.int_only = rt.int_only[slot] != 0;
            c.psd_n = static_cast<uint32_t>(rt.psd_n[slot]);
            if (c.psd_n > 0) {
              // 线性域取均值再 log10 (log 域均值会系统性偏低: E log < log E); 周期升序存
              const double inv = 1.0 / static_cast<double>(c.psd_n);
              for (size_t j = 0; j < kPvPsdPts; ++j) {
                const double p = rt.psd_sum[slot][kPvPsdK(j)] * inv;
                c.psd[j] = p > 1e-20 ? static_cast<float>(std::log10(p)) : -20.0f;
              }
            }
          }
          std::lock_guard<std::mutex> lock(mutex);
          for (size_t i = 0; i < nb; ++i)
            cells[feat_cols_[f0 + i]] = block_cells[i];
        }
        epoch.fetch_add(1, std::memory_order_release);
      }

      round_done.arrive_and_wait(); // → round_end() (单线程): 进度 / 下一轮共享列
    }
  };

  load_round(0); // 首轮共享列在起线程前载好
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
