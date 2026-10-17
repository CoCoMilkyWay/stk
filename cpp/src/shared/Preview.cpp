#include "shared/Preview.hpp"
#include "features/Storage/DayBatchPlane.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "features/DataDefine.hpp" // kPxEps (涨跌停比较容差, 全库统一)
#include "misc/profiler.hpp"

#include <algorithm>
#include <cmath>

using namespace analysis;

// ============================================================================
// Runtime: worker 私有状态 (UI 不看)
// 并行面 = 特征块: 轮内一块一任务 (IO + 扫描 + 发布全在块内), 每特征槽一轮只被一个任务写,
// 每轮一次 dispatch (返回即栅栏) 隔开相邻轮 → 每特征累积器无锁无 shard, 也不需要归约
// ============================================================================

struct Preview::Runtime {
  // ---- 每特征累积 (跨轮) ----
  std::vector<KLLcache> klls;              // [n_preview] 每特征累积 sketch
  std::vector<Integrity> integ;            // [n_preview] 每特征累积账目
  std::vector<PsdAcc> psd;                 // [n_preview] 单日谱 (每 bin 占比) 累加 (含恒值日的零票)
  std::vector<uint32_t> cage_n, cage_miss; // [n_preview] price 笼账目 (受检/笼外) 累积
  std::vector<uint8_t> int_only;           // [n_preview] flag 探测: 有效值至今全整数 (粘性)
  // ---- 轮共享只读 (轮首单线程载入, 块任务只读) ----
  std::vector<float> cage_dn, cage_up; // [n_draw][VR] 当轮抽样资产的笼快照 (NaN = 当日无值)
  FeatureRead::DayColumns cage_day;    // [T][A_all] × {lim_dn, lim_up} (Bar 档旁读, 全轴)
  FeatureRead::DayColumns round;       // [T][A] × {ts_valid} (门控时; 轮内各块共用, 免每块重读)
  std::vector<size_t> round_cols;
  std::vector<uint32_t> asset_order; // [A] 固定种子洗牌 (轮间旋转取片)
  // ---- 线程私有 (按 tid) ----
  struct Shard {
    TsPlane plane;                        // [块列][A][1][VR]
    fmeta::ColumnSet cols;                // 块值列 (不带门控: 门控行外给)
    FeatureRead::DayColumns day;          // IO 暂存
    std::array<Cell, kPvBlockCols> cells; // 块成品 staging (锁外填, 短锁拷贝)
    std::vector<float> samples;           // 单 (特征, 轮) 有效样本缓冲
    std::array<float, kVR> day_buf{};     // 单 (资产, 日) 序列 (缺/门控 NaN → 谱前 ffill)
    std::unique_ptr<DayPSD> day_psd;      // FFT workspace (大对象, 堆上)
  };
  std::vector<Shard> shards; // [n_threads]
  std::unique_ptr<misc::exec::ForkJoin> ex;

  // 容量准备 (幂等, 尺寸对得上零分配), 返回执行器
  misc::exec::ForkJoin &prepare(const Spec &spec, size_t A, size_t A_all) {
    TraceN("PreviewPrepare");
    const size_t n_pv = spec.cols.n();
    prepare_slots(klls, n_pv, kPvKllCapacity, kPvKllResolution);
    integ.assign(n_pv, Integrity{});
    psd.assign(n_pv, PsdAcc{});
    cage_n.assign(n_pv, 0);
    cage_miss.assign(n_pv, 0);
    int_only.assign(n_pv, 1);
    const size_t n_draw = std::min(kPvAssetsPerRound, A);
    cage_dn.resize(n_draw * kVR);
    cage_up.resize(n_draw * kVR);
    cage_day.preallocate(A_all, kLevel, 2);
    round_cols.clear();
    if (spec.cols.gated())
      round_cols.push_back(spec.cols.gate());
    round.preallocate(A, kLevel, 1);
    asset_order = shuffled_order(A); // 无偏, 轮间旋转取片覆盖不同资产
    // 线程数 = min(硬件并发, 块数): 块是抢任务单位, 多于块数的线程整轮空转
    const size_t n_threads = threads_for((n_pv + kPvBlockCols - 1) / kPvBlockCols);
    shards.resize(n_threads);
    for (Shard &sh : shards) {
      sh.cols.io.reserve(kPvBlockCols);
      sh.cols.vts.reserve(kPvBlockCols);
      sh.day.preallocate(A, kLevel, kPvBlockCols);
      sh.samples.reserve(kPvAssetsPerRound * kVR);
      if (!sh.day_psd)
        sh.day_psd = std::make_unique<DayPSD>();
      // plane 按块 prepare (块列数随尾块变)
    }
    return misc::exec::ensure(ex, static_cast<int>(n_threads));
  }
};

Preview::Preview() = default;
Preview::~Preview() = default;

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

void Preview::reset(ReadScope scope, Spec sp) {
  TraceN("PreviewReset");
  const size_t n_pv = sp.cols.n();
  assert(n_pv > 0 && std::is_sorted(sp.cols.io.begin(), sp.cols.io.begin() + n_pv));
  assert(sp.cols.io[n_pv - 1] < sp.n_features);
  assert(sp.cage.size() == 2);
  assert(scope.lib.axis.size() > 0 && !scope.months.empty());
  assert(scope.bar.axis.size() > 0 && scope.lib.axis.ids.back() < scope.bar.axis.size());
  std::lock_guard<std::mutex> lock(mutex);

  scope_ = std::move(scope);
  spec = std::move(sp);

  // 预览是全表底图: 输入变了旧格子无参照意义, 清零 (尺寸对得上就地 fill, 零分配)
  if (snap.cells.size() != spec.n_features)
    snap.cells.assign(spec.n_features, Cell{});
  else
    std::fill(snap.cells.begin(), snap.cells.end(), Cell{});
  if (!rt_)
    rt_ = std::make_unique<Runtime>();

  publish();
}

// ============================================================================
// Build (轮训: 轮 = 抽样日, 轮内按特征分块, 块末发布; 首帧 = 一块的 IO + 扫描)
// ============================================================================

bool Preview::build(FeatureRead &reader, const FeatureRead &all, const std::atomic<bool> &cancel) {
  TraceN("PreviewBuild");
  assert(rt_ && "reset 先于 build");
  Runtime &rt = *rt_;
  const size_t VR = kVR;
  const size_t A = scope_.lib.axis.size();
  assert(level_valid_rows(kLevel) == VR);
  assert(A > 0 && spec.cols.n() > 0 && "reset 先于 build");

  // 天序: 洗牌取前 kPvRounds 个 (无偏覆盖全区间, 轮序即收敛序)
  const DayPlan plan = plan_days(reader, scope_.months, DayOrder::Shuffle, kPvRounds);
  if (plan.size() == 0)
    return true; // 库为空: 直接 Done, 表格全 "—"
  const size_t n_rounds = plan.size();
  total.store(n_rounds, std::memory_order_release);

  const fmeta::ColumnSet &cols = spec.cols;
  const size_t n_pv = cols.n();
  const size_t n_draw = std::min(kPvAssetsPerRound, A);
  const size_t n_blocks = (n_pv + kPvBlockCols - 1) / kPvBlockCols;
  const size_t A_all = scope_.bar.axis.size();
  const uint32_t *ids = scope_.lib.axis.ids.data(); // 主轴 a → 全轴
  misc::exec::ForkJoin &ex = rt.prepare(spec, A, A_all);
  const bool gated = cols.gated();

  // 轮首共享 IO (单线程): Bar 档 lim_dn / lim_up 一次读 → 抽样资产当日笼快照 (price 逐日判定融合进块扫描);
  // 主库 ts_valid (门控时) 原样留在 round 供各块平面门控
  auto load_round = [&](size_t r) {
    TraceN("PreviewRoundIO");
    if (gated)
      reader.load_day_columns(plan.dates[r], rt.round_cols, rt.round);
    all.load_day_columns(plan.dates[r], spec.cage, rt.cage_day);
    // 笼外扩容差 (存快照时做, 热循环保持严格比较): 贴板成交价与 lim 两列来源不同
    // (整数分换算 vs 解析/计算), 原始 float 差 ≤ kPxEps, 落 f16 后可差 1 ulp (≈ 值/1024);
    // 严格比较会把涨跌停分钟误判笼外 (一字板整天 close == lim_up). NaN 经算术照传
    constexpr float kF16Rel = 1.0f / 1024.0f; // f16 尾数 10 bit → 1 ulp ≤ |值|/1024
    const size_t a_off = (r * n_draw) % A;    // 洗牌序旋转取片
    for (size_t k = 0; k < n_draw; ++k) {
      const size_t a = rt.asset_order[(a_off + k) % A];
      const feature_storage_t *dn_p = rt.cage_day.col(0) + ids[a];
      const feature_storage_t *up_p = rt.cage_day.col(1) + ids[a];
      for (size_t t = 0; t < VR; ++t) {
        const float dn = static_cast<float>(dn_p[t * A_all]), up = static_cast<float>(up_p[t * A_all]);
        rt.cage_dn[k * VR + t] = dn - (std::fabs(dn) * kF16Rel + kPxEps);
        rt.cage_up[k * VR + t] = up + (std::fabs(up) * kF16Rel + kPxEps);
      }
    }
  };

  // ==========================================================================
  // 轮循环: 轮首共享列 (单线程) → 特征块一块一任务 (块 = IO + 扫描 + 发布, 全在锁外, 只在拷 cells 时短锁),
  // 一次 dispatch (返回即栅栏) → 轮末进度 (单线程)
  // ==========================================================================
  for (size_t r = 0; r < n_rounds; ++r) {
    load_round(r);
    const size_t a_off = (r * n_draw) % A;
    const feature_storage_t *gate = gated ? rt.round.col(0) : nullptr;

    run_phase(ex, n_blocks, cancel, [&](size_t blk, size_t tid) {
      Runtime::Shard &sh = rt.shards[tid];
      DayPSD &day_psd = *sh.day_psd;
      std::array<Cell, kPvBlockCols> &block_cells = sh.cells;
      const size_t f0 = blk * kPvBlockCols;
      const size_t f1 = std::min(f0 + kPvBlockCols, n_pv);
      const size_t nb = f1 - f0;

      // 块 IO: nb 值列 (门控行在 round, 各列按自己的 vt 折哨兵)
      {
        TraceN("PreviewIO");
        sh.cols.io.assign(cols.io.begin() + f0, cols.io.begin() + f1);
        sh.cols.vts.assign(cols.vts.begin() + f0, cols.vts.begin() + f1);
        sh.plane.prepare(A, kLevel, 1, nb);
        sh.plane.load_day(reader, plan.dates[r], sh.cols, 0, sh.day, gate);
      }

      // 块扫描: 每特征抽 n_draw 个资产
      TraceN("PreviewScan");
      for (size_t i = 0; i < nb; ++i) {
        const size_t slot = f0 + i;
        Integrity &it = rt.integ[slot];
        uint32_t cage_n = 0, cage_miss = 0; // 本 (特征, 轮) price 笼账目
        bool nonint = false;                // 本 (特征, 轮) 见过非整数有效值 (flag 探测)
        sh.samples.clear();

        for (size_t k = 0; k < n_draw; ++k) {
          const size_t a = rt.asset_order[(a_off + k) % A];
          const feature_storage_t *v = sh.plane.series(i, a, 0);
          it.n_total += VR;
          size_t n_ok = 0;
          for (size_t t = 0; t < VR; ++t) {
            // 账目与 Dist 同口径 (按位分账: 门控不过 / NaN / ±inf / 有限); 只有有限值进 sketch / PSD
            const bool ok = it.add_cell(v[t]);
            const float x = static_cast<float>(v[t]);
            if (ok) {
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
            [[maybe_unused]] const bool done = day_psd.compute(sh.day_buf.data());
            assert(done && "ffill 后稠密, 有效数 = VR");
            rt.psd[slot].add_share(day_psd);
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
          const PsdAcc &acc = rt.psd[slot];
          c.psd_n = static_cast<uint32_t>(acc.n);
          if (c.psd_n > 0) {
            // 线性域取均值再 log10 (log 域均值会系统性偏低: E log < log E); 周期升序存
            for (size_t j = 0; j < kPvPsdPts; ++j) {
              const double p = acc.mean(kPvPsdK(j));
              c.psd[j] = p > 1e-20 ? static_cast<float>(std::log10(p)) : -20.0f;
            }
          }
        }
        std::lock_guard<std::mutex> lock(mutex);
        for (size_t i = 0; i < nb; ++i)
          snap.cells[cols.io[f0 + i]] = block_cells[i];
      }
      epoch.fetch_add(1, std::memory_order_release);
    });

    if (cancel.load(std::memory_order_relaxed))
      return false;
    publish(1);
  }
  return true;
}

// ============================================================================
// Clear
// ============================================================================

void Preview::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存
  snap = Snap{};
  spec = Spec{};
  rt_.reset(); // worker 已 join, 整体释放
  scope_ = ReadScope{};
  idle();
}
