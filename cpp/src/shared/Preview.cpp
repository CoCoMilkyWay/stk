#include "shared/Preview.hpp"
#include "features/DataDefine.hpp" // kPxEps (涨跌停比较容差, 全库统一)
#include "features/Storage/DayBatchPlane.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <cmath>

using namespace analysis;

// ============================================================================
// Runtime: worker 私有状态 (UI 不看)
// 并行面 = 特征块: 轮内一块一任务 (IO + 扫描 + 发布全在块内), 每特征槽一轮只被一个任务写,
// 每轮一次 dispatch (返回即栅栏) 隔开相邻轮 → 每特征累积器无锁无 shard, 也不需要归约
// ============================================================================

PsdAxis psd_axis(size_t N) {
  const size_t K = N / 2;
  PsdAxis ax;
  ax.n = static_cast<uint32_t>(std::min(kPvPsdBins, K));
  const size_t n = ax.n;
  // k 升序边界: 桶 b = [e[b], e[b+1]); 几何 (K+1)^(b/n), 低 k 端不足一个 bin 的桶顺延 (每桶 ≥ 1 个 k)
  std::array<size_t, kPvPsdBins + 1> e{};
  e[0] = 1;
  for (size_t b = 1; b < n; ++b)
    e[b] = std::max(e[b - 1] + 1, static_cast<size_t>(std::pow(static_cast<double>(K + 1), static_cast<double>(b) / n)));
  e[n] = K + 1;
  assert(n == 0 || e[n - 1] < e[n]);
  for (size_t b = 0; b < n; ++b) {
    const size_t j = n - 1 - b; // 周期升序
    ax.k0[j] = static_cast<uint16_t>(e[b]);
    ax.k1[j] = static_cast<uint16_t>(e[b + 1] - 1);
    ax.period[j] = static_cast<float>(static_cast<double>(N) / std::sqrt(static_cast<double>(ax.k0[j]) * ax.k1[j]));
  }
  return ax;
}

// 一层的网格无关状态: 每特征累积 (跨轮, 槽位 = 预览列下标) + 段序 + 块读计划 + 线程私有平面/暂存 + 轮首笼快照
struct Preview::Level {
  size_t level = 0, rpd = 0, days = 1;
  SegPlan plan;
  std::vector<ReadPlan> blocks; // [块] 单源读计划 (预览列 [f0[b], f0[b] + n) + 该层 ts_valid)
  std::vector<size_t> f0;       // [块] 块首预览列下标
  // ---- 每特征累积 (跨轮) ----
  std::vector<KLLcache> klls;              // [n_preview] 每特征累积 sketch
  std::vector<Integrity> integ;            // [n_preview] 每特征累积账目
  std::vector<uint32_t> cage_n, cage_miss; // [n_preview] price 笼账目 (受检/笼外) 累积
  std::vector<uint8_t> int_only;           // [n_preview] flag 探测: 有效值至今全整数 (粘性)
  // ---- 线程私有 (按 tid) ----
  std::vector<TsPlane> planes; // [块列][A][1][R]
  std::vector<IoSlot> io;      // IO 暂存
  // ---- 轮共享只读 (轮首单线程载入, 块任务只读) ----
  std::vector<float> cage_dn, cage_up; // [n_draw][days] 当轮抽样资产的逐日笼 (NaN = 当日无值 / 空位)

  virtual ~Level() = default;
  // 一轮: 轮首笼 (单线程) → 特征块一块一任务 (IO + 扫描 + 块末发布), 一次 dispatch
  virtual void round(Preview &pv, Runtime &rt, const Readers &rd, size_t r, const std::atomic<bool> &cancel) = 0;
};

struct Preview::Runtime {
  std::array<std::unique_ptr<Level>, LEVEL_COUNT> lv; // 空 = 该层不跑
  FeatureRead::DayColumns cage_day;                   // L_day [T][A_bar] × {lim_dn, lim_up} (Bar 档, Bar 库轴)
  std::vector<uint32_t> asset_order;                  // [A] 固定种子洗牌 (轮间旋转取片)
  // ---- 线程私有 (按 tid; 各层共用) ----
  struct Shard {
    std::array<Cell, kPvBlockCols> cells; // 块成品 staging (锁外填, 短锁拷贝)
    std::vector<float> samples;           // 单 (特征, 轮) 有效样本缓冲
  };
  std::vector<Shard> shards; // [n_threads]
  std::unique_ptr<misc::exec::ForkJoin> ex;
};

template <class G>
struct Preview::LevelOf final : Preview::Level {
  using PSD = typename G::PSD;
  static constexpr size_t R = G::R;
  // 块宽: 长段层按行数折减 (平面 + 暂存 ∝ 块宽 × R × A)
  static constexpr size_t kBlk = std::clamp<size_t>(kPvBlockCols * TRADE_MINUTES_PER_DAY / R, 1, kPvBlockCols);

  std::vector<PsdAcc<PSD>> psd;         // [n_preview] 单段谱 (每 bin 占比) 累加 (含恒值段的零票)
  std::vector<std::unique_ptr<PSD>> ws; // [n_threads] FFT workspace (大对象, 堆上)
  std::vector<std::vector<float>> bufs; // [n_threads][R] 单 (资产, 段) 序列 (缺/门控 NaN → 谱前 ffill)

  // 容量准备: 每特征累积 + 块读计划 + 线程私有
  void prepare(const ReadPlan &pv_plan, size_t A, const PerLib<size_t> &As, size_t n_threads) {
    level = G::kLevel;
    rpd = G::kRowsPerDay;
    days = G::kDays;
    const size_t n_pv = pv_plan.n();
    prepare_slots(klls, n_pv, kPvKllCapacity, kPvKllResolution);
    integ.assign(n_pv, Integrity{});
    psd.assign(n_pv, PsdAcc<PSD>{});
    cage_n.assign(n_pv, 0);
    cage_miss.assign(n_pv, 0);
    int_only.assign(n_pv, 1);
    assert(pv_plan.srcs.size() == 1 && "预览列 = 主库该层 (Preview::reset 核对主库)");
    const Source &src = pv_plan.srcs[0];
    blocks.clear();
    f0.clear();
    for (size_t b = 0; b < n_pv; b += kBlk) {
      const size_t e = std::min(b + kBlk, n_pv);
      ReadPlan bp;
      bp.level = pv_plan.level;
      bp.cols.assign(pv_plan.cols.begin() + b, pv_plan.cols.begin() + e);
      Source s = src;
      s.cs.io.assign(src.cs.io.begin() + b, src.cs.io.begin() + e);
      s.cs.vts.assign(src.cs.vts.begin() + b, src.cs.vts.begin() + e);
      if (src.cs.gated())
        s.cs.io.push_back(src.cs.gate());
      s.slot.resize(e - b);
      for (size_t i = 0; i < e - b; ++i)
        s.slot[i] = static_cast<uint32_t>(i);
      bp.srcs.push_back(std::move(s));
      blocks.push_back(std::move(bp));
      f0.push_back(b);
    }
    const size_t n_draw = std::min(kPvAssetsPerRound, A);
    cage_dn.resize(n_draw * days);
    cage_up.resize(n_draw * days);
    planes.resize(n_threads);
    io.resize(n_threads);
    ws.resize(n_threads);
    bufs.resize(n_threads);
    for (size_t i = 0; i < n_threads; ++i) {
      io[i].prepare(blocks[0], As, kBlk);
      if (!ws[i])
        ws[i] = std::make_unique<PSD>();
      bufs[i].resize(R);
    }
  }

  void round(Preview &pv, Runtime &rt, const Readers &rd, size_t r, const std::atomic<bool> &cancel) override;
};

Preview::Preview() = default;
Preview::~Preview() = default;

// 段序列缺口前向填充. 填 0 的问题: 水平量 (价格 ~10 元) 固定分钟槽缺 → 一根 −值 尖刺, 能量比特征自身
// 波动大几个数量级, 平铺全谱盖掉谱形 (全表水平类特征长一个样). ffill 让缺口成平台 (零变动, 对谱中性),
// 真实跳变仍在; 段首缺口用首个有效值回填. 调用方保证至少一个有效值
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
  for (size_t l = 0; l < LEVEL_COUNT; ++l) {
    const ReadPlan &p = sp.plans[l];
    if (p.n() == 0)
      continue;
    assert(p.level == l && std::is_sorted(p.cols.begin(), p.cols.end()));
    assert(p.cols.back().level == l && p.cols.back().col < sp.n_features[l]);
    assert(p.srcs.size() == 1 && p.srcs[0].lib == scope.main && lib_tier(scope.main) == sp.tier);
  }
  assert(sp.cage.size() == 2 && scope.has(kAllLib) && "price 笼读 all 库");
  assert(scope.axis.size() > 0 && !scope.months.empty());
  std::lock_guard<std::mutex> lock(mutex);

  scope_ = std::move(scope);
  spec = std::move(sp);

  // 预览是全表底图: 输入变了旧格子无参照意义, 清零 (尺寸对得上就地 fill, 零分配)
  for (size_t l = 0; l < LEVEL_COUNT; ++l) {
    std::vector<Cell> &cells = snap.cells[l];
    if (cells.size() != spec.n_features[l])
      cells.assign(spec.n_features[l], Cell{});
    else
      std::fill(cells.begin(), cells.end(), Cell{});
    snap.axis[l] = psd_axis(math::spectral::next_pow2(seg_rows(l)));
  }
  if (!rt_)
    rt_ = std::make_unique<Runtime>();

  publish();
}

// ============================================================================
// Build (轮训: 轮 = 抽样日, 轮内按特征分块, 块末发布; 首帧 = 一块的 IO + 扫描)
// ============================================================================

bool Preview::build(const PerLib<const FeatureRead *> &fr, const std::atomic<bool> &cancel) {
  TraceN("PreviewBuild");
  assert(rt_ && "reset 先于 build");
  Runtime &rt = *rt_;
  const size_t A = scope_.axis.size();
  const PerLib<size_t> As = scope_.sizes();
  assert(A > 0 && "reset 先于 build");
  const Readers rd = scope_.readers(fr);

  // 线程数 = min(硬件并发, 最多块数): 块是抢任务单位, 多于块数的线程整轮空转
  size_t max_blocks = 1;
  for (const ReadPlan &p : spec.plans)
    max_blocks = std::max(max_blocks, p.n());
  const size_t n_threads = threads_for(max_blocks);
  misc::exec::ensure(rt.ex, static_cast<int>(n_threads));
  rt.shards.resize(n_threads);
  for (Runtime::Shard &sh : rt.shards)
    sh.samples.reserve(kPvAssetsPerRound * TRADE_MINUTES_PER_DAY);
  rt.cage_day.preallocate(As[kAllLib], L_day, 2);
  rt.asset_order = shuffled_order(A); // 无偏, 轮间旋转取片覆盖不同资产

  // 各层: 段序 (洗牌取前 kPvRounds 个, 无偏覆盖全区间, 轮序即收敛序) + 容量
  size_t n_rounds = 0, n_total = 0;
  for (size_t l = 0; l < LEVEL_COUNT; ++l) {
    rt.lv[l].reset();
    const ReadPlan &p = spec.plans[l];
    if (p.n() == 0)
      continue;
    rt.lv[l] = with_grid(l, [&](auto g) -> std::unique_ptr<Level> {
      auto lv = std::make_unique<LevelOf<decltype(g)>>();
      lv->prepare(p, A, As, n_threads);
      return lv;
    });
    rt.lv[l]->plan = plan_segs(rd.lib(), scope_.months, l, DayOrder::Shuffle, kPvRounds);
    n_rounds = std::max(n_rounds, rt.lv[l]->plan.size());
    n_total += rt.lv[l]->plan.size();
  }
  if (n_total == 0)
    return true; // 库为空: 直接 Done, 表格全 "—"
  total.store(n_total, std::memory_order_release);

  // 轮循环: 各层轮交错 (层序 = L_min 先: 特征最多, 再粗层, L_sec 最重最后)
  constexpr std::array<size_t, LEVEL_COUNT> kOrder{L_min, L_5min, L_15min, L_30min, L_60min, L_day, L_sec};
  for (size_t r = 0; r < n_rounds; ++r)
    for (const size_t l : kOrder) {
      Level *lv = rt.lv[l].get();
      if (!lv || r >= lv->plan.size())
        continue;
      lv->round(*this, rt, rd, r, cancel);
      if (cancel.load(std::memory_order_relaxed))
        return false;
      publish(1);
    }
  return true;
}

// ============================================================================
// 一层一轮: 轮首笼 (单线程) → 特征块一块一任务 (块 = IO + 扫描 + 发布, 全在锁外, 只在拷 cells 时短锁),
// 一次 dispatch (返回即栅栏)
// ============================================================================

template <class G>
void Preview::LevelOf<G>::round(Preview &pv, Runtime &rt, const Readers &rd, size_t r, const std::atomic<bool> &cancel) {
  const size_t A = rt.asset_order.size();
  const size_t n_draw = std::min(kPvAssetsPerRound, A);
  const size_t a_off = (r * n_draw) % A; // 洗牌序旋转取片
  const ReadPlan &pv_plan = pv.spec.plans[G::kLevel];
  size_t n_real = 0; // 本段真实日数 (尾段空位不入账)
  {
    // 轮首笼 (单线程): Bar 档 L_day lim_dn / lim_up 逐日读 → 抽样资产逐日笼快照 (price 逐日判定融合进块扫描).
    // 笼外扩容差 (存快照时做, 热循环保持严格比较): 贴板成交价与 lim 两列来源不同
    // (整数分换算 vs 解析/计算), 原始 float 差 ≤ kPxEps, 落 f16 后可差 1 ulp (≈ 值/1024);
    // 严格比较会把涨跌停分钟误判笼外 (一字板整天 close == lim_up). NaN 经算术照传
    TraceN("PreviewRoundIO");
    constexpr float kF16Rel = 1.0f / 1024.0f;   // f16 尾数 10 bit → 1 ulp ≤ |值|/1024
    const uint32_t *cage_ids = rd.ids[kAllLib]; // nullptr = 主库即 all (同轴); all = 全轴, 不会 kAbsent
    for (size_t d = 0; d < days; ++d) {
      const misc::Ymd date = plan.dates[r * days + d];
      n_real += date != 0;
      if (date != 0)
        rd.of(kAllLib).load_day_columns(date, pv.spec.cage, rt.cage_day);
      for (size_t k = 0; k < n_draw; ++k) {
        const size_t a = rt.asset_order[(a_off + k) % A];
        const size_t s = cage_ids ? cage_ids[a] : a;
        const float dn = date ? static_cast<float>(rt.cage_day.col(0)[s]) : kNaN; // L_day 行 0 (当日常量)
        const float up = date ? static_cast<float>(rt.cage_day.col(1)[s]) : kNaN;
        cage_dn[k * days + d] = dn - (std::fabs(dn) * kF16Rel + kPxEps);
        cage_up[k * days + d] = up + (std::fabs(up) * kF16Rel + kPxEps);
      }
    }
  }
  const size_t n_rows = n_real * rpd; // 本段真实格数 / (资产, 特征)
  const PsdAxis &axis = pv.snap.axis[G::kLevel];
  const double K = static_cast<double>(PSD::N / 2);

  run_phase(*rt.ex, blocks.size(), cancel, [&](size_t blk, size_t tid) {
    Runtime::Shard &sh = rt.shards[tid];
    PSD &seg_psd = *ws[tid];
    float *seg_buf = bufs[tid].data();
    TsPlane &plane = planes[tid];
    std::array<Cell, kPvBlockCols> &block_cells = sh.cells;
    const ReadPlan &bp = blocks[blk];
    const size_t nb = bp.n();
    const size_t fb = f0[blk];

    // 块 IO: nb 值列 + 该层 ts_valid (各列按自己的 vt 折哨兵)
    {
      TraceN("PreviewIO");
      plane.prepare(A, G::kLevel, 1, nb);
      plane.load_seg(rd, bp, plan, r, 0, io[tid]);
    }

    // 块扫描: 每特征抽 n_draw 个资产
    TraceN("PreviewScan");
    for (size_t i = 0; i < nb; ++i) {
      const size_t slot = fb + i;
      Integrity &it = integ[slot];
      uint32_t c_n = 0, c_miss = 0; // 本 (特征, 轮) price 笼账目
      bool nonint = false;          // 本 (特征, 轮) 见过非整数有效值 (flag 探测)
      sh.samples.clear();

      for (size_t k = 0; k < n_draw; ++k) {
        const size_t a = rt.asset_order[(a_off + k) % A];
        const feature_storage_t *v = plane.series(i, a, 0);
        it.n_total += n_rows;
        size_t n_ok = 0;
        for (size_t t = 0; t < R; ++t) {
          // 账目与 Dist 同口径 (按位分账: 门控不过 / NaN / ±inf / 有限); 只有有限值进 sketch / PSD
          const bool ok = it.add_cell(v[t]);
          const float x = static_cast<float>(v[t]);
          if (ok) {
            // flag: 整数判定必须在有限值分支内 (NaN/±inf 转 int 是 UB); f16 值域 ±65504,
            // int32 截断安全; -0.0 == 0 → 整数. 无分支 OR 累积, 轮末一次并进粘性位
            nonint |= (x != static_cast<float>(static_cast<int32_t>(x)));
            // price 笼: 对照该资产当日 [lim_dn, lim_up] (NaN 笼 = 当日无值, 不受检)
            const size_t d = t / rpd;
            const float dn = cage_dn[k * days + d], up = cage_up[k * days + d];
            if (dn == dn && up == up) {
              ++c_n;
              c_miss += !(x >= dn && x <= up);
            }
          }
          seg_buf[t] = ok ? x : kNaN; // 缺/门控/±inf 记 NaN, 谱前 ffill_day (不填 0, 见其注释)
          n_ok += ok;
          if (ok)
            sh.samples.push_back(x);
        }

        // 单段谱 → Parseval 归一累加: 除以当段总功率 (k ∈ [1, N/2], 跳 DC) → 每 bin = 方差占比.
        // 特征量纲被除掉, 跨特征绝对可比 (白噪声参照 = 1/(N/2), 发布时 × N/2 → 全层恒 1);
        // 每 (资产, 段) 一票, 高波动段不再以方差加权主导谱形.
        // 有一个有效值就算谱 (ffill 后稠密; 稀疏段 = 阶梯序列, 谱偏红是其真实形态, 不设最少样本门槛 ——
        // 门控只留少数行的特征否则整列无谱). 恒值段 (日频常量 / 停牌) total = 0 谱无定义, 按
        // "占比全 0" 投一票: 曲线整体下沉 log10(有动的段占比), 图上同时看到 "多久动一次" (高度) 与
        // "怎么动" (形状, 重心尺度无关); 全部恒值 → 全地板 (贴底平线). 绝对方差在 Range 列 sd
        if (n_ok > 0) {
          ffill_day(seg_buf, R);
          [[maybe_unused]] const bool done = seg_psd.compute(seg_buf);
          assert(done && "ffill 后稠密, 有效数 = R");
          psd[slot].add_share(seg_psd);
        }
      }
      if (!sh.samples.empty())
        klls[slot].addBatch(sh.samples.data(), sh.samples.size());
      cage_n[slot] += c_n;
      cage_miss[slot] += c_miss;
      int_only[slot] &= static_cast<uint8_t>(!nonint);
    }

    // 块末发布: 锁外导出成品, 一次短锁拷贝整块进 cells
    {
      TraceN("PreviewPublish");
      for (size_t i = 0; i < nb; ++i) {
        const size_t slot = fb + i;
        Cell &c = block_cells[i];
        c = Cell{}; // staging 复用: psd_n == 0 时不填 psd, 必须清掉上一块的残值
        c.fill(klls[slot], kPvMinSamples);
        c.integrity = integ[slot];
        c.cage_n = cage_n[slot];
        c.cage_miss = cage_miss[slot];
        c.int_only = int_only[slot] != 0;
        const PsdAcc<PSD> &acc = psd[slot];
        c.psd_n = static_cast<uint32_t>(acc.n);
        if (c.psd_n > 0) {
          // 线性域取桶内均值再 log10 (log 域均值会系统性偏低: E log < log E); 周期升序存
          for (size_t j = 0; j < axis.n; ++j) {
            double p = 0.0;
            for (size_t k = axis.k0[j]; k <= axis.k1[j]; ++k)
              p += acc.mean(k);
            p *= K / static_cast<double>(axis.k1[j] - axis.k0[j] + 1);
            c.psd[j] = p > 1e-20 ? static_cast<float>(std::log10(p)) : kPvPsdFloor;
          }
        }
      }
      std::lock_guard<std::mutex> lock(pv.mutex);
      for (size_t i = 0; i < nb; ++i)
        pv.snap.cells[G::kLevel][pv_plan.cols[fb + i].col] = block_cells[i];
    }
    pv.epoch.fetch_add(1, std::memory_order_release);
  });
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
