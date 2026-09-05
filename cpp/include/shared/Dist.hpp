#pragma once

#include "features/Backend/FeatureRead.hpp"
#include "math/distribution/KLLcache.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

// ============================================================================
// Distribution Analysis (KLL-based, 分批流式)
// ============================================================================
// 天是流式维度: 每批 kDaysPerBatch 个抽样天, 批内扫全部资产, 全部视图逐批收敛.
// 首帧 = 第一批的 IO + 扫描 (几十 ms), 与总区间长度无关 —— 列存按天分文件,
// 任何"某资产的完整历史"都要等全量 IO, 所以终态式发布与快速首帧不可兼得,
// 这里选收敛式: 视图从第一批起就是全市场全时段的完整图景, 只是精度逐批收紧.
//
// 发布协议 (单调收敛, UI 画已发布快照):
//
//   worker (DistService 单线程编排; build 起一波 n_threads 常驻线程, 每批两道栅栏):
//     for 每批 kDaysPerBatch 个抽样天:
//       Phase IO:   抢单天并行载入 [T][列][A] → 转置进批平面 [A][批天][分钟]
//                   (f16; valid 门控与真 NaN 折叠成统一哨兵, NaN 就地记账)
//       ── 栅栏 ──
//       Phase 扫描: 抢 kAssetBlock 个资产一块, 全在锁外:
//                   integrity 账目 + stride 抽样喂聚合槽私有副本 (全部资产);
//                   绘制子集另吃全量样本 → 私有资产 sketch → 顺手导出整条
//                   AssetLine (PDF/矩/W2) 到 staging; 块末短锁 merge 聚合槽
//       ── 栅栏 (completion, 单线程): 短锁 swap 发布 lines + NaN 账目 + 进度 ──
//
//   UI (每帧): 持 mutex 渲染; 资产截面消费 lines 快照, 零计算零重建只画
//     (增量收敛每批都作废 sketch 缓存, 拉模式会让 UI 每帧重建几百条 — 故推模式).
//     聚合视图 (月/星期/小时/全局) 槽数少, 仍 lazy 导出.
//   生命周期: 改参数 → 新请求即取消在跑重算; 切走 Tab → 立刻中断 + clear() 释放;
//             切回 Tab → 自动重算 (首帧几十 ms, 不留常驻).
// ============================================================================

static constexpr size_t kMinSamples = 1000;         // sample 不够的不纳入统计
static constexpr size_t kMinAssetSamples = 100;     // 资产纳入截面视图的最小样本数
static constexpr size_t KLL_CAPACITY = 512;         // 月/小时/星期/全局 sketch
static constexpr size_t KLL_RESOLUTION = 256;       // 小面板 ~400px, 255 点 PDF 足够
static constexpr size_t KLL_ASSET_CAPACITY = 256;   // 每资产 sketch (精度换内存)
static constexpr size_t KLL_ASSET_RESOLUTION = 128; // 资产 PDF 网格 (画细线, 128 点足够)
static constexpr size_t kDistLevel = 1;             // Dist 只在 L1 上跑
static constexpr size_t kDaysPerBatch = 8;          // 批大小: 首帧 = 一批的 IO + 扫描
static constexpr int kW2Deciles = 19;               // W2 用的分位点: 5%, 10%, ..., 95%

// 绘制子集大小: 固定种子随机抽 → 无偏, 画面统计形态与全量等价 (同一哲学: 任意随机
// 子集即全市场抽样). 资产 sketch 只为绘制服务, 其余资产只进 integrity 与聚合槽.
static constexpr size_t kDrawAssets = 512;

// 总样本预算: 超出则日抽样 (stride 与交易周互质避免星期偏置). 分批后平面只存一批,
// 内存不再约束天数 —— 这个预算只是总扫描/IO 时长的旋钮, 5 年 × 5000 标的在预算内全量.
static constexpr size_t kMaxTotalSamples = size_t(2) << 30;

// 聚合槽 (月/星期/小时/全局) 的目标样本量. KLL 的分位误差 ε≈1/k 只由容量决定, 与样本数
// 无关 —— 5 年全市场灌进去的几亿样本, 最终也只存下 k·log2(n/k) ≈ 1 万个点, 早已饱和.
// 所以按总量自适应 stride 抽到这个量级即可, 图上看不出差别. 绘制子集的资产槽仍吃全量.
static constexpr size_t kAggTargetSamples = size_t(32) << 20; // 32M

// 区间月份枚举 "YYYYMM" 升序 (start/end: "YYYY-MM-DD" 或 "YYYYMMDD"; Service 与 UI 共用)
std::vector<std::string> dist_enumerate_months(const std::string &start_date,
                                               const std::string &end_date);

struct Dist {

  // ==========================================================================
  // Integrity (全区间账目)
  // ==========================================================================

  struct Integrity {
    size_t n_total = 0;
    size_t n_valid = 0;
    size_t n_zero = 0;
    size_t n_nan = 0;
    size_t n_pos_inf = 0;
    size_t n_neg_inf = 0;
    // 无有效样本时恒为 ±inf: 空账目可与任意账目无条件合并 (UI 层 n_valid == 0 时显示 "--").
    // bit 模式构造: 本头会被 fast-math TU 包含, 不能碰 numeric_limits::infinity();
    // ±inf 的比较只发生在 precise-math TU (Dist.cpp / TabDist.cpp)
    float val_min = std::bit_cast<float>(0x7F800000u);
    float val_max = std::bit_cast<float>(0xFF800000u);

    void add(const Integrity &o) {
      n_total += o.n_total;
      n_valid += o.n_valid;
      n_zero += o.n_zero;
      n_nan += o.n_nan;
      n_pos_inf += o.n_pos_inf;
      n_neg_inf += o.n_neg_inf;
      val_min = std::min(val_min, o.val_min);
      val_max = std::max(val_max, o.val_max);
    }

    float zero_pct() const { return n_valid > 0 ? 100.0f * n_zero / n_valid : 0.0f; }
    float nan_pct() const { return n_total > 0 ? 100.0f * n_nan / n_total : 0.0f; }
    float inf_pct() const { return n_total > 0 ? 100.0f * (n_pos_inf + n_neg_inf) / n_total : 0.0f; }

    void clear() { *this = Integrity{}; }
  };

  // ==========================================================================
  // Slots (worker 写, UI 持锁读)
  // ==========================================================================

  // 每月聚合 (月度漂移视图 + 滑条标签); 资产槽直接是 KLLcache (全区间累积, 不按月×资产存)
  struct MonthSlot {
    std::string month; // "YYYYMM"
    KLLcache kll{KLL_CAPACITY, KLL_RESOLUTION};
  };

  // 绘制子集一条线的发布快照: worker 批末在锁外算好整条 (PDF/矩/W2), 短锁 swap 进
  // lines. UI 消费快照零计算零重建 —— 收敛式构建下 sketch 缓存每批作废, 不能让 UI 拉.
  struct AssetLine {
    uint32_t asset = 0; // 资产下标 (行业色 / 详情面板)
    uint64_t n = 0;     // 累积样本数 (全量)
    float mean = 0.0f, var = 0.0f, skew = 0.0f, kurt = 0.0f;
    float w2 = -1.0f;                                     // 均值校准 W2, 相对上一批末的全局分位 (逐批收敛); < 0 = 参考未就绪
    uint32_t n_pts = 0;                                   // 折线点数; 0 = 样本不足, 本条不画
    std::array<float, KLL_ASSET_RESOLUTION - 1> x{}, y{}; // PDF 折线
  };

  // ==========================================================================
  // State
  // ==========================================================================

  enum class Status : uint8_t { Idle,
                                Building,
                                Done,
                                Cancelled };

  // 进度: 原子, UI 免锁读
  std::atomic<Status> status{Status::Idle};
  std::atomic<size_t> days_loaded{0};   // 已完成批的累计天数
  std::atomic<size_t> days_total{0};    // 抽样后总天数
  std::atomic<uint64_t> lines_epoch{0}; // 每批发布 +1 (UI 以此触发 autofit)
  // 聚合槽抽样 stride (1 = 全量). 月/星期/小时/全局视图的 totalCount 是抽样后的数,
  // 绘制子集的资产线恒为全量 —— UI 得把这个比例说出来, 免得两边的 n 并列看着矛盾.
  std::atomic<size_t> agg_stride{1};

  // 聚合状态: mutex 保护 (worker 块末/批末短锁发布; UI 渲染帧内持锁)
  mutable std::mutex mutex;

  std::vector<MonthSlot> months;                // [n_months]
  std::vector<AssetLine> lines;                 // [n_draw] 绘制子集快照 (每批整体换新)
  std::vector<KLLcache> by_hour;                // [24] 全区间 (KLL_CAPACITY/RESOLUTION, 下同)
  std::vector<KLLcache> by_weekday;             // [7]  全区间
  KLLcache total{KLL_CAPACITY, KLL_RESOLUTION}; // 全区间
  Integrity integrity;                          // 全区间

  // ==========================================================================
  // Methods (worker 线程调用, 内部按需加锁)
  // ==========================================================================

  // 重置全部状态并进入 Building (sketch 容量复用, 稳态零分配)
  void reset_for_build(std::vector<size_t> cols, const std::vector<std::string> &month_keys,
                       size_t n_assets);

  // 全区间构建: 分批流式 (每批 IO → 扫描 → 发布); 被取消返回 false
  bool build(FeatureRead &reader, const std::atomic<bool> &cancel);

  void clear();

private:
  // 构建参数/内部映射 (UI 不看): reset 时定好, build 全程只读
  std::vector<size_t> columns;    // [值列 (+ valid 列)], GUI 线程解析好的快照
  std::vector<int32_t> draw_pos_; // [A] 资产 → 绘制子集槽位; -1 = 不画 (固定种子随机抽)

  // 上一批末的全局分位参考 (批末 completion 单线程更新, 扫描线程只读 — 栅栏同步)
  struct W2Ref {
    std::array<float, kW2Deciles> q{};
    float mean = 0.0f;
    bool valid = false;
  };
  W2Ref w2_ref_;

  // 平面里"不可用"的统一哨兵 (f16 qNaN): valid 门控不过 与 真 NaN 都折叠到它.
  // 两者的分账在 Phase IO 就地做完 (那里还看得见 valid 列, 判得比事后猜位模式更准),
  // 所以平面不必区分二者 —— 热扫描一次 v != v 就能跳过, 也不用怕哨兵撞上数据里的 NaN.
  static constexpr uint16_t kInvalidBits = 0x7E00u;

  static constexpr size_t kAssetBlock = 64; // Phase 扫描 抢块粒度 (聚合槽每块 merge 一次)

  struct DayGroup {
    uint32_t begin, end;
    uint16_t month;
    uint8_t weekday;
  }; // shard.agg_samples 按天切片 → months / by_weekday
  struct HourRun {
    uint32_t begin, end;
    uint8_t hour;
  }; // 同小时连续段 → by_hour

  // 每线程私有: 扫描缓冲 + 聚合槽副本. 重活全在锁外做完, 只把 sketch 级结果并入全局.
  struct Shard {
    FeatureRead::DayColumns staging; // Phase IO: 单日 [T][列][A] 暂存 (只前 kDaysPerBatch 个线程用)
    uint64_t nan_seen = 0;           // Phase IO: 本批真 NaN 数 (批末并入 integrity)
    std::vector<float> samples;      // Phase 扫描: 绘制子集单资产本批全量样本
    std::vector<float> agg_samples;  // Phase 扫描: stride 抽样样本 → 聚合槽 (下面两表索引它)
    std::vector<DayGroup> day_groups;
    std::vector<HourRun> hour_runs;
    std::vector<KLLcache> months;     // [n_months]
    std::vector<KLLcache> by_hour;    // [24]
    std::vector<KLLcache> by_weekday; // [7]
    KLLcache total{KLL_CAPACITY, KLL_RESOLUTION};
    Integrity integrity;
  };

  // worker 私有 (clear() 只在 worker join 之后调用, 无竞争)
  std::vector<KLLcache> asset_klls_;     // [n_draw] 绘制子集累积 sketch (UI 不读, 全程无锁)
  std::vector<AssetLine> lines_staging_; // [n_draw] 扫描线程各写各槽, 批末与 lines 交换
  std::vector<feature_storage_t> plane_; // [A][批天][分钟] 资产主序批平面 (f16, ~20MB)
  std::vector<Shard> shards_;            // [n_threads]
};
