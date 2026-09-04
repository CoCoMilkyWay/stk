#pragma once

#include "features/Backend/FeatureRead.hpp"
#include "math/distribution/KLLcache.hpp"
#include <algorithm>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

// ============================================================================
// Distribution Analysis (KLL-based, 资产优先流式)
// ============================================================================
// 层次: 资产优先 —— 先拿到一个资产的全时段 (抽样) 数据, 再流下一个资产.
// 这样月度漂移/时刻/星期视图在前几百个资产完成时就是覆盖全时段的图景,
// 而不是等所有月份跑完才拼齐漂移.
//
// 发布协议 (与 FeatureStore 的门控同一姿态: 单调发布, UI 画已完成前缀):
//
//   worker (单线程, DistService):
//     Phase IO:  日期枚举 → 自适应日抽样 (平面 ≤ kMaxBlockBytes; 区间小则 stride=1 全量;
//                stride 与交易周互质避免星期偏置) → 逐日载入 [T][列][A] 暂存, 分块转置进
//                资产主序平面 [A][天][分钟] (f32, valid 门控折叠成哨兵 NaN), days_loaded++
//     Phase 流:  for a in shuffle(0..A):         ← 随机资产序: 已发布集合恒为全市场无偏抽样
//                  顺序扫描本资产平面 (无锁): 样本进单一缓冲, 按天记 (月, 星期) 切片,
//                  同小时连续段记 run → 加锁发布 (切片直喂 KLL, 零拷贝):
//                    assets[a] (发布即终态) / months[*] / by_hour / by_weekday / total
//                  ++assets_done
//
//   UI (每帧): 持 mutex 渲染全部视图; 进度 (status/days_loaded/assets_done) 原子免锁.
//     已发布资产 = 槽 count > 0 (发布即终态, 无需知道顺序); 偏移散点 (W2 相对当前全局
//     分位) 与行业色均为每帧派生 —— 无收尾阶段, 一切视图全程流式.
//   生命周期: 改参数 → 新请求即取消在跑重算; 切走 Tab → 立刻中断 + clear() 释放;
//             切回 Tab → 自动重算 (构建只需秒级, 不留常驻).
// ============================================================================

static constexpr size_t kMinSamples = 1000;     // sample 不够的不纳入统计
static constexpr size_t kMinAssetSamples = 100; // 资产纳入截面视图的最小样本数
static constexpr size_t KLL_CAPACITY = 512;     // 月/小时/星期/全局 sketch
static constexpr size_t KLL_RESOLUTION = 1024;
static constexpr size_t KLL_ASSET_CAPACITY = 256;         // 每资产 sketch (5000+ 个, 精度换内存)
static constexpr size_t KLL_ASSET_RESOLUTION = 128;       // 资产 PDF 网格 (画细线, 128 点足够)
static constexpr size_t kMaxBlockBytes = size_t(2) << 30; // 常驻平面上限, 超出则日抽样
static constexpr size_t kDistLevel = 1;                   // Dist 只在 L1 上跑

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

  // ==========================================================================
  // State
  // ==========================================================================

  enum class Status : uint8_t { Idle,
                                Building,
                                Done,
                                Cancelled };

  // 进度: 原子, UI 免锁读
  std::atomic<Status> status{Status::Idle};
  std::atomic<size_t> days_loaded{0}; // Phase IO 进度
  std::atomic<size_t> days_total{0};  // 抽样后入平面天数
  std::atomic<size_t> assets_done{0}; // Phase 流 进度 (全程单调, 槽发布即终态)

  // 聚合状态: mutex 保护 (worker 逐资产短锁发布; UI 渲染帧内持锁)
  mutable std::mutex mutex;

  std::vector<size_t> columns;                  // 本次构建参数: [值列 (+ valid 列)], GUI 线程解析好的快照
  std::vector<size_t> order;                    // [A] 随机资产序 (reset 时洗牌, worker 按此流)
  std::vector<MonthSlot> months;                // [n_months]
  std::vector<KLLcache> assets;                 // [A] 全区间累积 (KLL_ASSET_CAPACITY/RESOLUTION)
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

  // 全区间构建: Phase IO (抽样转置入平面) + Phase 流 (随机序逐资产发布); 被取消返回 false
  bool build(FeatureRead &reader, const std::atomic<bool> &cancel);

  void clear();

private:
  // invalid (valid 门控不过) 的哨兵: 带载荷的 qNaN. _Float16 NaN → float 的低 13 位恒 0,
  // 不会与数据里的真 NaN 撞车 → 平面单值即可区分 [有效值 | 真 NaN | invalid]
  static constexpr uint32_t kInvalidBits = 0x7FC00001u;

  struct DayGroup {
    uint32_t begin, end;
    uint16_t month;
    uint8_t weekday;
  }; // samples_ 按天切片
  struct HourRun {
    uint32_t begin, end;
    uint8_t hour;
  }; // 同小时连续段

  // worker 私有 (无锁访问; clear() 只在 worker join 之后调用, 无竞争)
  std::vector<float> plane_;         // [A][抽样天][分钟] 资产主序常驻平面
  std::vector<float> samples_;       // 单资产全时段有效样本 (跨资产复用)
  std::vector<DayGroup> day_groups_; // 单资产: 按天切片 → months / by_weekday
  std::vector<HourRun> hour_runs_;   // 单资产: 小时 run → by_hour
};
