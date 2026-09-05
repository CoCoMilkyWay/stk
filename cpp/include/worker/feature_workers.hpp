#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// Forward declarations
struct SharedData;
class GlobalFeatureStore;
class CoreSequential;

// ============================================================================
// TS 调度面: per-asset 处置权 + 认领/完成游标 (负载再平衡)
//
// 资产静态分给 worker 只是初始形态: 权重模型有偏差 + 负载时间分布不均, 差距
// 必然拉开, 而落后者的活别人接不了就只能全员等它. 所以处置权可转移 —— 领跑
// worker 在池边干等 slot 时, 对落后 ≥2 天的候选按前沿升序逐个尝试, 接手其
// 最轻的资产, 立刻回填该资产落下的旧日期 —— 含 victim 的当天 (claim CAS
// 裁决, victim 见 owner 已变即跳过, 当天即刻变轻; 旧日 slot 未计满必为
// BUSY, 见 FeatureStore), 之后并入自己的日循环. 反馈零延迟.
// 节流是单一阈值 adopt_pct (%, UI 可调, 0 = 关闭领养): victim 每天 (按其
// 前沿滚动的窗口) 最多让出持仓的 N%, 领跑者每天最多领养平均每核持仓的 N%
// —— 让完这波 victim 下一轮几乎不再是最慢者, 也不会被掏空; 领跑者也不会
// 一天吃成新的 lagger.
//
//   owner:   处置权路标 (领养 CAS victim→adopter; 原 owner 观察到即弃)
//   claimed: 谁处理 (asset, d) 由 CAS claimed d-1→d 唯一裁决 (转移瞬间的
//            双写/漏写由它兜底, owner 只是"该不该去试"的提示)
//   done:    已完成日 didx, release 发布 core 状态 (接手方 acquire 后可续算)
//
// cores 由初始 owner 在自己核上构造 (first-touch); 领养方经 store 前沿计数的
// release/acquire 链观察到构造完成后才会触碰.
// ============================================================================
struct TsSchedule {
  std::vector<std::atomic<int32_t>> owner;            // 处置权 worker id
  std::vector<std::atomic<int32_t>> claimed;          // 已认领日 didx, -1 起
  std::vector<std::atomic<int32_t>> done;             // 已完成日 didx, -1 起
  std::vector<size_t> weight;                         // 回测期逐笔总条数 (领养挑最轻)
  std::vector<std::unique_ptr<CoreSequential>> cores; // per-asset 跨日状态

  // Per-worker 被领养预算窗口: (victim 前沿 << 16) | 本窗已被领走数.
  // 前沿推进即换窗重置 —— "下一天最多能被领走多少个"的全局记账.
  std::vector<std::atomic<uint64_t>> adopt_window;

  // 领养节流阈值 N (%), 来自 UI 配置 (worker 启动前写好, 运行期只读);
  // 0 = 关闭领养.
  uint64_t adopt_pct = 10;

  TsSchedule(size_t num_assets, size_t num_workers);
  ~TsSchedule();
};

// ============================================================================
// 特征计算进度 —— 拉取式仪表盘 (总条 + 阶段行 + TS 热力条 + 慢核明细)
//
// worker 不再各持一行进度句柄: 前沿/持仓等共享状态本就躺在 FeatureStore /
// TsSchedule 里, worker 只把剩下的少量本地计数发布成原子 (ComputeStats,
// 单写者, 渲染线程只读). FeatureProgress 的采样线程每 100ms 拉取全量状态,
// 原地重画固定 5 行:
//
//   [#######>            ]  48% 238/500 天 | 12m03s, ETA 13m10s (3.1s/天)
//   预取 251/500 20230811 812MB/s (1.2TB) | 截面 240/500 | 落盘 238/500
//   时序 前沿 242..248/500 (20230805) Σ14.2M/s (28.4G单) 持仓 69..75
//   核压 [▆█▇█▅█▇█▂█▇█...]   每核1格: 块高 = 当日进度, 颜色 = 落后领跑者天数
//   慢核 12:-3天 [34/75] 0.9M/s · 41:-2天 [51/70] 1.3M/s   (差距≥2天, 按差距降序)
//
// 总条以落盘天数为准 (端到端真完成). TS 核间前沿差被池深钳住 (≤ slots),
// 差距本身就是"压力", 白→红着色; 健康核逐核数值全在热力条里 (前沿 = 领跑
// 前沿 − 颜色档, 当日进度 = 块高), 只有逐核速度浓缩进 Σ —— 一旦某核落后
// ≥2 天, 其完整数值 (核号/差距/当日计数/速度) 自动展开到慢核行.
// ============================================================================
struct ComputeStats {
  struct alignas(64) Ts {
    std::atomic<size_t> orders{0};       // 累计已处理逐笔条数 (速度 = orders / elapsed)
    std::atomic<uint32_t> done_today{0}; // 当日已完成资产数 (日循环头清零)
  };
  std::atomic<size_t> prefetch_days{0};
  std::atomic<size_t> prefetch_bytes{0};
  std::atomic<size_t> io_days{0}; // 已落盘天数
  std::vector<Ts> ts;
  explicit ComputeStats(size_t num_ts) : ts(num_ts) {}
};

class FeatureProgress {
public:
  // dates = 回测日期轴 (渲染期内调用方保证存活); 构造即预留 5 行并启动采样线程
  FeatureProgress(const GlobalFeatureStore &store, const TsSchedule &sched,
                  const ComputeStats &stats, const std::vector<std::string> &dates);
  ~FeatureProgress();

  void stop(); // 终画一帧并换行 (幂等)

private:
  void render();
  long long elapsed_ms() const;

  const GlobalFeatureStore &store_;
  const TsSchedule &sched_;
  const ComputeStats &stats_;
  const std::vector<std::string> &dates_;
  std::chrono::steady_clock::time_point start_time_;

  // ETA 标定: 首个落盘样本 (时刻, 当时天数), 只用之后的增量算每天耗时 ——
  // 首日落盘要等流水线灌满, 混进平均只会虚高
  long long first_io_ms_ = -1;
  size_t first_io_days_ = 0;

  std::atomic<bool> running_{true};
  std::thread refresh_thread_;
};

// ============================================================================
// PHASE 2 WORKERS —— 四角色统一签名: void xxx_worker(WorkerCtx)
//
// sched 只有 TS 用, 其余角色不碰 —— 统一签名换来统一 launch (见 ComputeService).
// worker_id = pin 的核号; pin 由 launch 侧完成, worker 内只用它做日志/stats 下标.
// ============================================================================
struct WorkerCtx {
  int worker_id;
  SharedData &data;
  GlobalFeatureStore &store;
  TsSchedule &sched;
  const std::atomic<bool> &cancel;
  ComputeStats &stats;
};

// 预取: 顺日期把 .bin 读进 page cache, 让 TS 的 decode 只吃缓存不等磁盘.
//       门控 = 领先最慢 TS (query_ts_days_done) 不超过 pool slots + 余量.
void prefetch_worker(WorkerCtx ctx);

// 时序: 逐资产 decode + LOB 重建 + DAG, 写 L0/L1/DEPTH 张量 (日期主序遍历);
//       领跑核在池边领养落后核的资产并回填 (处置权转移, 见 TsSchedule).
void sequential_worker(WorkerCtx ctx);

// 截面: cs_open 等本日全部 TS 写完, 整日扫截面列, cs_close 交给 IO.
void crosssectional_worker(WorkerCtx ctx);

// 落盘: 摘 DONE slot 落盘并归还池 (FREE), 直到全部日期刷完.
void io_worker(WorkerCtx ctx);
