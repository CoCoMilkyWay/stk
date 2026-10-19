#pragma once

#include "features/Storage/FeatureLevels.hpp" // PerTier
#include "misc/date.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Forward declarations
struct SharedData;
struct FeatureLib;
class StoreRing;
template <class T>
class FeatureStore;

// ============================================================================
// Stage Layout - 核布局: 按物理核分 stage, 由机器拓扑 + 预取 / 截面 / 落盘核数 (按档配, 见 ComputeConfig) 推导
//
//   [TS × (P − K_cs − K_io − K_pf)] [CS × K_cs] [IO × K_io] [预取 × K_pf]     P = 物理核数
//
// 超线程对布局透明: stage 只占物理核, 再展开到该核的逻辑 CPU ——
//   TS:             每个逻辑 CPU 一个 worker, pin 单个逻辑 CPU (超线程吞吐全归 TS; 无超线程即每核一个)
//   CS / IO / 预取: 各 K 个 worker 各独占一个物理核 (pin 该核全部逻辑 CPU), 按日轮转 (日 d 归第 d % K 个;
//                   三段日间无状态, 逐值与 K 无关)
// CS / IO / 预取不要落在 TS 物理核的超线程兄弟上, 也不要互相共核: 同核争访存资源, 慢数倍, 整条流水线被池子卡住
// (Bar 硬源的预取是 parquet 解码, 与落盘编码同为 CPU 活).
//
// 全部线程 pin 死 —— 没有浮动线程抢占 pinned worker 的时间片.
// worker_id = 全局序号 [TS 0..num_ts−1][CS × K_cs][IO × K_io][预取 × K_pf] (TS 用作 frontier / stats 下标), 与 pin 的 CPU 号无关.
// stage 顺序 = UI 表格顺序 = 预取, TS..., CS, IO.
// 进度不按行挂 worker: 仪表盘 (FeatureProgress) 自行采样, 见下.
// ============================================================================
enum class ComputeStage : size_t {
  Prefetch = 0,
  TS,
  CS,
  IO,
};

struct StageLayout {
  using Topology = std::vector<std::vector<unsigned int>>; // [物理核][逻辑 CPU], 见 misc::Affinity::physical_cores

  struct Stage {
    ComputeStage kind = ComputeStage::Prefetch;
    const char *name = "";
    const char *desc = "";
    int threads = 0;      // worker thread 数
    int cores = 0;        // 独占物理核数
    int first_worker = 0; // worker_id 区间 [first_worker, first_worker + threads)
    int first_phys = 0;   // 物理核区间 [first_phys, last_phys]
    int last_phys = 0;
  };

  static constexpr size_t kStageCount = 4;

  const Topology *topo = nullptr; // 进程级常驻 (physical_cores 的 static)
  int num_ts_phys = 0;            // TS 物理核数, 物理核 0..num_ts_phys-1
  int num_ts = 0;                 // TS worker 数 = 这些物理核上的逻辑 CPU 总数
  int num_cs = 0;                 // CS worker 数 = 物理核数, 紧接 TS 之后
  int num_io = 0;                 // IO worker 数 = 物理核数, 紧接 CS 之后
  int num_pf = 0;                 // 预取 worker 数 = 物理核数, 紧接 IO 之后 (末尾)

  int total_phys() const { return static_cast<int>(topo->size()); }
  int total_threads() const { return num_ts + num_cs + num_io + num_pf; }

  Stage stage(ComputeStage kind) const {
    const int cs = num_ts_phys, io = cs + num_cs, pf = io + num_io;
    switch (kind) {
    case ComputeStage::Prefetch:
      return {kind, "预取 Prefetch", "顺日期备源 (Lob: 逐笔帧读进 page cache; Bar: 解码 bar1m 进源环), 领先最慢时序核不超过 ring slots + 2 天; 多核按日轮转",
              num_pf, num_pf, num_ts + num_cs + num_io, pf, pf + num_pf - 1};
    case ComputeStage::TS:
      return {kind, "时序 TS", "逐资产喂源 + DAG, 写 L_sec/L_1min 张量; 领跑核自动接手落后核的资产 (处置权转移)",
              num_ts, num_ts_phys, 0, 0, num_ts_phys - 1};
    case ComputeStage::CS:
      return {kind, "截面 CS", "本日全部时序核写完后逐拍重放截面 (与实盘同一调用); 多核按日轮转",
              num_cs, num_cs, num_ts, cs, io - 1};
    case ComputeStage::IO:
      return {kind, "落盘 IO", "本日截面写完后逐层落盘, 清零交还 slot; 多核按日轮转",
              num_io, num_io, num_ts + num_cs, io, pf - 1};
    }
    assert(false && "unknown compute stage");
    return {};
  }

  std::array<Stage, kStageCount> stages() const {
    return {stage(ComputeStage::Prefetch), stage(ComputeStage::TS), stage(ComputeStage::CS), stage(ComputeStage::IO)};
  }

  // stage 内第 i 个 worker pin 的逻辑 CPU: TS 单个逻辑 CPU (物理核序展开), 其余 = 所在物理核 (第 i 个) 全部逻辑 CPU
  std::vector<unsigned int> cpus_of(const Stage &s, int i) const {
    assert(i >= 0 && i < s.threads);
    if (s.kind != ComputeStage::TS)
      return (*topo)[static_cast<size_t>(s.first_phys + i)];
    for (int p = 0; p < num_ts_phys; ++p) {
      const std::vector<unsigned int> &cpus = (*topo)[static_cast<size_t>(p)];
      if (i < static_cast<int>(cpus.size()))
        return {cpus[static_cast<size_t>(i)]};
      i -= static_cast<int>(cpus.size());
    }
    assert(false && "TS worker 序号越界");
    return {};
  }

  static std::string core_text(const Stage &s) {
    std::ostringstream os;
    os << "#" << s.first_phys;
    if (s.last_phys != s.first_phys)
      os << "-" << s.last_phys;
    return os.str();
  }

  std::string summary() const {
    std::ostringstream os;
    os << "Stages (物理核):";
    bool first = true;
    for (const Stage &s : stages()) {
      os << (first ? " " : " | ") << s.name << "×" << s.threads << " (" << core_text(s) << ")";
      first = false;
    }
    return os.str();
  }

  static StageLayout make(const Topology &topo, int num_pf, int num_cs, int num_io) {
    assert(num_pf >= 1 && num_cs >= 1 && num_io >= 1 && "预取 / 截面 / 落盘各至少 1 核");
    StageLayout l;
    l.topo = &topo;
    l.num_cs = num_cs;
    l.num_io = num_io;
    l.num_pf = num_pf;
    l.num_ts_phys = static_cast<int>(topo.size()) - num_cs - num_io - num_pf;
    assert(l.num_ts_phys >= 1 && "物理核不足: 至少 TS × 1 + CS × K_cs + IO × K_io + 预取 × K_pf");
    for (int p = 0; p < l.num_ts_phys; ++p)
      l.num_ts += static_cast<int>(topo[static_cast<size_t>(p)].size());
    return l;
  }
};

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
// 不要改成"自由领任务 (全局队列 / 每日游标)"或"按日窃取 (借一天, 次日归还)":
//   各核专注自己的资产是刻意的 —— per-asset core 状态 (~百 KB, first-touch 在初始 owner 核)
//   跟着 owner 走, 换核才付冷启动. 日间负载噪声由池深吸收 (各核前沿可相差至多 P 天), 不搬资产;
//   只有持续落后 (领跑者被池子卡住 且 对方落后 ≥ 2 天) 才永久转移处置权 —— 一次转移修正一次
//   权重偏差, 之后不再换核. 自由领 / 按日窃取会把每次噪声都变成换核, 且偏差不收敛 (天天重复偷).
//
//   owner:    处置权路标 (领养 CAS victim→adopter; 原 owner 观察到即弃)
//   claimed:  谁处理 (asset, d) 由 CAS claimed d-1→d 唯一裁决 (转移瞬间的
//             双写/漏写由它兜底, owner 只是"该不该去试"的提示)
//   done:     已完成日 didx, release 发布 core 状态 (接手方 acquire 后可续算)
//   frontier: per-worker 已完成日数 (领养的观测面)
//
// cores (per-asset 跨日状态, 档的 Core, 见 feature_pipeline.hpp TsRole) 由初始 owner 在自己核上构造 (first-touch);
// 领养方经 frontier 的 release/acquire 链观察到构造完成后才会触碰.
// ============================================================================
struct TsSchedule {
  // 全部数组按档 A 轴下标 (sub) 索引, 大小 = A 轴大小 (= 张量 A 维)
  std::vector<std::atomic<int32_t>> owner;   // 处置权 worker id (每个资产都有 owner)
  std::vector<std::atomic<int32_t>> claimed; // 已认领日 didx, -1 起
  std::vector<std::atomic<int32_t>> done;    // 已完成日 didx, -1 起
  std::vector<size_t> weight;                // 区间内源负载 (Lob: 逐笔总条数; Bar: 均匀) —— 初始分配 / 领养挑最轻

  // Per-worker 已完成日数 (worker 自报, release; 领养方 acquire 读 —— 与被领养资产的
  // core 构造/状态写入构成 happens-before). 大小 = TS worker 数.
  std::vector<std::atomic<uint32_t>> frontier;

  // Per-worker 被领养预算窗口: (victim 前沿 << 16) | 本窗已被领走数.
  // 前沿推进即换窗重置 —— "下一天最多能被领走多少个"的全局记账.
  std::vector<std::atomic<uint64_t>> adopt_window;

  // 领养节流阈值 N (%), 来自 UI 配置 (worker 启动前写好, 运行期只读);
  // 0 = 关闭领养.
  uint64_t adopt_pct = 10;

  TsSchedule(size_t num_assets, size_t num_workers)
      : owner(num_assets), claimed(num_assets), done(num_assets),
        weight(num_assets, 0), frontier(num_workers), adopt_window(num_workers) {
    for (size_t i = 0; i < num_assets; ++i) {
      owner[i].store(-1, std::memory_order_relaxed);
      claimed[i].store(-1, std::memory_order_relaxed);
      done[i].store(-1, std::memory_order_relaxed);
    }
    for (size_t w = 0; w < num_workers; ++w) {
      frontier[w].store(0, std::memory_order_relaxed);
      adopt_window[w].store(0, std::memory_order_relaxed);
    }
  }
};

// ============================================================================
// 特征计算进度 —— 拉取式仪表盘 (总条 + 四类核心统一热力图 + 慢核明细)
//
// worker 不再各持一行进度句柄: 各段已完成天数在 FeatureStore (query_*_done), TS 逐核
// 前沿/持仓在 TsSchedule, worker 只发布统一的两件事 (ComputeStats::Core, 单写者,
// 渲染线程只读): 单调工作量计数 work + 累计干等微秒 idle_us.
// FeatureProgress 的采样线程每 100ms 拉取全量状态, 原地重画固定 5 行:
//
//   [#######>            ]  48% 238/500 天 | 12m03s, ETA 13m10s (3.1s/天) | 最忙 落盘 98%
//   预取 [▆▇] 忙 均 21% 峰 24% 251/500 20230811 812MB/s (1.2TB)
//   时序 [▆█▇█▅█▇█▂█▇█...] 忙 均 35% 峰 52% 前沿 242..248/500 (20230805) 28.4G单 持仓 69..75
//   截面 [▃] 忙 64% 240/500 (20230801) 8.2k格/s
//   落盘 [▁▂] 忙 均 98% 峰 99% 238/500 20230801 95MB/s (0.9TB)
//
// 每核 1 格, 四类同一个指标 —— 忙碌占比 t (滑窗内不在干等的时间比例), 双重编码:
// 块高 = 1−t (满 = 闲, 空 = 力竭), 颜色 = t 白→红, 基本全在干等 / 本段已完成 = 绿+满块.
// 红色小矮柱 = 这个核在满负荷干活 (力竭); 某段整行红矮 = 该段是瓶颈, 上游各段随之变闲 (池满背压).
// 行内 "忙" = 该段的忙碌占比 (多核段: 均 / 峰); 总行 "最忙" = 段均值最高的段.
//
// 忙碌占比 = 1 − Δidle_us/Δ墙钟 (滑窗): worker 把每次干等实测的微秒数 (steady_clock, 含 sleep 超时余量)
// 累计进 idle_us, 渲染线程滑窗轧差 —— 连续、无采样撞相位问题, 且不依赖
// 带宽标定 (落盘一天一跳的粗粒度计数没法测瞬时带宽). 干等之外都算忙 (含时序池边领养回填).
// 总条以落盘天数为准 (端到端真完成). 时序核间的进度差看 "前沿 最慢..领跑".
// ============================================================================
struct ComputeStats {
  // 每核一槽, 四类统一: work = 单调工作量 (预取=字节, 时序=源条数 (Lob 逐笔 / Bar 分钟 K),
  // 截面=拍格, 落盘=字节), idle_us = 累计干等微秒 (每次 sleep 后按实测时长累加)
  struct alignas(64) Core {
    std::atomic<size_t> work{0};
    std::atomic<size_t> idle_us{0};
  };
  std::vector<Core> prefetch;
  std::vector<Core> cs;
  std::vector<Core> io;
  std::vector<Core> ts;
  ComputeStats(size_t num_pf, size_t num_ts, size_t num_cs, size_t num_io) : prefetch(num_pf), cs(num_cs), io(num_io), ts(num_ts) {}
};

class FeatureProgress {
public:
  // dates = 档的日期轴 (渲染期内调用方保证存活); unit = 时序工作量单位 (Lob "单" = 逐笔条, Bar "bar" = 分钟 K);
  // 构造即预留 5 行并启动采样线程
  FeatureProgress(const StoreRing &store, const TsSchedule &sched,
                  const ComputeStats &stats, const std::vector<misc::Ymd> &dates, const char *unit);
  ~FeatureProgress();

  void stop(); // 终画一帧并换行 (幂等)

private:
  void render();
  long long elapsed_ms() const;

  const StoreRing &store_;
  const char *unit_;
  const TsSchedule &sched_;
  const ComputeStats &stats_;
  const std::vector<misc::Ymd> &dates_;
  std::chrono::steady_clock::time_point start_time_;

  // 滑窗历史 (仅渲染线程触碰): work → 当前速率 (速度展示),
  // idle_us → 忙碌占比 (压力). 核序 = [预取 × K_pf, ts0..tsN-1, 截面 × K_cs, 落盘 × K_io].
  std::vector<size_t> hist_work_; // [core][kWindow] 环形
  std::vector<size_t> hist_idle_; // [core][kWindow] 环形
  std::vector<long long> hist_ms_;
  size_t tick_ = 0;

  // ETA 标定: 首个落盘样本 (时刻, 当时天数), 只用之后的增量算每天耗时 ——
  // 首日落盘要等流水线灌满, 混进平均只会虚高
  long long first_io_ms_ = -1;
  size_t first_io_days_ = 0;

  std::atomic<bool> running_{true};
  std::thread refresh_thread_;
};

// ============================================================================
// PHASE 2 —— 日流水线四角色 (两档同一骨架, 档只换源 TierFeed<T>, 见 feature_pipeline.hpp), 统一骨架:
//   for d in dates: wait_open(store.X_try_open(d)) → 干活 → store.X_close
// 分核依据 = 两条一致性契约 (见 CoreTs.hpp / CoreCs.hpp):
// (核按物理核分, 超线程透明, 见 StageLayout)
//   预取 K 线程: 备源 —— 软源 (Lob: 只暖 page cache, TS 不等它) / 硬源 (Bar: 解码 bar1m 进源环, TS 等 pf_ready); 日间无状态 ⇒ 按日轮转
//   时序 N 线程: TS 是资产局部纯函数 ⇒ 沿 A 维并行; 其余物理核全给它 (每逻辑 CPU 一个; 处置权转移再平衡, 见 TsSchedule)
//   截面 K 线程: CS 只读张量第 t 行, 要整个截面到齐 ⇒ 日内沿 T 逐拍串行; 日间无状态 ⇒ 按日轮转
//   落盘 K 线程: 逐列编码 + 写盘 (见 FeatureDay); 日间无状态 ⇒ 按日轮转
//   (按日轮转 = 日 d 归第 d % K 个, 核内日序递增; 各独占一个物理核)
// 段间握手只经 FeatureStore 的各段已完成天数 (日环, 见 FeatureStore.hpp).
//
// StageCtx = 四角色共有的面; 预取 / 时序另带档的源 (TierFeed) / 调度面 (TsSchedule), 见 feature_pipeline.hpp.
// dates = 档的日期轴 (Lob: 回测日历; Bar: 交易日 [PIPELINE_START_DATE, end_date]), 四角色的 date_idx 都是它的下标.
// worker_id = 全局 worker 序号 (TS 0..N−1 在前, 见 StageLayout), 与 pin 的 CPU 号无关; pin 由 launch 侧完成,
// worker 内只用它做日志 / stats / frontier 下标.
// ============================================================================
template <class T>
struct StageCtx {
  int worker_id;
  FeatureStore<T> &store;
  const PerTier<FeatureLib> &libs; // 本档库 libs[T::kId] (A 轴 / 目录) + CS 旁读档的库 libs[TierSide<T>] (无旁读列时不读); 余档空
  std::span<const misc::Ymd> dates;
  const std::atomic<bool> &cancel;
  ComputeStats::Core &stat; // 本核发布槽 (单写者)
};

// 干等原语 (四角色共用): try_open() 为真即返回它; 否则 cancel → 返回空, 未 cancel → on_idle()
// (干等时可做的活: 时序 = 领养回填, 其余无), sleep kIdleMs 并把实测睡眠时长记入 idle_us (仪表盘忙碌占比; on_idle 算忙).
// 先试后查 cancel: 已就绪的照做 —— 落盘靠它在取消后刷完截面已关的日子.
inline constexpr size_t kIdleMs = 10;

template <class TryOpen, class OnIdle>
auto wait_open(const std::atomic<bool> &cancel, ComputeStats::Core &stat, TryOpen &&try_open, OnIdle &&on_idle) {
  for (;;) {
    if (auto opened = try_open())
      return opened;
    if (cancel.load(std::memory_order_relaxed))
      return decltype(try_open()){};
    on_idle();
    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(kIdleMs));
    const auto slept = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0);
    stat.idle_us.fetch_add(static_cast<size_t>(slept.count()), std::memory_order_relaxed);
  }
}

template <class TryOpen>
auto wait_open(const std::atomic<bool> &cancel, ComputeStats::Core &stat, TryOpen &&try_open) {
  return wait_open(cancel, stat, try_open, [] {});
}

// 截面 / 落盘: 第 lane 核 (共 lanes 核) 处理日 lane, lane + lanes, ...
// 截面: cs_try_open 等本日全部 TS 写完, 逐拍 compute_and_store (实盘同一调用), cs_close 交给 IO.
// 落盘: io_try_open 等本日截面写完, 逐层落盘, io_close 清零交还 slot, 直到本核的日期刷完.
// 档无关, 各自 TU 显式实例化两档 (预取 / 时序随档的源进档 TU, 见 feature_pipeline.hpp)
template <class T>
void cs_loop(const StageCtx<T> &ctx, size_t lane, size_t lanes);
template <class T>
void io_loop(const StageCtx<T> &ctx, size_t lane, size_t lanes);

// ============================================================================
// 特征管线入口 (两档同一骨架, 定义见 feature_pipeline.hpp, 档 TU 显式实例化): ComputeService 备好共享前置
// (日期轴 / 写读库 / fund_panel / 清库 / 日志) 后调用, 返回 = 未取消. on_io(n) = 已落盘天数 (协调线程每 100ms 调, 进度面镜像)
// ============================================================================
struct PipelineArgs {
  SharedData &data;
  const PerTier<FeatureLib> &libs; // 本档库 + CS 旁读档的库 (余档空), 见 StageCtx
  const std::vector<misc::Ymd> &dates;
  size_t ring_slots;
  uint64_t adopt_pct;
  int pf_cores; // 预取核数 (按日轮转, 见 StageLayout)
  int cs_cores; // 截面核数
  int io_cores; // 落盘核数
  const std::atomic<bool> &cancel;
  std::function<void(size_t)> on_io;
};
template <class T>
bool pipeline(const PipelineArgs &args);
