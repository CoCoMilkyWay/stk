#pragma once

#include "features/Storage/FeatureLevels.hpp" // PerTier
#include "misc/date.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Forward declarations
struct SharedData;
struct FeatureLib;
namespace fund {
class Panel;
}
namespace pool {
class Panel;
}
struct SnapHeader;
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
// 进度不按行挂 worker: 仪表盘 (ComputeProgress) 自行采样, 见下.
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
    const char *tag = ""; // 短名: 线程名 <tag>_<段内序号> (perf / top 按线程分核, 见 thread_name)
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
      return {kind, "pf", "预取 Prefetch", "顺日期备源 (Lob: 逐笔帧读进 page cache; Bar: 解码 bar1m 进源格), 领先计满日不超过日环格数 + 2 天; 多核按日轮转",
              num_pf, num_pf, num_ts + num_cs + num_io, pf, pf + num_pf - 1};
    case ComputeStage::TS:
      return {kind, "ts", "时序 TS", "逐资产喂源 + DAG, 写 L_sec/L_1min 张量; 领跑核自动接手落后核的资产 (处置权转移)",
              num_ts, num_ts_phys, 0, 0, num_ts_phys - 1};
    case ComputeStage::CS:
      return {kind, "cs", "截面 CS", "本日全部时序核写完后逐拍重放截面 (与实盘同一调用); 多核按日轮转",
              num_cs, num_cs, num_ts, cs, io - 1};
    case ComputeStage::IO:
      return {kind, "io", "落盘 IO", "本日截面写完后逐层落盘, 清零交还 slot; 多核按日轮转",
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

  // stage 内第 i 个 worker 的线程名
  static std::string thread_name(const Stage &s, int i) { return std::string(s.tag) + "_" + std::to_string(i); }

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
// 资产静态分给 worker 只是初始形态: 权重模型有偏差 + 核速不一, 差距持续拉开 (漂移),
// 而落后者的活别人接不了就只能全员等它. 所以处置权可转移 —— 领跑 worker 在池边
// 干等 slot 时, 对落后 ≥2 天且该让出 (配额 > 0) 的候选按前沿升序逐个尝试, 接手其
// 放得进双方余额的最重资产, 立刻回填该资产落下的旧日期 —— 含 victim 的当天 (claim CAS
// 裁决, victim 见 owner 已变即跳过, 当天即刻变轻; 旧日 slot 未计满必为
// BUSY, 见 FeatureStore), 之后并入自己的日循环. 反馈零延迟.
// 额度 = quota (按权重计, 协调线程按实测忙碌占比每窗重算, 半量修正, 见 TsTuner): 偏差小于一个资产就不动.
//
// 不要改成"自由领任务 (全局队列 / 每日游标)"或"按日窃取 (借一天, 次日归还)":
//   各核专注自己的资产是刻意的 —— per-asset core 状态 (~百 KB, first-touch 在初始 owner 核)
//   跟着 owner 走, 换核才付冷启动. 日间负载起伏 (轮换) 由池深吸收 (各核前沿可相差至多 P 天, 按需扩格), 不搬资产;
//   只有持续落后 (领跑者被池子卡住 且 对方落后 ≥ 2 天) 才永久转移处置权 —— 一次转移修正一次
//   权重偏差, 之后不再换核. 自由领 / 按日窃取会把每次噪声都变成换核, 且偏差不收敛 (天天重复偷).
//
//   owner:    处置权路标 (领养 CAS victim→adopter; 原 owner 观察到即弃)
//   claimed:  谁处理 (asset, d) 由 CAS claimed d-1→d 唯一裁决 (转移瞬间的
//             双写/漏写由它兜底, owner 只是"该不该去试"的提示)
//   done:     已完成日 didx, release 发布 core 状态 (接手方 acquire 后可续算)
//             (两者从 first − 1 起: first = 首个要算的日, 续算 = 尾窗日数, 全量 0)
//   frontier: per-worker 已完成日数 (领养的观测面; 0 起 —— 续算也是, f ≥ 1 才说明对方的核已构造 / 接回)
//
// cores (per-asset 跨日状态, 档的 Core, 见 Pipeline.hpp TsRole) 由初始 owner 在自己核上构造 (first-touch);
// 领养方经 frontier 的 release/acquire 链观察到构造完成后才会触碰.
// ============================================================================
struct TsSchedule {
  // 全部数组按档 A 轴下标 (sub) 索引, 大小 = A 轴大小 (= 张量 A 维)
  std::vector<std::atomic<int32_t>> owner;   // 处置权 worker id (每个资产都有 owner)
  std::vector<std::atomic<int32_t>> claimed; // 已认领日 didx, first − 1 起
  std::vector<std::atomic<int32_t>> done;    // 已完成日 didx, first − 1 起
  std::vector<int64_t> weight;               // 区间内源负载 (TierFeed::weight 逐日和) —— 初始分配 / 领养配额的单位

  // Per-worker 已完成日数 (worker 自报, release; 领养方 acquire 读 —— 与被领养资产的
  // core 构造/状态写入构成 happens-before). 大小 = TS worker 数.
  std::vector<std::atomic<uint32_t>> frontier;

  // Per-worker 领养配额 (权重): 正 = 该让出, 负 = 可接手, 0 = 不动. 协调线程每窗重写 (TsTuner), 领养方 CAS 扣减
  std::vector<std::atomic<int64_t>> quota;
  TsSchedule(size_t num_assets, size_t num_workers, size_t first)
      : owner(num_assets), claimed(num_assets), done(num_assets),
        weight(num_assets, 0), frontier(num_workers), quota(num_workers) {
    const int32_t before = static_cast<int32_t>(first) - 1;
    for (size_t i = 0; i < num_assets; ++i) {
      owner[i].store(-1, std::memory_order_relaxed);
      claimed[i].store(before, std::memory_order_relaxed);
      done[i].store(before, std::memory_order_relaxed);
    }
    for (size_t w = 0; w < num_workers; ++w) {
      frontier[w].store(0, std::memory_order_relaxed);
      quota[w].store(0, std::memory_order_relaxed);
    }
  }
};

// ============================================================================
// 特征计算进度 —— 拉取式仪表盘 (总条 + 四类核心统一热力图 + 慢核明细)
//
// worker 不持进度句柄: 各段已完成天数在 FeatureStore (query_*_done), TS 逐核
// 前沿/配额在 TsSchedule, worker 只发布统一的两件事 (ComputeStats::Core, 单写者,
// 渲染线程只读): 单调工作量计数 work + 累计干等微秒 idle_us.
// ComputeProgress 的采样线程每 100ms 拉取全量状态, 原地重画固定 5 行:
//
//   [#######>            ]  48% 238/500 天 | 12m03s, ETA 13m10s (3.1s/天) | 最忙 落盘 98%
//   预取 [▆▇] 忙 均 21% 峰 24% 251/500 20230811 812MB/s (1.2TB)
//   时序 [▆█▇█▅█▇█▂█▇█...] 忙 均 35% 峰 52% 前沿 242..248/500 (20230805) 28.4G单 环 10/16/311 待领养 0.8%
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
// 总条以落盘天数为准 (端到端真完成). 时序核间的进度差看 "前沿 最慢..领跑", "环" = 在用 / 现有 / 上限:
// 在用 = 领跑时序前沿 − 落盘前沿 (逐 tick 实测), 现有 = 日环当前格数 (运行中按需扩, 见 TsTuner);
// "待领养" = 本窗正配额之和占总权重.
// ============================================================================
struct ComputeStats {
  // 每核一槽, 四类统一: work = 单调工作量 (预取=字节, 时序=源条数 (Lob 逐笔 / Bar 分钟 K),
  // 截面=拍格, 落盘=字节), idle_us = 累计干等微秒 (每次 sleep 后按实测时长累加);
  // edge_us = 其中的池边干等 (只时序: 源已就绪而池满, 扩格的观测面, 见 TsTuner)
  struct alignas(64) Core {
    std::atomic<size_t> work{0};
    std::atomic<size_t> idle_us{0};
    std::atomic<size_t> edge_us{0};
  };
  std::vector<Core> prefetch;
  std::vector<Core> cs;
  std::vector<Core> io;
  std::vector<Core> ts;
  ComputeStats(size_t num_pf, size_t num_ts, size_t num_cs, size_t num_io) : prefetch(num_pf), cs(num_cs), io(num_io), ts(num_ts) {}
};

class ComputeProgress {
public:
  // dates = 档的日期轴 (渲染期内调用方保证存活); unit = 时序工作量单位 (Lob "单" = 逐笔条, Bar "bar" = 分钟 K);
  // 构造即预留 5 行并启动采样线程
  ComputeProgress(const StoreRing &store, const TsSchedule &sched,
                  const ComputeStats &stats, const std::vector<misc::Ymd> &dates, const char *unit);
  ~ComputeProgress();

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
// 特征计算采样 (misc::perf::kEnabled = PROFILE 模式, 否则全空): 协调线程每 100ms tick, 取消 / 管线收尾 finish.
//   首日落盘起 (流水线已灌满, 不含 InitCores / 灌池) perf record 本进程, 满 misc::perf::kSeconds / 取消 / 管线收尾即停,
//   落 <config::PROFILE_DIR>/<池>/<开采时刻 YYYYMMDD_HHMMSS>/ (每轮一目录, 前后两轮可对比):
//     meta.json   本轮配置 (池 / 档 / A 轴 / 日期轴 / 核布局) + 采样窗实测 (日环首末格数; 落盘天速; 逐核工作量速率 / 忙碌占比)
//     perf 报告   按段分组 (组名 = 段 tag, 组内线程 = StageLayout::thread_name), 组首 / 组尾两核 + 全段合并 (见 misc/perf.hpp)
//   忙碌占比口径同仪表盘 (1 − 干等 / 墙钟); perf 样本只计在 CPU 上的时间, 两者之差 = 缺页 / 抢占等非干等的阻塞.
// ============================================================================
namespace misc::perf {
class Capture;
}

struct ProfileSpec {
  std::string pool;
  TierId tier;
  const char *unit; // 时序工作量单位 (TierFeed::kUnit)
  size_t assets;
  std::span<const misc::Ymd> dates;
};

class ComputeProfile {
public:
  ComputeProfile(ProfileSpec spec, const StageLayout &layout, const StoreRing &store, const ComputeStats &stats);
  ~ComputeProfile();
  ComputeProfile(const ComputeProfile &) = delete;
  ComputeProfile &operator=(const ComputeProfile &) = delete;

  void tick();   // 首日落盘起开采; 满 kSeconds 即 close, 报告在报告线程出, 管线照跑
  void finish(); // close + 等报告出齐 (幂等; 未开采则无产物). 取消即调: 采样窗止于取消时刻

private:
  void close(); // 停采 + 落 meta.json + 起报告线程 (幂等)
  // 核序 = layout.stages() 段序 × 段内序号
  struct Snap {
    std::chrono::steady_clock::time_point t;
    size_t io_done = 0;
    size_t slots = 0;
    std::vector<size_t> work, idle_us;
  };
  Snap snap() const;
  void write_meta(const Snap &end) const;

  ProfileSpec spec_;
  const StageLayout &layout_;
  const StoreRing &store_;
  const ComputeStats &stats_;
  std::string dir_;
  std::unique_ptr<misc::perf::Capture> capture_;
  Snap start_;
  bool done_ = false;
  std::thread report_;
};

// ============================================================================
// PHASE 2 —— 日流水线四角色 (两档同一骨架, 档只换源 TierFeed<T>, 见 Pipeline.hpp), 统一骨架:
//   for d in dates: wait_open(store.X_try_open(d)) → 干活 → store.X_close
// 分核依据 = 两条一致性契约 (见 CoreTs.hpp / CoreCs.hpp):
// (核按物理核分, 超线程透明, 见 StageLayout)
//   预取 K 线程: 备源 —— 软源 (Lob: 只暖 page cache, TS 不等它) / 硬源 (Bar: 解码 bar1m 进源格, TS 等 pf_ready); 日间无状态 ⇒ 按日轮转
//   时序 N 线程: TS 是资产局部纯函数 ⇒ 沿 A 维并行; 其余物理核全给它 (每逻辑 CPU 一个; 处置权转移再平衡, 见 TsSchedule)
//   截面 K 线程: CS 只读张量第 t 行, 要整个截面到齐 ⇒ 日内沿 T 逐拍串行; 日间无状态 ⇒ 按日轮转
//   落盘 K 线程: 逐列编码 + 写盘 (见 FeatureDay); 日间无状态 ⇒ 按日轮转
//   (按日轮转 = 日 d 归第 d % K 个, 核内日序递增; 各独占一个物理核)
// 段间握手只经 FeatureStore 的日窗 (日环, 见 FeatureStore.hpp).
//
// StageCtx = 四角色共有的面; 预取 / 时序另带档的源 (TierFeed) / 调度面 (TsSchedule), 见 Pipeline.hpp.
// dates = 本库日期轴 (本档源日历 ∩ 上游库日期轴; 见 FeatureLib.hpp lib_parent), 四角色的 date_idx 都是它的下标.
//   续算: dates = 库末 tail 日 (尾窗) + 新日; 尾窗日张量启动前已读回日环 (FeatureStore::seed),
//   预取 / 截面对它们只 close, 时序从 tail 起 (核由续算点接回, CoreTs::resume), 落盘照常重写 (标签此轮才结清).
// worker_id = 全局 worker 序号 (TS 0..N−1 在前, 见 StageLayout), 与 pin 的 CPU 号无关; pin 由 launch 侧完成,
// worker 内只用它做日志 / stats / frontier 下标.
// ============================================================================
template <class T>
struct StageCtx {
  int worker_id;
  FeatureStore<T> &store;
  const PerTier<FeatureLib> &libs; // 本库 libs[T::kId] (A 轴 / 目录) + 同池 CS 旁读档的库 libs[TierSide<T>] (无旁读列时不读); 余档空
  std::span<const misc::Ymd> dates;
  size_t tail; // 尾窗日数 (dates 前 tail 日; 全量 0)
  const std::atomic<bool> &cancel;
  ComputeStats::Core &stat; // 本核发布槽 (单写者)
};

// 干等原语 (四角色共用): try_open() 为真即返回它; 否则 cancel → 返回空, 未 cancel → on_idle()
// (干等时可做的活: 时序 = 领养回填, 其余无), sleep kIdleMs 并把实测睡眠时长记入 idle_us (仪表盘忙碌占比; on_idle 算忙).
// on_idle 返回 true = 这一轮是池边干等 (时序: 源已就绪而池满), 睡眠另记入 edge_us.
// 先试后查 cancel: 已就绪的照做 —— 落盘靠它在取消后刷完截面已关的日子.
inline constexpr size_t kIdleMs = 10;

template <class TryOpen, class OnIdle>
auto wait_open(const std::atomic<bool> &cancel, ComputeStats::Core &stat, TryOpen &&try_open, OnIdle &&on_idle) {
  for (;;) {
    if (auto opened = try_open())
      return opened;
    if (cancel.load(std::memory_order_relaxed))
      return decltype(try_open()){};
    const bool edge = on_idle();
    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(kIdleMs));
    const size_t slept = static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0).count());
    stat.idle_us.fetch_add(slept, std::memory_order_relaxed);
    if (edge)
      stat.edge_us.fetch_add(slept, std::memory_order_relaxed);
  }
}

template <class TryOpen>
auto wait_open(const std::atomic<bool> &cancel, ComputeStats::Core &stat, TryOpen &&try_open) {
  return wait_open(cancel, stat, try_open, [] { return false; });
}

// 截面 / 落盘: 第 lane 核 (共 lanes 核) 处理日 lane, lane + lanes, ...
// 截面: cs_try_open 等本日全部 TS 写完, 逐拍 compute_and_store (实盘同一调用), cs_close 交给 IO.
// 落盘: io_try_open 等本日截面写完, 逐层落盘, io_close 清零交还 slot, 直到本核的日期刷完.
// 档无关, 各自 TU 显式实例化两档 (预取 / 时序随档的源进档 TU, 见 Pipeline.hpp)
// 截面用时 (特征面板 Node Time): 每截面核一份, 跑完由 pipeline 合并落盘.
//   cols[层][列] = CoreCs::col_cycles; total = 各日 begin_day → end_day 的 TSC 周期 (total − Σ列 = 有效集扫描 / 旁读读入 / 节拍调度)
struct CsClock {
  std::array<std::vector<uint64_t>, LEVEL_COUNT> cols;
  uint64_t total = 0;
  uint64_t days = 0;
};
template <class T>
void cs_loop(const StageCtx<T> &ctx, size_t lane, size_t lanes, CsClock &clock);
template <class T>
void io_loop(const StageCtx<T> &ctx, size_t lane, size_t lanes);

// ============================================================================
// 时序调参 —— 日环深度与领养额度全自动 (无 UI 参数). 时序核间的偏差分两类, 各有各的解:
//   轮换 (各核负载随时段起伏, 均值相当): 池深吸收. 领跑核在池边干等, 之后它自己又成了最慢者
//     —— 当初的干等落在了关键路径上, 记为已确认损失; 加深日环只对这一类有收益.
//   漂移 (某核持续偏慢: 权重模型误差 / 核速差): 领跑核干等但最慢者始终是同一个 (损失永不确认),
//     扩格只是推迟撞池边, 总耗时不变 —— 由领养修正 (TsSchedule::quota).
//   下游瓶颈 (截面 / 落盘跟不上, 时序再快也不多出活): 两者都无益.
// 收益上限 = 下游饥饿: 窗内截面 / 落盘各段平均干等占比取小 × 窗长 —— 下游空等时序的时间, 时序提速至多换回这么多.
//
// 初始日环 (开跑前, 见 pipeline): 下游段 (悬挂 + 截面 / 落盘在飞, 少于 MIN_RING_SLOTS 死锁) + 时序余量 S;
//   S = 初始分配下各核逐日负载 (TierFeed::weight, 同速假设) 的离散模拟 (ts_lead_days), 再按内存截断.
// 运行中 (TsTuner, 协调线程每 100ms tick), 窗口 = 最慢前沿推进 kWindowDays 天 (且不短于 kWindowMinMs):
//   扩格: min(窗内已确认损失, 下游饥饿) ≥ kLossFrac × 窗长, 且内存余量够 (RingBudget) ⇒ 扩一格 (每窗至多一格)
//   配额: 下游饥饿 ≥ kLossFrac × 窗长且窗内有池边干等 ⇒ 各核忙碌占比 β (1 − 干等 / 墙钟) 偏离均值的半量:
//         quota = ½ × 持仓权重 × (β − β̄) / β (正 = 该让出, 负 = 可接手); 误差逐窗减半, 不被噪声带着过冲. 否则清零
// ============================================================================

// 日环内存预算 (Linux MemAvailable 口径: 可回收的 page cache 算可用): 用到总内存的 1 − kMemMargin 为止
struct RingBudget {
  static constexpr double kMemMargin = 0.1;
  size_t per_slot;           // 一格的字节 (张量 + 源格)
  size_t affordable() const; // 现在还能再添的格数
  size_t cap() const;        // 总内存口径的格数上限
};

// 时序余量 S (天): load[w][d] = 第 w 核第 d 日的负载; 核开工日 d 须 ≤ 最慢核已完成日数 + S.
// 取模拟总耗时不超过无限深 (1 + kLeadSlack) 倍的最小 S (≥ 1: 同负载也有日内相位噪声), 至多 max_lead
size_t ts_lead_days(const std::vector<std::vector<double>> &load, size_t max_lead);

class TsTuner {
public:
  static constexpr size_t kWindowDays = 10;
  static constexpr long long kWindowMinMs = 5000;
  static constexpr double kLossFrac = 0.01;

  // grow = 扩一格 (FeatureStore::grow); total = 日期轴天数 (前沿到底的核不参与)
  TsTuner(const StoreRing &store, TsSchedule &sched, const ComputeStats &stats, size_t total, RingBudget budget, std::function<void()> grow);
  void tick();

private:
  void close_window(long long now_us, size_t f_min, const std::vector<size_t> &f);

  const StoreRing &store_;
  TsSchedule &sched_;
  const ComputeStats &stats_;
  const size_t total_;
  const RingBudget budget_;
  const std::function<void()> grow_;
  const std::chrono::steady_clock::time_point t0_;

  std::vector<size_t> edge_;    // [ts] 上一 tick 的 edge_us
  std::vector<size_t> pending_; // [ts] 自上次是最慢者以来的池边干等 (µs), 它再成最慢者时确认
  std::vector<size_t> idle0_;   // [ts] 窗口起点的 idle_us
  size_t cs_idle0_ = 0;         // 窗口起点截面各核 idle_us 之和
  size_t io_idle0_ = 0;         // 窗口起点落盘各核 idle_us 之和
  long long win_us_ = 0;        // 窗口起点
  size_t win_f_ = 0;            // 窗口起点的最慢前沿
  size_t loss_us_ = 0;          // 窗内已确认损失
  size_t edge_win_us_ = 0;      // 窗内领跑核的池边干等
};

// ============================================================================
// 特征管线入口 (两档同一骨架 pipeline<T>, 定义见 Pipeline.hpp, 经档入口表 TierOps 调): ComputeService 备好共享前置
// (日期轴 / 写读库 / 基本面面板 / 池面板 / 清库 / 日志) 后调用, 返回 = 未取消. on_io(n) = 已落盘天数 (协调线程每 100ms 调, 进度面镜像)
// 跑完 (未取消) 即落本库续算点 (末日 = dates.back(), 见 FeatureSnap.hpp); tail > 0 = 续算, 从 dates[tail − 1] 的续算点接着算
// ============================================================================
struct PipelineArgs {
  SharedData &data;
  const PerTier<FeatureLib> &libs; // 本库 + 同池 CS 旁读档的库 (余档空), 见 StageCtx
  const fund::Panel &fund;         // 日频 PIT 基本面 (全市场, 库日期轴), TS worker 只读共享
  const pool::Panel &members;      // 本库逐日成员 (轴 = 本库 A 轴, 日期轴同 dates), TS worker 只读共享 → Meta 的 cs_valid
  const std::vector<misc::Ymd> &dates;
  size_t tail;  // 尾窗日数, 见 StageCtx
  int pf_cores; // 预取核数 (按日轮转, 见 StageLayout)
  int cs_cores; // 截面核数
  int io_cores; // 落盘核数
  const std::atomic<bool> &cancel;
  std::function<void(size_t)> on_io;
};

// ============================================================================
// 档入口表: 档 TU 各定义一份 <ns>_ops (= tier_ops_of<ns::Tier>(), 见 Pipeline.hpp), 按运行时档号取 (tier_ops);
// 调用方 (ComputeService / 库表) 不碰档类型与字段表
// ============================================================================
struct TierOps {
  const char *unit;                                           // 时序工作量单位 (TierFeed::kUnit)
  size_t pend_days;                                           // 悬挂日环 = 续算尾窗上限 (LabelHold::PEND_DAYS)
  std::vector<misc::Ymd> (*source_dates)(const SharedData &); // 源日历: 本档原始数据现在能服务的交易日 (TierFeed::source_dates)
  SnapHeader (*snap_header)(const FeatureLib &, misc::Ymd);   // 续算点头: 库截到某日可续算的全部前提 (见 FeatureSnap.hpp)
  bool (*pipeline)(const PipelineArgs &);                     // 管线, 返回 = 未取消
};
#define TIER_OPS_DECL(T, ns) extern const TierOps ns##_ops;
ALL_TIERS(TIER_OPS_DECL)
#undef TIER_OPS_DECL
inline const TierOps &tier_ops(TierId t) {
#define TIER_OPS_ONE(T, ns) \
  if (t == TierId::T)       \
    return ns##_ops;
  ALL_TIERS(TIER_OPS_ONE)
#undef TIER_OPS_ONE
  assert(false && "tier_ops: 未知档");
  __builtin_unreachable();
}
