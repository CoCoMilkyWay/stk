// Compute Service - Manages feature computation with multi-threaded workers
// High-performance mode: Prefetch + TS workers + CS worker + IO worker
// 线程模型 (gui/util/Worker.hpp, 随对象): 进度面 = 自身 (done/total = 已落盘天 / 回测交易日);
//   store / 调度面 / 仪表盘全是 run 的局部, 一轮一建
#pragma once

#include "features/Backend/FeatureLevels.hpp" // TierId
#include "gui/util/Worker.hpp"

#include <array>
#include <cassert>
#include <cstddef>
#include <sstream>
#include <string>
#include <vector>

// Forward declarations
struct SharedData;

namespace GUI::Features {

// ============================================================================
// Stage Layout - 核布局: 按物理核分 stage, 由机器拓扑唯一推导 (worker 数不可配)
//
//   [TS × (P − 2)] [CS] [IO + 预取]     P = 物理核数
//
// 超线程对布局透明: stage 只占物理核, 再展开到该核的逻辑 CPU ——
//   TS:        每个逻辑 CPU 一个 worker, pin 单个逻辑 CPU (超线程吞吐全归 TS; 无超线程即每核一个)
//   CS:        1 个 worker 独占一个物理核 (截面日后还要加, 留整核)
//   IO + 预取: 共一个物理核 (预取大部分时间阻塞在读盘, 落盘大部分时间等截面), 两者都 pin 该核全部逻辑 CPU
// CS / IO 不要落在 TS 物理核的超线程兄弟上: 同核争访存资源, 落盘慢数倍, 整条流水线被池子卡住.
//
// 全部线程 pin 死 —— 没有浮动线程抢占 pinned worker 的时间片.
// worker_id = 全局序号 [TS 0..num_ts−1][CS][IO][预取] (TS 用作 frontier / stats 下标), 与 pin 的 CPU 号无关.
// stage 顺序 = UI 表格顺序 = 预取, TS..., CS, IO.
// 进度不再按行挂 worker: 仪表盘 (FeatureProgress) 自行采样, 见 feature_workers.hpp.
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
    int cores = 0;        // 独占物理核数 (共核 stage 为 0)
    int first_worker = 0; // worker_id 区间 [first_worker, first_worker + threads)
    int first_phys = 0;   // 物理核区间 [first_phys, last_phys]
    int last_phys = 0;
    bool shared = false;
  };

  static constexpr size_t kStageCount = 4;

  const Topology *topo = nullptr; // 进程级常驻 (physical_cores 的 static)
  int num_ts_phys = 0;            // TS 物理核数, 物理核 0..num_ts_phys-1
  int num_ts = 0;                 // TS worker 数 = 这些物理核上的逻辑 CPU 总数

  int total_phys() const { return static_cast<int>(topo->size()); }
  int total_threads() const { return num_ts + 3; }

  Stage stage(ComputeStage kind) const {
    const int cs = num_ts_phys, io = num_ts_phys + 1;
    switch (kind) {
    case ComputeStage::Prefetch:
      return {kind, "预取 Prefetch", "顺日期把逐笔帧读进 page cache, 领先最慢时序核不超过 pool slots + 2 天",
              1, 0, num_ts + 2, io, io, true};
    case ComputeStage::TS:
      return {kind, "时序 TS", "逐资产 decode + LOB 重建 + DAG, 写 L0/L1 张量; 领跑核自动接手落后核的资产 (处置权转移)",
              num_ts, num_ts_phys, 0, 0, num_ts_phys - 1, false};
    case ComputeStage::CS:
      return {kind, "截面 CS", "本日全部时序核写完后逐秒重放截面 (与实盘同一调用)",
              1, 1, num_ts, cs, cs, false};
    case ComputeStage::IO:
      return {kind, "落盘 IO", "本日截面写完后逐层落盘, 清零交还 slot",
              1, 1, num_ts + 1, io, io, false};
    }
    assert(false && "unknown compute stage");
    return {};
  }

  std::array<Stage, kStageCount> stages() const {
    return {stage(ComputeStage::Prefetch), stage(ComputeStage::TS), stage(ComputeStage::CS), stage(ComputeStage::IO)};
  }

  // stage 内第 i 个 worker pin 的逻辑 CPU: TS 单个逻辑 CPU (物理核序展开), 其余 = 所在物理核全部逻辑 CPU
  std::vector<unsigned int> cpus_of(const Stage &s, int i) const {
    assert(i >= 0 && i < s.threads);
    if (s.kind != ComputeStage::TS)
      return (*topo)[static_cast<size_t>(s.first_phys)];
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
    if (s.shared)
      os << " (共享落盘核)";
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

  static StageLayout make(const Topology &topo) {
    StageLayout l;
    l.topo = &topo;
    l.num_ts_phys = static_cast<int>(topo.size()) - 2;
    assert(l.num_ts_phys >= 1 && "物理核不足: 至少 TS + CS + (IO + 预取)");
    for (int p = 0; p < l.num_ts_phys; ++p)
      l.num_ts += static_cast<int>(topo[static_cast<size_t>(p)].size());
    return l;
  }
};

// ============================================================================
// Compute Config (= 请求)
// ============================================================================

struct ComputeConfig {
  // 池下限 = LabelReturn::MIN_POOL_SLOTS (开盘档标签 T+N 跨日回填, 旧日 slot 持到结清; 少于此死锁), ComputeService.cpp static_assert 对账
  static constexpr int kMinPoolSlots = 12;
  int pool_slots = kMinPoolSlots + 2;
  int adopt_pct = 50;         // TS 负载再平衡阈值 N (%), 0 = 关闭领养 (只 Pool 档)
  TierId tier = TierId::Pool; // 算哪一档 (两档各一个按钮; 只在因子层合并). All 日期轴 = 交易日 [config::PIPELINE_START_DATE, end_date]
};

// ============================================================================
// Compute Service
// ============================================================================

class ComputeService : public Worker<ComputeService, ComputeConfig>, public Progress {
  friend Worker<ComputeService, ComputeConfig>;

public:
  static constexpr const char *kName = "ComputeWorker";
  explicit ComputeService(SharedData &data); // 末尾 Start
  ~ComputeService() { Stop(); }

  // 核布局由机器核数自动推导 (StageLayout::make), UI 只交 ComputeConfig; 先断言再转 Worker::Request
  void Request(ComputeConfig config);
  bool cancelling() const { return busy() && cancel_.load(std::memory_order_relaxed); }

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return *this; }
  bool run(ComputeConfig &config);
  bool run_pool(ComputeConfig &config);
  bool run_all(ComputeConfig &config);

  SharedData &data_;
};

} // namespace GUI::Features
