#pragma once

#include <array>
#include <cassert>
#include <sstream>
#include <string>
#include <vector>

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
// 进度不按行挂 worker: 仪表盘 (ComputeProgress) 自行采样, 见 Progress.hpp.
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
