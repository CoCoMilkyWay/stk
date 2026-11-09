#pragma once

#include "misc/date.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>
#include <vector>

class StoreRing;
struct TsSchedule;

// ============================================================================
// 特征计算进度 —— 拉取式仪表盘 (总条 + 四类核心统一热力图 + 慢核明细)
//
// worker 不持进度句柄: 各段已完成天数在 FeatureStore (query_*_done), TS 逐核
// 前沿/配额在 TsSchedule, worker 只发布统一的两件事 (ComputeStats::Core, 单写者,
// 渲染线程只读): 单调工作量计数 work + 累计干等微秒 idle_us.
// ComputeDashboard 的采样线程每 100ms 拉取全量状态, 原地重画固定 5 行:
//
//   [#######>            ]  48% 238/500 天 | 12m03s, ETA 13m10s (3.1s/天) | 最忙 落盘 98%
//   预取 [▆▇] 忙 均 21% 峰 24% 251/500 20230811 812MB/s (1.2TB)
//   时序 [▆█▇█▅█▇█▂█▇█...] 忙 均 35% 峰 52% 前沿 242..248/500 (20230805) 28.4G单 环 10/16/311 待领养 0.8%
//   截面 [▃] 忙 64% 240/500 (20230801) 8.2k格/s
//   落盘 [▁▂] 忙 均 98% 峰 99% 238/500 20230801 95MB/s (0.9TB)
//
// 每核 1 格, 四类同一个指标 —— 忙碌占比 t (滑窗内不在干等的时间比例), 双重编码:
// 块高 = 1−t (满 = 闲, 空 = 力竭), 颜色 = t 白→红, 基本全在干等 / 本段已完成 = 绿+满块.
// 红色小矮柱 = 这个核在满负荷干活 (力竭); 某段整行红矮 = 该段是瓶颈, 上游各段随之变闲 (日环满背压).
// 行内 "忙" = 该段的忙碌占比 (多核段: 均 / 峰); 总行 "最忙" = 段均值最高的段.
//
// 忙碌占比 = 1 − Δidle_us/Δ墙钟 (滑窗): worker 把每次干等实测的微秒数 (steady_clock, 含 sleep 超时余量)
// 累计进 idle_us, 渲染线程滑窗轧差 —— 连续、无采样撞相位问题, 且不依赖
// 带宽标定 (落盘一天一跳的粗粒度计数没法测瞬时带宽). 干等之外都算忙 (含时序日环边领养回填).
// 总条以落盘天数为准 (端到端真完成). 时序核间的进度差看 "前沿 最慢..领跑", "环" = 在用 / 现有 / 上限:
// 在用 = 领跑时序前沿 − 落盘前沿 (逐 tick 实测), 现有 = 日环当前格数 (运行中按需扩, 见 TsTuner);
// "待领养" = 本窗正配额之和占总权重.
// ============================================================================
struct ComputeStats {
  // 每核一槽, 四类统一: work = 单调工作量 (预取=字节, 时序=源条数 (Lob 逐笔 / Bar 分钟 K),
  // 截面=拍格, 落盘=字节), idle_us = 累计干等微秒 (每次 sleep 后按实测时长累加);
  // edge_us = 其中的日环边干等 (只时序: 源已就绪而日环满, 扩格的观测面, 见 TsTuner)
  struct alignas(64) Core {
    std::atomic<size_t> work{0};
    std::atomic<size_t> idle_us{0};
    std::atomic<size_t> edge_us{0};
  };
  std::vector<Core> prefetch;
  std::vector<Core> ts;
  std::vector<Core> cs;
  std::vector<Core> io;
  ComputeStats(size_t num_pf, size_t num_ts, size_t num_cs, size_t num_io) : prefetch(num_pf), ts(num_ts), cs(num_cs), io(num_io) {}
};

class ComputeDashboard {
public:
  // dates = 档的日期轴 (渲染期内调用方保证存活); unit = 时序工作量单位 (Lob "单" = 逐笔条, Bar "bar" = 分钟 K);
  // 构造即预留 5 行并启动采样线程
  ComputeDashboard(const StoreRing &store, const TsSchedule &sched,
                   const ComputeStats &stats, const std::vector<misc::Ymd> &dates, const char *unit);
  ~ComputeDashboard();

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
