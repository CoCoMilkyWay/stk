#pragma once

#include "misc/progress_parallel.hpp"
#include <atomic>
#include <cstdint>
#include <memory>
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
// worker 发现差距超阈值时, 从最慢 worker 接手其最轻的一个资产, 立刻回填该
// 资产落下的旧日期 (旧日 slot 未计满必为 BUSY, 见 FeatureStore), 之后并入
// 自己的日循环. 落后者下一天马上变轻, 反馈零延迟; 全局冷却限速, 一次一个.
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
  std::atomic<int64_t> last_adopt_didx{INT64_MIN};    // 全局领养冷却

  explicit TsSchedule(size_t num_assets);
  ~TsSchedule();
};

// ============================================================================
// PHASE 2: SEQUENTIAL WORKER (TIME-SERIES, DATE-FIRST TRAVERSAL)
// ============================================================================

void sequential_worker(int worker_id,
                       SharedData &data,
                       GlobalFeatureStore &store,
                       TsSchedule &sched,
                       const std::atomic<bool> &cancel_requested,
                       misc::ProgressHandle progress_handle);
