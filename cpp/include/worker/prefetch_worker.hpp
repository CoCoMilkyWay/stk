#pragma once

#include "misc/progress_parallel.hpp"
#include <atomic>

// Forward declarations
struct SharedData;
class GlobalFeatureStore;

// ============================================================================
// PHASE 2: PREFETCH WORKER (READ-AHEAD, DATE-FIRST TRAVERSAL)
// ============================================================================
// 顺日期把 .bin 读进 page cache, 让 TS worker 的 decode 只吃缓存不等磁盘.
// 门控: 领先最慢 TS (store.query_ts_days_done) 不超过 pool slots + 余量 ——
// 再往前读只会把 TS 正在用的缓存挤掉.

void prefetch_worker(int worker_id,
                     SharedData &data,
                     GlobalFeatureStore &store,
                     const std::atomic<bool> &cancel_requested,
                     misc::ProgressHandle progress_handle);
