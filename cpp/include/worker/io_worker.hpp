#pragma once

#include "misc/progress_parallel.hpp"
#include <atomic>

// Forward declarations
struct SharedData;
class GlobalFeatureStore;

// ============================================================================
// PHASE 2: IO WORKER (FLUSH, SINGLE-THREADED)
// ============================================================================
// 摘 DONE slot 落盘并归还池 (FREE), 直到全部日期刷完.

void io_worker(int worker_id,
               SharedData &data,
               GlobalFeatureStore &store,
               const std::atomic<bool> &cancel_requested,
               misc::ProgressHandle progress_handle);
