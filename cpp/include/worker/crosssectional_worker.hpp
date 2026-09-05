#pragma once

#include "misc/progress_parallel.hpp"
#include <atomic>

// Forward declarations
struct SharedData;
class GlobalFeatureStore;

// ============================================================================
// PHASE 2: CROSS-SECTIONAL WORKER (DATE-FIRST, SINGLE-THREADED)
// ============================================================================

void crosssectional_worker(int worker_id,
                           SharedData &data,
                           GlobalFeatureStore &store,
                           const std::atomic<bool> &cancel_requested,
                           misc::ProgressHandle progress_handle);
