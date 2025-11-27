#pragma once

#include "misc/progress_parallel.hpp"

// Forward declarations
struct SharedData;
class GlobalFeatureStore;

// ============================================================================
// PHASE 2: SEQUENTIAL WORKER (TIME-SERIES, DATE-FIRST TRAVERSAL)
// ============================================================================

void sequential_worker(SharedData &data,
                      int worker_id,
                      GlobalFeatureStore *feature_store,
                      misc::ProgressHandle progress_handle);

