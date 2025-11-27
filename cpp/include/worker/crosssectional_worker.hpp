#pragma once

#include "misc/progress_parallel.hpp"

// Forward declarations
struct SharedData;
class GlobalFeatureStore;

// ============================================================================
// CROSS-SECTIONAL WORKER (DATE-FIRST, SINGLE-THREADED)
// ============================================================================

void crosssectional_worker(SharedData &data,
                           GlobalFeatureStore* feature_store,
                           int worker_id,
                           misc::ProgressHandle progress_handle);

