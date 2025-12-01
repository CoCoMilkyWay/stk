#pragma once

#include "misc/progress_parallel.hpp"

// Forward declarations
struct SharedData;
class GlobalFeatureStore;

// ============================================================================
// CROSS-SECTIONAL WORKER (DATE-FIRST, SINGLE-THREADED)
// ============================================================================

void crosssectional_worker(int worker_id,
                           SharedData &data,
                           GlobalFeatureStore &store,
                           misc::ProgressHandle progress_handle);

