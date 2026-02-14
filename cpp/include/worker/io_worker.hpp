#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "misc/progress_parallel.hpp"

// IO Worker: Flush CS_DONE tensors to disk
// - Scans tensor pool for CS_DONE state (lockfree, IO worker exclusive access)
// - Flushes to disk and resets to UNUSED
// - Updates progress display
void io_worker(int worker_id,
               GlobalFeatureStore &store,
               misc::ProgressHandle progress_handle,
               size_t total_dates);

