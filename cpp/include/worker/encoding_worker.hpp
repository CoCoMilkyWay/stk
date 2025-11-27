#pragma once

#include "misc/progress_parallel.hpp"

#include <atomic>
#include <mutex>
#include <vector>

// Forward declarations
struct SharedData;

// ============================================================================
// PHASE 1: ENCODING WORKER
// ============================================================================

void encoding_worker(SharedData &data, 
                    std::vector<size_t> &asset_id_queue, 
                    std::mutex &queue_mutex, 
                    std::atomic<bool> *cancel_flag,
                    unsigned int core_id, 
                    misc::ProgressHandle progress_handle);

