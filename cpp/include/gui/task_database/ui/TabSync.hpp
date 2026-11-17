// Tab Sync - 数据源 (BigQuant / Tushare / 新浪) 状态、同步与远端核对面板
#pragma once

#include "gui/Tasks.hpp"
#include "gui/quote/services/QuoteService.hpp"

#include <cstdint>

namespace GUI::Database {

class StateManager;
class SyncService;

struct SyncTabState {
  std::uint64_t fit_epoch = ~0ull;
  double fit_last_time = 0.0;
  int fit_frames = 0;
};

// Sync 状态标签 (左栏子行 / 状态头 / TabSync 共用)
TaskStatus SyncStatus(const StateManager &sm, const SyncService &sync_service);

void RenderTabSync(
    StateManager &sm,
    SyncService &sync_service,
    const Quote::QuoteStatus *sina,
    SyncTabState &ui);

} // namespace GUI::Database
