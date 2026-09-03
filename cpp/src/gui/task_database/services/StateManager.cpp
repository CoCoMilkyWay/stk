// State Manager Implementation
#include "gui/task_database/services/StateManager.hpp"

#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>

namespace GUI::Database {

// ============================================================================
// Lifecycle
// ============================================================================

awaitable<void> StateManager::initialize() {
  co_await sync_and_scan();
}

awaitable<void> StateManager::sync_and_scan() {
  // Yield immediately to let GUI render before the (possibly networked) sync
  co_await boost::asio::steady_timer(co_await boost::asio::this_coro::executor, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

  // Step 1: 基本面前置 — 启动即水位增量同步 (pending 全 fresh 时零网络直接构建).
  // 交易日历是 L2 覆盖检查的 ground truth, 股票全量是 A 轴的来源, 都必须先就绪.
  co_await fundamental_svc_->update_all();

  // Step 2: 股票全量 → A 轴注册表 → Asset::items (全市场, 无人工名单).
  if (fundamental_svc_->is_ready())
    AssetLoader::load(data_);

  // Step 3: 日历就绪后才扫 L2 (覆盖判定以日历为准).
  // 基本面 Error (本地 parquet 缺失且同步失败) 时不扫 — Encode 保持锁定.
  if (fundamental_svc_->is_ready())
    scan_svc_->trigger_scan();

  refresh_state();
}

// ============================================================================
// State Management
// ============================================================================

void StateManager::refresh_state() {
  // Fundamental data status (BigQuant + Tushare → AssetInfo)
  state_.fundamental_status = fundamental_svc_->get_state().status;

  // Get database check result from scan service
  const auto &check_result = scan_svc_->get_last_check_result();

  // ============================================================================
  // Tab Access Control (基本面 → L2 → 消费端)
  // ============================================================================

  // Overview (基本面面板): 流水线第一步, 永远可进
  state_.tabs.can_access_overview = true;

  // Encode: 基本面 Ready 后解锁 (L2 覆盖检查以交易日历为 ground truth)
  bool ready = state_.all_json_ready();
  state_.tabs.can_access_encode = ready;

  // Table/Browser: 基本面 Ready 且 L2 覆盖检查至少跑过一遍 (Encode 页扫描
  // 完成) —— 不要求覆盖 100% Pass, Table 看的是基本面, Browser 本身就是
  // 来看覆盖缺口的. 100% Pass 只卡 Features→Compute (见 TaskFeatures.cpp
  // 的 database.ready()). 扫描前 Asset::items 等数据还没建好, 依然要灰着.
  bool scanned = check_result.status != DatabaseStatus::Unchecked;
  state_.tabs.can_access_table = ready && scanned;
  state_.tabs.can_access_browser = ready && scanned;
}

} // namespace GUI::Database
