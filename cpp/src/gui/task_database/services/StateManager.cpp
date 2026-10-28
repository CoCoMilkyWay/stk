// State Manager Implementation
#include "gui/task_database/services/StateManager.hpp"
#include "gui/task_database/services/AssetLoader.hpp"
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_database/services/FundamentalService.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <utility>

namespace GUI::Database {

// ============================================================================
// Lifecycle
// ============================================================================

StateManager::StateManager(SharedData &data, FundamentalService &fund, ScanService &scan, EncodingService &enc,
                           AuditService &audit)
    : data_(data), fund_(fund), scan_(scan), enc_(enc), audit_(audit) {
  // 上次核对结果: 读逐表状态 (零网络)
  this->audit(AuditOp::Load, {}, false);
  refresh();
}

void StateManager::refresh() {
  fund_.Request({fund_ch_.next(), data_.config.sync_fundamental, data_.config.sync_bar1m});
}

void StateManager::encode(int workers, bool skip_existing) {
  assert(!syncing() && !scanning() && !encoding() && "encode: 同步 / 扫描 / 编码在飞");
  enc_.Request({enc_ch_.next(), workers, skip_existing});
}

void StateManager::audit(AuditOp op, std::string table, bool incremental) {
  assert(!auditing() && "audit: 上一轮在飞");
  audit_op_ = op;
  audit_.Request({audit_ch_.next(), op, std::move(table), incremental});
}

void StateManager::scan(ScanMode mode) {
  const bool cache_matches_config =
      data_.asset.binary.scanned &&
      data_.asset.archive.scanned &&
      data_.asset.binary.path == data_.config.orders_dir &&
      data_.asset.archive.path == data_.config.archive_dir;
  if (mode == ScanMode::RecomputeCoverage && !cache_matches_config)
    mode = ScanMode::RescanStorage;
  scan_.Request({scan_ch_.next(), mode});
}

// ============================================================================
// Per-frame pipeline
// ============================================================================

void StateManager::tick() {
  // 核对: Repair 改了分片 → 重走刷新链
  if (auto seq = audit_.handoff.take()) {
    audit_ch_.taken = *seq;
    if (audit_ch_.latest(*seq) && audit_op_ == AuditOp::Repair)
      refresh();
  }

  // 编码: 日文件被 rename 换掉 → 重新逐日扫盘 (扫描整体重建, 不用告诉它动过哪些天)
  if (auto seq = enc_.handoff.take()) {
    enc_ch_.taken = *seq;
    if (enc_ch_.latest(*seq))
      scan(ScanMode::RescanStorage);
  }

  // 扫描: 产物换进 Asset + 留存覆盖结论 (scan 空 = 两库皆无, Asset 不动)
  if (auto r = scan_.handoff.take()) {
    scan_ch_.taken = r->seq;
    if (scan_ch_.latest(r->seq)) {
      if (r->scan)
        data_.asset.adopt(std::move(*r->scan));
      state_.check = std::move(r->check);
    }
  }

  // 基本面: 扫描 / 编码在飞时不收 (它们正读 assetinfo / items), 件留在邮箱里等
  if (!scanning() && !encoding()) {
    if (auto r = fund_.handoff.take()) {
      fund_ch_.taken = r->seq;
      if (fund_ch_.latest(r->seq)) {
        state_.fundamental_taken = true;
        state_.fundamental_ok = r->ok;
        // 失败 (本地 parquet 缺失) 不扫: 日历是覆盖判定的 ground truth, Encode 保持锁定
        if (r->ok) {
          data_.assetinfo = std::move(r->assetinfo);
          // 股票全量 → A 轴注册表 → Asset::items (全市场, 无人工名单)
          AssetLoader::load(data_);
          scan(ScanMode::RecomputeCoverage);
        }
      }
    }
  }

  // ============================================================================
  // Tab Access Control (基本面 → L2 → 消费端)
  // ============================================================================

  // Overview (基本面面板): 流水线第一步, 永远可进
  state_.tabs.can_access_overview = true;

  // Encode: 基本面 Ready 后解锁 (L2 覆盖检查以交易日历为 ground truth)
  const bool ready = state_.all_json_ready();
  state_.tabs.can_access_encode = ready;

  // Table/Browser: 基本面 Ready 且 L2 覆盖检查至少跑过一遍 (Encode 页扫描
  // 完成) —— 不要求覆盖 100% Pass, Table 看的是基本面, Browser 本身就是
  // 来看覆盖缺口的. 扫描前 Asset::items 等数据还没建好, 依然要灰着.
  const bool scanned = state_.check.status != DatabaseStatus::Unchecked;
  state_.tabs.can_access_table = ready && scanned;
  state_.tabs.can_access_browser = ready && scanned;

  publish_task_state();
}

void StateManager::publish_task_state() {
  auto &ts = data_.taskstate.database;
  const DatabaseCheckResult &check = state_.check;

  ts.json_update_inflight = syncing();
  ts.l2_scan_inflight = scanning();
  ts.binary_scanned = (check.status != DatabaseStatus::Unchecked);
  ts.binary_pass = (check.status == DatabaseStatus::Pass);
  ts.all_json_ready = state_.all_json_ready();

  // busy 态优先: 流水线顺序 基本面同步 → L2 扫描
  if (ts.json_update_inflight) {
    ts.status = TaskState::Database::Status::Syncing;
  } else if (ts.l2_scan_inflight) {
    ts.status = TaskState::Database::Status::Scanning;
  } else if (check.status == DatabaseStatus::Error ||
             check.status == DatabaseStatus::NoData ||
             check.status == DatabaseStatus::NeedArchive) {
    ts.status = TaskState::Database::Status::Error;
  } else if (check.status == DatabaseStatus::Unchecked) {
    ts.status = TaskState::Database::Status::NotScanned;
  } else if (check.status != DatabaseStatus::Pass || !state_.all_json_ready()) {
    ts.status = TaskState::Database::Status::Incomplete;
  } else {
    ts.status = TaskState::Database::Status::Ready;
  }
}

} // namespace GUI::Database
