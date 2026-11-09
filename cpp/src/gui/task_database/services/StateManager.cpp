// State Manager Implementation
#include "gui/task_database/services/StateManager.hpp"
#include "gui/task_database/services/EncodeService.hpp"
#include "gui/task_database/services/SyncService.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <utility>

namespace GUI::Database {

// ============================================================================
// Lifecycle
// ============================================================================

StateManager::StateManager(SharedData &data, SyncService &sync, ScanService &scan, EncodeService &enc,
                           AuditService &audit)
    : data_(data), sync_(sync), scan_(scan), enc_(enc), audit_(audit) {
  // 上次核对结果: 读逐表状态 (零网络)
  this->audit(AuditOp::Load, {}, false);
  refresh();
}

void StateManager::refresh() {
  sync_.Request({sync_ch_.next(), data_.config.sync_fundamental, data_.config.sync_bar1m});
}

bool StateManager::frozen() const { return data_.task_state.compute.computing; }

void StateManager::encode(int workers, bool skip_existing) {
  assert(can_encode() && "encode: 同步 / 扫描 / 编码在飞, 或共享数据冻结");
  enc_.Request({enc_ch_.next(), workers, skip_existing});
}

void StateManager::audit(AuditOp op, std::string table, bool incremental) {
  assert(!auditing() && "audit: 上一轮在飞");
  audit_op_ = op;
  audit_.Request({audit_ch_.next(), op, std::move(table), incremental});
}

void StateManager::scan(ScanMode mode) {
  const bool cache_matches_config =
      data_.asset.orders.scanned &&
      data_.asset.archive.scanned &&
      data_.asset.orders.path == data_.config.orders_dir &&
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

  // 扫描: 产物换进 Asset + 留存覆盖结论 (scan 空 = 两库皆无, Asset 不动); 冻结时不收
  if (!frozen()) {
    if (auto r = scan_.handoff.take()) {
      scan_ch_.taken = r->seq;
      if (scan_ch_.latest(r->seq)) {
        if (r->scan)
          data_.asset.adopt(std::move(*r->scan));
        state_.coverage = std::move(r->coverage);
      }
    }
  }

  // 基本面: 扫描 / 编码在飞时不收 (它们正读 asset_info / items), 冻结时不收, 件留在邮箱里等
  if (!frozen() && !scanning() && !encoding()) {
    if (auto r = sync_.handoff.take()) {
      sync_ch_.taken = r->seq;
      if (sync_ch_.latest(r->seq)) {
        state_.sync_taken = true;
        state_.asset_info_ready = r->ok;
        // 失败 (本地 parquet 缺失) 不扫: 日历是覆盖判定的 ground truth, Encode 保持锁定
        if (r->ok) {
          data_.asset_info = std::move(r->asset_info);
          // 股票全量 → A 轴注册表 → Asset::items (全市场, 无人工名单)
          data_.asset.materialize(data_.asset_info);
          scan(ScanMode::RecomputeCoverage);
        }
      }
    }
  }

  // ============================================================================
  // Tab Access Control (基本面 → L2 → 消费端)
  // ============================================================================

  // Sync (数据源同步): 流水线第一步, 永远可进
  state_.tabs.can_access_sync = true;

  // Encode: 基本面 Ready 后解锁 (L2 覆盖检查以交易日历为 ground truth)
  const bool ready = state_.asset_info_ready;
  state_.tabs.can_access_encode = ready;

  // Calendar: 基本面 Ready 且 L2 覆盖检查至少跑过一遍 (Encode 页扫描完成) ——
  // 不要求编码全部 done, 它本身就是来看覆盖缺口的. 扫描前 Asset::items 等数据还没建好, 依然要灰着.
  const bool scanned = state_.coverage.status != CoverageStatus::Unchecked;
  state_.tabs.can_access_calendar = ready && scanned;

  publish_task_state();
}

void StateManager::publish_task_state() {
  auto &ts = data_.task_state.database;
  const CoverageCheck &coverage = state_.coverage;

  ts.sync_inflight = syncing();
  ts.scan_inflight = scanning();
  ts.encode_inflight = encoding();
  ts.scanned = (coverage.status != CoverageStatus::Unchecked);
  ts.asset_info_ready = state_.asset_info_ready;
}

} // namespace GUI::Database
