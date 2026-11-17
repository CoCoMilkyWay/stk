#include "gui/task_database/services/StateManager.hpp"
#include "gui/task_database/services/EncodeService.hpp"
#include "gui/task_database/services/SyncService.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <utility>

namespace GUI::Database {

StateManager::StateManager(SharedData &data, SyncService &sync, ScanService &scan, EncodeService &enc)
    : data_(data), sync_(sync), scan_(scan), enc_(enc) {
  refresh(std::exchange(data_.reinit.online, true));
}

void StateManager::refresh(bool online) {
  sync_.Request({sync_ch_.next(), SyncOp::Sync, online && data_.config.sync_fundamental, online && data_.config.sync_bar1m});
}

void StateManager::audit(std::string table, bool incremental) {
  assert(can_audit() && "audit: 同步 / 扫描 / 编码在飞, 或未开启联网");
  sync_.Request({sync_ch_.next(), SyncOp::Audit, false, false, std::move(table), incremental});
}

void StateManager::repair(std::string table) {
  assert(can_audit() && !table.empty() && "repair: 同步 / 扫描 / 编码在飞, 或未开启联网");
  sync_.Request({sync_ch_.next(), SyncOp::Repair, false, false, std::move(table), false});
}

bool StateManager::frozen() const { return data_.task_state.compute.computing; }

bool StateManager::online() const { return data_.config.sync_fundamental || data_.config.sync_bar1m; }

void StateManager::encode(int workers, bool skip_existing) {
  assert(can_encode() && "encode: 同步 / 扫描 / 编码在飞, 或共享数据冻结");
  enc_.Request({enc_ch_.next(), workers, skip_existing});
}

void StateManager::scan(ScanMode mode) {
  const bool cache_matches_config =
      data_.asset.orders.scanned &&
      data_.asset.archive.scanned &&
      data_.asset.archive.path == data_.config.archive_dir;
  if (mode == ScanMode::RecomputeCoverage && !cache_matches_config)
    mode = ScanMode::RescanStorage;
  scan_.Request({scan_ch_.next(), mode});
}

void StateManager::tick() {
  // 编码: 日文件被 rename 换掉 → 重新逐日扫盘
  if (auto seq = enc_.handoff.take()) {
    enc_ch_.taken = *seq;
    if (enc_ch_.latest(*seq))
      scan(ScanMode::RescanStorage);
  }

  // 扫描: 产物换进 Asset + 留存覆盖结论; 冻结时不收
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

  // 数据集: 扫描 / 编码在飞或冻结时不收; 只读 Audit (rebuilt = false) 仅结清通道
  if (!frozen() && !scanning() && !encoding()) {
    if (auto r = sync_.handoff.take()) {
      sync_ch_.taken = r->seq;
      if (sync_ch_.latest(r->seq) && r->rebuilt) {
        state_.sync_taken = true;
        state_.asset_info_ready = r->ok;
        if (r->ok) {
          data_.asset_info = std::move(r->asset_info);
          data_.asset.materialize(data_.asset_info);
          scan(ScanMode::RecomputeCoverage);
        }
      }
    }
  }

  state_.tabs.can_access_sync = true;
  const bool ready = state_.asset_info_ready;
  state_.tabs.can_access_encode = ready;
  state_.tabs.can_access_calendar = ready && (state_.coverage.status != CoverageStatus::Unchecked);

  publish_task_state();
}

void StateManager::publish_task_state() {
  auto &ts = data_.task_state.database;
  ts.sync_inflight = syncing();
  ts.scan_inflight = scanning();
  ts.encode_inflight = encoding();
  ts.scanned = (state_.coverage.status != CoverageStatus::Unchecked);
  ts.asset_info_ready = state_.asset_info_ready;
}

} // namespace GUI::Database
