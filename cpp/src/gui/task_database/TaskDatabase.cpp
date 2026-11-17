#include "gui/task_database/TaskDatabase.hpp"
#include "gui/Tasks.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "gui/task_database/services/EncodeService.hpp"
#include "gui/task_database/services/FileCheckService.hpp"
#include "gui/task_database/services/ScanService.hpp"
#include "gui/task_database/services/StateManager.hpp"
#include "gui/task_database/services/SyncService.hpp"
#include "gui/task_database/ui/TabCalendar.hpp"
#include "gui/task_database/ui/TabEncode.hpp"
#include "gui/task_database/ui/TabSync.hpp"
#include "imgui.h"
#include "misc/exec.hpp"
#include "shared/SharedData.hpp"
#include <cassert>
#include <memory>
#include <mutex>
#include <utility>

namespace GUI::Tasks {
namespace {

using namespace GUI::Database;

class DatabaseTask {
private:
  std::unique_ptr<SyncService> sync_svc_;
  std::unique_ptr<ScanService> scan_svc_;
  std::unique_ptr<EncodeService> encode_svc_;
  std::unique_ptr<FileCheckService> filecheck_svc_;
  std::unique_ptr<StateManager> state_mgr_;
  std::shared_ptr<const Quote::QuoteHub> quote_;

  SyncTabState sync_state_;
  EncodeState encode_state_;
  CalendarState calendar_state_;

  bool is_expanded_ = false;
  bool initialized_ = false;
  SharedData *data_ = nullptr;

public:
  explicit DatabaseTask(std::shared_ptr<const Quote::QuoteHub> quote) : quote_(std::move(quote)) {}

  const char *GetName() const { return "Database"; }

  void Init(SharedData &data) {
    if (initialized_)
      return;
    data_ = &data;
    InitializeServices(data);
    initialized_ = true;
  }

  void OnExpand() { is_expanded_ = true; }

  void OnCollapse() { is_expanded_ = false; }

  void Update(SharedData & /*data*/) {
    if (state_mgr_)
      state_mgr_->tick();
  }

  bool Enabled(const SharedData & /*data*/, int idx) const {
    if (idx <= 0)
      return true;
    if (!state_mgr_)
      return false;
    const auto &tabs = state_mgr_->get_state().tabs;
    switch (idx) {
    case 1:
      return tabs.can_access_encode;
    case 2:
      return tabs.can_access_calendar;
    }
    return false;
  }

  TaskStatus Status(const SharedData &data, int idx) const {
    if (!state_mgr_)
      return {TaskStatus::Kind::Muted, "initializing"};
    switch (idx) {
    case 0:
      return GUI::Database::SyncStatus(*state_mgr_, *sync_svc_);
    case 1:
      return GUI::Database::EncodeStatus(*state_mgr_, *encode_svc_, *scan_svc_);
    case 2:
      return BrowseStatus(data, state_mgr_->get_state().tabs.can_access_calendar);
    }
    assert(false && "Database 子行越界");
    return {};
  }

  void Draw(SharedData & /*data*/, int idx) {
    assert(idx >= 0 && idx < 3);
    RenderUI(idx);

    if (encode_state_.trigger_start) {
      encode_state_.trigger_start = false;
      const int workers = encode_state_.num_workers > 0 ? encode_state_.num_workers : misc::exec::hw();
      state_mgr_->encode(workers, encode_state_.skip_existing);
    }
  }

private:
  void InitializeServices(SharedData &data) {
    sync_svc_ = std::make_unique<SyncService>();
    scan_svc_ = std::make_unique<ScanService>(data);
    encode_svc_ = std::make_unique<EncodeService>(data);
    filecheck_svc_ = std::make_unique<FileCheckService>();
    state_mgr_ = std::make_unique<StateManager>(data, *sync_svc_, *scan_svc_, *encode_svc_);
  }

  TaskStatus BrowseStatus(const SharedData &data, bool can_access) const {
    if (data.task_state.database.sync_inflight)
      return {TaskStatus::Kind::Busy, "syncing"};
    if (data.task_state.database.scan_inflight)
      return {TaskStatus::Kind::Busy, "scanning"};
    if (can_access)
      return {TaskStatus::Kind::Ready, "done"};
    return {};
  }

  void RenderUI(int idx) {
    if (!state_mgr_) {
      ImGui::TextDisabled("Initializing services...");
      return;
    }

    const auto &state = state_mgr_->get_state();
    const CoverageCheck &check_result = state.coverage;

    const TaskStatus fstatus = GUI::Database::SyncStatus(*state_mgr_, *sync_svc_);
    ImGui::Text("Sync: ");
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(fstatus.kind), "[%s]", fstatus.text.c_str());
    if (fstatus.kind == TaskStatus::Kind::Warn || fstatus.kind == TaskStatus::Kind::Error) {
      std::lock_guard lk(sync_svc_->mutex);
      ImGui::SameLine();
      ImGui::TextDisabled("%s", sync_svc_->state.message.c_str());
    }

    ImGui::SameLine();
    ImGui::TextDisabled("|");
    ImGui::SameLine();
    const TaskStatus estatus = GUI::Database::EncodeStatus(*state_mgr_, *encode_svc_, *scan_svc_);
    ImGui::Text("Orders: ");
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(estatus.kind), "[%s]", estatus.text.c_str());
    if (estatus.kind == TaskStatus::Kind::Warn) {
      ImGui::SameLine();
      ImGui::TextDisabled("%zu days pending", check_result.pending.size());
    } else if (estatus.kind == TaskStatus::Kind::Error) {
      ImGui::SameLine();
      ImGui::TextDisabled("%s", check_result.error_message.c_str());
    }

    ImGui::Separator();

    const auto &tabs = state.tabs;
    switch (idx) {
    case 0:
      DrawTabSync();
      break;
    case 1:
      if (tabs.can_access_encode)
        DrawTabEncode();
      else
        ImGui::TextDisabled("Sync fundamental data in Sync tab first");
      break;
    case 2:
      if (tabs.can_access_calendar)
        DrawTabCalendar();
      else
        ImGui::TextDisabled("Requires fundamental data ready and at least one coverage scan (Encode tab)");
      break;
    default:
      break;
    }
  }

  void DrawTabSync() {
    const Quote::QuoteStatus sina = quote_->started() ? quote_->sina() : Quote::QuoteStatus{};
    RenderTabSync(*state_mgr_, *sync_svc_, quote_->started() ? &sina : nullptr, sync_state_);
  }

  void DrawTabEncode() {
    RenderTabEncode(*state_mgr_, *encode_svc_, *filecheck_svc_, *scan_svc_, encode_state_, data_->asset);
  }

  void DrawTabCalendar() {
    RenderTabCalendar(data_->asset_info, data_->asset, data_->asset.backtest.start, data_->asset.backtest.end,
                      calendar_state_);
  }
};

} // namespace

TaskHandle CreateDatabaseTask(std::shared_ptr<const Quote::QuoteHub> quote) {
  auto instance = std::make_shared<DatabaseTask>(std::move(quote));

  TaskHandle handle;
  handle.name = instance->GetName();
  handle.storage = instance;
  handle.tabs = {"Sync", "Encode", "Calendar"};
  handle.Init = [instance](SharedData &data) { instance->Init(data); };
  handle.Update = [instance](SharedData &data) { instance->Update(data); };
  handle.OnExpand = [instance]() { instance->OnExpand(); };
  handle.OnCollapse = [instance]() { instance->OnCollapse(); };
  handle.Status = [instance](const SharedData &data, int idx) { return instance->Status(data, idx); };
  handle.Enabled = [instance](const SharedData &data, int idx) { return instance->Enabled(data, idx); };
  handle.Draw = [instance](SharedData &data, int idx) { instance->Draw(data, idx); };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
