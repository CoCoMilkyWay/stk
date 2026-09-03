#include "gui/task_database/TaskDatabase.hpp"
#include "gui/Tasks.hpp"
#include "gui/coro/CoroManager.hpp"
#include "gui/task_database/services/AssetLoader.hpp"
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_database/services/FundamentalService.hpp"
#include "gui/task_database/services/L2DatabaseService.hpp"
#include "gui/task_database/services/ScanService.hpp"
#include "gui/task_database/services/StateManager.hpp"
#include "gui/task_database/ui/TabBrowser.hpp"
#include "gui/task_database/ui/TabEncode.hpp"
#include "gui/task_database/ui/TabOverview.hpp"
#include "gui/task_database/ui/TabTable.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <memory>
#include <thread>

namespace GUI::Tasks {
namespace {

using namespace GUI::Database;

class DatabaseTask {
private:
  // Service layer
  std::unique_ptr<FundamentalService> fundamental_svc_;
  std::unique_ptr<ScanService> scan_svc_;
  std::unique_ptr<EncodingService> encoding_svc_;
  std::unique_ptr<L2DatabaseService> l2_svc_;
  std::unique_ptr<StateManager> state_mgr_;

  // UI state
  EncodeState encode_state_;
  TableState table_state_;
  BrowserState browser_state_;

  // Lifecycle
  bool is_expanded_ = false;
  bool initialized_ = false;
  CoroManager *coro_mgr_ = nullptr;
  SharedData *data_ = nullptr; // Pointer to shared data
  Config *config_ = nullptr;   // Pointer to config for accessing backtest dates
  std::string config_dir_ = "../../config";

public:
  DatabaseTask() = default;

  const char *GetName() const {
    return "Database";
  }

  // 与 DrawPanel/选中态解耦: 创建后立即起后台检查 (基本面 sync → L2 scan),
  // 不需要用户手动点开 Database 页面. Init 只调一次 (initialized_ 兜底),
  // 顺序依赖见 Tasks.cpp::CreateAllTasks (Settings 先落盘配置到内存).
  void Init(SharedData &data) {
    if (initialized_)
      return;
    coro_mgr_ = &data.coromgr;
    data_ = &data;
    config_ = &data.config;
    InitializeServices(data);
    initialized_ = true;
  }

  void OnExpand() {
    is_expanded_ = true;
  }

  void OnCollapse() {
    is_expanded_ = false;
  }

  void DrawPanel(SharedData &data) {
    // Services 已在 Init() 里提前起好, 中途打开本页只管渲染当前进度.
    RenderUI();

    // Handle encoding trigger from UI
    if (encode_state_.trigger_start && encoding_svc_ && !encoding_svc_->is_running()) {
      encode_state_.trigger_start = false;

      int workers = encode_state_.num_workers;
      if (workers <= 0) {
        workers = std::thread::hardware_concurrency();
        if (workers <= 0)
          workers = 8;
      }

      // Start encoding in background thread (non-blocking)
      encoding_svc_->start_encoding(workers, encode_state_.skip_existing);
    }
  }

private:
  // Trigger unified refresh flow: sync fundamental parquet + rebuild AssetInfo
  // A 轴/日历来自 parquet 数据源; 成功后补扫 L2 (日历可能延长, 覆盖判定要重算)
  void TriggerRefreshFlow() {
    auto &ts = data_->taskstate.database;
    if (ts.json_update_inflight || !fundamental_svc_)
      return;

    auto &io = coro_mgr_->GetIoContext();
    ts.json_update_inflight = true;

    boost::asio::co_spawn(
        io,
        [this]() -> boost::asio::awaitable<void> {
          auto &ts = data_->taskstate.database;
          struct FlagReset {
            bool &flag;
            ~FlagReset() { flag = false; }
          } update_reset{ts.json_update_inflight};

          co_await fundamental_svc_->update_all();
          if (fundamental_svc_->is_ready()) {
            // 新上市的股票在这里追加到 A 轴尾部, 再重建 items
            AssetLoader::load(*data_);
            scan_svc_->trigger_scan();
          }
          state_mgr_->refresh_state();
          UpdateTaskState();
        }(),
        boost::asio::detached);
  }

  void InitializeServices(SharedData &data) {
    auto &io = coro_mgr_->GetIoContext();

    // Create services (in dependency order)
    fundamental_svc_ = std::make_unique<FundamentalService>(io, data);
    scan_svc_ = std::make_unique<ScanService>(data, io, &data.terminal);
    encoding_svc_ = std::make_unique<EncodingService>(data, &data.terminal);
    l2_svc_ = std::make_unique<L2DatabaseService>(data);
    state_mgr_ = std::make_unique<StateManager>(data, fundamental_svc_.get(), scan_svc_.get());

    // Set encoding completion callback to trigger scan
    encoding_svc_->set_scan_callback([this]() {
      scan_svc_->trigger_scan();
    });

    // Set scan completion callback to update task state
    scan_svc_->set_on_complete([this]() {
      UpdateTaskState();
    });

    // Initialize: 本地 parquet → AssetInfo
    // Non-blocking, user sees progress in terminal
    boost::asio::co_spawn(
        io,
        [this]() -> boost::asio::awaitable<void> {
          co_await state_mgr_->initialize();
          // Update task state after initialization completes
          UpdateTaskState();
        }(),
        boost::asio::detached);
  }

  void UpdateTaskState() {
    auto &ts = data_->taskstate.database;

    if (!state_mgr_ || !scan_svc_) {
      ts.status = TaskState::Database::Status::Initializing;
      return;
    }

    auto check_result = scan_svc_->get_last_check_result();
    const auto &state = state_mgr_->get_state();

    // Update flags
    ts.binary_scanned = (check_result.status != DatabaseStatus::Unchecked);
    ts.binary_pass = (check_result.status == DatabaseStatus::Pass);
    ts.all_json_ready = state.all_json_ready();

    // Update status
    if (check_result.status == DatabaseStatus::Error ||
        check_result.status == DatabaseStatus::NoData ||
        check_result.status == DatabaseStatus::NeedArchive) {
      ts.status = TaskState::Database::Status::Error;
    } else if (check_result.status != DatabaseStatus::Pass || !state.all_json_ready()) {
      ts.status = TaskState::Database::Status::Incomplete;
    } else {
      ts.status = TaskState::Database::Status::Ready;
    }
  }

  void RenderUI() {
    if (!state_mgr_) {
      ImGui::TextDisabled("Initializing services...");
      return;
    }

    // Refresh state before rendering
    state_mgr_->refresh_state();
    UpdateTaskState();
    const auto &state = state_mgr_->get_state();

    // Get database check result from scan service
    auto check_result = scan_svc_->get_last_check_result();

    // Status indicator at top (流水线顺序: 基本面 → L2)
    const auto &fstate = fundamental_svc_->get_state();
    ImGui::Text("Fundamental: ");
    ImGui::SameLine();
    switch (fstate.status) {
    case FundamentalStatus::Ready:
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "[Ready]");
      break;
    case FundamentalStatus::Updating:
    case FundamentalStatus::Building:
      ImGui::TextColored(ImVec4(1.0f, 0.95f, 0.3f, 1.0f), "[%s]",
                         GetFundamentalStatusName(fstate.status));
      break;
    case FundamentalStatus::Error:
      ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.0f, 1.0f), "[Error]");
      ImGui::SameLine();
      ImGui::TextDisabled("%s", fstate.message.c_str());
      break;
    case FundamentalStatus::Idle:
      ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "[Idle]");
      break;
    }

    ImGui::SameLine();
    ImGui::TextDisabled("|");
    ImGui::SameLine();
    ImGui::Text("L2 Database: ");
    ImGui::SameLine();

    // L2 database coverage check (required_dates = 基本面交易日历)
    switch (check_result.status) {
    case DatabaseStatus::Unchecked:
      ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "[Not checked]");
      ImGui::SameLine();
      ImGui::TextDisabled("(scans after fundamental sync)");
      break;

    case DatabaseStatus::Pass:
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "[Pass]");
      break;

    case DatabaseStatus::Incomplete:
      ImGui::TextColored(ImVec4(1.0f, 0.95f, 0.3f, 1.0f), "[Incomplete]");
      ImGui::SameLine();
      ImGui::TextDisabled("(Missing %zu dates, %zu can encode)",
                          check_result.missing_dates.size(),
                          check_result.missing_can_encode.size());
      break;

    case DatabaseStatus::NeedArchive:
      ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.0f, 1.0f), "[NeedArchive]");
      ImGui::SameLine();
      ImGui::TextDisabled("(Missing %zu dates without archive)",
                          check_result.missing_no_archive.size());
      break;

    case DatabaseStatus::NotEncoded:
      ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "[NotEncoded]");
      ImGui::SameLine();
      ImGui::TextDisabled("(Archive available, need to encode)");
      break;

    case DatabaseStatus::NoData:
    case DatabaseStatus::Error:
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "[ERROR]");
      ImGui::SameLine();
      ImGui::TextDisabled("%s", check_result.error_message.c_str());
      break;
    }

    ImGui::Separator();

    // TabBar structure (流水线顺序: Overview 基本面 → Encode L2 → Table/Browser)
    if (ImGui::BeginTabBar("DatabaseTabs", ImGuiTabBarFlags_None)) {
      // Get tab access control (managed centrally)
      const auto &tabs = state.tabs;

      // Overview tab (基本面面板) - 流水线第一步, 永远可进
      if (ImGui::BeginTabItem("Overview")) {
        DrawTabOverview();
        ImGui::EndTabItem();
      }

      // Encode tab - 基本面 Ready 后解锁 (覆盖检查依赖交易日历)
      ImGui::BeginDisabled(!tabs.can_access_encode);
      if (ImGui::BeginTabItem("Encode")) {
        if (tabs.can_access_encode)
          DrawTabEncode();
        ImGui::EndTabItem();
      }
      ImGui::EndDisabled();

      if (!tabs.can_access_encode && ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
        ImGui::SetTooltip("Sync fundamental data in Overview tab first");
      }

      // Table tab - 基本面 Ready 且已扫描过一遍 (不要求 L2 覆盖 Pass)
      ImGui::BeginDisabled(!tabs.can_access_table);
      if (ImGui::BeginTabItem("Table")) {
        if (tabs.can_access_table)
          DrawTabTable();
        ImGui::EndTabItem();
      }
      ImGui::EndDisabled();

      if (!tabs.can_access_table && ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
        ImGui::SetTooltip("Requires fundamental data ready and at least one coverage scan (Encode tab)");
      }

      // Browser tab - 基本面 Ready 且已扫描过一遍 (不要求 L2 覆盖 Pass, Browser 本身就是来看覆盖缺口的)
      ImGui::BeginDisabled(!tabs.can_access_browser);
      if (ImGui::BeginTabItem("Browser")) {
        if (tabs.can_access_browser)
          DrawTabBrowser();
        ImGui::EndTabItem();
      }
      ImGui::EndDisabled();

      if (!tabs.can_access_browser && ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
        ImGui::SetTooltip("Requires fundamental data ready and at least one coverage scan (Encode tab)");
      }

      ImGui::EndTabBar();
    }
  }

  void DrawTabOverview() {
    // Safety: check if services are initialized
    if (!fundamental_svc_ || !l2_svc_) {
      ImGui::TextDisabled("Services initializing...");
      return;
    }

    bool update_clicked = false;
    bool refresh_scan = false;

    auto &ts = data_->taskstate.database;
    bool busy = ts.json_update_inflight || fundamental_svc_->is_busy();
    bool scan_busy = ts.l2_scan_inflight;

    RenderTabOverview(
        fundamental_svc_->get_state(),
        &update_clicked, &refresh_scan,
        busy,
        scan_busy);

    // Handle button events
    if (update_clicked && !busy) {
      TriggerRefreshFlow();
    }

    if (refresh_scan && !ts.json_update_inflight && !ts.l2_scan_inflight) {
      // Assets are already scanned in StateManager::initialize()
      // No need for async operation, just refresh state directly
      state_mgr_->refresh_state();
      UpdateTaskState();
    }
  }

  void DrawTabTable() {
    RenderTabTable(
        data_->asset,
        data_->assetinfo.get_stock_info(),
        table_state_);
  }

  void DrawTabBrowser() {
    // Lazy compute browser statistics on first access
    // Requirements: (1) Binary database scanned (has date_info)
    //               (2) Fundamental data ready (stock_info, stock_days)
    //               (3) Not yet computed (date_stats empty)
    if (data_->asset.date_stats.empty() &&
        data_->asset.binary.scanned &&
        !data_->asset.items.empty() &&
        fundamental_svc_->is_ready()) [[unlikely]] {
      data_->asset.compute_coverage_statistics(
          data_->assetinfo.get_stock_info(),
          data_->assetinfo.get_stock_days(),
          data_->assetinfo.get_suspended());
    }

    RenderTabBrowser(
        data_->assetinfo.get_stock_days(),
        data_->assetinfo.get_stock_factor(),
        data_->assetinfo.get_stock_info(),
        data_->asset,
        config_->start_date,
        config_->end_date,
        browser_state_);
  }

  void DrawTabEncode() {
    if (!encoding_svc_ || !scan_svc_ || !data_) {
      ImGui::TextDisabled("Services not initialized...");
      return;
    }
    RenderTabEncode(encoding_svc_.get(), scan_svc_.get(), encode_state_, data_->asset);
  }
};

} // namespace

TaskHandle CreateDatabaseTask() {
  auto instance = std::make_shared<DatabaseTask>();

  TaskHandle handle;
  handle.name = instance->GetName();
  handle.task_instance = instance.get();
  handle.storage = instance;
  handle.Init = [instance](SharedData &data) { instance->Init(data); };
  handle.OnExpand = [instance]() { instance->OnExpand(); };
  handle.OnCollapse = [instance]() { instance->OnCollapse(); };
  handle.DrawPanel = [instance](SharedData &data) {
    instance->DrawPanel(data);
  };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
