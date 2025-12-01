#include "gui/task_database/TaskDatabase.hpp"
#include "gui/Tasks.hpp"
#include "gui/coro/CoroManager.hpp"
#include "gui/task_database/services/BaostockService.hpp"
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_database/services/L2DatabaseService.hpp"
#include "gui/task_database/services/StateManager.hpp"
#include "gui/task_database/ui/TabBrowser.hpp"
#include "gui/task_database/ui/TabEncode.hpp"
#include "gui/task_database/ui/TabOverview.hpp"
#include "gui/task_database/ui/TabTable.hpp"
#include "imgui.h"
#include "shared/GuiState.hpp"
#include "shared/SharedData.hpp"
#include <algorithm>
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
  std::unique_ptr<BaostockService> baostock_svc_;
  std::unique_ptr<EncodingService> encoding_svc_;
  std::unique_ptr<L2DatabaseService> l2_svc_;
  std::unique_ptr<StateManager> state_mgr_;

  // UI state
  EncodeState encode_state_;
  TableState table_state_;
  BrowserState browser_state_;
  bool overview_tab_was_open_ = false;  // Track if Overview tab was open last frame
  bool refresh_flow_triggered_ = false; // Track if refresh flow has been triggered (only once per session)
  bool json_update_inflight_ = false;   // Prevent concurrent JSON update flows
  bool l2_scan_inflight_ = false;       // Prevent overlapping L2 scans

  // Lifecycle
  bool is_expanded_ = false;
  bool initialized_ = false;
  CoroManager *coro_mgr_ = nullptr;
  std::string database_dir_;
  SharedData *data_ = nullptr; // Pointer to shared data
  Config *config_ = nullptr;   // Pointer to config for accessing backtest dates
  std::string config_dir_ = "../../config";

public:
  DatabaseTask() = default;

  const char *GetName() const {
    return "Database";
  }

  const char *GetStatus() const {
    if (!state_mgr_ || !encoding_svc_)
      return "";

    auto check_result = encoding_svc_->get_last_check_result();
    const auto &state = state_mgr_->get_state();

    // Priority: Error > Incomplete > Ready
    if (check_result.status == DatabaseStatus::Error ||
        check_result.status == DatabaseStatus::NoData ||
        check_result.status == DatabaseStatus::NeedArchive) {
      return "error";
    }
    if (check_result.status != DatabaseStatus::Pass) {
      return "incomplete";
    }
    if (!state.all_json_ready()) {
      return "incomplete";
    }
    return "ready";
  }

  void OnExpand() {
    is_expanded_ = true;
  }

  void OnCollapse() {
    is_expanded_ = false;
  }

  void DrawPanel(SharedData &data) {
    if (!coro_mgr_) {
      coro_mgr_ = &data.gui.Coro();
      database_dir_ = data.config.database_dir;
      data_ = &data;
      config_ = &data.config;
    }

    // Initialize services on first draw (non-blocking)
    if (!initialized_) {
      InitializeServices(data);
      initialized_ = true;
    }

    // Always render UI - show initialization progress if not ready
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
  // Trigger unified refresh flow: scan L2 first, then update JSONs
  void TriggerRefreshFlow() {
    if (json_update_inflight_ || l2_scan_inflight_ || !baostock_svc_ || !l2_svc_)
      return;

    auto &io = coro_mgr_->GetIoContext();
    l2_scan_inflight_ = true;
    json_update_inflight_ = true;

    boost::asio::co_spawn(
        io,
        [this]() -> boost::asio::awaitable<void> {
          struct FlagReset {
            bool &flag;
            ~FlagReset() { flag = false; }
          };

          FlagReset scan_reset{l2_scan_inflight_};
          FlagReset update_reset{json_update_inflight_};

          // Assets are already loaded and scanned in StateManager::initialize()
          // Just refresh state to update UI
          state_mgr_->refresh_state();

          // Step 2: Extract stock codes from shared data (already loaded from assets.json)
          const auto &assets = l2_svc_->get_assets();
          const auto &all_dates = l2_svc_->get_all_dates();
          std::vector<std::string> stock_codes;
          stock_codes.reserve(assets.size());

          for (const auto &asset : assets) {
            // Convert AssetInfo to Baostock format: sh.600000, sz.000001
            std::string exchange_lower = asset.exchange;
            std::transform(exchange_lower.begin(), exchange_lower.end(),
                           exchange_lower.begin(), ::tolower);
            stock_codes.push_back(exchange_lower + "." + asset.asset_code);
          }

          // Get L2 database start date and convert format: YYYYMMDD -> YYYY-MM-DD
          std::string l2_start_date;
          std::string l2_end_date;
          if (!all_dates.empty()) {
            std::string start_yyyymmdd = all_dates.front();
            std::string end_yyyymmdd = all_dates.back();

            if (start_yyyymmdd.length() == 8) {
              l2_start_date = start_yyyymmdd.substr(0, 4) + "-" + start_yyyymmdd.substr(4, 2) + "-" + start_yyyymmdd.substr(6, 2);
            }
            if (end_yyyymmdd.length() == 8) {
              l2_end_date = end_yyyymmdd.substr(0, 4) + "-" + end_yyyymmdd.substr(4, 2) + "-" + end_yyyymmdd.substr(6, 2);
            }

            // Store L2 database date range in DataManager
            if (!start_yyyymmdd.empty() && !end_yyyymmdd.empty()) {
              baostock_svc_->get_data_manager()->set_l2_database_date_range(start_yyyymmdd, end_yyyymmdd);
              // Note: Will be saved together with stock_codes below
            }
          }

          // Step 3: Update DataManager with L2-derived stock codes (this will save config)
          if (!stock_codes.empty()) {
            co_await baostock_svc_->get_data_manager()->set_stock_codes(stock_codes);
          } else {
            // If no stock codes, still need to save L2 date range
            co_await baostock_svc_->get_data_manager()->save_config(config_->config_dir + "/" + config_->baostock_data_manager_file);
          }

          // Step 4: Update all JSON files based on L2 assets (use L2 start date from config)
          co_await baostock_svc_->update_all(l2_start_date);
          // refresh_state() is called inside update_all()

          // Note: L2 summary will be calculated on next frame when JSONs are ready
        }(),
        boost::asio::detached);
  }

  void InitializeServices(SharedData &data) {
    auto &io = coro_mgr_->GetIoContext();

    // Create services
    baostock_svc_ = std::make_unique<BaostockService>(io, &data.config, &data.gui.terminal);
    encoding_svc_ = std::make_unique<EncodingService>(data, io, &data.gui.terminal);
    l2_svc_ = std::make_unique<L2DatabaseService>(data);
    state_mgr_ = std::make_unique<StateManager>(data, baostock_svc_.get(), encoding_svc_.get());

    // Initialize: login workers + load existing JSON
    // Non-blocking, user sees progress in terminal
    boost::asio::co_spawn(
        io,
        state_mgr_->initialize(),
        boost::asio::detached);
  }

  void RenderUI() {
    if (!state_mgr_) {
      ImGui::TextDisabled("Initializing services...");
      return;
    }

    // Refresh state before rendering
    state_mgr_->refresh_state();
    const auto &state = state_mgr_->get_state();

    // Get database check result
    auto check_result = encoding_svc_->get_last_check_result();

    // Status indicator at top
    ImGui::Text("Database Status: ");
    ImGui::SameLine();

    // Primary status: database coverage check
    switch (check_result.status) {
    case DatabaseStatus::Unchecked:
      ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "[Not checked]");
      ImGui::SameLine();
      ImGui::TextDisabled("(Click 'Check Database' in Encode tab)");
      break;

    case DatabaseStatus::Pass:
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "[Pass]");
      if (!state.all_json_ready()) {
        ImGui::SameLine();
        ImGui::TextColored(ImVec4(1.0f, 0.95f, 0.3f, 1.0f), "(JSON Incomplete)");
      }
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

    // TabBar structure
    if (ImGui::BeginTabBar("DatabaseTabs", ImGuiTabBarFlags_None)) {
      // Encode tab - always accessible (first step: create binary database)
      if (ImGui::BeginTabItem("Encode")) {
        DrawTabEncode();
        ImGui::EndTabItem();
      }

      // Get tab access control (managed centrally)
      const auto &tabs = state.tabs;

      // Overview tab - unlocked when database check passes
      ImGui::BeginDisabled(!tabs.can_access_overview);
      bool overview_tab_is_open = ImGui::BeginTabItem("Overview");
      if (overview_tab_is_open && tabs.can_access_overview) {
        // Trigger refresh flow ONLY ONCE when first opening Overview tab
        if (!refresh_flow_triggered_ && !overview_tab_was_open_ && baostock_svc_ && l2_svc_ && !json_update_inflight_) {
          TriggerRefreshFlow();
          refresh_flow_triggered_ = true;
        }
        DrawTabOverview();
        ImGui::EndTabItem();
      } else if (overview_tab_is_open) {
        ImGui::EndTabItem();
      }
      ImGui::EndDisabled();

      if (!tabs.can_access_overview && ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
        ImGui::SetTooltip("Complete encoding in Encode tab first");
      }
      overview_tab_was_open_ = overview_tab_is_open;

      // Table tab - unlocked when all JSON files ready
      ImGui::BeginDisabled(!tabs.can_access_table);
      if (ImGui::BeginTabItem("Table")) {
        if (tabs.can_access_table)
          DrawTabTable();
        ImGui::EndTabItem();
      }
      ImGui::EndDisabled();

      if (!tabs.can_access_table && ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
        ImGui::SetTooltip("Update all JSON files in Overview tab first");
      }

      // Browser tab - unlocked when all JSON files ready
      ImGui::BeginDisabled(!tabs.can_access_browser);
      if (ImGui::BeginTabItem("Browser")) {
        if (tabs.can_access_browser)
          DrawTabBrowser();
        ImGui::EndTabItem();
      }
      ImGui::EndDisabled();

      if (!tabs.can_access_browser && ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
        ImGui::SetTooltip("Update all JSON files in Overview tab first");
      }

      ImGui::EndTabBar();
    }
  }

  void DrawTabOverview() {
    // Safety: check if services are initialized
    if (!baostock_svc_ || !l2_svc_) {
      ImGui::TextDisabled("Services initializing...");
      return;
    }

    bool stock_factor_update = false;
    bool stock_factor_remove = false;
    bool stock_factor_view = false;
    bool stock_info_update = false;
    bool stock_info_remove = false;
    bool stock_info_view = false;
    bool stock_days_update = false;
    bool stock_days_remove = false;
    bool stock_days_view = false;
    bool update_all = false;
    bool refresh_scan = false;

    // Get L2 summary (internally cached, will only compute once when JSONs are ready)
    std::string backtest_start = config_ ? config_->start_date : "";
    std::string backtest_end = config_ ? config_->end_date : "";
    // Trading days data available in baostock service if needed
    // const auto &trading_days = baostock_svc_->get_stock_days_data();

    // Convert start_date and end_date from "YYYY-MM-DD" to "YYYYMMDD"
    if (!backtest_start.empty()) {
      backtest_start.erase(std::remove(backtest_start.begin(), backtest_start.end(), '-'), backtest_start.end());
    }
    if (!backtest_end.empty()) {
      backtest_end.erase(std::remove(backtest_end.begin(), backtest_end.end(), '-'), backtest_end.end());
    }

    const auto &stock_factor_state = baostock_svc_->get_stock_factor_state();
    const auto &stock_info_state = baostock_svc_->get_stock_info_state();
    const auto &stock_days_state = baostock_svc_->get_stock_days_state();
    const auto &crawler_state = baostock_svc_->get_crawler_state();

    bool json_busy = json_update_inflight_ ||
                     stock_factor_state.status == JsonFileStatus::Updating ||
                     stock_info_state.status == JsonFileStatus::Updating ||
                     stock_days_state.status == JsonFileStatus::Updating ||
                     crawler_state.status == CrawlerStatus::Running;

    bool scan_busy = l2_scan_inflight_;

    bool check_integrity = false;

    RenderTabOverview(
        stock_factor_state,
        stock_info_state,
        stock_days_state,
        crawler_state,
        &stock_factor_update, &stock_factor_remove, &stock_factor_view,
        &stock_info_update, &stock_info_remove, &stock_info_view,
        &stock_days_update, &stock_days_remove, &stock_days_view,
        &update_all, &check_integrity, &refresh_scan,
        json_busy,
        scan_busy);

    // Handle button events
    if (update_all && !json_update_inflight_) {
      json_update_inflight_ = true;
      // Don't invalidate cache yet - only invalidate if update actually changes data
      boost::asio::co_spawn(
          coro_mgr_->GetIoContext(),
          [this]() -> boost::asio::awaitable<void> {
            struct FlagReset {
              bool &flag;
              ~FlagReset() { flag = false; }
            };

            {
              FlagReset update_reset{json_update_inflight_};
              // TODO: update_all should return whether data changed
              co_await baostock_svc_->update_all();
              // For now, don't invalidate if no network calls were made
              // The cache is only invalid if actual data changed
              state_mgr_->refresh_state();
            }

            // Assets are already scanned in StateManager::initialize()
            // No need to rescan, just refresh state
            state_mgr_->refresh_state();
          }(),
          boost::asio::detached);
    }

    if (stock_factor_update && !json_update_inflight_) {
      json_update_inflight_ = true;
      boost::asio::co_spawn(
          coro_mgr_->GetIoContext(),
          [this]() -> boost::asio::awaitable<void> {
            struct FlagReset {
              bool &flag;
              ~FlagReset() { flag = false; }
            } reset{json_update_inflight_};

            co_await baostock_svc_->update_stock_factor();
            // Don't invalidate L2 cache - stock_factor doesn't affect L2 binaries
            state_mgr_->refresh_state();
          }(),
          boost::asio::detached);
    }

    if (stock_info_update && !json_update_inflight_) {
      json_update_inflight_ = true;
      boost::asio::co_spawn(
          coro_mgr_->GetIoContext(),
          [this]() -> boost::asio::awaitable<void> {
            struct FlagReset {
              bool &flag;
              ~FlagReset() { flag = false; }
            } reset{json_update_inflight_};

            co_await baostock_svc_->update_stock_info();
            // Don't invalidate L2 cache - stock_info doesn't affect L2 binaries
            state_mgr_->refresh_state();
          }(),
          boost::asio::detached);
    }

    if (stock_days_update && !json_update_inflight_) {
      json_update_inflight_ = true;
      boost::asio::co_spawn(
          coro_mgr_->GetIoContext(),
          [this]() -> boost::asio::awaitable<void> {
            struct FlagReset {
              bool &flag;
              ~FlagReset() { flag = false; }
            } reset{json_update_inflight_};

            co_await baostock_svc_->update_stock_days();
            state_mgr_->refresh_state();
          }(),
          boost::asio::detached);
    }

    if (stock_factor_remove) {
      if (baostock_svc_->force_remove_stock_factor()) {
        state_mgr_->refresh_state();
      }
    }

    if (stock_info_remove) {
      if (baostock_svc_->force_remove_stock_info()) {
        state_mgr_->refresh_state();
      }
    }

    if (stock_days_remove) {
      if (baostock_svc_->force_remove_stock_days()) {
        state_mgr_->refresh_state();
      }
    }

    if (check_integrity) {
      baostock_svc_->check_all_integrity();
      // Don't invalidate L2 cache - integrity check doesn't change data
      state_mgr_->refresh_state();
    }

    if (refresh_scan && !json_update_inflight_ && !l2_scan_inflight_) {
      // Assets are already scanned in StateManager::initialize()
      // No need for async operation, just refresh state directly
      state_mgr_->refresh_state();
    }
  }

  void DrawTabTable() {
    RenderTabTable(
        l2_svc_->get_assets(),
        baostock_svc_->get_stock_info_data(),
        table_state_);
  }

  void DrawTabBrowser() {
    // Lazy compute browser statistics on first access
    // Requirements: (1) Binary database scanned (has date_info)
    //               (2) Baostock data ready (stock_info, stock_days)
    //               (3) Not yet computed (date_stats empty)
    if (data_->asset.date_stats.empty() &&
        data_->asset.binary.scanned &&
        !data_->asset.items.empty() &&
        baostock_svc_->all_ready()) [[unlikely]] {
      data_->asset.compute_browser_statistics(
          baostock_svc_->get_stock_info_data(),
          baostock_svc_->get_stock_days_data());
    }
 
    RenderTabBrowser(
        baostock_svc_->get_stock_days_data(),
        baostock_svc_->get_stock_factor_data(),
        data_->asset,
        config_->start_date,
        config_->end_date,
        browser_state_);
  }

  void DrawTabEncode() {
    if (!encoding_svc_ || !data_) {
      ImGui::TextDisabled("Encoding service not initialized...");
      return;
    }
    RenderTabEncode(encoding_svc_.get(), encode_state_, data_->asset);
  }
};

} // namespace

TaskHandle CreateDatabaseTask() {
  auto instance = std::make_shared<DatabaseTask>();

  TaskHandle handle;
  handle.name = instance->GetName();
  handle.task_instance = instance.get();
  handle.storage = instance;
  handle.GetStatus = [instance]() { return instance->GetStatus(); };
  handle.OnExpand = [instance]() { instance->OnExpand(); };
  handle.OnCollapse = [instance]() { instance->OnCollapse(); };
  handle.DrawPanel = [instance](SharedData &data) {
    instance->DrawPanel(data);
  };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
