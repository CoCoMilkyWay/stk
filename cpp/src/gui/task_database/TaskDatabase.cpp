#include "gui/task_database/TaskDatabase.hpp"
#include "gui/Tasks.hpp"
#include "gui/coro/CoroManager.hpp"
#include "gui/task_database/services/BaostockService.hpp"
#include "gui/task_database/services/L2DatabaseService.hpp"
#include "gui/task_database/services/StateManager.hpp"
#include "gui/task_database/ui/TabBrowser.hpp"
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

namespace GUI::Tasks {
namespace {

using namespace GUI::Database;

class DatabaseTask {
private:
  // Service layer
  std::unique_ptr<BaostockService> baostock_svc_;
  std::unique_ptr<L2DatabaseService> l2_svc_;
  std::unique_ptr<StateManager> state_mgr_;

  // UI state
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
  Config *config_ = nullptr; // Pointer to config for accessing backtest dates
  std::string config_dir_ = "../../config";

public:
  DatabaseTask() = default;

  const char *GetName() const {
    return "Database";
  }

  const char *GetStatus() const {
    if (!state_mgr_)
      return "";
    return state_mgr_->get_state().get_overall_status();
  }

  void OnExpand() {
    is_expanded_ = true;
  }

  void OnCollapse() {
    is_expanded_ = false;
  }

  void DrawPanel(SharedData &data, GuiState &gui_state) {
    if (!coro_mgr_) {
      coro_mgr_ = gui_state.coro_mgr;
      database_dir_ = data.config.database_dir;
      config_ = &data.config;
    }

    // Initialize services on first draw (non-blocking)
    if (!initialized_) {
      InitializeServices(data, gui_state);
      initialized_ = true;
    }

    // Always render UI - show initialization progress if not ready
    RenderUI();
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

          // Step 1: Scan L2 database first to get asset list and date range
          co_await l2_svc_->scan_database();
          state_mgr_->refresh_state();

          // Step 2: Extract stock codes and database start date from L2 scan results
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
            co_await baostock_svc_->get_data_manager()->save_config(config_->config_dir + "/" + config_->baostock_data_manager_config);
          }

          // Step 4: Update all JSON files based on L2 assets (use L2 start date from config)
          co_await baostock_svc_->update_all(l2_start_date);
          // refresh_state() is called inside update_all()

          // Note: L2 summary will be calculated on next frame when JSONs are ready
        }(),
        boost::asio::detached);
  }

  void InitializeServices(SharedData &data, GuiState &gui_state) {
    auto &io = coro_mgr_->GetIoContext();

    // Create services
    baostock_svc_ = std::make_unique<BaostockService>(io, &data.config, gui_state.terminal);
    l2_svc_ = std::make_unique<L2DatabaseService>(database_dir_, &data.config);
    state_mgr_ = std::make_unique<StateManager>(baostock_svc_.get(), l2_svc_.get());

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

    const auto &state = state_mgr_->get_state();

    // Status indicator at top
    ImGui::Text("Status: ");
    ImGui::SameLine();
    if (state.all_json_ready()) {
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "[Ready]");
    } else {
      ImGui::TextColored(ImVec4(1.0f, 0.95f, 0.3f, 1.0f), "[Incomplete]");
      ImGui::SameLine();
      ImGui::TextDisabled("(Update data in Overview tab)");
    }

    ImGui::Separator();

    // TabBar structure
    if (ImGui::BeginTabBar("DatabaseTabs", ImGuiTabBarFlags_None)) {
      // Overview tab - always accessible
      bool overview_tab_is_open = ImGui::BeginTabItem("Overview");
      if (overview_tab_is_open) {
        // Trigger refresh flow ONLY ONCE when first opening Overview tab
        if (!refresh_flow_triggered_ && !overview_tab_was_open_ && baostock_svc_ && l2_svc_ && !json_update_inflight_) {
          TriggerRefreshFlow();
          refresh_flow_triggered_ = true;
        }

        DrawTabOverview();
        ImGui::EndTabItem();
      }
      overview_tab_was_open_ = overview_tab_is_open;

      // Table/Browser tabs - gated by data readiness
      bool can_access = state_mgr_->can_access_table_tab();

      ImGui::BeginDisabled(!can_access);
      if (ImGui::BeginTabItem("Table")) {
        if (can_access)
          DrawTabTable();
        ImGui::EndTabItem();
      }

      if (ImGui::BeginTabItem("Browser")) {
        if (can_access)
          DrawTabBrowser();
        ImGui::EndTabItem();
      }
      ImGui::EndDisabled();

      // Tooltip for disabled tabs
      if (!can_access && ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
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
    const auto &trading_days = baostock_svc_->get_stock_days_data();

    // Convert start_date and end_date from "YYYY-MM-DD" to "YYYYMMDD"
    if (!backtest_start.empty()) {
      backtest_start.erase(std::remove(backtest_start.begin(), backtest_start.end(), '-'), backtest_start.end());
    }
    if (!backtest_end.empty()) {
      backtest_end.erase(std::remove(backtest_end.begin(), backtest_end.end(), '-'), backtest_end.end());
    }

    // get_summary() internally caches and only computes once
    const auto &l2_summary = l2_svc_->get_summary(backtest_start, backtest_end, trading_days);

    const auto &stock_factor_state = baostock_svc_->get_stock_factor_state();
    const auto &stock_info_state = baostock_svc_->get_stock_info_state();
    const auto &stock_days_state = baostock_svc_->get_stock_days_state();
    const auto &crawler_state = baostock_svc_->get_crawler_state();

    bool json_busy = json_update_inflight_ ||
                     stock_factor_state.status == JsonFileStatus::Updating ||
                     stock_info_state.status == JsonFileStatus::Updating ||
                     stock_days_state.status == JsonFileStatus::Updating ||
                     crawler_state.status == CrawlerStatus::Running;

    bool scan_busy = l2_scan_inflight_ ||
                     l2_svc_->get_status() == L2ScanStatus::Scanning;

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
        l2_summary,
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

            if (!state_mgr_->get_state().all_json_ready() || l2_scan_inflight_)
              co_return;

            l2_scan_inflight_ = true;
            FlagReset scan_reset{l2_scan_inflight_};

            co_await l2_svc_->scan_database();
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
      l2_scan_inflight_ = true;
      boost::asio::co_spawn(
          coro_mgr_->GetIoContext(),
          [this]() -> boost::asio::awaitable<void> {
            struct FlagReset {
              bool &flag;
              ~FlagReset() { flag = false; }
            } reset{l2_scan_inflight_};

            co_await l2_svc_->scan_database();
            state_mgr_->refresh_state();
          }(),
          boost::asio::detached);
    }
  }

  void DrawTabTable() {
    RenderTabTable(
        l2_svc_->get_assets(),
        baostock_svc_->get_stock_info_data(),
        table_state_);
  }

  void DrawTabBrowser() {
    RenderTabBrowser(
        baostock_svc_->get_stock_days_data(),
        browser_state_);
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
  handle.DrawPanel = [instance](SharedData &data, GuiState &gui) {
    instance->DrawPanel(data, gui);
  };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
