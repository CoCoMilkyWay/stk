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
  bool overview_tab_was_open_ = false; // Track if Overview tab was open last frame
  bool json_update_inflight_ = false;  // Prevent concurrent JSON update flows
  bool l2_scan_inflight_ = false;      // Prevent overlapping L2 scans

  // Lifecycle
  bool is_expanded_ = false;
  bool initialized_ = false;
  CoroManager *coro_mgr_ = nullptr;
  std::string database_dir_;
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
  // Trigger unified refresh flow: update + scan
  void TriggerRefreshFlow() {
    if (json_update_inflight_ || !baostock_svc_ || !l2_svc_)
      return;

    auto &io = coro_mgr_->GetIoContext();
    json_update_inflight_ = true;

    boost::asio::co_spawn(
        io,
        [this]() -> boost::asio::awaitable<void> {
          struct FlagReset {
            bool &flag;
            ~FlagReset() { flag = false; }
          };

          {
            FlagReset update_reset{json_update_inflight_};
            co_await baostock_svc_->update_all(false);
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

  void InitializeServices(SharedData &data, GuiState &gui_state) {
    auto &io = coro_mgr_->GetIoContext();

    // Create services
    baostock_svc_ = std::make_unique<BaostockService>(io, &data.config, gui_state.terminal);
    l2_svc_ = std::make_unique<L2DatabaseService>(database_dir_);
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
        // Trigger refresh flow when user switches to Overview tab
        if (!overview_tab_was_open_ && baostock_svc_ && l2_svc_ && !json_update_inflight_) {
          TriggerRefreshFlow();
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

    // Safe to call - returns empty summary if not scanned yet
    auto l2_summary = l2_svc_->get_summary();

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

    RenderTabOverview(
        stock_factor_state,
        stock_info_state,
        stock_days_state,
        crawler_state,
        &stock_factor_update, &stock_factor_remove, &stock_factor_view,
        &stock_info_update, &stock_info_remove, &stock_info_view,
        &stock_days_update, &stock_days_remove, &stock_days_view,
        &update_all, &refresh_scan,
        l2_summary.total_assets,
        l2_summary.encoded_assets,
        l2_summary.missing_assets,
        l2_summary.coverage_percent,
        l2_summary.disk_usage_gb,
        json_busy,
        scan_busy);

    // Handle button events
    if (update_all && !json_update_inflight_) {
      json_update_inflight_ = true;
      boost::asio::co_spawn(
          coro_mgr_->GetIoContext(),
          [this]() -> boost::asio::awaitable<void> {
            struct FlagReset {
              bool &flag;
              ~FlagReset() { flag = false; }
            };

            {
              FlagReset update_reset{json_update_inflight_};
              co_await baostock_svc_->update_all(false);
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

            co_await baostock_svc_->update_stock_factor(true);
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

            co_await baostock_svc_->update_stock_info(true);
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

            co_await baostock_svc_->update_stock_days(true);
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
