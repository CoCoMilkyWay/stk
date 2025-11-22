#include "gui/task_database/TaskDatabase.hpp"
#include "gui/task_database/AssetInfo.hpp"
#include "gui/task_database/CoroScanner.hpp"
#include "gui/Tasks.hpp"
#include "gui/coro/CoroManager.hpp"
#include "shared/GuiState.hpp"
#include "shared/SharedData.hpp"
#include "imgui.h"
#include <memory>
#include <boost/asio/io_context.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/this_coro.hpp>

namespace GUI::Tasks {
namespace {

using namespace GUI::Database;

// Coroutine function implementations (must be defined before use in anonymous namespace with LTO)
asio::awaitable<void> scan_database_coro(DatabaseState &state, const std::string &database_dir) {
  state.scan_status = DatabaseState::ScanStatus::Scanning;
  state.status_message = "Scanning database...";
  
  co_await asio::this_coro::executor;
  
  CoroScanner scanner(state);
  scanner.scan_binary_directory(database_dir);
  
  // If no assets found in database, try fallback to targets.json
  if (state.assets.empty()) {
    scanner.load_targets_json("config/targets.json");
  }
  
  state.scan_status = DatabaseState::ScanStatus::Complete;
  state.status_message = "Scan complete";
  
  co_return;
}

asio::awaitable<void> refresh_metadata_coro(DatabaseState &state, asio::io_context & /*io_ctx*/) {
  state.crawl_status = DatabaseState::CrawlStatus::FetchingSZ;
  state.status_message = "Fetching metadata...";
  
  co_await asio::this_coro::executor;
  
  // Placeholder implementation
  state.crawl_status = DatabaseState::CrawlStatus::Complete;
  state.status_message = "Metadata refresh complete";
  
  co_return;
}

class DatabaseTask {
private:
  DatabaseState state_;
  bool is_expanded_ = false;
  bool scan_triggered_ = false;
  bool metadata_triggered_ = false;
  std::unique_ptr<CoroutineHandle> scanner_handle_;
  std::unique_ptr<CoroutineHandle> crawler_handle_;
  CoroManager *coro_mgr_ = nullptr;
  std::string database_dir_;

public:
  DatabaseTask() = default;

  const char *GetName() const {
    return "Database";
  }

  const char *GetStatus() const {
    if (state_.scan_status == DatabaseState::ScanStatus::Scanning) {
      return "scanning";
    }
    if (state_.crawl_status == DatabaseState::CrawlStatus::FetchingSH ||
        state_.crawl_status == DatabaseState::CrawlStatus::FetchingSZ) {
      return "updating";
    }
    if (!state_.assets.empty()) {
      return "ready";
    }
    return "";
  }

  void OnExpand() {
    is_expanded_ = true;
  }

  void OnCollapse() {
    is_expanded_ = false;
    
    // Cancel ongoing operations
    if (scanner_handle_) {
      scanner_handle_->Cancel();
      scanner_handle_.reset();
    }
    if (crawler_handle_) {
      crawler_handle_->Cancel();
      crawler_handle_.reset();
    }
    
    // Reset triggers for next expand
    scan_triggered_ = false;
    metadata_triggered_ = false;
  }

  void DrawPanel(SharedData &data, GuiState &gui_state) {
    if (!coro_mgr_) {
      coro_mgr_ = gui_state.coro_mgr;
      database_dir_ = data.config.database_dir;
    }
    
    // Auto-trigger scan on expand
    if (!scan_triggered_ && 
        state_.scan_status == DatabaseState::ScanStatus::Idle) {
      StartScan();
      scan_triggered_ = true;
    }
    
    // Auto-trigger metadata update after scan completes
    if (scan_triggered_ && !metadata_triggered_ &&
        state_.scan_status == DatabaseState::ScanStatus::Complete &&
        !state_.assets.empty() &&
        state_.crawl_status == DatabaseState::CrawlStatus::Idle) {
      StartCrawler();
      metadata_triggered_ = true;
    }
    
    RenderUI();
  }

private:
  void StartScan() {
    if (scanner_handle_) return;
    
    scanner_handle_ = coro_mgr_->Spawn(scan_database_coro(state_, database_dir_));
  }

  void StartCrawler() {
    if (crawler_handle_) return;
    
    crawler_handle_ = coro_mgr_->Spawn(refresh_metadata_coro(state_, coro_mgr_->GetIoContext()));
  }

  void RenderUI() {
    RenderStatsPanel();
    ImGui::Spacing();
    RenderFilterBar();
    ImGui::Spacing();
    
    // Split view: Asset Table (left) | Detail Panel (right)
    ImGui::BeginGroup();
    {
      float available_width = ImGui::GetContentRegionAvail().x;
      float available_height = ImGui::GetContentRegionAvail().y - 30.0f; // Reserve space for status bar
      
      // Asset table (60% width)
      ImGui::BeginChild("AssetTable", ImVec2(available_width * 0.6f, available_height), true);
      RenderAssetTable();
      ImGui::EndChild();
      
      ImGui::SameLine();
      
      // Detail panel (40% width)
      ImGui::BeginChild("DetailPanel", ImVec2(0, available_height), true);
      RenderDetailPanel();
      ImGui::EndChild();
    }
    ImGui::EndGroup();
    
    ImGui::Spacing();
    RenderStatusBar();
  }

  void RenderStatsPanel() {
    const ImVec4 COLOR_GREEN = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
    const ImVec4 COLOR_YELLOW = ImVec4(1.0f, 0.95f, 0.3f, 1.0f);
    const ImVec4 COLOR_BLUE = ImVec4(0.3f, 0.7f, 1.0f, 1.0f);
    
    ImGui::BeginGroup();
    
    // Line 1: Assets count
    ImGui::Text("Assets:");
    ImGui::SameLine();
    ImGui::TextColored(COLOR_BLUE, "%zu", state_.total_assets());
    
    ImGui::SameLine(0, 20);
    ImGui::Text("Trading Days:");
    ImGui::SameLine();
    ImGui::TextColored(COLOR_BLUE, "%zu", state_.total_trading_days());
    
    ImGui::SameLine(0, 20);
    ImGui::Text("Encoded:");
    ImGui::SameLine();
    ImGui::TextColored(COLOR_GREEN, "%zu", state_.total_encoded_dates());
    
    ImGui::SameLine(0, 20);
    ImGui::Text("Missing:");
    ImGui::SameLine();
    ImGui::TextColored(COLOR_YELLOW, "%zu", state_.total_missing_dates());
    
    ImGui::SameLine(0, 20);
    ImGui::Text("Orders:");
    ImGui::SameLine();
    ImGui::TextColored(COLOR_BLUE, "%zu", state_.total_orders());
    
    ImGui::SameLine(0, 20);
    ImGui::Text("Disk:");
    ImGui::SameLine();
    ImGui::TextColored(COLOR_BLUE, "%.1f GB", state_.disk_usage_gb);
    
    // Line 2: Action buttons
    if (ImGui::Button("Refresh Scan")) {
      state_.assets.clear();
      state_.all_dates.clear();
      state_.scan_status = DatabaseState::ScanStatus::Idle;
      scan_triggered_ = false;
      metadata_triggered_ = false;
      scanner_handle_.reset();
      crawler_handle_.reset();
      StartScan();
      scan_triggered_ = true;
    }
    
    ImGui::SameLine();
    if (ImGui::Button("Update Metadata")) {
      crawler_handle_.reset();
      metadata_triggered_ = false;
      StartCrawler();
      metadata_triggered_ = true;
    }
    
    ImGui::EndGroup();
  }

  void RenderFilterBar() {
    // Search box
    static char search_buf[256] = "";
    ImGui::SetNextItemWidth(300.0f);
    if (ImGui::InputText("##Search", search_buf, sizeof(search_buf))) {
      state_.search_query = search_buf;
    }
    
    ImGui::SameLine();
    ImGui::Checkbox("Missing Only", &state_.filter_missing_only);
    
    ImGui::SameLine();
    ImGui::Checkbox("SH Only", &state_.filter_sh_only);
    
    ImGui::SameLine();
    ImGui::Checkbox("SZ Only", &state_.filter_sz_only);
    
    // Count shown
    size_t visible_count = GetFilteredAssetCount();
    ImGui::SameLine();
    ImGui::Text("  Showing: %zu / %zu", visible_count, state_.total_assets());
  }

  size_t GetFilteredAssetCount() const {
    size_t count = 0;
    for (const auto &asset : state_.assets) {
      if (!ShouldShowAsset(asset)) continue;
      count++;
    }
    return count;
  }

  bool ShouldShowAsset(const AssetInfo &asset) const {
    if (state_.filter_missing_only && asset.get_missing_count() == 0) {
      return false;
    }
    if (state_.filter_sh_only && asset.exchange != "SH") {
      return false;
    }
    if (state_.filter_sz_only && asset.exchange != "SZ") {
      return false;
    }
    if (!state_.search_query.empty()) {
      std::string query = state_.search_query;
      if (asset.asset_code.find(query) == std::string::npos &&
          asset.metadata.name_cn.find(query) == std::string::npos) {
        return false;
      }
    }
    return true;
  }

  void RenderAssetTable() {
    const ImVec4 COLOR_SH = ImVec4(0.0f, 0.4f, 0.8f, 1.0f);
    const ImVec4 COLOR_SZ = ImVec4(0.0f, 0.6f, 0.5f, 1.0f);
    const ImVec4 COLOR_GREEN = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
    const ImVec4 COLOR_YELLOW = ImVec4(1.0f, 0.95f, 0.3f, 1.0f);
    
    if (ImGui::BeginTable("AssetsTable", 7,
                          ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                          ImGuiTableFlags_Sortable | ImGuiTableFlags_ScrollY |
                          ImGuiTableFlags_Resizable)) {
      
      ImGui::TableSetupColumn("Code", ImGuiTableColumnFlags_WidthFixed, 80.0f);
      ImGui::TableSetupColumn("Name", ImGuiTableColumnFlags_WidthFixed, 150.0f);
      ImGui::TableSetupColumn("Exch", ImGuiTableColumnFlags_WidthFixed, 60.0f);
      ImGui::TableSetupColumn("Encoded", ImGuiTableColumnFlags_WidthFixed, 80.0f);
      ImGui::TableSetupColumn("Missing", ImGuiTableColumnFlags_WidthFixed, 80.0f);
      ImGui::TableSetupColumn("Orders", ImGuiTableColumnFlags_WidthFixed, 100.0f);
      ImGui::TableSetupColumn("%", ImGuiTableColumnFlags_WidthFixed, 60.0f);
      ImGui::TableSetupScrollFreeze(0, 1);
      ImGui::TableHeadersRow();
      
      int row_idx = 0;
      for (auto &asset : state_.assets) {
        if (!ShouldShowAsset(asset)) continue;
        
        ImGui::TableNextRow();
        
        bool is_selected = (state_.selected_asset_idx == row_idx);
        ImGui::PushID(row_idx);
        
        // Code
        ImGui::TableSetColumnIndex(0);
        if (ImGui::Selectable(asset.asset_code.c_str(), is_selected,
                              ImGuiSelectableFlags_SpanAllColumns)) {
          state_.selected_asset_idx = row_idx;
        }
        
        // Name
        ImGui::TableSetColumnIndex(1);
        ImGui::Text("%s", asset.get_display_name().c_str());
        
        // Exchange
        ImGui::TableSetColumnIndex(2);
        ImGui::TextColored(asset.exchange == "SH" ? COLOR_SH : COLOR_SZ,
                          "%s", asset.exchange.c_str());
        
        // Encoded
        ImGui::TableSetColumnIndex(3);
        ImGui::Text("%zu/%zu", asset.get_encoded_count(), asset.get_total_trading_days());
        
        // Missing
        ImGui::TableSetColumnIndex(4);
        size_t missing = asset.get_missing_count();
        ImGui::TextColored(missing > 0 ? COLOR_YELLOW : COLOR_GREEN,
                          "%zu", missing);
        
        // Orders
        ImGui::TableSetColumnIndex(5);
        size_t orders = asset.get_total_order_count();
        if (orders >= 1000000) {
          ImGui::Text("%.1fM", orders / 1000000.0);
        } else if (orders >= 1000) {
          ImGui::Text("%.1fK", orders / 1000.0);
        } else {
          ImGui::Text("%zu", orders);
        }
        
        // Percentage
        ImGui::TableSetColumnIndex(6);
        float pct = asset.get_total_trading_days() > 0
                       ? 100.0f * asset.get_encoded_count() / asset.get_total_trading_days()
                       : 0.0f;
        ImGui::Text("%.0f%%", pct);
        
        ImGui::PopID();
        row_idx++;
      }
      
      ImGui::EndTable();
    }
  }

  void RenderDetailPanel() {
    if (state_.selected_asset_idx < 0 ||
        state_.selected_asset_idx >= (int)state_.assets.size()) {
      ImGui::TextDisabled("No asset selected");
      return;
    }
    
    const auto &asset = state_.assets[state_.selected_asset_idx];
    
    ImGui::Text("Asset: %s%s", asset.exchange.c_str(), asset.asset_code.c_str());
    ImGui::Separator();
    
    // Basic info
    ImGui::Text("Name: %s", asset.metadata.name_cn.c_str());
    if (!asset.metadata.name_cn_full.empty()) {
      ImGui::Text("Full Name: %s", asset.metadata.name_cn_full.c_str());
    }
    if (!asset.metadata.industry.empty()) {
      ImGui::Text("Industry: %s", asset.metadata.industry.c_str());
    }
    if (!asset.metadata.province.empty()) {
      ImGui::Text("Province: %s", asset.metadata.province.c_str());
    }
    
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
    
    // DB status
    ImGui::Text("Database Status:");
    ImGui::Text("  Total Days: %zu", asset.get_total_trading_days());
    ImGui::Text("  Encoded: %zu", asset.get_encoded_count());
    ImGui::Text("  Missing: %zu", asset.get_missing_count());
    ImGui::Text("  Orders: %zu", asset.get_total_order_count());
    
    float pct = asset.get_total_trading_days() > 0
                   ? (float)asset.get_encoded_count() / asset.get_total_trading_days()
                   : 0.0f;
    ImGui::ProgressBar(pct, ImVec2(-1, 0));
    
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
    
    // Missing dates
    auto missing = asset.get_missing_dates();
    if (!missing.empty()) {
      ImGui::Text("Missing Dates (%zu):", missing.size());
      ImGui::BeginChild("MissingDates", ImVec2(0, 150), true);
      
      size_t show_count = std::min(missing.size(), size_t(100));
      for (size_t i = 0; i < show_count; ++i) {
        ImGui::Text("%s", missing[i].c_str());
      }
      if (missing.size() > 100) {
        ImGui::Text("... and %zu more", missing.size() - 100);
      }
      
      ImGui::EndChild();
    }
    
    ImGui::Spacing();
    
    // Action buttons
    if (ImGui::Button("Encode This Asset")) {
      // TODO: Trigger encoding
    }
  }

  void RenderStatusBar() {
    const ImVec4 COLOR_BLUE = ImVec4(0.3f, 0.7f, 1.0f, 1.0f);
    const ImVec4 COLOR_GREEN = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
    
    ImGui::BeginGroup();
    
    ImGui::Text("Scan:");
    ImGui::SameLine();
    if (state_.scan_status == DatabaseState::ScanStatus::Idle) {
      ImGui::Text("Idle");
    } else if (state_.scan_status == DatabaseState::ScanStatus::Scanning) {
      ImGui::TextColored(COLOR_BLUE, "Scanning...");
    } else if (state_.scan_status == DatabaseState::ScanStatus::Complete) {
      ImGui::TextColored(COLOR_GREEN, "Complete");
    } else {
      ImGui::TextColored(ImVec4(1, 0, 0, 1), "Error");
    }
    
    ImGui::SameLine(0, 20);
    ImGui::Text("Crawler:");
    ImGui::SameLine();
    if (state_.crawl_status == DatabaseState::CrawlStatus::Idle) {
      ImGui::Text("Idle");
    } else if (state_.crawl_status == DatabaseState::CrawlStatus::FetchingSZ) {
      ImGui::TextColored(COLOR_BLUE, "Fetching SZ %d/%d",
                        state_.crawled_count, state_.total_to_crawl);
    } else if (state_.crawl_status == DatabaseState::CrawlStatus::FetchingSH) {
      ImGui::TextColored(COLOR_BLUE, "Fetching SH %d/%d",
                        state_.crawled_count, state_.total_to_crawl);
    } else if (state_.crawl_status == DatabaseState::CrawlStatus::Complete) {
      ImGui::TextColored(COLOR_GREEN, "Complete");
    } else {
      ImGui::TextColored(ImVec4(1, 0, 0, 1), "Error");
    }
    
    if (!state_.status_message.empty()) {
      ImGui::SameLine(0, 20);
      ImGui::TextDisabled("| %s", state_.status_message.c_str());
    }
    
    ImGui::EndGroup();
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

