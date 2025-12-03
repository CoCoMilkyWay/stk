// TabOrderFlow Implementation - OrderFlow Visualization
// Layout:
//   Top: L0 订单流 (price curve + depth), anchor snaps to valid tick
//   Bottom: L1 K线 (all dates), anchor snaps to day start
// Features:
//   - Even time indexing: global_x = day_n * CAPACITY + local_idx
//   - Sparse plotting: only valid data points, uniform time axis
//   - K-line X-axis: YY/MM/DD at day starts only
//   - OrderFlow X-axis: HH:MM:SS every 15 minutes

#include "gui/task_features/ui/TabOrderFlow.hpp"
#include "gui/task_features/services/DataLoader.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"

#include <algorithm>
#include <cmath>
#include <cstdio>

namespace GUI::Features {

// ============================================================================
// Format Helpers
// ============================================================================

static void FormatTimeHMS(char *buf, size_t size, const TimeHMS &t) {
  std::snprintf(buf, size, "%02d:%02d:%02d", t.hour, t.minute, t.second);
}

static void FormatDate(char *buf, size_t size, const std::string &date) {
  if (date.size() == 8) {
    std::snprintf(buf, size, "%s-%s-%s",
                  date.substr(0, 4).c_str(),
                  date.substr(4, 2).c_str(),
                  date.substr(6, 2).c_str());
  } else {
    std::snprintf(buf, size, "%s", date.c_str());
  }
}

// Format date as YY/MM/DD
static void FormatDateShort(char *buf, size_t size, const std::string &date) {
  if (date.size() == 8) {
    std::snprintf(buf, size, "%s/%s/%s",
                  date.substr(2, 2).c_str(),
                  date.substr(4, 2).c_str(),
                  date.substr(6, 2).c_str());
  } else {
    std::snprintf(buf, size, "%s", date.c_str());
  }
}

// ============================================================================
// Depth Panel
// ============================================================================

static void DrawDepthPanel(const L0Cache::DepthData &depth, const std::string &date, float panel_width) {
  if (!depth.valid) {
    ImGui::TextDisabled("No valid data");
    return;
  }

  char date_buf[16], time_buf[16];
  FormatDate(date_buf, sizeof(date_buf), date);
  FormatTimeHMS(time_buf, sizeof(time_buf), depth.time);
  ImGui::Text("%s", date_buf);
  ImGui::Text("%s", time_buf);
  ImGui::Separator();

  float max_vol = 1.0f;
  for (int i = 0; i < 10; ++i) {
    max_vol = std::max(max_vol, std::abs((*depth.bid_volume)[static_cast<size_t>(i)]));
    max_vol = std::max(max_vol, std::abs((*depth.ask_volume)[static_cast<size_t>(i)]));
  }

  const float bar_height = 14.0f;
  const float bar_max_width = panel_width - 80.0f;

  // Ask side (red)
  for (int i = 4; i >= 0; --i) {
    size_t idx = static_cast<size_t>(i);
    if ((*depth.ask_price)[idx] <= 0)
      continue;
    float ratio = std::abs((*depth.ask_volume)[idx]) / max_vol;
    ImGui::PushStyleColor(ImGuiCol_PlotHistogram, ImVec4(0.8f, 0.3f, 0.3f, 0.8f));
    ImGui::ProgressBar(ratio, ImVec2(bar_max_width, bar_height), "");
    ImGui::PopStyleColor();
    ImGui::SameLine();
    ImGui::Text("%.2f", (*depth.ask_price)[idx]);
  }

  ImGui::Separator();
  ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "MID: %.2f", depth.mid_price);
  ImGui::Separator();

  // Bid side (green)
  for (int i = 0; i < 5; ++i) {
    size_t idx = static_cast<size_t>(i);
    if ((*depth.bid_price)[idx] <= 0)
      continue;
    float ratio = std::abs((*depth.bid_volume)[idx]) / max_vol;
    ImGui::PushStyleColor(ImGuiCol_PlotHistogram, ImVec4(0.3f, 0.8f, 0.3f, 0.8f));
    ImGui::ProgressBar(ratio, ImVec2(bar_max_width, bar_height), "");
    ImGui::PopStyleColor();
    ImGui::SameLine();
    ImGui::Text("%.2f", (*depth.bid_price)[idx]);
  }
}

// ============================================================================
// Main Render Function
// ============================================================================

void RenderTabOrderFlow(DataLoader *loader, SharedData &data) {
  if (!loader) {
    ImGui::TextDisabled("DataLoader not initialized");
    return;
  }

  auto &of = data.orderflow;
  auto &ui = of.ui;
  const size_t num_assets = data.asset.items.size();
  const size_t asset_idx = static_cast<size_t>(ui.selected_asset_idx);

  // ========================================================================
  // Data Loading
  // ========================================================================

  // L1: Synchronous load once on first render
  if (num_assets > 0) {
    loader->EnsureL1Loaded(of, num_assets);
  }

  // Start L0 coroutine if not running
  if (of.l1.loaded && !of.loader.coro_running) {
    loader->StartL0Loader(data.gui.Coro(), of);
    // Request initial L0 load
    if (!ui.l1_anchor_date.empty()) {
      loader->RequestL0Load(of, ui.l1_anchor_date, asset_idx);
    }
  }

  // L0: Request load on anchor/asset change (coroutine handles it)
  if (ui.check_and_update() && !ui.l1_anchor_date.empty()) {
    loader->RequestL0Load(of, ui.l1_anchor_date, asset_idx);
  }

  // ========================================================================
  // Layout
  // ========================================================================
  const float content_height = ImGui::GetContentRegionAvail().y;
  const float content_width = ImGui::GetContentRegionAvail().x;

  const float depth_panel_width = 160.0f;
  const float top_view_height = content_height * 0.55f;
  const float bottom_view_height = content_height * 0.45f - 5.0f;
  const float chart_width = content_width - depth_panel_width - 10.0f;

  // ========================================================================
  // Top Section: L0 Order Flow + Depth
  // ========================================================================
  ImGui::BeginChild("TopSection", ImVec2(0, top_view_height), false);
  ImGui::BeginChild("L0Chart", ImVec2(chart_width, -1), false);

  // Always show plot if data exists, show loading indicator as overlay
  if (of.l0.loaded && !of.l0.plot_x.empty()) {
    if (ImPlot::BeginPlot("##L0Price", ImVec2(-1, -1))) {
      // Get current day's range for axis limits
      size_t day_idx = of.l0.days.empty() ? 0 : of.l0.days[0].day_idx;
      double x_min = static_cast<double>(day_idx * OrderFlowConst::L0_CAPACITY);
      double x_max = static_cast<double>((day_idx + 1) * OrderFlowConst::L0_CAPACITY);

      ImPlot::SetupAxes("Time", "Price", 0, ImPlotAxisFlags_AutoFit);
      ImPlot::SetupAxisLimits(ImAxis_X1, x_min, x_max, ImPlotCond_Always);

      // Setup L0 axis ticks: every 15 minutes (15 * 60 = 900 seconds)
      constexpr size_t TICK_INTERVAL = 15 * 60; // 15 minutes in seconds
      std::vector<double> tick_positions;
      std::vector<const char *> tick_labels;
      std::vector<std::string> tick_label_storage;

      for (size_t t = 0; t < OrderFlowConst::L0_CAPACITY; t += TICK_INTERVAL) {
        tick_positions.push_back(x_min + static_cast<double>(t));
        ClockTime ct = trading_seconds_to_clock(t);
        char buf[16];
        std::snprintf(buf, sizeof(buf), "%02d:%02d", ct.hour, ct.minute);
        tick_label_storage.push_back(buf);
      }
      for (const auto &s : tick_label_storage) {
        tick_labels.push_back(s.c_str());
      }

      if (!tick_positions.empty()) {
        ImPlot::SetupAxisTicks(ImAxis_X1, tick_positions.data(), static_cast<int>(tick_positions.size()),
                               tick_labels.data());
      }

      // Plot sparse data (only valid ticks) on uniform time axis
      ImPlot::PlotLine("Mid Price", of.l0.plot_x.data(), of.l0.plot_y.data(),
                       static_cast<int>(of.l0.plot_x.size()));

      // Anchor line (orange, draggable)
      double anchor_x = ui.l0_anchor_plot_idx < of.l0.plot_x.size()
                            ? of.l0.plot_x[ui.l0_anchor_plot_idx]
                            : x_min;
      if (ImPlot::DragLineX(0, &anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f)) {
        ui.l0_anchor_plot_idx = of.l0.snap_to_valid_plot_idx(anchor_x);
      }

      // Click to set anchor
      if (ImPlot::IsPlotHovered() && ImGui::IsMouseClicked(0)) {
        ImPlotPoint mouse = ImPlot::GetPlotMousePos();
        ui.l0_anchor_plot_idx = of.l0.snap_to_valid_plot_idx(mouse.x);
      }

      // Annotation on anchor
      if (ui.l0_anchor_plot_idx < of.l0.plot_x.size()) {
        auto depth = of.l0.get_depth(ui.l0_anchor_plot_idx);
        if (depth.valid) {
          char time_buf[16];
          FormatTimeHMS(time_buf, sizeof(time_buf), depth.time);
          ImPlot::Annotation(anchor_x, of.l0.plot_y[ui.l0_anchor_plot_idx],
                             ImVec4(1, 0.5f, 0, 1), ImVec2(5, -15), false, "%s", time_buf);
        }
      }

      ImPlot::EndPlot();
    }
  } else if (!of.l0.loaded) {
    ImGui::TextDisabled("Waiting for L1 load...");
  } else {
    ImGui::TextDisabled("No L0 data for this date");
  }

  ImGui::EndChild();

  // Right: Depth panel
  ImGui::SameLine();
  ImGui::BeginChild("DepthPanel", ImVec2(depth_panel_width, -1), true);

  if (of.l0.loaded && ui.l0_anchor_plot_idx < of.l0.plot_x.size()) {
    auto depth = of.l0.get_depth(ui.l0_anchor_plot_idx);
    std::string date = of.l0.get_date(ui.l0_anchor_plot_idx);
    DrawDepthPanel(depth, date, depth_panel_width);
  } else {
    ImGui::TextDisabled("No L0 data");
  }

  ImGui::EndChild();
  ImGui::EndChild(); // TopSection

  // ========================================================================
  // Bottom Section: L1 K-line + Controls
  // ========================================================================
  ImGui::BeginChild("BottomSection", ImVec2(0, bottom_view_height), true);

  // Control row
  ImGui::Text("Asset:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(400);

  // Asset combo with formatted labels: 代码-SZ/SH-汉语名字+(DL/"")
  if (num_assets > 0) {
    const auto &current_asset = data.asset.items[asset_idx];
    const std::string &latest_date = data.asset.all_dates.back();
    const bool current_delisted = current_asset.end_date < latest_date;
    
    char preview_buf[256];
    std::snprintf(preview_buf, sizeof(preview_buf), "%s-%s-%s%s",
                  current_asset.asset_code.c_str(),
                  current_asset.exchange.c_str(),
                  current_asset.asset_name.c_str(),
                  current_delisted ? " (DL)" : "");
    
    if (ImGui::BeginCombo("##asset", preview_buf)) {
      for (size_t i = 0; i < num_assets; ++i) {
        const auto &asset = data.asset.items[i];
        const bool is_delisted = asset.end_date < latest_date;
        
        char label[256];
        std::snprintf(label, sizeof(label), "%s-%s-%s%s",
                      asset.asset_code.c_str(),
                      asset.exchange.c_str(),
                      asset.asset_name.c_str(),
                      is_delisted ? " (DL)" : "");
        
        if (is_delisted) {
          ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(0.5f, 0.5f, 0.5f, 1.0f));
        }
        
        const bool is_selected = (ui.selected_asset_idx == static_cast<int>(i));
        if (ImGui::Selectable(label, is_selected, is_delisted ? ImGuiSelectableFlags_Disabled : 0)) {
          ui.selected_asset_idx = static_cast<int>(i);
          of.l1.build_plot_data(i);
        }
        
        if (is_delisted) {
          ImGui::PopStyleColor();
        }
        
        if (is_selected) {
          ImGui::SetItemDefaultFocus();
        }
      }
      ImGui::EndCombo();
    }
  }

  // Status
  ImGui::SameLine();
  if (of.l1.loaded) {
    ImGui::TextColored(ImVec4(0.3f, 0.8f, 0.3f, 1.0f), "[L1: %zu days]", of.l1.num_days);
  }
  ImGui::SameLine();
  if (!ui.l1_anchor_date.empty()) {
    char date_buf[16];
    FormatDate(date_buf, sizeof(date_buf), ui.l1_anchor_date);
    ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "Anchor: %s", date_buf);
  }
  ImGui::SameLine();
  if (of.loader.l0_load_requested) {
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.0f, 1.0f), "[Loading L0...]");
  } else if (of.l0.loaded) {
    ImGui::TextColored(ImVec4(0.3f, 0.6f, 0.9f, 1.0f), "[L0: %zu valid]", of.l0.total_valid());
  }

  // K-line chart (ALL dates for current asset)
  const float kline_height = ImGui::GetContentRegionAvail().y;

  if (of.l1.loaded && asset_idx < of.l1.plot_data.size()) {
    const auto &pd = of.l1.plot_data[asset_idx];

    // Debug: show current asset data stats
    ImGui::Text("Asset %zu: %zu points, x_range=[%.0f, %.0f]",
                asset_idx, pd.x.size(),
                pd.x.empty() ? 0.0 : pd.x.front(),
                pd.x.empty() ? 0.0 : pd.x.back());

    if (ImPlot::BeginPlot("##KLine", ImVec2(-1, kline_height))) {
      // Setup axis with uniform time scale (all days)
      double x_min = 0;
      double x_max = static_cast<double>(of.l1.num_days * OrderFlowConst::L1_CAPACITY);
      if (x_max <= x_min)
        x_max = x_min + 1; // Guard against empty

      // Lock X axis to full range (linear, time-uniform), Y auto-fits
      ImPlot::SetupAxes("Time Index", "Price", 0, ImPlotAxisFlags_AutoFit);
      ImPlot::SetupAxisLimits(ImAxis_X1, x_min, x_max, ImPlotCond_Always);

      // Setup K-line axis ticks: only at day starts, format YY/MM/DD
      std::vector<double> tick_positions;
      std::vector<const char *> tick_labels;
      std::vector<std::string> tick_label_storage;

      for (size_t d = 0; d < of.l1.num_days; ++d) {
        tick_positions.push_back(static_cast<double>(d * OrderFlowConst::L1_CAPACITY));
        char buf[16];
        FormatDateShort(buf, sizeof(buf), of.l1.dates[d]);
        tick_label_storage.push_back(buf);
      }
      for (const auto &s : tick_label_storage) {
        tick_labels.push_back(s.c_str());
      }

      if (!tick_positions.empty()) {
        ImPlot::SetupAxisTicks(ImAxis_X1, tick_positions.data(), static_cast<int>(tick_positions.size()),
                               tick_labels.data());
      }

      // Plot sparse data (only valid bars) on uniform time axis
      if (!pd.x.empty()) {
        ImPlot::PlotLine("Close", pd.x.data(), pd.y.data(), static_cast<int>(pd.x.size()));

        // Anchor line (orange, snaps to day start)
        double anchor_x = ui.l1_anchor_x;
        if (ImPlot::DragLineX(0, &anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f)) {
          ui.l1_anchor_x = of.l1.snap_to_day_start(anchor_x);
          ui.l1_anchor_date = of.l1.get_date(ui.l1_anchor_x);
        }

        // Click to set anchor
        if (ImPlot::IsPlotHovered() && ImGui::IsMouseClicked(0)) {
          ImPlotPoint mouse = ImPlot::GetPlotMousePos();
          ui.l1_anchor_x = of.l1.snap_to_day_start(mouse.x);
          ui.l1_anchor_date = of.l1.get_date(ui.l1_anchor_x);
        }

        // Annotation on anchor (show date)
        if (!ui.l1_anchor_date.empty()) {
          auto it = std::lower_bound(pd.x.begin(), pd.x.end(), anchor_x);
          double anchor_y = 0;
          if (it != pd.x.end()) {
            size_t idx = static_cast<size_t>(it - pd.x.begin());
            if (idx < pd.y.size())
              anchor_y = pd.y[idx];
          }

          char date_buf[16];
          FormatDateShort(date_buf, sizeof(date_buf), ui.l1_anchor_date);
          ImPlot::Annotation(anchor_x, anchor_y, ImVec4(1, 0.5f, 0, 1), ImVec2(5, -15), false,
                             "%s", date_buf);
        }
      }

      ImPlot::EndPlot();
    }
  } else {
    ImGui::TextDisabled("No L1 data available");
  }

  ImGui::EndChild(); // BottomSection
}

void StopTabOrderFlow(DataLoader *loader, SharedData &data) {
  if (loader && data.orderflow.loader.coro_running) {
    loader->StopL0Loader(data.gui.Coro(), data.orderflow);
  }
}

} // namespace GUI::Features
