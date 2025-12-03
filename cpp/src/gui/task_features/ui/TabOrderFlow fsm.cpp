// TabOrderFlow Implementation - OrderFlow Visualization
// Architecture:
//   1. Config: All constants and layout parameters
//   2. State: Data loading state machine
//   3. Formatters: Reusable formatting utilities
//   4. TickGenerator: Static tick label generators
//   5. PlotRenderer: Unified plot rendering logic
//   6. InteractionHandler: Mouse interaction abstraction
//   7. Main: Orchestration

#include "gui/task_features/ui/TabOrderFlow.hpp"
#include "gui/task_features/services/DataLoader.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h"

#include <algorithm>
#include <cmath>
#include <cstdio>

namespace GUI::Features {

// ============================================================================
// 1. Configuration & Constants
// ============================================================================

namespace Config {
  // Layout
  static constexpr float DEPTH_PANEL_WIDTH = 160.0f;
  static constexpr float TOP_VIEW_RATIO = 0.55f;
  static constexpr float SPACING = 10.0f;
  
  // Candlestick
  static constexpr double CANDLE_HALF_WIDTH = 0.3;
  static constexpr ImU32 COLOR_BULL = IM_COL32(0, 200, 0, 255);
  static constexpr ImU32 COLOR_BEAR = IM_COL32(200, 0, 0, 255);
  static constexpr ImU32 COLOR_LEGEND = IM_COL32(128, 128, 128, 255);
  
  // Anchor
  static constexpr ImVec4 COLOR_ANCHOR = ImVec4(1, 0.5f, 0, 1);
  static constexpr float ANCHOR_THICKNESS = 2.0f;
  static constexpr ImVec2 ANNOTATION_OFFSET = ImVec2(5, -15);
  
  // L0 Ticks
  static constexpr size_t L0_TICK_INTERVAL = 15 * 60; // 15 minutes in seconds
  
  // Status colors
  static constexpr ImVec4 COLOR_STATUS_INFO = ImVec4(0.3f, 0.8f, 0.3f, 1.0f);
  static constexpr ImVec4 COLOR_STATUS_WARN = ImVec4(1.0f, 0.8f, 0.0f, 1.0f);
  static constexpr ImVec4 COLOR_STATUS_ANCHOR = ImVec4(1.0f, 0.5f, 0.0f, 1.0f);
  static constexpr ImVec4 COLOR_STATUS_L0 = ImVec4(0.3f, 0.6f, 0.9f, 1.0f);
  static constexpr ImVec4 COLOR_DELISTED = ImVec4(0.5f, 0.5f, 0.5f, 1.0f);
  
  // Depth panel
  static constexpr float DEPTH_BAR_HEIGHT = 14.0f;
  static constexpr float DEPTH_BAR_MARGIN = 80.0f;
  static constexpr ImVec4 DEPTH_ASK_COLOR = ImVec4(0.8f, 0.3f, 0.3f, 0.8f);
  static constexpr ImVec4 DEPTH_BID_COLOR = ImVec4(0.3f, 0.8f, 0.3f, 0.8f);
  static constexpr ImVec4 DEPTH_MID_COLOR = ImVec4(1.0f, 1.0f, 0.0f, 1.0f);
}

// ============================================================================
// 2. Formatters - Reusable String Formatting
// ============================================================================

namespace Formatter {
  inline void TimeHMS(char *buf, size_t size, const TimeHMS &t) {
    std::snprintf(buf, size, "%02d:%02d:%02d", t.hour, t.minute, t.second);
  }
  
  inline void DateFull(char *buf, size_t size, const std::string &date) {
    if (date.size() == 8) {
      std::snprintf(buf, size, "%s-%s-%s",
                    date.substr(0, 4).c_str(),
                    date.substr(4, 2).c_str(),
                    date.substr(6, 2).c_str());
    } else {
      std::snprintf(buf, size, "%s", date.c_str());
    }
  }
  
  inline void DateShort(char *buf, size_t size, const std::string &date) {
    if (date.size() == 8) {
      std::snprintf(buf, size, "%s/%s/%s",
                    date.substr(2, 2).c_str(),
                    date.substr(4, 2).c_str(),
                    date.substr(6, 2).c_str());
    } else {
      std::snprintf(buf, size, "%s", date.c_str());
    }
  }
  
  inline void AssetLabel(char *buf, size_t size, const AssetItem &asset, bool is_delisted) {
    std::snprintf(buf, size, "%s-%s-%s%s",
                  asset.asset_code.c_str(),
                  asset.exchange.c_str(),
                  asset.asset_name.c_str(),
                  is_delisted ? " (DL)" : "");
  }
}

// ============================================================================
// 3. Tick Generator - Static Cached Tick Labels
// ============================================================================

class TickGenerator {
public:
  // L0: Generate time ticks (HH:MM every 15 minutes)
  struct L0Ticks {
    std::vector<double> positions;
    std::vector<const char*> labels;
    
    void Generate(double x_min) {
      static const auto offsets = []() {
        std::vector<double> v;
        v.reserve(OrderFlowConst::L0_CAPACITY / Config::L0_TICK_INTERVAL + 1);
        for (size_t t = 0; t < OrderFlowConst::L0_CAPACITY; t += Config::L0_TICK_INTERVAL) {
          v.push_back(static_cast<double>(t));
        }
        return v;
      }();
      
      static const auto label_ptrs = []() {
        static std::vector<std::string> storage;
        storage.reserve(OrderFlowConst::L0_CAPACITY / Config::L0_TICK_INTERVAL + 1);
        for (size_t t = 0; t < OrderFlowConst::L0_CAPACITY; t += Config::L0_TICK_INTERVAL) {
          ClockTime ct = trading_seconds_to_clock(t);
          char buf[16];
          std::snprintf(buf, sizeof(buf), "%02d:%02d", ct.hour, ct.minute);
          storage.push_back(buf);
        }
        
        std::vector<const char*> ptrs;
        ptrs.reserve(storage.size());
        for (const auto &s : storage) {
          ptrs.push_back(s.c_str());
        }
        return ptrs;
      }();
      
      positions.clear();
      positions.reserve(offsets.size());
      for (double offset : offsets) {
        positions.push_back(x_min + offset);
      }
      
      labels = label_ptrs;
    }
  };
  
  // L1: Generate date ticks (YY/MM/DD at day starts)
  struct L1Ticks {
    std::vector<double> positions;
    std::vector<const char*> labels;
    
    void Update(const std::vector<std::string> &dates) {
      static std::vector<std::string> storage;
      static std::vector<const char*> label_ptrs;
      static size_t cached_count = 0;
      
      if (cached_count != dates.size()) {
        storage.clear();
        storage.reserve(dates.size());
        positions.clear();
        positions.reserve(dates.size());
        
        for (size_t d = 0; d < dates.size(); ++d) {
          positions.push_back(static_cast<double>(d * OrderFlowConst::L1_CAPACITY));
          char buf[16];
          Formatter::DateShort(buf, sizeof(buf), dates[d]);
          storage.push_back(buf);
        }
        
        label_ptrs.clear();
        label_ptrs.reserve(storage.size());
        for (const auto &s : storage) {
          label_ptrs.push_back(s.c_str());
        }
        
        cached_count = dates.size();
      }
      
      labels = label_ptrs;
    }
  };
};

// ============================================================================
// 4. Plot Renderer - Unified Plotting Logic
// ============================================================================

class PlotRenderer {
public:
  // Candlestick plot
  static void Candlestick(const char *label, const double *xs, const double *opens,
                          const double *highs, const double *lows, const double *closes,
                          int count) {
    if (count <= 0) return;
    
    ImDrawList *draw_list = ImPlot::GetPlotDrawList();
    
    if (ImPlot::BeginItem(label)) {
      ImPlot::GetCurrentItem()->Color = Config::COLOR_LEGEND;
      
      if (ImPlot::FitThisFrame()) {
        for (int i = 0; i < count; ++i) {
          ImPlot::FitPoint(ImPlotPoint(xs[i], lows[i]));
          ImPlot::FitPoint(ImPlotPoint(xs[i], highs[i]));
        }
      }
      
      for (int i = 0; i < count; ++i) {
        const double o = opens[i], h = highs[i], l = lows[i], c = closes[i];
        const ImVec2 open_pos = ImPlot::PlotToPixels(xs[i] - Config::CANDLE_HALF_WIDTH, o);
        const ImVec2 close_pos = ImPlot::PlotToPixels(xs[i] + Config::CANDLE_HALF_WIDTH, c);
        const ImVec2 low_pos = ImPlot::PlotToPixels(xs[i], l);
        const ImVec2 high_pos = ImPlot::PlotToPixels(xs[i], h);
        const ImU32 color = c >= o ? Config::COLOR_BULL : Config::COLOR_BEAR;
        
        draw_list->AddLine(low_pos, high_pos, color);
        draw_list->AddRectFilled(open_pos, close_pos, color);
      }
      
      ImPlot::EndItem();
    }
  }
  
  // Setup axes with default limits and optional reset
  static ImPlotCond SetupAxesWithLimits(const char *x_label, const char *y_label,
                                        double x_min, double x_max, double y_min, double y_max,
                                        ImPlotCond default_cond) {
    ImPlot::SetupAxes(x_label, y_label, 0, 0);
    
    const bool reset_view = ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(1);
    const ImPlotCond cond = reset_view ? ImPlotCond_Always : default_cond;
    
    ImPlot::SetupAxisLimits(ImAxis_X1, x_min, x_max, cond);
    ImPlot::SetupAxisLimits(ImAxis_Y1, y_min, y_max, cond);
    
    return cond;
  }
};

// ============================================================================
// 5. Interaction Handler - Mouse & Anchor Control
// ============================================================================

class InteractionHandler {
public:
  // Handle draggable anchor line with snap function
  template<typename SnapFunc>
  static bool DraggableAnchor(double &anchor_x, SnapFunc snap_func) {
    if (ImPlot::DragLineX(0, &anchor_x, Config::COLOR_ANCHOR, Config::ANCHOR_THICKNESS)) {
      anchor_x = snap_func(anchor_x);
      return true;
    }
    return false;
  }
  
  // Handle double-click to set anchor
  template<typename SnapFunc>
  static bool DoubleClickAnchor(double &anchor_x, SnapFunc snap_func) {
    if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0)) {
      ImPlotPoint mouse = ImPlot::GetPlotMousePos();
      anchor_x = snap_func(mouse.x);
      return true;
    }
    return false;
  }
  
  // Show annotation at anchor
  static void ShowAnnotation(double x, double y, const char *text) {
    ImPlot::Annotation(x, y, Config::COLOR_ANCHOR, Config::ANNOTATION_OFFSET, false, "%s", text);
  }
};

// ============================================================================
// 6. Depth Panel Renderer
// ============================================================================

class DepthPanelRenderer {
public:
  static void Render(const L0Cache::DepthData &depth, const std::string &date, float panel_width) {
    if (!depth.valid) {
      ImGui::TextDisabled("No valid data");
      return;
    }
    
    // Header
    char date_buf[16], time_buf[16];
    Formatter::DateFull(date_buf, sizeof(date_buf), date);
    Formatter::TimeHMS(time_buf, sizeof(time_buf), depth.time);
    ImGui::Text("%s", date_buf);
    ImGui::Text("%s", time_buf);
    ImGui::Separator();
    
    // Find max volume for scaling (first 10 levels)
    float max_vol = 1.0f;
    for (int i = 0; i < 10; ++i) {
      max_vol = std::max(max_vol, std::abs((*depth.bid_volume)[static_cast<size_t>(i)]));
      max_vol = std::max(max_vol, std::abs((*depth.ask_volume)[static_cast<size_t>(i)]));
    }
    
    const float bar_max_width = panel_width - Config::DEPTH_BAR_MARGIN;
    
    // Ask side (red, top-down) - show first 5 levels
    for (int i = 4; i >= 0; --i) {
      RenderDepthBar(i, *depth.ask_price, *depth.ask_volume, max_vol, bar_max_width, 
                     Config::DEPTH_ASK_COLOR);
    }
    
    // Mid price
    ImGui::Separator();
    ImGui::TextColored(Config::DEPTH_MID_COLOR, "MID: %.2f", depth.mid_price);
    ImGui::Separator();
    
    // Bid side (green, bottom-up) - show first 5 levels
    for (int i = 0; i < 5; ++i) {
      RenderDepthBar(i, *depth.bid_price, *depth.bid_volume, max_vol, bar_max_width,
                     Config::DEPTH_BID_COLOR);
    }
  }
  
private:
  static void RenderDepthBar(int i, const std::array<float, OrderFlowConst::LOB_DEPTH> &prices, 
                             const std::array<float, OrderFlowConst::LOB_DEPTH> &volumes,
                             float max_vol, float bar_width, ImVec4 color) {
    size_t idx = static_cast<size_t>(i);
    if (prices[idx] <= 0) return;
    
    float ratio = std::abs(volumes[idx]) / max_vol;
    ImGui::PushStyleColor(ImGuiCol_PlotHistogram, color);
    ImGui::ProgressBar(ratio, ImVec2(bar_width, Config::DEPTH_BAR_HEIGHT), "");
    ImGui::PopStyleColor();
    ImGui::SameLine();
    ImGui::Text("%.2f", prices[idx]);
  }
};

// ============================================================================
// 7. Asset Selector
// ============================================================================

class AssetSelector {
public:
  static bool Render(SharedData &data, OrderFlow &of) {
    const size_t num_assets = data.asset.items.size();
    if (num_assets == 0) return false;
    
    auto &ui = of.ui;
    const size_t asset_idx = static_cast<size_t>(ui.selected_asset_idx);
    const std::string &latest_date = data.asset.all_dates.back();
    
    // Preview
    char preview[256];
    const auto &current = data.asset.items[asset_idx];
    const bool current_delisted = current.end_date < latest_date;
    Formatter::AssetLabel(preview, sizeof(preview), current, current_delisted);
    
    bool changed = false;
    
    ImGui::Text("Asset:");
    ImGui::SameLine();
    ImGui::SetNextItemWidth(400);
    
    if (ImGui::BeginCombo("##asset", preview)) {
      for (size_t i = 0; i < num_assets; ++i) {
        const auto &asset = data.asset.items[i];
        const bool is_delisted = asset.end_date < latest_date;
        const bool is_selected = (ui.selected_asset_idx == static_cast<int>(i));
        
        char label[256];
        Formatter::AssetLabel(label, sizeof(label), asset, is_delisted);
        
        if (is_delisted) {
          ImGui::PushStyleColor(ImGuiCol_Text, Config::COLOR_DELISTED);
        }
        
        if (ImGui::Selectable(label, is_selected, is_delisted ? ImGuiSelectableFlags_Disabled : 0)) {
          ui.selected_asset_idx = static_cast<int>(i);
          of.l1.build_plot_data(i);
          changed = true;
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
    
    return changed;
  }
};

// ============================================================================
// 8. Status Bar
// ============================================================================

class StatusBar {
public:
  static void Render(const OrderFlow &of, size_t asset_idx) {
    // L1 status
    if (of.l1.loaded && asset_idx < of.l1.plot_data.size()) {
      const auto &pd = of.l1.plot_data[asset_idx];
      ImGui::TextColored(Config::COLOR_STATUS_INFO, "[L1: %zu days, %zu points]",
                         of.l1.num_days, pd.x.size());
    }
    
    // Anchor date
    ImGui::SameLine();
    if (!of.ui.l1_anchor_date.empty()) {
      char date_buf[16];
      Formatter::DateShort(date_buf, sizeof(date_buf), of.ui.l1_anchor_date);
      ImGui::TextColored(Config::COLOR_STATUS_ANCHOR, "Anchor: %s", date_buf);
    }
    
    // L0 status
    ImGui::SameLine();
    if (of.loader.l0_load_requested) {
      ImGui::TextColored(Config::COLOR_STATUS_WARN, "[Loading L0...]");
    } else if (of.l0.loaded) {
      ImGui::TextColored(Config::COLOR_STATUS_L0, "[L0: %zu valid]", of.l0.total_valid());
    }
  }
};

// ============================================================================
// 9. L0 Plot - Order Flow
// ============================================================================

class L0PlotRenderer {
public:
  static void Render(OrderFlow &of, ImPlotCond default_cond) {
    if (!of.l0.loaded || of.l0.plot_x.empty()) {
      ImGui::TextDisabled(of.l0.loaded ? "No L0 data for this date" : "Waiting for L1 load...");
      return;
    }
    
    auto &ui = of.ui;
    
    if (ImPlot::BeginPlot("##L0Price", ImVec2(-1, -1))) {
      // Compute ranges
      const size_t day_idx = of.l0.days.empty() ? 0 : of.l0.days[0].day_idx;
      const double x_min = static_cast<double>(day_idx * OrderFlowConst::L0_CAPACITY);
      const double x_max = static_cast<double>((day_idx + 1) * OrderFlowConst::L0_CAPACITY);
      const double y_min = of.l0.y_min_with_margin;
      const double y_max = of.l0.y_max_with_margin;
      
      // Setup axes
      PlotRenderer::SetupAxesWithLimits("Time", "Price", x_min, x_max, y_min, y_max, default_cond);
      
      // Setup ticks
      TickGenerator::L0Ticks ticks;
      ticks.Generate(x_min);
      if (!ticks.positions.empty()) {
        ImPlot::SetupAxisTicks(ImAxis_X1, ticks.positions.data(), 
                               static_cast<int>(ticks.positions.size()), ticks.labels.data());
      }
      
      // Plot data
      ImPlot::PlotLine("Mid Price", of.l0.plot_x.data(), of.l0.plot_y.data(),
                       static_cast<int>(of.l0.plot_x.size()));
      
      // Anchor interaction
      double anchor_x = ui.l0_anchor_plot_idx < of.l0.plot_x.size()
                            ? of.l0.plot_x[ui.l0_anchor_plot_idx]
                            : x_min;
      
      if (InteractionHandler::DraggableAnchor(anchor_x, [&](double x) -> double {
            ui.l0_anchor_plot_idx = of.l0.snap_to_valid_plot_idx(x);
            return of.l0.plot_x[ui.l0_anchor_plot_idx];
          })) {
        // Anchor updated
      }
      
      if (InteractionHandler::DoubleClickAnchor(anchor_x, [&](double x) -> double {
            ui.l0_anchor_plot_idx = of.l0.snap_to_valid_plot_idx(x);
            return of.l0.plot_x[ui.l0_anchor_plot_idx];
          })) {
        // Anchor updated
      }
      
      // Annotation
      if (ui.l0_anchor_plot_idx < of.l0.plot_x.size()) {
        auto depth = of.l0.get_depth(ui.l0_anchor_plot_idx);
        if (depth.valid) {
          char time_buf[16];
          Formatter::TimeHMS(time_buf, sizeof(time_buf), depth.time);
          InteractionHandler::ShowAnnotation(anchor_x, of.l0.plot_y[ui.l0_anchor_plot_idx], time_buf);
        }
      }
      
      ImPlot::EndPlot();
    }
  }
};

// ============================================================================
// 10. L1 Plot - K-Line
// ============================================================================

class L1PlotRenderer {
public:
  static void Render(OrderFlow &of, size_t asset_idx, ImPlotCond default_cond, float height) {
    if (!of.l1.loaded || asset_idx >= of.l1.plot_data.size()) {
      ImGui::TextDisabled("No L1 data available");
      return;
    }
    
    auto &ui = of.ui;
    const auto &pd = of.l1.plot_data[asset_idx];
    
    if (ImPlot::BeginPlot("##KLine", ImVec2(-1, height))) {
      // Compute ranges
      const double x_min = 0;
      double x_max = static_cast<double>(of.l1.num_days * OrderFlowConst::L1_CAPACITY);
      if (x_max <= x_min) x_max = x_min + 1;
      const double y_min = pd.y_min;
      const double y_max = pd.y_max;
      
      // Setup axes
      PlotRenderer::SetupAxesWithLimits("Time Index", "Price", x_min, x_max, y_min, y_max, default_cond);
      
      // Setup ticks
      TickGenerator::L1Ticks ticks;
      ticks.Update(of.l1.dates);
      if (!ticks.positions.empty()) {
        ImPlot::SetupAxisTicks(ImAxis_X1, ticks.positions.data(),
                               static_cast<int>(ticks.positions.size()), ticks.labels.data());
      }
      
      // Plot candlestick
      if (!pd.x.empty()) {
        PlotRenderer::Candlestick("OHLC", pd.x.data(), pd.open.data(), pd.high.data(),
                                  pd.low.data(), pd.close.data(), static_cast<int>(pd.x.size()));
        
        // Anchor interaction
        double anchor_x = ui.l1_anchor_x;
        
        if (InteractionHandler::DraggableAnchor(anchor_x, [&](double x) -> double {
              ui.l1_anchor_x = of.l1.snap_to_day_start(x);
              ui.l1_anchor_date = of.l1.get_date(ui.l1_anchor_x);
              return ui.l1_anchor_x;
            })) {
          // Anchor updated
        }
        
        if (InteractionHandler::DoubleClickAnchor(anchor_x, [&](double x) -> double {
              ui.l1_anchor_x = of.l1.snap_to_day_start(x);
              ui.l1_anchor_date = of.l1.get_date(ui.l1_anchor_x);
              return ui.l1_anchor_x;
            })) {
          // Anchor updated
        }
        
        // Annotation
        if (!ui.l1_anchor_date.empty()) {
          auto it = std::lower_bound(pd.x.begin(), pd.x.end(), anchor_x);
          double anchor_y = 0;
          if (it != pd.x.end()) {
            size_t idx = static_cast<size_t>(it - pd.x.begin());
            if (idx < pd.close.size()) anchor_y = pd.close[idx];
          }
          
          char date_buf[16];
          Formatter::DateShort(date_buf, sizeof(date_buf), ui.l1_anchor_date);
          InteractionHandler::ShowAnnotation(anchor_x, anchor_y, date_buf);
        }
      }
      
      ImPlot::EndPlot();
    }
  }
};

// ============================================================================
// 11. Main Orchestration
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
  // State: Data Loading
  // ========================================================================
  
  // L1: Synchronous load once on first render
  if (num_assets > 0) {
    loader->EnsureL1Loaded(of, num_assets);
  }
  
  // L0 coroutine lifecycle
  if (of.l1.loaded && !of.loader.coro_running) {
    loader->StartL0Loader(data.gui.Coro(), of);
    if (!ui.l1_anchor_date.empty()) {
      loader->RequestL0Load(of, ui.l1_anchor_date, asset_idx);
    }
  }
  
  // L0: Request load on parameter change
  const bool params_changed = ui.check_and_update();
  if (params_changed && !ui.l1_anchor_date.empty()) {
    loader->RequestL0Load(of, ui.l1_anchor_date, asset_idx);
  }
  
  // Compute shared axis condition
  const ImPlotCond axis_cond = params_changed ? ImPlotCond_Always : ImPlotCond_Once;
  
  // ========================================================================
  // Layout: Compute dimensions
  // ========================================================================
  
  const float content_height = ImGui::GetContentRegionAvail().y;
  const float content_width = ImGui::GetContentRegionAvail().x;
  const float top_view_height = content_height * Config::TOP_VIEW_RATIO;
  const float bottom_view_height = content_height * (1.0f - Config::TOP_VIEW_RATIO) - 5.0f;
  const float chart_width = content_width - Config::DEPTH_PANEL_WIDTH - Config::SPACING;
  
  // ========================================================================
  // Top Section: L0 Order Flow + Depth
  // ========================================================================
  
  ImGui::BeginChild("TopSection", ImVec2(0, top_view_height), false);
  ImGui::BeginChild("L0Chart", ImVec2(chart_width, -1), false);
  
  L0PlotRenderer::Render(of, axis_cond);
  
  ImGui::EndChild();
  
  // Depth panel (right side)
  ImGui::SameLine();
  ImGui::BeginChild("DepthPanel", ImVec2(Config::DEPTH_PANEL_WIDTH, -1), true);
  
  if (of.l0.loaded && ui.l0_anchor_plot_idx < of.l0.plot_x.size()) {
    auto depth = of.l0.get_depth(ui.l0_anchor_plot_idx);
    std::string date = of.l0.get_date(ui.l0_anchor_plot_idx);
    DepthPanelRenderer::Render(depth, date, Config::DEPTH_PANEL_WIDTH);
  } else {
    ImGui::TextDisabled("No L0 data");
  }
  
  ImGui::EndChild();
  ImGui::EndChild(); // TopSection
  
  // ========================================================================
  // Bottom Section: L1 K-Line + Controls
  // ========================================================================
  
  ImGui::BeginChild("BottomSection", ImVec2(0, bottom_view_height), true);
  
  // Controls: Asset selector + Status
  AssetSelector::Render(data, of);
  ImGui::SameLine();
  StatusBar::Render(of, asset_idx);
  
  // K-Line plot
  const float kline_height = ImGui::GetContentRegionAvail().y;
  L1PlotRenderer::Render(of, asset_idx, axis_cond, kline_height);
  
  ImGui::EndChild(); // BottomSection
}

void StopTabOrderFlow(DataLoader *loader, SharedData &data) {
  if (loader && data.orderflow.loader.coro_running) {
    loader->StopL0Loader(data.gui.Coro(), data.orderflow);
  }
}

} // namespace GUI::Features
