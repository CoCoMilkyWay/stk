// TabOrderFlow Implementation - OrderFlow Visualization
// Architecture:
//   1. State Machine: Unified state detection (params_changed)
//   2. Conditional Rendering: Single axis_cond for all plots
//   3. Static Caching: No redundant computations
//   4. Modular Helpers: Clear separation of concerns

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
// Constants
// ============================================================================

namespace {
  constexpr float DEPTH_PANEL_WIDTH = 160.0f;
  constexpr float TOP_VIEW_RATIO = 0.55f;
  constexpr size_t L0_TICK_INTERVAL = 15 * 60; // 15 minutes
}

// ============================================================================
// Formatters
// ============================================================================

static void FormatTimeHMS(char *buf, size_t size, const TimeHMS &t) {
  std::snprintf(buf, size, "%02d:%02d:%02d", t.hour, t.minute, t.second);
}

static void FormatDateFull(char *buf, size_t size, const std::string &date) {
  if (date.size() == 8) {
    std::snprintf(buf, size, "%s-%s-%s",
                  date.substr(0, 4).c_str(), date.substr(4, 2).c_str(), date.substr(6, 2).c_str());
  } else {
    std::snprintf(buf, size, "%s", date.c_str());
  }
}

static void FormatDateShort(char *buf, size_t size, const std::string &date) {
  if (date.size() == 8) {
    std::snprintf(buf, size, "%s/%s/%s",
                  date.substr(2, 2).c_str(), date.substr(4, 2).c_str(), date.substr(6, 2).c_str());
  } else {
    std::snprintf(buf, size, "%s", date.c_str());
  }
}

// ============================================================================
// Candlestick Renderer
// ============================================================================

static void PlotCandlestick(const char *label_id, const double *xs, const double *opens,
                            const double *highs, const double *lows, const double *closes, int count) {
  if (count <= 0) return;

  ImDrawList *draw_list = ImPlot::GetPlotDrawList();
  // Each bar occupies 1 X-unit (index), no gap between bars
  constexpr double half_width = 0.5;

  if (ImPlot::BeginItem(label_id)) {
    ImPlot::GetCurrentItem()->Color = IM_COL32(128, 128, 128, 255);

    if (ImPlot::FitThisFrame()) {
      for (int i = 0; i < count; ++i) {
        ImPlot::FitPoint(ImPlotPoint(xs[i], lows[i]));
        ImPlot::FitPoint(ImPlotPoint(xs[i], highs[i]));
      }
    }

    for (int i = 0; i < count; ++i) {
      const double o = opens[i], h = highs[i], l = lows[i], c = closes[i];
      const ImVec2 open_pos = ImPlot::PlotToPixels(xs[i] - half_width, o);
      const ImVec2 close_pos = ImPlot::PlotToPixels(xs[i] + half_width, c);
      const ImVec2 low_pos = ImPlot::PlotToPixels(xs[i], l);
      const ImVec2 high_pos = ImPlot::PlotToPixels(xs[i], h);
      const ImU32 color = c >= o ? IM_COL32(0, 200, 0, 255) : IM_COL32(200, 0, 0, 255);

      // Draw wick (high to low line)
      draw_list->AddLine(low_pos, high_pos, color);
      
      // Draw body - ensure minimum visible height when open == close
      ImVec2 body_top = open_pos;
      ImVec2 body_bottom = close_pos;
      
      constexpr float min_body_height = 1.0f;  // At least 1 pixel visible
      if (std::abs(body_bottom.y - body_top.y) < min_body_height) {
        // Draw horizontal line when body is too small
        const float mid_y = (body_top.y + body_bottom.y) * 0.5f;
        body_top.y = mid_y - min_body_height * 0.5f;
        body_bottom.y = mid_y + min_body_height * 0.5f;
      }
      
      draw_list->AddRectFilled(body_top, body_bottom, color);
    }

    ImPlot::EndItem();
  }
}

// ============================================================================
// Depth Panel Renderer
// ============================================================================

static void RenderDepthPanel(const L0Cache::DepthData &depth, const std::string &date, float panel_width) {
  if (!depth.valid) {
    ImGui::TextDisabled("No valid data");
    return;
  }

  char date_buf[16], time_buf[16];
  FormatDateFull(date_buf, sizeof(date_buf), date);
  FormatTimeHMS(time_buf, sizeof(time_buf), depth.time);
  
  // Use minimal font and spacing
  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]);  // Use default small font
  ImGui::PushStyleVar(ImGuiStyleVar_ItemSpacing, ImVec2(0, 0));  // No spacing
  ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(2, 1));  // Minimal padding
  
  ImGui::Text("%s %s", date_buf, time_buf);
  ImGui::Separator();

  const float bar_max_width = panel_width - 70.0f;

  // Ask side (red) - 10 levels, from top (ask10) to bottom (ask1)
  for (int i = 9; i >= 0; --i) {
    float price = (*depth.ask_price)[i];
    float volume = (*depth.ask_volume)[i];  // Volume in lots (1 lot = 100 shares), already negative for ask
    float amount = volume * price * OrderFlowConst::SHARES_PER_LOT;
    
    if (price <= 0) continue;  // Skip invalid levels
    
    float abs_amount = std::abs(amount);
    float ratio = std::min(1.0f, abs_amount / OrderFlowConst::AMOUNT_MAX_VISIBLE);
    
    // Color based on amount sign and magnitude
    ImVec4 bar_color;
    if (amount < 0) {
      // Negative (ask side): red
      float intensity = std::min(1.0f, abs_amount / OrderFlowConst::AMOUNT_MAX_VISIBLE);
      bar_color = ImVec4(0.8f, 0.3f * (1.0f - intensity * 0.5f), 0.3f * (1.0f - intensity * 0.5f), 0.8f);
    } else {
      // Positive (anomaly on ask side): show as yellow warning
      bar_color = ImVec4(0.9f, 0.9f, 0.3f, 0.8f);
    }
    
    ImGui::PushStyleColor(ImGuiCol_PlotHistogram, bar_color);
    ImGui::ProgressBar(ratio, ImVec2(bar_max_width, 8.0f), "");  // Minimal height
    ImGui::PopStyleColor();
    ImGui::SameLine();
    ImGui::Text("%.2f", price);
  }

  // Mid price - no separator to save space
  ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%.2f", depth.mid_price);

  // Bid side (green) - 10 levels, from top (bid1) to bottom (bid10)
  for (int i = 0; i < 10; ++i) {
    float price = (*depth.bid_price)[i];
    float volume = (*depth.bid_volume)[i];  // Volume in lots (1 lot = 100 shares)
    float amount = volume * price * OrderFlowConst::SHARES_PER_LOT;
    
    if (price <= 0) continue;  // Skip invalid levels
    
    float abs_amount = std::abs(amount);
    float ratio = std::min(1.0f, abs_amount / OrderFlowConst::AMOUNT_MAX_VISIBLE);
    
    // Color based on amount sign and magnitude
    ImVec4 bar_color;
    if (amount > 0) {
      // Positive (bid side): green
      float intensity = std::min(1.0f, abs_amount / OrderFlowConst::AMOUNT_MAX_VISIBLE);
      bar_color = ImVec4(0.3f * (1.0f - intensity * 0.5f), 0.8f, 0.3f * (1.0f - intensity * 0.5f), 0.8f);
    } else {
      // Negative (anomaly on bid side): show as yellow warning
      bar_color = ImVec4(0.9f, 0.9f, 0.3f, 0.8f);
    }
    
    ImGui::PushStyleColor(ImGuiCol_PlotHistogram, bar_color);
    ImGui::ProgressBar(ratio, ImVec2(bar_max_width, 8.0f), "");  // Minimal height
    ImGui::PopStyleColor();
    ImGui::SameLine();
    ImGui::Text("%.2f", price);
  }
  
  ImGui::PopStyleVar(2);
  ImGui::PopFont();
}

// ============================================================================
// L0 Plot Renderer
// ============================================================================

// Helper: Map log10(amount) to color intensity [0, 1]
// Amount range: threshold to 10M RMB (log10=7)
// Below threshold: transparent (0), at 10M: full intensity (1)
static float MapAmountToIntensity(float amount, float log_threshold) {
  float abs_amount = std::abs(amount);
  if (abs_amount < std::pow(10.0f, log_threshold)) return 0.0f;  // Below threshold
  
  float log_amount = std::log10(abs_amount);
  float log_max = std::log10(OrderFlowConst::AMOUNT_MAX_VISIBLE);
  
  // Map [threshold, log_max] to [0, 1]
  float normalized = (log_amount - log_threshold) / (log_max - log_threshold);
  return std::min(1.0f, std::max(0.0f, normalized));
}

// Helper: Convert signed amount (int32_t) to color based on sign and intensity
// Positive amount (bid) = green, Negative amount (ask) = red
static ImU32 AmountToColor(int32_t amount_rmb, float log_threshold) {
  float amount = static_cast<float>(amount_rmb);
  float intensity = MapAmountToIntensity(amount, log_threshold);
  if (intensity <= 0.0f) return IM_COL32(0, 0, 0, 0);  // Transparent
  
  uint8_t alpha = static_cast<uint8_t>(intensity * 200 + 55);  // [55, 255]
  
  if (amount_rmb > 0) {
    // Positive amount (bid side): green spectrum
    uint8_t g = static_cast<uint8_t>(100 + intensity * 155);
    uint8_t b = static_cast<uint8_t>(intensity * 100);
    return IM_COL32(0, g, b, alpha);
  } else {
    // Negative amount (ask side): red spectrum
    uint8_t r = static_cast<uint8_t>(150 + intensity * 105);
    uint8_t g = static_cast<uint8_t>(intensity * 50);
    return IM_COL32(r, g, 0, alpha);
  }
}

static void RenderL0Plot(OrderFlow &of, bool force_reset) {
  if (!of.l0.loaded || of.l0.plot_t.empty()) {
    ImGui::TextDisabled(of.l0.loaded ? "No L0 data for this date" : "Waiting for L1 load...");
    return;
  }

  auto &ui = of.ui;

  if (ImPlot::BeginPlot("##L0Price", ImVec2(-1, -1))) {
    const size_t day_idx = of.l0.days.empty() ? 0 : of.l0.days[0].day_idx;
    const double x_min = static_cast<double>(day_idx * OrderFlowConst::L0_CAPACITY);
    const double x_max = static_cast<double>((day_idx + 1) * OrderFlowConst::L0_CAPACITY);

    // Reset view when params changed or on first load
    ImPlotCond cond = force_reset ? ImPlotCond_Always : ImPlotCond_Once;

    ImPlot::SetupAxes("Time", "Price", 0, 0);
    ImPlot::SetupAxisLimits(ImAxis_X1, x_min, x_max, cond);
    ImPlot::SetupAxisLimits(ImAxis_Y1, of.l0.y_min_with_margin, of.l0.y_max_with_margin, cond);

    // Setup L0 ticks (static labels, dynamic positions)
    static const std::vector<double> tick_offsets = []() {
      std::vector<double> v;
      for (size_t t = 0; t < OrderFlowConst::L0_CAPACITY; t += L0_TICK_INTERVAL) {
        v.push_back(static_cast<double>(t));
      }
      return v;
    }();

    static const std::vector<std::string> tick_label_storage = []() {
      std::vector<std::string> labels;
      for (size_t t = 0; t < OrderFlowConst::L0_CAPACITY; t += L0_TICK_INTERVAL) {
        ClockTime ct = trading_seconds_to_clock(t);
        char buf[16];
        std::snprintf(buf, sizeof(buf), "%02d:%02d", ct.hour, ct.minute);
        labels.push_back(buf);
      }
      return labels;
    }();

    std::vector<double> tick_positions;
    std::vector<const char*> tick_labels;
    for (double offset : tick_offsets) {
      tick_positions.push_back(x_min + offset);
    }
    for (const auto &s : tick_label_storage) {
      tick_labels.push_back(s.c_str());
    }

    if (!tick_positions.empty()) {
      ImPlot::SetupAxisTicks(ImAxis_X1, tick_positions.data(),
                             static_cast<int>(tick_positions.size()), tick_labels.data());
    }

    // Render heatmap if enabled (using pre-built merged cache)
    if (ui.show_heatmap && !of.l0.days.empty()) {
      auto &merged_cache = of.l0.heatmap_merged_cache;
      auto &colored_cache = of.l0.heatmap_colored_cache;
      
      // ========================================================================
      // LEVEL 1: Merged cache (already built in DataLoader)
      // ========================================================================
      if (!merged_cache.valid) {
        // Fallback: rebuild if somehow invalid
        of.l0.build_heatmap_merged_cache();
      }
      
      // ========================================================================
      // LEVEL 2: Colored cache (rebuild only when threshold changes)
      // ========================================================================
      bool need_rebuild_colored = !colored_cache.valid || 
                                   colored_cache.cached_threshold != ui.log_amount_threshold ||
                                   colored_cache.cached_data_version != of.l0.data_version;
      
      if (need_rebuild_colored) {
        colored_cache.clear();
        
        // Reserve space for colored rects (aggressive allocation)
        constexpr size_t ESTIMATED_PRICE_LEVELS = 100;
        constexpr size_t ESTIMATED_RECTS_PER_LEVEL = 1000;
        constexpr size_t ESTIMATED_COLORED_RECTS = ESTIMATED_PRICE_LEVELS * ESTIMATED_RECTS_PER_LEVEL;
        
        colored_cache.reserve(ESTIMATED_COLORED_RECTS);
        
        // Convert merged rects to colored rects (unified levels, bid and ask)
        for (const auto &price_level : merged_cache.levels) {
          for (const auto &merged_rect : price_level.rects) {
            ImU32 color = AmountToColor(merged_rect.amount_rmb, ui.log_amount_threshold);
            if (color == IM_COL32(0, 0, 0, 0))  // Skip transparent
              continue;
            
            // Convert tick indices to global X coordinates
            double x1 = static_cast<double>(merged_rect.tick_start);
            double x2 = static_cast<double>(merged_rect.tick_end);
            double y1 = static_cast<double>(merged_rect.price_top);
            double y2 = static_cast<double>(merged_rect.price_bottom);
            
            colored_cache.rects.push_back({x1, y1, x2, y2, color});
          }
        }
        
        colored_cache.cached_threshold = ui.log_amount_threshold;
        colored_cache.cached_data_version = of.l0.data_version;
        colored_cache.valid = true;
      }
      
      // ========================================================================
      // RENDER: Use colored cache (static, no CPU work per frame)
      // ========================================================================
      ImPlot::PushPlotClipRect();
      ImDrawList* draw_list = ImPlot::GetPlotDrawList();
      
      for (const auto &rect : colored_cache.rects) {
        ImVec2 p_min = ImPlot::PlotToPixels(rect.x1, rect.y1);
        ImVec2 p_max = ImPlot::PlotToPixels(rect.x2, rect.y2);
        draw_list->AddRectFilled(p_min, p_max, rect.color);
      }
      
      ImPlot::PopPlotClipRect();
    }

    // Draw best bid and ask lines with fill between
    ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.3f, 0.8f, 0.3f, 0.7f));
    ImPlot::PlotLine("Best Bid", of.l0.plot_t.data(), of.l0.plot_best_bid.data(),
                     static_cast<int>(of.l0.plot_t.size()));
    ImPlot::PopStyleColor();
    
    ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.8f, 0.3f, 0.3f, 0.7f));
    ImPlot::PlotLine("Best Ask", of.l0.plot_t.data(), of.l0.plot_best_ask.data(),
                     static_cast<int>(of.l0.plot_t.size()));
    ImPlot::PopStyleColor();
    
    // Fill between best bid and best ask with solid yellow
    ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(1.0f, 1.0f, 0.0f, 0.6f));
    ImPlot::PlotShaded("Spread", of.l0.plot_t.data(), of.l0.plot_best_bid.data(), 
                       of.l0.plot_best_ask.data(), static_cast<int>(of.l0.plot_t.size()));
    ImPlot::PopStyleColor();

    // Draw mid price with step mode (for sparse data)
    ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(1.0f, 1.0f, 1.0f, 0.9f));
    ImPlot::PlotStairs("Mid Price", of.l0.plot_t.data(), of.l0.plot_mid_price.data(),
                       static_cast<int>(of.l0.plot_t.size()));
    ImPlot::PopStyleColor();

    double anchor_x = ui.l0_anchor_plot_idx < of.l0.plot_t.size()
                          ? of.l0.plot_t[ui.l0_anchor_plot_idx] : x_min;

    if (ImPlot::DragLineX(0, &anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f)) {
      ui.l0_anchor_plot_idx = of.l0.snap_to_valid_plot_idx(anchor_x);
    }

    if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0)) {
      ImPlotPoint mouse = ImPlot::GetPlotMousePos();
      ui.l0_anchor_plot_idx = of.l0.snap_to_valid_plot_idx(mouse.x);
    }

    if (ui.l0_anchor_plot_idx < of.l0.plot_t.size()) {
      auto depth = of.l0.get_depth(ui.l0_anchor_plot_idx);
      if (depth.valid) {
        char time_buf[16];
        FormatTimeHMS(time_buf, sizeof(time_buf), depth.time);
        ImPlot::Annotation(anchor_x, of.l0.plot_mid_price[ui.l0_anchor_plot_idx],
                           ImVec4(1, 0.5f, 0, 1), ImVec2(5, -15), false, "%s", time_buf);
      }
    }

    ImPlot::EndPlot();
  }
}

// ============================================================================
// L1 Plot Renderer
// ============================================================================

static void RenderL1Plot(OrderFlow &of, size_t asset_idx, float height, bool force_reset) {
  if (!of.l1.loaded || asset_idx >= of.l1.plot_data.size()) {
    ImGui::TextDisabled("No L1 data available");
    return;
  }

  auto &ui = of.ui;
  const auto &pd = of.l1.plot_data[asset_idx];

  if (ImPlot::BeginPlot("##KLine", ImVec2(-1, height))) {
    const double x_min = 0;
    double x_max = static_cast<double>(of.l1.num_days * OrderFlowConst::L1_CAPACITY);
    if (x_max <= x_min) x_max = x_min + 1;

    // Reset view when params changed or on first load
    ImPlotCond cond = force_reset ? ImPlotCond_Always : ImPlotCond_Once;

    ImPlot::SetupAxes("Time Index", "Price", 0, 0);
    ImPlot::SetupAxisLimits(ImAxis_X1, x_min, x_max, cond);
    ImPlot::SetupAxisLimits(ImAxis_Y1, pd.y_min, pd.y_max, cond);

    // Setup K-line ticks (cached, rebuild on data change)
    static std::vector<double> tick_positions;
    static std::vector<const char*> tick_labels;
    static std::vector<std::string> tick_label_storage;
    static size_t cached_num_days = 0;

    if (cached_num_days != of.l1.num_days) {
      tick_positions.clear();
      tick_label_storage.clear();

      for (size_t d = 0; d < of.l1.num_days; ++d) {
        tick_positions.push_back(static_cast<double>(d * OrderFlowConst::L1_CAPACITY));
        char buf[16];
        FormatDateShort(buf, sizeof(buf), of.l1.dates[d]);
        tick_label_storage.push_back(buf);
      }

      tick_labels.clear();
      for (const auto &s : tick_label_storage) {
        tick_labels.push_back(s.c_str());
      }

      cached_num_days = of.l1.num_days;
    }

    if (!tick_positions.empty()) {
      ImPlot::SetupAxisTicks(ImAxis_X1, tick_positions.data(),
                             static_cast<int>(tick_positions.size()), tick_labels.data());
    }

    if (!pd.x.empty()) {
      PlotCandlestick("OHLC", pd.x.data(), pd.open.data(), pd.high.data(),
                      pd.low.data(), pd.close.data(), static_cast<int>(pd.x.size()));

      double anchor_x = ui.l1_anchor_x;
      if (ImPlot::DragLineX(0, &anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f)) {
        ui.l1_anchor_x = of.l1.snap_to_day_start(anchor_x);
        ui.l1_anchor_date = of.l1.get_date(ui.l1_anchor_x);
      }

      if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0)) {
        ImPlotPoint mouse = ImPlot::GetPlotMousePos();
        ui.l1_anchor_x = of.l1.snap_to_day_start(mouse.x);
        ui.l1_anchor_date = of.l1.get_date(ui.l1_anchor_x);
      }

      if (!ui.l1_anchor_date.empty()) {
        auto it = std::lower_bound(pd.x.begin(), pd.x.end(), anchor_x);
        double anchor_y = 0;
        if (it != pd.x.end()) {
          size_t idx = static_cast<size_t>(it - pd.x.begin());
          if (idx < pd.close.size()) anchor_y = pd.close[idx];
        }

        char date_buf[16];
        FormatDateShort(date_buf, sizeof(date_buf), ui.l1_anchor_date);
        ImPlot::Annotation(anchor_x, anchor_y, ImVec4(1, 0.5f, 0, 1),
                           ImVec2(5, -15), false, "%s", date_buf);
      }
    }

    ImPlot::EndPlot();
  }
}

// ============================================================================
// Asset Selector
// ============================================================================

static void RenderAssetSelector(SharedData &data, OrderFlow &of) {
  const size_t num_assets = data.asset.items.size();
  if (num_assets == 0) return;

  auto &ui = of.ui;
  const size_t asset_idx = static_cast<size_t>(ui.selected_asset_idx);
  const std::string &latest_date = data.asset.all_dates.back();

  ImGui::Text("Asset:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(200);

  const auto &current_asset = data.asset.items[asset_idx];
  const bool current_delisted = current_asset.end_date < latest_date;

  char preview_buf[256];
  std::snprintf(preview_buf, sizeof(preview_buf), "%s-%s-%s%s",
                current_asset.asset_code.c_str(), current_asset.exchange.c_str(),
                current_asset.asset_name.c_str(), current_delisted ? " (DL)" : "");

  if (ImGui::BeginCombo("##asset", preview_buf)) {
    for (size_t i = 0; i < num_assets; ++i) {
      const auto &asset = data.asset.items[i];
      const bool is_delisted = asset.end_date < latest_date;

      char label[256];
      std::snprintf(label, sizeof(label), "%s-%s-%s%s",
                    asset.asset_code.c_str(), asset.exchange.c_str(),
                    asset.asset_name.c_str(), is_delisted ? " (DL)" : "");

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

// ============================================================================
// Status Bar
// ============================================================================

static void RenderStatusBar(const OrderFlow &of, size_t asset_idx) {
  ImGui::SameLine();
  if (of.l1.loaded && asset_idx < of.l1.plot_data.size()) {
    const auto &pd = of.l1.plot_data[asset_idx];
    ImGui::TextColored(ImVec4(0.3f, 0.8f, 0.3f, 1.0f), "[L1: %zu days, %zu points]",
                       of.l1.num_days, pd.x.size());
    if (ImGui::IsItemHovered()) {
      ImGui::SetTooltip("L1 (Minute-level) uses data_valid only\n"
                        "All displayed bars have valid OHLCV data");
    }
  }
  ImGui::SameLine();
  if (!of.ui.l1_anchor_date.empty()) {
    char date_buf[16];
    FormatDateShort(date_buf, sizeof(date_buf), of.ui.l1_anchor_date);
    ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "Anchor: %s", date_buf);
  }
  ImGui::SameLine();
  if (of.loader.l0_load_requested) {
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.0f, 1.0f), "[Loading L0...]");
  } else if (of.l0.loaded) {
    auto counts = of.l0.count_valid();
    ImGui::TextColored(ImVec4(0.3f, 0.6f, 0.9f, 1.0f), "[L0: %zu/%zu]", counts.depth_valid, counts.data_valid);
    if (ImGui::IsItemHovered()) {
      ImGui::SetTooltip("Depth Valid / Data Valid\n"
                        "Depth Valid: LOB depth buffer complete (bid/ask prices/volumes, mid_price)\n"
                        "Data Valid: Event-driven data present (other tick features)");
    }
  }
}

// ============================================================================
// Heatmap Controls
// ============================================================================

static void RenderHeatmapControls(OrderFlow &of) {
  auto &ui = of.ui;
  
  ImGui::Checkbox("Heatmap", &ui.show_heatmap);
  
  if (ui.show_heatmap) {
    ImGui::SameLine();
    ImGui::SetNextItemWidth(150);
    ImGui::SliderFloat("Threshold", &ui.log_amount_threshold, 3.0f, 7.0f, "%.1f");
    
    if (ImGui::IsItemHovered()) {
      ImGui::SetNextWindowSize(ImVec2(350, 0), ImGuiCond_Always);
      ImGui::BeginTooltip();
      ImGui::Text("Log10(Amount) Lower Threshold");
      ImGui::Separator();
      ImGui::Text("3.0 = 1K RMB (show all levels >= 1K)");
      ImGui::Text("4.0 = 10K RMB");
      ImGui::Text("5.0 = 100K RMB");
      ImGui::Text("6.0 = 1M RMB");
      ImGui::Text("7.0 = 10M RMB (only show thick levels)");
      ImGui::Separator();
      ImGui::TextWrapped("Range: [threshold, 10M] maps to [transparent, full color]");
      ImGui::TextWrapped("Lower threshold: see more thin levels");
      ImGui::TextWrapped("Higher threshold: focus on thick levels only");
      ImGui::EndTooltip();
    }
  }
}

// ============================================================================
// Main Orchestration
// ============================================================================

void RenderTabOrderFlow(DataLoader *loader, SharedData &data) {
  if (!loader) {
    ImGui::TextDisabled("DataLoader not initialized");
    return;
  }

  // Configure input mapping: Left-click for box select, Right-click for pan
  static bool input_map_configured = false;
  if (!input_map_configured) {
    ImPlot::MapInputReverse();
    input_map_configured = true;
  }

  auto &of = data.orderflow;
  auto &ui = of.ui;
  const size_t num_assets = data.asset.items.size();
  const size_t asset_idx = static_cast<size_t>(ui.selected_asset_idx);

  // ==========================================================================
  // Data Loading
  // ==========================================================================

  // L1: Synchronous load once
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

  // Detect parameter changes (date/asset changed)
  const bool params_changed = ui.check_and_update();

  // L0: Request load on parameter change
  if (params_changed && !ui.l1_anchor_date.empty()) {
    loader->RequestL0Load(of, ui.l1_anchor_date, asset_idx);
  }

  // Detect L0 data just finished loading (for auto-zoom)
  static bool prev_l0_loading = false;
  const bool l0_just_loaded = prev_l0_loading && !of.loader.l0_load_requested;
  prev_l0_loading = of.loader.l0_load_requested;

  // Force reset when params changed OR when L0 data just finished loading
  const bool force_reset = params_changed || l0_just_loaded;

  // ==========================================================================
  // LAYOUT: Compute dimensions
  // ==========================================================================

  const float content_height = ImGui::GetContentRegionAvail().y;
  const float content_width = ImGui::GetContentRegionAvail().x;
  const float top_view_height = content_height * TOP_VIEW_RATIO;
  const float bottom_view_height = content_height * (1.0f - TOP_VIEW_RATIO) - 5.0f;
  const float chart_width = content_width - DEPTH_PANEL_WIDTH - 10.0f;

  // ==========================================================================
  // TOP SECTION: L0 Order Flow + Depth
  // ==========================================================================

  ImGui::BeginChild("TopSection", ImVec2(0, top_view_height), false);
  ImGui::BeginChild("L0Chart", ImVec2(chart_width, -1), false);

  RenderL0Plot(of, force_reset);

  ImGui::EndChild();

  ImGui::SameLine();
  ImGui::BeginChild("DepthPanel", ImVec2(DEPTH_PANEL_WIDTH, -1), true);

  if (of.l0.loaded && ui.l0_anchor_plot_idx < of.l0.plot_t.size()) {
    auto depth = of.l0.get_depth(ui.l0_anchor_plot_idx);
    std::string date = of.l0.get_date(ui.l0_anchor_plot_idx);
    RenderDepthPanel(depth, date, DEPTH_PANEL_WIDTH);
  } else {
    ImGui::TextDisabled("No L0 data");
  }

  ImGui::EndChild();
  ImGui::EndChild(); // TopSection

  // ==========================================================================
  // BOTTOM SECTION: L1 K-Line + Controls
  // ==========================================================================

  ImGui::BeginChild("BottomSection", ImVec2(0, bottom_view_height), true);

  // First line: Asset selector + Heatmap controls
  RenderAssetSelector(data, of);
  ImGui::SameLine();
  RenderHeatmapControls(of);
  
  // Second line: Status bar
  RenderStatusBar(of, asset_idx);

  const float kline_height = ImGui::GetContentRegionAvail().y;
  RenderL1Plot(of, asset_idx, kline_height, params_changed);

  ImGui::EndChild(); // BottomSection
}

void StopTabOrderFlow(DataLoader *loader, SharedData &data) {
  if (loader && data.orderflow.loader.coro_running) {
    loader->StopL0Loader(data.gui.Coro(), data.orderflow);
  }
}

} // namespace GUI::Features
