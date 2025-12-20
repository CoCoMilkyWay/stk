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
// All parameters moved to OrderFlowConst namespace in OrderFlow.hpp
}

// ============================================================================
// Formatters
// ============================================================================

// Custom formatter converts X coordinate → time index → actual time
// Example: X=900 → tick_idx=900 → index2tick(900) → 09:30
static int L0TimeFormatter(double value, char *buff, int size, void * /*user_data*/) {
  // Extract time index from global_x
  // global_x = day_idx * L0_CAPACITY + tick_idx, where tick_idx is time index
  const size_t global_x = static_cast<size_t>(value);
  const size_t tick_idx = global_x % OrderFlowConst::L0_CAPACITY;

  // Convert time index (0-15299) to ClockTime
  ClockTime ct = index2tick(tick_idx);

  // Format as HH:MM (matching TimeAxisLUT labels)
  return std::snprintf(buff, size, "%02d:%02d", ct.hour, ct.minute);
}

static void FormatTimeHMS(char *buf, size_t size, uint8_t hour, uint8_t minute, uint8_t second) {
  std::snprintf(buf, size, "%02d:%02d:%02d", hour, minute, second);
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
  if (count <= 0)
    return;

  ImDrawList *draw_list = ImPlot::GetPlotDrawList();
  // Each bar occupies 1 X-unit (index), no gap between bars
  constexpr double half_width = OrderFlowConst::CANDLESTICK_HALF_WIDTH;

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

      constexpr float min_body_height = OrderFlowConst::MIN_CANDLESTICK_BODY_HEIGHT;
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

static void RenderDepthPanel(const OrderFlow::L0Cache::DepthSnapshot &depth, const std::string &date, float panel_width) {
  if (!depth.valid) {
    ImGui::TextDisabled("No valid data");
    return;
  }

  char date_buf[16], time_buf[16];
  FormatDateFull(date_buf, sizeof(date_buf), date);
  FormatTimeHMS(time_buf, sizeof(time_buf), depth.time.hour, depth.time.minute, depth.time.second);

  // Use smaller font and tighter spacing for more compact display
  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]);               // Use default small font
  ImGui::SetWindowFontScale(0.75f);                              // Scale down to 85%
  ImGui::PushStyleVar(ImGuiStyleVar_ItemSpacing, ImVec2(0, 0));  // No spacing
  ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(1, 0)); // Ultra minimal padding

  ImGui::Text("%s %s", date_buf, time_buf);
  ImGui::Separator();

  const float bar_max_width = panel_width - 100.0f;

  // Helper lambda to render a single depth level
  // NOTE: volume is SIGNED (bid_volume > 0, ask_volume < 0), so amount preserves sign
  // Sentinel values (NaN) are already filtered at data load time
  auto render_level = [&](float price, float volume, bool is_bid) {
    float amount = volume_to_amount(volume, price); // Preserves sign: bid+, ask-
    float abs_amount = std::abs(amount);
    float ratio = std::min(1.0f, abs_amount / OrderFlowConst::DEPTH_BAR_MAX_AMOUNT); // 100W = full bar
    float amount_in_wan = amount_to_wan(amount);

    // Color intensity based on depth bar max (100W)
    ImVec4 bar_color;
    bool expected_sign = is_bid ? (amount > 0) : (amount < 0);

    if (expected_sign) {
      // Normal case: green for bid (amount > 0), red for ask (amount < 0)
      float intensity = std::min(1.0f, abs_amount / OrderFlowConst::DEPTH_BAR_MAX_AMOUNT);
      if (is_bid) {
        bar_color = ImVec4(0.3f * (1.0f - intensity * 0.5f), 0.8f, 0.3f * (1.0f - intensity * 0.5f), 0.8f);
      } else {
        bar_color = ImVec4(0.8f, 0.3f * (1.0f - intensity * 0.5f), 0.3f * (1.0f - intensity * 0.5f), 0.8f);
      }
    } else {
      // Anomaly: wrong sign, show as yellow warning
      bar_color = ImVec4(0.9f, 0.9f, 0.3f, 0.8f);
    }

    ImGui::PushStyleColor(ImGuiCol_PlotHistogram, bar_color);
    ImGui::ProgressBar(ratio, ImVec2(bar_max_width, 5.0f), ""); // Compact height
    ImGui::PopStyleColor();
    ImGui::SameLine();
    ImGui::Text("%6.2f元 %+7.2f万", price, amount_in_wan); // Fixed format: xxx.xx +-xxx.xx
  };

  // Ask side (red) - 10 levels, from top (ask10) to bottom (ask1)
  for (int i = 9; i >= 0; --i) {
    render_level((*depth.ask_price)[i], (*depth.ask_volume)[i], false);
  }

  // Mid price - no separator to save space
  ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%.2f元", depth.mid_price);

  // Bid side (green) - 10 levels, from top (bid1) to bottom (bid10)
  for (int i = 0; i < 10; ++i) {
    render_level((*depth.bid_price)[i], (*depth.bid_volume)[i], true);
  }

  ImGui::SetWindowFontScale(1.0f); // Reset font scale
  ImGui::PopStyleVar(2);
  ImGui::PopFont();
}

// ============================================================================
// L0 Plot Renderer
// ============================================================================

static void RenderL0Plot(OrderFlow &of, bool force_reset) {
  if (!of.l0.loaded || of.l0.plot.x.empty()) {
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

    ImPlot::SetupAxes(nullptr, nullptr, 0, 0);
    ImPlot::SetupAxisLimits(ImAxis_X1, x_min, x_max, cond);
    ImPlot::SetupAxisLimits(ImAxis_Y1, of.l0.plot.y_min_with_margin, of.l0.plot.y_max_with_margin, cond);

    // Setup X-axis formatter: converts X coordinates (time index) to time strings
    // This ensures correct time display for ALL positions, not just tick marks
    ImPlot::SetupAxisFormat(ImAxis_X1, L0TimeFormatter);

    // Setup L0 ticks (use pre-computed TimeAxisLUT)
    if (ui.l0_cached_day_idx != day_idx) {
      const auto &lut = TimeAxisLUT::instance();

      ui.l0_tick_positions.clear();
      ui.l0_tick_labels.clear();

      for (size_t offset : lut.l0_tick_offsets) {
        double tick_pos = x_min + static_cast<double>(offset);
        ui.l0_tick_positions.push_back(tick_pos);
      }
      for (const auto &label : lut.l0_tick_labels) {
        ui.l0_tick_labels.push_back(label.c_str());
      }

      ui.l0_cached_day_idx = day_idx;
    }

    if (!ui.l0_tick_positions.empty()) {
      ImPlot::SetupAxisTicks(ImAxis_X1, ui.l0_tick_positions.data(),
                             static_cast<int>(ui.l0_tick_positions.size()), ui.l0_tick_labels.data());
    }

    // Render heatmap if enabled (using multi-level cache)
    if (ui.show_heatmap && !of.l0.days.empty()) {
      // ========================================================================
      // LEVEL 2: Merged cache (already built in DataLoader)
      // ========================================================================
      if (!of.l0.check_heatmap_merged_cache()) {
        // Fallback: rebuild if somehow invalid
        of.l0.build_heatmap_merged();
      }

      // ========================================================================
      // LEVEL 3: Colored cache (rebuild only when threshold changes)
      // ========================================================================
      if (!of.l0.check_heatmap_colored_cache(ui.log_amount_threshold)) {
        of.l0.build_heatmap_colored(ui.log_amount_threshold);
      }

      // ========================================================================
      // RENDER: Use colored cache (static, no CPU work per frame)
      // ========================================================================
      ImPlot::PushPlotClipRect();
      ImDrawList *draw_list = ImPlot::GetPlotDrawList();

      for (const auto &rect : of.l0.heatmap_colored.rects) {
        ImVec2 p_min = ImPlot::PlotToPixels(rect.x1, rect.y1);
        ImVec2 p_max = ImPlot::PlotToPixels(rect.x2, rect.y2);
        draw_list->AddRectFilled(p_min, p_max, rect.color);
      }

      ImPlot::PopPlotClipRect();
    }

    // Draw best bid and ask lines with fill between
    ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.3f, 0.8f, 0.3f, 0.7f));
    ImPlot::PlotStairs("Best Bid", of.l0.plot.x.data(), of.l0.plot.best_bid.data(),
                       static_cast<int>(of.l0.plot.x.size()));
    ImPlot::PopStyleColor();

    ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.8f, 0.3f, 0.3f, 0.7f));
    ImPlot::PlotStairs("Best Ask", of.l0.plot.x.data(), of.l0.plot.best_ask.data(),
                       static_cast<int>(of.l0.plot.x.size()));
    ImPlot::PopStyleColor();

    // Fill between best bid and best ask with solid yellow
    ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(1.0f, 1.0f, 0.0f, 0.6f));
    ImPlot::PlotShaded("Spread", of.l0.plot.x.data(), of.l0.plot.best_bid.data(),
                       of.l0.plot.best_ask.data(), static_cast<int>(of.l0.plot.x.size()));
    ImPlot::PopStyleColor();

    // Draw mid price with step mode (for sparse data)
    ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(1.0f, 1.0f, 1.0f, 0.9f));
    ImPlot::PlotStairs("Mid Price", of.l0.plot.x.data(), of.l0.plot.mid_price.data(),
                       static_cast<int>(of.l0.plot.x.size()));
    ImPlot::PopStyleColor();

    // Anchor: get snapped X from plot_idx (single source of truth)
    double anchor_x = ui.l0_anchor_plot_idx < of.l0.plot.x.size()
                          ? of.l0.plot.x[ui.l0_anchor_plot_idx]
                          : x_min;

    // DragLineX: modifies anchor_x during drag, returns true when changed
    bool drag_changed = ImPlot::DragLineX(0, &anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f);
    bool drag_active = ImGui::IsItemActive();

    // Snap only when drag is released (changed but no longer active)
    if (drag_changed && !drag_active) {
      ui.l0_anchor_plot_idx = of.l0.snap_to_valid_plot_idx(anchor_x);
    }

    if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0)) {
      ui.l0_anchor_plot_idx = of.l0.snap_to_valid_plot_idx(ImPlot::GetPlotMousePos().x);
    }

    // Annotation: use snapped data (re-fetch from plot_idx for consistency)
    if (ui.l0_anchor_plot_idx < of.l0.plot.x.size()) {
      auto depth = of.l0.query_depth(ui.l0_anchor_plot_idx);
      if (depth.valid) {
        char time_buf[16];
        FormatTimeHMS(time_buf, sizeof(time_buf), depth.time.hour, depth.time.minute, depth.time.second);
        ImPlot::Annotation(of.l0.plot.x[ui.l0_anchor_plot_idx], of.l0.plot.mid_price[ui.l0_anchor_plot_idx],
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
    if (x_max <= x_min)
      x_max = x_min + 1;

    // Reset view when params changed or on first load
    ImPlotCond cond = force_reset ? ImPlotCond_Always : ImPlotCond_Once;

    ImPlot::SetupAxes(nullptr, nullptr, 0, 0);
    ImPlot::SetupAxisLimits(ImAxis_X1, x_min, x_max, cond);
    ImPlot::SetupAxisLimits(ImAxis_Y1, pd.y_min, pd.y_max, cond);

    // Setup K-line ticks: show all tick lines, but only label at intervals
    if (ui.l1_cached_num_days != of.l1.num_days) {
      ui.l1_tick_positions.clear();
      ui.l1_tick_label_storage.clear();

      // Adaptive label interval: limit to max 16 ticks
      constexpr size_t MAX_TICKS = 16;
      size_t label_interval = std::max<size_t>(1, (of.l1.num_days + MAX_TICKS - 1) / MAX_TICKS);

      // Add tick positions only at label intervals (to avoid overcrowding grid lines)
      for (size_t d = 0; d < of.l1.num_days; d += label_interval) {
        ui.l1_tick_positions.push_back(static_cast<double>(d * OrderFlowConst::L1_CAPACITY));
        
        char buf[16];
        FormatDateShort(buf, sizeof(buf), of.l1.dates[d]);
        ui.l1_tick_label_storage.push_back(buf);
      }

      ui.l1_tick_labels.clear();
      for (const auto &s : ui.l1_tick_label_storage) {
        ui.l1_tick_labels.push_back(s.c_str());
      }

      ui.l1_cached_num_days = of.l1.num_days;
    }

    if (!ui.l1_tick_positions.empty()) {
      ImPlot::SetupAxisTicks(ImAxis_X1, ui.l1_tick_positions.data(),
                             static_cast<int>(ui.l1_tick_positions.size()), ui.l1_tick_labels.data());
    }

    if (!pd.x.empty()) {
      PlotCandlestick("OHLC", pd.x.data(), pd.open.data(), pd.high.data(),
                      pd.low.data(), pd.close.data(), static_cast<int>(pd.x.size()));

      // Anchor: use snapped X (ui.l1_anchor_x is source of truth)
      double anchor_x = ui.l1_anchor_x;

      // DragLineX: snap only when drag is released
      bool drag_changed = ImPlot::DragLineX(0, &anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f);
      bool drag_active = ImGui::IsItemActive();

      if (drag_changed && !drag_active) {
        ui.l1_anchor_x = of.l1.snap_to_day_start(anchor_x);
        ui.l1_anchor_date = of.l1.date_from_x(ui.l1_anchor_x);
      }

      if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0)) {
        ui.l1_anchor_x = of.l1.snap_to_day_start(ImPlot::GetPlotMousePos().x);
        ui.l1_anchor_date = of.l1.date_from_x(ui.l1_anchor_x);
      }

      // Annotation: use snapped position and find corresponding Y value
      if (!ui.l1_anchor_date.empty()) {
        auto it = std::lower_bound(pd.x.begin(), pd.x.end(), ui.l1_anchor_x);
        double anchor_y = 0;
        if (it != pd.x.end()) {
          size_t idx = static_cast<size_t>(it - pd.x.begin());
          if (idx < pd.close.size())
            anchor_y = pd.close[idx];
        }

        char date_buf[16];
        FormatDateShort(date_buf, sizeof(date_buf), ui.l1_anchor_date);
        ImPlot::Annotation(ui.l1_anchor_x, anchor_y, ImVec4(1, 0.5f, 0, 1),
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
  if (num_assets == 0)
    return;

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
      ImGui::SetTooltip("L1 (分钟级别):\n"
                        "所有显示的K线都有完整的OHLCV数据\n"
                        "仅使用 data_valid 标记");
    }
  }
  ImGui::SameLine();
  if (!of.ui.l1_anchor_date.empty()) {
    char date_buf[16];
    FormatDateShort(date_buf, sizeof(date_buf), of.ui.l1_anchor_date);
    ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "Anchor: %s", date_buf);
  }
  ImGui::SameLine();
  if (of.loader.l0_requested) {
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.0f, 1.0f), "[Loading L0...]");
  } else if (of.l0.loaded) {
    auto stats = of.l0.compute_stats();
    ImGui::TextColored(ImVec4(0.3f, 0.6f, 0.9f, 1.0f), "[L0: %zu/%zu/%zu]",
                       stats.heatmap_rects, stats.depth_valid, stats.data_valid);
    if (ImGui::IsItemHovered()) {
      ImGui::SetTooltip("合并矩形数 / 有效深度 / 有效数据\n"
                        "合并矩形数: 热力图缓存中的矩形总数(已合并)\n"
                        "有效深度: LOB深度缓冲完整的tick数(含买卖价格/挂单量/中间价)\n"
                        "有效数据: 事件驱动数据存在的tick数(含其他tick特征)");
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
      ImGui::Text("Log10(金额) 下限阈值");
      ImGui::Separator();
      ImGui::Text("3.0 = 1千元 (显示所有 >= 1千的档位)");
      ImGui::Text("4.0 = 1万元");
      ImGui::Text("5.0 = 10万元");
      ImGui::Text("6.0 = 100万元");
      ImGui::Text("7.0 = 1000万元 (仅显示大额档位)");
      ImGui::Separator();
      ImGui::TextWrapped("范围: [阈值, 1000万] 映射到 [透明, 完全实色]");
      ImGui::TextWrapped("降低阈值: 看到更多薄档位");
      ImGui::TextWrapped("提高阈值: 只关注大额档位");
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
  const bool params_changed = ui.detect_and_update_changes();

  // L0: Request load on parameter change
  if (params_changed && !ui.l1_anchor_date.empty()) {
    loader->RequestL0Load(of, ui.l1_anchor_date, asset_idx);
  }

  // Detect L0 data just finished loading (for auto-zoom)
  const bool l0_just_loaded = ui.prev_l0_loading && !of.loader.l0_requested;
  ui.prev_l0_loading = of.loader.l0_requested;

  // Force reset when params changed OR when L0 data just finished loading
  const bool force_reset = params_changed || l0_just_loaded;

  // ==========================================================================
  // LAYOUT: Compute dimensions
  // ==========================================================================

  const float content_height = ImGui::GetContentRegionAvail().y;
  const float content_width = ImGui::GetContentRegionAvail().x;
  const float top_view_height = content_height * OrderFlowConst::TOP_VIEW_RATIO;
  const float bottom_view_height = content_height * (1.0f - OrderFlowConst::TOP_VIEW_RATIO) - 5.0f;
  const float chart_width = content_width - OrderFlowConst::DEPTH_PANEL_WIDTH - 10.0f;

  // ==========================================================================
  // TOP SECTION: L0 Order Flow + Depth
  // ==========================================================================

  ImGui::BeginChild("TopSection", ImVec2(0, top_view_height), false);
  ImGui::BeginChild("L0Chart", ImVec2(chart_width, -1), false);

  RenderL0Plot(of, force_reset);

  ImGui::EndChild();

  ImGui::SameLine();
  ImGui::BeginChild("DepthPanel", ImVec2(OrderFlowConst::DEPTH_PANEL_WIDTH, -1), true);

  if (of.l0.loaded && ui.l0_anchor_plot_idx < of.l0.plot.x.size()) {
    auto depth = of.l0.query_depth(ui.l0_anchor_plot_idx);
    std::string date = of.l0.date_from_plot_idx(ui.l0_anchor_plot_idx);
    RenderDepthPanel(depth, date, OrderFlowConst::DEPTH_PANEL_WIDTH);
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
