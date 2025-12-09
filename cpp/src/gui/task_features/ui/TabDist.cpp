#include "gui/task_features/ui/TabDist.hpp"
#include "gui/task_features/services/DistService.hpp"
#include "shared/SharedData.hpp"
#include "shared/Dist.hpp"
#include "shared/Feature.hpp"

#include "imgui.h"
#include "implot.h"

#include <cstdio>
#include <string>

namespace GUI::Features {

// ============================================================================
// Helper Functions
// ============================================================================

// static const char *get_grouping_name(int idx) {
//   static const char *names[] = {"None", "Hour", "Weekday", "Month", "Year"};
//   return names[idx];
// }

static const char *get_status_text(Dist::Compute::Status status) {
  switch (status) {
    case Dist::Compute::Status::Idle: return "Idle";
    case Dist::Compute::Status::LoadingData: return "Loading data...";
    case Dist::Compute::Status::Grouping: return "Grouping data...";
    case Dist::Compute::Status::Computing: return "Computing statistics...";
    case Dist::Compute::Status::BuildingCache: return "Building cache...";
    case Dist::Compute::Status::Completed: return "Completed";
    case Dist::Compute::Status::Error: return "Error";
    case Dist::Compute::Status::Cancelled: return "Cancelled";
    default: return "Unknown";
  }
}

// ============================================================================
// UI Rendering Sections
// ============================================================================

static void RenderControlPanel(DistService *service, SharedData &data, DistUIState &ui_state) {
  ImGui::BeginChild("ControlPanel", ImVec2(0, 120), true);
  
  ImGui::Text("Distribution Analysis Control");
  ImGui::Separator();
  
  // Primary feature display
  int primary_idx = data.feature.selection.primary_feature_idx;
  if (primary_idx >= 0 && primary_idx < (int)data.feature.metadata.features_l0.size()) {
    const auto &meta = data.feature.metadata.features_l0[primary_idx];
    ImGui::Text("Primary Feature: %s (%s)", meta.name_en, meta.code);
  } else {
    ImGui::TextColored(ImVec4(1, 0, 0, 1), "No primary feature selected!");
  }
  
  // Time grouping selection
  ImGui::Text("Time Grouping:");
  ImGui::SameLine();
  if (ImGui::Combo("##Grouping", &ui_state.selected_grouping, "None\0Hour\0Weekday\0Month\0Year\0")) {
    data.dist.input.time_grouping = static_cast<Dist::Input::TimeGrouping>(ui_state.selected_grouping);
  }
  
  // PDF sensitivity
  ImGui::Text("PDF Sensitivity:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(200);
  if (ImGui::SliderFloat("##PDFSens", &ui_state.pdf_sensitivity_ui, 0.1f, 10.0f, "%.2f")) {
    data.dist.input.pdf_sensitivity = ui_state.pdf_sensitivity_ui;
  }
  
  // Compute button
  ImGui::Separator();
  if (ImGui::Button("Compute Distribution", ImVec2(180, 0))) {
    if (primary_idx >= 0) {
      service->RequestCompute();
    }
  }
  
  ImGui::SameLine();
  if (ImGui::Button("Rebuild Cache", ImVec2(120, 0))) {
    service->RequestCacheRebuild();
  }
  
  ImGui::SameLine();
  if (ImGui::Button("Cancel", ImVec2(80, 0))) {
    data.dist.cancel_compute();
  }
  
  // Status display
  ImGui::SameLine();
  const char *status_text = get_status_text(data.dist.compute.status);
  ImGui::Text("Status: %s", status_text);
  
  if (data.dist.compute.status == Dist::Compute::Status::Computing) {
    ImGui::SameLine();
    float progress = data.dist.compute.get_progress_pct();
    char progress_text[32];
    snprintf(progress_text, sizeof(progress_text), "%.1f%%", progress);
    ImGui::ProgressBar(progress / 100.0f, ImVec2(100, 0), progress_text);
  }
  
  if (!data.dist.compute.error_message.empty()) {
    ImGui::TextColored(ImVec4(1, 0, 0, 1), "Error: %s", data.dist.compute.error_message.c_str());
  }
  
  ImGui::EndChild();
}

static void RenderIntegritySection(const Dist &dist) {
  ImGui::BeginChild("Integrity", ImVec2(0, 60), true);
  
  const auto &integrity = dist.stats.integrity;
  
  // Compact single-line display
  ImGui::Text("[Integrity] Valid: %zu/%.1f%% | Zero: %zu/%.1f%% | NaN: %zu/%.1f%% | +Inf: %zu/%.1f%% | -Inf: %zu/%.1f%%",
              integrity.valid_count, integrity.valid_pct,
              integrity.zero_count, integrity.zero_pct,
              integrity.nan_count, integrity.nan_pct,
              integrity.pos_inf_count, integrity.pos_inf_pct,
              integrity.neg_inf_count, integrity.neg_inf_pct);
  
  ImGui::EndChild();
}

static void RenderMomentsSection(const Dist &dist, DistUIState &ui_state) {
  ImGui::BeginChild("Moments", ImVec2(350, 0), true);
  
  ImGui::Text("[Moments]");
  ImGui::Separator();
  
  if (dist.stats.moments.empty()) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }
  
  // Bin selection
  int n_bins = dist.stats.moments.size();
  if (ImGui::SliderInt("Bin##MomentsBin", &ui_state.selected_bin_idx, 0, n_bins - 1)) {
    if (ui_state.selected_bin_idx >= n_bins) {
      ui_state.selected_bin_idx = n_bins - 1;
    }
  }
  
  ImGui::Text("Bin: %s", dist.grouped.bins[ui_state.selected_bin_idx].key.c_str());
  ImGui::Separator();
  
  // Show moments for selected bin (aggregate across assets)
  const auto &bin_moments = dist.stats.moments[ui_state.selected_bin_idx];
  
  // Compute aggregate moments
  double sum_mean = 0.0, sum_var = 0.0, sum_skew = 0.0, sum_kurt = 0.0;
  size_t count = 0;
  
  for (const auto &m : bin_moments.per_asset) {
    sum_mean += m.mean;
    sum_var += m.variance;
    sum_skew += m.skewness;
    sum_kurt += m.kurtosis;
    ++count;
  }
  
  if (count > 0) {
    double avg_mean = sum_mean / count;
    double avg_var = sum_var / count;
    double avg_skew = sum_skew / count;
    double avg_kurt = sum_kurt / count;
    
    ImGui::Text("Mean (1st moment): %.4f", avg_mean);
    ImGui::Text("Variance (2nd moment): %.4f", avg_var);
    ImGui::Text("Skewness (3rd moment): %.4f", avg_skew);
    ImGui::Text("Kurtosis (4th moment): %.4f", avg_kurt);
  }
  
  ImGui::EndChild();
}

static void RenderDistributionPlot(const Dist &dist, DistUIState /*&ui_state*/) {
  ImGui::BeginChild("DistPlot", ImVec2(0, 300), true);
  
  ImGui::Text("[Distribution Density]");
  ImGui::Separator();
  
  if (dist.vis_cache.pdf_plots.empty()) {
    ImGui::Text("No PDF data available");
    ImGui::EndChild();
    return;
  }
  
  // TODO: Render PDF plot using ImPlot
  // This requires actual PDF computation (KDE)
  ImGui::Text("PDF plot (KDE) - Not yet implemented");
  ImGui::Text("Will show probability density curves for each asset");
  
  ImGui::EndChild();
}

static void RenderTrajectoryPlot(const Dist &dist) {
  ImGui::BeginChild("TrajectoryPlot", ImVec2(0, 400), true);
  
  ImGui::Text("[Cross-section Distribution Trajectory]");
  ImGui::Separator();
  
  if (dist.vis_cache.trajectory_plots.empty()) {
    ImGui::Text("No trajectory data available");
    ImGui::EndChild();
    return;
  }
  
  if (ImPlot::BeginPlot("##Trajectory", ImVec2(-1, -1))) {
    ImPlot::SetupAxes("Robust Skewness", "Tail Thickness");
    
    // Plot each bin's trajectory
    for (size_t bin_idx = 0; bin_idx < dist.vis_cache.trajectory_plots.size(); ++bin_idx) {
      const auto &plot = dist.vis_cache.trajectory_plots[bin_idx];
      
      if (!plot.x.empty()) {
        std::string label = dist.grouped.bins[bin_idx].key;
        ImPlot::PlotScatter(label.c_str(), plot.x.data(), plot.y.data(), plot.x.size());
      }
    }
    
    ImPlot::EndPlot();
  }
  
  ImGui::EndChild();
}

static void RenderQuantileHeatmap(const Dist &dist) {
  ImGui::BeginChild("QuantileHeatmap", ImVec2(0, 300), true);
  
  ImGui::Text("[Quantile Stability Heatmap]");
  ImGui::Separator();
  
  const auto &heatmap = dist.vis_cache.quantile_heatmap;
  
  if (heatmap.matrix.empty()) {
    ImGui::Text("No quantile data available");
    ImGui::EndChild();
    return;
  }
  
  if (ImPlot::BeginPlot("##QuantileHeatmap", ImVec2(-1, -1))) {
    ImPlot::SetupAxes("Time", "Quantile");
    
    // Plot heatmap
    // Each row is a quantile (q01, q05, q25, q50, q75, q95, q99)
    // Each column is a time bin
    
    static const char *quantile_labels[] = {"q01", "q05", "q25", "q50", "q75", "q95", "q99"};
    
    // Plot as line series (one line per quantile)
    for (size_t q_idx = 0; q_idx < heatmap.n_rows; ++q_idx) {
      std::vector<double> x_vals(heatmap.n_cols);
      std::vector<double> y_vals(heatmap.n_cols);
      
      for (size_t bin_idx = 0; bin_idx < heatmap.n_cols; ++bin_idx) {
        x_vals[bin_idx] = bin_idx;
        y_vals[bin_idx] = heatmap.matrix[q_idx * heatmap.n_cols + bin_idx];
      }
      
      ImPlot::PlotLine(quantile_labels[q_idx], x_vals.data(), y_vals.data(), heatmap.n_cols);
    }
    
    ImPlot::EndPlot();
  }
  
  ImGui::EndChild();
}

static void RenderHeterogeneitySection(const Dist &dist) {
  ImGui::BeginChild("Heterogeneity", ImVec2(0, 150), true);
  
  ImGui::Text("[Heterogeneity Consistency]");
  ImGui::Separator();
  
  if (dist.stats.heterogeneity.empty()) {
    ImGui::Text("No heterogeneity data");
    ImGui::EndChild();
    return;
  }
  
  // Show Gini and HHI for each bin
  for (size_t bin_idx = 0; bin_idx < dist.stats.heterogeneity.size(); ++bin_idx) {
    const auto &h = dist.stats.heterogeneity[bin_idx];
    ImGui::Text("%s: Gini=%.4f, HHI=%.4f", 
                dist.grouped.bins[bin_idx].key.c_str(), h.gini, h.hhi);
  }
  
  ImGui::EndChild();
}

static void RenderScaleRobustnessSection(const Dist &dist) {
  ImGui::BeginChild("ScaleRobustness", ImVec2(0, 120), true);
  
  ImGui::Text("[Scale Robustness]");
  ImGui::Separator();
  
  const auto &sr = dist.stats.scale_robustness;
  
  ImGui::Text("Rank Correlation:");
  ImGui::Text("  raw <-> zscore:  %.2f%%", sr.rank_corr_raw_zscore * 100.0f);
  ImGui::ProgressBar(sr.rank_corr_raw_zscore, ImVec2(-1, 0));
  
  ImGui::Text("  raw <-> minmax:  %.2f%%", sr.rank_corr_raw_minmax * 100.0f);
  ImGui::ProgressBar(sr.rank_corr_raw_minmax, ImVec2(-1, 0));
  
  ImGui::Text("  raw <-> robust:  %.2f%%", sr.rank_corr_raw_robust * 100.0f);
  ImGui::ProgressBar(sr.rank_corr_raw_robust, ImVec2(-1, 0));
  
  ImGui::EndChild();
}

// ============================================================================
// Main Render Function
// ============================================================================

void RenderTabDist(DistService *service, SharedData &data, DistUIState &ui_state) {
  // Ensure compute coroutine is running (start if not already)
  if (!service->is_running()) {
    service->StartCompute(data.gui.Coro(), data);
  }
  
  // Sync UI state with Dist state
  ui_state.selected_grouping = static_cast<int>(data.dist.input.time_grouping);
  ui_state.pdf_sensitivity_ui = data.dist.input.pdf_sensitivity;
  
  // Layout: Control panel at top, then scrollable content
  RenderControlPanel(service, data, ui_state);
  
  ImGui::BeginChild("ScrollContent", ImVec2(0, 0), false);
  
  // Only show results if data is loaded
  if (!data.dist.is_data_loaded()) {
    ImGui::TextColored(ImVec4(1, 1, 0, 1), "No data loaded. Click 'Compute Distribution' to start.");
    ImGui::EndChild();
    return;
  }
  
  // Integrity section
  RenderIntegritySection(data.dist);
  
  // Two-column layout for moments and distribution plot
  ImGui::Columns(2, "MomentsAndDist", false);
  
  RenderMomentsSection(data.dist, ui_state);
  
  ImGui::NextColumn();
  
  RenderDistributionPlot(data.dist, ui_state);
  
  ImGui::Columns(1);
  
  // Trajectory plot
  if (ui_state.show_trajectory) {
    RenderTrajectoryPlot(data.dist);
  }
  
  // Quantile heatmap
  if (ui_state.show_quantile_heatmap) {
    RenderQuantileHeatmap(data.dist);
  }
  
  // Two-column layout for heterogeneity and scale robustness
  ImGui::Columns(2, "HeteroAndScale", false);
  
  RenderHeterogeneitySection(data.dist);
  
  ImGui::NextColumn();
  
  RenderScaleRobustnessSection(data.dist);
  
  ImGui::Columns(1);
  
  ImGui::EndChild();
}

void StopTabDist(DistService *service, SharedData &data) {
  service->StopCompute(data.gui.Coro(), data);
}

} // namespace GUI::Features

