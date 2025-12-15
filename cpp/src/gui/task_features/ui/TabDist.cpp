#include "gui/task_features/ui/TabDist.hpp"
#include "gui/task_features/services/DistService.hpp"
#include "shared/Asset.hpp"
#include "shared/Config.hpp"
#include "shared/Dist.hpp"
#include "shared/Feature.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"

#include <algorithm>
#include <cmath>
#include <cstdio>

namespace GUI::Features {

// ============================================================================
// Helpers
// ============================================================================

static const char *StatusText(Dist::Compute::Status s) {
  switch (s) {
  case Dist::Compute::Status::Idle:
    return "Idle";
  case Dist::Compute::Status::Building:
    return "Building...";
  case Dist::Compute::Status::Querying:
    return "Querying...";
  case Dist::Compute::Status::Done:
    return "Done";
  case Dist::Compute::Status::Error:
    return "Error";
  case Dist::Compute::Status::Cancelled:
    return "Cancelled";
  }
  return "?";
}

// ============================================================================
// Integrity Panel
// ============================================================================

static void RenderIntegrity(const Dist::Integrity &integrity) {
  ImGui::Text("Zero: %zu (%.1f%%)  NaN: %zu (%.1f%%)  +Inf: %zu (%.1f%%)  "
              "-Inf: %zu (%.1f%%)",
              integrity.n_zero, integrity.zero_pct(), integrity.n_nan,
              integrity.nan_pct(), integrity.n_pos_inf, integrity.inf_pct(),
              integrity.n_neg_inf, integrity.inf_pct());
}

// ============================================================================
// Window Control Panel
// Row 1: Compute | Cancel | Status (n/m) | By selector
// Row 2: Month slider (always visible, from config date range)
// ============================================================================

// Format month key "YYYYMM" -> "YYYY/MM"
static std::string format_month(const std::string &m) {
  if (m.size() >= 6)
    return m.substr(0, 4) + "/" + m.substr(4, 2);
  return m;
}

// Get slider label: "YYYY/MM (n_samples)" or "YYYY/MM" if no cache
static std::string get_month_label(const Dist &dist, int idx,
                                   const std::vector<std::string> &months) {
  if (idx < 0 || idx >= static_cast<int>(months.size()))
    return "";
  std::string label = format_month(months[idx]);
  // Add n_samples if cache available
  if (idx < static_cast<int>(dist.cache.size()) && dist.cache[idx].valid) {
    label += " (" + std::to_string(dist.cache[idx].total.count()) + ")";
  }
  return label;
}

// Generate months list from config date range
static std::vector<std::string> generate_months(const std::string &start_date,
                                                const std::string &end_date) {
  std::vector<std::string> months;
  // Parse dates (YYYY-MM-DD or YYYYMMDD)
  auto parse_month = [](const std::string &date) -> std::string {
    std::string d;
    for (char c : date)
      if (c != '-')
        d += c;
    return d.size() >= 6 ? d.substr(0, 6) : "";
  };

  std::string start_month = parse_month(start_date);
  std::string end_month = parse_month(end_date);
  if (start_month.empty() || end_month.empty())
    return months;

  int y = std::stoi(start_month.substr(0, 4));
  int m = std::stoi(start_month.substr(4, 2));
  while (true) {
    char buf[8];
    snprintf(buf, sizeof(buf), "%04d%02d", y, m);
    std::string month_key = buf;
    if (month_key > end_month)
      break;
    months.push_back(month_key);
    if (++m > 12) {
      m = 1;
      ++y;
    }
  }
  return months;
}

static void RenderWindowControl(DistService *service, SharedData &data,
                                DistUIState &ui) {
  auto &dist = data.dist;

  // Row 1: Compute | Cancel | Status | By selector
  bool can_compute =
      !dist.compute.is_busy() && data.feature.selection.primary_feature_idx >= 0;
  ImGui::BeginDisabled(!can_compute);
  if (ImGui::Button("Compute")) {
    service->RequestCompute();
  }
  ImGui::EndDisabled();

  ImGui::SameLine();
  if (ImGui::Button("Cancel")) {
    dist.cancel();
  }

  ImGui::SameLine();
  size_t done = dist.compute.done.load();
  size_t total = dist.compute.total;
  ImGui::Text("Status: %s (%zu/%zu)", StatusText(dist.compute.status), done,
              total);

  ImGui::SameLine(ImGui::GetWindowWidth() - 150);
  ImGui::Text("By:");
  ImGui::SameLine();
  const char *items[] = {"None", "Hour", "Weekday", "Month"};
  ImGui::SetNextItemWidth(100);
  if (ImGui::Combo("##GroupBy", &ui.group_by, items, 4)) {
    dist.input.group_by = static_cast<Dist::Input::GroupBy>(ui.group_by);
    service->RequestQuery();
  }

  // Row 2: Month slider (always visible from config date range)
  auto months = generate_months(data.config.start_date, data.config.end_date);
  if (!months.empty()) {
    int n_months = static_cast<int>(months.size());
    ui.focus_month_idx = std::clamp(ui.focus_month_idx, 0, n_months - 1);

    std::string label = get_month_label(dist, ui.focus_month_idx, months);
    ImGui::SetNextItemWidth(-1);
    if (ImGui::SliderInt("##FocusMonth", &ui.focus_month_idx, 0, n_months - 1,
                         label.c_str())) {
      dist.input.focus_month_idx = ui.focus_month_idx;
    }
  }
}

// ============================================================================
// Moments Panel (Left Column)
// Color bands with boundary labels, current month value vs all months MAD
// ============================================================================

// Zone determination: 0=normal(green), 1=warn(yellow), 2=anomaly(red)
static int get_zone(float val, float normal_lo, float normal_hi, float warn_lo,
                    float warn_hi) {
  if (val >= normal_lo && val <= normal_hi)
    return 0;
  if (val >= warn_lo && val <= warn_hi)
    return 1;
  return 2;
}

static ImU32 zone_color(int zone) {
  if (zone == 0)
    return IM_COL32(60, 200, 60, 255);  // green
  if (zone == 1)
    return IM_COL32(220, 200, 60, 255); // yellow
  return IM_COL32(220, 80, 80, 255);    // red
}

// Color band with boundary labels
// Format: |range_lo |warn_lo | | | warn_hi| range_hi|
static void RenderMomentBand(const char *label, float current_val,
                             float global_mean, float global_mad,
                             float range_lo, float range_hi, float normal_lo,
                             float normal_hi, float warn_lo, float warn_hi) {
  ImDrawList *draw = ImGui::GetWindowDrawList();
  ImVec2 pos = ImGui::GetCursorScreenPos();
  float width = ImGui::GetContentRegionAvail().x;
  float bar_height = 16.0f;
  float label_height = 14.0f;

  float range = range_hi - range_lo;
  auto to_x = [&](float v) {
    return pos.x + std::clamp((v - range_lo) / range, 0.0f, 1.0f) * width;
  };

  // All bands share same vertical bounds
  float y_top = pos.y + label_height;
  float y_bot = y_top + bar_height;

  // Draw color bands (all aligned y_top to y_bot)
  // Anomaly bands (red)
  draw->AddRectFilled(ImVec2(to_x(range_lo), y_top),
                      ImVec2(to_x(warn_lo), y_bot),
                      IM_COL32(180, 60, 60, 200));
  draw->AddRectFilled(ImVec2(to_x(warn_hi), y_top),
                      ImVec2(to_x(range_hi), y_bot),
                      IM_COL32(180, 60, 60, 200));

  // Warn bands (yellow)
  draw->AddRectFilled(ImVec2(to_x(warn_lo), y_top),
                      ImVec2(to_x(normal_lo), y_bot),
                      IM_COL32(200, 180, 60, 200));
  draw->AddRectFilled(ImVec2(to_x(normal_hi), y_top),
                      ImVec2(to_x(warn_hi), y_bot),
                      IM_COL32(200, 180, 60, 200));

  // Normal band (green)
  draw->AddRectFilled(ImVec2(to_x(normal_lo), y_top),
                      ImVec2(to_x(normal_hi), y_bot),
                      IM_COL32(60, 160, 60, 200));

  // MAD range markers (slightly inset)
  float mad_lo = global_mean - 2.5f * global_mad;
  float mad_hi = global_mean + 2.5f * global_mad;
  draw->AddRect(ImVec2(to_x(mad_lo), y_top + 2),
                ImVec2(to_x(mad_hi), y_bot - 2),
                IM_COL32(255, 255, 255, 180), 0.0f, 0, 2.0f);

  // Current value marker (cyan diamond, centered)
  float cv_x = to_x(current_val);
  float cy = (y_top + y_bot) * 0.5f;
  draw->AddQuadFilled(ImVec2(cv_x, cy - 5), ImVec2(cv_x + 4, cy),
                      ImVec2(cv_x, cy + 5), ImVec2(cv_x - 4, cy),
                      IM_COL32(0, 255, 255, 255));

  // Boundary labels (1.1f format, skip middle)
  char buf[16];
  snprintf(buf, sizeof(buf), "%.1f", range_lo);
  draw->AddText(ImVec2(to_x(range_lo), pos.y), IM_COL32(200, 200, 200, 255), buf);
  snprintf(buf, sizeof(buf), "%.1f", warn_lo);
  draw->AddText(ImVec2(to_x(warn_lo) - 15, pos.y), IM_COL32(200, 200, 200, 255), buf);
  snprintf(buf, sizeof(buf), "%.1f", warn_hi);
  draw->AddText(ImVec2(to_x(warn_hi) - 10, pos.y), IM_COL32(200, 200, 200, 255), buf);
  snprintf(buf, sizeof(buf), "%.1f", range_hi);
  draw->AddText(ImVec2(to_x(range_hi) - 25, pos.y), IM_COL32(200, 200, 200, 255), buf);

  ImGui::Dummy(ImVec2(width, label_height + bar_height));

  // Text: "Label: current (mad_lo/mad_hi)" with zone colors, bold
  int zone_cur = get_zone(current_val, normal_lo, normal_hi, warn_lo, warn_hi);
  int zone_mad_lo = get_zone(mad_lo, normal_lo, normal_hi, warn_lo, warn_hi);
  int zone_mad_hi = get_zone(mad_hi, normal_lo, normal_hi, warn_lo, warn_hi);

  ImGui::Text("%s: ", label);
  ImGui::SameLine(0, 0);
  ImGui::PushStyleColor(ImGuiCol_Text, zone_color(zone_cur));
  ImGui::Text("%.2f", current_val);
  ImGui::PopStyleColor();
  ImGui::SameLine(0, 0);
  ImGui::Text(" (");
  ImGui::SameLine(0, 0);
  ImGui::PushStyleColor(ImGuiCol_Text, zone_color(zone_mad_lo));
  ImGui::Text("%.2f", mad_lo);
  ImGui::PopStyleColor();
  ImGui::SameLine(0, 0);
  ImGui::Text("/");
  ImGui::SameLine(0, 0);
  ImGui::PushStyleColor(ImGuiCol_Text, zone_color(zone_mad_hi));
  ImGui::Text("%.2f", mad_hi);
  ImGui::PopStyleColor();
  ImGui::SameLine(0, 0);
  ImGui::Text(")");
}

// Compute median and MAD from a vector of values
static std::pair<float, float> compute_median_mad(const std::vector<float> &vals) {
  if (vals.empty())
    return {0.0f, 0.0f};
  std::vector<float> sorted = vals;
  std::sort(sorted.begin(), sorted.end());
  float median = sorted[sorted.size() / 2];
  std::vector<float> abs_devs;
  abs_devs.reserve(vals.size());
  for (float v : vals) {
    abs_devs.push_back(std::abs(v - median));
  }
  std::sort(abs_devs.begin(), abs_devs.end());
  float mad = abs_devs[abs_devs.size() / 2];
  return {median, mad};
}

static void RenderMomentsPanel(const Dist &dist, int focus_idx) {
  ImGui::BeginChild("MomentsPanel", ImVec2(350, 0), true);
  ImGui::Text("[Moments Status]");
  ImGui::Separator();

  if (dist.result.bins.empty()) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  // Collect all bin values for computing MAD
  std::vector<float> means, vars, skews, kurts;
  for (const auto &b : dist.result.bins) {
    means.push_back(b.mean);
    vars.push_back(b.variance);
    skews.push_back(b.skewness);
    kurts.push_back(b.kurtosis);
  }

  auto [med_mean, mad_mean] = compute_median_mad(means);
  auto [med_var, mad_var] = compute_median_mad(vars);
  auto [med_skew, mad_skew] = compute_median_mad(skews);
  auto [med_kurt, mad_kurt] = compute_median_mad(kurts);

  // Get focus bin (current month)
  const auto *bin = &dist.result.bins[0];
  if (focus_idx >= 0 && focus_idx < static_cast<int>(dist.result.bins.size())) {
    bin = &dist.result.bins[focus_idx];
  }

  // Color bands with boundaries
  RenderMomentBand("Mean", bin->mean, med_mean, mad_mean, -1.0f, 1.0f, -0.1f,
                   0.1f, -0.3f, 0.3f);
  ImGui::Spacing();

  RenderMomentBand("Variance", bin->variance, med_var, mad_var, 0.0f, 3.0f,
                   0.5f, 1.2f, 0.2f, 2.0f);
  ImGui::Spacing();

  RenderMomentBand("Skewness", bin->skewness, med_skew, mad_skew, -4.0f, 4.0f,
                   -0.5f, 0.5f, -1.5f, 1.5f);
  ImGui::Spacing();

  RenderMomentBand("Kurtosis", bin->kurtosis, med_kurt, mad_kurt, -3.0f, 15.0f,
                   0.0f, 3.0f, -1.0f, 6.0f);

  ImGui::EndChild();
}

// ============================================================================
// PDF Evolution Panel (Right Column) - No legend, tooltip on hover
// ============================================================================

static void RenderPDFEvolution(const Dist &dist, int focus_idx) {
  ImGui::BeginChild("PDFPanel", ImVec2(0, 0), true);
  ImGui::Text("[PDF Evolution]");
  ImGui::Separator();

  if (dist.result.bins.empty()) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  // Track hovered bin for tooltip
  int hovered_bin = -1;
  double hover_dist_sq = 1e9;

  if (ImPlot::BeginPlot("##PDFEvolution", ImVec2(-1, -1),
                        ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes("Value", "Density");

    int n_bins = static_cast<int>(dist.result.bins.size());

    // First pass: draw all lines (zero-copy, direct double pointers)
    for (int i = 0; i < n_bins; ++i) {
      const auto &bin = dist.result.bins[i];
      if (bin.pdf_n == 0)
        continue;

      // Alpha decay based on distance from focus
      float alpha = 1.0f;
      if (focus_idx >= 0) {
        int dist_from_focus = std::abs(i - focus_idx);
        alpha = std::max(0.1f, 1.0f - dist_from_focus * 0.2f);
      }

      ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight,
                           i == focus_idx ? 2.0f : 1.0f);
      ImPlot::SetNextLineStyle(IMPLOT_AUTO_COL, alpha);
      // Zero-copy: use KLL internal pointers directly
      ImPlot::PlotLine("##pdf", bin.pdf_x, bin.pdf_y, static_cast<int>(bin.pdf_n));
      ImPlot::PopStyleVar();
    }

    // Second pass: find hovered line
    if (ImPlot::IsPlotHovered()) {
      ImPlotPoint mouse = ImPlot::GetPlotMousePos();
      for (int i = 0; i < n_bins; ++i) {
        const auto &bin = dist.result.bins[i];
        if (bin.pdf_n == 0)
          continue;

        // Find closest point on this line
        for (size_t j = 0; j < bin.pdf_n; ++j) {
          double dx = mouse.x - bin.pdf_x[j];
          double dy = mouse.y - bin.pdf_y[j];
          double d2 = dx * dx + dy * dy * 100.0; // Scale y more since density range smaller
          if (d2 < hover_dist_sq && d2 < 0.1) {
            hover_dist_sq = d2;
            hovered_bin = i;
          }
        }
      }
    }

    ImPlot::EndPlot();
  }

  // Tooltip for hovered bin
  if (hovered_bin >= 0 && hovered_bin < static_cast<int>(dist.result.bins.size())) {
    const auto &bin = dist.result.bins[hovered_bin];
    ImGui::BeginTooltip();
    ImGui::Text("%s", bin.key.c_str());
    ImGui::Text("n=%zu", bin.n_samples);
    ImGui::Text("mean=%.4f var=%.4f", bin.mean, bin.variance);
    ImGui::Text("skew=%.4f kurt=%.4f", bin.skewness, bin.kurtosis);
    ImGui::EndTooltip();
  }

  ImGui::EndChild();
}

// ============================================================================
// Trajectory Plot
// ============================================================================

static void RenderTrajectory(Dist &dist, const Asset &asset, DistUIState &ui) {
  ImGui::Text("[Cross-Section Distribution Trajectory]");
  ImGui::Separator();

  if (!dist.trajectory.valid || dist.trajectory.paths.empty()) {
    ImGui::Text("No trajectory data");
    return;
  }

  if (ImPlot::BeginPlot("##Trajectory", ImVec2(-1, 400))) {
    ImPlot::SetupAxes("Robust Skewness", "Tail Thickness");

    size_t n_assets = dist.trajectory.n_assets;
    size_t n_months = dist.trajectory.months.size();

    ui.hovered_asset = -1;

    for (size_t a = 0; a < n_assets; ++a) {
      const auto &path = dist.trajectory.paths[a];
      if (path.empty())
        continue;

      // Collect valid points
      std::vector<double> xs, ys, sizes;
      for (size_t m = 0; m < n_months; ++m) {
        const auto &pt = path[m];
        if (pt.n < 10)
          continue;
        xs.push_back(pt.x);
        ys.push_back(pt.y);
        sizes.push_back(std::sqrt(pt.size) * 10.0); // Scale size
      }

      if (xs.empty())
        continue;

      // Plot as scatter with paths
      std::string label =
          a < asset.items.size() ? asset.items[a].asset_code : std::to_string(a);

      // Color by peakedness (average)
      float avg_color = 0.0f;
      for (const auto &pt : path) {
        avg_color += pt.color;
      }
      avg_color /= path.size();
      ImVec4 color = ImPlot::SampleColormap(avg_color);

      ImPlot::SetNextMarkerStyle(ImPlotMarker_Circle, 4, color, 1.0f, color);
      ImPlot::PlotScatter(label.c_str(), xs.data(), ys.data(),
                          static_cast<int>(xs.size()));

      // Draw path lines connecting consecutive months
      if (xs.size() > 1) {
        ImPlot::SetNextLineStyle(color, 0.5f);
        ImPlot::PlotLine(("##path" + std::to_string(a)).c_str(), xs.data(),
                         ys.data(), static_cast<int>(xs.size()));
      }

      // Check hover
      if (ImPlot::IsPlotHovered()) {
        ImPlotPoint mouse = ImPlot::GetPlotMousePos();
        for (size_t i = 0; i < xs.size(); ++i) {
          double dx = mouse.x - xs[i];
          double dy = mouse.y - ys[i];
          if (dx * dx + dy * dy < 0.01) {
            ui.hovered_asset = static_cast<int>(a);
            break;
          }
        }
      }
    }

    ImPlot::EndPlot();
  }

  // Tooltip
  if (ui.hovered_asset >= 0 &&
      static_cast<size_t>(ui.hovered_asset) < asset.items.size()) {
    ImGui::Text("Asset: %s", asset.items[ui.hovered_asset].asset_code.c_str());
  }
}

// ============================================================================
// Main Render
// ============================================================================

void RenderTabDist(DistService *service, SharedData &data, DistUIState &ui) {
  // Auto-start coroutine
  if (!service->is_running()) {
    service->StartCompute(data.gui.Coro(), data);
  }

  auto &dist = data.dist;

  // Integrity
  ImGui::BeginChild("IntegrityBar", ImVec2(0, 25), true);
  RenderIntegrity(dist.result.integrity);
  ImGui::EndChild();

  // Window control
  ImGui::BeginChild("WindowCtrl", ImVec2(0, 80), true);
  RenderWindowControl(service, data, ui);
  ImGui::EndChild();

  // Moments + PDF (side by side)
  float h = ImGui::GetContentRegionAvail().y * 0.4f;
  ImGui::BeginChild("MomentsPDF", ImVec2(0, h), false);
  ImGui::Columns(2, "MomPDFCols", true);

  // Find focus bin index for MONTH grouping
  int focus_bin_idx = -1;
  if (ui.group_by == 3 && ui.focus_month_idx >= 0) {
    focus_bin_idx = ui.focus_month_idx;
  }

  RenderMomentsPanel(dist, focus_bin_idx);

  ImGui::NextColumn();

  RenderPDFEvolution(dist, focus_bin_idx);

  ImGui::Columns(1);
  ImGui::EndChild();

  // Trajectory
  ImGui::BeginChild("TrajectorySection", ImVec2(0, 0), false);
  RenderTrajectory(dist, data.asset, ui);
  ImGui::EndChild();
}

void StopTabDist(DistService *service, SharedData &data) {
  service->StopCompute(data.gui.Coro(), data);
}

} // namespace GUI::Features
