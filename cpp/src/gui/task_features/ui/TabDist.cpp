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

// Calculate distance from point to line segment (normalized coords)
static double point_to_segment_dist_sq(double px, double py, double x1, double y1,
                                       double x2, double y2) {
  double dx = x2 - x1;
  double dy = y2 - y1;
  double len_sq = dx * dx + dy * dy;
  if (len_sq < 1e-10)
    return (px - x1) * (px - x1) + (py - y1) * (py - y1);

  double t = ((px - x1) * dx + (py - y1) * dy) / len_sq;
  t = std::max(0.0, std::min(1.0, t));
  double closest_x = x1 + t * dx;
  double closest_y = y1 + t * dy;
  return (px - closest_x) * (px - closest_x) + (py - closest_y) * (py - closest_y);
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

// Compute alpha based on distance from focus index
static float compute_focus_alpha(int idx, int focus_idx, float decay_rate = 0.1f) {
  if (focus_idx < 0)
    return 1.0f;
  int dist_from_focus = std::abs(idx - focus_idx);
  return std::max(0.1f, 1.0f - dist_from_focus * decay_rate);
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
    if (ImGui::SliderInt("##FocusMonth", &ui.focus_month_idx, 0, n_months - 1, label.c_str())) {
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
    return IM_COL32(60, 200, 60, 255); // green
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
// PDF Panels - Multiple views with hover tooltips
// ============================================================================

// Common PDF data structure
struct PDFData {
  const float *x = nullptr;
  const float *y = nullptr;
  size_t n = 0;
  std::string label;
};

// Generic PDF rendering with hover detection
template <typename GetTooltipFunc>
static int RenderPDFPlot(const char *plot_id, const std::vector<PDFData> &pdfs,
                         int focus_idx, GetTooltipFunc get_tooltip) {
  int hovered_idx = -1;
  double min_dist_sq = 1e9;

  if (ImPlot::BeginPlot(plot_id, ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel, ImPlotAxisFlags_NoLabel);

    int n_items = static_cast<int>(pdfs.size());

    // Draw all lines with alpha based on distance from focus
    for (int i = 0; i < n_items; ++i) {
      if (pdfs[i].n == 0)
        continue;

      float alpha = compute_focus_alpha(i, focus_idx);
      float hue = static_cast<float>(i) / static_cast<float>(n_items);
      ImVec4 color = ImPlot::SampleColormap(hue, ImPlotColormap_Viridis);

      float line_weight = (i == focus_idx) ? 2.0f : 1.0f;
      ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, line_weight);
      ImPlot::SetNextLineStyle(color, alpha);
      ImPlot::PlotLine("##pdf", pdfs[i].x, pdfs[i].y, static_cast<int>(pdfs[i].n));
      ImPlot::PopStyleVar();
    }

    // Detect hover
    if (ImPlot::IsPlotHovered()) {
      ImPlotPoint mouse = ImPlot::GetPlotMousePos();
      ImPlotRect limits = ImPlot::GetPlotLimits();
      double x_range = limits.X.Max - limits.X.Min;
      double y_range = limits.Y.Max - limits.Y.Min;

      for (int i = 0; i < n_items; ++i) {
        if (pdfs[i].n < 2)
          continue;

        for (size_t j = 0; j + 1 < pdfs[i].n; ++j) {
          double nx1 = (pdfs[i].x[j] - limits.X.Min) / x_range;
          double ny1 = (pdfs[i].y[j] - limits.Y.Min) / y_range;
          double nx2 = (pdfs[i].x[j + 1] - limits.X.Min) / x_range;
          double ny2 = (pdfs[i].y[j + 1] - limits.Y.Min) / y_range;
          double nmx = (mouse.x - limits.X.Min) / x_range;
          double nmy = (mouse.y - limits.Y.Min) / y_range;

          double d_sq = point_to_segment_dist_sq(nmx, nmy, nx1, ny1, nx2, ny2);
          if (d_sq < min_dist_sq) {
            min_dist_sq = d_sq;
            hovered_idx = i;
          }
        }
      }
    }

    // Highlight hovered line
    if (hovered_idx >= 0 && min_dist_sq < 0.001) {
      float hue = static_cast<float>(hovered_idx) / static_cast<float>(n_items);
      ImVec4 color = ImPlot::SampleColormap(hue, ImPlotColormap_Viridis);

      ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 4.0f);
      ImPlot::SetNextLineStyle(color, 1.0f);
      ImPlot::PlotLine("##hovered", pdfs[hovered_idx].x, pdfs[hovered_idx].y,
                       static_cast<int>(pdfs[hovered_idx].n));
      ImPlot::PopStyleVar();
    }

    ImPlot::EndPlot();
  }

  // Tooltip
  if (hovered_idx >= 0 && min_dist_sq < 0.001) {
    ImGui::BeginTooltip();
    get_tooltip(hovered_idx);
    ImGui::EndTooltip();
  }

  return hovered_idx;
}

static void RenderPDFByMonth(const Dist &dist, int focus_month_idx) {
  ImGui::BeginChild("PDFByMonth", ImVec2(0, 0), true);
  ImGui::Text("[PDF by Month]");
  ImGui::Separator();

  if (dist.cache.empty()) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  int n_months = static_cast<int>(dist.cache.size());
  std::vector<PDFData> pdfs(n_months);

  for (int m = 0; m < n_months; ++m) {
    const auto &mc = dist.cache[m];
    if (mc.valid && mc.total.count() >= 10) {
      mc.total.exportPDF(pdfs[m].x, pdfs[m].y, pdfs[m].n);
      pdfs[m].label = mc.month;
    }
  }

  RenderPDFPlot("##PDFMonth", pdfs, focus_month_idx, [&](int idx) {
    const auto &kll = dist.cache[idx].total;
    ImGui::Text("%s", pdfs[idx].label.c_str());
    ImGui::Text("n=%zu", kll.count());
    ImGui::Text("mean=%.4f std=%.4f", kll.mean(), std::sqrt(kll.var()));
    ImGui::Text("skew=%.4f kurt=%.4f", kll.skew(), kll.kurt());
  });

  ImGui::EndChild();
}

static void RenderPDFByWeekday(const Dist &dist, int focus_month_idx) {
  ImGui::BeginChild("PDFByWeekday", ImVec2(0, 0), true);
  ImGui::Text("[PDF by Weekday]");
  ImGui::Separator();

  if (dist.cache.empty() || focus_month_idx < 0 ||
      focus_month_idx >= static_cast<int>(dist.cache.size())) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  const auto &month_cache = dist.cache[focus_month_idx];
  if (!month_cache.valid || month_cache.by_weekday.size() != 7) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  const char *wd_names[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
  std::vector<PDFData> pdfs(7);

  for (size_t wd = 0; wd < 7; ++wd) {
    if (month_cache.by_weekday[wd].count() >= 10) {
      month_cache.by_weekday[wd].exportPDF(pdfs[wd].x, pdfs[wd].y, pdfs[wd].n);
      pdfs[wd].label = wd_names[wd];
    }
  }

  RenderPDFPlot("##PDFWeekday", pdfs, -1, [&](int idx) {
    const auto &kll = month_cache.by_weekday[idx];
    ImGui::Text("%s", wd_names[idx]);
    ImGui::Text("n=%zu", kll.count());
    ImGui::Text("mean=%.4f std=%.4f", kll.mean(), std::sqrt(kll.var()));
    ImGui::Text("skew=%.4f kurt=%.4f", kll.skew(), kll.kurt());
  });

  ImGui::EndChild();
}

static void RenderPDFByHour(const Dist &dist, int focus_month_idx) {
  ImGui::BeginChild("PDFByHour", ImVec2(0, 0), true);
  ImGui::Text("[PDF by Hour]");
  ImGui::Separator();

  if (dist.cache.empty() || focus_month_idx < 0 ||
      focus_month_idx >= static_cast<int>(dist.cache.size())) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  const auto &month_cache = dist.cache[focus_month_idx];
  if (!month_cache.valid || month_cache.by_hour.size() != 24) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  std::vector<PDFData> pdfs(24);

  for (size_t h = 0; h < 24; ++h) {
    if (month_cache.by_hour[h].count() >= 10) {
      month_cache.by_hour[h].exportPDF(pdfs[h].x, pdfs[h].y, pdfs[h].n);
      pdfs[h].label = "Hour " + std::to_string(h);
    }
  }

  RenderPDFPlot("##PDFHour", pdfs, -1, [&](int idx) {
    const auto &kll = month_cache.by_hour[idx];
    ImGui::Text("Hour %d", idx);
    ImGui::Text("n=%zu", kll.count());
    ImGui::Text("mean=%.4f std=%.4f", kll.mean(), std::sqrt(kll.var()));
    ImGui::Text("skew=%.4f kurt=%.4f", kll.skew(), kll.kurt());
  });

  ImGui::EndChild();
}

// ============================================================================
// Trajectory Plot
// ============================================================================

static void RenderTrajectory(Dist &dist, const Asset &asset, DistUIState &ui, int focus_month_idx) {
  ImGui::Text("[Cross-Section Distribution Trajectory]");
  ImGui::Separator();

  if (!dist.trajectory.valid || dist.trajectory.paths.empty()) {
    ImGui::Text("No trajectory data");
    return;
  }

  if (ImPlot::BeginPlot("##Trajectory", ImVec2(-1, -1))) {
    ImPlot::SetupAxes("Robust Skewness", "Tail Thickness");

    size_t n_assets = dist.trajectory.n_assets;
    size_t n_months = dist.trajectory.months.size();

    ui.hovered_asset = -1;
    double min_hover_dist = 1e9;

    for (size_t a = 0; a < n_assets; ++a) {
      const auto &path = dist.trajectory.paths[a];
      if (path.empty())
        continue;

      // Collect valid points with alpha based on distance from focus
      std::vector<double> xs, ys;
      std::vector<float> alphas;
      std::vector<int> month_indices;

      for (size_t m = 0; m < n_months; ++m) {
        const auto &pt = path[m];
        if (pt.n < 10)
          continue;

        xs.push_back(pt.x);
        ys.push_back(pt.y);
        month_indices.push_back(static_cast<int>(m));
        alphas.push_back(compute_focus_alpha(static_cast<int>(m), focus_month_idx));
      }

      if (xs.empty())
        continue;

      std::string label =
          a < asset.items.size() ? asset.items[a].asset_code : std::to_string(a);

      // Color by peakedness (average)
      float avg_color = 0.0f;
      for (const auto &pt : path) {
        avg_color += pt.color;
      }
      avg_color /= path.size();
      ImVec4 color = ImPlot::SampleColormap(avg_color);

      // Draw points individually with different alphas
      for (size_t i = 0; i < xs.size(); ++i) {
        ImVec4 alpha_color = color;
        alpha_color.w = alphas[i];
        float marker_size = (month_indices[i] == focus_month_idx) ? 6.0f : 4.0f;

        ImPlot::SetNextMarkerStyle(ImPlotMarker_Circle, marker_size, alpha_color, alphas[i], alpha_color);
        ImPlot::PlotScatter(("##pt" + std::to_string(a) + "_" + std::to_string(i)).c_str(),
                            &xs[i], &ys[i], 1);
      }

      // Draw path lines with alpha
      for (size_t i = 0; i + 1 < xs.size(); ++i) {
        float line_alpha = std::min(alphas[i], alphas[i + 1]);
        ImPlot::SetNextLineStyle(color, line_alpha);
        double seg_x[2] = {xs[i], xs[i + 1]};
        double seg_y[2] = {ys[i], ys[i + 1]};
        ImPlot::PlotLine(("##path" + std::to_string(a) + "_" + std::to_string(i)).c_str(),
                         seg_x, seg_y, 2);
      }

      // Check hover
      if (ImPlot::IsPlotHovered()) {
        ImPlotPoint mouse = ImPlot::GetPlotMousePos();
        for (size_t i = 0; i < xs.size(); ++i) {
          double dx = mouse.x - xs[i];
          double dy = mouse.y - ys[i];
          double dist = dx * dx + dy * dy;
          if (dist < min_hover_dist && dist < 0.01) {
            min_hover_dist = dist;
            ui.hovered_asset = static_cast<int>(a);
          }
        }
      }
    }

    ImPlot::EndPlot();
  }

  // Tooltip
  if (ui.hovered_asset >= 0 &&
      static_cast<size_t>(ui.hovered_asset) < asset.items.size()) {
    ImGui::BeginTooltip();
    ImGui::Text("Asset: %s", asset.items[ui.hovered_asset].asset_code.c_str());
    ImGui::EndTooltip();
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
  ImGui::BeginChild("WindowCtrl", ImVec2(0, 50), true);
  RenderWindowControl(service, data, ui);
  ImGui::EndChild();

  // Main content: Left (Moments) + Right (PDFs + Trajectory)
  float content_height = ImGui::GetContentRegionAvail().y;
  ImGui::Columns(2, "MainCols", true);
  ImGui::SetColumnWidth(0, 350);

  // Left column: Moments Panel
  ImGui::BeginChild("MomentsSection", ImVec2(0, content_height), false);

  // Find focus bin index for MONTH grouping
  int focus_bin_idx = -1;
  if (ui.group_by == 3 && ui.focus_month_idx >= 0) {
    focus_bin_idx = ui.focus_month_idx;
  }

  RenderMomentsPanel(dist, focus_bin_idx);
  ImGui::EndChild();

  ImGui::NextColumn();

  // Right column: PDF panels + Trajectory
  ImGui::BeginChild("RightSection", ImVec2(0, content_height), false);

  // Top: Three PDF panels in a row
  float pdf_height = content_height * 0.5f;
  ImGui::BeginChild("PDFRow", ImVec2(0, pdf_height), false);
  ImGui::Columns(3, "PDFCols", false);

  // PDF by Month (focus month only)
  RenderPDFByMonth(dist, ui.focus_month_idx);
  ImGui::NextColumn();

  // PDF by Weekday (from focus month)
  RenderPDFByWeekday(dist, ui.focus_month_idx);
  ImGui::NextColumn();

  // PDF by Hour (from focus month)
  RenderPDFByHour(dist, ui.focus_month_idx);

  ImGui::Columns(1);
  ImGui::EndChild();

  // Bottom: Trajectory
  ImGui::BeginChild("TrajectorySection", ImVec2(0, 0), true);
  RenderTrajectory(dist, data.asset, ui, ui.focus_month_idx);
  ImGui::EndChild();

  ImGui::EndChild();

  ImGui::Columns(1);
}

void StopTabDist(DistService *service, SharedData &data) {
  service->StopCompute(data.gui.Coro(), data);
}

} // namespace GUI::Features
