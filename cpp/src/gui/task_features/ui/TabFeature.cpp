// Tab Feature Implementation
#include "gui/task_features/ui/TabFeature.hpp"
#include "shared/Feature.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

namespace GUI::Features {

// ============================================================================
// Helper Functions
// ============================================================================

// Chinese name mappings
static const char *to_string_cn(FeatureDataType type) {
  switch (type) {
  case FeatureDataType::TS:
    return "时序";
  case FeatureDataType::CS:
    return "截面";
  case FeatureDataType::LB:
    return "标签";
  case FeatureDataType::SH:
    return "共享";
  case FeatureDataType::META:
    return "元数据";
  }
  return "未知";
}

static const char *to_string_cn(FeatureCategoryL1 cat) {
  switch (cat) {
  case FeatureCategoryL1::PRICE:
    return "价格";
  case FeatureCategoryL1::VOLUME:
    return "量能";
  case FeatureCategoryL1::VOLATILITY:
    return "波动率";
  case FeatureCategoryL1::MOMENTUM:
    return "动量";
  case FeatureCategoryL1::LIQUIDITY:
    return "流动性";
  case FeatureCategoryL1::IMBALANCE:
    return "失衡";
  case FeatureCategoryL1::MICROSTRUCTURE:
    return "微结构";
  case FeatureCategoryL1::LABEL:
    return "标签";
  case FeatureCategoryL1::META:
    return "元数据";
  }
  return "未知";
}

static const char *to_string_cn(FeatureCategoryL2 cat) {
  switch (cat) {
  case FeatureCategoryL2::RAW:
    return "原始";
  case FeatureCategoryL2::NORMALIZED:
    return "标准化";
  case FeatureCategoryL2::OSCILLATOR:
    return "震荡器";
  case FeatureCategoryL2::DEVIATION:
    return "偏离";
  case FeatureCategoryL2::RATIO:
    return "比率";
  case FeatureCategoryL2::RANK:
    return "排名";
  case FeatureCategoryL2::FUTURE_RET:
    return "未来收益";
  case FeatureCategoryL2::SCORE:
    return "评分";
  case FeatureCategoryL2::UNIVERSE:
    return "全域统计";
  case FeatureCategoryL2::BENCHMARK:
    return "基准";
  }
  return "未知";
}

static const char *to_string_cn(NormMethod method) {
  switch (method) {
  case NormMethod::NONE:
    return "无";
  case NormMethod::ZSCORE:
    return "Z标准化";
  case NormMethod::RANK_NORM:
    return "排名标准化";
  case NormMethod::CLIP:
    return "截断";
  case NormMethod::TANH:
    return "双曲正切";
  case NormMethod::WINSOR:
    return "缩尾";
  case NormMethod::LOG_NORM:
    return "对数标准化";
  case NormMethod::PCT_RANK:
    return "百分位排名";
  }
  return "未知";
}

// Get current level features based on selection
static const std::vector<FeatureMetadata> &get_current_level_features(const Feature &feature) {
  switch (feature.selection.selected_level) {
  case 0:
    return feature.metadata.features_l0;
  case 1:
    return feature.metadata.features_l1;
  case 2:
    return feature.metadata.features_l2;
  default:
    return feature.metadata.features_l0;
  }
}

// Filter features based on current filter settings
static std::vector<int> get_filtered_indices(const Feature::Selection &sel, const std::vector<FeatureMetadata> &features) {
  std::vector<int> result;
  for (int i = 0; i < (int)features.size(); ++i) {
    bool pass = true;

    // Filter by data_type
    if (!sel.filter_data_type.empty() && sel.filter_data_type.find(features[i].data_type) == sel.filter_data_type.end())
      pass = false;

    // Filter by cat_l1
    if (!sel.filter_cat_l1.empty() && sel.filter_cat_l1.find(features[i].cat_l1) == sel.filter_cat_l1.end())
      pass = false;

    // Filter by cat_l2
    if (!sel.filter_cat_l2.empty() && sel.filter_cat_l2.find(features[i].cat_l2) == sel.filter_cat_l2.end())
      pass = false;

    // Filter by norm_method
    if (!sel.filter_norm_method.empty() && sel.filter_norm_method.find(features[i].norm_method) == sel.filter_norm_method.end())
      pass = false;

    if (pass)
      result.push_back(i);
  }
  return result;
}

// ============================================================================
// UI Components
// ============================================================================

// Render multi-select dropdown for filters
template <typename EnumType, typename ToStringFunc, typename ToStringCnFunc>
static void render_filter_dropdown(const char *label, bool &show_dropdown, std::set<EnumType> &selected_values,
                                   ToStringFunc to_string_func, ToStringCnFunc to_string_cn_func, int num_values) {
  ImGui::Text("%s:", label);
  ImGui::SameLine();

  // Display selected count or "All"
  char button_label[128];
  if (selected_values.empty()) {
    snprintf(button_label, sizeof(button_label), "All###%s", label);
  } else {
    snprintf(button_label, sizeof(button_label), "%d###%s", (int)selected_values.size(), label);
  }

  if (ImGui::Button(button_label, ImVec2(80, 0))) {
    show_dropdown = !show_dropdown;
  }

  // Show popup for multi-select
  if (show_dropdown) {
    ImGui::SetNextWindowPos(ImVec2(ImGui::GetItemRectMin().x, ImGui::GetItemRectMax().y));
    ImGui::Begin(label, &show_dropdown, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_AlwaysAutoResize);

    for (int i = 0; i < num_values; ++i) {
      EnumType value = static_cast<EnumType>(i);
      const char *name_en = to_string_func(value);
      const char *name_cn = to_string_cn_func(value);
      char display_name[128];
      snprintf(display_name, sizeof(display_name), "%s (%s)", name_en, name_cn);

      bool is_selected = selected_values.find(value) != selected_values.end();

      if (ImGui::Checkbox(display_name, &is_selected)) {
        if (is_selected) {
          selected_values.insert(value);
        } else {
          selected_values.erase(value);
        }
      }
    }

    ImGui::End();
  }
}

// ============================================================================
// Main Render Function
// ============================================================================

void RenderTabFeature(SharedData &data, FeatureUIState &ui_state) {
  Feature &feature = data.feature;
  Feature::Selection &sel = feature.selection;

  // Initialize default filters: TS and CS
  static bool first_time = true;
  if (first_time) {
    sel.filter_data_type.insert(FeatureDataType::TS);
    sel.filter_data_type.insert(FeatureDataType::CS);
    first_time = false;
  }

  // ==========================================================================
  // Section 1: Level Selection
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "1. Level:");
  ImGui::SameLine();

  bool level_changed = false;
  int prev_level = sel.selected_level;

  ImGui::RadioButton("L0", &sel.selected_level, 0);
  ImGui::SameLine();
  ImGui::RadioButton("L1", &sel.selected_level, 1);
  ImGui::SameLine();
  ImGui::RadioButton("L2", &sel.selected_level, 2);

  level_changed = (sel.selected_level != prev_level);

  // Clear selection when level changes
  if (level_changed) {
    sel.primary_feature_idx = -1;
    sel.secondary_features.clear();
  }

  ImGui::Separator();

  // ==========================================================================
  // Section 2: Filters
  // ==========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "2. Filters:");
  ImGui::SameLine();

  // All filters in one line
  render_filter_dropdown("DataType", ui_state.show_filter_data_type, sel.filter_data_type, [](FeatureDataType t) { return to_string(t); }, [](FeatureDataType t) { return to_string_cn(t); }, 5);
  ImGui::SameLine();
  render_filter_dropdown("Cat L1", ui_state.show_filter_cat_l1, sel.filter_cat_l1, [](FeatureCategoryL1 t) { return to_string(t); }, [](FeatureCategoryL1 t) { return to_string_cn(t); }, 9);
  ImGui::SameLine();
  render_filter_dropdown("Cat L2", ui_state.show_filter_cat_l2, sel.filter_cat_l2, [](FeatureCategoryL2 t) { return to_string(t); }, [](FeatureCategoryL2 t) { return to_string_cn(t); }, 10);
  ImGui::SameLine();
  render_filter_dropdown("Norm", ui_state.show_filter_norm_method, sel.filter_norm_method, [](NormMethod t) { return to_string(t); }, [](NormMethod t) { return to_string_cn(t); }, 8);
  ImGui::SameLine();

  // Reset filters button
  if (ImGui::Button("Reset", ImVec2(60, 0))) {
    sel.filter_data_type.clear();
    sel.filter_cat_l1.clear();
    sel.filter_cat_l2.clear();
    sel.filter_norm_method.clear();
  }

  ImGui::Separator();

  // ==========================================================================
  // Section 3: Feature Table
  // ==========================================================================
  const auto &features = get_current_level_features(feature);
  auto filtered_indices = get_filtered_indices(sel, features);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "3. Features:");
  ImGui::SameLine();
  ImGui::Text("Showing %d / %d", (int)filtered_indices.size(), (int)features.size());

  // Feature table
  if (ImGui::BeginTable("FeatureTable", 8,
                        ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY |
                            ImGuiTableFlags_Resizable | ImGuiTableFlags_Sortable,
                        ImVec2(0, 400))) {

    // Table headers
    ImGui::TableSetupColumn("Primary", ImGuiTableColumnFlags_WidthFixed, 60.0f);
    ImGui::TableSetupColumn("Multi", ImGuiTableColumnFlags_WidthFixed, 50.0f);
    ImGui::TableSetupColumn("Code", ImGuiTableColumnFlags_WidthFixed, 150.0f);
    ImGui::TableSetupColumn("Width", ImGuiTableColumnFlags_WidthFixed, 50.0f);
    ImGui::TableSetupColumn("DataType", ImGuiTableColumnFlags_WidthFixed, 70.0f);
    ImGui::TableSetupColumn("Cat L1", ImGuiTableColumnFlags_WidthFixed, 120.0f);
    ImGui::TableSetupColumn("Cat L2", ImGuiTableColumnFlags_WidthFixed, 120.0f);
    ImGui::TableSetupColumn("Name CN", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableSetupScrollFreeze(0, 1); // Freeze header row

    // Custom header row with tooltips
    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    const char *headers[] = {"Primary", "Multi", "Code", "Width", "DataType", "Cat L1", "Cat L2", "Name CN"};
    const char *tooltips[] = {
        "主特征：用于分析的主要特征",
        "多选：选择多个特征进行对比",
        "代码：特征的唯一标识符",
        "宽度：特征的维度数量",
        "数据类型: TS=时序, CS=截面, LB=标签, SH=共享, META=元数据",
        "一级分类：特征的主要类别",
        "二级分类：特征的子类别",
        "中文名称：特征的描述性名称"};

    for (int column = 0; column < 8; column++) {
      ImGui::TableSetColumnIndex(column);
      ImGui::TableHeader(headers[column]);
      if (ImGui::IsItemHovered()) {
        ImGui::SetTooltip("%s", tooltips[column]);
      }
    }

    // Table rows
    for (int idx : filtered_indices) {
      const FeatureMetadata &f = features[idx];

      // Check if this row is selected
      bool is_primary = (sel.primary_feature_idx == idx);
      bool is_secondary = (sel.secondary_features.find(idx) != sel.secondary_features.end());
      bool is_selected = is_primary || is_secondary;

      ImGui::TableNextRow();

      // Highlight selected rows
      if (is_selected) {
        ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg0, ImGui::GetColorU32(ImVec4(0.2f, 0.4f, 0.6f, 0.3f)));
      }

      // Column: Primary (RadioButton)
      ImGui::TableNextColumn();
      char radio_label[32];
      snprintf(radio_label, sizeof(radio_label), "##primary_%d", idx);
      if (ImGui::RadioButton(radio_label, sel.primary_feature_idx == idx)) {
        sel.primary_feature_idx = idx;
        // Remove from secondary if present
        sel.secondary_features.erase(idx);
      }

      // Column: Multi (Checkbox)
      ImGui::TableNextColumn();
      char check_label[32];
      snprintf(check_label, sizeof(check_label), "##multi_%d", idx);
      bool is_multi_checked = is_secondary;
      if (ImGui::Checkbox(check_label, &is_multi_checked)) {
        if (is_multi_checked) {
          // Add to secondary only if not primary
          if (sel.primary_feature_idx != idx) {
            sel.secondary_features.insert(idx);
          }
        } else {
          sel.secondary_features.erase(idx);
        }
      }

      // Column: Code
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(f.code);

      // Column: Width
      ImGui::TableNextColumn();
      ImGui::Text("%d", f.width);

      // Column: DataType
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(to_string(f.data_type));

      // Column: Cat L1
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(to_string(f.cat_l1));

      // Column: Cat L2
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(to_string(f.cat_l2));

      // Column: Name CN (with tooltip)
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(f.name_cn);

      // Tooltip on hover (show long fields)
      if (ImGui::IsItemHovered()) {
        ImGui::BeginTooltip();
        ImGui::PushTextWrapPos(ImGui::GetFontSize() * 35.0f);
        ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%s", f.name_en);
        ImGui::Separator();
        ImGui::Text("Formula:");
        ImGui::TextWrapped("%s", f.formula);
        ImGui::Spacing();
        ImGui::Text("Description:");
        ImGui::TextWrapped("%s", f.description);
        ImGui::Spacing();
        ImGui::Text("Norm Method: %s", to_string(f.norm_method));
        ImGui::PopTextWrapPos();
        ImGui::EndTooltip();
      }
    }

    ImGui::EndTable();
  }

  // Select all filtered button
  if (ImGui::Button("Select All Multi", ImVec2(120, 0))) {
    for (int idx : filtered_indices) {
      if (sel.primary_feature_idx != idx) {
        sel.secondary_features.insert(idx);
      }
    }
  }
  ImGui::SameLine();
  if (ImGui::Button("Clear All", ImVec2(80, 0))) {
    sel.primary_feature_idx = -1;
    sel.secondary_features.clear();
  }
}

} // namespace GUI::Features
