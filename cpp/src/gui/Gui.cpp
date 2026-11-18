#include "gui/Gui.hpp"
#include "gui/Tasks.hpp"
#include "gui/icon_bar/IconBar.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"
#include <cassert>
#include <cstdio>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

namespace GUI {

// 行状态标签: 行名后画 [text], 颜色查全局色表
static void DrawStatus(const TaskStatus &s) {
  if (s.kind == TaskStatus::Kind::None || s.text.empty())
    return;
  ImGui::SameLine();
  ImGui::TextColored(StatusColor(s.kind), "[%s]", s.text.c_str());
}

// idx == -1 任务行, >= 0 子行
static void DrawRowStatus(const SharedData &data, const TaskHandle &t, int idx) { DrawStatus(RowStatus(data, t, idx)); }

void LoadFonts(float dpi_scale) {
  const char *main_path = "fonts/MapleMonoNormal-NF-CN-Regular.ttf";
  const char *fallback_path = "fonts/DroidSansFallbackFull.ttf";
  assert(std::ifstream(main_path).good() && "Font file not found!");
  assert(std::ifstream(fallback_path).good() && "Fallback font file not found!");

  const float base_font_size = 16.0f;
  const float font_size = base_font_size * dpi_scale;
  std::printf("Font: %.1fpx (base: %.1f, DPI: %.2f, physical pixels)\n", font_size, base_font_size, dpi_scale);

  ImGuiIO &io = ImGui::GetIO();
  io.Fonts->Clear();
  ImFontConfig config;
  config.PixelSnapH = false; // Better for CJK
  config.OversampleH = 1;
  config.OversampleV = 1;
  config.RasterizerDensity = dpi_scale; // Key: high-res font rendering
  [[maybe_unused]] const ImFont *font = io.Fonts->AddFontFromFileTTF(main_path, font_size, &config);
  assert(font);
  config.MergeMode = true;
  font = io.Fonts->AddFontFromFileTTF(fallback_path, font_size, &config);
  assert(font);
}

// Shared business logic: Draw GUI layout (called by both OpenGL and Vulkan pipelines)
void DrawGUILayout(SharedData &data, TaskTree &tree) {

  // 帧首: 全部任务 Update (无论选中) —— 状态标签/使能同帧就绪,
  // 左栏不依赖 "打开过页面" 的上一帧缓存
  for (auto &t : tree.tasks)
    if (t.Update)
      t.Update(data);

  // Get window size
  int display_w = (int)ImGui::GetIO().DisplaySize.x;
  int display_h = (int)ImGui::GetIO().DisplaySize.y;

  // Left panel: 多层选择栏 (任务树, 叶子=可选项; 纯渲染, 不认识任何具体任务)
  ImGui::SetNextWindowPos(ImVec2(0, 0));
  ImGui::SetNextWindowSize(ImVec2(200, display_h));
  ImGui::Begin("Tasks", nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  // Task tree (leave minimal space for compact icon bar)
  ImGui::BeginChild("TaskList", ImVec2(0, -ImGui::GetTextLineHeightWithSpacing() * 1.2f), false);
  const int task0 = tree.selected, tab0 = tree.tasks[task0].selected_tab;

  for (int i = 0; i < (int)tree.tasks.size(); i++) {
    TaskHandle &t = tree.tasks[i];

    if (t.tabs.empty()) {
      // 无子项: 直接作为叶子
      bool enabled = t.Enabled ? t.Enabled(data, -1) : true;
      bool is_selected = (tree.selected == i);
      char name_label[256];
      snprintf(name_label, sizeof(name_label), "> %s###Task_%d", t.name.c_str(), i);
      if (!enabled)
        ImGui::BeginDisabled();
      if (ImGui::Selectable(name_label, is_selected)) {
        tree.Select(i, -1);
      }
      if (!enabled)
        ImGui::EndDisabled();
      DrawRowStatus(data, t, -1);
    } else {
      // 有子项: 父节点可展开 (本身不可选), 子项为叶子 (按 '/' 分组的收进组节点, 组节点同样只折叠不可选)
      char header_label[256];
      snprintf(header_label, sizeof(header_label), "%s###TaskHeader_%d", t.name.c_str(), i);
      // 父节点选中态: 任一子项被选中时高亮父节点
      ImGuiTreeNodeFlags flags = ImGuiTreeNodeFlags_DefaultOpen |
                                 ImGuiTreeNodeFlags_FramePadding;
      if (tree.selected == i)
        flags |= ImGuiTreeNodeFlags_Selected;
      bool opened = ImGui::TreeNodeEx(header_label, flags);
      DrawRowStatus(data, t, -1);
      if (opened) {
        // path = 当前所在的各层组 (各为子行名的前缀, 含末尾 '/'); 其前 shown 层已展开, 其后折起 (子行不画)
        // 组行与子行同格式 ("  ▸ 组名" 对 "  > 叶名", 与同级对齐), 点击切折叠; 展开态存窗口状态表
        std::vector<std::string_view> path;
        size_t shown = 0;
        for (int j = 0; j < (int)t.tabs.size(); j++) {
          const std::string_view name = t.tabs[j];
          std::vector<std::string_view> groups;
          for (size_t p = name.find('/'); p != std::string_view::npos; p = name.find('/', p + 1))
            groups.push_back(name.substr(0, p + 1));
          size_t k = 0;
          while (k < path.size() && k < groups.size() && path[k] == groups[k])
            ++k;
          for (; path.size() > k; path.pop_back())
            if (shown == path.size())
              ImGui::Unindent(), --shown;
          while (path.size() < groups.size()) {
            const bool visible = shown == path.size();
            const size_t from = path.empty() ? 0 : path.back().size();
            const std::string_view g = groups[path.size()];
            path.push_back(g);
            if (!visible)
              continue;
            int hi = j;
            while (hi < (int)t.tabs.size() && std::string_view(t.tabs[hi]).starts_with(g))
              ++hi;
            ImGuiStorage *st = ImGui::GetStateStorage();
            // 组的身份 = 整段前缀 g (外层组与其首个子组同起于子行 j, 不能拿 j 作键)
            const ImGuiID key = ImGui::GetID(("TaskGroupOpen_" + std::to_string(i) + "_" + std::string(g)).c_str());
            const bool open = st->GetBool(key, path.size() > 1);
            char group_label[256];
            snprintf(group_label, sizeof(group_label), "  %s %.*s###TaskGroup_%d_%.*s", open ? "▾" : "▸", (int)(g.size() - 1 - from), g.data() + from, i, (int)g.size(), g.data());
            if (ImGui::Selectable(group_label, tree.selected == i && t.selected_tab >= j && t.selected_tab < hi))
              st->SetBool(key, !open);
            DrawStatus(SpanStatus(data, t, j, hi));
            if (open)
              ImGui::Indent(), ++shown;
          }
          if (shown != path.size())
            continue;
          const std::string_view leaf = name.substr(groups.empty() ? 0 : groups.back().size());
          bool enabled = t.Enabled ? t.Enabled(data, j) : true;
          bool is_selected = (tree.selected == i && t.selected_tab == j);
          char child_label[256];
          snprintf(child_label, sizeof(child_label), "  > %.*s###%d_%d", (int)leaf.size(), leaf.data(), i, j);
          if (!enabled)
            ImGui::BeginDisabled();
          if (ImGui::Selectable(child_label, is_selected)) {
            tree.Select(i, j);
          }
          if (!enabled) {
            if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
              ImGui::SetTooltip("Tab not ready");
            ImGui::EndDisabled();
          }
          DrawRowStatus(data, t, j);
        }
        for (; shown > 0; --shown)
          ImGui::Unindent();
        ImGui::TreePop();
      }
    }
  }
  if (data.reinit.on_leave && (tree.selected != task0 || tree.tasks[tree.selected].selected_tab != tab0))
    data.reinit.requested = true; // 下一帧首重启, 留在新选中的页

  ImGui::EndChild();

  // Icon bar at bottom
  ImGui::Separator();
  IconBar::Draw();

  ImGui::End();

  // Right panel: Selected leaf content (full height)
  ImGui::SetNextWindowPos(ImVec2(200, 0));
  ImGui::SetNextWindowSize(ImVec2(display_w - 200, display_h));
  char panel_title[64];
  snprintf(panel_title, sizeof(panel_title), "Panel (%dx%d)###Panel", display_w, display_h);
  ImGui::Begin(panel_title, nullptr, ImGuiWindowFlags_NoMove | ImGuiWindowFlags_NoResize | ImGuiWindowFlags_NoCollapse);

  if (tree.selected >= 0 && tree.selected < (int)tree.tasks.size()) {
    TaskHandle &t = tree.tasks[tree.selected];
    int idx = t.selected_tab;
    if (!t.tabs.empty() && (idx < 0 || idx >= (int)t.tabs.size()))
      idx = 0;
    assert(t.Draw && "task must provide Draw");
    if (t.Enabled && !t.Enabled(data, idx)) // 选中后才变不可用 (进程内重启回到原页, 输入还没就绪)
      ImGui::TextDisabled("Tab not ready");
    else
      t.Draw(data, idx);
  }

  ImGui::End();
}

} // namespace GUI
