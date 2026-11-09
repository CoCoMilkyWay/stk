#include "gui/task_features/ui/TabNodeTime.hpp"
#include "features/Storage/FeatureLib.hpp" // lib_dir / lib_name
#include "gui/task_features/ui/Common.hpp" // 作用域条
#include "shared/Feature.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace GUI::Features {

// ============================================================================
// Node Time: 当前库 <库目录>/node_time.json (Rebuild / Append 时计时, 每轮覆盖) 按节点文件归并.
//   时序 / 截面两段各自成 100% (不同 worker): 时序 = 抽样 asset-day 的节点 + 标签 + 写张量 + 源与调度; 截面 = 逐列 + 调度
// ============================================================================

struct NodeTimeFile {
  struct Node {
    std::string name, file;
    uint64_t cycles;
  };
  struct Col {
    size_t level;
    std::string code;
    uint64_t cycles;
  };
  std::string path;
  std::filesystem::file_time_type mtime{};
  double checked = -1.0; // 上次查 mtime 的时刻 (限速 1s)
  bool ok = false;       // 文件在且已读
  uint64_t ts_samples = 0, ts_total = 0, ts_label = 0, ts_write = 0;
  std::vector<Node> nodes;
  uint64_t cs_days = 0, cs_total = 0;
  std::vector<Col> cols;
};

static void load_node_time(NodeTimeFile &nt, const std::string &path) {
  const double now = ImGui::GetTime();
  if (nt.path == path && now - nt.checked < 1.0)
    return;
  nt.checked = now;
  const bool same = nt.path == path;
  nt.path = path;
  if (!std::filesystem::exists(path)) {
    nt.ok = false;
    return;
  }
  const std::filesystem::file_time_type mtime = std::filesystem::last_write_time(path);
  if (same && nt.ok && mtime == nt.mtime)
    return;
  std::ifstream f(path);
  assert(f.is_open() && "node_time.json: 打不开");
  const nlohmann::json j = nlohmann::json::parse(f);
  nt.mtime = mtime;
  nt.ok = true;
  const nlohmann::json &ts = j.at("ts"), &cs = j.at("cs");
  nt.ts_samples = ts.at("samples").get<uint64_t>();
  nt.ts_total = ts.at("total").get<uint64_t>();
  nt.ts_label = ts.at("label").get<uint64_t>();
  nt.ts_write = ts.at("write").get<uint64_t>();
  nt.nodes.clear();
  for (const nlohmann::json &n : ts.at("nodes"))
    nt.nodes.push_back({n.at("node").get<std::string>(), n.at("file").get<std::string>(), n.at("cycles").get<uint64_t>()});
  nt.cs_days = cs.at("days").get<uint64_t>();
  nt.cs_total = cs.at("total").get<uint64_t>();
  nt.cols.clear();
  for (const nlohmann::json &c : cs.at("cols"))
    nt.cols.push_back({c.at("level").get<size_t>(), c.at("code").get<std::string>(), c.at("cycles").get<uint64_t>()});
}

// "features/Node/Bar/TS/Basic/Kline.hpp" → "Bar/TS/Basic/Kline"
static std::string_view short_node_file(std::string_view f) {
  constexpr std::string_view pre = "features/Node/", ext = ".hpp";
  if (f.starts_with(pre))
    f.remove_prefix(pre.size());
  if (f.ends_with(ext))
    f.remove_suffix(ext.size());
  return f;
}

// 一行 = 一个节点文件 (file 非空) 或一段非节点用时 (file 空, 显示 name); items = 节点 / 截面列 (名, 周期)
struct NodeTimeRow {
  std::string_view file;
  std::string name;
  uint64_t cycles = 0;
  std::vector<std::pair<std::string, uint64_t>> items;
};

static NodeTimeRow &node_time_row(std::vector<NodeTimeRow> &rows, std::string_view file) {
  for (NodeTimeRow &r : rows)
    if (!r.file.empty() && r.file == file)
      return r;
  return rows.emplace_back(NodeTimeRow{file, std::string(short_node_file(file))});
}

// 一段一条通栏堆叠条 + 图例: 色块宽 = 周期 / total (只看本层时余下留空), 行与 items 按周期降序.
//   一个文件一个色系 (文件行按序取黄金角色相, 非节点行灰), 文件内节点 / 列由亮到暗; 色块放得下就写名字 (+ 占比).
//   悬停色块 / 图例 → 该文件高亮 (其余变暗) + 明细浮窗; 图例画在条之后, 条用上一帧的图例悬停
static void render_node_time_bar(const char *id, std::vector<NodeTimeRow> &rows, uint64_t total) {
  for (NodeTimeRow &r : rows)
    std::sort(r.items.begin(), r.items.end(), [](const auto &a, const auto &b) { return a.second > b.second; });
  std::stable_sort(rows.begin(), rows.end(), [](const NodeTimeRow &a, const NodeTimeRow &b) { return a.cycles > b.cycles; });
  const auto share = [&](uint64_t c) { return total > 0 ? static_cast<float>(static_cast<double>(c) / static_cast<double>(total)) : 0.0f; };

  std::vector<float> hue(rows.size());
  for (size_t r = 0, k = 0; r < rows.size(); ++r)
    hue[r] = rows[r].file.empty() ? -1.0f : std::fmod(0.61803398875f * static_cast<float>(k++), 1.0f);
  const auto color = [&](size_t r, size_t j, float alpha) {
    const size_t n = rows[r].items.size();
    const float v = 0.92f - (n > 1 ? 0.38f * static_cast<float>(j) / static_cast<float>(n - 1) : 0.0f);
    float cr, cg, cb;
    ImGui::ColorConvertHSVtoRGB(std::max(hue[r], 0.0f), hue[r] < 0.0f ? 0.0f : 0.55f, hue[r] < 0.0f ? v * 0.75f : v, cr, cg, cb);
    return ImGui::GetColorU32(ImVec4(cr, cg, cb, alpha));
  };
  const auto tooltip = [&](size_t r, size_t hot_item) {
    ImGui::BeginTooltip();
    ImGui::TextColored(ImGui::ColorConvertU32ToFloat4(color(r, 0, 1.0f)), "%s", rows[r].name.c_str());
    ImGui::SameLine();
    ImGui::Text("%.1f%%", 100.0f * share(rows[r].cycles));
    if (!rows[r].file.empty())
      ImGui::TextDisabled("%s", std::string(rows[r].file).c_str());
    if (!rows[r].items.empty())
      ImGui::Separator();
    for (size_t j = 0; j < rows[r].items.size(); ++j) {
      const auto &[name, cycles] = rows[r].items[j];
      ImGui::PushID(static_cast<int>(j));
      ImGui::ColorButton("##sw", ImGui::ColorConvertU32ToFloat4(color(r, j, 1.0f)), ImGuiColorEditFlags_NoTooltip, ImVec2(10, 10));
      ImGui::PopID();
      ImGui::SameLine();
      if (j == hot_item)
        ImGui::Text("%s  %.1f%%", name.c_str(), 100.0f * share(cycles));
      else
        ImGui::TextDisabled("%s  %.1f%%", name.c_str(), 100.0f * share(cycles));
    }
    ImGui::EndTooltip();
  };

  ImGuiStorage *storage = ImGui::GetStateStorage();
  ImGui::PushID(id);
  const ImGuiID hot_id = ImGui::GetID("hot");
  ImGui::PopID();
  const int legend_hot = storage->GetInt(hot_id, -1);
  int hot = -1;

  // 条: 色块 = (文件行 r, item j; 无 items 的行一块)
  struct Seg {
    size_t r, j;
    float x0, x1;
  };
  const ImVec2 p0 = ImGui::GetCursorScreenPos();
  const float W = ImGui::GetContentRegionAvail().x, H = ImGui::GetFrameHeight() * 1.4f;
  ImGui::InvisibleButton(id, ImVec2(W, H));
  const bool bar_hovered = ImGui::IsItemHovered();
  std::vector<Seg> segs;
  std::vector<std::pair<float, float>> spans(rows.size()); // 文件行的横向范围
  double acc = 0.0;
  const auto x_at = [&](double c) { return p0.x + (total > 0 ? static_cast<float>(c / static_cast<double>(total)) * W : 0.0f); };
  for (size_t r = 0; r < rows.size(); ++r) {
    spans[r].first = x_at(acc);
    if (rows[r].items.empty()) {
      segs.push_back({r, 0, x_at(acc), x_at(acc + static_cast<double>(rows[r].cycles))});
      acc += static_cast<double>(rows[r].cycles);
    }
    for (size_t j = 0; j < rows[r].items.size(); ++j) {
      segs.push_back({r, j, x_at(acc), x_at(acc + static_cast<double>(rows[r].items[j].second))});
      acc += static_cast<double>(rows[r].items[j].second);
    }
    spans[r].second = x_at(acc);
  }
  size_t hot_seg = SIZE_MAX;
  if (bar_hovered)
    for (size_t k = 0; k < segs.size(); ++k)
      if (ImGui::GetIO().MousePos.x >= segs[k].x0 && ImGui::GetIO().MousePos.x < segs[k].x1) {
        hot_seg = k;
        hot = static_cast<int>(segs[k].r);
      }
  const int lit = hot >= 0 ? hot : legend_hot;

  ImDrawList *dl = ImGui::GetWindowDrawList();
  const float y0 = p0.y, y1 = p0.y + H;
  dl->AddRectFilled(ImVec2(p0.x, y0), ImVec2(p0.x + W, y1), ImGui::GetColorU32(ImGuiCol_FrameBg));
  const ImU32 ink = IM_COL32(20, 20, 20, 255), gap = ImGui::GetColorU32(ImGuiCol_WindowBg);
  for (const Seg &s : segs) {
    const bool dim = lit >= 0 && static_cast<int>(s.r) != lit;
    dl->AddRectFilled(ImVec2(s.x0, y0), ImVec2(s.x1, y1), color(s.r, s.j, dim ? 0.25f : 1.0f));
    if (dim)
      continue;
    const std::string &name = rows[s.r].items.empty() ? rows[s.r].name : rows[s.r].items[s.j].first;
    const uint64_t c = rows[s.r].items.empty() ? rows[s.r].cycles : rows[s.r].items[s.j].second;
    char text[160];
    std::snprintf(text, sizeof(text), "%s %.1f%%", name.c_str(), 100.0f * share(c));
    const char *shown = ImGui::CalcTextSize(text).x + 8.0f <= s.x1 - s.x0 ? text : ImGui::CalcTextSize(name.c_str()).x + 8.0f <= s.x1 - s.x0 ? name.c_str()
                                                                                                                                             : nullptr;
    if (shown)
      dl->AddText(ImVec2(s.x0 + 4.0f, y0 + 0.5f * (H - ImGui::GetTextLineHeight())), ink, shown);
  }
  for (size_t r = 1; r < rows.size(); ++r)
    dl->AddLine(ImVec2(spans[r].first, y0), ImVec2(spans[r].first, y1), gap, 2.0f);
  if (lit >= 0)
    dl->AddRect(ImVec2(spans[static_cast<size_t>(lit)].first, y0), ImVec2(spans[static_cast<size_t>(lit)].second, y1), IM_COL32(255, 255, 255, 230), 0.0f, 0, 2.0f);
  if (const float x = x_at(acc); x + 8.0f < p0.x + W) {
    char rest[48];
    std::snprintf(rest, sizeof(rest), "其余 %.1f%%", 100.0f * (1.0f - share(static_cast<uint64_t>(acc))));
    if (ImGui::CalcTextSize(rest).x + 8.0f <= p0.x + W - x)
      dl->AddText(ImVec2(x + 4.0f, y0 + 0.5f * (H - ImGui::GetTextLineHeight())), ImGui::GetColorU32(ImGuiCol_TextDisabled), rest);
  }
  if (hot_seg != SIZE_MAX)
    tooltip(segs[hot_seg].r, rows[segs[hot_seg].r].items.empty() ? SIZE_MAX : segs[hot_seg].j);

  // 图例: 色块 + 文件名 + 占比, 超宽折行
  for (size_t r = 0; r < rows.size(); ++r) {
    char pct[16];
    std::snprintf(pct, sizeof(pct), "%.1f%%", 100.0f * share(rows[r].cycles));
    const float sq = ImGui::GetTextLineHeight();
    const ImGuiStyle &st = ImGui::GetStyle();
    const float w = sq + ImGui::CalcTextSize(rows[r].name.c_str()).x + ImGui::CalcTextSize(pct).x + 2.0f * st.ItemInnerSpacing.x;
    if (r > 0) {
      ImGui::SameLine(0.0f, 14.0f);
      if (ImGui::GetContentRegionAvail().x < w)
        ImGui::NewLine();
    }
    const bool dim = lit >= 0 && static_cast<int>(r) != lit;
    ImGui::PushID(static_cast<int>(r));
    ImGui::PushStyleVar(ImGuiStyleVar_Alpha, dim ? 0.4f : 1.0f);
    ImGui::BeginGroup();
    ImGui::ColorButton("##sw", ImGui::ColorConvertU32ToFloat4(color(r, 0, 1.0f)), ImGuiColorEditFlags_NoTooltip, ImVec2(sq, sq));
    ImGui::SameLine(0.0f, st.ItemInnerSpacing.x);
    ImGui::TextUnformatted(rows[r].name.c_str());
    ImGui::SameLine(0.0f, st.ItemInnerSpacing.x);
    ImGui::TextDisabled("%s", pct);
    ImGui::EndGroup();
    ImGui::PopStyleVar();
    ImGui::PopID();
    if (ImGui::IsItemHovered()) {
      hot = static_cast<int>(r);
      tooltip(r, SIZE_MAX);
    }
  }
  storage->SetInt(hot_id, hot);
}

// 两段 (时序 / 截面) 各一条堆叠条, 各自成 100%. 全部 = 全档文件 + 非节点行 (合计 100%);
// 只看本层 = 只留本层字段所在文件 / 本层截面列, 去掉非节点行 (占比仍按本段全量, 节点可能同时服务别的层, 各层视图不互斥)
static void render_node_time(SharedData &data, const Feature &feature) {
  static NodeTimeFile s_nt;
  static bool s_level_only = false;
  const LibId lib = feature.selection.lib();
  load_node_time(s_nt, (std::filesystem::path(lib_dir(data.config.feature_dir, lib)) / "node_time.json").string());
  if (!s_nt.ok) {
    ImGui::TextDisabled("%s 尚无用时: Rebuild / Append 跑完即有", lib_name(lib).c_str());
    return;
  }
  const auto &meta = feature.meta().features;
  const size_t level = static_cast<size_t>(feature.selection.selected_level);
  std::vector<std::string_view> level_files;
  for (const FeatureMetadata &f : meta[level])
    if (std::find(level_files.begin(), level_files.end(), f.file) == level_files.end())
      level_files.push_back(f.file);
  const auto in_level = [&](std::string_view file) { return std::find(level_files.begin(), level_files.end(), file) != level_files.end(); };
  std::string_view label_file;
  for (const auto &fs : meta)
    for (const FeatureMetadata &f : fs)
      if (f.data_type == FeatureDataType::LB && label_file.empty())
        label_file = f.file;

  char toggle[48];
  std::snprintf(toggle, sizeof(toggle), "只看本层 (%s)", level_name(level));
  ImGui::Checkbox(toggle, &s_level_only);
  ImGui::SameLine();
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("TSC 周期, 时序 / 截面是不同 worker, 两段各自成 100%%\n"
                      "时序: 每轮约 4096 个 asset-day 抽样, 整个 asset-day 计时, 节点 compute / flush 逐个计,\n"
                      "      标签 / 写张量单列, 余下 = 源与调度 (解码 / 订单簿 / 重采样 / 日句柄)\n"
                      "截面: 每列每拍全计, 余下 = 调度 (有效集扫描 / 旁读读入)\n"
                      "只看本层: 文件的节点可能同时服务别的层, 各层视图不互斥");

  // 时序
  std::vector<NodeTimeRow> ts_rows;
  uint64_t ts_named = s_nt.ts_label + s_nt.ts_write, ts_shown = 0;
  for (const NodeTimeFile::Node &n : s_nt.nodes) {
    ts_named += n.cycles;
    if (s_level_only && !in_level(n.file))
      continue;
    NodeTimeRow &r = node_time_row(ts_rows, n.file);
    r.cycles += n.cycles;
    r.items.emplace_back(n.name, n.cycles);
  }
  assert(s_nt.ts_total >= ts_named && "node_time.json: 时序分项之和超过整段");
  if (!label_file.empty() && (!s_level_only || in_level(label_file))) {
    NodeTimeRow &r = node_time_row(ts_rows, label_file);
    r.cycles += s_nt.ts_label;
    r.items.emplace_back("标签", s_nt.ts_label);
  }
  for (const NodeTimeRow &r : ts_rows)
    ts_shown += r.cycles;
  if (!s_level_only) {
    ts_rows.push_back({{}, "写张量 (TS 行 + 元列)", s_nt.ts_write, {}});
    ts_rows.push_back({{}, "源与调度", s_nt.ts_total - ts_named, {}});
  }

  // 截面: 列 → 文件 (按 (层, code) 查字段表; 查不到 = 字段表已变而用时是旧轮的, 按 code 单列)
  std::vector<NodeTimeRow> cs_rows;
  uint64_t cs_named = 0, cs_shown = 0;
  for (const NodeTimeFile::Col &c : s_nt.cols) {
    cs_named += c.cycles;
    if (s_level_only && c.level != level)
      continue;
    const auto &fs = meta[c.level];
    const auto it = std::find_if(fs.begin(), fs.end(), [&](const FeatureMetadata &f) { return c.code == f.code; });
    NodeTimeRow &r = it != fs.end() ? node_time_row(cs_rows, it->file) : cs_rows.emplace_back(NodeTimeRow{{}, c.code});
    r.cycles += c.cycles;
    r.items.emplace_back(s_level_only ? c.code : c.code + "@" + level_name(c.level), c.cycles);
    cs_shown += c.cycles;
  }
  assert(s_nt.cs_total >= cs_named && "node_time.json: 截面分项之和超过整段");
  if (!s_level_only)
    cs_rows.push_back({{}, "调度 (有效集 / 旁读)", s_nt.cs_total - cs_named, {}});

  const auto pct = [](uint64_t c, uint64_t total) { return total > 0 ? 100.0 * static_cast<double>(c) / static_cast<double>(total) : 0.0; };
  char title[128];
  std::snprintf(title, sizeof(title), "时序 · 抽样 %llu asset-day · %s %.1f%%", static_cast<unsigned long long>(s_nt.ts_samples),
                s_level_only ? "本层相关合计" : "合计", s_level_only ? pct(ts_shown, s_nt.ts_total) : 100.0);
  ImGui::SeparatorText(title);
  render_node_time_bar("##node_time_ts", ts_rows, s_nt.ts_total);

  std::snprintf(title, sizeof(title), "截面 · %llu 日 · %s %.1f%%", static_cast<unsigned long long>(s_nt.cs_days),
                s_level_only ? "本层合计" : "合计", s_level_only ? pct(cs_shown, s_nt.cs_total) : 100.0);
  ImGui::SeparatorText(title);
  render_node_time_bar("##node_time_cs", cs_rows, s_nt.cs_total);
}

void RenderTabNodeTime(SharedData &data) {
  RenderScope(data.feature);
  render_node_time(data, data.feature);
}

} // namespace GUI::Features
