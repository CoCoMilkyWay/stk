// Tab Overview - 基本面数据源 (BigQuant + Tushare) 状态与同步入口
// 数据落地 = output/fundamental/YYYY-MM/*.parquet (水位增量, 常量见 shared/Config.hpp);
// AssetInfo 由 FundamentalService 从 parquet 构建, 此处只渲染状态 + 回写按钮 flag.
//
// 逐表清单的两半信息各有出处, 谁也不抄谁:
//   静态元描述 (增量键 / 抓法 / 就绪 / 说明) — 直接遍历 bigquant::SPECS +
//     tushare::SPECS, spec 加一张表这里自动多一行, 不存在漏配.
//   本地落盘状态 (分片数 / 最新月 / 体积 / 落盘时刻) — FundamentalState::tables,
//     工作线程扫好的快照, 按表名关联; 渲染帧内零 IO.
#include "gui/task_database/ui/TabOverview.hpp"
#include "api/bigquant/spec.hpp"
#include "api/tushare/spec.hpp"
#include "imgui.h"

#include <cstdint>

namespace GUI::Database {

namespace {

constexpr ImVec4 kMissing = ImVec4(1.0f, 0.45f, 0.35f, 1.0f);

ImVec4 status_color(FundamentalStatus s) {
  switch (s) {
  case FundamentalStatus::Ready:
    return ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
  case FundamentalStatus::Updating:
  case FundamentalStatus::Building:
    return ImVec4(1.0f, 0.95f, 0.3f, 1.0f);
  case FundamentalStatus::Error:
    return ImVec4(1.0f, 0.3f, 0.2f, 1.0f);
  case FundamentalStatus::Idle:
    return ImVec4(0.7f, 0.7f, 0.7f, 1.0f);
  }
  return ImVec4(0.7f, 0.7f, 0.7f, 1.0f);
}

// FetchKind × FetchFreq → 抓法一词 (与 bigquant::fetch 的 SQL 模板一一对应)
const char *fetch_name(bigquant::FetchKind k, bigquant::FetchFreq f) {
  if (f == bigquant::FetchFreq::MonthFirst)
    return "月初";
  switch (k) {
  case bigquant::FetchKind::Static:
    return "整刷";
  case bigquant::FetchKind::Partition:
    return "分区";
  case bigquant::FetchKind::Where:
    return "条件";
  case bigquant::FetchKind::Snapshot:
    return "快照";
  }
  return "-";
}

void text_bytes(std::uint64_t b) {
  double v = static_cast<double>(b);
  const char *unit = "B";
  if (v >= 1024.0 * 1024.0 * 1024.0) {
    v /= 1024.0 * 1024.0 * 1024.0;
    unit = "GB";
  } else if (v >= 1024.0 * 1024.0) {
    v /= 1024.0 * 1024.0;
    unit = "MB";
  } else if (v >= 1024.0) {
    v /= 1024.0;
    unit = "KB";
  }
  ImGui::Text("%.1f %s", v, unit);
}

// 两张清单共用的列定义 —— DAI 与 Tushare 逐列对仗
void setup_columns() {
  ImGui::TableSetupColumn("Table");
  ImGui::TableSetupColumn("增量键");
  ImGui::TableSetupColumn("抓法");
  ImGui::TableSetupColumn("就绪");
  ImGui::TableSetupColumn("分片");
  ImGui::TableSetupColumn("最新月");
  ImGui::TableSetupColumn("体积");
  ImGui::TableSetupColumn("最后落盘");
  ImGui::TableSetupColumn("内容", ImGuiTableColumnFlags_WidthStretch);
  ImGui::TableHeadersRow();
}

void text_wrapped_disabled(const char *s) {
  ImGui::PushStyleColor(ImGuiCol_Text, ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
  ImGui::TextWrapped("%s", s);
  ImGui::PopStyleColor();
}

// 一行 = 静态元描述 + 本地落盘状态.
//   scanned=false: 首轮同步还没跑完, 落盘列一律留白 —— 此时把整张表标成
//     missing 是误报, 只是还没人去扫盘.
//   stat == nullptr (scanned=true): 本地确实一个文件都没有.
//   meta_single: Static/Snapshot 表落 _meta/ 单文件, 没有"分片/最新月"的概念.
void table_row(const std::string &name, const std::string &key,
               const char *fetch, int avail_hour, const std::string &desc,
               bool meta_single, bool scanned, const TableFileStat *stat) {
  ImGui::TableNextRow();

  ImGui::TableNextColumn();
  if (scanned && !stat)
    ImGui::TextColored(kMissing, "%s", name.c_str());
  else
    ImGui::TextUnformatted(name.c_str());

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(key.empty() ? "-" : key.c_str());

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(fetch);

  ImGui::TableNextColumn();
  ImGui::Text("%02d:00", avail_hour);

  ImGui::TableNextColumn();
  if (!scanned)
    ImGui::TextDisabled("...");
  else if (!stat)
    ImGui::TextColored(kMissing, "missing");
  else if (meta_single)
    ImGui::TextDisabled("_meta");
  else
    ImGui::Text("%zu", stat->months);

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(stat && !stat->last_month.empty()
                             ? stat->last_month.c_str()
                             : "-");

  ImGui::TableNextColumn();
  if (stat)
    text_bytes(stat->bytes);
  else
    ImGui::TextUnformatted("-");

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(stat && !stat->mtime.empty() ? stat->mtime.c_str()
                                                      : "-");

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(desc.c_str());
}

// 数据集总量 (只统计 SPECS 里在册的表, 目录里的历史遗留文件不计)
struct Totals {
  std::size_t listed = 0;
  std::size_t present = 0;
  std::uint64_t bytes = 0;
};

Totals count_totals(const FundamentalState &state) {
  Totals t;
  for (const auto &spec : bigquant::SPECS) {
    ++t.listed;
    if (const TableFileStat *s = state.find_table(spec.name)) {
      ++t.present;
      t.bytes += s->bytes;
    }
  }
  for (const auto &spec : tushare::SPECS) {
    ++t.listed;
    if (const TableFileStat *s = state.find_table(spec.name)) {
      ++t.present;
      t.bytes += s->bytes;
    }
  }
  return t;
}

constexpr ImGuiTableFlags kTableFlags =
    ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
    ImGuiTableFlags_Resizable | ImGuiTableFlags_SizingFixedFit;

} // namespace

void RenderTabOverview(
    const FundamentalState &state,
    bool *update_clicked,
    bool disable_update_controls) {

  ImGui::Spacing();
  ImGui::SeparatorText("Fundamental Data (BigQuant DAI + Tushare)");

  // 状态行
  ImGui::Text("Status:");
  ImGui::SameLine();
  ImGui::TextColored(status_color(state.status),
                     "[%s]", GetFundamentalStatusName(state.status));
  if (!state.message.empty()) {
    ImGui::SameLine();
    ImGui::TextDisabled("%s", state.message.c_str());
  }
  if (state.status == FundamentalStatus::Updating ||
      state.status == FundamentalStatus::Building) {
    // 不确定进度条 (总量未知: 水位增量按表滚动)
    float t = static_cast<float>(ImGui::GetTime());
    ImGui::ProgressBar(-1.0f * t, ImVec2(-1.0f, 0.0f), "syncing...");
  }

  ImGui::Spacing();

  // 构建结果统计
  if (state.status == FundamentalStatus::Ready) {
    ImGui::Text("Stocks: %zu", state.stock_count);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::Text("Adjust-factor series: %zu", state.factor_stock_count);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::Text("Trading days: %zu", state.trading_days_count);

    if (!state.date_range_start.empty()) {
      ImGui::Text("Calendar range: %s ~ %s", state.date_range_start.c_str(),
                  state.date_range_end.c_str());
    }
    if (!state.last_update.empty()) {
      ImGui::TextDisabled("Last build: %s", state.last_update.c_str());
    }
  }

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // 操作区
  ImGui::BeginDisabled(disable_update_controls);
  if (ImGui::Button("Update (sync + rebuild)", ImVec2(220.0f, 0.0f))) {
    *update_clicked = true;
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
    ImGui::SetTooltip("pending 判定 → BigQuant/Tushare 水位增量同步 → 重建 AssetInfo\n"
                      "已到水位时零网络, 秒级完成");
  }

  // ==========================================================================
  // 逐表清单 — 数据集里到底有什么, 各自落到哪一步
  // ==========================================================================
  ImGui::Spacing();
  const bool scanned = !state.tables.empty();
  const Totals totals = count_totals(state);
  ImGui::SeparatorText("Tables");

  if (scanned) {
    ImGui::Text("已落盘 %zu / %zu 表", totals.present, totals.listed);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::TextUnformatted("总体积");
    ImGui::SameLine(0.0f, 6.0f);
    text_bytes(totals.bytes);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::TextDisabled("output/fundamental/  (随每轮 Update 重扫)");
  } else {
    ImGui::Text("在册 %zu 表", totals.listed);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::TextDisabled("本地落盘状态待首轮同步后填充");
  }

  text_wrapped_disabled(
      "增量键 = 因果安全可见日列, 水位按它记, 只拉该列大于水位的新行  |  "
      "抓法: 分区=date 分区裁剪, 条件=SQL WHERE 事件列, 月初=窗口内最早一天, "
      "快照=窗口内最新一天, 整刷=无 date 全量  |  "
      "就绪 = 服务端当日数据完整的时点: 00:00 提前排程, 09:00 真盘前, "
      "10:00 盘前, 20:00 盘后批, 24:00 次日");

  ImGui::Spacing();

  if (ImGui::BeginTable("FundBigQuantTables", 9, kTableFlags)) {
    setup_columns();
    for (const bigquant::TableSpec &spec : bigquant::SPECS) {
      const bool meta_single = spec.kind == bigquant::FetchKind::Static ||
                               spec.kind == bigquant::FetchKind::Snapshot;
      table_row(spec.name, spec.visible_date,
                fetch_name(spec.kind, spec.freq), spec.avail_hour, spec.desc,
                meta_single, scanned, state.find_table(spec.name));
    }
    ImGui::EndTable();
  }

  ImGui::Spacing();
  ImGui::TextDisabled("Tushare — BigQuant 无等价表的事件型 fallback");

  if (ImGui::BeginTable("FundTushareTables", 9, kTableFlags)) {
    setup_columns();
    for (const tushare::InterfaceSpec &spec : tushare::SPECS) {
      table_row(spec.name, spec.visible_date, "条件", spec.avail_hour,
                spec.desc, /*meta_single=*/false, scanned,
                state.find_table(spec.name));
    }
    ImGui::EndTable();
  }
}

} // namespace GUI::Database
