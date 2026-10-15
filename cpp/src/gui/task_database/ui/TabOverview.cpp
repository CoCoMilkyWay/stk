// Tab Overview - 基本面数据源 (BigQuant + Tushare) 状态与同步入口
// 数据落地 = output/fundamental/YYYY-MM/*.parquet (水位增量, 常量见 shared/Config.hpp);
// AssetInfo 由 FundamentalService 从 parquet 构建, 此处只渲染状态 + 回写按钮 flag.
//
// 逐表清单的两半信息各有出处, 谁也不抄谁:
//   静态元描述 (增量键 / 抓法 / 就绪 / 说明) — 直接遍历 fundamental::SPECS
//     (按 src 分两张清单), spec 加一张表这里自动多一行, 不存在漏配.
//   本地落盘状态 (分片数 / 最新月 / 体积 / 落盘时刻) — FundamentalState::tables,
//     工作线程扫好的快照, 按表名关联; 渲染帧内零 IO.
#include "gui/task_database/ui/TabOverview.hpp"
#include "fundamental/Spec.hpp"
#include "imgui.h"
#include "misc/date.hpp"

#include <cstdint>
#include <string>

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
  case FundamentalStatus::Confirm:
    return ImVec4(1.0f, 0.6f, 0.2f, 1.0f);
  case FundamentalStatus::Error:
    return ImVec4(1.0f, 0.3f, 0.2f, 1.0f);
  case FundamentalStatus::Idle:
    return ImVec4(0.7f, 0.7f, 0.7f, 1.0f);
  }
  return ImVec4(0.7f, 0.7f, 0.7f, 1.0f);
}

// Shape × Pick × src → 抓法一词 (与 fetch 的取行方式一一对应)
const char *fetch_name(const fundamental::Spec &s) {
  using fundamental::Pick;
  if (s.pick == Pick::First)
    return "月初";
  if (s.pick == Pick::Last)
    return "快照";
  if (s.shape == fundamental::Shape::Meta)
    return "整刷";
  const auto *dai = std::get_if<fundamental::Dai>(&s.src);
  return dai && dai->partition ? "分区" : "条件";
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

// 两张清单共用的列定义 —— DAI 与 Tushare 逐列对仗 (Audit 列 Tushare 恒 "-")
constexpr int kTableColumns = 13;
void setup_columns() {
  ImGui::TableSetupColumn("Table");
  ImGui::TableSetupColumn("增量键");
  ImGui::TableSetupColumn("抓法");
  ImGui::TableSetupColumn("就绪");
  ImGui::TableSetupColumn("分片");
  ImGui::TableSetupColumn("最新月");
  ImGui::TableSetupColumn("体积");
  ImGui::TableSetupColumn("最后落盘");
  ImGui::TableSetupColumn("问题天数");
  ImGui::TableSetupColumn("行差 (+/−)");
  ImGui::TableSetupColumn("修复流量");
  ImGui::TableSetupColumn("全表流量");
  ImGui::TableSetupColumn("内容", ImGuiTableColumnFlags_WidthStretch);
  ImGui::TableHeadersRow();
}

constexpr ImVec4 kOk = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
constexpr ImVec4 kWarn = ImVec4(1.0f, 0.95f, 0.3f, 1.0f);
constexpr ImVec4 kBad = ImVec4(1.0f, 0.6f, 0.2f, 1.0f);

// 0 绿 / <1% 黄 / <10% 橙 / 其余红
ImVec4 pct_color(double p) {
  return p == 0.0 ? kOk : p < 1.0 ? kWarn
                      : p < 10.0  ? kBad
                                  : kMissing;
}

// 741234 → "741.2K"
std::string fmt_count(std::int64_t v) {
  char buf[32];
  const double d = static_cast<double>(v);
  if (v >= 1'000'000'000)
    std::snprintf(buf, sizeof(buf), "%.2fG", d / 1e9);
  else if (v >= 1'000'000)
    std::snprintf(buf, sizeof(buf), "%.1fM", d / 1e6);
  else if (v >= 1'000)
    std::snprintf(buf, sizeof(buf), "%.1fK", d / 1e3);
  else
    std::snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(v));
  return buf;
}

// cells + 占周配额 (weekly 未知则只给 cells); 超过一周配额标红
void text_cells(std::int64_t cells, std::int64_t weekly) {
  if (cells == 0) {
    ImGui::TextDisabled("-");
    return;
  }
  if (weekly <= 0) {
    ImGui::TextUnformatted(fmt_count(cells).c_str());
    return;
  }
  const double p = 100.0 * static_cast<double>(cells) / static_cast<double>(weekly);
  const ImVec4 c = p >= 100.0 ? kMissing : p >= 30.0 ? kBad
                                                     : ImGui::GetStyleColorVec4(ImGuiCol_Text);
  ImGui::TextColored(c, "%s (%.1f%%)", fmt_count(cells).c_str(), p);
}

// Audit 四列. a == nullptr: 未核对 (Tushare / _meta 表 / 从未跑过)
void audit_cells(const AuditTableStat *a, std::int64_t weekly) {
  if (!a) {
    for (int i = 0; i < 4; ++i) {
      ImGui::TableNextColumn();
      ImGui::TextDisabled("-");
    }
    return;
  }
  ImGui::TableNextColumn();
  ImGui::TextColored(pct_color(a->bad_pct()), "%.2f%%", a->bad_pct());
  ImGui::SameLine();
  ImGui::TextDisabled("%zu/%lld", a->bad_days,
                      static_cast<long long>(a->local_days + a->remote_only_days));

  ImGui::TableNextColumn();
  if (a->added == 0 && a->removed == 0) {
    ImGui::TextDisabled("-");
  } else {
    ImGui::TextColored(pct_color(a->row_pct()), "+%s/−%s", fmt_count(a->added).c_str(),
                       fmt_count(a->removed).c_str());
    ImGui::SameLine();
    ImGui::TextDisabled("%.2f%%", a->row_pct());
  }
  ImGui::TableNextColumn();
  text_cells(a->repair_cells, weekly);
  if (a->repair_cells > 0 && ImGui::IsItemHovered())
    ImGui::SetTooltip("重拉 %zu 个有差异的月 (整月 SELECT *, 远端行 × 列)", a->bad_months);

  ImGui::TableNextColumn();
  text_cells(a->full_cells, weekly);
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
//   Shape::Meta: 落 _meta/ 单文件, 没有"分片/最新月"的概念.
void table_row(const fundamental::Spec &spec, bool scanned, const TableFileStat *stat,
               const AuditTableStat *audit, std::int64_t weekly) {
  const std::string &name = spec.name;
  const bool meta_single = spec.shape == fundamental::Shape::Meta;
  ImGui::TableNextRow();

  ImGui::TableNextColumn();
  if (scanned && !stat)
    ImGui::TextColored(kMissing, "%s", name.c_str());
  else
    ImGui::TextUnformatted(name.c_str());

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(spec.vd.empty() ? "-" : spec.vd.c_str());

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(fetch_name(spec));

  ImGui::TableNextColumn();
  ImGui::Text("%02d:00", spec.avail_hour);

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
  ImGui::TextUnformatted(stat && stat->last_month ? misc::dash_ym(stat->last_month).c_str() : "-");

  ImGui::TableNextColumn();
  if (stat)
    text_bytes(stat->bytes);
  else
    ImGui::TextUnformatted("-");

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(stat && !stat->mtime.empty() ? stat->mtime.c_str()
                                                      : "-");

  audit_cells(audit, weekly);

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(spec.desc.c_str());
}

// 数据集总量 (只统计 SPECS 里在册的表, 目录里的历史遗留文件不计)
struct Totals {
  std::size_t listed = 0;
  std::size_t present = 0;
  std::uint64_t bytes = 0;
};

Totals count_totals(const FundamentalState &state) {
  Totals t;
  for (const fundamental::Spec &spec : fundamental::SPECS) {
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
    ImGuiTableFlags_Resizable | ImGuiTableFlags_SizingFixedFit |
    ImGuiTableFlags_NoSavedSettings;

// Audit 区: 按钮 / 进度 / 配额消耗 / 总账 (逐表结果并入 Tables 清单)
void audit_section(const AuditState &a, bool *audit_clicked,
                   bool *audit_cancel_clicked, bool disable_audit,
                   std::int64_t weekly) {
  ImGui::Spacing();
  ImGui::SeparatorText("Audit (BigQuant 远端 vs 本地冻结月)");

  if (a.running) {
    if (ImGui::Button("Cancel audit", ImVec2(220.0f, 0.0f)))
      *audit_cancel_clicked = true;
  } else {
    ImGui::BeginDisabled(disable_audit);
    if (ImGui::Button("Audit", ImVec2(220.0f, 0.0f)))
      *audit_clicked = true;
    ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
      ImGui::SetTooltip("两轮: 先逐月行数 (补录 / 删除), 再逐列内容指纹 (改值)\n"
                        "表间轮转乱序; 不一致的月自动按日下钻\n"
                        "只报告不改盘; 同步进行中 / 未开联网不可用");
    }
  }

  ImGui::SameLine(0.0f, 24.0f);
  if (a.running && a.total == 0)
    ImGui::TextDisabled("规划中 ...");
  else if (a.running)
    ImGui::Text("%zu / %zu", a.done, a.total);
  else if (a.total > 0)
    ImGui::Text("%s %zu / %zu", a.cancelled ? "[cancelled]" : "[done]", a.done,
                a.total);

  if (a.quota_start >= 0) {
    ImGui::SameLine(0.0f, 24.0f);
    const double left = 1.0 - static_cast<double>(a.quota_used) /
                                  static_cast<double>(a.quota_weekly);
    ImGui::TextDisabled("配额消耗 %lld cells  (本周剩余 %.1f%%)",
                        static_cast<long long>(a.quota_used - a.quota_start),
                        left * 100.0);
  }

  if (!a.log_path.empty())
    ImGui::TextDisabled("差异日志 (含本地 / 远端原值): %s", a.log_path.c_str());

  if (a.running && a.total > 0) {
    const float frac = static_cast<float>(a.done) / static_cast<float>(a.total);
    ImGui::ProgressBar(frac, ImVec2(-1.0f, 0.0f), a.current.c_str());
  }

  // 逐表结果在下方 Tables 清单的 Audit 列; 这里只给总账
  if (a.loading) {
    ImGui::TextDisabled("读取上次核对日志 + 清点本地分片 ...");
    return;
  }
  if (a.tables.empty()) {
    ImGui::TextDisabled("尚无核对记录");
    return;
  }
  std::size_t bad_tables = 0;
  std::int64_t repair = 0, full = 0;
  for (const AuditTableStat &t : a.tables) {
    bad_tables += t.bad_days > 0 ? 1 : 0;
    repair += t.repair_cells;
    full += t.full_cells;
  }
  ImGui::Text("核对: %s", a.log_status.c_str());
  ImGui::SameLine(0.0f, 24.0f);
  if (bad_tables == 0)
    ImGui::TextColored(kOk, "全部一致");
  else
    ImGui::TextColored(kBad, "问题表 %zu / %zu", bad_tables, a.tables.size());
  ImGui::SameLine(0.0f, 24.0f);
  ImGui::TextUnformatted("修复共需");
  ImGui::SameLine();
  text_cells(repair, weekly);
  ImGui::SameLine(0.0f, 24.0f);
  ImGui::TextUnformatted("全表重拉共需");
  ImGui::SameLine();
  text_cells(full, weekly);
  text_wrapped_disabled(
      "问题天数 = 任一不一致的日 / (本地天数 + 远端独有天数)  |  "
      "行差 = 远端比本地多 (+, 晚到回填) / 少 (−, 删除), 占比按本地总行数  |  "
      "修复 / 全表流量 = 重拉有差异的月 / 全部冻结月 的 cells (行 × 列), 括号内占周配额");
}

} // namespace

void RenderTabOverview(
    const FundamentalState &state,
    bool *update_clicked,
    bool disable_update_controls,
    const AuditState &audit,
    bool *audit_clicked,
    bool *audit_cancel_clicked,
    bool disable_audit) {

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

  // BigQuant 周配额余额 (配色阈值与 bigquant::preflight 终端输出一致)
  ImGui::Text("BigQuant 本周配额:");
  ImGui::SameLine();
  if (state.quota_time.empty()) {
    ImGui::TextDisabled("未取到 (联网同步时刷新)");
  } else {
    const double left = 1.0 - static_cast<double>(state.quota_used) /
                                  static_cast<double>(state.quota_weekly);
    const ImVec4 color = left >= 0.5   ? ImVec4(0.3f, 0.95f, 0.4f, 1.0f)
                         : left >= 0.2 ? ImVec4(1.0f, 0.95f, 0.3f, 1.0f)
                                       : ImVec4(1.0f, 0.3f, 0.2f, 1.0f);
    ImGui::TextColored(color, "剩余 %.1f%%", left * 100.0);
    ImGui::SameLine(0.0f, 12.0f);
    ImGui::TextDisabled("%lld / %lld cells  (读于 %s, 每周重置)",
                        static_cast<long long>(state.quota_weekly - state.quota_used),
                        static_cast<long long>(state.quota_weekly),
                        state.quota_time.c_str());
  }

  ImGui::Spacing();

  // 构建结果统计
  if (state.status == FundamentalStatus::Ready) {
    ImGui::Text("Stocks: %zu", state.stock_count);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::Text("Adjust-factor series: %zu", state.factor_stock_count);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::Text("Trading days: %zu", state.trading_days_count);

    if (state.date_range_start) {
      ImGui::Text("Calendar range: %s ~ %s", misc::dash(state.date_range_start).c_str(),
                  misc::dash(state.date_range_end).c_str());
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

  // 周配额: Update 的 preflight 读数优先, 没有则用 Audit 自己取的
  const std::int64_t weekly = state.quota_weekly > 0 ? state.quota_weekly : audit.quota_weekly;
  audit_section(audit, audit_clicked, audit_cancel_clicked, disable_audit, weekly);

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

  if (ImGui::BeginTable("FundBigQuantTables", kTableColumns, kTableFlags)) {
    setup_columns();
    for (const fundamental::Spec &spec : fundamental::SPECS)
      if (fundamental::from<fundamental::Dai>(spec))
        table_row(spec, scanned, state.find_table(spec.name), audit.find_table(spec.name), weekly);
    ImGui::EndTable();
  }

  ImGui::Spacing();
  ImGui::TextDisabled("Tushare — BigQuant 无等价表的事件型 fallback (不参与 Audit)");

  if (ImGui::BeginTable("FundTushareTables", kTableColumns, kTableFlags)) {
    setup_columns();
    for (const fundamental::Spec &spec : fundamental::SPECS)
      if (fundamental::from<fundamental::Tu>(spec))
        table_row(spec, scanned, state.find_table(spec.name), nullptr, weekly);
    ImGui::EndTable();
  }
}

} // namespace GUI::Database
