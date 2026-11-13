// Tab Sync - 数据源 (BigQuant / Tushare / 东财 / 新浪) 状态与同步入口
// 数据落地 = output/data/fundamental/{YYYY-MM, _meta}/<表>.parquet + YYYY-MM/<表>/YYYY-MM-DD.parquet (Daily)
//   (水位增量, 常量见 fundamental/Spec.hpp) + output/data/concept/YYYYMMDD.tsv + output/data/quote/live.bin;
// AssetInfo 由 SyncService 从 parquet 构建, 此处只渲染状态 + 回写按钮 flag.
//
// 一源一张分表 (BigQuant / Tushare / 东财 / 新浪), 列同形; 表头 = 源名 + 说明 + 各道同步状态
//   (Update 四道 = SyncState::lanes, 道序即执行序; 新浪当日 = QuoteStatus, 常驻轮询).
//
// 逐表清单的两半信息各有出处, 谁也不抄谁:
//   静态元描述 (增量键 / 抓法 / 就绪 / 说明) — 直接遍历 fundamental::SPECS
//     (按 src 分两张清单), spec 加一张表这里自动多一行, 不存在漏配.
//   本地落盘状态 (分片数 / 最新分片 / 体积 / 落盘时刻) — SyncState::tables,
//     工作线程扫好的快照, 按表名关联; 渲染帧内零 IO.
#include "gui/task_database/ui/TabSync.hpp"
#include "fundamental/Spec.hpp"
#include "imgui.h"
#include "imgui_internal.h" // TableSetColumnWidthAutoAll
#include "misc/date.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <string>

namespace GUI::Database {

namespace {

constexpr ImVec4 kMissing = ImVec4(1.0f, 0.45f, 0.35f, 1.0f);

// 一帧的核对视图 = 展示字段 + 进度面
struct AuditView {
  const AuditState &s;
  bool active;  // Audit / Repair 在跑
  bool loading; // 逐表状态尚未读完 (Load 未收尾)
  std::size_t done, total;
};

// 进度条: 有总量给比例, 否则不确定条; 叠字 = 阶段名
void progress_bar(const Progress &p, const char *overlay) {
  const std::size_t total = p.total.load(std::memory_order_relaxed);
  const float frac = total > 0 ? static_cast<float>(p.done.load(std::memory_order_relaxed)) / static_cast<float>(total)
                               : -1.0f * static_cast<float>(ImGui::GetTime());
  ImGui::ProgressBar(frac, ImVec2(-1.0f, 0.0f), overlay);
}

// Shape × Snap × src → 抓法一词 (与 fetch 的取行方式一一对应)
const char *fetch_name(const fundamental::Spec &s) {
  if (s.shape == fundamental::Shape::Daily)
    return "逐日";
  const bool month = s.shape == fundamental::Shape::Month;
  if (s.snap == fundamental::Snap::Day)
    return month ? "月初" : "快照";
  if (!month)
    return "整刷";
  const auto *bq = std::get_if<fundamental::Bq>(&s.src);
  return bq && bq->prune == fundamental::Prune::Partition ? "分区" : "条件";
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
// fit: 本帧请求列宽贴合内容 (须在表头行锁定布局之前)
constexpr int kTableColumns = 14;
void setup_columns(bool fit) {
  ImGui::TableSetupColumn("Table");
  ImGui::TableSetupColumn("增量键");
  ImGui::TableSetupColumn("抓法");
  ImGui::TableSetupColumn("就绪");
  ImGui::TableSetupColumn("分片");
  ImGui::TableSetupColumn("最新分片");
  ImGui::TableSetupColumn("体积");
  ImGui::TableSetupColumn("最后落盘");
  ImGui::TableSetupColumn("核对");
  ImGui::TableSetupColumn("问题天数");
  ImGui::TableSetupColumn("行差");
  ImGui::TableSetupColumn("修复流量");
  ImGui::TableSetupColumn("全表流量");
  ImGui::TableSetupColumn("内容", ImGuiTableColumnFlags_WidthStretch);
  if (fit) {
    ImGuiTable *table = ImGui::GetCurrentTable();
    assert(table);
    ImGui::TableSetColumnWidthAutoAll(table);
  }
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

void dash_cells(int n) {
  for (int i = 0; i < n; ++i) {
    ImGui::TableNextColumn();
    ImGui::TextDisabled("-");
  }
}

// Audit 五列. a == nullptr: 不参与核对 (Tushare / _meta 表)
//   核对列: 单表 audit 小按钮 + 覆盖 (该表在跑则换成进度)
//   修复流量列: 有差异月则前置 sync 小按钮
void audit_cells(const std::string &name, const AuditTableStat *a, const AuditView &audit,
                 bool disable, std::string *audit_table, std::string *repair_table, std::int64_t weekly) {
  if (!a) {
    dash_cells(5);
    return;
  }
  ImGui::TableNextColumn();
  const bool mine = audit.active && audit.s.table == name;
  if (mine) {
    ImGui::TextColored(kWarn, "%s %zu/%zu", audit.s.op == AuditOp::Repair ? "修复中" : "核对中", audit.done,
                       audit.total);
  } else {
    ImGui::BeginDisabled(disable);
    if (ImGui::SmallButton(("audit##" + name).c_str()))
      *audit_table = name;
    ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
      ImGui::SetTooltip("只核这张表的全部冻结月 (月扫描 + 逐级定位), 覆盖该表状态; 不改盘\n"
                        "同步 / 核对 / 修复进行中或未开联网不可用");
    ImGui::SameLine();
    ImGui::TextColored(a->audited_months >= a->frozen_months ? kOk : kWarn, "%zu/%zu", a->audited_months,
                       a->frozen_months);
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("已核 %zu / 冻结 %zu 月\n最近核对 %s", a->audited_months, a->frozen_months,
                        a->audited() ? a->updated.c_str() : "-");
  }
  if (!a->audited()) {
    dash_cells(4);
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
  if (a->bad_months > 0 && !mine) {
    ImGui::BeginDisabled(disable);
    if (ImGui::SmallButton(("sync##" + name).c_str()))
      *repair_table = name;
    ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
      ImGui::SetTooltip("只重拉定位到的组 (%zu 天, 涉及 %zu 月, 其中 %zu 月只定位到月须整月)\n"
                        "→ 只复核这些月 → 重建 AssetInfo\n"
                        "不可取消; 同步 / 核对 / 修复进行中或未开联网不可用",
                        a->bad_days, a->bad_months, a->whole_months);
    ImGui::SameLine();
  }
  text_cells(a->repair_cells, weekly);
  if (a->repair_cells > 0 && ImGui::IsItemHovered())
    ImGui::SetTooltip("1 cell / 条查询: 每个差异月一条 (整月 %zu 月, 其余 Splice 日 / 资产)\n"
                      "分钟线每个定位到的日一条",
                      a->whole_months);

  ImGui::TableNextColumn();
  text_cells(a->full_cells, weekly);
}

void text_wrapped_disabled(const char *s) {
  ImGui::PushStyleColor(ImGuiCol_Text, ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
  ImGui::TextWrapped("%s", s);
  ImGui::PopStyleColor();
}

// 一行的静态元描述; SPECS 的表取自 spec (meta_of), 东财 / 新浪各自填, 四源同列
struct RowMeta {
  std::string name, vd, fetch, ready, desc;
  bool meta_single = false; // 落 _meta/ 单文件
};

RowMeta meta_of(const fundamental::Spec &spec) {
  char ready[16];
  std::snprintf(ready, sizeof(ready), "%02d:00", spec.avail_hour);
  return {spec.name, spec.vd.empty() ? "-" : spec.vd, fetch_name(spec), ready, spec.desc,
          spec.shape == fundamental::Shape::Meta};
}

// 一行 = 静态元描述 + 本地落盘状态 + 核对.
//   scanned=false: 首轮同步还没跑完 (新浪: 未起), 落盘列一律留白 —— 此时把整张表标成
//     missing 是误报, 只是还没人去扫盘.
//   stat == nullptr (scanned=true): 本地确实一个文件都没有.
//   meta_single: 没有"分片/最新分片"的概念; 日分片的最新分片显示到日.
//   audit == nullptr: 不参与核对
//   busy: 正在往盘上写这张表 (同步在拉 / 修复在重拉 / 新浪在补分钟 K) → 整行底色 = 进行中色
void table_row(const RowMeta &m, bool scanned, const TableFileStat *stat, bool busy,
               const AuditTableStat *audit, const AuditView &audit_state, bool disable_audit,
               std::string *audit_table, std::string *repair_table, std::int64_t weekly) {
  ImGui::TableNextRow();
  if (busy) {
    ImVec4 c = StatusColor(TaskStatus::Kind::Busy);
    c.w = 0.25f;
    ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, ImGui::GetColorU32(c));
  }

  ImGui::TableNextColumn();
  if (scanned && !stat)
    ImGui::TextColored(kMissing, "%s", m.name.c_str());
  else
    ImGui::TextUnformatted(m.name.c_str());

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(m.vd.c_str());

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(m.fetch.c_str());

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(m.ready.c_str());

  ImGui::TableNextColumn();
  if (!scanned)
    ImGui::TextDisabled("...");
  else if (!stat)
    ImGui::TextColored(kMissing, "missing");
  else if (m.meta_single)
    ImGui::TextDisabled("_meta");
  else
    ImGui::Text("%zu", stat->months);

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(stat && stat->last_day     ? misc::dash(stat->last_day).c_str()
                         : stat && stat->last_month ? misc::dash_ym(stat->last_month).c_str()
                                                    : "-");

  ImGui::TableNextColumn();
  if (stat)
    text_bytes(stat->bytes);
  else
    ImGui::TextUnformatted("-");

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(stat && !stat->mtime.empty() ? stat->mtime.c_str()
                                                      : "-");

  audit_cells(m.name, audit, audit_state, disable_audit, audit_table, repair_table, weekly);

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(m.desc.c_str());
}

// 分表表头: 源名 + 说明, 其后逐道 sync_chip
void caption(const char *title, const char *note) {
  ImGui::Spacing();
  ImGui::TextUnformatted(title);
  ImGui::SameLine(0.0f, 12.0f);
  ImGui::TextDisabled("%s", note);
}

// 一道的同步状态 "tag [状态] 本轮 @ 时刻" (四源同一写法)
void sync_chip(const char *tag, const TaskStatus &st, const std::string &round, const std::string &time) {
  ImGui::SameLine(0.0f, 24.0f);
  ImGui::TextUnformatted(tag);
  ImGui::SameLine();
  if (st.kind == TaskStatus::Kind::None)
    ImGui::TextDisabled("[-]");
  else
    ImGui::TextColored(StatusColor(st.kind), "[%s]", st.text.c_str());
  if (!round.empty()) {
    ImGui::SameLine();
    ImGui::TextDisabled("%s", round.c_str());
  }
  if (!time.empty()) {
    ImGui::SameLine();
    ImGui::TextDisabled("@ %s", time.c_str());
  }
}

// Update 的一道: 未联网同步过 → [-]; 计划为空 → 已到水位 (概念: 当日已有); 否则 计步 · 行数; 时刻 = 收尾
void lane_chip(const SyncState &state, Lane l) {
  const LaneStat &s = state.lanes[l];
  TaskStatus st;
  switch (s.run) {
  case LaneRun::Idle:
    break;
  case LaneRun::Queued:
    st = {TaskStatus::Kind::Muted, "waiting"};
    break;
  case LaneRun::Syncing:
    st = {TaskStatus::Kind::Busy, "syncing"};
    break;
  case LaneRun::Done:
    st = {TaskStatus::Kind::Ready, "done"};
    break;
  case LaneRun::Cancelled:
    st = {TaskStatus::Kind::Muted, "cancelled"};
    break;
  }
  std::string round;
  if (s.run != LaneRun::Idle)
    round = s.total == 0 ? (l == kLaneConcept ? "当日已有" : "已到水位")
                         : std::to_string(s.done) + "/" + std::to_string(s.total) + " · " + fmt_count(s.rows) + " rows";
  sync_chip(kLaneTag[l], st, round, s.time);
}

// 新浪当日 (常驻轮询): 未起 → [-]; 补分钟 K 中 → syncing, 否则 live; 时刻 = 最新快照
void sina_chip(const Quote::QuoteStatus *q) {
  if (!q) {
    sync_chip("sina.live", {}, "未起 (Market 任务首次打开才起)", "");
    return;
  }
  const TaskStatus st = q->pending > 0 ? TaskStatus{TaskStatus::Kind::Busy, "syncing"}
                                       : TaskStatus{TaskStatus::Kind::Muted, "live"};
  const std::string round = !q->fill ? "只快照 (行情日已入 bar1m)"
                                     : (q->holes ? "缺口 " + std::to_string(q->holes) + " 只 · " : std::string()) + std::to_string(q->fetched) + "/" +
                                           std::to_string(q->fetched + q->pending) + " · 分钟 K 补根";
  char time[32] = "";
  if (q->date)
    std::snprintf(time, sizeof(time), "%02d-%02d %02d:%02d", q->date / 100 % 100, q->date % 100, q->sec / 3600,
                  q->sec / 60 % 60);
  sync_chip("sina.live", st, round, time);
}

// 数据集总量 (只统计 SPECS 里在册的表, 目录里的历史遗留文件不计)
struct Totals {
  std::size_t listed = 0;
  std::size_t present = 0;
  std::uint64_t bytes = 0;
};

Totals count_totals(const SyncState &state) {
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
void audit_section(const AuditView &v, bool *audit_clicked, bool *audit_incr_clicked,
                   bool *audit_cancel_clicked, bool disable_audit,
                   std::int64_t weekly) {
  const AuditState &a = v.s;
  ImGui::Spacing();
  ImGui::SeparatorText("Audit (BigQuant 远端 vs 本地冻结月)");

  std::size_t pend = 0; // 未核的 表·月
  for (const AuditTableStat &t : a.tables)
    pend += t.frozen_months - std::min(t.audited_months, t.frozen_months);

  if (v.active && a.op == AuditOp::Audit) {
    if (ImGui::Button("Cancel audit", ImVec2(220.0f, 0.0f)))
      *audit_cancel_clicked = true;
  } else {
    ImGui::BeginDisabled(disable_audit);
    if (ImGui::Button("Audit all", ImVec2(160.0f, 0.0f)))
      *audit_clicked = true;
    ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
      ImGui::SetTooltip("全部表全部冻结月重核\n"
                        "先逐月指纹扫描 (行数 + 逐列内容), 不一致的月自动逐级定位到 日 / 资产\n"
                        "表间轮转乱序; 只报告不改盘; 每单元覆盖该表状态文件里的月, 只留最新状态\n"
                        "单表核对见清单 \"核对\" 列; 同步 / 修复进行中 / 未开联网不可用");
    }
    ImGui::SameLine();
    ImGui::BeginDisabled(disable_audit || pend == 0);
    if (ImGui::Button("Audit incre", ImVec2(160.0f, 0.0f)))
      *audit_incr_clicked = true;
    ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
      ImGui::SetTooltip("增量: 跳过状态里已核的月, 只核新冻结 / 上次没跑完的\n"
                        "待核 %zu 表·月\n"
                        "已核月的远端晚到回填测不出, 需要时用 Audit all 或单表 audit",
                        pend);
    }
  }

  ImGui::SameLine(0.0f, 24.0f);
  if (v.active && v.total == 0)
    ImGui::TextDisabled("规划中 ...");
  else if (v.active)
    ImGui::Text("%s %s%s  %zu / %zu", a.op == AuditOp::Repair ? "修复" : "核对",
                a.table.empty() ? "全部表" : a.table.c_str(), a.incremental ? " (增量)" : "", v.done,
                v.total);

  if (a.quota_start >= 0) {
    ImGui::SameLine(0.0f, 24.0f);
    const double left = 1.0 - static_cast<double>(a.quota_used) /
                                  static_cast<double>(a.quota_weekly);
    ImGui::TextDisabled("配额消耗 %lld cells  (本周剩余 %.1f%%)",
                        static_cast<long long>(a.quota_used - a.quota_start),
                        left * 100.0);
  }

  if (!a.log_dir.empty())
    ImGui::TextDisabled("逐表状态 <表>.tsv (逐月核对时刻 / 汇总 + 不一致格原值): %s", a.log_dir.c_str());

  if (v.active && v.total > 0) {
    const float frac = static_cast<float>(v.done) / static_cast<float>(v.total);
    ImGui::ProgressBar(frac, ImVec2(-1.0f, 0.0f), a.current.c_str());
  }

  // 逐表结果在下方 Tables 清单的 Audit 列; 这里只给总账
  if (v.loading) {
    ImGui::TextDisabled("读取逐表状态 ...");
    return;
  }
  std::size_t audited = 0, bad_tables = 0, audited_m = 0, frozen_m = 0;
  std::int64_t repair = 0, full = 0;
  std::string updated;
  for (const AuditTableStat &t : a.tables) {
    audited += t.audited() ? 1 : 0;
    bad_tables += t.bad_days > 0 ? 1 : 0;
    audited_m += t.audited_months;
    frozen_m += t.frozen_months;
    repair += t.repair_cells;
    full += t.full_cells;
    updated = std::max(updated, t.updated);
  }
  if (audited == 0) {
    ImGui::TextDisabled("尚无核对记录");
    return;
  }
  ImGui::TextColored(audited_m >= frozen_m ? kOk : kWarn, "已核 %zu / 冻结 %zu 表·月", audited_m, frozen_m);
  ImGui::SameLine(0.0f, 12.0f);
  ImGui::TextDisabled("(最近核对 %s)", updated.c_str());
  ImGui::SameLine(0.0f, 24.0f);
  if (bad_tables == 0)
    ImGui::TextColored(kOk, "已核表全部一致");
  else
    ImGui::TextColored(kBad, "问题表 %zu / %zu", bad_tables, audited);
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
      "修复 / 全表流量 = 重拉定位到的组 / 全部已核月 的 cells (1 cell / 条查询), 括号内占周配额  |  "
      "核对 = audit 只核该表全部冻结月, 右侧 已核 / 冻结 月  |  "
      "sync = 只重拉该表定位到的 日 / 资产 (只定位到月的整月), 复核这些月, 完成后重建 AssetInfo");
}

} // namespace

void RenderTabSync(
    const SyncState &state,
    const Progress &progress,
    const TaskStatus &status,
    const Quote::QuoteStatus *sina,
    bool *update_clicked,
    bool *update_cancel_clicked,
    bool disable_update_controls,
    const AuditState &audit,
    const Progress &audit_progress,
    bool *audit_clicked,
    bool *audit_incr_clicked,
    bool *audit_cancel_clicked,
    bool disable_audit,
    std::string *audit_table,
    std::string *repair_table,
    SyncTabState &ui) {
  const bool running = progress.busy();
  const Phase phase = progress.phase.load(std::memory_order_acquire);
  const AuditView av{audit, audit_progress.busy() && audit.op != AuditOp::Load,
                     audit.op == AuditOp::Load && audit_progress.phase.load(std::memory_order_acquire) != Phase::Done,
                     audit_progress.done.load(std::memory_order_relaxed),
                     audit_progress.total.load(std::memory_order_relaxed)};

  ImGui::Spacing();
  ImGui::SeparatorText("Data Sources (BigQuant DAI · Tushare · 东财 · 新浪)");

  // 状态行
  ImGui::Text("Status:");
  ImGui::SameLine();
  ImGui::TextColored(StatusColor(status.kind), "[%s]", status.text.c_str());
  if (!state.message.empty()) {
    ImGui::SameLine();
    ImGui::TextDisabled("%s", state.message.c_str());
  }
  // 阶段: scanning 外来写者重写 / loading 同步 (计步 = 任务) / running 构建 (计步 = 分片)
  if (running)
    progress_bar(progress, PhaseName(phase));

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

  // 构建结果统计 (最近一次构建成功的)
  if (!state.last_update.empty()) {
    ImGui::Text("Stocks: %zu", state.stock_count);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::Text("Adjust-factor series: %zu", state.factor_stock_count);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::Text("Trading days: %zu", state.trading_days_count);

    if (state.date_range_start) {
      ImGui::Text("Calendar range: %s ~ %s", misc::dash(state.date_range_start).c_str(),
                  misc::dash(state.date_range_end).c_str());
    }
    ImGui::TextDisabled("Last build: %s", state.last_update.c_str());
  }

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // 操作区 (与 audit_section 同形: 跑时按钮换成 Cancel)
  if (running) {
    if (ImGui::Button("Cancel sync", ImVec2(220.0f, 0.0f)))
      *update_cancel_clicked = true;
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("当前任务的查询跑完即停, 余下同步任务跳过, 照常重建 AssetInfo");
  } else {
    ImGui::BeginDisabled(disable_update_controls);
    if (ImGui::Button("Update (sync + rebuild)", ImVec2(220.0f, 0.0f)))
      *update_clicked = true;
    ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
      ImGui::SetTooltip("pending 判定 → 依次同步 bigquant / tushare (水位增量) → eastmoney.concept (当日缺才拉)\n"
                        "→ bigquant.bar1m (逐日) → 重建 AssetInfo\n"
                        "已到水位时零网络, 秒级完成; 新浪当日不在其中 (Market 任务起后常驻轮询)");
    }
  }

  // 周配额: Update 的 preflight 读数优先, 没有则用 Audit 自己取的
  const std::int64_t weekly = state.quota_weekly > 0 ? state.quota_weekly : audit.quota_weekly;
  audit_section(av, audit_clicked, audit_incr_clicked, audit_cancel_clicked, disable_audit, weekly);

  // ==========================================================================
  // 逐表清单 — 数据集里到底有什么, 各自落到哪一步
  // ==========================================================================
  // 列宽贴合: 逐表落盘状态 / 核对统计 / 进度字样都随两服务发布而变, 列宽须跟着重贴,
  // 否则可拖宽的定宽列锁在首帧宽度, 变长的格互相遮挡. 发布代变了 (限速 0.5s, 都停下立即) → 连发 2 帧
  {
    const uint64_t ep = progress.epoch.load(std::memory_order_acquire) +
                        audit_progress.epoch.load(std::memory_order_acquire);
    const double now = ImGui::GetTime();
    if (ui.fit_epoch != ep && (now - ui.fit_last_time >= 0.5 || !(running || av.active))) {
      ui.fit_epoch = ep;
      ui.fit_last_time = now;
      ui.fit_frames = 2;
    }
  }
  const bool fit = ui.fit_frames > 0;
  if (fit)
    ui.fit_frames--;

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
    ImGui::TextDisabled("output/data/fundamental/  (随每轮 Update 重扫)");
  } else {
    ImGui::Text("在册 %zu 表", totals.listed);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::TextDisabled("本地落盘状态待首轮同步后填充");
  }

  text_wrapped_disabled(
      "增量键 = 因果安全可见日列, 水位按它记, 只拉该列大于水位的新行  |  "
      "抓法: 分区=date 分区裁剪, 条件=SQL WHERE 事件列, 月初=窗口内最早一天, "
      "快照=窗口内最新一天, 整刷=无 date 全量, 逐日=一交易日一文件 (分片=天数), 轮询=常驻定时拉  |  "
      "就绪 = 服务端当日数据完整的时点: 00:00 提前排程, 09:00 真盘前, "
      "10:00 盘前, 20:00 盘后批, 24:00 次日  |  "
      "表头 = 各道同步状态 [状态] 本轮 (计步 · 行数) @ 时刻");

  // 一源一张分表, 列同形; 表头 = 源名 + 说明 + 各道同步状态
  const auto table = [&](const char *id, auto &&rows) {
    if (ImGui::BeginTable(id, kTableColumns, kTableFlags)) {
      setup_columns(fit);
      rows();
      ImGui::EndTable();
    }
  };
  // 在写的表: 同步在拉的 / 修复在重拉的
  const auto writing = [&](const std::string &name) {
    return (running && state.syncing == name) || (av.active && audit.op == AuditOp::Repair && audit.table == name);
  };
  // SPECS 的表按源分 (只 BigQuant 参与核对)
  const auto spec_rows = [&](bool tushare) {
    for (const fundamental::Spec &spec : fundamental::SPECS)
      if (fundamental::from<fundamental::Tu>(spec) == tushare)
        table_row(meta_of(spec), scanned, state.find_table(spec.name), writing(spec.name),
                  tushare ? nullptr : audit.find_table(spec.name), av, disable_audit, audit_table, repair_table, weekly);
  };

  caption("BigQuant DAI", "日频表 + 分钟线, output/data/fundamental/");
  lane_chip(state, kLaneBq);
  lane_chip(state, kLaneBar1m);
  table("FundBigQuantTables", [&] { spec_rows(false); });

  caption("Tushare", "BigQuant 无等价表的事件型 fallback, output/data/fundamental/ (不参与 Audit)");
  lane_chip(state, kLaneTu);
  table("FundTushareTables", [&] { spec_rows(true); });

  caption("东财", "题材概念成分快照, output/data/concept/ (不参与 Audit)");
  lane_chip(state, kLaneConcept);
  table("FundEastmoneyTables", [&] {
    table_row({kConceptTable, "-", "快照", "-", "题材概念板块成分 (一行一概念: 板块代码 / 名称 / 成分码), 一日一份"}, scanned,
              state.concept_files.months ? &state.concept_files : nullptr, writing(kConceptTable), nullptr, av,
              disable_audit, audit_table, repair_table, weekly);
  });

  caption("新浪", "当日行情, output/data/quote/ (Market 任务起后常驻轮询, 不在 Update 内; 不参与 Audit)");
  sina_chip(sina);
  table("FundSinaTables", [&] {
    TableFileStat live;
    if (sina) {
      live.months = 1;
      live.last_day = sina->date;
      live.bytes = sina->bytes;
      live.mtime = sina->saved ? short_time(sina->saved) : "";
    }
    char desc[256];
    std::snprintf(desc, sizeof(desc), "当日分钟 K (快照合成 + 分钟 K 补根) + 五档, 只存行情日一天; 快照盘中 %ds / 盘外 %ds 一轮",
                  Quote::QuoteService::kTick, Quote::QuoteService::kIdleTick);
    table_row({"live", "-", "轮询", "实时", desc}, sina != nullptr, sina && sina->bytes ? &live : nullptr,
              sina && sina->pending > 0, nullptr, av, disable_audit, audit_table, repair_table, weekly);
  });
}

} // namespace GUI::Database
