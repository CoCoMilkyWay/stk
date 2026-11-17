// Tab Sync - 数据源 (BigQuant / Tushare / 新浪) 状态、同步与远端核对入口
// 数据落地 = output/data/fundamental/{YYYY-MM, _meta}/<表>.parquet + YYYY-MM/<表>/YYYY-MM-DD.parquet (Daily)
//          + output/data/quote/live.bin
//
// 一源一张分表 (BigQuant / Tushare / 新浪), 列同形; 表头 = 源名 + 说明 + 各道同步状态
//   (Update 三道 = SyncState::lanes, 双轨并行; 新浪当日 = QuoteStatus, 常驻轮询).
// 静态元描述取自 fundamental::SPECS, 动态状态取自与 SPECS 等长同序的 SyncState::tables (O(1) 对齐, 帧内零 IO).
#include "gui/task_database/ui/TabSync.hpp"

#include "disk/Disk.hpp"
#include "fundamental/Spec.hpp"
#include "gui/task_database/services/StateManager.hpp"
#include "gui/task_database/services/SyncService.hpp"
#include "imgui.h"
#include "imgui_internal.h"
#include "misc/date.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <utility>
#include <variant>

namespace GUI::Database {

namespace {

constexpr ImVec4 kOk = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
constexpr ImVec4 kWarn = ImVec4(1.0f, 0.95f, 0.3f, 1.0f);
constexpr ImVec4 kBad = ImVec4(1.0f, 0.6f, 0.2f, 1.0f);
constexpr ImVec4 kMissing = ImVec4(1.0f, 0.45f, 0.35f, 1.0f);

enum class Act : std::uint8_t { None,
                                Update,
                                Cancel,
                                AuditAll,
                                AuditIncr,
                                AuditOne,
                                RepairOne };

struct PendingAct {
  Act kind = Act::None;
  std::string table;
};

struct RunView {
  bool sync_busy = false;  // Sync 在跑
  bool audit_busy = false; // Audit / Repair 在跑
  Phase phase = Phase::Idle;
  std::size_t done = 0;
  std::size_t total = 0;
};

void progress_bar(std::size_t done, std::size_t total, const char *overlay) {
  const float frac = total > 0 ? static_cast<float>(done) / static_cast<float>(total)
                               : -1.0f * static_cast<float>(ImGui::GetTime());
  ImGui::ProgressBar(frac, ImVec2(-1.0f, 0.0f), overlay);
}

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

ImVec4 pct_color(double p) {
  return p == 0.0 ? kOk : p < 1.0 ? kWarn
                      : p < 10.0  ? kBad
                                  : kMissing;
}

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

void text_wrapped_disabled(const char *s) {
  ImGui::PushStyleColor(ImGuiCol_Text, ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
  ImGui::TextWrapped("%s", s);
  ImGui::PopStyleColor();
}

struct RowMeta {
  std::string name, vd, fetch, ready, desc;
  bool meta_single = false;
};

RowMeta meta_of(const fundamental::Spec &spec) {
  char ready[16];
  std::snprintf(ready, sizeof(ready), "%02d:00", spec.avail_hour);
  return {spec.name, spec.vd.empty() ? "-" : spec.vd, fetch_name(spec), ready, spec.desc,
          spec.shape == fundamental::Shape::Meta};
}

void audit_cells(const std::string &name, const AuditStat *a, const SyncState &state, const RunView &rv,
                 bool disable, PendingAct &act) {
  if (!a) {
    dash_cells(5);
    return;
  }
  ImGui::TableNextColumn();
  const bool mine = rv.audit_busy && state.table == name;
  if (mine) {
    ImGui::TextColored(kWarn, "%s %zu/%zu", state.op == SyncOp::Repair ? "修复中" : "核对中", rv.done, rv.total);
  } else {
    ImGui::BeginDisabled(disable);
    if (ImGui::SmallButton(("audit##" + name).c_str()))
      act = {Act::AuditOne, name};
    ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
      ImGui::SetTooltip("只核这张表的全部冻结月 (月级扫描 + 日级定位), 覆盖该表状态; 不改盘\n"
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
  ImGui::TextDisabled("%zu/%lld", a->bad_days, static_cast<long long>(a->local_days + a->remote_only_days));

  ImGui::TableNextColumn();
  if (a->added == 0 && a->removed == 0) {
    ImGui::TextDisabled("-");
  } else {
    ImGui::TextColored(pct_color(a->row_pct()), "+%s/−%s", fmt_count(a->added).c_str(), fmt_count(a->removed).c_str());
    ImGui::SameLine();
    ImGui::TextDisabled("%.2f%%", a->row_pct());
  }

  ImGui::TableNextColumn();
  if (a->bad_months > 0 && !mine) {
    ImGui::BeginDisabled(disable);
    if (ImGui::SmallButton(("sync##" + name).c_str()))
      act = {Act::RepairOne, name};
    ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
      ImGui::SetTooltip("重拉不一致分片 (%zu 天, 涉及 %zu 月) → 复核这些月 → 重建 AssetInfo\n"
                        "不可取消; 同步 / 核对 / 修复进行中或未开联网不可用",
                        a->bad_days, a->bad_months);
    ImGui::SameLine();
  }
  text_cells(a->repair_cells, state.quota_weekly);
  if (a->repair_cells > 0 && ImGui::IsItemHovered())
    ImGui::SetTooltip("1 cell / 条查询: 月表每差异月整月覆盖一条, 分钟线每差异日一条");

  ImGui::TableNextColumn();
  text_cells(a->full_cells, state.quota_weekly);
}

void table_row(const RowMeta &m, bool scanned, const TableStat *stat, bool busy, const AuditStat *audit,
               const SyncState &state, const RunView &rv, bool disable_audit, PendingAct &act) {
  ImGui::TableNextRow();
  if (busy) {
    ImVec4 c = StatusColor(TaskStatus::Kind::Busy);
    c.w = 0.25f;
    ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, ImGui::GetColorU32(c));
  }

  const bool present = stat && stat->present;
  ImGui::TableNextColumn();
  if (scanned && !present)
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
  else if (!present)
    ImGui::TextColored(kMissing, "missing");
  else if (m.meta_single)
    ImGui::TextDisabled("_meta");
  else
    ImGui::Text("%zu", stat->shards);

  ImGui::TableNextColumn();
  const misc::Ymd last = present ? stat->last_at : 0;
  ImGui::TextUnformatted(last % 100 ? misc::dash(last).c_str()
                         : last     ? misc::dash_ym(misc::ym_of(last)).c_str()
                                    : "-");

  ImGui::TableNextColumn();
  if (present)
    text_bytes(stat->bytes);
  else
    ImGui::TextUnformatted("-");

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(present && !stat->mtime.empty() ? stat->mtime.c_str() : "-");

  audit_cells(m.name, scanned ? audit : nullptr, state, rv, disable_audit, act);

  ImGui::TableNextColumn();
  ImGui::TextUnformatted(m.desc.c_str());
}

void caption(const char *title, const char *note) {
  ImGui::Spacing();
  ImGui::TextUnformatted(title);
  ImGui::SameLine(0.0f, 12.0f);
  ImGui::TextDisabled("%s", note);
}

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
    round = s.total == 0 ? "已到水位"
                         : std::to_string(s.done) + "/" + std::to_string(s.total) + " · " + fmt_count(s.rows) + " rows";
  sync_chip(kLaneDef[l].tag, st, round, s.time);
}

void sina_chip(const Quote::QuoteStatus *q) {
  if (!q) {
    sync_chip("sina.live", {}, "未起 (Market 任务首次打开才起)", "");
    return;
  }
  const TaskStatus st = q->pending > 0 ? TaskStatus{TaskStatus::Kind::Busy, "syncing"}
                                       : TaskStatus{TaskStatus::Kind::Muted, "live"};
  const std::string round = !q->fill ? "只快照 (行情日已入基本面)"
                                     : (q->holes ? "缺口 " + std::to_string(q->holes) + " 只 · " : std::string()) +
                                           std::to_string(q->fetched) + "/" + std::to_string(q->fetched + q->pending) +
                                           " · 分钟 K 补根";
  char time[32] = "";
  if (q->date)
    std::snprintf(time, sizeof(time), "%02d-%02d %02d:%02d", q->date / 100 % 100, q->date % 100, q->sec / 3600,
                  q->sec / 60 % 60);
  sync_chip("sina.live", st, round, time);
}

constexpr ImGuiTableFlags kTableFlags =
    ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
    ImGuiTableFlags_Resizable | ImGuiTableFlags_SizingFixedFit |
    ImGuiTableFlags_NoSavedSettings;

void audit_section(const SyncState &state, const RunView &rv, bool scanned, bool disable_audit, PendingAct &act) {
  ImGui::Spacing();
  ImGui::SeparatorText("Audit (BigQuant 远端 vs 本地冻结月)");

  std::size_t pend = 0;
  std::size_t audited = 0, bad_tables = 0, audited_m = 0, frozen_m = 0;
  std::int64_t repair = 0, full = 0;
  std::string updated;
  if (scanned) {
    for (std::size_t i = 0; i < fundamental::SPECS.size(); ++i) {
      if (!fundamental::audited(fundamental::SPECS[i]))
        continue;
      const AuditStat &t = state.tables[i].audit;
      pend += t.frozen_months - std::min(t.audited_months, t.frozen_months);
      audited += t.audited() ? 1 : 0;
      bad_tables += t.bad_days > 0 ? 1 : 0;
      audited_m += t.audited_months;
      frozen_m += t.frozen_months;
      repair += t.repair_cells;
      full += t.full_cells;
      updated = std::max(updated, t.updated);
    }
  }

  if (rv.audit_busy && state.op == SyncOp::Audit) {
    if (ImGui::Button("Cancel audit", ImVec2(220.0f, 0.0f)))
      act = {Act::Cancel, {}};
  } else {
    ImGui::BeginDisabled(disable_audit);
    if (ImGui::Button("Audit all", ImVec2(160.0f, 0.0f)))
      act = {Act::AuditAll, {}};
    ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
      ImGui::SetTooltip("全部表全部冻结月重核\n"
                        "先逐月指纹扫描 (行数 + 逐列内容), 不一致的月自动下钻定位到日\n"
                        "表间轮转乱序; 只报告不改盘; 每单元覆盖该表状态文件里的月, 只留最新状态\n"
                        "单表核对见清单 \"核对\" 列; 同步 / 修复进行中 / 未开联网不可用");
    ImGui::SameLine();
    ImGui::BeginDisabled(disable_audit || pend == 0);
    if (ImGui::Button("Audit incre", ImVec2(160.0f, 0.0f)))
      act = {Act::AuditIncr, {}};
    ImGui::EndDisabled();
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
      ImGui::SetTooltip("增量: 跳过状态里已核的月, 只核新冻结 / 上次没跑完的\n"
                        "待核 %zu 表·月\n"
                        "已核月的远端晚到回填测不出, 需要时用 Audit all 或单表 audit",
                        pend);
  }

  ImGui::SameLine(0.0f, 24.0f);
  if (rv.audit_busy && rv.total == 0)
    ImGui::TextDisabled("规划中 ...");
  else if (rv.audit_busy)
    ImGui::Text("%s %s%s  %zu / %zu", state.op == SyncOp::Repair ? "修复" : "核对",
                state.table.empty() ? "全部表" : state.table.c_str(), state.incremental ? " (增量)" : "", rv.done,
                rv.total);

  if (state.op != SyncOp::Sync && state.quota_start >= 0 && state.quota_weekly > 0) {
    ImGui::SameLine(0.0f, 24.0f);
    const double left = 1.0 - static_cast<double>(state.quota_used) / static_cast<double>(state.quota_weekly);
    ImGui::TextDisabled("配额消耗 %lld cells  (本周剩余 %.1f%%)",
                        static_cast<long long>(state.quota_used - state.quota_start), left * 100.0);
  }

  static const std::string kAuditDir = disk::path(disk::FundAudit).string();
  ImGui::TextDisabled("逐表状态 <表>.tsv (逐月核对时刻 / 汇总 + 不一致格原值): %s", kAuditDir.c_str());

  if (rv.audit_busy && rv.total > 0)
    progress_bar(rv.done, rv.total, state.message.c_str());

  if (!scanned) {
    ImGui::TextDisabled("读取逐表状态 ...");
    return;
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
  text_cells(repair, state.quota_weekly);
  ImGui::SameLine(0.0f, 24.0f);
  ImGui::TextUnformatted("全表重拉共需");
  ImGui::SameLine();
  text_cells(full, state.quota_weekly);
  text_wrapped_disabled(
      "问题天数 = 任一不一致的日 / (本地天数 + 远端独有天数)  |  "
      "行差 = 远端比本地多 (+, 晚到回填) / 少 (−, 删除), 占比按本地总行数  |  "
      "修复 / 全表流量 = 重拉不一致分片 / 全部已核月 的 cells (1 cell / 条查询), 括号内占周配额  |  "
      "核对 = audit 只核该表全部冻结月, 右侧 已核 / 冻结 月  |  "
      "sync = 重拉该表不一致分片, 复核这些月, 完成后重建 AssetInfo");
}

} // namespace

TaskStatus SyncStatus(const StateManager &sm, const SyncService &sync_service) {
  if (sync_service.busy()) {
    SyncOp op = SyncOp::Sync;
    {
      std::lock_guard lk(sync_service.mutex);
      op = sync_service.state.op;
    }
    if (op == SyncOp::Audit)
      return {TaskStatus::Kind::Busy, "auditing"};
    if (op == SyncOp::Repair)
      return {TaskStatus::Kind::Busy, "repairing"};
    return ProgressStatus(sync_service);
  }
  if (sm.syncing())
    return {TaskStatus::Kind::Busy, "syncing"};
  if (sm.get_state().asset_info_ready)
    return {TaskStatus::Kind::Ready, "done"};
  if (sm.get_state().sync_taken)
    return {TaskStatus::Kind::Error, "error"};
  return {TaskStatus::Kind::Muted, "waiting"};
}

void RenderTabSync(
    StateManager &sm,
    SyncService &sync_service,
    const Quote::QuoteStatus *sina,
    SyncTabState &ui) {
  const TaskStatus status = SyncStatus(sm, sync_service);
  const bool disable_update = !sm.can_sync();
  const bool disable_audit = !sm.can_audit();
  PendingAct act;

  {
    std::lock_guard lk(sync_service.mutex);
    const SyncState &state = sync_service.state;
    const bool busy = sync_service.busy();
    const RunView rv{
        busy && state.op == SyncOp::Sync,
        busy && state.op != SyncOp::Sync,
        sync_service.phase.load(std::memory_order_acquire),
        sync_service.done.load(std::memory_order_relaxed),
        sync_service.total.load(std::memory_order_relaxed),
    };

    ImGui::Spacing();
    ImGui::SeparatorText("Data Sources (BigQuant DAI · Tushare · 新浪)");

    ImGui::Text("Status:");
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(status.kind), "[%s]", status.text.c_str());
    if (!state.message.empty() && !rv.audit_busy) {
      ImGui::SameLine();
      ImGui::TextDisabled("%s", state.message.c_str());
    }
    if (rv.sync_busy)
      progress_bar(rv.done, rv.total, PhaseName(rv.phase));

    ImGui::Text("BigQuant 本周配额:");
    ImGui::SameLine();
    if (state.quota_time.empty()) {
      ImGui::TextDisabled("未取到 (联网同步或核对时刷新)");
    } else {
      const double left = 1.0 - static_cast<double>(state.quota_used) / static_cast<double>(state.quota_weekly);
      const ImVec4 color = left >= 0.5 ? kOk : left >= 0.2 ? kWarn
                                                           : ImVec4(1.0f, 0.3f, 0.2f, 1.0f);
      ImGui::TextColored(color, "剩余 %.1f%%", left * 100.0);
      ImGui::SameLine(0.0f, 12.0f);
      ImGui::TextDisabled("%lld / %lld cells  (读于 %s, 每周重置)",
                          static_cast<long long>(state.quota_weekly - state.quota_used),
                          static_cast<long long>(state.quota_weekly), state.quota_time.c_str());
    }

    ImGui::Spacing();
    if (!state.last_update.empty()) {
      ImGui::Text("Stocks: %zu", state.stock_count);
      ImGui::SameLine(0.0f, 24.0f);
      ImGui::Text("Adjust-factor series: %zu", state.factor_stock_count);
      ImGui::SameLine(0.0f, 24.0f);
      ImGui::Text("Trading days: %zu", state.trading_days_count);
      if (state.date_range_start)
        ImGui::Text("Calendar range: %s ~ %s", misc::dash(state.date_range_start).c_str(),
                    misc::dash(state.date_range_end).c_str());
      ImGui::TextDisabled("Last build: %s", state.last_update.c_str());
    }

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    if (rv.sync_busy) {
      if (ImGui::Button("Cancel sync", ImVec2(220.0f, 0.0f)))
        act = {Act::Cancel, {}};
      if (ImGui::IsItemHovered())
        ImGui::SetTooltip("当前查询跑完即停, 余下同步任务跳过, 照常重建 AssetInfo");
    } else {
      ImGui::BeginDisabled(disable_update);
      if (ImGui::Button("Update (sync + rebuild)", ImVec2(220.0f, 0.0f)))
        act = {Act::Update, {}};
      ImGui::EndDisabled();
      if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
        ImGui::SetTooltip("纯本地水位判定 → BigQuant 轨 (bigquant → bar1m) 与 Tushare 轨并行流水同步 → 重建 AssetInfo\n"
                          "已到水位时零网络, 秒级完成; 新浪当日不在其中 (Market 任务起后常驻轮询)");
    }

    const bool scanned = state.tables.size() == fundamental::SPECS.size();
    audit_section(state, rv, scanned, disable_audit, act);

    {
      const std::uint64_t ep = sync_service.epoch.load(std::memory_order_acquire);
      const double now = ImGui::GetTime();
      if (ui.fit_epoch != ep && (now - ui.fit_last_time >= 0.5 || !busy)) {
        ui.fit_epoch = ep;
        ui.fit_last_time = now;
        ui.fit_frames = 2;
      }
    }
    const bool fit = ui.fit_frames > 0;
    if (fit)
      --ui.fit_frames;

    ImGui::Spacing();
    const std::size_t listed = fundamental::SPECS.size();
    std::size_t present = 0;
    std::uint64_t total_bytes = 0;
    if (scanned) {
      for (const TableStat &t : state.tables)
        if (t.present) {
          ++present;
          total_bytes += t.bytes;
        }
    }
    ImGui::SeparatorText("Tables");
    if (scanned) {
      ImGui::Text("已落盘 %zu / %zu 表", present, listed);
      ImGui::SameLine(0.0f, 24.0f);
      ImGui::TextUnformatted("总体积");
      ImGui::SameLine(0.0f, 6.0f);
      text_bytes(total_bytes);
      ImGui::SameLine(0.0f, 24.0f);
      ImGui::TextDisabled("output/data/fundamental/  (随每轮 Update / Repair 重扫)");
    } else {
      ImGui::Text("在册 %zu 表", listed);
      ImGui::SameLine(0.0f, 24.0f);
      ImGui::TextDisabled("本地落盘状态待首轮扫描后填充");
    }

    text_wrapped_disabled(
        "增量键 = 因果安全可见日列, 水位按它记, 只拉该列大于水位的新行  |  "
        "抓法: 分区=date 分区裁剪, 条件=SQL WHERE 事件列, 月初=窗口内最早一天, "
        "快照=窗口内最新交易日, 整刷=无 date 全量, 逐日=一交易日一文件 (分片=天数), 轮询=常驻定时拉  |  "
        "就绪 = 服务端当日数据完整的时点: 00:00 提前排程, 09:00 真盘前, "
        "10:00 盘前, 20:00 盘后批, 24:00 次日  |  "
        "表头 = 各道同步状态 [状态] 本轮 (计步 · 行数) @ 时刻");

    const auto table = [&](const char *id, auto &&rows) {
      if (ImGui::BeginTable(id, kTableColumns, kTableFlags)) {
        setup_columns(fit);
        rows();
        ImGui::EndTable();
      }
    };
    const auto writing = [&](const std::string &name) {
      if (rv.sync_busy) {
        for (const LaneStat &l : state.lanes)
          if (l.run == LaneRun::Syncing && l.current == name)
            return true;
      }
      return rv.audit_busy && state.op == SyncOp::Repair && state.table == name;
    };
    const auto spec_rows = [&](bool tushare) {
      for (std::size_t i = 0; i < fundamental::SPECS.size(); ++i) {
        const fundamental::Spec &spec = fundamental::SPECS[i];
        if (fundamental::from<fundamental::Tu>(spec) != tushare)
          continue;
        const TableStat *st = scanned ? &state.tables[i] : nullptr;
        const AuditStat *au = scanned && fundamental::audited(spec) ? &st->audit : nullptr;
        table_row(meta_of(spec), scanned, st, writing(spec.name), au, state, rv, disable_audit, act);
      }
    };

    caption("BigQuant DAI", "日频表 + 分钟线, output/data/fundamental/");
    lane_chip(state, kLaneBq);
    lane_chip(state, kLaneBar1m);
    table("FundBigQuantTables", [&] { spec_rows(false); });

    caption("Tushare", "事件表 + 东财题材概念快照, output/data/fundamental/ (不参与 Audit)");
    lane_chip(state, kLaneTu);
    table("FundTushareTables", [&] { spec_rows(true); });

    caption("新浪", "当日行情, output/data/quote/ (Market 任务起后常驻轮询, 不在 Update 内; 不参与 Audit)");
    sina_chip(sina);
    table("FundSinaTables", [&] {
      TableStat live;
      if (sina && sina->bytes) {
        live.present = true;
        live.shards = 1;
        live.last_at = sina->date;
        live.bytes = sina->bytes;
        live.mtime = sina->saved ? short_time(sina->saved) : "";
      }
      char desc[256];
      std::snprintf(desc, sizeof(desc), "当日分钟 K (快照合成 + 分钟 K 补根) + 五档, 只存行情日一天; 快照盘中 %ds / 盘外 %ds 一轮",
                    Quote::QuoteService::kTick, Quote::QuoteService::kIdleTick);
      table_row({"live", "-", "轮询", "实时", desc}, sina != nullptr, sina ? &live : nullptr,
                sina && sina->pending > 0, nullptr, state, rv, disable_audit, act);
    });
  }

  switch (act.kind) {
  case Act::None:
    break;
  case Act::Cancel:
    sync_service.RequestCancel();
    break;
  case Act::Update:
    if (sm.can_sync())
      sm.refresh();
    break;
  case Act::AuditAll:
    if (sm.can_audit())
      sm.audit({}, false);
    break;
  case Act::AuditIncr:
    if (sm.can_audit())
      sm.audit({}, true);
    break;
  case Act::AuditOne:
    if (sm.can_audit())
      sm.audit(std::move(act.table), false);
    break;
  case Act::RepairOne:
    if (sm.can_audit())
      sm.repair(std::move(act.table));
    break;
  }
}

} // namespace GUI::Database
