#include "gui/task_database/TaskDatabase.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_database/services/AuditService.hpp"
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_database/services/FundamentalService.hpp"
#include "gui/task_database/services/L2DatabaseService.hpp"
#include "gui/task_database/services/ScanService.hpp"
#include "gui/task_database/services/StateManager.hpp"
#include "gui/task_database/ui/TabBrowser.hpp"
#include "gui/task_database/ui/TabEncode.hpp"
#include "gui/task_database/ui/TabOverview.hpp"
#include "gui/task_database/ui/TabTable.hpp"
#include "imgui.h"
#include "shared/Config.hpp"
#include "shared/SharedData.hpp"
#include <memory>
#include <mutex>
#include <thread>

namespace GUI::Tasks {
namespace {

using namespace GUI::Database;

class DatabaseTask {
private:
  // Service layer
  std::unique_ptr<FundamentalService> fundamental_svc_;
  std::unique_ptr<AuditService> audit_svc_;
  std::unique_ptr<ScanService> scan_svc_;
  std::unique_ptr<EncodingService> encoding_svc_;
  std::unique_ptr<FileCheckService> filecheck_svc_;
  std::unique_ptr<L2DatabaseService> l2_svc_;
  std::unique_ptr<StateManager> state_mgr_;

  // UI state
  EncodeState encode_state_;
  TableState table_state_;
  BrowserState browser_state_;

  // Lifecycle
  bool is_expanded_ = false;
  bool initialized_ = false;
  SharedData *data_ = nullptr; // Pointer to shared data
  Config *config_ = nullptr;   // Pointer to config for accessing backtest dates
  std::string config_dir_ = "../../config";

public:
  DatabaseTask() = default;

  const char *GetName() const {
    return "Database";
  }

  // 与 DrawTab/选中态解耦: 创建后立即起后台检查 (基本面 sync → L2 scan),
  // 不需要用户手动点开 Database 页面. Init 只调一次 (initialized_ 兜底),
  // 顺序依赖见 Tasks.cpp::CreateAllTasks (Settings 先落盘配置到内存).
  void Init(SharedData &data) {
    if (initialized_)
      return;
    data_ = &data;
    config_ = &data.config;
    InitializeServices(data);
    initialized_ = true;
  }

  void OnExpand() {
    is_expanded_ = true;
  }

  void OnCollapse() {
    is_expanded_ = false;
  }

  // 每帧 (无论选中): 收交接 / 推流水线 → taskstate.database (左栏标签/使能同帧读)
  void Update(SharedData & /*data*/) {
    if (state_mgr_)
      state_mgr_->tick();
    // 配额确认弹窗放这里而非 Draw: 启动即后台同步, 用户可能根本没点开 Database,
    // 模态必须每帧都在 (工作线程此刻挂起, 不答就一直等)
    DrawQuotaConfirm();
  }

  // BigQuant 周配额剩余 < BIGQUANT_QUOTA_CONFIRM_RATIO ⇒ 模态二选一, 答案回给
  // FundamentalService::confirm (工作线程据此走 / 跳过 BigQuant 同步)
  void DrawQuotaConfirm() {
    if (!fundamental_svc_ || !fundamental_svc_->confirm_pending.load(std::memory_order_acquire))
      return;
    std::lock_guard lk(fundamental_svc_->mutex);
    const auto &fs = fundamental_svc_->state;
    static constexpr const char *kId = "BigQuant 周配额不足##QuotaConfirm";
    if (!ImGui::IsPopupOpen(kId)) {
      ImGui::OpenPopup(kId);
      ImGui::SetNextWindowPos(ImGui::GetMainViewport()->GetCenter(),
                              ImGuiCond_Appearing, ImVec2(0.5f, 0.5f));
    }
    if (!ImGui::BeginPopupModal(kId, nullptr, ImGuiWindowFlags_AlwaysAutoResize))
      return;
    const double left = 1.0 - static_cast<double>(fs.quota_used) /
                                  static_cast<double>(fs.quota_weekly);
    ImGui::TextColored(ImVec4(1.0f, 0.6f, 0.2f, 1.0f), "本周剩余 %.1f%%  (阈值 %.0f%%)",
                       left * 100.0, config::BIGQUANT_QUOTA_CONFIRM_RATIO * 100.0);
    ImGui::TextDisabled("used %lld / weekly %lld cells  (每周重置, 服务端不给重置时刻)",
                        static_cast<long long>(fs.quota_used),
                        static_cast<long long>(fs.quota_weekly));
    ImGui::Spacing();
    ImGui::TextWrapped("有待拉的 BigQuant 月度分片. 继续会消耗配额; "
                       "跳过则本轮不碰 BigQuant (Tushare / 本地构建照常), "
                       "下次 Update 再问.");
    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();
    if (ImGui::Button("继续同步", ImVec2(160.0f, 0.0f))) {
      fundamental_svc_->confirm(true);
      ImGui::CloseCurrentPopup();
    }
    ImGui::SameLine();
    if (ImGui::Button("跳过 BigQuant", ImVec2(160.0f, 0.0f))) {
      fundamental_svc_->confirm(false);
      ImGui::CloseCurrentPopup();
    }
    ImGui::EndPopup();
  }

  // Enabled: 左栏叶子是否可点, 直读 StateManager 的解锁进度 (Update 同帧已刷新)
  // idx: -1=任务行(不可点选, 恒真) 0=Overview 1=Encode 2=Table 3=Browser
  bool Enabled(const SharedData & /*data*/, int idx) const {
    if (idx <= 0)
      return true; // 任务行 / Overview 永远可用
    if (!state_mgr_)
      return false;
    const auto &tabs = state_mgr_->get_state().tabs;
    switch (idx) {
    case 1:
      return tabs.can_access_encode;
    case 2:
      return tabs.can_access_table;
    case 3:
      return tabs.can_access_browser;
    }
    return false;
  }

  // Status: 行状态标签. idx == -1 任务行 (总体), 0 Overview (基本面), 1 Encode
  // (编码进度/覆盖检查结果), 2 Table / 3 Browser (纯浏览: 前置计算中显示进行中,
  // 解锁后 done, 锁定且无在跑 none)
  TaskStatus Status(const SharedData &data, int idx) const {
    switch (idx) {
    case -1:
      return TaskLevelStatus(data);
    case 0:
      return OverviewStatus();
    case 1:
      return EncodeStatus(data);
    case 2:
      return BrowseStatus(data, state_mgr_ && state_mgr_->get_state().tabs.can_access_table);
    case 3:
      return BrowseStatus(data, state_mgr_ && state_mgr_->get_state().tabs.can_access_browser);
    }
    return {};
  }

  // Draw: 渲染指定 tab (状态头 + 对应 DrawTabX) + 处理编码触发
  void Draw(SharedData &data, int idx) {
    assert(idx >= 0 && idx < 4);
    RenderUI(idx);

    // Handle encoding trigger from UI (Encode tab 设置 trigger_start; Start 按钮已按同一条件使能)
    if (encode_state_.trigger_start) {
      encode_state_.trigger_start = false;

      int workers = encode_state_.num_workers;
      if (workers <= 0) {
        workers = std::thread::hardware_concurrency();
        if (workers <= 0)
          workers = 8;
      }

      if (!state_mgr_->syncing() && !state_mgr_->scanning() && !state_mgr_->encoding())
        state_mgr_->encode(workers, encode_state_.skip_existing);
    }
  }

private:
  void InitializeServices(SharedData &data) {
    // Create services (in dependency order); 各自构造即起 worker
    fundamental_svc_ = std::make_unique<FundamentalService>();
    audit_svc_ = std::make_unique<AuditService>();
    scan_svc_ = std::make_unique<ScanService>(data);
    encoding_svc_ = std::make_unique<EncodingService>(data);
    filecheck_svc_ = std::make_unique<FileCheckService>();
    l2_svc_ = std::make_unique<L2DatabaseService>(data);
    // 构造即发首批请求 (核对 Load + 基本面 → 扫描); 进度在终端与各页
    state_mgr_ = std::make_unique<StateManager>(data, *fundamental_svc_, *scan_svc_, *encoding_svc_, *audit_svc_);
  }

  // 基本面状态标签: 待确认 > 在跑 (进度面) > 在飞未起跑 / 件待收 > 最近一件结论
  TaskStatus FundamentalStatus() const {
    if (fundamental_svc_->confirm_pending.load(std::memory_order_acquire))
      return {TaskStatus::Kind::Warn, "confirm quota"};
    if (fundamental_svc_->busy())
      return ProgressStatus(*fundamental_svc_);
    if (state_mgr_->syncing())
      return {TaskStatus::Kind::Busy, "syncing"};
    if (state_mgr_->get_state().fundamental_ok)
      return {TaskStatus::Kind::Ready, "done"};
    if (state_mgr_->get_state().fundamental_taken)
      return {TaskStatus::Kind::Error, "error"};
    return {TaskStatus::Kind::Muted, "idle"};
  }

  // 任务行标签: taskstate.database.status → (Kind, text)
  TaskStatus TaskLevelStatus(const SharedData &data) const {
    switch (data.taskstate.database.status) {
    case TaskState::Database::Status::Initializing:
      return {TaskStatus::Kind::Muted, "initializing"};
    case TaskState::Database::Status::Syncing:
      return {TaskStatus::Kind::Busy, "syncing"};
    case TaskState::Database::Status::Scanning:
      return {TaskStatus::Kind::Busy, "scanning"};
    case TaskState::Database::Status::NotScanned:
      return {TaskStatus::Kind::Warn, "not scanned"};
    case TaskState::Database::Status::Incomplete:
      return {TaskStatus::Kind::Warn, "incomplete"};
    case TaskState::Database::Status::Error:
      return {TaskStatus::Kind::Error, "error"};
    case TaskState::Database::Status::Ready:
      return {TaskStatus::Kind::Ready, "done"};
    case TaskState::Database::Status::None:
      break;
    }
    return {};
  }

  // Overview 子行标签: 基本面流水线状态 (BigQuant + Tushare → AssetInfo)
  TaskStatus OverviewStatus() const {
    if (!state_mgr_)
      return {TaskStatus::Kind::Muted, "initializing"};
    if (audit_svc_->busy()) {
      std::lock_guard lk(audit_svc_->mutex);
      if (audit_svc_->state.op == AuditOp::Audit)
        return {TaskStatus::Kind::Busy, "auditing"};
      if (audit_svc_->state.op == AuditOp::Repair)
        return {TaskStatus::Kind::Busy, "repairing"};
    }
    return FundamentalStatus();
  }

  // Encode 子行标签: 编码进行中显示进度, 否则显示 L2 覆盖检查结果
  TaskStatus EncodeStatus(const SharedData & /*data*/) const {
    if (!state_mgr_)
      return {};
    if (encoding_svc_->cancelling())
      return {TaskStatus::Kind::Busy, "cancelling"};
    if (encoding_svc_->busy())
      return ProgressStatus(*encoding_svc_);
    if (state_mgr_->encoding())
      return {TaskStatus::Kind::Busy, "encoding"};
    if (scan_svc_->busy())
      return ProgressStatus(*scan_svc_);
    if (state_mgr_->scanning())
      return {TaskStatus::Kind::Busy, "scanning"};
    switch (state_mgr_->get_state().check.status) {
    case DatabaseStatus::Unchecked:
      return {}; // 未扫描时任务行已有 not scanned, 子行不重复
    case DatabaseStatus::Pass:
      return {TaskStatus::Kind::Ready, "done"};
    case DatabaseStatus::Incomplete:
      return {TaskStatus::Kind::Warn, "incomplete"};
    case DatabaseStatus::NotEncoded:
      return {TaskStatus::Kind::Warn, "not encoded"};
    case DatabaseStatus::NeedArchive:
      return {TaskStatus::Kind::Error, "need archive"};
    case DatabaseStatus::NoData:
    case DatabaseStatus::Error:
      return {TaskStatus::Kind::Error, "error"};
    }
    return {};
  }

  // Table / Browser 子行标签: 纯浏览页, 状态跟随前置计算.
  // 前置 (基本面同步 / L2 扫描) 在跑 → 显示进行中; 解锁后 → done; 锁定且无在跑 → none
  TaskStatus BrowseStatus(const SharedData &data, bool can_access) const {
    if (data.taskstate.database.json_update_inflight)
      return {TaskStatus::Kind::Busy, "syncing"};
    if (data.taskstate.database.l2_scan_inflight)
      return {TaskStatus::Kind::Busy, "scanning"};
    if (can_access)
      return {TaskStatus::Kind::Ready, "done"};
    return {};
  }

  void RenderUI(int idx) {
    if (!state_mgr_) {
      ImGui::TextDisabled("Initializing services...");
      return;
    }

    // 状态已在 Update (帧首) 刷新, 这里只读
    const auto &state = state_mgr_->get_state();

    const DatabaseCheckResult &check_result = state.check;

    // Status indicator at top (流水线顺序: 基本面 → L2)
    const TaskStatus fstatus = FundamentalStatus();
    ImGui::Text("Fundamental: ");
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(fstatus.kind), "[%s]", fstatus.text.c_str());
    if (fstatus.kind == TaskStatus::Kind::Warn || fstatus.kind == TaskStatus::Kind::Error) {
      std::lock_guard lk(fundamental_svc_->mutex);
      ImGui::SameLine();
      ImGui::TextDisabled("%s", fundamental_svc_->state.message.c_str());
    }

    ImGui::SameLine();
    ImGui::TextDisabled("|");
    ImGui::SameLine();
    ImGui::Text("L2 Database: ");
    ImGui::SameLine();

    // L2 database coverage check (required_dates = 基本面交易日历)
    switch (check_result.status) {
    case DatabaseStatus::Unchecked:
      ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "[Not checked]");
      ImGui::SameLine();
      ImGui::TextDisabled("(scans after fundamental sync)");
      break;

    case DatabaseStatus::Pass:
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "[Pass]");
      break;

    case DatabaseStatus::Incomplete:
      ImGui::TextColored(ImVec4(1.0f, 0.95f, 0.3f, 1.0f), "[Incomplete]");
      ImGui::SameLine();
      ImGui::TextDisabled("(Missing %zu dates, %zu can encode)",
                          check_result.missing_dates.size(),
                          check_result.missing_can_encode.size());
      break;

    case DatabaseStatus::NeedArchive:
      ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.0f, 1.0f), "[NeedArchive]");
      ImGui::SameLine();
      ImGui::TextDisabled("(Missing %zu dates without archive)",
                          check_result.missing_no_archive.size());
      break;

    case DatabaseStatus::NotEncoded:
      ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "[NotEncoded]");
      ImGui::SameLine();
      ImGui::TextDisabled("(Archive available, need to encode)");
      break;

    case DatabaseStatus::NoData:
    case DatabaseStatus::Error:
      ImGui::TextColored(ImVec4(1.0f, 0.0f, 0.0f, 1.0f), "[ERROR]");
      ImGui::SameLine();
      ImGui::TextDisabled("%s", check_result.error_message.c_str());
      break;
    }

    ImGui::Separator();

    const auto &tabs = state.tabs;

    // 渲染当前选中 tab (左栏已按 can_access_* 屏蔽未解锁项, 这里直接分发)
    switch (idx) {
    case 0:
      DrawTabOverview();
      break;
    case 1:
      if (tabs.can_access_encode)
        DrawTabEncode();
      else
        ImGui::TextDisabled("Sync fundamental data in Overview tab first");
      break;
    case 2:
      if (tabs.can_access_table)
        DrawTabTable();
      else
        ImGui::TextDisabled("Requires fundamental data ready and at least one coverage scan (Encode tab)");
      break;
    case 3:
      if (tabs.can_access_browser)
        DrawTabBrowser();
      else
        ImGui::TextDisabled("Requires fundamental data ready and at least one coverage scan (Encode tab)");
      break;
    default:
      break;
    }
  }

  void DrawTabOverview() {
    // Safety: check if services are initialized
    if (!fundamental_svc_ || !l2_svc_) {
      ImGui::TextDisabled("Services initializing...");
      return;
    }

    bool update_clicked = false;
    bool update_cancel_clicked = false;
    bool audit_clicked = false;
    bool audit_incr_clicked = false;
    bool audit_cancel_clicked = false;
    std::string audit_table;
    std::string repair_table;

    // 编码读着 assetinfo / items: 编码在飞时基本面件不收, Update 也不给点
    const bool busy = state_mgr_->syncing() || state_mgr_->scanning() || state_mgr_->encoding();
    // 同步改写的正是被核对 / 修复的分片: 三者互斥
    const bool auditing = state_mgr_->auditing();
    const bool disable_audit = busy || auditing || !data_->config.network_sync;
    const TaskStatus fstatus = FundamentalStatus();

    {
      // 展示字段持锁读 (worker 只短锁写)
      std::scoped_lock lk(fundamental_svc_->mutex, audit_svc_->mutex);
      RenderTabOverview(
          fundamental_svc_->state,
          *fundamental_svc_,
          fstatus,
          &update_clicked,
          &update_cancel_clicked,
          busy || auditing,
          audit_svc_->state,
          *audit_svc_,
          &audit_clicked,
          &audit_incr_clicked,
          &audit_cancel_clicked,
          disable_audit,
          &audit_table,
          &repair_table);
    }

    // Handle button events (一帧至多一个请求)
    if (update_clicked && !busy && !auditing)
      state_mgr_->refresh();
    if (update_cancel_clicked)
      fundamental_svc_->RequestCancel();
    if (audit_cancel_clicked)
      audit_svc_->RequestCancel();
    if (disable_audit)
      return;
    if (audit_clicked || audit_incr_clicked)
      state_mgr_->audit(AuditOp::Audit, {}, audit_incr_clicked);
    else if (!audit_table.empty())
      state_mgr_->audit(AuditOp::Audit, audit_table, false);
    else if (!repair_table.empty()) // 修复改了分片: 收尾交接后 StateManager 走一遍 refresh (重建 AssetInfo → 补扫 L2)
      state_mgr_->audit(AuditOp::Repair, repair_table, false);
  }

  void DrawTabTable() {
    RenderTabTable(
        data_->asset,
        data_->assetinfo.get_stock_info(),
        table_state_);
  }

  void DrawTabBrowser() {
    // date_stats 由扫描的 Phase 5 统一产出 (ScanService::run → Asset::adopt), 这里不再
    // 惰性补算: 渲染帧里现算是双重遍历, 而且会跟扫描抢着写同一份统计.
    // 扫描没跑完时 date_stats 是空的, 表格照常渲染, 数字等扫描落地.
    RenderTabBrowser(
        data_->assetinfo.get_stock_days(),
        data_->assetinfo.get_stock_factor(),
        data_->assetinfo.get_stock_info(),
        data_->asset,
        config_->start_date,
        config_->end_date,
        browser_state_);
  }

  void DrawTabEncode() {
    RenderTabEncode(*state_mgr_, *encoding_svc_, *filecheck_svc_, *scan_svc_, encode_state_, data_->asset);
  }
};

} // namespace

TaskHandle CreateDatabaseTask() {
  auto instance = std::make_shared<DatabaseTask>();

  TaskHandle handle;
  handle.name = instance->GetName();
  handle.storage = instance;
  // 子项 (叶子) 顺序: Overview / Encode / Table / Browser
  handle.tabs = {"Overview", "Encode", "Table", "Browser"};
  handle.Init = [instance](SharedData &data) { instance->Init(data); };
  handle.Update = [instance](SharedData &data) { instance->Update(data); };
  handle.OnExpand = [instance]() { instance->OnExpand(); };
  handle.OnCollapse = [instance]() { instance->OnCollapse(); };
  handle.Status = [instance](const SharedData &data, int idx) { return instance->Status(data, idx); };
  handle.Enabled = [instance](const SharedData &data, int idx) { return instance->Enabled(data, idx); };
  handle.Draw = [instance](SharedData &data, int idx) { instance->Draw(data, idx); };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
