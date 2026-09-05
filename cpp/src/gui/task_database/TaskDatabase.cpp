#include "gui/task_database/TaskDatabase.hpp"
#include "gui/Tasks.hpp"
#include "gui/coro/CoroManager.hpp"
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
#include "shared/SharedData.hpp"
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <memory>
#include <thread>

namespace GUI::Tasks {
namespace {

using namespace GUI::Database;

class DatabaseTask {
private:
  // Service layer
  std::unique_ptr<FundamentalService> fundamental_svc_;
  std::unique_ptr<ScanService> scan_svc_;
  std::unique_ptr<EncodingService> encoding_svc_;
  std::unique_ptr<L2DatabaseService> l2_svc_;
  std::unique_ptr<StateManager> state_mgr_;

  // UI state
  EncodeState encode_state_;
  TableState table_state_;
  BrowserState browser_state_;

  // Lifecycle
  bool is_expanded_ = false;
  bool initialized_ = false;
  // IsTabEnabled 缓存: DrawTab 每帧更新, 左栏渲染时读 (一帧延迟不可见)
  // 默认仅 Overview 可进 (基本面未同步前其它 tab 锁定)
  bool tab_enabled_cache_[4] = {true, false, false, false};
  CoroManager *coro_mgr_ = nullptr;
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
    coro_mgr_ = &data.coromgr;
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

  // IsTabEnabled: 左栏叶子是否可用 (读上一帧 DrawTab 缓存的使能状态)
  // idx: 0=Overview 1=Encode 2=Table 3=Browser
  bool IsTabEnabled(int idx) const {
    if (idx < 0 || idx >= 4)
      return false;
    return tab_enabled_cache_[idx];
  }

  // DrawTab: 渲染指定 tab (状态头 + 对应 DrawTabX) + 处理编码触发
  void DrawTab(SharedData &data, int idx) {
    assert(idx >= 0 && idx < 4);
    RenderUI(idx);

    // Handle encoding trigger from UI (Encode tab 设置 trigger_start)
    if (encode_state_.trigger_start && encoding_svc_ && !encoding_svc_->is_running()) {
      encode_state_.trigger_start = false;

      int workers = encode_state_.num_workers;
      if (workers <= 0) {
        workers = std::thread::hardware_concurrency();
        if (workers <= 0)
          workers = 8;
      }

      // Start encoding in background thread (non-blocking)
      encoding_svc_->start_encoding(workers, encode_state_.skip_existing);
    }
  }

private:
  // Trigger unified refresh flow: sync fundamental parquet + rebuild AssetInfo
  // A 轴/日历来自 parquet 数据源; 成功后补扫 L2 (日历可能延长, 覆盖判定要重算)
  void TriggerRefreshFlow() {
    auto &ts = data_->taskstate.database;
    if (ts.json_update_inflight || ts.l2_scan_inflight || !state_mgr_)
      return;

    auto &io = coro_mgr_->GetIoContext();
    ts.json_update_inflight = true;

    boost::asio::co_spawn(
        io,
        [this]() -> boost::asio::awaitable<void> {
          auto &ts = data_->taskstate.database;
          struct FlagReset {
            bool &flag;
            ~FlagReset() { flag = false; }
          } update_reset{ts.json_update_inflight};

          co_await state_mgr_->sync_and_scan(ScanMode::RecomputeCoverage);
          UpdateTaskState();
        },
        boost::asio::detached);
  }

  void InitializeServices(SharedData &data) {
    auto &io = coro_mgr_->GetIoContext();

    // Create services (in dependency order)
    fundamental_svc_ = std::make_unique<FundamentalService>(io, data);
    scan_svc_ = std::make_unique<ScanService>(data, io);
    encoding_svc_ = std::make_unique<EncodingService>(data);
    l2_svc_ = std::make_unique<L2DatabaseService>(data);
    state_mgr_ = std::make_unique<StateManager>(data, fundamental_svc_.get(), scan_svc_.get());

    // Set encoding completion callback to trigger scan
    encoding_svc_->set_scan_callback([this]() {
      scan_svc_->trigger_scan(ScanMode::RescanStorage);
    });

    // Set scan completion callback to update task state
    scan_svc_->set_on_complete([this]() {
      state_mgr_->refresh_state();
      UpdateTaskState();
    });

    // Initialize: 本地 parquet → AssetInfo
    // Non-blocking, user sees progress in terminal
    boost::asio::co_spawn(
        io,
        [this]() -> boost::asio::awaitable<void> {
          co_await state_mgr_->initialize();
          // Update task state after initialization completes
          UpdateTaskState();
        },
        boost::asio::detached);
  }

  void UpdateTaskState() {
    auto &ts = data_->taskstate.database;

    if (!state_mgr_ || !scan_svc_) {
      ts.status = TaskState::Database::Status::Initializing;
      return;
    }

    auto check_result = scan_svc_->get_last_check_result();
    const auto &state = state_mgr_->get_state();

    // Update flags
    ts.binary_scanned = (check_result.status != DatabaseStatus::Unchecked);
    ts.binary_pass = (check_result.status == DatabaseStatus::Pass);
    ts.all_json_ready = state.all_json_ready();

    // Update status
    if (check_result.status == DatabaseStatus::Error ||
        check_result.status == DatabaseStatus::NoData ||
        check_result.status == DatabaseStatus::NeedArchive) {
      ts.status = TaskState::Database::Status::Error;
    } else if (check_result.status != DatabaseStatus::Pass || !state.all_json_ready()) {
      ts.status = TaskState::Database::Status::Incomplete;
    } else {
      ts.status = TaskState::Database::Status::Ready;
    }
  }

  void RenderUI(int idx) {
    if (!state_mgr_) {
      ImGui::TextDisabled("Initializing services...");
      return;
    }

    // Refresh state before rendering
    state_mgr_->refresh_state();
    UpdateTaskState();
    const auto &state = state_mgr_->get_state();

    // Get database check result from scan service
    auto check_result = scan_svc_->get_last_check_result();

    // Status indicator at top (流水线顺序: 基本面 → L2)
    const auto &fstate = fundamental_svc_->get_state();
    ImGui::Text("Fundamental: ");
    ImGui::SameLine();
    switch (fstate.status) {
    case FundamentalStatus::Ready:
      ImGui::TextColored(ImVec4(0.3f, 0.95f, 0.4f, 1.0f), "[Ready]");
      break;
    case FundamentalStatus::Updating:
    case FundamentalStatus::Building:
      ImGui::TextColored(ImVec4(1.0f, 0.95f, 0.3f, 1.0f), "[%s]",
                         GetFundamentalStatusName(fstate.status));
      break;
    case FundamentalStatus::Error:
      ImGui::TextColored(ImVec4(1.0f, 0.3f, 0.0f, 1.0f), "[Error]");
      ImGui::SameLine();
      ImGui::TextDisabled("%s", fstate.message.c_str());
      break;
    case FundamentalStatus::Idle:
      ImGui::TextColored(ImVec4(0.7f, 0.7f, 0.7f, 1.0f), "[Idle]");
      break;
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

    // 缓存 tab 使能状态供左栏 IsTabEnabled 下一帧读
    const auto &tabs = state.tabs;
    tab_enabled_cache_[0] = true; // Overview 永远可进
    tab_enabled_cache_[1] = tabs.can_access_encode;
    tab_enabled_cache_[2] = tabs.can_access_table;
    tab_enabled_cache_[3] = tabs.can_access_browser;

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

    auto &ts = data_->taskstate.database;
    bool busy = ts.json_update_inflight || ts.l2_scan_inflight ||
                fundamental_svc_->is_busy() || scan_svc_->is_scanning();

    RenderTabOverview(
        fundamental_svc_->get_state(),
        &update_clicked,
        busy);

    // Handle button events
    if (update_clicked && !busy) {
      TriggerRefreshFlow();
    }
  }

  void DrawTabTable() {
    RenderTabTable(
        data_->asset,
        data_->assetinfo.get_stock_info(),
        table_state_);
  }

  void DrawTabBrowser() {
    // date_stats 由扫描的 Phase 5 统一产出 (ScanService::coro_scan), 这里不再
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
    if (!encoding_svc_ || !scan_svc_ || !data_) {
      ImGui::TextDisabled("Services not initialized...");
      return;
    }
    RenderTabEncode(encoding_svc_.get(), scan_svc_.get(), encode_state_, data_->asset);
  }
};

} // namespace

TaskHandle CreateDatabaseTask() {
  auto instance = std::make_shared<DatabaseTask>();

  TaskHandle handle;
  handle.name = instance->GetName();
  handle.task_instance = instance.get();
  handle.storage = instance;
  handle.Init = [instance](SharedData &data) { instance->Init(data); };
  handle.OnExpand = [instance]() { instance->OnExpand(); };
  handle.OnCollapse = [instance]() { instance->OnCollapse(); };
  // 子项 (叶子) 顺序: Overview / Encode / Table / Browser
  handle.tab_names = {"Overview", "Encode", "Table", "Browser"};
  handle.IsTabEnabled = [instance](int i) -> bool {
    return instance->IsTabEnabled(i);
  };
  handle.DrawTab = [instance](SharedData &data, int idx) {
    instance->DrawTab(data, idx);
  };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
