```
┌────────────────────────────────────────────────────────────────────────────┐
│ [Update All] [Scan Assets] [Check Integrity]                               │
├─────────────────────────────────────┬──────────────────────────────────────┤
│ Left Panel (60%): JSON Files        │ Right Panel (40%): Crawler Monitor   │
│                                     │                                      │
│ ┌─ ✓ stock_factor.json ──────────┐ │ ┌─ Baostock Crawler ───────────────┐ │
│ │ Status: [Ready]                 │ │ │ Status: Complete                 │ │
│ │ Stocks: 8  Records: 1,234       │ │ │ Workers: 0/4 active              │ │
│ │ Last Update: 2025-11-25 09:30   │ │ │ Throughput: 0 req/s              │ │
│ │ [Force Update] [Force Remove]   │ │ │ Success: 100.0%  Errors: 0       │ │
│ └─────────────────────────────────┘ │ │                                  │ │
│                                     │ │ Progress: 8/8                    │ │
│ ┌─ ⚠ stock_info.json ────────────┐ │ │ Elapsed: 2s  ETA: 0s             │ │
│ │ Status: [Outdated]              │ │ │                                  │ │
│ │ Stocks: 8  Complete: 100%       │ │ │ [Pause All] [Stop All]           │ │
│ │ Warnings: 2 incomplete stocks   │ │ └──────────────────────────────────┘ │
│ │ [Force Update] [Force Remove]   │ │                                      │
│ └─────────────────────────────────┘ │                                      │
│                                     │                                      │
│ ┌─ ✓ stock_days.json ────────────┐ │                                      │
│ │ Status: [Ready]                 │ │                                      │
│ │ Range: 2024-01-01 ~ 2025-11-25  │ │                                      │
│ │ Trading Days: 245/330 days      │ │                                      │
│ │ [Force Update] [Force Remove]   │ │                                      │
│ └─────────────────────────────────┘ │                                      │
│                                     │                                      │
│ ┌─ L2 Database Summary ───────────┐ │                                      │
│ │ Assets: 500  Encoded: 480       │ │                                      │
│ │ Coverage: 96.0%  Disk: 45.3 GB  │ │                                      │
│ └─────────────────────────────────┘ │                                      │
└─────────────────────────────────────┴──────────────────────────────────────┘
```

**stock_factor.json** - Adjust factors per stock
```json
{
  "sh.600000": [
    ["1999-11-10", "1.000000"],
    ["2000-11-10", "1.006502"]
  ]
}
```

**stock_info.json** - Stock metadata (weekly + daily fields)
```json
{
  "sh.600000": {
    "name": "浦发银行",
    "ipoDate": "1999-11-10",
    "outDate": "",
    "ind_code": "J66",
    "ind_name": "货币金融服务",
    "update_date": "2025-11-03",
    "volume": "123384169",
    "amount": "1414090218.4300",
    "turn": "0.370500",
    "tradestatus": "1",
    "isST": "0",
    "peTTM": "7.778839",
    "pbMRQ": "0.542106",
    "psTTM": "2.194248",
    "pcfNcfTTM": "-1.792867"
  }
}
```

**stock_days.json** - Trading calendar
```json
[
  ["2024-11-01", "1"],
  ["2024-11-02", "0"]
]
```

# Database Overview Tab - 伪代码流程

## 核心内存数据结构

### TaskDatabase 主控制器

```cpp
class TaskDatabase {
  // 状态管理
  DataManager data_mgr_;                    // JSON文件管理器
  StateManager state_mgr_;                  // JSON状态跟踪
  CoroScanner scanner_;                     // L2数据库扫描器
  BaostockService baostock_svc_;            // Baostock API服务
  
  // 运行时状态
  bool data_mgr_updating_;                  // 正在更新JSON文件
  bool scanner_running_;                    // 正在扫描L2数据库
  bool auto_update_triggered_;              // 自动更新已触发（防重入）
  
  // 配置和缓存
  std::string config_dir_;                  // "config/" 目录路径
  std::vector<std::string> stock_list_;     // 股票代码列表
  
  // UI状态
  std::string selected_tab_;                // 当前选中的标签页
  bool prev_overview_active_;               // 上一帧Overview标签是否活跃
};
```

### DataManager 数据管理器

```cpp
class DataManager {
  // JSON文件状态
  struct FileState {
    enum Status { Ready, Outdated, Error, Updating };
    Status status;
    std::string error_msg;
    std::chrono::system_clock::time_point last_update;
  };
  
  FileState stock_factor_state_;
  FileState stock_info_state_;
  FileState stock_days_state_;
  
  // 内存数据
  std::map<std::string, std::vector<std::pair<std::string, std::string>>> stock_factor_data_;
  std::map<std::string, StockInfo> stock_info_data_;
  std::vector<std::pair<std::string, std::string>> stock_days_data_;
  
  // 更新控制
  std::atomic<int> pending_tasks_;          // 待完成的任务数
  std::vector<std::string> update_errors_;  // 更新错误日志
};
```

### CoroScanner L2扫描器

```cpp
class CoroScanner {
  struct ScanResult {
    int total_assets;
    int encoded_assets;
    double total_size_gb;
    std::map<std::string, L2AssetData> asset_map;  // code -> metadata
  };
  
  ScanResult current_result_;
  std::atomic<bool> scanning_;
  std::atomic<int> progress_;
  std::atomic<int> total_;
};
```

### BaostockService 爬虫服务

```cpp
class BaostockService {
  BaostockPool connection_pool_;            // 连接池（4个worker）
  
  struct CrawlerStats {
    int active_workers;
    int total_tasks;
    int completed_tasks;
    int failed_tasks;
    double throughput;                      // req/s
    std::chrono::steady_clock::time_point start_time;
  };
  
  CrawlerStats stats_;
  std::queue<Task> task_queue_;
  std::atomic<bool> paused_;
  std::atomic<bool> stopping_;
};
```

---

## 流程1: 打开Overview标签页

### 触发条件
- 用户点击"Overview"标签
- `prev_overview_active_ == false && current_tab == "Overview"`

### 伪代码流程

```cpp
void TaskDatabase::DrawOverviewTab() {
  // 1. 检测标签页切换
  bool overview_active = ImGui::IsItemActive();  // 当前帧Overview是否活跃
  
  if (overview_active && !prev_overview_active_) {
    // 2. 标签页刚刚被激活，触发自动更新
    if (!auto_update_triggered_) {
      OnOverviewTabOpened();
      auto_update_triggered_ = true;
    }
  }
  
  prev_overview_active_ = overview_active;
  
  // 3. 绘制UI
  DrawTopButtons();        // [Update All] [Scan Assets] [Check Integrity]
  DrawLeftPanel();         // JSON文件状态卡片
  DrawRightPanel();        // Crawler Monitor
}

void TaskDatabase::OnOverviewTabOpened() {
  // 并行启动两个协程
  coro_mgr_->spawn([this]() -> asio::awaitable<void> {
    co_await UpdateAll(/*force=*/false);
  });
  
  coro_mgr_->spawn([this]() -> asio::awaitable<void> {
    co_await ScanDatabase();
  });
}
```

### 数据流向

```
用户点击"Overview"
    ↓
prev_overview_active_ = false → true
    ↓
触发 OnOverviewTabOpened()
    ↓
  ┌─────────────────┴─────────────────┐
  ↓                                   ↓
UpdateAll(false)               ScanDatabase()
  ↓                                   ↓
data_mgr_updating_ = true      scanner_running_ = true
  ↓                                   ↓
检查每个JSON文件状态              扫描L2二进制文件
  ↓                                   ↓
提交Baostock任务                 更新 scanner_.current_result_
  ↓                                   ↓
data_mgr_updating_ = false     scanner_running_ = false
```

---

## 流程2: [Update All] 按钮

### 触发条件
- 用户手动点击"Update All"按钮

### 伪代码流程

```cpp
void TaskDatabase::DrawTopButtons() {
  if (ImGui::Button("Update All")) {
    OnUpdateAllClicked();
  }
}

void TaskDatabase::OnUpdateAllClicked() {
  if (data_mgr_updating_) {
    return;  // 已在更新中，忽略
  }
  
  // 强制更新（force=true）
  coro_mgr_->spawn([this]() -> asio::awaitable<void> {
    co_await UpdateAll(/*force=*/true);
  });
}

asio::awaitable<void> TaskDatabase::UpdateAll(bool force) {
  data_mgr_updating_ = true;
  
  // Step 1: 更新 stock_days.json
  co_await UpdateStockDays();
  
  // Step 2: 更新 stock_factor.json
  co_await UpdateStockFactor(force);
  
  // Step 3: 更新 stock_info.json（基础 + 每日）
  co_await UpdateStockInfoBasic(force);
  co_await UpdateStockInfoDaily();
  
  // Step 4: 完整性检查
  CheckAllIntegrity();
  
  data_mgr_updating_ = false;
}
```

### 内存数据结构变化

**Before:**
```cpp
data_mgr_.stock_factor_state_.status = Outdated;
data_mgr_.stock_info_state_.status = Outdated;
data_mgr_.stock_days_state_.status = Ready;
```

**During:**
```cpp
data_mgr_updating_ = true;
baostock_svc_.stats_.active_workers = 4;
baostock_svc_.stats_.total_tasks = 16;  // 8 stocks × 2 types
```

**After:**
```cpp
// 内存数据更新
data_mgr_.stock_factor_data_["sh.600000"].push_back({"2025-11-25", "1.008234"});
data_mgr_.stock_info_data_["sh.600000"].update_date = "2025-11-25";
data_mgr_.stock_days_data_.push_back({"2025-11-25", "1"});

// 状态更新
data_mgr_.stock_factor_state_.status = Ready;
data_mgr_.stock_info_state_.status = Ready;
data_mgr_.stock_days_state_.status = Ready;
data_mgr_updating_ = false;
```

### 详细子流程

#### UpdateStockDays()

```cpp
asio::awaitable<void> TaskDatabase::UpdateStockDays() {
  // 1. 加载现有数据
  auto existing_data = LoadJSON("config/stock_days.json");
  
  // 2. 检查是否需要更新
  std::string last_date = existing_data.empty() ? "1990-01-01" : existing_data.back().first;
  std::string today = GetTodayDate();
  
  if (last_date >= today) {
    return;  // 已是最新，跳过
  }
  
  // 3. 查询新数据
  auto new_days = co_await baostock_svc_.QueryTradeDates(last_date, today);
  
  // 4. 合并并去重
  existing_data.insert(existing_data.end(), new_days.begin(), new_days.end());
  auto deduped = DeduplicateAndSort(existing_data);
  
  // 5. 保存到文件
  SaveJSON("config/stock_days.json", deduped);
  
  // 6. 更新内存
  data_mgr_.stock_days_data_ = deduped;
  data_mgr_.stock_days_state_.status = Ready;
}
```

#### UpdateStockFactor(force)

```cpp
asio::awaitable<void> TaskDatabase::UpdateStockFactor(bool force) {
  auto existing_data = LoadJSON("config/stock_factor.json");
  
  bool is_monday = GetWeekday() == 1;
  bool should_update = force || is_monday;
  
  std::vector<Task> tasks;
  
  for (const auto& code : stock_list_) {
    // 检查该股票是否需要更新
    bool has_data = existing_data.contains(code);
    std::string start_date = has_data ? existing_data[code].back().first : GetIPODate(code);
    
    if (!has_data || should_update) {
      tasks.push_back({
        .type = TaskType::AdjustFactor,
        .code = code,
        .start_date = start_date,
        .end_date = GetTodayDate()
      });
    }
  }
  
  if (tasks.empty()) {
    return;  // 无需更新
  }
  
  // 提交到连接池并发执行
  auto results = co_await baostock_svc_.ExecuteTasks(tasks);
  
  // 合并结果
  for (const auto& [code, new_records] : results) {
    auto& stock_data = existing_data[code];
    stock_data.insert(stock_data.end(), new_records.begin(), new_records.end());
    stock_data = DeduplicateAndSort(stock_data);
  }
  
  SaveJSON("config/stock_factor.json", existing_data);
  data_mgr_.stock_factor_data_ = existing_data;
  data_mgr_.stock_factor_state_.status = Ready;
}
```

#### UpdateStockInfoBasic(force)

```cpp
asio::awaitable<void> TaskDatabase::UpdateStockInfoBasic(bool force) {
  auto existing_data = LoadJSON("config/stock_info.json");
  
  bool is_monday = GetWeekday() == 1;
  bool should_update = force || is_monday;
  
  std::vector<Task> tasks;
  
  for (const auto& code : stock_list_) {
    bool missing_basic = !existing_data.contains(code) || 
                         existing_data[code].name.empty();
    
    if (missing_basic || should_update) {
      tasks.push_back({
        .type = TaskType::StockBasic,
        .code = code
      });
    }
  }
  
  auto results = co_await baostock_svc_.ExecuteTasks(tasks);
  
  for (const auto& [code, info] : results) {
    existing_data[code].name = info.name;
    existing_data[code].ipoDate = info.ipoDate;
    existing_data[code].outDate = info.outDate;
    existing_data[code].ind_code = info.ind_code;
    existing_data[code].ind_name = info.ind_name;
  }
  
  SaveJSON("config/stock_info.json", existing_data);
  data_mgr_.stock_info_data_ = existing_data;
}
```

#### UpdateStockInfoDaily()

```cpp
asio::awaitable<void> TaskDatabase::UpdateStockInfoDaily() {
  auto existing_data = data_mgr_.stock_info_data_;
  std::string last_trading_day = GetLastTradingDay();
  
  std::vector<Task> tasks;
  
  for (const auto& code : stock_list_) {
    // 跳过退市股票
    if (!existing_data[code].outDate.empty()) {
      continue;
    }
    
    // 检查是否需要更新
    if (existing_data[code].update_date < last_trading_day) {
      tasks.push_back({
        .type = TaskType::ProfitData,
        .code = code,
        .date = last_trading_day
      });
    }
  }
  
  auto results = co_await baostock_svc_.ExecuteTasks(tasks);
  
  for (const auto& [code, daily_info] : results) {
    existing_data[code].update_date = last_trading_day;
    existing_data[code].volume = daily_info.volume;
    existing_data[code].amount = daily_info.amount;
    existing_data[code].turn = daily_info.turn;
    existing_data[code].peTTM = daily_info.peTTM;
    existing_data[code].pbMRQ = daily_info.pbMRQ;
  }
  
  SaveJSON("config/stock_info.json", existing_data);
  data_mgr_.stock_info_data_ = existing_data;
  data_mgr_.stock_info_state_.status = Ready;
}
```

---

## 流程3: [Scan Assets] 按钮

### 触发条件
- 用户手动点击"Scan Assets"按钮

### 伪代码流程

```cpp
void TaskDatabase::DrawTopButtons() {
  ImGui::SameLine();
  if (ImGui::Button("Scan Assets")) {
    OnScanAssetsClicked();
  }
}

void TaskDatabase::OnScanAssetsClicked() {
  if (scanner_running_) {
    return;  // 已在扫描中，忽略
  }
  
  coro_mgr_->spawn([this]() -> asio::awaitable<void> {
    co_await ScanDatabase();
  });
}

asio::awaitable<void> TaskDatabase::ScanDatabase() {
  scanner_running_ = true;
  scanner_.progress_ = 0;
  
  // 1. 获取所有资产目录
  auto asset_dirs = GetAllAssetDirectories("/path/to/l2_db");
  scanner_.total_ = asset_dirs.size();
  
  // 2. 遍历每个资产目录
  CoroScanner::ScanResult result;
  result.total_assets = asset_dirs.size();
  
  for (const auto& dir : asset_dirs) {
    // 解析资产代码
    std::string code = ExtractCodeFromPath(dir);
    
    // 读取二进制文件元数据
    L2AssetData asset_data;
    asset_data.code = code;
    asset_data.encoded_days = CountEncodedDays(dir);
    asset_data.total_days = GetTotalTradingDays();
    asset_data.coverage_pct = (double)asset_data.encoded_days / asset_data.total_days;
    asset_data.disk_size = GetDirectorySize(dir);
    
    result.asset_map[code] = asset_data;
    
    if (asset_data.encoded_days > 0) {
      result.encoded_assets++;
    }
    
    result.total_size_gb += asset_data.disk_size;
    
    scanner_.progress_++;
    co_await asio::this_coro::yield();  // 让出控制权
  }
  
  // 3. 保存结果
  scanner_.current_result_ = result;
  scanner_running_ = false;
}
```

### 内存数据结构变化

**Before:**
```cpp
scanner_.current_result_ = {
  .total_assets = 0,
  .encoded_assets = 0,
  .total_size_gb = 0.0,
  .asset_map = {}
};
scanner_running_ = false;
```

**During:**
```cpp
scanner_running_ = true;
scanner_.progress_ = 250;
scanner_.total_ = 500;
```

**After:**
```cpp
scanner_.current_result_ = {
  .total_assets = 500,
  .encoded_assets = 480,
  .total_size_gb = 45.3,
  .asset_map = {
    {"sh.600000", {.code="sh.600000", .encoded_days=245, .coverage_pct=0.98, ...}},
    {"sh.600519", {.code="sh.600519", .encoded_days=240, .coverage_pct=0.96, ...}},
    // ... 498 more entries
  }
};
scanner_running_ = false;
```

### UI显示

```cpp
void TaskDatabase::DrawLeftPanel() {
  // ... JSON文件卡片 ...
  
  // L2 Database Summary
  ImGui::BeginChild("L2Summary");
  ImGui::Text("L2 Database Summary");
  ImGui::Separator();
  
  auto& result = scanner_.current_result_;
  ImGui::Text("Assets: %d  Encoded: %d", result.total_assets, result.encoded_assets);
  
  double coverage = (double)result.encoded_assets / result.total_assets * 100.0;
  ImGui::Text("Coverage: %.1f%%  Disk: %.1f GB", coverage, result.total_size_gb);
  
  ImGui::EndChild();
}
```

---

## 流程4: [Check Integrity] 按钮

### 触发条件
- 用户手动点击"Check Integrity"按钮

### 伪代码流程

```cpp
void TaskDatabase::DrawTopButtons() {
  ImGui::SameLine();
  if (ImGui::Button("Check Integrity")) {
    OnCheckIntegrityClicked();
  }
}

void TaskDatabase::OnCheckIntegrityClicked() {
  CheckAllIntegrity();
}

void TaskDatabase::CheckAllIntegrity() {
  // 检查 stock_factor.json
  auto factor_errors = CheckStockFactorIntegrity();
  if (factor_errors.empty()) {
    data_mgr_.stock_factor_state_.status = Ready;
  } else {
    data_mgr_.stock_factor_state_.status = Error;
    data_mgr_.stock_factor_state_.error_msg = JoinErrors(factor_errors);
  }
  
  // 检查 stock_info.json
  auto info_errors = CheckStockInfoIntegrity();
  if (info_errors.empty()) {
    data_mgr_.stock_info_state_.status = Ready;
  } else {
    data_mgr_.stock_info_state_.status = Error;
    data_mgr_.stock_info_state_.error_msg = JoinErrors(info_errors);
  }
  
  // 检查 stock_days.json
  auto days_errors = CheckStockDaysIntegrity();
  if (days_errors.empty()) {
    data_mgr_.stock_days_state_.status = Ready;
  } else {
    data_mgr_.stock_days_state_.status = Error;
    data_mgr_.stock_days_state_.error_msg = JoinErrors(days_errors);
  }
}

std::vector<std::string> TaskDatabase::CheckStockFactorIntegrity() {
  std::vector<std::string> errors;
  
  // 规则1: 所有股票必须有记录
  for (const auto& code : stock_list_) {
    if (!data_mgr_.stock_factor_data_.contains(code)) {
      errors.push_back(fmt::format("Missing data for {}", code));
    }
  }
  
  // 规则2: 日期必须排序且无重复
  for (const auto& [code, records] : data_mgr_.stock_factor_data_) {
    std::string prev_date = "";
    for (const auto& [date, factor] : records) {
      if (!prev_date.empty() && date <= prev_date) {
        errors.push_back(fmt::format("{}: dates not sorted or duplicated", code));
        break;
      }
      prev_date = date;
    }
  }
  
  return errors;
}

std::vector<std::string> TaskDatabase::CheckStockInfoIntegrity() {
  std::vector<std::string> errors;
  
  // 规则1: 所有股票必须有记录
  for (const auto& code : stock_list_) {
    if (!data_mgr_.stock_info_data_.contains(code)) {
      errors.push_back(fmt::format("Missing entry for {}", code));
      continue;
    }
    
    const auto& info = data_mgr_.stock_info_data_[code];
    
    // 规则2: 活跃股票必须有基础信息
    if (info.outDate.empty()) {
      if (info.name.empty()) {
        errors.push_back(fmt::format("{}: missing name", code));
      }
      if (info.ipoDate.empty()) {
        errors.push_back(fmt::format("{}: missing ipoDate", code));
      }
    }
  }
  
  return errors;
}

std::vector<std::string> TaskDatabase::CheckStockDaysIntegrity() {
  std::vector<std::string> errors;
  auto& days = data_mgr_.stock_days_data_;
  
  // 规则1: 日期连续性检查
  for (size_t i = 1; i < days.size(); i++) {
    auto prev_date = ParseDate(days[i-1].first);
    auto curr_date = ParseDate(days[i].first);
    
    if ((curr_date - prev_date).days() > 1) {
      errors.push_back(fmt::format("Date gap: {} to {}", days[i-1].first, days[i].first));
    }
  }
  
  // 规则2: is_trading_day 必须是 "0" 或 "1"
  for (const auto& [date, flag] : days) {
    if (flag != "0" && flag != "1") {
      errors.push_back(fmt::format("{}: invalid trading flag '{}'", date, flag));
    }
  }
  
  // 规则3: 无重复日期
  std::set<std::string> seen;
  for (const auto& [date, _] : days) {
    if (seen.contains(date)) {
      errors.push_back(fmt::format("Duplicate date: {}", date));
    }
    seen.insert(date);
  }
  
  return errors;
}
```

### 内存数据结构变化

**触发前:**
```cpp
data_mgr_.stock_factor_state_ = {.status = Ready, .error_msg = ""};
data_mgr_.stock_info_state_ = {.status = Outdated, .error_msg = ""};
data_mgr_.stock_days_state_ = {.status = Ready, .error_msg = ""};
```

**检查后（假设stock_info有问题）:**
```cpp
data_mgr_.stock_factor_state_ = {.status = Ready, .error_msg = ""};
data_mgr_.stock_info_state_ = {
  .status = Error,
  .error_msg = "sh.600000: missing name; sz.000001: missing ipoDate"
};
data_mgr_.stock_days_state_ = {.status = Ready, .error_msg = ""};
```

### UI显示

```cpp
void TaskDatabase::DrawLeftPanel() {
  // stock_factor.json 卡片
  ImGui::BeginChild("StockFactorCard");
  
  const char* icon = "✓";
  ImVec4 color = ImVec4(0, 1, 0, 1);  // 绿色
  
  if (data_mgr_.stock_factor_state_.status == Error) {
    icon = "✗";
    color = ImVec4(1, 0, 0, 1);  // 红色
  } else if (data_mgr_.stock_factor_state_.status == Outdated) {
    icon = "⚠";
    color = ImVec4(1, 1, 0, 1);  // 黄色
  }
  
  ImGui::TextColored(color, "%s stock_factor.json", icon);
  
  if (!data_mgr_.stock_factor_state_.error_msg.empty() && 
      ImGui::IsItemHovered()) {
    ImGui::SetTooltip("%s", data_mgr_.stock_factor_state_.error_msg.c_str());
  }
  
  ImGui::EndChild();
}
```

---

## 流程5: [Force Update] 按钮（单个JSON文件）

### 触发条件
- 用户点击某个JSON文件卡片上的"Force Update"按钮

### 伪代码流程

```cpp
void TaskDatabase::DrawLeftPanel() {
  // stock_factor.json 卡片
  ImGui::BeginChild("StockFactorCard");
  ImGui::Text("✓ stock_factor.json");
  ImGui::Text("Status: [Ready]");
  ImGui::Text("Stocks: 8  Records: 1,234");
  
  if (ImGui::Button("Force Update##factor")) {
    OnForceUpdateStockFactor();
  }
  ImGui::EndChild();
}

void TaskDatabase::OnForceUpdateStockFactor() {
  if (data_mgr_updating_) {
    return;  // 已在更新中，忽略
  }
  
  coro_mgr_->spawn([this]() -> asio::awaitable<void> {
    data_mgr_updating_ = true;
    co_await UpdateStockFactor(/*force=*/true);
    CheckStockFactorIntegrity();
    data_mgr_updating_ = false;
  });
}
```

### 数据流向

```
用户点击 [Force Update##factor]
    ↓
OnForceUpdateStockFactor()
    ↓
UpdateStockFactor(force=true)
    ↓
为所有8只股票创建任务（无论是否已最新）
    ↓
baostock_svc_.ExecuteTasks(tasks)
    ↓
tasks = [
  {type: AdjustFactor, code: "sh.600000", start_date: "2025-11-24", end_date: "2025-11-25"},
  {type: AdjustFactor, code: "sh.600519", start_date: "2025-11-24", end_date: "2025-11-25"},
  ... (6 more)
]
    ↓
连接池并发执行（4 workers）
    ↓
合并结果到内存
    ↓
SaveJSON("config/stock_factor.json", data)
    ↓
CheckStockFactorIntegrity()
    ↓
data_mgr_.stock_factor_state_.status = Ready
```

---

## 流程6: [Force Remove] 按钮（单个JSON文件）

### 触发条件
- 用户点击某个JSON文件卡片上的"Force Remove"按钮

### 伪代码流程

```cpp
void TaskDatabase::DrawLeftPanel() {
  ImGui::BeginChild("StockFactorCard");
  // ...
  
  ImGui::SameLine();
  if (ImGui::Button("Force Remove##factor")) {
    OnForceRemoveStockFactor();
  }
  ImGui::EndChild();
}

void TaskDatabase::OnForceRemoveStockFactor() {
  // 1. 备份现有文件
  std::string src = "config/stock_factor.json";
  std::string backup = "config/stock_factor.json.backup";
  std::filesystem::copy_file(src, backup, std::filesystem::copy_options::overwrite_existing);
  
  // 2. 删除文件
  std::filesystem::remove(src);
  
  // 3. 清空内存数据
  data_mgr_.stock_factor_data_.clear();
  
  // 4. 更新状态
  data_mgr_.stock_factor_state_.status = Error;
  data_mgr_.stock_factor_state_.error_msg = "File removed";
}
```

### 内存数据结构变化

**Before:**
```cpp
data_mgr_.stock_factor_data_ = {
  {"sh.600000", {{"1999-11-10", "1.0"}, {"2025-11-25", "1.008234"}}},
  {"sh.600519", {{"2001-08-27", "1.0"}, {"2025-11-25", "1.123456"}}},
  // ... 6 more
};
data_mgr_.stock_factor_state_.status = Ready;
```

**After:**
```cpp
data_mgr_.stock_factor_data_ = {};  // 空
data_mgr_.stock_factor_state_.status = Error;
data_mgr_.stock_factor_state_.error_msg = "File removed";
```

**文件系统:**
```bash
config/
├── stock_factor.json.backup          # 新增备份
├── stock_info.json
└── stock_days.json
# stock_factor.json 已删除
```

---

## 流程7: [Pause All] / [Stop All] 按钮（Crawler Monitor）

### 触发条件
- 用户点击右侧Crawler Monitor面板上的"Pause All"或"Stop All"按钮

### 伪代码流程

```cpp
void TaskDatabase::DrawRightPanel() {
  ImGui::BeginChild("CrawlerMonitor");
  ImGui::Text("Baostock Crawler");
  ImGui::Separator();
  
  auto& stats = baostock_svc_.stats_;
  ImGui::Text("Status: %s", GetCrawlerStatus());
  ImGui::Text("Workers: %d/4 active", stats.active_workers);
  ImGui::Text("Throughput: %.1f req/s", stats.throughput);
  ImGui::Text("Success: %.1f%%  Errors: %d", 
              GetSuccessRate(), stats.failed_tasks);
  
  ImGui::Text("Progress: %d/%d", stats.completed_tasks, stats.total_tasks);
  ImGui::Text("Elapsed: %ds  ETA: %ds", GetElapsed(), GetETA());
  
  ImGui::Separator();
  
  if (baostock_svc_.paused_) {
    if (ImGui::Button("Resume All")) {
      baostock_svc_.Resume();
    }
  } else {
    if (ImGui::Button("Pause All")) {
      baostock_svc_.Pause();
    }
  }
  
  ImGui::SameLine();
  if (ImGui::Button("Stop All")) {
    baostock_svc_.Stop();
  }
  
  ImGui::EndChild();
}
```

### Pause All

```cpp
void BaostockService::Pause() {
  paused_ = true;
  
  // 连接池中的worker会检查 paused_ 标志
  // 当前任务完成后，不再从队列取新任务
}

// Worker协程内部逻辑
asio::awaitable<void> BaostockService::Worker() {
  while (!stopping_) {
    if (paused_) {
      co_await asio::steady_timer(io_ctx_, 100ms).async_wait();
      continue;
    }
    
    auto task = task_queue_.pop();
    if (!task) {
      break;
    }
    
    auto result = co_await ExecuteTask(task);
    // ...
  }
}

void BaostockService::Resume() {
  paused_ = false;
  // Worker协程会自动恢复工作
}
```

### Stop All

```cpp
void BaostockService::Stop() {
  stopping_ = true;
  paused_ = false;
  
  // 清空任务队列
  std::queue<Task> empty;
  std::swap(task_queue_, empty);
  
  // 等待所有worker完成当前任务
  // (实际实现可能需要使用 std::condition_variable 或协程同步)
}
```

### 内存数据结构变化

**Pause前:**
```cpp
baostock_svc_.paused_ = false;
baostock_svc_.stopping_ = false;
baostock_svc_.stats_ = {
  .active_workers = 4,
  .total_tasks = 16,
  .completed_tasks = 8,
  .failed_tasks = 0,
  .throughput = 2.5
};
```

**Pause后:**
```cpp
baostock_svc_.paused_ = true;
baostock_svc_.stats_ = {
  .active_workers = 0,       // worker暂停
  .total_tasks = 16,
  .completed_tasks = 8,      // 保持不变
  .failed_tasks = 0,
  .throughput = 0.0          // 吞吐量归零
};
```

**Stop后:**
```cpp
baostock_svc_.stopping_ = true;
baostock_svc_.paused_ = false;
baostock_svc_.task_queue_.size() = 0;  // 队列清空
baostock_svc_.stats_ = {
  .active_workers = 0,
  .total_tasks = 16,
  .completed_tasks = 8,      // 部分完成
  .failed_tasks = 0,
  .throughput = 0.0
};
data_mgr_updating_ = false;  // 更新流程终止
```

---

## 流程8: 状态轮询与UI更新

### 渲染循环

```cpp
void TaskDatabase::Draw() {
  // 每帧调用（60 FPS）
  
  if (ImGui::BeginTabBar("DatabaseTabs")) {
    if (ImGui::BeginTabItem("Overview")) {
      DrawOverviewTab();
      ImGui::EndTabItem();
    }
    
    // Table和Browser标签需要JSON数据就绪
    ImGui::BeginDisabled(!all_json_ready());
    if (ImGui::BeginTabItem("Table")) {
      DrawTableTab();
      ImGui::EndTabItem();
    }
    if (ImGui::BeginTabItem("Browser")) {
      DrawBrowserTab();
      ImGui::EndTabItem();
    }
    ImGui::EndDisabled();
    
    ImGui::EndTabBar();
  }
}

bool TaskDatabase::all_json_ready() const {
  return data_mgr_.stock_factor_state_.status == Ready &&
         data_mgr_.stock_info_state_.status == Ready &&
         data_mgr_.stock_days_state_.status == Ready;
}

const char* TaskDatabase::GetStatus() const {
  if (data_mgr_updating_) return "updating";
  if (scanner_running_) return "scanning";
  if (!all_json_ready()) return "incomplete";
  return "ready";
}
```

### 实时统计更新

```cpp
void BaostockService::UpdateStats() {
  // 由worker线程更新
  stats_.completed_tasks++;
  
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
    now - stats_.start_time
  ).count();
  
  if (elapsed > 0) {
    stats_.throughput = (double)stats_.completed_tasks / elapsed;
  }
  
  stats_.active_workers = CountActiveWorkers();
}

// UI线程读取
void TaskDatabase::DrawRightPanel() {
  // 无锁读取atomic变量
  int completed = baostock_svc_.stats_.completed_tasks;
  int total = baostock_svc_.stats_.total_tasks;
  double throughput = baostock_svc_.stats_.throughput;
  
  ImGui::Text("Progress: %d/%d", completed, total);
  ImGui::ProgressBar((float)completed / total);
  ImGui::Text("Throughput: %.1f req/s", throughput);
}
```

---

## 完整状态机图

```
[App启动]
    ↓
加载 data_manager_config.json
    ↓
初始化 TaskDatabase
    ↓
  stock_list_ = [8 default stocks]
  data_mgr_updating_ = false
  scanner_running_ = false
  auto_update_triggered_ = false
    ↓
用户点击 "Overview" 标签
    ↓
prev_overview_active_: false → true
    ↓
触发 OnOverviewTabOpened()
    ↓
  ┌────────────────┴────────────────┐
  ↓                                 ↓
UpdateAll(false)            ScanDatabase()
  ↓                                 ↓
data_mgr_updating_ = true    scanner_running_ = true
  ↓                                 ↓
检查stock_days → 已最新，跳过      扫描500个资产目录
  ↓                                 ↓
检查stock_factor → 周一，需更新    进度: 0 → 250 → 500
  ↓                                 ↓
提交8个任务到连接池                scanner_.current_result_ 更新
  ↓                                 ↓
连接池: 4 workers并发执行          scanner_running_ = false
  ↓
worker1: sh.600000 → 成功 (0.5s)
worker2: sh.600519 → 成功 (0.6s)
worker3: sz.000001 → 成功 (0.4s)
worker4: sz.000002 → 成功 (0.7s)
  ↓
继续: worker1: sh.688001 → 成功
      worker2: sh.688009 → 成功
      worker3: sz.300001 → 成功
      worker4: sz.300750 → 成功
  ↓
合并结果 → stock_factor_data_
  ↓
SaveJSON("stock_factor.json")
  ↓
检查stock_info → 每日字段过期，需更新
  ↓
提交8个任务（跳过退市股票）
  ↓
执行 → 合并 → SaveJSON("stock_info.json")
  ↓
CheckAllIntegrity()
  ↓
data_mgr_.stock_factor_state_.status = Ready
data_mgr_.stock_info_state_.status = Ready
data_mgr_.stock_days_state_.status = Ready
  ↓
data_mgr_updating_ = false
  ↓
all_json_ready() = true
  ↓
Table和Browser标签可用
```

---

## 关键设计要点

### 1. 防重入保护

```cpp
// 通过标志位防止重复触发
if (!auto_update_triggered_) {
  OnOverviewTabOpened();
  auto_update_triggered_ = true;
}

// 在下次切换到其他标签时重置
if (!overview_active && prev_overview_active_) {
  auto_update_triggered_ = false;
}
```

### 2. 并行执行

```cpp
// UpdateAll和ScanDatabase独立运行
coro_mgr_->spawn(UpdateAll(false));
coro_mgr_->spawn(ScanDatabase());

// 连接池内部也是并发
std::vector<asio::awaitable<Result>> workers;
for (int i = 0; i < 4; i++) {
  workers.push_back(Worker());
}
co_await asio::experimental::wait_for_all(workers);
```

### 3. 智能更新策略

```cpp
// 仅在需要时更新
if (force || is_monday || missing_data || stale_data) {
  DoUpdate();
} else {
  Skip();  // 0 API调用
}
```

### 4. 数据完整性优先

```cpp
// 即使错过周一更新，也能在下次打开时自动修复
if (!has_data || data_incomplete) {
  UpdateImmediately();  // 无视schedule
}
```

### 5. 错误容忍

```cpp
// 单个股票失败不影响其他股票
for (auto task : tasks) {
  try {
    auto result = co_await ExecuteTask(task);
    results[task.code] = result;
  } catch (const std::exception& e) {
    LogWarning("Failed to update {}: {}", task.code, e.what());
    // 继续处理下一个
  }
}
```

---

## 总结

### 核心数据流
```
用户交互 → UI按钮 → 协程任务 → 连接池/扫描器 → 内存数据 → JSON文件 → 状态更新 → UI渲染
```

### 关键内存结构
- `TaskDatabase`: 主控制器，持有所有子系统
- `DataManager`: JSON文件状态 + 内存缓存
- `CoroScanner`: L2数据库扫描结果
- `BaostockService`: 爬虫连接池 + 实时统计

### 按钮映射表

| 按钮 | 触发函数 | 主要操作 | 更新的内存结构 |
|------|---------|---------|---------------|
| Update All | `OnUpdateAllClicked()` | `UpdateAll(true)` | `data_mgr_.*_data_` + `*_state_` |
| Scan Assets | `OnScanAssetsClicked()` | `ScanDatabase()` | `scanner_.current_result_` |
| Check Integrity | `OnCheckIntegrityClicked()` | `CheckAllIntegrity()` | `data_mgr_.*_state_.error_msg` |
| Force Update (单个) | `OnForceUpdate*()` | `Update*(true)` | 对应的 `*_data_` |
| Force Remove (单个) | `OnForceRemove*()` | `Remove() + Backup()` | 对应的 `*_data_` + 文件系统 |
| Pause All | `baostock_svc_.Pause()` | `paused_ = true` | `baostock_svc_.paused_` |
| Stop All | `baostock_svc_.Stop()` | `stopping_ = true` | `task_queue_` + `stopping_` |

### 自动触发逻辑
- 切换到Overview标签 → `UpdateAll(false)` + `ScanDatabase()`（并行）
- JSON保存后 → `CheckIntegrity()`（自动）
- 打开空配置 → 初始化8个默认股票（自动）

