# Merge strategy for data sync
_Exported on 8/31/2026 at 17:01:23 GMT+8 from Cursor (3.18.9)_

---

**User**

主项目@README 
附属项目: @qmt @qmt/README.md 

附属项目的长处在日频基本面数据的sync和处理

如果我想把这一部分merge到主项目, 替换掉主项目的baostock
有什么干净的方案嘛?

---

**Cursor**

我先读一下两个 README,了解主项目和 qmt 附属项目的结构。

我先查一下主项目里 baostock 的使用范围,再评估方案。

qmt 是嵌在 stk 里的独立 git 仓库。现在我看一下主项目里 baostock 到底承担了什么职责,涉及面有多大。

我把两边的结构都看完了,先说关键发现,再给方案。

## 关键发现:主项目对 baostock 的消费面其实很窄

主项目里 Baostock 相关代码约 1500+ 行(`BaostockClient/Pool/Service/DataManager` + GUI 状态展示),但**下游真正消费的只有 `shared/AssetInfo.hpp` 里三个容器**:

- `stock_days_`(交易日历)→ `is_trading_day`
- `stock_info_`(name/ipoDate/outDate/行业 + 日频 turn/amount/isST/peTTM/pbMRQ/psTTM/pcfNcfTTM)→ `TabDist` 显示、`ClassifierService` 判 ST、`Asset::compute_browser_statistics`
- `stock_factor_`(复权因子)

这些在 qmt 侧全有对应:`all_trading_days`、`cn_stock_basic_info` + `cn_stock_industry_component`、`cn_stock_real_bar1d` / `cn_stock_status`、财务表自算估值。所以替换的本质是:**换掉 `AssetInfo` 的填充来源,抓取层整体删除**,GUI 消费端几乎不用动。

## 三个方案,按干净程度排

**方案 A(推荐)— 单一 writer,主项目只读 parquet**

- qmt 保持独立,继续负责 sync(它的 `plan_months` 水位/关月冻结/原子写已经把增量完整性解决得很好,不值得搬家重做)。
- 主项目引入 qmt 的 `misc/parquet.hpp`(`read_table` + `TableView`,只需 arrow 的 **parquet 读**部分,不需要 Flight/gRPC),写一个 `AssetInfoBuilder`:扫 `qmt/data/YYYY-MM/*.parquet` + `_meta` → 填 `AssetInfo` 三容器。
- 删除 `Baostock*` 全家 + `DataManager` 抓取路径;`TaskDatabase` 的 Overview 改为显示 parquet 数据新鲜度(直接复用 qmt 的 mtime/水位判定逻辑)。
- 边界清晰:qmt 写、stk 读,文件即接口,tmp+rename 原子写天然安全。

**方案 B — qmt 加导出器,主项目零改动**

qmt 侧加一个小导出步骤,直接产出主项目现有三个 JSON 的格式。stk 只删抓取层,`load_from_json` 原样保留。改动最小,但长期迁就旧 JSON 格式(全 string、无 PIT 语义),属于过渡方案。

**方案 C — 全量合并 api 子系统**

把 `api/bigquant` + `api/tushare` + `misc/schedule` 搬进 stk,主项目自己 sync。代价最大:vendored arrow(含 Flight)进主项目 CMake、两边 spec 表双份维护,且 qmt 自己还要用同一套 —— 除非打算把 qmt 整个并成 submodule/subtree,否则不建议。

## 两个决定性取舍,想先确认

1. 主项目能接受"**sync 依赖 qmt 进程跑过**"吗?(方案 A/B 都是 qmt 管抓取;若 stk 必须自给自足,只能 C)
2. 能接受 **arrow parquet 库进主项目构建**吗?(A 需要;B 完全不需要)

如果你对这两点都倾向宽松,我建议 A:数据所有权单点、抓取逻辑不重复、主项目删掉一整层旧代码。确认后我可以细化 A 的落地步骤。

---

**User**

qmt项目最终是要删掉的, 需要逐渐merge到主项目, 现在是把基本面数据的sync和处理先merge进去, 完全替换baostock, 同时特征表计算也要merge, 把附属项目的特征直接按照主项目的标准做法, 实现到L1 分钟级特征表里, 而不是再独立开发一套

特征merge可以先靠后, 先把基本面数据sync功能完整merge, replace到主项目

---

**Cursor**

这个方向明确了——qmt 最终整体并入 stk,现在先把基本面数据 sync 完整搬进主项目替换 baostock。这是一次大改动(vendored arrow 进主项目构建、api 子系统搬迁、AssetInfo 数据源替换、Baostock 层删除),涉及不少落地决策,我先切到规划模式,把两边的构建系统和接缝摸清楚后给你一份可确认的方案。

明白,直接开工。我先把两边的构建系统、依赖和接缝完整摸一遍,再定合并方案。

现在我需要摸清几处关键接缝。我并行派几个探索任务,同时自己看配置文件。

两个探索任务还在跑(一个梳理主项目 Baostock 的完整集成面,一个梳理 qmt sync 子系统的依赖边界)。目前已确认的关键事实:

- **api 子系统边界很干净**:`api/bigquant` + `api/tushare` 只依赖 `misc/{parquet,schedule,date,fs}` + `config_main.hpp`,不碰 feature 层。
- **misc 同名文件无实质冲突**:qmt 的 `affinity/logging/timer` 是 stk 版的 Linux-only 简化,合并时保留 stk 版,只需搬入 qmt 独有的 `date/fs/parquet/schedule`。
- **依赖成本**:vendored arrow 约 95 MB 共享库(flight + parquet + arrow)+ 5.5 MB 头文件,需进 stk 的 CMake。

等探索结果回来后,我会给出完整的合并方案(目录布局、CMake 接入、AssetInfo 替换路径、Baostock 删除清单)再动手。

---

**User**

<timestamp>Monday, Aug 31, 2026, 11:27 AM (UTC+8)</timestamp>
<system_notification>
The following task has finished. If you were already aware, ignore this notification and do not restate prior responses.

<task>
kind: subagent
status: success
task_id: 6073e80e-8bb0-487a-a0db-6e4820660550
title: 摸清 stk Baostock 集成面
tool_call_id: toolu_01Qzcwgyd5jpD5kZQej61FgE
agent_id: 6073e80e-8bb0-487a-a0db-6e4820660550
detail: <user_visible_high_level_summary>
主项目 Baostock 集成：BaostockService 由 TaskDatabase 创建并由 StateManager 初始化；三个 JSON 落在 config_dir（相对路径 ../../../../config → 仓库 /config）；stock_factor 除 GUI（Browser 分红检测）外未被 features/encoding 消费。
</user_visible_high_level_summary>
<response>
## 1. BaostockService 创建 / 调用（TaskDatabase + TabOverview）

**创建与初始化**
- `TaskDatabase.cpp` 首次 `DrawPanel` 时 `InitializeServices`：构造 `BaostockService(io, data, &terminal)`，再构造 `StateManager(data, baostock_svc, scan_svc)`，然后 `co_spawn` → `state_mgr_->initialize()`。
 - 创建：`186:190:/home/chuyin/work/stk/cpp/src/gui/task_database/TaskDatabase.cpp`
 - 初始化协程：`202:211:/home/chuyin/work/stk/cpp/src/gui/task_database/TaskDatabase.cpp`
- `BaostockService` 构造时创建 `DataManager` 并挂 progress/crawler 回调：`14:55:/home/chuyin/work/stk/cpp/src/gui/task_database/services/BaostockService.cpp`
- 真正“登录+加载 JSON”走 `StateManager::initialize` → `baostock_svc_->load_all_json()` → `DataManager::initialize()`（lazy login，不立刻连网）：`11:41:/home/chuyin/work/stk/cpp/src/gui/task_database/services/StateManager.cpp`，`63:77:/home/chuyin/work/stk/cpp/src/gui/task_database/services/BaostockService.cpp`

**Update 触发时机**
1. **自动（每会话一次）**：首次打开 Overview 且可访问时 `TriggerRefreshFlow()` → 设 L2 日期/股票代码 → `baostock_svc_->update_all(l2_start_date)`：`322:327`、`103:179:/home/chuyin/work/stk/cpp/src/gui/task_database/TaskDatabase.cpp`
2. **手动**：Overview 按钮 → `update_all` / `update_stock_factor` / `update_stock_info` / `update_stock_days` / `force_remove_*` / `check_all_integrity`：`431:543:/home/chuyin/work/stk/cpp/src/gui/task_database/TaskDatabase.cpp`
3. TabOverview 本身只渲染+回写按钮 flag，不直接持有 Service：`544:616:/home/chuyin/work/stk/cpp/src/gui/task_database/ui/TabOverview.cpp`

**“自动更新调度”**
- **没有后台定时器/cron**。`baostock_weekly_update_day` 只在调用 `update_stock_info` 时作为是否跑 weekly 的判定（`should_run_weekly_update`）：`133:148:/home/chuyin/work/stk/cpp/src/gui/task_database/infrastructure/DataManager.cpp`，`208:228:/home/chuyin/work/stk/cpp/src/gui/task_database/services/BaostockService.cpp`

**UI 显示状态**（`RenderCompactJsonCard` + `RenderCrawlerMonitor`）
- 每文件：Integrity ✓/✗、状态 Idle/Loading/Ready/Outdated/Error/Updating、stocks/records/days、last_update、date range、更新进度/stage、Trigger Update / Force Remove / View JSON：`24:266:/home/chuyin/work/stk/cpp/src/gui/task_database/ui/TabOverview.cpp`
- 右侧 Crawler：Baostock 数据源、session idle/logging in/active/logging out、worker/query 进度：`272:317+`

---

## 2. DataManager 职责与 BaostockClient/Pool 依赖

**职责概要**（`DataManager.cpp`）
| 流程 | 逻辑 | API |
|------|------|-----|
| **stock_days** | 空/完整性失败 → 从 L2 start 全量重建；否则从最后一天+1 增量到今天；写 `stock_days.json` | `client->query_trade_dates`（单 client）`492:601` |
| **stock_factor** | 先更新 days；按股判断缺数据或 `last_update != today`；池化增量拉复权因子；存 `[date, adjust_factor]`；写 JSON | `client.query_adjust_factor` + `pool_->submit_task` / `get_client` `603:787` |
| **stock_info weekly** | 完整性失败或缺 name/ipoDate/到了 weekly day → `query_stock_industry` + 每股 `query_stock_basic` | `789+`、`854`、`911` |
| **stock_info daily** | 完整性 OK 且 `update_date < last_trading_day` → 每股 `query_history_k_data_plus` 填日行情字段 | `1009+`、`1108` |
| **update_all** | 预检 needs_* → 统一 login → days → factor → info weekly/daily → logout | `1212:1333` |

**依赖的接口**
- **BaostockPool**：构造、`initialize`/`clients_`、`submit_task`、`get_next_task`、`has_pending_tasks`、`get_client`、`set_progress_callback`（Pool 头文件 `47:204:/home/chuyin/work/stk/cpp/include/gui/task_database/infrastructure/BaostockPool.hpp`）
- **BaostockClient**：`login` / `logout`；`query_trade_dates`；`query_adjust_factor`；`query_stock_basic`；`query_stock_industry`；`query_history_k_data_plus`（`375:480:/home/chuyin/work/stk/cpp/include/gui/task_database/infrastructure/BaostockClient.hpp`）

数据存于 `SharedData::assetinfo`，经 `load_*_from_json` / `save_*_to_json` 持久化。

---

## 3. `stock_factor` 真实消费点（除 GUI Overview 卡片）

| 位置 | 用途 |
|------|------|
| `AssetInfo` load/save | 读写内存/JSON：`168:191`、`353+` `/home/chuyin/work/stk/cpp/src/shared/AssetInfo.cpp` |
| DataManager / BaostockService | 抓取、完整性、uptodate、状态 |
| **TabBrowser** | `BuildDailyStats` 用相邻因子比值检测分红/拆分，累加 `dividend_split_count`：`121:148:/home/chuyin/work/stk/cpp/src/gui/task_database/ui/TabBrowser.cpp`；由 `TaskDatabase::DrawTabBrowser` 传入 `get_stock_factor()`：`574:580` |
| StateManager | 仅同步 `stock_factor_status` 做 Tab 解锁，不读因子数值 |

**未被消费**：`cpp/src/worker/`、`features/`、encoding 路径中 **无** `stock_factor` / `get_stock_factor` / `StockFactor` 引用。`py/` 主项目侧也无。结论：**复权因子目前只服务 Browser 日历事件标注 + 数据层维护，不参与特征计算或 encoding。**

---

## 4. 配置项（Config.hpp + config.json）

`Config.hpp` `35:42`、`61:65`；`config/config.json` `7:14`：

| 键 | 默认/当前值 | 含义 |
|----|-------------|------|
| `config_dir` | `"../../../../config"` | JSON 根目录（相对进程 CWD） |
| `baostock_data_manager_file` | `data_manager.json` | 股票列表/L2 日期/weekly 元数据 |
| `baostock_stock_factor_file` | `stock_factor.json` | |
| `baostock_stock_info_file` | `stock_info.json` | |
| `baostock_stock_days_file` | `stock_days.json` | |
| `baostock_max_workers` | `10` | 连接池大小 |
| `baostock_weekly_update_day` | `1`（周一） | stock_info weekly 触发日 |

`data_manager.json` 另有 `settings.weekly_update_day`、`l2_database.date_range_*`、`metadata.stock_info_last_update`（运行时由 DataManager 读写）。

---

## 5. StateManager ↔ BaostockService

- StateManager **持有** `BaostockService*`（不拥有生命周期；由 TaskDatabase 拥有）：`54:63:/home/chuyin/work/stk/cpp/include/gui/task_database/services/StateManager.hpp`
- `initialize`：从 assets 推代码 → `get_data_manager()->set_stock_codes` → `load_all_json` → `refresh_state`
- `refresh_state`：调用 `baostock_svc_->refresh_state()`，拷贝三个 JSON status，结合 ScanService 结果算 Tab 解锁（Overview / Table / Browser）：`48:74:/home/chuyin/work/stk/cpp/src/gui/task_database/services/StateManager.cpp`
- 角色：**门禁协调器**（资产加载 + 扫描 + Baostock 就绪 → UI 门控），不是抓取实现本身。

---

## 6. 三个 JSON 实际目录（config_dir）

- 代码一律：`config_dir + "/" + baostock_stock_*_file`（如 `AssetInfo.cpp:75,103,144`，`DataManager.cpp:440+`）
- 配置值：`config_dir = "../../../../config"`（`Config.hpp:19`，`config.json:14`）
- 相对路径相对**进程 CWD**；仓库里与 `config.json` / `data_manager.json` 同级的目标目录是 **`/home/chuyin/work/stk/config/`**
- 完整路径应为：
 - `/home/chuyin/work/stk/config/stock_factor.json`
 - `/home/chuyin/work/stk/config/stock_info.json`
 - `/home/chuyin/work/stk/config/stock_days.json`
- 这三个文件在 `config/.gitignore` 中被忽略；当前磁盘上尚不存在（首次 update/save 后才会生成）。
</response>
output_path: /home/chuyin/.cursor/projects/home-chuyin-work-stk/agent-transcripts/5da5f509-bda1-4f97-918c-3b5e166ae9c5/subagents/6073e80e-8bb0-487a-a0db-6e4820660550.jsonl
</task>
</system_notification>
<user_query>Perform any necessary follow-up actions in response to the subagent completion above. If no follow-up work is needed, no further action is required. If you mention an agent or subagent in your response, link it with the `[Name](id)` Don't use generic label such as `[agent]`, `[worker]`, or `[subagent]`. For cloud subagents, when the agent has edited code, link to `[Review](bc-id#changes)`, or, if you know the exact added and deleted line counts, `[Review +A −D](bc-id#changes)`, replacing A and D with those counts. Never write A or D literally. Use `[Try Live](bc-id#desktop)` only when the agent used computer use. Don't repeat the same confirmation every time.</user_query>

---

**Cursor**

stk 侧的集成面已经摸清:三个 JSON 落在 `config/` 目录,`stock_factor` 除 GUI 的 Browser 分红标注外没有被特征计算消费,GUI 更新触发全在 `TaskDatabase` + `StateManager`,没有后台定时调度。还差 qmt 侧 sync 子系统边界的探索结果,回来后我给出完整合并方案。

---

**User**

<timestamp>Monday, Aug 31, 2026, 11:27 AM (UTC+8)</timestamp>
<system_notification>
The following task has finished. If you were already aware, ignore this notification and do not restate prior responses.

<task>
kind: subagent
status: success
task_id: 3bacf896-d303-4cbb-a507-922f8aef613d
title: 摸清 qmt 数据 sync 子系统边界
tool_call_id: toolu_0148ic2ZRJCg9uxhYWmZBVYW
agent_id: 3bacf896-d303-4cbb-a507-922f8aef613d
detail: <user_visible_high_level_summary>
基本面 sync 边界已摸清：pipeline 只暴露 update/pending（tushare 另有 probe）；preflight 在 main.cpp；api 只依赖 misc 的 date/parquet/schedule（经 parquet 间接用 fs），不碰 feature；boost 为项目内 header-only asio/beast；http/dai 无全局单例。
</user_visible_high_level_summary>
<response>
## 1. Pipeline 入口与 main 调用

**bigquant** (`cpp/include/api/bigquant/pipeline.hpp:18-25`)
```cpp
void update(std::string_view start, std::string_view end,
 const std::vector<TableSpec> &specs, int lookback_days);
bool pending(std::string_view start, std::string_view end,
 const std::vector<TableSpec> &specs, int lookback_days);
```
无 `preflight` / `probe`。

**tushare** (`cpp/include/api/tushare/pipeline.hpp:13-23`)
```cpp
void update(std::string_view start, std::string_view end,
 const std::vector<InterfaceSpec> &specs, int lookback_days);
bool pending(...); // 同上签名
void probe(); // 积分探针
```

**main 编排** (`cpp/src/main.cpp:211-268`)：
- `preflight()` 是 **main 匿名命名空间本地函数**（非 pipeline 导出）：调 `tushare::probe()` + `bigquant::DaiClient{}.quota()`，再交互确认。
- 门控：`config::PIPELINE_UPDATE`
- `pending(START, today, SPECS, LOOKBACK)` → 若需更新 → `preflight()` → `bigquant::update` → `tushare::update`（参数同上）。

---

## 2. api 对 misc / config / 其他子系统

**直接 `#include` 的 misc：**
| 文件 | 依赖 |
|------|------|
| `src/api/bigquant/pipeline.cpp` | `misc/parquet.hpp`, `misc/schedule.hpp` |
| `src/api/tushare/pipeline.cpp` | `misc/date.hpp`, `misc/parquet.hpp`, `misc/schedule.hpp` |

**间接**：`parquet.cpp` → `misc/fs.hpp`（`git_root` 等）。api **不用** `logging.hpp` / `npy` / `mmap` / `timer` / `affinity`。

**config_main 常量（api 层实际引用）：**
- 凭据：`BIGQUANT_AK/SK`，`TUSHARE_TOKEN`
- 端点：`BIGQUANT_FLIGHT_URI`，`BIGQUANT_FLIGHT_GRPC_MAX_METADATA_SIZE`，`TUSHARE_HTTP_{HOST,PORT,TIMEOUT_SECONDS,RETRY_MAX,RETRY_INTERVAL_SECONDS}`
- 流水线：`PIPELINE_DEDUP_WINDOW_SECONDS` 
（`PIPELINE_UPDATE/START_DATE/LOOKBACK_DAYS` 只在 **main** 编排层用）

**feature/ 及其他**：api 树内 **零** `#include` feature/analysis/backtest/strategy/mine/report。确认不依赖。

另依赖：`package/yyjson`、vendored Arrow（dai/parse/parquet）。

---

## 3. misc 五件套

| 头文件 | namespace | 接口概要 |
|--------|----------|----------|
| `date.hpp` | `misc` | `today_yyyymmdd` / `parse_*` / `fmt_*` / `iter_days` / `add_days` / `month_last_dd` |
| `fs.hpp` | `misc` | `git_root`（static 缓存）/ `read_file_all` / `atomic_write` / `atomic_write_json` |
| `parquet.hpp` | `misc::pq` | `month_path`/`meta_path`/`list_month_files`/`read|write|append_table_atomic` + `Col`/`TableView` |
| `schedule.hpp` | `misc` | `FetchMonth` / `plan_months` / `meta_fresh` |
| `logging.hpp` | `Logger`（非 misc） | `init`/`close`/`reg`/`log`/`is_initialized` |

**相互依赖：**
- `date`：独立 
- `fs`：只依赖 yyjson 
- `parquet` → `fs`（实现） 
- `schedule` → `date` + `parquet` 
- `logging`：独立；**api 不用**

---

## 4. Arrow vendor

`cpp/include/package/arrow/vendor_from_pyarrow.sh`：查 PyPI 最新 pyarrow manylinux wheel → 解压 → 拷 `include/{arrow,parquet}` + `libarrow.so.*` / `libarrow_flight.so.*` / `libparquet.so.*`，并建无版本 symlink。

**lib/**（约 90.6 MiB 实体）：
- `libarrow.so.2400` ≈ **52.7 MiB**（+ `libarrow.so`）
- `libarrow_flight.so.2400` ≈ **25.6 MiB**
- `libparquet.so.2400` ≈ **12.2 MiB**

**include/**：合计约 **5.5M**（arrow ~4.8M，parquet ~660K）。另有缓存 wheel `pyarrow-24.0.0-...whl` ~47M。

---

## 5. `config_main.hpp` 概要

路径：`cpp/include/config_main.hpp`，`namespace config`。

| 段 | 常量 |
|----|------|
| A 凭据（**明文**） | `BIGQUANT_AK`=`"6dS0GYgxocXL"`；`BIGQUANT_SK`=64 字符明文；`TUSHARE_TOKEN`=56 字符明文 |
| B 端点 | Flight URI/`GRPC_MAX_METADATA_SIZE`；Tushare host/port/timeout/retry |
| C 流水线 | `PIPELINE_UPDATE=true`；`START_DATE="20150101"`；`LOOKBACK_DAYS=7`；`DEDUP_WINDOW_SECONDS=3600` |
| F 回测 | `CAPITAL_BASE`/`PRICE_LIMIT_EPS`/`BUY|SELL|MIN_COST` |
| G 分析 | `N_QUANTILES`/`IC_MA_WINDOW`/`DIST_N_GRID` |
| H/H2/J | `DESCRIBE_*`；`TENSOR_DUMP_ENABLE`；`FEATURE_TABLE_ENABLE` |

注释提到可挪到 gitignore 的 `secrets.hpp`，当前仍内嵌明文。

---

## 6. Boost

**是**，仅 tushare HTTP：`cpp/src/api/tushare/http.cpp:4-20` 用 **boost::asio** + **boost::beast**（`tcp_stream` POST 明文 80）+ `boost::system::system_error`。bigquant 不碰 boost（走 Arrow Flight）。

来源：**项目内 vendored header-only**，`cpp/include/boost/`（`BOOST_VERSION 109000` = 1.90），经 `CMakeLists.txt:97` `include_directories(../../include)` 解析；README 写明 “header-only boost”。非系统包。

---

## 7. 全局状态 / 单例

**无。**

- `tushare::Http`：实例成员 `token_`；每次 `call` 新建本地 `io_context`/`tcp_stream`（`http.hpp:11-25`，`http.cpp:22-63`）。
- `bigquant::DaiClient`：实例成员 `ak_/sk_/flight_/flight_auth_`；Flight 在 `ensure_flight()` **按实例** lazy（`dai.hpp:43-69`，`dai.cpp:70-100`）。无进程级单例。

（旁注：`misc::git_root()` 有 function-local static，属 fs 层，非 http/dai。）
</response>
output_path: /home/chuyin/.cursor/projects/home-chuyin-work-stk/agent-transcripts/5da5f509-bda1-4f97-918c-3b5e166ae9c5/subagents/3bacf896-d303-4cbb-a507-922f8aef613d.jsonl
</task>
</system_notification>
<user_query>Perform any necessary follow-up actions in response to the subagent completion above. If no follow-up work is needed, no further action is required. If you mention an agent or subagent in your response, link it with the `[Name](id)` Don't use generic label such as `[agent]`, `[worker]`, or `[subagent]`. For cloud subagents, when the agent has edited code, link to `[Review](bc-id#changes)`, or, if you know the exact added and deleted line counts, `[Review +A −D](bc-id#changes)`, replacing A and D with those counts. Never write A or D literally. Use `[Try Live](bc-id#desktop)` only when the agent used computer use. Don't repeat the same confirmation every time.</user_query>

---

**Cursor**

qmt 侧边界也清楚了,比预期还干净:api 子系统只依赖 4 个 misc 文件 + yyjson + arrow,tushare 用 boost beast(stk 自己也带了 boost)。我再核实最后几个事实:BigQuant 有没有现成的个股估值表(替代 baostock 的 peTTM/pbMRQ/psTTM/pcfNcfTTM)、复权因子在哪张表、`StockDaysVec` 的确切语义、两边 boost 版本,以及 qmt/data 现有数据体积。

**基本面 sync 合并的 4 个决策点**

> **qmt/data 现有 6.2 GB 月度 parquet(2015 至今 142 个月),合并后 stk 的数据目录是 stk/data/。现有数据怎么处理?**
> - 复制到 stk/data(qmt 保持可用,双份占 12.4 GB)(Recommended)
> - 移动到 stk/data(qmt 立即不可跑)
> - stk 从零重新拉取(耗 DAI 配额)
>
> Answer: 复制, 但是和主项目的数据并列放, 放到output/fundamental比较好, 我们盘很大, 不用剩空间

> **baostock 的估值字段(PE/PB/PS/PCF/市值)和复权因子需要新增两张 BigQuant 表:cn_stock_valuation(免费)+ cn_stock_bar1d(只 SELECT 复权因子列,省配额)。同意新增吗?**
> - 两张都加(功能完整对齐 baostock)(Recommended)
> - 只加 cn_stock_valuation,复权因子先置空(Browser 分红标注失效)
> - 都不加,估值和复权因子先置空
>
> Answer: 应该不需要, 我们qmt本身的日频因子应该可以完整cover baostock的, 然后改一下计算用实时价格实现到主项目标准L1特征表里, 就能做的很干净

> **config/ 下三个 JSON(stock_factor/stock_info/stock_days)是否彻底废弃?新方案 AssetInfo 每次启动直接从 parquet 构建内存结构(全市场规模下毫秒~秒级)。**
> - 彻底废弃 JSON,parquet 是唯一落地(Recommended) ✔
> - 保留 JSON 作为 parquet 的缓存层(多一层同步逻辑)

> **BigQuant AK/SK 和 Tushare token 目前在 qmt 是明文 constexpr 头文件。stk 也是 git 仓库,怎么放?**
> - 照搬明文头文件(与 qmt 现状一致,后续想隔离再挪)(Recommended)
> - 这次就做 secrets.hpp + .gitignore 隔离
>
> Answer: A, 不用保护这些, 里面没有余额, 只是权限API, 明文就好

源码和 arrow 都已搬入,6.2 GB 数据在后台复制。现在调整搬入文件:先看 `config_main.hpp` 的引用点和 parquet 层的数据根路径。

`git_root()` 从 CWD 向上找 `.git`,在 stk 里会正确解析到仓库根,无需改。现在创建配置头文件并修正路径。

include 全部落在现有 include 路径内。接下来接入 CMake。

开始写新服务。先快速确认 `TabBrowser` 消费 `stock_factor` 的行格式和 `SharedData` 对 baostock 的引用。

格式确认完毕。现在写 `FundamentalService`。

现在重写 `TabOverview` 为单一数据源状态卡。

改造 `StateManager`。

改造 `TaskDatabase.cpp`。

删除 Baostock 全家,并把 `SharedTypes.hpp` 裁剪到只剩 `BoardType`。

现在清理 `AssetInfo` 的 JSON 持久化和 `Config` 的 baostock 配置键。

Now update the `AssetInfo.hpp` header to drop the JSON persistence API.

检查 `stock_days` 消费方是否依赖“非交易日行”(baostock 日历含 0/1 全日历,我构建的只有交易日)。

数据复制完成(同盘 reflink,秒级)。清掉误拷的 qmt pool 缓存,并确认 stk 的 imgui 版本支持我用的两个 API(`SeparatorText`、不定进度条)。

类型访问全覆盖。补上 CMake 的 HEADERS 清单(IDE 索引用),然后做收尾核查。

做最后一遍完整自查。

自查通过,全部改动收口。

合并完成,baostock 已完全替换。净变化 −4600 行 + 新增 sync 子系统。请你构建验证(我按规则不编译)。

**搬入(qmt → stk,近乎原样)**
- `cpp/{include,src}/api/`(bigquant + tushare 全套 27 表 SPECS)、`misc/{date,fs,parquet,schedule}`、`package/yyjson`、`package/arrow`(vendored headers + 3 个 .so)
- 仅两处适配:`config_main.hpp` A/B/C 段 → `cpp/include/api/config.hpp`(4 个 include 改行);`parquet.cpp` 数据根 `data/` → `output/fundamental/`
- 6.2 GB 月度 parquet 已复制到 `output/fundamental/`(qmt 原样保留可跑;误拷的 `pool/` 缓存已删)

**新写**
- `FundamentalService`:阻塞的 `pending → bigquant::update → tushare::update → 构建` 全在独立工作线程跑,协程 100ms 轮询进度,`AssetInfo` 替换只发生在 io 线程,GUI 不卡
- AssetInfo 构建映射:交易日历 ← `all_trading_days`(全日历展开,保住 Browser 节假日标注);元数据 ← `basic_info` + `industry_component` 最新快照;日频 volume/amount/turn/isST/tradestatus ← `real_bar1d` + `status` 每股最新行;**复权因子 ← `real_bar1d.adjust_factor` 变点序列**(不需要新表,Browser 分红标注保住);PE/PB/PS/PCF 置空待特征阶段自算;代码格式 `000001.SZ → sz.000001`

**删除**:`BaostockClient/Pool/Service`、`DataManager`、`BaostockData.hpp`、AssetInfo 全部 JSON 持久化、Config 的 6 个 baostock 键、`config/data_manager.json`、TaskSettings 对应设置区、TabOverview 三卡片+爬虫面板(换成单张数据源状态卡)

两个需要留意的点:

1. **`-ffast-math` 冲突**:qmt 代码靠 NaN 标记缺失值,与 stk 生产模式的 `-ffast-math` 不兼容——所有新搬的 `.cpp`(含 `FundamentalService.cpp`)已加进 `PRECISE_MATH_FLAG` 列表,这是本次最容易踩的坑,已处理。
2. **arrow .so 约 91 MB 目前是 untracked**,你 commit 时会进 stk 的 git(与 qmt 现状一致);token 按你的决定明文放在 `api/config.hpp`。

首次启动:打开 Database → Overview,若本地 parquet 就绪会直接构建(状态卡显示 stocks/交易日统计);点 Update 走水位增量同步,已到水位时零网络秒过。

---

**User**

1. 这部分package@cpp/include/package 
挪到@cpp/package , 注意用通用格式, 最大化对仗
2. @cpp/include/api/config.hpp 这部分配置直接merge到主项目配置里, 不要单独的config, 注意去掉重复的(应该和主项目统一的)config

---

**Cursor**

我先看主项目 `Config.hpp` 全貌和 `cpp/package` 现有布局,确定对仗格式和重复项。

消费点都在 api 内部深处(重试循环、ctor 默认参),保持 constexpr 常量、并入 `shared/Config.hpp` 是对的做法。开始执行:先挪 package。

CMake 更新:include 根、arrow 路径、yyjson 源文件路径。

现在把 api 常量并入 `shared/Config.hpp`,删掉独立的 `api/config.hpp`。

完成,两项都已就位:

1. **package 挪位**:`cpp/include/package/{arrow, yyjson, nlohmann, utfcpp, README}` 全部移到 `cpp/package/`,与 zstd/glfw 等并列。CMake 加了 `../../package` include 根,所有引用统一成 `"yyjson/yyjson.h"`、`"nlohmann/json.hpp"`、`"utfcpp/utf8.hpp"` 这种通用格式;`ARROW_VENDOR`、yyjson.c 源路径、vendor 脚本注释同步更新。

2. **config 合并**:`api/config.hpp` 已删除,常量(凭据/端点/流水线参数)并入 `shared/Config.hpp` 底部的 `namespace config` constexpr 块,5 个消费文件的 include 改指向它。保持编译期常量而非进 config.json——消费点在 api 层深处(ctor 默认参、重试循环),运行时化要穿透整个 api 签名。

去重说明:逐项对过,唯一疑似重复是 `PIPELINE_START_DATE`(20150101)与 `Config::start_date`(回测窗口 2025-01-01),语义不同——基本面历史要为后续特征表全周期服务,不随回测窗口收窄,所以保留并在注释里写明了理由。其余(token/端点/超时)主项目无对应项,无可去重。编译验证留给你跑。

