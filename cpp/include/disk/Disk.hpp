#pragma once

#include "misc/date.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

// ============================================================================
// 落盘 — 仓库下 output/ 的唯一布局 + 唯一路径入口
//
// 布局表 DISK_LAYOUT (一行一节点): 拼路径 path / 列举 list / 整理 sweep (disk/Sweep.hpp) 同读这一张表, 盘上只许出现表里的节点.
//   节点名 = 前缀 + 段 + 后缀. 段 Seg = 字面, 或键 Key 的一个字段 (池 / 档 / 层 / 日期 …); 键沿父链累积, 子节点可依赖祖先已定的字段.
//   类 Kind: Dir 目录 (子节点逐个在表里) / File 文件 / Bundle 目录 (内部由写者自理, 不展开)
//
// output/ = config/ 人定的输入 + 按产研流水线分段的产物 + run/ 各段运行产物的同构镜像 (整删无损):
//   config/      配置: 一任务一目录 (settings / beta / market / agent / live, 与左栏任务树对仗); 本机件由 output/.gitignore 逐项列出
//   data/        ① 数据 ← 外部 + 样例       run/data/       编码日志 / 归档修复
//   features/    ② 特征 ← data              run/features/   计算日志 / 节点用时 / perf 采样 (同 <池>/<档>)
//   factors/     ③ 因子 ← features          run/factors/    Search 日志 / Inspect 导出 (同 <池>/<层>) + 算子表
//   portfolios/  ④ 组合 ← factors
//   agent/       ③ 的旁路: 挖候选 → 入库      run/agent/      Search 日志 (同会话作用域) + 工具结果样本 (同 <main|test>)
//   live/        实盘: 当日两融条款          run/live/       调仓事务日志
//                                           run/app/        进程级: TSan 报告 / 崩溃栈
//
// 读写: 小文件 (清单 / 配置 / 因子 / 记录行) 走 read / write / append; write = <正式名>.tmp → rename 原子替换.
//   大块数据 (逐笔日文件 / 特征日文件 / 快照 / parquet / CTA 库 / 日志流 / Bundle 内部) 只从 path 取路径, 读写自理,
//   临时文件同样只许 <正式名>.tmp. 删走 remove: 目录先 rename 成墓碑 <名>.trash.<序> 再删 (多方并发删同一目录安全).
//   .tmp / 墓碑是唯一许可的残留 (崩溃留下), sweep 直接清.
// ============================================================================
namespace disk {

enum class Kind : std::uint8_t { Dir,
                                 File,
                                 Bundle };

enum class Seg : std::uint8_t {
  Lit,       // 字面 (前缀即全名)
  Pool,      // Key::pool   特征池 (feature_pools)
  Tier,      // Key::tier   特征档
  Lvl,       // Key::lvl    特征层 (该档有的层)
  Level,     // Key::level  因子层 (kFactorLevels)
  Hold,      // Key::hold   持有期 (factor::stat::hold_token)
  Mode,      // Key::test   main / test (Agent 正式 / 自检)
  Year,      // Key::date   YYYY
  Month,     // Key::date   MM
  Day,       // Key::date   DD
  Ymd,       // Key::date   YYYYMMDD
  Date,      // Key::date   YYYY-MM-DD
  Ym,        // Key::ym     YYYY-MM (缺省取 Key::date 的年月)
  Stamp,     // Key::stamp  YYYYMMDD-HHMMSS (本地时刻)
  Table,     // Key::name   基本面表 (SPECS, 按所在目录的形态)
  Factor,    // Key::name   因子名 (ValidFactorName)
  Portfolio, // Key::name   组合名 (ValidFactorName)
  Export,    // Key::name   <因子>_w<权重号>_top<N>_{log,snap}
  Tool,      // Key::name   Agent 工具名
  Note,      // Key::name   Agent 全局记录类
  Prompt,    // Key::name   Agent 提示词模板
  Log,       // Key::name   日志名
  List,      // Key::name   实盘调仓名单名
};

// clang-format off
#define DISK_LAYOUT(X)                                                                                   \
  /* 节点               父                 类      段         前缀 / 字面          后缀        基石 */        \
  X(Root,              Root,              Dir,    Lit,       "",                  "",         false)       \
  /* ======== output/ ======== */                                                                        \
  X(Output,            Root,              Dir,    Lit,       "output",            "",         false)       \
  X(OutputIgnore,      Output,            File,   Lit,       ".gitignore",        "",         false)       \
  /* 配置 (一任务一目录) */                                                                                \
  X(Config,            Output,            Dir,    Lit,       "config",            "",         false)       \
  X(ConfigSettings,    Config,            Dir,    Lit,       "settings",          "",         false)       \
  X(SettingsFile,      ConfigSettings,    File,   Lit,       "settings.json",     "",         true)        \
  X(ConfigBeta,        Config,            Dir,    Lit,       "beta",              "",         false)       \
  X(PoolsFile,         ConfigBeta,        File,   Lit,       "pools.json",        "",         false)       \
  X(ConfigMarket,      Config,            Dir,    Lit,       "market",            "",         false)       \
  X(WatchFile,         ConfigMarket,      File,   Lit,       "watch.json",        "",         true)        \
  X(ConfigAgent,       Config,            Dir,    Lit,       "agent",             "",         false)       \
  X(PromptDir,         ConfigAgent,       Dir,    Lit,       "prompt",            "",         false)       \
  X(PromptFile,        PromptDir,         File,   Prompt,    "",                  ".md",      false)       \
  X(FamilyFile,        ConfigAgent,       File,   Lit,       "families.md",       "",         false)       \
  X(AgentKeyFile,      ConfigAgent,       File,   Lit,       "keys.json",         "",         true)        \
  X(AgentUiFile,       ConfigAgent,       File,   Lit,       "ui.json",           "",         true)        \
  X(ConfigLive,        Config,            Dir,    Lit,       "live",              "",         false)       \
  X(LiveLists,         ConfigLive,        Dir,    Lit,       "lists",             "",         true)        \
  X(LiveListFile,      LiveLists,         File,   List,      "",                  ".csv",     false)       \
  /* ① 数据 */                                                                                            \
  X(Data,              Output,            Dir,    Lit,       "data",              "",         false)       \
  X(Sample,            Data,              Bundle, Lit,       "sample",            "",         false)       \
  X(Orders,            Data,              Dir,    Lit,       "orders",            "",         true)        \
  X(OrdersYear,        Orders,            Dir,    Year,      "",                  "",         false)       \
  X(OrdersMonth,       OrdersYear,        Dir,    Month,     "",                  "",         false)       \
  X(OrdersDay,         OrdersMonth,       File,   Ymd,       "",                  ".bin",     false)       \
  X(Fundamental,       Data,              Dir,    Lit,       "fundamental",       "",         true)        \
  X(FundMonth,         Fundamental,       Dir,    Ym,        "",                  "",         false)       \
  X(FundShard,         FundMonth,         File,   Table,     "",                  ".parquet", false)       \
  X(FundDaily,         FundMonth,         Dir,    Table,     "",                  "",         false)       \
  X(FundDay,           FundDaily,         File,   Date,      "",                  ".parquet", false)       \
  X(FundMeta,          Fundamental,       Dir,    Lit,       "_meta",             "",         false)       \
  X(FundMetaShard,     FundMeta,          File,   Table,     "",                  ".parquet", false)       \
  X(FundAudit,         Fundamental,       Dir,    Lit,       "_audit",            "",         false)       \
  X(FundAuditSheet,    FundAudit,         File,   Table,     "",                  ".tsv",     false)       \
  X(AssetAxisFile,     Fundamental,       File,   Lit,       "asset_axis.json",   "",         false)       \
  X(Concept,           Data,              Dir,    Lit,       "concept",           "",         true)        \
  X(ConceptDay,        Concept,           File,   Ymd,       "",                  ".tsv",     false)       \
  X(Quote,             Data,              Dir,    Lit,       "quote",             "",         false)       \
  X(QuoteLive,         Quote,             File,   Lit,       "live.bin",          "",         false)       \
  X(Doc,               Data,              Bundle, Lit,       "doc",               "",         false)       \
  /* ② 特征 */                                                                                            \
  X(Features,          Output,            Dir,    Lit,       "features",          "",         false)       \
  X(LibPool,           Features,          Dir,    Pool,      "",                  "",         false)       \
  X(Lib,               LibPool,           Dir,    Tier,      "",                  "",         false)       \
  X(LibManifest,       Lib,               File,   Lit,       "lib.json",          "",         false)       \
  X(LibSnapshot,       Lib,               File,   Lit,       "snapshot.bin",      "",         false)       \
  X(LibYear,           Lib,               Dir,    Year,      "",                  "",         false)       \
  X(LibMonth,          LibYear,           Dir,    Month,     "",                  "",         false)       \
  X(LibDay,            LibMonth,          Dir,    Day,       "",                  "",         false)       \
  X(LibDayFile,        LibDay,            File,   Lvl,       "features_",         ".bin",     false)       \
  X(LibCache,          Lib,               Dir,    Lit,       "cache",             "",         false)       \
  X(LibPreview,        LibCache,          File,   Lit,       "features.json",     "",         false)       \
  X(LibBaskets,        LibCache,          File,   Lit,       "baskets.bin",       "",         false)       \
  X(LibCta,            LibCache,          Dir,    Lit,       "cta",               "",         false)       \
  X(LibCtaHead,        LibCta,            File,   Lit,       "head.bin",          "",         false)       \
  X(LibCtaStock,       LibCta,            File,   Lit,       "stock.bin",         "",         false)       \
  X(LibCtaBasket,      LibCta,            File,   Lit,       "basket.bin",        "",         false)       \
  /* ③ 因子 */                                                                                            \
  X(Factors,           Output,            Dir,    Lit,       "factors",           "",         false)       \
  X(FactorPool,        Factors,           Dir,    Pool,      "",                  "",         false)       \
  X(FactorLevel,       FactorPool,        Dir,    Level,     "",                  "",         false)       \
  X(FactorFile,        FactorLevel,       File,   Factor,    "",                  ".json",    false)       \
  /* ④ 组合 */                                                                                            \
  X(Portfolios,        Output,            Dir,    Lit,       "portfolios",        "",         false)       \
  X(PortfolioFile,     Portfolios,        File,   Portfolio, "",                  ".json",    false)       \
  /* Agent */                                                                                             \
  X(Agent,             Output,            Dir,    Lit,       "agent",             "",         false)       \
  X(AgentMode,         Agent,             Dir,    Mode,      "",                  "",         false)       \
  X(AgentSessions,     AgentMode,         Dir,    Lit,       "sessions",          "",         false)       \
  X(AgentPool,         AgentSessions,     Dir,    Pool,      "",                  "",         false)       \
  X(AgentLevel,        AgentPool,         Dir,    Level,     "",                  "",         false)       \
  X(AgentScope,        AgentLevel,        Dir,    Hold,      "",                  "",         false)       \
  X(AgentFactors,      AgentScope,        Dir,    Lit,       "factors",           "",         false)       \
  X(AgentFactorFile,   AgentFactors,      File,   Factor,    "",                  ".json",    false)       \
  X(AgentChats,        AgentScope,        Dir,    Lit,       "chat",              "",         false)       \
  X(AgentChat,         AgentChats,        Bundle, Stamp,     "",                  "",         false)       \
  X(AgentNotes,        AgentMode,         Dir,    Lit,       "notes",             "",         false)       \
  X(AgentNoteFile,     AgentNotes,        File,   Note,      "",                  ".jsonl",   false)       \
  /* 实盘 */                                                                                              \
  X(Live,              Output,            Dir,    Lit,       "live",              "",         false)       \
  X(LiveTerms,         Live,              Dir,    Lit,       "terms",             "",         false)       \
  X(LiveTermsDay,      LiveTerms,         File,   Ymd,       "",                  ".csv",     false)       \
  /* 运行产物 (各段的同构镜像) */                                                                           \
  X(Run,               Output,            Dir,    Lit,       "run",               "",         false)       \
  X(RunData,           Run,               Dir,    Lit,       "data",              "",         false)       \
  X(RunOrders,         RunData,           Dir,    Lit,       "orders",            "",         false)       \
  X(RunOrdersLog,      RunOrders,         Dir,    Lit,       "log",               "",         false)       \
  X(RunOrdersLogFile,  RunOrdersLog,      File,   Log,       "",                  ".log",     false)       \
  X(RunRepair,         RunData,           Bundle, Lit,       "repair",            "",         false)       \
  X(RunFeatures,       Run,               Dir,    Lit,       "features",          "",         false)       \
  X(RunLibPool,        RunFeatures,       Dir,    Pool,      "",                  "",         false)       \
  X(RunLib,            RunLibPool,        Dir,    Tier,      "",                  "",         false)       \
  X(RunLibLog,         RunLib,            Dir,    Lit,       "log",               "",         false)       \
  X(RunLibLogFile,     RunLibLog,         File,   Log,       "",                  ".log",     false)       \
  X(RunLibNodeTime,    RunLib,            File,   Lit,       "node_time.json",    "",         false)       \
  X(RunLibProfiles,    RunLib,            Dir,    Lit,       "profile",           "",         false)       \
  X(RunLibProfile,     RunLibProfiles,    Bundle, Stamp,     "",                  "",         false)       \
  X(RunFactors,        Run,               Dir,    Lit,       "factors",           "",         false)       \
  X(RunOperators,      RunFactors,        File,   Lit,       "operators.json",    "",         false)       \
  X(RunFactorPool,     RunFactors,        Dir,    Pool,      "",                  "",         false)       \
  X(RunFactorLevel,    RunFactorPool,     Dir,    Level,     "",                  "",         false)       \
  X(RunSearch,         RunFactorLevel,    Dir,    Lit,       "search",            "",         false)       \
  X(RunSearchLog,      RunSearch,         File,   Factor,    "",                  ".log",     false)       \
  X(RunExport,         RunFactorLevel,    Dir,    Lit,       "export",            "",         false)       \
  X(RunExportFile,     RunExport,         File,   Export,    "",                  ".csv",     false)       \
  X(RunAgent,          Run,               Dir,    Lit,       "agent",             "",         false)       \
  X(RunAgentMode,      RunAgent,          Dir,    Mode,      "",                  "",         false)       \
  X(RunAgentTools,     RunAgentMode,      Dir,    Lit,       "tools",             "",         false)       \
  X(RunAgentToolFile,  RunAgentTools,     File,   Tool,      "",                  ".md",      false)       \
  X(RunAgentSessions,  RunAgentMode,      Dir,    Lit,       "sessions",          "",         false)       \
  X(RunAgentPool,      RunAgentSessions,  Dir,    Pool,      "",                  "",         false)       \
  X(RunAgentLevel,     RunAgentPool,      Dir,    Level,     "",                  "",         false)       \
  X(RunAgentScope,     RunAgentLevel,     Dir,    Hold,      "",                  "",         false)       \
  X(RunAgentSearch,    RunAgentScope,     Dir,    Lit,       "search",            "",         false)       \
  X(RunAgentSearchLog, RunAgentSearch,    File,   Factor,    "",                  ".log",     false)       \
  X(RunLive,           Run,               Dir,    Lit,       "live",              "",         false)       \
  X(RunLiveRebal,      RunLive,           Dir,    Lit,       "rebal",             "",         false)       \
  X(RunLiveRebalLog,   RunLiveRebal,      File,   Stamp,     "",                  ".jsonl",   false)       \
  X(RunApp,            Run,               Dir,    Lit,       "app",               "",         false)       \
  X(RunAppTsan,        RunApp,            File,   Lit,       "tsan_report.log",   "",         false)       \
  X(RunAppCrash,       RunApp,            File,   Lit,       "crash_stacks.txt",  "",         false)
// clang-format on

enum Node : std::uint16_t {
#define DISK_NODE_ID(id, parent, kind, seg, prefix, suffix, core) id,
  DISK_LAYOUT(DISK_NODE_ID)
#undef DISK_NODE_ID
      kNodeCount
};

struct Def {
  const char *id;
  Node parent;
  Kind kind;
  Seg seg;
  const char *prefix, *suffix;
  bool core; // 基石 (外部重拉不回 / 重做代价大 / 人手填的): 同名文件 / 目录出现在布局外 → sweep 整个搬到布局位置
};
inline constexpr Def kDefs[kNodeCount] = {
#define DISK_NODE_DEF(id, parent, kind, seg, prefix, suffix, core) {#id, parent, Kind::kind, Seg::seg, prefix, suffix, core},
    DISK_LAYOUT(DISK_NODE_DEF)
#undef DISK_NODE_DEF
};
inline constexpr const Def &def(Node n) { return kDefs[n]; }

// 键: 一个节点实例的全部段值; 只填路上用到的字段 (designated initializer 按声明序)
struct Key {
  std::string pool, tier, lvl, level, hold;
  bool test = false;
  misc::Ymd date = 0; // Year / Month / Day 逐级定: YYYY0000 → YYYYMM00 → YYYYMMDD
  misc::Ym ym = 0;
  std::string stamp, name;
  bool operator==(const Key &) const = default;
};

std::filesystem::path root(); // 仓库根 (cwd 向上第一个含 .git 的目录)

// 节点实例的路径; 路上每一段须由 k 定得出 (缺 → assert)
std::filesystem::path path(Node n, const Key &k = {});
bool resolvable(Node n, const Key &k); // 路上每一段都定得出
// 盘上 n 的全部实例 (k 已定的段照用, 未定的逐级展开), 按路径序
std::vector<Key> list(Node n, const Key &k = {});
// 名字 name 是否是 n 的实例 (只验写法): 是则段值记进 k
bool parse(Node n, std::string_view name, Key &k);
// 当前本地时刻 (Seg::Stamp 的写法)
std::string stamp_now();

bool exists(Node n, const Key &k = {});
std::string read(Node n, const Key &k = {});              // 须存在
void write(Node n, const Key &k, std::string_view bytes); // 原子替换, 父目录按需建
void append(Node n, const Key &k, std::string_view line); // 追加一行 (末尾补 '\n'), 父目录按需建
void remove(Node n, const Key &k = {});                   // 文件 / 目录树; 不在则无事
void rename(Node n, const Key &from, const Key &to);      // 同一节点的两个实例; from 须在, to 须不在
std::filesystem::path ensure(Node n, const Key &k = {});  // 目录节点: 建出来并返回路径

} // namespace disk
