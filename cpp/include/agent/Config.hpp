#pragma once

// =============================================================================
// agent 配置 (自成一体, 不进 shared/Config.hpp)
// =============================================================================
//   LLM 两后端都走 HTTPS 443 (agent/Llm.hpp), 端点 / key / 默认连接见后端表 kBackends; key 不进仓库, 放本地 kKeyFile (gitignore):
//     智谱    对话补全 chat/completions; key = 开放平台 API Keys
//     Cursor  Cloud Agents API; key = Dashboard → API Keys 的 user API key (无仓库 agent, 起 VM 分钟级)
//   模型不写死: 各后端的模型表由 GET models 嗅探 (llm::models), Config 页从中选
//   读超时 = 流里两次增量之间的最长静默 (思考期也有思考增量 / 心跳); 重试 = 网络瞬抖 + 智谱限流 (429 1302 / 1305), 不限次数
//   退出: 任务 Destroy 置取消旗, 全部请求 / 重试等待 1 秒内收手 (只剩 Cursor 远端取消, 限时 kCancelTimeoutSeconds)
//   目录相对 app 工作目录 (与 shared::Config 的默认目录同一个基准)
// =============================================================================

namespace agent {

// ---- LLM ----
// 后端表 (唯一真相, 下标 = llm::Backend)
//   fake: 假 key, 格式同真 key (智谱 {id}.{secret}, Cursor crsr_ + 64 位 hex), 全 0; 读到等同未填
//   proxy: 默认连接 (Config 页可改); Cursor 的部分模型按出口地区限售, 默认走代理
struct BackendDef {
  const char *name;   // 显示名 (Config 页 / 会话日志)
  const char *field;  // kKeyFile 里的字段
  const char *fake;   // 假 key
  const char *host;   // HTTPS 443
  const char *models; // GET 模型表
  bool proxy;         // 默认经 HTTP 代理 (CONNECT)
  bool local;         // 对话历史留在本地每轮重发 (旧消息可换成折叠版); 否则在远端
};
inline constexpr BackendDef kBackends[] = {
    {"Zhipu", "zhipu", "00000000000000000000000000000000.0000000000000000", "open.bigmodel.cn", "/api/paas/v4/models", false, true},
    {"Cursor", "cursor", "crsr_0000000000000000000000000000000000000000000000000000000000000000", "api.cursor.com", "/v1/models", true, false},
};
inline constexpr const char *kZhipuChat = "/api/paas/v4/chat/completions";
inline constexpr const char *kZhipuEffort = "high"; // reasoning_effort

inline constexpr const char *kProxyHost = "127.0.0.1";
inline constexpr int kProxyPort = 10808;

inline constexpr int kReadTimeoutSeconds = 600;
inline constexpr int kRetryIntervalSeconds = 30; // 不限次数, 直到成功或取消
inline constexpr int kCancelTimeoutSeconds = 5;  // 取消后通知远端停 run (Cursor) 的每步限时: 取消 / 退出时唯一不看取消旗的请求

// ---- 挖掘 ----
inline constexpr int kMaxCalls = 12;   // 一条回复的工具调用数上限 (agent/Tools.hpp)
inline constexpr int kBatch = 3;       // 一条回复的 eval_factor 数上限 (提示词 {{batch}})
inline constexpr int kHypEvals = 3;    // 一个假设至多挂几个落了文件的变体 (提示词 {{hyp_evals}})
inline constexpr int kHypKnobs = 1;    // 其中至多几个是旋钮类改动 (换平滑 / 换压缩 / 换口径; 只在 base 达门槛时可用; 提示词 {{hyp_knobs}})
inline constexpr int kMaxOps = 10;     // 一个表达式的算子节点数上限 (超了不落文件; 提示词 {{max_ops}})
inline constexpr int kParts = kMaxOps - 1; // eval_factor 的功能块至多几个: 要说明的块各是根之外的一个算子节点, 装得下算子数上限的式子
inline constexpr int kPartChars = 30;  // 一个功能块的说明至多几字, 不计空格标点 (提示词 {{part_chars}})
inline constexpr int kEvidence = 6;    // judge 的证据至多引用几个调用
inline constexpr int kGoodOps = 5;     // 建议的算子数 (提示词 {{good_ops}})
inline constexpr int kListTop = 80;    // list_tried / list_library 一次至多列几个 (只列可见的: 同评估期且不超 kMaxOps; 按该持有期 score 降序)
inline constexpr int kNotesTop = 40;   // list_notes 一次至多列几条 (新到旧)
inline constexpr int kOutChars = 6000; // 一个调用的结果交给 agent 的字数上限 (超了截断并说明; 提示词 {{out_chars}})
inline constexpr int kStockDays = 60;  // view_stock_days 一次至多看几日
inline constexpr int kStockRows = 40;  // view_stock_day 逐行时段一次至多几行 (逐行表要装进 kOutChars)
inline constexpr int kBookMins = 30;   // view_book 一次重放的窗口至多几分钟 (交易时段内)
inline constexpr int kBookRows = 30;   // view_book 逐时表至多几行 (每行 step 秒; 表要与事件 / 剖面一起装进 kOutChars)
inline constexpr int kKeepRounds = 3;  // 会话历史里原样留着的最近 user 消息 kKeepRounds..2·kKeepRounds 条; 攒满即把更早的结果消息成批换成折叠版 (只智谱: 本地留历史; 提示词 {{history}})

// 入库门槛默认值 (Config 页可改, 下次 New 生效): agent 提交的候选至少一个权重全部达到才拷进正式因子库
inline constexpr double kGateMono = 0.95; // 分层单调性
inline constexpr double kGateRic = 0.03;  // rIC 均值

// ---- 目录 ----
//   kWorkDir/<池>/<层>/<持有期>/factors/   候选因子文件 (与正式因子同格式; Search 日志在其 log/)
//   kWorkDir/<池>/<层>/<持有期>/chat/<时间>/ 一段会话的日志 (agent/Journal.hpp)
//   kGlobalDir/                             不随作用域的 (与会话目录分开):
//     <类>.jsonl                            全局记录, 一类一个文件 (特征申请 / 工具申请 / 发现 / 建议与 bug; agent/Notes.hpp)
//     tools/<工具名>.md                     各工具最近一次交给 agent 的结果 (跨会话互相覆盖; 开会话时删掉工具表里已没有的)
//     test/<池>/<层>/<持有期>/              自检会话的 factors/ 与 chat/ (同会话目录的结构)
//     test/<类>.jsonl                       自检会话的全局记录
//   kPromptDir/{system,first,round,retry,test}.md 提示词模板 ({{键}} 占位, agent/Prompt.hpp)
//   kFamilyFile                             特征族卡: 每族一节 "# <族> <族名>" + 卡文 (族 = 节点文件, 见 Leaf::family; 系统提示的特征表按族排)
//   kDocDir/resources-{tag,proc}/           研报库: 标注 + tag_stat.md / 全文 (agent/Docs.hpp, 只读)
inline constexpr const char *kWorkDir = "../../../../output/agent";
inline constexpr const char *kGlobalDir = "../../../../output/agent/global";
inline constexpr const char *kToolOutDir = "../../../../output/agent/global/tools";
inline constexpr const char *kDocDir = "../../../../output/doc";
inline constexpr const char *kPromptDir = "../../../../config/agent/prompt";
inline constexpr const char *kFamilyFile = "../../../../config/agent/families.md";
inline constexpr const char *kKeyFile = "../../../../config/agent/keys.json"; // {<BackendDef::field>: key}; 无文件则按后端表填假 key 生成
inline constexpr const char *kUiFile = "../../../../config/agent/ui.json";    // Mine / Config 页的选项 (gitignore): 改了即写, 首次 Draw 读回; 无文件 = 用上面的默认值

} // namespace agent
