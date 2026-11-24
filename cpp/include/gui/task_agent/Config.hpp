#pragma once

// =============================================================================
// agent 配置 (自成一体, 不进 shared/Config.hpp)
// =============================================================================
//   大脑四后端 (gui/task_agent/Llm.hpp), 端点 / key / 默认连接见后端表 kBackends; key 不进仓库, 放本机 output/config/agent/keys.json:
//     智谱    HTTPS 443 对话补全 chat/completions; key = 开放平台 API Keys
//     Cursor  HTTPS 443 Cloud Agents API; key = Dashboard → API Keys 的 user API key (每轮新建无仓库 agent, 起 VM 分钟级)
//     Devin   本机 devin acp 子进程 (Agent Client Protocol, stdio JSON-RPC); key = app.devin.ai Settings → Devin API → PATs 的 cog_ token
//     MCP     外部 agent 经本机 MCP 接口接入 (gui/task_agent/Remote.hpp), 无 key / 模型
//   模型不写死: 各后端的模型表由嗅探得来 (llm::models: HTTPS 后端 GET models, Devin 读 session/new 的模型项), Config 页从中选
//   读超时 = 流里两次增量之间的最长静默 (思考期也有思考增量 / 心跳); 重试 = 网络瞬抖 + 智谱限流 (429 1302 / 1305), 不限次数 (Devin 子进程自理)
//   退出: 任务 Destroy 置取消旗, 全部请求 / 重试等待 1 秒内收手 (Devin 子进程即杀; 只剩 Cursor 远端取消, 限时 kCancelTimeoutSeconds)
// =============================================================================

#include <string_view>

namespace GUI::Agent {

// ---- LLM ----
// 后端表 (唯一真相, 下标 = llm::Backend)
//   fake: 假 key, 格式同真 key (智谱 {id}.{secret}, Cursor crsr_ + 64 位 hex, Devin cog_ 前缀), 全 0; 读到等同未填
//   proxy: 默认连接 (Config 页可改); 均默认直连, Cursor / Devin 的部分模型按出口地区限售, 需要时在 Config 页开代理
struct BackendDef {
  const char *name;   // 显示名 (Config 页 / 会话日志)
  const char *id;     // keys.json 里的字段 = 会话存档里的写法
  const char *fake;   // 假 key; nullptr = 无需 key (外部 MCP)
  const char *host;   // HTTPS 443; nullptr = 非 HTTPS
  const char *models; // GET 模型表 (HTTPS 后端)
  bool proxy;         // 默认经 HTTP 代理 (HTTPS 后端 CONNECT, 子进程经 HTTPS_PROXY 等环境变量)
};
inline constexpr BackendDef kBackends[] = {
    {"Zhipu", "zhipu", "00000000000000000000000000000000.0000000000000000", "open.bigmodel.cn", "/api/paas/v4/models", false},
    {"Cursor", "cursor", "crsr_0000000000000000000000000000000000000000000000000000000000000000", "api.cursor.com", "/v1/models", false},
    {"Devin", "devin", "cog_00000000000000000000000000000000", nullptr, nullptr, false},
    {"MCP", "mcp", nullptr, nullptr, nullptr, false},
};
inline constexpr const char *kZhipuChat = "/api/paas/v4/chat/completions";
inline constexpr const char *kZhipuEffort = "high"; // reasoning_effort
inline constexpr const char *kDevinExe = "devin";   // 按 PATH 找; 工作区 = 系统临时目录下的 stk-devin (仓库外, 免读进仓库的 AGENT.md 等项目规则)

inline constexpr const char *kProxyHost = "127.0.0.1";
inline constexpr int kProxyPort = 10808;

inline constexpr int kReadTimeoutSeconds = 600;
inline constexpr int kRetryIntervalSeconds = 30; // 不限次数, 直到成功或取消
inline constexpr int kCancelTimeoutSeconds = 5;  // 取消后通知远端停 run (Cursor) 的每步限时: 取消 / 退出时唯一不看取消旗的请求; 也是 Devin 收尾 (删会话 / 等子进程退) 的限时

// ---- 挖掘 ----
inline constexpr int kMaxCalls = 12;   // 一条回复的工具调用数上限 (gui/task_agent/Tools.hpp)
inline constexpr int kBatch = 3;       // 一条回复的 eval_factor 数上限 (提示词 {{batch}})
inline constexpr int kHypEvals = 3;    // 一个假设至多挂几个落了文件的变体 (提示词 {{hyp_evals}})
inline constexpr int kMaxOps = 10;     // 一个表达式的算子节点数上限 (超了不落文件; 提示词 {{max_ops}})
inline constexpr int kGoodOps = 5;     // 建议的算子数 (提示词 {{good_ops}})
inline constexpr int kEvidence = 6;    // judge 的证据至多引用几个调用
inline constexpr int kListTop = 80;    // list_tried / list_library 一次至多列几个 (只列可见的: 同评估期且不超 kMaxOps; 按该持有期 score 降序)
inline constexpr int kNotesTop = 40;   // list_notes 一次至多列几条 (新到旧)
inline constexpr int kOutChars = 6000; // 一个调用的结果交给 agent 的字数上限 (超了截断并说明; 提示词 {{out_chars}})
inline constexpr int kStockDays = 60;  // view_stock_days 一次至多看几日
inline constexpr int kStockRows = 40;  // view_stock_day 逐行时段一次至多几行 (逐行表要装进 kOutChars)
inline constexpr int kBookMins = 30;   // view_book 一次重放的窗口至多几分钟 (交易时段内)
inline constexpr int kBookRows = 30;   // view_book 逐时表至多几行 (每行 step 秒; 表要与事件 / 剖面一起装进 kOutChars)
inline constexpr int kMaxRetries = 5;  // 连续这么多条回复不合协议即停下, 等研究员 (多半是模型或提示词出了问题)

// 常量占位 {{键}} (提示词模板与工具说明同一张表; 模板另有随作用域的键, 见 gui/task_agent/Prompt.hpp)
struct ConstDef {
  const char *key;
  int value;
};
inline constexpr ConstDef kConsts[] = {{"max_calls", kMaxCalls}, {"batch", kBatch}, {"hyp_evals", kHypEvals}, {"max_ops", kMaxOps}, {"good_ops", kGoodOps}, {"out_chars", kOutChars}, {"stock_days", kStockDays}, {"stock_rows", kStockRows}, {"book_mins", kBookMins}, {"book_rows", kBookRows}};
inline const ConstDef *const_of(std::string_view key) {
  for (const ConstDef &c : kConsts)
    if (key == c.key)
      return &c;
  return nullptr;
}

// 入库门槛默认值 (Config 页可改, 下次 New 生效): agent 提交的候选至少一个权重达到才拷进正式因子库
inline constexpr double kGateRic = 0.03; // 中位 rIC (滚动窗 rIC 的中位, 同 Search 的目标)

// ---- 外部 agent 接口 (gui/task_agent/Remote.hpp): 本机 MCP (Streamable HTTP), 程序一开即监听; 外部 agent 经它当会话的大脑 ----
inline constexpr const char *kRemoteHost = "127.0.0.1";
inline constexpr int kRemotePort = 47821; // 避开常见调试端口 (8000 / 8080 / 8765 …)
inline constexpr const char *kRemotePath = "/mcp";
inline constexpr int kRemoteWait = 25; // run / 直接调研究工具一次至多等多少秒 (还没做完先交回进度, 调无参 run 续等)

// ---- 落盘 (disk::, 布局见 disk/Disk.hpp; <模式> = main | test, test = 自检会话) ----
//   output/agent/<模式>/sessions/<池>/<层>/<持有期>/
//     factors/                          候选因子文件 (与正式因子同格式)
//     chat/<时间>/                      一段会话的存档与可读日志 (session.json / brief.json / system.md / log.md)
//   output/agent/<模式>/notes/<类>.jsonl 全局记录, 一类一个文件 (特征申请 / 工具申请 / 发现 / 建议与 bug; gui/task_agent/Notes.hpp)
//   output/run/agent/
//     remote.json                       外部 agent 的 MCP 地址 {"url": …} (程序一开即写; py/agent.py 读它)
//     <模式>/sessions/<池>/<层>/<持有期>/search/ 候选的 Search 日志
//   output/data/doc/resources-{tag,proc}/ 研报库: 标注 + tag_stat.md / 全文 (gui/task_agent/Docs.hpp, 只读)
//   output/config/agent/
//     prompt/<名>.md                    提示词模板 ({{键}} 占位, gui/task_agent/Prompt.hpp)
//     families.md                       特征族卡: 每族一节 "# <族> <族名>" + 卡文 (族 = 节点文件, 见 Leaf::family; 系统提示的特征表按族排)
//     keys.json                         {<BackendDef::id>: key}; 无文件则按后端表填假 key 生成 (本机, 不进仓库)
//     ui.json                           Mine / Config 页的选项: 改了即写, 首次 Draw 读回; 无文件 = 用上面的默认值 (本机)

} // namespace GUI::Agent
