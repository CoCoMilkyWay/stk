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
};
inline constexpr BackendDef kBackends[] = {
    {"Zhipu", "zhipu", "00000000000000000000000000000000.0000000000000000", "open.bigmodel.cn", "/api/paas/v4/models", false},
    {"Cursor", "cursor", "crsr_0000000000000000000000000000000000000000000000000000000000000000", "api.cursor.com", "/v1/models", true},
};
inline constexpr const char *kZhipuChat = "/api/paas/v4/chat/completions";
inline constexpr const char *kZhipuEffort = "high"; // reasoning_effort

inline constexpr const char *kProxyHost = "127.0.0.1";
inline constexpr int kProxyPort = 10808;

inline constexpr int kReadTimeoutSeconds = 600;
inline constexpr int kRetryIntervalSeconds = 30; // 不限次数, 直到成功或取消
inline constexpr int kCancelTimeoutSeconds = 5;  // 取消后通知远端停 run (Cursor) 的每步限时: 取消 / 退出时唯一不看取消旗的请求

// ---- 挖掘 ----
inline constexpr int kBatch = 8;     // 每轮变体数上限 (提示词 {{batch}})
inline constexpr int kMaxOps = 10;   // 一个表达式的算子节点数上限 (超了不落文件; 提示词 {{max_ops}})
inline constexpr int kGoodOps = 5;   // 建议的算子数 (提示词 {{good_ops}})
inline constexpr int kLibTop = 40;   // 系统提示里列的正式因子 (按该持有期 score 降序)
inline constexpr int kTriedTop = 80; // 首条消息里列的本作用域已试候选 (新的在前)

// 入库门槛默认值 (Config 页可改, 下次 New 生效): agent 提交的候选至少一个权重全部达到才拷进正式因子库
inline constexpr double kGateMono = 0.95; // 分层单调性
inline constexpr double kGateRic = 0.03;  // rIC 均值

// ---- 目录 ----
//   kWorkDir/<池>/<层>/<持有期>/factors/   候选因子文件 (与正式因子同格式; Search 日志在其 log/)
//   kWorkDir/<池>/<层>/<持有期>/chat/<时间>/ 一段会话的日志 (agent/Journal.hpp)
//   kPromptDir/{system,first,answer,round,retry}.md 提示词模板 ({{键}} 占位, agent/Prompt.hpp)
inline constexpr const char *kWorkDir = "../../../../output/agent";
inline constexpr const char *kPromptDir = "../../../../config/agent/prompt";
inline constexpr const char *kKeyFile = "../../../../config/agent/keys.json"; // {<BackendDef::field>: key}; 无文件则按后端表填假 key 生成
inline constexpr const char *kUiFile = "../../../../config/agent/ui.json";    // Mine / Config 页的选项 (gitignore): 改了即写, 首次 Draw 读回; 无文件 = 用上面的默认值

} // namespace agent
