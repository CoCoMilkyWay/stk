#pragma once

// =============================================================================
// agent 配置 (自成一体, 不进 shared/Config.hpp)
// =============================================================================
//   LLM 两后端都走 HTTPS 443 (agent/Llm.hpp):
//     智谱    对话补全 chat/completions; key = 开放平台 API Keys
//     Cursor  Cloud Agents API; key = Dashboard → API Keys 的 user API key (无仓库 agent, 起 VM 分钟级)
//   读超时 = 流里两次增量之间的最长静默 (思考期也有思考增量 / 心跳); 重试 = 网络瞬抖 + 智谱限流 (429 1302 / 1305)
//   目录相对 app 工作目录 (与 shared::Config 的默认目录同一个基准)
// =============================================================================

namespace agent {

// ---- LLM ----
inline constexpr const char *kZhipuKey = "";
inline constexpr const char *kZhipuHost = "open.bigmodel.cn";
inline constexpr const char *kZhipuTarget = "/api/paas/v4/chat/completions";
inline constexpr const char *kZhipuModels[] = {"glm-5.2", "glm-5.3", "glm-5.3-flash"};
inline constexpr const char *kZhipuEffort = "high"; // reasoning_effort

inline constexpr const char *kCursorKey = "";
inline constexpr const char *kCursorHost = "api.cursor.com";
inline constexpr const char *kCursorModels[] = {"default", "composer-latest"}; // default = 账号默认模型 (请求不带 model); 其余 id 见 GET /v1/models

inline constexpr int kReadTimeoutSeconds = 600;
inline constexpr int kRetryMax = 4; // 共 5 次尝试
inline constexpr int kRetryIntervalSeconds = 30;

// ---- 挖掘 ----
inline constexpr int kBatch = 8;     // 每轮要的模板数 (提示词 {{batch}})
inline constexpr int kLibTop = 40;   // 系统提示里列的正式因子 (按该持有期 score 降序)
inline constexpr int kTriedTop = 80; // 首条消息里列的本作用域已试候选 (新的在前)

// ---- 目录 ----
//   kWorkDir/<池>/<层>/<持有期>/factors/   候选因子文件 (与正式因子同格式; Search 日志在其 log/)
//   kWorkDir/<池>/<层>/<持有期>/chat/<时间>/ 一段对话: system.md + 每轮 NN.user.md / NN.reply.md / NN.thinking.md + rounds.jsonl
//   kPromptDir/{system,first,round,retry}.md 提示词模板 ({{键}} 占位, agent/Prompt.hpp)
inline constexpr const char *kWorkDir = "../../../../output/agent";
inline constexpr const char *kPromptDir = "../../../../config/agent/prompt";

} // namespace agent
