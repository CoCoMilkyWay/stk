#pragma once

// =============================================================================
// Docs — 研报库: search_docs / search_factors / read_doc 共用的库与公共件 (在 Lab worker 内执行, 不卡界面)
// =============================================================================
//   库 = output/data/doc 下的加工结果, 只读:
//     resources-tag/report/<机构>/<系列>/<id>.json        标注索引, 首次调用加载一次
//     resources-tag/tag_stat.md                          阶段 → 流派分面树骨架
//     resources-proc/report/<机构>/<系列>/<id>/report.md  全文 (去图片行); 按篇懒加载并缓存, 全文检索逐篇看取消旗
//   三个工具各一个入口 (tools/query/<工具>.cpp), Browse 按工具分派
// =============================================================================

#include "gui/task_agent/Tools.hpp"
#include "gui/task_agent/tools/Query.hpp"
#include "nlohmann/json.hpp"

#include <array>
#include <atomic>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace GUI::Agent {

ToolOut Browse(const Call &c, const std::atomic<bool> &cancel);
ToolOut SearchDocs(const Call &c, const std::atomic<bool> &cancel);
ToolOut SearchFactors(const Call &c, const std::atomic<bool> &cancel);
ToolOut ReadDoc(const Call &c, const std::atomic<bool> &cancel);

namespace docs {

inline constexpr size_t kBrief = 90;
inline constexpr size_t kEvidence = 120;
inline constexpr size_t kSnip = 40;
inline constexpr int kSnips = 2;
inline constexpr size_t kTop = 12;
inline constexpr size_t kReadTail = 300;

inline constexpr std::array<std::string_view, 8> kPeriods = {"second", "intraday", "daily", "weekly", "monthly", "quarterly", "event", "na"};
inline constexpr std::array<const char *, 4> kStats = {"IC", "RankIC", "ICIR", "多空年化"};

struct Stage {
  std::string code, cn;
  std::vector<std::string> approaches;
};
struct Factor {
  size_t doc;
  std::string name, family, direction, horizon, formula, evidence;
  std::vector<std::string> period;
  std::array<double, 4> stats;
  std::string hay;
};
struct Doc {
  nlohmann::json tag;
  std::string id, dir, broker, genre, primary, value;
  int year = 0;
  std::vector<std::string> asset, stages, approaches, l1, freqs, findings, builds_on, external;
  std::vector<size_t> cited_by, factors;
  std::string hay;
};
struct Text {
  std::string body, low;
  std::vector<size_t> at;
  size_t end(size_t k) const { return k + 1 < at.size() ? at[k + 1] : body.size(); }
  size_t section(size_t p) const;
  std::string title(size_t k) const;
};

struct Lib {
  std::vector<Stage> stages;
  std::map<std::string, std::string> cn, stage_of;
  std::vector<Doc> docs;
  std::vector<Factor> factors;
  std::map<std::string, size_t> by_id;
  mutable std::mutex mu;
  mutable std::vector<std::optional<Text>> texts;

  const Text &text_at(size_t i) const;
  bool ensure_all_texts(const std::atomic<bool> &cancel) const;
};
const Lib &lib();
const Lib *loaded(); // 已加载的库 (未加载 = nullptr; 工作台取材用, 不触发加载)

using Count = std::map<std::string, int>;
bool has(const std::vector<std::string> &v, const std::string &x);
int count_in(const std::string &hay, const std::vector<std::string> &ws);
Ranks ranked(const Count &c);                        // 计数降序 (同数按键序)
std::string ranked_text(const Ranks &v, size_t top); // "键 数 · …" (超 top 的报另几项)
std::string named(const Lib &L, const std::string &code);
size_t period_ix(const std::string &p);
std::string stats_of(const std::array<double, 4> &s);
std::string factor_head(const Factor &f);
int value_rank(const std::string &v);
std::vector<std::string> snippets(const Text &t, const std::string &w); // 命中处的小节与片段 (至多 kSnips 处, 一节一处)
std::string conditions(const Call &c);
std::optional<size_t> doc_index(const Lib &L, const std::string &id, std::string &error);
std::string setup_of(const nlohmann::json &s);

} // namespace docs
} // namespace GUI::Agent
