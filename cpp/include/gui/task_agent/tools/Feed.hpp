#pragma once

// =============================================================================
// Feed — 工作台的随机输入: 按本作用域取材 (开放的叶 / 候选 / 评估期的日与行 / 池内代码 / 当日有逐笔的票 / 做成了的调用编号)
// =============================================================================
//   清单一行一个 Sample<工具>(Feed&) → 该工具一组用得上的参数 (定义在 tools/<类>/<工具>.cpp, 与工具函数同处); SampleArgs 按工具分派
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace GUI::Agent {

struct Spot {
  std::string code, day, clock; // 代码 / 交易日 YYYY-MM-DD / 行末时刻 HH:MM (日层空)
};

struct Feed {
  const Desk &desk;
  const Context &ctx;
  const std::vector<Result> &tried;
  const Gate &gate;
  Steps timeline;
  int me;
  std::mt19937_64 &rng;

  int below(int n); // [0, n)
  bool coin(double p);
  std::string pick(Tool t, std::string_view key); // 参数表里该参数可选集的一项 (缺省为空串的另含空串)
  template <class T>
  const T &one(const std::vector<T> &v) {
    return v[static_cast<size_t>(below(static_cast<int>(v.size())))];
  }
  std::string leaf();                                       // 开放的叶
  std::vector<std::string> leaves(int n);                   // 至多 n 个互不相同的开放的叶
  std::vector<std::string> grid();                          // 开放的本层叶 (因子须含本层的叶)
  std::string factor();                                     // 可见且有权重的候选名; 没有就拿开放的本层叶套开放的归一根
  std::string day();                                        // 评估期内的交易日 YYYY-MM-DD
  std::pair<std::string, std::string> span(int lo, int hi); // 评估期内连续 lo..hi 个交易日的首末日
  std::string code();                                       // 池内代码
  std::string clock();                                      // 本层某一行的行末时刻 (不含竞价行; 日层空)
  std::string tod(int cap, const std::string &around = {}); // 本层连续 1..cap 行的行末时段 HH:MM-HH:MM (给了 around 就罩住它; 日层空)
  Spot spot();                                              // 一只当日有逐笔的票的一天: 多半取工作台里 view_trades 明细列过的段
  std::vector<std::string> refs(int n);                     // 做成了的调用编号 (新到旧)
};

#define AGENT_TOOL_SAMPLE(T, id, cn, F, C, keep, R) nlohmann::ordered_json Sample##T(Feed &);
AGENT_TOOLS(AGENT_TOOL_SAMPLE)
#undef AGENT_TOOL_SAMPLE

nlohmann::ordered_json SampleArgs(Tool t, Feed &p);

} // namespace GUI::Agent
