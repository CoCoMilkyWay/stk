#pragma once

// =============================================================================
// Widgets — Agent 各页的界面公共件: Mine / Tools / Config 页与各工具的展示 (ui/<类>/<工具>.cpp) 共用
// =============================================================================
//   文本: 输入框 / 悬浮提示 / 换行文本 / 可折叠的长文框 (按段排版缓存, 每帧只画看得见的段)
//   量: 用时 / 千位 / 字数 / 状态色
//   作用域行: 池 · 层 · 持有期 · lob · 评估年数 (只在 Mine 页; Tools 页随它)
//   展示: 节标题 / 数格 (涨红跌绿) / 表 / 20 组柱 / 折线 / 分箱柱
// =============================================================================

#include "gui/Tasks.hpp"
#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Tools.hpp"

#include "imgui.h"

#include <initializer_list>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Agent::ui {

inline constexpr ImVec4 kColHead(0.4f, 0.8f, 1.0f, 1.0f);
inline constexpr ImVec4 kColExpr(0.9f, 0.8f, 0.55f, 1.0f);
inline constexpr float kThinkLines = 12.0f, kBoxLines = 20.0f; // 思考 / 在收正文框 · 其余文本框的最大行高

// ---- 文本 ----
bool input_text(const char *id, std::string &s, const char *hint = nullptr);
bool input_multiline(const char *id, std::string &s, ImVec2 size);
void tip(const char *s);
void tip_long(const std::string &s);
void wrapped(const std::string &s);
void wrapped(const ImVec4 &c, const std::string &s);
void wrapped_dim(const std::string &s);
void copy_button(const char *id, const std::string &s);
void box(const std::string &body, bool dim, float lines, bool tail);
bool fold(const char *id, ImGuiTreeNodeFlags flags, const std::string &label, const std::string &body, bool dim, float lines, bool tail);

// ---- 量 ----
std::string dur(double sec);
std::string kilo(double n);
std::string chars(const std::string &s);
ImVec4 color(TaskStatus::Kind k);
std::string stamp_text(const std::string &st);

// ---- 表 (随页宽: 前列 FixedFit, 末列吃剩下的宽并换行; 可滚) ----
inline constexpr ImGuiTableFlags kTableFlags = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY;
bool begin_table(const char *id, std::initializer_list<const char *> heads, float h = -1.0f);

// ---- 作用域行 ----
void scope_row(SharedData &data, Scope &o, std::vector<catalog::PoolRow> &rows, const Lab &lab, bool locked, const char *title);

// ---- ask_human 的逐题点选 (edit_asked 在问它, 返回按了交答案; draw_asked 只读展示) ----
bool edit_asked(Asked &a);
void draw_asked(const Asked &a);

// ---- 工具展示 ----
void heading(const std::string &s);                               // 节标题
bool grid(const char *id, const std::vector<std::string> &heads); // 自适应高的表 (不滚; 末列不伸)
void cell(const std::string &s);
void cell_dim(const std::string &s);
void cell_num(double v, const char *fmt, bool tint = false); // tint: 正红负绿
void cell_bp(double r);                                      // 收益 → bp, 正红负绿
void cell_pct(double x);
void groups_plot(const char *id, const std::vector<double> &grp, float h, const char *unit = "bp"); // 20 组柱 G1 → G20 (收益按 bp 画)
void line_plot(const char *id, const std::vector<double> &ys, float h, const char *label, const std::vector<std::string> *ticks = nullptr);
void hist_plot(const char *id, const std::vector<double> &hist, double lo, double hi, float h);
void bars_plot(const char *id, const std::vector<std::string> &names, const std::vector<double> &vals, float h); // 横条 (名 → 值)

} // namespace GUI::Agent::ui
