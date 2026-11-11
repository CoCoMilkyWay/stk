// Features 页公共 UI 件: 作用域条 (Table / NodeTime 页顶) + 流式分析 (Distribution / Transform / Table 页预览) 的展示件
// 对仗 gui/task_features/services/Analysis.hpp: 状态面 (Published) / 值账目 (Integrity) 的展示都只有这一份
#pragma once

#include "gui/task_features/services/Analysis.hpp"
#include "shared/Progress.hpp"

#include <cstddef>

struct ImVec4;
struct SharedData;

namespace GUI::Features {

// ---- 作用域条 (Table / NodeTime 页顶): 池 × 档 = 库, 层 = 视图; Features 自持, 不与 Alpha 同步; 池下拉可拖动调池表序 ----
void RenderScope(SharedData &data);

// ---- 状态面 (阶段名/颜色走 gui/Tasks.hpp TextPhase) ----
// Row: Compute | (仅 L_1min) | Cancel | Status: xxx (unit done/total)
// 选中特征且 L_1min 且不忙才可 Compute. 返回值: 1 = Compute 按下, -1 = Cancel 按下, 0 = 无
int RenderStreamControl(const Progress &st, const SharedData &data, const char *unit);

// ---- 值账目 ----
// 着色阈值 (Distribution 完整性条 与 Table 页账目列 共用)
ImVec4 GetMinMaxColor(float val);    // |val| > 100 红
ImVec4 GetZeroPctColor(float pct);   // ≥10% 红, ≥5% 黄
ImVec4 GetNanInfPctColor(float pct); // ≥1% 红, >0 黄
// 一行账目: Zero/NaN/±Inf (计数 + 着色百分比) + Min/Max
void RenderIntegrity(const analysis::Integrity &it);

// ---- 绘图 ----
// 高亮线 (焦点/hover 置顶): 白描边 + cyan
void PlotHighlightLine(const float *x, const float *y, size_t n);
template <class Pdf>
void PlotHighlightLine(const Pdf &p) { PlotHighlightLine(p.x.data(), p.y.data(), p.n_pts); }

} // namespace GUI::Features
