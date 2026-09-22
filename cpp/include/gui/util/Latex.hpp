// LaTeX 公式渲染 (MicroTeX → ImGui drawlist) 的公共件: 特征表 (Name CN 悬停) 与 Factors→Operators 表 (Formula 列) 共用.
// 解析结果按 (公式串指针, 字号) 缓存, 公式串必须是静态存活的字面量 (字段表 / OpTable 都是).
#pragma once

namespace tex {
class TeXRender;
}

namespace GUI::Latex {

// 解析 (首次) 并缓存; 解析失败返回 nullptr, 调用方回退纯文本. 引擎按需 init("res")
tex::TeXRender *Get(const char *formula, float text_size);

// 在当前光标处绘制并按渲染尺寸占位 (Dummy). min_height = 占位高度下限 (只放宽占位, 不缩放绘制):
// 表格内联渲染传 ImGui::GetTextLineHeight(), 无上下标的公式才不会比邻行矮几像素 (行高由最高格决定)
void Draw(tex::TeXRender *render, float min_height = 0.0f);

} // namespace GUI::Latex
