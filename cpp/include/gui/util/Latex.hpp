// LaTeX 公式渲染 (MicroTeX → ImGui drawlist) 的公共件: 特征表 (Name CN 悬停) 与 Factors→Operators 表 (Formula 列) 共用.
// 解析结果按 (公式串指针, 字号) 缓存, 公式串必须是静态存活的字面量 (字段表 / OpTable 都是).
#pragma once

namespace tex {
class TeXRender;
}

namespace GUI::Latex {

// 解析 (首次) 并缓存; 解析失败返回 nullptr, 调用方回退纯文本. 引擎按需 init("res")
tex::TeXRender *Get(const char *formula, float text_size);

// 在当前光标处绘制并按渲染尺寸占位 (Dummy)
void Draw(tex::TeXRender *render);

} // namespace GUI::Latex
