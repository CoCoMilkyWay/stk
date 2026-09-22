// 见头文件. 原为 TabFeature.cpp 的文件内静态函数, Operators 表也要用, 抽到这里.
#include "gui/util/Latex.hpp"

#include "graphic/graphic_basic.h"
#include "imgui.h"
#include "latex.h"
#include "platform/imgui/graphic_imgui.h"
#include "render.h"
#include "utfcpp/utf8.hpp"

#include <algorithm>
#include <cassert>
#include <string>
#include <string_view>
#include <unordered_map>

namespace GUI::Latex {

namespace {

std::wstring utf8ToWide(std::string_view s) {
  auto u16 = utf8::utf8to16(s);
  return {u16.begin(), u16.end()};
}

struct Key {
  const char *formula;
  float size;
  bool operator==(const Key &o) const { return formula == o.formula && size == o.size; }
};
struct KeyHash {
  size_t operator()(const Key &k) const {
    return std::hash<const void *>()(k.formula) ^ (std::hash<float>()(k.size) << 1);
  }
};

std::unordered_map<Key, tex::TeXRender *, KeyHash> s_cache;

} // namespace

tex::TeXRender *Get(const char *formula, float text_size) {
  const Key key{formula, text_size};
  auto it = s_cache.find(key);
  if (it != s_cache.end())
    return it->second;

  static bool s_initialized = false;
  if (!s_initialized) {
    tex::LaTeX::init("res");
    s_initialized = true;
  }

  const std::wstring wlatex = utf8ToWide(formula);
  tex::TeXRender *render = tex::LaTeX::parse(wlatex, 0, text_size, 5.0f, tex::green);
  s_cache[key] = render; // 解析失败 = nullptr, 也缓存 (不必每帧重试)
  return render;
}

void Draw(tex::TeXRender *render, float min_height) {
  assert(render);
  // 解析可能加了新字形, 字体图集失效则重建
  tex::Font_imgui::rebuildFontAtlasIfNeeded();

  ImDrawList *draw_list = ImGui::GetWindowDrawList();
  const ImVec2 cursor_pos = ImGui::GetCursorScreenPos();
  tex::Graphics2D_imgui g2(draw_list);
  g2.translate(cursor_pos.x, cursor_pos.y);
  render->draw(g2, 0, 0);
  ImGui::Dummy(ImVec2((float)render->getWidth(), std::max((float)render->getHeight(), min_height)));
}

} // namespace GUI::Latex
