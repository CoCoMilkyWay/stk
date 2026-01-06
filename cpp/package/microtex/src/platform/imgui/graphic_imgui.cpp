#if defined(BUILD_IMGUI)

#include "graphic_imgui.h"
#include <cassert>
#include <cstdint>

namespace tex {

// ============================================================================
// Helper functions
// ============================================================================

static std::string wideToUtf8(const std::wstring &wstr) {
  if (wstr.empty())
    return "";

  std::string result;
  for (wchar_t wc : wstr) {
    auto cp = static_cast<uint32_t>(wc);

    if (cp < 0x80) {
      result += static_cast<char>(cp);
    } else if (cp < 0x800) {
      result += static_cast<char>(0xC0 | (cp >> 6));
      result += static_cast<char>(0x80 | (cp & 0x3F));
    } else if (cp < 0x10000) {
      result += static_cast<char>(0xE0 | (cp >> 12));
      result += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
      result += static_cast<char>(0x80 | (cp & 0x3F));
    } else {
      result += static_cast<char>(0xF0 | (cp >> 18));
      result += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
      result += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
      result += static_cast<char>(0x80 | (cp & 0x3F));
    }
  }
  return result;
}

static std::string wcharToUtf8(wchar_t c) {
  std::wstring ws(1, c);
  return wideToUtf8(ws);
}

// ============================================================================
// Font_imgui
// ============================================================================

Font_imgui::Font_imgui(const std::string &file, float size)
    : _size(size), _style(PLAIN), _imfont(nullptr) {
  // Use ImGui's default font - loading from file not implemented
  _imfont = ImGui::GetFont();
}

Font_imgui::Font_imgui(const std::string &name, int style, float size)
    : _size(size), _style(style), _imfont(nullptr) {
  _imfont = ImGui::GetFont();
}

sptr<Font> Font_imgui::deriveFont(int style) const {
  return std::make_shared<Font_imgui>("", style, _size);
}

bool Font_imgui::operator==(const Font &f) const {
  const Font_imgui *other = dynamic_cast<const Font_imgui *>(&f);
  if (!other)
    return false;
  return _size == other->_size && _style == other->_style;
}

bool Font_imgui::operator!=(const Font &f) const {
  return !(*this == f);
}

// Static factory methods required by MicroTeX
Font *Font::create(const std::string &file, float size) {
  return new Font_imgui(file, size);
}

sptr<Font> Font::_create(const std::string &name, int style, float size) {
  return std::make_shared<Font_imgui>(name, style, size);
}

// ============================================================================
// TextLayout_imgui
// ============================================================================

TextLayout_imgui::TextLayout_imgui(const std::wstring &src, const sptr<Font_imgui> &font)
    : _text(src), _font(font) {}

void TextLayout_imgui::getBounds(Rect &bounds) {
  std::string utf8 = wideToUtf8(_text);
  ImFont *font = _font ? _font->getImFont() : ImGui::GetFont();
  float fontSize = _font ? _font->getSize() : ImGui::GetFontSize();

  ImVec2 size = font->CalcTextSizeA(fontSize, FLT_MAX, 0.0f, utf8.c_str());
  bounds.x = 0;
  bounds.y = -fontSize * 0.8f; // Approximate ascent
  bounds.w = size.x;
  bounds.h = fontSize;
}

void TextLayout_imgui::draw(Graphics2D &g2, float x, float y) {
  g2.drawText(_text, x, y);
}

// Static factory method required by MicroTeX
sptr<TextLayout> TextLayout::create(const std::wstring &src, const sptr<Font> &font) {
  auto imgui_font = std::dynamic_pointer_cast<Font_imgui>(font);
  if (!imgui_font) {
    imgui_font = std::make_shared<Font_imgui>("", PLAIN, 20.0f);
  }
  return std::make_shared<TextLayout_imgui>(src, imgui_font);
}

// ============================================================================
// Graphics2D_imgui
// ============================================================================

Graphics2D_imgui::Graphics2D_imgui(ImDrawList *drawList)
    : _drawList(drawList) {
  assert(_drawList != nullptr);
}

ImU32 Graphics2D_imgui::toImColor(color c) {
  // tex::color is ARGB, ImU32 is ABGR
  return IM_COL32(color_r(c), color_g(c), color_b(c), color_a(c));
}

void Graphics2D_imgui::setColor(color c) {
  _color = c;
  _imColor = toImColor(c);
}

void Graphics2D_imgui::transformPoint(float x, float y, float &ox, float &oy) const {
  // Apply scale
  float sx = x * _transform.sx;
  float sy = y * _transform.sy;

  // Apply rotation
  if (_transform.rotation != 0.0f) {
    float cos_r = std::cos(_transform.rotation);
    float sin_r = std::sin(_transform.rotation);
    float rx = sx * cos_r - sy * sin_r;
    float ry = sx * sin_r + sy * cos_r;
    sx = rx;
    sy = ry;
  }

  // Apply translation
  ox = sx + _transform.tx;
  oy = sy + _transform.ty;
}

void Graphics2D_imgui::translate(float dx, float dy) {
  _transform.tx += dx * _transform.sx;
  _transform.ty += dy * _transform.sy;
}

void Graphics2D_imgui::scale(float sx, float sy) {
  _transform.sx *= sx;
  _transform.sy *= sy;
}

void Graphics2D_imgui::rotate(float angle) {
  _transform.rotation += angle;
}

void Graphics2D_imgui::rotate(float angle, float px, float py) {
  translate(px, py);
  rotate(angle);
  translate(-px, -py);
}

void Graphics2D_imgui::reset() {
  _transform = TransformState();
}

void Graphics2D_imgui::drawChar(wchar_t c, float x, float y) {
  float tx, ty;
  transformPoint(x, y, tx, ty);

  std::string utf8 = wcharToUtf8(c);
  float fontSize = _font ? static_cast<const Font_imgui *>(_font)->getSize() : ImGui::GetFontSize();
  fontSize *= std::abs(_transform.sy);

  _drawList->AddText(nullptr, fontSize, ImVec2(tx, ty - fontSize * 0.8f), _imColor, utf8.c_str());
}

void Graphics2D_imgui::drawText(const std::wstring &text, float x, float y) {
  float tx, ty;
  transformPoint(x, y, tx, ty);

  std::string utf8 = wideToUtf8(text);
  float fontSize = _font ? static_cast<const Font_imgui *>(_font)->getSize() : ImGui::GetFontSize();
  fontSize *= std::abs(_transform.sy);

  _drawList->AddText(nullptr, fontSize, ImVec2(tx, ty - fontSize * 0.8f), _imColor, utf8.c_str());
}

void Graphics2D_imgui::drawLine(float x1, float y1, float x2, float y2) {
  float tx1, ty1, tx2, ty2;
  transformPoint(x1, y1, tx1, ty1);
  transformPoint(x2, y2, tx2, ty2);

  _drawList->AddLine(ImVec2(tx1, ty1), ImVec2(tx2, ty2), _imColor, _stroke.lineWidth);
}

void Graphics2D_imgui::drawRect(float x, float y, float w, float h) {
  float tx1, ty1, tx2, ty2;
  transformPoint(x, y, tx1, ty1);
  transformPoint(x + w, y + h, tx2, ty2);

  _drawList->AddRect(ImVec2(tx1, ty1), ImVec2(tx2, ty2), _imColor, 0.0f, 0, _stroke.lineWidth);
}

void Graphics2D_imgui::fillRect(float x, float y, float w, float h) {
  float tx1, ty1, tx2, ty2;
  transformPoint(x, y, tx1, ty1);
  transformPoint(x + w, y + h, tx2, ty2);

  _drawList->AddRectFilled(ImVec2(tx1, ty1), ImVec2(tx2, ty2), _imColor);
}

void Graphics2D_imgui::drawRoundRect(float x, float y, float w, float h, float rx, float ry) {
  float tx1, ty1, tx2, ty2;
  transformPoint(x, y, tx1, ty1);
  transformPoint(x + w, y + h, tx2, ty2);

  float rounding = std::min(rx, ry) * std::abs(_transform.sx);
  _drawList->AddRect(ImVec2(tx1, ty1), ImVec2(tx2, ty2), _imColor, rounding, 0, _stroke.lineWidth);
}

void Graphics2D_imgui::fillRoundRect(float x, float y, float w, float h, float rx, float ry) {
  float tx1, ty1, tx2, ty2;
  transformPoint(x, y, tx1, ty1);
  transformPoint(x + w, y + h, tx2, ty2);

  float rounding = std::min(rx, ry) * std::abs(_transform.sx);
  _drawList->AddRectFilled(ImVec2(tx1, ty1), ImVec2(tx2, ty2), _imColor, rounding);
}

} // namespace tex

#endif // BUILD_IMGUI
