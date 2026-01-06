#if defined(BUILD_IMGUI)

#include "graphic_imgui.h"
#include <cassert>
#include <cstdint>
#include <fstream>

namespace tex {

// ============================================================================
// Static member initialization
// ============================================================================
std::map<std::string, ImFont*> Font_imgui::_fontCache;
std::vector<Font_imgui::PendingFont> Font_imgui::_pendingFonts;
bool Font_imgui::_needRebuildAtlas = false;

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

// Glyph range for TeX fonts (covers 0x00-0xFF which includes most TeX symbols)
static const ImWchar kTexGlyphRanges[] = {
  0x0020, 0x00FF,  // Basic Latin + Latin Supplement
  0x0100, 0x017F,  // Latin Extended-A
  0x0180, 0x024F,  // Latin Extended-B
  0x2200, 0x22FF,  // Mathematical Operators
  0x2A00, 0x2AFF,  // Supplemental Mathematical Operators
  0,
};

void Font_imgui::loadFont(const std::string &file) {
  _fontPath = file;
  
  // Check if already in cache
  auto it = _fontCache.find(file);
  if (it != _fontCache.end()) {
    _imfont = it->second;
    return;
  }
  
  // Check if file exists
  std::ifstream f(file);
  if (!f.good()) {
    // Font file not found, use default
    _imfont = ImGui::GetFont();
    return;
  }
  f.close();
  
  // Add to pending fonts for atlas rebuild
  _pendingFonts.push_back({file, _size});
  _needRebuildAtlas = true;
  
  // For now use default font, will be updated after atlas rebuild
  _imfont = ImGui::GetFont();
}

void Font_imgui::rebuildFontAtlasIfNeeded() {
  if (!_needRebuildAtlas || _pendingFonts.empty()) {
    return;
  }
  
  ImGuiIO &io = ImGui::GetIO();
  
  // Add pending fonts to atlas
  for (const auto &pending : _pendingFonts) {
    // Skip if already cached
    if (_fontCache.find(pending.path) != _fontCache.end()) {
      continue;
    }
    
    ImFontConfig config;
    config.MergeMode = false;
    config.PixelSnapH = false;
    config.OversampleH = 2;
    config.OversampleV = 2;
    
    // Load at a reasonable base size - actual scaling is done via Graphics2D transform
    // Use a larger size (e.g., 48px) to maintain quality when scaled up
    constexpr float kBaseFontSize = 48.0f;
    
    ImFont *font = io.Fonts->AddFontFromFileTTF(
      pending.path.c_str(), 
      kBaseFontSize, 
      &config, 
      kTexGlyphRanges
    );
    
    if (font) {
      _fontCache[pending.path] = font;
    }
  }
  
  // Rebuild atlas
  io.Fonts->Build();
  
  _pendingFonts.clear();
  _needRebuildAtlas = false;
}

ImFont *Font_imgui::getImFont() const {
  // Check cache first - font might have been loaded after this instance was created
  if (!_fontPath.empty()) {
    auto it = _fontCache.find(_fontPath);
    if (it != _fontCache.end()) {
      return it->second;
    }
  }
  return _imfont ? _imfont : ImGui::GetFont();
}

Font_imgui::Font_imgui(const std::string &file, float size)
    : _size(size), _style(PLAIN), _imfont(nullptr) {
  if (!file.empty()) {
    loadFont(file);
  } else {
    _imfont = ImGui::GetFont();
  }
}

Font_imgui::Font_imgui(const std::string &name, int style, float size)
    : _size(size), _style(style), _fontPath(name), _imfont(nullptr) {
  _imfont = ImGui::GetFont();
}

sptr<Font> Font_imgui::deriveFont(int style) const {
  auto font = std::make_shared<Font_imgui>(_fontPath, style, _size);
  font->_imfont = _imfont;  // Share the same ImFont
  return font;
}

bool Font_imgui::operator==(const Font &f) const {
  const Font_imgui *other = dynamic_cast<const Font_imgui *>(&f);
  if (!other)
    return false;
  return _size == other->_size && _style == other->_style && _fontPath == other->_fontPath;
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
  
  // Get baked font metrics for the requested size
  ImFontBaked *baked = font->GetFontBaked(fontSize);
  float ascent = baked->Ascent;
  float descent = -baked->Descent;  // Descent is negative in ImGui
  
  bounds.x = 0;
  bounds.y = -ascent;  // Top of text relative to baseline
  bounds.w = size.x;
  bounds.h = ascent + descent;  // Total height
}

void TextLayout_imgui::draw(Graphics2D &g2, float x, float y) {
  // Use our own font, not the current g2 font (which might be a TeX font without CJK glyphs)
  const Font *oldFont = g2.getFont();
  g2.setFont(_font.get());
  g2.drawText(_text, x, y);
  g2.setFont(oldFont);
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
  // Apply current transformation matrix to point
  // Matrix is: [sx  0  tx]   [x]
  //            [0  sy  ty] * [y]
  //            [0   0   1]   [1]
  ox = x * _transform.sx + _transform.tx;
  oy = y * _transform.sy + _transform.ty;
}

void Graphics2D_imgui::translate(float dx, float dy) {
  // In matrix terms: T(dx,dy) * CurrentMatrix
  // This means: new_point = T * M * old_point
  // So translation is applied BEFORE current transform
  // tx' = tx + dx * sx, ty' = ty + dy * sy
  _transform.tx += dx * _transform.sx;
  _transform.ty += dy * _transform.sy;
}

void Graphics2D_imgui::scale(float sx, float sy) {
  // In matrix terms: S(sx,sy) * CurrentMatrix
  // This means scale is applied BEFORE current transform
  // Also need to scale the translation part
  _transform.sx *= sx;
  _transform.sy *= sy;
  // Note: translation stays the same relative to output space
}

void Graphics2D_imgui::rotate(float angle) {
  // Rotation is not commonly used in TeX, keeping simple
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

  const Font_imgui *fontImgui = _font ? static_cast<const Font_imgui *>(_font) : nullptr;
  ImFont *imfont = fontImgui ? fontImgui->getImFont() : ImGui::GetFont();
  float fontSize = fontImgui ? fontImgui->getSize() : ImGui::GetFontSize();
  fontSize *= std::abs(_transform.sy);

  // Check if this is a TeX extension font that needs special handling
  bool isExtensionFont = false;
  if (fontImgui) {
    const std::string& path = fontImgui->getFontPath();
    isExtensionFont = (path.find("cmex10") != std::string::npos || 
                       path.find("moustache") != std::string::npos);
  }
  
  if (isExtensionFont) {
    // cmex10 等字体的 glyph 在 TTF 中按较小比例存储，需要额外放大
    // scale = fontSize * 0.8 / baked->Ascent（补偿异常小的 ascent）
    // lineTop = ty - fontSize * 0.8（ascent 项被抵消）
    constexpr float kLoadedFontSize = 48.0f;
    ImFontBaked *baked = imfont->GetFontBaked(kLoadedFontSize);
    ImFontGlyph *glyph = baked->FindGlyph((ImWchar)c);
    if (!glyph || !glyph->Visible) return;
    
    float scale = fontSize * 0.8f / baked->Ascent;
    float lineTop = ty - fontSize * 0.8f;
    
    _drawList->PrimReserve(6, 4);
    _drawList->PrimRectUV(
      ImVec2(tx + glyph->X0 * scale, lineTop + glyph->Y0 * scale),
      ImVec2(tx + glyph->X1 * scale, lineTop + glyph->Y1 * scale),
      ImVec2(glyph->U0, glyph->V0), ImVec2(glyph->U1, glyph->V1),
      _imColor);
  } else {
    // Normal fonts - use standard RenderChar
    ImFontBaked *baked = imfont->GetFontBaked(fontSize);
    ImVec2 pos(tx, ty - baked->Ascent);
    imfont->RenderChar(_drawList, fontSize, pos, _imColor, (ImWchar)c, nullptr);
  }
}

void Graphics2D_imgui::drawText(const std::wstring &text, float x, float y) {
  float tx, ty;
  transformPoint(x, y, tx, ty);

  const Font_imgui *fontImgui = _font ? static_cast<const Font_imgui *>(_font) : nullptr;
  ImFont *imfont = fontImgui ? fontImgui->getImFont() : ImGui::GetFont();
  float fontSize = fontImgui ? fontImgui->getSize() : ImGui::GetFontSize();
  fontSize *= std::abs(_transform.sy);

  // Check if this is a TeX extension font
  bool isExtensionFont = false;
  if (fontImgui) {
    const std::string& path = fontImgui->getFontPath();
    isExtensionFont = (path.find("cmex10") != std::string::npos || 
                       path.find("moustache") != std::string::npos);
  }

  if (isExtensionFont) {
    // cmex10 等字体需要额外放大（见 drawChar 注释）
    constexpr float kLoadedFontSize = 48.0f;
    ImFontBaked *baked = imfont->GetFontBaked(kLoadedFontSize);
    float scale = fontSize * 0.8f / baked->Ascent;
    float lineTop = ty - fontSize * 0.8f;
    float xPos = tx;
    
    for (wchar_t c : text) {
      ImFontGlyph *glyph = baked->FindGlyph((ImWchar)c);
      if (!glyph) continue;
      if (glyph->Visible) {
        _drawList->PrimReserve(6, 4);
        _drawList->PrimRectUV(
          ImVec2(xPos + glyph->X0 * scale, lineTop + glyph->Y0 * scale),
          ImVec2(xPos + glyph->X1 * scale, lineTop + glyph->Y1 * scale),
          ImVec2(glyph->U0, glyph->V0), ImVec2(glyph->U1, glyph->V1),
          _imColor);
      }
      xPos += glyph->AdvanceX * scale;
    }
  } else {
    // Normal fonts - use standard RenderChar
    ImFontBaked *baked = imfont->GetFontBaked(fontSize);
    float xPos = tx;
    float lineTop = ty - baked->Ascent;
    
    for (wchar_t c : text) {
      ImFontGlyph *glyph = baked->FindGlyph((ImWchar)c);
      if (!glyph) continue;
      
      imfont->RenderChar(_drawList, fontSize, ImVec2(xPos, lineTop), _imColor, (ImWchar)c, nullptr);
      xPos += glyph->AdvanceX;
    }
  }
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
