#if defined(BUILD_IMGUI)

#ifndef GRAPHIC_IMGUI_H_INCLUDED
#define GRAPHIC_IMGUI_H_INCLUDED

#include "graphic/graphic.h"
#include "imgui.h"

#include <stack>

namespace tex {

// ============================================================================
// Font implementation for ImGui
// ============================================================================
class Font_imgui : public Font {
private:
  float _size;
  int _style;
  ImFont *_imfont;

public:
  Font_imgui(const std::string &file, float size);
  Font_imgui(const std::string &name, int style, float size);

  float getSize() const override { return _size; }
  int getStyle() const { return _style; }
  ImFont *getImFont() const { return _imfont; }

  sptr<Font> deriveFont(int style) const override;
  bool operator==(const Font &f) const override;
  bool operator!=(const Font &f) const override;

  virtual ~Font_imgui() = default;
};

// ============================================================================
// TextLayout implementation for ImGui
// ============================================================================
class TextLayout_imgui : public TextLayout {
private:
  std::wstring _text;
  sptr<Font_imgui> _font;

public:
  TextLayout_imgui(const std::wstring &src, const sptr<Font_imgui> &font);

  void getBounds(Rect &bounds) override;
  void draw(Graphics2D &g2, float x, float y) override;
};

// ============================================================================
// Transform state for matrix operations
// ============================================================================
struct TransformState {
  float tx = 0.0f, ty = 0.0f; // translation
  float sx = 1.0f, sy = 1.0f; // scale
  float rotation = 0.0f;      // rotation in radians
};

// ============================================================================
// Graphics2D implementation for ImGui
// ============================================================================
class Graphics2D_imgui : public Graphics2D {
private:
  ImDrawList *_drawList;
  color _color = black;
  ImU32 _imColor = IM_COL32(0, 0, 0, 255);
  const Font *_font = nullptr;
  Stroke _stroke;

  // Transform stack
  std::stack<TransformState> _transformStack;
  TransformState _transform;

  // Helper: apply transform to point
  void transformPoint(float x, float y, float &ox, float &oy) const;

  // Helper: convert tex::color to ImU32
  static ImU32 toImColor(color c);

public:
  Graphics2D_imgui(ImDrawList *drawList);
  virtual ~Graphics2D_imgui() = default;

  void setColor(color c) override;
  color getColor() const override { return _color; }

  void setStroke(const Stroke &s) override { _stroke = s; }
  const Stroke &getStroke() const override { return _stroke; }
  void setStrokeWidth(float w) override { _stroke.lineWidth = w; }

  const Font *getFont() const override { return _font; }
  void setFont(const Font *font) override { _font = font; }

  void translate(float dx, float dy) override;
  void scale(float sx, float sy) override;
  void rotate(float angle) override;
  void rotate(float angle, float px, float py) override;
  void reset() override;

  float sx() const override { return _transform.sx; }
  float sy() const override { return _transform.sy; }

  void drawChar(wchar_t c, float x, float y) override;
  void drawText(const std::wstring &c, float x, float y) override;
  void drawLine(float x1, float y1, float x2, float y2) override;
  void drawRect(float x, float y, float w, float h) override;
  void fillRect(float x, float y, float w, float h) override;
  void drawRoundRect(float x, float y, float w, float h, float rx, float ry) override;
  void fillRoundRect(float x, float y, float w, float h, float rx, float ry) override;
};

} // namespace tex

#endif // GRAPHIC_IMGUI_H_INCLUDED
#endif // BUILD_IMGUI
