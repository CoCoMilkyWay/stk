#pragma once

// Color value type - simple RGBA wrapper for type safety
struct Color {
  float r = 1.0f, g = 1.0f, b = 1.0f, a = 1.0f;

  Color() = default;
  Color(float r_, float g_, float b_, float a_ = 1.0f) : r(r_), g(g_), b(b_), a(a_) {}

  static Color White() { return {1.0f, 1.0f, 1.0f, 1.0f}; }
  static Color Green() { return {0.0f, 1.0f, 0.0f, 1.0f}; }
  static Color Red() { return {1.0f, 0.0f, 0.0f, 1.0f}; }
  static Color Yellow() { return {1.0f, 1.0f, 0.0f, 1.0f}; }
  static Color Blue() { return {0.3f, 0.7f, 1.0f, 1.0f}; }
  static Color Gray() { return {0.5f, 0.5f, 0.5f, 1.0f}; }
};
