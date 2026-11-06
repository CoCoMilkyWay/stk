#pragma once

// 最小 JSON: 读库清单 lib.json / 字段表 features.json (数 = double; 字符串转义只认 \" \\ \/ \b \f \n \r \t \uXXXX (BMP))
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

struct Json {
  enum Kind { kNull,
              kBool,
              kNum,
              kStr,
              kArr,
              kObj };
  Kind kind = kNull;
  double num = 0;
  std::string str;
  std::vector<Json> arr;         // 数组元素 / 对象的值
  std::vector<std::string> keys; // 对象的键 (与 arr 同序)

  const Json &operator[](std::string_view k) const {
    assert(kind == kObj && "json: 不是对象");
    for (size_t i = 0; i < keys.size(); ++i)
      if (keys[i] == k)
        return arr[i];
    assert(false && "json: 缺键");
    return *this;
  }
};

namespace json {

struct Parser {
  std::string_view s;
  size_t i = 0;

  void ws() {
    while (i < s.size() && (s[i] == ' ' || s[i] == '\n' || s[i] == '\r' || s[i] == '\t'))
      ++i;
  }
  char peek() {
    ws();
    assert(i < s.size() && "json: 意外结尾");
    return s[i];
  }
  void eat(char c) {
    assert(peek() == c && "json: 语法错");
    ++i;
  }
  std::string text() {
    eat('"');
    std::string o;
    for (; s[i] != '"'; ++i) {
      if (s[i] != '\\') {
        o += s[i];
        continue;
      }
      const char e = s[++i];
      if (e == 'u') {
        const unsigned u = static_cast<unsigned>(std::strtoul(std::string(s.substr(i + 1, 4)).c_str(), nullptr, 16));
        i += 4;
        if (u < 0x80)
          o += static_cast<char>(u);
        else if (u < 0x800)
          o += static_cast<char>(0xC0 | u >> 6), o += static_cast<char>(0x80 | (u & 0x3F));
        else
          o += static_cast<char>(0xE0 | u >> 12), o += static_cast<char>(0x80 | (u >> 6 & 0x3F)), o += static_cast<char>(0x80 | (u & 0x3F));
        continue;
      }
      constexpr std::string_view from = "\"\\/bfnrt", to = "\"\\/\b\f\n\r\t";
      const size_t k = from.find(e);
      assert(k != std::string_view::npos && "json: 未知转义");
      o += to[k];
    }
    ++i;
    return o;
  }
  Json value() {
    Json v;
    const char c = peek();
    if (c == '{') {
      v.kind = Json::kObj;
      eat('{');
      if (peek() == '}')
        return ++i, v;
      for (;;) {
        v.keys.push_back(text());
        eat(':');
        v.arr.push_back(value());
        if (peek() == '}')
          return ++i, v;
        eat(',');
      }
    }
    if (c == '[') {
      v.kind = Json::kArr;
      eat('[');
      if (peek() == ']')
        return ++i, v;
      for (;;) {
        v.arr.push_back(value());
        if (peek() == ']')
          return ++i, v;
        eat(',');
      }
    }
    if (c == '"') {
      v.kind = Json::kStr;
      v.str = text();
      return v;
    }
    for (const auto &[w, k, b] : {std::tuple{"true", Json::kBool, 1.0}, std::tuple{"false", Json::kBool, 0.0}, std::tuple{"null", Json::kNull, 0.0}})
      if (s.substr(i).starts_with(w)) {
        i += std::string_view(w).size();
        v.kind = k, v.num = b;
        return v;
      }
    char *end = nullptr;
    const std::string rest(s.substr(i, 64));
    v.kind = Json::kNum;
    v.num = std::strtod(rest.c_str(), &end);
    assert(end != rest.c_str() && "json: 不是值");
    i += static_cast<size_t>(end - rest.c_str());
    return v;
  }
};

inline Json read(const std::string &path) {
  std::ifstream f(path, std::ios::binary);
  assert(f && "json: 打不开");
  const std::string blob((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
  Parser p{blob};
  Json v = p.value();
  p.ws();
  assert(p.i == blob.size() && "json: 尾部多余");
  return v;
}

} // namespace json
