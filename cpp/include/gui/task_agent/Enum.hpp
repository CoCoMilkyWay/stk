#pragma once

// =============================================================================
// Enum — 枚举的表 (唯一真相): 枚举值 = 表的下标; 一个枚举一行 AGENT_ROWS 挂上它的表
// =============================================================================
//   def(e) 取行 / kNum<E> 个数 / FindId<E>(id) / IdOf<E>(id) 按行的 id 反查
//   行带 id 的枚举 (Named), JSON 里一律写 id: 会话存档 / 全局记录同一种写法
// =============================================================================

#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

namespace GUI::Agent {

// 挂了表的枚举: enum_rows(E) (ADL, 与枚举同命名空间, AGENT_ROWS 生成) 给出它的表
template <class E>
concept Tabled = std::is_enum_v<E> && requires { enum_rows(E{}); };
template <class E>
concept Named = Tabled<E> && requires { std::string_view(enum_rows(E{})[0].id); };

template <Tabled E>
inline constexpr size_t kNum = std::size(enum_rows(E{}));

template <Tabled E>
constexpr const auto &def(E e) { return enum_rows(E{})[static_cast<size_t>(e)]; }

template <Named E>
constexpr std::optional<E> FindId(std::string_view id) {
  const auto &t = enum_rows(E{});
  const auto it = std::find_if(std::begin(t), std::end(t), [&](const auto &r) { return id == r.id; });
  return it == std::end(t) ? std::nullopt : std::optional<E>(static_cast<E>(it - std::begin(t)));
}

template <Named E>
E IdOf(std::string_view id) {
  const std::optional<E> e = FindId<E>(id);
  assert(e.has_value() && "表里没有这个 id");
  return *e;
}

} // namespace GUI::Agent

// 枚举 E 挂上表 TABLE (行序 = 枚举序; LAST = 末个枚举值, 核对行数); 写在 E 所在的命名空间
#define AGENT_ROWS(E, TABLE, LAST)                     \
  constexpr const auto &enum_rows(E) { return TABLE; } \
  static_assert(std::size(TABLE) == static_cast<size_t>(E::LAST) + 1, #TABLE " 的行与 " #E " 一一对应")

NLOHMANN_JSON_NAMESPACE_BEGIN
template <GUI::Agent::Named E>
struct adl_serializer<E> {
  template <class J>
  static void to_json(J &j, E e) { j = GUI::Agent::def(e).id; }
  template <class J>
  static void from_json(const J &j, E &e) { e = GUI::Agent::IdOf<E>(j.template get<std::string>()); }
};
NLOHMANN_JSON_NAMESPACE_END
