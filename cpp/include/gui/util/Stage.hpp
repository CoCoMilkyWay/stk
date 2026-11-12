// 键控缓存 Stage<Key, Val>: 键 = 本级读的全部东西 (上级以其 gen 代表), 键变才重算, gen 计重算次数 ⇒ 级联的下级把上级 gen 放进键即自动失效
#pragma once

#include <cstdint>

namespace GUI {

template <class Key, class Val>
struct Stage {
  Key key{};
  uint64_t gen = 0; // 本级重算次数 (0 = 未算)
  Val val{};
  // 首次 / 键变 → f(val) 重算, gen +1
  template <class F>
  void update(const Key &k, F &&f) {
    if (gen > 0 && k == key)
      return;
    key = k;
    f(val);
    ++gen;
  }
};

} // namespace GUI
