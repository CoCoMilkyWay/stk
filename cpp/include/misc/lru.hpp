#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

// 最近用留存 (LRU): 键 → 不可变值 (shared_ptr<const V>; 空 = 记 "没有"), 各带代价; 新 → 旧排. 两种用法对仗:
//   条数封顶 get (命中沿用, 否则现建; 留最近 cap 个, 不看外面持不持有: 读出的序列)
//   代价封顶 put + trim (逐出只逐外面无人持有的, 直到总代价不超: 因子引擎的宿主平面)
// 业务无关.
namespace misc {

template <class Key, class V>
class Lru {
public:
  using Ptr = std::shared_ptr<const V>;

  // 键 k 的值: 留着 → 提到最新; 否则 make() 现建收为最新 (代价 1), 逐出最旧的直到只剩 cap 个
  template <class Make>
  Ptr get(const Key &k, Make &&make, size_t cap) {
    assert(cap >= 1);
    if (const Ptr *p = find(k))
      return *p;
    put(k, make(), 1.0);
    while (items_.size() > cap)
      cost_ -= items_.back().cost, items_.pop_back();
    return items_.front().v;
  }
  // 留着 → 提到最新; 不在 → nullptr
  const Ptr *find(const Key &k) {
    for (size_t i = 0; i < items_.size(); ++i)
      if (items_[i].k == k) {
        std::rotate(items_.begin(), items_.begin() + static_cast<std::ptrdiff_t>(i), items_.begin() + static_cast<std::ptrdiff_t>(i + 1));
        return &items_.front().v;
      }
    return nullptr;
  }
  // 收为最新 (键须不在)
  void put(const Key &k, Ptr v, double cost) {
    assert(std::ranges::none_of(items_, [&](const Item &x) { return x.k == k; }) && "键已在");
    items_.insert(items_.begin(), Item{k, std::move(v), cost});
    cost_ += cost;
  }
  // 自最旧起逐出外面无人持有的, 直到 over(总代价) 为假
  template <class Over>
  void trim(Over &&over) {
    for (size_t i = items_.size(); i-- > 0 && over(cost_);)
      if (items_[i].v.use_count() <= 1)
        cost_ -= items_[i].cost, items_.erase(items_.begin() + static_cast<std::ptrdiff_t>(i));
  }
  // 新 → 旧 f(键, 值)
  template <class F>
  void each(F &&f) const {
    for (const Item &x : items_)
      f(x.k, x.v);
  }
  double cost() const { return cost_; }
  void clear() { items_.clear(), cost_ = 0.0; }

private:
  struct Item {
    Key k;
    Ptr v;
    double cost;
  };
  std::vector<Item> items_; // 新 → 旧
  double cost_ = 0.0;
};

} // namespace misc
