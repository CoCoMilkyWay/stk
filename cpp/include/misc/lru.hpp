#pragma once

#include <cassert>
#include <cstddef>
#include <list>
#include <map>
#include <memory>
#include <utility>

// 最近用留存 (LRU, O(log N) 查找 + O(1) 提新 / 淘汰): 键 → 不可变值 (shared_ptr<const V>; 空 = 记 "没有"), 各带代价; 新 → 旧排. 两种用法对仗:
//   条数封顶 get (命中沿用, 否则现建; 留最近 cap 个, 不看外面持不持有: 读出的序列 / 参数账)
//   代价封顶 find + put + trim (put 键已在则原地换新提首; trim 逐出只逐外面无人持有的, 直到总代价不超: 宿主平面 / 组合账)
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
      pop_back();
    return items_.front().v;
  }
  // 留着 → 提到最新; 不在 → nullptr
  const Ptr *find(const Key &k) {
    const auto it = index_.find(k);
    if (it == index_.end())
      return nullptr;
    items_.splice(items_.begin(), items_, it->second);
    return &items_.front().v;
  }
  // 收为最新 (键已在则替换值与代价并提首)
  void put(const Key &k, Ptr v, double cost = 1.0) {
    if (const auto it = index_.find(k); it != index_.end()) {
      cost_ += cost - it->second->cost;
      it->second->v = std::move(v);
      it->second->cost = cost;
      items_.splice(items_.begin(), items_, it->second);
      return;
    }
    items_.push_front(Item{k, std::move(v), cost});
    index_.emplace(items_.front().k, items_.begin());
    cost_ += cost;
  }
  // 自最旧起逐出外面无人持有的, 直到 over(总代价) 为假
  template <class Over>
  void trim(Over &&over) {
    for (auto it = items_.end(); it != items_.begin() && over(cost_);) {
      --it;
      if (it->v.use_count() <= 1) {
        cost_ -= it->cost;
        index_.erase(it->k);
        it = items_.erase(it);
      }
    }
  }
  // 新 → 旧 f(键, 值)
  template <class F>
  void each(F &&f) const {
    for (const Item &x : items_)
      f(x.k, x.v);
  }
  double cost() const { return cost_; }
  void clear() { items_.clear(), index_.clear(), cost_ = 0.0; }

private:
  struct Item {
    Key k;
    Ptr v;
    double cost;
  };
  void pop_back() {
    cost_ -= items_.back().cost;
    index_.erase(items_.back().k);
    items_.pop_back();
  }
  std::list<Item> items_; // 新 → 旧
  std::map<Key, typename std::list<Item>::iterator> index_;
  double cost_ = 0.0;
};

} // namespace misc
