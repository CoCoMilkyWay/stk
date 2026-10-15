#include "fundamental/Scan.hpp"

#include <thread>

namespace fundamental {

namespace pq = misc::pq;

Ids::Ids(const Catalog &cat, const Spec &s) : miss_(Miss::Assert) {
  assert(s.shape == Shape::Meta && "Ids: id 空间只取自 Meta 表");
  const Shard *sh = cat.find(s.name, 0);
  assert(sh && "Ids: _meta 分片缺失 (基本面未同步?)");
  const std::string key(key_col(s));
  const pq::TableView v(read(*sh, {key}));
  const pq::Col k = v.col(key);
  for (std::int64_t i = 0; i < v.rows(); ++i)
    add(k.str(i));
}

std::uint32_t Ids::add(std::string_view code) {
  assert(!code.empty() && "Ids: 代码为空");
  const auto id = size();
  const auto [it, inserted] = ids_.try_emplace(std::string(code), id);
  assert(inserted && "Ids: 代码重复");
  codes_.push_back(&it->first);
  return id;
}

std::uint32_t Ids::find(std::string_view code) const {
  const auto it = ids_.find(code);
  if (it != ids_.end())
    return it->second;
  assert(miss_ == Miss::Skip && "Ids: 代码不在 id 空间内");
  return kNone;
}

Rows Scratch::open(const Shard &sh, const Spec &s, const std::vector<std::string> &cols, const Ids &ids,
                   Snap snap) {
  const std::string key(key_col(s));
  std::vector<std::string> need = cols;
  for (const std::string &c : {key, s.vd})
    if (!c.empty() && std::find(need.begin(), need.end(), c) == need.end())
      need.push_back(c);
  t_ = std::make_unique<pq::TableView>(read(sh, need, {key}));
  Rows r;
  r.t_ = t_.get();
  r.n_ = t_->rows();
  if (r.n_ == 0)
    return r;
  key_ = t_->dict_col(key);
  dict_ids_.resize(static_cast<std::size_t>(key_.dict_size()));
  for (std::int32_t k = 0; k < key_.dict_size(); ++k)
    dict_ids_[static_cast<std::size_t>(k)] = ids.find(key_.dict_value(k));
  if (s.vd.empty())
    dates_.assign(static_cast<std::size_t>(r.n_), 0);
  else
    t_->col(s.vd).yyyymmdd_all(dates_);
  r.key_ = key_.indices();
  r.dict_ids_ = dict_ids_.data();
  r.dates_ = dates_.data();
  if (snap == Snap::Day) {
    r.day_ = *std::max_element(dates_.begin(), dates_.end());
    assert(r.day_ > 0 && "Scratch: Snap::Day 的分片 vd 全 null");
  }
  return r;
}

Scan::Scan(const Catalog &cat, const Ids &ids, std::size_t threads) : cat_(cat), ids_(ids), threads_(threads) {
  assert(threads_ > 0);
}

void Scan::run(std::atomic<std::size_t> &done) {
  std::sort(jobs_.begin(), jobs_.end(), [](const Job &a, const Job &b) { return a.bytes > b.bytes; });
  std::atomic<std::size_t> next{0};
  auto worker = [this, &next, &done] {
    Scratch x;
    for (std::size_t i; (i = next.fetch_add(1, std::memory_order_relaxed)) < jobs_.size();)
      jobs_[i].body(x, done);
  };
  std::vector<std::thread> ts;
  for (std::size_t t = 0; t < std::min(threads_, jobs_.size()); ++t)
    ts.emplace_back(worker);
  for (std::thread &t : ts)
    t.join();
  jobs_.clear();
}

void snapshot(const Catalog &cat, const Ids &ids, const Spec &s, const std::vector<std::string> &cols,
              const std::function<void(const Rows &)> &fn) {
  assert(s.shape == Shape::Month && "snapshot: 只读 Month 表");
  const std::vector<Shard> &shards = cat.shards(s.name);
  Scratch x;
  for (auto it = shards.rbegin(); it != shards.rend(); ++it) {
    const Rows r = x.open(*it, s, cols, ids, Snap::Day);
    if (r.size() == 0)
      continue; // 0 行月 (拉过为空) → 往前找
    fn(r);
    return;
  }
}

void meta(const Catalog &cat, const Ids &ids, const Spec &s, const std::vector<std::string> &cols,
          const std::function<void(const Rows &)> &fn) {
  assert(s.shape == Shape::Meta && "meta: 只读 Meta 表");
  const Shard *sh = cat.find(s.name, 0);
  assert(sh && "meta: _meta 分片缺失 (基本面未同步?)");
  Scratch x;
  fn(x.open(*sh, s, cols, ids));
}

std::vector<Ymd> calendar(const Catalog &cat, Ymd today) {
  std::vector<Ymd> out, d;
  for (const Shard &sh : cat.shards("all_trading_days")) {
    pq::TableView v(read(sh, {"date", "market_code"}));
    if (v.rows() == 0)
      continue;
    v.col("date").yyyymmdd_all(d);
    const pq::Col mc = v.col("market_code");
    for (std::int64_t i = 0; i < v.rows(); ++i)
      if (mc.str(i) == "CN" && d[static_cast<std::size_t>(i)] > 0 && d[static_cast<std::size_t>(i)] <= today)
        out.push_back(d[static_cast<std::size_t>(i)]); // 排程提前含未来日, 截到 today
  }
  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  assert(!out.empty() && "all_trading_days 无 CN 行 (基本面未同步?)");
  return out;
}

} // namespace fundamental
