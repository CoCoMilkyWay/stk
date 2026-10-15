#include "fundamental/Scan.hpp"

#include <thread>

namespace fundamental {

namespace pq = misc::pq;

std::uint32_t Ids::add(std::string_view code) {
  const auto id = size();
  const bool inserted = ids_.try_emplace(std::string(code), id).second;
  assert(inserted && "Ids: 代码重复");
  return id;
}

std::uint32_t Ids::find(std::string_view code) const {
  const auto it = ids_.find(code);
  if (it != ids_.end())
    return it->second;
  assert(miss_ == Miss::Skip && "Ids: 代码不在 id 空间内");
  return kNone;
}

struct Scan::Scratch {
  std::unique_ptr<pq::TableView> t;
  pq::DictCol key;
  std::vector<std::uint32_t> dict_ids;
  std::vector<Ymd> dates;
};

Scan::Scan(const Catalog &cat, const Ids &ids, std::size_t threads) : cat_(cat), ids_(ids), threads_(threads) {
  assert(threads_ > 0);
}

Rows Scan::open(const Shard &sh, const Spec &s, const std::vector<std::string> &cols, Scratch &x) const {
  const std::string_view key = key_col(s);
  x.t = std::make_unique<pq::TableView>(pq::read_table(sh.path, cols, {std::string(key)}));
  Rows r;
  r.t_ = x.t.get();
  r.n_ = x.t->rows();
  if (r.n_ == 0)
    return r;
  x.key = x.t->dict_col(key);
  x.dict_ids.resize(static_cast<std::size_t>(x.key.dict_size()));
  for (std::int32_t k = 0; k < x.key.dict_size(); ++k)
    x.dict_ids[static_cast<std::size_t>(k)] = ids_.find(x.key.dict_value(k));
  x.t->col(s.vd).yyyymmdd_all(x.dates);
  r.key_ = x.key.indices();
  r.dict_ids_ = x.dict_ids.data();
  r.dates_ = x.dates.data();
  return r;
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

pq::TableView meta(const Catalog &cat, std::string_view name, const std::vector<std::string> &cols) {
  assert(spec(name).shape == Shape::Meta && "meta: 只读 Meta 表");
  const Shard *sh = cat.find(name, 0);
  assert(sh && "meta: _meta 分片缺失 (基本面未同步?)");
  return pq::TableView(pq::read_table(sh->path, cols));
}

std::vector<Ymd> calendar(const Catalog &cat, Ymd today) {
  std::vector<Ymd> out, d;
  for (const Shard &sh : cat.shards("all_trading_days")) {
    pq::TableView v(pq::read_table(sh.path, {"date", "market_code"}));
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
