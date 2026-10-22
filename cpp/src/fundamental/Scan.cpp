#include "fundamental/Scan.hpp"
#include "misc/exec.hpp"

#include <iostream>

namespace fundamental {

namespace pq = misc::pq;

namespace {

void line(const char *level, const Spec &s, At at, std::string_view what) {
  std::cout << "[fund] " << level << " " << s.name;
  if (at.d > 0)
    std::cout << " " << at.d;
  if (!at.code.empty())
    std::cout << " " << at.code;
  std::cout << " " << what << std::endl;
}

} // namespace

void error(const Spec &s, At at, std::string_view what) {
  assert(from<Bq>(s) && "fund: 读侧检查只查 Bq 表");
  line("error", s, at, what);
  assert(false && "fund: 基本面读侧检查失败 (见上一行 [fund] error)");
}

const Shard &need(const Catalog &cat, const Spec &s) {
  assert(s.shape == Shape::Meta && "need: 只取 Meta 分片");
  const Shard *sh = cat.find(s.name, 0);
  if (!sh)
    error(s, {}, "_meta 分片缺失 (基本面未同步?)");
  return *sh;
}

Ids::Ids(const Catalog &cat, const Spec &s) {
  const std::string key(key_col(s));
  const pq::TableView v(read(need(cat, s), {key}));
  expect(v.rows() > 0, s, {}, "0 行");
  const pq::Col k = v.col(key);
  for (std::int64_t i = 0; i < v.rows(); ++i) {
    const std::string_view code = k.str(i);
    expect(code.find('.') != std::string_view::npos, s, {.code = code}, "代码不是 <码>.<交易所> 形");
    expect(find(code) == kNone, s, {.code = code}, "代码重复");
    add(code);
  }
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
  return it != ids_.end() ? it->second : kNone;
}

void Ids::miss(const Spec &s, std::string_view code) const {
  if (cover_ == Cover::Part || !from<Bq>(s))
    return;
  const std::lock_guard lock(miss_mu_);
  if (!live_loaded_) {
    const Catalog cat;
    const Spec &ls = spec("cn_stock_static_data");
    const std::string key(key_col(ls));
    const pq::TableView v(read(need(cat, ls), {key, "name"}));
    const pq::Col k = v.col(key), name = v.col("name");
    for (std::int64_t i = 0; i < v.rows(); ++i)
      live_.emplace(k.str(i), name.str(i));
    live_loaded_ = true;
  }
  const auto it = live_.find(code);
  if (it == live_.end())
    return;
  std::string k(s.name);
  k += ' ';
  k += code;
  if (warned_.insert(std::move(k)).second)
    line("warning", s, {.code = code}, "在市 (" + it->second + ") 但不在 id 空间, 跳过");
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
  r.s_ = &s;
  r.t_ = t_.get();
  r.n_ = t_->rows();
  if (r.n_ == 0)
    return r;
  key_ = t_->dict_col(key);
  dict_ids_.resize(static_cast<std::size_t>(key_.dict_size()));
  for (std::int32_t k = 0; k < key_.dict_size(); ++k) {
    const std::string_view code = key_.dict_value(k);
    const std::uint32_t id = ids.find(code);
    if (id == Ids::kNone)
      ids.miss(s, code);
    dict_ids_[static_cast<std::size_t>(k)] = id;
  }
  if (s.vd.empty())
    dates_.assign(static_cast<std::size_t>(r.n_), 0);
  else
    t_->col(s.vd).yyyymmdd_all(dates_);
  r.key_ = key_.indices();
  r.dict_ids_ = dict_ids_.data();
  r.dates_ = dates_.data();
  if (snap == Snap::Day) {
    r.day_ = *std::max_element(dates_.begin(), dates_.end());
    expect(r.day_ > 0, s, {}, "Snap::Day 分片 " + std::to_string(sh.ym) + " vd 全 null");
  }
  return r;
}

Scan::Scan(const Catalog &cat, const Ids &ids, std::size_t threads) : cat_(cat), ids_(ids), threads_(threads) {
  assert(threads_ > 0);
}

void Scan::run(std::atomic<std::size_t> &done) {
  std::sort(jobs_.begin(), jobs_.end(), [](const Job &a, const Job &b) { return a.bytes > b.bytes; });
  misc::exec::ForkJoin ex(static_cast<int>(std::min(threads_, std::max<std::size_t>(1, jobs_.size()))));
  std::vector<Scratch> xs(static_cast<std::size_t>(ex.threads())); // 按 tid
  ex.tasks(static_cast<int>(jobs_.size()), [&](int i, int tid) { jobs_[static_cast<std::size_t>(i)].body(xs[static_cast<std::size_t>(tid)], done); });
  jobs_.clear();
}

bool snapshot(const Catalog &cat, const Ids &ids, const Spec &s, const std::vector<std::string> &cols,
              const std::function<void(const Rows &)> &fn) {
  assert(s.shape == Shape::Month && "snapshot: 只读 Month 表");
  const std::vector<Shard> &shards = cat.shards(s.name);
  Scratch x;
  for (auto it = shards.rbegin(); it != shards.rend(); ++it) {
    const Rows r = x.open(*it, s, cols, ids, Snap::Day);
    if (r.size() == 0)
      continue; // 0 行月 (拉过为空) → 往前找
    fn(r);
    return true;
  }
  return false;
}

void meta(const Catalog &cat, const Ids &ids, const Spec &s, const std::vector<std::string> &cols,
          const std::function<void(const Rows &)> &fn) {
  Scratch x;
  fn(x.open(need(cat, s), s, cols, ids));
}

bool daily(const Catalog &cat, const Ids &ids, const Spec &s, Ymd d, const std::vector<std::string> &cols, Scratch &x,
           const std::function<void(const Rows &)> &fn) {
  assert(s.shape == Shape::Daily && "daily: 只读 Daily 表");
  const Shard *sh = cat.find_day(s.name, d);
  if (!sh)
    return false;
  fn(x.open(*sh, s, cols, ids));
  return true;
}

} // namespace fundamental
