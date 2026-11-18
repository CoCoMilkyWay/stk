#include "gui/quote/PriceLib.hpp"

#include "account/Ledger.hpp"
#include "features/Bar1mDay.hpp"
#include "features/Panel/PoolSelect.hpp" // all_bar / picks / extends
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "fundamental/Scan.hpp"
#include "gui/account/AccountSide.hpp" // 账户一侧的回放
#include "gui/quote/BasketRow.hpp"
#include "gui/quote/services/QuoteService.hpp" // 新浪归档
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_table

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <mutex>
#include <numeric>
#include <set>
#include <string>
#include <system_error>

namespace GUI::Quote {

namespace {

constexpr uint64_t kMagic = 0x4441454852504B53ULL;    // "SKPRHEAD" 小端
constexpr uint64_t kSegMagic = 0x5452415052504B53ULL; // "SKPRPART"
constexpr size_t kB = PriceLib::kBlock;
constexpr disk::Node kSideDir[kMarkets] = {disk::LibPriceStock, disk::LibPriceBasket}; // [侧]
constexpr disk::Node kBlockFile[kMarkets] = {disk::LibPriceStockBlock, disk::LibPriceBasketBlock};
constexpr disk::Node kDayFile[kMarkets] = {disk::LibPriceStockDay, disk::LibPriceBasketDay};

// 一行一日的日根 (只存有日根的): 存价口径 (个股 = 原价, 篮子 = 比), ref = 前收 (篮子 = 1); k = 当日分钟紧凑根的价基 / 步长
struct DayRec {
  double f; // 因子 (过链后)
  float open, high, low, close, ref, amount;
  DayFlow flow;
  Scale k;
};

// 段 = SegHead + RowAt [rows] + DayRec [recs] + Packed [recs] (分钟根与日根同位: 有日根缺分钟根的占一格空根);
//   tag = 篮子侧所依的篮子表指纹 (个股侧 0)
struct SegHead {
  uint64_t magic, tag;
  uint32_t days, rows;
  uint64_t recs;
};
// 段里的一行: 日根 / 分钟根的位图 (位 j = 段内第 j 日; 分钟根 ⊂ 日根), 日根与分钟根同自 at 起按日序连续
struct RowAt {
  uint32_t key, at;
  uint64_t day, min;
};
size_t seg_bytes(const SegHead &h) { return sizeof(SegHead) + h.rows * sizeof(RowAt) + h.recs * (sizeof(DayRec) + sizeof(Packed)); }
uint64_t below(size_t j) { return (uint64_t{1} << j) - 1; }
// 行 r 段内第 j 日 (须有日根) 的格
size_t slot(const RowAt &r, size_t j) { return r.at + static_cast<size_t>(std::popcount(r.day & below(j))); }
// 空根 (占位的格)
const Packed &void_packed() {
  static const Packed p = [] {
    Packed x;
    for (Slot &s : x.s)
      s = {0, 0, 0, kNoBar, 0, 0};
    return x;
  }();
  return p;
}
// m 的各位 j 升序: f(j)
template <class F>
void bits(uint64_t m, F &&f) {
  for (; m; m &= m - 1)
    f(static_cast<size_t>(std::countr_zero(m)));
}

// 一段的只读映射 (各份 Series 的分钟视图随 shared_ptr 共持; 文件被删 / 换掉后映射照旧可读)
class Seg {
public:
  // p 是 days 日的完整段 (头 + 长度对得上): 掉电截断 / 写坏的段当作缺
  static bool whole(const std::filesystem::path &p, size_t days) {
    std::error_code ec;
    const uintmax_t n = std::filesystem::file_size(p, ec);
    if (ec || n < sizeof(SegHead))
      return false;
    SegHead h{};
    std::FILE *f = std::fopen(p.c_str(), "rb");
    const bool got = f && std::fread(&h, sizeof(h), 1, f) == 1;
    if (f)
      std::fclose(f);
    return got && h.magic == kSegMagic && h.days == days && seg_bytes(h) == n;
  }
  explicit Seg(const std::filesystem::path &p) {
    const int fd = ::open(p.c_str(), O_RDONLY);
    assert(fd >= 0 && "price/: 段打不开");
    struct stat sb{};
    [[maybe_unused]] const int st = ::fstat(fd, &sb);
    assert(st == 0);
    n_ = static_cast<size_t>(sb.st_size);
    void *m = ::mmap(nullptr, n_, PROT_READ, MAP_SHARED, fd, 0);
    ::close(fd);
    assert(m != MAP_FAILED && "price/: 段映射失败");
    ::madvise(m, n_, MADV_RANDOM);
    base_ = static_cast<const std::byte *>(m);
    h_ = reinterpret_cast<const SegHead *>(base_);
    assert(n_ >= sizeof(SegHead) && h_->magic == kSegMagic && seg_bytes(*h_) == n_ && "price/: 段损坏");
    rows_ = {reinterpret_cast<const RowAt *>(h_ + 1), h_->rows};
    recs_ = reinterpret_cast<const DayRec *>(rows_.data() + rows_.size());
    mins_ = reinterpret_cast<const Packed *>(recs_ + h_->recs);
  }
  Seg(const Seg &) = delete;
  Seg &operator=(const Seg &) = delete;
  ~Seg() { ::munmap(const_cast<std::byte *>(base_), n_); }

  size_t days() const { return h_->days; }
  uint64_t tag() const { return h_->tag; }
  std::span<const RowAt> rows() const { return rows_; }
  const RowAt *row(uint32_t key) const {
    const auto it = std::ranges::lower_bound(rows_, key, {}, &RowAt::key);
    return it != rows_.end() && it->key == key ? &*it : nullptr;
  }
  // 行 r 段内第 j 日 (须有日根) 的日根 / 分钟根 (无 → nullptr)
  const DayRec &day(const RowAt &r, size_t j) const {
    assert(r.day >> j & 1);
    return recs_[slot(r, j)];
  }
  const Packed *min(const RowAt &r, size_t j) const { return r.min >> j & 1 ? &mins_[slot(r, j)] : nullptr; }
  // 行 r 的记录先一并发出读
  void will(const RowAt &r, bool minute) const {
    const size_t n = static_cast<size_t>(std::popcount(r.day));
    advise(recs_ + r.at, n * sizeof(DayRec));
    if (minute)
      advise(mins_ + r.at, n * sizeof(Packed));
  }

private:
  static void advise(const void *p, size_t n) {
    static const uintptr_t pg = static_cast<uintptr_t>(::sysconf(_SC_PAGESIZE));
    const uintptr_t a = reinterpret_cast<uintptr_t>(p) / pg * pg;
    if (n > 0)
      ::madvise(reinterpret_cast<void *>(a), reinterpret_cast<uintptr_t>(p) + n - a, MADV_WILLNEED);
  }
  const std::byte *base_ = nullptr;
  size_t n_ = 0;
  const SegHead *h_ = nullptr;
  std::span<const RowAt> rows_;
  const DayRec *recs_ = nullptr;
  const Packed *mins_ = nullptr;
};

// 一段的写: tmp 上顺序写, done 换上
class SegOut {
public:
  explicit SegOut(std::filesystem::path p) : p_(std::move(p)), tmp_(p_) {
    tmp_ += ".tmp";
    f_ = std::fopen(tmp_.c_str(), "wb");
    assert(f_ && "price/: 段建不出来");
  }
  template <class T>
  void all(std::span<const T> x) {
    [[maybe_unused]] const size_t n = std::fwrite(x.data(), sizeof(T), x.size(), f_);
    assert(n == x.size() && "price/: 段写入失败");
  }
  template <class T>
  void put(const T &x) {
    all(std::span<const T>(&x, 1));
  }
  void done() {
    [[maybe_unused]] const int c = std::fclose(f_);
    assert(c == 0 && "price/: 段写入失败");
    std::filesystem::rename(tmp_, p_);
  }

private:
  std::filesystem::path p_, tmp_;
  std::FILE *f_ = nullptr;
};

// 一段的定位写 (库段整块直写): tmp 定长, 各线程按偏移并发写, done 换上; 没 done 即弃 tmp
class SegAt {
public:
  SegAt(std::filesystem::path p, size_t bytes) : p_(std::move(p)), tmp_(p_) {
    tmp_ += ".tmp";
    fd_ = ::open(tmp_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    assert(fd_ >= 0 && "price/: 段建不出来");
    [[maybe_unused]] const int t = ::ftruncate(fd_, static_cast<off_t>(bytes));
    assert(t == 0 && "price/: 段建不出来");
  }
  SegAt(const SegAt &) = delete;
  SegAt &operator=(const SegAt &) = delete;
  ~SegAt() {
    if (fd_ >= 0) {
      ::close(fd_);
      std::filesystem::remove(tmp_);
    }
  }
  void put(size_t off, const void *x, size_t n) const {
    [[maybe_unused]] const ssize_t w = ::pwrite(fd_, x, n, static_cast<off_t>(off));
    assert(w == static_cast<ssize_t>(n) && "price/: 段写入失败");
  }
  void done() {
    [[maybe_unused]] const int c = ::close(fd_);
    assert(c == 0 && "price/: 段写入失败");
    fd_ = -1;
    std::filesystem::rename(tmp_, p_);
  }

private:
  std::filesystem::path p_, tmp_;
  int fd_ = -1;
};

disk::Key at_day(const disk::Key &lib, misc::Ymd d) {
  disk::Key k = lib;
  k.date = d;
  return k;
}

// 两侧逐个: f.template operator()<侧>()
template <class F>
void each_side(F &&f) {
  f.template operator()<kStock>();
  f.template operator()<kBasket>();
}

using misc::snap::items;

// 一行一日过链 z: base = 当日前收 (存价口径), close = 当日收; origin = 首个有根日的因子. 返回当日因子
double step(PriceLib::Link &z, float base, float close, double origin) {
  assert(close > 0);
  if (z.ref > 0 && base > 0)
    z.f *= z.ref / base, z.ref = base;
  if (!(z.f > 0))
    z.f = origin;
  z.ref = close;
  return z.f;
}
template <Side S>
constexpr double origin() { return S == kStock ? 1.0 : PriceLib::kBase; }

// 一行一日的 收 / 前收: 成员过日的漂移比
float rel_of(const DayRec &r) { return r.ref > 0 ? r.close / r.ref : 1.f; }
float rel_of(const Candle &c) { return traded(c) && c.ref > 0 ? c.close / c.ref : 1.f; }

// 存价 → 有效价的倍数: 个股 1 (原价), 篮子 = 因子
template <Side S>
double scale_of(const DayRec &r) { return S == kBasket ? r.f : 1.0; }

template <Side S>
Candle candle_of(const DayRec &r, misc::Ymd d) {
  const double a = scale_of<S>(r);
  const auto at = [a](float v) { return static_cast<float>(v * a); };
  return {{at(r.open), at(r.high), at(r.low), at(r.close), r.amount}, d, at(r.ref), r.flow};
}

// 一只一日 (个股侧存的日根 d + 分钟根 m, nullptr = 无) → 篮子合成的 Leg (基准 = 存的前收, 根 = 存价即原价); 分钟根只解槽 [lo, hi)
Leg leg_of(const DayRec &d, const Packed *m, Cell &c, Bars &b, int lo, int hi) {
  b.has.reset();
  c = cell(d.open, d.high, d.low, d.close, d.amount);
  if (m)
    for (int s = lo; s < hi; ++s)
      if (const Slot &y = m->s[static_cast<size_t>(s)]; y.c != kNoBar)
        b.add(s, unpack(y, d.k, 1.0));
  return {d.ref, &c, &b};
}

// 篮子的源 = 个股侧一段的段内第 j 日 (行 = 全局 id; 该日无根的行 = 无日根无分钟根)
struct SegDay {
  misc::Ymd date;
  const Seg &sg;
  size_t j;
  size_t size() const { return sg.rows().size(); }
  uint32_t id(size_t r) const { return sg.rows()[r].key; }
  Leg leg(size_t r, Cell &c, Bars &b, int lo, int hi) const {
    const RowAt &x = sg.rows()[r];
    if (!(x.day >> j & 1)) {
      b.has.reset();
      return {1.f, nullptr, &b};
    }
    return leg_of(sg.day(x, j), sg.min(x, j), c, b, lo, hi);
  }
};

// 一段 n 日写到 p: 行 = 键 keys (升序) 里有根的; rec(q, j) / min(q, j) = 第 q 个键段内第 j 日的日根 / 分钟根 (nullptr = 无; 分钟根只随日根)
template <class Rec, class Min>
void write_seg(const std::filesystem::path &p, uint64_t tag, size_t n, std::span<const uint32_t> keys, Rec &&rec, Min &&min) {
  assert(n >= 1 && n <= kB);
  std::vector<RowAt> rows;
  std::vector<size_t> qs; // [行] 键位
  uint64_t recs = 0;
  for (size_t q = 0; q < keys.size(); ++q) {
    RowAt x{keys[q], static_cast<uint32_t>(recs), 0, 0};
    for (size_t j = 0; j < n; ++j)
      if (rec(q, j)) {
        x.day |= uint64_t{1} << j;
        if (min(q, j))
          x.min |= uint64_t{1} << j;
      }
    if (!x.day)
      continue;
    recs += static_cast<uint64_t>(std::popcount(x.day));
    rows.push_back(x), qs.push_back(q);
  }
  SegOut o(p);
  o.put(SegHead{kSegMagic, tag, static_cast<uint32_t>(n), static_cast<uint32_t>(rows.size()), recs});
  o.all(std::span<const RowAt>(rows));
  for (size_t r = 0; r < rows.size(); ++r)
    bits(rows[r].day, [&](size_t j) { o.put(*rec(qs[r], j)); });
  for (size_t r = 0; r < rows.size(); ++r)
    bits(rows[r].day, [&](size_t j) {
      const Packed *m = min(qs[r], j);
      o.put(m ? *m : void_packed());
    });
  o.done();
}

// kBlock 个单日段 ds (日序) 并成一块 p
void seal_block(const std::filesystem::path &p, std::span<const Seg *const> ds) {
  assert(ds.size() == kB);
  std::vector<uint32_t> keys;
  for (const Seg *d : ds)
    for (const RowAt &r : d->rows())
      keys.push_back(r.key);
  std::ranges::sort(keys);
  keys.erase(std::ranges::unique(keys).begin(), keys.end());
  write_seg(
      p, ds[0]->tag(), kB, keys,
      [&](size_t q, size_t j) -> const DayRec * { const RowAt *r = ds[j]->row(keys[q]); return r ? &ds[j]->day(*r, 0) : nullptr; },
      [&](size_t q, size_t j) -> const Packed * { const RowAt *r = ds[j]->row(keys[q]); return r ? ds[j]->min(*r, 0) : nullptr; });
}

} // namespace

// 两侧的段 (各自日序): 一段 = 首日下标 + 映射 (与读出的全史共持)
struct PriceStore {
  struct Part {
    size_t day = 0;
    std::shared_ptr<const Seg> seg;
  };
  PerMarket<std::vector<Part>> side;
  // 一侧日 i 所在的段 + 段内日
  std::pair<const Seg *, size_t> at(Side s, size_t i) const {
    const std::vector<Part> &ps = side[s];
    const auto it = std::ranges::upper_bound(ps, i, {}, &Part::day);
    assert(it != ps.begin() && i - it[-1].day < it[-1].seg->days());
    return {it[-1].seg.get(), i - it[-1].day};
  }
};

// 一侧一日的件: 行按键升序追加, 过链后落成单日段
struct DayBuild {
  std::vector<uint32_t> keys;
  std::vector<DayRec> recs;
  std::vector<int32_t> at; // [行] 分钟根在 mins 的下标 (−1 = 无)
  std::vector<Packed> mins;

  void clear() { keys.clear(), recs.clear(), at.clear(), mins.clear(); }
  // 第 q 行的分钟根; 无 → nullptr
  const Packed *minute(size_t q) const { return at[q] >= 0 ? &mins[static_cast<size_t>(at[q])] : nullptr; }
  // 键 key 的日根 / 分钟根; 无 → nullptr
  const DayRec *rec(uint32_t key) const {
    const size_t q = find(key);
    return q < keys.size() ? &recs[q] : nullptr;
  }
  const Packed *min(uint32_t key) const {
    const size_t q = find(key);
    return q < keys.size() ? minute(q) : nullptr;
  }
  // 一行: 日根 c (存价口径) + 前收 ref + 资金流摘要 + 分钟根 b (nullptr / 无根 = 无; 各根净流入 net)
  template <Side S>
  void add(uint32_t key, const Cell &c, float ref, const DayFlow &flow, const Bars *b, std::span<const double, kSlots> net) {
    assert(keys.empty() || keys.back() < key);
    DayRec r{0, c.open, c.high, c.low, c.close, ref, static_cast<float>(c.amount), flow, {}};
    const bool m = b && b->has.any();
    if (m)
      r.k = pack(*b, net, S == kStock, mins.emplace_back());
    keys.push_back(key), recs.push_back(r), at.push_back(m ? static_cast<int32_t>(mins.size()) - 1 : -1);
  }
  // 个股各行 ← 源的一日 (无成交不收; 净流入 = 本只分钟涨跌法)
  template <class Src>
  void put_stocks(const Src &src) {
    Cell c;
    Bars b;
    std::array<double, kSlots> net;
    for (size_t j = 0; j < src.size(); ++j)
      if (const Leg g = src.leg(j, c, b); g.day) {
        net.fill(0);
        net_flow(*g.bars, g.base, net);
        add<kStock>(src.id(j), *g.day, g.base, day_flow(*g.bars, net), g.bars, net);
      }
  }
  // 篮子各行 ← Lane 的一日 (前日 = 1)
  void put_baskets(std::span<const Day> ys) {
    for (size_t b = 0; b < ys.size(); ++b)
      if (const Day &y = ys[b]; y.candle.date)
        add<kBasket>(static_cast<uint32_t>(b), y.candle, 1.f, y.candle.flow, &y.bars, y.bars.net);
  }
  // 落成单日段 (tag 见 SegHead)
  void write(const std::filesystem::path &p, uint64_t tag) const {
    write_seg(p, tag, 1, keys, [&](size_t q, size_t) { return &recs[q]; }, [&](size_t q, size_t) { return minute(q); });
  }

private:
  size_t find(uint32_t key) const {
    const auto it = std::ranges::lower_bound(keys, key);
    return it != keys.end() && *it == key ? static_cast<size_t>(it - keys.begin()) : keys.size();
  }
};

namespace {

// 一侧一日的件 b 过链 z (就地推进, 填因子)
template <Side S>
void chain(DayBuild &b, std::vector<PriceLib::Link> &z) {
  for (size_t j = 0; j < b.keys.size(); ++j) {
    DayRec &r = b.recs[j];
    r.f = step(z[b.keys[j]], r.ref, r.close, origin<S>());
  }
}

// 篮子的源 = 个股侧一日的件 (库段同趟, 内存里; 与落盘后的 SegDay 同读法)
struct BuildDay {
  misc::Ymd date;
  const DayBuild &b;
  size_t size() const { return b.keys.size(); }
  uint32_t id(size_t r) const { return b.keys[r]; }
  Leg leg(size_t r, Cell &c, Bars &bars, int lo, int hi) const { return leg_of(b.recs[r], b.minute(r), c, bars, lo, hi); }
};

// 库段一趟的一件 (每线程一份, 逐日复用): 日根行 / 分钟的读 + 个股一日的件 + 篮子合成件 (同趟时)
struct LibWork {
  LibWork(size_t A, size_t baskets) : lane(baskets) {
    row_alloc(dc, A);
    minute_alloc(mc, A);
  }
  FeatureRead::DayColumns dc, mc;
  DayBuild b;
  Lane lane;
};

// 个股补段一日的件 (逐批复用): 源的读 (基本面段 = bar1m, 新浪段 = 归档的日根 / 分钟根 [全局 id]) + 一日的件
struct TailWork {
  Bar1mDay bd;
  fundamental::Scratch xs;
  std::vector<Candle> spx;
  std::vector<Bars> sbars;
  DayBuild b;
};

// 篮子一日的件 (逐批复用): 合成件 + 一日的件
struct FoldWork {
  explicit FoldWork(size_t baskets) : lane(baskets) {}
  Lane lane;
  DayBuild b;
};

// 一侧盘上第 i 日的根照样过链 z (留着的日: 只重放链, 不动段)
template <Side S>
void rechain(const PriceStore &st, size_t i, std::vector<PriceLib::Link> &z) {
  const auto [sg, j] = st.at(S, i);
  for (const RowAt &r : sg->rows())
    if (r.day >> j & 1) {
      const DayRec &x = sg->day(r, j);
      step(z[r.key], x.ref, x.close, origin<S>());
    }
}

// 一侧的首日 (只定成员, 无根): 空段
std::shared_ptr<const Seg> first_day(Side s, const disk::Key &lib, misc::Ymd d, uint64_t tag) {
  disk::ensure(kSideDir[s], lib);
  const std::filesystem::path p = disk::path(kDayFile[s], at_day(lib, d));
  DayBuild{}.write(p, tag);
  return std::make_shared<const Seg>(p);
}

} // namespace

PriceLib::PriceLib(std::shared_ptr<const Calendar> cal, std::function<void(const Handoff &)> bind) : cal_(std::move(cal)), bind_(std::move(bind)) {
  assert(cal_ && !cal_->days.empty() && bind_);
  Start();
}
PriceLib::~PriceLib() {
  quit_.store(true, std::memory_order_relaxed);
  Stop();
}

template <class Ar>
void PriceLib::snap(Ar &ar) {
  ar(prefix_);
  ar(table_);
  items(ar, bkeys_);
  items(ar, dates_);
  ar(lib_days_);
  ar(fund_days_);
  ar(bdays_);
  for (size_t &n : sealed_)
    ar(n);
  for (std::vector<Link> &l : link_)
    items(ar, l);
}

void PriceLib::save_head() {
  snap_write(disk::LibPriceHead, lib_, head_, [&](auto &ar) { snap(ar); });
}

PriceStamp PriceLib::stamp() const { return {head_.lib, table_, dates_.back(), dates_.size(), lib_days_, fund_days_}; }

bool PriceLib::open_side(Side s) {
  const size_t D = days(s), B = sealed_[s] * kB;
  std::set<misc::Ymd> blocks, singles;
  for (size_t b = 0; b < sealed_[s]; ++b)
    blocks.insert(dates_[b * kB]);
  for (size_t i = B; i < D; ++i)
    singles.insert(dates_[i]);
  for (const disk::Key &k : disk::list(kBlockFile[s], lib_))
    if (!blocks.contains(k.date))
      disk::remove(kBlockFile[s], k);
  for (const disk::Key &k : disk::list(kDayFile[s], lib_))
    if (!singles.contains(k.date))
      disk::remove(kDayFile[s], k);
  const auto path = [&](size_t i) { return disk::path(i < B ? kBlockFile[s] : kDayFile[s], at_day(lib_, dates_[i])); };
  for (size_t i = 0; i < D; i += i < B ? kB : 1)
    if (!Seg::whole(path(i), i < B ? kB : 1))
      return false;
  std::vector<PriceStore::Part> &ps = store_->side[s];
  ps.clear();
  for (size_t i = 0; i < D; i += i < B ? kB : 1)
    ps.push_back({i, std::make_shared<const Seg>(path(i))});
  return true;
}

void PriceLib::wipe(const std::vector<misc::Ymd> &dates) {
  store_ = std::make_unique<PriceStore>();
  dates_ = {dates.front()}, lib_days_ = 1, fund_days_ = 0, sealed_[kStock] = 0;
  link_[kStock].assign(asset_axis().size(), Link{});
  prefix_ = asset_axis().hash_at(rows(kStock));
  disk::remove(disk::LibPrice, lib_);
  store_->side[kStock].push_back({0, first_day(kStock, lib_, dates_[0], 0)});
  wipe_baskets();
}

void PriceLib::wipe_baskets() {
  const BasketTable &bt = basket_table();
  table_ = bt.hash(), bkeys_ = bt.keys(), bdays_ = 1, sealed_[kBasket] = 0;
  link_[kBasket].assign(bt.size(), Link{});
  disk::remove(kSideDir[kBasket], lib_);
  store_->side[kBasket].clear();
  store_->side[kBasket].push_back({0, first_day(kBasket, lib_, dates_[0], table_)});
  save_head();
}

void PriceLib::reopen(const FeatureLib &lib, const std::vector<misc::Ymd> &dates) {
  lib_ = lib_key(lib.id);
  const AssetAxis &ax = asset_axis();
  const Head want{kMagic, lib.axis.hash, dates.front()};
  const auto fits = [&] { return head_ == want && pool::extends(std::span(dates_).first(lib_days_), dates); };
  if (!(store_ && fits())) {
    store_ = std::make_unique<PriceStore>();
    head_ = {}, prefix_ = table_ = 0, dates_.clear(), lib_days_ = fund_days_ = bdays_ = 0, sealed_ = {}, link_ = {}, at_ = {};
    if (snap_read(disk::LibPriceHead, lib_, want, [&](auto &ar) { snap(ar); })) {
      head_ = want;
      assert(lib_days_ >= 1 && lib_days_ + fund_days_ <= dates_.size() && bdays_ >= 1 && bdays_ <= dates_.size() && "price/head.bin: 段日数越界");
      assert(sealed_[kStock] * kB <= lib_days_ && sealed_[kBasket] * kB <= std::min(bdays_, lib_days_) && "price/head.bin: 封块数越界");
    }
    if (!(fits() && rows(kStock) <= ax.size() && ax.hash_at(rows(kStock)) == prefix_ && open_side(kStock))) {
      head_ = want;
      wipe(dates);
    } else if (!open_side(kBasket)) {
      wipe_baskets();
    }
  }
  // 篮子表变了: 留给 refold 按身份重排; 段里混着第三张表的 (换到一半表又变) → 篮子侧从头. 个股侧不动
  if (const uint64_t h = basket_table().hash();
      table_ != h && !std::ranges::all_of(store_->side[kBasket], [&](const PriceStore::Part &p) { return p.seg->tag() == table_ || p.seg->tag() == h; }))
    wipe_baskets();
  if (rows(kStock) < ax.size()) { // 资产轴长了: 新行的链自无起
    link_[kStock].resize(ax.size());
    prefix_ = ax.hash_at(rows(kStock));
    save_head();
  }
  axis_ = lib.axis;
}

template <Side S>
void PriceLib::commit(misc::Ymd d, DayBuild &b, std::vector<Link> &z) {
  const size_t i = days(S);
  assert(S == kStock || (i < dates_.size() && dates_[i] == d));
  chain<S>(b, z);
  const std::filesystem::path f = disk::path(kDayFile[S], at_day(lib_, d));
  b.write(f, S == kBasket ? table_ : 0);
  b.clear();
  store_->side[S].push_back({i, std::make_shared<const Seg>(f)});
  if constexpr (S == kStock)
    dates_.push_back(d);
  else
    ++bdays_;
}

template <Side S>
void PriceLib::seal() {
  std::vector<PriceStore::Part> &ps = store_->side[S];
  size_t &n = sealed_[S];
  while ((n + 1) * kB <= std::min(days(S), lib_days_)) {
    const size_t i0 = n * kB, at = n; // 块前各段都是块 ⇒ 首个单日段在 ps[n]
    assert(ps[at].day == i0 && ps[at + kB - 1].day == i0 + kB - 1);
    std::vector<const Seg *> ds;
    for (size_t j = 0; j < kB; ++j)
      ds.push_back(ps[at + j].seg.get());
    const std::filesystem::path f = disk::path(kBlockFile[S], at_day(lib_, dates_[i0]));
    seal_block(f, ds);
    ++n;
    save_head();
    ps.erase(ps.begin() + static_cast<ptrdiff_t>(at), ps.begin() + static_cast<ptrdiff_t>(at + kB));
    ps.insert(ps.begin() + static_cast<ptrdiff_t>(at), PriceStore::Part{i0, std::make_shared<const Seg>(f)});
    for (size_t j = 0; j < kB; ++j)
      disk::remove(kDayFile[S], at_day(lib_, dates_[i0 + j]));
  }
}

void PriceLib::cut(size_t D) {
  assert(D >= lib_days_ && D <= dates_.size());
  const std::vector<misc::Ymd> gone(dates_.begin() + static_cast<ptrdiff_t>(D), dates_.end());
  dates_.resize(D), fund_days_ = std::min(fund_days_, D - lib_days_), bdays_ = std::min(bdays_, D);
  save_head();
  for (std::vector<PriceStore::Part> &ps : store_->side)
    while (ps.back().day >= D)
      ps.pop_back();
  for (const misc::Ymd d : gone)
    for (const disk::Node n : kDayFile)
      disk::remove(n, at_day(lib_, d));
}

void PriceLib::cut_baskets(size_t B) {
  assert(B >= std::max<size_t>(1, sealed_[kBasket] * kB) && B <= bdays_);
  const size_t was = bdays_;
  bdays_ = B;
  save_head();
  std::vector<PriceStore::Part> &ps = store_->side[kBasket];
  while (ps.back().day >= B)
    ps.pop_back();
  for (size_t i = B; i < was; ++i)
    disk::remove(kDayFile[kBasket], at_day(lib_, dates_[i]));
}

bool PriceLib::sync(const FeatureLib &lib, const std::vector<misc::Ymd> &dates, misc::Ymd mark) {
  assert(dates.size() >= 2 && mark > 0);
  reopen(lib, dates);
  const fundamental::Catalog cat;
  const std::vector<misc::Ymd> &trading = cal_->days;
  const auto lo = std::upper_bound(trading.begin(), trading.end(), dates.back());
  const auto mid = std::max(lo, std::upper_bound(trading.begin(), trading.end(), mark));
  auto hi = mid;
  while (hi != trading.end() && disk::exists(disk::QuoteDay, {.date = *hi}))
    ++hi;
  const std::vector<misc::Ymd> tail(lo, hi);
  const size_t F = static_cast<size_t>(mid - lo);
  const bool grow = lib_days_ < dates.size();
  const bool rekey = table_ != basket_table().hash(); // 篮子表换了: 篮子侧按身份重排 (refold)
  size_t keep = 0;                                    // 补段留着的日: 库段没长时, 打头 (日, 截) 都不变的
  if (!grow)
    while (keep < std::min(dates_.size() - lib_days_, tail.size()) && dates_[lib_days_ + keep] == tail[keep] && (keep < fund_days_) == (keep < F))
      ++keep;
  const bool fresh = !grow && !rekey && keep == tail.size() && dates_.size() == lib_days_ + keep && bdays_ == dates_.size();
  if (fresh && at_ == stamp())
    return true;
  at_ = {};
  const std::shared_ptr<const pool::Picks> pk = pool::picks(lib, &quit_);
  if (!pk || pk->tape->dates != dates) // 库在落新日: 下趟再补
    return false;
  if (dates_.size() > lib_days_ + keep)
    cut(lib_days_ + keep);
  if (rekey && bdays_ > lib_days_) // 篮子侧的补段日 (冻结成员) 随后由 fold 重合
    cut_baskets(lib_days_);
  // 进度一次定总量: 个股库段新日 + 补段要算的日 + 篮子侧要重排的日 + 要合成的日 (日脊终长 = 库日轴 + 补段)
  begin(Phase::Loading, (grow ? dates.size() - lib_days_ : 0) + (tail.size() - keep) + (rekey ? bdays_ : 0) + (dates.size() + tail.size() - bdays_));

  // 篮子侧跟得上 (表没换, 日数同个股侧) → 库段新日的篮子同趟合成, 不回读个股段
  if (grow && !lib_pass(lib, dates, !rekey && bdays_ == lib_days_ ? pk.get() : nullptr))
    return false;
  if (!fill(cat, tail, F, keep) || (rekey && !refold(*pk)) || !fold(*pk))
    return false;
  drop_archived(dates_[lib_days_ + fund_days_ - 1]);
  at_ = stamp();
  bind_(Handoff{mem_, level_});
  return true;
}

bool PriceLib::lib_pass(const FeatureLib &lib, const std::vector<misc::Ymd> &dates, const pool::Picks *pk) {
  FeatureRead rd(lib, &quit_);
  const PoolAxis &ax = lib.axis;
  const size_t A = ax.size(), N = dates.size(), T = static_cast<size_t>(pool_.threads());
  assert(lib_days_ >= 1 && dates_.size() == lib_days_ && (!pk || bdays_ == lib_days_));
  std::vector<std::unique_ptr<LibWork>> works;
  for (size_t t = 0; t < T; ++t)
    works.push_back(std::make_unique<LibWork>(A, pk ? rows(kBasket) : 0));
  std::vector<Row> rws(kB + 1);                 // [批内日 + 1] 日根行 (首项 = 批首日的前一日)
  std::vector<DayBuild> sb(T), bb(pk ? kB : 0); // [批内日] 个股件 (单日段) / 篮子件
  // 批内第 k 日 (线程 t): 读分钟 → 个股件 b; 同趟 → 篮子件 bb[k]
  const auto day = [&](size_t i0, size_t k, int t, DayBuild &b) {
    LibWork &w = *works[static_cast<size_t>(t)];
    const misc::Ymd d = dates[i0 + k];
    read_minutes(rd, d, w.mc);
    b.clear();
    b.put_stocks(LibDay{d, ax, rws[k], rws[k + 1], w.mc});
    if (pk) {
      members(*pk, ax, d, w.lane.mem);
      w.lane.run(BuildDay{d, b});
      bb[k].clear();
      bb[k].put_baskets(w.lane.day);
    }
    publish(pk ? 2 : 1);
  };
  const auto stale = [&] {
    if (rd.stale()) // 库在重算: 下次从头核对
      head_ = {};
    return rd.stale();
  };
  while (lib_days_ < N) {
    if (quit_.load(std::memory_order_relaxed))
      return false;
    const size_t i0 = lib_days_;
    const bool whole = i0 % kB == 0 && N - i0 >= kB; // 整块: 直写成块, 不经单日段
    const size_t n = whole ? kB : std::min({T, N - i0, kB - i0 % kB});
    pool_.tasks(static_cast<int>(n + 1), [&](int k, int t) {
      read_row(rd, dates[i0 - 1 + static_cast<size_t>(k)], works[static_cast<size_t>(t)]->dc, rws[static_cast<size_t>(k)]);
    });
    if (stale())
      return false;
    if (whole) {
      assert(sealed_[kStock] * kB == i0);
      // 行表 = 库轴行 (全局 id 升序) 在块内有日根的日 (判据同 LibDay) ⇒ 各行的格先定, 文件定长
      std::vector<RowAt> rs;
      uint64_t R = 0;
      for (size_t a = 0; a < A; ++a) {
        uint64_t m = 0;
        for (size_t j = 0; j < kB; ++j)
          m |= uint64_t{traded_row(rws[j + 1], a)} << j;
        if (m)
          rs.push_back({ax.global(a), static_cast<uint32_t>(R), m, 0}), R += static_cast<uint64_t>(std::popcount(m));
      }
      const SegHead h{kSegMagic, 0, static_cast<uint32_t>(kB), static_cast<uint32_t>(rs.size()), R};
      const size_t at_rec = sizeof(SegHead) + rs.size() * sizeof(RowAt), at_min = at_rec + R * sizeof(DayRec);
      const std::filesystem::path f = disk::path(kBlockFile[kStock], at_day(lib_, dates[i0]));
      SegAt out(f, seg_bytes(h));
      std::vector<DayRec> recs(R);
      std::vector<uint8_t> has(kB * rs.size()); // [日][行] 带分钟根
      // 各日并行: 个股件的行 = 行表里该日有根的行 (同序) → 日根进格, 分钟根当场写进格
      pool_.tasks(static_cast<int>(kB), [&](int kk, int t) {
        const size_t k = static_cast<size_t>(kk);
        DayBuild &b = works[static_cast<size_t>(t)]->b;
        day(i0, k, t, b);
        size_t r = 0;
        for (size_t q = 0; q < b.keys.size(); ++q, ++r) {
          while (r < rs.size() && !(rs[r].day >> k & 1))
            ++r;
          assert(r < rs.size() && rs[r].key == b.keys[q] && "库段整块: 当日的根与行表不符");
          const size_t s = slot(rs[r], k);
          const Packed *m = b.minute(q);
          recs[s] = b.recs[q], has[k * rs.size() + r] = m != nullptr;
          out.put(at_min + s * sizeof(Packed), m ? m : &void_packed(), sizeof(Packed));
        }
        assert(std::ranges::count_if(rs, [&](const RowAt &x) { return x.day >> k & 1; }) == static_cast<ptrdiff_t>(b.keys.size()) && "库段整块: 当日的根与行表不符");
      });
      if (stale())
        return false;
      // 块末: 逐行过链 (行内即日序), 补分钟位图, 写行表 + 日根后换上
      for (size_t r = 0; r < rs.size(); ++r) {
        RowAt &x = rs[r];
        bits(x.day, [&](size_t j) {
          DayRec &d = recs[slot(x, j)];
          d.f = step(link_[kStock][x.key], d.ref, d.close, origin<kStock>());
          x.min |= uint64_t{has[j * rs.size() + r]} << j;
        });
      }
      out.put(0, &h, sizeof(h));
      out.put(sizeof(h), rs.data(), rs.size() * sizeof(RowAt));
      out.put(at_rec, recs.data(), recs.size() * sizeof(DayRec));
      out.done();
      store_->side[kStock].push_back({i0, std::make_shared<const Seg>(f)});
      dates_.insert(dates_.end(), dates.begin() + static_cast<ptrdiff_t>(i0), dates.begin() + static_cast<ptrdiff_t>(i0 + kB));
      ++sealed_[kStock];
    } else {
      pool_.tasks(static_cast<int>(n), [&](int k, int t) { day(i0, static_cast<size_t>(k), t, sb[static_cast<size_t>(k)]); });
      if (stale())
        return false;
      for (size_t k = 0; k < n; ++k)
        commit<kStock>(dates[i0 + k], sb[k], link_[kStock]);
    }
    for (size_t k = 0; pk && k < n; ++k)
      commit<kBasket>(dates[i0 + k], bb[k], link_[kBasket]);
    lib_days_ = dates_.size();
    seal<kStock>();
    if (pk)
      seal<kBasket>();
    save_head();
  }
  return true;
}

bool PriceLib::fill(const fundamental::Catalog &cat, const std::vector<misc::Ymd> &tail, size_t F, size_t keep) {
  const size_t N = rows(kStock), T = tail.size(), L = lib_days_;
  assert(F <= T && keep <= T && dates_.size() == L + keep);
  std::vector<Link> z = link_[kStock];
  for (size_t i = L; i < L + keep; ++i) // 留着的日: 链照盘上的根重放
    rechain<kStock>(*store_, i, z);
  if (keep < T) {
    fundamental::Ids ids(fundamental::Ids::Cover::Part); // 资产轴前 N 条, 下标 = 全局 id
    for (size_t a = 0; a < N; ++a)
      ids.add(asset_axis().code(a));
    // 基本面段要算的日的日根: bar1d 行 [日 − keep][全局 id] (无行 → 收 0)
    std::vector<Candle> px(F > keep ? (F - keep) * N : 0);
    if (F > keep) {
      fundamental::Scratch xs;
      const auto f0 = tail.begin() + static_cast<std::ptrdiff_t>(keep), f1 = tail.begin() + static_cast<std::ptrdiff_t>(F);
      for (const fundamental::Shard &sh : cat.shards(kBar1d)) {
        if (sh.ym() < misc::ym_of(*f0) || sh.ym() > misc::ym_of(f1[-1]))
          continue;
        each_daily(xs, sh, ids, [&](uint32_t a, const Candle &c) {
          const auto it = std::lower_bound(f0, f1, c.date);
          if (it != f1 && *it == c.date)
            px[static_cast<size_t>(it - f0) * N + a] = c;
        });
      }
    }
    const size_t W = std::min(static_cast<size_t>(pool_.threads()), T - keep);
    std::vector<std::unique_ptr<TailWork>> works;
    for (size_t j = 0; j < W; ++j)
      works.push_back(std::make_unique<TailWork>());
    // 每批 W 日: 并行读源合成 → 串行过链落盘
    for (size_t k0 = keep; k0 < T; k0 += W) {
      if (quit_.load(std::memory_order_relaxed))
        return false;
      const size_t n = std::min(W, T - k0);
      pool_.tasks(static_cast<int>(n), [&](int j, int) {
        const size_t k = k0 + static_cast<size_t>(j);
        TailWork &w = *works[static_cast<size_t>(j)];
        if (k < F) {
          w.bd.load(cat, ids, w.xs, tail[k]);
          w.b.put_stocks(FundDay{tail[k], std::span<const Candle>(px.data() + (k - keep) * N, N), w.bd});
        } else {
          archived_day(tail[k], N, w.spx, w.sbars);
          w.b.put_stocks(SinaDay{tail[k], w.spx, w.sbars});
        }
      });
      for (size_t j = 0; j < n; ++j) {
        commit<kStock>(tail[k0 + j], works[j]->b, z);
        publish(1);
      }
    }
    fund_days_ = F;
    save_head();
  }
  return true;
}

bool PriceLib::refold(const pool::Picks &pk) {
  const BasketTable &bt = basket_table();
  const size_t NB = bt.size(), W = static_cast<size_t>(pool_.threads());
  assert(bdays_ <= lib_days_ && bkeys_.size() == rows(kBasket));
  // 新下标 → 旧下标 (身份对得上); 对不上的 (fresh) 合成
  std::vector<std::pair<uint64_t, uint32_t>> was;
  for (size_t b = 0; b < bkeys_.size(); ++b)
    was.emplace_back(bkeys_[b], static_cast<uint32_t>(b));
  std::ranges::sort(was);
  assert(std::ranges::adjacent_find(was, {}, &std::pair<uint64_t, uint32_t>::first) == was.end() && "篮子身份重复");
  std::vector<uint32_t> from(NB, Ref::kNone);
  std::vector<uint8_t> fresh(NB, 1);
  for (size_t b = 0; b < NB; ++b)
    if (const auto it = std::ranges::lower_bound(was, bt.key(b), {}, &std::pair<uint64_t, uint32_t>::first); it != was.end() && it->first == bt.key(b))
      from[b] = it->second, fresh[b] = 0;
  const bool any = std::ranges::any_of(fresh, [](uint8_t f) { return f != 0; });
  std::vector<uint32_t> keys(NB);
  std::iota(keys.begin(), keys.end(), 0u);
  std::vector<Link> z(NB); // fresh 的链
  std::vector<std::unique_ptr<FoldWork>> works;
  for (size_t j = 0; any && j < W; ++j)
    works.push_back(std::make_unique<FoldWork>(NB));
  std::vector<DayBuild> bs(kB); // [段内日] fresh 的件
  std::vector<const RowAt *> old(NB);
  for (PriceStore::Part &p : store_->side[kBasket]) {
    if (quit_.load(std::memory_order_relaxed))
      return false;
    const size_t n = p.seg->days();
    if (p.seg->tag() == bt.hash()) { // 中断前已换上: 链照盘上的根重放
      for (size_t i = p.day; i < p.day + n; ++i)
        rechain<kBasket>(*store_, i, z);
      publish(n);
      continue;
    }
    // 段内各日并行合成 fresh (首日只定成员, 无根), 再按日序过链
    for (size_t j0 = 0; any && j0 < n; j0 += W)
      pool_.tasks(static_cast<int>(std::min(W, n - j0)), [&](int jj, int) {
        const size_t j = j0 + static_cast<size_t>(jj), i = p.day + j;
        bs[j].clear();
        if (i == 0)
          return;
        FoldWork &w = *works[static_cast<size_t>(jj)];
        members(pk, axis_, dates_[i], w.lane.mem, fresh);
        const auto [sg, d] = store_->at(kStock, i);
        w.lane.run(SegDay{dates_[i], *sg, d});
        bs[j].put_baskets(w.lane.day);
      });
    for (size_t j = 0; any && j < n; ++j)
      chain<kBasket>(bs[j], z);
    const Seg &sg = *p.seg;
    for (size_t b = 0; b < NB; ++b)
      old[b] = fresh[b] ? nullptr : sg.row(from[b]);
    const std::filesystem::path f = disk::path(p.day < sealed_[kBasket] * kB ? kBlockFile[kBasket] : kDayFile[kBasket], at_day(lib_, dates_[p.day]));
    write_seg(
        f, bt.hash(), n, keys,
        [&](size_t b, size_t j) -> const DayRec * { return fresh[b] ? bs[j].rec(static_cast<uint32_t>(b)) : old[b] && (old[b]->day >> j & 1) ? &sg.day(*old[b], j)
                                                                                                                                             : nullptr; },
        [&](size_t b, size_t j) -> const Packed * { return fresh[b] ? bs[j].min(static_cast<uint32_t>(b)) : old[b] ? sg.min(*old[b], j)
                                                                                                                   : nullptr; });
    p.seg = std::make_shared<const Seg>(f);
    publish(n);
  }
  std::vector<Link> l(NB); // 搬来的照旧, fresh 的接 z
  for (size_t b = 0; b < NB; ++b)
    l[b] = fresh[b] ? z[b] : link_[kBasket][from[b]];
  link_[kBasket] = std::move(l), bkeys_ = bt.keys(), table_ = bt.hash();
  save_head();
  return true;
}

bool PriceLib::fold(const pool::Picks &pk) {
  const size_t D = dates_.size(), L = lib_days_, N = rows(kStock), NB = rows(kBasket);
  assert(bdays_ >= 1 && bdays_ <= D);
  Members tm; // 补段的成员: 库末日之后冻结, 逐日过 drift (漂移比 = 个股侧当日的 收 / 前收; 无日根 1)
  members(pk, axis_, cal_->next(dates_[L - 1]), tm);
  std::vector<float> rel(N);
  const auto pass = [&](size_t i) {
    std::ranges::fill(rel, 1.f);
    const auto [sg, j] = store_->at(kStock, i);
    for (const RowAt &r : sg->rows())
      if (r.day >> j & 1)
        rel[r.key] = rel_of(sg->day(r, j));
    drift(tm, dates_[i], cal_->next(dates_[i]), rel);
  };
  std::vector<Link> z = link_[kBasket]; // 补段的链 (库段的链落在 link_)
  for (size_t i = L; i < bdays_; ++i) { // 留着的补段日: 链 / 成员照盘上的根重放
    rechain<kBasket>(*store_, i, z);
    pass(i);
  }
  if (bdays_ < D) {
    const size_t W = std::min(static_cast<size_t>(pool_.threads()), D - bdays_);
    std::vector<std::unique_ptr<FoldWork>> works;
    for (size_t j = 0; j < W; ++j)
      works.push_back(std::make_unique<FoldWork>(NB));
    // 每批 W 日: 串行接补段日的成员 → 并行合成 (库段日各取选股表) → 串行过链落盘
    while (bdays_ < D) {
      if (quit_.load(std::memory_order_relaxed))
        return false;
      const size_t i0 = bdays_, n = std::min(W, D - i0);
      for (size_t j = 0; j < n; ++j)
        if (const size_t i = i0 + j; i >= L) {
          works[j]->lane.mem = tm;
          pass(i);
        }
      pool_.tasks(static_cast<int>(n), [&](int jj, int) {
        const size_t j = static_cast<size_t>(jj), i = i0 + j;
        FoldWork &w = *works[j];
        if (i < L)
          members(pk, axis_, dates_[i], w.lane.mem);
        const auto [sg, d] = store_->at(kStock, i);
        w.lane.run(SegDay{dates_[i], *sg, d});
        w.b.put_baskets(w.lane.day);
      });
      for (size_t j = 0; j < n; ++j) {
        const size_t i = i0 + j;
        commit<kBasket>(dates_[i], works[j]->b, i < L ? link_[kBasket] : z);
        if (i + 1 == L)
          z = link_[kBasket];
        publish(1);
      }
      seal<kBasket>();
      save_head();
    }
  }
  const std::vector<Link> &e = D > L ? z : link_[kBasket];
  level_.resize(NB);
  for (size_t b = 0; b < NB; ++b)
    level_[b] = e[b].f > 0 ? e[b].f * e[b].ref : kBase;
  mem_ = std::move(tm);
  return true;
}

bool PriceLib::run(PriceRequest &req) {
  assert(std::ranges::count(req.wants, false, &Want::minute) <= static_cast<ptrdiff_t>(kKeep) &&
         std::ranges::count(req.wants, true, &Want::minute) <= static_cast<ptrdiff_t>(kKeepMinutes) && "请求超出留存");
  assert(std::ranges::none_of(req.wants, [](const Want &w) { return w.ref.side == kAccount; }) && "账户一侧整条随发布");
  const pool::AllBar ab = pool::all_bar("行情历史由 all.bar 特征库合成");
  if (!ab.lack.empty()) {
    if (!result || result->lack != ab.lack)
      put(std::make_shared<PriceView>(PriceView{.lack = ab.lack}));
    return true;
  }
  if (!sync(ab.lib, ab.dates, req.mark))
    return false;
  if (!(built_ == at_))
    rebuild();
  if (!account_ || ledger_ != req.ledger || !(spot_ == req.spot))
    replay(req.ledger, req.spot);
  for (const Want &w : req.wants) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    if (w.minute)
      minute_.get(w.ref, at_, [&](Series &s) { return series(w.ref, true, s); });
    else if (w.ref.side == kStock)
      daily_.get(w.ref, at_, [&](Series &s) { return series(w.ref, false, s); });
  }
  // 发布 = 本戳留着的全部 (不只本次要的: 切页不闪), 键升序; 连分钟的盖过不连的
  using Item = std::pair<uint32_t, std::shared_ptr<const Series>>;
  std::vector<Item> ks, bm;
  minute_.each(at_, [&](Ref r, const std::shared_ptr<const Series> &s) { (r.side == kStock ? ks : bm).emplace_back(r.key, s); });
  daily_.each(at_, [&](Ref r, const std::shared_ptr<const Series> &s) { ks.emplace_back(r.key, s); });
  std::ranges::stable_sort(ks, {}, &Item::first);
  ks.erase(std::ranges::unique(ks, {}, &Item::first).begin(), ks.end());
  auto v = std::make_shared<PriceView>();
  v->stamp = at_;
  v->dates = spine_;
  v->tails = tails_, v->census = census_, v->equity = equity_;
  auto st = std::make_shared<Tape>();
  for (auto &[k, s] : ks)
    st->keys.push_back(k), st->list.push_back(std::move(s));
  std::shared_ptr<const Tape> bt = baskets_;
  if (!bm.empty()) {
    auto t = std::make_shared<Tape>(*baskets_);
    for (auto &[k, s] : bm)
      if (const size_t j = t->find(k); s && j < t->keys.size())
        t->list[j] = std::move(s);
    bt = std::move(t);
  }
  const PriceView *o = result && result->lack.empty() && result->stamp == at_ ? result.get() : nullptr;
  const bool same_stocks = o && o->tape[kStock]->keys == st->keys && o->tape[kStock]->list == st->list;
  const bool same_baskets = o && o->tape[kBasket]->list == bt->list;
  v->tape[kStock] = same_stocks ? o->tape[kStock] : std::move(st);
  v->tape[kBasket] = same_baskets ? o->tape[kBasket] : std::move(bt);
  v->tape[kAccount] = account_;
  if (o && same_stocks && same_baskets && o->tape[kAccount] == account_ && o->tails == tails_ && o->census == census_)
    return true;
  put(std::move(v));
  return true;
}

void PriceLib::put(std::shared_ptr<const PriceView> v) {
  std::lock_guard<std::mutex> lock(mutex);
  result = std::move(v);
  Progress::publish();
}

void PriceLib::rebuild() {
  built_ = at_;
  account_ = nullptr;
  spine_ = std::make_shared<const std::vector<misc::Ymd>>(dates_);
  // 个股日脊末段: 自末段往前, 各只取够 kTailDays 个有根日
  const size_t N = rows(kStock);
  std::vector<std::array<Candle, kTailDays>> c(N);
  std::vector<uint8_t> n(N, 0);
  for (auto p = store_->side[kStock].rbegin(); p != store_->side[kStock].rend(); ++p) {
    const Seg &sg = *p->seg;
    for (const RowAt &r : sg.rows())
      for (uint64_t m = r.day; m && n[r.key] < kTailDays;) {
        const size_t j = static_cast<size_t>(63 - std::countl_zero(m));
        c[r.key][n[r.key]++] = candle_of<kStock>(sg.day(r, j), dates_[p->day + j]);
        m &= ~(uint64_t{1} << j);
      }
  }
  auto t = std::make_shared<Tails>();
  for (uint32_t id = 0; id < N; ++id) {
    if (!n[id])
      continue;
    t->ids.push_back(id);
    for (size_t k = n[id]; k-- > 0;)
      t->daily.v.push_back(c[id][k]);
    t->daily.off.push_back(static_cast<uint32_t>(t->daily.v.size()));
  }
  tails_ = std::move(t);
  auto b = std::make_shared<Tape>();
  b->keys.resize(rows(kBasket));
  std::iota(b->keys.begin(), b->keys.end(), 0u);
  b->list.resize(b->keys.size());
  pool_.tasks(static_cast<int>(b->keys.size()), [&](int j, int) {
    if (auto s = std::make_shared<Series>(); series({kBasket, b->keys[static_cast<size_t>(j)]}, false, *s))
      b->list[static_cast<size_t>(j)] = std::move(s);
  });
  baskets_ = std::move(b);
  census_ = std::make_shared<const Census>(census(mem_, rows(kBasket)));
}

bool PriceLib::series(Ref ref, bool minute, Series &out, size_t from) const {
  assert(ref.ok() && ref.side < kMarkets);
  if (ref.key >= rows(ref.side))
    return false;
  out.minute = minute;
  each_side([&]<Side S>() {
    if (S != ref.side)
      return;
    std::vector<const PriceStore::Part *> on; // 有该行的段 (先一并发出读)
    std::vector<const RowAt *> at;
    for (const PriceStore::Part &p : store_->side[S])
      if (p.day + p.seg->days() > from)
        if (const RowAt *r = p.seg->row(ref.key)) {
          p.seg->will(*r, minute);
          on.push_back(&p), at.push_back(r);
        }
    for (size_t q = 0; q < on.size(); ++q) {
      const PriceStore::Part &p = *on[q];
      const RowAt &r = *at[q];
      if (minute && r.min)
        out.hold.push_back(p.seg);
      bits(r.day, [&](size_t j) {
        if (const size_t i = p.day + j; i >= from) {
          const DayRec &x = p.seg->day(r, j);
          out.daily.push_back(candle_of<S>(x, dates_[i]));
          out.adj.push_back(S == kStock ? x.f : 1.0);
          if (minute)
            out.mins.push_back({p.seg->min(r, j), x.k, scale_of<S>(x)});
        }
      });
    }
  });
  return !out.daily.empty();
}

void PriceLib::replay(uint64_t ledger, const account::Spot &spot) {
  ledger_ = ledger, spot_ = spot;
  Account::Replay r = Account::replay(account::load(), dates_, spot, [&](uint32_t id, size_t from, Series &s) { return series({kStock, id}, true, s, from); });
  auto t = std::make_shared<Tape>();
  t->keys = {0};
  t->list = {std::move(r.se)};
  equity_ = r.eq;
  account_ = std::move(t);
}

} // namespace GUI::Quote
