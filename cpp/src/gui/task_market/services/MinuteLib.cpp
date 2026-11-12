#include "gui/task_market/services/MinuteLib.hpp"

#include "features/Panel/PoolSelect.hpp" // all_bar / picks
#include "features/Storage/FeatureLevels.hpp" // feature_storage_t
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "gui/quote/BasketRow.hpp"
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_table

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <type_traits>

namespace GUI::Market {

namespace {

using Quote::kBasket;
using Quote::kStock;
using Quote::Side;

constexpr uint64_t kMagic = 0x58504E494D544B53ULL;                                         // "SKTMINPX" 小端
constexpr size_t kSlots = Quote::kSlots, kCell = kSlots * MinuteLib::kFields;              // 一行一日的值数
constexpr disk::Node kFile[Quote::kSides] = {disk::LibMinuteStock, disk::LibMinuteBasket}; // [侧]

// 两侧的价: 个股 = 库存原价 (f16, 与库逐位一致), 篮子 = 日内点位 (f32)
template <Side S>
using Px = std::conditional_t<S == kStock, feature_storage_t, float>;

// 一行一日
template <Side S>
struct Rec {
  double f;       // 因子 (0 = 当日无根)
  Px<S> x[kCell]; // [槽][高 低 收] (无根 = NaN)
};

// 一侧 N 行 days 日的文件长
template <Side S>
size_t bytes(size_t N, size_t days) { return days * N * sizeof(Rec<S>); }

// 两侧逐个: f.template operator()<侧>()
template <class F>
void each_side(F &&f) {
  f.template operator()<kStock>();
  f.template operator()<kBasket>();
}

using misc::snap::items;

// 一行一日的价 ← 分钟根 (无根的槽 = NaN)
template <Side S>
void put(Rec<S> &r, const Quote::Bars &b) {
  std::fill(r.x, r.x + kCell, static_cast<Px<S>>(NAN));
  for (size_t s = 0; s < kSlots; ++s)
    if (b.has[s]) {
      const Quote::Cell &c = b.cell[s];
      Px<S> *p = r.x + s * MinuteLib::kFields;
      p[MinuteLib::kHigh] = static_cast<Px<S>>(c.high), p[MinuteLib::kLow] = static_cast<Px<S>>(c.low), p[MinuteLib::kClose] = static_cast<Px<S>>(c.close);
    }
}

// 一侧一日的合成: 记录 (因子按日序补) + 收盘
template <Side S>
struct Out {
  explicit Out(size_t N) : rec(N), last(N) {}
  std::vector<Rec<S>> rec; // [N]
  std::vector<float> last; // [N] D 的收盘 (0 = 当日无根)
  // 第 r 行 ← 一日: c = 收盘 (0 = 当日无根), b = 分钟根 (c > 0 必给)
  void set(size_t r, float c, const Quote::Bars *b) {
    rec[r] = {}, last[r] = c;
    if (c > 0)
      put(rec[r], *b);
  }
};

// 一侧第 row 行 (共 N 行) 的 D 日逐个有根日 f(日下标, 因子, 价): 整文件只读映射, 逐日跨 N 行取一条
template <Side S>
void scan(const disk::Key &lib, size_t N, size_t D, size_t row, const MinuteLib::Each &f) {
  const size_t n = bytes<S>(N, D);
  if (n == 0)
    return;
  const int fd = ::open(disk::path(kFile[S], lib).c_str(), O_RDONLY);
  assert(fd >= 0 && "minute/: 打不开");
  struct stat sb {};
  const int st = ::fstat(fd, &sb);
  assert(st == 0 && static_cast<size_t>(sb.st_size) >= n && "minute/: 短于头");
  void *m = ::mmap(nullptr, n, PROT_READ, MAP_SHARED, fd, 0);
  ::close(fd);
  assert(m != MAP_FAILED && "minute/: 映射失败");
  const Rec<S> *rec = static_cast<const Rec<S> *>(m);
  float px[kCell];
  for (size_t i = 0; i < D; ++i)
    if (const Rec<S> &r = rec[i * N + row]; r.f > 0) {
      for (size_t k = 0; k < kCell; ++k)
        px[k] = static_cast<float>(r.x[k]);
      f(i, r.f, px);
    }
  ::munmap(m, n);
}

} // namespace

template <class Ar>
void MinuteLib::snap(Ar &ar) {
  for (std::vector<uint32_t> &k : keys_)
    items(ar, k);
  items(ar, dates_);
  for (std::vector<Link> &l : link_)
    items(ar, l);
}

void MinuteLib::save_head() {
  Quote::snap_write(disk::LibMinuteHead, lib_, head_, [&](auto &ar) { snap(ar); });
}

size_t MinuteLib::row_of(Quote::Ref ref) const {
  assert(ref.ok());
  const std::vector<uint32_t> &ks = keys_[ref.side];
  return static_cast<size_t>(std::find(ks.begin(), ks.end(), ref.key) - ks.begin());
}

bool MinuteLib::sync(const FeatureLib &lib, const FeatureRead &rd, const pool::Picks &pk, const std::atomic<bool> &cancel, Progress &x,
                     misc::exec::ForkJoin &pool) {
  std::lock_guard<std::mutex> lock(mu_);
  const std::vector<misc::Ymd> &dates = pk.tape->dates;
  assert(dates.size() >= 2);
  lib_ = lib_key(lib.id);
  const BasketTable &bt = basket_table();
  std::vector<uint32_t> baskets(bt.size());
  std::iota(baskets.begin(), baskets.end(), 0u);
  const Quote::PerSide<std::vector<uint32_t>> keys{lib.axis.ids, std::move(baskets)};
  const size_t NA = keys[kStock].size(), NB = keys[kBasket].size();
  const Head want{kMagic, bt.hash(), dates.front()};
  const auto fits = [&] { return head_ == want && keys_ == keys && pool::extends(dates_, dates); };
  if (!fits()) {
    head_ = {}, keys_ = {}, dates_.clear(), link_ = {};
    if (Quote::snap_read(disk::LibMinuteHead, lib_, want, [&](auto &ar) { snap(ar); })) {
      head_ = want;
      assert(link_[kStock].size() == keys_[kStock].size() && link_[kBasket].size() == keys_[kBasket].size() && "minute/head.bin: 链长与键表不符");
    }
    bool whole = fits();
    each_side([&]<Side S>() {
      std::error_code ec;
      const uintmax_t have = std::filesystem::file_size(disk::path(kFile[S], lib_), ec);
      whole = whole && !ec && have >= bytes<S>(keys_[S].size(), dates_.size());
    });
    if (!whole) {
      head_ = want, keys_ = keys, dates_ = {dates.front()};
      disk::remove(disk::LibMinute, lib_);
      disk::ensure(disk::LibMinute, lib_);
      each_side([&]<Side S>() {
        link_[S].assign(keys_[S].size(), Link{});
        std::ofstream f(disk::path(kFile[S], lib_), std::ios::binary | std::ios::trunc);
        const std::vector<Rec<S>> none(keys_[S].size(), Rec<S>{});
        f.write(reinterpret_cast<const char *>(none.data()), static_cast<std::streamsize>(none.size() * sizeof(Rec<S>)));
        assert(f && "minute/: 写失败");
      });
      save_head();
    }
    each_side([&]<Side S>() { std::filesystem::resize_file(disk::path(kFile[S], lib_), bytes<S>(keys_[S].size(), dates_.size())); }); // 头之后写了一半的块
  }
  if (dates_.size() == dates.size())
    return true;

  // 库段一趟 (Quote::lib_pass, 与 BasketHistory 同一趟): 个股的记录在并行段逐只备 (批内第 k 件一份), 篮子的在串行段取 Lane 的;
  //   按日序过链落盘, 每批落一次头
  Quote::PerSide<std::ofstream> f;
  each_side([&]<Side S>() { f[S].open(disk::path(kFile[S], lib_), std::ios::binary | std::ios::app); });
  const size_t T = static_cast<size_t>(pool.threads());
  std::vector<Out<kStock>> so(T, Out<kStock>(NA));
  Out<kBasket> bo(NB);
  const auto each = [&](size_t k, size_t a, const Quote::Leg &g) {
    so[k].set(a, g.day && g.bars->has.any() ? g.day->close : 0.f, g.bars);
  };
  const auto chain = [&](size_t i, size_t k, Quote::LibWork &w) {
    for (size_t b = 0; b < NB; ++b) {
      const Quote::Day &y = w.lane.day[b];
      bo.set(b, y.candle.date ? y.candle.close : 0.f, &y.bars);
    }
    each_side([&]<Side S>() {
      Out<S> &o = [&]() -> Out<S> & { if constexpr (S == kStock) return so[k]; else return bo; }();
      std::vector<Link> &ln = link_[S];
      for (size_t r = 0; r < o.rec.size(); ++r) {
        Link &z = ln[r];
        if (const float p = S == kStock ? w.prev.pre[r] : 1.f; z.ref > 0 && p > 0)
          z.f *= z.ref / p, z.ref = p;
        if (o.last[r] > 0) {
          if (!(z.f > 0))
            z.f = 1.0;
          z.ref = o.last[r];
          o.rec[r].f = z.f;
        }
      }
      f[S].write(reinterpret_cast<const char *>(o.rec.data()), static_cast<std::streamsize>(o.rec.size() * sizeof(Rec<S>)));
    });
    dates_.push_back(dates[i]);
  };
  const auto batch = [&] {
    each_side([&]<Side S>() {
      f[S].flush();
      assert(f[S] && "minute/: 写失败");
    });
    save_head();
  };
  if (Quote::lib_pass(lib.axis, rd, pk, dates_.size(), dates_.size(), pool, cancel, x, each, chain, batch))
    return true;
  if (rd.stale()) // 库在重算: 下次从头核对
    head_ = {};
  return false;
}

std::optional<MinuteOpen> MinuteLib::open(std::string_view what, const std::atomic<bool> &cancel, Progress &x, misc::exec::ForkJoin &pool) {
  const pool::AllBar ab = pool::all_bar(what);
  if (!ab.lack.empty())
    return MinuteOpen{.lack = ab.lack};
  const FeatureLib &lib = ab.lib;
  FeatureRead rd(lib, &cancel);
  const std::shared_ptr<const pool::Picks> pk = pool::picks(lib, &cancel);
  if (!pk || !sync(lib, rd, *pk, cancel, x, pool))
    return std::nullopt;
  const std::vector<misc::Ymd> &dates = pk->tape->dates; // 已落盘日 (清单日期轴可能还在算)
  return MinuteOpen{.dates = dates, .stamp = {lib.axis.hash, dates.back(), dates.size()}};
}

bool MinuteLib::each(Quote::Ref ref, const Each &f) const {
  std::lock_guard<std::mutex> lock(mu_);
  const size_t row = row_of(ref), N = keys_[ref.side].size();
  if (row == N)
    return false;
  each_side([&]<Side S>() {
    if (S == ref.side)
      scan<S>(lib_, N, dates_.size(), row, f);
  });
  return true;
}

} // namespace GUI::Market