#include "gui/task_market/services/CtaLib.hpp"

#include "features/Storage/FeatureLevels.hpp" // feature_storage_t
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "gui/quote/BasketRow.hpp"
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_table

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <type_traits>

namespace GUI::Market {

namespace {

using Quote::kBasket;
using Quote::kStock;
using Quote::Side;

constexpr uint64_t kMagic = 0x5850415443544B53ULL;                                   // "SKTCTAPX" 小端
constexpr size_t kSlots = Quote::kSlots, kCell = kSlots * CtaLib::kFields;           // 一行一日的值数
constexpr disk::Node kFile[Quote::kSides] = {disk::LibCtaStock, disk::LibCtaBasket}; // [侧]

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
  std::fill(r.x, r.x + kCell, static_cast<Px<S>>(cta::kNaN));
  for (size_t s = 0; s < kSlots; ++s)
    if (b.has[s]) {
      const Quote::Cell &c = b.cell[s];
      Px<S> *p = r.x + s * CtaLib::kFields;
      p[CtaLib::kHigh] = static_cast<Px<S>>(c.high), p[CtaLib::kLow] = static_cast<Px<S>>(c.low), p[CtaLib::kClose] = static_cast<Px<S>>(c.close);
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

// 一根 (高 低 收) × 因子 f → 有效根: 收 ≤ 0 / NaN 不收; 高 / 低缺 → 收, 且包住收
template <class T>
void push(cta::Series &se, size_t t, const T *p, double f) {
  const float c = static_cast<float>(p[CtaLib::kClose]);
  if (!(c > 0))
    return;
  const float h = static_cast<float>(p[CtaLib::kHigh]), l = static_cast<float>(p[CtaLib::kLow]);
  const auto at = [f](float v) { return static_cast<float>(v * f); };
  se.c.push_back(at(c));
  se.h.push_back(at(h > 0 ? std::max(h, c) : c));
  se.l.push_back(at(l > 0 ? std::min(l, c) : c));
  se.t.push_back(static_cast<uint32_t>(t));
}

// 一侧第 row 行 (共 N 行) 的 D 日 → 有效根
template <Side S>
void load(const disk::Key &lib, size_t N, size_t D, size_t row, cta::Series &se) {
  std::ifstream f(disk::path(kFile[S], lib), std::ios::binary);
  Rec<S> r;
  for (size_t i = 0; i < D; ++i) {
    f.seekg(static_cast<std::streamoff>((i * N + row) * sizeof(Rec<S>)));
    f.read(reinterpret_cast<char *>(&r), sizeof(Rec<S>));
    assert(f && "cta/: 短于头");
    if (r.f > 0)
      for (size_t s = 0; s < kSlots; ++s)
        push(se, i * kSlots + s, r.x + s * CtaLib::kFields, r.f);
  }
}

} // namespace

template <class Ar>
void CtaLib::snap(Ar &ar) {
  for (std::vector<uint32_t> &k : keys_)
    items(ar, k);
  items(ar, dates_);
  for (std::vector<Link> &l : link_)
    items(ar, l);
}

void CtaLib::save_head() {
  Quote::snap_write(disk::LibCtaHead, lib_, head_, [&](auto &ar) { snap(ar); });
}

bool CtaLib::sync(const FeatureLib &lib, const FeatureRead &rd, const pool::Picks &pk) {
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
    if (Quote::snap_read(disk::LibCtaHead, lib_, want, [&](auto &ar) { snap(ar); })) {
      head_ = want;
      assert(link_[kStock].size() == keys_[kStock].size() && link_[kBasket].size() == keys_[kBasket].size() && "cta/head.bin: 链长与键表不符");
    }
    bool whole = fits();
    each_side([&]<Side S>() {
      std::error_code ec;
      const uintmax_t have = std::filesystem::file_size(disk::path(kFile[S], lib_), ec);
      whole = whole && !ec && have >= bytes<S>(keys_[S].size(), dates_.size());
    });
    if (!whole) {
      head_ = want, keys_ = keys, dates_ = {dates.front()};
      disk::remove(disk::LibCta, lib_);
      disk::ensure(disk::LibCta, lib_);
      each_side([&]<Side S>() {
        link_[S].assign(keys_[S].size(), Link{});
        std::ofstream f(disk::path(kFile[S], lib_), std::ios::binary | std::ios::trunc);
        const std::vector<Rec<S>> none(keys_[S].size(), Rec<S>{});
        f.write(reinterpret_cast<const char *>(none.data()), static_cast<std::streamsize>(none.size() * sizeof(Rec<S>)));
        assert(f && "cta/: 写失败");
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
  const size_t T = static_cast<size_t>(pool_.threads());
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
      assert(f[S] && "cta/: 写失败");
    });
    save_head();
  };
  if (Quote::lib_pass(lib.axis, rd, pk, dates_.size(), dates_.size(), pool_, cancel_, x_, each, chain, batch))
    return true;
  if (rd.stale()) // 库在重算: 下次从头核对
    head_ = {};
  return false;
}

bool CtaLib::series(Quote::Ref ref, cta::Series &se) const {
  assert(ref.ok());
  const std::vector<uint32_t> &ks = keys_[ref.side];
  const size_t row = static_cast<size_t>(std::find(ks.begin(), ks.end(), ref.key) - ks.begin());
  if (row == ks.size())
    return false;
  se = {};
  each_side([&]<Side S>() {
    if (S == ref.side)
      load<S>(lib_, ks.size(), dates_.size(), row, se);
  });
  return true;
}

} // namespace GUI::Market