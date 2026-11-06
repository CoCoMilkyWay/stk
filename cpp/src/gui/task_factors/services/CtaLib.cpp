#include "gui/task_factors/services/CtaLib.hpp"

#include "features/Storage/FeatureLevels.hpp" // feature_storage_t
#include "features/Storage/FeatureRead.hpp"
#include "gui/task_database/services/BasketRow.hpp"
#include "gui/task_database/services/Kline.hpp"
#include "misc/fs.hpp"
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_group / basket_count
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <span>

namespace GUI::Factors {

namespace {

constexpr uint64_t kMagic = 0x4D31415443544B53ULL;                            // "SKTCTA1M" 小端
constexpr size_t kSlots = Database::kSlots, kCell = kSlots * CtaLib::kFields; // 一线 / 一只一日的值数

// stock.bin 的一只一日
struct Rec {
  double f;                   // 后复权因子 (0 = 当日无根)
  feature_storage_t x[kCell]; // [槽][高 低 收] 原价 (无根 = NaN)
};
static_assert(sizeof(Rec) == sizeof(double) + kCell * sizeof(feature_storage_t));

// 定长元素的序列: 长度 + 逐个
template <class Ar, class T>
void items(Ar &ar, std::vector<T> &v) {
  uint64_t n = v.size();
  ar(n);
  if constexpr (Ar::kLoad)
    v.resize(n);
  for (T &x : v)
    ar(x);
}

// 一根 (高 低 收) × lvl → 有效根: 收 ≤ 0 / NaN 不收; 高 / 低缺 → 收, 且包住收
template <class T>
void push(cta::Series &se, size_t t, const T *p, double lvl) {
  const float c = static_cast<float>(p[CtaLib::kClose]);
  if (!(c > 0))
    return;
  const float h = static_cast<float>(p[CtaLib::kHigh]), l = static_cast<float>(p[CtaLib::kLow]);
  const auto at = [lvl](float v) { return static_cast<float>(v * lvl); };
  se.c.push_back(at(c));
  se.h.push_back(at(h > 0 ? std::max(h, c) : c));
  se.l.push_back(at(l > 0 ? std::min(l, c) : c));
  se.t.push_back(static_cast<uint32_t>(t));
}

// 一个线程的合成件 (逐日复用)
struct Lane {
  Lane(size_t A, size_t B, size_t nd, size_t nm) : fold(B), day(B) {
    dc.preallocate(A, L_1day, nd);
    mc.preallocate(A, L_1min, nm);
  }
  FeatureRead::DayColumns dc, mc;
  Database::Row prev, cur; // D−1 / D 收盘行
  Database::Fold fold;
  std::vector<Database::Day> day; // [B]
  Database::Bars bars;            // 一只一日的分钟根
  Database::Members mem;
  std::vector<Database::Members::Weight> ws; // 一只滤到线表的所属线
};

// 一日的合成: 篮子 B 条线 (前日点位 = 1) + 个股 A 只原价 (因子按日序补)
struct DayOut {
  std::vector<float> x;      // [B][kSlots][kFields] (无根 = NaN)
  std::vector<double> close; // [B] 日收 (0 = 该线当日无根)
  std::vector<Rec> rec;      // [A]
  std::vector<float> pre;    // [A] D 的前收 (D−1 行)
  std::vector<float> last;   // [A] D 的收盘 (0 = 当日无根)
};

} // namespace

void CtaLib::save_head() {
  std::vector<std::byte> blob;
  misc::snap::Save ar(blob);
  ar(head_);
  items(ar, lines_);
  items(ar, dates_);
  items(ar, level_);
  items(ar, adj_);
  items(ar, ref_);
  misc::atomic_write(dir_ + "/head.bin", reinterpret_cast<const char *>(blob.data()), blob.size());
}

bool CtaLib::sync(const FeatureLib &lib, const FeatureRead &rd, const std::vector<misc::Ymd> &dates) {
  assert(dates.size() >= 2);
  dir_ = lib.dir + "/cta";
  const std::string bin = dir_ + "/1m.bin", sbin = dir_ + "/stock.bin";
  std::vector<uint32_t> lines;
  for (int g = 0; g < kBasketGroups; ++g)
    for (const size_t l : basket_group(static_cast<BasketGroup>(g)))
      lines.push_back(static_cast<uint32_t>(l));
  const size_t NA = lib.axis.size(), NB = lines.size();
  const Head want{kMagic, lib.axis.hash, concepts().hash, static_cast<uint32_t>(NA), static_cast<uint32_t>(NB), dates.front()};
  const auto fits = [&] {
    return head_ == want && lines_ == lines && !dates_.empty() && dates_.size() <= dates.size() && std::equal(dates_.begin(), dates_.end(), dates.begin());
  };
  const auto block = [&](size_t days) { return days * NB * kCell * sizeof(float); };
  const auto sblock = [&](size_t days) { return days * NA * sizeof(Rec); };
  if (!fits()) {
    head_ = {}, lines_.clear(), dates_.clear();
    if (std::ifstream f(dir_ + "/head.bin", std::ios::binary); f) {
      const std::vector<char> blob((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
      misc::snap::Load ar(std::as_bytes(std::span(blob)));
      ar(head_);
      if (head_ == want) {
        items(ar, lines_);
        items(ar, dates_);
        items(ar, level_);
        items(ar, adj_);
        items(ar, ref_);
        assert(ar.done() && "cta/head.bin: 文件尾有多余字节");
        assert(level_.size() == NB && adj_.size() == NA && ref_.size() == NA && "cta/head.bin: 状态长度与头不符");
      }
    }
    std::error_code ec, sec;
    const uintmax_t have = std::filesystem::file_size(bin, ec), shave = std::filesystem::file_size(sbin, sec);
    if (!fits() || ec || sec || have < block(dates_.size()) || shave < sblock(dates_.size())) {
      head_ = want, lines_ = lines, dates_ = {dates.front()};
      level_.assign(NB, 1.0), adj_.assign(NA, 0.0), ref_.assign(NA, 0.f);
      std::filesystem::create_directories(dir_);
      std::ofstream f(bin, std::ios::binary | std::ios::trunc), g(sbin, std::ios::binary | std::ios::trunc);
      const std::vector<float> nan(NB * kCell, cta::kNaN);
      const std::vector<Rec> none(NA, Rec{});
      f.write(reinterpret_cast<const char *>(nan.data()), static_cast<std::streamsize>(nan.size() * sizeof(float)));
      g.write(reinterpret_cast<const char *>(none.data()), static_cast<std::streamsize>(none.size() * sizeof(Rec)));
      assert(f && g && "cta/: 写失败");
      f.close(), g.close();
      save_head();
    }
    std::filesystem::resize_file(bin, block(dates_.size())); // 头之后写了一半的块
    std::filesystem::resize_file(sbin, sblock(dates_.size()));
  }
  row_.assign(basket_count(), static_cast<uint32_t>(NB));
  for (size_t j = 0; j < NB; ++j)
    row_[lines_[j]] = static_cast<uint32_t>(j);
  if (dates_.size() == dates.size())
    return true;

  // dates[i] (i ≥ 已合成日数) 逐日: 按线程一日一件 (读 D−1 / D 收盘行 + D 的 L_1min) → 按日序落盘, 每批落一次头
  std::ofstream f(bin, std::ios::binary | std::ios::app), g(sbin, std::ios::binary | std::ios::app);
  const size_t NP = pool_count(), N = dates.size(), T = static_cast<size_t>(pool_.threads());
  const std::vector<size_t> dcols = Database::row_cols(), mcols = Database::minute_cols();
  std::vector<std::unique_ptr<Lane>> lanes;
  for (size_t t = 0; t < T; ++t)
    lanes.push_back(std::make_unique<Lane>(NA, NB, dcols.size(), mcols.size()));
  std::vector<DayOut> out(T);
  const std::vector<double> one(NB, 1.0);
  const feature_storage_t nan16 = static_cast<feature_storage_t>(cta::kNaN);
  x_.begin(Phase::Loading, N - dates_.size());
  for (size_t i0 = dates_.size(); i0 < N; i0 += T) {
    if (cancel_.load(std::memory_order_relaxed) || rd.stale())
      break;
    const size_t n = std::min(T, N - i0);
    pool_.tasks(static_cast<int>(n), [&](int k, int tid) {
      Lane &L = *lanes[static_cast<size_t>(tid)];
      DayOut &o = out[static_cast<size_t>(k)];
      const size_t i = i0 + static_cast<size_t>(k);
      const misc::Ymd d = dates[i];
      rd.load_day_columns(dates[i - 1], dcols, L.dc);
      Database::read_row(L.dc, NA, NP, L.prev);
      rd.load_day_columns(d, dcols, L.dc);
      Database::read_row(L.dc, NA, NP, L.cur);
      rd.load_day_columns(d, mcols, L.mc);
      Database::members(L.prev, lib.axis, NP, L.mem);
      L.fold.open(Database::kSlots);
      o.rec.assign(NA, Rec{});
      o.pre.assign(L.prev.pre.begin(), L.prev.pre.end());
      o.last.assign(NA, 0.f);
      for (size_t a = 0; a < NA; ++a) {
        const float c = L.cur.px[3][a];
        const bool on = L.cur.trade[a] && c > 0;
        const Database::Cell day = Database::cell(L.cur.px[0][a], L.cur.px[1][a], L.cur.px[2][a], c, 0, L.cur.amount[a]);
        Database::bars_of(L.mc, a, d, L.bars);
        if (on && L.bars.has.any()) {
          o.last[a] = c;
          feature_storage_t *p = o.rec[a].x;
          std::fill(p, p + kCell, nan16);
          for (size_t s = 0; s < kSlots; ++s)
            if (L.bars.has[s]) {
              const Database::Cell &b = L.bars.cell[s];
              p[s * kFields + kHigh] = static_cast<feature_storage_t>(b.high);
              p[s * kFields + kLow] = static_cast<feature_storage_t>(b.low);
              p[s * kFields + kClose] = static_cast<feature_storage_t>(b.close);
            }
        }
        L.ws.clear();
        for (const Database::Members::Weight &w : L.mem.of(a))
          if (const uint32_t j = row_[w.basket]; j < NB)
            L.ws.push_back({j, w.w});
        if (!L.ws.empty())
          L.fold.add({L.prev.pre[a], on ? &day : nullptr, &L.bars}, L.ws);
      }
      L.fold.finish(d, dates[i - 1], one, L.day);
      o.x.assign(NB * kCell, cta::kNaN);
      o.close.assign(NB, 0);
      for (size_t b = 0; b < NB; ++b) {
        const Database::Day &y = L.day[b];
        if (!y.candle.date)
          continue;
        o.close[b] = y.candle.close;
        float *p = o.x.data() + b * kCell;
        for (size_t s = 0; s < kSlots; ++s)
          if (y.bars.has[s]) {
            const Database::Cell &c = y.bars.cell[s];
            p[s * kFields + kHigh] = c.high, p[s * kFields + kLow] = c.low, p[s * kFields + kClose] = c.close;
          }
      }
    });
    if (rd.stale())
      break;
    for (size_t k = 0; k < n; ++k) {
      DayOut &o = out[k];
      for (size_t b = 0; b < NB; ++b) {
        if (!(o.close[b] > 0))
          continue;
        for (float *p = o.x.data() + b * kCell, *e = p + kCell; p < e; ++p)
          *p = static_cast<float>(*p * level_[b]);
        level_[b] *= o.close[b];
      }
      for (size_t a = 0; a < NA; ++a) {
        if (const float p = o.pre[a]; ref_[a] > 0 && p > 0) {
          adj_[a] *= ref_[a] / p;
          ref_[a] = p;
        }
        if (o.last[a] > 0) {
          if (!(adj_[a] > 0))
            adj_[a] = 1.0;
          ref_[a] = o.last[a];
          o.rec[a].f = adj_[a];
        }
      }
      f.write(reinterpret_cast<const char *>(o.x.data()), static_cast<std::streamsize>(o.x.size() * sizeof(float)));
      g.write(reinterpret_cast<const char *>(o.rec.data()), static_cast<std::streamsize>(o.rec.size() * sizeof(Rec)));
      dates_.push_back(dates[i0 + k]);
      x_.publish(1);
    }
    f.flush(), g.flush();
    assert(f && g && "cta/: 写失败");
    save_head();
  }
  if (dates_.size() == N)
    return true;
  if (rd.stale()) // 库在重算: 下次从头核对
    head_ = {};
  return false;
}

bool CtaLib::basket(size_t line, cta::Series &se) const {
  assert(line < row_.size());
  const size_t b = row_[line];
  if (b >= B())
    return false;
  se = {};
  std::ifstream f(dir_ + "/1m.bin", std::ios::binary);
  std::vector<float> x(kCell);
  for (size_t i = 0; i < dates_.size(); ++i) {
    f.seekg(static_cast<std::streamoff>((i * B() + b) * kCell * sizeof(float)));
    f.read(reinterpret_cast<char *>(x.data()), static_cast<std::streamsize>(kCell * sizeof(float)));
    assert(f && "cta/1m.bin: 短于头");
    for (size_t s = 0; s < kSlots; ++s)
      push(se, i * kSlots + s, x.data() + s * kFields, 1.0);
  }
  return true;
}

void CtaLib::stock(size_t a, cta::Series &se) const {
  assert(a < A());
  se = {};
  std::ifstream f(dir_ + "/stock.bin", std::ios::binary);
  Rec r;
  for (size_t i = 0; i < dates_.size(); ++i) {
    f.seekg(static_cast<std::streamoff>((i * A() + a) * sizeof(Rec)));
    f.read(reinterpret_cast<char *>(&r), sizeof(Rec));
    assert(f && "cta/stock.bin: 短于头");
    if (r.f > 0)
      for (size_t s = 0; s < kSlots; ++s)
        push(se, i * kSlots + s, r.x + s * kFields, r.f);
  }
}

} // namespace GUI::Factors
