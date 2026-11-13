#include "gui/quote/services/History.hpp"

#include "gui/quote/BasketRow.hpp" // census
#include "shared/Basket.hpp"       // basket_table

#include <cassert>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace GUI::Quote {

History::History(Side s, PriceLib &lib, QuoteService &live)
    : side_(s), lib_(lib), live_(live), kept_(s == kStock ? kKeep : basket_table().size()) { Start(); }

void History::bind(const Handoff &h) {
  const PoolAxis &market = live_.axis();
  std::vector<uint32_t> row(h.axis.size(), UINT32_MAX); // [库轴] mem 的行
  for (size_t k = 0; k < h.mem.size(); ++k)
    row[h.mem.id[k]] = static_cast<uint32_t>(k);
  Members m; // 资产 = 全市场
  for (size_t q = 0; q < market.size(); ++q) {
    if (const size_t a = h.axis.sub_of(market.global(q)); a < h.axis.size() && row[a] != UINT32_MAX)
      for (const Members::Weight &w : h.mem.of(row[a]))
        m.add(w.key, w.w);
    m.next(static_cast<uint32_t>(q));
  }
  live_.bind(std::move(m), h.level);
}

bool History::run(HistoryRequest &req) {
  assert(side_ == kBasket || req.keys.size() <= kKeep);
  auto out = std::make_shared<Tape>();
  out->keys = req.keys;
  while (true) { // 读到一半库被别的服务补了新日 (交接对不上戳) → 照新库重读
    const std::optional<PriceOpen> o = lib_.open(side_ == kStock ? "个股历史由 all.bar 特征库合成" : "指数 / 行业由 all.bar 特征库合成", cancel_, x_);
    if (!o)
      return false;
    out->list.clear();
    if (!o->lack.empty()) {
      out->lack = o->lack;
      out->list.resize(req.keys.size());
      break;
    }
    out->dates = o->dates, out->lib = o->lib;
    for (const uint32_t k : req.keys) {
      if (cancel_.load(std::memory_order_relaxed))
        return false;
      const Ref ref{side_, k};
      out->list.push_back(kept_.get(ref, o->stamp, [&](Kline &kl) { return lib_.kline(ref, kDailyDays, kMinuteDays, kl); }));
    }
    if (side_ == kStock)
      break;
    if (const std::optional<Handoff> h = lib_.handoff(o->stamp)) {
      census(h->mem, h->axis, req.keys, *out);
      bind(*h);
      break;
    }
  }
  std::lock_guard<std::mutex> lock(x_.mutex);
  x_.gen = req.gen;
  x_.data = std::move(out);
  x_.publish();
  return true;
}

} // namespace GUI::Quote
