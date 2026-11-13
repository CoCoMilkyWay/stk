#include "gui/quote/services/History.hpp"

#include "gui/quote/BasketRow.hpp" // census
#include "shared/Basket.hpp"       // basket_table

#include <cassert>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

namespace GUI::Quote {

History::History(Side s, PriceLib &lib) : side_(s), lib_(lib), kept_(s == kStock ? kKeep : basket_table().size()) { Start(); }

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
