#include "gui/task_market/services/MinuteService.hpp"

#include <algorithm>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

namespace GUI::Market {

const float *MinuteCloses::on(misc::Ymd d) const {
  const auto it = std::lower_bound(dates.begin(), dates.end(), d);
  return it != dates.end() && *it == d ? close.data() + static_cast<size_t>(it - dates.begin()) * Quote::kSlots : nullptr;
}

const MinuteCloses *MinuteResult::of(Quote::Ref ref) const {
  const auto it = std::find(refs.begin(), refs.end(), ref);
  return it == refs.end() ? nullptr : list[static_cast<size_t>(it - refs.begin())].get();
}

bool MinuteResult::covers(Quote::Ref ref) const { return std::ranges::find(refs, ref) != refs.end(); }

bool MinuteService::publish(const MinuteRequest &req, std::shared_ptr<MinuteResult> out) {
  out->gen = req.gen, out->refs = req.refs;
  out->list.resize(req.refs.size());
  std::lock_guard<std::mutex> lock(mutex);
  result = std::move(out);
  Progress::publish();
  return true;
}

bool MinuteService::run(MinuteRequest &req) {
  auto out = std::make_shared<MinuteResult>();
  const std::optional<Quote::PriceOpen> lib = lib_.open("NAV / VAL 分钟线由 all.bar 特征库合成", cancel_, *this);
  if (!lib)
    return false;
  if (!lib->lack.empty()) {
    out->lack = lib->lack;
    return publish(req, std::move(out));
  }
  for (const Quote::Ref ref : req.refs) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    out->list.push_back(kept_.get(ref, lib->stamp, [&](MinuteCloses &mc) {
      return lib_.each(ref, [&](size_t i, double, const float *px) {
        mc.dates.push_back(lib->dates[i]);
        for (size_t s = 0; s < static_cast<size_t>(Quote::kSlots); ++s)
          mc.close.push_back(px[s * Quote::PriceLib::kFields + Quote::PriceLib::kClose]);
      });
    }));
  }
  return publish(req, std::move(out));
}

} // namespace GUI::Market
