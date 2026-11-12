#include "gui/task_market/services/MinuteService.hpp"

#include "features/Panel/PoolSelect.hpp"
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureRead.hpp"

#include <algorithm>
#include <memory>
#include <mutex>
#include <utility>

namespace GUI::Market {

const MinuteCloses *MinuteResult::of(Quote::Ref ref) const {
  const auto it = std::find(refs.begin(), refs.end(), ref);
  return it == refs.end() ? nullptr : list[static_cast<size_t>(it - refs.begin())].get();
}

bool MinuteService::publish(std::shared_ptr<MinuteResult> out) {
  std::lock_guard<std::mutex> lock(mutex);
  result = std::move(out);
  Progress::publish();
  return true;
}

bool MinuteService::run(MinuteRequest &req) {
  auto out = std::make_shared<MinuteResult>();
  out->gen = req.gen, out->refs = req.refs;
  out->list.resize(req.refs.size());
  const pool::AllBar ab = pool::all_bar("NAV 分钟线由 all.bar 特征库合成");
  if (!ab.lack.empty()) {
    out->lack = ab.lack;
    return publish(std::move(out));
  }
  const FeatureLib &lib = ab.lib;
  FeatureRead rd(lib, &cancel_);
  const std::shared_ptr<const pool::Picks> pk = pool::picks(lib, &cancel_);
  if (!pk || !lib_.sync(lib, rd, *pk, cancel_, *this, pool_))
    return false;
  const std::vector<misc::Ymd> &dates = pk->tape->dates;
  std::vector<Kept> kept;
  for (size_t i = 0; i < req.refs.size(); ++i) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    const Quote::Ref ref = req.refs[i];
    const auto hit = std::find_if(kept_.begin(), kept_.end(), [&](const Kept &k) {
      return k.ref == ref && k.axis == lib.axis.hash && k.last == dates.back() && k.days == dates.size();
    });
    std::shared_ptr<const MinuteCloses> mc;
    if (hit != kept_.end()) {
      mc = hit->mc;
    } else if (auto c = std::make_shared<MinuteCloses>(); lib_.closes(ref, *c)) {
      mc = std::move(c);
    }
    out->list[i] = mc;
    kept.push_back({ref, lib.axis.hash, dates.back(), dates.size(), std::move(mc)});
  }
  kept_ = std::move(kept);
  return publish(std::move(out));
}

} // namespace GUI::Market
