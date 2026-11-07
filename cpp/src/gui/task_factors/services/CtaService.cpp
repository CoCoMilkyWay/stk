#include "gui/task_factors/services/CtaService.hpp"

#include "features/Storage/FeatureRead.hpp"
#include "misc/date.hpp"
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <cassert>
#include <mutex>
#include <string>
#include <utility>

namespace GUI::Factors {

bool CtaService::publish(const CtaRequest &req, std::string lack, std::shared_ptr<CtaResult> out) {
  if (!out)
    out = std::make_shared<CtaResult>();
  out->gen = req.gen, out->side = req.side, out->key = req.key, out->spec = req.spec;
  out->lack = std::move(lack);
  std::lock_guard<std::mutex> lock(mutex);
  result = std::move(out);
  Progress::publish();
  return true;
}

bool CtaService::run(CtaRequest &req) {
  const CtaSpec &sp = req.spec;
  assert(sp.roll >= static_cast<int>(cta::kMinRoll));
  const FeatureLib lib = load_lib(cfg_, kAllBar);
  if (!lib.ready())
    return publish(req, "CTA 价格由 all.bar 特征库合成: 先在 Features → Compute 算好 all 池的 Bar 库.");
  FeatureRead rd(lib, &cancel_);
  std::vector<misc::Ymd> dates; // 已落盘日 (清单日期轴可能还在算)
  for (const misc::Ymd d : lib.dates)
    if (rd.has_date(d))
      dates.push_back(d);
  if (dates.size() < 2)
    return publish(req, "all.bar 特征库落盘不足 2 日.");
  if (!lib_.sync(lib, rd, dates))
    return false;
  if (req.key == kCtaNone)
    return publish(req, {});

  const auto hit = std::find_if(kept_.begin(), kept_.end(), [&](const Kept &k) {
    return k.side == req.side && k.key == req.key && k.axis == lib.axis.hash && k.last == dates.back() && k.days == dates.size();
  });
  std::shared_ptr<const cta::Series> se;
  if (hit != kept_.end()) {
    se = hit->se;
    kept_.erase(hit);
  } else {
    auto s = std::make_shared<cta::Series>();
    if (!lib_.series(req.side, req.key, *s)) {
      assert(req.side == Database::kStock && "CTA 篮子 = 篮子目录三组的线");
      return publish(req, "该股不在 all.bar 库轴上 (库建成之后上市 / 进池; 全量重算 all.bar 即并入).");
    }
    se = std::move(s);
  }
  kept_.insert(kept_.begin(), Kept{req.side, req.key, lib.axis.hash, dates.back(), dates.size(), se});
  if (kept_.size() > kKeep)
    kept_.pop_back();

  constexpr size_t S = Database::kSlots;
  const size_t W = static_cast<size_t>(sp.roll), nd = cta::days_of(*se, S).size();
  if (nd < 2)
    return publish(req, "有效根只跨 " + std::to_string(nd) + " 日, 至少 2 日.");
  auto out = std::make_shared<CtaResult>();
  out->dates = std::move(dates);
  out->se = se;
  const double years = static_cast<double>((misc::to_days(out->dates.back()) - misc::to_days(out->dates.front())).count() + 1) / 365.25;
  const cta::Setup u{{sp.commission_bp * 1e-4, sp.stamp_bp * 1e-4, sp.impact_bp * 1e-4}, {sp.vol, sp.buffer}, static_cast<double>(out->dates.size() * S) / years};
  cta::Grid grid = cta::grid_of(S, sp.kind, static_cast<size_t>(sp.dir), sp.step);
  begin(Phase::Running, nd - 1);
  const bool done = cta::trace(
      *se, std::move(grid), sp.goal, W, u, pool_, out->tr, [&] { return cancel_.load(std::memory_order_relaxed); },
      [&] { Progress::publish(1); });
  return done && publish(req, {}, std::move(out));
}

} // namespace GUI::Factors
