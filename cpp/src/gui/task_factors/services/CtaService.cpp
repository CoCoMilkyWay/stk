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
    if (req.side == Database::kBasket) {
      [[maybe_unused]] const bool in = lib_.basket(req.key, *s);
      assert(in && "CTA 篮子 = 篮子目录三组的线");
    } else {
      const size_t a = lib.axis.sub_of(req.key);
      if (a >= lib.axis.size())
        return publish(req, "该股不在 all.bar 库轴上 (库建成之后上市 / 进池; 全量重算 all.bar 即并入).");
      lib_.stock(a, *s);
    }
    se = std::move(s);
  }
  kept_.insert(kept_.begin(), Kept{req.side, req.key, lib.axis.hash, dates.back(), dates.size(), se});
  if (kept_.size() > kKeep)
    kept_.pop_back();

  constexpr size_t S = Database::kSlots;
  const size_t W = static_cast<size_t>(sp.roll), nd = cta::days_of(*se, S);
  if (se->c.size() < 2 || nd <= W)
    return publish(req, "有效根只跨 " + std::to_string(nd) + " 日, 须长于滚动窗 " + std::to_string(W) + " 日.");
  auto out = std::make_shared<CtaResult>();
  out->dates = std::move(dates);
  out->se = se;
  const double years = static_cast<double>((misc::to_days(out->dates.back()) - misc::to_days(out->dates.front())).count() + 1) / 365.25;
  out->bpy = static_cast<double>(out->dates.size() * S) / years;
  const cta::Setup u{sp.cost_bp * 1e-4, sp.vol, sp.buffer, out->bpy};
  begin(Phase::Running, cta::specs_of(static_cast<double>(S), sp.kind).size());
  const bool done = cta::trace(
      *se, S, sp.kind, W, u, out->tr, [&] { return cancel_.load(std::memory_order_relaxed); },
      [&] { Progress::publish(1); });
  return done && publish(req, {}, std::move(out));
}

} // namespace GUI::Factors
