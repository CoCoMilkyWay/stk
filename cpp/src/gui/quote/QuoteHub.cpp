#include "gui/quote/QuoteHub.hpp"

#include "account/Ledger.hpp"
#include "fundamental/AssetAxis.hpp"
#include "imgui.h"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <tuple>
#include <vector>

namespace GUI::Quote {

void QuoteHub::frame(const SharedData &data) {
  assert(data.task_state.database.asset_info_ready);
  if (frame_ == ImGui::GetFrameCount())
    return;
  frame_ = ImGui::GetFrameCount();
  const misc::Ymd mark = data.asset_info.quote_end;
  if (!quote_) {
    std::vector<uint32_t> ids; // 在市股的全局 id (升序)
    const StockInfos &stocks = data.asset_info.stocks;
    for (uint32_t id = 0; id < stocks.size(); ++id)
      if (stocks[id].known && !stocks[id].out_date)
        ids.push_back(id);
    auto cal = std::make_shared<Calendar>();
    for (const CalendarDay &d : data.asset_info.days)
      if (d.trading)
        cal->days.push_back(d.date);
    cal_ = std::move(cal);
    quote_ = std::make_unique<QuoteService>(std::make_shared<const PoolAxis>(pool_axis(std::move(ids))), mark, cal_);
    price_ = std::make_unique<PriceLib>(cal_, [q = quote_.get()](const Handoff &h) { q->bind(h); });
    mark_ = mark;
  }
  if (mark != mark_) {
    mark_ = mark;
    quote_->set_fund_end(mark);
  }
  status = quote_->status();
  live = quote_->live();
  std::vector<Want> &ws = want_.wants;
  std::ranges::sort(ws, {}, [](const Want &w) { return std::tuple(w.ref.side, w.ref.key, !w.minute); });
  ws.erase(std::ranges::unique(ws, {}, &Want::ref).begin(), ws.end());
  want_.mark = mark;
  want_.live = status.date;
  want_.ledger = account::generation();
  if (!sent_ || !(*sent_ == want_)) {
    sent_ = want_;
    price_->Request(want_);
  }
  ws.clear();
  view = latest(*price_);
}

void QuoteHub::want(Ref r, bool minute) {
  assert(r.ok());
  if (minute || r.side == kStock)
    want_.wants.push_back({r, minute});
}

} // namespace GUI::Quote
