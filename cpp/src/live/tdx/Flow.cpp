#include "live/tdx/Flow.hpp"

#include "live/tdx/Tables.hpp"
#include "live/tdx/Trace.hpp"

#include <algorithm>
#include <iterator>
#include <string>

namespace live::tdx::flow {

namespace {

constexpr int kSpan = 30;               // 一窗天数 (客户端上限)
constexpr int kIdle = 6;                // 往前查到连续这么多窗无流水即到开户前
constexpr int kFrom = 0x7e, kTo = 0x7f; // 入参: 开始日期 / 终止日期 (YYYYMMDD)

// 一窗 [lo, hi] 的流水 (客户端原序), 各行须在窗内
std::vector<CashFlow> ask(misc::Ymd lo, misc::Ymd hi) {
  trace::Span span("窗", {{"lo", lo}, {"hi", hi}});
  std::vector<CashFlow> rows = tables::read<tables::CashFlows>({{kFrom, misc::dense(lo)}, {kTo, misc::dense(hi)}});
  for (const CashFlow &r : rows)
    need(lo <= r.date && r.date <= hi, strf("资金流水 %s ~ %s 的应答里有 %d 的行", misc::dash(lo).c_str(), misc::dash(hi).c_str(), r.date));
  return rows;
}

} // namespace

std::vector<CashFlow> pull(misc::Ymd from, const Log &log, const Live &live) {
  const misc::Ymd end = misc::add_days(misc::today_ymd(), -1);
  std::vector<std::vector<CashFlow>> wins; // 日序
  const auto one = [&](misc::Ymd lo, misc::Ymd hi) {
    std::vector<CashFlow> rows = ask(lo, hi);
    log("window", {{"lo", lo}, {"hi", hi}, {"n", rows.size()}});
    live(strf("资金流水 %s ~ %s: %zu 条", misc::dash(lo).c_str(), misc::dash(hi).c_str(), rows.size()));
    return rows;
  };
  if (from) {
    for (misc::Ymd lo = from; lo <= end;) {
      const misc::Ymd hi = std::min(misc::add_days(lo, kSpan - 1), end);
      wins.push_back(one(lo, hi));
      lo = misc::add_days(hi, 1);
    }
  } else {
    misc::Ymd hi = end;
    for (int idle = 0; idle < kIdle;) {
      const misc::Ymd lo = misc::add_days(hi, 1 - kSpan);
      wins.insert(wins.begin(), one(lo, hi));
      idle = wins.front().empty() ? idle + 1 : 0;
      hi = misc::add_days(lo, -1);
    }
  }
  std::vector<CashFlow> out;
  for (std::vector<CashFlow> &w : wins)
    out.insert(out.end(), std::make_move_iterator(w.begin()), std::make_move_iterator(w.end()));
  return out;
}

} // namespace live::tdx::flow
