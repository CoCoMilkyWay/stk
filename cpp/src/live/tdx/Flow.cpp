#include "live/tdx/Flow.hpp"

#include "live/tdx/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"
#include "live/tdx/Win.hpp"

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

namespace live::tdx::flow {

namespace {

using Snap = mem::Snap<tables::CashFlows>;

const std::string kFunc = "资金流水", kCol = "剩余金额";
constexpr long kFrom = 1128, kTo = 1131, kQuery = 1140, kCount = 1243; // 起 / 止日控件、查询按钮、条数标签
constexpr int kSpan = 30;                                              // 一窗天数 (客户端上限)
constexpr int kIdle = 6;                                               // 往前查到连续这么多窗无流水即到开户前
constexpr int kYearX = 16;                                             // 起止日控件上年份段距左缘 [px]
constexpr unsigned kWmKeydown = 0x100, kWmChar = 0x102;
constexpr long kVkLeft = 0x25, kVkRight = 0x27;
constexpr double kAnswer = 30; // 一窗应答最长等多久 [s]
constexpr double kSettle = 1;  // 条数标签写回后须这么久不变才采信 [s]

struct Page {
  win::Hwnd dlg, from, to, count;
};

void chars(const win::Hwnd &h, std::string_view digits) {
  for (const char c : digits)
    win::post(h, kWmChar, c);
}
void key(const win::Hwnd &h, long vk) { win::post(h, kWmKeydown, vk); }

// 起止日控件 (对话框 dlg 上 id 为 cid 的 h) 设为 d: 点年份段 → 敲年 → 右移两段到日敲 01 (改月时不越月末) → 左移到月敲月 → 右移到日敲日
void set_date(const win::Hwnd &dlg, const win::Hwnd &h, long cid, misc::Ymd d) {
  std::optional<win::Node> n;
  for (win::Node &k : win::enum_(dlg))
    if (k.depth == 1 && k.id == cid)
      n = std::move(k);
  need(n.has_value(), strf("「%s」页上没有起止日控件 %ld", kFunc.c_str(), cid));
  win::clickat(h, n->rect[0] + kYearX, (n->rect[1] + n->rect[3]) / 2);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  const std::string s = misc::dense(d), want = misc::dash(d);
  chars(h, std::string_view(s).substr(0, 4));
  key(h, kVkRight);
  key(h, kVkRight);
  chars(h, "01");
  key(h, kVkLeft);
  chars(h, std::string_view(s).substr(4, 2));
  key(h, kVkRight);
  chars(h, std::string_view(s).substr(6, 2));
  std::string got;
  until([&] { return strf("起止日控件 %ld 设为 %s (读到 %s)", cid, want.c_str(), got.c_str()); },
        [&] { return (got = win::text(h)) == want; }, 3);
}

// 一窗 [lo, hi] 的流水 (客户端原序)
std::vector<CashFlow> ask(mem::Table<tables::CashFlows> &t, const mem::HeapP &heap, const Page &p, misc::Ymd lo, misc::Ymd hi) {
  trace::Span span("窗", {{"lo", lo}, {"hi", hi}});
  set_date(p.dlg, p.to, kTo, hi);
  set_date(p.dlg, p.from, kFrom, lo);
  win::settext(p.count, "");
  t.arm(mem::scan_of<tables::CashFlows>(*heap));
  win::command(p.dlg, kQuery);
  const std::string range = misc::dash(lo) + " ~ " + misc::dash(hi);
  std::optional<int> last;
  Clock::time_point since = Clock::now();
  const int n = *until(
      said("「" + kFunc + "」" + range + "的条数标签写回"),
      [&]() -> std::optional<int> {
        const std::optional<int> c = ui::page_count(p.count);
        const Clock::time_point now = Clock::now();
        if (c != last) {
          last = c;
          since = now;
          return std::nullopt;
        }
        return c && now - since >= Secs(kSettle) ? c : std::nullopt;
      },
      kAnswer, 0.1);
  trace::ev("count", {{"n", n}});
  if (n == 0)
    return {};
  Snap s = std::move(*until(
      said(strf("「%s」%s的 %d 行应答", kFunc.c_str(), range.c_str(), n)),
      [&]() -> std::optional<Snap> {
        std::optional<Snap> s = t.try_pick(heap, mem::scan_of<tables::CashFlows>(*heap));
        if (!s || s->rows.size() != static_cast<size_t>(n))
          return std::nullopt;
        return s;
      },
      kAnswer, 0.1));
  for (const CashFlow &r : s.rows)
    need(lo <= r.date && r.date <= hi, strf("「%s」%s的应答里有 %d 的行", kFunc.c_str(), range.c_str(), r.date));
  trace::ev("ask", {{"tab", kFunc}, {"rows", s.rows.size()}, {"addr", mem::addrs(s.mark)}});
  return std::move(s.rows);
}

} // namespace

std::vector<CashFlow> pull(mem::Table<tables::CashFlows> &t, const mem::HeapP &heap, misc::Ymd from, const Log &log, const Live &live) {
  ui::show_bar();
  live("开「" + kFunc + "」页…");
  log("page", {{"func", kFunc}});
  const win::Hwnd dlg = ui::open_page(kFunc, kCol, kCount);
  const auto c = win::ctls(dlg, {{"from", kFrom}, {"to", kTo}, {"count", kCount}});
  const Page p{dlg, c.at("from"), c.at("to"), c.at("count")};
  const misc::Ymd end = misc::add_days(misc::today_ymd(), -1);
  std::vector<std::vector<CashFlow>> wins; // 日序
  const auto one = [&](misc::Ymd lo, misc::Ymd hi) {
    std::vector<CashFlow> rows = ask(t, heap, p, lo, hi);
    log("window", {{"lo", lo}, {"hi", hi}, {"n", rows.size()}});
    live(strf("「%s」%s ~ %s: %zu 条", kFunc.c_str(), misc::dash(lo).c_str(), misc::dash(hi).c_str(), rows.size()));
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
  ui::click_tool("持仓");
  std::vector<CashFlow> out;
  for (std::vector<CashFlow> &w : wins)
    out.insert(out.end(), std::make_move_iterator(w.begin()), std::make_move_iterator(w.end()));
  return out;
}

} // namespace live::tdx::flow
