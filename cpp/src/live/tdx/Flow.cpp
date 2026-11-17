#include "live/tdx/Flow.hpp"

#include "live/tdx/Tables.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"
#include "live/tdx/Win.hpp"

#include <algorithm>
#include <optional>
#include <string>
#include <string_view>

namespace live::tdx::flow {

namespace {

using Snap = mem::Snap<tables::CashFlows>;

const std::string kFunc = "资金流水", kCol = "剩余金额";
constexpr long kFrom = 1128, kTo = 1131, kQuery = 1140; // 起 / 止日控件、查询键
constexpr int kSpan = 30;                               // 一窗天数 (客户端上限)
constexpr int kIdle = 6;                                // 往前查到连续这么多窗无流水即到开户前
constexpr int kYearX = 16;                              // 起止日控件上年份段距左缘 [px]
constexpr unsigned kWmKeydown = 0x100, kWmChar = 0x102;
constexpr long kVkLeft = 0x25, kVkRight = 0x27;

// 查询页 + 起 / 止日控件
struct Form {
  ui::Page page;
  win::Hwnd from, to;
};

void chars(const win::Hwnd &h, std::string_view digits) {
  for (const char c : digits)
    win::post(h, kWmChar, c);
}
void key(const win::Hwnd &h, long vk) { win::post(h, kWmKeydown, vk); }

// 起止日控件 (对话框 dlg 上 id 为 cid 的 h) 设为 d: 点年份段 → 敲年 → 右移两段到日敲 01 (改月时不越月末) → 左移到月敲月 → 右移到日敲日.
// 点下即取焦点、选中所点的段, 段位在控件画过一次后才有; 投递的键按序在点之后处理, 读回等它们处理完
void set_date(const win::Hwnd &dlg, const win::Hwnd &h, long cid, misc::Ymd d) {
  std::optional<win::Node> n;
  for (win::Node &k : win::enum_(dlg))
    if (k.depth == 1 && k.id == cid)
      n = std::move(k);
  need(n.has_value(), strf("「%s」页上没有起止日控件 %ld", kFunc.c_str(), cid));
  win::clickat(h, n->rect[0] + kYearX, (n->rect[1] + n->rect[3]) / 2);
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

// 一窗 [lo, hi] 的流水 (客户端原序): 设起止日、点查询等查完, 页上 N 条; N > 0 即点查询前那遍之后新冒出的块, 须恰 N 行
std::vector<CashFlow> ask(const mem::HeapP &heap, const Form &f, misc::Ymd lo, misc::Ymd hi) {
  trace::Span span("窗", {{"lo", lo}, {"hi", hi}});
  set_date(f.page.dlg, f.to, kTo, hi);
  set_date(f.page.dlg, f.from, kFrom, lo);
  mem::Table<tables::CashFlows> t;
  t.arm(mem::scan_of<tables::CashFlows>(*heap));
  ui::press(f.page, kQuery);
  const int n = f.page.count.read();
  if (n == 0)
    return {};
  const std::string range = misc::dash(lo) + " ~ " + misc::dash(hi);
  Snap s = t.read(heap);
  need(s.rows.size() == static_cast<size_t>(n), strf("「%s」%s查完 (页上 %d 条) 新块 %zu 行", kFunc.c_str(), range.c_str(), n, s.rows.size()));
  for (const CashFlow &r : s.rows)
    need(lo <= r.date && r.date <= hi, strf("「%s」%s的应答里有 %d 的行", kFunc.c_str(), range.c_str(), r.date));
  trace::ev("ask", {{"tab", kFunc}, {"n", n}, {"rows", s.rows.size()}, {"addr", mem::addrs(s.mark)}});
  return std::move(s.rows);
}

} // namespace

std::vector<CashFlow> pull(const mem::HeapP &heap, misc::Ymd from, const Log &log, const Live &live) {
  ui::show_bar();
  live("开「" + kFunc + "」页…");
  log("page", {{"func", kFunc}});
  const ui::Page pg = ui::open_page(kFunc, kCol);
  const auto c = win::ctls(pg.dlg, {{"from", kFrom}, {"to", kTo}});
  const Form f{pg, c.at("from"), c.at("to")};
  const misc::Ymd end = misc::add_days(misc::today_ymd(), -1);
  std::vector<std::vector<CashFlow>> wins; // 日序
  const auto one = [&](misc::Ymd lo, misc::Ymd hi) {
    std::vector<CashFlow> rows = ask(heap, f, lo, hi);
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
  ui::open_tab("持仓");
  std::vector<CashFlow> out;
  for (std::vector<CashFlow> &w : wins)
    out.insert(out.end(), std::make_move_iterator(w.begin()), std::make_move_iterator(w.end()));
  return out;
}

} // namespace live::tdx::flow
