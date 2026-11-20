#include "live/tdx/Flash.hpp"

#include "live/Book.hpp"
#include "live/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cmath>

namespace live::tdx::flash {

using win::Hwnd;

namespace {

constexpr const char *kLink = "Stock";
constexpr double kLinkTimeout = 6, kOpenWait = 6, kFieldWait = 1;
const std::vector<std::string> kConfirmTitles = {"买入交易确认", "卖出交易确认", "交易确认"};
const std::string kResultTitle = "提示";
constexpr long kEst = 8915;
const std::vector<std::string> kNotReady = {"价格档位上的价格为空"};

struct Kind {
  int side;
  Credit credit;
  long cmd;
  const char *title;
  long code, price, qty, unit, quote, max, ok;
};
constexpr Kind kKinds[] = {
    {kBuy, Credit::Plain, 0x13DD, "闪电买入", 12005, 12006, 12007, 12016, 3067, 2022, 1},
    {kBuy, Credit::Margin, 0x1441, "闪电融资买入", 313, 314, 315, 320, 220, 199, 1},
    {kSell, Credit::Plain, 0x1404, "闪电卖出", 12005, 12006, 12007, 12016, 3067, 2065, 1},
};

const Kind *kind_of(int side, Credit credit) {
  for (const Kind &k : kKinds)
    if (k.side == side && k.credit == credit)
      return &k;
  return nullptr;
}

bool ours(const std::string &title) {
  return title == kResultTitle || std::ranges::find(kConfirmTitles, title) != kConfirmTitles.end() ||
         std::ranges::any_of(kKinds, [&](const Kind &k) { return title == k.title; });
}

void sweep() {
  const win::Tops all = win::tops();
  win::Tops keep = all;
  std::erase_if(keep, [](const auto &x) { return ours(x.second.title); });
  if (keep.size() == all.size())
    return;
  trace::Span span("清场");
  ui::close_new(keep);
  for (const auto &[h, n] : win::tops())
    need(!ours(n.title), strf("上一单留下的「%s」关不掉", n.title.c_str()));
}

struct Ctx {
  const OrderReq &o;
  const Kind &k;
  Hwnd main, dlg, box;
  std::string px_s;
  double px = 0;
  int64_t n = 0;
  std::string msg;
  bool sent = false;
};

bool is_num(const std::string &t) {
  const size_t dot = t.find('.');
  const auto digits = [](std::string_view s) { return !s.empty() && std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c); }); };
  return dot == std::string::npos ? digits(t) : digits(std::string_view(t).substr(0, dot)) && digits(std::string_view(t).substr(dot + 1));
}

int64_t max_qty(const Hwnd &h) {
  const std::string t = win::text(h);
  need(is_num(t), "最大可买 / 可卖读到「" + t + "」");
  return static_cast<int64_t>(std::stod(t));
}

int64_t order_qty(const OrderReq &o, double px, int64_t cap) {
  const auto [first, step] = lot(o.sec.code);
  int64_t n = o.qty.is_shares() ? static_cast<int64_t>(std::llround(o.qty.v)) : static_cast<int64_t>(o.qty.v / px) / step * step;
  if (o.side == kBuy) {
    need(n >= first, strf("%g %s 按 %g 不足起买量 %lld 股", o.qty.v, o.qty.is_shares() ? "股" : "元", px, static_cast<long long>(first)));
    cap = cap / step * step;
    need(cap >= first, strf("最大可买 %lld 股, 不足起买量 %lld 股", static_cast<long long>(cap), static_cast<long long>(first)));
  } else if (!o.qty.is_shares()) {
    n = std::max(n, first);
  }
  need(cap >= 1, "最大可卖为 0");
  return std::min(n, cap);
}

void settext(const Hwnd &h, const std::string &s) {
  win::settext(h, s);
  until([&] { return "写入「" + s + "」后读回「" + win::text(h) + "」"; }, [&] { return win::text(h) == s; }, 1);
}

std::string after(const std::string &text, const std::string &label, bool dots, size_t take, const std::string &unit) {
  for (size_t at = text.find(label); at != std::string::npos; at = text.find(label, at + 1)) {
    size_t i = at + label.size();
    while (i < text.size() && text[i] == ' ')
      ++i;
    const size_t s = i;
    while (i < text.size() && (std::isdigit(static_cast<unsigned char>(text[i])) || (dots && text[i] == '.')))
      ++i;
    if (take ? i - s >= take : i > s && text.compare(i, unit.size(), unit) == 0)
      return text.substr(s, take ? take : i - s);
  }
  fail("确认框缺" + label + " " + text);
}

void go_to(const Hwnd &main, const Sec &x) {
  const std::string want = win::bare("-" + x.name + "]");
  const auto there = [&] { return win::bare(win::title(main)).ends_with(want); };
  if (there())
    return;
  need(x.mkt != Mkt::BJ, x.code + " 北交所没有联动前缀");
  const long wp = (x.mkt == Mkt::SH ? 7 : 6) * 1'000'000L + std::stol(x.code);
  const unsigned link = win::regmsg(kLink);
  win::post(main, link, wp);
  bool repost = false;
  until([&] { return strf("主窗切到「%s」, 现为「%s」(联动 %ld)", x.name.c_str(), win::title(main).c_str(), wp); },
        [&] {
          if (there())
            return true;
          bool late = false;
          for (const auto &[h, n] : win::tops())
            if (ours(n.title)) {
              late = true;
              if (n.en) {
                trace::ev("late", {{"title", n.title}, {"hwnd", h}, {"body", ui::body(h)}});
                win::post(h, win::kWmClose, 0);
              }
            }
          if (late)
            repost = true;
          else if (repost) {
            win::post(main, link, wp);
            repost = false;
          }
          return false;
        },
        kLinkTimeout);
}

void switch_to(Ctx &c) {
  c.main = ui::main_wnd();
  go_to(c.main, c.o.sec);
}

void open(Ctx &c) {
  for (const auto &[h, n] : win::tops())
    need(n.title != c.k.title, strf("已有未关的「%s」窗", c.k.title));
  c.dlg = ui::popup({c.k.title}, kOpenWait, [&] { win::post(c.main, win::kWmCommand, c.k.cmd); }, kNotReady);
}

void form(Ctx &c) {
  const auto ids = win::ctls(c.dlg, {{"code", c.k.code}, {"price", c.k.price}, {"qty", c.k.qty}, {"unit", c.k.unit}, {"quote", c.k.quote}, {"max", c.k.max}});
  const std::string got = win::text(ids.at("code"));
  need(got == c.o.sec.code, "闪电窗证券 " + got + " ≠ " + c.o.sec.code);
  const std::string quote = win::text(ids.at("quote"));
  need(quote.find("限价") != std::string::npos, "报价方式为「" + quote + "」, 需限价委托");
  const std::string dflt = *until(said("闪电窗默认价"), [&]() -> std::optional<std::string> {
    std::string t = win::text(ids.at("price"));
    return t.empty() ? std::nullopt : std::optional<std::string>(t); }, kFieldWait);
  const size_t dot = dflt.find('.');
  need(dot != std::string::npos, "默认价「" + dflt + "」定不出小数位");
  c.px_s = strf("%.*f", static_cast<int>(dflt.size() - dot - 1), c.o.px);
  const std::string unit = win::text(ids.at("unit"));
  need(unit == "股" || unit == "元", "数量单位「" + unit + "」不认识");
  until(said("闪电窗最大可买 / 可卖"), [&] { return !win::text(ids.at("max")).empty(); }, kFieldWait);
  const int64_t cap0 = max_qty(ids.at("max"));
  settext(ids.at("price"), c.px_s);
  c.px = std::stod(c.px_s);
  int64_t cap = max_qty(ids.at("max"));
  cap = std::min(cap, c.o.side == kBuy ? static_cast<int64_t>(static_cast<double>(cap0) * std::stod(dflt) / c.px) : cap0);
  c.n = order_qty(c.o, c.px, cap);
  trace::ev("form", {{"px", c.px_s}, {"dflt", dflt}, {"unit", unit}, {"cap0", cap0}, {"cap", cap}, {"n", c.n}});
  if (unit == "股") {
    settext(ids.at("qty"), std::to_string(c.n));
  } else {
    settext(ids.at("qty"), std::to_string(static_cast<int64_t>(std::ceil(std::round(static_cast<double>(c.n) * c.px * 100) / 100))));
    const Hwnd est = win::ctls(c.dlg, {{"est", kEst}}).at("est");
    const auto shares = [&]() -> int64_t {
      const std::string t = win::text(est);
      const std::string tail = "股)";
      const size_t op = t.rfind('(');
      if (!t.ends_with(tail) || op == std::string::npos || op + 1 + tail.size() > t.size())
        return -1;
      const std::string d = t.substr(op + 1, t.size() - op - 1 - tail.size());
      if (!is_num(d) || d.find('.') != std::string::npos || d.size() > 12)
        return -1;
      return std::stoll(d);
    };
    until([&] { return strf("预估 %lld 股, 现为「%s」", static_cast<long long>(c.n), win::text(est).c_str()); }, [&] { return shares() == c.n; }, 1);
  }
  trace::live(strf("%lld股 @ %s (最大 %lld)", static_cast<long long>(c.n), c.px_s.c_str(), static_cast<long long>(cap)));
}

void confirm(Ctx &c) {
  c.box = ui::popup(kConfirmTitles, 5, [&] { win::command(c.dlg, c.k.ok); });
  const std::string b = ui::body(c.box);
  const std::string code = after(b, "股票代码:", false, 6, "");
  const std::string px = after(b, "委托价格:", true, 0, "");
  const std::string qty = after(b, "委托数量:", false, 0, "股");
  need(code == c.o.sec.code && is_num(px) && std::stod(px) == std::stod(c.px_s) && std::stoll(qty) == c.n,
       strf("确认框 (%s, %s, %s) ≠ 预期 (%s, %s, %lld): %s", code.c_str(), px.c_str(), qty.c_str(), c.o.sec.code.c_str(), c.px_s.c_str(), static_cast<long long>(c.n), b.c_str()));
}

void reply(Ctx &c) {
  const Hwnd res = ui::popup({kResultTitle}, 10, [&] { ui::yes(c.box), c.sent = true; });
  c.msg = ui::body(res);
  ui::yes(res);
  if (c.msg.find("合同号") == std::string::npos) {
    c.sent = false;
    fail("委托未成功: " + c.msg);
  }
  const Hwnd mine[3] = {res, c.box, c.dlg};
  until(
      [&] {
        std::string s;
        for (const auto &[h, n] : win::tops())
          if (std::find(std::begin(mine), std::end(mine), h) != std::end(mine))
            s += "「" + n.title + "」";
        return "回报框 / 确认框 / 闪电窗关掉, 还开着: " + s;
      },
      [&] {
        const win::Tops t = win::tops();
        return std::none_of(std::begin(mine), std::end(mine), [&](const Hwnd &h) { return t.contains(h); });
      },
      3);
}

struct Stage {
  std::string_view name;
  void (*run)(Ctx &);
};
constexpr Stage kStages[] = {{"切换", switch_to}, {"开窗", open}, {"填单", form}, {"确认", confirm}, {"回报", reply}};
constexpr std::array<std::string_view, std::size(kStages)> kStepNames = {"切换", "开窗", "填单", "确认", "回报"};

} // namespace

std::span<const std::string_view> steps() { return kStepNames; }

Placed place(const OrderReq &o) {
  const Kind *k = kind_of(o.side, o.credit);
  assert(k);
  assert(o.qty.v > 0 && o.px > 0);
  Ctx c{.o = o, .k = *k};
  sweep();
  const win::Tops before = win::tops();
  try {
    for (const Stage &s : kStages) {
      trace::Span span(std::string(s.name));
      s.run(c);
    }
  } catch (const Fail &f) {
    ui::close_new(before);
    if (c.sent)
      fail("已点确认, 委托可能已提交 (先查委托再重下): " + f.why);
    throw;
  }
  trace::ev("placed", {{"qty", c.n}, {"px", c.px}, {"msg", c.msg}});
  return {c.n, c.px, c.msg};
}

} // namespace live::tdx::flash
