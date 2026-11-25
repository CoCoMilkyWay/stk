#include "live/tdx/Flash.hpp"

#include "live/Book.hpp"
#include "live/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <tuple>

namespace live::tdx::flash {

using win::Hwnd;

namespace {

// 通达信外部联动: 注册消息「Stock」投给主窗, wParam = 市场前缀 × 10⁶ + 代码, 图表即切到该只
constexpr const char *kLink = "Stock";
constexpr double kLinkTimeout = 6; // 投联动后等标题换过来至多几秒
const std::vector<std::string> kConfirmTitles = {"买入交易确认", "卖出交易确认", "交易确认"};
const std::string kResultTitle = "提示";
constexpr long kEst = 8915; // 闪电窗按额时的「预估金额:…(N股)」
// 图表刚切过来、盘口还没到时开闪电窗, 客户端弹「…的价格档位上的价格为空!」: 关掉再开, 至多等 kOpenWait 秒
const std::vector<std::string> kNotReady = {"价格档位上的价格为空"};
constexpr double kOpenWait = 6;
// 闪电窗开出后默认价 / 最大可买·可卖由客户端另查才填上, 至多等 kFieldWait 秒
constexpr double kFieldWait = 1;
// 各等待宁短勿长: 批量下单 (上百单) 一单卡住即整批停在半路, 留下方向与价格敞口; 超时即该单失败, 记下接着下后面的.
//   实测连下几十单后客户端变慢: 刚切图表 + 盘口为空重开时, 最大可买 / 可卖 1s 多才填上、闪电窗 6s 多才出过 —— 认这几单失败
//   (末了列出, 人工补), 不为它们拉长超时

// 闪电窗: 主窗命令 id、窗标题、各控件 id (证券代码 / 价格 / 数量 / 数量单位 / 报价方式 / 最大可买·可卖·可融 (股) / 提交)

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

// 本适配开的框: 各闪电窗 / 确认框 / 回报框
bool ours(const std::string &title) {
  return title == kResultTitle || std::ranges::find(kConfirmTitles, title) != kConfirmTitles.end() ||
         std::ranges::any_of(kKinds, [&](const Kind &k) { return title == k.title; });
}

// 上一单失败、收尾之后才显示出来的闪电窗 / 确认框 / 回报框先关掉: 留着则主窗一直禁用, 切不了图表
void sweep() {
  const win::Tops all = win::tops();
  win::Tops keep = all;
  std::erase_if(keep, [](const auto &x) { return ours(x.second.title); });
  if (keep.size() == all.size())
    return;
  trace::Span span("清场");
  ui::close_new(keep);
  for (const auto &[h, n] : win::tops())
    need(!ours(n.title), "上一单留下的「%s」关不掉", n.title.c_str());
}

// 本单上下文: 各步依次填写 (main 切换 / dlg 开窗 / px_s px n 填单 / box 确认 / msg sent 回报)
struct Ctx {
  const OrderReq &o;
  const Kind &k;
  Hwnd main, dlg, box;
  std::string px_s;
  double px = 0;
  int64_t n = 0;
  std::string msg;
  bool sent = false; // 已点确认: 此后的 Fail = 委托可能已提交
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

// 股数以闪电窗给的最大可买 / 可卖 cap 封顶 (钱或券不够就按能做的最大量): 买入须够起买量、封顶按步长取整; 卖出可卖不足即全卖 (零股只能一次卖完)
int64_t order_qty(const OrderReq &o, int64_t cap) {
  if (o.side == kBuy) {
    const auto [first, step] = lot(o.code);
    need(o.qty >= first, "%lld 股不足起买量 %lld 股", static_cast<long long>(o.qty), static_cast<long long>(first));
    cap = cap / step * step;
    need(cap >= first, "最大可买 %lld 股, 不足起买量 %lld 股", static_cast<long long>(cap), static_cast<long long>(first));
  }
  need(cap >= 1, "最大可卖为 0");
  return std::min(o.qty, cap);
}

// 写入后客户端的 EN_CHANGE 处理会先改写一下, 等读回恰为 s
void settext(const Hwnd &h, const std::string &s) {
  win::settext(h, s);
  until([&] { return "写入「" + s + "」后读回「" + win::text(h) + "」"; }, [&] { return win::text(h) == s; }, 1);
}

// 正文里首个「label 空格* 数字串 unit」的数字串 (dots: 可含小数点); take > 0 = 只取开头 take 位 (须够, 不看 unit)
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

// 图表标题里的名称与 name 认作同一只: 去掉除权除息前缀 (XD / XR / DR) 后一方是另一方的开头 (客户端名称限 8 字节, 加了前缀即截掉尾字)
bool same_name(std::string a, std::string b) {
  const auto strip = [](std::string &s) {
    for (const char *p : {"XD", "XR", "DR"})
      if (s.starts_with(p))
        return void(s.erase(0, 2));
  };
  strip(a);
  strip(b);
  return !a.empty() && !b.empty() && (a.starts_with(b) || b.starts_with(a));
}

// 图表切到 sec: 主窗标题已是「…-名称]」(same_name; 认宽了也有填单 / 确认两处按代码核) 即跳过, 否则向主窗投联动消息、等标题换过来 (客户端查着时照样处理联动)
//   迟到的框: 上一单开窗超时 (已记失败、收尾时还没出来) 后其闪电窗可能这时才冒出来. 它是模态的, 主窗随之禁用、投的联动作废
//   (实测: 下一单因此切不过去, 连带失败). 等的同时见到本适配的框即关掉, 关后下一轮重投联动; 不另加等待, 仍在 kLinkTimeout 内
void go_to(const Hwnd &main, const Sec &x) {
  const std::string want = win::bare(x.name);
  const auto there = [&] {
    const std::string t = win::bare(win::title(main));
    const size_t dash = t.rfind('-');
    return t.ends_with("]") && dash != std::string::npos && same_name(t.substr(dash + 1, t.size() - dash - 2), want);
  };
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
  go_to(c.main, c.o);
}

void open(Ctx &c) {
  for (const auto &[h, n] : win::tops())
    need(n.title != c.k.title, "已有未关的「%s」窗", c.k.title);
  c.dlg = ui::popup({c.k.title}, kOpenWait, [&] { win::post(c.main, win::kWmCommand, c.k.cmd); }, kNotReady);
}

void form(Ctx &c) {
  const auto ids = win::ctls(c.dlg, {{"code", c.k.code}, {"price", c.k.price}, {"qty", c.k.qty}, {"unit", c.k.unit}, {"quote", c.k.quote}, {"max", c.k.max}});
  const std::string got = win::text(ids.at("code"));
  need(got == c.o.code, "闪电窗证券 " + got + " ≠ " + c.o.code);
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
  // 最大可买按默认价算; 改价后客户端未必即时重算, 按价反比折一份, 与重读的取小
  int64_t cap = max_qty(ids.at("max"));
  cap = std::min(cap, c.o.side == kBuy ? static_cast<int64_t>(static_cast<double>(cap0) * std::stod(dflt) / c.px) : cap0);
  c.n = order_qty(c.o, cap);
  trace::ev("form", {{"px", c.px_s}, {"dflt", dflt}, {"unit", unit}, {"cap0", cap0}, {"cap", cap}, {"n", c.n}});
  if (unit == "股") {
    settext(ids.at("qty"), std::to_string(c.n));
  } else {
    // 向上取整到元: 多出的不足一股, 折回恰为 n 股
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
  need(code == c.o.code && is_num(px) && std::stod(px) == std::stod(c.px_s) && std::stoll(qty) == c.n, "确认框 (%s, %s, %s) ≠ 预期 (%s, %s, %lld): %s", code.c_str(), px.c_str(), qty.c_str(), c.o.code.c_str(), c.px_s.c_str(), static_cast<long long>(c.n), b.c_str());
}

// 读回报、点掉; 等回报框、确认框、闪电窗都关了才算完 (下一单紧接着切图表、开窗)
void reply(Ctx &c) {
  const Hwnd res = ui::popup({kResultTitle}, 10, [&] { ui::yes(c.box), c.sent = true; });
  c.msg = ui::body(res);
  ui::yes(res);
  if (c.msg.find("合同号") == std::string::npos) {
    c.sent = false; // 回报明说没成
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

} // namespace

Steps steps() {
  static_assert(kStages[std::size(kStages) - 1].name == "回报");
  Steps s{.unsent = std::size(kStages) - 1};
  for (const Stage &x : kStages)
    s.names.emplace_back(x.name);
  return s;
}

Placed place(const OrderReq &o) {
  const Kind *k = kind_of(o.side, o.credit);
  need(k != nullptr, "没有%s%s的闪电窗", credit_name(o.credit), side_name(o.side));
  need(o.qty > 0 && o.px > 0, "委托量 %lld / 价 %g 不为正", static_cast<long long>(o.qty), o.px);
  Ctx c{.o = o, .k = *k};
  sweep();
  const win::Tops before = win::tops();
  const std::string err = guard([&] {
    for (const Stage &s : kStages) {
      trace::Span span(std::string(s.name));
      s.run(c);
    }
  });
  if (!err.empty()) {
    std::ignore = guard([&] { ui::close_new(before); }); // 本单弹出的各框关掉, 下一单接着做; 收尾自身出错也不盖掉本单的原因
    fail(c.sent ? "已点确认, 委托可能已提交 (先查委托再重下): " + err : err);
  }
  trace::ev("placed", {{"qty", c.n}, {"px", c.px}, {"msg", c.msg}});
  return {c.n, c.px, c.msg};
}

} // namespace live::tdx::flash
