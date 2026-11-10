#include "live/tdx/Flash.hpp"

#include "live/Book.hpp"
#include "live/tdx/Ui.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cmath>
#include <map>
#include <thread>

namespace live::tdx::flash {

namespace {

// 通达信外部联动: 注册消息「Stock」投给主窗, wParam = 市场前缀 × 10⁶ + 代码, 图表即切到该只
constexpr const char *kLink = "Stock";
constexpr double kLinkRepost = 1, kLinkTimeout = 6; // 标题没换过来每几秒重投、共等几秒
constexpr const char *kMainWnd = "class=TdxW_MainFrame_Class";
const std::vector<std::string> kConfirmTitles = {"买入交易确认", "卖出交易确认", "交易确认"};
const std::string kResultTitle = "提示";
constexpr long kMsgText = 7013, kMsgYes = 7015; // 确认框 / 回报框: 正文 Static、确认按钮
constexpr long kEst = 8915;                     // 闪电窗按额时的「预估金额:…(N股)」

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

// 本单上下文
struct Ctx {
  const OrderReq &o;
  const Kind &k;
  std::string main, dlg, box, px_s;
  long n = 0;
};

using Tops = std::map<std::string, ui::Node>;

// 可见顶层窗口
Tops tops() {
  Tops out;
  for (ui::Node &n : ui::enum_())
    if (n.depth == 0 && n.vis)
      out.emplace(n.hwnd, std::move(n));
  return out;
}

// 确认 / 提示框正文, 没有该控件 → 空串
std::string body(const std::string &h) {
  std::vector<std::string> hits;
  for (const ui::Node &n : ui::enum_(h))
    if (n.depth == 1 && n.id == kMsgText)
      hits.push_back(n.hwnd);
  return hits.size() == 1 ? ui::squash(ui::text(hits[0])) : "";
}

// 做 act, 等其后新出现、标题在 titles 内的可见对话框 (须唯一); 超时报出期间冒出的新窗口及正文
template <class Act>
std::string popup(const std::vector<std::string> &titles, double timeout, Act &&act) {
  const Tops before = tops();
  act();
  std::string want;
  for (const std::string &t : titles)
    want += (want.empty() ? "" : "/") + t;
  const auto fresh = [&] {
    Tops out;
    for (auto &[h, n] : tops())
      if (!before.contains(h) && n.cls == "#32770")
        out.emplace(h, n);
    return out;
  };
  return *ui::wait(
      [&] {
        std::string s;
        for (const auto &[h, n] : fresh())
          s += (s.empty() ? "" : "; ") + ("「" + n.title + "」") + body(h);
        return "「" + want + "」, 期间新窗口: " + (s.empty() ? "无" : s);
      },
      [&]() -> std::optional<std::string> {
        std::vector<std::string> hits;
        for (const auto &[h, n] : fresh())
          if (std::find(titles.begin(), titles.end(), n.title) != titles.end())
            hits.push_back(h);
        if (hits.size() > 1)
          fail(strf("「%s」同时出现 %zu 个", want.c_str(), hits.size()));
        return hits.empty() ? std::nullopt : std::optional<std::string>(hits[0]);
      },
      timeout);
}

bool is_num(const std::string &t) {
  const size_t dot = t.find('.');
  const auto digits = [](std::string_view s) { return !s.empty() && std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isdigit(c); }); };
  return dot == std::string::npos ? digits(t) : digits(std::string_view(t).substr(0, dot)) && digits(std::string_view(t).substr(dot + 1));
}

long max_qty(const std::string &h) {
  const std::string t = ui::text(h);
  if (!is_num(t))
    fail("最大可买 / 可卖读到「" + t + "」");
  return static_cast<long>(std::stod(t));
}

// 股数: 给了 qty 即用, 否则 size 元按 lot 折股; 再以闪电窗给的最大可买 / 可卖 cap 封顶 (钱或券不够就按能做的最大量):
// 买入按步长取整、须够起买量; 按额卖出不足起卖量即抬到起卖量, 可卖不足即全卖 (零股只能一次卖完)
long order_qty(const OrderReq &o, double px, long cap) {
  const auto [first, step] = lot(o.sec.code);
  long n = o.qty ? static_cast<long>(o.qty) : static_cast<long>(o.size / px) / step * step;
  if (o.side == kBuy) {
    if (n < first)
      fail(strf("%.0f 元 / %.0f 股按 %g 不足起买量 %d 股", o.size, o.qty, px, first));
    cap = cap / step * step;
    if (cap < first)
      fail(strf("最大可买 %ld 股, 不足起买量 %d 股", cap, first));
  } else if (!o.qty) {
    n = std::max<long>(n, first);
  }
  need(cap >= 1, "最大可卖为 0");
  return std::min(n, cap);
}

// 写入后客户端的 EN_CHANGE 处理会先改写一下, 等读回恰为 s
void settext(const std::string &h, const std::string &s) {
  ui::settext(h, s);
  ui::wait([&] { return "写入「" + s + "」后读回「" + ui::text(h) + "」"; }, [&] { return ui::text(h) == s; }, 1);
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

// 图表切到 sec: 主窗标题已是「…-名称]」即跳过, 否则向主窗投联动消息、等标题换过来.
// 切到同一只重投无副作用: 界面线程忙时消息可能落空, 标题没换过来即每 kLinkRepost 秒重投
void go_to(const std::string &main, const Sec &x) {
  const std::string want = ui::bare("-" + x.name + "]");
  if (ui::bare(ui::title(main)).ends_with(want))
    return;
  if (x.mkt == Mkt::BJ)
    fail(x.code + " 北交所没有联动前缀");
  const unsigned msg = ui::regmsg(kLink);
  const long wp = (x.mkt == Mkt::SH ? 7 : 6) * 1'000'000L + std::stol(x.code);
  const auto t0 = std::chrono::steady_clock::now();
  int n = 0;
  while (!ui::bare(ui::title(main)).ends_with(want)) {
    const double dt = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    if (dt >= kLinkTimeout)
      fail(strf("%gs 内主窗未切到「%s」, 现为「%s」(联动 %ld 已投 %d 次)", kLinkTimeout, x.name.c_str(), ui::title(main).c_str(), wp, n));
    if (dt >= n * kLinkRepost) {
      ui::post(main, msg, wp);
      ++n;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

void switch_to(Ctx &c) {
  const std::vector<ui::Node> main = ui::find_wnd(kMainWnd);
  if (main.size() != 1)
    fail("找不到通达信主窗");
  c.main = main[0].hwnd;
  go_to(c.main, c.o.sec);
}

void open(Ctx &c) {
  for (const auto &[h, n] : tops())
    if (n.title == c.k.title)
      fail(strf("已有未关的「%s」窗", c.k.title));
  c.dlg = popup({c.k.title}, 3, [&] { ui::post(c.main, ui::kWmCommand, c.k.cmd); });
}

std::string form(Ctx &c, double &px_out) {
  const auto ids = ui::ctls(c.dlg, {{"code", c.k.code}, {"price", c.k.price}, {"qty", c.k.qty}, {"unit", c.k.unit}, {"quote", c.k.quote}, {"max", c.k.max}});
  const std::string got = ui::text(ids.at("code"));
  if (got != c.o.sec.code)
    fail("闪电窗证券 " + got + " ≠ " + c.o.sec.code);
  const std::string quote = ui::text(ids.at("quote"));
  if (quote.find("限价") == std::string::npos)
    fail("报价方式为「" + quote + "」, 需限价委托");
  const std::string dflt = *ui::wait(ui::said("闪电窗默认价"), [&]() -> std::optional<std::string> {
    std::string t = ui::text(ids.at("price"));
    return t.empty() ? std::nullopt : std::optional<std::string>(t); }, 1);
  const size_t dot = dflt.find('.');
  if (dot == std::string::npos)
    fail("默认价「" + dflt + "」定不出小数位");
  c.px_s = strf("%.*f", static_cast<int>(dflt.size() - dot - 1), c.o.px);
  const std::string unit = ui::text(ids.at("unit"));
  if (unit != "股" && unit != "元")
    fail("数量单位「" + unit + "」不认识");
  ui::wait(ui::said("闪电窗最大可买 / 可卖"), [&] { return !ui::text(ids.at("max")).empty(); }, 1);
  const long cap0 = max_qty(ids.at("max"));
  settext(ids.at("price"), c.px_s);
  const double px = std::stod(c.px_s);
  // 最大可买按默认价算; 改价后客户端未必即时重算, 按价反比折一份, 与重读的取小
  long cap = max_qty(ids.at("max"));
  cap = std::min(cap, c.o.side == kBuy ? static_cast<long>(cap0 * std::stod(dflt) / px) : cap0);
  c.n = order_qty(c.o, px, cap);
  if (unit == "股") {
    settext(ids.at("qty"), std::to_string(c.n));
  } else {
    // 向上取整到元: 多出的不足一股, 折回恰为 n 股
    settext(ids.at("qty"), std::to_string(static_cast<long>(std::ceil(std::round(c.n * px * 100) / 100))));
    const std::string est = ui::ctls(c.dlg, {{"est", kEst}}).at("est");
    const auto shares = [&]() -> long {
      const std::string t = ui::text(est);
      const std::string tail = "股)";
      const size_t open = t.rfind('(');
      if (!t.ends_with(tail) || open == std::string::npos || open + 1 + tail.size() > t.size())
        return -1;
      const std::string d = t.substr(open + 1, t.size() - open - 1 - tail.size());
      if (!is_num(d) || d.find('.') != std::string::npos || d.size() > 12)
        return -1;
      return std::stol(d);
    };
    ui::wait([&] { return strf("预估 %ld 股, 现为「%s」", c.n, ui::text(est).c_str()); }, [&] { return shares() == c.n; }, 1);
  }
  px_out = px;
  return strf("%ld股 @ %s (最大 %ld)", c.n, c.px_s.c_str(), cap);
}

void confirm(Ctx &c) {
  c.box = popup(kConfirmTitles, 5, [&] { ui::command(c.dlg, c.k.ok); });
  const std::string b = body(c.box);
  const std::string code = after(b, "股票代码:", false, 6, "");
  const std::string px = after(b, "委托价格:", true, 0, "");
  const std::string qty = after(b, "委托数量:", false, 0, "股");
  if (!(code == c.o.sec.code && is_num(px) && std::stod(px) == std::stod(c.px_s) && std::stol(qty) == c.n))
    fail(strf("确认框 (%s, %s, %s) ≠ 预期 (%s, %s, %ld): %s", code.c_str(), px.c_str(), qty.c_str(), c.o.sec.code.c_str(), c.px_s.c_str(), c.n, b.c_str()));
}

// 读回报、点掉; 等回报框、确认框、闪电窗都关了才算完 (下一单紧接着切图表、开窗)
std::string reply(Ctx &c) {
  const std::string res = popup({kResultTitle}, 10, [&] { ui::command(c.box, kMsgYes); });
  const std::string b = body(res);
  ui::command(res, kMsgYes);
  if (b.find("合同号") == std::string::npos)
    fail("委托未成功: " + b);
  const std::string mine[3] = {res, c.box, c.dlg};
  ui::wait(
      [&] {
        std::string s;
        for (const auto &[h, n] : tops())
          if (std::find(std::begin(mine), std::end(mine), h) != std::end(mine))
            s += "「" + n.title + "」";
        return "回报框 / 确认框 / 闪电窗关掉, 还开着: " + s;
      },
      [&] {
        const Tops t = tops();
        return std::none_of(std::begin(mine), std::end(mine), [&](const std::string &h) { return t.contains(h); });
      },
      3);
  return b;
}

} // namespace

const std::vector<std::string> &steps() {
  static const std::vector<std::string> s = {"切换", "开窗", "填单", "确认", "回报"};
  return s;
}

bool supports(int side, Credit credit) { return kind_of(side, credit) != nullptr; }

Placed place(const OrderReq &o, const Step &step) {
  const Kind *k = kind_of(o.side, o.credit);
  assert(k);
  assert((o.qty > 0) != (o.size > 0) && o.px > 0);
  Ctx c{o, *k, {}, {}, {}, {}};
  Placed p;
  step(0, "");
  switch_to(c);
  step(1, "");
  open(c);
  step(2, "");
  p.detail = form(c, p.px);
  p.qty = static_cast<double>(c.n);
  step(2, p.detail);
  step(3, "");
  confirm(c);
  step(4, "");
  p.msg = reply(c);
  return p;
}

} // namespace live::tdx::flash
