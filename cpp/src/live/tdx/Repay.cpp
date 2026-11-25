#include "live/tdx/Repay.hpp"

#include "live/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <map>
#include <optional>
#include <set>

namespace live::tdx::repay {

using win::Hwnd;

namespace {

// 页: 功能树上的名与上级、认页的标签; 各控件 id (还款金额框 / 确定 / 启用指定合约勾选框 / 可用金额)
const std::string kPage = "现金还款", kParent = "融资业务", kMarkText = "还款金额:";
constexpr long kMark = 203, kAmount = 315, kOk = 196, kPick = 1018, kAvail = 322;
// 选择合约框: 标题 / 列表 id (框开出后另查, 2 ~ 3 秒才有行; 其间条数标签写着共0条) / 认合约的列; 双击后的提示正文
const std::string kPickTitle = "选择合约", kIdCol = "流水号", kCodeCol = "证券代码", kNameCol = "证券名称";
constexpr long kPickList = 155;
constexpr double kListWait = 10;
const std::string kOne = "共有1笔合约";
const std::string kConfirmTitle = "交易确认", kKind = "现金还款", kDone = "已提交";
constexpr unsigned kBmGetCheck = 0xF0;
constexpr double kCent = 0.005;
constexpr double kFieldWait = 3;  // 页开出后可用金额由客户端另查才填上, 至多等几秒
constexpr double kSettleWait = 5; // 提交后页重查 (可用金额减下来), 至多等几秒

// 本适配开的框: 选择合约框 / 确认框 / 回报框
const std::vector<std::string> kOurs = {kPickTitle, kConfirmTitle, ui::kResult};

// 窗口的中心 (屏幕坐标)
std::pair<int, int> mid(const Hwnd &h) {
  const win::Node n = win::node(h);
  return {(n.rect[0] + n.rect[2]) / 2, (n.rect[1] + n.rect[3]) / 2};
}

// 列表的一行: 列名 → 字; 证券代码格中心的屏幕坐标 (双击处; 靠左的列, 不怕列表横向放不下)
struct Row {
  std::map<std::string, std::string> cell;
  int x = 0, y = 0;
};

// 列表 list 逐页读 (win::pages), 每页按表头拼成各行给 fn; fn 回 true 即停在这页 (行的坐标此刻有效), 回 true; 翻到底 → false
template <class F>
bool scan(const Hwnd &list, F &&fn) {
  std::optional<Hwnd> head;
  for (const win::Node &n : win::enum_(list))
    if (n.depth == 1 && n.cls == "SysHeader32")
      head = n.hwnd;
  need(head.has_value(), "选择合约列表没有表头");
  std::map<int, std::string> cols; // 列左缘 → 列名
  for (const win::Item &it : win::detail(*head).items)
    if (it.text)
      cols[it.l] = *it.text;
  const win::Node ln = win::node(list);
  return win::pages(list, [&](const win::Layer &l) {
    std::map<int, Row> rows; // 行上缘 → 行
    for (const win::Item &it : l.items)
      if (it.text && !it.text->empty())
        if (const auto c = cols.upper_bound(it.l); c != cols.begin()) {
          const std::string &name = std::prev(c)->second;
          Row &r = rows[it.t];
          r.cell[name] = *it.text;
          if (name == kCodeCol)
            r.x = ln.rect[0] + l.off[0] + (it.l + it.r) / 2, r.y = ln.rect[1] + l.off[1] + (it.t + it.b) / 2;
        }
    std::vector<Row> page;
    for (auto &[t, r] : rows)
      page.push_back(std::move(r));
    return fn(page);
  });
}

// 「启用指定合约」勾着即点掉 (框关了却还勾着的收尾)
void unpick(const Hwnd &pick) {
  if (win::send(pick, kBmGetCheck, 0, 0) == 0)
    return;
  const auto [x, y] = mid(pick);
  win::clickat(pick, x, y);
  until(said("「启用指定合约」取消勾选"), [&] { return win::send(pick, kBmGetCheck, 0, 0) == 0; }, 2);
}

// 本笔上下文: 页与控件; 选约填上的合约 / 应还; 填额后的额; 确认框; 回报
struct Ctx {
  Hwnd dlg, amount, ok, pick, avail;
  const std::vector<std::string> &codes; // 还的序
  const std::set<std::string> &done;
  double left = 0, cash = 0; // 余下额度 / 可用金额
  Repaid r;
  Hwnd box;
  std::string amt_s;
  bool sent = false;
};

// 选约: 没有可还的合约 → false (框关掉、取消勾选)
bool choose(Ctx &c) {
  need(win::send(c.pick, kBmGetCheck, 0, 0) == 0, "「启用指定合约」开头就勾着");
  const Hwnd box = ui::popup({kPickTitle}, 5, [&] {
    const auto [x, y] = mid(c.pick);
    win::clickat(c.pick, x, y);
  });
  const Hwnd list = win::ctls(box, {{"list", kPickList}}).at("list");
  until(said("选择合约列表查完 (有行)"), [&] { return !win::detail(list).items.empty(); }, kListWait, 0.2);
  // 行的 codes 序 (不在 codes 里 / 已还过 = codes.size())
  const auto rank = [&](const Row &r) {
    const auto id = r.cell.find(kIdCol), code = r.cell.find(kCodeCol);
    need(id != r.cell.end() && code != r.cell.end() && r.x > 0, "选择合约列表的行缺「%s」或「%s」", kIdCol.c_str(), kCodeCol.c_str());
    return c.done.contains(id->second) ? c.codes.size() : static_cast<size_t>(std::ranges::find(c.codes, code->second) - c.codes.begin());
  };
  size_t best = c.codes.size(); // 通读: 还有未还合约的、序最前的一只
  scan(list, [&](const std::vector<Row> &page) {
    for (const Row &r : page)
      best = std::min(best, rank(r));
    return false;
  });
  std::optional<Row> hit;
  if (best < c.codes.size())
    scan(list, [&](const std::vector<Row> &page) {
      for (const Row &r : page)
        if (rank(r) == best) {
          hit = r;
          return true;
        }
      return false;
    });
  if (!hit) {
    win::post(box, win::kWmClose, 0);
    until(said("「选择合约」关掉"), [&] { return !win::tops().contains(box); }, 3);
    unpick(c.pick);
    return false;
  }
  c.r.code = hit->cell.at(kCodeCol);
  c.r.mkt = market_of(c.r.code);
  c.r.contract = hit->cell.at(kIdCol);
  if (const auto n = hit->cell.find(kNameCol); n != hit->cell.end())
    c.r.name = n->second;
  trace::live(strf("合约 %s %s %s", c.r.code.c_str(), c.r.name.c_str(), c.r.contract.c_str()));
  const Hwnd ask = ui::popup({ui::kResult}, 5, [&] {
    for (const char *k : {"ldown", "lup", "ldbl", "lup"})
      win::mouse(k, hit->x, hit->y);
  });
  const std::string b = ui::body(ask);
  need(b.find(kOne) != std::string::npos, "双击合约后的提示不是「%s」: %s", kOne.c_str(), b.c_str());
  ui::yes(ask);
  c.r.due = *until(
      said("还款金额框填上合约应还"), [&]() -> std::optional<double> {
        if (win::tops().contains(box) || win::tops().contains(ask))
          return std::nullopt;
        const std::optional<double> v = parse_num(win::text(c.amount));
        return v && *v > 0 ? v : std::nullopt;
      },
      3);
  return true;
}

void fill(Ctx &c) {
  c.r.amount = std::min(c.left, c.r.due);
  c.amt_s = strf("%.2f", c.r.amount);
  if (c.r.amount < c.r.due - kCent) {
    win::settext(c.amount, c.amt_s);
    until([&] { return "写入「" + c.amt_s + "」后读回「" + win::text(c.amount) + "」"; },
          [&] { const std::optional<double> v = parse_num(win::text(c.amount)); return v && std::abs(*v - c.r.amount) < kCent; }, 1);
  }
  trace::live(strf("%s %s 应还 %.2f 还 %s", c.r.code.c_str(), c.r.contract.c_str(), c.r.due, c.amt_s.c_str()));
}

void confirm(Ctx &c) {
  c.box = ui::popup({kConfirmTitle}, 5, [&] { win::command(c.dlg, kOk); });
  const std::string b = ui::body(c.box);
  const std::optional<double> amt = parse_num(ui::field(b, "还款金额:", true));
  need(b.find(kKind) != std::string::npos && amt && std::abs(*amt - c.r.amount) < kCent, "确认框 ≠ 预期 (%s %s 元): %s", kKind.c_str(), c.amt_s.c_str(), b.c_str());
}

// 提交、读回报; 等回报框、确认框关了
void reply(Ctx &c) {
  c.r.msg = ui::submit(c.box, c.sent, kDone, "还款");
  // 提交后页另查一遍 (其间不理切页、点勾选框): 等可用金额减下来再往下做; 没等到只记一笔
  const double want = c.cash - c.r.amount;
  const bool seen = poll([&] { const std::optional<double> v = parse_num(win::text(c.avail)); return v && std::abs(*v - want) < kCent; }, kSettleWait, 0.2);
  trace::ev("avail", {{"want", want}, {"seen", seen}, {"now", win::text(c.avail)}});
}

} // namespace

std::vector<Repaid> run(const std::vector<std::string> &codes, double cap) {
  need(!codes.empty() && cap > 0, "现金还款: 没有要还的合约 / 额度 %.2f", cap);
  ui::sweep(kOurs);
  const Hwnd dlg = ui::form(kPage, kParent, kMark, kMarkText);
  const auto ids = win::ctls(dlg, {{"amount", kAmount}, {"ok", kOk}, {"pick", kPick}, {"avail", kAvail}});
  unpick(ids.at("pick"));
  const std::optional<double> avail = until([&] { return "可用金额填上, 现为「" + win::text(ids.at("avail")) + "」"; }, [&] { return parse_num(win::text(ids.at("avail"))); }, kFieldWait);
  std::set<std::string> done;
  std::vector<Repaid> out;
  double left = std::min(cap, *avail), cash = *avail;
  trace::note("repay", strf("现金还款 %zu 只, 额度 %.2f (可用金额 %.2f)", codes.size(), left, *avail), {{"codes", codes}, {"cap", cap}, {"avail", *avail}});
  while (left >= 0.01) {
    Ctx c{dlg, ids.at("amount"), ids.at("ok"), ids.at("pick"), ids.at("avail"), codes, done, left, cash, {}, {}, {}, false};
    bool more = true;
    const std::string err = ui::attempt(
        c.sent, "先查负债再重还",
        [&] {
          trace::Span span(strf("第 %zu 笔", out.size() + 1));
          if (!(more = choose(c)))
            return;
          fill(c);
          confirm(c);
          reply(c);
        },
        [&] { unpick(c.pick); });
    need(err.empty(), "%s %s: %s", c.r.code.c_str(), c.r.contract.c_str(), err.c_str());
    if (!more)
      break;
    done.insert(c.r.contract);
    left -= c.r.amount, cash -= c.r.amount;
    trace::note("repaid", strf("%s %s 合约 %s 应还 %.2f 还 %.2f · %s", c.r.code.c_str(), c.r.name.c_str(), c.r.contract.c_str(), c.r.due, c.r.amount, c.r.msg.c_str()),
                {{"code", c.r.code}, {"contract", c.r.contract}, {"due", c.r.due}, {"amount", c.r.amount}, {"msg", c.r.msg}});
    out.push_back(std::move(c.r));
  }
  return out;
}

} // namespace live::tdx::repay
