// Market 行情页右图例 (NAV / VAL / FLOW / CTA 同一骨架的右表) — 见 LinesCommon.hpp
#include "gui/task_market/ui/LinesCommon.hpp"

#include "disk/Disk.hpp"
#include "gui/util/Board.hpp" // GetBoardType / MarketAbbr
#include "gui/util/Chart.hpp" // tint / toggle_button / button_bar
#include "gui/util/HoverSync.hpp"
#include "gui/util/Reorder.hpp"
#include "gui/util/Sheet.hpp" // sheet_head / draw_cell
#include "gui/util/Tip.hpp"
#include "imgui.h"
#include "imgui_internal.h"  // TableFindByID (右表内容宽)
#include "shared/Basket.hpp" // basket_table

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <initializer_list>
#include <map>
#include <numeric>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace GUI::Market {

namespace {

// 行组名 [RowGroup] (= kGroupDefs::name; 类别条 = 前 PageDef::groups 个)
constexpr std::array<const char *, kRowGroups> kRowGroupName = [] {
  std::array<const char *, kRowGroups> n{};
  for (int g = 0; g < kRowGroups; ++g)
    n[static_cast<size_t>(g)] = kGroupDefs[g].name;
  return n;
}();

// ---- 自选 (星列 / 名格右键; ui.watch = 文件, 改了即存) ----
// 星 (Nerd Font: 实 = 已自选 / 空 = 未自选) + 实星色
constexpr const char *kStarOn = "\xef\x80\x85", *kStarOff = "\xef\x80\x86";
constexpr ImVec4 kStarCol(1.0f, 0.8f, 0.2f, 1.0f);
const SheetCol kStarHead = {"##star", "星 = 自选\n"
                                      "• 点星 = 加入 / 移出自选 (名格右键同); 实星 = 已在自选\n"
                                      "• 自选 = 类别条 自选 组: 全局一份, 各页同一份; 新加的排末尾, 原序下拖动调序\n"
                                      "• 适用: 篮子行 + 成分行; 因子 / 策略无"};
bool watched(const LinesState &ui, const std::string &w) { return std::ranges::find(ui.watch, w) != ui.watch.end(); }
// 加入 (排末尾) ↔ 移出
void watch_flip(LinesState &ui, const std::string &w) {
  assert(!w.empty());
  if (const auto it = std::ranges::find(ui.watch, w); it != ui.watch.end())
    ui.watch.erase(it);
  else
    ui.watch.push_back(w);
  SaveWatch(ui.watch);
}
// 星格: 先于行的 Selectable / TreeNode (跨全列) 交, 悬停归它
void star_cell(LinesState &ui, const std::string &w) {
  const bool on = watched(ui, w);
  ImGui::PushStyleColor(ImGuiCol_Button, ImVec4(0, 0, 0, 0));
  ImGui::PushStyleColor(ImGuiCol_Text, on ? kStarCol : ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
  if (ImGui::SmallButton(on ? kStarOn : kStarOff))
    watch_flip(ui, w);
  ImGui::PopStyleColor(2);
}

// 页 page 点行: 选中 ↔ 取消 (单选 = 换掉原选中); 多选里带行情键的行满 kQuotedPicks 不再加, 刚选中 → 其线在主图例里回默认显隐 (理想列显, 可成交列隐)
void pick(LinesState &ui, int page, const LinesRow &r) {
  const int m = kPageDefs[page].pick;
  std::map<std::string, Quote::Ref> &s = ui.pick.rows[m];
  if (s.erase(r.id)) {
    if (m == kPickMany)
      SavePick(ui.pick);
    return;
  }
  if (m == kPickOne)
    s.clear();
  else if (r.ref.ok() && std::ranges::count_if(s, [](const auto &x) { return x.second.ok(); }) >= static_cast<ptrdiff_t>(kQuotedPicks))
    return;
  s.emplace(r.id, r.ref);
  if (m == kPickMany) {
    for (int l = 0; l < kPairLines; ++l)
      ui.reveal.insert(line_id(r, l));
    SavePick(ui.pick);
  }
}

// 净值线的近况 (RecentOf 同一核): 日末视图自末个活跃日往前 (起点 = 首日前一日的日末), 前收 = 前一日末
Recent recent_of(const Line &c) {
  Mark m[kMarks]; // 新 → 旧
  size_t n = 0;
  for (int k = c.e.empty() ? c.f - 1 : c.l + 1; k >= c.f && n < std::size(m); --k)
    m[n++] = {c.end(k), k > c.f ? c.end(k - 1) : kNaN};
  return RecentOf({m, n});
}

// 右图例表头排序 (三张右表同一份; 三态: 升 → 降 → 原序): order (表行下标) 按当前排序列重排;
// 表列 0 = 星 (不排序); 名列 (c = 0) 按 name(a, b) 三路比较, 其余按 num(i, c), 不可算的恒在末
template <class Name, class Num>
void sort_rows(std::vector<size_t> &order, const Name &name, const Num &num) {
  const ImGuiTableSortSpecs *ss = ImGui::TableGetSortSpecs();
  if (!ss || ss->SpecsCount == 0)
    return;
  const int c = ss->Specs[0].ColumnIndex - 1;
  const bool desc = ss->Specs[0].SortDirection == ImGuiSortDirection_Descending;
  std::stable_sort(order.begin(), order.end(), [&](size_t a, size_t b) {
    if (c == 0) {
      const int r = name(a, b);
      return desc ? r > 0 : r < 0;
    }
    return sort_less(num(a, c), num(b, c), desc);
  });
}
// 名格悬停 (星列之后; 行 Selectable 跨全列, 只认本格) → 行的悬停
void name_tip(const LinesRow &r) {
  if (ImGui::IsItemHovered() && ImGui::TableGetHoveredColumn() == 1)
    row_tip(r);
}

// ---- 右表 (各页同一骨架 table): 列 = 星 + 名 + 各段 (Seg) 的列 (SheetCol), 格 = 文本 + 排序键 + 底色名次 (SheetCell);
//      格缓存 (sheet_of: 来源变才整组重填) → 列出 (listing: 篮子组按过滤条, 选中 / 展开行恒列) → 量列出行各列最宽 → 表头排序 (组内) → 画 (sheet) ----
// 涨跌 / 流入: 百分比两位, 按正负着色
SheetCell signed_cell(double y) { return sheet_cell(y, tint(y, 0.0), "%+.2f%%", 100.0 * y); }
// 带名次的值 (NAV 数值列 / VAL 估值分位): 按 fmt 显示 q.x, 底色 = q.rank
SheetCell ranked_cell(const Ranked &q, const char *fmt) {
  SheetCell c = sheet_cell(q.x, ImGui::GetStyleColorVec4(ImGuiCol_Text), fmt, q.x);
  c.rank = q.rank;
  return c;
}
// 一张右表: 组 g 的行, 列 = 星 + cols (cols[0] = 名); order = 列出的组内行 (按表头排序重排后画); ps = 该组的格 (sheet_of);
// on(t) = 选中 (高亮), 点行 → click(t) (t = 表行, 见 LinesSheet::at); 悬停表行 t 的数值列 k → hover(t, k).
// 宽由外面定: 名列伸满余宽, 内容更宽 → 横滚 (星 / 名列钉住). 篮子行名前 ▸ 展开 / 收起 (ui.pick.open, 至多一行): 展开行钉在表头下,
// 其下先列成分行 (缩进; 同列排序), 再列其余组内行; 字色 = 线色; 有自选键的行: 星格点 / 名格右键 = 加入 / 移出自选; 只画可见的行;
// 原序 = 指数组按池表待生效序 (pool_order; 全A 两行在首), 其余组按行号; 原序下拖动调序 (Reorder): 自选组 = 改 ui.watch 即存,
// 指数组子池 = 交 ui.pool_move (改池表); 悬停联动 (ui.hover): 图上悬停的行底色亮 + 滚到居中, 本表悬停的行 → ui.hover.row
template <class On, class Click, class Hover>
void sheet(const char *id, const std::vector<SheetCol> &cols, LinesState &ui, const LinesDerived &d, int g, std::vector<size_t> order,
           const LinesSheet &ps, const On &on, const Click &click, const Hover &hover) {
  const std::vector<LinesRow> &rs = d.rows[g];
  const size_t nc = cols.size() - 1, R = rs.size(); // 名之后的列 / 组内行
  assert(ps.nc == nc && ps.R == R && ps.cells.size() == ps.at.size() * nc);
  std::vector<size_t> by(R); // 原序位 → 组内行
  std::iota(by.begin(), by.end(), size_t{0});
  if (g == kGroupPool)
    std::ranges::stable_sort(by, {}, [&](size_t r) { return pool_rank(rs[r].pool); });
  std::vector<int> pos(R); // 组内行 → 原序位 (= 拖动下标; 成分行 t ≥ R 不动, 下标取 t)
  for (size_t k = 0; k < R; ++k)
    pos[by[k]] = static_cast<int>(k);
  std::ranges::sort(order, {}, [&](size_t r) { return pos[r]; });
  const auto key = [&](size_t t) { return t < R ? pos[t] : static_cast<int>(t); };
  const auto pin_at = std::ranges::find_if(order, [&](size_t r) { return rs[r].id == ui.pick.open; });
  const size_t pin = pin_at == order.end() ? SIZE_MAX : *pin_at; // 钉住的展开行
  if (pin != SIZE_MAX)
    order.erase(pin_at);
  // 横滚宽 = 上帧各列内容宽之和 (EndTable 按内容更新; 列不可拖 → 纯内容宽), 不足表宽 → 表宽 (名列伸满); 首帧 0 = 按内容
  const ImGuiTable *last = ImGui::TableFindByID(ImGui::GetID(id));
  const float inner = last ? std::max(last->ColumnsAutoFitWidth, ImGui::GetContentRegionAvail().x - ImGui::GetStyle().ScrollbarSize) : 0.f;
  if (!ImGui::BeginTable(id, static_cast<int>(cols.size() + 1),
                         ImGuiTableFlags_ScrollX | ImGuiTableFlags_ScrollY | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerV |
                             ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate,
                         ImVec2(0, 0), inner))
    return;
  ImGui::TableSetupScrollFreeze(2, pin == SIZE_MAX ? 1 : 2);
  ImGui::TableSetupColumn("##star", ImGuiTableColumnFlags_WidthFixed | ImGuiTableColumnFlags_NoSort,
                          ImGui::CalcTextSize(kStarOn).x + 2.0f * ImGui::GetStyle().FramePadding.x);
  ImGui::TableSetupColumn(cols[0].name.c_str(), ImGuiTableColumnFlags_WidthStretch);
  std::vector<float> cw(nc); // 列出行的最宽 (表头起)
  for (size_t k = 0; k < nc; ++k) {
    ImGui::TableSetupColumn(cols[1 + k].name.c_str());
    cw[k] = ImGui::CalcTextSize(cols[1 + k].name.c_str()).x;
  }
  sheet_head(static_cast<int>(cols.size() + 1), [&](int c) -> const SheetCol & { return c == 0 ? kStarHead : cols[static_cast<size_t>(c - 1)]; });
  const auto at = [&](size_t t) { return ps.cells.data() + t * nc; };
  const auto num = [&](size_t t, int c) { return at(t)[c - 1].key; };
  const auto row_of = [&](size_t t) -> const LinesRow & { return d.row(ps.at[t]); };
  sort_rows(
      order, [&](size_t a, size_t b) { // 名列: 按色相 (相似的挨着; 未定色 = 按 cn 字典序)
        const float ha = rs[a].hue, hb = rs[b].hue;
        if (!std::isnan(ha) && !std::isnan(hb))
          return (ha > hb) - (ha < hb);
        return rs[a].cn.compare(rs[b].cn);
      },
      num);
  // 拖动调序: 原序 (无表头排序) 下的子池 / 自选行; 全A 两行 / 自选里的账户行 / 钉住的展开行 / 成分行不动
  const ImGuiTableSortSpecs *ss = ImGui::TableGetSortSpecs();
  const bool drag = (g == kGroupPool || g == kGroupWatch) && (!ss || ss->SpecsCount == 0);
  const auto movable = [&](size_t t) {
    return drag && t < R && t != pin && (g != kGroupPool || rs[t].pool >= kPinnedPools) && (g != kGroupWatch || !rs[t].watch.empty());
  };
  Reorder rd((std::string(id) + "##order").c_str());
  std::vector<size_t> lines; // 钉行之下的画序: 钉行的成分行 t ≥ R, 再其余组内行 t < R
  if (pin != SIZE_MAX) {
    for (size_t t = R; t < ps.at.size(); ++t)
      lines.push_back(t);
    sort_rows(lines, [&](size_t a, size_t b) { return row_of(a).cn.compare(row_of(b).cn); }, num); // 名列: 按简称
    for (size_t k = 0; k < nc; ++k)
      cw[k] = std::max(cw[k], at(pin)[k].text.w);
  }
  lines.insert(lines.end(), order.begin(), order.end());
  for (const size_t t : lines)
    for (size_t k = 0; k < nc; ++k)
      cw[k] = std::max(cw[k], at(t)[k].text.w);
  // 图上悬停的行: 滚到其行居中 (钉住的展开行不滚)
  const std::optional<std::string> &plot_hot = ui.hover.plot;
  const auto lit = std::ranges::find_if(lines, [&](size_t t) { return row_of(t).id == plot_hot; });
  const int jump = ui.hover.follow(lit != lines.end() ? static_cast<int>(lit - lines.begin()) : -1);
  std::optional<std::string> hot;
  constexpr ImGuiTreeNodeFlags kNode = ImGuiTreeNodeFlags_SpanAllColumns | ImGuiTreeNodeFlags_NoTreePushOnOpen;
  const auto row = [&](size_t t) {
    ImGui::TableNextRow();
    ImGui::PushID(static_cast<int>(t)); // 自选组里同一只可既是自选行又是展开行的成分行
    const LinesRow &h = row_of(t);
    if (h.id == plot_hot)
      hot_row();
    ImGui::TableSetColumnIndex(0);
    if (jump >= 0 && t == lines[static_cast<size_t>(jump)])
      ImGui::SetScrollHereY(0.5f);
    if (!h.watch.empty())
      star_cell(ui, h.watch);
    ImGui::TableSetColumnIndex(1);
    const std::string label = h.cn + "##" + h.id;
    ImGui::PushStyleColor(ImGuiCol_Text, row_color(d, ps.at[t], kTrade));
    if (t >= R) {
      ImGui::Indent();
      ImGui::TreeNodeEx(label.c_str(), kNode | ImGuiTreeNodeFlags_Leaf | (on(t) ? ImGuiTreeNodeFlags_Selected : 0));
      ImGui::Unindent();
      rd.item(key(t), false);
      if (rd.tapped(key(t)))
        click(t);
    } else if (h.opens()) {
      const bool open = t == pin;
      ImGui::SetNextItemOpen(open, ImGuiCond_Always);
      ImGui::TreeNodeEx(label.c_str(), kNode | ImGuiTreeNodeFlags_OpenOnArrow | (on(t) ? ImGuiTreeNodeFlags_Selected : 0));
      rd.item(key(t), movable(t));
      if (ImGui::IsItemToggledOpen()) {
        ui.pick.open = open ? std::string{} : h.id;
        ImGui::SetScrollY(0.f);
      } else if (rd.tapped(key(t))) {
        click(t);
      }
    } else {
      ImGui::Selectable(label.c_str(), on(t), ImGuiSelectableFlags_SpanAllColumns);
      rd.item(key(t), movable(t));
      if (rd.tapped(key(t)))
        click(t);
    }
    ImGui::PopStyleColor();
    const int hov = ImGui::IsItemHovered() ? ImGui::TableGetHoveredColumn() - 2 : -1; // 行跨全列: 悬停的数值列
    if (ImGui::IsItemHovered())
      hot = h.id;
    name_tip(h);
    if (!h.watch.empty() && ImGui::BeginPopupContextItem("##watch")) {
      if (ImGui::MenuItem(watched(ui, h.watch) ? "移出自选" : "加入自选"))
        watch_flip(ui, h.watch);
      ImGui::EndPopup();
    }
    for (size_t k = 0; k < nc; ++k) {
      ImGui::TableSetColumnIndex(static_cast<int>(2 + k));
      draw_cell(at(t)[k], cw[k]);
    }
    if (hov >= 0)
      hover(t, hov);
    ImGui::PopID();
  };
  if (pin != SIZE_MAX) // 冻结行须在 clipper 之前交
    row(pin);
  ImGuiListClipper clip;
  clip.Begin(static_cast<int>(lines.size()));
  if (jump >= 0)
    clip.IncludeItemByIndex(jump);
  while (clip.Step())
    for (int i = clip.DisplayStart; i < clip.DisplayEnd; ++i)
      row(lines[static_cast<size_t>(i)]);
  ImGui::EndTable();
  ui.hover.row = hot;
  // 落定: 原序位 → 池表待生效序 / 自选键的移动 (原序与二者同序, 端点照搬即同一移动)
  const std::optional<Move> m = rd.end();
  if (!m)
    return;
  const LinesRow &a = rs[by[static_cast<size_t>(m->from)]], &b = rs[by[static_cast<size_t>(m->to)]];
  if (g == kGroupPool) {
    ui.pool_move = Move{pool_rank(a.pool), pool_rank(b.pool)};
    return;
  }
  assert(g == kGroupWatch);
  const auto index = [&](const std::string &w) { return static_cast<int>(std::ranges::find(ui.watch, w) - ui.watch.begin()); };
  apply_move(ui.watch, {index(a.watch), index(b.watch)});
  SaveWatch(ui.watch);
}

// ---- 当日四列 (TodayCol; NAV / FLOW 右表全四列, VAL 只数 / 总市值; 不随区间): 只数 / 总市值 / 换手不着色, 流入按正负着色; 换手 / 流入格悬停看分子分母金额 ----
const SheetCol kTodayCols[kTodays] = {
    {"只数", "只数 = 成分只数; 个股行 = 交易所.板块.行业首字 (如 SH.主.电 = 沪市主板 电子)\n"
             "• 算法: 成员表的成分计数 (同切片 .等 / .加 同一份); 个股行: 板块按代码前缀, 行业 = 申万一级 (同 ASSET 的 板块 / 行业 列)\n"
             "• 数据: 成员表 = 历史末日收盘定 (= 当日 bind 给新浪行情的那份); 行业 = cn_stock_industry_component 最新月度快照\n"
             "• 显示: 整数, 不着色; 个股行按板块排序\n"
             "• 账户行: 持仓只数 (券商实时持仓, 未连券商 = 回放末日收后持仓; 含库轴外的 ETF / LOF)\n"
             "• 适用: 指数 / 行业 / 概念 / 自选 + 成分行; 不随区间"},
    {"总市值", "总市值 = 成分总市值之和\n"
               "• 算法: Σ 成分 末日收盘 × 总股本; 加权篮子当日接上再 × 当日涨跌 (今收 / 前收), 等权篮子停在末日; 成分行 = 本只 (基本面末日总市值, 当日接上再 × 今收 / 前收)\n"
               "• 数据: 成员表同只数; 总股本取末日适用\n"
               "• 显示: 亿 / 万亿, 不着色\n"
               "• 账户行: 此刻净资产 = 净资产 (券商 / 回放末日) + Σ 持仓 股 × (新浪当日收 − 计价)\n"
               "• 适用: 指数 / 行业 / 概念 / 自选 + 成分行; 不随区间"},
    {"换手", "换手 = 当日换手率\n"
             "• 算法: 成交额 / 总市值; 成交额 = Σ 成分 当日成交额 (成分行 = 本只), 总市值同上列 (总股本口径, 非流通)\n"
             "• 数据: 新浪行情日 (盘后 / 重启照样全天); 无新浪行情日 / 当日无成交 → -\n"
             "• 显示: 百分比两位, 不着色; 悬停格看成交额 / 总市值\n"
             "• 账户行: 今日成交额 (券商成交, 买 + 卖) / 此刻净资产; 未连券商 → -\n"
             "• 适用: 指数 / 行业 / 概念 / 自选 + 成分行; 不随区间"},
    {"流入", "流入 = 当日资金流入占成交额\n"
             "• 算法: 净流入 / 成交额; 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法; 成分行 = 本只): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟\n"
             "• 数据: 新浪行情日分钟根 (快照合成 + 分钟 K 补), 含开收盘集合竞价; 成分之和不加权, 分不出大单\n"
             "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额\n"
             "• 账户行: (今日买入额 − 卖出额) / 今日成交额 (券商成交); 未连券商 / 今日无成交 → -\n"
             "• 适用: 指数 / 行业 / 概念 / 自选 + 成分行; 不随区间"},
};
// 金额 y [亿元] 的显示: ≥ 1 万亿按万亿, 其余按亿 (越大小数越少); 带符号 sign
std::string yi_text(double y, bool sign = false) {
  const double a = std::abs(y);
  char s[32];
  const char *pm = !sign ? "" : y < 0 ? "-"
                                      : "+";
  if (a >= 1e4)
    std::snprintf(s, sizeof(s), a >= 1e6 ? "%s%.0f万亿" : "%s%.1f万亿", pm, a / 1e4);
  else
    std::snprintf(s, sizeof(s), a >= 10 ? "%s%.0f亿" : a >= 1 ? "%s%.1f亿"
                                                              : "%s%.2f亿",
                  pm, a);
  return s;
}
// 表行的源: 位置 / 行 + 基本面档案 (个股才有, 无 → nullptr) + 行情源 (QuoteHub::of, 与叠加图同一查法; 无行情键 / 行情未起 = 空)
// + 当日 (源的当日有成交才有, 否则 nullptr) 及其前一交易日
struct RowSrc {
  LinesAt at;
  const LinesRow *row = nullptr;
  const StockInfo *info = nullptr;
  Source src;
  const Quote::Day *day = nullptr;
  misc::Ymd prev = 0;
};
RowSrc src_of(const Quote::QuoteHub *mk, const StockInfos &infos, const LinesDerived &d, const LinesAt &a) {
  RowSrc x{a, &d.row(a)};
  const Quote::Ref q = x.row->ref;
  if (!q.ok())
    return x;
  if (q.is(Quote::kStock) && q.key < infos.size() && infos[q.key].known)
    x.info = &infos[q.key];
  if (!mk)
    return x;
  x.src = mk->of(q);
  x.day = x.src.today;
  x.prev = x.day ? mk->live->prev : 0;
  return x;
}
// 篮子行的当日 (SheetToday): 只数 / 总市值 = Census::members / cap (加权线的总市值当日接上 × 当日涨跌 = 成员总市值的当日变化; 等权线停在末日收盘);
// 成交额 / 流入 = 当日 Day (不看接续); 换手 = 成交额 / 总市值
SheetToday basket_today(const Quote::Census &t, const RowSrc &x) {
  SheetToday o;
  if (!x.src.se || t.members.empty())
    return o;
  const size_t l = x.row->ref.key;
  assert(l < t.members.size());
  double w = t.cap[l];
  if (basket_table().weight(l) == pool::kWeightCap && x.src.joined)
    w *= x.day->candle.close / x.day->candle.ref;
  o.v[kTodayMembers] = t.members[l];
  o.v[kTodayCap] = w > 0 ? w : kNaN;
  if (x.day && x.day->candle.amount > 0)
    o.amount = x.day->candle.amount / 1e8, o.v[kTodayFlow] = x.day->candle.flow.net / x.day->candle.amount;
  o.v[kTodayTurn] = o.amount / o.v[kTodayCap];
  return o;
}
// 个股行的当日 (SheetToday): 总市值 = 档案末日收盘总市值 (当日接上档案末日再 × 当日涨跌); 成交额 / 流入 = 当日 Day (本只分钟涨跌法);
// 换手 = 成交额 / 总市值; 无只数 (只数格画板块, 见 board_cell)
SheetToday stock_today(const RowSrc &x) {
  SheetToday o;
  double cap = x.info ? x.info->mcap : kNaN;
  if (x.info && x.day && x.prev == x.info->update_date && x.day->candle.ref > 0)
    cap *= x.day->candle.close / x.day->candle.ref;
  o.v[kTodayCap] = cap > 0 ? cap : kNaN;
  if (x.day && x.day->candle.amount > 0)
    o.amount = x.day->candle.amount / 1e8, o.v[kTodayFlow] = x.day->candle.flow.net / x.day->candle.amount;
  o.v[kTodayTurn] = o.amount / o.v[kTodayCap];
  return o;
}
// 账户行的当日 (SheetToday, 账户自身口径): 只数 = 持仓只数; 总市值 = 此刻净资产 (Holdings::now, y = 当日);
// 成交额 = 今日券商成交 (未连券商 → 换手 / 流入不可算), 换手 = 成交额 / 净资产, 流入 = (买入 − 卖出) / 成交额
SheetToday account_today(const Quote::Holdings &h, const Quote::Live *y) {
  SheetToday o;
  const double net = h.now(y) / 1e8;
  o.v[kTodayMembers] = static_cast<double>(h.list.size());
  o.v[kTodayCap] = net > 0 ? net : kNaN;
  if (!h.broker)
    return o;
  o.amount = h.amount / 1e8;
  o.v[kTodayTurn] = o.amount / o.v[kTodayCap];
  if (h.amount > 0)
    o.v[kTodayFlow] = (2.0 * h.buy - h.amount) / h.amount;
  return o;
}
// 表行的当日: 按行情键 (篮子 = 价格库篮子史的该线 + 当日; 个股 = 档案 + 当日; 账户 = 持仓 hold + 当日 y; 无 / 篮子史未到 / 无持仓 = 全不可算)
SheetToday today_of(const Quote::Census *bt, const Quote::Holdings *hold, const Quote::Live *y, const RowSrc &x) {
  const Quote::Ref q = x.row->ref;
  if (q.is(Quote::kBasket))
    return bt ? basket_today(*bt, x) : SheetToday{};
  if (q.is(Quote::kStock))
    return stock_today(x);
  return q.is(Quote::kAccount) && hold ? account_today(*hold, y) : SheetToday{};
}
// 表行的近况 (RecentOf 同一核): 有行情键 = 行情源 (篮子 / 个股同 ASSET 一查法); 无 = 可成交线日末
Recent recent_row(const RowSrc &x) {
  if (x.row->ref.ok())
    return RecentOf(x.src);
  return recent_of(*x.row->line[kTrade]);
}
// 四列 → out[0, kTodays)
void today_cells(const SheetToday &t, SheetCell *out) {
  const ImVec4 &txt = ImGui::GetStyleColorVec4(ImGuiCol_Text);
  const double *v = t.v;
  out[kTodayMembers] = sheet_cell(v[kTodayMembers], txt, "%.0f", v[kTodayMembers]);
  out[kTodayCap] = sheet_cell(v[kTodayCap], txt, "%s", yi_text(v[kTodayCap]).c_str());
  out[kTodayTurn] = sheet_cell(v[kTodayTurn], txt, "%.2f%%", 100.0 * v[kTodayTurn]);
  out[kTodayFlow] = signed_cell(v[kTodayFlow]);
}
// 个股行的只数格 = 交易所.板块.行业首字 (同 ASSET 的 板块 + 行业 列; 无档案 → 到板块止); 排序键 = 板块
SheetCell board_cell(const RowSrc &x) {
  const BoardType b = GetBoardType(x.row->en);
  std::string s = MarketAbbr(b);
  if (x.info && !x.info->ind_name.empty()) {
    size_t n = 1; // UTF-8 首字的字节数
    while (n < x.info->ind_name.size() && (static_cast<unsigned char>(x.info->ind_name[n]) & 0xC0) == 0x80)
      ++n;
    s += "." + x.info->ind_name.substr(0, n);
  }
  return {num_cell(ImGui::GetStyleColorVec4(ImGuiCol_Text), "%s", s.c_str()), static_cast<double>(b)};
}
// 悬停四列第 k 列 (k 不在 [0, kTodays) = 别的列) → 换手 / 流入的分子分母
void today_tip(const SheetToday &t, int k) {
  if (k == kTodayTurn && std::isfinite(t.v[kTodayTurn]))
    tip("换手 %.2f%% = 成交额 %s / 总市值 %s", 100.0 * t.v[kTodayTurn], yi_text(t.amount).c_str(), yi_text(t.v[kTodayCap]).c_str());
  else if (k == kTodayFlow && std::isfinite(t.v[kTodayFlow]))
    tip("流入 %+.2f%% = 净流入 %s / 成交额 %s", 100.0 * t.v[kTodayFlow], yi_text(t.v[kTodayFlow] * t.amount, true).c_str(), yi_text(t.amount).c_str());
}
// 篮子行的当日过 f (见 BasketFilter)
bool passes(const BasketFilter &f, const SheetToday &t) {
  const auto in = [](double x, double lo, double hi) { return (lo <= 0 && hi <= 0) || (std::isfinite(x) && x >= lo && (hi <= 0 || x <= hi)); };
  return in(t.v[kTodayMembers], f.n_lo, f.n_hi) && in(t.v[kTodayCap], f.cap_lo, f.cap_hi) &&
         (f.flow <= 0 || (std::isfinite(t.v[kTodayFlow]) && 100.0 * std::abs(t.v[kTodayFlow]) >= f.flow));
}
// 过滤条一项的界 [lo, hi] (0 = 该端不限)
template <class T>
struct Bound {
  T lo, hi;
};
// 界的摘要: 不限 / ≥lo / ≤hi / lo~hi (值按 fmt)
template <class T, class Fmt>
std::string bound_text(T lo, T hi, const Fmt &fmt) {
  if (lo > 0 && hi > 0)
    return fmt(lo) + "~" + fmt(hi);
  if (lo > 0)
    return "≥" + fmt(lo);
  if (hi > 0)
    return "≤" + fmt(hi);
  return "不限";
}
// 过滤条一项: 按钮 = 名 + 摘要 (设了 → 按下色), 点 = 弹出 预设 (点即设) + 精确输入 (in_fmt; 0 = 不限); hi = nullptr → 只有下限
template <class T, class Fmt>
void bound_chip(const char *id, const char *name, T &lo, T *hi, std::initializer_list<Bound<T>> presets, const char *in_fmt, const Fmt &fmt) {
  const T h = hi ? *hi : T{};
  if (toggle_button((std::string(name) + " " + bound_text(lo, h, fmt) + "###" + id).c_str(), lo > 0 || h > 0))
    ImGui::OpenPopup(id);
  if (!ImGui::BeginPopup(id))
    return;
  for (const Bound<T> &p : presets)
    if (ImGui::Selectable(bound_text(p.lo, p.hi, fmt).c_str(), p.lo == lo && p.hi == h)) {
      lo = p.lo;
      if (hi)
        *hi = p.hi;
    }
  ImGui::Separator();
  constexpr ImGuiDataType kType = std::is_integral_v<T> ? ImGuiDataType_S32 : ImGuiDataType_Float;
  const float w = ImGui::CalcTextSize("0000000").x + 2.0f * ImGui::GetStyle().FramePadding.x;
  ImGui::TextDisabled("≥");
  ImGui::SameLine(0, 2);
  ImGui::SetNextItemWidth(w);
  ImGui::InputScalar("##lo", kType, &lo, nullptr, nullptr, in_fmt);
  lo = std::max(lo, T{});
  if (hi) {
    ImGui::SameLine();
    ImGui::TextDisabled("≤");
    ImGui::SameLine(0, 2);
    ImGui::SetNextItemWidth(w);
    ImGui::InputScalar("##hi", kType, hi, nullptr, nullptr, in_fmt);
    *hi = std::max(*hi, T{});
  }
  ImGui::SameLine();
  ImGui::TextDisabled("(0 = 不限)");
  ImGui::EndPopup();
}
// 篮子组右表的过滤条 (各页同一份): [只数 …] [市值 …] [|流入| …] 列出 / 总数; 改动下帧生效
void filter_bar(BasketFilter &f, size_t listed, size_t total) {
  const auto pct = [](float v) {
    char s[16];
    std::snprintf(s, sizeof(s), "%g%%", v);
    return std::string(s);
  };
  bound_chip<int>("pf_f_n", "只数", f.n_lo, &f.n_hi, {{0, 0}, {0, 50}, {0, 200}, {0, 500}, {50, 0}, {200, 0}}, "%d",
                  [](int v) { return std::to_string(v); });
  ImGui::SameLine(0, 2);
  bound_chip<float>("pf_f_cap", "市值", f.cap_lo, &f.cap_hi, {{0, 0}, {500, 0}, {2000, 0}, {10000, 0}, {0, 2000}, {0, 10000}}, "%.0f",
                    [](float v) { return yi_text(v); });
  ImGui::SameLine(0, 2);
  bound_chip<float>("pf_f_flow", "|流入|", f.flow, nullptr, {{0, 0}, {1, 0}, {2, 0}, {5, 0}, {10, 0}}, "%.1f", pct);
  ImGui::SameLine();
  ImGui::TextDisabled("%zu/%zu", listed, total);
  if (ImGui::IsItemHovered())
    tip("过滤本组的行 (指数 / 行业 / 概念各一份, 各页共用): 只数 / 总市值 [亿元] 在区间内, |资金流入| [%%] ≥ 下限\n"
        "• 点一项 = 弹出: 预设 (点即设) / 精确输入 (0 = 该端不限); 设了的项按下色\n"
        "• 设了的条件缺值 (行情未到 / 无新浪行情日) 即不列; 已选中 / 展开的行恒列\n"
        "• 本处 = 列出 / 总数");
}
// 右表的一段列 (SegDef 的实例): 列 + 表行的格 + 悬停格 + 段自己的输入 (进格缓存键; 公共来源已在键里)
struct Seg {
  std::vector<SheetCol> cols;
  std::function<void(const RowSrc &, size_t, LinesSheet &, SheetCell *)> cell; // 表行 (源, 下标) → 段内格 out[0, cols)
  std::function<void(const LinesSheet &, size_t, int)> tip;                    // 悬停表行的段内列 k; 空 = 无
  std::vector<int64_t> key;
};
size_t cols_in(const std::vector<Seg> &segs) {
  size_t n = 0;
  for (const Seg &s : segs)
    n += s.cols.size();
  return n;
}
// 名列 + 各段的列
std::vector<SheetCol> cols_of(const SheetCol &name, const std::vector<Seg> &segs) {
  std::vector<SheetCol> o{name};
  for (const Seg &s : segs)
    o.insert(o.end(), s.cols.begin(), s.cols.end());
  return o;
}
// 页 page 组 g 的格 (ui.sheet[page]): 键 (公共来源 + 各段的输入) 变 → 全部表行重填: 表行 = 组内行 + 展开行在本组时其下的成分行
// (成分组前 n_cons 行); 逐表行: 源 → 当日 + 各段依次填格
const LinesSheet &sheet_of(LinesState &ui, const Quote::QuoteHub *mk, const StockInfos &infos, const LinesDerived &d, int page, int g,
                           const std::vector<Seg> &segs) {
  std::vector<int64_t> in;
  for (const Seg &s : segs)
    in.insert(in.end(), s.key.begin(), s.key.end());
  const LinesSheetKey k{g, ui.view.gen, infos.size(), std::move(in), ImGui::GetFontSize(), ImGui::GetColorU32(ImGuiCol_Text)};
  const Quote::Census *bt = mk && mk->view ? mk->view->census.get() : nullptr;
  ui.sheet[page].update(k, [&](LinesSheet &ps) {
    ps.nc = cols_in(segs);
    ps.R = d.rows[g].size();
    ps.at.clear();
    for (size_t r = 0; r < ps.R; ++r)
      ps.at.push_back({g, r});
    if (std::ranges::any_of(d.rows[g], [&](const LinesRow &r) { return r.id == ui.pick.open; }))
      for (size_t r = 0; r < d.n_cons; ++r)
        ps.at.push_back({kGroupKid, r});
    const size_t n = ps.at.size();
    ps.today.clear();
    ps.flow.assign(n, {});
    ps.cells.assign(n * ps.nc, {});
    for (size_t t = 0; t < n; ++t) {
      const RowSrc x = src_of(mk, infos, d, ps.at[t]);
      ps.today.push_back(today_of(bt, d.hold.get(), mk ? mk->live.get() : nullptr, x));
      SheetCell *out = ps.cells.data() + t * ps.nc;
      for (const Seg &s : segs)
        s.cell(x, t, ps, out), out += s.cols.size();
    }
  });
  return ui.sheet[page].val;
}
// 自选组表上一行: 项数 + 找不到行的自选键 (lost; 点 = 一并移出)
void watch_bar(LinesState &ui, size_t n, const std::vector<std::string> &lost) {
  ImGui::TextDisabled("%zu 项", n);
  if (ImGui::IsItemHovered())
    tip("自选 = 星列 / 名格右键加入的篮子行与成分行; 新加的排末尾, 原序下左键 / 中键拖动调序\n"
        "• 全局一份 (%s), 与组合无关, 各页同一份\n"
        "• 与原行同一份选中 / 展开 (篮子行照样 ▸ 展开); 数值列底色 = 自选之间名次",
        disk::path(disk::WatchFile).string().c_str());
  if (lost.empty())
    return;
  ImGui::SameLine();
  std::string keys;
  for (const std::string &w : lost)
    keys += "\n• " + w;
  if (ImGui::SmallButton(("移出失效 " + std::to_string(lost.size())).c_str())) {
    std::erase_if(ui.watch, [&](const std::string &w) { return std::ranges::find(lost, w) != lost.end(); });
    SaveWatch(ui.watch);
  }
  if (ImGui::IsItemHovered())
    tip("找不到行的自选键 (概念换快照没了 / 代码不在资产轴), 点 = 一并移出:%s", keys.c_str());
}
// 组 g 右表列出的行: 有过滤条的组 (kGroupDefs::filter) = 过过滤条的 ∪ 选中的 (on) ∪ 展开的 (表上画过滤条), 其余组全列 (自选组表上画项数)
template <class On>
std::vector<size_t> listing(LinesState &ui, const LinesSheet &ps, const LinesDerived &d, int g, const On &on) {
  const std::vector<LinesRow> &rs = d.rows[g];
  const bool filter = kGroupDefs[g].filter;
  std::vector<size_t> o;
  const size_t n = ps.R;
  for (size_t r = 0; r < n; ++r)
    if (!filter || on(r) || rs[r].id == ui.pick.open || passes(ui.filter[g], ps.today[r]))
      o.push_back(r);
  if (filter)
    filter_bar(ui.filter[g], o.size(), n);
  else if (g == kGroupWatch)
    watch_bar(ui, n, d.watch_lost);
  return o;
}
// 页 page 的右表 (各页同一条; 组 = 类别条选中的): 格 (sheet_of) → 列出 (listing) → 画 (sheet: 点表行 = 按页的选中方式选 / 取消,
// 悬停数值格 → 所在段的 tip)
void table(LinesState &ui, const Quote::QuoteHub *mk, const StockInfos &infos, const LinesDerived &d, int page, const SheetCol &name,
           const std::vector<Seg> &segs) {
  const int g = ui.cat[page];
  const LinesSheet &ps = sheet_of(ui, mk, infos, d, page, g, segs);
  const auto on = [&](size_t t) { return picked(ui, page, d.row(ps.at[t])); };
  const std::vector<size_t> order = listing(ui, ps, d, g, on);
  ImGui::PushID(static_cast<int>(ps.nc)); // 列数可变 (资金流段随档位)
  sheet(
      kPageDefs[page].table, cols_of(name, segs), ui, d, g, order, ps, on, [&](size_t t) { pick(ui, page, d.row(ps.at[t])); },
      [&](size_t t, int k) {
        for (const Seg &s : segs) {
          const int n = static_cast<int>(s.cols.size());
          if (k < n) {
            if (s.tip)
              s.tip(ps, t, k);
            return;
          }
          k -= n;
        }
      });
  ImGui::PopID();
}
// 段: 当日四列的前 n 列 (NAV / FLOW = 四列, VAL = 只数 / 总市值)
Seg today_seg(int n = kTodays) {
  return {{std::begin(kTodayCols), std::begin(kTodayCols) + n},
          [n](const RowSrc &x, size_t t, LinesSheet &p, SheetCell *out) {
            SheetCell c[kTodays];
            today_cells(p.today[t], c);
            if (x.row->ref.is(Quote::kStock))
              c[kTodayMembers] = board_cell(x);
            std::copy_n(c, n, out);
          },
          [](const LinesSheet &p, size_t t, int k) { today_tip(p.today[t], k); }};
}

// ---- 段 (SegKind; 页表 PageDef::segs 拼): 近况 | NAV 数值列 | 各口估值分位 | 资金流; 当日四列见上 ----
// 近况段 (不随区间; Recent)
enum RecentCol : int { kRecentChg,
                       kRecentMa5 };
constexpr int kRecents = 2;
const SheetCol kRecentCols[kRecents] = {
    {"涨幅", "涨幅 = 当日涨跌幅\n"
             "• 算法: 最新一日收盘 / 前收 − 1\n"
             "• 数据: 篮子 = Market 篮子历史 (当日接上 = 新浪快照合成, 否则 all.bar 末日); 成分行 = 个股同一查法 (价格库日脊末段 + 新浪当日); 因子 / 策略 = 净值末点\n"
             "• 显示: 百分比两位, 按正负着色\n"
             "• 适用: 全部组 + 成分行; 不随区间"},
    {"Δ5日线", "Δ5日线 = 对 5 日均线的偏离\n"
               "• 算法: 最新一日收盘 / MA5 (含当日) − 1\n"
               "• 数据: 篮子 = Market 篮子历史 (当日接上 = 新浪快照合成, 否则 all.bar 末日); 成分行 = 个股同一查法; 因子 / 策略 = 净值末 5 点\n"
               "• 显示: 百分比两位, 按正负着色\n"
               "• 适用: 全部组 + 成分行; 不随区间"},
};
// 右段 [NavNum]
constexpr const char *kNavNumFmt[kNavNums] = {"%+.1f%%", "%+.1f%%", "%+.2f", "%.1f倍"};
const SheetCol kNavNumCols[kNavNums] = {
    {"可成交", "可成交 = 可成交线区间年化\n"
               "• 算法: 区间起点归一, 按区间复利年化\n"
               "• 数据: 篮子 = 标签指数 (收盘封板不建仓 / 平仓顺延); 因子 / 策略 = 有冲击净值; 成分行 = 后复权收盘\n"
               "• 显示: 百分比一位, 底色 = 组内名次 (高绿 → 低红; 成分行 = 成分之间)\n"
               "• 适用: 全部组 + 成分行; 随区间"},
    {"理想", "理想 = 理想线区间年化\n"
             "• 算法: 区间起点归一, 按区间复利年化\n"
             "• 数据: 篮子 = 价格指数 (成员 / 权重取前一日收盘, 不看可成交); 因子 / 策略 = 无冲击净值; 成分行 = 后复权收盘 (同可成交列)\n"
             "• 显示: 百分比一位, 底色 = 组内名次 (高绿 → 低红; 成分行 = 成分之间)\n"
             "• 适用: 全部组 + 成分行; 随区间"},
    {"夏普", "夏普 = 可成交线区间夏普\n"
             "• 算法: 区间内日收益 mean / sd × √每年交易日\n"
             "• 数据: 篮子 = 标签指数; 因子 / 策略 = 有冲击净值; 成分行 = 后复权收盘\n"
             "• 显示: 两位小数, 底色 = 组内名次 (高绿 → 低红; 成分行 = 成分之间)\n"
             "• 适用: 全部组 + 成分行; 随区间"},
    {"年换手", "年换手 = 单边年换手\n"
               "• 算法: 自首次建仓起的日均增仓量 × 每年交易日 (倍/年)\n"
               "• 数据: 篮子 = 成分增仓 Σ (w − 漂移后上一成分日 w)⁺ (再平衡 + 成分进出); 因子 = G20 多头增仓量; 策略 = 各因子按当日份额加权 (不计因子间对冲)\n"
               "• 显示: 一位小数 + 倍, 底色 = 组内名次 (低绿 → 高红)\n"
               "• 适用: 全部组; 成分行 -; 不随区间"},
};
void recent_cells(const Recent &x, SheetCell *out) { out[kRecentChg] = signed_cell(x.chg), out[kRecentMa5] = signed_cell(x.ma5); }
// 段: 近况 (recent_row)
Seg recent_seg() {
  return {{std::begin(kRecentCols), std::end(kRecentCols)},
          [](const RowSrc &x, size_t, LinesSheet &, SheetCell *out) { recent_cells(recent_row(x), out); },
          {}};
}
// 段: NAV 数值列 (LinesView::num) 里位集 mask 的列; 底色 = 同组同列截面名次 (好绿 → 差红)
Seg num_seg(const LinesView &v, int mask) {
  Seg s;
  for (int k = 0; k < kNavNums; ++k)
    if (mask >> k & 1)
      s.cols.push_back(kNavNumCols[k]);
  s.cell = [&v, mask](const RowSrc &x, size_t, LinesSheet &, SheetCell *out) {
    for (int k = 0; k < kNavNums; ++k)
      if (mask >> k & 1)
        *out++ = ranked_cell(v.num[x.at.g][x.at.r][k], kNavNumFmt[k]);
  };
  return s;
}
// 各口估值分位段 [PoolVal]: 区间末各口的估值分位, 底色 = 同组同口内的截面名次 (便宜绿 → 贵红); 说明同一模板, 只换分子 / 负值口径
const std::array<SheetCol, kPoolVals> kValCols = [] {
  constexpr const char *num[kPoolVals] = {"归母净利TTM", "归母普通股权益MRQ", "营业总收入TTM", "经营现金流TTM", "近4季报告期税前分红"};
  constexpr const char *neg[kPoolVals] = {"亏损计负", "负权益计负", "无收入计 0", "负现金流计负", "不分红计 0"};
  std::array<SheetCol, kPoolVals> o;
  for (int b = 0; b < kPoolVals; ++b) {
    char s[1024];
    std::snprintf(s, sizeof(s),
                  "%s = %s 倒数 (%s / 总市值) 的估值分位\n"
                  "• 算法: 区间末日 %s 在过去 N 年 (类别条旁滑条) 窗内的加权分位; 收益率越低越贵 (高 = 贵, ≤ 0 排最贵端)\n"
                  "• 数据: 分子 = %s (%s); .加 = Σ分子 / Σ总市值, .等 = 成分 %s 的算术平均; 成分 / 财务取当日适用 (无前视)\n"
                  "• 显示: 分位百分比, 底色 = 组内同口名次 (便宜绿 → 贵红); 绝对值看主图 Band 图例\n"
                  "• 成分行: 该股本只 %s (%s / 该股总市值) 同式的分位, 底色 = 成分之间同口名次\n"
                  "• 适用: 指数 / 行业 / 概念 / 自选 + 成分行; 随区间末日, 估值史不满 1 年 → -",
                  kVal[b].name, kVal[b].inv, num[b], kVal[b].name, num[b], neg[b], kVal[b].name, kVal[b].name, num[b]);
    o[static_cast<size_t>(b)] = {kVal[b].name, s};
  }
  return o;
}();
// 段: 各口估值分位 (LinesView::pct)
Seg val_seg(const LinesView &v) {
  return {{kValCols.begin(), kValCols.end()},
          [&v](const RowSrc &x, size_t, LinesSheet &, SheetCell *out) {
            for (int b = 0; b < kPoolVals; ++b)
              out[b] = ranked_cell(v.pct[x.at.g][x.at.r][b], "%.0f%%");
          },
          {}};
}
// 资金流段: 日轴各日 (MM-DD) 净流入占当日成交额 | 档内合计净流入 (亿) | 合计占比; 按正负着色, 悬停格看净流入 / 成交额
// 列: 一日 (表头 = 该日 MM-DD, 拼在说明前) / 合计两列
const std::string kFlowDayTip = " = 该日资金流入占成交额\n"
                                "• 算法: 净流入 / 成交额; 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法; 成分行 = 本只): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟\n"
                                "• 数据: 当日 = 新浪快照合成 (实时); 历史日 = all.bar 库 L_1min (其后至基本面末日 = bar1m) 合成; 成分之和不加权;\n"
                                "  首根 09:31 (开盘集合竞价) / 末根 15:00 (收盘集合竞价) 按图左上角 开收盘 开关计入 (默认不计)\n"
                                "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额; 该日无分钟根 → -\n"
                                "• 适用: 指数 / 行业 / 概念 / 自选 + 成分行 (选中的 = 个股历史各日齐; 其余只当日, 历史日 -); 随档位 (日内 / 2日 … 5日)";
const SheetCol kFlowSumCols[2] = {
    {"净流入", "净流入 = 档内合计净流入\n"
               "• 算法: 日轴各日净流入之和 (各日同上列)\n"
               "• 数据: 同各日列\n"
               "• 显示: 亿 / 万亿带符号, 按正负着色; 悬停格看成交额\n"
               "• 适用: 指数 / 行业 / 概念 / 自选; 成分行同各日列 (未选中的只档位 = 日内); 随档位"},
    {"占比", "占比 = 档内合计净流入占合计成交额\n"
             "• 算法: Σ 各日净流入 / Σ 各日成交额 (左图占比的分母是近 5 日日均成交额, 两者不同)\n"
             "• 数据: 同各日列\n"
             "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额\n"
             "• 适用: 指数 / 行业 / 概念 / 自选; 成分行同各日列 (未选中的只档位 = 日内); 随档位"},
};
// 一段的净流入占成交额 (无根 → NaN)
double flow_pct(const Flow &x) { return x.amount > 0 ? x.net / x.amount : kNaN; }
// 段: 日轴各日净流入占比 + 档内合计两列 (Source::flow_of, 篮子 / 个股同一条); 各行流水记进 p.flow[行] (末项 = 合计; 悬停看金额)
Seg flow_seg(bool ends, std::vector<misc::Ymd> days) {
  const size_t nd = days.size();
  Seg s;
  s.key.assign(days.begin(), days.end());
  s.key.push_back(ends);
  for (const misc::Ymd day : days) {
    char t[16];
    std::snprintf(t, sizeof(t), "%02d-%02d", day / 100 % 100, day % 100);
    s.cols.push_back({t, t + kFlowDayTip});
  }
  s.cols.insert(s.cols.end(), std::begin(kFlowSumCols), std::end(kFlowSumCols));
  // 行 row 的各日资金流 x (日轴同序; whole = 各日齐全才有合计) → p.flow[row] + 格
  const auto fill = [nd](std::vector<Flow> x, bool whole, size_t row, LinesSheet &p, SheetCell *out) {
    assert(x.size() == nd);
    Flow sum;
    if (whole)
      for (const Flow &y : x)
        sum.net += y.net, sum.amount += y.amount;
    x.push_back(sum);
    for (size_t k = 0; k < nd; ++k)
      out[k] = signed_cell(flow_pct(x[k]));
    const double net = sum.amount > 0 ? sum.net / 1e8 : kNaN;
    out[nd] = sheet_cell(net, tint(net, 0.0), "%s", yi_text(net, true).c_str());
    out[nd + 1] = signed_cell(flow_pct(sum));
    p.flow[row] = std::move(x);
  };
  // 有历史 → 无根的日即无成交 (合计照算); 只当日 → 各日都有根才算合计; 无行情键的行 (自选里的账户行) 全空
  s.cell = [ends, days = std::move(days), fill](const RowSrc &x, size_t t, LinesSheet &p, SheetCell *out) {
    if (!x.row->ref.ok())
      return fill(std::vector<Flow>(days.size()), false, t, p, out);
    std::vector<Flow> fl;
    for (const misc::Ymd d : days)
      fl.push_back(x.src.flow_of(d, ends));
    const bool whole = x.src.se || std::ranges::all_of(fl, [](const Flow &y) { return y.amount > 0; });
    fill(std::move(fl), whole, t, p, out);
  };
  s.tip = [nd](const LinesSheet &p, size_t row, int k) {
    const Flow &x = p.flow[row][std::min(static_cast<size_t>(k), nd)]; // 合计两列同一段
    if (x.amount > 0)
      tip("净流入 %s / 成交额 %s = %+.2f%%", yi_text(x.net / 1e8, true).c_str(), yi_text(x.amount / 1e8).c_str(), 100.0 * x.net / x.amount);
  };
  return s;
}
// 段 SegDef → 实例; 资金流段的日轴 = 资金流叠加图的 (行情未起 = 空)
Seg seg_of(const SegDef &s, const LinesState &ui, const LinesView &v, const Quote::QuoteHub *mk) {
  switch (s.kind) {
  case kSegNone:
    break;
  case kSegRecent:
    return recent_seg();
  case kSegToday:
    return today_seg(s.arg);
  case kSegNum:
    return num_seg(v, s.arg);
  case kSegVal:
    return val_seg(v);
  case kSegFlow:
    return flow_seg(ui.overlay.flow_ends, mk ? OverlayDays(kOverlayFlow, *mk, ui.overlay) : std::vector<misc::Ymd>{});
  }
  assert(false && "未知段种类");
  return {};
}

// ---- 类别条旁的页件 (PageTool): 日内栏开关 / Band 滚动窗滑条 ----
void page_tool(PageTool t, LinesState &ui) {
  if (t == kToolNone)
    return;
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  switch (t) {
  case kToolNone:
    break;
  case kToolIntraday:
    if (toggle_button("日内", ui.intraday))
      ui.intraday = !ui.intraday;
    if (ImGui::IsItemHovered())
      tip("日内栏开关: 主图上方 (日内 : 主图 = 1 : 2, 上下分) 叠加选中的指数 / 行业 / 概念 / 成分行的日内收盘线 (同主图线色); 因子 / 策略无日内\n"
          "• y = 对首日前收的涨跌幅, 横网格的涨跌幅小字标在右内壁; 悬停看十字光标处时刻 / 涨跌幅与该分钟各条读数\n"
          "• 只选一行: 另带现价十字 (虚线, 标涨跌幅 / 时刻)\n"
          "• 篮子 = Market 篮子历史: all.bar 库合成历史, 当日由新浪快照合成; 档位 日内 / 2日 … 5日 叠在图左上角");
    return;
  case kToolBandYears:
    ImGui::SetNextItemWidth(ImGui::CalcTextSize("Band 滚动窗 00 年").x + 2.0f * ImGui::GetStyle().FramePadding.x);
    ImGui::SliderInt("##pf_band_years", &ui.band_years, 1, kBandYearsMax, "Band 滚动窗 %d 年", ImGuiSliderFlags_AlwaysClamp);
    if (ImGui::IsItemHovered())
      tip("Band 分位的滚动窗长 N: 每日取过去 N 年 (含当日) 的收益率加权分位, 拖动即时重算\n"
          "• 权重 = 1 - exp(-10 u), u = 1 - 距今 / 窗跨度: 近 ~3/4 窗约等权, 窗尾段快速降到 0 (老点出窗时分位不跳)\n"
          "• 估值史不足 N 年时先扩张窗 (权重按实际史长缩放); 不满 1 年不画");
    return;
  }
  assert(false && "未知页件");
}

} // namespace

void RenderLinesSheet(LinesState &ui, int page, const Quote::QuoteHub *mk, const StockInfos &infos) {
  const PageDef &pd = kPageDefs[page];
  button_bar(kRowGroupName.data(), pd.groups, ui.cat[page]);
  page_tool(pd.tool, ui);
  ImGui::SameLine();
  ImGui::TextColored(kHead, pd.pick == kPickMany ? "(点行 = 画)" : "(单选)");
  if (ImGui::IsItemHovered())
    tip("%s", pd.hint);
  std::vector<Seg> segs;
  for (const SegDef &s : pd.segs)
    if (s.kind != kSegNone)
      segs.push_back(seg_of(s, ui, ui.view.val, mk));
  table(ui, mk, infos, ui.der.val, page, {"名", pd.name_tip}, segs);
}

} // namespace GUI::Market