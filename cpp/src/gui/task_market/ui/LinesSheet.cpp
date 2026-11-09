// Market 行情页右图例 (NAV / VAL / FLOW / CTA 同一骨架的右表) — 见 LinesCommon.hpp
#include "gui/task_market/ui/LinesCommon.hpp"

#include "gui/util/Chart.hpp" // tint / button_bar
#include "gui/util/Tip.hpp"
#include "imgui.h"
#include "imgui_internal.h"  // TableFindByID (右表内容宽)
#include "shared/Basket.hpp" // basket_line / cut_of_line

#include <algorithm>
#include <array>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <initializer_list>
#include <map>
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
// 右表格名次底色的 alpha
constexpr float kValCellA = 0.45f;

// ---- 自选 (星列 / 名格右键; ui.watch = 文件, 改了即存) ----
// 星 (Nerd Font: 实 = 已自选 / 空 = 未自选) + 实星色
constexpr const char *kStarOn = "\xef\x80\x85", *kStarOff = "\xef\x80\x86";
constexpr ImVec4 kStarCol(1.0f, 0.8f, 0.2f, 1.0f);
constexpr const char *kStarTip = "星 = 自选\n"
                                 "• 点星 = 加入 / 移出自选 (名格右键同); 实星 = 已在自选\n"
                                 "• 自选 = 类别条 自选 组: 全局一份, 各页同一份, 加入序\n"
                                 "• 适用: 篮子行 + 成分行; 因子 / 策略无";
bool watched(const LinesUIState &ui, const std::string &w) { return std::ranges::find(ui.watch, w) != ui.watch.end(); }
// 加入 (排末尾) ↔ 移出
void watch_flip(LinesUIState &ui, const std::string &w) {
  assert(!w.empty());
  if (const auto it = std::ranges::find(ui.watch, w); it != ui.watch.end())
    ui.watch.erase(it);
  else
    ui.watch.push_back(w);
  SaveWatch(ui.watch_path, ui.watch);
}
// 星格: 先于行的 Selectable / TreeNode (跨全列) 交, 悬停归它
void star_cell(LinesUIState &ui, const std::string &w) {
  const bool on = watched(ui, w);
  ImGui::PushStyleColor(ImGuiCol_Button, ImVec4(0, 0, 0, 0));
  ImGui::PushStyleColor(ImGuiCol_Text, on ? kStarCol : ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
  if (ImGui::SmallButton(on ? kStarOn : kStarOff))
    watch_flip(ui, w);
  ImGui::PopStyleColor(2);
}

// 表格数值格 (NumCell): 填格时量文本宽, 画时右对齐到该列最宽
NumCell num_cell(const ImVec4 &col, const char *fmt, ...) {
  NumCell c;
  va_list ap;
  va_start(ap, fmt);
  std::vsnprintf(c.s, sizeof(c.s), fmt, ap);
  va_end(ap);
  c.col = col, c.w = ImGui::CalcTextSize(c.s).x;
  return c;
}
NumCell num_none() { return num_cell(ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled), "-"); }
void draw_cell(const NumCell &c, float col_w) {
  ImGui::SetCursorPosX(ImGui::GetCursorPosX() + col_w - c.w);
  ImGui::TextColored(c.col, "%s", c.s);
}

// 页 page 点行: 选中 ↔ 取消 (单选 = 换掉原选中); 多选里个股满 StockHistory::kKeep 只不再加, 刚选中 → 其线在主图例里回默认显隐 (理想列显, 可成交列隐)
void pick(LinesUIState &ui, int page, const LinesRow &r) {
  const int m = kPageDefs[page].pick;
  std::map<std::string, LinesQuote> &s = ui.pick.rows[m];
  if (s.erase(r.id))
    return;
  if (m == kPickOne)
    s.clear();
  else if (r.q.is(Market::kStock) && ui.pick.stocks(m).size() >= Market::StockHistory::kKeep)
    return;
  s.emplace(r.id, r.q);
  if (m == kPickMany)
    for (int l = 0; l < kPairLines; ++l)
      ui.reveal.insert(line_id(r, l));
}

// 净值线的近况 (Market::RecentOf 同一核): 日末视图自末个活跃日往前 (起点 = 首日前一日的日末), 前收 = 前一日末
Market::Recent recent_of(const Line &c) {
  Market::Mark m[Market::kMarks]; // 新 → 旧
  size_t n = 0;
  for (int k = c.e.empty() ? c.f - 1 : c.l + 1; k >= c.f && n < std::size(m); --k)
    m[n++] = {c.end(k), k > c.f ? c.end(k - 1) : kNaN};
  return Market::RecentOf({m, n});
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
    const double x = num(a, c), y = num(b, c);
    if (!std::isfinite(x) || !std::isfinite(y))
      return std::isfinite(x) && !std::isfinite(y);
    return desc ? x > y : x < y;
  });
}
// 名格悬停 (星列之后; 行 Selectable 跨全列, 只认本格) → 提示
void name_tip(const std::string &s) {
  if (ImGui::IsItemHovered() && ImGui::TableGetHoveredColumn() == 1)
    tip("%s", s.c_str());
}

// ---- 右表 (各页同一骨架 table): 列 = 星 + 名 + 各段 (Seg) 的列 (SheetCol), 格 = 文本 + 排序键 + 底色名次 (SheetCell);
//      格缓存 (sheet_of: 来源变才整组重填) → 列出 (listing: 篮子组按过滤条, 选中 / 展开行恒列) → 量列出行各列最宽 → 表头排序 (组内) → 画 (sheet) ----
// 一列: 表头 / 悬停说明 (各列同一格式: "列名 = 全称" + 算法 / 数据 / 显示 / 适用 四行)
struct SheetCol {
  std::string name, tip;
};
// 值 y 的格: 字色 col, 按 fmt 显示 a; 不可算 → "-"; 排序键 = y
template <class... A>
SheetCell sheet_cell(double y, const ImVec4 &col, const char *fmt, A... a) {
  return {std::isfinite(y) ? num_cell(col, fmt, a...) : num_none(), y};
}
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
// 其下先列成分行 (缩进; 同列排序), 再列其余组内行; 字色 = 线色; 有自选键的行: 星格点 / 名格右键 = 加入 / 移出自选; 只画可见的行
template <class On, class Click, class Hover>
void sheet(const char *id, const std::vector<SheetCol> &cols, LinesUIState &ui, const LinesDerived &d, int g, std::vector<size_t> order,
           const LinesSheet &ps, const On &on, const Click &click, const Hover &hover) {
  const std::vector<LinesRow> &rs = d.rows[g];
  const size_t nc = cols.size() - 1, R = rs.size(); // 名之后的列 / 组内行
  assert(ps.nc == nc && ps.R == R && ps.cells.size() == ps.at.size() * nc);
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
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers); // 同 TableHeadersRow, 另加悬停说明
  for (int c = 0; c <= static_cast<int>(cols.size()); ++c) {
    if (!ImGui::TableSetColumnIndex(c))
      continue;
    ImGui::PushID(c);
    ImGui::TableHeader(c == 0 ? "##star" : cols[static_cast<size_t>(c - 1)].name.c_str());
    ImGui::PopID();
    if (ImGui::IsItemHovered())
      tip("%s", c == 0 ? kStarTip : cols[static_cast<size_t>(c - 1)].tip.c_str());
  }
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
  constexpr ImGuiTreeNodeFlags kNode = ImGuiTreeNodeFlags_SpanAllColumns | ImGuiTreeNodeFlags_NoTreePushOnOpen;
  const auto row = [&](size_t t) {
    ImGui::TableNextRow();
    ImGui::PushID(static_cast<int>(t)); // 自选组里同一只可既是自选行又是展开行的成分行
    const LinesRow &h = row_of(t);
    ImGui::TableSetColumnIndex(0);
    if (!h.watch.empty())
      star_cell(ui, h.watch);
    ImGui::TableSetColumnIndex(1);
    const std::string label = h.cn + "##" + h.id;
    ImGui::PushStyleColor(ImGuiCol_Text, row_color(d, ps.at[t], kTrade));
    if (t >= R) {
      ImGui::Indent();
      ImGui::TreeNodeEx(label.c_str(), kNode | ImGuiTreeNodeFlags_Leaf | (on(t) ? ImGuiTreeNodeFlags_Selected : 0));
      ImGui::Unindent();
      if (ImGui::IsItemClicked())
        click(t);
    } else if (h.q.is(Market::kBasket)) {
      const bool open = t == pin;
      ImGui::SetNextItemOpen(open, ImGuiCond_Always);
      ImGui::TreeNodeEx(label.c_str(), kNode | ImGuiTreeNodeFlags_OpenOnArrow | (on(t) ? ImGuiTreeNodeFlags_Selected : 0));
      if (ImGui::IsItemToggledOpen()) {
        ui.pick.open = open ? std::string{} : h.id;
        ImGui::SetScrollY(0.f);
      } else if (ImGui::IsItemClicked()) {
        click(t);
      }
    } else if (ImGui::Selectable(label.c_str(), on(t), ImGuiSelectableFlags_SpanAllColumns)) {
      click(t);
    }
    ImGui::PopStyleColor();
    const int hov = ImGui::IsItemHovered() ? ImGui::TableGetHoveredColumn() - 2 : -1; // 行跨全列: 悬停的数值列
    name_tip(h.hover);
    if (!h.watch.empty() && ImGui::BeginPopupContextItem("##watch")) {
      if (ImGui::MenuItem(watched(ui, h.watch) ? "移出自选" : "加入自选"))
        watch_flip(ui, h.watch);
      ImGui::EndPopup();
    }
    for (size_t k = 0; k < nc; ++k) {
      ImGui::TableSetColumnIndex(static_cast<int>(2 + k));
      const SheetCell &c = at(t)[k];
      if (std::isfinite(c.rank))
        ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::ColorConvertFloat4ToU32(band_col(c.rank, kValCellA)));
      draw_cell(c.text, cw[k]);
    }
    if (hov >= 0)
      hover(t, hov);
    ImGui::PopID();
  };
  if (pin != SIZE_MAX) // 冻结行须在 clipper 之前交
    row(pin);
  ImGuiListClipper clip;
  clip.Begin(static_cast<int>(lines.size()));
  while (clip.Step())
    for (int i = clip.DisplayStart; i < clip.DisplayEnd; ++i)
      row(lines[static_cast<size_t>(i)]);
  ImGui::EndTable();
}

// ---- 当日四列 (TodayCol; NAV / FLOW 右表全四列, VAL 只数 / 总市值; 不随区间): 只数 / 总市值 / 换手不着色, 流入按正负着色; 换手 / 流入格悬停看分子分母金额 ----
const SheetCol kTodayCols[kTodays] = {
    {"只数", "只数 = 成分只数\n"
             "• 算法: 成员表的成分计数 (.等 / .加 同一份)\n"
             "• 数据: 成员表 = 历史末日收盘定 (= 当日 bind 给新浪行情的那份)\n"
             "• 显示: 整数, 不着色\n"
             "• 适用: 指数 / 行业 / 概念 / 自选; 成分行 -; 不随区间"},
    {"总市值", "总市值 = 成分总市值之和\n"
               "• 算法: Σ 成分 末日收盘 × 总股本; 当日接上再 × 加权线当日涨跌 (今收 / 前收); 成分行 = 本只 (基本面末日总市值, 当日接上再 × 今收 / 前收)\n"
               "• 数据: 成员表同只数; 总股本取末日适用\n"
               "• 显示: 亿 / 万亿, 不着色\n"
               "• 适用: 指数 / 行业 / 概念 / 自选 + 成分行; 不随区间"},
    {"换手", "换手 = 当日换手率\n"
             "• 算法: 成交额 / 总市值; 成交额 = Σ 成分 当日成交额 (成分行 = 本只), 总市值同上列 (总股本口径, 非流通)\n"
             "• 数据: 新浪行情日 (盘后 / 重启照样全天); 无新浪行情日 / 当日无成交 → -\n"
             "• 显示: 百分比两位, 不着色; 悬停格看成交额 / 总市值\n"
             "• 适用: 指数 / 行业 / 概念 / 自选 + 成分行; 不随区间"},
    {"流入", "流入 = 当日资金流入占成交额\n"
             "• 算法: 净流入 / 成交额; 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法; 成分行 = 本只): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟\n"
             "• 数据: 新浪行情日分钟根 (快照合成 + 分钟 K 补), 含开收盘集合竞价; 成分之和不加权, 分不出大单\n"
             "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额\n"
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
// 表行的源: 位置 / 行 + 行情键的源 (基本面档案: 个股才有, 无 → nullptr; 当日 Day: 篮子 = 篮子 Feed 的该线 / 个股 = 新浪当日,
// 未取 / 当日无成交 → nullptr)
struct RowSrc {
  LinesAt at;
  const LinesRow *row = nullptr;
  const StockInfo *info = nullptr;
  const Market::Day *day = nullptr;
};
RowSrc src_of(const LinesUIState &ui, const StockInfos &infos, const LinesDerived &d, const LinesAt &a) {
  RowSrc x{a, &d.row(a)};
  const LinesQuote &q = x.row->q;
  const Market::Day *t = nullptr;
  if (const Market::Feed &f = ui.basket.feed; q.is(Market::kBasket) && f.data && f.today) {
    assert(f.today->size() == f.data->list.size());
    t = &(*f.today)[q.key];
  } else if (q.is(Market::kStock)) {
    if (q.key < infos.size() && infos[q.key].known)
      x.info = &infos[q.key];
    const auto it = std::ranges::lower_bound(ui.cons.ids, q.key);
    if (ui.cons.day && it != ui.cons.ids.end() && *it == q.key)
      t = &(*ui.cons.day)[static_cast<size_t>(it - ui.cons.ids.begin())];
  }
  if (t && t->candle.date != 0)
    x.day = t;
  return x;
}
// 篮子行的当日 (SheetToday): 只数 / 总市值 = Tape::members / weight 的同切片等权线 / 加权线 (总市值当日接上 × 加权线当日涨跌);
// 成交额 / 流入 = 当日 Day (不看接续); 换手 = 成交额 / 总市值
SheetToday basket_today(const Market::Feed &f, const RowSrc &x) {
  SheetToday o;
  if (!f.data || f.data->members.empty())
    return o;
  const size_t c = cut_of_line(x.row->q.key), eq = basket_line(c, kWeightEq), cap = basket_line(c, kWeightCap);
  assert(f.data->keys[eq] == eq && f.data->keys[cap] == cap && f.today && f.today->size() == f.data->list.size());
  double w = f.data->weight[cap];
  if (const Market::Day &t = (*f.today)[cap]; Market::joins(*f.data->list[cap], t))
    w *= t.candle.close / t.candle.ref;
  o.v[kTodayMembers] = f.data->members[eq];
  o.v[kTodayCap] = w > 0 ? w : kNaN;
  if (x.day && x.day->candle.amount > 0)
    o.amount = x.day->candle.amount / 1e8, o.v[kTodayFlow] = x.day->flow / x.day->candle.amount;
  o.v[kTodayTurn] = o.amount / o.v[kTodayCap];
  return o;
}
// 个股行的当日 (SheetToday): 总市值 = 档案末日收盘总市值 (当日接上档案末日再 × 当日涨跌); 成交额 / 流入 = 当日 Day (本只分钟涨跌法);
// 换手 = 成交额 / 总市值; 无只数
SheetToday stock_today(const RowSrc &x) {
  SheetToday o;
  double cap = x.info ? x.info->mcap : kNaN;
  if (x.info && x.day && x.day->prev == x.info->update_date && x.day->candle.ref > 0)
    cap *= x.day->candle.close / x.day->candle.ref;
  o.v[kTodayCap] = cap > 0 ? cap : kNaN;
  if (x.day && x.day->candle.amount > 0)
    o.amount = x.day->candle.amount / 1e8, o.v[kTodayFlow] = x.day->flow / x.day->candle.amount;
  o.v[kTodayTurn] = o.amount / o.v[kTodayCap];
  return o;
}
// 表行的当日: 按行情键 (篮子 = 篮子 Feed 的同切片两线 + 当日; 个股 = 档案 + 当日; 无 = 全不可算)
SheetToday today_of(const Market::Feed &f, const RowSrc &x) {
  const LinesQuote &q = x.row->q;
  return q.is(Market::kBasket) ? basket_today(f, x) : q.is(Market::kStock) ? stock_today(x)
                                                                           : SheetToday{};
}
// 表行的近况 (Market::RecentOf 同一核): 篮子 = Market 篮子历史同一份; 个股 = ASSET 个股同一份 (无档案 → 不可算); 无行情键 = 可成交线日末
Market::Recent recent_row(const Market::Feed &f, const RowSrc &x) {
  const LinesQuote &q = x.row->q;
  if (q.is(Market::kBasket))
    return Market::RecentOf(f, q.key);
  if (q.is(Market::kStock))
    return !x.info ? Market::Recent{} : x.day ? Market::RecentOf(*x.info, x.day->candle, x.day->prev)
                                              : Market::RecentOf(*x.info, Market::Candle{}, 0);
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
  const bool on = lo > 0 || h > 0;
  if (on)
    ImGui::PushStyleColor(ImGuiCol_Button, ImGui::GetStyleColorVec4(ImGuiCol_ButtonActive));
  if (ImGui::SmallButton((std::string(name) + " " + bound_text(lo, h, fmt) + "###" + id).c_str()))
    ImGui::OpenPopup(id);
  if (on)
    ImGui::PopStyleColor();
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
// 右表的一段列 (各页由段拼成, 同一段各页同一份): 列 + 表行的格 + 悬停格
struct Seg {
  std::vector<SheetCol> cols;
  std::function<void(const RowSrc &, size_t, LinesSheet &, SheetCell *)> cell; // 表行 (源, 下标) → 段内格 out[0, cols)
  std::function<void(const LinesSheet &, size_t, int)> tip;                    // 悬停表行的段内列 k; 空 = 无
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
// 页 page 组 g 的格 (ui.sheet[page]): 键 (days / ends 只 FLOW 给) 变 → 全部表行重填: 表行 = 组内行 + 展开行在本组时其下的成分行
// (成分组前 n_cons 行); 逐表行: 源 → 当日 + 各段依次填格
const LinesSheet &sheet_of(LinesUIState &ui, const StockInfos &infos, const LinesDerived &d, int page, int g, const std::vector<Seg> &segs,
                           std::vector<misc::Ymd> days, bool ends) {
  const Market::Feed &f = ui.basket.feed;
  const LinesSheetKey k{g, ui.view.gen, f.data, f.today, ui.cons.day, infos.size(), std::move(days), ends, ImGui::GetFontSize(),
                        ImGui::GetColorU32(ImGuiCol_Text)};
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
      const RowSrc x = src_of(ui, infos, d, ps.at[t]);
      ps.today.push_back(today_of(f, x));
      SheetCell *out = ps.cells.data() + t * ps.nc;
      for (const Seg &s : segs)
        s.cell(x, t, ps, out), out += s.cols.size();
    }
  });
  return ui.sheet[page].val;
}
// 自选组表上一行: 项数 + 找不到行的自选键 (lost; 点 = 一并移出)
void watch_bar(LinesUIState &ui, size_t n, const std::vector<std::string> &lost) {
  ImGui::TextDisabled("%zu 项", n);
  if (ImGui::IsItemHovered())
    tip("自选 = 星列 / 名格右键加入的篮子行与成分行, 加入序\n"
        "• 全局一份 (%s), 与组合无关, 各页同一份\n"
        "• 与原行同一份选中 / 展开 (篮子行照样 ▸ 展开); 数值列底色 = 自选之间名次",
        ui.watch_path.c_str());
  if (lost.empty())
    return;
  ImGui::SameLine();
  std::string keys;
  for (const std::string &w : lost)
    keys += "\n• " + w;
  if (ImGui::SmallButton(("移出失效 " + std::to_string(lost.size())).c_str())) {
    std::erase_if(ui.watch, [&](const std::string &w) { return std::ranges::find(lost, w) != lost.end(); });
    SaveWatch(ui.watch_path, ui.watch);
  }
  if (ImGui::IsItemHovered())
    tip("找不到行的自选键 (概念换快照没了 / 代码不在资产轴), 点 = 一并移出:%s", keys.c_str());
}
// 组 g 右表列出的行: 有过滤条的组 (kGroupDefs::filter) = 过过滤条的 ∪ 选中的 (on) ∪ 展开的 (表上画过滤条), 其余组全列 (自选组表上画项数)
template <class On>
std::vector<size_t> listing(LinesUIState &ui, const LinesSheet &ps, const LinesDerived &d, int g, const On &on) {
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
void table(LinesUIState &ui, const StockInfos &infos, const LinesDerived &d, int page, const SheetCol &name, const std::vector<Seg> &segs,
           std::vector<misc::Ymd> days = {}, bool ends = false) {
  const int g = ui.cat[page];
  const LinesSheet &ps = sheet_of(ui, infos, d, page, g, segs, std::move(days), ends);
  const auto on = [&](size_t t) { return picked(ui, page, d.row(ps.at[t])); };
  const std::vector<size_t> order = listing(ui, ps, d, g, on);
  ImGui::PushID(static_cast<int>(ps.nc)); // 列数可变 (FLOW 随档位)
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
          [n](const RowSrc &, size_t t, LinesSheet &p, SheetCell *out) {
            SheetCell c[kTodays];
            today_cells(p.today[t], c);
            std::copy_n(c, n, out);
          },
          [](const LinesSheet &p, size_t t, int k) { today_tip(p.today[t], k); }};
}

// ---- NAV 右图例: 类别条 (指数 / 行业 / 概念 / 自选 / 因子 / 策略, 一次看一组) + 日内开关; 表 = 该组一行一对线, 点行 = 选中 / 取消 (选中 = 主图画该行全部线, 单条在主图例里再点掉;
//      选中跨组保留; 选中的篮子行另叠进日内栏). 列 = 中文名 (线色; 悬停看 hover) | 近况 (当日涨幅 | Δ5日线) | 当日四列
//      | 可成交 | 理想 (区间年化) | 夏普 (可成交列) | 年换手; 近况按正负着色, 末段底色 = 同组同列截面名次 (好绿 → 差红); 表头点排序 (组内) ----
// 近况段 (不随区间; Market::Recent)
enum RecentCol : int { kRecentChg,
                       kRecentMa5 };
constexpr int kRecents = 2;
const SheetCol kRecentCols[kRecents] = {
    {"涨幅", "涨幅 = 当日涨跌幅\n"
             "• 算法: 最新一日收盘 / 前收 − 1\n"
             "• 数据: 篮子 = Market 篮子历史 (当日接上 = 新浪快照合成, 否则 all.bar 末日); 成分行 = ASSET 个股同一份 (新浪当日接上 bar1d); 因子 / 策略 = 净值末点\n"
             "• 显示: 百分比两位, 按正负着色\n"
             "• 适用: 全部组 + 成分行; 不随区间"},
    {"Δ5日线", "Δ5日线 = 对 5 日均线的偏离\n"
               "• 算法: 最新一日收盘 / MA5 (含当日) − 1\n"
               "• 数据: 篮子 = Market 篮子历史 (当日接上 = 新浪快照合成, 否则 all.bar 末日); 成分行 = ASSET 个股同一份; 因子 / 策略 = 净值末 5 点\n"
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
const SheetCol kNavName = {"名", "名 = 行名\n"
                                 "• 算法: 篮子 = 切片名 + .等 (等权) / .加 (总市值加权); 因子 / 策略 = 中文名; 成分行 = 简称\n"
                                 "• 数据: 切片 = 全A / 池 / 申万一级行业 / 东财题材概念\n"
                                 "• 显示: 字色 = 主图线色 (四组一起按 max(成员重合, 剔全A.加的残差相关) 定色相: 色差 ∝ 不相似; 没算完全灰); 点本列表头 = 按颜色排 (相似的挨着; 没算完按名); 超长截断补 …, 悬停看全名\n"
                                 "• 成分行: 篮子名前 ▸ 展开 (至多一行), 该行钉在表头下, 其下缩进列出成分 (默认权降序, 点表头在本篮子内排, 名列按简称);\n"
                                 "  与组内行同一套: 点 = 选中 (主图画线 + 日内叠加), 字色 = 线色\n"
                                 "• 适用: 全部组; 成分行只指数 / 行业 / 概念 / 自选 (篮子行展开)"};
void recent_cells(const Market::Recent &x, SheetCell *out) { out[kRecentChg] = signed_cell(x.chg), out[kRecentMa5] = signed_cell(x.ma5); }
// 段: 近况 (recent_row)
Seg recent_seg(const Market::Feed &f) {
  return {{std::begin(kRecentCols), std::end(kRecentCols)},
          [&f](const RowSrc &x, size_t, LinesSheet &, SheetCell *out) { recent_cells(recent_row(f, x), out); },
          {}};
}
// 段: NAV 数值列 (LinesView::num) 里的 ks 列 (NAV / VAL = 全部, CTA = 理想 / 夏普)
Seg num_seg(const LinesView &v, std::vector<int> ks = {kNumTrade, kNumIdeal, kNumSharpe, kNumTurn}) {
  Seg s;
  for (const int k : ks)
    s.cols.push_back(kNavNumCols[k]);
  s.cell = [&v, ks](const RowSrc &x, size_t, LinesSheet &, SheetCell *out) {
    for (const int k : ks)
      *out++ = ranked_cell(v.num[x.at.g][x.at.r][k], kNavNumFmt[k]);
  };
  return s;
}
void legend_nav(LinesUIState &ui, const LinesDerived &d, const LinesView &v, const StockInfos &infos) {
  button_bar(kRowGroupName.data(), kPageDefs[kPageNav].groups, ui.cat[kPageNav]);
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  if (ui.intraday)
    ImGui::PushStyleColor(ImGuiCol_Button, ImGui::GetStyleColorVec4(ImGuiCol_ButtonActive));
  const bool flip = ImGui::SmallButton("日内");
  if (ui.intraday)
    ImGui::PopStyleColor();
  if (flip)
    ui.intraday = !ui.intraday;
  if (ImGui::IsItemHovered())
    tip("日内栏开关: 主图上方 (日内 : 主图 = 1 : 2, 上下分) 叠加选中的指数 / 行业 / 概念 / 成分行的日内收盘线 (同主图线色); 因子 / 策略无日内\n"
        "• y = 对首日前收的涨跌幅, 横网格的涨跌幅小字标在右内壁; 悬停看十字光标处时刻 / 涨跌幅与该分钟各条读数\n"
        "• 只选一行: 改画分钟蜡烛 (窄了自动并根) + 现价十字 (虚线, 标涨跌幅 / 时刻)\n"
        "• 篮子 = Market 篮子历史: all.bar 库合成历史, 当日由新浪快照合成; 档位 日内 / 2日 … 5日 叠在图左上角");
  ImGui::SameLine();
  ImGui::TextColored(kHead, "(点行 = 画)");
  if (ImGui::IsItemHovered())
    tip("点行 = 选中 / 取消, 选中 = 主图画该行两条线, 主图例默认只显理想列, 可成交列点图例显 (显隐变了自动重新 fit); 默认不选; 选中换组保留\n"
        "• 指数 = 各池等权指数 (all 池拆 全A.等 / 全A.加); 行业 = 申万一级 (全A 内切, 总市值加权); 概念 = 东财题材概念 (全A 内切, 总市值加权):\n"
        "    ◦ 可成交 = 标签指数 (收盘封板不建仓 / 平仓顺延, 与因子回测同口径)\n"
        "    ◦ 理想 = 价格指数 (Market 篮子历史同一份: 成员 / 权重取前一日收盘, 点位按前收合成, 不看可成交, 同中证 / 万得口径); 两者之差 = 可成交拖累\n"
        "• 因子 = 单因子各自的 G20 多头净值: 可成交列 = 有冲击 (它自己的冲击段: Lob 最小金额段 / Bar 固定冲击), 理想列 = 无冲击\n"
        "• 策略 = 本组合 G20 多头: 可成交列 = 有冲击, 理想列 = 无冲击\n"
        "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
        "• 自选 = 首列星 (或名格右键) 加入的篮子行 / 成分行: 全局一份, 各页同一份, 与原行同一份选中; 名次在自选之间\n"
        "• 指数 / 行业 / 概念表上过滤条 (各页同一份): 按只数 / 总市值 / |资金流入| 筛本组的行, 已画 / 展开的行恒列\n"
        "• 指数 / 行业 / 概念行名前 ▸ = 展开成分股 (至多一行, 各页同一份): 该行钉在表头下, 成分接在其下 (列同篮子行, 不能算的 -);\n"
        "  成分行 = 后复权收盘 (两条同一条), 与组内行同一套: 点 = 选中 (主图 + 日内栏 / FLOW 图; 至多 64 只), 收起后选中的照画\n"
        "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)");
  table(ui, infos, d, kPageNav, kNavName, {recent_seg(ui.basket.feed), today_seg(), num_seg(v)});
}

// ---- VAL 右图例: 滚动窗长 + 类别条 (指数 / 行业 / 概念 / 自选, 一次看一组) + 篮子单选表格; 列 = 中文名 (悬停看名) | 只数 | 总市值 | 可成交 | 理想 | 夏普 | 年换手 | 各口;
//      各口格 = 区间末各口的估值分位, 底色 = 同组同口内的截面名次 (便宜绿 → 贵红); 表头点排序 (组内) ----
// 各口 [PoolVal]: 同一模板, 只换分子 / 负值口径
const std::array<SheetCol, kPoolVals> kValCols = [] {
  constexpr const char *num[kPoolVals] = {"归母净利TTM", "归母权益MRQ", "营业总收入TTM", "经营现金流TTM", "近4季报告期税前分红"};
  constexpr const char *neg[kPoolVals] = {"亏损计负", "负权益计负", "无收入计 0", "负现金流计负", "不分红计 0"};
  std::array<SheetCol, kPoolVals> o;
  for (int b = 0; b < kPoolVals; ++b) {
    char s[1024];
    std::snprintf(s, sizeof(s),
                  "%s = %s 倒数 (%s / 总市值) 的估值分位\n"
                  "• 算法: 区间末日 %s 在过去 N 年 (上方滑条) 窗内的加权分位; 收益率越低越贵 (高 = 贵, ≤ 0 排最贵端)\n"
                  "• 数据: 分子 = %s (%s); .加 = Σ分子 / Σ总市值, .等 = 成分 %s 的算术平均; 成分 / 财务取当日适用 (无前视)\n"
                  "• 显示: 分位百分比, 底色 = 组内同口名次 (便宜绿 → 贵红); 绝对值看主图 Band 图例\n"
                  "• 成分行: 该股本只 %s (%s / 该股总市值) 同式的分位, 底色 = 成分之间同口名次\n"
                  "• 适用: 指数 / 行业 / 概念 / 自选 + 成分行; 随区间末日, 估值史不满 1 年 → -",
                  kVal[b].name, kVal[b].inv, num[b], kVal[b].name, num[b], neg[b], kVal[b].name, kVal[b].name, num[b]);
    o[static_cast<size_t>(b)] = {kVal[b].name, s};
  }
  return o;
}();
const SheetCol kValName = {"名", "名 = 篮子名\n"
                                 "• 算法: 切片名 + .等 (等权) / .加 (总市值加权)\n"
                                 "• 数据: 切片 = 全A / 池 / 申万一级行业 / 东财题材概念\n"
                                 "• 显示: 超长截断补 …, 悬停看全名; 选中行高亮 (主图画它)\n"
                                 "• 成分行: 篮子名前 ▸ 展开, 缩进列出成分 (同 NAV 页); 与篮子行同一个单选\n"
                                 "• 适用: 指数 / 行业 / 概念 / 自选"};
// 段: 各口估值分位 (LinesView::pct)
Seg val_seg(const LinesView &v) {
  return {{kValCols.begin(), kValCols.end()},
          [&v](const RowSrc &x, size_t, LinesSheet &, SheetCell *out) {
            for (int b = 0; b < kPoolVals; ++b)
              out[b] = ranked_cell(v.pct[x.at.g][x.at.r][b], "%.0f%%");
          },
          {}};
}
void legend_val(LinesUIState &ui, const LinesDerived &d, const LinesView &v, const StockInfos &infos) {
  ImGui::SetNextItemWidth(-FLT_MIN);
  ImGui::SliderInt("##pf_band_years", &ui.band_years, 1, kBandYearsMax, "Band 滚动窗 %d 年", ImGuiSliderFlags_AlwaysClamp);
  if (ImGui::IsItemHovered())
    tip("Band 分位的滚动窗长 N: 每日取过去 N 年 (含当日) 的收益率加权分位, 拖动即时重算\n"
        "• 权重 = 1 - exp(-10 u), u = 1 - 距今 / 窗跨度: 近 ~3/4 窗约等权, 窗尾段快速降到 0 (老点出窗时分位不跳)\n"
        "• 估值史不足 N 年时先扩张窗 (权重按实际史长缩放); 不满 1 年不画");
  button_bar(kRowGroupName.data(), kPageDefs[kPageVal].groups, ui.cat[kPageVal]);
  ImGui::SameLine();
  ImGui::TextColored(kHead, "(单选)");
  if (ImGui::IsItemHovered())
    tip("类别条选一组 (指数 = 各池, 行业 = 申万一级, 概念 = 东财题材概念), 点行 / 成分行选一个: 主图画它的价格 + EP / BP / SP / CP / DY Band; 选中换组保留\n"
        "• 价格: 全A.加 / 行业 / 概念 = 总市值加权指数 (全A 内切); 全A.等 / 其余池 = 等权指数 (同 NAV 池线); 成分股 = 后复权收盘\n"
        "• 行业成员 = 全A 成分按当日适用 (盘前已知) 的 SW2021 一级行业 (ind_l1) 切; 行业未知的只进全A\n"
        "• 概念成员 = 全A 成分 ∩ 最近一份东财概念快照的成分 (全史同一份)\n"
        "• 估值与价格同权重: 个股收益率 = 分子 / 总市值; 加权 = 整体法 Σ分子 / Σ总市值, 等权 = 成分个股收益率的算术平均 (= 等权组合每元的分子); 成分股 = 本只\n"
        "  分子: EP = 归母净利TTM, BP = 归母权益MRQ, SP = 营业总收入TTM, CP = 经营现金流TTM, DY = 近4季报告期税前分红 (亏损 / 负权益 / 负现金流照计为负)\n"
        "• 成分 = 当日持仓 (同池指数: 上一交易日收盘建仓的池); 市值 = 收盘价 × 当日适用总股本, 财务取当日适用 (无前视)\n"
        "• 估值分位 = 当日收益率在过去 N 年 (含当日; 上方滑条) 窗内的加权分位 (窗尾降权, 见滑条提示), 收益率越低越贵 (高 = 贵; ≤ 0 排在最贵端), 逐日滚动; 估值史不满 1 年不画\n"
        "• Band = 价格 × 当日收益率 / q, q = 窗内 5/10/25/50/75/90/95%% 贵度分位的收益率 (与倍数口径的 Band 同一条线);\n"
        "  当日 ≤ 0 或窗内收益率 > 0 的日子不足 80%% 整段不画, 某档 q ≤ 0 该档线断开 (价格无解); 分位照常. 最便宜档以下绿 → 最贵档以上红\n"
        "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
        "• 表上过滤条 (各页同一份): 按只数 / 总市值 / |资金流入| 筛本组的行, 选中 / 展开行恒列\n"
        "• 行名前 ▸ = 展开成分股 (至多一行, 各页同一份; 该行钉在表头下): 各口 = 该股本只的估值分位 (底色 = 成分之间名次); 点成分行 = 选该股\n"
        "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)");
  table(ui, infos, d, kPageVal, kValName, {today_seg(kTodayTurn), num_seg(v), val_seg(v)});
}

// ---- FLOW 右图例: 类别条 (指数 / 行业 / 概念 / 自选) + 过滤条; 表 = 该组一行一篮子, 点行 = 选中 / 取消 (与 NAV 同一份选中, 左图画选中的).
//      列 = 中文名 (线色) | 当日四列 | 日轴各日 (MM-DD) 净流入占当日成交额 | 档内合计净流入 (亿) | 合计占比; 按正负着色, 悬停格看净流入 / 成交额; 表头点排序 (组内) ----
// 列: 名 / 一日 (表头 = 该日 MM-DD, 拼在说明前) / 合计两列
const SheetCol kFlowNameCol = {"名", "名 = 篮子名\n"
                                     "• 算法: 切片名 + .等 (等权) / .加 (总市值加权); 资金流不分权重, 同切片两行同成分即同值\n"
                                     "• 数据: 切片 = 全A / 池 / 申万一级行业 / 东财题材概念\n"
                                     "• 显示: 字色 = 线色 (同 NAV); 超长截断补 …, 悬停看全名\n"
                                     "• 成分行: 篮子名前 ▸ 展开, 缩进列出成分 (同 NAV 页); 与篮子行同一份选中\n"
                                     "• 适用: 指数 / 行业 / 概念 / 自选"};
const std::string kFlowDayTip = " = 该日资金流入占成交额\n"
                                "• 算法: 净流入 / 成交额; 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法; 成分行 = 本只): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟\n"
                                "• 数据: 当日 = 新浪快照合成 (实时); 历史日 = all.bar 库 L_1min (其后至基本面末日 = bar1m) 合成; 成分之和不加权;\n"
                                "  首根 09:31 (开盘集合竞价) / 末根 15:00 (收盘集合竞价) 按图左上角 开收盘 开关计入 (默认不计)\n"
                                "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额; 该日无分钟根 → -\n"
                                "• 适用: 指数 / 行业 / 概念 / 自选; 成分行只当日 (历史日 -, 看左图叠加); 随档位 (日内 / 2日 … 5日)";
const SheetCol kFlowSumCols[2] = {
    {"净流入", "净流入 = 档内合计净流入\n"
               "• 算法: 日轴各日净流入之和 (各日同上列)\n"
               "• 数据: 同各日列\n"
               "• 显示: 亿 / 万亿带符号, 按正负着色; 悬停格看成交额\n"
               "• 适用: 指数 / 行业 / 概念 / 自选; 成分行只档位 = 日内; 随档位"},
    {"占比", "占比 = 档内合计净流入占合计成交额\n"
             "• 算法: Σ 各日净流入 / Σ 各日成交额 (左图占比的分母是近 5 日日均成交额, 两者不同)\n"
             "• 数据: 同各日列\n"
             "• 显示: 百分比两位, 按正负着色; 悬停格看净流入 / 成交额\n"
             "• 适用: 指数 / 行业 / 概念 / 自选; 成分行只档位 = 日内; 随档位"},
};
// 一段的净流入占成交额 (无根 → NaN)
double flow_pct(const Market::Flow &x) { return x.amount > 0 ? x.net / x.amount : kNaN; }
// 段: 日轴各日净流入占比 + 档内合计两列 (按行情键: 篮子 = Market 篮子历史同一份; 个股只当日); 各行流水记进 p.flow[行] (末项 = 合计; 悬停看金额)
Seg flow_seg(const Market::BasketState &bs, const std::vector<misc::Ymd> &days) {
  const size_t nd = days.size();
  Seg s;
  for (const misc::Ymd day : days) {
    char t[16];
    std::snprintf(t, sizeof(t), "%02d-%02d", day / 100 % 100, day % 100);
    s.cols.push_back({t, t + kFlowDayTip});
  }
  s.cols.insert(s.cols.end(), std::begin(kFlowSumCols), std::end(kFlowSumCols));
  // 行 row 的各日资金流 x (日轴同序; whole = 各日齐全才有合计) → p.flow[row] + 格
  const auto fill = [nd](std::vector<Market::Flow> x, bool whole, size_t row, LinesSheet &p, SheetCell *out) {
    x.resize(nd);
    Market::Flow sum;
    if (whole)
      for (const Market::Flow &y : x)
        sum.net += y.net, sum.amount += y.amount;
    x.push_back(sum);
    for (size_t k = 0; k < nd; ++k)
      out[k] = signed_cell(flow_pct(x[k]));
    const double net = sum.amount > 0 ? sum.net / 1e8 : kNaN;
    out[nd] = sheet_cell(net, tint(net, 0.0), "%s", yi_text(net, true).c_str());
    out[nd + 1] = signed_cell(flow_pct(sum));
    p.flow[row] = std::move(x);
  };
  s.cell = [&bs, &days, fill, nd](const RowSrc &x, size_t t, LinesSheet &p, SheetCell *out) {
    const LinesQuote &q = x.row->q;
    assert(q.ok() && "FLOW 表行都有行情键");
    if (q.is(Market::kBasket))
      return fill(Market::FlowOf(bs, q.key, days), true, t, p, out);
    std::vector<Market::Flow> fl(nd);
    bool whole = true;
    for (size_t k = 0; k < nd; ++k)
      if (x.day && x.day->candle.date == days[k])
        fl[k] = Market::FlowOf(x.day->bars, bs.flow_ends);
      else
        whole = false;
    fill(std::move(fl), whole, t, p, out);
  };
  s.tip = [nd](const LinesSheet &p, size_t row, int k) {
    const Market::Flow &x = p.flow[row][std::min(static_cast<size_t>(k), nd)]; // 合计两列同一段
    if (x.amount > 0)
      tip("净流入 %s / 成交额 %s = %+.2f%%", yi_text(x.net / 1e8, true).c_str(), yi_text(x.amount / 1e8).c_str(), 100.0 * x.net / x.amount);
  };
  return s;
}
void legend_flow(LinesUIState &ui, const LinesDerived &d, const std::vector<misc::Ymd> &days, const StockInfos &infos) {
  button_bar(kRowGroupName.data(), kPageDefs[kPageFlow].groups, ui.cat[kPageFlow]);
  ImGui::SameLine();
  ImGui::TextColored(kHead, "(点行 = 画)");
  if (ImGui::IsItemHovered())
    tip("点行 = 选中 / 取消, 选中 = 左图画该篮子的累计净流入 (与 NAV 页同一份选中: 这里选的 NAV 主图 / 日内栏也画); 选中换组保留\n"
        "• 指数 = 各池 (all 池拆 全A.等 / 全A.加); 行业 = 申万一级; 概念 = 东财题材概念 (全A 内切)\n"
        "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
        "• 表上过滤条 (各页同一份): 按只数 / 总市值 / |当日资金流入| 筛本组的行, 已画 / 展开的行恒列\n"
        "• 行名前 ▸ = 展开成分股 (至多一行, 各页同一份; 该行钉在表头下): 资金流只当日; 点成分行 = 选中, 左图画它 (个股历史 + 新浪当日, 跨日接续)\n"
        "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)");
  table(ui, infos, d, kPageFlow, kFlowNameCol, {today_seg(), flow_seg(ui.basket, days)}, days, ui.basket.flow_ends);
}

// ---- CTA 右图例: 类别条 (指数 / 行业 / 概念 / 自选) + 过滤条; 表 = 该组一行一篮子, 点行 = 选为回测标的 (与 VAL 同一份单选). 列 = 中文名 | 理想 | 夏普 (同 NAV) ----
const SheetCol kCtaName = {"名", "名 = 篮子名\n"
                                 "• 算法: 切片名 + .等 (等权) / .加 (总市值加权)\n"
                                 "• 数据: 切片 = 全A / 池 / 申万一级行业 / 东财题材概念; 回测价格 = all.bar 库合成的 1 分钟点位 (同 Market 篮子历史口径)\n"
                                 "• 显示: 超长截断补 …, 悬停看全名; 选中行高亮 (左图回测它)\n"
                                 "• 成分行: 篮子名前 ▸ 展开, 缩进列出成分 (同 NAV 页); 点成分行 = 回测该股 (后复权 1 分钟价)\n"
                                 "• 适用: 指数 / 行业 / 概念 / 自选"};
void legend_cta(LinesUIState &ui, const LinesDerived &d, const LinesView &v, const StockInfos &infos) {
  button_bar(kRowGroupName.data(), kPageDefs[kPageCta].groups, ui.cat[kPageCta]);
  ImGui::SameLine();
  ImGui::TextColored(kHead, "(单选)");
  if (ImGui::IsItemHovered())
    tip("类别条选一组 (指数 = 各池, 行业 = 申万一级, 概念 = 东财题材概念), 点行 / 成分行选一个 = 左图回测它 (与 VAL 页同一份选中); 再点 = 取消\n"
        "• 篮子价格 = all.bar 库逐日合成 (成员 / 权重 / 基准取前一日收盘, 同 Market 篮子历史); 成分股价格 = 库存原价 × 后复权因子\n"
        "• 两者首次打开本页一趟合成, 存在库目录 cta/ 下, 之后只补新日\n"
        "• 理想 / 夏普 = NAV 页同名列 (随 NAV 区间), 供挑标的参考; 悬停表头看算法\n"
        "• 表上过滤条 (各页同一份): 按只数 / 总市值 / |资金流入| 筛本组的行, 选中 / 展开行恒列\n"
        "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序");
  table(ui, infos, d, kPageCta, kCtaName, {num_seg(v, {kNumIdeal, kNumSharpe})});
}

} // namespace

void RenderLinesSheet(LinesUIState &ui, int page, const std::vector<misc::Ymd> &flow_days, const StockInfos &infos) {
  const LinesDerived &d = ui.der.val;
  const LinesView &v = ui.view.val;
  if (page == kPageNav)
    legend_nav(ui, d, v, infos);
  else if (page == kPageVal)
    legend_val(ui, d, v, infos);
  else if (page == kPageFlow)
    legend_flow(ui, d, flow_days, infos);
  else
    legend_cta(ui, d, v, infos);
}

void SyncLinesCons(LinesUIState &ui, const Market::QuoteService &q) {
  std::vector<uint32_t> ids;
  for (const LinesRow &r : ui.der.val.rows[kGroupKid])
    ids.push_back(r.q.key);
  std::ranges::sort(ids);
  const Market::QuoteStatus st = q.status();
  const int64_t at = static_cast<int64_t>(st.date) * 86400 + st.sec;
  if (ids == ui.cons.ids && at == ui.cons.at && (ui.cons.day || ids.empty()))
    return;
  ui.cons.day = ids.empty() ? nullptr : q.today(Market::kStock, ids);
  ui.cons.ids = std::move(ids);
  ui.cons.at = at;
}

} // namespace GUI::Market