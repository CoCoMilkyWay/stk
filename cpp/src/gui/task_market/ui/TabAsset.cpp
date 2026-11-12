// Tab Asset - 资产表 + 右栏 Quotes (选中标的行情) / Statistics (选中列的截面分析)
//   列定义一张表 (kCols): 表头 / 悬停 / 取值 / 文本 / 画格; 排序 / 截面色 / 截面分析都只读它

#include "gui/task_market/ui/TabAsset.hpp"
#include "gui/task_market/ui/CrossSectionAnalysis.hpp"
#include "gui/util/AssetFilter.hpp" // 筛选控件 (与 OrderFlow 共用)
#include "gui/util/Board.hpp"
#include "gui/util/Chart.hpp" // tint / band_col / rank_by / sort_less
#include "imgui.h"
#include "imgui_internal.h" // TableGetHoveredRow / TableGetRowIndex
#include "implot.h"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <iterator>
#include <limits>
#include <map>
#include <numbers>
#include <numeric>
#include <set>
#include <span>
#include <string>
#include <string_view>

namespace GUI::Market {

namespace {

constexpr ImVec4 kShCol(0.0f, 0.4f, 0.8f, 1.0f), kSzCol(0.0f, 0.6f, 0.5f, 1.0f), kBjCol(0.8f, 0.5f, 0.1f, 1.0f); // 交易所色
constexpr ImVec4 kStCol(0.95f, 0.3f, 0.3f, 1.0f);                                                                // ST 标
constexpr ImVec4 kDimCol(0.6f, 0.6f, 0.6f, 1.0f);                                                                // "-" / 退市标
constexpr ImVec4 kHoverCol(0.3f, 0.3f, 0.4f, 0.3f);                                                              // 悬停列的格底
constexpr ImVec4 kPickCol(0.26f, 0.59f, 0.98f, 0.35f);                                                           // 选中行的行底
constexpr ImVec4 kSecCol(0.4f, 0.7f, 1.0f, 1.0f);                                                                // 截面分析的节标题
constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

// 在市时长 (ipo_date → out_date 或今天), 分解成 年 / 月 / 日
struct ListedSpan {
  int years = 0;
  int months = 0;
  int days = 0;
  int total_days = 0;
  bool valid = false;
};

ListedSpan listed_span(const StockInfo &info) {
  ListedSpan span;
  if (!info.ipo_date)
    return span;

  using namespace std::chrono;

  const year_month_day ipo{misc::to_days(info.ipo_date)};
  // 退市股的在市时长截到退市日, 不跟着今天一起涨
  const year_month_day end{info.out_date ? misc::to_days(info.out_date)
                                         : floor<days>(system_clock::now())};
  if (!ipo.ok() || !end.ok() || sys_days(end) < sys_days(ipo))
    return span;

  span.total_days = static_cast<int>((sys_days(end) - sys_days(ipo)).count());

  int y = static_cast<int>(end.year()) - static_cast<int>(ipo.year());
  int m = static_cast<int>(static_cast<unsigned>(end.month())) -
          static_cast<int>(static_cast<unsigned>(ipo.month()));
  int d = static_cast<int>(static_cast<unsigned>(end.day())) -
          static_cast<int>(static_cast<unsigned>(ipo.day()));
  if (d < 0) {
    --m;
    // 借上一个月的天数 (相对 end)
    const year_month prev = year_month{end.year(), end.month()} - months{1};
    d += static_cast<int>(static_cast<unsigned>((prev / last).day()));
  }
  if (m < 0) {
    --y;
    m += 12;
  }

  span.years = y;
  span.months = m;
  span.days = d;
  span.valid = true;
  return span;
}

// 0 = 正常 (含无基本面), 1 = ST, 2 = *ST (退市风险警示); st = cn_stock_status.st_status 原值
int st_level(const StockInfo *info) { return info ? info->st : 0; }

// 行业展示名: 优先申万一级名, 缺名回落到代码
const std::string &industry_name(const StockInfo &info) { return info.ind_name.empty() ? info.ind_code : info.ind_name; }

// 基本面未载入 (stock_info 空) 或档案无此代码 → nullptr
const StockInfo *find_info(const AssetItem &asset, const StockInfos &stock_info) {
  if (stock_info.empty())
    return nullptr;
  assert(asset.asset_id < stock_info.size() && "AssetInfo 未与 items 对齐");
  const StockInfo &s = stock_info[asset.asset_id];
  return s.known ? &s : nullptr;
}

bool shown(const AssetItem &asset, const StockInfo *info, const AssetFilter &f) {
  if (!f.st.empty() && !f.st.count(st_level(info)))
    return false;
  // 无基本面 = 在市与否不明, 设了在市条件即不过
  const int listed = !info ? -1 : (info->out_date ? 1 : 0);
  if (!f.listed.empty() && (listed < 0 || !f.listed.count(listed)))
    return false;
  if (!f.board.empty() && !f.board.count(GetBoardType(asset.asset_code)))
    return false;
  if (!f.industry.empty() && (!info || !f.industry.count(info->ind_code)))
    return false;
  return f.search.empty() || asset.asset_code.find(f.search) != std::string::npos ||
         (info && info->name.find(f.search) != std::string::npos);
}

// 一行的取数源: 资产 + 基本面 (无 = nullptr) + 行统计 + 近况
struct Row {
  const AssetItem &a;
  const StockInfo *info;
  const Asset::AssetStats &s;
  Recent rc;
};

Row make_row(const Asset &asset, const StockInfos &stock_info, const Today<Candle> &today, size_t id) {
  assert(today.day && id < today.day->size());
  const AssetItem &a = asset.items[id];
  const StockInfo *info = find_info(a, stock_info);
  return {a, info, asset.asset_stats[id], info ? RecentOf(*info, (*today.day)[id], today.prev) : Recent{}};
}

// 文本 (只排序) / 分类 (饼图) / 数值 (分布 + 排名)
enum class Kind { Text,
                  Category,
                  Number };
// 一列: num = 排序 / 截面色 / 统计同一个键 (数值升序; NaN = 无可比值, 排序恒末, 不着色);
//   good = 截面色口径 (GUI::rank_by: +1 = 高为好, −1 = 低为好, 0 = 不着色)
struct Column {
  const char *head; // 表头
  const char *name; // Statistics 标题
  const char *tip;  // 表头悬停
  Kind kind;
  int good;
  const char *fmt;                               // Number 的默认画格格式
  double (*num)(const Row &);                    // 键; 空 = 按 text 排
  std::string_view (*text)(const Row &);         // Text / Category 的文本 ("-" = 无)
  void (*cell)(const Row &, double v, ImU32 cs); // 画格 (v = num, cs = 截面色 / 0); 空 = Number 按 fmt, 其余按 text
};

template <float StockInfo::*F>
double field(const Row &r) { return r.info ? r.info->*F : kNaN; }

// cs 非 0 → 文字着截面色
template <class F>
void tinted(ImU32 cs, F &&draw) {
  if (cs)
    ImGui::PushStyleColor(ImGuiCol_Text, cs);
  draw();
  if (cs)
    ImGui::PopStyleColor();
}
void dash_cell() { ImGui::TextColored(kDimCol, "-"); }
// 近况格: v [%], 按涨跌着色
void pct_cell(const Row &, double v, ImU32) {
  if (std::isnan(v))
    return dash_cell();
  ImGui::TextColored(tint(v, 0.0), "%+.2f%%", v);
}
// 估值列 (PE / PB / PS / PCF) 的键 = 倍数 F 的倒数 (收益率 %): 便宜 → 高, 亏损为负 (越亏越低); 倍数 0 / 无 → NaN.
//   升序 = 亏得多 → 亏得少 → 贵 → 便宜, 与 DY 列同向 (高为好)
template <float StockInfo::*F>
double yield(const Row &r) {
  const float m = r.info ? r.info->*F : 0.f;
  return m != 0.f && std::isfinite(m) ? 100.0 / m : kNaN;
}
// 估值格: 照画倍数 (P 位小数), 字着截面色
template <float StockInfo::*F, int P>
void multiple_cell(const Row &r, double v, ImU32 cs) {
  if (std::isnan(v))
    return dash_cell();
  tinted(cs, [&] { ImGui::Text("%.*f", P, r.info->*F); });
}

const Column kCols[] = {
    {"Code", "Code",
     "证券代码 (Code)\n股票的唯一标识符\n格式:6位数字(如600000、000001、688001)",
     Kind::Text, 0, nullptr, nullptr,
     [](const Row &r) -> std::string_view { return r.a.asset_code; }, nullptr},

    {"Name", "Name",
     "股票名称 (Name)\n公司在交易所的简称 (逐日 PIT)\n来源: cn_stock_instruments 最新交易日\n\n退市股回落到 cn_stock_basic_info 的最后简称",
     Kind::Text, 0, nullptr, nullptr,
     [](const Row &r) -> std::string_view {
       if (r.info && !r.info->name.empty())
         return r.info->name;
       return "-";
     },
     nullptr},

    {"Mkt", "Market",
     "市场 = 交易所.板块 (板块按代码前缀, 色按交易所)\nSH.主 = 沪市主板 (600/601/603/605)\nSH.科 = 科创板 (688/689)\nSZ.主 = 深市主板 (000/001/002/003/004)\nSZ.创 = 创业板 (300/301/302/309)\nBJ.北 = 北交所 (43/83/87/88/92)",
     Kind::Category, 0, nullptr,
     [](const Row &r) { return static_cast<double>(GetBoardType(r.a.asset_code)); },
     [](const Row &r) -> std::string_view { return MarketAbbr(GetBoardType(r.a.asset_code)); },
     [](const Row &r, double, ImU32) {
       const char *t = MarketAbbr(GetBoardType(r.a.asset_code));
       if (t[0] == '-')
         return dash_cell();
       ImGui::TextColored(r.a.exchange == "SH" ? kShCol : r.a.exchange == "SZ" ? kSzCol
                                                                               : kBjCol,
                          "%s", t);
     }},

    {"Chg", "当日涨幅 (%)",
     "当日涨幅 = 最新一日收 / 前收 − 1\n\n最新一日: 新浪当日接得上 (其前一交易日 = 本只 bar1d 水位) 取新浪快照, 否则取 bar1d 最新有成交日\n与 Lines NAV 表的近况列同一核; 随快照刷新",
     Kind::Number, 0, nullptr, [](const Row &r) { return 100.0 * r.rc.chg; }, nullptr, &pct_cell},

    {"ΔMA5", "Δ5日线 (%)",
     "Δ5日线 = 最新收 / MA5 − 1\nMA5 = 最近 5 个有成交日的收盘均值 (含最新一日)\n\n取数同 Chg (新浪当日接上 bar1d 最近几个有成交日); 不复权, 窗内除权的跳空照算",
     Kind::Number, 0, nullptr, [](const Row &r) { return 100.0 * r.rc.ma5; }, nullptr, &pct_cell},

    {"Ind", "Industry",
     "行业 (Industry)\n申万一级行业名称 (industry_level1_name)\n来源: cn_stock_industry_component 最新月度快照\n\nhover 单元格可看行业代码 (ind_code)",
     Kind::Category, 0, nullptr, nullptr, // 按展示名排, 与列内容一致
     [](const Row &r) -> std::string_view {
       if (r.info && !industry_name(*r.info).empty())
         return industry_name(*r.info);
       return "-";
     },
     [](const Row &r, double, ImU32) {
       if (!r.info || industry_name(*r.info).empty())
         return dash_cell();
       ImGui::TextUnformatted(industry_name(*r.info).c_str());
       if (ImGui::IsItemHovered() && !r.info->ind_code.empty())
         ImGui::SetTooltip("%s", r.info->ind_code.c_str());
     }},

    {"PE", "EP = 1/PE(TTM) (%)",
     "滚动市盈率 (PE TTM)\npeTTM = Trailing Twelve Months P/E Ratio\n= 股票收盘价 / 每股盈余TTM\n= (收盘价 x 总股本) / 归属母公司股东净利润TTM\n\nTTM = 过去12个月滚动数据\n反映公司盈利能力,数值越低估值越便宜\n\n排序 / 截面色 / 统计按倒数 (收益率 %): 高 = 便宜, 亏损为负排最差一端, 0 → 不可比",
     Kind::Number, 1, nullptr, &yield<&StockInfo::pe_ttm>, nullptr, &multiple_cell<&StockInfo::pe_ttm, 1>},

    {"PB", "BP = 1/PB(MRQ) (%)",
     "市净率 (PB MRQ)\npbMRQ = Price-to-Book Ratio (Most Recent Quarter)\n= 总市值 / 归母普通股权益MRQ\n= (收盘价 x 总股本) / (total_equity_to_parent_shareholders - other_equity_instruments)\n其他权益工具 = 永续债 / 优先股, 不属普通股东\n\nMRQ = 最新报告期 (max report_date 的最新可见行)\n反映账面价值,通常>1,<1可能破净\n\n排序 / 截面色 / 统计按倒数 (收益率 %): 高 = 便宜, 亏损为负排最差一端, 0 → 不可比",
     Kind::Number, 1, nullptr, &yield<&StockInfo::pb_mrq>, nullptr, &multiple_cell<&StockInfo::pb_mrq, 2>},

    {"PS", "SP = 1/PS(TTM) (%)",
     "滚动市销率 (PS TTM)\npsTTM = Price-to-Sales Ratio (TTM)\n= 股票收盘价 / 每股销售额\n= (收盘价 x 总股本) / 营业总收入TTM\n\n反映每单位营收对应的市值\n适用于尚未盈利但有营收的公司\n\n排序 / 截面色 / 统计按倒数 (收益率 %): 高 = 便宜, 亏损为负排最差一端, 0 → 不可比",
     Kind::Number, 1, nullptr, &yield<&StockInfo::ps_ttm>, nullptr, &multiple_cell<&StockInfo::ps_ttm, 2>},

    {"PCF", "CP = 1/PCF (%)",
     "滚动市现率 (PCF TTM)\npcfNcfTTM = Price-to-Cash-Flow Ratio (TTM)\n= 总市值 / 经营活动现金流量净额TTM\n= (收盘价 x 总股本) / net_cffoa_ttm\n\n注意分母是经营现金流, 不是现金及现金等价物净增加额\n烧钱(经营现金流为负) 保留负值, 不置空\n\n反映现金流创造能力\n比PE更难以通过会计手段操纵\n\n排序 / 截面色 / 统计按倒数 (收益率 %): 高 = 便宜, 亏损为负排最差一端, 0 → 不可比",
     Kind::Number, 1, nullptr, &yield<&StockInfo::pcf_ttm>, nullptr, &multiple_cell<&StockInfo::pcf_ttm, 1>},

    {"DY1", "DY 近1年 (年化 %)",
     "股息率 近1年 (Dividend Yield, 年化)\n= 近365日税前分红总额 / 总市值 x 100%\n\n说明:\n- 单位: %\n- 窗口以分红公告日 (publish_date) 锚定, 非除权日\n- 口径与 L_1min 特征 dy_raw 一致\n- 0.00 = 窗口内确实没有分红公告 (非缺失)",
     Kind::Number, 1, "%.2f", &field<&StockInfo::dy1y>, nullptr, nullptr},

    {"DY3", "DY 近3年 (年化 %)",
     "股息率 近3年 (年化平均)\n= 近3年(1095日)税前分红总额 / 年数 / 总市值 x 100%\n\n说明:\n- 已年化, 可与 DY1 / DY5 直接横向比较\n- 上市不足3年的按实际上市年数年化, 不被系统性摊薄\n- 上市不足一个季度 → 留空 (年化无意义)\n- DY3 明显低于 DY1 = 近年才开始分红",
     Kind::Number, 1, "%.2f", &field<&StockInfo::dy3y>, nullptr, nullptr},

    {"DY5", "DY 近5年 (年化 %)",
     "股息率 近5年 (年化平均)\n= 近5年(1825日)税前分红总额 / 年数 / 总市值 x 100%\n\n说明:\n- 已年化, 可与 DY1 / DY3 直接横向比较\n- 上市不足5年的按实际上市年数年化\n- DY1 ≈ DY3 ≈ DY5 = 长期稳定分红",
     Kind::Number, 1, "%.2f", &field<&StockInfo::dy5y>, nullptr, nullptr},

    {"Cap", "Market Cap (亿元)",
     "总市值 (Market Cap)\n= 收盘价 x 总股本 / 1亿\n= close x total_shares (不复权真价)\n\n说明:\n- 单位: 亿元\n- 与 PE/PB/PS/PCF/DY 的分子同源\n- 数据来自 cn_stock_real_bar1d.close + cn_stock_shares.total_shares",
     Kind::Number, 1, "%.1f",
     [](const Row &r) { return r.info && r.info->mcap > 0 ? static_cast<double>(r.info->mcap) : kNaN; }, // 0 / NaN = 缺收盘价或股本, 不是"市值为零"
     nullptr, nullptr},

    {"ST", "ST",
     "ST股 (Special Treatment)\n来源: cn_stock_status.st_status (逐日)\n\nST  = 特别处理 (连续两年亏损等), 涨跌幅限制 ±5%\n*ST = 退市风险警示 (风险更高一级)\n-   = 正常\n\n与 Name 列同源同日 (简称前缀 ↔ 本列取值 严格一致)",
     Kind::Category, 0, nullptr,
     [](const Row &r) { return static_cast<double>(st_level(r.info)); }, // 正常 < ST < *ST
     [](const Row &r) -> std::string_view {
       const int level = st_level(r.info);
       return level == 2 ? "*ST" : level == 1 ? "ST"
                                              : "正常";
     },
     [](const Row &, double v, ImU32) {
       if (v > 0)
         ImGui::TextColored(kStCol, "%s", v == 2 ? "*ST" : "ST");
       else
         ImGui::TextUnformatted("-");
     }},

    {"DL", "DL",
     "退市 (Delisted)\n是否已退市或处于退市状态\nDL = 已退市\nhover 单元格可看退市日期",
     Kind::Category, 0, nullptr,
     [](const Row &r) { return r.info && r.info->out_date ? 1.0 : 0.0; },
     [](const Row &r) -> std::string_view { return r.info && r.info->out_date ? "退市" : "在市"; },
     [](const Row &r, double v, ImU32) {
       if (!(v > 0)) {
         ImGui::TextUnformatted("-");
         return;
       }
       ImGui::TextColored(kDimCol, "DL");
       if (ImGui::IsItemHovered())
         ImGui::SetTooltip("Delisted: %s", misc::dash(r.info->out_date).c_str());
     }},

    {"Listed", "Listed Days (在市总天数)",
     "在市时间 (Listed Duration)\n紧凑格式 年/月/日 — 如 11/02/06 = 11年2个月6天\n\n区间: 上市日 → 退市日 (退市股截到退市日) 或今天\nhover 单元格可看起止日期与在市总天数\n排序与横截面分析口径为在市总天数",
     Kind::Number, 1, nullptr,
     [](const Row &r) {
       const ListedSpan span = r.info ? listed_span(*r.info) : ListedSpan{};
       return span.valid ? static_cast<double>(span.total_days) : kNaN;
     },
     nullptr,
     [](const Row &r, double v, ImU32 cs) {
       if (std::isnan(v))
         return dash_cell();
       const ListedSpan span = listed_span(*r.info);
       tinted(cs, [&] { ImGui::Text("%02d/%02d/%02d", span.years, span.months, span.days); });
       if (ImGui::IsItemHovered())
         ImGui::SetTooltip("上市 %s → %s\n在市 %d 年 %d 月 %d 天 (共 %d 天)",
                           misc::dash(r.info->ipo_date).c_str(),
                           r.info->out_date ? misc::dash(r.info->out_date).c_str() : "至今",
                           span.years, span.months, span.days, span.total_days);
     }},

    {"Days", "Trading Days",
     "交易日数 (Trading Days)\n该股票在数据库中有数据的总交易日数\n= 帧目录中已编码的天数\n可用于判断数据完整性",
     Kind::Number, 0, "%.0f",
     [](const Row &r) { return static_cast<double>(r.s.total_days); }, nullptr, nullptr},

    {"Orders/Day", "Avg Orders / Day",
     "日均逐笔数 (Average Orders per Trading Day)\n所有已有逐笔数据交易日的平均记录数量 (委托+成交合并后)\n= Σ order_count / 有逐笔数据的交易日数\n\n说明:\n- 单位:条记录/日\n- 显示格式:>1M用M(百万), >1K用K(千)\n- 条数取自日文件目录, 扫描时一并读到",
     Kind::Number, 0, nullptr,
     [](const Row &r) {
       return r.s.total_days > 0 ? static_cast<double>(r.s.total_orders) / static_cast<double>(r.s.total_days) : kNaN;
     },
     nullptr,
     [](const Row &, double v, ImU32) {
       if (std::isnan(v))
         dash_cell();
       else if (v > 1000000.0)
         ImGui::Text("%.2fM", v / 1000000.0);
       else if (v > 1000.0)
         ImGui::Text("%.1fK", v / 1000.0);
       else
         ImGui::Text("%.0f", v);
     }},

    // expected_days == 0 = 该资产在回测区间内本就不该有数据 (北交所 / 尚未上市 / 已退市), 那不是缺口, 留空而不是 0%
    {"Orders%", "Orders % (回测区间完整性)",
     "逐笔完整性 (Orders Coverage)\n回测区间内已编码的交易日占比\n= (应有天数 - 缺失天数) / 应有天数\n\n分母是该资产\"本该有逐笔\"的交易日:\n已上市未退市, 且排除当日全天停牌\n(北交所不在 L2 覆盖范围, 整体留空)\n\n与 Calendar 页的完整性、Encode 页的缺失表同源\nhover 单元格可看缺失天数",
     Kind::Number, 0, nullptr,
     [](const Row &r) { return r.s.expected_days > 0 ? static_cast<double>(r.s.orders_coverage_percent()) : kNaN; },
     nullptr,
     [](const Row &r, double v, ImU32) {
       if (std::isnan(v))
         return dash_cell();
       const ImVec4 color = v >= 99.9   ? ImVec4(0.3f, 0.95f, 0.4f, 1.0f)
                            : v >= 95.0 ? ImVec4(1.0f, 1.0f, 0.0f, 1.0f)
                                        : ImVec4(1.0f, 0.5f, 0.0f, 1.0f);
       ImGui::TextColored(color, "%.1f%%", v);
       if (ImGui::IsItemHovered())
         ImGui::SetTooltip("Missing %zu of %zu trading days", r.s.orders_missing, r.s.expected_days);
     }},
};
constexpr int kNumCols = static_cast<int>(std::size(kCols));

void draw_cell(const Column &c, const Row &r, ImU32 cs) {
  const double v = c.num ? c.num(r) : kNaN;
  if (c.cell)
    return c.cell(r, v, cs);
  if (c.kind == Kind::Number) {
    assert(c.num && c.fmt);
    if (std::isnan(v))
      return dash_cell();
    return tinted(cs, [&] { ImGui::Text(c.fmt, v); });
  }
  const std::string_view t = c.text(r);
  if (t == "-")
    return dash_cell();
  ImGui::TextUnformatted(t.data(), t.data() + t.size());
}

// 视图 = 过滤 + 排序 (sort_less: NaN 恒末) + 截面色: 过滤后 pool 内的名次 (GUI::rank_by, 并列取中) → band_col.
//   用名次而非数值归一化: 估值 / 市值重尾, min-max 染色会被几个极端值压成一片同色; 并列取中: DY 成片的 0 不被摊成渐变
void rebuild_view(AssetView &view, const Asset &asset, const StockInfos &stock_info, const Today<Candle> &today, const AssetViewKey &key) {
  const size_t count = asset.items.size();
  assert(asset.asset_stats.size() == count);
  const auto row = [&](size_t id) { return make_row(asset, stock_info, today, id); };

  view.rows.clear();
  for (size_t id = 0; id < count; ++id)
    if (shown(asset.items[id], find_info(asset.items[id], stock_info), key.filter))
      view.rows.push_back(id);

  if (key.sort_column >= 0) {
    assert(key.sort_column < kNumCols);
    const Column &c = kCols[key.sort_column];
    const bool desc = !key.sort_ascending;
    if (c.num) {
      std::vector<double> v(count, kNaN);
      for (const size_t id : view.rows)
        v[id] = c.num(row(id));
      std::stable_sort(view.rows.begin(), view.rows.end(), [&](size_t a, size_t b) { return sort_less(v[a], v[b], desc); });
    } else {
      std::vector<std::string_view> t(count);
      for (const size_t id : view.rows)
        t[id] = c.text(row(id));
      std::stable_sort(view.rows.begin(), view.rows.end(), [&](size_t a, size_t b) { return desc ? t[b] < t[a] : t[a] < t[b]; });
    }
  }

  view.cs.assign(kNumCols, {});
  for (int ci = 0; ci < kNumCols; ++ci) {
    const Column &c = kCols[ci];
    if (c.good == 0)
      continue;
    std::vector<uint32_t> &cs = view.cs[ci];
    cs.assign(view.rows.size(), 0);
    rank_by(
        view.rows.size(), c.good, [&](size_t pos) { return c.num(row(view.rows[pos])); },
        [&](size_t pos, double u) { cs[pos] = ImGui::ColorConvertFloat4ToU32(band_col(u, 1.f)); });
  }

  view.key = key;
  view.built = true;
}

// 视图过期就重建 (表格与截面分析共用 state.view)
void sync_view(AssetState &state, const Asset &asset, const StockInfos &stock_info) {
  const AssetViewKey key{asset.generation, asset.items.size(), stock_info.size(), state.today.ver, state.sort_column, state.sort_ascending, state.filter};
  if (!state.view.built || !(state.view.key == key))
    rebuild_view(state.view, asset, stock_info, state.today, key);
}

// 新浪当日日根: 键集 = 全部资产 (Today 照版本取)
void sync_today(Today<Candle> &t, const QuoteService &q, const Asset &asset) {
  std::vector<uint32_t> keys;
  keys.reserve(asset.items.size());
  for (const AssetItem &a : asset.items)
    keys.push_back(static_cast<uint32_t>(a.asset_id));
  Sync(q, kStock, keys, t);
}

// 过滤条: 搜索 / ST / 在市 / 板块 / 行业 + 列出数 + 截面色图例 + 右栏开关
void filter_bar(AssetState &state, const std::vector<AssetItem> &assets, const StockInfos &stock_info) {
  ImGui::SetNextItemWidth(250.0f);
  if (ImGui::InputTextWithHint("##Search", "Search code/name...", state.search_buf.data(), state.search_buf.size()))
    state.filter.search = state.search_buf.data();

  // ST/Listed/Board: 与 Features → OrderFlow 共用控件 (gui/util/AssetFilter.hpp).
  // ST 只有 0..2 (st_level 口径, 无退市整理期一级); Listed 看 out_date 是否为 0
  ImGui::SameLine();
  GUI::Filter::CommonFilters<BoardType>("Assets", false, state.filter.st, state.filter.listed, state.filter.board);

  // 行业维自绘: 这里的键是字符串行业码 ind_code, 与 OrderFlow 的数字 SW2021 ID
  // 不同源, 且选项要从当前 stock_info 动态扫出来
  if (state.industries_of != stock_info.size()) {
    std::map<std::string, std::string> ind_map; // code -> name
    for (const AssetItem &asset : assets)
      if (const StockInfo *info = find_info(asset, stock_info); info && !info->ind_code.empty())
        ind_map[info->ind_code] = info->ind_name;
    state.industries.clear();
    for (const auto &[code, name] : ind_map)
      state.industries.emplace_back(code, name.empty() ? code : name);
    state.industries_of = stock_info.size();
  }
  ImGui::SameLine();
  GUI::Filter::MultiSelectCombo("Industry##IndFilter", 150.0f, state.industries, state.filter.industry);

  ImGui::SameLine();
  ImGui::Text("Showing:%zu/%zu", state.view.rows.size(), assets.size());

  ImGui::SameLine();
  ImGui::BeginGroup();
  ImGui::TextColored(band_col(0.0, 1.f), "■");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(band_col(0.5, 1.f), "■");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(band_col(1.0, 1.f), "■");
  ImGui::EndGroup();
  if (ImGui::IsItemHovered()) {
    ImGui::SetTooltip(
        "截面色 (Listed / PE / PB / PS / PCF / DY1 / DY3 / DY5 / Cap)\n"
        "绿 = 好, 红 = 差; 取的是当前 filter 后 pool 内的名次分位\n"
        "  Listed: 在市越久越绿\n"
        "  PE/PB/PS/PCF: 倒数 (收益率) 越高越绿 (亏损的负值排在最差一端)\n"
        "  DY1/DY3/DY5: 分红越多越绿\n"
        "  Cap: 市值越大越绿\n\n"
        "用名次分位而非数值归一化: 估值/市值是重尾分布,\n"
        "按 min-max 染色会被几个极端值压成一片同色");
  }

  ImGui::SameLine();
  if (ImGui::SmallButton(state.show_right_panel ? "Hide" : "Show"))
    state.show_right_panel = !state.show_right_panel;
}

// 资产表: 只画可见的行 (clipper); 点行 = 选中 (Quotes 看它), Statistics 页再切截面分析的列
void data_table(const Asset &asset_data, const StockInfos &stock_info, AssetState &asset_state) {
  ImGuiTableFlags flags = ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                          ImGuiTableFlags_Sortable | ImGuiTableFlags_ScrollX | ImGuiTableFlags_ScrollY |
                          ImGuiTableFlags_Resizable | ImGuiTableFlags_Reorderable |
                          ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_NoSavedSettings;

  if (!ImGui::BeginTable("AssetsTable", kNumCols, flags))
    return;

  for (int ci = 0; ci < kNumCols; ++ci)
    ImGui::TableSetupColumn(kCols[ci].head, ci == 0 ? ImGuiTableColumnFlags_DefaultSort | ImGuiTableColumnFlags_PreferSortAscending : 0);
  ImGui::TableSetupScrollFreeze(0, 1);

  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  for (int ci = 0; ci < kNumCols; ++ci) {
    ImGui::TableSetColumnIndex(ci);
    ImGui::PushID(ci);
    ImGui::TableHeader(kCols[ci].head);
    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::TextUnformatted(kCols[ci].tip);
      ImGui::EndTooltip();
    }
    ImGui::PopID();
  }

  // 排序规则的变化由 SpecsDirty 告知; 重排在 rebuild_view 里, 只在键变时做一次 (见 AssetView)
  if (ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs()) {
    if (sort_specs->SpecsDirty && sort_specs->SpecsCount > 0) {
      const auto &spec = sort_specs->Specs[0];
      asset_state.sort_column = spec.ColumnIndex;
      asset_state.sort_ascending = spec.SortDirection == ImGuiSortDirection_Ascending;
    }
    sort_specs->SpecsDirty = false;
  }

  sync_view(asset_state, asset_data, stock_info);
  const AssetView &view = asset_state.view;
  const int hovered_col = ImGui::TableGetHoveredColumn();
  const int hovered_row = ImGui::TableGetHoveredRow(); // 上帧的表行号 (0 = 表头; clipper 下 = 本帧第几个交的行)
  size_t hit = SIZE_MAX;                               // 悬停行的 rows 位置
  ImGuiListClipper clip;
  clip.Begin(static_cast<int>(view.rows.size()));
  while (clip.Step())
    for (int i = clip.DisplayStart; i < clip.DisplayEnd; ++i) {
      const size_t pos = static_cast<size_t>(i), id = view.rows[pos];
      // StockInfo 每帧现查 (基本面可能整体重载, 缓存指针会失效)
      const Row r = make_row(asset_data, stock_info, asset_state.today, id);
      ImGui::TableNextRow();
      if (ImGui::TableGetRowIndex() == hovered_row)
        hit = pos;
      if (asset_state.selected_asset == static_cast<int>(id))
        ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, ImGui::GetColorU32(kPickCol));
      for (int ci = 0; ci < kNumCols; ++ci) {
        ImGui::TableSetColumnIndex(ci);
        if (hovered_col == ci)
          ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(kHoverCol));
        draw_cell(kCols[ci], r, view.cs[ci].empty() ? 0 : view.cs[ci][pos]);
      }
    }

  // 左键点行内任意处 (格子的文字 / 内边距 / 边框都算) 即点该行: 行 = 表的行背景带 (上下相接无缝), 列 = 列区 (左右相接无缝); 滚动条不算
  const ImRect inner = ImGui::GetCurrentWindow()->InnerClipRect;
  if (ImGui::IsMouseClicked(ImGuiMouseButton_Left) && ImGui::IsWindowHovered() && inner.Contains(ImGui::GetMousePos()) && hit != SIZE_MAX &&
      hovered_col >= 0 && hovered_col < kNumCols) {
    asset_state.selected_asset = static_cast<int>(view.rows[hit]);
    asset_state.show_right_panel = true;
    if (!asset_state.quotes_page)
      asset_state.selected_column_idx = asset_state.selected_column_idx == hovered_col ? -1 : hovered_col;
  }

  ImGui::EndTable();
}

// 数值列的截面分析: 分板块统计 + 分布 (去首尾 5%; 直方 / 拟合正态 / 经验 CDF) + 首尾 10 名. 行 = 表格的视图 (只取有值的)
void numeric_analysis(const Asset &asset_data, const StockInfos &stock_info, const AssetState &asset_state, const Column &col) {
  std::vector<std::string> names;
  std::vector<float> values;
  std::vector<std::string> codes;
  for (const size_t id : asset_state.view.rows) {
    const Row r = make_row(asset_data, stock_info, asset_state.today, id);
    const double v = col.num(r);
    if (std::isnan(v))
      continue;
    names.push_back(r.info && !r.info->name.empty() ? r.info->name : r.a.asset_code);
    values.push_back(static_cast<float>(v));
    codes.push_back(r.a.asset_code);
  }

  if (values.empty()) {
    ImGui::Text("No valid data");
    return;
  }

  ImGui::TextColored(kSecCol, "Board Statistics");
  auto board_stats = GroupNumericByBoard(codes, values);

  if (ImGui::BeginTable("BoardStatsTable", 5,
                        ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit |
                            ImGuiTableFlags_Resizable | ImGuiTableFlags_NoSavedSettings)) {
    ImGui::TableSetupColumn("Board");
    ImGui::TableSetupColumn("Mean");
    ImGui::TableSetupColumn("Median");
    ImGui::TableSetupColumn("StdDev");
    ImGui::TableSetupColumn("Count");
    ImGui::TableHeadersRow();

    for (const auto &bs : board_stats) {
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("%s", bs.board_name.c_str());
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("%.2f", bs.mean);
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("%.2f", bs.median);
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("%.2f", bs.std_dev);
      ImGui::TableSetColumnIndex(4);
      ImGui::Text("%zu", bs.count);
    }
    ImGui::EndTable();
  }

  ImGui::Spacing();

  ImGui::TextColored(kSecCol, "Distribution (Outliers Removed)");
  const std::vector<float> kept = RemoveOutliers(values, 5.0f); // 升序
  if (!kept.empty()) {
    constexpr int kBins = 100, kCurve = 200; // 直方格数 / 曲线点数
    const float lo = kept.front(), hi = kept.back(), range = hi - lo, bin = range / kBins;
    const float n = static_cast<float>(kept.size());
    const float mean = std::accumulate(kept.begin(), kept.end(), 0.0f) / n;
    float var = 0.0f;
    for (const float v : kept)
      var += (v - mean) * (v - mean);
    const float sd = std::sqrt(var / n);

    // 直方 (密度) / 拟合正态 / 经验 CDF (右轴)
    std::vector<float> hx(kBins), hy(kBins, 0.0f);
    for (const float v : kept)
      hy[static_cast<size_t>(std::clamp(static_cast<int>((v - lo) / bin), 0, kBins - 1))] += 1.0f;
    for (int i = 0; i < kBins; ++i)
      hx[i] = lo + (i + 0.5f) * bin, hy[i] /= n * bin;
    std::vector<float> cx(kCurve), pdf(kCurve), cdf(kCurve);
    for (int i = 0; i < kCurve; ++i) {
      const float x = lo + i * range / (kCurve - 1), z = (x - mean) / sd;
      cx[i] = x;
      pdf[i] = std::exp(-0.5f * z * z) / (sd * std::sqrt(2.0f * std::numbers::pi_v<float>));
      cdf[i] = static_cast<float>(std::upper_bound(kept.begin(), kept.end(), x) - kept.begin()) / n;
    }
    const float ymax = std::max(*std::max_element(hy.begin(), hy.end()), *std::max_element(pdf.begin(), pdf.end())) * 1.15f;

    if (ImPlot::BeginPlot("##Distribution", ImVec2(-1, 350))) {
      ImPlot::SetupAxes("Value", "Density");
      ImPlot::SetupAxisLimits(ImAxis_X1, lo, hi, ImPlotCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, ymax, ImPlotCond_Always);
      ImPlot::SetupAxis(ImAxis_Y2, "CDF", ImPlotAxisFlags_AuxDefault);
      ImPlot::SetupAxisLimits(ImAxis_Y2, 0, 1.05, ImPlotCond_Always);
      ImPlot::SetNextFillStyle(ImVec4(0.5f, 0.7f, 1.0f, 0.4f));
      ImPlot::PlotBars("Histogram", hx.data(), hy.data(), kBins, bin * 0.9);
      ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), 2.5f);
      ImPlot::PlotLine("Fitted PDF", cx.data(), pdf.data(), kCurve);
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
      ImPlot::SetNextLineStyle(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), 2.5f);
      ImPlot::PlotLine("Empirical CDF", cx.data(), cdf.data(), kCurve);
      ImPlot::EndPlot();
    }
  }

  ImGui::Spacing();
  const float half = ImGui::GetContentRegionAvail().x * 0.48f;
  const auto top = [&](const char *id, const char *title, const ImVec4 &c, bool desc) {
    ImGui::BeginChild(id, ImVec2(half, 250), true);
    ImGui::TextColored(c, "%s", title);
    const std::vector<std::pair<std::string, float>> t = GetTopN(names, values, 10, desc);
    for (size_t i = 0; i < t.size(); ++i)
      ImGui::Text("%zu. %s: %.2f", i + 1, t[i].first.c_str(), t[i].second);
    ImGui::EndChild();
  };
  top("Top10", "Top 10", ImVec4(0.4f, 1.0f, 0.4f, 1.0f), true);
  ImGui::SameLine();
  top("Bottom10", "Bottom 10", ImVec4(1.0f, 0.4f, 0.4f, 1.0f), false);
}

// 饼图: 各类的只数
void pie(const char *id, const std::vector<CategoryCount> &cs, float height, double radius) {
  if (cs.empty() || !ImPlot::BeginPlot(id, ImVec2(-1, height)))
    return;
  std::vector<const char *> labels;
  std::vector<float> counts;
  for (const CategoryCount &c : cs)
    labels.push_back(c.label.c_str()), counts.push_back(static_cast<float>(c.count));
  ImPlot::PlotPieChart(labels.data(), counts.data(), static_cast<int>(counts.size()), 0.5, 0.5, radius);
  ImPlot::EndPlot();
}

// 分类列的截面分析: 全体饼图 + 分板块饼图 (两列). 行 = 表格的视图
void category_analysis(const Asset &asset_data, const StockInfos &stock_info, const AssetState &asset_state, const Column &col) {
  std::vector<std::string> categories, codes;
  for (const size_t id : asset_state.view.rows) {
    const Row r = make_row(asset_data, stock_info, asset_state.today, id);
    categories.emplace_back(col.text(r));
    codes.push_back(r.a.asset_code);
  }
  if (categories.empty()) {
    ImGui::Text("No valid data");
    return;
  }

  ImGui::TextColored(kSecCol, "Overall Distribution");
  pie("##OverallPie", CountCategories(categories), 250, 0.4);
  ImGui::Spacing();

  ImGui::TextColored(kSecCol, "Board Breakdown");
  constexpr int kPerRow = 2;
  const float w = ImGui::GetContentRegionAvail().x / kPerRow - 10;
  const std::vector<BoardCategoryBreakdown> boards = GroupCategoricalByBoard(codes, categories);
  for (size_t i = 0; i < boards.size(); ++i) {
    if (i % kPerRow != 0)
      ImGui::SameLine();
    ImGui::BeginChild(("BoardPie_" + std::to_string(i)).c_str(), ImVec2(w, 220), true);
    ImGui::Text("%s", boards[i].board_name.c_str());
    pie("##BoardPie", boards[i].categories, 180, 0.35);
    ImGui::EndChild();
  }
}

// 截面分析: 表格里点中的列 (Statistics 页), 按列的种类分派
void cross_section(const Asset &asset_data, const StockInfos &stock_info, const AssetState &asset_state) {
  const int ci = asset_state.selected_column_idx;
  if (ci < 0) {
    ImGui::TextWrapped("Click on any cell to view cross-section analysis.");
    return;
  }
  assert(ci < kNumCols);
  const Column &col = kCols[ci];
  ImGui::Text("Column: %s", col.name);
  ImGui::Separator();
  switch (col.kind) {
  case Kind::Text:
    ImGui::TextDisabled("文本列, 无截面分析.");
    break;
  case Kind::Category:
    category_analysis(asset_data, stock_info, asset_state, col);
    break;
  case Kind::Number:
    numeric_analysis(asset_data, stock_info, asset_state, col);
    break;
  }
}

} // namespace

void RenderTabAsset(Asset &asset, const StockInfos &stock_info, AssetState &asset_state, QuoteHub &mk) {
  const std::vector<AssetItem> &assets = asset.items;
  // 每资产统计由扫描一次算好 (见 ScanService::run), 这里只等它到位
  if (asset.asset_stats.size() != assets.size()) {
    ImGui::TextDisabled("Waiting for database scan...");
    return;
  }
  assert(mk.quote);
  sync_today(asset_state.today, *mk.quote, asset);
  sync_view(asset_state, asset, stock_info); // 过滤条的列出数也读它

  // 左 = 过滤条 + 资产表, 右 = Quotes / Statistics
  const ImVec2 avail = ImGui::GetContentRegionAvail();
  const float left = asset_state.show_right_panel ? avail.x * asset_state.table_split_ratio : avail.x;
  ImGui::BeginChild("LeftTable", ImVec2(left, avail.y), true, ImGuiWindowFlags_HorizontalScrollbar);
  filter_bar(asset_state, assets, stock_info);
  ImGui::Spacing();
  data_table(asset, stock_info, asset_state);
  ImGui::EndChild();

  if (asset_state.show_right_panel) {
    ImGui::SameLine();
    ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(4, 4));
    ImGui::BeginChild("RightPanel", ImVec2(0, avail.y), true);
    ImGui::PopStyleVar();
    if (ImGui::BeginTabBar("##RightPanel")) {
      if (ImGui::BeginTabItem("Quotes")) {
        asset_state.quotes_page = true;
        // 键 = asset_id (== 全局 id); 未选 = 空键集
        const uint32_t id = static_cast<uint32_t>(asset_state.selected_asset);
        Sync(mk, kStock, asset_state.selected_asset >= 0 ? std::span<const uint32_t>(&id, 1) : std::span<const uint32_t>(),
             asset_state.stock.feed);
        const Feed &f = asset_state.stock.feed;
        std::string title;
        if (f.data && !f.data->keys.empty()) {
          assert(f.data->keys.front() < assets.size());
          const AssetItem &a = assets[f.data->keys.front()];
          const StockInfo *info = find_info(a, stock_info);
          title = (info && !info->name.empty() ? info->name : std::string("-")) + "  " + a.code_ex();
        }
        RenderStockPanel(mk, asset_state.stock, title);
        ImGui::EndTabItem();
      }
      if (ImGui::BeginTabItem("Statistics")) {
        asset_state.quotes_page = false;
        cross_section(asset, stock_info, asset_state);
        ImGui::EndTabItem();
      }
      ImGui::EndTabBar();
    }
    ImGui::EndChild();
  }
}

} // namespace GUI::Market