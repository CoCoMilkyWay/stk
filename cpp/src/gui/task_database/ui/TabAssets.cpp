// Tab Assets - 资产表 + 右栏 Quotes (选中标的行情) / Statistics (选中列的截面分析)
//   列定义一张表 (kCols): 表头 / 悬停 / 取值 / 文本 / 画格; 排序 / 截面色 / 截面分析都只读它

#include "gui/task_database/ui/TabAssets.hpp"
#include "gui/task_database/models/SharedTypes.hpp"
#include "gui/task_database/ui/CrossSectionAnalysis.hpp"
#include "gui/util/AssetFilter.hpp" // 筛选控件 (与 ORDERFLOW 共用)
#include "imgui.h"
#include "imgui_internal.h" // TableGetHoveredRow
#include "implot.h"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <iterator>
#include <limits>
#include <map>
#include <numeric>
#include <set>
#include <span>
#include <string>
#include <string_view>

namespace GUI::Database {

namespace {

// Color constants
constexpr ImVec4 COLOR_SH = ImVec4(0.0f, 0.4f, 0.8f, 1.0f);
constexpr ImVec4 COLOR_SZ = ImVec4(0.0f, 0.6f, 0.5f, 1.0f);
constexpr ImVec4 COLOR_BJ = ImVec4(0.8f, 0.5f, 0.1f, 1.0f);
constexpr ImVec4 COLOR_RED = ImVec4(0.95f, 0.3f, 0.3f, 1.0f);
constexpr ImVec4 COLOR_GRAY = ImVec4(0.6f, 0.6f, 0.6f, 1.0f);
constexpr ImVec4 COLOR_HOVER_COL = ImVec4(0.3f, 0.3f, 0.4f, 0.3f);    // 悬停列的格底
constexpr ImVec4 COLOR_SELECTED = ImVec4(0.26f, 0.59f, 0.98f, 0.35f); // 选中行的行底
constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

// ============================================================================
// Helper: 在市时长 (ipo_date → out_date 或今天), 分解成 年/月/日
// ============================================================================

struct ListedSpan {
  int years = 0;
  int months = 0;
  int days = 0;
  int total_days = 0;
  bool valid = false;
};

ListedSpan CalculateListedSpan(const StockInfo &info) {
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
int GetStLevel(const StockInfo *info) {
  return info ? info->st : 0;
}

// 行业展示名: 优先申万一级名, 缺名回落到代码
const std::string &GetIndustryDisplay(const StockInfo &info) {
  return info.ind_name.empty() ? info.ind_code : info.ind_name;
}

// "sh.600000" — stock_info 的键
std::string MakeStockKey(const AssetItem &asset) {
  std::string exchange_lower = asset.exchange;
  std::transform(exchange_lower.begin(), exchange_lower.end(),
                 exchange_lower.begin(), ::tolower);
  return exchange_lower + "." + asset.asset_code;
}

const StockInfo *FindStockInfo(const AssetItem &asset, const StockInfoMap &stock_info) {
  auto it = stock_info.find(MakeStockKey(asset));
  return it != stock_info.end() ? &it->second : nullptr;
}

bool ShouldShowAsset(const AssetItem &asset, const StockInfo *info, const AssetsFilter &f) {
  if (!f.st.empty() && !f.st.count(GetStLevel(info)))
    return false;
  // 未知基本面按"不确定"排除
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

// 分位 → 颜色. p = 1 最好 (绿), p = 0 最差 (红), 中间过黄.
// 直接过渡红→绿会在中段压成暗棕, 所以走黄这个中继.
ImVec4 RampColorVec4(float p) {
  constexpr ImVec4 kWorst = ImVec4(0.95f, 0.35f, 0.30f, 1.0f);
  constexpr ImVec4 kMid = ImVec4(0.95f, 0.90f, 0.35f, 1.0f);
  constexpr ImVec4 kBest = ImVec4(0.40f, 0.95f, 0.45f, 1.0f);

  p = std::clamp(p, 0.0f, 1.0f);
  const ImVec4 &a = (p < 0.5f) ? kWorst : kMid;
  const ImVec4 &b = (p < 0.5f) ? kMid : kBest;
  const float t = (p < 0.5f) ? p * 2.0f : (p - 0.5f) * 2.0f;
  return ImVec4(a.x + (b.x - a.x) * t,
                a.y + (b.y - a.y) * t,
                a.z + (b.z - a.z) * t,
                1.0f);
}

// 0 是 cs 里"无有效值"的哨兵, 所以这里必须给足 alpha (不能返回 0)
ImU32 RampColor(float p) { return ImGui::GetColorU32(RampColorVec4(p)); }

// ============================================================================
// 列定义
// ============================================================================

// 一行的取数源: 资产 + 基本面 (无 = nullptr) + 行统计
struct Row {
  const AssetItem &a;
  const StockInfo *info;
  const Asset::AssetStats &s;
};

// 文本 (只排序) / 分类 (饼图) / 数值 (分布 + 排名)
enum class Kind { Text,
                  Category,
                  Number };
// 截面色口径: 不着色 / 估值序越前越好 / 越高越好
enum class Good { None,
                  Cheap,
                  High };

// 估值序 (PE/PB/PS/PCF): 便宜 → 贵 → 亏得少 → 亏得多
//   0.1 → 10000 → -10000 → -0.1
// 负值的绝对值越大 = 亏损相对市值越小, 所以负数区间照样升序; 0 (无意义) 垫底.
bool ValuationLess(double va, double vb) {
  const int ta = (va > 0) ? 0 : (va < 0) ? 1
                                         : 2;
  const int tb = (vb > 0) ? 0 : (vb < 0) ? 1
                                         : 2;
  return (ta != tb) ? (ta < tb) : (va < vb);
}
// 列的升序 (排序与截面着色同一个口径): Cheap 列 = 估值序, 其余 = 数值升
bool Ascends(Good g, double a, double b) { return g == Good::Cheap ? ValuationLess(a, b) : a < b; }
// a 比 b 好 (截面着色: Cheap 列升序在前, High 列降序在前)
bool Better(Good g, double a, double b) { return g == Good::Cheap ? Ascends(g, a, b) : Ascends(g, b, a); }

struct Column {
  const char *head; // 表头
  const char *name; // Statistics 标题
  const char *tip;  // 表头悬停
  Kind kind;
  Good good;
  const char *fmt;                               // Number 的默认画格格式
  double (*num)(const Row &);                    // 排序 / 截面键 (NaN = 无可比值, 排序恒末); 空 = 按 text 排
  std::string_view (*text)(const Row &);         // Text / Category 的文本 ("-" = 无)
  void (*cell)(const Row &, double v, ImU32 cs); // 画格 (v = num, cs = 截面色 / 0); 空 = Number 按 fmt, 其余按 text
};

template <float StockInfo::*F>
double Field(const Row &r) { return r.info ? r.info->*F : kNaN; }

// cs 非 0 → 文字着截面色
template <class F>
void Tinted(ImU32 cs, F &&draw) {
  if (cs)
    ImGui::PushStyleColor(ImGuiCol_Text, cs);
  draw();
  if (cs)
    ImGui::PopStyleColor();
}
void Dash() { ImGui::TextColored(COLOR_GRAY, "-"); }

const Column kCols[] = {
    {"Code", "Code",
     "证券代码 (Code)\n股票的唯一标识符\n格式:6位数字(如600000、000001、688001)",
     Kind::Text, Good::None, nullptr, nullptr,
     [](const Row &r) -> std::string_view { return r.a.asset_code; }, nullptr},

    {"Name", "Name",
     "股票名称 (Name)\n公司在交易所的简称 (逐日 PIT)\n来源: cn_stock_instruments 最新交易日\n\n退市股回落到 cn_stock_basic_info 的最后简称",
     Kind::Text, Good::None, nullptr, nullptr,
     [](const Row &r) -> std::string_view {
       if (r.info && !r.info->name.empty())
         return r.info->name;
       return "-";
     },
     nullptr},

    {"Exch", "Exchange",
     "交易所 (Exchange)\nSH = 上海证券交易所 (Shanghai Stock Exchange)\nSZ = 深圳证券交易所 (Shenzhen Stock Exchange)\nBJ = 北京证券交易所 (Beijing Stock Exchange)",
     Kind::Category, Good::None, nullptr, nullptr,
     [](const Row &r) -> std::string_view { return r.a.exchange; },
     [](const Row &r, double, ImU32) {
       ImGui::TextColored(r.a.exchange == "SH" ? COLOR_SH : r.a.exchange == "SZ" ? COLOR_SZ
                                                                                 : COLOR_BJ,
                          "%s", r.a.exchange.c_str());
     }},

    {"Board", "Board",
     "板块 (Board)\n市场分类:\n- 沪市主板 (600/601/603/605)\n- 深市主板 (000/001/002/003/004)\n- 科创板 (688/689)\n- 创业板 (300/301/302/309)\n- 北交所 (43/83/87/88/92)",
     Kind::Category, Good::None, nullptr,
     [](const Row &r) { return static_cast<double>(GetBoardType(r.a.asset_code)); },
     [](const Row &r) -> std::string_view { return GetBoardName(GetBoardType(r.a.asset_code)); }, nullptr},

    {"ST", "ST",
     "ST股 (Special Treatment)\n来源: cn_stock_status.st_status (逐日)\n\nST  = 特别处理 (连续两年亏损等), 涨跌幅限制 ±5%\n*ST = 退市风险警示 (风险更高一级)\n-   = 正常\n\n与 Name 列同源同日 (简称前缀 ↔ 本列取值 严格一致)",
     Kind::Category, Good::None, nullptr,
     [](const Row &r) { return static_cast<double>(GetStLevel(r.info)); }, // 正常 < ST < *ST
     [](const Row &r) -> std::string_view {
       const int level = GetStLevel(r.info);
       return level == 2 ? "*ST" : level == 1 ? "ST"
                                              : "正常";
     },
     [](const Row &, double v, ImU32) {
       if (v > 0)
         ImGui::TextColored(COLOR_RED, "%s", v == 2 ? "*ST" : "ST");
       else
         ImGui::TextUnformatted("-");
     }},

    {"DL", "DL",
     "退市 (Delisted)\n是否已退市或处于退市状态\nDL = 已退市\nhover 单元格可看退市日期",
     Kind::Category, Good::None, nullptr,
     [](const Row &r) { return r.info && r.info->out_date ? 1.0 : 0.0; },
     [](const Row &r) -> std::string_view { return r.info && r.info->out_date ? "退市" : "在市"; },
     [](const Row &r, double v, ImU32) {
       if (!(v > 0)) {
         ImGui::TextUnformatted("-");
         return;
       }
       ImGui::TextColored(COLOR_GRAY, "DL");
       if (ImGui::IsItemHovered())
         ImGui::SetTooltip("Delisted: %s", misc::dash(r.info->out_date).c_str());
     }},

    {"Listed", "Listed Days (在市总天数)",
     "在市时间 (Listed Duration)\n紧凑格式 年/月/日 — 如 11/02/06 = 11年2个月6天\n\n区间: 上市日 → 退市日 (退市股截到退市日) 或今天\nhover 单元格可看起止日期与在市总天数\n排序与横截面分析口径为在市总天数",
     Kind::Number, Good::High, nullptr,
     [](const Row &r) {
       const ListedSpan span = r.info ? CalculateListedSpan(*r.info) : ListedSpan{};
       return span.valid ? static_cast<double>(span.total_days) : kNaN;
     },
     nullptr,
     [](const Row &r, double v, ImU32 cs) {
       if (std::isnan(v))
         return Dash();
       const ListedSpan span = CalculateListedSpan(*r.info);
       Tinted(cs, [&] { ImGui::Text("%02d/%02d/%02d", span.years, span.months, span.days); });
       if (ImGui::IsItemHovered())
         ImGui::SetTooltip("上市 %s → %s\n在市 %d 年 %d 月 %d 天 (共 %d 天)",
                           misc::dash(r.info->ipo_date).c_str(),
                           r.info->out_date ? misc::dash(r.info->out_date).c_str() : "至今",
                           span.years, span.months, span.days, span.total_days);
     }},

    {"Ind", "Industry",
     "行业 (Industry)\n申万一级行业名称 (industry_level1_name)\n来源: cn_stock_industry_component 最新月度快照\n\nhover 单元格可看行业代码 (ind_code)",
     Kind::Category, Good::None, nullptr, nullptr, // 按展示名排, 与列内容一致
     [](const Row &r) -> std::string_view {
       if (r.info && !GetIndustryDisplay(*r.info).empty())
         return GetIndustryDisplay(*r.info);
       return "-";
     },
     [](const Row &r, double, ImU32) {
       if (!r.info || GetIndustryDisplay(*r.info).empty())
         return Dash();
       ImGui::TextUnformatted(GetIndustryDisplay(*r.info).c_str());
       if (ImGui::IsItemHovered() && !r.info->ind_code.empty())
         ImGui::SetTooltip("%s", r.info->ind_code.c_str());
     }},

    {"PE", "PE(TTM)",
     "滚动市盈率 (PE TTM)\npeTTM = Trailing Twelve Months P/E Ratio\n= 股票收盘价 / 每股盈余TTM\n= (收盘价 x 总股本) / 归属母公司股东净利润TTM\n\nTTM = 过去12个月滚动数据\n反映公司盈利能力,数值越低估值越便宜",
     Kind::Number, Good::Cheap, "%.1f", &Field<&StockInfo::pe_ttm>, nullptr, nullptr},

    {"PB", "PB(MRQ)",
     "市净率 (PB MRQ)\npbMRQ = Price-to-Book Ratio (Most Recent Quarter)\n= 总市值 / 归属母公司股东权益MRQ\n= (收盘价 x 总股本) / total_equity_to_parent_shareholders\n\nMRQ = 最新报告期 (max report_date 的最新可见行)\n反映账面价值,通常>1,<1可能破净",
     Kind::Number, Good::Cheap, "%.2f", &Field<&StockInfo::pb_mrq>, nullptr, nullptr},

    {"PS", "PS(TTM)",
     "滚动市销率 (PS TTM)\npsTTM = Price-to-Sales Ratio (TTM)\n= 股票收盘价 / 每股销售额\n= (收盘价 x 总股本) / 营业总收入TTM\n\n反映每单位营收对应的市值\n适用于尚未盈利但有营收的公司",
     Kind::Number, Good::Cheap, "%.2f", &Field<&StockInfo::ps_ttm>, nullptr, nullptr},

    {"PCF", "PCF",
     "滚动市现率 (PCF TTM)\npcfNcfTTM = Price-to-Cash-Flow Ratio (TTM)\n= 总市值 / 经营活动现金流量净额TTM\n= (收盘价 x 总股本) / net_cffoa_ttm\n\n注意分母是经营现金流, 不是现金及现金等价物净增加额\n烧钱(经营现金流为负) 保留负值, 不置空\n\n反映现金流创造能力\n比PE更难以通过会计手段操纵",
     Kind::Number, Good::Cheap, "%.1f", &Field<&StockInfo::pcf_ttm>, nullptr, nullptr},

    {"DY1", "DY 近1年 (年化 %)",
     "股息率 近1年 (Dividend Yield, 年化)\n= 近365日税前分红总额 / 总市值 x 100%\n\n说明:\n- 单位: %\n- 窗口以分红公告日 (publish_date) 锚定, 非除权日\n- 口径与 L_1min 特征 dy_raw 一致\n- 0.00 = 窗口内确实没有分红公告 (非缺失)",
     Kind::Number, Good::High, "%.2f", &Field<&StockInfo::dy1y>, nullptr, nullptr},

    {"DY3", "DY 近3年 (年化 %)",
     "股息率 近3年 (年化平均)\n= 近3年(1095日)税前分红总额 / 年数 / 总市值 x 100%\n\n说明:\n- 已年化, 可与 DY1 / DY5 直接横向比较\n- 上市不足3年的按实际上市年数年化, 不被系统性摊薄\n- 上市不足一个季度 → 留空 (年化无意义)\n- DY3 明显低于 DY1 = 近年才开始分红",
     Kind::Number, Good::High, "%.2f", &Field<&StockInfo::dy3y>, nullptr, nullptr},

    {"DY5", "DY 近5年 (年化 %)",
     "股息率 近5年 (年化平均)\n= 近5年(1825日)税前分红总额 / 年数 / 总市值 x 100%\n\n说明:\n- 已年化, 可与 DY1 / DY3 直接横向比较\n- 上市不足5年的按实际上市年数年化\n- DY1 ≈ DY3 ≈ DY5 = 长期稳定分红",
     Kind::Number, Good::High, "%.2f", &Field<&StockInfo::dy5y>, nullptr, nullptr},

    {"Cap", "Market Cap (亿元)",
     "总市值 (Market Cap)\n= 收盘价 x 总股本 / 1亿\n= close x total_shares (不复权真价)\n\n说明:\n- 单位: 亿元\n- 与 PE/PB/PS/PCF/DY 的分子同源\n- 数据来自 cn_stock_real_bar1d.close + cn_stock_shares.total_shares",
     Kind::Number, Good::High, "%.1f",
     [](const Row &r) { return r.info && r.info->mcap > 0 ? static_cast<double>(r.info->mcap) : kNaN; }, // 0 / NaN = 缺收盘价或股本, 不是"市值为零"
     nullptr, nullptr},

    {"Days", "Trading Days",
     "交易日数 (Trading Days)\n该股票在数据库中有数据的总交易日数\n= 帧目录中已编码的天数\n可用于判断数据完整性",
     Kind::Number, Good::None, "%.0f",
     [](const Row &r) { return static_cast<double>(r.s.total_days); }, nullptr, nullptr},

    {"Orders/Day", "Avg Orders / Day",
     "日均逐笔数 (Average Orders per Trading Day)\n所有已有逐笔数据交易日的平均记录数量 (委托+成交合并后)\n= Σ order_count / 有逐笔数据的交易日数\n\n说明:\n- 单位:条记录/日\n- 显示格式:>1M用M(百万), >1K用K(千)\n- 条数取自日文件目录, 扫描时一并读到",
     Kind::Number, Good::None, nullptr,
     [](const Row &r) {
       return r.s.total_days > 0 ? static_cast<double>(r.s.total_orders) / static_cast<double>(r.s.total_days) : kNaN;
     },
     nullptr,
     [](const Row &, double v, ImU32) {
       if (std::isnan(v))
         Dash();
       else if (v > 1000000.0)
         ImGui::Text("%.2fM", v / 1000000.0);
       else if (v > 1000.0)
         ImGui::Text("%.1fK", v / 1000.0);
       else
         ImGui::Text("%.0f", v);
     }},

    // expected_days == 0 = 该资产在回测区间内本就不该有数据 (北交所 / 尚未上市 / 已退市), 那不是缺口, 留空而不是 0%
    {"Orders%", "Orders % (回测区间完整性)",
     "逐笔完整性 (Orders Coverage)\n回测区间内已编码的交易日占比\n= (应有天数 - 缺失天数) / 应有天数\n\n分母是该资产\"本该有逐笔\"的交易日:\n已上市未退市, 且排除当日全天停牌\n(北交所不在 L2 覆盖范围, 整体留空)\n\n与 Dates 页的完整性、Encode 页的缺失表同源\nhover 单元格可看缺失天数",
     Kind::Number, Good::None, nullptr,
     [](const Row &r) { return r.s.expected_days > 0 ? static_cast<double>(r.s.orders_coverage_percent()) : kNaN; },
     nullptr,
     [](const Row &r, double v, ImU32) {
       if (std::isnan(v))
         return Dash();
       const ImVec4 color = v >= 99.9   ? ImVec4(0.3f, 0.95f, 0.4f, 1.0f)
                            : v >= 95.0 ? ImVec4(1.0f, 1.0f, 0.0f, 1.0f)
                                        : ImVec4(1.0f, 0.5f, 0.0f, 1.0f);
       ImGui::TextColored(color, "%.1f%%", v);
       if (ImGui::IsItemHovered())
         ImGui::SetTooltip("Missing %zu of %zu trading days", r.s.orders_missing, r.s.expected_days);
     }},
};
constexpr int kNumCols = static_cast<int>(std::size(kCols));

void DrawCell(const Column &c, const Row &r, ImU32 cs) {
  const double v = c.num ? c.num(r) : kNaN;
  if (c.cell)
    return c.cell(r, v, cs);
  if (c.kind == Kind::Number) {
    assert(c.num && c.fmt);
    if (std::isnan(v))
      return Dash();
    return Tinted(cs, [&] { ImGui::Text(c.fmt, v); });
  }
  const std::string_view t = c.text(r);
  if (t == "-")
    return Dash();
  ImGui::TextUnformatted(t.data(), t.data() + t.size());
}

// ============================================================================
// 视图缓存: 过滤 + 排序 + 截面色
// ============================================================================

// 截面着色: 过滤后 pool 内的名次分位 → 红 → 黄 → 绿.
// 用名次分位而不是数值归一化: 估值和市值都是重尾分布, 按 (v-min)/(max-min)
// 染色的话几个极端值就把其余几千个资产全压成同一个颜色.
// v = [rows 位置] 该列取值 (NaN = 不参与: 缺值的行不能挤占最差那一位)
void CrossSectionColors(Good g, const std::vector<double> &v, std::vector<uint32_t> &colors) {
  colors.assign(v.size(), 0);
  std::vector<size_t> keys; // 有值的 rows 位置, 排完序 keys[0] 最好
  for (size_t pos = 0; pos < v.size(); ++pos)
    if (!std::isnan(v[pos]))
      keys.push_back(pos);
  std::sort(keys.begin(), keys.end(), [&](size_t a, size_t b) { return Better(g, v[a], v[b]); });

  // 并列同值取平均名次 —— 否则 DY 里成片的 0.00 会被摊成一整条渐变,
  // 看着像有高低之分
  const size_t m = keys.size();
  for (size_t i = 0; i < m;) {
    size_t j = i + 1;
    while (j < m && v[keys[j]] == v[keys[i]])
      ++j;
    const float rank = 0.5f * static_cast<float>(i + j - 1);
    const float p = (m > 1) ? 1.0f - rank / static_cast<float>(m - 1) : 0.5f;
    const ImU32 color = RampColor(p);
    for (size_t k = i; k < j; ++k)
      colors[keys[k]] = color;
    i = j;
  }
}

void RebuildView(AssetsView &view, const Asset &asset, const StockInfoMap &stock_info, const AssetsViewKey &key) {
  const size_t count = asset.items.size();
  assert(asset.asset_stats.size() == count);

  // StockInfo 指针只在本次重建期间用 (基本面整体重载会让它们失效, 所以
  // 不进 view). 每资产一次 map 查找, 排序 / 着色都直接吃这份.
  std::vector<const StockInfo *> infos(count, nullptr);
  const auto row = [&](size_t id) { return Row{asset.items[id], infos[id], asset.asset_stats[id]}; };

  view.rows.clear();
  for (size_t id = 0; id < count; ++id) {
    infos[id] = FindStockInfo(asset.items[id], stock_info);
    if (ShouldShowAsset(asset.items[id], infos[id], key.filter))
      view.rows.push_back(id);
  }

  // 严格弱序: 降序 = 交换实参, 不是对结果取反 (取反在相等时会同时声称 a<b 与 b<a, 那是 UB); NaN 两个方向都垫底
  if (key.sort_column >= 0) {
    assert(key.sort_column < kNumCols);
    const Column &c = kCols[key.sort_column];
    const bool up = key.sort_ascending;
    if (c.num) {
      std::vector<double> v(count, kNaN);
      for (const size_t id : view.rows)
        v[id] = c.num(row(id));
      std::stable_sort(view.rows.begin(), view.rows.end(), [&](size_t ia, size_t ib) {
        const double a = v[ia], b = v[ib];
        if (std::isnan(a) || std::isnan(b))
          return !std::isnan(a) && std::isnan(b);
        return up ? Ascends(c.good, a, b) : Ascends(c.good, b, a);
      });
    } else {
      std::vector<std::string_view> t(count);
      for (const size_t id : view.rows)
        t[id] = c.text(row(id));
      std::stable_sort(view.rows.begin(), view.rows.end(), [&](size_t ia, size_t ib) { return up ? t[ia] < t[ib] : t[ib] < t[ia]; });
    }
  }

  view.cs.assign(kNumCols, {});
  std::vector<double> v(view.rows.size());
  for (int ci = 0; ci < kNumCols; ++ci) {
    const Column &c = kCols[ci];
    if (c.good == Good::None)
      continue;
    for (size_t pos = 0; pos < view.rows.size(); ++pos)
      v[pos] = c.num(row(view.rows[pos]));
    CrossSectionColors(c.good, v, view.cs[ci]);
  }

  view.key = key;
  view.built = true;
}

// 视图过期就重建. 表格与横截面面板都先调它, 之后共用 state.view.
void SyncView(AssetsState &state, const Asset &asset, const StockInfoMap &stock_info) {
  const AssetsViewKey key{asset.generation, asset.items.size(), stock_info.size(), state.sort_column, state.sort_ascending, state.filter};
  if (!state.view.built || !(state.view.key == key))
    RebuildView(state.view, asset, stock_info, key);
}

// ============================================================================
// Helper: Render filter bar
// ============================================================================

void RenderFilterBar(
    AssetsState &state,
    const std::vector<AssetItem> &assets,
    const StockInfoMap &stock_info) {

  ImGui::SetNextItemWidth(250.0f);
  if (ImGui::InputTextWithHint("##Search", "Search code/name...", state.search_buf.data(), state.search_buf.size()))
    state.filter.search = state.search_buf.data();

  // ST/Listed/Board: 与 FEATURES/ORDERFLOW 共用控件 (gui/util/AssetFilter.hpp).
  // ST 只有 0..2 (GetStLevel 口径, 无退市整理期一级); Listed 看 out_date 是否为 0
  ImGui::SameLine();
  GUI::Filter::CommonFilters<BoardType>("Assets", false, state.filter.st, state.filter.listed, state.filter.board);

  // 行业维自绘: 这里的键是字符串行业码 ind_code, 与 ORDERFLOW 的数字 SW2021 ID
  // 不同源, 且选项要从当前 stock_info 动态扫出来
  if (state.industries_of != stock_info.size()) {
    std::map<std::string, std::string> ind_map; // code -> name
    for (const AssetItem &asset : assets)
      if (const StockInfo *info = FindStockInfo(asset, stock_info); info && !info->ind_code.empty())
        ind_map[info->ind_code] = info->ind_name;
    state.industries.clear();
    for (const auto &[code, name] : ind_map)
      state.industries.emplace_back(code, name.empty() ? code : name);
    state.industries_of = stock_info.size();
  }
  ImGui::SameLine();
  GUI::Filter::MultiSelectCombo("Industry##IndFilter", 150.0f, state.industries, state.filter.industry);

  // Count & panel toggle
  ImGui::SameLine();
  ImGui::Text("Showing:%zu/%zu", state.view.rows.size(), assets.size());

  ImGui::SameLine();
  ImGui::BeginGroup();
  ImGui::TextColored(RampColorVec4(1.0f), "■");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(RampColorVec4(0.5f), "■");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(RampColorVec4(0.0f), "■");
  ImGui::EndGroup();
  if (ImGui::IsItemHovered()) {
    ImGui::SetTooltip(
        "截面色 (Listed / PE / PB / PS / PCF / DY1 / DY3 / DY5 / Cap)\n"
        "绿 = 好, 红 = 差; 取的是当前 filter 后 pool 内的名次分位\n"
        "  Listed: 在市越久越绿\n"
        "  PE/PB/PS/PCF: 越低越绿 (亏损的负值排在最差一端)\n"
        "  DY1/DY3/DY5: 分红越多越绿\n"
        "  Cap: 市值越大越绿\n\n"
        "用名次分位而非数值归一化: 估值/市值是重尾分布,\n"
        "按 min-max 染色会被几个极端值压成一片同色");
  }

  ImGui::SameLine();
  if (ImGui::SmallButton(state.show_right_panel ? "Hide" : "Show"))
    state.show_right_panel = !state.show_right_panel;
}

// ============================================================================
// Helper: Render data table
// ============================================================================

void RenderDataTable(
    const Asset &asset_data,
    const StockInfoMap &stock_info,
    AssetsState &assets_state) {

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

  // 排序规则的变化由 ImGui 的 SpecsDirty 告知; 真正的重排在 RebuildView 里,
  // 只在规则/过滤/数据代数变化时做一次 (见 AssetsView).
  if (ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs()) {
    if (sort_specs->SpecsDirty && sort_specs->SpecsCount > 0) {
      const auto &spec = sort_specs->Specs[0];
      assets_state.sort_column = spec.ColumnIndex;
      assets_state.sort_ascending = spec.SortDirection == ImGuiSortDirection_Ascending;
    }
    sort_specs->SpecsDirty = false;
  }

  SyncView(assets_state, asset_data, stock_info);
  const AssetsView &view = assets_state.view;
  const int hovered_col = ImGui::TableGetHoveredColumn();

  for (size_t pos = 0; pos < view.rows.size(); ++pos) {
    const size_t id = view.rows[pos];
    // StockInfo 每帧现查 (基本面可能整体重载, 缓存指针会失效)
    const Row r{asset_data.items[id], FindStockInfo(asset_data.items[id], stock_info), asset_data.asset_stats[id]};
    ImGui::TableNextRow();
    if (assets_state.selected_asset == static_cast<int>(id))
      ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, ImGui::GetColorU32(COLOR_SELECTED));
    for (int ci = 0; ci < kNumCols; ++ci) {
      ImGui::TableSetColumnIndex(ci);
      if (hovered_col == ci)
        ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(COLOR_HOVER_COL));
      DrawCell(kCols[ci], r, view.cs[ci].empty() ? 0 : view.cs[ci][pos]);
    }
  }

  // 左键点行内任意处 (格子的文字 / 内边距 / 边框都算): 选中该行 (Quotes 看它); Statistics 页再切截面分析的列.
  // 行 = 表的行背景带 (上下相接无缝, 行 0 = 表头); 列 = 列区 (左右相接无缝); 滚动条不算
  const int hit = ImGui::TableGetHoveredRow() - 1;
  const ImRect inner = ImGui::GetCurrentWindow()->InnerClipRect;
  if (ImGui::IsMouseClicked(ImGuiMouseButton_Left) && ImGui::IsWindowHovered() && inner.Contains(ImGui::GetMousePos()) &&
      hit >= 0 && static_cast<size_t>(hit) < view.rows.size() && hovered_col >= 0 && hovered_col < kNumCols) {
    assets_state.selected_asset = static_cast<int>(view.rows[static_cast<size_t>(hit)]);
    assets_state.show_right_panel = true;
    if (!assets_state.quotes_page)
      assets_state.selected_column_idx = assets_state.selected_column_idx == hovered_col ? -1 : hovered_col;
  }

  ImGui::EndTable();
}

// ============================================================================
// Numeric Column Analysis
// ============================================================================

void RenderNumericAnalysis(
    const Asset &asset_data,
    const StockInfoMap &stock_info,
    const AssetsState &assets_state,
    const Column &col) {

  // 过滤结果直接用表格的缓存视图 (见 AssetsView); 只取有值的行
  std::vector<std::string> names;
  std::vector<float> values;
  std::vector<std::string> codes;
  for (const size_t id : assets_state.view.rows) {
    const Row r{asset_data.items[id], FindStockInfo(asset_data.items[id], stock_info), asset_data.asset_stats[id]};
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

  // === 1. Board Statistics Table (Compact) ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Board Statistics");
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

  // === 2. Distribution Plot (Remove top/bottom 5% outliers) ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Distribution (Outliers Removed)");
  auto filtered_values = RemoveOutliers(values, 5.0);

  if (!filtered_values.empty()) {
    const int num_bins = 100;

    // Calculate statistics for Gaussian fit
    auto minmax = std::minmax_element(filtered_values.begin(), filtered_values.end());
    float min_val = *minmax.first;
    float max_val = *minmax.second;
    float range = max_val - min_val;
    float bin_width = range / num_bins;

    float sum = std::accumulate(filtered_values.begin(), filtered_values.end(), 0.0);
    float mean = sum / filtered_values.size();

    float sq_sum = 0.0;
    for (float v : filtered_values) {
      sq_sum += (v - mean) * (v - mean);
    }
    float std_dev = std::sqrt(sq_sum / filtered_values.size());

    // Calculate histogram bins
    std::vector<float> hist_bins(num_bins, 0.0);
    for (float v : filtered_values) {
      int bin_idx = static_cast<int>((v - min_val) / bin_width);
      if (bin_idx >= num_bins)
        bin_idx = num_bins - 1;
      if (bin_idx < 0)
        bin_idx = 0;
      hist_bins[bin_idx] += 1.0;
    }

    // Normalize histogram to density
    float n = filtered_values.size();
    std::vector<float> hist_density(num_bins);
    for (int i = 0; i < num_bins; ++i) {
      hist_density[i] = hist_bins[i] / (n * bin_width);
    }

    // Prepare X axis positions for histogram
    std::vector<float> x_positions(num_bins);
    for (int i = 0; i < num_bins; ++i) {
      x_positions[i] = min_val + (i + 0.5) * bin_width;
    }

    // Generate fitted Gaussian PDF (200 points for smooth curve)
    const int pdf_points = 200;
    std::vector<float> pdf_x(pdf_points);
    std::vector<float> pdf_y(pdf_points);
    const float pi = 3.14159265358979323846;

    for (int i = 0; i < pdf_points; ++i) {
      float x = min_val + (i * range) / (pdf_points - 1);
      pdf_x[i] = x;
      float z = (x - mean) / std_dev;
      pdf_y[i] = (1.0 / (std_dev * std::sqrt(2.0 * pi))) * std::exp(-0.5 * z * z);
    }

    // Calculate empirical CDF
    std::vector<float> sorted_vals = filtered_values;
    std::sort(sorted_vals.begin(), sorted_vals.end());
    std::vector<float> cdf_x(pdf_points);
    std::vector<float> cdf_y(pdf_points);

    for (int i = 0; i < pdf_points; ++i) {
      float x = min_val + (i * range) / (pdf_points - 1);
      cdf_x[i] = x;
      auto it = std::upper_bound(sorted_vals.begin(), sorted_vals.end(), x);
      cdf_y[i] = (float)std::distance(sorted_vals.begin(), it) / sorted_vals.size();
    }

    // Find max density for Y axis
    float max_hist_density = *std::max_element(hist_density.begin(), hist_density.end());
    float max_pdf = *std::max_element(pdf_y.begin(), pdf_y.end());
    float y_max = std::max(max_hist_density, max_pdf) * 1.15;

    if (ImPlot::BeginPlot("##Distribution", ImVec2(-1, 350))) {
      ImPlot::SetupAxes("Value", "Density");
      ImPlot::SetupAxisLimits(ImAxis_X1, min_val, max_val, ImPlotCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, y_max, ImPlotCond_Always);
      ImPlot::SetupAxis(ImAxis_Y2, "CDF", ImPlotAxisFlags_AuxDefault);
      ImPlot::SetupAxisLimits(ImAxis_Y2, 0, 1.05, ImPlotCond_Always);

      // Plot histogram bars
      ImPlot::SetNextFillStyle(ImVec4(0.5f, 0.7f, 1.0f, 0.4f));
      ImPlot::PlotBars("Histogram", x_positions.data(), hist_density.data(), num_bins, bin_width * 0.9);

      // Plot fitted Gaussian PDF
      ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), 2.5f);
      ImPlot::PlotLine("Fitted PDF", pdf_x.data(), pdf_y.data(), pdf_points);

      // Plot empirical CDF on secondary Y axis
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
      ImPlot::SetNextLineStyle(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), 2.5f);
      ImPlot::PlotLine("Empirical CDF", cdf_x.data(), cdf_y.data(), pdf_points);

      ImPlot::EndPlot();
    }
  }

  ImGui::Spacing();

  // === 3. Rankings (Top 10 / Bottom 10) ===
  float half_width = ImGui::GetContentRegionAvail().x * 0.48f;

  // Top 10
  ImGui::BeginChild("Top10", ImVec2(half_width, 250), true);
  ImGui::TextColored(ImVec4(0.4f, 1.0f, 0.4f, 1.0f), "Top 10");
  auto top10 = GetTopN(names, values, 10, true);
  for (size_t i = 0; i < top10.size(); ++i) {
    ImGui::Text("%zu. %s: %.2f", i + 1, top10[i].first.c_str(), top10[i].second);
  }
  ImGui::EndChild();

  ImGui::SameLine();

  // Bottom 10
  ImGui::BeginChild("Bottom10", ImVec2(half_width, 250), true);
  ImGui::TextColored(ImVec4(1.0f, 0.4f, 0.4f, 1.0f), "Bottom 10");
  auto bottom10 = GetTopN(names, values, 10, false);
  for (size_t i = 0; i < bottom10.size(); ++i) {
    ImGui::Text("%zu. %s: %.2f", i + 1, bottom10[i].first.c_str(), bottom10[i].second);
  }
  ImGui::EndChild();
}

// ============================================================================
// Categorical Column Analysis
// ============================================================================

void RenderCategoricalAnalysis(
    const Asset &asset_data,
    const StockInfoMap &stock_info,
    const AssetsState &assets_state,
    const Column &col) {

  // 过滤结果直接用表格的缓存视图 (见 AssetsView)
  std::vector<std::string> categories;
  std::vector<std::string> codes;
  for (const size_t id : assets_state.view.rows) {
    const Row r{asset_data.items[id], FindStockInfo(asset_data.items[id], stock_info), asset_data.asset_stats[id]};
    categories.emplace_back(col.text(r));
    codes.push_back(r.a.asset_code);
  }

  if (categories.empty()) {
    ImGui::Text("No valid data");
    return;
  }

  // === 1. Overall Pie Chart ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Overall Distribution");
  auto overall_counts = CountCategories(categories);

  if (!overall_counts.empty() && ImPlot::BeginPlot("##OverallPie", ImVec2(-1, 250))) {
    std::vector<const char *> labels;
    std::vector<float> counts;
    for (const auto &cc : overall_counts) {
      labels.push_back(cc.label.c_str());
      counts.push_back((float)cc.count);
    }
    ImPlot::PlotPieChart(labels.data(), counts.data(), (int)counts.size(), 0.5, 0.5, 0.4);
    ImPlot::EndPlot();
  }

  ImGui::Spacing();

  // === 2. Board Breakdown Pie Charts (Multi-column) ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Board Breakdown");
  auto board_breakdown = GroupCategoricalByBoard(codes, categories);

  int charts_per_row = 2;
  float chart_width = ImGui::GetContentRegionAvail().x / charts_per_row - 10;

  for (size_t i = 0; i < board_breakdown.size(); ++i) {
    const auto &breakdown = board_breakdown[i];

    if (i % charts_per_row != 0) {
      ImGui::SameLine();
    }

    ImGui::BeginChild(("BoardPie_" + std::to_string(i)).c_str(), ImVec2(chart_width, 220), true);
    ImGui::Text("%s", breakdown.board_name.c_str());

    if (!breakdown.categories.empty() && ImPlot::BeginPlot("##BoardPie", ImVec2(-1, 180))) {
      std::vector<const char *> labels;
      std::vector<float> counts;
      for (const auto &cc : breakdown.categories) {
        labels.push_back(cc.label.c_str());
        counts.push_back((float)cc.count);
      }
      ImPlot::PlotPieChart(labels.data(), counts.data(), (int)counts.size(), 0.5, 0.5, 0.35);
      ImPlot::EndPlot();
    }

    ImGui::EndChild();
  }
}

// ============================================================================
// Helper: Render cross-section analysis panel
// ============================================================================

void RenderCrossSectionPanel(
    const Asset &asset_data,
    const StockInfoMap &stock_info,
    const AssetsState &assets_state) {

  const int ci = assets_state.selected_column_idx;
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
    RenderCategoricalAnalysis(asset_data, stock_info, assets_state, col);
    break;
  case Kind::Number:
    RenderNumericAnalysis(asset_data, stock_info, assets_state, col);
    break;
  }
}

} // namespace

// ============================================================================
// Main TabAssets Render Function
// ============================================================================

void RenderTabAssets(
    Asset &asset,
    const StockInfoMap &stock_info,
    AssetsState &assets_state,
    Market &market) {

  const std::vector<AssetItem> &assets = asset.items;

  // 每资产统计由扫描末尾一次算好 (见 ScanService Phase 5), 这里只是等它到位
  if (asset.asset_stats.size() != assets.size()) {
    ImGui::TextDisabled("Waiting for database scan...");
    return;
  }

  // 过滤 + 排序结果的缓存, 顺便给下面的 Showing:x/y 供数
  SyncView(assets_state, asset, stock_info);

  // Get window dimensions
  float window_width = ImGui::GetContentRegionAvail().x;
  float window_height = ImGui::GetContentRegionAvail().y;

  // Calculate left table width
  float left_width = assets_state.show_right_panel ? window_width * assets_state.table_split_ratio : window_width;

  // Left: 过滤条 + 数据表
  ImGui::BeginChild("LeftTable", ImVec2(left_width, window_height), true,
                    ImGuiWindowFlags_HorizontalScrollbar);
  RenderFilterBar(assets_state, assets, stock_info);
  ImGui::Spacing();
  RenderDataTable(asset, stock_info, assets_state);
  ImGui::EndChild();

  // Right: Quotes / Statistics
  if (assets_state.show_right_panel) {
    ImGui::SameLine();
    ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(4, 4));
    ImGui::BeginChild("RightPanel", ImVec2(0, window_height), true);
    ImGui::PopStyleVar();
    if (ImGui::BeginTabBar("##RightPanel")) {
      if (ImGui::BeginTabItem("Quotes")) {
        assets_state.quotes_page = true;
        // 键 = asset_id (== 全局 id); 未选 = 空键集
        const uint32_t id = static_cast<uint32_t>(assets_state.selected_asset);
        Sync(market, kStock, assets_state.selected_asset >= 0 ? std::span<const uint32_t>(&id, 1) : std::span<const uint32_t>(),
             assets_state.stock.feed);
        const Feed &f = assets_state.stock.feed;
        std::string title;
        if (f.data && !f.data->keys.empty()) {
          assert(f.data->keys.front() < assets.size());
          const AssetItem &a = assets[f.data->keys.front()];
          const StockInfo *info = FindStockInfo(a, stock_info);
          title = (info && !info->name.empty() ? info->name : std::string("-")) + "  " + a.code_ex();
        }
        RenderStockPanel(market, assets_state.stock, title);
        ImGui::EndTabItem();
      }
      if (ImGui::BeginTabItem("Statistics")) {
        assets_state.quotes_page = false;
        RenderCrossSectionPanel(asset, stock_info, assets_state);
        ImGui::EndTabItem();
      }
      ImGui::EndTabBar();
    }
    ImGui::EndChild();
  }
}

} // namespace GUI::Database
