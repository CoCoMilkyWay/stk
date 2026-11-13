// Tab Asset - 资产表 + 右栏 Quotes (选中标的行情) / Statistics (选中列的截面分析)
//   列定义一张表 (kCols): 列 (表头 + 说明) / 取值 / 文本 / 格 / 悬停; 格 / 排序 / 名次底色 / 截面分析都只读它

#include "gui/task_market/ui/TabAsset.hpp"
#include "gui/task_market/ui/CrossSectionAnalysis.hpp"
#include "gui/util/AssetFilter.hpp" // 筛选控件 (与 OrderFlow 共用)
#include "gui/util/Board.hpp"
#include "gui/util/Chart.hpp" // tint / band_col / rank_by / sort_less
#include "gui/util/Sheet.hpp" // SheetCol / SheetCell / sheet_head / draw_cell
#include "gui/util/Tip.hpp"
#include "imgui.h"
#include "imgui_internal.h" // TableGetHoveredRow / TableGetRowIndex
#include "implot.h"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
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
constexpr ImVec4 kDimCol(0.6f, 0.6f, 0.6f, 1.0f);                                                                // 退市标
constexpr ImVec4 kHoverCol(0.3f, 0.3f, 0.4f, 0.3f);                                                              // 悬停列的格底 (Statistics 页)
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

Row make_row(const Asset &asset, const StockInfos &stock_info, const Joint &quotes, size_t id) {
  const AssetItem &a = asset.items[id];
  return {a, find_info(a, stock_info), asset.asset_stats[id], RecentOf(quotes.of(static_cast<uint32_t>(a.asset_id)))};
}

// 文本 (只排序, 左对齐) / 分类 (饼图, 左对齐) / 数值 (分布 + 排名, 右对齐)
enum class Kind { Text,
                  Category,
                  Number };
// 一列: num = 排序 / 名次 / 统计同一个键 (数值升序; NaN = 无可比值, 排序恒末, 不铺底色); 无 num = 按格文本排;
//   good = 名次底色口径 (GUI::rank_by: +1 = 高为好, −1 = 低为好, 0 = 不铺)
struct Column {
  SheetCol col; // 表头 / 说明 (首行 = Statistics 标题)
  Kind kind;
  int good;
  const char *fmt;                        // Number 的默认格式
  double (*num)(const Row &);             // 键
  std::string_view (*text)(const Row &);  // Text / Category 的文本 ("-" = 无)
  NumCell (*cell)(const Row &, double v); // 格文本 (v = num); 空 = Number 按 fmt, 其余按 text
  void (*hover)(const Row &);             // 悬停格的补充; 空 = 无
  bool quote = false;                     // 行情列: 只读近况 (随快照 / 价格库发布重填)
};

template <float StockInfo::*F>
double field(const Row &r) { return r.info ? r.info->*F : kNaN; }

const ImVec4 &ink() { return ImGui::GetStyleColorVec4(ImGuiCol_Text); }
// 近况格: v [%], 按涨跌着色
NumCell pct_cell(const Row &, double v) { return std::isnan(v) ? num_none() : num_cell(tint(v, 0.0), "%+.2f%%", v); }
// 估值列 (PE / PB / PS / PCF) 的键 = 倍数 F 的倒数 (收益率 %): 便宜 → 高, 亏损为负 (越亏越低); 倍数 0 / 无 → NaN.
//   升序 = 亏得多 → 亏得少 → 贵 → 便宜, 与 DY 列同向 (高为好)
template <float StockInfo::*F>
double yield(const Row &r) {
  const float m = r.info ? r.info->*F : 0.f;
  return m != 0.f && std::isfinite(m) ? 100.0 / m : kNaN;
}
// 估值格: 照画倍数 (P 位小数)
template <float StockInfo::*F, int P>
NumCell multiple_cell(const Row &r, double v) { return std::isnan(v) ? num_none() : num_cell(ink(), "%.*f", P, static_cast<double>(r.info->*F)); }

const Column kCols[] = {
    {{"代码", "代码 = 证券代码\n"
              "• 算法: 6 位数字 (如 600000 / 000001 / 688001)\n"
              "• 数据: 资产轴 (帧目录扫出)\n"
              "• 显示: 原样, 按字典序排\n"
              "• 适用: 全部资产"},
     Kind::Text,
     0,
     nullptr,
     nullptr,
     [](const Row &r) -> std::string_view { return r.a.asset_code; },
     nullptr,
     nullptr},

    {{"简称", "简称 = 公司在交易所的简称\n"
              "• 算法: 逐日 PIT 取最新交易日; 退市股回落到最后简称\n"
              "• 数据: cn_stock_instruments 最新交易日 / cn_stock_basic_info\n"
              "• 显示: 原样, 按字典序排; 简称前缀与 ST 列同源同日\n"
              "• 适用: 有基本面的资产"},
     Kind::Text,
     0,
     nullptr,
     nullptr,
     [](const Row &r) -> std::string_view { return r.info && !r.info->name.empty() ? std::string_view(r.info->name) : "-"; },
     nullptr,
     nullptr},

    {{"板块", "板块 = 交易所.板块\n"
              "• 算法: 板块按代码前缀: SH.主 = 沪市主板 (600/601/603/605), SH.科 = 科创板 (688/689), SZ.主 = 深市主板 (000/001/002/003/004),\n"
              "  SZ.创 = 创业板 (300/301/302/309), BJ.北 = 北交所 (43/83/87/88/92)\n"
              "• 数据: 证券代码 + 交易所\n"
              "• 显示: 字色按交易所 (沪蓝 / 深青 / 北橙); 按板块排\n"
              "• 适用: 全部资产"},
     Kind::Category,
     0,
     nullptr,
     [](const Row &r) { return static_cast<double>(GetBoardType(r.a.asset_code)); },
     [](const Row &r) -> std::string_view { return MarketAbbr(GetBoardType(r.a.asset_code)); },
     [](const Row &r, double) {
       const char *t = MarketAbbr(GetBoardType(r.a.asset_code));
       return t[0] == '-' ? num_none() : num_cell(r.a.exchange == "SH" ? kShCol : r.a.exchange == "SZ" ? kSzCol
                                                                                                       : kBjCol,
                                                  "%s", t);
     },
     nullptr},

    {{"涨幅", "涨幅 = 当日涨跌幅\n"
              "• 算法: 最新一日收盘 / 前收 − 1\n"
              "• 数据: 新浪当日接得上 (其前一交易日 = 本只 bar1d 水位) 取新浪快照, 否则 bar1d 最新有成交日; 与 Lines 右表近况列同一核\n"
              "• 显示: 百分比两位, 按正负着色; 随快照刷新\n"
              "• 适用: 有基本面的资产"},
     Kind::Number,
     0,
     nullptr,
     [](const Row &r) { return 100.0 * r.rc.chg; },
     nullptr,
     &pct_cell,
     nullptr,
     true},

    {{"Δ5日线", "Δ5日线 = 对 5 日均线的偏离\n"
                "• 算法: 最新一日收盘 / MA5 − 1, MA5 = 最近 5 个有成交日的收盘均值 (含最新一日)\n"
                "• 数据: 同涨幅列 (新浪当日接上 bar1d 最近几个有成交日); 不复权, 窗内除权的跳空照算\n"
                "• 显示: 百分比两位, 按正负着色\n"
                "• 适用: 有基本面的资产"},
     Kind::Number,
     0,
     nullptr,
     [](const Row &r) { return 100.0 * r.rc.ma5; },
     nullptr,
     &pct_cell,
     nullptr,
     true},

    {{"行业", "行业 = 申万一级行业\n"
              "• 算法: industry_level1_name, 缺名回落到行业代码\n"
              "• 数据: cn_stock_industry_component 最新月度快照\n"
              "• 显示: 原样, 按名排; 悬停格看行业代码 (ind_code)\n"
              "• 适用: 有基本面的资产"},
     Kind::Category,
     0,
     nullptr,
     nullptr,
     [](const Row &r) -> std::string_view { return r.info && !industry_name(*r.info).empty() ? std::string_view(industry_name(*r.info)) : "-"; },
     nullptr,
     [](const Row &r) {
       if (r.info && !r.info->ind_code.empty())
         tip("%s", r.info->ind_code.c_str());
     }},

    {{"PE", "PE = 滚动市盈率 (TTM); 键 = EP = 1/PE (%)\n"
            "• 算法: 总市值 / 归母净利润TTM; 排序 / 名次 / 统计按倒数 EP: 高 = 便宜, 亏损为负排最差一端\n"
            "• 数据: 总市值同总市值列; 净利取当日可见的最新财务\n"
            "• 显示: 倍数一位小数, 底色 = 过滤后截面名次 (EP 高绿 → 低红); 0 → -\n"
            "• 适用: 有基本面的资产"},
     Kind::Number,
     1,
     nullptr,
     &yield<&StockInfo::pe_ttm>,
     nullptr,
     &multiple_cell<&StockInfo::pe_ttm, 1>,
     nullptr},

    {{"PB", "PB = 市净率 (MRQ); 键 = BP = 1/PB (%)\n"
            "• 算法: 总市值 / 归母普通股权益MRQ (total_equity_to_parent_shareholders − other_equity_instruments: 永续债 / 优先股不属普通股东);\n"
            "  排序 / 名次 / 统计按倒数 BP: 高 = 便宜, 负权益为负排最差一端\n"
            "• 数据: MRQ = 最新报告期 (max report_date 的最新可见行); 总市值同总市值列\n"
            "• 显示: 倍数两位小数, 底色 = 过滤后截面名次 (BP 高绿 → 低红); < 1 = 破净; 0 → -\n"
            "• 适用: 有基本面的资产"},
     Kind::Number,
     1,
     nullptr,
     &yield<&StockInfo::pb_mrq>,
     nullptr,
     &multiple_cell<&StockInfo::pb_mrq, 2>,
     nullptr},

    {{"PS", "PS = 滚动市销率 (TTM); 键 = SP = 1/PS (%)\n"
            "• 算法: 总市值 / 营业总收入TTM; 排序 / 名次 / 统计按倒数 SP: 高 = 便宜\n"
            "• 数据: 营收取当日可见的最新财务; 总市值同总市值列\n"
            "• 显示: 倍数两位小数, 底色 = 过滤后截面名次 (SP 高绿 → 低红); 0 → -\n"
            "• 适用: 有基本面的资产; 未盈利但有营收的公司照样可比"},
     Kind::Number,
     1,
     nullptr,
     &yield<&StockInfo::ps_ttm>,
     nullptr,
     &multiple_cell<&StockInfo::ps_ttm, 2>,
     nullptr},

    {{"PCF", "PCF = 滚动市现率 (TTM); 键 = CP = 1/PCF (%)\n"
             "• 算法: 总市值 / 经营活动现金流量净额TTM (net_cffoa_ttm, 非现金净增加额); 排序 / 名次 / 统计按倒数 CP: 高 = 便宜, 经营现金流为负保留负值排最差一端\n"
             "• 数据: 现金流取当日可见的最新财务; 总市值同总市值列\n"
             "• 显示: 倍数一位小数, 底色 = 过滤后截面名次 (CP 高绿 → 低红); 0 → -\n"
             "• 适用: 有基本面的资产"},
     Kind::Number,
     1,
     nullptr,
     &yield<&StockInfo::pcf_ttm>,
     nullptr,
     &multiple_cell<&StockInfo::pcf_ttm, 1>,
     nullptr},

    {{"股息1年", "股息1年 = 近 1 年股息率 (%)\n"
                 "• 算法: 近 365 日税前分红总额 / 总市值; 窗口以分红公告日 (publish_date) 锚定, 非除权日\n"
                 "• 数据: 分红公告 + 总市值 (同总市值列); 口径同 L_1min 特征 dy_raw\n"
                 "• 显示: 两位小数, 底色 = 过滤后截面名次 (高绿 → 低红); 0.00 = 窗口内确无分红公告 (非缺失)\n"
                 "• 适用: 有基本面的资产"},
     Kind::Number,
     1,
     "%.2f",
     &field<&StockInfo::dy1y>,
     nullptr,
     nullptr,
     nullptr},

    {{"股息3年", "股息3年 = 近 3 年股息率 (年化平均 %)\n"
                 "• 算法: 近 3 年 (1095 日) 税前分红总额 / 年数 / 总市值; 上市不足 3 年按实际上市年数年化 (不被摊薄), 不足一季 → -\n"
                 "• 数据: 同股息1年\n"
                 "• 显示: 两位小数, 底色 = 过滤后截面名次 (高绿 → 低红); 明显低于股息1年 = 近年才开始分红\n"
                 "• 适用: 有基本面的资产"},
     Kind::Number,
     1,
     "%.2f",
     &field<&StockInfo::dy3y>,
     nullptr,
     nullptr,
     nullptr},

    {{"股息5年", "股息5年 = 近 5 年股息率 (年化平均 %)\n"
                 "• 算法: 近 5 年 (1825 日) 税前分红总额 / 年数 / 总市值; 上市不足 5 年按实际上市年数年化\n"
                 "• 数据: 同股息1年\n"
                 "• 显示: 两位小数, 底色 = 过滤后截面名次 (高绿 → 低红); 股息1年 ≈ 3年 ≈ 5年 = 长期稳定分红\n"
                 "• 适用: 有基本面的资产"},
     Kind::Number,
     1,
     "%.2f",
     &field<&StockInfo::dy5y>,
     nullptr,
     nullptr,
     nullptr},

    {{"总市值", "总市值 = 收盘价 × 总股本 (亿元)\n"
                "• 算法: close × total_shares / 1亿 (不复权真价); 即估值 / 股息各列的分母\n"
                "• 数据: cn_stock_real_bar1d.close + cn_stock_shares.total_shares\n"
                "• 显示: 一位小数, 底色 = 过滤后截面名次 (大绿 → 小红); 缺收盘价或股本 → - (不是市值为零)\n"
                "• 适用: 有基本面的资产"},
     Kind::Number,
     1,
     "%.1f",
     [](const Row &r) { return r.info && r.info->mcap > 0 ? static_cast<double>(r.info->mcap) : kNaN; },
     nullptr,
     nullptr,
     nullptr},

    {{"ST", "ST = 风险警示\n"
            "• 算法: ST = 特别处理 (连续两年亏损等, 涨跌幅限制 ±5%), *ST = 退市风险警示 (高一级)\n"
            "• 数据: cn_stock_status.st_status (逐日); 与简称列同源同日 (简称前缀 ↔ 本列严格一致)\n"
            "• 显示: ST / *ST 红字, 正常 -; 按 正常 < ST < *ST 排\n"
            "• 适用: 有基本面的资产"},
     Kind::Category,
     0,
     nullptr,
     [](const Row &r) { return static_cast<double>(st_level(r.info)); },
     [](const Row &r) -> std::string_view {
       const int level = st_level(r.info);
       return level == 2 ? "*ST" : level == 1 ? "ST"
                                              : "正常";
     },
     [](const Row &, double v) { return v > 0 ? num_cell(kStCol, "%s", v == 2 ? "*ST" : "ST") : num_none(); },
     nullptr},

    {{"退市", "退市 = 已退市\n"
              "• 算法: 退市日 (out_date) 非 0\n"
              "• 数据: cn_stock_basic_info\n"
              "• 显示: 已退市 = 灰字 退市, 在市 -; 悬停格看退市日\n"
              "• 适用: 有基本面的资产"},
     Kind::Category,
     0,
     nullptr,
     [](const Row &r) { return r.info && r.info->out_date ? 1.0 : 0.0; },
     [](const Row &r) -> std::string_view { return r.info && r.info->out_date ? "退市" : "在市"; },
     [](const Row &, double v) { return v > 0 ? num_cell(kDimCol, "退市") : num_none(); },
     [](const Row &r) {
       if (r.info && r.info->out_date)
         tip("退市 %s", misc::dash(r.info->out_date).c_str());
     }},

    {{"在市", "在市 = 在市时长\n"
              "• 算法: 上市日 → 退市日 (退市股截到退市日) 或今天; 排序 / 名次 / 统计按在市总天数\n"
              "• 数据: 上市日 ipo_date / 退市日 out_date\n"
              "• 显示: 年/月/日 (11/02/06 = 11 年 2 个月 6 天), 底色 = 过滤后截面名次 (久绿 → 短红); 悬停格看起止日期与总天数\n"
              "• 适用: 有基本面的资产"},
     Kind::Number,
     1,
     nullptr,
     [](const Row &r) {
       const ListedSpan span = r.info ? listed_span(*r.info) : ListedSpan{};
       return span.valid ? static_cast<double>(span.total_days) : kNaN;
     },
     nullptr,
     [](const Row &r, double v) {
       if (std::isnan(v))
         return num_none();
       const ListedSpan span = listed_span(*r.info);
       return num_cell(ink(), "%02d/%02d/%02d", span.years, span.months, span.days);
     },
     [](const Row &r) {
       const ListedSpan span = r.info ? listed_span(*r.info) : ListedSpan{};
       if (span.valid)
         tip("上市 %s → %s\n在市 %d 年 %d 月 %d 天 (共 %d 天)", misc::dash(r.info->ipo_date).c_str(),
             r.info->out_date ? misc::dash(r.info->out_date).c_str() : "至今", span.years, span.months, span.days, span.total_days);
     }},

    {{"交易日", "交易日 = 有数据的交易日数\n"
                "• 算法: 帧目录里已编码的天数\n"
                "• 数据: 帧目录 (扫描时一并读到)\n"
                "• 显示: 整数, 不着色; 看数据完整性\n"
                "• 适用: 全部资产"},
     Kind::Number,
     0,
     "%.0f",
     [](const Row &r) { return static_cast<double>(r.s.total_days); },
     nullptr,
     nullptr,
     nullptr},

    {{"日均笔数", "日均笔数 = 日均逐笔记录数\n"
                  "• 算法: Σ 逐笔条数 (委托 + 成交合并后) / 有数据的交易日数\n"
                  "• 数据: 日文件目录的条数 (扫描时一并读到)\n"
                  "• 显示: > 1M 按 M (百万), > 1K 按 K (千), 不着色\n"
                  "• 适用: 有逐笔数据的资产"},
     Kind::Number,
     0,
     nullptr,
     [](const Row &r) { return r.s.total_days > 0 ? static_cast<double>(r.s.total_orders) / static_cast<double>(r.s.total_days) : kNaN; },
     nullptr,
     [](const Row &, double v) {
       if (std::isnan(v))
         return num_none();
       return v > 1e6 ? num_cell(ink(), "%.2fM", v / 1e6) : v > 1e3 ? num_cell(ink(), "%.1fK", v / 1e3)
                                                                    : num_cell(ink(), "%.0f", v);
     },
     nullptr},

    {{"逐笔完整", "逐笔完整 = 回测区间内逐笔的完整度\n"
                  "• 算法: (应有天数 − 缺失天数) / 应有天数; 应有 = 已上市未退市且非全天停牌的交易日\n"
                  "• 数据: 帧目录; 与 Calendar 页的完整性、Encode 页的缺失表同源\n"
                  "• 显示: 百分比一位: ≥ 99.9% 绿 / ≥ 95% 黄 / 其余橙; 悬停格看缺失天数; 区间内本不该有数据 (北交所 / 未上市 / 已退市) → - (不是缺口)\n"
                  "• 适用: L2 覆盖的资产 (北交所整体 -)"},
     Kind::Number,
     0,
     nullptr,
     [](const Row &r) { return r.s.expected_days > 0 ? static_cast<double>(r.s.orders_coverage_percent()) : kNaN; },
     nullptr,
     [](const Row &, double v) {
       if (std::isnan(v))
         return num_none();
       const ImVec4 color = v >= 99.9   ? ImVec4(0.3f, 0.95f, 0.4f, 1.0f)
                            : v >= 95.0 ? ImVec4(1.0f, 1.0f, 0.0f, 1.0f)
                                        : ImVec4(1.0f, 0.5f, 0.0f, 1.0f);
       return num_cell(color, "%.1f%%", v);
     },
     [](const Row &r) {
       if (r.s.expected_days > 0)
         tip("缺 %zu / 应有 %zu 个交易日", r.s.orders_missing, r.s.expected_days);
     }},
};
constexpr int kNumCols = static_cast<int>(std::size(kCols));

// 列 c 在行 r 的格: 文本 = c.cell / Number 按 fmt / 其余按 text; 键 = c.num
SheetCell cell_of(const Column &c, const Row &r) {
  const double v = c.num ? c.num(r) : kNaN;
  if (c.cell)
    return {c.cell(r, v), v};
  if (c.kind == Kind::Number) {
    assert(c.num && c.fmt);
    return sheet_cell(v, ink(), c.fmt, v);
  }
  const std::string_view t = c.text(r);
  return {t == "-" ? num_none() : num_cell(ink(), "%.*s", static_cast<int>(t.size()), t.data()), v};
}

// 列 c (格已填) 的名次 (过滤后 pool 内, GUI::rank_by, 并列取中) + 最宽.
//   用名次而非数值归一化: 估值 / 市值重尾, min-max 染色会被几个极端值压成一片同色; 并列取中: 股息成片的 0 不被摊成渐变
void rank_col(AssetCells &o, size_t c) {
  const size_t n = o.ids.size(), nc = static_cast<size_t>(kNumCols);
  if (kCols[c].good != 0)
    rank_by(n, kCols[c].good, [&](size_t i) { return o.cells[i * nc + c].key; }, [&](size_t i, double u) { o.cells[i * nc + c].rank = u; });
  o.cw[c] = ImGui::CalcTextSize(kCols[c].col.name.c_str()).x;
  for (size_t i = 0; i < n; ++i)
    o.cw[c] = std::max(o.cw[c], o.cells[i * nc + c].text.w);
}

// 格: 过滤 (资产序) → 逐行逐列填格 → 各列名次 / 最宽
void fill_cells(AssetCells &o, const Asset &asset, const StockInfos &stock_info, const Joint &quotes, const AssetFilter &filter) {
  assert(asset.asset_stats.size() == asset.items.size());
  o.ids.clear();
  for (size_t id = 0; id < asset.items.size(); ++id)
    if (shown(asset.items[id], find_info(asset.items[id], stock_info), filter))
      o.ids.push_back(id);
  const size_t n = o.ids.size(), nc = static_cast<size_t>(kNumCols);
  o.cells.assign(n * nc, {});
  for (size_t i = 0; i < n; ++i) {
    const Row r = make_row(asset, stock_info, quotes, o.ids[i]);
    for (size_t c = 0; c < nc; ++c)
      o.cells[i * nc + c] = cell_of(kCols[c], r);
  }
  o.cw.assign(nc, 0.f);
  for (size_t c = 0; c < nc; ++c)
    rank_col(o, c);
}

// 只重填行情列 (行 / 其余列不动)
void fill_quote(AssetCells &o, const Asset &asset, const StockInfos &stock_info, const Joint &quotes) {
  const size_t nc = static_cast<size_t>(kNumCols);
  for (size_t i = 0; i < o.ids.size(); ++i) {
    const Row r = make_row(asset, stock_info, quotes, o.ids[i]);
    for (size_t c = 0; c < nc; ++c)
      if (kCols[c].quote)
        o.cells[i * nc + c] = cell_of(kCols[c], r);
  }
  for (size_t c = 0; c < nc; ++c)
    if (kCols[c].quote)
      rank_col(o, c);
}

// 显示序: 排序列有键 → 按键 (sort_less: NaN 恒末), 否则按格文本; 无排序列 = 资产序
void fill_order(std::vector<size_t> &o, const AssetCells &cs, int sort_column, bool ascending) {
  o.resize(cs.ids.size());
  std::iota(o.begin(), o.end(), size_t{0});
  if (sort_column < 0)
    return;
  assert(sort_column < kNumCols);
  const bool desc = !ascending;
  const auto at = [&](size_t i) -> const SheetCell & { return cs.cells[i * kNumCols + static_cast<size_t>(sort_column)]; };
  if (kCols[sort_column].num)
    std::ranges::stable_sort(o, [&](size_t a, size_t b) { return sort_less(at(a).key, at(b).key, desc); });
  else
    std::ranges::stable_sort(o, [&](size_t a, size_t b) {
      const int r = std::strcmp(at(a).text.s, at(b).text.s);
      return desc ? r > 0 : r < 0;
    });
}

// 格 → 显示序 (三级: 快照只重填行情列, 排序只重排, 都不重填其余格; 表格与截面分析共用)
void sync_view(AssetState &state, const Asset &asset, const StockInfos &stock_info) {
  const Joint &q = state.quotes;
  const Tape *t = q.tape();
  const AssetQuoteKey qk{t ? t->tails : nullptr, q.live ? static_cast<uint64_t>(q.live->date) * 86400 + static_cast<uint64_t>(q.live->sec) : 0};
  state.cells.update(AssetCellsKey{asset.generation, asset.items.size(), stock_info.size(), state.filter, ImGui::GetFontSize(), ImGui::GetColorU32(ImGuiCol_Text)},
                     [&](AssetCells &o) {
                       fill_cells(o, asset, stock_info, q, state.filter);
                       o.quote = qk;
                     });
  if (AssetCells &o = state.cells.val; !(o.quote == qk)) {
    fill_quote(o, asset, stock_info, q);
    o.quote = qk;
    ++o.quote_gen;
  }
  state.order.update(AssetOrderKey{state.cells.gen, state.cells.val.quote_gen, state.sort_column, state.sort_ascending},
                     [&](std::vector<size_t> &o) { fill_order(o, state.cells.val, state.sort_column, state.sort_ascending); });
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
  ImGui::Text("Showing:%zu/%zu", state.cells.val.ids.size(), assets.size());

  ImGui::SameLine();
  ImGui::BeginGroup();
  ImGui::TextColored(band_col(0.0, 1.f), "■");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(band_col(0.5, 1.f), "■");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(band_col(1.0, 1.f), "■");
  ImGui::EndGroup();
  if (ImGui::IsItemHovered())
    tip("名次底色 (PE / PB / PS / PCF / 股息 / 总市值 / 在市; 同 Lines 右表): 绿 = 好, 红 = 差, 取当前过滤后 pool 内的名次分位\n"
        "• PE / PB / PS / PCF: 倒数 (收益率) 越高越绿, 亏损的负值排在最差一端\n"
        "• 股息: 分红越多越绿; 总市值: 越大越绿; 在市: 越久越绿\n"
        "• 用名次分位而非数值归一化: 估值 / 市值重尾, min-max 染色会被几个极端值压成一片同色");

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
    ImGui::TableSetupColumn(kCols[ci].col.name.c_str(), ci == 0 ? ImGuiTableColumnFlags_DefaultSort | ImGuiTableColumnFlags_PreferSortAscending : 0);
  ImGui::TableSetupScrollFreeze(0, 1);
  sheet_head(kNumCols, [](int c) -> const SheetCol & { return kCols[c].col; });

  // 排序规则的变化由 SpecsDirty 告知; 重排在 fill_order 里, 只在键变时做一次
  if (ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs()) {
    if (sort_specs->SpecsDirty && sort_specs->SpecsCount > 0) {
      const auto &spec = sort_specs->Specs[0];
      asset_state.sort_column = spec.ColumnIndex;
      asset_state.sort_ascending = spec.SortDirection == ImGuiSortDirection_Ascending;
    }
    sort_specs->SpecsDirty = false;
  }

  sync_view(asset_state, asset_data, stock_info);
  const AssetCells &cs = asset_state.cells.val;
  const std::vector<size_t> &order = asset_state.order.val;
  const int hovered_col = ImGui::TableGetHoveredColumn();
  const int hovered_row = ImGui::TableGetHoveredRow(); // 上帧的表行号 (0 = 表头; clipper 下 = 本帧第几个交的行)
  size_t hit = SIZE_MAX;                               // 悬停行 (cells 的行下标)
  ImGuiListClipper clip;
  clip.Begin(static_cast<int>(order.size()));
  while (clip.Step())
    for (int i = clip.DisplayStart; i < clip.DisplayEnd; ++i) {
      const size_t at = order[static_cast<size_t>(i)], id = cs.ids[at];
      ImGui::TableNextRow();
      if (ImGui::TableGetRowIndex() == hovered_row)
        hit = at;
      if (asset_state.selected_asset == static_cast<int>(id))
        ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, ImGui::GetColorU32(kPickCol));
      for (int ci = 0; ci < kNumCols; ++ci) {
        ImGui::TableSetColumnIndex(ci);
        const Column &c = kCols[ci];
        const SheetCell &x = cs.cells[at * kNumCols + static_cast<size_t>(ci)];
        draw_cell(x, c.kind == Kind::Number ? cs.cw[static_cast<size_t>(ci)] : x.text.w);
        if (!asset_state.quotes_page && hovered_col == ci) // 点格切截面分析的列: 盖过名次底色
          ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(kHoverCol));
        if (c.hover && ImGui::IsItemHovered()) // StockInfo 现查 (基本面可能整体重载, 不缓存指针)
          c.hover(make_row(asset_data, stock_info, asset_state.quotes, id));
      }
    }

  // 左键点行内任意处 (格子的文字 / 内边距 / 边框都算) 即点该行: 行 = 表的行背景带 (上下相接无缝), 列 = 列区 (左右相接无缝); 滚动条不算
  const ImRect inner = ImGui::GetCurrentWindow()->InnerClipRect;
  if (ImGui::IsMouseClicked(ImGuiMouseButton_Left) && ImGui::IsWindowHovered() && inner.Contains(ImGui::GetMousePos()) && hit != SIZE_MAX &&
      hovered_col >= 0 && hovered_col < kNumCols) {
    asset_state.selected_asset = static_cast<int>(cs.ids[hit]);
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
  for (const size_t id : asset_state.cells.val.ids) {
    const Row r = make_row(asset_data, stock_info, asset_state.quotes, id);
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
  for (const size_t id : asset_state.cells.val.ids) {
    const Row r = make_row(asset_data, stock_info, asset_state.quotes, id);
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
  const std::string &t = col.col.tip;
  assert(t.find('\n') != std::string::npos);
  ImGui::TextUnformatted(t.c_str(), t.c_str() + t.find('\n'));
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
  assert(mk.started());
  asset_state.quotes = mk.joint(kStock);
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
        // 键 = asset_id (== 全局 id)
        Want(mk, asset_state.stock, asset_state.selected_asset >= 0 ? static_cast<uint32_t>(asset_state.selected_asset) : Ref::kNone);
        std::string title;
        if (const uint32_t k = asset_state.stock.shown; k != Ref::kNone) {
          assert(k < assets.size());
          const AssetItem &a = assets[k];
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