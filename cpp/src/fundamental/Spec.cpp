#include "fundamental/Spec.hpp"

#include <cassert>
#include <set>

namespace fundamental {

namespace {

// Spec 不变式的唯一检查点 (见 Spec.hpp snap 注释); 下游 (Remote / Store / Scan) 不再逐处重判
std::vector<Spec> checked(std::vector<Spec> v) {
  std::set<std::string_view> names;
  for (const Spec &s : v) {
    assert(!s.name.empty() && names.insert(s.name).second && "SPECS: 表名空或重复");
    assert(s.avail_hour >= 0 && s.avail_hour <= 24 && "SPECS: avail_hour 越界");
    assert((!s.vd.empty() || (s.shape == Shape::Meta && s.snap == Snap::All)) && "SPECS: vd 空只允许 Meta·All");
    assert((!from<Tu>(s) || (s.shape == Shape::Month && s.snap == Snap::All)) && "SPECS: Tushare 只有 Month·All");
    assert((!from<Bq>(s) || std::get<Bq>(s.src).prune == Prune::Where || s.vd == "date") && "SPECS: 分区表的 vd 须是 date");
    for (const std::string &c : s.drop)
      assert(c != s.vd && c != key_col(s) && "SPECS: drop 不能剥 vd / 键列");
  }
  return v;
}

// SPECS 行内裸写枚举值, 列对齐
using enum Shape;
using enum Snap;
using enum Prune;

} // namespace

// ============================================================================
// SPECS — BigQuant 段顺序严格对齐 doc/bigquant/used/api.md 自上而下; Tushare 段 = BigQuant 无等价的 3 张事件表.
// 落盘 = 服务端响应原样 (行结构 / 去重语义信任服务端 PIT).
// 列: name, vd, Shape, Snap, avail_hour, src, desc[, drop]
// ============================================================================
// clang-format off
const std::vector<Spec> SPECS = checked({
    // ======================== BigQuant DAI ========================
    // all_trading_days = 中国 A 股全年日历 (节假日预计算, 排程提前入库) → avail_hour=0, 当日行随时可拉
    //   (D 轴 last_d 依赖). Partition 未验证, 纯 WHERE 任何表成立 (配额按返回 cell 计, 无差价).
    //   ⚠ 多市场 trading_days 已弃用: 其 CN 行当日盘后 ~19:00 才入库, 盘中 D 轴缺当日 ⇒ overlay 新鲜度断言必炸;
    //   且 UK/IT 等早到行把开放月水位推过 CN 缺行日 ⇒ 当月 CN 行永久缺失, 只能关月兜回.
    {"all_trading_days",                        "date",         Month, All, 0,  Bq{Where},
     "A 股交易日历 (节前预排入库) — D 轴唯一来源, 取 market_code='CN'"},
    {"holidays",                                "date",         Month, All, 0,  Bq{Partition},
     "节假日表 (与交易日历互补)"},
    {"cn_stock_instruments",                    "date",         Month, All, 20, Bq{Partition},
     "逐日在册标的 + PIT 简称 (戴帽/改名当日即变)"},
    // industry_component: 月初快照 (低频), 月内变动靠 industry_change 增量 cover.
    {"cn_stock_industry_component",             "date",         Month, Day, 20, Bq{Partition},
     "行业成分月初快照: 一/二/三级行业码与名"},
    // industry_change: 行业进出事件 (fund::Pool 仅取 change_flag=1 进入新行业一侧).
    {"cn_stock_industry_change",                "date",         Month, All, 20, Bq{Partition},
     "行业进出事件 (change_flag=1 为进入) — 月内增量盖在月初快照上"},
    // industry_real_bar1d: 行业不复权日行情 (instrument 列实际是 industry_code).
    {"cn_stock_industry_real_bar1d",            "date",         Month, All, 20, Bq{Partition},
     "行业指数不复权日线 (instrument 列实为 industry_code)"},
    {"cn_stock_industry_valuation",             "date",         Month, All, 20, Bq{Partition},
     "行业估值: 成分股数 / pe_trailing / pe_ttm / pb"},
    // basic_info: 静态全市场快照 (无 date 列, _meta 单文件, 日级整刷).
    {"cn_stock_basic_info",                     "",             Meta,  All, 20, Bq{Where},
     "全市场静态档案 38 列 (含退市): 上市板/证券类型/上市退市日/IPO 价·发行 PE/员工数"},
    {"cn_stock_capital",                        "publish_date", Month, All, 20, Bq{Where},
     "股本结构变更事件: 变更日/原因/限售·B 股·H 股拆分"},
    {"cn_stock_dividend",                       "publish_date", Month, All, 20, Bq{Where},
     "分红送转: 税前·税后每股现金 / 送股·转增率 / 登记日 / 除权日"},
    {"cn_stock_allotment",                      "publish_date", Month, All, 20, Bq{Where},
     "配股: 配股价 / 比例 / 股数 / 登记日 / 除权日 / 上市日"},
    // margin: 真盘前 10:00 入库, 当日盘中即可增量拉到当日行.
    {"cn_stock_margin_trading_detail",          "date",         Month, All, 10, Bq{Partition},
     "个股两融 22 列: 融资余额·买入·偿还, 融券余量·卖出, 可融券量"},
    {"cn_stock_margin_trading_market",          "date",         Month, All, 10, Bq{Partition},
     "全市场两融汇总 (同口径, 按交易所)"},
    {"cn_stock_shareholder",                    "publish_date", Month, All, 20, Bq{Where},
     "股东户数与户均持股 (总股本口径 + 流通口径, 均带环比)"},
    {"cn_stock_shares",                         "date",         Month, All, 20, Bq{Partition},
     "逐日股本: 总股本 / A 股流通 / 自由流通 — 总市值的分子"},
    {"cn_stock_status",                         "date",         Month, All, 20, Bq{Partition},
     "逐日状态位: st_status(0 正常/1 ST/2 *ST) / 风险警示 / 停牌 / 涨跌停状态 / 除权除息"},
    {"cn_stock_suspend",                        "date",         Month, All, 20, Bq{Partition},
     "停复牌明细: 停牌时段 + 停牌原因"},
    // name_change: visible=end_date (简称失效日, 此后才确知本段区间).
    {"cn_stock_name_change",                    "end_date",     Month, All, 20, Bq{Where},
     "简称变更区间 [start_date, end_date]"},
    {"cn_stock_dragon_list",                    "date",         Month, All, 20, Bq{Partition},
     "龙虎榜: 上榜原因 / 净买额 / 成交占比 / 流通市值 / 前 1·2·5 日涨幅"},
    // real_bar1d: 股票不复权日行情 (项目统一走未复权).
    {"cn_stock_real_bar1d",                     "date",         Month, All, 20, Bq{Partition},
     "不复权日线 OHLC/量/额/笔数/换手 + adjust_factor (复权因子变点 = 分红拆分日)"},
    {"cn_stock_limit_price",                    "date",         Month, All, 20, Bq{Partition},
     "当日涨停价 / 跌停价"},
    // static_data: 真盘前 09:00 全市场快照, 只取窗口内最新一天一份 (Meta·Day).
    //   avail_hour=9: 文件内快照日已 ≥ horizon → Frozen (每天最多整刷一次).
    {"cn_stock_static_data",                    "date",         Meta,  Day, 9,  Bq{Partition},
     "真盘前全市场快照 19 列: 昨收/涨跌停/复权因子/停牌/ST/两融资格/自由流通股"},
    {"cn_stock_financial_income_general_pit",   "date",         Month, All, 20, Bq{Partition},
     "利润表 82 列: 营收/营业利润/利润总额/归母净利/EPS/研发费"},
    {"cn_stock_financial_cashflow_general_pit", "date",         Month, All, 20, Bq{Partition},
     "现金流量表 109 列: 经营·投资·筹资净现金流及全部明细"},
    {"cn_stock_financial_balance_general_pit",  "date",         Month, All, 20, Bq{Partition},
     "资产负债表 133 列: 总资产/总负债/归母权益/存货/商誉/货币资金"},
    // shift 字段定位某 (date, instrument, report_date) 的偏移序列, 各 shift 独立行.
    {"cn_stock_financial_ttm_shift",            "date",         Month, All, 20, Bq{Partition},
     "TTM 三表 185 列 × shift 同期偏移: 归母净利 / 营收 / 经营现金流 TTM"},
    {"cn_stock_financial_notes_shift",          "date",         Month, All, 20, Bq{Partition},
     "财报附注 76 列 × shift: 非经常性损益明细 (政府补助/公允价值变动/债务重组)"},

    // ======================== Tushare ========================
    // avail_hour=24: 公告当日全天涓流发布, 次日才视为完整 (增量水位永不吃半天).
    // num: 按 tushare doc 的 float 字段逐列声明 (doc/api/tushare/financial/{forecast,express,disclosure_date}.md);
    //   声明列必须出现在响应 fields 里 (client 断言), API 改字段名会立刻暴露.
    {"forecast",                                "ann_date",     Month, All, 24, Tu{"forecast_vip",    {},           {"p_change_min", "p_change_max", "net_profit_min", "net_profit_max", "last_parent_net"}},
     "业绩预告: 类型(预增/预亏/续亏) / 净利上下限 / 变动幅度上下限 / 上年同期归母"},
    {"express",                                 "ann_date",     Month, All, 24, Tu{"express_vip",     {},           {"revenue", "operate_profit", "total_profit", "n_income", "total_assets", "total_hldr_eqy_exc_min_int", "diluted_eps", "diluted_roe", "yoy_net_profit", "bps", "open_net_assets", "open_bps"}},
     "业绩快报: 营收/营业利润/净利/总资产/归母权益/摊薄 EPS·ROE/BPS"},
    // disclosure: 逐日接口; actual_date / modify_date 在 ann_date 之后回填 = 未来信息, 不落盘.
    {"disclosure",                              "ann_date",     Month, All, 24, Tu{"disclosure_date", {"ann_date"}, {}},
     "财报披露计划: 预约披露日 pre_date", {"actual_date", "modify_date"}},
});
// clang-format on

const Spec &spec(std::string_view name) {
  for (const Spec &s : SPECS)
    if (s.name == name)
      return s;
  assert(false && "fundamental::spec: 表不在 SPECS");
  return SPECS.front();
}

} // namespace fundamental
