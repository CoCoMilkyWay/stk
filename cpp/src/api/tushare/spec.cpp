#include "api/tushare/spec.hpp"

namespace tushare {

// ============================================================================
// SPECS — BigQuant 无等价的事件型 fallback (3 张)
//   forecast    业绩预告:     visible=ann_date (pit.cpp 直接读该列)
//   express     业绩快报:     visible=ann_date
//   disclosure  财报披露计划: visible=ann_date, per-day API;
//               drop actual_date/modify_date (ann_date 之后回填 = 未来信息)
// ============================================================================
// avail_hour=24: 公告当日全天涓流发布, 次日才视为完整 (增量水位永不吃半天).
// num_fields: 按 tushare doc 的 float 字段逐列声明 (doc/api/tushare/financial/
//   {forecast,express,disclosure_date}.md 输出参数表); 未声明的列一律 string.
//   声明列必须出现在响应 fields 里 (parse 断言), API 改字段名会立刻暴露.
const std::vector<InterfaceSpec> SPECS = {
    {"forecast", "forecast_vip", "ann_date", 24, {}, {}, {"p_change_min", "p_change_max", "net_profit_min", "net_profit_max", "last_parent_net"}, "业绩预告: 类型(预增/预亏/续亏) / 净利上下限 / 变动幅度上下限 / 上年同期归母"},
    {"express", "express_vip", "ann_date", 24, {}, {}, {"revenue", "operate_profit", "total_profit", "n_income", "total_assets", "total_hldr_eqy_exc_min_int", "diluted_eps", "diluted_roe", "yoy_net_profit", "bps", "open_net_assets", "open_bps"}, "业绩快报: 营收/营业利润/净利/总资产/归母权益/摊薄 EPS·ROE/BPS"},
    {"disclosure", "disclosure_date", "ann_date", 24, {"ann_date"}, {"actual_date", "modify_date"}, {}, "财报披露计划: 预约披露日 pre_date"},
};

} // namespace tushare
