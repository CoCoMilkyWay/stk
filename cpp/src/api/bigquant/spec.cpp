#include "api/bigquant/spec.hpp"

#include <cassert>
#include <string>
#include <string_view>

namespace bigquant {

// ============================================================================
// SPECS — 顺序严格对齐 doc/bigquant/used/api.md 自上而下.
// 落盘 = 服务端响应原样 (行结构 / 去重语义信任服务端 PIT, 与 archive 同源同构).
// ============================================================================
const std::vector<TableSpec> SPECS = {
    // axis 源 (all_trading_days/holidays): 普通月度表, axis.cpp 直接扫月 parquet 读 D 轴.
    //   all_trading_days = 中国A股全年日历 (根据节假日预计算, 排程提前入库) →
    //   avail_hour=0, 当日行随时可拉 (D 轴 last_d 依赖). Where kind: 是否 date 分区
    //   未验证, 纯 SQL WHERE 任何表成立 (配额按返回 cell 计, 无差价).
    //   ⚠ 多市场 trading_days 已弃用: 其 CN 行当日盘后 ~19:00 才入库 (api.md
    //   "至截止最新日期"), 盘中 D 轴缺当日 ⇒ overlay 新鲜度断言必炸; 且 UK/IT 等
    //   早到行把开放月水位推过 CN 缺行日 ⇒ 当月 CN 行永久缺失, 只能关月兜回.
    {"all_trading_days", "date", FetchKind::Where, FetchFreq::Day, 0},
    {"holidays", "date", FetchKind::Partition, FetchFreq::Day, 0},
    {"cn_stock_instruments", "date", FetchKind::Partition, FetchFreq::Day, 20},
    // industry_component: 月初快照 (低频), 月内变动靠 industry_change 增量 cover.
    {"cn_stock_industry_component", "date", FetchKind::Partition, FetchFreq::MonthFirst, 20},
    // industry_change: 行业进出事件 (pit.cpp 仅取 change_flag=1 进入新行业一侧).
    {"cn_stock_industry_change", "date", FetchKind::Partition, FetchFreq::Day, 20},
    // industry_real_bar1d: 行业不复权日行情 (instrument 列实际是 industry_code).
    {"cn_stock_industry_real_bar1d", "date", FetchKind::Partition, FetchFreq::Day, 20},
    {"cn_stock_industry_valuation", "date", FetchKind::Partition, FetchFreq::Day, 20},
    // basic_info: 静态全市场快照 (Static, 无 date 列, 走 _meta 单文件, 日级整刷).
    {"cn_stock_basic_info", "", FetchKind::Static, FetchFreq::Day, 20},
    {"cn_stock_capital", "publish_date", FetchKind::Where, FetchFreq::Day, 20},
    {"cn_stock_dividend", "publish_date", FetchKind::Where, FetchFreq::Day, 20},
    {"cn_stock_allotment", "publish_date", FetchKind::Where, FetchFreq::Day, 20},
    // margin: 真盘前 10:00 入库 (api.md), 当日盘中即可增量拉到当日行.
    {"cn_stock_margin_trading_detail", "date", FetchKind::Partition, FetchFreq::Day, 10},
    {"cn_stock_margin_trading_market", "date", FetchKind::Partition, FetchFreq::Day, 10},
    {"cn_stock_shareholder", "publish_date", FetchKind::Where, FetchFreq::Day, 20},
    {"cn_stock_shares", "date", FetchKind::Partition, FetchFreq::Day, 20},
    {"cn_stock_status", "date", FetchKind::Partition, FetchFreq::Day, 20},
    {"cn_stock_suspend", "date", FetchKind::Partition, FetchFreq::Day, 20},
    // name_change: visible=end_date (简称失效日, 此后才确知本段区间).
    {"cn_stock_name_change", "end_date", FetchKind::Where, FetchFreq::Day, 20},
    {"cn_stock_dragon_list", "date", FetchKind::Partition, FetchFreq::Day, 20},
    // real_bar1d: 股票不复权日行情 (项目统一走未复权).
    {"cn_stock_real_bar1d", "date", FetchKind::Partition, FetchFreq::Day, 20},
    {"cn_stock_limit_price", "date", FetchKind::Partition, FetchFreq::Day, 20},
    // static_data: 真盘前 09:00 全市场快照. Snapshot kind → 只取 [s,e] 内最新一天
    //   (MAX(date)) 一份, 落 data/_meta/cn_stock_static_data.parquet 单文件;
    //   pit overlay 阶段填 row=last_d, 给实盘当日提供真盘前可见数据.
    //   avail_hour=9: 文件内快照日已 ≥ horizon → skip (每天最多整刷一次).
    {"cn_stock_static_data", "date", FetchKind::Snapshot, FetchFreq::Day, 9},
    {"cn_stock_financial_income_general_pit", "date", FetchKind::Partition, FetchFreq::Day, 20},
    {"cn_stock_financial_cashflow_general_pit", "date", FetchKind::Partition, FetchFreq::Day, 20},
    {"cn_stock_financial_balance_general_pit", "date", FetchKind::Partition, FetchFreq::Day, 20},
    // shift 字段定位某 (date, instrument, report_date) 的偏移序列, 各 shift 独立行.
    {"cn_stock_financial_ttm_shift", "date", FetchKind::Partition, FetchFreq::Day, 20},
    {"cn_stock_financial_notes_shift", "date", FetchKind::Partition, FetchFreq::Day, 20},
};

// ============================================================================
// fetch — 自动按 (kind, freq) 选 SQL 模板, 一步式 DAI 查询
// ============================================================================

namespace {

// "YYYYMMDD" -> "YYYY-MM-DD" (DAI 接受格式)
std::string to_dashed(std::string_view yyyymmdd) {
  assert(yyyymmdd.size() == 8);
  std::string out;
  out.reserve(10);
  out.append(yyyymmdd.data(), 4);
  out.push_back('-');
  out.append(yyyymmdd.data() + 4, 2);
  out.push_back('-');
  out.append(yyyymmdd.data() + 6, 2);
  return out;
}

} // namespace

std::shared_ptr<arrow::Table> fetch(DaiClient &client, const TableSpec &spec,
                                    std::string_view start,
                                    std::string_view end,
                                    std::string_view min_vd) {
  // ---- Static: 整表全量, 忽略 start/end ----
  if (spec.kind == FetchKind::Static) {
    assert(spec.visible_date.empty() && "Static 表不应配 visible_date");
    assert(min_vd.empty() && "Static 表无 visible_date, 不支持增量");
    return client.query("SELECT * FROM " + spec.name);
  }

  // ---- 非 Static: 必须有 visible_date + 完整 [start, end] ----
  assert(!spec.visible_date.empty() && "非 Static 表必须配 visible_date");
  assert(start.size() == 8 && end.size() == 8 && "start/end 须为 YYYYMMDD");
  assert(min_vd.empty() || (min_vd.size() == 8 && min_vd >= start &&
                            min_vd <= end)); // 增量下界须落在窗口内

  std::string ds = to_dashed(start);
  std::string de = to_dashed(end);
  // 增量抬升后的窗口下界 (Partition Day / Where 直接用; 分区裁剪同步缩窗).
  std::string dm = min_vd.empty() ? ds : to_dashed(min_vd);

  if (spec.kind == FetchKind::Snapshot) {
    // Snapshot: 取窗口内最新一天的全量行 (假日则顺延前; 与 MonthFirst MIN 对仗).
    //   start 仅决定服务端分区扫描下界, 不影响 MAX(<vd>) 结果, 也不影响配额
    //   (配额按返回 cell 数计, 与扫描窗口无关) — 故窗口原样透传.
    assert(spec.freq == FetchFreq::Day && "Snapshot 当前仅支持 FetchFreq::Day");
    std::string sql = "SELECT * FROM " + spec.name + " WHERE " +
                      spec.visible_date + " = (SELECT MAX(" +
                      spec.visible_date + ") FROM " + spec.name + " WHERE " +
                      spec.visible_date + " >= '" + ds + "' AND " +
                      spec.visible_date + " <= '" + de + "')";
    return client.query(sql, {{"date", {ds, de}}});
  }

  if (spec.kind == FetchKind::Partition) {
    // 分区裁剪走 filters; Day SQL 不带 WHERE, MonthFirst 仍带 sub-select.
    if (spec.freq == FetchFreq::Day) {
      return client.query("SELECT * FROM " + spec.name, {{"date", {dm, de}}});
    }
    // MonthFirst: 取窗口内最早一天的全量行 (月初遇假期则顺延). MIN 子查询与
    // 分区裁剪保持整月窗口 (快照日语义不受增量影响), 增量仅外层叠 vd >= min_vd
    // ⇒ 快照已入盘时返回 0 行 (免费).
    std::string sql = "SELECT * FROM " + spec.name + " WHERE " +
                      spec.visible_date + " = (SELECT MIN(" +
                      spec.visible_date + ") FROM " + spec.name + " WHERE " +
                      spec.visible_date + " >= '" + ds + "' AND " +
                      spec.visible_date + " <= '" + de + "')";
    if (!min_vd.empty())
      sql += " AND " + spec.visible_date + " >= '" + dm + "'";
    return client.query(sql, {{"date", {ds, de}}});
  }

  assert(spec.kind == FetchKind::Where);
  assert(spec.freq == FetchFreq::Day && "Where + MonthFirst 当前不支持");
  std::string sql = "SELECT * FROM " + spec.name + " WHERE " +
                    spec.visible_date + " >= '" + dm + "' AND " +
                    spec.visible_date + " <= '" + de + "'";
  return client.query(sql);
}

} // namespace bigquant
