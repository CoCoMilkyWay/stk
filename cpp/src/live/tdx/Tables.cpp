#include "live/tdx/Tables.hpp"

#include "misc/date.hpp"

namespace live::tdx::tables {

// 「市值 ≈ 现价 × 余额」交叉验证数值列没错位; 停牌价为 0 时不核
void Positions::check(const Shape &t, const Row &r) {
  const double mv = r.market_value, px = r.price, bal = r.balance;
  if (mv && px && bal && !mem::isclose(px * bal, mv, 0.02, 1.0))
    fail(strf("%s %s 市值 (%.2f) ≠ 现价 × 余额 (%.2f), 数值列疑似错位", t.name, r.code.c_str(), mv, px * bal));
}

std::string Fills::tag() { return misc::dense(misc::today_ymd()); }

void Fills::check(const Shape &t, const Row &r) {
  mem::need_row(t, r.qty > 0 && r.price > 0 && !r.fill_id.empty(), r.code, "成交行数值异常");
  mem::need_row(t, mem::isclose(r.price * r.qty, r.amount, 1e-4, 0.02), r.code, "金额 ≠ 价 × 量");
}

void Pendings::check(const Shape &t, const Row &r) {
  mem::need_row(t, !r.status.empty() && !r.order_id.empty(), r.code, "挂单状态 / 委托编号为空");
  mem::need_row(t, r.price > 0 && r.qty > 0 && 0 <= r.filled && r.filled < r.qty, r.code, "挂单量价异常");
}

void Debts::check(const Shape &t, const Row &r) { mem::need_row(t, r.qty >= 0 && r.amount >= 0, r.code, "融资负债数值异常"); }

void Assets::check(const Shape &t, const Row &r) { mem::need_row(t, !r.kv[0].first.empty(), "", "行首组名称为空"); }

void Margins::check(const Shape &t, const Row &r) { mem::need_row(t, r.ratio > 0, r.code, "融资保证金比例不为正"); }

void Collats::check(const Shape &t, const Row &r) { mem::need_row(t, 0 <= r.rate && r.rate <= 1, r.code, "担保折算率不在 [0, 1]"); }

} // namespace live::tdx::tables
