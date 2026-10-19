// fund — 日频 PIT 基本面实现 (qmt pit.cpp + def/ 链路移植)
//
// 结构: Panel::build   parquet 月度分片 → 网格 (raw cutoff 单点应用, ffill, 切到 [首回测日−15, 末]) + per-A 事件链 (v 升序)
//                     全部 15 张月度表登记进同一个 fundamental::Scan (列裁剪 + 字典键 + 跨表重段先跑)
//       Stream        per-A 沿 D 轴逐日推进的状态机 (估值分母 / 因子 raw / filter), 与 qmt 批量扫描逐日等价:
//                       ttm/balance 取 latest (上市前事件丢弃), 年报 annuals, 分红 365 日滑窗 + 3 年双阈值,
//                       预亏/营收区间 = "d < max{off : on ≤ d}" (区间并集的流式形式), 行业 replay, trading_st 连续计数
//                     每日产出 Fund::kCount 列 (缺失 = NaN, fp16 存不下的极值也归 NaN).
//
// 注意: 本文件依赖 NaN 语义, 必须在 CMake PRECISE_MATH 列表里 (-fno-fast-math).
#include "features/Method/Fundamental.hpp"

#include "features/Node/Common/Fund/Fund.hpp" // Fund::Out 输出行布局
#include "fundamental/Scan.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "misc/parquet.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <map>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fund {
namespace {

namespace pq = misc::pq;
using fundamental::Append;
using fundamental::Part;
using fundamental::Rows;
using fundamental::Sink;
using misc::Ymd;

constexpr float NaNF = std::numeric_limits<float>::quiet_NaN();
constexpr float InfF = std::numeric_limits<float>::infinity();

// NaN (缺失) 透传给 ffill; finite 违反约束 → +inf 标记"业务异常" (ffill 不传播).
inline float positive_or_inf(float v) {
  if (std::isnan(v))
    return v;
  return (std::isfinite(v) && v > 0.0f) ? v : InfF;
}
inline float non_negative_or_inf(float v) {
  if (std::isnan(v))
    return v;
  return (std::isfinite(v) && v >= 0.0f) ? v : InfF;
}
// 财报合成项: 分项缺失视为 0, 全部缺失 → NaN
inline float sum_nan0(std::initializer_list<float> xs) {
  double s = 0.0;
  bool any = false;
  for (float x : xs)
    if (std::isfinite(x)) {
      s += static_cast<double>(x);
      any = true;
    }
  return any ? static_cast<float>(s) : NaNF;
}
// 首选缺失 → 回退
inline float first_finite(float a, float b) { return std::isfinite(a) ? a : b; }

inline int year_of(Ymd d) { return d / 10000; }
inline int month_of(Ymd d) { return d / 100 % 100; }

// SW2021 一级行业表 (SW2021_L1_NAMES / sw2021_l1_name_to_id) 见 Fundamental.hpp

// ============================================================================
// 轴 + 静态 meta
// ============================================================================
struct Axes {
  std::vector<Ymd> dates; // 升序, 全交易日历截到 today
  std::vector<std::chrono::sys_days> date_days;
  int n_a_ = 0;

  int n_d() const { return static_cast<int>(dates.size()); }
  int n_a() const { return n_a_; }

  // min{i : dates[i] >= d}; 无 → n_d
  int lower(Ymd d) const {
    return static_cast<int>(std::lower_bound(dates.begin(), dates.end(), d) - dates.begin());
  }
  // d 是交易日 → 其下标; 否则 -1
  int exact(Ymd d) const {
    const int i = lower(d);
    return (i < n_d() && dates[static_cast<std::size_t>(i)] == d) ? i : -1;
  }
  // max{i : dates[i] <= d}; d < dates[0] → -1 (事件 visible → 上一交易日)
  int floor(Ymd d) const {
    return static_cast<int>(std::upper_bound(dates.begin(), dates.end(), d) - dates.begin()) - 1;
  }
};

struct Meta {
  // 与 codes 同序; 0 = 缺失 (list 缺失 = 永未上市)
  std::vector<Ymd> list, delist;
  std::vector<std::chrono::sys_days> list_day, delist_day; // 同上, 仅 != 0 时有效
  std::vector<std::uint8_t> main_board;                    // list_sector == 1
};

// 上市日在 D 轴的 lower_bound; 无 list_date → n_d (永未上市)
int get_list_d(int a, const Axes &axes, const Meta &meta) {
  const Ymd ld = meta.list[static_cast<std::size_t>(a)];
  return ld ? axes.lower(ld) : axes.n_d();
}

// ============================================================================
// PIT 池 (qmt PitPool 精简版: 只留本模块用到的字段)
// ============================================================================
struct FinancialTtmEv {
  std::int32_t v;
  std::int32_t report_date;
  float total_operating_revenue_ttm;
  float net_profit_to_parent_shareholders_ttm;
  float net_profit_ttm; // 含少数 (roa 分子)
  float net_cffoa_ttm;
  float net_cffoa_ttm_shift4;
};
struct FinancialBalanceEv {
  std::int32_t v;
  std::int32_t report_date;
  float total_equity_to_parent_shareholders;
  float total_assets;
  // MRQ 原始项 (Fund::bs_*); 合成项在读入时 NaN→0 求和, 全缺 → NaN
  float total_liabilities;
  float total_noncurr_liabilities;
  float ib_debt; // 有息负债 = 短期借款 + 一年内到期非流动负债 + 长期借款 + 应付债券
  float total_current_assets;
  float total_current_liabilities;
  float moneytary_assets;
  float inventories;
  float accounts_receivable;
  float accounts_payable;
  float fixed_cip; // 固定资产 + 在建工程
  float intang_gw; // 无形资产 + 商誉
  float minority_interests;
};
// 利润表 / 现金流量表 YTD 事件 (每公告一行, 含追溯重述行; 值 = 报告期年初至今累计).
// 三口径 (Q 单季 / TTM / LYR) 在 State 里由 report_date 差分得到, 不依赖 ttm 表.
enum IncomeItem : std::size_t {
  inc_rev,       // 营业总收入
  inc_cogs,      // 营业成本
  inc_cost,      // 营业总成本
  inc_op,        // 营业利润
  inc_ebit,      // 利润总额 + 利息费用 (利息费用缺 → 财务费用)
  inc_int,       // 利息费用 (同上回退)
  inc_tax,       // 所得税
  inc_np,        // 净利润 (含少数)
  inc_np_parent, // 归母净利润
  inc_eps,       // 基本 EPS [元]
  INC_COUNT
};
enum CashflowItem : std::size_t {
  cf_ocf,   // 经营活动现金流净额
  cf_icf,   // 投资活动现金流净额
  cf_fin,   // 筹资活动现金流净额
  cf_net,   // 现金及等价物净增加
  cf_capex, // 购建固定/无形/长期资产支付
  cf_div,   // 分配股利利润或偿付利息支付
  cf_da,    // 折旧 + 无形摊销 + 长期待摊摊销 (间接法附表, 多为年报/半年报)
  CF_COUNT
};
template <std::size_t N>
struct YtdEv {
  std::int32_t v;
  std::int32_t report_date;
  float x[N];
};
struct NonrecurEv { // cn_stock_financial_notes_shift (shift=0): 非经常性损益 TTM (归母口径)
  std::int32_t v;
  std::int32_t report_date;
  float nonrecurring_ttm;
};
struct SharesEv { // total_shares 全史网格变点 (切片前抽取; shares_issued_yoy 回看 1 年)
  int d;
  float shares;
};
struct FinancialIncomeAnnualEv {
  std::int32_t v;
  std::int32_t report_date;
  float net_profit_to_parent_shareholders;
};
struct DividendEv {
  std::int32_t v;
  std::int32_t report_date;
  float cash_before_tax;
  float cash_after_tax;
  // 公告日股本快照 × 每股现金 (build 末尾按全史网格标注, 之后网格切片不再覆盖历史事件)
  float amt_pre;   // dy 365 日滑窗: cash_before_tax × shares[v]; 非 finite / ≤0 → 0
  float amt_after; // dividend_st 3 年累计: cash_after_tax × shares[v]; 任一非 finite → NaN (跳过)
};
enum class ForecastType : std::uint8_t { Other = 0,
                                         FirstLoss = 1,
                                         ContinueLoss = 2 };
struct ForecastEv {
  std::int32_t v;
  std::int32_t end_date;
  ForecastType type;
  float last_parent_net;
};
struct IndustryEv {
  std::int32_t v;
  std::uint8_t l1_id;
};

struct PitPool {
  // 网格 [a-major, d-minor]; build 期覆盖全 D 轴 (g0=0, n_g=n_d), slice() 后只留 [g0, g0+n_g)
  int g0 = 0, n_g = 0;
  template <class T>
  T at(const std::vector<T> &g, int a, int d) const {
    assert(d >= g0 && d < g0 + n_g && "网格切片不覆盖该日");
    return g[static_cast<std::size_t>(a) * static_cast<std::size_t>(n_g) + static_cast<std::size_t>(d - g0)];
  }

  std::vector<float> close;          // bar1d.close (不复权, cutoff=-1, ffill)
  std::vector<float> pre_close;      // bar1d.pre_close (除权后前收, cutoff=0: 交易所盘前公布, ffill)
  std::vector<float> adj_factor;     // bar1d.adjust_factor (cutoff=0: 除权除息事先公告, ffill)
  std::vector<float> total_shares;   // cutoff=0 (按生效日记账, 与 adj_factor 同日), ffill
  std::vector<float> a_float_shares; // cutoff=0, ffill
  std::vector<float> lim_up;         // cutoff=0: row T = T 当日适用 (源表按适用日记账, 由 pre_close 推出, 盘前可知), ffill
  std::vector<float> lim_dn;
  std::vector<std::int8_t> st_status; // cutoff=0, 4 态派生, 不 ffill
  std::vector<std::uint8_t> suspended;
  std::vector<std::uint8_t> is_margin; // cutoff=0
  std::vector<float> fin_balance;      // 融资余额 (cutoff=-1: T 日盘后数据 T+1 可用), 不 ffill
  std::vector<float> sec_balance;      // 融券余额
  std::vector<float> fin_purchase;     // 融资买入额
  std::vector<float> fin_repayment;    // 融资偿还额
  std::vector<float> sec_sales_qty;    // 融券卖出量 [股]
  std::vector<float> sec_repay_qty;    // 融券偿还量 [股]

  // 事件 (per-a, v 升序)
  std::vector<std::vector<FinancialTtmEv>> ttm;
  std::vector<std::vector<FinancialBalanceEv>> balance;
  std::vector<std::vector<FinancialIncomeAnnualEv>> income_annual;
  std::vector<std::vector<YtdEv<INC_COUNT>>> income_ytd;
  std::vector<std::vector<YtdEv<CF_COUNT>>> cashflow_ytd;
  std::vector<std::vector<NonrecurEv>> nonrecur;
  std::vector<std::vector<DividendEv>> dividend;
  std::vector<std::vector<ForecastEv>> forecast;
  std::vector<std::vector<IndustryEv>> industry_component;
  std::vector<std::vector<IndustryEv>> industry_change;
  std::vector<std::vector<SharesEv>> shares_chg; // d 升序 (build 末尾从全史 total_shares 网格抽取)

  // 网格切到 [new_g0, n_d): 逐列拷贝 (峰值 = 全网格 + 一列切片), 事件链不动
  void slice(int n_a, int new_g0) {
    assert(g0 == 0 && new_g0 >= 0 && new_g0 <= n_g);
    const int nn = n_g - new_g0;
    auto cut = [&](auto &g) {
      using V = std::decay_t<decltype(g)>;
      V out(static_cast<std::size_t>(n_a) * static_cast<std::size_t>(nn));
      for (int a = 0; a < n_a; ++a)
        std::copy_n(g.begin() + static_cast<std::ptrdiff_t>(a) * n_g + new_g0, nn,
                    out.begin() + static_cast<std::ptrdiff_t>(a) * nn);
      g.swap(out);
    };
    cut(close);
    cut(pre_close);
    cut(adj_factor);
    cut(total_shares);
    cut(a_float_shares);
    cut(lim_up);
    cut(lim_dn);
    cut(st_status);
    cut(suspended);
    cut(is_margin);
    cut(fin_balance);
    cut(sec_balance);
    cut(fin_purchase);
    cut(fin_repayment);
    cut(sec_sales_qty);
    cut(sec_repay_qty);
    g0 = new_g0;
    n_g = nn;
  }
};

} // namespace

// ============================================================================
// Data: Panel 的全部内容 (只读共享)
// ============================================================================
struct Data {
  Axes axes;
  Meta meta;
  PitPool pit;
  int axes_warmup_d = 0; // dividend_st 数据轴 warmup: 轴起点 + 3 年
};

namespace {

// ============================================================================
// vd 日 → D 轴 row (qmt GridRowMemo / EventRowMemo 合一)
//   Exact: 须是交易日 (网格);  Floor: 落到 ≤ 它的最近交易日 (事件)
//   再按 cutoff 平移 (row = idx − cutoff), 越界 / 缺失 → -1. 行按日成块, 只在换日时查一次.
// ============================================================================
enum class Align { Exact,
                   Floor };

class RowOf {
public:
  RowOf(const Axes &axes, Align align, int cutoff) : axes_(axes), align_(align), cutoff_(cutoff) {}
  int operator()(Ymd d) {
    if (d != last_) {
      last_ = d;
      row_ = resolve(d);
    }
    return row_;
  }

private:
  int resolve(Ymd d) const {
    if (d <= 0)
      return -1;
    const int i = align_ == Align::Exact ? axes_.exact(d) : axes_.floor(d);
    const int r = i - cutoff_;
    return (i >= 0 && r >= 0 && r < axes_.n_d()) ? r : -1;
  }
  const Axes &axes_;
  Align align_;
  int cutoff_;
  Ymd last_ = -1;
  int row_ = -1;
};

// 事件命中 (资产, 事件); 段序归约后 = (ym, 文件内) 序
template <class Ev>
struct Hit {
  std::uint32_t a;
  Ev ev;
};
template <class Ev>
using Hits = Append<Hit<Ev>>;

// 命中 → per-A 事件链 (v 升序; 同 v 保持段序 —— 结果与线程调度无关)
template <class Ev>
std::vector<std::vector<Ev>> to_chains(const Hits<Ev> &h, int n_a) {
  std::vector<std::size_t> counts(static_cast<std::size_t>(n_a), 0);
  for (const Hit<Ev> &x : h.v)
    ++counts[x.a];
  std::vector<std::vector<Ev>> out(static_cast<std::size_t>(n_a));
  for (std::size_t a = 0; a < out.size(); ++a)
    out[a].reserve(counts[a]);
  for (const Hit<Ev> &x : h.v)
    out[x.a].push_back(x.ev);
  for (auto &c : out)
    std::stable_sort(c.begin(), c.end(), [](const Ev &x, const Ev &y) { return x.v < y.v; });
  return out;
}

// 网格 per-A forward fill: finite 记 last; NaN 用 last 填; +inf 保留标记
void grid_ffill(std::vector<float> &grid, int n_a, int n_d) {
  for (int a = 0; a < n_a; ++a) {
    std::size_t base = static_cast<std::size_t>(a) * static_cast<std::size_t>(n_d);
    float last = NaNF;
    for (int d = 0; d < n_d; ++d) {
      float v = grid[base + static_cast<std::size_t>(d)];
      if (std::isfinite(v))
        last = v;
      else if (std::isnan(v) && std::isfinite(last))
        grid[base + static_cast<std::size_t>(d)] = last;
    }
  }
}

// ============================================================================
// itf 构建 (qmt pit.cpp 各 itf_* 移植; cutoff 语义逐一保持)
// ============================================================================
// 网格表 (5 张): fold 直写 p 的全 D 轴网格. 同一 (a, row) 只来自唯一一个源日期 (row = idx − cutoff
// 是双射) ⇒ 分片写集不相交, 免锁 (Sink). add 登记, Scan::run 之后 finish 做 ffill.
struct GridScan {
  Part<Sink> bar, shares, limit, status, margin;
  void add(fundamental::Scan &scan, const Axes &axes, PitPool &p);
  void finish(const Axes &axes, PitPool &p) const;
};

void GridScan::add(fundamental::Scan &scan, const Axes &axes, PitPool &p) {
  using fundamental::spec;
  const std::size_t n = static_cast<std::size_t>(axes.n_a()) *
                        static_cast<std::size_t>(axes.n_d());
  const std::size_t n_d = static_cast<std::size_t>(axes.n_d());
  p.g0 = 0;
  p.n_g = axes.n_d();

  auto alloc_f = [&](std::vector<float> &g) { g.assign(n, NaNF); };
  alloc_f(p.close);
  alloc_f(p.pre_close);
  alloc_f(p.adj_factor);
  alloc_f(p.total_shares);
  alloc_f(p.a_float_shares);
  alloc_f(p.lim_up);
  alloc_f(p.lim_dn);
  alloc_f(p.fin_balance);
  alloc_f(p.sec_balance);
  alloc_f(p.fin_purchase);
  alloc_f(p.fin_repayment);
  alloc_f(p.sec_sales_qty);
  alloc_f(p.sec_repay_qty);
  p.st_status.assign(n, 0);
  p.suspended.assign(n, 0);
  p.is_margin.assign(n, 0);

  // ---- cn_stock_real_bar1d: close CUTOFF=-1; pre_close / adjust_factor CUTOFF=0 (盘前可知: 交易所公布前收, 除权事先公告) ----
  scan.add(bar, spec("cn_stock_real_bar1d"), {"close", "pre_close", "adjust_factor"}, Sink{},
           [&axes, &p, n_d](const Rows &r, Sink &) {
             const pq::Col close = r.col("close"), pre = r.col("pre_close"), adj = r.col("adjust_factor");
             RowOf row_of(axes, Align::Exact, -1), row0_of(axes, Align::Exact, 0);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               const std::size_t base = static_cast<std::size_t>(a) * n_d;
               const int row = row_of(d);
               if (row >= 0)
                 p.close[base + static_cast<std::size_t>(row)] = positive_or_inf(close.f32(i));
               const int row0 = row0_of(d);
               if (row0 >= 0) {
                 p.pre_close[base + static_cast<std::size_t>(row0)] = positive_or_inf(pre.f32(i));
                 p.adj_factor[base + static_cast<std::size_t>(row0)] = positive_or_inf(adj.f32(i));
               }
             });
           });

  // ---- cn_stock_shares (CUTOFF=0: 按生效日记账, 除权日与 adj_factor 同日变动, 盘前公告可知;
  //      曾误按 -1 → 送转/除权日股本滞后一天而价格已调整, mcap/换手率当日错位) ----
  scan.add(shares, spec("cn_stock_shares"), {"total_shares", "a_float_shares"}, Sink{},
           [&axes, &p, n_d](const Rows &r, Sink &) {
             const pq::Col ts = r.col("total_shares"), fs = r.col("a_float_shares");
             RowOf row_of(axes, Align::Exact, 0);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               const int row = row_of(d);
               if (row < 0)
                 return;
               const std::size_t off = static_cast<std::size_t>(a) * n_d + static_cast<std::size_t>(row);
               p.total_shares[off] = positive_or_inf(ts.f32(i));
               p.a_float_shares[off] = positive_or_inf(fs.f32(i));
             });
           });

  // ---- cn_stock_limit_price (CUTOFF=0: date=T 行即 T 当日适用涨跌停, 由 pre_close(T) 推出,
  //      盘前可知, 同 pre_close/adj_factor; 曾误按 -1 解析 → 全列滞后一天, 涨跌停次日必被打穿) ----
  scan.add(limit, spec("cn_stock_limit_price"), {"upper_limit", "lower_limit"}, Sink{},
           [&axes, &p, n_d](const Rows &r, Sink &) {
             const pq::Col up = r.col("upper_limit"), dn = r.col("lower_limit");
             RowOf row_of(axes, Align::Exact, 0);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               const int row = row_of(d);
               if (row < 0)
                 return;
               const std::size_t off = static_cast<std::size_t>(a) * n_d + static_cast<std::size_t>(row);
               p.lim_up[off] = positive_or_inf(up.f32(i));
               p.lim_dn[off] = positive_or_inf(dn.f32(i));
             });
           });

  // ---- cn_stock_status (CUTOFF=0, 4 态派生; 不 ffill) ----
  scan.add(status, spec("cn_stock_status"), {"st_status", "is_risk_warning", "suspended"}, Sink{},
           [&axes, &p, n_d](const Rows &r, Sink &) {
             const pq::Col st_c = r.col("st_status"), rw_c = r.col("is_risk_warning");
             const pq::Col sp_c = r.col("suspended");
             RowOf row_of(axes, Align::Exact, 0);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               const int row = row_of(d);
               if (row < 0)
                 return;
               const std::size_t off = static_cast<std::size_t>(a) * n_d + static_cast<std::size_t>(row);
               const int st = st_c.i32(i, 0), rw = rw_c.i32(i, 0), sp = sp_c.i32(i, 0);
               // 4 态派生: st 1/2 优先; 否则 risk_warning=1 → 3 (退市整理期)
               p.st_status[off] = (st == 1)   ? std::int8_t{1}
                                  : (st == 2) ? std::int8_t{2}
                                  : (rw != 0) ? std::int8_t{3}
                                              : std::int8_t{0};
               p.suspended[off] = (sp != 0) ? std::uint8_t{1} : std::uint8_t{0};
             });
           });

  // ---- cn_stock_margin_trading_detail (CUTOFF=-1; 不 ffill): date=T 行是 T 日盘后余额/流量
  //      (余额差 = 当行买入-偿还, 已验证), 交易所 T+1 盘前公布 → T+1 起可用;
  //      曾误按 0 解析 = 盘中用当日盘后数据 (未来函数) ----
  scan.add(margin, spec("cn_stock_margin_trading_detail"),
           {"financing_balance", "securities_lending_balance", "financing_purchase", "financing_repayment",
            "securities_lending_sales_quantity", "securities_lending_repayment_quantity"},
           Sink{}, [&axes, &p, n_d](const Rows &r, Sink &) {
             const pq::Col fb = r.col("financing_balance");
             const pq::Col sb = r.col("securities_lending_balance");
             const pq::Col fp = r.col("financing_purchase"), fr = r.col("financing_repayment");
             const pq::Col ss = r.col("securities_lending_sales_quantity");
             const pq::Col sr = r.col("securities_lending_repayment_quantity");
             RowOf row_of(axes, Align::Exact, -1);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               const int row = row_of(d);
               if (row < 0)
                 return;
               const std::size_t off = static_cast<std::size_t>(a) * n_d + static_cast<std::size_t>(row);
               p.is_margin[off] = 1;
               p.fin_balance[off] = non_negative_or_inf(fb.f32(i));
               p.sec_balance[off] = non_negative_or_inf(sb.f32(i));
               p.fin_purchase[off] = non_negative_or_inf(fp.f32(i));
               p.fin_repayment[off] = non_negative_or_inf(fr.f32(i));
               p.sec_sales_qty[off] = non_negative_or_inf(ss.f32(i));
               p.sec_repay_qty[off] = non_negative_or_inf(sr.f32(i));
             });
           });
}

void GridScan::finish(const Axes &axes, PitPool &p) const {
  grid_ffill(p.close, axes.n_a(), axes.n_d());
  grid_ffill(p.pre_close, axes.n_a(), axes.n_d());
  grid_ffill(p.adj_factor, axes.n_a(), axes.n_d());
  grid_ffill(p.total_shares, axes.n_a(), axes.n_d());
  grid_ffill(p.a_float_shares, axes.n_a(), axes.n_d());
  grid_ffill(p.lim_up, axes.n_a(), axes.n_d());
  grid_ffill(p.lim_dn, axes.n_a(), axes.n_d());
}

// 事件表 (9 张): fold 只产命中 (Hits, 段内追加), Scan::run 之后 finish 按资产分桶成链 (v 升序).
// 原先逐资产一把 mutex + 追加顺序随线程调度漂移; 现在零锁且同 v 事件的先后确定.
struct IncomeHits {
  Hits<YtdEv<INC_COUNT>> ytd;
  Hits<FinancialIncomeAnnualEv> annual;
  void merge(IncomeHits &&o) {
    ytd.merge(std::move(o.ytd));
    annual.merge(std::move(o.annual));
  }
};

struct EventScan {
  Part<Hits<FinancialTtmEv>> ttm;
  Part<Hits<FinancialBalanceEv>> balance;
  Part<IncomeHits> income;
  Part<Hits<YtdEv<CF_COUNT>>> cashflow;
  Part<Hits<NonrecurEv>> nonrecur;
  Part<Hits<DividendEv>> dividend;
  Part<Hits<ForecastEv>> forecast;
  Part<Hits<IndustryEv>> component, change;
  void add(fundamental::Scan &scan, const Axes &axes);
  void finish(int n_a, PitPool &p) &&;
};

void EventScan::add(fundamental::Scan &scan, const Axes &axes) {
  using fundamental::spec;

  // ---- cn_stock_financial_ttm_shift (CUTOFF=-1; shift=0 主记录 + shift=4 配对) ----
  scan.add(ttm, spec("cn_stock_financial_ttm_shift"),
           {"shift", "report_date", "total_operating_revenue_ttm", "net_profit_to_parent_shareholders_ttm",
            "net_profit_ttm", "net_cffoa_ttm"},
           Hits<FinancialTtmEv>{}, [&axes](const Rows &r, Hits<FinancialTtmEv> &h) {
             const pq::Col shift = r.col("shift"), rd = r.col("report_date");
             const pq::Col rev = r.col("total_operating_revenue_ttm");
             const pq::Col np = r.col("net_profit_to_parent_shareholders_ttm");
             const pq::Col npa = r.col("net_profit_ttm");
             const pq::Col cf = r.col("net_cffoa_ttm");
             auto key_of = [&](Ymd d, std::uint32_t a) {
               return static_cast<std::int64_t>(d) * (axes.n_a() + 1) + a;
             };

             std::unordered_map<std::int64_t, float> shift4_cf;
             shift4_cf.reserve(static_cast<std::size_t>(r.size()) / 32 + 1);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               if (shift.i32(i, -1) == 4)
                 shift4_cf[key_of(d, a)] = cf.f32(i);
             });

             RowOf row_of(axes, Align::Floor, -1);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               if (shift.i32(i, -1) != 0)
                 return;
               const int row = row_of(d);
               if (row < 0)
                 return;
               FinancialTtmEv ev;
               ev.v = row;
               ev.report_date = rd.yyyymmdd(i);
               ev.total_operating_revenue_ttm = rev.f32(i);
               ev.net_profit_to_parent_shareholders_ttm = np.f32(i);
               ev.net_profit_ttm = npa.f32(i);
               ev.net_cffoa_ttm = cf.f32(i);
               auto it = shift4_cf.find(key_of(d, a));
               ev.net_cffoa_ttm_shift4 = (it != shift4_cf.end()) ? it->second : NaNF;
               h.push({a, ev});
             });
           });

  // ---- cn_stock_financial_balance_general_pit (CUTOFF=-1; 全报告期入) ----
  scan.add(
      balance, spec("cn_stock_financial_balance_general_pit"),
      {"report_date", "total_equity_to_parent_shareholders", "total_assets", "total_liabilities",
       "total_noncurr_liabilities", "shortterm_borrowings", "noncurr_liabilities_due_within_1y",
       "longterm_borrowings", "bonds_payable", "total_current_assets", "total_current_liabilities",
       "moneytary_assets", "inventories", "accounts_receivable", "accounts_payable", "fixed_assets",
       "fixed_assets_sum", "construction_in_progress", "construction_in_progress_sum", "intangible_assets",
       "goodwill", "minority_interests"},
      Hits<FinancialBalanceEv>{}, [&axes](const Rows &r, Hits<FinancialBalanceEv> &h) {
        const pq::Col rd = r.col("report_date");
        const pq::Col tep = r.col("total_equity_to_parent_shareholders");
        const pq::Col ta = r.col("total_assets");
        const pq::Col tl = r.col("total_liabilities"), ncl = r.col("total_noncurr_liabilities");
        const pq::Col stb = r.col("shortterm_borrowings"), ncl1y = r.col("noncurr_liabilities_due_within_1y");
        const pq::Col ltb = r.col("longterm_borrowings"), bonds = r.col("bonds_payable");
        const pq::Col ca = r.col("total_current_assets"), cl = r.col("total_current_liabilities");
        const pq::Col cash = r.col("moneytary_assets"), inv = r.col("inventories");
        const pq::Col ar = r.col("accounts_receivable"), ap = r.col("accounts_payable");
        // 2018 报表格式后 fixed_assets / construction_in_progress 半数为空, 值在 *_sum (固定资产合计 / 在建工程合计); 旧年份反之
        const pq::Col fa = r.col("fixed_assets"), fa_sum = r.col("fixed_assets_sum");
        const pq::Col cip = r.col("construction_in_progress"), cip_sum = r.col("construction_in_progress_sum");
        const pq::Col ia = r.col("intangible_assets"), gw = r.col("goodwill");
        const pq::Col mi = r.col("minority_interests");
        RowOf row_of(axes, Align::Floor, -1);
        r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
          const int row = row_of(d);
          if (row < 0)
            return;
          FinancialBalanceEv ev;
          ev.v = row;
          ev.report_date = rd.yyyymmdd(i);
          ev.total_equity_to_parent_shareholders = tep.f32(i);
          ev.total_assets = ta.f32(i);
          ev.total_liabilities = tl.f32(i);
          ev.total_noncurr_liabilities = ncl.f32(i);
          ev.ib_debt = sum_nan0({stb.f32(i), ncl1y.f32(i), ltb.f32(i), bonds.f32(i)});
          ev.total_current_assets = ca.f32(i);
          ev.total_current_liabilities = cl.f32(i);
          ev.moneytary_assets = cash.f32(i);
          ev.inventories = inv.f32(i);
          ev.accounts_receivable = ar.f32(i);
          ev.accounts_payable = ap.f32(i);
          ev.fixed_cip = sum_nan0({first_finite(fa_sum.f32(i), fa.f32(i)), first_finite(cip_sum.f32(i), cip.f32(i))});
          ev.intang_gw = sum_nan0({ia.f32(i), gw.f32(i)});
          ev.minority_interests = mi.f32(i);
          h.push({a, ev});
        });
      });

  // ---- cn_stock_financial_income_general_pit (CUTOFF=-1; 年报链 ni_raw + 全报告期 YTD 链) ----
  scan.add(
      income, spec("cn_stock_financial_income_general_pit"),
      {"fs_quarter_index", "report_date", "net_profit_to_parent_shareholders", "total_operating_revenue",
       "operating_costs", "total_operating_costs", "operating_profit", "total_profit", "fin_interest_expense",
       "finance_expense", "income_tax_expense", "net_profit", "eps_basic"},
      IncomeHits{}, [&axes](const Rows &r, IncomeHits &h) {
        const pq::Col fqi = r.col("fs_quarter_index"), rd = r.col("report_date");
        const pq::Col np = r.col("net_profit_to_parent_shareholders");
        const pq::Col rev = r.col("total_operating_revenue"), cogs = r.col("operating_costs");
        const pq::Col cost = r.col("total_operating_costs"), op = r.col("operating_profit");
        const pq::Col tp = r.col("total_profit"), fie = r.col("fin_interest_expense"), fe = r.col("finance_expense");
        const pq::Col tax = r.col("income_tax_expense"), npa = r.col("net_profit"), eps = r.col("eps_basic");
        RowOf row_of(axes, Align::Floor, -1);
        r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
          const int row = row_of(d);
          if (row < 0)
            return;
          YtdEv<INC_COUNT> ev;
          ev.v = row;
          ev.report_date = rd.yyyymmdd(i);
          const float interest = first_finite(fie.f32(i), fe.f32(i));
          ev.x[inc_rev] = rev.f32(i);
          ev.x[inc_cogs] = cogs.f32(i);
          ev.x[inc_cost] = cost.f32(i);
          ev.x[inc_op] = op.f32(i);
          ev.x[inc_ebit] = tp.f32(i) + interest; // 任一缺 → NaN
          ev.x[inc_int] = interest;
          ev.x[inc_tax] = tax.f32(i);
          ev.x[inc_np] = npa.f32(i);
          ev.x[inc_np_parent] = np.f32(i);
          ev.x[inc_eps] = eps.f32(i);
          h.ytd.push({a, ev});
          if (fqi.i32(i, -1) == 4)
            h.annual.push({a, {ev.v, ev.report_date, ev.x[inc_np_parent]}});
        });
      });

  // ---- cn_stock_financial_cashflow_general_pit (CUTOFF=-1; 全报告期 YTD 链) ----
  scan.add(
      cashflow, spec("cn_stock_financial_cashflow_general_pit"),
      {"report_date", "net_cffoa", "net_cffia", "net_cfffa", "netinc_in_cce", "cash_paid_for_filt_assets",
       "cash_paid_for_dividends_profits_interests", "depreciation_of_fa_oga_pba",
       "amorization_of_intangible_assets", "amortization_of_longterm_deferred_expenses"},
      Hits<YtdEv<CF_COUNT>>{}, [&axes](const Rows &r, Hits<YtdEv<CF_COUNT>> &h) {
        const pq::Col rd = r.col("report_date");
        const pq::Col ocf = r.col("net_cffoa"), icf = r.col("net_cffia"), fcf = r.col("net_cfffa");
        const pq::Col net = r.col("netinc_in_cce"), capex = r.col("cash_paid_for_filt_assets");
        const pq::Col div = r.col("cash_paid_for_dividends_profits_interests");
        const pq::Col dep = r.col("depreciation_of_fa_oga_pba"), am_ia = r.col("amorization_of_intangible_assets");
        const pq::Col am_ld = r.col("amortization_of_longterm_deferred_expenses");
        RowOf row_of(axes, Align::Floor, -1);
        r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
          const int row = row_of(d);
          if (row < 0)
            return;
          YtdEv<CF_COUNT> ev;
          ev.v = row;
          ev.report_date = rd.yyyymmdd(i);
          ev.x[cf_ocf] = ocf.f32(i);
          ev.x[cf_icf] = icf.f32(i);
          ev.x[cf_fin] = fcf.f32(i);
          ev.x[cf_net] = net.f32(i);
          ev.x[cf_capex] = capex.f32(i);
          ev.x[cf_div] = div.f32(i);
          ev.x[cf_da] = sum_nan0({dep.f32(i), am_ia.f32(i), am_ld.f32(i)});
          h.push({a, ev});
        });
      });

  // ---- cn_stock_financial_notes_shift (CUTOFF=-1; shift=0 非经常性损益 TTM) ----
  //   事件与 ttm_shift 一一对齐, 无附注数据的事件该列为 null: 丢弃, 否则顶掉同报告期的有效值
  scan.add(nonrecur, spec("cn_stock_financial_notes_shift"),
           {"shift", "report_date", "nonrecurring_income_to_owner_ttm"}, Hits<NonrecurEv>{},
           [&axes](const Rows &r, Hits<NonrecurEv> &h) {
             const pq::Col shift = r.col("shift"), rd = r.col("report_date");
             const pq::Col nr_owner = r.col("nonrecurring_income_to_owner_ttm");
             RowOf row_of(axes, Align::Floor, -1);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               if (shift.i32(i, -1) != 0 || nr_owner.null(i))
                 return;
               const int row = row_of(d);
               if (row < 0)
                 return;
               h.push({a, {row, rd.yyyymmdd(i), nr_owner.f32(i)}});
             });
           });

  // ---- cn_stock_dividend (CUTOFF=-1; v ← publish_date = vd) ----
  scan.add(dividend, spec("cn_stock_dividend"), {"report_date", "cash_before_tax", "cash_after_tax"},
           Hits<DividendEv>{}, [&axes](const Rows &r, Hits<DividendEv> &h) {
             const pq::Col rd = r.col("report_date");
             const pq::Col cash_b = r.col("cash_before_tax");
             const pq::Col cash = r.col("cash_after_tax");
             RowOf row_of(axes, Align::Floor, -1);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               const int row = row_of(d);
               if (row < 0)
                 return;
               DividendEv ev;
               ev.v = row;
               ev.report_date = rd.yyyymmdd(i);
               ev.cash_before_tax = cash_b.f32(i);
               ev.cash_after_tax = cash.f32(i);
               ev.amt_pre = 0.0f; // build 末尾标注
               ev.amt_after = NaNF;
               h.push({a, ev});
             });
           });

  // ---- forecast (Tushare, CUTOFF=-1; vd = ann_date, 键 = ts_code, "YYYYMMDD" string 列) ----
  scan.add(forecast, spec("forecast"), {"end_date", "type", "last_parent_net"}, Hits<ForecastEv>{},
           [&axes](const Rows &r, Hits<ForecastEv> &h) {
             const pq::Col ed = r.col("end_date"), type = r.col("type");
             const pq::Col lpn = r.col("last_parent_net");
             RowOf row_of(axes, Align::Floor, -1);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               const int row = row_of(d);
               if (row < 0)
                 return;
               ForecastEv ev;
               ev.v = row;
               ev.end_date = ed.yyyymmdd(i);
               const std::string_view t = type.str(i);
               ev.type = (t == "首亏")   ? ForecastType::FirstLoss
                         : (t == "续亏") ? ForecastType::ContinueLoss
                                         : ForecastType::Other;
               ev.last_parent_net = lpn.f32(i);
               h.push({a, ev});
             });
           });

  // ---- cn_stock_industry_component / change (CUTOFF=-1, sw2021 L_min) ----
  scan.add(component, spec("cn_stock_industry_component"), {"industry", "industry_level1_name"},
           Hits<IndustryEv>{}, [&axes](const Rows &r, Hits<IndustryEv> &h) {
             const pq::Col ind = r.col("industry"), l1 = r.col("industry_level1_name");
             RowOf row_of(axes, Align::Floor, -1);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               if (ind.str(i) != "sw2021")
                 return;
               const int row = row_of(d);
               if (row < 0)
                 return;
               h.push({a, {row, sw2021_l1_name_to_id(l1.str(i))}});
             });
           });
  scan.add(change, spec("cn_stock_industry_change"), {"industry", "industry_level", "change_flag", "industry_name"},
           Hits<IndustryEv>{}, [&axes](const Rows &r, Hits<IndustryEv> &h) {
             const pq::Col ind = r.col("industry"), lvl = r.col("industry_level");
             const pq::Col flag = r.col("change_flag"), name = r.col("industry_name");
             RowOf row_of(axes, Align::Floor, -1);
             r.each([&](std::int64_t i, std::uint32_t a, Ymd d) {
               if (ind.str(i) != "sw2021" || lvl.i32(i, 0) != 1 || flag.i32(i, -1) != 1)
                 return;
               const int row = row_of(d);
               if (row < 0)
                 return;
               h.push({a, {row, sw2021_l1_name_to_id(name.str(i))}});
             });
           });
}

void EventScan::finish(int n_a, PitPool &p) && {
  IncomeHits inc = std::move(income).take();
  p.ttm = to_chains(std::move(ttm).take(), n_a);
  p.balance = to_chains(std::move(balance).take(), n_a);
  p.income_ytd = to_chains(inc.ytd, n_a);
  p.income_annual = to_chains(inc.annual, n_a);
  p.cashflow_ytd = to_chains(std::move(cashflow).take(), n_a);
  p.nonrecur = to_chains(std::move(nonrecur).take(), n_a);
  p.dividend = to_chains(std::move(dividend).take(), n_a);
  p.forecast = to_chains(std::move(forecast).take(), n_a);
  p.industry_component = to_chains(std::move(component).take(), n_a);
  p.industry_change = to_chains(std::move(change).take(), n_a);
}

// ============================================================================
// 财务 helper (qmt def/detail.hpp 移植)
// ============================================================================

// report_date 的上一个季末; 非标准季末 → 0
std::int32_t prev_quarter_end(std::int32_t rd) {
  std::int32_t y = rd / 10000, md = rd % 10000;
  switch (md) {
  case 1231:
    return y * 10000 + 930;
  case 930:
    return y * 10000 + 630;
  case 630:
    return y * 10000 + 331;
  case 331:
    return (y - 1) * 10000 + 1231;
  default:
    return 0;
  }
}

// 报告期季序 1..4; 非标准季末 → 0
inline int quarter_of(std::int32_t rd) {
  switch (rd % 10000) {
  case 331:
    return 1;
  case 630:
    return 2;
  case 930:
    return 3;
  case 1231:
    return 4;
  default:
    return 0;
  }
}

// YTD 链 → 三口径 (qmt 无此链路; 差分口径为标准定义):
//   Q   = YTD(R) − YTD(上季末 R)            (Q1 即 YTD)
//   TTM = YTD(R) + YTD(上年年报) − YTD(去年同期 R)  (年报即 YTD)
//   LYR = YTD(≤ R 的最近年报)
// 任一所需报告期缺失 → NaN; 各表 R 取各自 latest 的最大 report_date.
template <std::size_t N>
struct YtdState {
  std::map<std::int32_t, YtdEv<N>> by_rd; // report_date → latest (追溯重述覆盖)
  std::size_t p = 0;
  int last_v = -1; // 最近一次公告 row (上市前事件丢弃)

  void absorb(const std::vector<YtdEv<N>> &ev, int d, int list_d) {
    while (p < ev.size() && ev[p].v <= d) {
      if (ev[p].v >= list_d) {
        by_rd[ev[p].report_date] = ev[p];
        last_v = ev[p].v;
      }
      ++p;
    }
  }

  const float *find(std::int32_t rd) const {
    auto it = by_rd.find(rd);
    return it == by_rd.end() ? nullptr : it->second.x;
  }

  void emit(float *q, float *ttm, float *lyr) const {
    for (std::size_t i = 0; i < N; ++i)
      q[i] = ttm[i] = lyr[i] = NaNF;
    if (by_rd.empty())
      return;
    const std::int32_t R = by_rd.rbegin()->first;
    const int qi = quarter_of(R);
    if (qi == 0)
      return;
    const int y = year_of(R);
    const float *cur = by_rd.rbegin()->second.x;
    const float *prev_q = (qi == 1) ? nullptr : find(prev_quarter_end(R));
    const float *ly_annual = find((y - 1) * 10000 + 1231);
    const float *ly_same = find(R - 10000);
    // LYR: ≤ R 的最近年报 (R 本身为年报时即 R)
    const float *lyr_row = (qi == 4) ? cur : ly_annual;
    for (std::size_t i = 0; i < N; ++i) {
      if (qi == 1)
        q[i] = cur[i];
      else if (prev_q)
        q[i] = cur[i] - prev_q[i];
      if (qi == 4)
        ttm[i] = cur[i];
      else if (ly_annual && ly_same)
        ttm[i] = cur[i] + ly_annual[i] - ly_same[i];
      if (lyr_row)
        lyr[i] = lyr_row[i];
    }
  }
};

// TTM 窗口 5 点平均 (anchor + 前 4 季末; 任一缺失 → NaN)
float ttm_window_avg(std::int32_t anchor,
                     const std::map<std::int32_t, FinancialBalanceEv> &by_rd,
                     float FinancialBalanceEv::*field) {
  double sum = 0.0;
  std::int32_t rd = anchor;
  for (int i = 0; i < 5; ++i) {
    if (rd == 0)
      return NaNF;
    auto it = by_rd.find(rd);
    if (it == by_rd.end())
      return NaNF;
    float v = it->second.*field;
    if (!std::isfinite(v))
      return NaNF;
    sum += static_cast<double>(v);
    rd = prev_quarter_end(rd);
  }
  return static_cast<float>(sum / 5.0);
}

// forecast 触发 → 终止 d: min(对应 report_date 正式年报 PIT 首见 row, 次年 4/30 ceil)
int find_forecast_off_d(const ForecastEv &fe,
                        const std::vector<FinancialIncomeAnnualEv> &financials,
                        const Axes &axes) {
  int n_d = axes.n_d();
  int financial_d = -1;
  for (const auto &r : financials) {
    if (r.report_date == fe.end_date) {
      financial_d = r.v;
      break;
    }
  }
  const int Y = year_of(fe.end_date);
  const int deadline_d = Y > 0 ? axes.lower((Y + 1) * 10000 + 430) : -1;
  int off = n_d;
  if (financial_d >= 0)
    off = std::min(off, financial_d);
  if (deadline_d >= 0)
    off = std::min(off, deadline_d);
  return off;
}

// D 轴日 d 的年份
inline int year_of_d(const Axes &axes, int d) {
  return year_of(axes.dates[static_cast<std::size_t>(d)]);
}

// 首个年份 ≥ y 的 D 轴下标; 无 → n_d
inline int first_d_of_year(const Axes &axes, int y) { return axes.lower(y * 10000 + 101); }

} // namespace

// ============================================================================
// Panel
// ============================================================================
Panel::Panel() = default;
Panel::~Panel() = default;

int Panel::date_index(Ymd d) const { return data().axes.exact(d); }

void Panel::build(const std::vector<std::string> &codes,
                  const std::vector<Ymd> &dates) {
  assert(!codes.empty() && "AssetAxis 为空");
  assert(!dates.empty() && "回测日为空");

  auto t0 = std::chrono::steady_clock::now();
  auto data = std::make_unique<Data>();
  Axes &axes = data->axes;
  Meta &meta = data->meta;
  PitPool &pit = data->pit;

  const fundamental::Catalog cat;

  // ---- D 轴: all_trading_days (market_code='CN', 截到 today) ----
  axes.dates = fundamental::calendar(cat, misc::today_ymd());
  axes.date_days.reserve(axes.dates.size());
  for (const Ymd d : axes.dates)
    axes.date_days.push_back(misc::to_days(d));

  // ---- A 轴: AssetAxis codes ("000001.SZ"); 月度表里轴外的代码 → kNone 跳过 ----
  fundamental::Ids ids(fundamental::Ids::Miss::Skip);
  for (const auto &c : codes)
    ids.add(c);
  axes.n_a_ = static_cast<int>(ids.size());
  const int n_a = axes.n_a();
  const int n_d = axes.n_d();

  // ---- 回测日必须全部命中交易日历; 首回测日决定网格切片起点 ----
  int first_d = n_d;
  for (const Ymd s : dates) {
    const int d = axes.exact(s);
    assert(d >= 0 && "回测日不在交易日历里");
    first_d = std::min(first_d, d);
  }

  // ---- 静态 meta: cn_stock_basic_info (_meta) ----
  meta.list.assign(static_cast<std::size_t>(n_a), 0);
  meta.delist.assign(static_cast<std::size_t>(n_a), 0);
  meta.list_day.assign(static_cast<std::size_t>(n_a), {});
  meta.delist_day.assign(static_cast<std::size_t>(n_a), {});
  meta.main_board.assign(static_cast<std::size_t>(n_a), 0);
  fundamental::meta(cat, ids, fundamental::spec("cn_stock_basic_info"), {"list_date", "delist_date", "list_sector"},
                    [&](const Rows &r) {
                      const pq::Col ld = r.col("list_date"), dd = r.col("delist_date"), ls = r.col("list_sector");
                      r.each([&](std::int64_t i, std::uint32_t a, Ymd) {
                        const Ymd l = std::max(ld.yyyymmdd(i), 0), dl = std::max(dd.yyyymmdd(i), 0);
                        meta.list[a] = l;
                        meta.delist[a] = dl;
                        if (l)
                          meta.list_day[a] = misc::to_days(l);
                        if (dl)
                          meta.delist_day[a] = misc::to_days(dl);
                        meta.main_board[a] = (ls.i32(i, 0) == 1) ? 1 : 0;
                      });
                    });

  // ---- PIT 池: 15 张月度表一次 Scan → 全 D 轴网格 + 事件链 ----
  {
    fundamental::Scan scan(cat, ids, static_cast<size_t>(misc::exec::hw()));
    GridScan grids;
    EventScan events;
    grids.add(scan, axes, pit);
    events.add(scan, axes);
    std::atomic<std::size_t> done{0};
    scan.run(done);
    grids.finish(axes, pit);
    std::move(events).finish(n_a, pit);
  }

  // ---- 分红事件标注公告日股本快照 (需全史网格, 在切片之前) ----
  for (int a = 0; a < n_a; ++a) {
    for (auto &e : pit.dividend[static_cast<std::size_t>(a)]) {
      const float sh = pit.at(pit.total_shares, a, e.v);
      const float c = e.cash_before_tax;
      e.amt_pre = (std::isfinite(c) && c > 0.0f && std::isfinite(sh)) ? c * sh : 0.0f;
      const float ca = e.cash_after_tax;
      e.amt_after = (std::isfinite(ca) && std::isfinite(sh)) ? ca * sh : NaNF;
    }
  }

  // ---- total_shares 全史变点链 (shares_issued_yoy 回看 1 年, 切片后网格不再覆盖) ----
  pit.shares_chg.assign(static_cast<std::size_t>(n_a), {});
  for (int a = 0; a < n_a; ++a) {
    auto &chain = pit.shares_chg[static_cast<std::size_t>(a)];
    float last = NaNF;
    for (int d = 0; d < n_d; ++d) {
      const float s = pit.at(pit.total_shares, a, d);
      if (std::isfinite(s) && s != last) {
        chain.push_back({d, s});
        last = s;
      }
    }
  }

  // ---- dividend_st 数据轴 warmup (轴起点 + 3 年) ----
  data->axes_warmup_d = first_d_of_year(axes, year_of_d(axes, 0) + 3);

  // ---- 网格切片: 首回测日前留 15 日 (trading_st 连续 15 日计数), 其余历史只在事件链上 ----
  pit.slice(n_a, std::max(0, first_d - 15));

  d_ = std::move(data);

  auto t1 = std::chrono::steady_clock::now();
  double sec = std::chrono::duration<double>(t1 - t0).count();
  std::printf("[fund::Panel] built: %d assets, D axis %d, grid [%d, %d) (%.1fs)\n",
              n_a, n_d, pit.g0, pit.g0 + pit.n_g, sec);
}

// ============================================================================
// State / Stream: per-A 日频状态机 (qmt 批量前扫的逐日形式; 状态只依赖 ≤ d 的事件)
// ============================================================================
struct State {
  struct Trig {
    int on, off; // 区间 [on, off)
    float thr;   // 仅 revenue_st: 营收阈值
  };
  struct Annual {
    std::int32_t report_date;
    float val;
    int last_v;
  };

  const Data &D;
  const int a;
  int list_d;   // 上市日 D 轴 lower_bound; 无 → n_d
  bool mb;      // 主板
  float mc_thr; // 低市值阈值 [元]
  int warmup_d; // dividend_st 生效起点 = max(轴起点+3y, 上市+3y)
  int cur_d = -1;

  // ttm / balance: latest (上市前事件丢弃)
  std::size_t tp = 0, bp = 0;
  int last_ttm = -1;
  std::map<std::int32_t, FinancialBalanceEv> latest_by_rd;
  // 年报 (ni_raw): 同 report_date 覆盖
  std::size_t ip = 0;
  std::vector<Annual> annuals;
  // 利润表 / 现金流量表 YTD 链 (三口径 raw) + 非经常性损益 TTM (latest)
  YtdState<INC_COUNT> income;
  YtdState<CF_COUNT> cashflow;
  std::size_t nrp = 0;
  int last_nonrecur = -1;
  // 分红: 365 日滑窗 (dy) + 3 年累计 (dividend_st, 公告日锚)
  std::size_t div_lo = 0, div_hi = 0;
  float cash_sum = 0.0f, sum_3y = 0.0f;
  // 预亏 / 营收预警: 区间并集 ⇔ d < max{off : on ≤ d}
  std::vector<Trig> profit_trig, revenue_trig;
  std::size_t pp = 0, rp = 0;
  int profit_until = 0, rev_until_1e8 = 0, rev_until_3e8 = 0;
  // 行业 replay
  std::size_t ic = 0, ig = 0;
  std::uint8_t industry = 0;
  // trading_st: 连续 (低价 ∨ 低市值) 日数
  int run = 0;

  State(const Data &data, int asset) : D(data), a(asset) {
    const Axes &axes = D.axes;
    const Meta &meta = D.meta;
    const std::size_t ai = static_cast<std::size_t>(a);
    const int n_d = axes.n_d();
    list_d = get_list_d(a, axes, meta);
    mb = meta.main_board[ai] != 0;
    mc_thr = mb ? 5e8f : 3e8f;

    int stock_warmup_d = n_d;
    if (meta.list[ai])
      stock_warmup_d = first_d_of_year(axes, year_of(meta.list[ai]) + 3);
    warmup_d = std::max(D.axes_warmup_d, stock_warmup_d);

    // 触发区间 (forecast v 升序 → on 升序); 年报预亏 (首亏/续亏, 12 月末报告期)
    const auto &inc = D.pit.income_annual[ai];
    for (const auto &e : D.pit.forecast[ai]) {
      if (month_of(e.end_date) != 12)
        continue;
      if (e.type != ForecastType::FirstLoss && e.type != ForecastType::ContinueLoss)
        continue;
      const int off = std::min(find_forecast_off_d(e, inc, axes), n_d);
      // profit_st: ∧ 上年归母净利 < 0
      if (std::isfinite(e.last_parent_net) && e.last_parent_net < 0.0f)
        profit_trig.push_back({e.v, off, 0.0f});
      // revenue_st: 主板 ∧ 2021 新规后 (报告期年 ≥ 2021, ann_date ≥ 20210101: e.v 是 row D, e.v-1 是 visible_d)
      if (mb) {
        const int end_y = year_of(e.end_date);
        if (end_y >= 2021 && e.v >= 1 && e.v < n_d &&
            axes.dates[static_cast<std::size_t>(e.v - 1)] >= 20210101)
          revenue_trig.push_back({e.v, off, (end_y >= 2024) ? 3e8f : 1e8f});
      }
    }
  }

  // 上市前 0 哨兵 (qmt fill_before_list)
  float close_raw(int d) const {
    return d < list_d ? 0.0f : D.pit.at(D.pit.close, a, d);
  }
  float mcap_raw(int d) const {
    if (d < list_d)
      return 0.0f;
    const float c = D.pit.at(D.pit.close, a, d);
    const float s = D.pit.at(D.pit.total_shares, a, d);
    return (std::isfinite(c) && std::isfinite(s)) ? c * s : NaNF;
  }

  // 推进一日: 吸收 v ≤ d 的事件, 更新连续计数
  void step(int d) {
    const Axes &axes = D.axes;
    const PitPool &p = D.pit;
    const std::size_t ai = static_cast<std::size_t>(a);

    {
      const auto &ev = p.ttm[ai];
      while (tp < ev.size() && ev[tp].v <= d) {
        if (ev[tp].v >= list_d)
          last_ttm = static_cast<int>(tp);
        ++tp;
      }
    }
    {
      const auto &ev = p.balance[ai];
      while (bp < ev.size() && ev[bp].v <= d) {
        if (ev[bp].v >= list_d)
          latest_by_rd[ev[bp].report_date] = ev[bp];
        ++bp;
      }
    }
    {
      const auto &ev = p.income_annual[ai];
      while (ip < ev.size() && ev[ip].v <= d) {
        const auto &e = ev[ip++];
        if (!std::isfinite(e.net_profit_to_parent_shareholders))
          continue;
        auto it = std::find_if(annuals.begin(), annuals.end(),
                               [&](const Annual &x) { return x.report_date == e.report_date; });
        if (it == annuals.end())
          annuals.push_back({e.report_date, e.net_profit_to_parent_shareholders, e.v});
        else {
          it->val = e.net_profit_to_parent_shareholders;
          it->last_v = e.v;
        }
      }
    }
    income.absorb(p.income_ytd[ai], d, list_d);
    cashflow.absorb(p.cashflow_ytd[ai], d, list_d);
    {
      const auto &ev = p.nonrecur[ai];
      while (nrp < ev.size() && ev[nrp].v <= d) {
        if (ev[nrp].v >= list_d)
          last_nonrecur = static_cast<int>(nrp);
        ++nrp;
      }
    }
    {
      const auto &divs = p.dividend[ai];
      // 入窗; 每到一笔公告重算 3 年累计 (公告年 ann_y 的前 3 个报告年)
      while (div_hi < divs.size() && divs[div_hi].v <= d) {
        const auto &e = divs[div_hi];
        cash_sum += e.amt_pre;
        if (mb && e.v >= 1) {
          const int ann_y = year_of_d(axes, e.v - 1);
          const int lo_y = ann_y - 3, hi_y = ann_y - 1;
          float sum = 0.0f;
          for (std::size_t j = 0; j <= div_hi; ++j) {
            const auto &pd = divs[j];
            const int py = year_of(pd.report_date);
            if (py < lo_y || py > hi_y)
              continue;
            if (!std::isfinite(pd.amt_after))
              continue;
            sum += pd.amt_after;
          }
          sum_3y = sum;
        }
        ++div_hi;
      }
      // 出窗: 公告日 ≤ T − 365
      const auto Tlo = axes.date_days[static_cast<std::size_t>(d)] - std::chrono::days{365};
      while (div_lo < div_hi && axes.date_days[static_cast<std::size_t>(divs[div_lo].v)] <= Tlo) {
        cash_sum -= divs[div_lo].amt_pre;
        ++div_lo;
      }
      if (div_lo >= div_hi)
        cash_sum = 0.0f;
    }
    while (pp < profit_trig.size() && profit_trig[pp].on <= d) {
      profit_until = std::max(profit_until, profit_trig[pp].off);
      ++pp;
    }
    while (rp < revenue_trig.size() && revenue_trig[rp].on <= d) {
      int &until = (revenue_trig[rp].thr > 2e8f) ? rev_until_3e8 : rev_until_1e8;
      until = std::max(until, revenue_trig[rp].off);
      ++rp;
    }
    {
      // component 月初快照 + change 月内增量 (同日 change 覆盖)
      const auto &comp = p.industry_component[ai];
      const auto &chg = p.industry_change[ai];
      while (ic < comp.size() && comp[ic].v <= d) {
        industry = comp[ic].l1_id;
        ++ic;
      }
      while (ig < chg.size() && chg[ig].v <= d) {
        industry = chg[ig].l1_id;
        ++ig;
      }
    }
    // trading_st: 网格切片起点 = 首回测日 − 15, 切片外的 warmup 日不计 (首回测日时已满 15 日)
    if (d >= p.g0) {
      const float c = close_raw(d), m = mcap_raw(d);
      const bool lp = std::isfinite(c) && c > 0.0f && c < 1.0f;
      const bool lmc = std::isfinite(m) && m > 0.0f && m < mc_thr;
      run = (lp || lmc) ? run + 1 : 0;
    }
    cur_d = d;
  }

  // 当日行 (布局 Fund::Out; 亿单位; fp16 饱和: 极值 / +inf 违约束标记 → NaN)
  void emit(int d, float *out) const {
    const Axes &axes = D.axes;
    const Meta &meta = D.meta;
    const PitPool &p = D.pit;
    const std::size_t ai = static_cast<std::size_t>(a);
    constexpr float kFp16Max = 65504.0f;
    auto sat = [](float v) { return (std::isfinite(v) && v > -kFp16Max && v < kFp16Max) ? v : NaNF; };
    auto pos = [](float v) { return (std::isfinite(v) && v > 0.0f) ? v : NaNF; };

    const float m = mcap_raw(d);
    const FinancialTtmEv *t = (last_ttm >= 0) ? &p.ttm[ai][static_cast<std::size_t>(last_ttm)] : nullptr;

    // -- 财务分母 / cffoa 改善 --
    float np = NaNF, rev = NaNF, cf = NaNF, cf_chg = NaNF;
    if (t) {
      const float n = t->net_profit_to_parent_shareholders_ttm;
      np = (std::isfinite(n) && n != 0.0f) ? n : NaNF;
      const float r = t->total_operating_revenue_ttm; // 负营收是源脏值 (qmt ps_raw 口径): ≤0 → NaN
      rev = (std::isfinite(r) && r > 0.0f) ? r : NaNF;
      const float c = t->net_cffoa_ttm;
      cf = (std::isfinite(c) && c != 0.0f) ? c : NaNF;
      const float c0 = t->net_cffoa_ttm, c4 = t->net_cffoa_ttm_shift4;
      cf_chg = (std::isfinite(m) && m > 0.0f && std::isfinite(c0) && std::isfinite(c4))
                   ? std::tanh((c0 - c4) / m)
                   : NaNF;
    }
    // -- 权益 MRQ / roe / roa (avg5 分母) --
    float eq = NaNF, roe = NaNF, roa = NaNF;
    if (!latest_by_rd.empty()) {
      const float e = latest_by_rd.rbegin()->second.total_equity_to_parent_shareholders;
      eq = (std::isfinite(e) && e != 0.0f) ? e : NaNF;
      if (t) {
        const float n = t->net_profit_to_parent_shareholders_ttm;
        const float e5 = ttm_window_avg(t->report_date, latest_by_rd,
                                        &FinancialBalanceEv::total_equity_to_parent_shareholders);
        roe = (std::isfinite(n) && std::isfinite(e5) && e5 > 0.0f) ? (n / e5) * 100.0f : NaNF;
        const float na = t->net_profit_ttm;
        const float a5 = ttm_window_avg(t->report_date, latest_by_rd, &FinancialBalanceEv::total_assets);
        roa = (std::isfinite(na) && std::isfinite(a5) && a5 > 0.0f) ? (na / a5) * 100.0f : NaNF;
      }
    }
    // -- dy: 365 日税前分红总额 / mcap --
    const float dy = (std::isfinite(m) && m > 0.0f) ? std::max(cash_sum / m, 0.0f) : NaNF;
    // -- ni_raw: 最近两份年报 (按 PIT 首见序) 归母净利均值 --
    float ni = NaNF;
    if (!annuals.empty()) {
      int i0 = -1, i1 = -1, v0 = -1, v1 = -1;
      for (std::size_t i = 0; i < annuals.size(); ++i) {
        const int v = annuals[i].last_v;
        if (v > v0) {
          v1 = v0;
          i1 = i0;
          v0 = v;
          i0 = static_cast<int>(i);
        } else if (v > v1) {
          v1 = v;
          i1 = static_cast<int>(i);
        }
      }
      if (i0 >= 0 && i1 >= 0)
        ni = (annuals[static_cast<std::size_t>(i0)].val + annuals[static_cast<std::size_t>(i1)].val) * 0.5f;
      else if (i0 >= 0)
        ni = annuals[static_cast<std::size_t>(i0)].val;
    }
    // -- filter --
    const bool profit_st = d < profit_until;
    const bool revenue_st = mb && std::isfinite(rev) &&
                            ((d < rev_until_1e8 && rev < 1e8f) || (d < rev_until_3e8 && rev < 3e8f));
    const bool dividend_st = mb && d >= warmup_d && std::isfinite(ni) && ni > 0.0f &&
                             sum_3y < 0.30f * ni && sum_3y < 5e7f;

    // 股本 / 估值分母 (亿单位)
    out[Fund::total_shares] = pos(p.at(p.total_shares, a, d)) * 1e-8f;
    out[Fund::float_shares] = pos(p.at(p.a_float_shares, a, d)) * 1e-8f;
    out[Fund::net_profit_ttm] = sat(np * 1e-8f);
    out[Fund::equity_mrq] = sat(eq * 1e-8f);
    out[Fund::revenue_ttm] = sat(rev * 1e-8f);
    out[Fund::cffoa_ttm] = sat(cf * 1e-8f);
    out[Fund::lim_up] = pos(p.at(p.lim_up, a, d));
    out[Fund::lim_dn] = pos(p.at(p.lim_dn, a, d));
    out[Fund::low_mc_thr] = mb ? 5.0f : 3.0f;

    // 日频因子 raw
    out[Fund::roe_ttm] = sat(roe);
    out[Fund::roa_ttm] = sat(roa);
    out[Fund::dy_ttm] = sat(dy);
    out[Fund::cfo_chg_ttm] = sat(cf_chg);
    out[Fund::rz_bal] = sat(p.at(p.fin_balance, a, d) * 1e-8f);
    out[Fund::rq_bal] = sat(p.at(p.sec_balance, a, d) * 1e-8f);

    // 上市龄 / 退市龄 (日历日; 未上市/未退市 = NaN)
    const auto day = axes.date_days[static_cast<std::size_t>(d)];
    float lage = NaNF, dage = NaNF;
    if (meta.list[ai]) {
      const float age = static_cast<float>((day - meta.list_day[ai]).count());
      if (age >= 0.0f)
        lage = age;
    }
    if (meta.delist[ai]) {
      const float age = static_cast<float>((day - meta.delist_day[ai]).count());
      if (age >= 0.0f)
        dage = age;
    }
    out[Fund::list_age] = lage;
    out[Fund::delist_age] = dage;

    // 状态 / filter (0/1 常量)
    out[Fund::ind_l1] = static_cast<float>(industry);
    out[Fund::is_margin] = static_cast<float>(p.at(p.is_margin, a, d));
    out[Fund::is_susp] = static_cast<float>(p.at(p.suspended, a, d));
    out[Fund::st_level] = static_cast<float>(p.at(p.st_status, a, d));
    out[Fund::st_profit] = profit_st ? 1.0f : 0.0f;
    out[Fund::st_revenue] = revenue_st ? 1.0f : 0.0f;
    out[Fund::st_dividend] = dividend_st ? 1.0f : 0.0f;
    out[Fund::st_trading] = (run >= 15) ? 1.0f : 0.0f;
    out[Fund::is_new] = (std::isfinite(lage) && lage < 60.0f) ? 1.0f : 0.0f;

    // ---- 行情日线项 / 两融明细 ----
    out[Fund::pre_close] = pos(p.at(p.pre_close, a, d));
    out[Fund::adj_factor] = pos(p.at(p.adj_factor, a, d));
    out[Fund::rz_buy_amt] = sat(p.at(p.fin_purchase, a, d) * 1e-8f);
    out[Fund::rz_repay_amt] = sat(p.at(p.fin_repayment, a, d) * 1e-8f);
    out[Fund::rq_sell_vol] = sat(p.at(p.sec_sales_qty, a, d) * 1e-4f);
    out[Fund::rq_repay_vol] = sat(p.at(p.sec_repay_qty, a, d) * 1e-4f);

    // ---- 距最近财报公告日 (利润表链 last_v 是 row = 公告可见日 + 1) ----
    out[Fund::days_since_report] =
        (income.last_v >= 1)
            ? static_cast<float>((day - axes.date_days[static_cast<std::size_t>(income.last_v - 1)]).count())
            : NaNF;

    // ---- 股本同比: total_shares(d) / total_shares(日历日 −365 的 D 轴 floor) − 1 ----
    {
      float yoy = NaNF;
      const float s_now = p.at(p.total_shares, a, d);
      const auto ago_it = std::upper_bound(axes.date_days.begin(), axes.date_days.end(), day - std::chrono::days{365});
      const int d_ago = static_cast<int>(std::distance(axes.date_days.begin(), ago_it)) - 1;
      const auto &chain = p.shares_chg[ai];
      if (d_ago >= 0 && std::isfinite(s_now)) {
        auto it = std::upper_bound(chain.begin(), chain.end(), d_ago,
                                   [](int x, const SharesEv &e) { return x < e.d; });
        if (it != chain.begin()) {
          const float s_ago = std::prev(it)->shares;
          yoy = s_now / s_ago - 1.0f;
        }
      }
      out[Fund::shares_yoy] = sat(yoy);
    }

    // ---- 资产负债表 MRQ (亿元) ----
    {
      const FinancialBalanceEv *b = latest_by_rd.empty() ? nullptr : &latest_by_rd.rbegin()->second;
      auto bs = [&](float FinancialBalanceEv::*f) { return b ? sat((b->*f) * 1e-8f) : NaNF; };
      out[Fund::bs_ta] = bs(&FinancialBalanceEv::total_assets);
      out[Fund::bs_tl] = bs(&FinancialBalanceEv::total_liabilities);
      out[Fund::bs_ncl] = bs(&FinancialBalanceEv::total_noncurr_liabilities);
      out[Fund::bs_ibd] = bs(&FinancialBalanceEv::ib_debt);
      out[Fund::bs_ca] = bs(&FinancialBalanceEv::total_current_assets);
      out[Fund::bs_cl] = bs(&FinancialBalanceEv::total_current_liabilities);
      out[Fund::bs_cash] = bs(&FinancialBalanceEv::moneytary_assets);
      out[Fund::bs_inv] = bs(&FinancialBalanceEv::inventories);
      out[Fund::bs_ar] = bs(&FinancialBalanceEv::accounts_receivable);
      out[Fund::bs_ap] = bs(&FinancialBalanceEv::accounts_payable);
      out[Fund::bs_fa_cip] = bs(&FinancialBalanceEv::fixed_cip);
      out[Fund::bs_intang_gw] = bs(&FinancialBalanceEv::intang_gw);
      out[Fund::bs_eq] = bs(&FinancialBalanceEv::total_equity_to_parent_shareholders);
      out[Fund::bs_minority] = bs(&FinancialBalanceEv::minority_interests);
    }

    // ---- 利润表 / 现金流量表 三口径 (亿元; eps 元) ----
    {
      float q[INC_COUNT], ttm[INC_COUNT], lyr[INC_COUNT];
      income.emit(q, ttm, lyr);
      auto put = [&](std::size_t item, float scale, Fund::Out oq, Fund::Out ottm, Fund::Out olyr) {
        out[oq] = sat(q[item] * scale);
        out[ottm] = sat(ttm[item] * scale);
        out[olyr] = sat(lyr[item] * scale);
      };
      put(inc_rev, 1e-8f, Fund::pl_rev_q, Fund::pl_rev_ttm, Fund::pl_rev_lyr);
      put(inc_cogs, 1e-8f, Fund::pl_cogs_q, Fund::pl_cogs_ttm, Fund::pl_cogs_lyr);
      put(inc_cost, 1e-8f, Fund::pl_cost_q, Fund::pl_cost_ttm, Fund::pl_cost_lyr);
      put(inc_op, 1e-8f, Fund::pl_op_q, Fund::pl_op_ttm, Fund::pl_op_lyr);
      put(inc_ebit, 1e-8f, Fund::pl_ebit_q, Fund::pl_ebit_ttm, Fund::pl_ebit_lyr);
      put(inc_int, 1e-8f, Fund::pl_int_q, Fund::pl_int_ttm, Fund::pl_int_lyr);
      put(inc_tax, 1e-8f, Fund::pl_tax_q, Fund::pl_tax_ttm, Fund::pl_tax_lyr);
      put(inc_np, 1e-8f, Fund::pl_np_q, Fund::pl_np_ttm, Fund::pl_np_lyr);
      put(inc_np_parent, 1e-8f, Fund::pl_npp_q, Fund::pl_npp_ttm, Fund::pl_npp_lyr);
      put(inc_eps, 1.0f, Fund::pl_eps_q, Fund::pl_eps_ttm, Fund::pl_eps_lyr);
      // 扣非归母 TTM = 归母 TTM (差分口径) − 非经常性损益 TTM (notes 表 latest); 报告期不一致 → NaN
      float np_ded = NaNF;
      if (last_nonrecur >= 0 && !income.by_rd.empty()) {
        const NonrecurEv &nr = p.nonrecur[ai][static_cast<std::size_t>(last_nonrecur)];
        if (nr.report_date == income.by_rd.rbegin()->first)
          np_ded = ttm[inc_np_parent] - nr.nonrecurring_ttm;
      }
      out[Fund::pl_npd_ttm] = sat(np_ded * 1e-8f);
    }
    {
      float q[CF_COUNT], ttm[CF_COUNT], lyr[CF_COUNT];
      cashflow.emit(q, ttm, lyr);
      auto put = [&](std::size_t item, Fund::Out oq, Fund::Out ottm, Fund::Out olyr) {
        out[oq] = sat(q[item] * 1e-8f);
        out[ottm] = sat(ttm[item] * 1e-8f);
        out[olyr] = sat(lyr[item] * 1e-8f);
      };
      put(cf_ocf, Fund::cf_ocf_q, Fund::cf_ocf_ttm, Fund::cf_ocf_lyr);
      put(cf_icf, Fund::cf_icf_q, Fund::cf_icf_ttm, Fund::cf_icf_lyr);
      put(cf_fin, Fund::cf_fin_q, Fund::cf_fin_ttm, Fund::cf_fin_lyr);
      put(cf_net, Fund::cf_net_q, Fund::cf_net_ttm, Fund::cf_net_lyr);
      put(cf_capex, Fund::cf_capex_q, Fund::cf_capex_ttm, Fund::cf_capex_lyr);
      put(cf_div, Fund::cf_div_q, Fund::cf_div_ttm, Fund::cf_div_lyr);
      put(cf_da, Fund::cf_da_q, Fund::cf_da_ttm, Fund::cf_da_lyr);
    }
  }
};

Stream::Stream(const Panel &panel, std::size_t asset_id)
    : s_(std::make_unique<State>(panel.data(), static_cast<int>(asset_id))) {
  assert(static_cast<int>(asset_id) < panel.data().axes.n_a() && "asset_id 越界 (AssetAxis)");
}
Stream::~Stream() = default;

void Stream::advance_to(int d, float *out) {
  assert(d >= 0 && d < s_->D.axes.n_d() && "D 轴越界");
  assert(d >= s_->cur_d && "advance_to 必须单调不减");
  for (int k = s_->cur_d + 1; k <= d; ++k)
    s_->step(k);
  s_->emit(d, out);
}

} // namespace fund
