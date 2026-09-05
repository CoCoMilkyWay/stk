// fund — 日频 PIT 基本面实现 (qmt pit.cpp + def/ 链路移植)
//
// 结构: Pool::build   parquet 月度分片 → 网格 (raw cutoff 单点应用, ffill, 切到 [首回测日−15, 末]) + per-A 事件链 (v 升序)
//       Stream        per-A 沿 D 轴逐日推进的状态机 (估值分母 / 因子 raw / filter), 与 qmt 批量扫描逐日等价:
//                       ttm/balance 取 latest (上市前事件丢弃), 年报 annuals, 分红 365 日滑窗 + 3 年双阈值,
//                       预亏/营收区间 = "d < max{off : on ≤ d}" (区间并集的流式形式), 行业 replay, trading_st 连续计数
//                     每日产出 Fund::kCount 列 (缺失 = NaN, fp16 存不下的极值也归 NaN).
//
// 注意: 本文件依赖 NaN 语义, 必须在 CMake PRECISE_MATH 列表里 (-fno-fast-math).
#include "features/Method/Fundamental.hpp"

#include "features/Operator/TS/Fund/Fund.hpp" // Fund::Out 输出行布局
#include "misc/date.hpp"
#include "misc/parquet.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fund {
namespace {

namespace pq = misc::pq;

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

inline int year_of(std::int32_t yyyymmdd) { return yyyymmdd / 10000; }
inline int month_of(std::int32_t yyyymmdd) { return yyyymmdd / 100 % 100; }

inline std::string ymd_str(std::int32_t v) {
  if (v <= 0)
    return {};
  char buf[9];
  std::snprintf(buf, sizeof(buf), "%08d", v);
  return std::string(buf, 8);
}

// SW2021 一级行业表 (SW2021_L1_NAMES / sw2021_l1_name_to_id) 见 Fundamental.hpp

// ============================================================================
// 轴 + 静态 meta
// ============================================================================
struct Axes {
  std::vector<std::string> dates; // "YYYYMMDD" 升序, 全交易日历截到 today
  std::vector<std::chrono::sys_days> date_days;
  std::unordered_map<std::string, int> date_idx;
  std::unordered_map<std::string, int> code_idx; // "000001.SZ" → a

  int n_d() const { return static_cast<int>(dates.size()); }
  int n_a() const { return static_cast<int>(code_idx.size()); }

  // max{i : dates[i] <= d}; d < dates[0] → -1 (事件 visible → 上一交易日)
  int floor_date(std::string_view d) const {
    auto it = std::upper_bound(dates.begin(), dates.end(), d);
    return static_cast<int>(std::distance(dates.begin(), it)) - 1;
  }
};

struct Meta {
  // 与 codes 同序; sys_days + valid 位 (list_date 缺失 = 永未上市)
  std::vector<std::chrono::sys_days> list_day;
  std::vector<std::uint8_t> has_list;
  std::vector<std::chrono::sys_days> delist_day;
  std::vector<std::uint8_t> has_delist;
  std::vector<std::string> list_date_str; // "YYYYMMDD" (上市前事件截断用)
  std::vector<std::uint8_t> main_board;   // list_sector == 1
};

// 上市日在 D 轴的 lower_bound; 无 list_date → n_d (永未上市)
int get_list_d(int a, const Axes &axes, const Meta &meta) {
  if (!meta.has_list[static_cast<std::size_t>(a)])
    return axes.n_d();
  const std::string &ld = meta.list_date_str[static_cast<std::size_t>(a)];
  auto it = std::lower_bound(axes.dates.begin(), axes.dates.end(), ld);
  return static_cast<int>(std::distance(axes.dates.begin(), it));
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
  std::vector<float> total_shares;   // cutoff=-1, ffill
  std::vector<float> a_float_shares; // cutoff=-1, ffill
  std::vector<float> up_lim;         // cutoff=-1 后 row T = T 当日适用, ffill
  std::vector<float> dn_lim;
  std::vector<std::int8_t> st_status; // cutoff=0, 4 态派生, 不 ffill
  std::vector<std::uint8_t> suspended;
  std::vector<std::uint8_t> is_margin; // cutoff=0
  std::vector<float> fin_balance;      // 融资余额, 不 ffill
  std::vector<float> sec_balance;      // 融券余额

  // 事件 (per-a, v 升序)
  std::vector<std::vector<FinancialTtmEv>> ttm;
  std::vector<std::vector<FinancialBalanceEv>> balance;
  std::vector<std::vector<FinancialIncomeAnnualEv>> income_annual;
  std::vector<std::vector<DividendEv>> dividend;
  std::vector<std::vector<ForecastEv>> forecast;
  std::vector<std::vector<IndustryEv>> industry_component;
  std::vector<std::vector<IndustryEv>> industry_change;

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
    cut(total_shares);
    cut(a_float_shares);
    cut(up_lim);
    cut(dn_lim);
    cut(st_status);
    cut(suspended);
    cut(is_margin);
    cut(fin_balance);
    cut(sec_balance);
    g0 = new_g0;
    n_g = nn;
  }
};

} // namespace

// ============================================================================
// Data: Pool 的全部内容 (只读共享)
// ============================================================================
struct Data {
  Axes axes;
  Meta meta;
  PitPool pit;
  int axes_warmup_d = 0; // dividend_st 数据轴 warmup: 轴起点 + 3 年
};

namespace {

// ============================================================================
// row 定位 memo (qmt GridRowMemo / EventRowMemo)
// ============================================================================
class GridRowMemo {
public:
  GridRowMemo(const Axes &axes, int cutoff) : axes_(axes), cutoff_(cutoff) {}
  int row(std::int32_t ymd) {
    auto it = memo_.find(ymd);
    if (it != memo_.end())
      return it->second;
    int r = -1;
    auto di = axes_.date_idx.find(ymd_str(ymd));
    if (di != axes_.date_idx.end()) {
      int cand = di->second - cutoff_;
      if (cand >= 0 && cand < axes_.n_d())
        r = cand;
    }
    memo_.emplace(ymd, r);
    return r;
  }

private:
  const Axes &axes_;
  int cutoff_;
  std::unordered_map<std::int32_t, int> memo_;
};

class EventRowMemo {
public:
  EventRowMemo(const Axes &axes, int cutoff) : axes_(axes), cutoff_(cutoff) {}
  int row(std::int32_t ymd) {
    auto it = memo_.find(ymd);
    if (it != memo_.end())
      return it->second;
    int r = -1;
    std::string s = ymd_str(ymd);
    if (!s.empty()) {
      int v_idx = axes_.floor_date(s);
      if (v_idx >= 0) {
        int cand = v_idx - cutoff_;
        if (cand >= 0 && cand < axes_.n_d())
          r = cand;
      }
    }
    memo_.emplace(ymd, r);
    return r;
  }

private:
  const Axes &axes_;
  int cutoff_;
  std::unordered_map<std::int32_t, int> memo_;
};

inline int lookup_a(const Axes &axes, std::string_view code) {
  if (code.empty())
    return -1;
  auto it = axes.code_idx.find(std::string(code));
  return it == axes.code_idx.end() ? -1 : it->second;
}

// per-月 parquet 并行驱动 (qmt parallel_parse_months)
template <class Body>
void parallel_parse_months(
    const std::vector<std::pair<std::string, std::filesystem::path>> &files,
    Body body) {
  std::size_t n = files.size();
  if (n == 0)
    return;
  unsigned nt = std::thread::hardware_concurrency();
  if (nt == 0)
    nt = 1;
  if (static_cast<std::size_t>(nt) > n)
    nt = static_cast<unsigned>(n);
  std::atomic<std::size_t> next{0};
  auto worker = [&]() {
    for (;;) {
      std::size_t i = next.fetch_add(1, std::memory_order_relaxed);
      if (i >= n)
        break;
      pq::TableView v(pq::read_table(files[i].second));
      if (v.rows() == 0)
        continue;
      body(v);
    }
  };
  std::vector<std::thread> ts;
  ts.reserve(nt);
  for (unsigned t = 0; t < nt; ++t)
    ts.emplace_back(worker);
  for (auto &t : ts)
    t.join();
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

template <class Ev>
void sort_events(std::vector<std::vector<Ev>> &chains) {
  for (auto &c : chains)
    std::stable_sort(c.begin(), c.end(),
                     [](const Ev &x, const Ev &y) { return x.v < y.v; });
}

// ============================================================================
// itf 构建 (qmt pit.cpp 各 itf_* 移植; cutoff 语义逐一保持)
// ============================================================================
void build_grids(const Axes &axes, PitPool &p) {
  const std::size_t n = static_cast<std::size_t>(axes.n_a()) *
                        static_cast<std::size_t>(axes.n_d());
  const std::size_t n_d = static_cast<std::size_t>(axes.n_d());
  p.g0 = 0;
  p.n_g = axes.n_d();

  auto alloc_f = [&](std::vector<float> &g) { g.assign(n, NaNF); };
  alloc_f(p.close);
  alloc_f(p.total_shares);
  alloc_f(p.a_float_shares);
  alloc_f(p.up_lim);
  alloc_f(p.dn_lim);
  alloc_f(p.fin_balance);
  alloc_f(p.sec_balance);
  p.st_status.assign(n, 0);
  p.suspended.assign(n, 0);
  p.is_margin.assign(n, 0);

  // ---- cn_stock_real_bar1d (CUTOFF=-1) ----
  parallel_parse_months(pq::list_month_files("cn_stock_real_bar1d"),
                        [&](const pq::TableView &v) {
                          pq::Col date = v.col("date"), inst = v.col("instrument");
                          pq::Col close = v.col("close");
                          GridRowMemo memo(axes, -1);
                          for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
                            int row = memo.row(date.yyyymmdd(i));
                            if (row < 0)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            p.close[static_cast<std::size_t>(a) * n_d + static_cast<std::size_t>(row)] =
                                positive_or_inf(close.f32(i));
                          }
                        });

  // ---- cn_stock_shares (CUTOFF=-1) ----
  parallel_parse_months(pq::list_month_files("cn_stock_shares"),
                        [&](const pq::TableView &v) {
                          pq::Col date = v.col("date"), inst = v.col("instrument");
                          pq::Col ts = v.col("total_shares"), fs = v.col("a_float_shares");
                          GridRowMemo memo(axes, -1);
                          for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
                            int row = memo.row(date.yyyymmdd(i));
                            if (row < 0)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            std::size_t off = static_cast<std::size_t>(a) * n_d + static_cast<std::size_t>(row);
                            p.total_shares[off] = positive_or_inf(ts.f32(i));
                            p.a_float_shares[off] = positive_or_inf(fs.f32(i));
                          }
                        });

  // ---- cn_stock_limit_price (CUTOFF=-1; row T = T 当日适用涨跌停) ----
  parallel_parse_months(pq::list_month_files("cn_stock_limit_price"),
                        [&](const pq::TableView &v) {
                          pq::Col date = v.col("date"), inst = v.col("instrument");
                          pq::Col up = v.col("upper_limit"), dn = v.col("lower_limit");
                          GridRowMemo memo(axes, -1);
                          for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
                            int row = memo.row(date.yyyymmdd(i));
                            if (row < 0)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            std::size_t off = static_cast<std::size_t>(a) * n_d + static_cast<std::size_t>(row);
                            p.up_lim[off] = positive_or_inf(up.f32(i));
                            p.dn_lim[off] = positive_or_inf(dn.f32(i));
                          }
                        });

  // ---- cn_stock_status (CUTOFF=0, 4 态派生; 不 ffill) ----
  parallel_parse_months(pq::list_month_files("cn_stock_status"),
                        [&](const pq::TableView &v) {
                          pq::Col date = v.col("date"), inst = v.col("instrument");
                          pq::Col st_c = v.col("st_status"), rw_c = v.col("is_risk_warning");
                          pq::Col sp_c = v.col("suspended");
                          GridRowMemo memo(axes, 0);
                          for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
                            int row = memo.row(date.yyyymmdd(i));
                            if (row < 0)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            std::size_t off = static_cast<std::size_t>(a) * n_d + static_cast<std::size_t>(row);
                            int st = st_c.i32(i, 0), rw = rw_c.i32(i, 0), sp = sp_c.i32(i, 0);
                            // 4 态派生: st 1/2 优先; 否则 risk_warning=1 → 3 (退市整理期)
                            p.st_status[off] = (st == 1)   ? std::int8_t{1}
                                               : (st == 2) ? std::int8_t{2}
                                               : (rw != 0) ? std::int8_t{3}
                                                           : std::int8_t{0};
                            p.suspended[off] = (sp != 0) ? std::uint8_t{1} : std::uint8_t{0};
                          }
                        });

  // ---- cn_stock_margin_trading_detail (CUTOFF=0; 不 ffill) ----
  parallel_parse_months(pq::list_month_files("cn_stock_margin_trading_detail"),
                        [&](const pq::TableView &v) {
                          pq::Col date = v.col("date"), inst = v.col("instrument");
                          pq::Col fb = v.col("financing_balance");
                          pq::Col sb = v.col("securities_lending_balance");
                          GridRowMemo memo(axes, 0);
                          for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
                            int row = memo.row(date.yyyymmdd(i));
                            if (row < 0)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            std::size_t off = static_cast<std::size_t>(a) * n_d + static_cast<std::size_t>(row);
                            p.is_margin[off] = 1;
                            p.fin_balance[off] = non_negative_or_inf(fb.f32(i));
                            p.sec_balance[off] = non_negative_or_inf(sb.f32(i));
                          }
                        });

  grid_ffill(p.close, axes.n_a(), axes.n_d());
  grid_ffill(p.total_shares, axes.n_a(), axes.n_d());
  grid_ffill(p.a_float_shares, axes.n_a(), axes.n_d());
  grid_ffill(p.up_lim, axes.n_a(), axes.n_d());
  grid_ffill(p.dn_lim, axes.n_a(), axes.n_d());
}

void build_events(const Axes &axes, PitPool &p) {
  const std::size_t n_a = static_cast<std::size_t>(axes.n_a());
  p.ttm.assign(n_a, {});
  p.balance.assign(n_a, {});
  p.income_annual.assign(n_a, {});
  p.dividend.assign(n_a, {});
  p.forecast.assign(n_a, {});
  p.industry_component.assign(n_a, {});
  p.industry_change.assign(n_a, {});
  std::vector<std::mutex> mu(n_a);

  // ---- cn_stock_financial_ttm_shift (CUTOFF=-1; shift=0 主记录 + shift=4 配对) ----
  parallel_parse_months(pq::list_month_files("cn_stock_financial_ttm_shift"),
                        [&](const pq::TableView &v) {
                          pq::Col date = v.col("date"), inst = v.col("instrument");
                          pq::Col shift = v.col("shift"), rd = v.col("report_date");
                          pq::Col rev = v.col("total_operating_revenue_ttm");
                          pq::Col np = v.col("net_profit_to_parent_shareholders_ttm");
                          pq::Col npa = v.col("net_profit_ttm");
                          pq::Col cf = v.col("net_cffoa_ttm");
                          std::int64_t nr = v.rows();

                          std::unordered_map<std::int64_t, float> shift4_cf;
                          shift4_cf.reserve(static_cast<std::size_t>(nr) / 32 + 1);
                          for (std::int64_t i = 0; i < nr; ++i) {
                            if (shift.i32(i, -1) != 4)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            std::int64_t key =
                                static_cast<std::int64_t>(date.yyyymmdd(i)) * (axes.n_a() + 1) + a;
                            shift4_cf[key] = cf.f32(i);
                          }

                          EventRowMemo memo(axes, -1);
                          for (std::int64_t i = 0; i < nr; ++i) {
                            if (shift.i32(i, -1) != 0)
                              continue;
                            int row = memo.row(date.yyyymmdd(i));
                            if (row < 0)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            FinancialTtmEv ev;
                            ev.v = row;
                            ev.report_date = rd.yyyymmdd(i);
                            ev.total_operating_revenue_ttm = rev.f32(i);
                            ev.net_profit_to_parent_shareholders_ttm = np.f32(i);
                            ev.net_profit_ttm = npa.f32(i);
                            ev.net_cffoa_ttm = cf.f32(i);
                            std::int64_t key =
                                static_cast<std::int64_t>(date.yyyymmdd(i)) * (axes.n_a() + 1) + a;
                            auto it = shift4_cf.find(key);
                            ev.net_cffoa_ttm_shift4 = (it != shift4_cf.end()) ? it->second : NaNF;
                            std::lock_guard<std::mutex> lk(mu[static_cast<std::size_t>(a)]);
                            p.ttm[static_cast<std::size_t>(a)].push_back(ev);
                          }
                        });

  // ---- cn_stock_financial_balance_general_pit (CUTOFF=-1; 全报告期入) ----
  parallel_parse_months(
      pq::list_month_files("cn_stock_financial_balance_general_pit"),
      [&](const pq::TableView &v) {
        pq::Col date = v.col("date"), inst = v.col("instrument");
        pq::Col rd = v.col("report_date");
        pq::Col tep = v.col("total_equity_to_parent_shareholders");
        pq::Col ta = v.col("total_assets");
        EventRowMemo memo(axes, -1);
        for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
          int row = memo.row(date.yyyymmdd(i));
          if (row < 0)
            continue;
          int a = lookup_a(axes, inst.str(i));
          if (a < 0)
            continue;
          FinancialBalanceEv ev;
          ev.v = row;
          ev.report_date = rd.yyyymmdd(i);
          ev.total_equity_to_parent_shareholders = tep.f32(i);
          ev.total_assets = ta.f32(i);
          std::lock_guard<std::mutex> lk(mu[static_cast<std::size_t>(a)]);
          p.balance[static_cast<std::size_t>(a)].push_back(ev);
        }
      });

  // ---- cn_stock_financial_income_general_pit (CUTOFF=-1; 仅年报) ----
  parallel_parse_months(
      pq::list_month_files("cn_stock_financial_income_general_pit"),
      [&](const pq::TableView &v) {
        pq::Col date = v.col("date"), inst = v.col("instrument");
        pq::Col fqi = v.col("fs_quarter_index"), rd = v.col("report_date");
        pq::Col np = v.col("net_profit_to_parent_shareholders");
        EventRowMemo memo(axes, -1);
        for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
          if (fqi.i32(i, -1) != 4)
            continue;
          int row = memo.row(date.yyyymmdd(i));
          if (row < 0)
            continue;
          int a = lookup_a(axes, inst.str(i));
          if (a < 0)
            continue;
          FinancialIncomeAnnualEv ev;
          ev.v = row;
          ev.report_date = rd.yyyymmdd(i);
          ev.net_profit_to_parent_shareholders = np.f32(i);
          std::lock_guard<std::mutex> lk(mu[static_cast<std::size_t>(a)]);
          p.income_annual[static_cast<std::size_t>(a)].push_back(ev);
        }
      });

  // ---- cn_stock_dividend (CUTOFF=-1; v ← publish_date) ----
  parallel_parse_months(pq::list_month_files("cn_stock_dividend"),
                        [&](const pq::TableView &v) {
                          pq::Col vd = v.col("publish_date"), inst = v.col("instrument");
                          pq::Col rd = v.col("report_date");
                          pq::Col cash_b = v.col("cash_before_tax");
                          pq::Col cash = v.col("cash_after_tax");
                          EventRowMemo memo(axes, -1);
                          for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
                            int row = memo.row(vd.yyyymmdd(i));
                            if (row < 0)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            DividendEv ev;
                            ev.v = row;
                            ev.report_date = rd.yyyymmdd(i);
                            ev.cash_before_tax = cash_b.f32(i);
                            ev.cash_after_tax = cash.f32(i);
                            ev.amt_pre = 0.0f; // build 末尾标注
                            ev.amt_after = NaNF;
                            std::lock_guard<std::mutex> lk(mu[static_cast<std::size_t>(a)]);
                            p.dividend[static_cast<std::size_t>(a)].push_back(ev);
                          }
                        });

  // ---- forecast (Tushare, CUTOFF=-1; ts_code / "YYYYMMDD" string 列) ----
  parallel_parse_months(pq::list_month_files("forecast"),
                        [&](const pq::TableView &v) {
                          pq::Col vd = v.col("ann_date"), inst = v.col("ts_code");
                          pq::Col ed = v.col("end_date"), type = v.col("type");
                          pq::Col lpn = v.col("last_parent_net");
                          EventRowMemo memo(axes, -1);
                          for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
                            int row = memo.row(vd.yyyymmdd(i));
                            if (row < 0)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            ForecastEv ev;
                            ev.v = row;
                            ev.end_date = ed.yyyymmdd(i);
                            std::string_view t = type.str(i);
                            ev.type = (t == "首亏")   ? ForecastType::FirstLoss
                                      : (t == "续亏") ? ForecastType::ContinueLoss
                                                      : ForecastType::Other;
                            ev.last_parent_net = lpn.f32(i);
                            std::lock_guard<std::mutex> lk(mu[static_cast<std::size_t>(a)]);
                            p.forecast[static_cast<std::size_t>(a)].push_back(ev);
                          }
                        });

  // ---- cn_stock_industry_component / change (CUTOFF=-1, sw2021 L1) ----
  parallel_parse_months(pq::list_month_files("cn_stock_industry_component"),
                        [&](const pq::TableView &v) {
                          pq::Col date = v.col("date"), inst = v.col("instrument");
                          pq::Col ind = v.col("industry"), l1 = v.col("industry_level1_name");
                          EventRowMemo memo(axes, -1);
                          for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
                            if (ind.str(i) != "sw2021")
                              continue;
                            int row = memo.row(date.yyyymmdd(i));
                            if (row < 0)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            IndustryEv ev{row, sw2021_l1_name_to_id(l1.str(i))};
                            std::lock_guard<std::mutex> lk(mu[static_cast<std::size_t>(a)]);
                            p.industry_component[static_cast<std::size_t>(a)].push_back(ev);
                          }
                        });
  parallel_parse_months(pq::list_month_files("cn_stock_industry_change"),
                        [&](const pq::TableView &v) {
                          pq::Col date = v.col("date"), inst = v.col("instrument");
                          pq::Col ind = v.col("industry"), lvl = v.col("industry_level");
                          pq::Col flag = v.col("change_flag"), name = v.col("industry_name");
                          EventRowMemo memo(axes, -1);
                          for (std::int64_t i = 0, nr = v.rows(); i < nr; ++i) {
                            if (ind.str(i) != "sw2021")
                              continue;
                            if (lvl.i32(i, 0) != 1)
                              continue;
                            if (flag.i32(i, -1) != 1)
                              continue;
                            int row = memo.row(date.yyyymmdd(i));
                            if (row < 0)
                              continue;
                            int a = lookup_a(axes, inst.str(i));
                            if (a < 0)
                              continue;
                            IndustryEv ev{row, sw2021_l1_name_to_id(name.str(i))};
                            std::lock_guard<std::mutex> lk(mu[static_cast<std::size_t>(a)]);
                            p.industry_change[static_cast<std::size_t>(a)].push_back(ev);
                          }
                        });

  sort_events(p.ttm);
  sort_events(p.balance);
  sort_events(p.income_annual);
  sort_events(p.dividend);
  sort_events(p.forecast);
  sort_events(p.industry_component);
  sort_events(p.industry_change);
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
  int Y = year_of(fe.end_date);
  int deadline_d = -1;
  if (Y > 0) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%04d0430", Y + 1);
    auto it = std::lower_bound(axes.dates.begin(), axes.dates.end(),
                               std::string(buf));
    deadline_d = (it == axes.dates.end())
                     ? n_d
                     : static_cast<int>(std::distance(axes.dates.begin(), it));
  }
  int off = n_d;
  if (financial_d >= 0)
    off = std::min(off, financial_d);
  if (deadline_d >= 0)
    off = std::min(off, deadline_d);
  return off;
}

// D 轴日 d 的年份
inline int year_of_d(const Axes &axes, int d) {
  return std::stoi(axes.dates[static_cast<std::size_t>(d)].substr(0, 4));
}

// 首个年份 ≥ y 的 D 轴下标; 无 → n_d
int first_d_of_year(const Axes &axes, int y) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04d0101", y);
  auto it = std::lower_bound(axes.dates.begin(), axes.dates.end(), std::string(buf));
  return static_cast<int>(std::distance(axes.dates.begin(), it));
}

} // namespace

// ============================================================================
// Pool
// ============================================================================
Pool::Pool() = default;
Pool::~Pool() = default;

int Pool::date_index(const std::string &yyyymmdd) const {
  const auto &idx = data().axes.date_idx;
  auto it = idx.find(yyyymmdd);
  return it == idx.end() ? -1 : it->second;
}

void Pool::build(const std::vector<std::string> &codes,
                 const std::vector<std::string> &dates) {
  assert(!codes.empty() && "AssetAxis 为空");
  assert(!dates.empty() && "回测日为空");

  auto t0 = std::chrono::steady_clock::now();
  auto data = std::make_unique<Data>();
  Axes &axes = data->axes;
  Meta &meta = data->meta;
  PitPool &pit = data->pit;

  // ---- D 轴: all_trading_days (market_code='CN', 截到 today) ----
  {
    const std::string today = misc::today_yyyymmdd();
    std::set<std::string> trading;
    for (auto &[ym, path] : pq::list_month_files("all_trading_days")) {
      pq::TableView v(pq::read_table(path));
      if (v.rows() == 0)
        continue;
      pq::Col date = v.col("date");
      pq::Col mc = v.col("market_code");
      for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
        if (mc.str(i) != "CN")
          continue;
        std::string s = ymd_str(date.yyyymmdd(i));
        if (s.empty() || s > today)
          continue;
        trading.insert(std::move(s));
      }
    }
    assert(!trading.empty() && "all_trading_days 无 CN 行 (基本面未同步?)");
    axes.dates.assign(trading.begin(), trading.end());
    axes.date_days.reserve(axes.dates.size());
    axes.date_idx.reserve(axes.dates.size());
    for (std::size_t i = 0; i < axes.dates.size(); ++i) {
      axes.date_days.push_back(misc::parse_yyyymmdd(axes.dates[i]));
      axes.date_idx.emplace(axes.dates[i], static_cast<int>(i));
    }
  }

  // ---- A 轴: AssetAxis codes ("000001.SZ") ----
  axes.code_idx.reserve(codes.size());
  for (std::size_t i = 0; i < codes.size(); ++i)
    axes.code_idx.emplace(codes[i], static_cast<int>(i));
  const int n_a = axes.n_a();
  const int n_d = axes.n_d();
  assert(static_cast<std::size_t>(n_a) == codes.size() && "AssetAxis code 重复");

  // ---- 回测日必须全部命中交易日历; 首回测日决定网格切片起点 ----
  int first_d = n_d;
  for (const auto &s : dates) {
    auto it = axes.date_idx.find(s);
    assert(it != axes.date_idx.end() && "回测日不在交易日历里");
    first_d = std::min(first_d, it->second);
  }

  // ---- 静态 meta: cn_stock_basic_info (_meta) ----
  meta.list_day.assign(static_cast<std::size_t>(n_a), {});
  meta.has_list.assign(static_cast<std::size_t>(n_a), 0);
  meta.delist_day.assign(static_cast<std::size_t>(n_a), {});
  meta.has_delist.assign(static_cast<std::size_t>(n_a), 0);
  meta.list_date_str.assign(static_cast<std::size_t>(n_a), {});
  meta.main_board.assign(static_cast<std::size_t>(n_a), 0);
  {
    auto bi_path = pq::meta_path("cn_stock_basic_info");
    assert(std::filesystem::exists(bi_path) && "cn_stock_basic_info 缺失");
    pq::TableView bi(pq::read_table(bi_path));
    pq::Col ins = bi.col("instrument");
    pq::Col ld = bi.col("list_date");
    pq::Col dd = bi.col("delist_date");
    pq::Col ls = bi.col("list_sector");
    for (std::int64_t i = 0, n = bi.rows(); i < n; ++i) {
      int a = lookup_a(axes, ins.str(i));
      if (a < 0)
        continue;
      std::size_t ai = static_cast<std::size_t>(a);
      std::int32_t l = ld.yyyymmdd(i);
      if (l > 0) {
        meta.list_day[ai] = misc::parse_yyyymmdd_int(l);
        meta.has_list[ai] = 1;
        meta.list_date_str[ai] = ymd_str(l);
      }
      std::int32_t dl = dd.yyyymmdd(i);
      if (dl > 0) {
        meta.delist_day[ai] = misc::parse_yyyymmdd_int(dl);
        meta.has_delist[ai] = 1;
      }
      meta.main_board[ai] = (ls.i32(i, 0) == 1) ? 1 : 0;
    }
  }

  // ---- PIT 池: 全 D 轴网格 + 事件链 ----
  build_grids(axes, pit);
  build_events(axes, pit);

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

  // ---- dividend_st 数据轴 warmup (轴起点 + 3 年) ----
  data->axes_warmup_d = first_d_of_year(axes, year_of_d(axes, 0) + 3);

  // ---- 网格切片: 首回测日前留 15 日 (trading_st 连续 15 日计数), 其余历史只在事件链上 ----
  pit.slice(n_a, std::max(0, first_d - 15));

  d_ = std::move(data);

  auto t1 = std::chrono::steady_clock::now();
  double sec = std::chrono::duration<double>(t1 - t0).count();
  std::printf("[fund::Pool] built: %d assets, D axis %d, grid [%d, %d) (%.1fs)\n",
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
    if (meta.has_list[ai])
      stock_warmup_d = first_d_of_year(axes, std::stoi(meta.list_date_str[ai].substr(0, 4)) + 3);
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
            axes.dates[static_cast<std::size_t>(e.v - 1)] >= "20210101")
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
    out[Fund::up_lim] = pos(p.at(p.up_lim, a, d));
    out[Fund::dn_lim] = pos(p.at(p.dn_lim, a, d));
    out[Fund::low_mc_thr] = mb ? 5.0f : 3.0f;

    // 日频因子 raw
    out[Fund::roe_raw] = sat(roe);
    out[Fund::roa_raw] = sat(roa);
    out[Fund::dy_raw] = sat(dy);
    out[Fund::cffoa_raw] = sat(cf_chg);
    out[Fund::mr_bal] = sat(p.at(p.fin_balance, a, d) * 1e-8f);
    out[Fund::ms_bal] = sat(p.at(p.sec_balance, a, d) * 1e-8f);

    // 上市龄 / 退市龄 (日历日; 未上市/未退市 = NaN)
    const auto day = axes.date_days[static_cast<std::size_t>(d)];
    float lage = NaNF, dage = NaNF;
    if (meta.has_list[ai]) {
      const float age = static_cast<float>((day - meta.list_day[ai]).count());
      if (age >= 0.0f)
        lage = age;
    }
    if (meta.has_delist[ai]) {
      const float age = static_cast<float>((day - meta.delist_day[ai]).count());
      if (age >= 0.0f)
        dage = age;
    }
    out[Fund::list_age] = lage;
    out[Fund::delist_age] = dage;

    // 状态 / filter (0/1 常量)
    out[Fund::industry_l1] = static_cast<float>(industry);
    out[Fund::is_margin] = static_cast<float>(p.at(p.is_margin, a, d));
    out[Fund::susp] = static_cast<float>(p.at(p.suspended, a, d));
    out[Fund::risk_warn] = static_cast<float>(p.at(p.st_status, a, d));
    out[Fund::profit_st] = profit_st ? 1.0f : 0.0f;
    out[Fund::revenue_st] = revenue_st ? 1.0f : 0.0f;
    out[Fund::dividend_st] = dividend_st ? 1.0f : 0.0f;
    out[Fund::trading_st] = (run >= 15) ? 1.0f : 0.0f;
    out[Fund::new_list] = (std::isfinite(lage) && lage < 60.0f) ? 1.0f : 0.0f;
  }
};

Stream::Stream(const Pool &pool, std::size_t asset_id)
    : s_(std::make_unique<State>(pool.data(), static_cast<int>(asset_id))) {
  assert(static_cast<int>(asset_id) < pool.data().axes.n_a() && "asset_id 越界 (AssetAxis)");
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
