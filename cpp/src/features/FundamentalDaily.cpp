// FundamentalDaily — 日频 PIT 基本面预计算实现 (qmt pit.cpp + def/ 链路移植)
//
// 结构: parquet 月度分片 → 网格/事件池 (raw cutoff 单点应用) → per-A 日频序列
//   (估值分母 / 因子 raw / filter 状态机) → 回测日采样 → 纯 float 行网格
//   (fund::kCount 列, 缺失 = NaN, fp16 存不下的极值也归 NaN).
//
// 注意: 本文件依赖 NaN 语义, 必须在 CMake PRECISE_MATH 列表里 (-fno-fast-math).
#include "features/Fundamental/FundamentalDaily.hpp"

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
#include <unordered_map>
#include <utility>
#include <vector>

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

// ============================================================================
// SW2021 一级行业 (与 qmt feature/industry.hpp 同表同 ID, 顺序不得改动)
// ============================================================================
constexpr std::string_view SW2021_L1_NAMES[32] = {
    "未知", "交通运输", "传媒", "公用事业", "农林牧渔", "医药生物", "商贸零售",
    "国防军工", "基础化工", "家用电器", "建筑材料", "建筑装饰", "房地产",
    "有色金属", "机械设备", "汽车", "煤炭", "环保", "电力设备", "电子",
    "石油石化", "社会服务", "纺织服饰", "综合", "美容护理", "计算机",
    "轻工制造", "通信", "钢铁", "银行", "非银金融", "食品饮料"};

std::uint8_t sw2021_l1_name_to_id(std::string_view name) {
  for (std::uint8_t i = 1; i < 32; ++i)
    if (SW2021_L1_NAMES[i] == name)
      return i;
  return 0;
}

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

struct Pool {
  // 网格 [a-major, d-minor]
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
};

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
void build_grids(const Axes &axes, Pool &p) {
  const std::size_t n = static_cast<std::size_t>(axes.n_a()) *
                        static_cast<std::size_t>(axes.n_d());
  const std::size_t n_d = static_cast<std::size_t>(axes.n_d());

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

void build_events(const Axes &axes, Pool &p) {
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
// 财务扫描 helper (qmt def/detail.hpp 移植; 上市前事件丢弃)
// ============================================================================

// ttm 流: per-A 沿 v 升序取 latest event; compute(d, ev*) 写 out[d]
template <class Compute>
void scan_latest_ttm(int a, const Axes &axes, const Pool &pool, int list_d,
                     float *out, Compute compute) {
  int n_d = axes.n_d();
  const auto &events = pool.ttm[static_cast<std::size_t>(a)];
  std::size_t ep = 0;
  int last_idx = -1;
  for (int d = 0; d < n_d; ++d) {
    while (ep < events.size() && events[ep].v <= d) {
      if (events[ep].v >= list_d)
        last_idx = static_cast<int>(ep);
      ++ep;
    }
    out[d] = (last_idx >= 0) ? compute(d, events[static_cast<std::size_t>(last_idx)]) : NaNF;
  }
}

// balance 流: 维护 map<report_date, ev>, 取 max(report_date) (MRQ)
template <class Compute>
void scan_latest_balance(int a, const Axes &axes, const Pool &pool, int list_d,
                         float *out, Compute compute) {
  int n_d = axes.n_d();
  const auto &events = pool.balance[static_cast<std::size_t>(a)];
  std::map<std::int32_t, FinancialBalanceEv> latest_by_rd;
  std::size_t ep = 0;
  for (int d = 0; d < n_d; ++d) {
    while (ep < events.size() && events[ep].v <= d) {
      if (events[ep].v >= list_d)
        latest_by_rd[events[ep].report_date] = events[ep];
      ++ep;
    }
    out[d] = latest_by_rd.empty() ? NaNF : compute(d, latest_by_rd.rbegin()->second);
  }
}

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

// ttm + balance 双流 (roe/roa)
template <class Compute>
void scan_latest_ttm_and_balance(int a, const Axes &axes, const Pool &pool,
                                 int list_d, float *out, Compute compute) {
  int n_d = axes.n_d();
  const auto &ttms = pool.ttm[static_cast<std::size_t>(a)];
  const auto &bals = pool.balance[static_cast<std::size_t>(a)];
  std::map<std::int32_t, FinancialBalanceEv> latest_by_rd;
  std::size_t tp = 0, bp = 0;
  int last_ttm = -1;
  for (int d = 0; d < n_d; ++d) {
    while (tp < ttms.size() && ttms[tp].v <= d) {
      if (ttms[tp].v >= list_d)
        last_ttm = static_cast<int>(tp);
      ++tp;
    }
    while (bp < bals.size() && bals[bp].v <= d) {
      if (bals[bp].v >= list_d)
        latest_by_rd[bals[bp].report_date] = bals[bp];
      ++bp;
    }
    out[d] = (last_ttm < 0 || latest_by_rd.empty())
                 ? NaNF
                 : compute(d, ttms[static_cast<std::size_t>(last_ttm)], latest_by_rd);
  }
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

// 区间状态机: 触发事件 [ev.v, find_off(ev)) 写 1, 多触发取并集
template <class TEv, class FindOff>
void state_machine_intervals(const std::vector<TEv> &triggers, int n_d,
                             FindOff find_off, float *dst) {
  std::fill(dst, dst + n_d, 0.0f);
  for (const TEv &e : triggers) {
    int on_d = e.v;
    int off_d = find_off(e);
    if (on_d < 0)
      on_d = 0;
    if (off_d > n_d)
      off_d = n_d;
    for (int d = on_d; d < off_d; ++d)
      dst[d] = 1.0f;
  }
}

} // anonymous namespace

// ============================================================================
// FundamentalDaily::build
// ============================================================================
void FundamentalDaily::build(const std::vector<std::string> &codes,
                             const std::vector<std::string> &dates) {
  assert(!codes.empty() && "AssetAxis 为空");
  assert(!dates.empty() && "回测日为空");

  auto t0 = std::chrono::steady_clock::now();

  // ---- D 轴: all_trading_days (market_code='CN', 截到 today) ----
  Axes axes;
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

  // ---- 回测日 → D 轴 idx (必须全部命中交易日历) ----
  std::vector<int> sample_d(dates.size());
  for (std::size_t i = 0; i < dates.size(); ++i) {
    auto it = axes.date_idx.find(dates[i]);
    assert(it != axes.date_idx.end() && "回测日不在交易日历里");
    sample_d[i] = it->second;
  }

  // ---- 静态 meta: cn_stock_basic_info (_meta) ----
  Meta meta;
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

  // ---- PIT 池构建 ----
  Pool pool;
  build_grids(axes, pool);
  build_events(axes, pool);

  // ---- 输出网格 ----
  dates_ = dates;
  date_idx_.clear();
  for (std::size_t i = 0; i < dates_.size(); ++i)
    date_idx_.emplace(dates_[i], i);
  n_a_ = static_cast<std::size_t>(n_a);
  grid_.assign(dates_.size() * n_a_ * fund::kCount, NaNF);

  // ---- dividend_st 数据轴 warmup (轴起点 + 3 年) ----
  int axes_warmup_d = n_d;
  {
    int start_y = std::stoi(axes.dates[0].substr(0, 4));
    for (int d = 0; d < n_d; ++d) {
      if (std::stoi(axes.dates[static_cast<std::size_t>(d)].substr(0, 4)) >=
          start_y + 3) {
        axes_warmup_d = d;
        break;
      }
    }
  }

  // ---- per-A 并行: 日频序列计算 + 回测日采样 ----
  std::atomic<int> next{0};
  unsigned nt = std::thread::hardware_concurrency();
  if (nt == 0)
    nt = 1;

  auto worker = [&]() {
    const std::size_t nd = static_cast<std::size_t>(n_d);
    // per-thread scratch (n_d 长的日频序列)
    std::vector<float> close_raw(nd), mcap_raw(nd);
    std::vector<float> np_ttm(nd), rev_ttm(nd), cffoa_ttm(nd), equity_mrq(nd);
    std::vector<float> roe(nd), roa(nd), dy(nd), cffoa_chg(nd);
    std::vector<float> rev_raw(nd), ni_raw(nd);
    std::vector<float> profit_st(nd), revenue_st(nd), dividend_st(nd);
    std::vector<std::uint8_t> industry(nd);

    for (;;) {
      int a = next.fetch_add(1, std::memory_order_relaxed);
      if (a >= n_a)
        break;
      const std::size_t ai = static_cast<std::size_t>(a);
      const std::size_t base = ai * nd;
      const int list_d = get_list_d(a, axes, meta);
      const bool mb = meta.main_board[ai] != 0;

      // -- close_raw / mcap_raw (上市前 0 哨兵, qmt fill_before_list) --
      for (int d = 0; d < n_d; ++d) {
        float c = pool.close[base + static_cast<std::size_t>(d)];
        float s = pool.total_shares[base + static_cast<std::size_t>(d)];
        close_raw[static_cast<std::size_t>(d)] = c;
        mcap_raw[static_cast<std::size_t>(d)] =
            (std::isfinite(c) && std::isfinite(s)) ? c * s : NaNF;
      }
      for (int d = 0; d < std::min(list_d, n_d); ++d) {
        close_raw[static_cast<std::size_t>(d)] = 0.0f;
        mcap_raw[static_cast<std::size_t>(d)] = 0.0f;
      }

      // -- 财务分母序列 (估值 = 分钟价 × 股本 / 这些分母) --
      scan_latest_ttm(a, axes, pool, list_d, np_ttm.data(),
                      [](int, const FinancialTtmEv &e) {
                        float n = e.net_profit_to_parent_shareholders_ttm;
                        return (std::isfinite(n) && n != 0.0f) ? n : NaNF;
                      });
      scan_latest_ttm(a, axes, pool, list_d, rev_ttm.data(),
                      [](int, const FinancialTtmEv &e) {
                        float r = e.total_operating_revenue_ttm;
                        // 负营收是源脏值 (qmt ps_raw 口径): ≤0 → NaN
                        return (std::isfinite(r) && r > 0.0f) ? r : NaNF;
                      });
      scan_latest_ttm(a, axes, pool, list_d, cffoa_ttm.data(),
                      [](int, const FinancialTtmEv &e) {
                        float c = e.net_cffoa_ttm;
                        return (std::isfinite(c) && c != 0.0f) ? c : NaNF;
                      });
      scan_latest_balance(a, axes, pool, list_d, equity_mrq.data(),
                          [](int, const FinancialBalanceEv &e) {
                            float eq = e.total_equity_to_parent_shareholders;
                            return (std::isfinite(eq) && eq != 0.0f) ? eq : NaNF;
                          });

      // -- roe / roa (ttm + balance 双流, avg5 分母) --
      scan_latest_ttm_and_balance(
          a, axes, pool, list_d, roe.data(),
          [](int, const FinancialTtmEv &t,
             const std::map<std::int32_t, FinancialBalanceEv> &by_rd) {
            float n = t.net_profit_to_parent_shareholders_ttm;
            float eq = ttm_window_avg(
                t.report_date, by_rd,
                &FinancialBalanceEv::total_equity_to_parent_shareholders);
            return (std::isfinite(n) && std::isfinite(eq) && eq > 0.0f)
                       ? (n / eq) * 100.0f
                       : NaNF;
          });
      scan_latest_ttm_and_balance(
          a, axes, pool, list_d, roa.data(),
          [](int, const FinancialTtmEv &t,
             const std::map<std::int32_t, FinancialBalanceEv> &by_rd) {
            float n = t.net_profit_ttm;
            float as = ttm_window_avg(t.report_date, by_rd,
                                      &FinancialBalanceEv::total_assets);
            return (std::isfinite(n) && std::isfinite(as) && as > 0.0f)
                       ? (n / as) * 100.0f
                       : NaNF;
          });

      // -- cffoa_raw = tanh((c0 - c4) / mcap) --
      scan_latest_ttm(a, axes, pool, list_d, cffoa_chg.data(),
                      [&](int d, const FinancialTtmEv &e) {
                        float m = mcap_raw[static_cast<std::size_t>(d)];
                        float c0 = e.net_cffoa_ttm;
                        float c4 = e.net_cffoa_ttm_shift4;
                        return (std::isfinite(m) && m > 0.0f &&
                                std::isfinite(c0) && std::isfinite(c4))
                                   ? std::tanh((c0 - c4) / m)
                                   : NaNF;
                      });

      // -- dy_raw: 365 日滑窗税前分红总额 / mcap (公告日锚, 股本取公告日快照) --
      {
        const auto &divs = pool.dividend[ai];
        std::vector<float> amt(divs.size(), 0.0f);
        for (std::size_t i = 0; i < divs.size(); ++i) {
          float c = divs[i].cash_before_tax;
          if (!std::isfinite(c) || c <= 0.0f)
            continue;
          float sh = pool.total_shares[base + static_cast<std::size_t>(divs[i].v)];
          if (std::isfinite(sh))
            amt[i] = c * sh;
        }
        std::size_t lo = 0, hi = 0;
        float cash_sum = 0.0f;
        for (int d = 0; d < n_d; ++d) {
          auto Tlo = axes.date_days[static_cast<std::size_t>(d)] -
                     std::chrono::days{365};
          while (hi < divs.size() && divs[hi].v <= d) {
            cash_sum += amt[hi];
            ++hi;
          }
          while (lo < hi &&
                 axes.date_days[static_cast<std::size_t>(divs[lo].v)] <= Tlo) {
            cash_sum -= amt[lo];
            ++lo;
          }
          if (lo >= hi)
            cash_sum = 0.0f;
          float m = mcap_raw[static_cast<std::size_t>(d)];
          if (!std::isfinite(m) || m <= 0.0f)
            dy[static_cast<std::size_t>(d)] = NaNF;
          else
            dy[static_cast<std::size_t>(d)] = std::max(cash_sum / m, 0.0f);
        }
      }

      // -- rev_raw / ni_raw (filter 依赖) --
      scan_latest_ttm(a, axes, pool, list_d, rev_raw.data(),
                      [](int, const FinancialTtmEv &e) {
                        float r = e.total_operating_revenue_ttm;
                        return (std::isfinite(r) && r > 0.0f) ? r : NaNF;
                      });
      {
        std::fill(ni_raw.begin(), ni_raw.end(), NaNF);
        struct Cell {
          float val;
          int last_v;
        };
        std::vector<std::pair<std::int32_t, Cell>> annuals;
        std::size_t ev_ptr = 0;
        const auto &events = pool.income_annual[ai];
        for (int d = 0; d < n_d; ++d) {
          while (ev_ptr < events.size() && events[ev_ptr].v <= d) {
            const auto &e = events[ev_ptr++];
            if (!std::isfinite(e.net_profit_to_parent_shareholders))
              continue;
            int idx = -1;
            for (std::size_t i = 0; i < annuals.size(); ++i)
              if (annuals[i].first == e.report_date) {
                idx = static_cast<int>(i);
                break;
              }
            if (idx < 0)
              annuals.emplace_back(
                  e.report_date, Cell{e.net_profit_to_parent_shareholders, e.v});
            else
              annuals[static_cast<std::size_t>(idx)].second =
                  Cell{e.net_profit_to_parent_shareholders, e.v};
          }
          if (annuals.empty())
            continue;
          int i0 = -1, i1 = -1, v0 = -1, v1 = -1;
          for (std::size_t i = 0; i < annuals.size(); ++i) {
            int v = annuals[i].second.last_v;
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
            ni_raw[static_cast<std::size_t>(d)] =
                (annuals[static_cast<std::size_t>(i0)].second.val +
                 annuals[static_cast<std::size_t>(i1)].second.val) *
                0.5f;
          else if (i0 >= 0)
            ni_raw[static_cast<std::size_t>(d)] =
                annuals[static_cast<std::size_t>(i0)].second.val;
        }
      }

      // -- profit_st: 年报预亏状态机 --
      {
        std::vector<ForecastEv> trig;
        for (const auto &e : pool.forecast[ai]) {
          if (month_of(e.end_date) != 12)
            continue;
          if (e.type != ForecastType::FirstLoss &&
              e.type != ForecastType::ContinueLoss)
            continue;
          if (!std::isfinite(e.last_parent_net) || e.last_parent_net >= 0.0f)
            continue;
          trig.push_back(e);
        }
        state_machine_intervals(trig, n_d, [&](const ForecastEv &fe) { return find_forecast_off_d(
                                                                           fe, pool.income_annual[ai], axes); }, profit_st.data());
      }

      // -- revenue_st: 主板营收退市预警 (2021 新规后) --
      {
        std::fill(revenue_st.begin(), revenue_st.end(), 0.0f);
        if (mb) {
          for (const auto &e : pool.forecast[ai]) {
            if (month_of(e.end_date) != 12)
              continue;
            if (e.type != ForecastType::FirstLoss &&
                e.type != ForecastType::ContinueLoss)
              continue;
            int end_y = year_of(e.end_date);
            if (end_y < 2021)
              continue;
            if (e.v < 1 || e.v >= n_d)
              continue;
            // ann_date >= 20210101: e.v 是 row D, e.v-1 是 visible_d
            if (axes.dates[static_cast<std::size_t>(e.v - 1)] < "20210101")
              continue;
            int on_d = e.v;
            int off_d = find_forecast_off_d(e, pool.income_annual[ai], axes);
            if (off_d > n_d)
              off_d = n_d;
            float thr = (end_y >= 2024) ? 3e8f : 1e8f;
            for (int d = on_d; d < off_d; ++d) {
              float r = rev_raw[static_cast<std::size_t>(d)];
              if (std::isfinite(r) && r < thr)
                revenue_st[static_cast<std::size_t>(d)] = 1.0f;
            }
          }
        }
      }

      // -- dividend_st: 主板分红不足预警 (3y 双阈值, 阶梯 forward fill) --
      {
        std::fill(dividend_st.begin(), dividend_st.end(), 0.0f);
        if (mb) {
          int stock_warmup_d = n_d;
          if (meta.has_list[ai]) {
            int list_y = static_cast<int>(
                std::stoi(meta.list_date_str[ai].substr(0, 4)));
            for (int d = 0; d < n_d; ++d) {
              if (std::stoi(axes.dates[static_cast<std::size_t>(d)].substr(0, 4)) >=
                  list_y + 3) {
                stock_warmup_d = d;
                break;
              }
            }
          }
          int warmup_d = std::max(axes_warmup_d, stock_warmup_d);
          const auto &divs = pool.dividend[ai];

          auto apply_segment = [&](int seg_start, int seg_end, float val_3ysum) {
            if (seg_start < warmup_d)
              seg_start = warmup_d;
            if (seg_start < 0)
              seg_start = 0;
            if (seg_end > n_d)
              seg_end = n_d;
            for (int d = seg_start; d < seg_end; ++d) {
              float ni = ni_raw[static_cast<std::size_t>(d)];
              if (!std::isfinite(ni) || ni <= 0.0f)
                continue;
              if (val_3ysum < 0.30f * ni && val_3ysum < 5e7f)
                dividend_st[static_cast<std::size_t>(d)] = 1.0f;
            }
          };

          float current_3ysum = 0.0f;
          int next_apply_d = 0;
          for (std::size_t ev_idx = 0; ev_idx < divs.size(); ++ev_idx) {
            const auto &e = divs[ev_idx];
            apply_segment(next_apply_d, e.v, current_3ysum);
            next_apply_d = e.v;

            int ann_y = (e.v >= 1 && e.v < n_d)
                            ? std::stoi(axes.dates[static_cast<std::size_t>(e.v - 1)]
                                            .substr(0, 4))
                            : 0;
            if (ann_y == 0)
              continue;
            int lo_y = ann_y - 3, hi_y = ann_y - 1;
            float sum = 0.0f;
            for (std::size_t j = 0; j <= ev_idx; ++j) {
              const auto &pd = divs[j];
              int py = year_of(pd.report_date);
              if (py < lo_y || py > hi_y)
                continue;
              if (!std::isfinite(pd.cash_after_tax))
                continue;
              float sh =
                  pool.total_shares[base + static_cast<std::size_t>(pd.v)];
              // 上市前 0 哨兵不适用 (grid 未填 0), 但 NaN 需跳过
              if (!std::isfinite(sh))
                continue;
              sum += pd.cash_after_tax * sh;
            }
            current_3ysum = sum;
          }
          apply_segment(next_apply_d, n_d, current_3ysum);
        }
      }

      // -- industry_l1 replay (component 月初快照 + change 月内增量) --
      {
        const auto &comp = pool.industry_component[ai];
        const auto &chg = pool.industry_change[ai];
        std::size_t ic = 0, ig = 0;
        std::uint8_t last_id = 0;
        for (int d = 0; d < n_d; ++d) {
          while (ic < comp.size() && comp[ic].v <= d) {
            last_id = comp[ic].l1_id;
            ++ic;
          }
          while (ig < chg.size() && chg[ig].v <= d) {
            last_id = chg[ig].l1_id;
            ++ig;
          }
          industry[static_cast<std::size_t>(d)] = last_id;
        }
      }

      // -- trading_st: 连续 15 日 (日频 low_p ∨ low_mc), 采样点直接算 run --
      // (与其余序列一起在采样循环外先算全序列)
      // low_p = close ∈ (0,1); low_mc = mcap ∈ (0, thr)
      const float mc_thr = mb ? 5e8f : 3e8f;

      // ---- 采样回测日 ----
      // trading_st run 需要顺序扫描, 单独预算
      // (放在这里避免多一份 n_d scratch: 用局部数组)
      {
        // run 计数序列化为采样值: 遍历一遍 n_d, 遇到采样点记录
        std::size_t si = 0;
        int run = 0;
        std::vector<std::uint8_t> trading_flag(dates_.size(), 0);
        for (int d = 0; d < n_d && si < sample_d.size(); ++d) {
          float c = close_raw[static_cast<std::size_t>(d)];
          float m = mcap_raw[static_cast<std::size_t>(d)];
          bool lp = std::isfinite(c) && c > 0.0f && c < 1.0f;
          bool lmc = std::isfinite(m) && m > 0.0f && m < mc_thr;
          run = (lp || lmc) ? run + 1 : 0;
          if (d == sample_d[si]) {
            trading_flag[si] = (run >= 15) ? 1 : 0;
            ++si;
          }
        }

        for (std::size_t s = 0; s < dates_.size(); ++s) {
          const int d = sample_d[s];
          const std::size_t di = static_cast<std::size_t>(d);
          float *out = grid_.data() + (s * n_a_ + ai) * fund::kCount;

          // fp16 饱和 (下游存 _Float16): 极值 → NaN; 违约束 (+inf 标记) 同归 NaN
          constexpr float kFp16Max = 65504.0f;
          auto sat = [](float v) {
            return (std::isfinite(v) && v > -kFp16Max && v < kFp16Max) ? v
                                                                       : NaNF;
          };
          auto pos = [](float v) {
            return (std::isfinite(v) && v > 0.0f) ? v : NaNF;
          };

          // 股本 / 估值分母 (亿单位)
          out[fund::total_shares] = pos(pool.total_shares[base + di]) * 1e-8f;
          out[fund::float_shares] = pos(pool.a_float_shares[base + di]) * 1e-8f;
          out[fund::net_profit_ttm] = sat(np_ttm[di] * 1e-8f);
          out[fund::equity_mrq] = sat(equity_mrq[di] * 1e-8f);
          out[fund::revenue_ttm] = sat(rev_ttm[di] * 1e-8f);
          out[fund::cffoa_ttm] = sat(cffoa_ttm[di] * 1e-8f);
          out[fund::up_lim] = pos(pool.up_lim[base + di]);
          out[fund::dn_lim] = pos(pool.dn_lim[base + di]);
          out[fund::low_mc_thr] = mb ? 5.0f : 3.0f;

          // 日频因子 raw
          out[fund::roe_raw] = sat(roe[di]);
          out[fund::roa_raw] = sat(roa[di]);
          out[fund::dy_raw] = sat(dy[di]);
          out[fund::cffoa_raw] = sat(cffoa_chg[di]);
          out[fund::mr_bal] = sat(pool.fin_balance[base + di] * 1e-8f);
          out[fund::ms_bal] = sat(pool.sec_balance[base + di] * 1e-8f);

          // 上市龄 / 退市龄 (日历日; 未上市/未退市 = NaN)
          float lage = NaNF;
          if (meta.has_list[ai]) {
            float age = static_cast<float>(
                (axes.date_days[di] - meta.list_day[ai]).count());
            if (age >= 0.0f)
              lage = age;
          }
          out[fund::list_age] = lage;
          if (meta.has_delist[ai]) {
            float age = static_cast<float>(
                (axes.date_days[di] - meta.delist_day[ai]).count());
            if (age >= 0.0f)
              out[fund::delist_age] = age;
          }

          // 状态 / filter (0/1 常量)
          out[fund::industry_l1] = static_cast<float>(industry[di]);
          out[fund::is_margin] = static_cast<float>(pool.is_margin[base + di]);
          out[fund::susp] = static_cast<float>(pool.suspended[base + di]);
          out[fund::risk_warn] = static_cast<float>(pool.st_status[base + di]);
          out[fund::profit_st] = profit_st[di] > 0.5f ? 1.0f : 0.0f;
          out[fund::revenue_st] = revenue_st[di] > 0.5f ? 1.0f : 0.0f;
          out[fund::dividend_st] = dividend_st[di] > 0.5f ? 1.0f : 0.0f;
          out[fund::trading_st] = static_cast<float>(trading_flag[s]);
          out[fund::new_list] =
              (std::isfinite(lage) && lage < 60.0f) ? 1.0f : 0.0f;
        }
      }
    }
  };

  {
    std::vector<std::thread> ts;
    ts.reserve(nt);
    for (unsigned t = 0; t < nt; ++t)
      ts.emplace_back(worker);
    for (auto &t : ts)
      t.join();
  }

  auto t1 = std::chrono::steady_clock::now();
  double sec = std::chrono::duration<double>(t1 - t0).count();
  std::printf("[FundamentalDaily] built: %zu dates x %zu assets (D axis %d, "
              "%.1fs)\n",
              dates_.size(), n_a_, n_d, sec);
}
