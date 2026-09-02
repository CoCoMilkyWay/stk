// Fundamental Service - 基本面数据 sync + AssetInfo 构建
// 抓取: api/bigquant + api/tushare (月度 parquet, 水位增量, 调度见 misc/schedule.hpp)
// 构建: parquet → AssetInfo{stock_info, stock_factor, stock_days}
//   stock_days   ← all_trading_days (market_code='CN', 截到 today)
//   stock_info   ← cn_stock_basic_info (_meta) + cn_stock_instruments (PIT 简称)
//                  + cn_stock_industry_component (最新快照)
//                  + cn_stock_real_bar1d (每股最新行) + cn_stock_status (每股最新行)
//   stock_factor ← cn_stock_real_bar1d.adjust_factor 变点序列 (分红/拆分事件日)
//   mcap/peTTM/pbMRQ/psTTM/pcfNcfTTM/dy{1,3,5}y ← close × total_shares / 财务分母
//                  (最新可见快照, 口径与 L1 特征表一致; 分钟实时版在特征表阶段算)
//                  dy{1,3,5}y 另取 cn_stock_dividend 近 1/3/5 年公告, 年化
#include "gui/task_database/services/FundamentalService.hpp"

#include "api/bigquant/pipeline.hpp"
#include "api/bigquant/spec.hpp"
#include "api/tushare/pipeline.hpp"
#include "api/tushare/spec.hpp"
#include "misc/date.hpp"
#include "misc/parquet.hpp"
#include "shared/AssetInfo.hpp"
#include "shared/Config.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <limits>
#include <map>
#include <set>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace GUI::Database {

namespace {

namespace pq = misc::pq;

// "000001.SZ" → "sz.000001" (与 L2 assets / GUI 查询 key 一致)
std::string to_asset_code(std::string_view instrument) {
  auto dot = instrument.rfind('.');
  assert(dot != std::string_view::npos && "instrument 无交易所后缀");
  std::string ex(instrument.substr(dot + 1));
  std::transform(ex.begin(), ex.end(), ex.begin(), ::tolower);
  return ex + "." + std::string(instrument.substr(0, dot));
}

// 简称里的全角 ASCII 拉回半角. 源里 cn_stock_instruments 的 A 股简称尾巴用
// 的是全角 Ａ (U+FF21) —— 深振业Ａ / 京东方Ａ / 张裕Ａ 一类, 共二十余只; 而
// cn_stock_basic_info 同一批股票写的是半角 A. 界面字体 (MapleMono-NF-CN) 的
// 全角块只覆盖标点, 全角字母一律落到豆腐块, 所以统一取半角: 既能显示, 也让
// 搜索框敲 "深振业A" 命中, 顺带抹平两张表的口径差.
std::string halfwidth_ascii(std::string_view text) {
  std::string out;
  out.reserve(text.size());
  for (std::size_t i = 0; i < text.size();) {
    // 全角 ASCII (U+FF01..U+FF5E) 的 UTF-8 一律是 EF BC/BD xx
    const unsigned char c0 = static_cast<unsigned char>(text[i]);
    if (c0 == 0xEF && i + 2 < text.size()) {
      const unsigned char c1 = static_cast<unsigned char>(text[i + 1]);
      const unsigned char c2 = static_cast<unsigned char>(text[i + 2]);
      const unsigned int cp = 0xF000u | ((c1 & 0x3Fu) << 6) | (c2 & 0x3Fu);
      if (cp >= 0xFF01u && cp <= 0xFF5Eu) {
        out.push_back(static_cast<char>(cp - 0xFF01u + '!'));
        i += 3;
        continue;
      }
    }
    out.push_back(text[i]);
    ++i;
  }
  return out;
}

// instrument → 密集 id. 全月扫描的三张大表各 1186 万行, 逐行做
// to_asset_code (两次堆分配) 再查 std::map 是整个构建的主要开销 (实测单表
// 2.1s, 换成下面这套 0.3s). 这里把它压成: 每个月文件里每个不同的
// instrument 只转一次码, 其余行走 vector 下标.
class CodeIntern {
public:
  std::uint32_t id(std::string_view instrument) {
    auto [it, inserted] =
        ids_.try_emplace(to_asset_code(instrument),
                         static_cast<std::uint32_t>(codes_.size()));
    if (inserted)
      codes_.push_back(it->first);
    return it->second;
  }
  const std::string &code(std::uint32_t id) const { return codes_[id]; }

private:
  std::unordered_map<std::string, std::uint32_t> ids_;
  std::vector<std::string> codes_;
};

// 单个月文件内的 instrument → id 缓存. string_view 指向 arrow buffer,
// 只在持有该文件 TableView 的作用域内有效.
class FileCodes {
public:
  explicit FileCodes(CodeIntern &intern) : intern_(intern) {}
  std::uint32_t operator()(std::string_view instrument) {
    auto it = cache_.find(instrument);
    if (it != cache_.end())
      return it->second;
    return cache_.emplace(instrument, intern_.id(instrument)).first->second;
  }

private:
  CodeIntern &intern_;
  std::unordered_map<std::string_view, std::uint32_t> cache_;
};

// id 索引的累加器按需扩容 (id 在扫描过程中递增分配)
template <typename T>
T &slot(std::vector<T> &v, std::uint32_t id) {
  if (id >= v.size())
    v.resize(id + 1);
  return v[id];
}

// 20150101 → "2015-01-01"; 0 (缺失) → 空串
std::string dash_date(std::int32_t v) {
  if (v <= 0)
    return {};
  char buf[11];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", v / 10000, v / 100 % 100,
                v % 100);
  return std::string(buf, 10);
}

std::string now_str() {
  std::time_t t = std::time(nullptr);
  std::tm tm_buf{};
  localtime_r(&t, &tm_buf);
  char buf[20];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);
  return std::string(buf);
}

// 工作线程 → 协程的进度/结果通道 (shared_ptr 持有, detached 线程安全退出)
struct Job {
  std::atomic<bool> done = false;
  std::mutex mu;
  std::string message; // 阶段说明 (worker 写, io 线程轮询读)
  bool ok = false;     // 构建是否成功 (false = 本地 parquet 缺失)
  AssetInfo assetinfo; // 构建产物
  FundamentalState st; // 构建统计

  void set_message(std::string m) {
    std::lock_guard lk(mu);
    message = std::move(m);
  }
  std::string get_message() {
    std::lock_guard lk(mu);
    return message;
  }
};

// parquet → AssetInfo. 返回 false = 本地数据缺失 (首跑需先联网同步);
// 结构性问题 (缺列/类型不符) 由 TableView 内部 assert fail fast.
bool build_asset_info(Job &job) {
  const std::string today = misc::today_yyyymmdd();

  // ---- 交易日历 ← all_trading_days (market_code='CN', 截到 today) ----
  job.set_message("构建交易日历 (all_trading_days)");
  auto td_files = pq::list_month_files("all_trading_days");
  auto bi_path = pq::meta_path("cn_stock_basic_info");
  if (td_files.empty() || !std::filesystem::exists(bi_path))
    return false; // 首跑: 本地无数据

  std::set<std::string> trading; // dense "YYYYMMDD"
  for (auto &[ym, path] : td_files) {
    pq::TableView v(pq::read_table(path, {"date", "market_code"}));
    if (v.rows() == 0)
      continue;
    pq::Col date = v.col("date");
    pq::Col mc = v.col("market_code");
    for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
      if (mc.str(i) != "CN")
        continue;
      std::int32_t d = date.yyyymmdd(i);
      if (d <= 0)
        continue;
      char dense[9];
      std::snprintf(dense, sizeof(dense), "%08d", d);
      if (std::string_view(dense, 8) > today)
        continue; // 排程提前含未来日, 截到 today
      trading.emplace(dense, 8);
    }
  }
  assert(!trading.empty() && "all_trading_days 无 market_code='CN' 行");

  // 全日历展开 (含周末/节假日 "0" 行) — Browser 日历的节假日标注依赖非交易日行
  auto &stock_days = job.assetinfo.mutable_stock_days();
  stock_days.clear();
  for (const std::string &d :
       misc::iter_days(*trading.begin(), *trading.rbegin())) {
    std::string dashed =
        d.substr(0, 4) + "-" + d.substr(4, 2) + "-" + d.substr(6, 2);
    stock_days.push_back({std::move(dashed), trading.count(d) ? "1" : "0"});
  }
  job.st.trading_days_count = trading.size();

  // ---- 元数据 ← cn_stock_basic_info (_meta 单文件, 全市场含退市) ----
  job.set_message("构建股票元数据 (cn_stock_basic_info)");
  auto &stock_info = job.assetinfo.mutable_stock_info();
  stock_info.clear();
  {
    pq::TableView bi(pq::read_table(bi_path, {"instrument", "name", "list_date", "delist_date"}));
    assert(bi.rows() > 0);
    pq::Col ins = bi.col("instrument");
    pq::Col name = bi.col("name");
    pq::Col ld = bi.col("list_date");
    pq::Col dd = bi.col("delist_date");
    for (std::int64_t i = 0, n = bi.rows(); i < n; ++i) {
      std::string_view s = ins.str(i);
      if (s.empty())
        continue;
      StockInfo &info = stock_info[to_asset_code(s)];
      info.name = halfwidth_ascii(name.str(i));
      info.ipoDate = dash_date(ld.yyyymmdd(i));
      info.outDate = dash_date(dd.yyyymmdd(i));
    }
  }

  // ---- 名称 ← cn_stock_instruments 最新月内最新快照 (逐日 PIT 简称) ----
  // basic_info.name 是过期快照 (ST 摘牌/更名后不回填), 与 status.st_status
  // 当日口径对不上; instruments 的逐日 name 与 st_status 严格一致 (ST/*ST
  // 前缀 ↔ 1/2). 退市股不在 instruments 里, 保留 basic_info 的最后简称.
  job.set_message("构建股票简称 (cn_stock_instruments)");
  {
    auto in_files = pq::list_month_files("cn_stock_instruments");
    for (auto it = in_files.rbegin(); it != in_files.rend(); ++it) {
      pq::TableView v(pq::read_table(it->second, {"date", "instrument", "name"}));
      if (v.rows() == 0)
        continue; // 0 行月 → 往前找
      pq::Col date = v.col("date");
      pq::Col ins = v.col("instrument");
      pq::Col name = v.col("name");
      std::int32_t max_d = 0;
      for (std::int64_t i = 0, n = v.rows(); i < n; ++i)
        max_d = std::max(max_d, date.yyyymmdd(i));
      for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
        if (date.yyyymmdd(i) != max_d)
          continue;
        auto found = stock_info.find(to_asset_code(ins.str(i)));
        if (found == stock_info.end())
          continue;
        found->second.name = halfwidth_ascii(name.str(i));
      }
      break;
    }
  }

  // ---- 行业 ← cn_stock_industry_component 最新月内最新快照 (申万一级) ----
  job.set_message("构建行业归属 (cn_stock_industry_component)");
  auto ic_files = pq::list_month_files("cn_stock_industry_component");
  for (auto it = ic_files.rbegin(); it != ic_files.rend(); ++it) {
    pq::TableView v(pq::read_table(it->second, {"date", "instrument", "industry_level1_code", "industry_level1_name"}));
    if (v.rows() == 0)
      continue; // 0 行月 (拉过为空) → 往前找
    pq::Col date = v.col("date");
    pq::Col ins = v.col("instrument");
    pq::Col code = v.col("industry_level1_code");
    pq::Col name = v.col("industry_level1_name");
    std::int32_t max_d = 0;
    for (std::int64_t i = 0, n = v.rows(); i < n; ++i)
      max_d = std::max(max_d, date.yyyymmdd(i));
    for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
      if (date.yyyymmdd(i) != max_d)
        continue;
      auto found = stock_info.find(to_asset_code(ins.str(i)));
      if (found == stock_info.end())
        continue;
      found->second.ind_code = std::string(code.str(i));
      found->second.ind_name = std::string(name.str(i));
    }
    break;
  }

  CodeIntern intern; // 以下几张表的全月扫描共用一套 id

  // ---- 日频行情 + 复权因子 ← cn_stock_real_bar1d 全月扫描 ----
  //   每股最新行 → update_date/volume/amount/turn;
  //   adjust_factor 全序列 → 排序 → 变点压缩 (分红/拆分事件日, TabBrowser 用)
  struct BarLatest {
    bool present = false; // 该 id 在 real_bar1d 里出现过 (对应原 map 的键存在性)
    std::int32_t date = 0;
    double volume = 0, amount = 0, turn = 0;
    double close = 0; // 不复权真价 (估值快照分子)
  };
  std::vector<BarLatest> bar_latest; // id →
  std::vector<std::vector<std::pair<std::int32_t, float>>> factors;
  {
    auto rb_files = pq::list_month_files("cn_stock_real_bar1d");
    std::size_t total = rb_files.size(), idx = 0;
    for (auto &[ym, path] : rb_files) {
      ++idx;
      job.set_message("扫描日频行情 " + ym + " (" + std::to_string(idx) + "/" +
                      std::to_string(total) + ")");
      pq::TableView v(pq::read_table(path, {"date", "instrument", "adjust_factor", "volume", "amount", "turn", "close"}));
      if (v.rows() == 0)
        continue;
      pq::Col date = v.col("date");
      pq::Col ins = v.col("instrument");
      pq::Col af = v.col("adjust_factor");
      pq::Col vol = v.col("volume");
      pq::Col amt = v.col("amount");
      pq::Col turn = v.col("turn");
      pq::Col close = v.col("close");
      FileCodes code(intern);
      for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
        std::uint32_t id = code(ins.str(i));
        std::int32_t d = date.yyyymmdd(i);
        BarLatest &b = slot(bar_latest, id);
        b.present = true;
        if (d > b.date) {
          b.date = d;
          b.volume = static_cast<double>(vol.f32(i));
          b.amount = static_cast<double>(amt.f32(i));
          b.turn = static_cast<double>(turn.f32(i));
          b.close = static_cast<double>(close.f32(i));
        }
        float f = af.f32(i);
        if (std::isfinite(f))
          slot(factors, id).emplace_back(d, f);
      }
    }
  }

  for (std::uint32_t id = 0; id < bar_latest.size(); ++id) {
    const BarLatest &b = bar_latest[id];
    if (!b.present)
      continue;
    auto found = stock_info.find(intern.code(id));
    if (found == stock_info.end())
      continue;
    StockInfo &info = found->second;
    char buf[32];
    info.update_date = dash_date(b.date);
    std::snprintf(buf, sizeof(buf), "%.0f", b.volume);
    info.volume = buf;
    std::snprintf(buf, sizeof(buf), "%.4f", b.amount);
    info.amount = buf;
    std::snprintf(buf, sizeof(buf), "%.6f", b.turn);
    info.turn = buf;
  }

  // ---- 估值快照 (MCAP/PE/PB/PS/PCF/DY) ← 最新 close × total_shares / 各分母 ----
  //   口径与 L1 特征表一致 (qmt 移植): 分子统一为总市值 mcap = close × total_shares
  //   (不复权真价); PE=mcap/归母净利TTM, PB=mcap/归母权益MRQ, PS=mcap/营业总收入TTM
  //   (≤0脏值→空), PCF=mcap/经营现金流净额TTM; 亏损/负权益/烧钱保留负值.
  //   DY1/3/5 = 近 1/3/5 年税前分红总额年化后 / mcap; 静态快照的股本恒为最新
  //   快照, 故等价于 Σ每股分红/年数/close (L1 dy_raw 用公告日当时股本, 窗口内
  //   有增发时会有微差). 无效 → 留空串 (Table 显示 "-").
  {
    struct ValLatest {
      std::int32_t shares_d = 0;
      double total_shares = 0;
      std::int32_t ttm_d = 0;
      double np = std::numeric_limits<double>::quiet_NaN();
      double rev = std::numeric_limits<double>::quiet_NaN();
      double cf = std::numeric_limits<double>::quiet_NaN();
      std::int32_t bal_rd = 0, bal_d = 0; // MRQ: max(report_date), 同 rd 取最新可见
      double equity = std::numeric_limits<double>::quiet_NaN();
      double dps[3] = {0, 0, 0}; // 近 1/3/5 年税前每股分红求和 (无分红 = 0, 非缺失)
    };
    // 三档股息率的窗口长度 [日历日] 与年化除数 [年]
    constexpr int kDyWindows = 3;
    constexpr int kDyDays[kDyWindows] = {365, 1095, 1825};
    constexpr double kDyYears[kDyWindows] = {1.0, 3.0, 5.0};
    std::vector<ValLatest> val; // id →

    // 股本: cn_stock_shares 每股最新行
    {
      auto files = pq::list_month_files("cn_stock_shares");
      std::size_t total = files.size(), idx = 0;
      for (auto &[ym, path] : files) {
        ++idx;
        job.set_message("扫描股本 " + ym + " (" + std::to_string(idx) + "/" +
                        std::to_string(total) + ")");
        pq::TableView v(
            pq::read_table(path, {"date", "instrument", "total_shares"}));
        if (v.rows() == 0)
          continue;
        pq::Col date = v.col("date");
        pq::Col ins = v.col("instrument");
        pq::Col ts = v.col("total_shares");
        FileCodes code(intern);
        for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
          ValLatest &vv = slot(val, code(ins.str(i)));
          std::int32_t d = date.yyyymmdd(i);
          if (d > vv.shares_d) {
            vv.shares_d = d;
            vv.total_shares = static_cast<double>(ts.f32(i));
          }
        }
      }
    }

    // TTM 财务: cn_stock_financial_ttm_shift, shift==0 每股最新行
    {
      auto files = pq::list_month_files("cn_stock_financial_ttm_shift");
      std::size_t total = files.size(), idx = 0;
      for (auto &[ym, path] : files) {
        ++idx;
        job.set_message("扫描财务TTM " + ym + " (" + std::to_string(idx) + "/" +
                        std::to_string(total) + ")");
        pq::TableView v(pq::read_table(path, {"date", "instrument", "shift", "net_profit_to_parent_shareholders_ttm", "total_operating_revenue_ttm", "net_cffoa_ttm"}));
        if (v.rows() == 0)
          continue;
        pq::Col date = v.col("date");
        pq::Col ins = v.col("instrument");
        pq::Col shift = v.col("shift");
        pq::Col np = v.col("net_profit_to_parent_shareholders_ttm");
        pq::Col rev = v.col("total_operating_revenue_ttm");
        pq::Col cf = v.col("net_cffoa_ttm");
        FileCodes code(intern);
        for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
          if (shift.i32(i, -1) != 0)
            continue;
          ValLatest &vv = slot(val, code(ins.str(i)));
          std::int32_t d = date.yyyymmdd(i);
          if (d > vv.ttm_d) {
            vv.ttm_d = d;
            vv.np = static_cast<double>(np.f32(i));
            vv.rev = static_cast<double>(rev.f32(i));
            vv.cf = static_cast<double>(cf.f32(i));
          }
        }
      }
    }

    // 权益 MRQ: cn_stock_financial_balance_general_pit, max(report_date) 的最新可见行
    {
      auto files = pq::list_month_files("cn_stock_financial_balance_general_pit");
      std::size_t total = files.size(), idx = 0;
      for (auto &[ym, path] : files) {
        ++idx;
        job.set_message("扫描资产负债 " + ym + " (" + std::to_string(idx) + "/" +
                        std::to_string(total) + ")");
        pq::TableView v(pq::read_table(path, {"date", "instrument", "report_date", "total_equity_to_parent_shareholders"}));
        if (v.rows() == 0)
          continue;
        pq::Col date = v.col("date");
        pq::Col ins = v.col("instrument");
        pq::Col rd = v.col("report_date");
        pq::Col eq = v.col("total_equity_to_parent_shareholders");
        FileCodes code(intern);
        for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
          ValLatest &vv = slot(val, code(ins.str(i)));
          std::int32_t r = rd.yyyymmdd(i);
          std::int32_t d = date.yyyymmdd(i);
          if (r > vv.bal_rd || (r == vv.bal_rd && d > vv.bal_d)) {
            vv.bal_rd = r;
            vv.bal_d = d;
            vv.equity = static_cast<double>(eq.f32(i));
          }
        }
      }
    }

    // 分红: cn_stock_dividend, publish_date ∈ (today-N日, today] 的税前每股分红
    // 求和, 三档窗口各自累加. 锚定公告日 (非除权日) — 与 L1 dy_raw 同口径.
    {
      std::int32_t div_lo[kDyWindows];
      for (int w = 0; w < kDyWindows; ++w)
        div_lo[w] = misc::to_yyyymmdd_int(misc::add_days(today, -kDyDays[w]));
      const std::int32_t div_hi = misc::to_yyyymmdd_int(today);
      auto files = pq::list_month_files("cn_stock_dividend");
      std::size_t total = files.size(), idx = 0;
      for (auto &[ym, path] : files) {
        ++idx;
        job.set_message("扫描分红 " + ym + " (" + std::to_string(idx) + "/" +
                        std::to_string(total) + ")");
        pq::TableView v(pq::read_table(
            path, {"instrument", "publish_date", "cash_before_tax"}));
        if (v.rows() == 0)
          continue;
        pq::Col ins = v.col("instrument");
        pq::Col pd = v.col("publish_date");
        pq::Col cash = v.col("cash_before_tax");
        FileCodes code(intern);
        for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
          std::int32_t d = pd.yyyymmdd(i);
          if (d <= div_lo[kDyWindows - 1] || d > div_hi)
            continue; // 最长窗口都不覆盖 → 三档都用不上
          float c = cash.f32(i);
          if (!std::isfinite(c) || c <= 0.0f)
            continue;
          ValLatest &vv = slot(val, code(ins.str(i)));
          for (int w = 0; w < kDyWindows; ++w)
            if (d > div_lo[w])
              vv.dps[w] += static_cast<double>(c);
        }
      }
    }

    job.set_message("计算估值快照 (MCAP/PE/PB/PS/PCF/DY)");
    const auto today_days = misc::parse_yyyymmdd(today);
    for (std::uint32_t id = 0; id < val.size(); ++id) {
      const ValLatest &vv = val[id];
      auto found = stock_info.find(intern.code(id));
      if (found == stock_info.end())
        continue;
      if (id >= bar_latest.size() || !bar_latest[id].present)
        continue;
      const double close = bar_latest[id].close;
      if (!std::isfinite(close) || close <= 0.0 || vv.total_shares <= 0.0)
        continue;
      const double mcap = close * vv.total_shares;
      StockInfo &info = found->second;
      char buf[32];
      std::snprintf(buf, sizeof(buf), "%.4f", mcap / 1e8); // [亿元]
      info.mcap = buf;
      auto set_ratio = [&](std::string &dst, double den, bool positive_only) {
        if (!std::isfinite(den) || den == 0.0 || (positive_only && den <= 0.0))
          return; // 留空 → Table 显示 "-"
        std::snprintf(buf, sizeof(buf), "%.4f", mcap / den);
        dst = buf;
      };
      set_ratio(info.peTTM, vv.np, false);
      set_ratio(info.pbMRQ, vv.equity, false);
      set_ratio(info.psTTM, vv.rev, true); // 负营收 = 源脏值
      set_ratio(info.pcfNcfTTM, vv.cf, false);

      // 年化股息率: Σ每股分红 / 年数 / close (等价于 Σ(分红×股本)/年数/mcap).
      // 年数取 min(窗长, 上市年数) — 否则次新股会被窗长系统性摊薄.
      // ipoDate 缺失 ⇒ 视为已满窗 (退回固定除数).
      double listed_years = std::numeric_limits<double>::infinity();
      if (info.ipoDate.size() == 10) {
        const std::string ipo = info.ipoDate.substr(0, 4) +
                                info.ipoDate.substr(5, 2) +
                                info.ipoDate.substr(8, 2);
        listed_years = (today_days - misc::parse_yyyymmdd(ipo)).count() / 365.0;
      }
      std::string *dy[kDyWindows] = {&info.dy1y, &info.dy3y, &info.dy5y};
      for (int w = 0; w < kDyWindows; ++w) {
        const double years = std::min(kDyYears[w], listed_years);
        if (years < 0.25)
          continue; // 上市不足一季度, 年化无意义 → 留空
        // 无分红是"确知的 0", 不是缺失 → 显式落 0.0000 而非留空
        std::snprintf(buf, sizeof(buf), "%.4f",
                      vv.dps[w] / years / close * 100.0); // [%]
        *dy[w] = buf;
      }
    }
  }

  job.set_message("压缩复权因子变点序列");
  auto &stock_factor = job.assetinfo.mutable_stock_factor();
  stock_factor.clear();
  const std::string factor_update = today.substr(0, 4) + "-" +
                                    today.substr(4, 2) + "-" +
                                    today.substr(6, 2);
  for (std::uint32_t id = 0; id < factors.size(); ++id) {
    auto &seq = factors[id];
    if (seq.empty())
      continue; // 该 id 无有限 adjust_factor (对应原 map 无此键)
    std::sort(seq.begin(), seq.end());
    StockFactorData sfd;
    sfd.last_update = factor_update;
    float prev = 0.0f;
    for (auto &[d, f] : seq) {
      if (!sfd.data.empty() && std::abs(f / prev - 1.0f) < 1e-5f)
        continue; // 非变点
      char buf[32];
      std::snprintf(buf, sizeof(buf), "%.6f", static_cast<double>(f));
      sfd.data.push_back({dash_date(d), buf});
      prev = f;
    }
    stock_factor.emplace(intern.code(id), std::move(sfd));
  }

  // ---- 状态 ← cn_stock_status ----
  //   每股最新行 → st_status / tradestatus;
  //   suspended≠0 的 (date, code) 全量 → suspended_ (逐日停牌名单).
  //   Browser 完整性把停牌股从当日分母里剔掉 — 全天停牌本就无逐笔可编码.
  {
    auto st_files = pq::list_month_files("cn_stock_status");
    std::size_t total = st_files.size(), idx = 0;
    struct StatusLatest {
      bool present = false; // 该 id 在 cn_stock_status 里出现过
      std::int32_t date = 0;
      int st = 0, suspended = 0;
    };
    std::vector<StatusLatest> latest; // id →
    auto &suspended = job.assetinfo.mutable_suspended();
    suspended.clear();
    for (auto &[ym, path] : st_files) {
      ++idx;
      job.set_message("扫描股票状态 " + ym + " (" + std::to_string(idx) + "/" +
                      std::to_string(total) + ")");
      pq::TableView v(pq::read_table(path, {"date", "instrument", "st_status", "suspended"}));
      if (v.rows() == 0)
        continue;
      pq::Col date = v.col("date");
      pq::Col ins = v.col("instrument");
      pq::Col st = v.col("st_status");
      pq::Col sp = v.col("suspended");
      FileCodes code(intern);
      for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
        std::uint32_t id = code(ins.str(i));
        std::int32_t d = date.yyyymmdd(i);
        int suspended_flag = sp.i32(i, 0);
        StatusLatest &cur = slot(latest, id);
        cur.present = true;
        if (d > cur.date)
          cur = {true, d, st.i32(i, 0), suspended_flag};
        if (suspended_flag != 0 && d > 0) {
          char dense[9];
          std::snprintf(dense, sizeof(dense), "%08d", d);
          suspended[std::string(dense, 8)].insert(intern.code(id));
        }
      }
    }
    for (std::uint32_t id = 0; id < latest.size(); ++id) {
      const StatusLatest &val = latest[id];
      if (!val.present)
        continue;
      auto found = stock_info.find(intern.code(id));
      if (found == stock_info.end())
        continue;
      // st_status 原值直传: 0=正常, 1=ST, 2=*ST (退市风险警示)
      found->second.isST = std::to_string(val.st);
      found->second.tradestatus = val.suspended != 0 ? "0" : "1";
    }
  }

  // ---- 统计 (trading_days_count 已在日历段填好) ----
  job.st.stock_count = stock_info.size();
  job.st.factor_stock_count = stock_factor.size();
  if (!stock_days.empty()) {
    job.st.date_range_start = stock_days.front()[0];
    job.st.date_range_end = stock_days.back()[0];
  }
  return true;
}

} // namespace

awaitable<void> FundamentalService::update_all() {
  co_await run(/*with_network=*/true);
}

awaitable<void> FundamentalService::run(bool with_network) {
  if (busy_.exchange(true))
    co_return; // 已有一轮在跑

  auto job = std::make_shared<Job>();
  state_.status =
      with_network ? FundamentalStatus::Updating : FundamentalStatus::Building;

  // 阻塞网络 + parquet IO 全在工作线程; 协程只轮询. 异常不捕获 (fail fast).
  std::thread([job, with_network]() {
    if (with_network) {
      const std::string today = misc::today_yyyymmdd();
      // pending 纯本地判定: 全部表在 dedup 窗口内 / 已到水位 ⇒ 零网络
      job->set_message("pending 判定 (水位/dedup 窗口)");
      bool need = bigquant::pending(config::PIPELINE_START_DATE, today,
                                    bigquant::SPECS,
                                    config::PIPELINE_LOOKBACK_DAYS) ||
                  tushare::pending(config::PIPELINE_START_DATE, today,
                                   tushare::SPECS,
                                   config::PIPELINE_LOOKBACK_DAYS);
      if (need) {
        job->set_message("同步 BigQuant DAI (月度 parquet 水位增量)");
        bigquant::update(config::PIPELINE_START_DATE, today, bigquant::SPECS,
                         config::PIPELINE_LOOKBACK_DAYS);
        job->set_message("同步 Tushare (forecast/express/disclosure)");
        tushare::update(config::PIPELINE_START_DATE, today, tushare::SPECS,
                        config::PIPELINE_LOOKBACK_DAYS);
      }
    }
    job->ok = build_asset_info(*job);
    job->done = true;
  }).detach();

  boost::asio::steady_timer timer(io_);
  while (!job->done) {
    // 构建中实时把工作线程的阶段说明透给 UI
    if (with_network && state_.status == FundamentalStatus::Updating &&
        job->get_message().starts_with("构建"))
      state_.status = FundamentalStatus::Building;
    state_.message = job->get_message();
    timer.expires_after(std::chrono::milliseconds(100));
    co_await timer.async_wait(boost::asio::use_awaitable);
  }

  if (job->ok) {
    // AssetInfo 替换只发生在 io 线程 (GUI 消费与此同线程, 无竞争)
    data_.assetinfo = std::move(job->assetinfo);
    data_.assetinfo.rebuild_cache();
    state_ = job->st;
    state_.status = FundamentalStatus::Ready;
    state_.message.clear();
    state_.last_update = now_str();
  } else {
    state_.status = FundamentalStatus::Error;
    state_.message = "本地 parquet 缺失 — 点击 Update 联网同步";
  }
  busy_ = false;
}

} // namespace GUI::Database
