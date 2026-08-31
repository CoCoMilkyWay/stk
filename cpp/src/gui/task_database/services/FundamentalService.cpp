// Fundamental Service - 基本面数据 sync + AssetInfo 构建
// 抓取: api/bigquant + api/tushare (月度 parquet, 水位增量, 调度见 misc/schedule.hpp)
// 构建: parquet → AssetInfo{stock_info, stock_factor, stock_days}
//   stock_days   ← all_trading_days (market_code='CN', 截到 today)
//   stock_info   ← cn_stock_basic_info (_meta) + cn_stock_instruments (PIT 简称)
//                  + cn_stock_industry_component (最新快照)
//                  + cn_stock_real_bar1d (每股最新行) + cn_stock_status (每股最新行)
//   stock_factor ← cn_stock_real_bar1d.adjust_factor 变点序列 (分红/拆分事件日)
//   peTTM/pbMRQ/psTTM/pcfNcfTTM 由特征表阶段用财务表自算, 此处不填.
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
#include <map>
#include <set>
#include <thread>
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
    pq::TableView v(pq::read_table(path));
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
    pq::TableView bi(pq::read_table(bi_path));
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
      info.name = std::string(name.str(i));
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
      pq::TableView v(pq::read_table(it->second));
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
        found->second.name = std::string(name.str(i));
      }
      break;
    }
  }

  // ---- 行业 ← cn_stock_industry_component 最新月内最新快照 (申万一级) ----
  job.set_message("构建行业归属 (cn_stock_industry_component)");
  auto ic_files = pq::list_month_files("cn_stock_industry_component");
  for (auto it = ic_files.rbegin(); it != ic_files.rend(); ++it) {
    pq::TableView v(pq::read_table(it->second));
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

  // ---- 日频行情 + 复权因子 ← cn_stock_real_bar1d 全月扫描 ----
  //   每股最新行 → update_date/volume/amount/turn;
  //   adjust_factor 全序列 → 排序 → 变点压缩 (分红/拆分事件日, TabBrowser 用)
  struct BarLatest {
    std::int32_t date = 0;
    double volume = 0, amount = 0, turn = 0;
  };
  std::map<std::string, BarLatest> bar_latest; // bs_code →
  std::map<std::string, std::vector<std::pair<std::int32_t, float>>> factors;
  {
    auto rb_files = pq::list_month_files("cn_stock_real_bar1d");
    std::size_t total = rb_files.size(), idx = 0;
    for (auto &[ym, path] : rb_files) {
      ++idx;
      job.set_message("扫描日频行情 " + ym + " (" + std::to_string(idx) + "/" +
                      std::to_string(total) + ")");
      pq::TableView v(pq::read_table(path));
      if (v.rows() == 0)
        continue;
      pq::Col date = v.col("date");
      pq::Col ins = v.col("instrument");
      pq::Col af = v.col("adjust_factor");
      pq::Col vol = v.col("volume");
      pq::Col amt = v.col("amount");
      pq::Col turn = v.col("turn");
      for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
        std::string key = to_asset_code(ins.str(i));
        std::int32_t d = date.yyyymmdd(i);
        BarLatest &b = bar_latest[key];
        if (d > b.date) {
          b.date = d;
          b.volume = static_cast<double>(vol.f32(i));
          b.amount = static_cast<double>(amt.f32(i));
          b.turn = static_cast<double>(turn.f32(i));
        }
        float f = af.f32(i);
        if (std::isfinite(f))
          factors[key].emplace_back(d, f);
      }
    }
  }

  for (auto &[key, b] : bar_latest) {
    auto found = stock_info.find(key);
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

  job.set_message("压缩复权因子变点序列");
  auto &stock_factor = job.assetinfo.mutable_stock_factor();
  stock_factor.clear();
  const std::string factor_update = today.substr(0, 4) + "-" +
                                    today.substr(4, 2) + "-" +
                                    today.substr(6, 2);
  for (auto &[key, seq] : factors) {
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
    stock_factor.emplace(key, std::move(sfd));
  }

  // ---- 状态 ← cn_stock_status ----
  //   每股最新行 → st_status / tradestatus;
  //   suspended≠0 的 (date, code) 全量 → suspended_ (逐日停牌名单).
  //   Browser 完整性把停牌股从当日分母里剔掉 — 全天停牌本就无逐笔可编码.
  {
    auto st_files = pq::list_month_files("cn_stock_status");
    std::size_t total = st_files.size(), idx = 0;
    std::map<std::string, std::pair<std::int32_t, std::pair<int, int>>>
        latest; // bs_code → (date, (st_status, suspended))
    auto &suspended = job.assetinfo.mutable_suspended();
    suspended.clear();
    for (auto &[ym, path] : st_files) {
      ++idx;
      job.set_message("扫描股票状态 " + ym + " (" + std::to_string(idx) + "/" +
                      std::to_string(total) + ")");
      pq::TableView v(pq::read_table(path));
      if (v.rows() == 0)
        continue;
      pq::Col date = v.col("date");
      pq::Col ins = v.col("instrument");
      pq::Col st = v.col("st_status");
      pq::Col sp = v.col("suspended");
      for (std::int64_t i = 0, n = v.rows(); i < n; ++i) {
        std::string key = to_asset_code(ins.str(i));
        std::int32_t d = date.yyyymmdd(i);
        int suspended_flag = sp.i32(i, 0);
        auto &cur = latest[key];
        if (d > cur.first)
          cur = {d, {st.i32(i, 0), suspended_flag}};
        if (suspended_flag != 0 && d > 0) {
          char dense[9];
          std::snprintf(dense, sizeof(dense), "%08d", d);
          suspended[std::string(dense, 8)].insert(std::move(key));
        }
      }
    }
    for (auto &[key, val] : latest) {
      auto found = stock_info.find(key);
      if (found == stock_info.end())
        continue;
      // st_status 原值直传: 0=正常, 1=ST, 2=*ST (退市风险警示)
      found->second.isST = std::to_string(val.second.first);
      found->second.tradestatus = val.second.second != 0 ? "0" : "1";
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
