// Fundamental Service - 基本面数据 sync + AssetInfo 构建
// 抓取: api/bigquant + api/tushare (月度 parquet, 水位增量, 调度见 misc/schedule.hpp)
// 构建: parquet → AssetInfo{stock_info, stock_factor, stock_days}
//   stock_days   ← all_trading_days (market_code='CN', 截到 today)
//   stock_info   ← cn_stock_basic_info (_meta) + cn_stock_instruments (PIT 简称)
//                  + cn_stock_industry_component (最新快照)
//                  + cn_stock_real_bar1d (最新行 + 最新有效报价行) + cn_stock_status
//   stock_factor ← cn_stock_real_bar1d.adjust_factor 变点序列 (分红/拆分事件日)
//   mcap/peTTM/pbMRQ/psTTM/pcfNcfTTM/dy{1,3,5}y ← close × total_shares / 财务分母
//                  (最新可见快照, 口径与 L1 特征表一致; 分钟实时版在特征表阶段算)
//                  dy{1,3,5}y 另取 cn_stock_dividend 近 1/3/5 年公告, 年化
//
// 扫描架构: 六张月度大表 (real_bar1d / status / shares / financial_ttm_shift /
//   balance_general_pit / dividend) 共 800+ 个分片, 按 (表 × 连续月区间) 切成
//   分片任务喂进 ScanThreadPool. id 空间开扫前就由 cn_stock_basic_info 钉死,
//   所以每个分片的累加器都是定长 vector —— 段间零共享、零锁, 归约只是按段序
//   逐个取最值/求和. 逐行路径上没有哈希也没有日期转换 (见 Universe / DictCol /
//   Col::yyyymmdd_all), 千万级的因子排序也在文件内预压缩后消失.
#include "gui/task_database/services/FundamentalService.hpp"

#include "api/bigquant/pipeline.hpp"
#include "api/bigquant/spec.hpp"
#include "api/tushare/pipeline.hpp"
#include "api/tushare/spec.hpp"
#include "gui/task_database/infrastructure/ScanThreadPool.hpp"
#include "misc/date.hpp"
#include "misc/fs.hpp"
#include "misc/parquet.hpp"
#include "shared/AssetInfo.hpp"
#include "shared/Config.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <functional>
#include <future>
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

// output/fundamental/ 单遍扫描 → 逐表落盘状态 (Overview 逐表列出用).
//   月度分片: 每个 YYYY-MM 目录只 readdir 一次, 顺手把当月所有表的文件归到各自
//   表名下 —— 比"每张表各扫一遍全库"少 30 倍 directory_iterator.
//   _meta 单文件表 (Static/Snapshot): months 恒 0, last_month 留空.
std::vector<TableFileStat> collect_table_stats() {
  namespace fs = std::filesystem;
  const fs::path root = misc::git_root() / "output" / "fundamental";
  assert(fs::exists(root) && "output/fundamental 缺失");

  struct Acc {
    TableFileStat st;
    fs::file_time_type newest{};
    bool has = false;
  };
  std::map<std::string, Acc> acc;

  auto absorb = [&acc](const fs::directory_entry &f, std::string_view ym) {
    if (f.path().extension() != ".parquet")
      return;
    std::string name = f.path().stem().string();
    Acc &a = acc[name];
    a.st.name = std::move(name);
    a.st.bytes += f.file_size();
    if (!ym.empty()) {
      ++a.st.months;
      if (ym > a.st.last_month)
        a.st.last_month = ym;
    }
    const fs::file_time_type mt = f.last_write_time();
    if (!a.has || mt > a.newest) {
      a.newest = mt;
      a.has = true;
    }
  };

  for (const fs::directory_entry &d : fs::directory_iterator(root)) {
    if (!d.is_directory())
      continue;
    const std::string dir = d.path().filename().string();
    const bool meta = (dir == "_meta");
    // 数据集只有 "_meta" 和 "YYYY-MM" 两种目录; 其余 (pool 等) 不是本流水线产物
    if (!meta && !(dir.size() == 7 && dir[4] == '-'))
      continue;
    for (const fs::directory_entry &f : fs::directory_iterator(d.path()))
      absorb(f, meta ? std::string_view{} : std::string_view(dir));
  }

  std::vector<TableFileStat> out;
  out.reserve(acc.size());
  for (auto &[name, a] : acc) {
    if (a.has) {
      const std::time_t t = std::chrono::system_clock::to_time_t(
          std::chrono::clock_cast<std::chrono::system_clock>(a.newest));
      std::tm tm_buf{};
      localtime_r(&t, &tm_buf);
      char buf[16];
      std::snprintf(buf, sizeof(buf), "%02d-%02d %02d:%02d", tm_buf.tm_mon + 1,
                    tm_buf.tm_mday, tm_buf.tm_hour, tm_buf.tm_min);
      a.st.mtime = buf;
    }
    out.push_back(std::move(a.st));
  }
  return out;
}

// 工作线程 → 协程的进度/结果通道 (shared_ptr 持有, detached 线程安全退出)
struct Job {
  std::atomic<bool> done = false;
  std::atomic<std::size_t> files_done{0};  // 并行扫描已完成的月分片数
  std::atomic<std::size_t> files_total{0}; // 0 = 当前不在扫描阶段
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

using MonthFiles = std::vector<std::pair<std::string, std::filesystem::path>>;

// ---------------------------------------------------------------------------
// Universe — id 空间, 钉死在 cn_stock_basic_info 全集上 (5894 只, 含退市).
// 月度表的 instrument 一律是它的子集 (七张表实测 0 例外; 破例则 assert), 于是
// 开扫之前 id 就全部确定: 各表累加器都是定长 vector, 并行分片间零共享、零锁、
// 零 resize; 且 id → StockInfo* 直连 (std::map 节点地址稳定), 归约不再逐股查表.
// ---------------------------------------------------------------------------
struct SvHash {
  using is_transparent = void;
  std::size_t operator()(std::string_view s) const noexcept {
    return std::hash<std::string_view>{}(s);
  }
};
struct SvEq {
  using is_transparent = void;
  bool operator()(std::string_view a, std::string_view b) const noexcept {
    return a == b;
  }
};

class Universe {
public:
  // 键是 instrument 原样 ("000001.SZ"), 月度表可拿源列 string_view 直查, 免临时 string
  void add(std::string_view instrument, std::string code, StockInfo *info) {
    const bool inserted =
        ids_.try_emplace(std::string(instrument),
                         static_cast<std::uint32_t>(codes_.size()))
            .second;
    assert(inserted && "cn_stock_basic_info: instrument 重复");
    codes_.push_back(std::move(code));
    infos_.push_back(info);
  }

  std::uint32_t size() const {
    return static_cast<std::uint32_t>(codes_.size());
  }
  const std::string &code(std::uint32_t id) const { return codes_[id]; }
  StockInfo &info(std::uint32_t id) const { return *infos_[id]; }

  std::uint32_t id(std::string_view instrument) const {
    auto it = ids_.find(instrument);
    assert(it != ids_.end() && "instrument 不在 cn_stock_basic_info 全集内");
    return it->second;
  }

private:
  std::unordered_map<std::string, std::uint32_t, SvHash, SvEq> ids_;
  std::vector<std::string> codes_; // id → "sz.000001"
  std::vector<StockInfo *> infos_; // id → &stock_info[code]
};

// 文件字典 → 全局 id. 每文件只做 dict_size() (≈5400) 次哈希, 逐行退化成一次
// 数组下标 —— 这是省掉 4300 万次 to_asset_code + 哈希查表的关键.
void map_dict(const pq::DictCol &d, const Universe &uni,
              std::vector<std::uint32_t> &out) {
  out.resize(static_cast<std::size_t>(d.dict_size()));
  for (std::int32_t k = 0; k < d.dict_size(); ++k)
    out[static_cast<std::size_t>(k)] = uni.id(d.dict_value(k));
}

// shards 份局部累加器 (每份按 nid 定长分配)
template <typename Local>
std::vector<Local> make_locals(std::size_t shards, std::uint32_t nid) {
  std::vector<Local> v;
  v.reserve(shards);
  for (std::size_t s = 0; s < shards; ++s)
    v.emplace_back(nid);
  return v;
}

// 一张表的并行扫描: files 均分成 locals.size() 段, 第 s 段独占 locals[s].
// 段是 ym 升序的连续区间, 归约也按段序进行 —— 复权因子序列的有序性依赖这点.
// 同表内月文件大小相近, 均分即均衡.
template <typename Local, typename ScanFn>
void submit_table(ScanThreadPool &pool, const MonthFiles &files,
                  std::vector<Local> &locals,
                  std::vector<std::future<void>> &futures,
                  std::atomic<std::size_t> &done, ScanFn scan) {
  const std::size_t shards = locals.size();
  for (std::size_t s = 0; s < shards; ++s) {
    const std::size_t lo = files.size() * s / shards;
    const std::size_t hi = files.size() * (s + 1) / shards;
    if (lo == hi)
      continue;
    futures.push_back(pool.submit([&files, &locals, &done, scan, lo, hi, s] {
      for (std::size_t i = lo; i < hi; ++i) {
        scan(files[i].second, locals[s]);
        done.fetch_add(1, std::memory_order_relaxed);
      }
    }));
  }
}

// 三档股息率的窗口长度 [日历日] 与年化除数 [年]
constexpr int kDyWindows = 3;
constexpr int kDyDays[kDyWindows] = {365, 1095, 1825};
constexpr double kDyYears[kDyWindows] = {1.0, 3.0, 5.0};

// ---------------------------------------------------------------------------
// 各表的分片累加器.
//
// 两条贯穿全表的约定:
//  1) "未出现" 用显式日期 (0 = 从无) 表示, 不拿 NaN 当哨兵. 缺失判据集中在扫描处
//     (Col::null 直读 bitmap), 归约和估值段只看日期是否为 0. 本文件在 CMake 的
//     PRECISE_MATH 列表里 (-fno-fast-math), isfinite 本可用, 但缺失语义不依赖它
//     —— 少一层浮点约定, 将来移出列表也不会静默出错.
//  2) 快照按字段各自取 "最后一个有效值", 而不是整行取 max(date). 源里最新那行常
//     常是半空的 —— 停牌日 close/amount 为 null, 停止披露的公司最后几期财报字段
//     为空 —— 整行取会把该股的估值成片打掉, 而更早的期次里是有值的.
// ---------------------------------------------------------------------------

// cn_stock_real_bar1d: 每股最新行 + 最新有效报价行 + 复权因子变点
//
// date / qdate 必须分开: 停牌日源里也落行, 但 close 与 amount 是 null. 退市前的
// 长停和长期停牌股尾部连着几十行全是这样, 拿最新行当报价快照就把估值整块打掉
// (实测 167 只受影响: 160 退市 + 7 在市). 惯例是停牌股按停牌前收盘价计市值, 故
// date 只当数据水位, 价量四项一律取自最后一个有真实成交价的行 (同日, 不跨日混搭).
// qdate == 0 ⇒ 全区间无成交 ⇒ 市值算不出 ⇒ 该股所有估值列一并留空 (吸收合并 /
// 转板退市的老代码就是这样, 8 只; 这种"要么全有要么全无"是刻意的一致性).
struct BarLatest {
  bool present = false;
  std::int32_t date = 0;                             // 最新行, 含停牌日 → update_date
  std::int32_t qdate = 0;                            // 最新有效报价行; 0 ⇒ 全区间无成交
  float volume = 0, amount = 0, turn = 0, close = 0; // 均取自 qdate 那行
};

struct FactorPoint {
  std::uint32_t id;
  std::int32_t date;
  float f;
};

struct BarLocal {
  explicit BarLocal(std::uint32_t n) : nid(n), latest(n) {}
  std::uint32_t nid;
  std::vector<BarLatest> latest;
  std::vector<FactorPoint> points; // 文件内已压过变点, 顺序 = (ym, id, date) 升序
  // 逐文件复用的临时缓冲
  std::vector<std::int32_t> dates, days;
  std::vector<std::uint32_t> dict;
  std::vector<float> grid;
  std::vector<std::uint8_t> filled;
};

// cn_stock_status: 每股最新行 + 逐日停牌名单
struct StatusLatest {
  bool present = false;
  std::int32_t date = 0;
  int st = 0, suspended = 0;
};

struct StatusLocal {
  explicit StatusLocal(std::uint32_t n) : latest(n) {}
  std::vector<StatusLatest> latest;
  std::vector<std::pair<std::int32_t, std::uint32_t>> halts; // (date, id)
  std::vector<std::int32_t> dates;
  std::vector<std::uint32_t> dict;
};

// cn_stock_shares: 每股最新总股本
struct SharesLatest {
  std::int32_t date = 0;
  float total = 0; // <= 0 ⇒ 未出现 / 源脏值, 估值段跳过
};

struct SharesLocal {
  explicit SharesLocal(std::uint32_t n) : latest(n) {}
  std::vector<SharesLatest> latest;
  std::vector<std::int32_t> dates;
  std::vector<std::uint32_t> dict;
};

// cn_stock_financial_ttm_shift: shift==0, 三个分母各自的最后一个有效期次
// 有效判据就地对齐估值段的门 (np/cf != 0, rev > 0), 故下游只需看 *_d 是否为 0.
struct TtmLatest {
  std::int32_t np_d = 0, rev_d = 0, cf_d = 0; // 各自的 date; 0 = 从无有效值
  float np = 0, rev = 0, cf = 0;
};

struct TtmLocal {
  explicit TtmLocal(std::uint32_t n) : latest(n) {}
  std::vector<TtmLatest> latest;
  std::vector<std::int32_t> dates;
  std::vector<std::uint32_t> dict;
};

// cn_stock_financial_balance_general_pit: eq 有效的行里 (report_date, date) 最大者
struct BalLatest {
  bool present = false; // eq 非 null 且 != 0
  std::int32_t rd = 0, date = 0;
  float eq = 0;
};

struct BalLocal {
  explicit BalLocal(std::uint32_t n) : latest(n) {}
  std::vector<BalLatest> latest;
  std::vector<std::int32_t> dates, rdates;
  std::vector<std::uint32_t> dict;
};

// cn_stock_dividend: 最长窗口内的原始分红行. 全表才 3.5 万行, 留到归约里串行
// 累加 —— 求和顺序与原来的逐文件单线程完全一致, dy 结果逐位相同.
struct DivRow {
  std::uint32_t id;
  std::int32_t date; // publish_date
  float cash;        // 税前每股分红
};

struct DivLocal {
  explicit DivLocal(std::uint32_t) {}
  std::vector<DivRow> rows;
  std::vector<std::int32_t> dates;
  std::vector<std::uint32_t> dict;
};

// parquet → AssetInfo. 返回 false = 本地数据缺失 (首跑需先联网同步);
// 结构性问题 (缺列/类型不符) 由 TableView 内部 assert fail fast.
bool build_asset_info(Job &job) {
  const std::string today = misc::today_yyyymmdd();

  // ---- 首跑本地无数据 → 直接退 ----
  const MonthFiles td_files = pq::list_month_files("all_trading_days");
  const std::filesystem::path bi_path = pq::meta_path("cn_stock_basic_info");
  if (td_files.empty() || !std::filesystem::exists(bi_path))
    return false;

  // ---- 元数据 + id 空间 ← cn_stock_basic_info (_meta 单文件, 全市场含退市) ----
  // 必须先于并行扫描: 后面每个分片的累加器都按这里定下的 id 空间定长分配.
  job.set_message("构建股票元数据 (cn_stock_basic_info)");
  auto &stock_info = job.assetinfo.mutable_stock_info();
  stock_info.clear();
  Universe uni;
  {
    pq::TableView bi(pq::read_table(
        bi_path, {"instrument", "name", "list_date", "delist_date"}));
    assert(bi.rows() > 0);
    pq::Col ins = bi.col("instrument");
    pq::Col name = bi.col("name");
    pq::Col ld = bi.col("list_date");
    pq::Col dd = bi.col("delist_date");
    for (std::int64_t i = 0, n = bi.rows(); i < n; ++i) {
      std::string_view s = ins.str(i);
      if (s.empty())
        continue;
      std::string code = to_asset_code(s);
      StockInfo &info = stock_info[code]; // map 节点地址稳定 ⇒ 可直接登记指针
      info.name = halfwidth_ascii(name.str(i));
      info.ipoDate = dash_date(ld.yyyymmdd(i));
      info.outDate = dash_date(dd.yyyymmdd(i));
      uni.add(s, std::move(code), &info);
    }
  }
  const std::uint32_t nid = uni.size();
  assert(nid > 0 && "cn_stock_basic_info 无有效 instrument");

  // ======================== 并行扫描阶段 ========================
  const MonthFiles rb_files = pq::list_month_files("cn_stock_real_bar1d");
  const MonthFiles tt_files =
      pq::list_month_files("cn_stock_financial_ttm_shift");
  const MonthFiles st_files = pq::list_month_files("cn_stock_status");
  const MonthFiles sh_files = pq::list_month_files("cn_stock_shares");
  const MonthFiles bl_files =
      pq::list_month_files("cn_stock_financial_balance_general_pit");
  const MonthFiles dv_files = pq::list_month_files("cn_stock_dividend");

  // 留 2 核给 io 线程 + GUI 渲染 (实测 8 线程已拿到 96% 的并行收益)
  const unsigned hw = std::thread::hardware_concurrency();
  const std::size_t shards = hw > 3 ? hw - 2 : 1;

  auto bar_locals = make_locals<BarLocal>(shards, nid);
  auto ttm_locals = make_locals<TtmLocal>(shards, nid);
  auto status_locals = make_locals<StatusLocal>(shards, nid);
  auto shares_locals = make_locals<SharesLocal>(shards, nid);
  auto bal_locals = make_locals<BalLocal>(shards, nid);
  auto div_locals = make_locals<DivLocal>(shards, nid);

  // 分红窗口: publish_date ∈ (today-N日, today] (锚定公告日, 非除权日 —
  // 与 L1 dy_raw 同口径)
  std::int32_t div_lo[kDyWindows];
  for (int w = 0; w < kDyWindows; ++w)
    div_lo[w] = misc::to_yyyymmdd_int(misc::add_days(today, -kDyDays[w]));
  const std::int32_t div_lo_min = div_lo[kDyWindows - 1];
  const std::int32_t div_hi = misc::to_yyyymmdd_int(today);

  struct IndRow {
    std::uint32_t id;
    std::string code, name;
  };
  std::set<std::string> trading;                            // dense "YYYYMMDD"
  std::vector<std::pair<std::uint32_t, std::string>> names; // 逐日 PIT 简称
  std::vector<IndRow> inds;

  // ---- 日频行情 + 复权因子 ← cn_stock_real_bar1d ----
  //   最新行 → update_date; 最新有效报价行 → volume/amount/turn/close;
  //   adjust_factor → 文件内变点压缩 (分红/拆分事件日, TabBrowser 用)
  auto scan_bar = [&uni](const std::filesystem::path &path, BarLocal &L) {
    pq::TableView v(pq::read_table(path,
                                   {"date", "instrument", "adjust_factor",
                                    "volume", "amount", "turn", "close"},
                                   {"instrument"}));
    const std::int64_t n = v.rows();
    if (n == 0)
      return;
    pq::DictCol ins = v.dict_col("instrument");
    map_dict(ins, uni, L.dict);
    v.col("date").yyyymmdd_all(L.dates);
    pq::Col af = v.col("adjust_factor");
    pq::Col vol = v.col("volume");
    pq::Col amt = v.col("amount");
    pq::Col turn = v.col("turn");
    pq::Col close = v.col("close");
    const std::int32_t *idx = ins.indices();

    // 该文件的不同 date. 行按日成块 (DAI 返回顺序, 块间不保证升序), 所以只在
    // 块首做一次线性查重 ⇒ 全文件只走 K 次 find.
    L.days.clear();
    std::int32_t seen_d = -1;
    for (std::int64_t i = 0; i < n; ++i) {
      const std::int32_t d = L.dates[static_cast<std::size_t>(i)];
      if (d == seen_d)
        continue;
      seen_d = d;
      if (std::find(L.days.begin(), L.days.end(), d) == L.days.end())
        L.days.push_back(d);
    }
    std::sort(L.days.begin(), L.days.end());
    const std::size_t K = L.days.size();
    assert(K > 0 && K <= 64 && "月分片内不同 date 数异常");

    // [id × 该月 date] 小网格 (5894×≤31×5B ≈ 0.9MB, 贴 L2): 先把因子铺进去,
    // 再按 date 升序逐 id 压变点.
    const std::size_t cells = static_cast<std::size_t>(L.nid) * K;
    L.grid.assign(cells, 0.0f);
    L.filled.assign(cells, 0);

    std::int32_t prev_d = -1;
    std::size_t rank = 0;
    for (std::int64_t i = 0; i < n; ++i) {
      const std::uint32_t id = L.dict[static_cast<std::size_t>(idx[i])];
      const std::int32_t d = L.dates[static_cast<std::size_t>(i)];
      if (d != prev_d) {
        prev_d = d;
        rank = static_cast<std::size_t>(
            std::lower_bound(L.days.begin(), L.days.end(), d) - L.days.begin());
      }
      BarLatest &b = L.latest[id];
      b.present = true;
      if (d > b.date)
        b.date = d;
      // amount 在有效成交行上仍有极少数 null (67/1158万), 落 0 而不是让 NaN 流到
      // snprintf 打出 "nan".
      if (d > b.qdate && !close.null(i)) {
        const float c = close.f32(i);
        if (c > 0.0f) {
          b.qdate = d;
          b.close = c;
          b.volume = vol.null(i) ? 0.0f : vol.f32(i);
          b.amount = amt.null(i) ? 0.0f : amt.f32(i);
          b.turn = turn.null(i) ? 0.0f : turn.f32(i);
        }
      }
      const float f = af.f32(i);
      if (!af.null(i) && f > 0.0f) {
        const std::size_t c = static_cast<std::size_t>(id) * K + rank;
        L.grid[c] = f;
        L.filled[c] = 1;
      }
    }

    // 文件内变点压缩. 这里只 drop "与前一保留值精确相等" 的点, 而全局那步的
    // 判据是 |f/prev-1| < 1e-5 —— 精确相等必然满足它, 所以两级压缩与单级的
    // 结果完全一致: 被 drop 的点等于其前一局部保留值 p, 若 p 被全局保留则
    // 比值为 1, 若 p 被全局 drop 则该点对全局 prev 的比值与 p 的相同.
    for (std::uint32_t id = 0; id < L.nid; ++id) {
      const std::size_t base = static_cast<std::size_t>(id) * K;
      float prev_f = 0.0f;
      bool have = false;
      for (std::size_t r = 0; r < K; ++r) {
        if (!L.filled[base + r])
          continue;
        const float f = L.grid[base + r];
        if (have && f == prev_f)
          continue;
        L.points.push_back({id, L.days[r], f});
        prev_f = f;
        have = true;
      }
    }
  };

  // ---- 状态 ← cn_stock_status ----
  //   每股最新行 → st_status / tradestatus;
  //   suspended≠0 的 (date, id) 全量 → suspended_ (逐日停牌名单).
  //   Browser 完整性把停牌股从当日分母里剔掉 — 全天停牌本就无逐笔可编码.
  auto scan_status = [&uni](const std::filesystem::path &path, StatusLocal &L) {
    pq::TableView v(pq::read_table(
        path, {"date", "instrument", "st_status", "suspended"}, {"instrument"}));
    const std::int64_t n = v.rows();
    if (n == 0)
      return;
    pq::DictCol ins = v.dict_col("instrument");
    map_dict(ins, uni, L.dict);
    v.col("date").yyyymmdd_all(L.dates);
    pq::Col st = v.col("st_status");
    pq::Col sp = v.col("suspended");
    const std::int32_t *idx = ins.indices();
    for (std::int64_t i = 0; i < n; ++i) {
      const std::uint32_t id = L.dict[static_cast<std::size_t>(idx[i])];
      const std::int32_t d = L.dates[static_cast<std::size_t>(i)];
      const int flag = sp.i32(i, 0);
      StatusLatest &cur = L.latest[id];
      cur.present = true;
      if (d > cur.date)
        cur = {true, d, st.i32(i, 0), flag};
      if (flag != 0 && d > 0)
        L.halts.emplace_back(d, id);
    }
  };

  // ---- 股本 ← cn_stock_shares 每股最新行 ----
  auto scan_shares = [&uni](const std::filesystem::path &path, SharesLocal &L) {
    pq::TableView v(pq::read_table(path, {"date", "instrument", "total_shares"},
                                   {"instrument"}));
    const std::int64_t n = v.rows();
    if (n == 0)
      return;
    pq::DictCol ins = v.dict_col("instrument");
    map_dict(ins, uni, L.dict);
    v.col("date").yyyymmdd_all(L.dates);
    pq::Col ts = v.col("total_shares");
    const std::int32_t *idx = ins.indices();
    for (std::int64_t i = 0; i < n; ++i) {
      SharesLatest &c = L.latest[L.dict[static_cast<std::size_t>(idx[i])]];
      const std::int32_t d = L.dates[static_cast<std::size_t>(i)];
      if (d > c.date) {
        c.date = d;
        c.total = ts.null(i) ? 0.0f : ts.f32(i);
      }
    }
  };

  // ---- TTM 财务 ← cn_stock_financial_ttm_shift, shift==0 每股最新行 ----
  auto scan_ttm = [&uni](const std::filesystem::path &path, TtmLocal &L) {
    pq::TableView v(
        pq::read_table(path,
                       {"date", "instrument", "shift",
                        "net_profit_to_parent_shareholders_ttm",
                        "total_operating_revenue_ttm", "net_cffoa_ttm"},
                       {"instrument"}));
    const std::int64_t n = v.rows();
    if (n == 0)
      return;
    pq::DictCol ins = v.dict_col("instrument");
    map_dict(ins, uni, L.dict);
    v.col("date").yyyymmdd_all(L.dates);
    pq::Col shift = v.col("shift");
    pq::Col np = v.col("net_profit_to_parent_shareholders_ttm");
    pq::Col rev = v.col("total_operating_revenue_ttm");
    pq::Col cf = v.col("net_cffoa_ttm");
    const std::int32_t *idx = ins.indices();
    for (std::int64_t i = 0; i < n; ++i) {
      if (shift.i32(i, -1) != 0)
        continue;
      TtmLatest &c = L.latest[L.dict[static_cast<std::size_t>(idx[i])]];
      const std::int32_t d = L.dates[static_cast<std::size_t>(i)];
      // 三个分母各自推进: 源里这三列 null 各约 10 万行, 且停止披露的公司最后几期
      // 是空的. 整行取 max(date) 会让 PE/PS/PCF 成片留空 (窗口内退市股 pe 缺 15、
      // ps 31、pcf 26); 各自回退到最后一个有效期次后, 三项一起收敛到 8 只 —— 那
      // 8 只是市值本身算不出的, 属于"整只全空"的一致情形.
      if (d > c.np_d && !np.null(i)) {
        const float v = np.f32(i);
        if (v != 0.0f) {
          c.np_d = d;
          c.np = v;
        }
      }
      if (d > c.rev_d && !rev.null(i)) {
        const float v = rev.f32(i);
        if (v > 0.0f) { // 负营收 = 源脏值
          c.rev_d = d;
          c.rev = v;
        }
      }
      if (d > c.cf_d && !cf.null(i)) {
        const float v = cf.f32(i);
        if (v != 0.0f) {
          c.cf_d = d;
          c.cf = v;
        }
      }
    }
  };

  // ---- 权益 MRQ ← cn_stock_financial_balance_general_pit ----
  //   max(report_date) 的最新可见行 (同 report_date 取 date 最大者)
  auto scan_bal = [&uni](const std::filesystem::path &path, BalLocal &L) {
    pq::TableView v(pq::read_table(path,
                                   {"date", "instrument", "report_date",
                                    "total_equity_to_parent_shareholders"},
                                   {"instrument"}));
    const std::int64_t n = v.rows();
    if (n == 0)
      return;
    pq::DictCol ins = v.dict_col("instrument");
    map_dict(ins, uni, L.dict);
    v.col("date").yyyymmdd_all(L.dates);
    v.col("report_date").yyyymmdd_all(L.rdates);
    pq::Col eq = v.col("total_equity_to_parent_shareholders");
    const std::int32_t *idx = ins.indices();
    for (std::int64_t i = 0; i < n; ++i) {
      // 先筛有效再定位: 无效行不参与 (report_date, date) 竞争, 否则停止披露公司
      // 最新那份空权益会盖掉上一期的真实值 —— 同 TTM 三列的处理.
      if (eq.null(i))
        continue;
      const float v = eq.f32(i);
      if (v == 0.0f)
        continue;
      BalLatest &c = L.latest[L.dict[static_cast<std::size_t>(idx[i])]];
      const std::int32_t r = L.rdates[static_cast<std::size_t>(i)];
      const std::int32_t d = L.dates[static_cast<std::size_t>(i)];
      if (!c.present || r > c.rd || (r == c.rd && d > c.date))
        c = {true, r, d, v};
    }
  };

  // ---- 分红 ← cn_stock_dividend, 最长窗口内的税前每股分红行 ----
  auto scan_div = [&uni, div_lo_min, div_hi](const std::filesystem::path &path,
                                             DivLocal &L) {
    pq::TableView v(pq::read_table(
        path, {"instrument", "publish_date", "cash_before_tax"},
        {"instrument"}));
    const std::int64_t n = v.rows();
    if (n == 0)
      return;
    pq::DictCol ins = v.dict_col("instrument");
    map_dict(ins, uni, L.dict);
    v.col("publish_date").yyyymmdd_all(L.dates);
    pq::Col cash = v.col("cash_before_tax");
    const std::int32_t *idx = ins.indices();
    for (std::int64_t i = 0; i < n; ++i) {
      const std::int32_t d = L.dates[static_cast<std::size_t>(i)];
      if (d <= div_lo_min || d > div_hi)
        continue; // 最长窗口都不覆盖 → 三档都用不上
      // 源里 625 行 cash_before_tax 是 null, 漏一个进求和就把该股 dy1y/3y/5y 全
      // 打成 "nan" —— 判空统一走 bitmap (见文件上方约定 1).
      const float c = cash.f32(i);
      if (cash.null(i) || c <= 0.0f)
        continue;
      L.rows.push_back({L.dict[static_cast<std::size_t>(idx[i])], d, c});
    }
  };

  // ---- 交易日历 ← all_trading_days (market_code='CN', 截到 today) ----
  // 141 个分片共 100KB, 单任务串行足够
  auto scan_calendar = [&td_files, &trading, &today, &job] {
    for (const auto &[ym, path] : td_files) {
      pq::TableView v(pq::read_table(path, {"date", "market_code"}));
      const std::int64_t n = v.rows();
      job.files_done.fetch_add(1, std::memory_order_relaxed);
      if (n == 0)
        continue;
      pq::Col date = v.col("date");
      pq::Col mc = v.col("market_code");
      for (std::int64_t i = 0; i < n; ++i) {
        if (mc.str(i) != "CN")
          continue;
        const std::int32_t d = date.yyyymmdd(i);
        if (d <= 0)
          continue;
        char dense[9];
        std::snprintf(dense, sizeof(dense), "%08d", d);
        if (std::string_view(dense, 8) > today)
          continue; // 排程提前含未来日, 截到 today
        trading.emplace(dense, 8);
      }
    }
  };

  // ---- 简称 ← cn_stock_instruments 最新非空月内的最新快照 (逐日 PIT 简称) ----
  // basic_info.name 是过期快照 (ST 摘牌/更名后不回填), 与 status.st_status
  // 当日口径对不上; instruments 的逐日 name 与 st_status 严格一致 (ST/*ST
  // 前缀 ↔ 1/2). 退市股不在 instruments 里, 保留 basic_info 的最后简称.
  auto scan_names = [&uni, &names] {
    const MonthFiles files = pq::list_month_files("cn_stock_instruments");
    std::vector<std::int32_t> dates;
    for (auto it = files.rbegin(); it != files.rend(); ++it) {
      pq::TableView v(
          pq::read_table(it->second, {"date", "instrument", "name"}));
      const std::int64_t n = v.rows();
      if (n == 0)
        continue; // 0 行月 → 往前找
      v.col("date").yyyymmdd_all(dates);
      pq::Col ins = v.col("instrument");
      pq::Col name = v.col("name");
      const std::int32_t max_d = *std::max_element(dates.begin(), dates.end());
      for (std::int64_t i = 0; i < n; ++i) {
        if (dates[static_cast<std::size_t>(i)] != max_d)
          continue;
        names.emplace_back(uni.id(ins.str(i)), halfwidth_ascii(name.str(i)));
      }
      break;
    }
  };

  // ---- 行业 ← cn_stock_industry_component 最新非空月内的最新快照 (申万一级) ----
  auto scan_industry = [&uni, &inds] {
    const MonthFiles files = pq::list_month_files("cn_stock_industry_component");
    std::vector<std::int32_t> dates;
    for (auto it = files.rbegin(); it != files.rend(); ++it) {
      pq::TableView v(pq::read_table(it->second,
                                     {"date", "instrument",
                                      "industry_level1_code",
                                      "industry_level1_name"}));
      const std::int64_t n = v.rows();
      if (n == 0)
        continue; // 0 行月 (拉过为空) → 往前找
      v.col("date").yyyymmdd_all(dates);
      pq::Col ins = v.col("instrument");
      pq::Col code = v.col("industry_level1_code");
      pq::Col name = v.col("industry_level1_name");
      const std::int32_t max_d = *std::max_element(dates.begin(), dates.end());
      for (std::int64_t i = 0; i < n; ++i) {
        if (dates[static_cast<std::size_t>(i)] != max_d)
          continue;
        inds.push_back({uni.id(ins.str(i)), std::string(code.str(i)),
                        std::string(name.str(i))});
      }
      break;
    }
  };

  job.files_done = 0;
  job.files_total = rb_files.size() + tt_files.size() + st_files.size() +
                    sh_files.size() + bl_files.size() + dv_files.size() +
                    td_files.size();
  job.set_message("构建 AssetInfo (并行扫描月度分片)");
  {
    ScanThreadPool pool(shards);
    std::vector<std::future<void>> futures;
    // 重表先 submit: 池是 FIFO, 长任务先起才不会在尾部留长尾
    submit_table(pool, rb_files, bar_locals, futures, job.files_done, scan_bar);
    submit_table(pool, tt_files, ttm_locals, futures, job.files_done, scan_ttm);
    submit_table(pool, st_files, status_locals, futures, job.files_done,
                 scan_status);
    submit_table(pool, sh_files, shares_locals, futures, job.files_done,
                 scan_shares);
    submit_table(pool, bl_files, bal_locals, futures, job.files_done, scan_bal);
    submit_table(pool, dv_files, div_locals, futures, job.files_done, scan_div);
    futures.push_back(pool.submit(scan_calendar));
    futures.push_back(pool.submit(scan_names));
    futures.push_back(pool.submit(scan_industry));
    for (auto &f : futures)
      f.get(); // 异常不捕获 (fail fast)
  }
  job.files_total = 0;
  job.set_message("构建 AssetInfo (归约 / 估值 / 因子)");

  // ======================== 归约阶段 (单线程) ========================

  // ---- 交易日历: 全日历展开 (含周末/节假日 "0" 行) ----
  // Browser 日历的节假日标注依赖非交易日行
  assert(!trading.empty() && "all_trading_days 无 market_code='CN' 行");
  auto &stock_days = job.assetinfo.mutable_stock_days();
  stock_days.clear();
  for (const std::string &d :
       misc::iter_days(*trading.begin(), *trading.rbegin())) {
    std::string dashed =
        d.substr(0, 4) + "-" + d.substr(4, 2) + "-" + d.substr(6, 2);
    stock_days.push_back({std::move(dashed), trading.count(d) ? "1" : "0"});
  }
  job.st.trading_days_count = trading.size();

  // ---- 简称 / 行业 ----
  for (auto &[id, nm] : names)
    uni.info(id).name = std::move(nm);
  for (IndRow &r : inds) {
    StockInfo &info = uni.info(r.id);
    info.ind_code = std::move(r.code);
    info.ind_name = std::move(r.name);
  }

  // ---- 日频行情: 每股最新行 (分片间按 date 取最大者) ----
  // date 与 qdate 各自独立取最大 —— 最新行和最新有效报价行可能落在不同分片
  // (长停股尾部整月全是停牌行), 整体替换会把靠后分片的空报价盖上去.
  std::vector<BarLatest> bar(nid);
  for (const BarLocal &L : bar_locals)
    for (std::uint32_t id = 0; id < nid; ++id) {
      const BarLatest &b = L.latest[id];
      if (!b.present)
        continue;
      BarLatest &dst = bar[id];
      dst.present = true;
      if (b.date > dst.date)
        dst.date = b.date;
      if (b.qdate > dst.qdate) {
        dst.qdate = b.qdate;
        dst.close = b.close;
        dst.volume = b.volume;
        dst.amount = b.amount;
        dst.turn = b.turn;
      }
    }

  for (std::uint32_t id = 0; id < nid; ++id) {
    const BarLatest &b = bar[id];
    if (!b.present)
      continue;
    StockInfo &info = uni.info(id);
    char buf[32];
    info.update_date = dash_date(b.date);
    if (b.qdate == 0)
      continue; // 全区间无成交 (5 只早年退市股) → 价量留空, 只留数据水位
    std::snprintf(buf, sizeof(buf), "%.0f", static_cast<double>(b.volume));
    info.volume = buf;
    std::snprintf(buf, sizeof(buf), "%.4f", static_cast<double>(b.amount));
    info.amount = buf;
    std::snprintf(buf, sizeof(buf), "%.6f", static_cast<double>(b.turn));
    info.turn = buf;
  }

  // ---- 估值快照 (MCAP/PE/PB/PS/PCF/DY) ← 最新 close × total_shares / 各分母 ----
  //   口径与 L1 特征表一致 (qmt 移植): 分子统一为总市值 mcap = close × total_shares
  //   (不复权真价); PE=mcap/归母净利TTM, PB=mcap/归母权益MRQ, PS=mcap/营业总收入TTM
  //   (≤0脏值→空), PCF=mcap/经营现金流净额TTM; 亏损/负权益/烧钱保留负值.
  //   DY1/3/5 = 近 1/3/5 年税前分红总额年化后 / mcap; 静态快照的股本恒为最新
  //   快照, 故等价于 Σ每股分红/年数/close (L1 dy_raw 用公告日当时股本, 窗口内
  //   有增发时会有微差). 无效 → 留空串 (Table 显示 "-").
  //
  //   缺失语义 (与 L1 一致: 只标记, 不横截面填充 —— 展示层不放非该股真实值):
  //     · mcap 算不出 (无有效成交价 / 无股本) → 该股估值列整片留空, 不留半成品;
  //     · 各分母独立回退到最后一个有效期次, 所以停止披露只影响到期次而非有无;
  //     · 仍为空的只剩两类结构性缺口 —— 2015-01 前退市 (整体在 PIPELINE_START_DATE
  //       之外, 源里无任何行) 和上市未满一个报告期的新股 (尚未披露 TTM/权益).
  {
    // 股本: 分片间按 date 取最大者
    std::vector<SharesLatest> shares(nid);
    for (const SharesLocal &L : shares_locals)
      for (std::uint32_t id = 0; id < nid; ++id)
        if (L.latest[id].date > shares[id].date)
          shares[id] = L.latest[id];

    // TTM: 三个分母各自按自己的 date 取最大 (它们的最后有效期次可能落在不同分片)
    std::vector<TtmLatest> ttm(nid);
    for (const TtmLocal &L : ttm_locals)
      for (std::uint32_t id = 0; id < nid; ++id) {
        const TtmLatest &s = L.latest[id];
        TtmLatest &dst = ttm[id];
        if (s.np_d > dst.np_d) {
          dst.np_d = s.np_d;
          dst.np = s.np;
        }
        if (s.rev_d > dst.rev_d) {
          dst.rev_d = s.rev_d;
          dst.rev = s.rev;
        }
        if (s.cf_d > dst.cf_d) {
          dst.cf_d = s.cf_d;
          dst.cf = s.cf;
        }
      }

    // 权益 MRQ: 按 (report_date, date) 取最大者
    std::vector<BalLatest> bal(nid);
    for (const BalLocal &L : bal_locals)
      for (std::uint32_t id = 0; id < nid; ++id) {
        const BalLatest &b = L.latest[id];
        if (!b.present)
          continue;
        BalLatest &dst = bal[id];
        if (!dst.present || b.rd > dst.rd ||
            (b.rd == dst.rd && b.date > dst.date))
          dst = b;
      }

    // 近 1/3/5 年税前每股分红求和 (无分红 = 确知的 0, 非缺失). 分片按 ym 升序
    // 连续切分且这里按段序遍历 ⇒ 求和顺序与单线程逐文件累加逐位一致.
    std::vector<std::array<double, kDyWindows>> dps(nid);
    for (const DivLocal &L : div_locals)
      for (const DivRow &r : L.rows)
        for (int w = 0; w < kDyWindows; ++w)
          if (r.date > div_lo[w])
            dps[r.id][w] += static_cast<double>(r.cash);

    const auto today_days = misc::parse_yyyymmdd(today);
    for (std::uint32_t id = 0; id < nid; ++id) {
      // qdate != 0 已保证 close 是非 null 正数 (见 BarLatest); 市值算不出的股票
      // 整只跳过, 估值列全空 —— 不留"一半有一半没有"的半成品.
      if (bar[id].qdate == 0 || shares[id].total <= 0.0f)
        continue;
      const double close = static_cast<double>(bar[id].close);
      const double mcap = close * static_cast<double>(shares[id].total);
      StockInfo &info = uni.info(id);
      char buf[32];
      std::snprintf(buf, sizeof(buf), "%.4f", mcap / 1e8); // [亿元]
      info.mcap = buf;
      // 分母有效性已在扫描处定完 (非 null 且过了 !=0 / >0 的门), 这里只除.
      // 亏损/负权益/烧钱保留负值 —— 那是真实信息, 不是缺失.
      auto set_ratio = [&](std::string &dst, double den) {
        std::snprintf(buf, sizeof(buf), "%.4f", mcap / den);
        dst = buf;
      };
      if (ttm[id].np_d != 0)
        set_ratio(info.peTTM, static_cast<double>(ttm[id].np));
      if (ttm[id].rev_d != 0)
        set_ratio(info.psTTM, static_cast<double>(ttm[id].rev));
      if (ttm[id].cf_d != 0)
        set_ratio(info.pcfNcfTTM, static_cast<double>(ttm[id].cf));
      if (bal[id].present)
        set_ratio(info.pbMRQ, static_cast<double>(bal[id].eq));

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
                      dps[id][w] / years / close * 100.0); // [%]
        *dy[w] = buf;
      }
    }
  }

  // ---- 复权因子变点序列 ----
  auto &stock_factor = job.assetinfo.mutable_stock_factor();
  stock_factor.clear();
  const std::string factor_update = today.substr(0, 4) + "-" +
                                    today.substr(4, 2) + "-" +
                                    today.substr(6, 2);
  {
    // 分片按 ym 升序连续切分, 片内按文件升序, 文件内按 (id, date) 升序 ⇒ 桶式
    // 分发后每个 id 的序列已是 date 全局升序, 原先千万级元素的 std::sort 因此
    // 完全消失 (下面的 assert 兜住这个不变量).
    std::vector<std::size_t> counts(nid, 0);
    for (const BarLocal &L : bar_locals)
      for (const FactorPoint &p : L.points)
        ++counts[p.id];
    std::vector<std::vector<std::pair<std::int32_t, float>>> seqs(nid);
    for (std::uint32_t id = 0; id < nid; ++id)
      seqs[id].reserve(counts[id]);
    for (const BarLocal &L : bar_locals)
      for (const FactorPoint &p : L.points)
        seqs[p.id].emplace_back(p.date, p.f);

    for (std::uint32_t id = 0; id < nid; ++id) {
      const auto &seq = seqs[id];
      if (seq.empty())
        continue; // 该股无有限 adjust_factor
      assert(std::is_sorted(seq.begin(), seq.end(),
                            [](const std::pair<std::int32_t, float> &a,
                               const std::pair<std::int32_t, float> &b) {
                              return a.first < b.first;
                            }) &&
             "adjust_factor 序列非 date 升序 — 月分片的段序被破坏");
      StockFactorData sfd;
      sfd.last_update = factor_update;
      float prev = 0.0f;
      for (const auto &[d, f] : seq) {
        if (!sfd.data.empty() && std::abs(f / prev - 1.0f) < 1e-5f)
          continue; // 非变点
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%.6f", static_cast<double>(f));
        sfd.data.push_back({dash_date(d), buf});
        prev = f;
      }
      stock_factor.emplace(uni.code(id), std::move(sfd));
    }
  }

  // ---- 状态: 每股最新行 + 逐日停牌名单 ----
  {
    std::vector<StatusLatest> latest(nid);
    for (const StatusLocal &L : status_locals)
      for (std::uint32_t id = 0; id < nid; ++id) {
        const StatusLatest &s = L.latest[id];
        if (s.present && (!latest[id].present || s.date > latest[id].date))
          latest[id] = s;
      }
    for (std::uint32_t id = 0; id < nid; ++id) {
      if (!latest[id].present)
        continue;
      StockInfo &info = uni.info(id);
      // st_status 原值直传: 0=正常, 1=ST, 2=*ST (退市风险警示)
      info.isST = std::to_string(latest[id].st);
      info.tradestatus = latest[id].suspended != 0 ? "0" : "1";
    }

    // 28 万条 (date, id): 先按 date 归并排序, 每个日期只建一次 map 键、只对
    // 目标 set 做一次 reserve —— 逐条查 map + 无 reserve 的 rehash 是可见开销.
    std::size_t total_halts = 0;
    for (const StatusLocal &L : status_locals)
      total_halts += L.halts.size();
    std::vector<std::pair<std::int32_t, std::uint32_t>> halts;
    halts.reserve(total_halts);
    for (const StatusLocal &L : status_locals)
      halts.insert(halts.end(), L.halts.begin(), L.halts.end());
    std::sort(halts.begin(), halts.end());

    auto &suspended = job.assetinfo.mutable_suspended();
    suspended.clear();
    for (std::size_t i = 0; i < halts.size();) {
      std::size_t j = i;
      while (j < halts.size() && halts[j].first == halts[i].first)
        ++j;
      char dense[9];
      std::snprintf(dense, sizeof(dense), "%08d", halts[i].first);
      auto &names_at_date = suspended[std::string(dense, 8)];
      names_at_date.reserve(j - i);
      for (std::size_t k = i; k < j; ++k)
        names_at_date.insert(uni.code(halts[k].second));
      i = j;
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

const TableFileStat *FundamentalState::find_table(const std::string &name) const {
  for (const TableFileStat &t : tables)
    if (t.name == name)
      return &t;
  return nullptr;
}

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
    // 同步之后再扫盘, 数字反映本轮落盘结果 (构建失败也扫 — 正是要看清哪张表缺)
    job->st.tables = collect_table_stats();
    job->done = true;
  }).detach();

  boost::asio::steady_timer timer(io_);
  while (!job->done) {
    // 构建中实时把工作线程的阶段说明透给 UI
    std::string msg = job->get_message();
    if (with_network && state_.status == FundamentalStatus::Updating &&
        msg.starts_with("构建"))
      state_.status = FundamentalStatus::Building;
    // 并行扫描阶段: 分片进度由 worker 的原子计数器给出 (逐文件不再抢 message 锁)
    const std::size_t total = job->files_total.load(std::memory_order_relaxed);
    if (total != 0) {
      const std::size_t n =
          std::min(job->files_done.load(std::memory_order_relaxed), total);
      msg += " " + std::to_string(n) + "/" + std::to_string(total);
    }
    state_.message = std::move(msg);
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
    state_.tables = std::move(job->st.tables);
    state_.status = FundamentalStatus::Error;
    state_.message = "本地 parquet 缺失 — 点击 Update 联网同步";
  }
  busy_ = false;
}

} // namespace GUI::Database
