// Assets → Quotes 右栏的行情数据: 当日 = QuoteService (新浪, 全市场内存缓存当天), 历史 = HistoryService (基本面库, 选中标的).
//
// 分钟槽: 一交易日 kSlots = 240 根, 槽 s 的标号 (bar 结束分钟) = 09:31 … 11:30, 13:01 … 15:00 (新浪与 bar1m 同一标号);
//   开盘集合竞价并入首根 (bar1m 单列的 09:25 撮合 bar 在读侧并入, 与新浪口径一致).
//
// QuoteService — 当日 1 分钟 K, 两路取数 (见 api/sina/client.hpp):
//   快照 (hq.sinajs.cn 一次 800 只, 全市场 ~8 次串行 ~1.6s): 盘中每 kTick 一轮 (交易所快照 3s 一档), 盘外每 kIdleTick.
//     逐只按累计量 / 额差分合成当日分钟 K, 槽 = 该快照时刻所在的根 (竞价并入首根, 午休并入 11:30, 15:01 后的盘后交易不计):
//     量 / 额 / 收精确 (快照边界上的量可能落到相邻根); 开 = 本根首个成交快照的现价; 高 / 低 = 采样现价, 日内新高 / 新低落在本根时恰为该值.
//   分钟 K (quotes.sina.cn 一只一请求 ~170ms, kFillers 路并发) 只补快照没盯住的根. 每只一个待补区间 [lo, hi] (多段取并包), 三个来源:
//     启动: 首快照累计量 > 0 (盘中 / 盘后启动) ⇒ [0, 首快照所在根];
//     断档: 相邻两轮快照之间的连续竞价时长 > kGap (网络卡 / 休眠) ⇒ 其间有成交的只 [上轮所在根, 本轮所在根];
//     选中: 定稿线以内首个非精确根起 ⇒ [该根, 定稿线] (选中标的已定稿部分恒精确, 每越过一根定稿线补一根).
//   取: 新浪只给 "最近 n 根", n = 此刻所在根 − lo + 2 (根数上界, 恰好盖到 lo) —— 缺几根取几根, 全天缺即整日.
//     时机: 区间整段定稿 (hi ≤ 定稿线) 才取, 一次补齐; 选中标的定稿部分一有就取, 插队. 一只同时至多一个请求.
//   写回 [lo, 定稿线]: 有根写根, 无根清空, 全段标精确 (此后快照不写); lo 推到定稿线后.
//   定稿线: 标号不晚于 "此刻 − kSettle" 的根不再变 (刚过去那根还会被修正); 行情日不是今天 (盘前看昨日 / 周末) → 整日定稿.
//   行情日 ≤ fund_end (基本面 bar1m 已入库) 不取分钟 K (那天由 bar1m 给).
//   落盘 config::SINA_CACHE: 单文件存行情日全部 Track (根 / 精确位 / 待补区间 / 累计基线) + 末轮快照时刻, 有变化时每 kSave 与析构各写一次.
//     换日 (含启动首轮) 先读同日缓存 —— 停机那段即一次断档, 照断档规则只补其间有成交的只; 收盘后重启零请求.
//   全市场 = 构造时给定的代码表 (新浪形); 线程: 快照 1 + 分钟 K kFillers, 随对象.
//   篮子 (指数 / 行业, 见 IndexService.hpp) 的当日由 compose 按成员 / 权重从快照与分钟根现合成.
//
// HistoryService — 选中标的的基本面历史 (gui/util/Service.hpp, 最新请求覆盖旧的):
//   日线 = cn_stock_real_bar1d 最近 kDailyMonths 个月分片; 分钟 = bar1m 最近 kMinuteDays 个交易日文件. 不复权, 无成交的行 / 根不收.
#pragma once

#include "api/sina/client.hpp"
#include "gui/util/Service.hpp"
#include "misc/date.hpp"
#include "shared/Progress.hpp"

#include <algorithm>
#include <bitset>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace GUI::Database {

inline constexpr int kSlots = 240;

// 槽 → 标号 (hh · 60 + mm)
constexpr int slot_label(int s) { return s < 120 ? 9 * 60 + 31 + s : 13 * 60 + 1 + (s - 120); }
// 标号 → 槽; 不是交易分钟 → -1
constexpr int label_slot(int m) {
  return m >= 9 * 60 + 31 && m <= 11 * 60 + 30 ? m - (9 * 60 + 31) : m >= 13 * 60 + 1 && m <= 15 * 60 ? 120 + m - (13 * 60 + 1)
                                                                                                      : -1;
}

// 一根: 1 分钟 (slot = 槽) 或 1 日 (date)
struct Candle {
  misc::Ymd date = 0;
  int slot = 0;
  float open = 0, high = 0, low = 0, close = 0;
  double volume = 0, amount = 0; // [股] [元]
};

// 一只的当日 (GUI 拷出)
struct LiveDay {
  misc::Ymd date = 0;       // 行情日; 0 = 尚无快照
  bool quoted = false;      // 本只收到过快照 (quote 有效)
  sina::Quote quote;        // 最新快照 (含五档)
  std::vector<Candle> bars; // 当日已有的根, 槽升序
};

// 一个篮子的当日 (QuoteService::compose): 点位 = 基点 × 成员 价 / 昨收 的加权均值, 额 = 成员之和 (量不合成)
struct IndexDay {
  misc::Ymd date = 0;       // 行情日; 0 = 尚无快照
  int sec = 0;              // 全市场最新快照时刻
  Candle day;               // 当日一根 (成员快照的 开 / 高 / 低 / 现价 各自合成)
  std::vector<Candle> bars; // 当日分钟根, 槽升序至成员最晚有成交的根 (成员缺根沿用其上根收盘)
};

struct QuoteStatus {
  misc::Ymd date = 0; // 行情日
  int sec = 0;        // 全市场最新快照时刻
  size_t pending = 0; // 待取 / 在取的分钟 K
  size_t fetched = 0; // 已取的分钟 K
  bool fill = false;  // 行情日 > fund_end (取分钟 K)
};

class QuoteService {
public:
  static constexpr int kTick = 3, kIdleTick = 30; // 快照间隔 [s]: 盘中 / 盘外
  static constexpr int kSettle = 90;              // 定稿延迟 [s]
  static constexpr int kGap = 10;                 // 断档判据: 两轮快照间连续竞价时长 [s]
  static constexpr int kSave = 300;               // 落盘间隔 [s]
  static constexpr int kFillers = 32;             // 分钟 K 并发 (实测 32 路 ~175 次/s, 全市场 ~35s)

  QuoteService(std::vector<std::string> syms, misc::Ymd fund_end);
  ~QuoteService();
  QuoteService(const QuoteService &) = delete;
  QuoteService &operator=(const QuoteService &) = delete;

  struct Weight { // 篮子成员: 全市场下标 / 权重
    uint32_t i;
    float w;
  };

  void select(const std::string &sym);                  // GUI: 选中标的 (空 = 无)
  bool day(const std::string &sym, LiveDay &out) const; // GUI: 拷出 sym 当日; 不在全市场 → false
  QuoteStatus status() const;
  uint32_t find(const std::string &sym) const; // 全市场下标; 不在 → ~0u (只读构造时的码表, 任意线程)
  // 篮子当日合成, base = 前日点位. 无昨收的成员不计, 当日无成交的按昨收 (比 1) 计; bars = 连分钟根一起合成
  void compose(const std::vector<Weight> &ws, double base, bool bars, IndexDay &out) const;

private:
  struct Cell {
    float open, high, low, close;
    double volume, amount;
  };
  enum class Fill : uint8_t { Idle,
                              Queued,
                              Busy };
  struct Track {
    std::vector<Cell> bars;    // kSlots 格, 首次写入才分配
    std::bitset<kSlots> has;   // 有成交的根
    std::bitset<kSlots> exact; // 来自分钟 K 的根 (含确认无成交的); 快照不写
    int lo = kSlots, hi = -1;  // 待补区间 (lo > hi = 无)
    double vol = -1, amt = 0;  // 上一快照累计 (vol < 0 = 尚无快照)
    float high = 0, low = 0;   // 上一快照日内高 / 低
    Fill fill = Fill::Idle;
  };

  void poll();
  void fill();
  // 以下须持 mu_
  void absorb(uint32_t i, const sina::Quote &q, int gap); // gap = 断档起始根 (-1 = 无)
  void schedule();
  void merge(uint32_t i, int lo, const std::vector<sina::Bar> &bars, int settled);
  int settled() const;
  int live_slot() const;
  void load();                   // 行情日的盘上缓存 → tracks_ / sec_ (换日后调)
  std::string serialize() const; // 当日全部 Track → 缓存文件内容
  Cell &cell(Track &t, int s);
  static void mark(Track &t, int lo, int hi) { t.lo = std::min(t.lo, lo), t.hi = std::max(t.hi, hi); }

  const std::vector<std::string> syms_;
  const misc::Ymd fund_end_;
  std::unordered_map<std::string, uint32_t> index_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false;
  misc::Ymd date_ = 0;
  uint64_t gen_ = 0; // 换日 +1: 在取的分钟 K 作废
  std::vector<Track> tracks_;
  std::vector<sina::Quote> last_;
  std::vector<uint8_t> quoted_;
  std::deque<uint32_t> todo_;
  uint32_t sel_ = ~0u;
  size_t inflight_ = 0, fetched_ = 0;
  bool changed_ = false; // 上次落盘后 tracks_ 变过
  int sec_ = 0;          // 行情日最新快照时刻 (断档判据的上轮时刻)

  std::thread poll_;
  std::vector<std::thread> fillers_;
};

// ---------------------------------------------------------------------------
// HistoryService
// ---------------------------------------------------------------------------
struct HistoryRequest {
  uint32_t gen = 0;
  std::string code; // "600000.SH"
};

struct History : Published {
  uint32_t gen = 0;                         // 已发布的请求代
  std::vector<Candle> daily;                // 日升序
  std::vector<std::vector<Candle>> minutes; // 交易日升序, 每日槽升序 (date 同日)
};

class HistoryService : public Service<HistoryService, HistoryRequest> {
  friend Service<HistoryService, HistoryRequest>;

public:
  static constexpr const char *kName = "QuoteHistory";
  static constexpr int kDailyMonths = 13, kMinuteDays = 4;

  HistoryService() { Start(); }
  ~HistoryService() { Stop(); }

  const History &history() const { return h_; }

private:
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return h_; }
  bool run(HistoryRequest &req);

  History h_;
};

} // namespace GUI::Database
