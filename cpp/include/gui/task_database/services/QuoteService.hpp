// Assets 行情的当日一路 (见 Kline.hpp): 新浪全市场内存缓存当天, 个股 / 篮子的当日同出于此 (today 两个重载).
//   个股当日 = 本只的根 + 最新快照; 篮子当日 = bind 的成员表每轮快照后在快照线程合成一次 (Fold, 只合到此刻所在根).
//   两张当日表各一个版本号 (QuoteStatus), GUI 见版本变了才拷.
//
// 当日 1 分钟 K, 两路取数 (见 api/sina/client.hpp):
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
//   全市场 = 构造时给定的代码表 (新浪形); 前一交易日取自构造时给定的交易日历; 线程: 快照 1 + 分钟 K kFillers, 随对象.
// 当日的 Day: 有成交才有 (个股 = 本只当日有成交, 篮子 = 成员有成交); 基准 = 新浪昨收.
#pragma once

#include "api/sina/client.hpp"
#include "gui/task_database/services/Kline.hpp"
#include "misc/date.hpp"

#include <bitset>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace GUI::Database {

struct QuoteStatus {
  misc::Ymd date = 0;                     // 行情日
  int sec = 0;                            // 全市场最新快照时刻
  size_t pending = 0;                     // 待取 / 在取的分钟 K
  size_t fetched = 0;                     // 已取的分钟 K
  bool fill = false;                      // 行情日 > fund_end (取分钟 K)
  uint64_t stock_ver = 0, basket_ver = 0; // 当日表版本: 个股 (每轮快照 / 每次补根 +1) / 篮子 (每次合成 +1)
};

class QuoteService {
public:
  static constexpr int kTick = 3, kIdleTick = 30; // 快照间隔 [s]: 盘中 / 盘外
  static constexpr int kSettle = 90;              // 定稿延迟 [s]
  static constexpr int kGap = 10;                 // 断档判据: 两轮快照间连续竞价时长 [s]
  static constexpr int kSave = 300;               // 落盘间隔 [s]
  static constexpr int kFillers = 32;             // 分钟 K 并发 (实测 32 路 ~175 次/s, 全市场 ~35s)

  QuoteService(std::vector<std::string> syms, misc::Ymd fund_end, std::vector<misc::Ymd> trading);
  ~QuoteService();
  QuoteService(const QuoteService &) = delete;
  QuoteService &operator=(const QuoteService &) = delete;

  size_t size() const { return syms_.size(); }  // 全市场只数
  uint32_t find(const std::string &code) const; // 全局轴码 "600000.SH" → 全市场下标; 不在 → ~0u (只读构造时的码表, 任意线程)
  void select(uint32_t i);                      // GUI: 选中标的 (~0u = 无)
  QuoteStatus status() const;
  // 篮子成员表 (下标 = 全市场) + 各篮子前日点位; 当场合成一次, 此后每轮快照一次 (BasketHistory 跑完调)
  void bind(Members m, std::vector<double> level);
  // GUI: 第 i 只的当日 + 最新快照 (未收到 → q.sym 空)
  void today(uint32_t i, Day &out, sina::Quote &q) const;
  // GUI: 各篮子当日 (末次合成); out.size() = bind 的篮子数
  void today(std::span<Day> out) const;

private:
  enum class Fill : uint8_t { Idle,
                              Queued,
                              Busy };
  struct Track {
    Bars bars;                 // 当日的根
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
  void fold(); // 篮子当日合成 → baskets_
  int settled() const;
  int live_slot() const;
  bool traded(const sina::Quote &q) const { return q.date == date_ && q.volume > 0 && q.price > 0; }
  void load();                   // 行情日的盘上缓存 → tracks_ / sec_ (换日后调)
  std::string serialize() const; // 当日全部 Track → 缓存文件内容
  static void mark(Track &t, int lo, int hi) { t.lo = std::min(t.lo, lo), t.hi = std::max(t.hi, hi); }

  const std::vector<std::string> syms_;
  const misc::Ymd fund_end_;
  const std::vector<misc::Ymd> trading_; // 交易日升序
  std::unordered_map<std::string, uint32_t> index_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false;
  misc::Ymd date_ = 0, prev_ = 0; // 行情日 / 其前一交易日
  uint64_t gen_ = 0;              // 换日 +1: 在取的分钟 K 作废
  std::vector<Track> tracks_;
  std::vector<sina::Quote> last_;
  std::deque<uint32_t> todo_;
  uint32_t sel_ = ~0u;
  size_t inflight_ = 0, fetched_ = 0;
  bool changed_ = false;      // 上次落盘后 tracks_ 变过
  int sec_ = 0;               // 行情日最新快照时刻 (断档判据的上轮时刻)
  Members members_;           // bind 的成员表
  std::vector<double> level_; // [篮子] 前日点位; 空 = 未 bind
  Fold fold_{0};
  std::vector<Day> baskets_; // [篮子] 当日
  uint64_t stock_ver_ = 0, basket_ver_ = 0;

  std::thread poll_;
  std::vector<std::thread> fillers_;
};

} // namespace GUI::Database
