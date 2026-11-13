// 行情的当日一路 (见 Quote.hpp): 新浪全市场内存缓存当天, 个股 / 篮子的当日同出于此 (today(Side, 键集)).
//   个股当日 = 本只的根 (五档另取 book); 篮子当日 = 交接 (PriceLib 每次定下即 bind) 的成员表合成 (Fold, 只合到此刻所在根), 当日有变才合:
//     每轮快照之后合一次; 其间补根改了当日 → 距上次合成满 kFoldGap 即合, 不等下一轮快照.
//   增量合成: 冻结线 = min(定稿线 + 1, 各只待补区间起点) 之前的槽不会再变, 只合一次 (Folding 跨轮持有累加与各只接续);
//     每轮只合日根与冻结线之后的几槽. 冻结段被改写 (快照 / 补根落在冻结线前) 或冻结线回退 (新标待补) → 从头合一次.
//   合成不持 mu_ 整趟: 成员表是不可变快照 (Basis), 逐 kFoldChunk 只持 mu_ 读一段, 末了持 mu_ 换上 (其间换日 / 重 bind → 弃).
//   两张当日表各一个版本号 (QuoteStatus::ver); 当日一只一份不可变 Day, GUI 只拷指针:
//     个股 = Track 上按需建的快照 (该只的根 / 快照有变即作废, 下次取时重建), 篮子 = 末次合成的整份 (各篮子指进同一块).
//   状态计数 (待补 / 缺口只数) 随改动增量维护: 改 Track 只经 touch, 取状态 O(1).
//
// 当日 1 分钟 K, 两路取数 (见 api/sina/client.hpp):
//   快照 (hq.sinajs.cn 一次 800 只, 全市场 ~8 批, 一批一条连接并发): 盘中每 kTick 一轮 (交易所快照 3s 一档), 盘外每 kIdleTick.
//     逐只按累计量 / 额差分合成当日分钟 K, 槽 = 该快照时刻所在的根 (竞价并入首根, 午休并入 11:30, 15:01 后的盘后交易不计):
//     量 / 额 / 收精确 (快照边界上的量可能落到相邻根); 开 = 本根首个成交快照的现价; 高 / 低 = 采样现价, 日内新高 / 新低落在本根时恰为该值.
//   分钟 K (quotes.sina.cn 一只一请求 ~170ms, kFillers 路并发) 只补快照没盯住的根. 每只一个待补区间 [lo, hi] (多段取并包), 三个来源:
//     启动: 首快照累计量 > 0 (盘中 / 盘后启动) ⇒ [0, 首快照所在根], 其前的根是缺口 (Bars::hole), 所在根暂写现价 (量 0);
//     断档: 相邻两轮快照跨根且其间连续竞价时长 > kGap (网络卡 / 休眠 / 停机重启) ⇒ 其间有成交的只 [上轮所在根, 本轮所在根], 两端之间的根是缺口;
//       其间无成交的只累计量不变, 其间的根确知无成交; 同根内的停顿不算 (量 / 额 / 收照样精确, 只少几个现价采样);
//     选中: 定稿线以内首个非精确根起 ⇒ [该根, 定稿线] (选中标的已定稿部分恒精确, 每越过一根定稿线补一根).
//   缺口有无成交未知: 个股 / 篮子 (成员有缺口) 该根都不画, 不拿前值充数; 缺口 ⊂ 待补区间 ⇒ 冻结线前无缺口.
//   取: 新浪只给 "最近 n 根", n = 此刻所在根 − lo + 2 (根数上界, 恰好盖到 lo) —— 缺几根取几根, 全天缺即整日.
//     时机: 有缺口即取 (插在补精确的请求之前), 回包把缺口当场补上 (未定稿的暂写); 无缺口的区间整段定稿 (hi ≤ 定稿线) 即取;
//       定稿段已长过未定稿的尾巴 (lo + kLag ≤ 定稿线, 长断档) 先取定稿段, 尾巴等整段定稿再取 (每段至多两次);
//       选中标的定稿部分一有就取, 插队. 一只同时至多一个请求.
//   写回 [lo, 定稿线]: 有根写根, 无根清空, 全段标精确 (此后快照不写); lo 推到定稿线后.
//     同一回包里尾巴上取时已走完的根 (定稿线, min(hi, 取时所在根)) 先暂写 (不标精确, 仍待补), 不等定稿也有图; 再取时整段覆盖.
//   定稿线: 标号不晚于 "此刻 − kSettle" 的根不再变 (刚过去那根还会被修正); 行情日不是今天 (盘前看昨日 / 周末) → 整日定稿.
//   行情日 ≤ fund_end (基本面 bar1m 已入库) 不取分钟 K (那天由 bar1m 给).
//   落盘 output/data/quote/live.bin: 行情日全部 Track + 末轮快照时刻, 定长文件整份 MAP_SHARED 映射; 原子粒度 = 一只的状态 / 一格 (只, 分钟),
//     改一处当场写一处 (进程被杀 / 崩溃不丢; 写到一半的条目读回当作没有: 状态 → 该只从零, 格 → 该格成缺口待补).
//     换日 (含启动首轮) 先读同日缓存 (按新浪码对上本次全市场) 再重写成本次布局 —— 停机那段即一次断档, 照断档规则只补其间有成交的只;
//     收盘后重启零请求.
//   全市场 = 构造时给定的全局轴子集 (PoolAxis; 对外一律全局 id, 内部下标 = sub, 新浪码由轴码派生); 前一交易日取自构造时给定的交易日历;
//   线程: 快照 1 (每批一路并发) + 分钟 K kFillers, 随对象.
// 当日的 Day: 有成交才有 (个股 = 本只当日有成交, 篮子 = 成员有成交); 基准 = 新浪昨收; 个股的净流入 = 本只分钟涨跌法 (net_flow).
#pragma once

#include "api/sina/client.hpp"
#include "fundamental/AssetAxis.hpp" // PoolAxis
#include "gui/quote/Quote.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <bitset>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <deque>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace GUI::Quote {

struct Handoff;

struct QuoteStatus {
  misc::Ymd date = 0;         // 行情日
  misc::Ymd prev = 0;         // 行情日的前一交易日
  int sec = 0;                // 全市场最新快照时刻
  size_t pending = 0;         // 待取 / 在取的分钟 K
  size_t fetched = 0;         // 已取的分钟 K
  size_t gaps = 0;            // 有待补区间的只 (含等定稿的); 不取分钟 K 的日恒 0
  size_t holes = 0;           // 其中有缺口的只 (缺口不画, 补到即画); 不取分钟 K 的日恒 0
  bool fill = false;          // 行情日 > fund_end (取分钟 K)
  PerSide<uint64_t> ver = {}; // 当日表版本: 个股 (每轮快照 / 每次补根 +1) / 篮子 (每次合成 +1)
  size_t bytes = 0;           // 盘上缓存字节; 0 = 未建 (尚无行情日)
  std::time_t saved = 0;      // 盘上缓存的末次写入时刻
};

class QuoteService {
public:
  static constexpr int kTick = 3, kIdleTick = 30;      // 快照间隔 [s]: 盘中 / 盘外
  static constexpr int kSettle = 90;                   // 定稿延迟 [s]
  static constexpr int kLag = (kSettle + 59) / 60 + 1; // 此刻所在根 − 定稿线 的上界 [根]
  static constexpr int kGap = 10;                      // 断档判据: 两轮快照跨根时其间连续竞价时长 [s]
  static constexpr int kFillers = 16;                  // 分钟 K 并发 (再高新浪会 RST)
  static constexpr int kFoldGap = 1;                   // 补根后篮子合成的最小间隔 [s]
  static constexpr uint32_t kFoldChunk = 256;          // 篮子合成每段成员只数 (持 mu_ 的粒度)

  QuoteService(PoolAxis axis, misc::Ymd fund_end, std::vector<misc::Ymd> trading);
  ~QuoteService();
  QuoteService(const QuoteService &) = delete;
  QuoteService &operator=(const QuoteService &) = delete;

  const PoolAxis &axis() const { return axis_; }                  // 全市场 (构造后不变, 任意线程)
  std::span<const misc::Ymd> trading() const { return trading_; } // 交易日升序 (构造后不变, 任意线程)
  void select(uint32_t id);                                       // GUI: 选中标的 (全局 id; ~0u = 无)
  QuoteStatus status() const;
  // 交接 (成员表下标: 库轴 → 全市场) 换上, 当场合成一次 (PriceLib 定下交接即调)
  void bind(const Handoff &h);
  // GUI: 键集的当日 (只读), [k] ↔ keys[k]; 个股不在全市场 / 当日无成交、篮子未合成 / 成员无成交 → nullptr.
  //   篮子键集须是全表 (keys[k] = k): 给末次合成的那份, 不拷
  std::shared_ptr<const Days> today(Side s, std::span<const uint32_t> keys) const;
  // GUI: 个股键集的当日日根 (不带分钟根), [k] ↔ keys[k], 无 → date 0
  std::shared_ptr<const Candles> candles(std::span<const uint32_t> keys) const;
  // GUI: 个股 id 的最新快照 (五档); 不在全市场 / 未收到 → sym 空
  sina::Quote book(uint32_t id) const;

private:
  enum class Fill : uint8_t { Idle,
                              Queued,
                              Busy };
  // 一只的当日: 快照合成 + 分钟 K 补的根, 待补区间, 快照累计基线
  struct Track {
    Bars bars;                              // 当日的根 (含缺口)
    std::bitset<kSlots> exact;              // 来自分钟 K 的根 (含确认无成交的); 快照不写
    int lo = kSlots, hi = -1;               // 待补区间 (lo > hi = 无)
    double vol = -1, amt = 0;               // 上一快照累计 (vol < 0 = 尚无快照)
    float high = 0, low = 0;                // 上一快照日内高 / 低
    Fill fill = Fill::Idle;                 // 运行态, 不落盘
    mutable std::shared_ptr<const Day> day; // 当日快照 (today 按需建; 空 = 待建)

    bool gap() const { return lo <= hi; }
    void mark(int l, int h) { lo = std::min(lo, l), hi = std::max(hi, h); }
    void hole(int l, int h);                    // [l, h] 成缺口 (精确根不动) 并入待补; l > h = 无
    bool holed(int done) const;                 // 槽 ≤ done (已走完, 取得到) 里有缺口
    bool absorb(const sina::Quote &q, int gap); // 同日快照; gap = 断档起始根 (-1 = 无); 返回 = 有变化
    // 分钟 K 写回: [l, settled] 定稿, 其后待补区间内取时 (此刻所在根 live) 已走完的根暂写, 写到的缺口当场补上; 返回写到的末根
    int merge(int l, int settled, int live, misc::Ymd date, const std::vector<sina::Bar> &got);
  };

  // bind 的成员表 + 前日点位 (不可变, 合成期间整份持有)
  struct Basis {
    Members members;
    std::vector<double> level;
  };
  // 篮子当日的增量合成 (fold_mu_): 同一 (成员表, 行情日) 内跨轮持有; 槽 [0, frozen) 已定稿, 累加在 fold 里
  struct Folding {
    std::shared_ptr<const Basis> basis;
    misc::Ymd date = 0;
    std::unique_ptr<Fold> fold;
    std::vector<Fold::Carry> carry; // [成员表行] 冻结线处的接续
    int frozen = 0;
  };

  void poll();
  void fill();
  void fold(); // 篮子当日合成 → baskets_; 不持 mu_ 调
  // 以下须持 mu_
  bool foldable() const { return dirty_ && basis_ && date_ != 0; }
  void schedule();
  int settled() const;
  int live_slot() const;
  int done() const { return std::max(settled(), live_slot() - 1); } // 已走完的末根 (分钟 K 取得到)
  bool traded(const sina::Quote &q) const { return q.date == date_ && q.volume > 0 && q.price > 0; }
  // 改 tracks_[i] (f(Track &) → 根有变) 的唯一入口: 计数增量跟上, 根有变 → 作废其当日快照
  template <class F>
  void touch(uint32_t i, F &&f) {
    Track &t = tracks_[i];
    gaps_ -= t.gap(), holes_ -= t.bars.hole.any();
    if (f(t))
      t.day.reset();
    gaps_ += t.gap(), holes_ += t.bars.hole.any();
  }
  void recount();              // 整份重数 gaps_ / holes_ (换日 / load 后)
  void load();                 // 行情日的盘上缓存 → tracks_ / sec_, 再重写成本次布局并映射 (换日后调)
  void put(uint32_t i);        // tracks_[i] 的状态 → 映射
  void put(uint32_t i, int s); // tracks_[i] 的格 s → 映射

  const PoolAxis axis_;
  const misc::Ymd fund_end_;
  const std::vector<misc::Ymd> trading_;            // 交易日升序
  std::vector<std::string> syms_;                   // [sub] 新浪码
  std::unordered_map<std::string, uint32_t> index_; // 新浪码 → sub

  mutable std::mutex mu_;
  std::condition_variable cv_;   // 分钟 K 线程: 待取 / 停
  std::condition_variable wake_; // 快照线程: 当日有变待合成 / 停
  bool stop_ = false;
  misc::Ymd date_ = 0, prev_ = 0; // 行情日 / 其前一交易日
  uint64_t gen_ = 0;              // 换日 +1: 在取的分钟 K 作废
  std::vector<Track> tracks_;
  std::vector<sina::Quote> last_;
  std::deque<uint32_t> todo_;
  uint32_t sel_ = ~0u;
  size_t inflight_ = 0, fetched_ = 0;
  size_t gaps_ = 0, holes_ = 0; // 有待补区间 / 有缺口的只 (QuoteStatus::gaps / holes)
  int sec_ = 0;                 // 行情日最新快照时刻 (断档判据的上轮时刻)
  std::byte *map_ = nullptr;    // 盘上缓存的映射 (bytes_ 字节; 空 = 尚无行情日)
  size_t bytes_ = 0;            // QuoteStatus::bytes / saved
  std::time_t saved_ = 0;
  std::shared_ptr<const Basis> basis_;  // 空 = 未 bind
  std::shared_ptr<const Days> baskets_; // [篮子] 当日 (末次合成, 整份不可变; 空 = 未合成)
  bool dirty_ = false;                  // 上次合成后当日有变 (快照 / 补根 / 换日 / bind)
  int touched_ = kSlots;                // 上次合成后改写过根的最早槽
  PerSide<uint64_t> ver_ = {};
  std::mutex fold_mu_; // 合成一次一趟 (快照线程 / bind 的调用方)
  Folding folding_;    // fold_mu_

  std::thread poll_;
  std::vector<std::thread> fillers_;
};

} // namespace GUI::Quote