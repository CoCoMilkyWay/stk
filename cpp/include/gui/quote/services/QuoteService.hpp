// 行情的当日一路 (见 Quote.hpp): 新浪全市场内存缓存当天, 个股 / 篮子的当日同出于此, 当日有变才发布一份 Live (live()):
//     发布线程独占发布: 当日有变 (快照 / 补根 / 订阅码) 即发, 两次之间至少 kFoldGap; 快照 / 补根线程只改状态、唤醒它, 从不等合成.
//   个股当日 = 本只的根 (+ 末快照的五档); 篮子当日 = 交接 (PriceLib 每次定下即 bind) 的成员表合成 (Fold, 只合到此刻所在根).
//   增量合成 (逐成员冻结线, Fold.hpp): 一只的冻结线 = min(定稿线 + 1, 本只待补区间起点), 其前的槽不会再变, 只合一次
//     (Folding 跨轮持有冻结累加与各只接续); 每轮各只只合日根与本只冻结线之后的几槽, 别只待补不拖累它.
//     有一只的冻结段被改写 (断档的缺口 / 选中补精确落在其冻结线前) 或冻结线回退 → 从头合一次.
//   发布只在重建个股 Day 时持 mu_ (有变的各只并行重建); 篮子不持 mu_: 源 = 本份的个股 Day + 成员表 (Basis), 都是不可变快照,
//     按篮子切段并行合成 (Fold); 末了持 pub_mu_ 换上. 当日一只一份不可变 Day, GUI 只拷指针:
//     个股 = Track 上的快照 (该只的根有变即作废, 下次发布重建), 篮子 = 每次合成的整份 (各篮子指进同一块).
//   状态 (QuoteStatus) 随改动持 mu_ 写一份副本 (pub_mu_), GUI 取状态 / 发布都不碰 mu_; 待补 / 缺口只数增量维护: 改 Track 只经 touch.
//
// 当日 1 分钟 K, 两路取数 (见 api/sina/client.hpp):
//   快照 (hq.sinajs.cn 一次 800 只, 全市场 ~8 批): 一批一线程一条连接, 各自盘中每 kTick 一轮 (交易所快照 3s 一档), 盘外每 kIdleTick;
//   取数一律一拍一试 (sina::Client 不重试): 取不到就是这一拍没有, 下一拍再来; 缺下的一律落进待补区间, 有机会即补 —— 没有另设的重试.
//     一批取不到只误本批, 别批照常; 断档按批判 (本批上次取到的时刻 bsec_), 停住的批回来即按断档补.
//     逐只按累计量 / 额差分合成当日分钟 K, 槽 = 该快照时刻所在的根 (竞价并入首根, 午休并入 11:30, 15:01 后的盘后交易不计):
//     量 / 额 / 收精确 (快照边界上的量可能落到相邻根); 开 = 本根首个成交快照的现价; 高 / 低 = 采样现价, 日内新高 / 新低落在本根时恰为该值.
//   分钟 K (quotes.sina.cn 一只一请求, 服务端 ~0.5–0.7s 与根数无关, kFillers 路并发; 实测每 IP 约 40 只/秒封顶) 只补快照没盯住的根. 每只一个待补区间 [lo, hi] (多段取并包), 三个来源:
//     启动: 首快照累计量 > 0 (盘中 / 盘后启动) ⇒ [0, 首快照所在根], 其前的根是缺口 (Bars::hole), 所在根暂写现价 (量 0);
//     断档: 同批相邻两次取到的快照跨根且其间连续竞价时长 > kGap (网络卡 / 休眠 / 停机重启) ⇒ 其间有成交的只 [上次所在根, 本次所在根], 两端之间的根是缺口;
//       其间无成交的只累计量不变, 其间的根确知无成交; 同根内的停顿不算 (量 / 额 / 收照样精确, 只少几个现价采样);
//     选中: 定稿线以内首个非精确根起 ⇒ [该根, 定稿线] (选中标的已定稿部分恒精确, 每越过一根定稿线补一根).
//   缺口有无成交未知, 发布时按最可能的路径估值 (Bars::guess; 个股 guess_holes: 段中缺 → 线性, 段尾缺 → 持平; 篮子按成员估价合成, 有成员是估值该根即估值),
//     只供画价格路径 (淡色), 补到即换成真根; 资金流 / K 线照当缺口. 缺口 ⊂ 待补区间 ⇒ 冻结线前无缺口 (不取分钟 K 的日, 有缺口的只冻结线也止于待补区间起点).
//   有缺口的只按打散序 (order_) 入队: 早补到的散在各篮子, 篮子线逐步均匀变真.
//   取: 新浪只给 "最近 n 根", n = 此刻所在根 − lo + 2 (根数上界, 恰好盖到 lo) —— 缺几根取几根, 全天缺即整日.
//     时机: 有缺口即取 (插在补精确的请求之前), 回包把缺口当场补上 (未定稿的暂写); 无缺口的区间整段定稿 (hi ≤ 定稿线) 即取;
//       定稿段已长过未定稿的尾巴 (lo + kLag ≤ 定稿线, 长断档) 先取定稿段, 尾巴等整段定稿再取 (每段至多两次);
//       选中标的定稿部分一有就取, 插队. 一只同时至多一个请求; 没取到 → 回空闲, 区间原样, 下次排队 (每批快照之后) 再入队.
//   写回 [lo, 定稿线]: 有根写根, 无根清空, 全段标精确 (此后快照不写); lo 推到定稿线后.
//     同一回包里尾巴上取时已走完的根 (定稿线, min(hi, 取时所在根)) 先暂写 (不标精确, 仍待补), 不等定稿也有图; 再取时整段覆盖.
//   定稿线: 标号不晚于 "此刻 − kSettle" 的根不再变 (刚过去那根还会被修正); 行情日不是今天 (盘前看昨日 / 周末) → 整日定稿.
//   行情日 ≤ fund_end (基本面段末日 AssetInfo::quote_end; 随基本面同步推进) 不取分钟 K (那天由基本面给).
//   落盘 output/data/quote/live.bin: 行情日全部 Track + 末轮快照时刻 (各批取到过的时刻之最早: 重启按它判断档), 定长文件整份 MAP_SHARED 映射; 原子粒度 = 一只的状态 / 一格 (只, 分钟),
//     改一处当场写一处 (进程被杀 / 崩溃不丢; 写到一半的条目读回当作没有: 状态 → 该只从零, 格 → 该格成缺口待补).
//     条目按全局 id 排 (头记只数 + 资产轴前缀指纹); 换日 (含启动首轮) 先读同日缓存 (指纹对得上才认, 全市场外的不收) 再重写成本次布局 ——
//     停机那段即一次断档, 照断档规则只补其间有成交的只; 收盘后重启零请求.
//   往日归档 output/data/quote/YYYYMMDD.bin: 换日时缓存是更早的行情日且收完了 (收盘后的快照 ∧ 无待补区间 / 缺口 / 写到一半的条目) → 原样转作归档,
//     否则弃; 不可变. PriceLib 读作补段的新浪段 (archived_day), 基本面段盖住即删 (drop_archived).
//   全市场 = 构造时给定的全局 id (在市股); 各只一律按全局 id 下标 (资产轴前 N 条, 全市场外的空着), 新浪码 = sina::sym_of(轴码); 前一交易日取自交易日历;
//   订阅码 (watch; 实盘的持仓 / 名单, 含全市场外的 ETF / LOF): 只要快照, 订阅线程单取订阅码一批 (同快照节拍), 末快照进 Live::watch;
//     换了即提前取、当场发布 (Board 等的就是它); 各码取较新的那份 (see). 其中全市场内的只补分钟 K 插队 (仅次于选中).
//   末快照每批只换变了的只 (Quotes 逐只共享), 发布只拷指针.
//   状态带各批末次取到的最早时刻 (QuoteStatus::oldest): 界面据此恒显快照是否停滞.
//   线程: 快照 (每批一个) + 订阅 1 + 发布 1 + 分钟 K kFillers, 随对象; 与界面是否在画无关.
// 当日的 Day: 有成交才有 (个股 = 本只当日有成交, 篮子 = 成员有成交); 基准 = 新浪昨收; 个股的净流入 = 本只分钟涨跌法 (net_flow).
//   开盘集合竞价撮合段 (9:15–9:25, 尚无成交) 另有竞价的 Day (Day::call): 日根 / 首根 = 虚拟匹配价 (新浪此段现价恒 0, 买一 = 卖一 = 匹配价), 无额;
//     只进发布, 不进 Track / 盘上缓存 / 分钟 K; 篮子照合 (成员的竞价根照计). 9:25 撮合出首笔成交即由真实的根取代.
#pragma once

#include "api/sina/client.hpp"
#include "gui/quote/Fold.hpp"
#include "gui/quote/Quote.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <bitset>
#include <chrono>
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
  misc::Ymd date = 0;                             // 行情日
  misc::Ymd prev = 0;                             // 行情日的前一交易日
  int sec = 0;                                    // 全市场最新快照时刻
  size_t pending = 0;                             // 待取 / 在取的分钟 K
  size_t fetched = 0;                             // 已取的分钟 K
  size_t gaps = 0;                                // 有待补区间的只 (含等定稿的); 不取分钟 K 的日恒 0
  size_t holes = 0;                               // 其中有缺口的只 (缺口不画, 补到即画); 不取分钟 K 的日恒 0
  bool fill = false;                              // 行情日 > fund_end (取分钟 K)
  size_t bytes = 0;                               // 盘上缓存字节; 0 = 未建 (尚无行情日)
  std::time_t saved = 0;                          // 盘上缓存的末次写入时刻
  double net_ms = 0;                              // 分钟 K 一请求的网络用时 [ms] (近期指数均值; 0 = 尚无)
  double lock_ms = 0;                             // 其回包后等 mu_ 的用时 [ms] (同上; 高 = 被快照 / 发布占锁拖慢)
  std::chrono::steady_clock::time_point oldest{}; // 各批快照末次取到之最早 (未取到过的批 = 服务起时): 距今即快照停了多久
};

// 一只当日落盘的状态 (live.bin 一条原子条目, 内存里即 Track 的头): 待补区间 + 上一快照累计基线; busy = 写到一半 (内存里恒 0)
struct TrackRec {
  int32_t lo = kSlots, hi = -1; // 待补区间 (lo > hi = 无)
  double vol = -1, amt = 0;     // 上一快照累计 (vol < 0 = 尚无快照)
  float high = 0, low = 0;      // 上一快照日内高 / 低
  float open = 0, close = 0;    // 上一快照今开 / 现价
  float ref = 0;                // 昨收
  uint8_t busy = 0;
};

class QuoteService {
public:
  static constexpr int kTick = 3, kIdleTick = 30;      // 快照间隔 [s]: 盘中 / 盘外
  static constexpr int kStall = 15;                    // 盘中快照距今超此 [s] 即算停滞 (界面告警)
  static constexpr int kSettle = 90;                   // 定稿延迟 [s]
  static constexpr int kLag = (kSettle + 59) / 60 + 1; // 此刻所在根 − 定稿线 的上界 [根]
  static constexpr int kGap = 10;                      // 断档判据: 两轮快照跨根时其间连续竞价时长 [s]
  static constexpr int kFillers = 16;                  // 分钟 K 并发 (再高新浪会 RST)
  static constexpr double kEma = 0.05;                 // 分钟 K 用时 (QuoteStatus::net_ms / lock_ms) 的指数均值权
  static constexpr int kFoldGap = 1;                   // 补根后发布的最小间隔 [s]
  static constexpr int kFoldThreads = 8;               // 发布的并行 (重建个股 Day / 篮子按段合成)
  static constexpr int kCallOpen = 9 * 3600 + 15 * 60; // 开盘集合竞价撮合段 [kCallOpen, kCallEnd) [当日秒]
  static constexpr int kCallEnd = 9 * 3600 + 25 * 60;

  // ids = 全市场 (全局 id)
  QuoteService(const std::vector<uint32_t> &ids, misc::Ymd fund_end, std::shared_ptr<const Calendar> cal);
  ~QuoteService();
  QuoteService(const QuoteService &) = delete;
  QuoteService &operator=(const QuoteService &) = delete;

  bool listed(uint32_t id) const { return id < syms_.size() && !syms_[id].empty(); } // 在全市场 (构造后不变, 任意线程)
  void select(uint32_t id);                                                          // GUI: 选中标的 (全局 id; ~0u = 无)
  void set_fund_end(misc::Ymd e);                                                    // GUI: 基本面段末日推进
  void watch(std::vector<std::string> syms);                                         // 任意线程: 订阅码 (新浪码) 整份换上
  QuoteStatus status() const;
  std::shared_ptr<const Live> live() const; // 末份当日; 空 = 尚未发布
  // 交接换上, 当场发布一次 (PriceLib 定下交接即调)
  void bind(const Handoff &h);
  static std::chrono::seconds tick(); // 此刻的快照节拍 (盘中 kTick / 盘外 kIdleTick)

private:
  enum class Fill : uint8_t { Idle,
                              Queued,
                              Busy };
  // 一只的当日: 落盘的状态 (TrackRec: 待补区间 + 快照累计基线) + 快照合成 + 分钟 K 补的根
  struct Track : TrackRec {
    Bars bars;                      // 当日的根 (含缺口)
    std::bitset<kSlots> exact;      // 来自分钟 K 的根 (含确认无成交的); 快照不写
    Fill fill = Fill::Idle;         // 以下运行态, 不落盘
    int touched = kSlots;           // 上次发布后改写过根的最早槽
    std::shared_ptr<const Day> day; // 当日快照 (发布时建; 空 = 待建; 竞价段 = 竞价的 Day, 匹配价变即重建)

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
  // 篮子当日的增量合成 (fold_mu_): 同一 (成员表, 行情日) 内跨轮持有; 各只冻结段的累加在 fold 里
  struct Folding {
    std::shared_ptr<const Basis> basis;
    misc::Ymd date = 0;
    std::unique_ptr<Fold> fold;
    std::vector<Fold::Carry> carry; // [成员表行] 冻结段
  };

  void snap(size_t k); // 快照线程: 第 k 批
  void fill();
  void rewatch();   // 订阅线程: 每节拍取订阅码一批; 换了即提前取、当场发布
  void publisher(); // 发布线程: 当日有变即发布, 两次之间至少 kFoldGap
  void publish();   // 当日有变 → 一份 Live; 不持 mu_ 调
  // 篮子当日合成到 y.days[kBasket] (源 = 个股的当日 stocks; 槽 [0, slots)); [全局 id] 本轮冻结线 freeze / 上次发布后改写过的最早槽 touched
  void fold(Live &y, const Days &stocks, const std::shared_ptr<const Basis> &b, int slots, std::span<const int> freeze, std::span<const int> touched);
  // 以下须持 mu_
  bool stale() const { return dirty_ && date_ != 0; }
  void post(); // 状态副本 → status_
  void schedule();
  int settled() const;
  int live_slot() const;
  int done() const { return std::max(settled(), live_slot() - 1); } // 已走完的末根 (分钟 K 取得到)
  bool traded(const sina::Quote *q) const { return q && q->date == date_ && q->volume > 0 && q->price > 0; }
  // 竞价段的虚拟匹配价 (买一 = 卖一); 不在竞价段 / 有成交 / 未撮合 → 0
  float call_px(const sina::Quote *q) const {
    return q && q->date == date_ && q->volume == 0 && q->sec >= kCallOpen && q->sec < kCallEnd && q->bid[0] > 0 && q->bid[0] == q->ask[0] ? q->bid[0] : 0.f;
  }
  // 改 tracks_[i] (f(Track &) → 根有变) 的唯一入口: 计数增量跟上, 根有变 → 作废其当日快照
  template <class F>
  void touch(uint32_t i, F &&f) {
    Track &t = tracks_[i];
    gaps_ -= t.gap(), holes_ -= t.bars.hole.any();
    if (f(t))
      t.day.reset();
    gaps_ += t.gap(), holes_ += t.bars.hole.any();
  }
  void take(size_t k, const std::vector<sina::Quote> &qs); // 第 k 批取到的快照并入 (换日 / 断档 / 合成分钟 K / 落盘 / 排补取)
  void see(std::vector<sina::Quote> fresh);                // 订阅码的末快照换上: 当前 watch_ 各码取 fresh 与旧的里较新的; 有变 → dirty_
  void recount();                                          // 整份重数 gaps_ / holes_ (换日 / load 后)
  void load();                                             // 行情日的盘上缓存 → tracks_ / sec_, 再重写成本次布局并映射 (换日后调)
  void put(uint32_t i);                                    // tracks_[i] 的状态 → 映射
  void put(uint32_t i, int s);                             // tracks_[i] 的格 s → 映射

  const std::shared_ptr<const Calendar> cal_;
  std::vector<std::string> syms_;                   // [全局 id] 新浪码 (全市场外空)
  std::unordered_map<std::string, uint32_t> index_; // 新浪码 → 全局 id
  std::vector<uint32_t> order_;                     // 入队序: 全市场打散 (构造后不变)
  std::vector<std::vector<std::string>> chunks_;    // [批] 新浪码 (构造后不变)

  mutable std::mutex mu_;
  std::condition_variable cv_;         // 分钟 K 线程: 待取 / 停
  std::condition_variable wake_;       // 发布线程: 当日有变待发布 / 停
  std::condition_variable rewatch_cv_; // 订阅线程: 订阅码换了 / 停
  std::condition_variable rest_;       // 快照 / 发布线程的节拍间歇: 停
  bool stop_ = false;
  misc::Ymd fund_end_;
  misc::Ymd date_ = 0, prev_ = 0;                        // 行情日 / 其前一交易日
  uint64_t gen_ = 0;                                     // 换日 +1: 在取的分钟 K 作废
  std::vector<Track> tracks_;                            // [全局 id]
  std::shared_ptr<const Quotes> last_;                   // 末快照 (每轮换变了的只)
  std::vector<std::string> watch_;                       // 订阅码 (升序)
  std::vector<uint32_t> watch_ids_;                      // 其中全市场内的 (全局 id): 补分钟 K 插队
  bool rewatch_ = false;                                 // 订阅码换了, 订阅线程未取
  std::shared_ptr<const std::vector<sina::Quote>> seen_; // 订阅码的末快照 (Live::watch)
  std::deque<uint32_t> todo_;
  uint32_t sel_ = ~0u;
  size_t inflight_ = 0, fetched_ = 0;
  double net_ms_ = 0, lock_ms_ = 0;                          // QuoteStatus::net_ms / lock_ms
  size_t gaps_ = 0, holes_ = 0;                              // 有待补区间 / 有缺口的只 (QuoteStatus::gaps / holes)
  int sec_ = 0;                                              // 行情日最新快照时刻 (各批之极)
  std::vector<int> bsec_;                                    // [批] 本批上次取到的快照时刻 (断档判据; 0 = 本行情日未取到, 换日按盘上缓存的末轮时刻起)
  std::vector<std::chrono::steady_clock::time_point> bseen_; // [批] 本批末次取到的本机时刻 (QuoteStatus::oldest)
  std::byte *map_ = nullptr;                                 // 盘上缓存的映射 (bytes_ 字节; 空 = 尚无行情日)
  size_t bytes_ = 0;                                         // QuoteStatus::bytes / saved
  std::time_t saved_ = 0;
  std::shared_ptr<const Basis> basis_;                       // 空 = 未 bind
  bool dirty_ = false;                                       // 上次发布后当日有变 (快照 / 补根 / 换日 / bind)
  std::mutex fold_mu_;                                       // 发布一次一趟 (发布线程 / 订阅线程 / bind 的调用方)
  Folding folding_;                                          // fold_mu_
  misc::exec::ForkJoin pool_{misc::exec::fit(kFoldThreads)}; // fold_mu_
  mutable std::mutex pub_mu_;                                // 以下: GUI 取
  QuoteStatus status_;
  std::shared_ptr<const Live> live_;
  uint64_t ver_ = 0;

  std::thread watcher_, pub_;
  std::vector<std::thread> snaps_, fillers_;
};

// 归档日 d (须在) 在资产轴前 N 条上: px [全局 id] 日根 (末快照, ref = 昨收; 归档无该只 → 收 0) / bars [全局 id] 分钟根
void archived_day(misc::Ymd d, size_t N, std::vector<Candle> &px, std::vector<Bars> &bars);
// 删 ≤ upto 的归档
void drop_archived(misc::Ymd upto);

} // namespace GUI::Quote