// ReplayService — agent 盘口重放 (view_book) 的单 worker: 一个 (资产, 日) 的日文件帧逐笔重放进订单簿 (与 OrderFlow Sec 同一条路:
// 帧目录快照定位 → DayReader 解码 → LimitOrderBook 无特征侧绑定逐笔 process), 落地原料不做汇总:
//   当日: 成交 (开高低收 / 成交额; 含竞价撮合) + 大单门槛 (给定, 或当日新挂单额的 kBigQuantile 分位);
//   窗口 [s0, s1) 内逐秒: 秒末盘口 (买一卖一 / 前五档挂单额 / 竞价预撮合; 与 depth_buffer 同节流) + 秒内流量 (主动成交 / 新挂 / 撤单额, 分侧);
//   窗口内大单事件: 新挂 / 撤单 / 被吃完 (按委托额 ≥ 门槛) + 扫单 (连续竞价里一个主动单连吃 ≥ 2 个价位, 或成交额 ≥ 门槛);
//   锚点秒末的全簿 (真实价位, 价升序).
// 委托维度 (申报量 / 挂单时刻 / 所在价位) 全取订单簿的逐笔快照 LOB_Feature::ord_*, 不自建 id 表. 汇总与成文在调用方 (agent/Views).
// 不碰 SharedData: 帧目录快照 / 日文件目录 / 交易所随请求给 (GUI 线程取). 结果只在内存, UI 持 mutex 取走; 首个请求才建订单簿 (奢侈容量, 常驻复用)
#pragma once

#include "codec/L2_DataType.hpp" // ExchangeType
#include "gui/util/Service.hpp"
#include "misc/date.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

struct OrdersCatalog;

namespace GUI::Features {

// 交易秒: 钟面秒 (h·3600 + m·60 + s) 在两个交易时段上的连续编号 09:15:00 → 0 … 11:29:59 → 8099, 13:00:00 → 8100 … 14:59:59 → 15299;
// 午休钳到 8100, 盘前 0, 盘后 15299. 与 L_sec 不同, 收盘集合竞价 14:57–15:00 逐秒可分
inline constexpr uint32_t kTradeSecs = 15300;
inline constexpr uint32_t kAmOpen = 9 * 3600 + 15 * 60, kAmClose = 11 * 3600 + 30 * 60, kPmOpen = 13 * 3600, kPmClose = 15 * 3600;
inline constexpr uint32_t trade_sec(uint32_t clock) {
  return clock < kAmOpen    ? 0
         : clock < kAmClose ? clock - kAmOpen
         : clock < kPmOpen  ? kAmClose - kAmOpen
         : clock < kPmClose ? kAmClose - kAmOpen + clock - kPmOpen
                            : kTradeSecs - 1;
}
inline constexpr uint32_t trade_clock(uint32_t sec) { return sec < kAmClose - kAmOpen ? kAmOpen + sec : kPmOpen + sec - (kAmClose - kAmOpen); }
static_assert(trade_sec(kAmClose - 1) == 8099 && trade_sec(kPmOpen) == 8100 && trade_sec(kPmClose - 1) == kTradeSecs - 1 && trade_clock(8100) == kPmOpen);

struct ReplayRequest {
  std::shared_ptr<const OrdersCatalog> orders; // 帧目录快照 (Asset::orders)
  std::string dir;                             // 日文件目录 (Config::orders_dir)
  uint32_t asset = 0;                          // 全局资产下标
  L2::ExchangeType exchange = L2::ExchangeType::UNKNOWN;
  misc::Ymd date = 0;
  uint32_t s0 = 0, s1 = 0; // 窗口 [s0, s1) (交易秒)
  uint32_t at = 0;         // 全簿剖面取这一秒末 (∈ [s0, s1))
  float big = 0.f;         // 大单门槛 (元); 0 = 当日新挂单额的 kBigQuantile 分位
};
inline constexpr float kBigQuantile = 0.99f;

// 窗口里的一秒 (下标 0 = 买侧 / 1 = 卖侧; 额 = 元)
struct ReplaySec {
  float bid = 0.f, ask = 0.f;                         // 秒末买一 / 卖一 (该侧无真实价位 = 0)
  float depth[2] = {};                                // 秒末前 5 价位挂单额
  float ref = 0.f, matched = 0.f, imbalance = 0.f;    // 秒末竞价预撮合: 参考价 / 匹配额 / 未匹配额 (+ 买剩 − 卖剩); 非竞价或簿未交叉 = 0
  float taker[2] = {}, maker[2] = {}, cancel[2] = {}; // 秒内主动成交额 (按主动方) / 新挂额 / 撤单额
  float last = 0.f;                                   // 秒内末笔成交价 (无成交 = 0)
};
enum class ReplayKind : uint8_t { New,     // 新挂
                                  Cancel,  // 撤单
                                  Filled,  // 挂单被吃完
                                  Sweep }; // 扫单
struct ReplayEvent {
  uint32_t tick = 0; // 时刻 (L2::make_tick 编码)
  uint32_t sec = 0;  // 交易秒
  ReplayKind kind = ReplayKind::New;
  uint8_t side = 0;    // 委托所在侧 (Sweep = 主动方)
  uint16_t levels = 0; // Sweep 吃过的价位数
  float price = 0.f;   // 元 (Sweep = 末笔价)
  float first = 0.f;   // Sweep = 首笔价; 其余 = price
  float amount = 0.f;  // 元: New / Cancel = 本笔委托 / 撤单额; Filled = 申报额; Sweep = 成交额
  float life = -1.f;   // Cancel / Filled: 距挂出的交易秒; 挂单时刻不可信 (乱序占位单) = −1
};
struct ReplayResult {
  std::string error;                                    // 非空 = 没重放 (当日无帧 / 帧为空), 其余字段空
  uint32_t orders = 0;                                  // 当日逐笔条数
  float open = 0.f, high = 0.f, low = 0.f, close = 0.f; // 当日成交价 (无成交 = 0)
  double amount = 0.0;                                  // 当日成交额 (元)
  float big = 0.f;                                      // 所用大单门槛 (元)
  std::vector<ReplaySec> secs;                          // [s1 − s0]
  std::vector<ReplayEvent> events;                      // 窗口内, 时序
  std::vector<std::pair<float, float>> book;            // 锚点秒末全簿: (价, 挂单额 元; + 买 − 卖), 价升序
};

class ReplayService : public Service<ReplayService, ReplayRequest>, public Published {
  friend Service<ReplayService, ReplayRequest>;

public:
  static constexpr const char *kName = "ReplayWorker";
  ReplayService(); // 两者都在 .cpp: 订单簿不完整
  ~ReplayService();

  std::string message; // 本轮说明 (与因子服务同形; 恒空)
  ReplayResult result; // UI 持 mutex 取走 (std::move)

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return *this; }
  bool run(ReplayRequest &req);

  struct Res; // 解码器 + 订单簿 + 盘口写出口
  std::unique_ptr<Res> res_;
};

} // namespace GUI::Features
