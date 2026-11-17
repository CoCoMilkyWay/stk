// ReplayService — agent 盘口重放 (view_book) 的单 worker: 一个 (资产, 日) 的日文件帧逐笔重放进订单簿 (与 OrderFlow Sec 同一条路:
// 帧目录快照定位 → DayReader 解码 → LimitOrderBook 无特征侧绑定逐笔 process), 落成 Book (task_agent/Book), 不做汇总:
//   当日: 成交 (开高低收 / 成交额; 含竞价撮合) + 大单门槛 (给定, 或当日新挂单额的 kBigQuantile 分位);
//   窗口 [from, to) 内逐秒: 秒末盘口 (买一卖一 / 前五档挂单额 / 竞价预撮合; 与 depth_buffer 同节流) + 秒内流量 (主动成交 / 新挂 / 撤单额, 分侧);
//   窗口内大单事件: 新挂 / 撤单 / 被吃完 (按委托额 ≥ 门槛) + 扫单 (连续竞价里一个主动单连吃 ≥ 2 个价位, 或成交额 ≥ 门槛);
//   锚点秒末的全簿 (真实价位, 价升序).
// 委托维度 (申报量 / 挂单时刻 / 所在价位) 全取订单簿的逐笔快照 LOB_Feature::ord_*, 不自建 id 表. 汇总与成文在调用方 (task_agent/Views).
// 不碰 SharedData: 帧目录快照 / 交易所随请求给 (GUI 线程取). 结果只在内存, UI 持 mutex 取走; 首个请求才建订单簿 (奢侈容量, 常驻复用)
#pragma once

#include "gui/task_agent/Book.hpp"
#include "gui/util/Service.hpp"
#include "orders/L2DataType.hpp" // ExchangeType

#include <cstdint>
#include <memory>
#include <string>

struct OrdersCatalog;

namespace GUI::Agent {

struct ReplayRequest {
  std::shared_ptr<const OrdersCatalog> orders; // 帧目录快照 (Asset::catalog)
  uint32_t asset = 0;                          // 全局资产下标
  L2::ExchangeType exchange = L2::ExchangeType::UNKNOWN;
  BookSpec spec;
};

class ReplayService : public Service<ReplayService, ReplayRequest>, public Published {
  friend Service<ReplayService, ReplayRequest>;

public:
  static constexpr const char *kName = "ReplayWorker";
  ReplayService(); // 两者都在 .cpp: 订单簿不完整
  ~ReplayService();

  std::string message; // 本轮说明 (与因子服务同形; 恒空)
  Book result;         // UI 持 mutex 取走 (std::move)

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return *this; }
  bool run(ReplayRequest &req);

  struct Res; // 解码器 + 订单簿 + 盘口写出口
  std::unique_ptr<Res> res_;
};

} // namespace GUI::Agent
