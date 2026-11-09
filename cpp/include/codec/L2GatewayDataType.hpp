#pragma once

#include <cstdint>

// 深圳L2行情网关 (MDGW: Market Data GateWay)
namespace L2_sz {

// 集中竞价交易行情快照消息(300111)
struct sz_stk_snapshot {
  // Standard Header
  uint32_t StandardHeader; // 消息头 MsgType=300111

  // Basic Info
  int64_t OrigTime;         // 数据生成时间 (YYYYMMDDHHMMSSsss)
  uint16_t ChannelNo;       // 频道代码
  char MDStreamID[3];       // 行情类别 现货:010 期权:040
  char SecurityID[8];       // 证券代码
  char SecurityIDSource[4]; // 证券代码源 深交:102 港交:103
  char TradingPhaseCode[8]; // 产品所处的交易阶段代码
                            // [0]: S:启动 O:开盘 T:连续竞价 B:休市 C:收盘集合竞价 E:已闭市 H:零时停牌 A:盘后交易 V:波动性中断
                            // [1]: 0:正常 1:全天停牌

  // Price and Volume Info
  int64_t PrevClosePx;      // 昨收价 Int64-N13(4)
  int64_t NumTrades;        // 成交笔数
  int64_t TotalVolumeTrade; // 成交总量 Int64-N15(2)
  int64_t TotalValueTrade;  // 成交总金额 Int64-N18(4)

  // MD Entries
  uint32_t NoMDEntries; // 行情条目个数

  struct MDEntry {
    char MDEntryType[2];    // MD Entry Type:
                            // 0=Buy (买入)
                            // 1=Sell (卖出)
                            // 2=Last Price (最近价)
                            // 4=Open Price (开盘价)
                            // 7=High Price (最高价)
                            // 8=Low Price (最低价)
                            // x1=Price Change 1 (升跌一)
                            // x2=Price Change 2 (升跌二)
                            // x3=Buy Summary (买入汇总-总量及加权平均价)
                            // x4=Sell Summary (卖出汇总-总量及加权平均价)
                            // x5=Stock PE Ratio 1 (股票市盈率一)
                            // (not used) x6=Stock PE Ratio 2 (股票市盈率二)
                            // x7=Fund T-1 NAV (基金T-1日净值)
                            // x8=Fund Real-time NAV/IOPV (基金实时参考净值/ETF的IOPV)
                            // x9=Warrant Premium Rate (权证溢价率)
                            // xe=Upper Limit Price (涨停价)
                            // xf=Lower Limit Price (跌停价)
                            // (not used) xg=Contract Open Interest (合约持仓量)
                            // (not used) xi=Reference Price (参考价)
    int64_t MDEntryPx;      // 价格 Int64-N18(6)
    int64_t MDEntrySize;    // 数量 Int64-N15(2)
    uint16_t MDPriceLevel;  // 买卖盘档位
    int64_t NumberOfOrders; // 价位总委托笔数
    uint32_t NoOrders;      // 价位揭示委托笔数
    int64_t OrderQty;       // 委托数量 Int64-N15(2)
  };

  MDEntry md_entries[10]; // Flexible array member
};

// 集中竞价交易逐笔成交消息(300191)
struct sz_stk_trade {
  // Standard Header
  uint32_t StandardHeader; // 消息头 MsgType=300191

  // Basic Info
  uint16_t ChannelNo;       // 频道代码
  int64_t ApplSeqNum;       // 消息记录号 (从1开始)
  char MDStreamID[3];       // 行情类别 现货:011 期权:041
  char SecurityID[8];       // 证券代码
  char SecurityIDSource[4]; // 证券代码源 深交:102 港交:103

  int64_t Price;        // 委托价格 Int64-N13(4)
  int64_t OrderQty;     // 委托数量 Int64-N15(2)
  char Side;            // 买卖方向 1:买 2:卖 G:借入 F:出借
  int64_t TransactTime; // 委托时间 (YYYYMMDDHHMMSSsss)
};

// 集中竞价交易逐笔委托消息(300192)
struct sz_stk_order {
  // Standard Header
  uint32_t StandardHeader; // 消息头 MsgType=300192

  // Basic Info
  uint16_t ChannelNo;       // 频道代码
  int64_t ApplSeqNum;       // 消息记录号 (从1开始)
  char MDStreamID[3];       // 行情类别 现货:010 期权:040
  char SecurityID[8];       // 证券代码
  char SecurityIDSource[4]; // 证券代码源 深交:102 港交:103

  int64_t Price;        // 委托价格 Int64-N13(4)
  int64_t OrderQty;     // 委托数量 Int64-N15(2)
  char Side;            // 买卖方向 1:买 2:卖 G:借入 F:出借
  char OrderType;       // 订单类型 1:市价 2:限价 U:本方最优(防止高频套利)
  int64_t TransactTime; // 委托时间 (YYYYMMDDHHMMSSsss)
};

} // namespace L2_sz

// 上海L2低延时行情发布系统 (LDDS: Low latency Data Distribution System)
namespace L2_sh {
// 08:30-16:00:静态数据准备完毕,VSS 可以发起 Rebuild 请求
// 08:45-16:00:竞价行情数据、指数行情数据、市场总览数据、竞价逐笔通道序号数据
// 09:15-15:35:竞价逐笔合并数据
// 09:25-16:00:逐笔合并数据重建
// 15:00-16:00:盘后固定价格交易行情数据
// 15:05-15:35:盘后固定价格交易逐笔成交消息
// 15:05-16:00:盘后固定交易价格逐笔成交数据重建

// 集中竞价交易行情快照消息(UA3202)
struct sh_stk_snapshot {
  // STEP Message Header
  char StandardHeader[6]; // 消息头 MsgType=UA3202
  int32_t CategoryID;     // 6
  int32_t MsgSeqID;       // 消息序号
  int32_t RawDataLength;  // FAST 数据长度

  // FAST Message Content
  int32_t TemplateID;    // 模板标示号 = 3202
  char MessageType[8];   // UA3202
  int32_t DataTimeStamp; // 最新订单时间(秒)143025 表示 14:30:25
  int32_t DataStatus;    // 1=重复数据 2=未获授权
  char SecurityID[8];    // 证券代码
  int32_t ImageStatus;   // 快照类型 1=全量

  // Price Information
  int32_t PreClosePx; // 昨收盘价格
  int32_t OpenPx;     // 开盘价格
  int32_t HighPx;     // 最高价格
  int32_t LowPx;      // 最低价格
  int32_t LastPx;     // 现价格
  int32_t ClosePx;    // 今日收盘价格

  // Trading Status
  char InstrumentStatus[8]; // 当前品种交易状态
                            // START: 启动
                            // OCALL: 开市集合竞价
                            // TRADE: 连续自动撮合
                            // SUSP:  停牌
                            // CCALL: 收盘集合竞价
                            // CLOSE: 闭市,自动计算闭市价格
                            // ENDTR: 交易结束
  char TradingPhaseCode[8]; // 产品实时阶段及标志
                            // [0]: S:启动(开市前) C:开盘集合竞价 T:连续交易 E:闭市 P:产品停牌 M:可恢复交易的熔断 N:不可恢复交易的熔断 U:收盘集合竞价
                            // [1]: 0:不可正常交易 1:可正常交易
                            // [2]: 0:未上市 1:已上市
                            // [3]: 0:不接受订单申报 1:可接受订单申报

  // Trading Statistics
  int32_t NumTrades;             // 成交笔数
  int64_t TotalVolumeTrade;      // 成交总量
  int64_t TotalValueTrade;       // 成交总金额
  int64_t TotalBidQty;           // 委托买入总量
  int32_t WeightedAvgBidPx;      // 加权平均委买价格
  int32_t AltWeightedAvgBidPx;   // 债券加权平均委买价格
  int64_t TotalOfferQty;         // 委托卖出总量
  int32_t WeightedAvgOfferPx;    // 加权平均委卖价格
  int32_t AltWeightedAvgOfferPx; // 债券加权平均委卖价格

  // ETF Information
  int32_t IOPV;          // ETF净值估值(IOPV低精度值)
  int32_t ETFBuyNumber;  // ETF申购笔数
  int64_t ETFBuyAmount;  // ETF申购数量
  int64_t ETFBuyMoney;   // ETF申购金额
  int32_t ETFSellNumber; // ETF赎回笔数
  int64_t ETFSellAmount; // ETF赎回数量
  int64_t ETFSellMoney;  // ETF赎回金额

  // Bond Information
  int32_t YieldToMaturity;     // 债券到期收益率
  int64_t TotalWarrantExecQty; // 权证执行的总数量
  int64_t WarLowerPx;          // 债券质押式回购品种加权平均价
  int64_t WarUpperPx;          // IOPV高精度值

  // Withdraw Information
  int32_t WithdrawBuyNumber;  // 买入撤单笔数
  int64_t WithdrawBuyAmount;  // 买入撤单数量
  int64_t WithdrawBuyMoney;   // 买入撤单金额
  int32_t WithdrawSellNumber; // 卖出撤单笔数
  int64_t WithdrawSellAmount; // 卖出撤单数量
  int64_t WithdrawSellMoney;  // 卖出撤单金额

  // Order Information
  int32_t TotalBidNumber;        // 买入总笔数
  int32_t TotalOfferNumber;      // 卖出总笔数
  int32_t BidTradeMaxDuration;   // 买入委托成交最大等待时间
  int32_t OfferTradeMaxDuration; // 卖出委托成交最大等待时间
  int32_t NumBidOrders;          // 买方委托价位数
  int32_t NumOfferOrders;        // 卖方委托价位数

  // Bid Level Information (买盘价位数量)
  int32_t NoBidLevel; // 买盘价位数量

  struct BidLevel {
    int32_t PriceLevelOperator; // 此字段不再适用
    int32_t Price;              // 价格
    int64_t OrderQty;           // 申买量
    int32_t NumOrders;          // 实际总委托笔数
    int32_t Orders;             // 发布委托笔数

    // Order Queue Information for this level
    struct OrderQueue {
      int32_t OrderQueueOperator;        // 此字段不再适用
      int32_t OrderQueueOperatorEntryID; // 此字段不再适用
      int64_t OrderQty;                  // 订单量
    } OrderQueues[50];                   // 最多50笔订单
    int32_t NumOrderQueues;              // 实际订单队列数量
  };

  // Offer Level Information (卖盘价位数量)
  int32_t NoOfferLevel; // 卖盘价位数量

  struct OfferLevel {
    int32_t PriceLevelOperator; // 此字段不再适用
    int32_t Price;              // 价格
    int64_t OrderQty;           // 申卖量
    int32_t NumOrders;          // 实际总委托笔数
    int32_t Orders;             // 发布委托笔数

    // Order Queue Information for this level
    struct OrderQueue {
      int32_t OrderQueueOperator;        // 此字段不再适用
      int32_t OrderQueueOperatorEntryID; // 此字段不再适用
      int64_t OrderQty;                  // 订单量
    } OrderQueues[50];                   // 最多50笔订单
    int32_t NumOrderQueues;              // 实际订单队列数量
  };

  // Dynamic arrays for bid and offer levels (max 10 levels each)
  BidLevel BidLevels[10];     // 最多十档买盘
  OfferLevel OfferLevels[10]; // 最多十档卖盘
};

// 集中竞价交易逐笔成交消息(UA3209) - 盘后固定价格交易
struct sh_stk_trade {
  // STEP Message Header
  char StandardHeader[6]; // 消息头 MsgType=UA3209
  int32_t CategoryID;     // 57
  int32_t MsgSeqID;       // 消息序号
  int32_t RawDataLength;  // FAST 数据长度

  // FAST Message Content
  int32_t TemplateID;   // 模板标示号 = 3209
  char MessageType[8];  // UA3209
  int32_t DataStatus;   // 1=重复数据 2=未获授权
  int32_t TradeIndex;   // 成交序号,从1开始,按Channel连续
  int32_t TradeChannel; // 通道
  char SecurityID[8];   // 证券代码
  int32_t TradeTime;    // 成交时间(百分之一秒)15102506表示15:10:25.06
  int32_t TradePrice;   // 成交价格(元)
  int64_t TradeQty;     // 成交数量(股票:股)
  int64_t TradeMoney;   // 成交金额(元)
  int64_t TradeBuyNo;   // 买方订单号
  int64_t TradeSellNo;  // 卖方订单号
  char TradeBSFlag[2];  // 内外盘标志:B=外盘(主动买) S=内盘(主动卖) N=未知
};

// 集中竞价交易逐笔委托消息(UA5803)
struct sh_stk_order {
  // STEP Message Header
  char StandardHeader[6]; // 消息头 MsgType=UA5803
  int32_t CategoryID;     // 9
  int32_t MsgSeqID;       // 消息序号
  int32_t RawDataLength;  // FAST 数据长度

  // FAST Message Content
  int32_t TemplateID;  // 模板标示号 = 5803
  char MessageType[8]; // UA5803
  int64_t BizIndex;    // 逐笔序号,从1开始,按Channel连续
  int32_t Channel;     // 通道
  char SecurityID[8];  // 证券代码
  int32_t TickTime;    // 订单或成交时间(百分之一秒)14302506表示14:30:25.06
  char Type[2];        // 类型:A=新增委托订单 D=删除委托订单 S=产品状态订单 T=成交
  int64_t BuyOrderNO;  // 买方订单(若为产品状态订单或卖方订单无意义)
  int64_t SellOrderNO; // 卖方订单(若为产品状态订单或买方订单无意义)
  int32_t Price;       // 价格(元)(若为产品状态订单、删除订单无意义)
  int64_t Qty;         // 数量(若为产品状态订单无意义)
  int64_t TradeMoney;  // 对于新增委托:已成交的委托数量(精度为三位)
                       // 对于成交:成交金额(单位为元,精度为五位)
                       // 其他无意义
  char TickBSFlag[8];  // 若为新增或删除委托订单:B=买单 S=卖单
                       // 若为产品状态订单:START=启动 OCALL=开市集合竞价 TRADE=连续自动撮合
                       //                 SUSP=停牌 CCALL=收盘集合竞价 CLOSE=闭市 ENDTR=交易结束
                       // 若为成交:B=外盘(主动买) S=内盘(主动卖) N=未知
};

} // namespace L2_sh
