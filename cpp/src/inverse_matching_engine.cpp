// 撮合反推引擎(Inverse Matching Engine(Trade Inference from LOB Snapshots))
// NOTE:
//    1. 达到或跨对手价的限价单，成交部分会被交易所自动转换为taker，如有剩余，则为maker
//    2. 跨档taker单会被交易所拆分为多个单档taker成交
// 美式行情: 
// L1(采样:逐笔成交; 数据:逐笔成交 + 快照) (TAQ:trade&quote):
//      | NASDAQ   | UTP Quotation Feed    | SIP (SCTA/UTP) |
//      | NYSE     | CTA Consolidated Feed | SIP (CTA)      |
//      | CME      | Top-of-Book           | MDP 3.0 (FAST) |
//      | ICE      | Top-of-Book           | ICE DataFeed   |
//      | OPRA     | Options Price Feed    | SIP (OPRA)     |
//      时间戳, 成交价/量(逐笔) 买/卖价量(一档, 成交后快照, 隐含挂单/撤单/修改信息)
// L2(采样:逐笔挂/撤/改/成交(委托); 数据:快照):
//      | NASDAQ   | TotalView     | ITCH             |
//      | NYSE     | OpenBook      | Arca Proprietary |
//      | CME      | Market Depth  | MDP 3.0 (FAST)   |
//      | ICE      | Depth of Book | ICE DataFeed     |
//      时间戳, 买/卖价量(5~50档, 聚合事件后快照, 隐含挂/撤/改/成交信息,但是无法得知具体成交)
// L3(采样:逐笔挂/撤/改/成交(委托); 数据:逐笔委托):
//      | NASDAQ   | TotalView-ITCH  | ITCH                |
//      | NYSE     | ArcaBook        | Arca Proprietary    |
//      | CME      | Full Order Book | MDP 3.0 (FAST/ITCH) |
//      | ICE      | Order Book Feed | ICE DataFeed        |
//      时间戳, 订单, 账户, 价格, 数量, 操作类型(挂撤改成交), 可以本地重建精确order book, 重现撮合过程
// ================================================================================================
// 中式行情:
// L1 (采样:定频; 数据:快照): (股票/指数:3s; ETF期权/期货/期货期权:0.5s;)
//      成交高开低收/累计成交量/累计成交额(VWAP,模糊反推撮合过程)  买/卖价量(5档, 隐含挂单/撤单/修改信息)
// L2 (采样:定频+逐笔; 数据:快照+逐笔成交+逐笔委托(挂/撤)): (期货/期货期权:0.25s(机构接口))
//      成交高开低收/累计成交量/累计成交额, 买/卖盘加权成交价格/数量, 买/卖价量(10档(上证)/500档(深证)）
//      其中委托和成交推送最优价位上前50笔, 可以通过3秒快照来同步
//      L2分为展示行情和非展示行情, 数据完全相同, 非展示行情可以转发(交易所托管), 原则上不出交易所机房, 年授权费30W, 托管成本更低
//      这两类行情在数据上相同, 但是非展示行情是binary encoded(25Mbps), 延迟更低, 建议用FPGA/ASIC在交易所机房部署
//      正向撮合规则: https://github.com/fpga2u/AXOrderBook/blob/main/doc/SE.md
//      https://www.szse.cn/marketServices/technicalservice/interface/P020220523530959450444.pdf
//      https://www.sseinfo.com/services/assortment/document/interface/c/10759998/files/f3ca62e905764efaa3983a7c20d9e1d9.pdf
// ================================================================================================
// 算法: 成交方向推断(Trade Direction Inference)
// | **Algorithm**     | **Core Idea**                                    | **Input Required**                | **Output**              | **Accuracy** | **Applicable Data Types**         | **Usage Notes**                               |
// | ----------------- | ------------------------------------------------ | --------------------------------- | ----------------------- | ------------ | --------------------------------- | --------------------------------------------- |
// | **Tick Rule**     | Compare current trade price with previous price  | Trade price time series           | Buy/Sell direction      | Low          | US L1, CN L2                      | Simple baseline method; quote-independent     |
// | **Quote Rule**    | Compare trade price to mid-price or best bid/ask | Trade price + bid/ask quotes      | Buy/Sell direction      | Medium       | US L1, CN L2                      | Works better with stable quotes               |
// | **Lee–Ready**     | Tick Rule + time-adjusted Quote Rule             | Trade price + bid/ask + timestamp | Buy/Sell direction      | Medium       | US L1 (e.g., TAQ)                 | Widely used for consolidated tape data        |
// | **EMO**           | Estimate expected matching based on book changes | Pre/post quote snapshots + volume | Matched trades + side   | High         | US L1/L2, CN L2                   | Snapshot-frequency dependent                  |
// | **BVC**           | Allocate aggregated volume based on book depth   | Aggregated volume + order book    | Agg. buy/sell volume    | Medium-High  | CN L1/L2                          | Good for time-bucketed or batch trade data    |
// | **CLNV**          | Rule-based price location inside/outside spread  | Trade price + quote               | Buy/Sell direction      | Medium       | US L1, CN L2                      | Handles edge cases better than Quote Rule     |
// | **ML Classifier** | Train model using labeled data (L3) + quote info | Price + quote + labels (L3 req.)  | Probabilistic direction | Very High    | Training: US L3; Inference: L1/L2 | Requires training on labeled order-level data |
// ================================================================================================
// 算法: 挂单簿重建(Order Book Reconstruction)
// | **Algorithm**              | **Core Idea**                                                    | **Input Required**                    | **Output**                   | **Accuracy** | **Applicable Data Types** | **Usage Notes**                                              |
// | -------------------------- | ---------------------------------------------------------------- | ------------------------------------- | ---------------------------- | ------------ | ------------------------- | ------------------------------------------------------------ |
// | **Snapshot Diff**          | Compare consecutive snapshots to infer order placement/cancel    | Snapshot sequence                     | Implied order events         | Medium       | US L2, CN L2              | Best-effort estimation based on time-diffed states           |
// | **Order Flow Inference**   | Use snapshot + trade volume deltas to estimate matching sequence | Snapshots + trade deltas              | Estimated matching actions   | High         | US L2, CN L2              | Requires precise volume alignment and low-latency feed       |
// | **Limit Order Imputation** | Simulate LOB using rules to guess unobserved orders              | Snapshot + trade aggregates           | Synthetic order book states  | Medium       | CN L1/L2                  | Useful when only top-of-book or summary data is available    |
// | **Full Order Book Replay** | Replay exact order messages with IDs and operations              | Full order messages (add/cancel/exec) | Full order book + trades     | Very High    | US L3                     | Reconstructs exact matching if feed (e.g. ITCH) is available |
// | **Matching Engine Replay** | Replay events through a matching engine with rule logic          | Order flow + exchange logic           | Reconstructed book & trades  | Very High    | US L3, CN L2 (private)    | Requires complete event sequence + exchange matching rules   |
// | **Bayesian Matching**      | Use probabilistic model to infer likely matches and transitions  | Snapshots + trades                    | Probabilistic trade sequence | High         | US L1/L2, CN L2           | Useful for uncertain or noisy data with missing event info   |
