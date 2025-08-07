| 一级因子类别                            | 二级因子                | 特征定义 / 说明                               | 示例因子名称 / 构建方式                                    |
| --------------------------------- | ------------------- | --------------------------------------- | ------------------------------------------------ |
| **订单侵略性<br>Order Aggressiveness** | 单笔订单侵略性             | 距离bid1/ask1的价格距离，越近越激进；可用 log 或相对tick表示 | `aggressiveness = log(best_price / order_price)` |
|                                   | 平均买卖侵略性             | 一段时间内所有买单 / 卖单的平均侵略性                    | `mean_aggr_bid`, `mean_aggr_ask`                 |
|                                   | 多空侵略性差              | 买单侵略性均值减去卖单侵略性均值                        | `delta_aggr = mean_aggr_bid - mean_aggr_ask`     |
|                                   | 市价单发生概率预估           | 根据订单簿厚度估算某侧执行概率                         | `P(exec_buy) = f(bid_depth)`                     |
|                                   | Aggressiveness 时间趋势 | 滑窗下的变化趋势，如斜率、均值变化                       | `slope(aggr_t)`, `std(aggr_t)`                   |
| **订单簿形状<br>Order Book Shape**     | 顶点位置（hump）          | LOB中挂单量最多位置（相对距离best）                   | `hump_loc = argmax(volume_i)`                    |
|                                   | 斜率（Slope）           | 不同价格档的挂单梯度变化                            | `LOB_slope = Δvol / Δprice`                      |
|                                   | 凸性/凹性（Convexity）    | 全部挂单位置拟合后的形状，如凸、凹、线性                    | `LOB_convexity_score`                            |
|                                   | 峰数（Hump Count）      | 单峰/双峰/多峰（通过聚类、滤波器等提取）                   | `n_peaks = count_peaks(LOB)`                     |
|                                   | 极端档异常值              | 在远离bid/ask档位处的异常挂单                      | `vol_at_dist_20ticks`                            |
|                                   | 时间演化特征              | hump位置/形状的变化率、迁移方向                      | `Δhump_loc / Δt`, `hump_trend`                   |
| **撤单行为<br>Cancellation Behavior** | 撤单量                 | 时间段内的撤单笔数 / 手数 / 金额                     | `cancel_vol`, `cancel_value`                     |
|                                   | 撤单分布密度              | 在价格档上的分布集中程度                            | `cancel_density(p)`                              |
|                                   | 撤挂比（撤单/挂单）          | `撤单量 / 同期挂单量`，体现交易意图强度                  | `cancel_to_post_ratio`                           |
|                                   | 大单撤单比例              | 超过X手的撤单数量占总撤单                           | `large_cancel_ratio (>1000 shares)`              |
|                                   | 成交前撤单比              | 撮合成交前撤单与成交的比率                           | `pre_trade_cancel_ratio`                         |
|                                   | Fleeting Order 占比   | 极短时间内挂→撤订单数量 / 总订单                      | `fleeting_ratio (<50ms)`                         |
|                                   | Spoofing特征          | 利用前后订单行为识别欺骗行为                          | `spoof_flag = 1 if vol↑ + cancel↑`               |
| **事件聚集<br>Event Clustering**      | 撤单聚集                | 单位时间内撤单事件个数                             | `cancel_event_count(window=1s)`                  |
|                                   | 大单挂单聚集              | 某类事件（如大挂单）在时间上的密度                       | `large_order_event_rate`                         |
|                                   | 高频事件指数              | 所有异常行为的单位时间指数                           | `event_entropy`, `event_rate`                    |
|                                   | 多事件联合聚集             | 大挂单 + 同侧主动成交 + 撤单是否同时发生                 | `multi_event_cluster_score`                      |
|                                   | 时序依赖性               | 聚集事件的自激性质（可用于Hawkes建模）                  | `event_A_followed_by_B_rate`                     |
| **订单簿韧性<br>LOB Resiliency**       | 恢复量                 | 市价单冲击后x秒内回补量                            | `replenish_vol = new_vol - old_vol`              |
|                                   | 恢复率                 | `恢复量 / 冲击移除量`，衡量重建意愿                    | `resiliency_ratio`                               |
|                                   | 恢复速度                | 达到恢复x%所需的时间                             | `t_80%_recovery`                                 |
|                                   | 恢复倾向方向              | 买卖两侧恢复速率是否对称                            | `buy_recovery_rate - sell_recovery_rate`         |
|                                   | 多次冲击下韧性             | 连续市价单冲击后的韧性演化趋势                         | `resiliency_decay_curve`                         |
| **异常挂单<br>Abnormal Orders**       | 涨跌停挂单量              | 距离涨跌停价1tick挂单总量                         | `vol_at_limit_price`                             |
|                                   | 无成交大挂单              | 长时间未成交的挂单                               | `stale_order_vol`                                |
|                                   | 持续时间长的挂单            | 在某档维持x秒以上的挂单量                           | `durable_order_vol`                              |
|                                   | 异常密集挂单              | 在偏离市场价格位置的集中大挂单                         | `blockage_at_price(p)`                           |
| **逐笔主动交易<br>Trade Initiation**    | 主动买/卖标记             | 基于成交价与mid的关系判断                          | `trade_direction = 1 (buy), -1 (sell)`           |
|                                   | 主买主卖比例              | 时间窗口内主动买成交量 / 总成交量                      | `active_buy_ratio`                               |
|                                   | 净流入/流出              | 主买金额 - 主卖金额                             | `net_inflow = buy_amt - sell_amt`                |
|                                   | 大单主买率               | 仅统计大于X手的主动买比例                           | `large_trade_buy_ratio`                          |
|                                   | 滑窗主动行为              | 分时段统计主动方向变化                             | `buy_sell_switch_rate`                           |
