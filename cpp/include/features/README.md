| 一级因子类别                            | 二级因子                | 特征定义 / 说明                               | 示例因子名称 / 构建方式                                    |
| --------------------------------- | ------------------- | --------------------------------------- | ------------------------------------------------ |
| **订单侵略性<br>Order Aggressiveness** | 单笔订单侵略性             | 距离bid1/ask1的价格距离，越近越激进；可用 log 或相对tick表示 | `aggressiveness = log(best_price / order_price)` |
|                                   | 平均买卖侵略性             | 一段时间内所有买单 / 卖单的平均侵略性                    | `mean_aggr_bid`, `mean_aggr_ask`                 |
|                                   | 多空侵略性差              | 买单侵略性均值减去卖单侵略性均值                        | `delta_aggr = mean_aggr_bid - mean_aggr_ask`     |
|                                   | 市价单发生概率预估           | 根据订单簿厚度估算某侧执行概率                         | `P(exec_buy) = f(bid_depth)`                     |
|                                   | Aggressiveness 时间趋势 | 滑窗下的变化趋势，如斜率、均值变化                       | `slope(aggr_t)`, `std(aggr_t)`                   |
| **订单簿形状<br>Order Book Shape**     | 顶点位置(hump)          | LOB中挂单量最多位置(相对距离best)                   | `hump_loc = argmax(volume_i)`                    |
|                                   | 斜率(Slope)           | 不同价格档的挂单梯度变化                            | `LOB_slope = Δvol / Δprice`                      |
|                                   | 凸性/凹性(Convexity)    | 全部挂单位置拟合后的形状，如凸、凹、线性                    | `LOB_convexity_score`                            |
|                                   | 峰数(Hump Count)      | 单峰/双峰/多峰(通过聚类、滤波器等提取)                   | `n_peaks = count_peaks(LOB)`                     |
|                                   | 极端档异常值              | 在远离bid/ask档位处的异常挂单                      | `vol_at_dist_20ticks`                            |
|                                   | 时间演化特征              | hump位置/形状的变化率、迁移方向                      | `Δhump_loc / Δt`, `hump_trend`                   |
| **撤单行为<br>Cancellation Behavior** | 撤单量                 | 时间段内的撤单笔数 / 手数 / 金额                     | `cancel_vol`, `cancel_value`                     |
|                                   | 撤单分布密度              | 在价格档上的分布集中程度                            | `cancel_density(p)`                              |
|                                   | 撤挂比(撤单/挂单)          | `撤单量 / 同期挂单量`，体现交易意图强度                  | `cancel_to_post_ratio`                           |
|                                   | 大单撤单比例              | 超过X手的撤单数量占总撤单                           | `large_cancel_ratio (>1000 shares)`              |
|                                   | 成交前撤单比              | 撮合成交前撤单与成交的比率                           | `pre_trade_cancel_ratio`                         |
|                                   | Fleeting Order 占比   | 极短时间内挂→撤订单数量 / 总订单                      | `fleeting_ratio (<50ms)`                         |
|                                   | Spoofing特征          | 利用前后订单行为识别欺骗行为                          | `spoof_flag = 1 if vol↑ + cancel↑`               |
| **事件聚集<br>Event Clustering**      | 撤单聚集                | 单位时间内撤单事件个数                             | `cancel_event_count(window=1s)`                  |
|                                   | 大单挂单聚集              | 某类事件(如大挂单)在时间上的密度                       | `large_order_event_rate`                         |
|                                   | 高频事件指数              | 所有异常行为的单位时间指数                           | `event_entropy`, `event_rate`                    |
|                                   | 多事件联合聚集             | 大挂单 + 同侧主动成交 + 撤单是否同时发生                 | `multi_event_cluster_score`                      |
|                                   | 时序依赖性               | 聚集事件的自激性质(可用于Hawkes建模)                  | `event_A_followed_by_B_rate`                     |
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

| 维度 | 检查方向 | 核心检验内容 | 入库标准 | 技术要点 |
|------|------------|----------------|-------------|-------------|
| **分布稳定性(Distribution Stability)** | 跨时间 | 不同时期特征分布形态(均值、方差、偏度、峰度)是否稳定 | 滚动窗口 KS-test / Wasserstein 距离 < 阈值(如 0.1) | 使用时间分桶检验特征漂移；记录漂移路径 |
| **分布对称性(Distribution Symmetry)** | 横截面 | 检查偏度/峰度异常；识别单侧饱和或极值偏移 | 偏度 ∈ [-1,1]；峰度 < 10(或自定义) | 对尾部值进行 Winsorize；异常分布打标 |
| **尺度鲁棒性(Scale Robustness)** | 正态化前后 | 特征分布在不同缩放方式下统计量稳定 | 不同 scale 下 Rank 相关 > 0.9 | 验证分布缩放不会改变排序结构 |
| **时序平稳性(Temporal Stationarity)** | 时间序列 | 检查单位根(ADF/P-P 检验)与滚动均值漂移 | 平稳性显著(p < 0.05)或弱平稳(差分后通过) | 非平稳特征仅可入库为动态特征(标注) |
| **时序一致性(Temporal Consistency)** | 不同频段 | 特征在不同时间粒度(1min/5min/1d)下统计特征一致 | 各尺度统计指标 RankCorr > 0.8 | 检验特征随采样频率变化的稳定性 |
| **自相关结构(Autocorrelation Structure)** | 时间序列 | 特征的自相关系数、PACF 是否快速衰减 | lag>5 时自相关绝对值 < 0.2 | 高滞后自相关说明信息滞留或构造缺陷 |
| **异方差特征(Heteroscedasticity)** | 波动时段 | 在高低波动期，特征方差变化比率 | 方差比 < 3 | 若差异显著，标注 regime-dependent |
| **信息半衰期(Information Half-life)** | 信号衰减 | 自协方差函数随滞后时间衰减至 0.5 的时间 | 半衰期 < 合理周期(如 5min/3d) | 太长 → 滞后；太短 → 噪声 |
| **时间覆盖率(Temporal Coverage)** | 缺失分布 | 时间轴上缺失段比例与集中度 | 缺失率 < 5%，无连续断点 | 时间覆盖不足易导致样本偏差 |
| **结构突变(Structural Breaks)** | 长期稳定性 | Chow test / CUSUM 检验结构突变 | 无显著突变(p>0.05) | 检查数据源或构造逻辑变动 |
| **分布可分性(Separability)** | 分位切片 | 高低分位间统计显著差异(均值/方差) | Top vs Bottom 均值差显著(t>2) | 验证特征具区分性但非预测性 |
| **噪声占比(Noise Ratio)** | 滚动窗口 | 方差分解:信号/噪声比 | SNR > 1 | 噪声主导时，标记为高噪特征 |
| **时序相依性(Temporal Dependency)** | 与滞后自身 | mutual information(lag) 峰值 | 信息峰值在小滞后范围内 | 识别时间耦合特征 |
| **非线性动态(Nonlinear Dynamics)** | 复杂性 | 计算近似熵/样本熵变化趋势 | 熵值稳定在合理区间 | 过高 = 噪声，过低 = 过度平滑 |
| **尾部鲁棒性(Tail Robustness)** | 极值事件 | 极端行情下的分布漂移比率 | 极端分位漂移 < 30% | 评估在 crisis regime 下可用性 |
| **跨资产一致性(Cross-asset Consistency)** | 不同标的 | 特征在不同股票/期货上的统计形态一致性 | KS-distance 平均 < 0.2 | 过度依赖单资产结构 = 不泛化 |
| **时间滞后敏感性(Lag Sensitivity)** | 构造延迟 | 滞后/提前一个采样周期后特征变化幅度 | 差分相关 < 0.2 | 检查延迟或未来信息污染 |
| **重采样稳定性(Resample Stability)** | 采样方案 | 改变 sampling 起点后统计一致 | 多起点间指标差异 < 5% | 检验时间对齐依赖性 |
| **条件分布稳定性(Conditional Stability)** | 市况分层 | 在不同波动/成交量/流动性 regime 下分布差异 | 条件 KS < 0.15 | 确认特征不随市场状态剧烈漂移 |
