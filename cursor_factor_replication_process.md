# Factor replication process
_Exported on 9/16/2026 at 16:35:42 GMT+8 from Cursor (3.20.21)_

---

**User**

我们研报库做好了, 你觉得因子复现应该怎么一步一步来
@README @output/doc/resources-tag/tag.md @output/doc/resources-tag/tag_stat.md 

---

**Cursor**

看完库和现状，先说我看到的三个错位，再给步骤。

**现状与研报库的错位**

- 预测期：库里 4748 个因子，horizon monthly 3264 / weekly 726 / daily 376，而我们只有 `lb_*_5m/10m/30m` 三档日内标签，没有日频以上的标签。绝大多数研报因子现在根本没法被检验。
- 检验器：`L2` 的 IC/RankIC/ICIR/分组收益 这套"裁判"代码还不存在，`output/factors` 只是预留目录。复现的第一产出物是数字，没有裁判就没有复现。
- 样本：smallcap400 × 2023-01~2026-08。400 只股票单日截面 RankIC 的噪声量级约 \(1/\sqrt{400}=0.05\)，和研报多数因子的 |RankIC| 同量级；3.5 年 vs 研报 10 年+。所以原文数字不能直接对标，只能对标 符号 → 量级 → 分组单调性 三级。

**建议的步骤**

1. 先造裁判，不碰因子。日频标签（T+1 开盘→T+n 开盘，n=1/5/20，对齐库里 daily/weekly/monthly 三档）+ 单因子检验（RankIC 序列、ICIR、十分组多空、换手、行业/市值中性前后各一份）。输出格式固定成和 `Factor.stats` 同位序 `[ic, rank_ic, icir, return_ls]`，这样复现结果能和 tag json 逐位比。
2. 用现有特征做校准。`mcap_cs`、`bp_mrq_cs`、`ep_ttm_cs`、`_ohlc_close` 反转、`_ohlc_volume/fmcap` 换手、`cnbi` 净买入——这些都是库里几十篇重复验证过的公认因子，不用新算子就能出。目的不是发现 alpha，是确认在我们的池子和年份里，公认因子的符号和量级长什么样。校准不过，后面全是白做。
3. 从库里筛复现候选，机器筛而不是人挑。条件：data source ⊆ 我们有的（market_l1_l2 / market_minute / market_daily / statement）、value=high、rank_ic 非 null、formula 有闭式（排掉 representation_learning / llm_generation）。我刚扫了一遍：value=high 且 data_period ∈ {second, intraday} 且给了 rank_ic 的因子有 547 个，这是我们相对券商最有优势的一块（我们有逐笔+LOB 还原，他们多数只有分钟线）。再按"同一因子被几家券商独立报道"排序，跨机构重复出现的优先。
4. 单因子复现固定流程：formula → 日内 TS 聚合成日频值（这一步就是库里 L0 reconstruction 那 39 篇干的事）→ 按原文 setup 做 CS 处理（去极值/中性化用现有 `Method/CS.hpp` 那套）→ 用原文 horizon 跑裁判 → 三级判定 → 原文 stats 与复现 stats 并排落库。先尽量贴原文 setup 复现，再迁到自己的 setup，两步分开，否则不知道差异来自因子还是来自环境。
5. 攒到 30~50 个通过的因子再做 L2_zoo：相关矩阵、正交化、按 approach 聚类看冗余。L3 合成在这之前不碰。

**需要你定的三件事**

- 因子存储：现在 `features` 是分钟级 `[T][F][A]`，因子是日频 `[D][A]`。是加一个日频 level 复用 `FeatureStore`，还是 `output/factors` 单独一套？这决定裁判读什么。
- 校准池：只用 smallcap400，还是为了和研报对标先跑一个 csi500/全市场 的日频池（只需日线+财务，不需要 L2）？后者能让第 2 步的量级对比有意义。
- 日频标签的定义：开→开 还是 收→收，要不要扣涨跌停/停牌不可交易的样本。

---

**User**

帮我评估一下, 准备主要先复现日频, 日内的因子
看下那些因子特别多的报告, 先把这个频段的, 综述类型的文章找出来, 然后再分析基建需要补充的部分

---

**Cursor**

**频段筛选结果**

data_period ⊆ {daily, intraday, second} 的因子：569 篇 / 2916 个（占全库 61%）。

- 数据周期：daily 1460、intraday 792、second 502，混合 162
- 预测周期：monthly 1806、weekly 558、daily 363、intraday 83
- 数据源（篇）：market_daily 354、market_minute 182、market_l1_l2 122
- 家族：microstructure 877、volatility 464、momentum 403、liquidity 263、reversal 191

关键判断：这个频段 80% 的因子是"高频数据、低频预测"——用分钟/逐笔数据算出日频值，再滚 20/60 天，预测下月。所以"复现日频/日内因子"的基建重心不在日内预测，在 日内→日频聚合 + 跨日滚动 + 周/月标签。

**因子最多的报告（本频段）**

| 因子数 | 体裁 | 数据 | 报告 |
|---|---|---|---|
| 55 | handbook | minute | 广发-多因子-048 高频数据的因子化研究 (2023) |
| 34 | research | l2 | 长江-高频因子-014 交易行情高频因子收益来源（无 IC）|
| 33 | research | mixed | 广发-深度学习-007 深度学习框架下高频数据因子挖掘 |
| 30 | handbook | daily | 西南-barra-001 CNE6 解读（风险因子，非 alpha）|
| 28 | research | mixed | 华泰-人工智能-072 全频段量价特征 |
| 25 | research | l2 | 广发-海量level二-001 94 个大小单因子 |
| 24 | research | daily | 华泰-人工智能-023 遗传规划因子挖掘 |
| 22 | handbook | daily | 中金-量化多因子-007 价量因子手册 |
| 22 | research | l2 | 国金-alpha掘金-004 逐笔遗憾规避因子 |
| 20 | handbook | minute | 长江-高频因子-016 2024 年有效因子 |
| 20 | research | minute | 开源-微观结构-027 成交量峰岭谷 |
| 18 | framework | mixed | 海通-指数增强-001 高频因子还是行业暴露 |
| 18 | handbook | mixed | 开源-微观结构-029 2023 以来高频因子回顾 |
| 17 | research | minute | 信达-因子选股-001 基于分钟线的高频选股因子 |
| 17 | research | mixed | 光大-多因子-024 短周期因子挖掘 |
| 17×3 | research | l2/l1 | 广发-海量L2-002 长短单、长江-神经网络-003 集合竞价、长江-高频-013 广义拥挤度 |
| 16×5 | research | l2/minute/daily | 海通-085 主动成交、国金-002 量价背离、方正-005 波动率的波动率、方正-008 草木皆兵、海通-093 模型动物园 |
| 15 | handbook | mixed | 光大-多因子-002 因子测试全集 (2017) |
| 13 | handbook | mixed | 中金-量化多因子-012 高频因子手册 |
| 13 | handbook | daily | 华泰-多因子-004 动量类因子（华泰单因子测试系列 004~013 共 8 篇 daily）|

综述类（review 按规则 factors 为空，只有方法论）本频段值得读的三篇：智臾科技 高频行情低频化因子库使用教程 (high, 2026)、海通-069 高频因子的现实与幻想、兴业-高频-001 高频漫谈。

复现起点建议按数据层次分三批：daily 批 = 中金-007 + 华泰单因子系列 8 篇（日 K 即可）；minute 批 = 广发-048 + 长江-016 + 信达-001 + 中金-012（4 本手册 105 个因子，公式闭式，大多给了 RankIC 或 ICIR）；l2 批 = 广发海量 L2 三篇 + 海通-085/066 + 开源-029。

**基建缺口（对照上面手册里的公式）**

1. 没有日频层。`ALL_LEVELS` 只有 L0 秒 / L1 分钟；`at_day_start` 每天 reset 全部节点，TS 节点无跨日状态。中金-012 每个因子都是"日内算一个值 → 每期取 std/均值"，长江-016 的 波峰 是"局部峰值数量的 20 日均值"，中金-007 全是 1M/3M/6M 滚动。需要：(a) 日频存储 `[D][F][A]`；(b) `at_day_end` 钩子（现在是空的）把日内聚合值落到日频层；(c) 跨日滚动算子（mean/std/skew/kurt/corr/OLS over N 天）——建议在日频张量上算而不是塞进 TS DAG，400 资产 × 900 天全放内存也很小。
2. 日 K 没进特征流水线。`cn_stock_real_bar1d` 已有 open/high/low/close/pre_close/volume/amount/deal_number/turn/adjust_factor/upper_limit/lower_limit，但 `fund::Pool` 没接它。1460 个 daily 因子（隔夜收益、振幅、上影线、换手率 std、Amihud、corr(价,换手)、剔除涨跌停动量）全靠这张表。这批不需要 L2，是最便宜的一批。
3. 分钟级原语缺三样。`MinuteData` 有 ohlc + 买卖方 volume/amount，缺：成交笔数（长江 每笔成交/高量成交占比、开源 单笔金额/QUA/MTS，这一族 ICIR 都在 2 以上）；5 分钟 bar（长江-016 多个因子以 5 分钟为切分）；日内 OLS/相关（中金 mmt_ols_beta、corr_pvl，需要 240 点上的回归，Meta 里没有）。vwap、分钟收益率可由现有字段推出。
4. 逐笔级原语。大小单已有 (`ctr_xl/l/m/s`, `cnbi_*`)；缺：撤单率与挂单时长（开源 买入/卖出高频撤单率，ICIR 2.9，需 LOB 按订单号跟踪生命周期）；集合竞价段（长江-003、广发-004，`ctr_ca` 只给了成交占比）；按订单量的 均值+kσ 大单界定（广发-001 是每日动态阈值，非固定档）。
5. 标签。只有 5m/10m/30m，需要 T+1 开→T+n 开（n=1/5/20），并用 `cn_stock_status` / `cn_stock_limit_price` 屏蔽停牌与涨跌停不可交易样本。
6. 裁判不存在。RankIC 序列、ICIR、十分组、多空、换手。另外注意 tag 里 ICIR 口径混杂（中金手册是月度 IC 的 ICIR、开源 -029 明显是年化过的），对标时得按报告分别记口径。
7. CS 处理够用。`NeutralRank/WinsorRank/Z` 覆盖手册的"市值行业中性化"；长江-016 的 FF3 残差族需要市场/规模/价值三因子收益序列，暂缓。

**顺序建议**

日频层 + 日频标签 + 裁判 → 接 `cn_stock_real_bar1d` 进 pool → 跑 中金-007/华泰单因子 做校准 → 补 成交笔数/5min/日内 OLS → 广发-048 等 4 本 minute 手册 → 撤单/挂单时长 → l2 批。

一个需要你定的点：daily 批不依赖 L2，可以在全市场跑，和研报 setup 直接对齐；是否为它单开一个 daily-only 的 universe（只走 parquet 不走 orders）？这会影响日频层的设计是复用 `FeatureStore` 还是独立一套。

