# Inspect image adjustments
_Exported on 10/26/2026 at 21:24:38 GMT+8 from Cursor (3.27.12)_

---

**User**

@README 

1. inspect的图1的分层cutoff 太sharp, 中间的还是要稍微有点渐变的, 不能全灰
2. 图1右边的截面缩放有问题, 没有动态顶满, 应该根据ledgend和cursor的选取动态autozoom 
3. 图1~4都应该根据ledgend自动触发autozoom
4. 同样一个因子, 为什么在因子看板算的LS和inspect的图1不一样, 差得多?

---

**Cursor**

我先读一下 README 和 Inspect 相关代码。

先查清楚问题 4 的两个 LS 口径分别从哪来。

问题 4 的口径差异基本查清了。接下来确认 ImPlot 的隐藏图例项会不会参与 fit，以及 `GetPlot` 接口是否存在。

ImPlot 隐藏项不参与 fit，`GetPlot(title)` 也可用。看一下 `InspectUIState` 的 K 线字段。

确认完毕，开始改。先改分层色：在 tanh 台阶下面叠一条线性底坡，同时把增益调低。

Now `fit_on_change`: mix the legend's show/hide state into the key.

Now the right-hand cross-section in chart 1: compute the bar values first, then auto-zoom the x axis to them (follow the legend and cursor).

Now chart 2 (K-line): toggling the legend should also re-fit y, and when visible, the index lines should be included in the fit.

Replace the two hard-coded line names with the constants, and update the comments.

Header: add the state field and update the stale comments.

Questions 1–3 are fixed in `TabInspect.cpp` (plus `TabInspect.hpp`) with no lint errors; I haven't compiled it, so please build and check it yourself. Question 4 isn't a bug: the two numbers use different metrics. Details below.

**1. Layer colors.** The color intensity is now a linear ramp plus the existing tanh step. I lowered the step gain from 12 to 8 and added a ramp share `kColRamp = 0.3`. The two outer groups on each side are still near full color. Inward from there, intensity fades gradually: roughly 0.6 at G3, 0.3 at G4, 0.2 at G5, down to about 0.02 next to the center, so the middle groups are no longer all gray. If you want stronger or weaker transitions, tune `kColRamp` and `kColGain`. The K-line candles and the asset strip use the same color function, so they change too.

**2. Cross-section on the right of chart 1.** Its x-range used to copy the left chart's y-range. But the left chart plots cumulative %, while the bars are annualized % at the cursor date, so the bars never filled the panel. The x-range now follows the min and max of the bars currently shown (and 0), plus a margin. It re-zooms automatically when you toggle legend items or move the cursor, and you can still zoom manually in between.

**3. Legend-triggered auto-zoom.**
- **Left of chart 1:** it already re-fit the y-range when the legend changed, so no change.
- **Charts 3 and 4** (rIC/alpha distributions, term structure): the re-fit trigger now includes each legend item's show/hide state, so toggling the legend re-fits the y-axis once. Hidden items are excluded from the fit.
- **Chart 2** (K-line): toggling either index line now re-fits y to the current x window. A visible index line is also included in that fit, which reverses the old "index lines don't affect zoom" rule. Double-click fit still ignores them.

**4. Why the factor board's LS and the chart 1 legend differ.** They are different metrics:
- **Annualization:** the board's LS% is the per-period mean times periods per year, i.e. simple interest. The chart 1 legend compounds the NAV and annualizes geometrically: \((1+\text{cum})^{242/\text{days}}-1\). For the same returns, a 50% board figure is about \(e^{0.5}-1 \approx 65\%\) in the legend, minus a volatility drag.
- **Trade execution:** the board assumes every position opens and closes at the label price. Chart 1 uses the batched-position NAV engine: if it can't buy, that slice stays in cash; if it can't sell, the position is held longer and revalued. This only happens on the Bar tier; the Lob tier can always exit.
- **T+N holding periods** (the biggest gap): the board's labels trade on every row's signal, buying at that minute's price and selling at the open N days later. Chart 1 trades only on the last row each day, buying at that day's close and selling at the close N days later. The signals, buy prices and sell prices all differ.
- **Sharpe:** the board computes it per holding period; chart 1 computes it from daily returns.

The LS in the stat row at the top of the Inspect page uses the same formula as the board (`ls_annual`), so you can read both on that page.

Two comments are out of date and probably set the expectation that the numbers match: the `ls_annual` comment in `Stat/Contract.hpp` says it equals the endpoint of Inspect's cumulative curve, and `TabFactors.cpp` says it is the same as Inspect's legend.

> **How should I handle the LS mismatch in question 4?**
> - (Recommended) Show both in the chart 1 legend: geometric annualized plus simple-interest annualized (mean daily return × 242), and fix the outdated comments
> - Switch the chart 1 legend to simple-interest annualized only (same annualization as the board; the remaining gap is execution), and fix the outdated comments
> - Change no logic, only fix the outdated comments

> **Which holding period did you see the gap on? (This tells me which cause dominates.)**
> - Minute-based (e.g. 30m / 60m)
> - Hold to close (close)
> - T+N

---

**User**

用中文重新问

---

**Cursor**

> **第 4 点的 LS 不一致, 怎么处理?**
> - (推荐) 图1 图例两个都显示: 几何年化 + 单利年化 (日收益均值 × 242), 并修正过期注释
> - 图1 图例只改成单利年化 (年化方式与看板一致, 剩下的差距只来自成交模型), 并修正过期注释
> - 逻辑不动, 只修正过期注释

> **你是在哪种持有期上看到差距的? (用来判断哪个原因是主因)**
> - 分钟型 (如 30m / 60m)
> - 持有到收盘 (close)
> - T+N

---

**User**

分钟级的lob, 两边年化基本差不多, 但是夏普算的不一样
比如ts_vwap_dev_60m #2这个因子, 
看板是: LS+16.48%, SR+2.71
inspect是: LS +17.89%, SR 7.79

但是日级别的差的就离谱了
all:day: amp_day #0 的因子,
看板是: LS+163.9%, SR+19.34
inspect是: LS -59.34%, SR -3.33


---

**Cursor**

Let me first confirm which execution model Inspect uses for day-level T+N.

> **日级别 T+N: 两边的卖出价怎么统一?**
> - Inspect 改成卖在 O(D+N), 跟标签和看板一致 (改净值引擎 Day 型, Portfolio 也会受影响)
> - 标签改成卖在 C(D+N), 看板改测收盘到收盘 (改特征标签, 要全量重算)
> - 不统一, 只在 UI 上标清楚两边的口径 (看板 = 隔夜, Inspect = 收盘到收盘)

> **分钟级夏普的口径差异怎么处理?**
> - 不改逻辑, 只修正过期注释 (现在注释写着看板 LS/SR 和 Inspect 图例同一份, 已经不对了)
> - 看板 SR 也改成按日聚合 (Stat 累加器要加按日的累计, CPU/GPU 契约都得改, 改动大)
> - Inspect 图例同时显示两种 SR (逐期估计 / 实际日收益)

---

**User**

你先别问我, 你先解释区别怎么造成的, 再问我

---

**Cursor**

## 日级别 `amp_day`：看板 +164% / SR 19，Inspect −59% / SR −3.3

两边的买入价相同，卖出价不同，所以测的是两段不同的收益。

**看板（Stat 标签）**
- 日层的第 D 行 = D 日收盘后的状态。T+N 标签 = 在 C(D) 买、在 **O(D+N)** 卖，见 `FactorsBook.hpp`【成交价】表 "T+N 标签" 那一行。
- 以 T+1 为例，每行收益 = O(D+1) / C(D) − 1，也就是**纯隔夜收益**。
- LS% = 每天 (G20 超额 + G1 做空超额) 的均值 × 242，单利年化。

**Inspect 图1（净值引擎 Day 型，`PxFill`）**
- 每天末行开一批，用 1/N 的净值，在后复权收盘价 C(D) 买、在 **C(D+N)** 卖（`FactorsEngine.hpp:430`，【成交价】表 "T+N 换仓" 那一行）。
- 以 T+1 为例，每批收益 = C(D+1) / C(D) − 1 = **隔夜段 × 次日日内段**。
- 另外它真按价格链成交：收盘买不进（比如涨停）那份钱就空着，卖不出（比如跌停）就卡住、继续按估值价重估。高振幅股票碰到涨跌停的比例高，这部分也会拉开差距，但是次要的。

所以 Inspect ≈ 看板（隔夜段）+ 同一篮子的次日日内段。

粗算一下：看板约 +1.64/年（单利），Inspect 几何 −59% 换成对数约 −0.89/年。差出来约 −2.5/年，就是日内段（还要扣掉一点波动拖累），平均每天两腿合计约 −1%。

隔夜段又稳又正，所以看板 SR 很高。日内段反向而且波动大，叠加以后整体变负。这正是 "高振幅股隔夜涨、日内回吐" 的形态。

**需要你核实的一点**：`amp_day` 用的是 D 日全天的高低价，两边都按 D 日收盘竞价价 C(D) 买入。如果高低点可能是在收盘竞价里才定的，就有轻微前视。看板 SR 19 偏离谱，和这一点可能有关。

## 分钟级 Lob `ts_vwap_dev_60m #2`：LS 接近，SR 2.71 对 7.79

设第 t 行开的那批在 h 期内的 LS 收益为 \(x_t\)，每天有 S 行。

- **看板**：\(\text{SR} = \dfrac{\text{mean}(x)}{\text{sd}(x)}\sqrt{242\,S/h}\)。这等于假设一年里那 \(242S/h\) 个不重叠的持有期**彼此独立**，年方差 = 期数 × 单期方差。
- **Inspect**：每行开 1/h 的一批，日收益 \(r_d \approx \frac1h \sum_{t \in d} x_t\)，\(\text{SR} = \dfrac{\text{mean}(r)}{\text{sd}(r)}\sqrt{242}\)。

**均值两边一致**：\(\text{mean}(r) \cdot 242 \approx \text{mean}(x) \cdot 242\,S/h\)，就是看板的 LS%。所以 16.5% 和 17.9% 很接近，剩下的差额来自几何复利和尾部行不开仓。

**方差只有在独立假设下才一致**：相邻行的批次重叠 h 期，代入可得 \(\text{var}(r) \approx (S/h)\,\text{var}(x)\)，年化以后和看板一样。

实际 Inspect 的 SR 是看板的约 2.9 倍，说明实际日波动只有独立假设下的约 \(1/8\)。也就是说，**同一天里不重叠的 60 分钟段之间是负相关的**：这一小时赚到的截面超额，下一小时会回吐一部分，日内聚合后大量相互抵消。这是从公式推出来的结论，我没有拿数据验证过。

还有一个次要因素：看板对不 ok 的行（60m 的话，每天最后 60 分钟不开仓）按 0 补满再算 SR，会让看板 SR 再低大约 \(\sqrt{n/n_{all}}\) 倍，量级约 0.87。

结论是：Inspect 那个是净值按日实际跑出来的夏普；看板是按 "各期独立" 外推的估计，在期间负相关时偏保守。两边的计算都没有错。

## 需要你定的两件事

1. **日级 T+N 两边卖出价是否统一**，三种做法：
   - Inspect 引擎改成在 O(D+N) 卖，向标签看齐。改的是 FactorsBook 的 Day 型，Portfolio 也会跟着变。
   - 标签改成在 C(D+N) 卖，向引擎看齐。要改特征标签并全量重算。
   - 不统一，只在 UI 上标清两边的口径：看板 = 隔夜，Inspect = 收盘到收盘。
2. **分钟级夏普**，三种做法：
   - 只修正过期注释。现在 `Stat/Contract.hpp` 和 `TabFactors.cpp` 的注释写着两边是同一份，已经不对了。
   - 看板 SR 也改成按日聚合。Stat 累加器的契约（CPU 和 GPU 都要改）改动很大。
   - Inspect 图例同时显示两种 SR。

---

**User**

1. 两边的时间级别应该写更清楚, UI那里应该买入卖出都写清楚
因子看板和inspect应该写: 
级别(选择), 
然后hold切换改成写清楚(比如):    开仓: C(min5) @ 什么价格算法, 清仓: O(T+1) @什么价格算法
对于因子评估和inspect, 我们应该有如下几个hold period:  1m, 5m, 15m, 30m, 60m, close, T+1O, T+1C, T+2C, T+3C 这几个级别, 才能满足要求, 每个级别有对应的开仓, 清仓写法, 如果级别还有少的, 没定义全的, 你告诉我


只有这样完全写清楚, 才能统一, 这个首先让两边都支持这些统一的口径, 不再有歧义
2. 如果给看板(run/search程序也加上判断是不是开仓, 是不是计算会慢很多)

先回答, 再做1

---

**Cursor**

先把现有的持有期 / 标签 / 成交价链路查清楚，再回答。

下面先回答第 2 个问题，再把第 1 点的完整口径表和缺口列出来。第 1 点牵涉特征标签、Stat、净值引擎、换手成本、Search 和 UI，改完后两档特征都要全量重算，所以动手前需要你确认几处口径。

## 问题 2：Run / Search 加上"能不能开仓、能不能平仓"的判断，会不会慢很多？

**开仓判断现在已经有了。** Stat 的标签有效位要求建仓那一行买卖两个方向都可成交（`FactorsLoad.cpp:626`，`kTrBoth`），买不进的格子不进统计。不过这只在 Bar 档生效；Lob 档没有可成交列，一律按可成交处理。

**缺的是平仓卖不出、仓位被卡住。** 净值引擎的做法是卡住就延长持有，之后能卖就卖，并且要看下一批是否仍持有它（仍持有就交接，不用卖）。这依赖路径：必须对每个资产沿时间顺序扫描，还和因子有关。Stat 和 Search 的架构是逐行独立：GPU 上一个 warp 算一行，Search 是一批候选沿时间分块直接攒累加器，不落一级 Row。

- **Run 照搬引擎**：每个因子 × 每个持有期，对 2 个腿篮子顺序扫一遍，量级和 Inspect 算净值差不多。会变慢但能接受。GPU 路径要把秩平面传回 CPU。
- **Search 照搬引擎**：每个候选都要扫一遍，慢好几倍，而且要推翻"不落 Row"的设计。这是定性估计，我没有实测。
- **更省的办法（推荐）**：把"卖不出就顺延到下一个可成交时刻的价格"直接写进标签。这和因子无关，算特征时一次算好，Run 和 Search 的速度完全不变。和引擎相比，只在"卡住的仓本来也会被交接"这种情况下有出入，影响很小。

## 第 1 点：统一后的持有期口径

价格算法：
- **E**（开仓价）：
  - Lob 档 = 行末 + 3 秒的可成交盘口中间价（记作 `mid+3s`）；
  - Bar 档 = 行末之后首个有成交的连续竞价分钟的 VWAP（记作 `VWAP(次分钟)`）；
  - 行末已过 14:57 时，改为按收盘价开仓。
- **C(D)** = 收盘竞价撮合价。
- **O(D)** = 09:25 开盘竞价撮合价。

| 持有期 | 适用层 | 开仓 | 清仓 |
|---|---|---|---|
| 1m | 仅 Lob min | C(min1) @ mid+3s | C(min1)+1m @ mid+3s |
| 5m / 15m / 30m / 60m | 行长 ≤ n 的分钟层 | C(本层) @ E | C(本层)+n @ E（同一价格算法）；越过 14:57 的行不开仓 |
| close | 非 day 层 | C(本层) @ E | C(D) @ 收盘竞价 |
| T+1O | 所有层 | C(本层) @ E；day 层 = C(D) @ 收盘竞价 | O(T+1) @ 开盘竞价 |
| T+1C / T+2C / T+3C | 所有层 | 同上 | C(T+N) @ 收盘竞价 |

UI 上，看板和 Inspect 都显示"级别"，持有期切换显示成 `开仓 C(min5) @ VWAP(次分钟) → 清仓 O(T+1) @ 开盘竞价` 这种格式。不适用的组合灰掉。

## 少的、没定义全的

1. **T+1C / T+2C / T+3C 的标签不存在。** 现在日级只有 t1、t3、t5，都是开盘平仓。要新增"收盘平仓"这一类，两档都得全量重算。t3、t5（开盘平仓）按你的列表删掉，已有因子文件里 hold 为 t3、t5 的权重和统计会失效。
2. **T+NC 平仓日停牌怎么办，没定义。** 我建议照 T+NO 的现有规则：顺延到之后首个有成交日的收盘，最多再等 N 日，仍无成交则取最近收盘价。
3. **净值引擎（Inspect / Portfolio）的日级持有期和标签对不上。** 现在不管哪一层，都只在每天末行开仓，按收盘价链 C(D)→C(D+N) 结算。要统一，引擎也得每行开一批，权重 1/持有行数，按标签收益结算。
   - 交接（留仓不卖）只在"平仓时刻 = 下一批开仓时刻"时才成立：分钟型成立；day 层 T+NC 成立；T+1O 不成立，每天开盘全部卖出。
   - O(T+1) 能否卖出要用开盘可成交列 `can_sell_open` / `can_buy_open`。特征里已经有，但引擎还没装载。
4. **换手成本里 T+NO 的"前一批"口径不对。** 现在按 t−h 行算交接（留仓不付成本）。但 O(T+1) 卖出到次日再买，并不是同一时刻，实际上应该全部算新进者。这个和第 3 条同一个判据。
5. **Lob 档没有可成交位。** 封板时中间价取涨跌停价，照样开仓，没有"买不进 / 卖不出"的判断。
6. **Search 的窗口要重定。** 现在是 30m 到 T+1 开盘，新口径下搜哪几个持有期需要你定。
7. **天然不存在的组合（不算缺口）**：1m 只在 Lob min 层有（Bar 档不落 L_1min 标签）；close 不在 day 层；day 层只有 T+1O 和 T+NC。

## 需要你确认

1. 是否删掉 t3、t5（开盘平仓），新增 T+1C、T+2C、T+3C（会全量重算特征）？
2. T+NC 平仓日停牌，是否按第 2 条建议的顺延规则？
3. 净值引擎是否改成每行开仓、按标签结算、"同一时刻才交接"？Portfolio 的日级线也会跟着变。
4. 换手成本是否按同一个交接判据，让 T+1O 每天全部算新进者？
5. "卖不出就顺延"是否写进标签（问题 2 推荐的做法）？还是这次先不做？
6. Search 搜哪几个持有期？
7. 我建议分步做：先改标签和持有期键（要重算），再改 Stat 和引擎，最后改 UI 显示。可以吗？

---

**User**

run, search, inspect, protfolio应该完全共用同一套引擎
1. 是的
2. 是的, 日级别顺延和分钟级别顺延处理是一样的
3. 日级别净值引擎, 用的配置其实固定是 买入用C(T), 卖出C(T+1), 也就是 T+1C的周期来评估的, 注意OPEN/CLOSE都要用复权价格, 缺了就加特征
其他级别也要和因子看板的配置完全对齐
4. 换手规则简化, 我们不再每天开1/n的仓位, 太过麻烦, 就是用普通的简单的无限细粒度换手, 比如时刻B的预期仓位-时刻A的预期仓位就是要调整的仓位, 这样非常简单稳定, 哪怕很多标的只调0.001的钱也先这么算, 统一改特征, 统一考虑到可交易性和顺延, 但是这个要在特征层面直接解决, 不能即兴算, run, search需要极端极致的实现,  
label特征里直接判断好tradabability(中间特征), 但是注意我们引擎只用label, 不再去读tradability, label里已经有了tradability, 这样才能极限实现
还有tradability是否需要开盘可卖/可买? 我理解只要普通可卖可买就好了, 开盘的tradability可以在label里即兴算, 反正计算量很小, 一天只有一两个数据点要算, 不需要独立特征
我们run, search, inspect, protfolio里需要计算冲击成本的很少, 真的需要算冲击的时候算上就好
5. 如上
6. search都是给定一个周期去做search的, 不需要我们判断, 上层会判断
7. 你觉得合理不? 这样是不是才能真正做统一, 做干净, 开始

---

**Cursor**

我认为这个方向合理。四个程序共用一套引擎、可交易性和顺延全部在标签里解决、换手按 \(\Delta w\) 算，这才真正统一得干净，而且 Run / Search 的速度不受影响。

但有几处业务口径你的描述可以有两种理解，而且分别落在特征、Stat 契约、GPU 核里。猜错就得全量重算加返工，所以先把我要实现的规格写清楚，你确认（或改）后我从阶段 1 开始。

## 统一引擎

现有 Stat 的数学稍作调整就是这套引擎：Run / Search 攒二级统计，Inspect / Portfolio 在同一份逐行结果上按日折叠成净值。FactorsBook 的分批开仓、卡住延长、价格链估值全部删掉。

**持仓**：每行 t 的信号篮子 \(s_t\) 沿用现有的持仓口径，每只的权重为 \(1/\max(n, \text{cap})\)。

**换手**：调整量是相邻两行目标仓位之差，即 \(\Delta w = w_B - w_A\)。

**持有期 h 跨多行时（如 1min 层持有 30m），目标仓位 \(w_t\) 怎么定，有两种理解：**
- **(A) 重叠，推荐**：\(w_t = \frac{1}{h}\sum_{k<h} s_{t-k}\)。
  - 每行都用上，与起点相位无关。
  - 换手 \(\Delta w_t = \frac{1}{h}(s_t - s_{t-h})\)，和现在的"新进者"成本是同一回事。
  - 年化等于"每期 LS 均值 × 每年期数"，看板和净值的年化方式自然一致。
  - 这只是权重，没有分批记账。
- **(B) 不重叠**：只在每 h 行的那一行换仓，\(w = s_{kh}\)。
  - 实现更直观，但只用了 1/h 的样本，结果受起点相位影响。
  - close / T+1O 在日内层上"哪一行换仓"也没有定义。

**净值**：逐行收益在平仓那一行实现，按日求和后复利成净值。

## 标签：可交易性和顺延写进标签，引擎只读标签

**开仓判断：**
- **买不进就作废，推荐**：开仓那一刻两个方向都要可成交，否则这一格标签作废（NaN），不进截面，也就是不建仓。这和现在 Stat 的口径一致。
- 另一种做法是开仓也顺延。但顺延开仓等于换了一笔交易，我不推荐。

**平仓顺延**：卖不出（做空腿是买不回）就顺延到下一个可成交时刻的价格。分钟级和日级同一规则，可以跨日。顺延最多等多少个交易日需要你定：超过上限就按最近收盘价结算，而且这个上限决定悬挂日环的长度。

**可成交的判据：**

| 档 | 时刻 | 可买 | 可卖 |
|---|---|---|---|
| Bar | 分钟 | low < 涨停价 | high > 跌停价 |
| Lob | 盘口 | 卖盘非空 | 买盘非空 |
| 两档 | 开盘价 O | O < 涨停价 | O > 跌停价 |
| 两档 | 收盘价 C | C < 涨停价 | C > 跌停价 |

- Bar 分钟的判据和现在的 `Tradable` 节点相同；Lob 现在封板时取涨跌停价当中间价，照样能开仓，改完就不行了。
- 开盘和收盘的判断在标签里现算，每天只有一两个点。

**持有期**：1m、5m、15m、30m、60m、close、T+1O、T+1C、T+2C、T+3C。新增"N 日后收盘平仓"这一类，删掉 t3、t5（开盘平仓）。

**复权**：标签里的价格都已乘当日复权比，跨日收益不含除权缺口，引擎不再需要单独的复权价特征。

**删掉的特征**：`Tradable` 的 `can_buy_open` / `can_sell_open` 两列删掉。`can_buy` / `can_sell` 留着，可以作因子输入，但引擎不读。

**冲击成本**：只在要算净线时扣，按 \(|\Delta w|\) 乘冲击。

## 还要你定的几件小事

1. **池等权指数**（Portfolio 基准线和 Inspect K 线叠加）：推荐也走同一引擎，用"池内全体成员"篮子和同一个标签。日层就是 C→C 等权，FactorsBook 可以整个删掉。
2. **已有因子文件里 hold 为 t3、t5 的权重和 stat**：推荐扫描时标记失效、不删文件，由你手动处理。也可以直接从文件里删掉。
3. **Search 的"给定周期"**：我理解就是看板上当前选中的 Hold，不再按 30m 到 T+1 的窗口逐个持有期搜。对吗？

## 分阶段

每个阶段结束都能编译：
1. **特征标签**：新的持有期组、"N 日后收盘平仓"类型、可交易性和顺延、开仓作废，标签版本号 +1。这一步要全量重算。
2. **Stat 和 Search**：持有期键的编码和名字、换手按 \(\Delta w\)、Search 只搜给定周期。
3. **引擎**：Inspect 分层和尾部、Portfolio、池指数改走 Stat 逐行结果；删掉 FactorsBook。
4. **UI**：看板和 Inspect 显示"级别"，持有期一律写成 `开仓 C(min5) @ VWAP(次分钟) → 清仓 O(T+1) @ 开盘竞价` 这种格式。

请确认：
- 目标仓位用 A 还是 B；
- 开仓买不进是"作废"还是"顺延"；
- 顺延最多等几天；
- 上面三件小事怎么定。

如果回"全按推荐"，顺延上限就默认 5 个交易日，我直接从阶段 1 开始。

---

**User**

1. 用A
2. 作废
3. 可以不设上限, 反正净值还在, 哪个好算用哪个
全按推荐

这次你真的要把run/search/看板/inspect, protfolio 完全用统一的引擎处理, 不要有重复代码


---

**Cursor**

开始做阶段 1（特征标签）。先通读标签在 `CoreTs` / `CoreBar` / `CoreLob` 里的驱动方式，以及 `Tradable` 节点。

先确认几个底层符号（`kPxEps`、`kNaN`、`TIER_DAG`、`MinuteData` 字段）。

Phase 1 starts now. First, the hold-period groups.

Now let me look at both tiers' current usage of this skeleton (`Entry`, `emit_min`, `close_day`) so the interface fits on both sides.

Now writing the new class body (`LabelPx` price point + non-template `LabelHold`).

The `daily()` call inside in-class constexpr initializers won't compile, because the class is still incomplete at that point. I'm moving the predicate into `LabelGroups.hpp`.

Now `LabelBar`: the per-minute buy/sell flags come from the `Tradable` node, and open/close tradability is computed by `auction_px` against the limit-up/limit-down prices.

Updating the `LabelBar` file-header comment and the column description macros.

Now `Tradable`: removing the `*_open` outputs.

Now updating the `LabelLob` header comment and column description macros.

Next I'll check whether anything downstream (field-type validation, or the label-column comments in `FeaturesDefine`) assumes label/cost columns are never NaN.

Lint is clean (header-only lint may not cover much). I'll re-read `LabelHold` from top to bottom to check the logic.

Phase 1 code is done. Before phase 2, I'll check whether any Python or other code depends on the old label names (`t1`/`t3`/`t5`).

Phase 1 is done. Starting phase 2 (Stat contract / Search); first I'll read through `Stat/Contract.hpp`.

Phase 2 touches the cost formula. Before editing, I'll check the transcript for the exact turnover-cost and netting rules I proposed earlier and the user confirmed, so I don't misremember them.

The surface is large (Stat CPU/GPU backends, Search, load layer, engine, three tabs). I'll read the Stat backends' cost and row loops first.

Costs are computed only on the host (`cost_row`); the GPU side only returns leg codes, so the Δw change is confined to the CPU path. Next I'll check where `CostAcc` / `turn` / `entry` are consumed.

The cost path is CPU-only (`cost_rows` → `CostAcc`, persisted via `StatJson`, consumed by Inspect's net lines). Now reading the Stat self-check, Search contract, and `StatJson`.

Changing the `CostLeg` fields changes `acc_cols`, so old files' cost sections will fail the header check and be recomputed. Next I'll check how factor-file stats are invalidated when labels change.

Now I'll look at how `FactorsLoad` builds the cost inputs (around `cost_tiers`).

I'm starting the phase 2 `Stat` contract edits: hold-period key encoding and names.

Now replacing the `CostLeg`/`CostAcc` fields with Δw buy/sell volumes.

Now `Host.hpp`: rewriting `cost_row` to compute Δw, with leg membership = leg code ∧ label valid bit.

Now Search: removing the hold-period window so it only searches the hold period it is given. First, where it's used:

Search will now take a single hold period from the caller. Next I'll find the hold the Factors board currently has selected and how `WriteFactorWeights` replaces weights.

Changing Search to the given hold: `SearchSpec` gains a `hold` field, and on write-back only weights for that hold are replaced (weights for other holds are kept). First, the two call sites:

Next, factor-file weights whose hold is no longer among the label groups get marked invalid (an error row; the file is not changed):

Now passing the board's current Hold at the two `MakeSearchRequest` call sites in TaskFactors:

Now removing the old search window from Search/Contract.hpp:

Now the TabFactors UI: set `search_hold` when Search is clicked, and update the tooltips.

Signature matches. Next: switch `cost_tiers` over to `labels_of(y)` and change the cost assert to "finite where the label is valid".

Now the cost keys in StatJson:

Next, I'm finding remaining usages of the old `CostLeg` fields (`e` / `wimp` / `we`) and of `CostAcc::cost` / `turn` / `entry`:

Inspect still uses `cost` / `turn` / `entry`, and their interfaces are unchanged. Next I'll check whether the display text there still matches the new meanings:

Updating the Inspect text to the Δw meaning (increased position, turnover = increase as a share of holdings):

The Phase 2 Stat and Search parts are done. Before Phase 3, I'll check that the hold-key functions in Contract and their users (`hold_ok`, the GPU path) agree:

Phase 2 is done. Updating the todos, then starting Phase 3. First I'm reading what currently depends on FactorsBook (Inspect, Portfolio, pool index).

FactorsBook is a full net-value engine with stuck positions, PxFill, and pool indexes. Before deleting it I need to see exactly how Inspect and Portfolio use it.

Before changing anything for Phase 3, I'm re-reading the `cost_row` I wrote in Host.hpp so the Portfolio per-row costs can reuse it directly.

Phase 2 is done; before starting Phase 3 there is one business decision I need you to make.

**Phase 2 changes:**
- **Stat cost:** `cost_rows` takes `Labels`, uses Δw for turnover, and only offsets positions when the two batches trade at the same price point. `lc_void` is removed. FactorsLoad's cost assert now checks "finite in cells where the label is valid".
- **Stat file columns:** the cost columns are now `imp,buy,sell,w,n`. Because `acc_cols` changed, existing stats are invalidated and recomputed on the next Run.
- **Stat check:** the hold periods are switched to the 10 new ones.
- **Search:**
  - It only searches the board's current Hold, which is captured when you click Search; Search all uses the same Hold for the whole run.
  - Write-back only replaces weights for that Hold.
  - `hold_in_window`, `kHoldMinMinutes` and `kHoldMaxOpen` are removed.
- **Factor files:** a weight whose hold is not among the current layer's hold periods becomes an error row ("已废的持有期, 作废"). The file itself is not touched.
- **Inspect cost tooltip:** reworded to the Δw meaning.

Nothing has been compiled yet.

**Phase 3 plan (single engine):**
- **Inspect layers, tails and LS:** take Stat's per-row `Row` / `TailRow` directly. For each row, the absolute return is `grp + cnt·mkt / book_den`, the excess is `grp`, and LS is `Row::ls`.
  - Each row's value is divided by h and booked on its exit row (exit day), summed by day, then compounded.
- **Portfolio G20:** same per-row values. Costs come from a per-row output of the same `cost_row` (Δw / h). Exposure is the sum of open batches / h.
- **Portfolio pool baskets (day layer):**
  - Each row's basket return is the weighted average of the T+1C label over pool members whose label is valid, using equal weights or market-cap weights. It goes through the same fold.
  - On the day layer this is C→C with adjusted prices, with tradability and 顺延 already in the label.
  - Valuations and turnover stay as they are.
- **FactorsBook:** deleted as a whole, together with the `trade` / `px` planes, `kTrBoth` and `book`.

**The open question is Inspect's K-line overlay of the pool equal-weight index on minute layers.** Built from labels, a minute-layer pool index loses the overnight gap and the last few minutes of each day (tail rows have no label). It would then be a "pool basket strategy net value", not a price index. The day layer has no such problem.

**阶段 3: Inspect K 线叠加的池指数**

> **Inspect 右上 K 线叠加的"池等权指数"在分钟层怎么算?**
> - (推荐) 走同一引擎, 按当前 Hold 的标签: 池内全体等权篮子的净值 (与因子同执行口径, 每个 Hold 一条; 分钟层不含隔夜) ✔
> - 保留价格指数: 池内等权 close_adj 逐行比值 (只是显示用, 不算引擎, 含隔夜; 不再有卡住逻辑)
> - 删掉 K 线上的池指数叠加 (只留滚动重锚线)

> **Portfolio 在 Bar 层 (没有冲击金额段) 的成本怎么记?**
> - (推荐) 增仓 Δw⁺ 和平仓量各付一份 fixed_impact (同旧版) ✔
> - 只平仓量付 fixed_impact (与 Lob 层同式, 增仓冲击记 0)

Both recommended options are confirmed. Starting Phase 3 with the contract-layer helper: the exit row of each batch, which every engine will place its returns by.

Next, `cost_row` / `cost_rows` gain two things: C = 0 (only turnover amounts, no impact, used by Portfolio on the Bar layer), and per-row output (Portfolio needs per-row costs).

Next, `cost_rows`: allow C = 0, and add an optional per-row output `land`.

The design is set; now the FactorsLoad changes. First, other users of the tradable columns (`ft.y.trade` / `has_trade`):

Removing the tradable columns from the field table (the engine reads only labels):

Next, FactorsLoad.hpp: removing the FactorsBook dependency, and moving `BookLines` / `compound` here together with the new `fold_exit`.

Next, FactorsLoad.cpp. First the include, `YSpec::key`, `cell_bytes`, and replacing `PoolIntent` / `pool_index` with the label-based engine:

Next, the load_y body: removing the tradable / px / book paths and loading close and price only for Full.

Checking the remaining references in FactorsLoad.cpp:

Merging the two adjacent `if (full)` blocks:

FactorsLoad is done. Next, deleting `BookPlan` / `RankBook` from FactorsEngine.hpp:

Now on to InspectService. Reading it in full:

Checking the `Row` fields and the 【净值】 doc in Contract.hpp before writing the nav:

Now rewriting the InspectService.cpp nav: the basket/line tables are deleted, and nav is computed from Row / TailRow via `fold_exit`.

Next, the InspectService.hpp comments and the type of `pool_index`:

Inspect is done. Next, PortfolioService:

Before rewriting the Portfolio curve, I'll check the `accum_rows` / sink / `cost_rows` / `CostAcc` interfaces:

Next, how TabPortfolio consumes these curve lines:

I have everything I need for Portfolio. Rewriting the factor curve: one hold's labels go through `accum_rows` to get G20 Rows and `cost_rows` to get per-row turnover, then `fold_exit`.

I'll replace the `using namespace` with explicit qualification to avoid name clashes:

Now the PortfolioService.hpp comments:

Now TabPortfolio: `inc_of` no longer depends on `book_kind`.

Next, TabInspect:

`section_at` is defined after `derive`, so it needs a forward declaration. Checking which namespace it's in:

Deleting FactorsBook.hpp and checking for leftover references across the codebase:

FactorsBook has no leftover references (the remaining `.px` hits are unrelated label nodes). Next, confirming that other `YSpec` constructions no longer pass the `book` argument:

Every `YSpec` construction now matches. Re-checking the `fold_exit` implementation:

`fold_exit` checks out. Confirming the `PassSpec` field order, since I changed Portfolio's per-cell extra to 1.0:

The order is correct. Last, sweeping the touched files for stale comments mentioning "可成交 / book / 账本":

Running lints on the files I changed:

第 3 阶段（统一引擎）的代码已经改完，还没编译，需要你来编。现在 Inspect 分层/尾部/LS、Portfolio 因子线、池篮子指数都走同一条路径：先用 Stat 逐行结果（Row / TailRow / 逐行换手），再进 `FactorsLoad.hpp` 里的 `fold_exit`，最后 `compound` 复利。`FactorsBook.hpp` 已删，全仓已经没有引用。

主要改动：
- **装载（`FactorsLoad`）**：去掉了 `YSpec::book`、`YPlanes::trade` / `px`、`book_market`，也不再读可成交列。标签有效位现在只看"在池且两腿有限"。
  - Pool 档池指数改为每个持有期一条，用该持有期标签做等权篮子。
  - Full 档（日层）只用 T+1C 标签，日层没有 T+1C 会断言。
- **Inspect**：`nav` 直接用 Row / TailRow 现成的值折叠到平仓行。`pool_index` 改成每个持有期一条，K 线叠加的是当前 Hold 那条。顶底 LS 图例的年化和 SR 改取 HoldStat，与看板同一份；尾部 LS 没有 HoldStat，仍用净值算。
- **Portfolio**：用本 hold 的单组标签跑 `accum_rows` 和带逐行输出的 `cost_rows`。
  - pnl：G20 绝对收益记到平仓行。
  - 成本：imp = 建仓冲击 / h；fix = 平仓量 / h，Bar 层再加增仓量 / h。
  - expo：在持各批仓位之和 / h。
- **`FactorsEngine.hpp`**：删掉了 `BookPlan` / `RankBook`。

有三处取舍你可能需要知道：
1. Portfolio 的成本都记在建仓行。不交接的那批平仓量（收盘型、T+N）也算在建仓行，所以日层 T+1C 的固定冲击会早一天落账。
2. 日层的增量统一画在当日收盘点。T+1O 实际在开盘实现，图上也画在收盘。
3. 单因子仓位不再会超过 100%，因为顺延已经在标签里，不再延长持仓。相关提示文字已同步改了。

接下来是第 4 阶段：UI 显示"级别"，持有期写成"开仓 C(层) @ 价格算法，清仓 … @ 价格算法"。要不要等你编译通过再开始？

---

**User**

@/home/chuyin/.cursor/projects/home-chuyin-work-stk/terminals/6.txt:393-487 

fix完开始剩下的

---

**Cursor**

编译已经全部通过，失败的只是链接器 `ld.lld` 自己启动不了，因为找不到 `libxml2.so.2`，属于环境问题。先查一下系统里的 libxml2：

`libxml2.so.16` 里的符号大都不带版本（Base），能不能加载直接试一下最可靠。先在临时目录里试，不改任何东西：

链接器已修好，接下来做第 4 阶段的 UI。先查清楚两件事：各层标签开仓、清仓实际用的价格算法，以及 UI 里现在怎么显示级别和 Hold。

`LABEL_DELAY_SECONDS` 是 LabelLob.hpp 里的常量，只在 TabFactors.cpp 里 include 复用，不另写死。再看一下 `cycler` 的实现，长文本可能会撑开宽度：

Now adding the shared `hold_desc` helper, which both the board and Inspect will use:

LabelLob.hpp 能否单独 include 要确认一下，它可能依赖调用方先 include 的类型：

Self-contained, OK. Next, the board (TabFactors) shows the level and uses `hold_desc` for Hold:

Next, the Inspect control row: add the level and use `hold_desc` for Hold:

Last, the old stale comments from my notes (FactorsService.cpp around line 516, TabFactors around line 385):

链接器问题修好了，第 4 阶段的 UI 也改完了，需要你重新跑 `run.py` 编译验证。

**链接失败的原因**：编译其实全部通过，是 `ld.lld` 自己启动不了。系统升级后 libxml2 从 `libxml2.so.2` 换成了 `libxml2.so.16`，而预编译的 LLVM 22 的 lld 还要旧名字。我在 LLVM 自己的 `lib` 目录里加了一个兼容链接 `LLVM-22.1.5-Linux-X64/lib/libxml2.so.2 → libxml2.so.16`，系统目录没动。实测 `ld.lld --version` 能正常运行。之后每次链接都会多打一行 "no version information available" 警告，可以忽略；lld 链 ELF 时用不到 libxml2，所以不影响结果。

**第 4 阶段的 UI 改动**：
- **级别**：看板显示成 "池 级别 5min"，Inspect 的控件行最前面加了 "级别 5min"。
- **Hold 切换**：看板的下拉框和 Inspect 的切换控件都改用共用的 `hold_desc()`（在 `TabFactors.cpp`），写法例如：
  - 5min 层，30m：`开仓: C(5min) @ 次分钟VWAP, 清仓: C(5min)+30m @ 次分钟VWAP`
  - min 层，T+1O：`开仓: C(min) @ +3s盘口中间价, 清仓: O(T+1) @ 开盘撮合价`
  - day 层，T+1C：`开仓: C(T) @ 收盘撮合价, 清仓: C(T+1) @ 收盘撮合价`
- **价格口径**：按标签特征的实际实现取。min 层（Lob）用锚点加下单延迟后的盘口中间价，延迟复用 `LABEL_DELAY_SECONDS`，没有另写常数；5min 到 60min 层（Bar）用之后首个有成交连续竞价分钟的 VWAP；收盘和开盘用集合竞价撮合价。
- **提示文字**：两处 Hold 的提示里补了价格定义，以及"建仓作废、平仓顺延"的规则。
- **旧注释**：顺手把几处 `t<N>`、`t5` 这类旧持有期写法改成了新的。

Portfolio 页的表格仍然用短名（如 `T+1C`），没有换成完整写法。

---

**User**

更新README
1. 还有我们总结的两个表, 一个周期对应开关仓和价格的, 一个开关仓策略的, 要注释在一个重要header的表头作为协议
2. 我让你在UI展示的开关仓具体写法你做干净了吗

---

**Cursor**

先从之前的对话记录里找出我们总结过的那两个表的原文：

Next, the Bar-tier per-minute tradable criterion (the `Tradable` node):

表 1 的列没对齐，重排一下：

接下来让 `hold_desc` 的注释指向协议表，两页 Hold 提示里的规则说明只留一份：

Next, the Portfolio page's places that show a hold (the entry list, table column, and tooltips):

Now README: deleting the FactorsBook entry and updating the descriptions of the loading layer, Inspect, Portfolio, Search, and the Stat contract:

三件事都改好了，没编译（按规矩留给你跑）。

**1. 两张协议表**：已经作为表头注释写进 `cpp/include/factor/Stat/Contract.hpp`。
- 【协议表 1】从持有期查开仓、清仓和价格，列有显示名、列名 token、键、适用层、开仓、清仓、h。表前先给出价点定义：E 分 Lob / Bar 两种，C(D) 是收盘撮合价，O(D) 是开盘撮合价。
- 【协议表 2】是开关仓策略，包括可成交判据、建仓作废、平仓顺延（上限 8 个交易日）、尾部、仓位 1/h、换手 Δw、成本、净值、复权。

**2. UI 开关仓写法现在统一了**：
- **一份写法**：看板、Inspect、Portfolio 里凡是切换或悬停持有期的地方，都调同一个 `hold_desc`，写成「开仓: C(层) @ 价, 清仓: 点 @ 价」。它的注释写明对应协议表 1 的一行。
- **规则提示去重**：看板和 Inspect 原来各复制一份 Hold 规则说明，现在合并成 `TabFactors.hpp` 里的一个常量 `kHoldRulesTip`，内容是协议表 1 和 2 的 UI 摘要。
- **Portfolio 补全**：表格的 Hold 列、「加入」按钮、条目列表这三处悬停时，都会显示完整写法。
- **只留短名的地方**：表格格子、期限结构横轴、图例这类紧凑位置仍然只显示 `T+1C` 这样的短名，避免挤爆版面。

**3. README**：
- 删掉 FactorsBook 条目。
- FactorsLoad 一条改为说明 `fold_exit` 是唯一的净值引擎，池篮子指数每个持有期一条，可成交和顺延已经写在标签里。
- Inspect 和 Portfolio 两条改为走 `fold_exit`。
- Search 一条改为只搜给定的持有期。
- Contract 一条加上两张协议表的说明，成本口径改为按 Δw。
- TabFactors 一条加上 `hold_desc` 的写法说明。

