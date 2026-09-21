# 因子算子表 (guideline)

因子 = 特征 (features.json L1 列) 上的非线性算子组合. 本表是算子全集: 名字 / 公式 / 流式状态 / 向量式对照 / 来源. 加算子 = 在表里加一行, 流式与向量式同名同义, 逐值对拍.

## 0. 约定

- 元数 = **序列操作数个数**. 窗口 D / 阈值 k / 时段 [a,b) 是模板参数, 不算元 (AlphaMining 把 window 算成一元, 这里不沿用).
- 时间轴三种: `Δt` 分钟内 (现有特征层已做完), `t` 分钟 (日内, 日 reset), `D` 交易日 (跨日). 同一算子按轴前缀区分: `Cum*` = 日内 expanding, `Ts*` = 跨日滚动 D 窗. 分钟级跨日滚动 (窗口跨过日界的分钟窗) 不做: 日内轮廊污染, 用 `Tod*` 同分钟槽替代.
- 流式 = 单资产因果, O(1) 状态 (Welford / ring / 单调队列), 契约见 `DataDefine.hpp` OPERATOR CONTRACT. 向量式 = 整段序列 `unfold` 一次算完 (AlphaMining `RollingOp_1D/2D`). 二者必须对拍: 同输入逐值 |Δ| ≤ 1e-5·scale (fp32), NaN 位置一致.
- 缺失一律 NaN; 样本数 < MIN_N 或 分母 = 0 → NaN (`Tod` 出 0 是 z 的中性值特例, 不推广).
- CS 只准接同一时刻的截面; TS 只准接本资产. 跨资产信息进 TS 唯一路径: CS 聚合 → 广播列 → 下一级 TS (见 §7).
- 状态列: ✅ 已有 (给出位置) / ⭕ 占位或有可复用数学件 / ❌ 缺.

## 1. TS 一元

### 1.1 逐值 (无状态, `ts::Tf` 家族, 落盘热路径内联)

| 算子                | 公式                               | 向量式对照 | 状态                                                  |
| ------------------- | ---------------------------------- | ---------- | ----------------------------------------------------- |
| Log                 | sign(x)·log1p(\|x\|)               | Log1p      | ✅ `Method/TS.hpp`                                     |
| Asinh / Tanh / Sqrt | asinh(x) / tanh(x) / sign(x)√\|x\| | —          | ✅ `Method/TS.hpp`                                     |
| Abs / Sign          | \|x\| / sign(x)                    | Abs / Sign | ❌ (平凡)                                              |
| SignedPow⟨α⟩        | sign(x)·\|x\|^α                    | Pow(x, α)  | ❌                                                     |
| Relu                | max(0, x)                          | Max(x, 0)  | ❌ 遗憾规避 / 上下行拆分                               |
| Recip               | 1/x, x=0→NaN                       | Div(1, x)  | ✅ cs 侧 `Reciprocal`, ts 侧缺                         |
| Clip⟨k⟩             | clamp(x, −k, k)                    | —          | ⭕ `math/normalize clip`                               |
| TodMask⟨a,b⟩        | 1[a ≤ t < b]                       | —          | ❌ 与 Mul / CumSum 组合出 开盘 30min / 尾盘 30min 占比 |

### 1.2 日内 expanding (`Cum*`, 开盘起累计, onMinute, 日 reset)

| 算子                                 | 公式                                                 | 流式状态                                          | 向量式对照 | 状态 / 来源                             |
| ------------------------------------ | ---------------------------------------------------- | ------------------------------------------------- | ---------- | --------------------------------------- |
| CumSum / CumMean                     | Σ_{s≤t} x_s / ÷ n                                    | s, n                                              | cumsum     | ⭕ Welford 雏形 (OnlineMean)             |
| CumStd / CumVar                      | Welford                                              | n, μ, M2                                          | —          | ⭕ OnlineMeanVariance                    |
| CumSkew / CumKurt                    | M3/M2^1.5, M4/M2²                                    | n, μ, M2, M3, M4 (一遍推进公式)                   | —          | ❌ 信达 42 / 中金 012 日内高阶矩         |
| CumMax / CumMin                      | 极值                                                 | 1 标量                                            | cummax     | ❌                                       |
| CumArgMax / CumArgMin                | 极值所在分钟位                                       | 1 标量 + 位                                       | —          | ❌ 峰岭谷: 量峰时刻                      |
| CumRank                              | x_t 在 {x_s, s≤t} 的 pct rank                        | 需 OSTree 或 255 槽全存 (日内 ≤ 240 点, 全存即可) | —          | ⭕ `math/normalize OSTree/ERank`         |
| Concentration⟨HHI/Entropy/Gini/TopK⟩ | 对 {x_s, s≤t} 的分布形状                             | 全存 240 点                                       | —          | ❌ 长江 拥挤度 / 开源 峰岭谷             |
| CumPeaks⟨k⟩                          | 局部极大点数 (x_{s−1}<x_s>x_{s+1}, 幅度 > k·CumMean) | 前两值 + 计数                                     | —          | ❌ 峰岭谷                                |
| CountIf⟨cond⟩                        | Σ 1[cond(x_s)]                                       | 计数                                              | —          | ❌ 草木皆兵: \|r\|>kσ 分钟数; 触板分钟数 |

### 1.3 跨日滚动 (`Ts*`, 日频序列上的 D 窗; `Tod` 的 ring+bitmap 泛化)

| 算子                        | 公式                             | 流式状态                                  | 向量式对照        | 状态 / 来源                             |
| --------------------------- | -------------------------------- | ----------------------------------------- | ----------------- | --------------------------------------- |
| TsDelay⟨D⟩ / TsDelta⟨D⟩     | x_{D−d} / x − x_{D−d}            | ring[D]                                   | TS_Ref / TS_Delta | ❌ Alpha101 原语                         |
| TsSum / TsMean⟨D⟩           | 窗和 / 均                        | ring + 和                                 | TS_Sum / TS_Mean  | ❌                                       |
| TsStd / TsVar⟨D⟩            | 窗样本方差 (两遍或 Welford 滑窗) | ring + s1 + s2 (fp32 用两遍, 见 Tod 注释) | TS_Std / TS_Var   | ⭕ `RollingZScore`                       |
| TsSkew / TsKurt⟨D⟩          | 窗三/四阶标准矩                  | ring + s1..s4                             | TS_Skew / TS_Kurt | ❌                                       |
| TsMax / TsMin⟨D⟩            | 窗极值                           | 单调双端队列                              | TS_Max / TS_Min   | ❌                                       |
| TsArgMax / TsArgMin⟨D⟩      | 极值距今天数                     | 同上                                      | —                 | ❌ Alpha101                              |
| TsMed / TsMad⟨D⟩            | 中位数 / 中位绝对偏差            | 两堆或 OSTree                             | TS_Med / TS_Mad   | ⭕ `DailyQuantile`                       |
| TsRank⟨D⟩                   | x 在窗内 pct rank                | ring, O(D) 扫 (D ≤ 250 可接受)            | TS_Rank           | ⭕ `ts::Rank` 占位; 华泰 013 历史分位数  |
| TsZ⟨D⟩                      | (x − μ_D)/σ_D                    | 同 TsStd                                  | —                 | ⭕ `ts::Z` 占位                          |
| TsWma⟨D⟩ / TsDecayLinear⟨D⟩ | Σ w_d x_d, w 线性                | ring + 两和 (S, 加权 S 递推)              | TS_WMA            | ❌ Alpha101 decay_linear                 |
| TsEma⟨λ⟩                    | y = λx + (1−λ)y                  | 1 标量                                    | TS_EMA            | ⭕ OnlineEMA                             |
| TsProduct⟨D⟩                | Π (1 + x_d) − 1                  | ring + log 和                             | —                 | ❌ 动量 (复合收益)                       |
| TsSlope⟨D⟩                  | x 对 d 的 OLS 斜率               | ring + Σx + Σdx (递推)                    | —                 | ⭕ `RecursiveLinReg`; 华泰 成长趋势 (8Q) |
| TsCountIf⟨cond, D⟩          | 窗内 cond 天数                   | ring 位图                                 | —                 | ❌                                       |
| YoY⟨≈250d⟩                  | x / TsDelay(x) − 1               | = TsDelay + Div                           | —                 | ❌ Fund `_ttm` 同比                      |
| EventAge                    | 距上次 x 变动天数                | 上次值 + 计数                             | —                 | ❌ 财报/两融更新龄                       |

## 2. TS 二元

| 算子                  | 公式                            | 流式状态                                  | 向量式对照            | 状态 / 来源                                                       |
| --------------------- | ------------------------------- | ----------------------------------------- | --------------------- | ----------------------------------------------------------------- |
| Add / Sub / Mul / Div | 逐值                            | 无                                        | Add / Sub / Mul / Div | ❌ (平凡)                                                          |
| Max / Min             | 逐值                            | 无                                        | Max / Min             | ❌                                                                 |
| Imb                   | (a − b)/(a + b), 和=0→NaN       | 无                                        | —                     | ❌ 广发 94/22 全部; 集合竞价 `qty_imb`; 盘口 obi 已在特征层        |
| Share                 | a / (a + b)                     | 无                                        | —                     | ❌ 大单占比 / 尾盘占比                                             |
| LogRatio              | ln(a/b), 非正→NaN               | 无                                        | Sub(Log a, Log b)     | ❌ ln(open/px_auc2), ln(close/vwap)                                |
| Where⟨cond⟩           | cond(a) ? b : NaN               | 无                                        | —                     | ❌ 条件筛选 (涨停日排除 / 大单时刻)                                |
| CumCorr / CumCov      | 日内 corr(x, y)                 | n, μx, μy, M2x, M2y, Cxy (Welford 协方差) | —                     | ❌ 量价相关 corr(Δln P, vol) — 中金 012 / 广发 048 / 国金 量价背离 |
| CumWMean              | Σ w_s x_s / Σ w_s               | 两和                                      | —                     | ❌ vwap 是 w=vol 特例; 显著效应 w=salience(r)                      |
| CumBeta / CumResid    | 日内 y 对 x OLS 斜率 / 当前残差 | 同 CumCov                                 | —                     | ❌ 草木皆兵: vol 对 \|r\| 的响应                                   |
| TsCorr / TsCov⟨D⟩     | 跨日窗 corr / cov               | ring[D]×2 + 五和                          | TS_Corr / TS_Cov      | ❌ Alpha101; corr(ret, turnover)                                   |
| TsBeta / TsResid⟨D⟩   | 跨日窗 OLS 斜率 / 残差          | 同上                                      | —                     | ❌ Barra Beta / 特质波动 (x = 市场收益, 来自 §5 CsMean 广播)       |
| TsWMean⟨D⟩            | Σ w_d x_d / Σ w_d               | ring×2 + 两和                             | —                     | ❌ 遗憾规避: 成交量加权历史均价                                    |
| RegretRelu            | Relu(TsWMean(P, vol) − P)       | 组合                                      | —                     | 组合算子, 不单列                                                  |

## 3. TS 三元

| 算子                | 公式               | 状态 / 来源                                                                  |
| ------------------- | ------------------ | ---------------------------------------------------------------------------- |
| Where(cond, a, b)   | cond ? a : b       | ❌ 唯一常用三元; 其余三元一律拆成二元组合                                     |
| CumCorrLag⟨k⟩(x, y) | corr(x_s, y_{s+k}) | ❌ 严格说二元 + 参数, 但流式需要 y 延迟 k 的 ring, 单列出来提醒; 领先滞后量价 |
| Clip(x, lo, hi)     | clamp              | ❌ lo/hi 为序列时才是三元 (如 涨跌停价钳)                                     |

AlphaMining 里 TS_Cov/TS_Corr 是三元 (x, y, window), 按本表约定归二元.

## 4. CS 一元 (`cs::Method` 家族, 单列截面, 每分钟一刷)

| 算子                                      | 公式                             | 状态 / 来源                                    |
| ----------------------------------------- | -------------------------------- | ---------------------------------------------- |
| Rank / NormRank                           | pct rank / Φ⁻¹                   | ✅ `Method/CS.hpp` (向量式 CS_Rank 对照)        |
| WinsorRank / WinsorZ / Z / Demean         | 见 CS.hpp 头注                   | ✅                                              |
| NeutralRank                               | 行业 + log 市值 中性化后 rank    | ✅ (硬编码 ctx, 是 §6 CsResid 的特例)           |
| CsMean / CsMedian / CsStd / CsQuantile⟨q⟩ | 截面聚合 → 标量广播成列          | ❌ 市场收益 / 市场换手 / 离散度; 唯一跨资产入口 |
| CsGroupMean / CsGroupRank⟨by⟩             | 按 `ind_l1` 分组聚合 / 组内 rank | ❌ 行业收益, 行业内相对换手 (拥挤度)            |
| CsBucket⟨K⟩                               | 分位桶号 0..K−1                  | ❌ 条件排序分析 (mining/README §3.3)            |
| CsWinsor⟨q⟩                               | 分位缩尾, 不 z                   | ⭕ 已内嵌在 WinsorRank, 缺独立版                |

## 5. CS 二元

| 算子                         | 公式                                 | 状态 / 来源                                                |
| ---------------------------- | ------------------------------------ | ---------------------------------------------------------- |
| CsResid(y \| x)              | y − (α + βx), OLS 残差               | ❌ 单变量中性化 (对 log 市值 / 对 beta); NeutralRank 的一半 |
| CsBeta(y \| x)               | 截面 OLS 斜率 (= 因子收益, 标量广播) | ❌ Fama-MacBeth / 因子动量                                  |
| CsRankDiff                   | Rank(a) − Rank(b)                    | ❌ 量价背离                                                 |
| CsCondRank(y \| CsBucket(x)) | x 桶内 y 的 rank                     | ❌ 条件分位                                                 |
| CsCorr(a, b)                 | 截面 Spearman (标量广播)             | ⭕ `shared/Correlation` 分析侧已有, 生产侧缺                |

## 6. CS 三元 及以上

| 算子                     | 公式                                           | 状态 / 来源                                                                   |
| ------------------------ | ---------------------------------------------- | ----------------------------------------------------------------------------- |
| CsResid(y \| x₁, x₂, …)  | 多元 OLS 残差 (FWL 逐个剥离 = 二元 CsResid 链) | ❌ Barra 风格正交 (非线性规模 = logmc³ ⊥ logmc); 行业哑变量 = CsGroupMean 去均 |
| CsGroupResid(y \| x, by) | 组内回归残差                                   | ❌ = NeutralRank 去掉末尾 rank 的通用版                                        |

多于二元的 CS 全部由 二元 CsResid 链 + CsGroupMean 组合, 不另写核.

## 7. 结构性前提

1. **日频层**: `ALL_LEVELS` 只有 L0/L1. §1.3 / §2 的 `Ts*` 要跑在日频序列上 (`Cum*` 15:00 结算值 或 Fund 日频值), 需要 onDay flush 的 Day 层, 状态量 = 特征数 × D, 不进分钟节点.
2. **CS → TS 回流**: 现有 DAG 是 TS → CS 单向. `CsMean` 广播列 (市场收益) 作 `TsBeta` 的 x, 需要因子层允许 CS 输出作下一级 TS 输入 (L1 每分钟 CS 已就位, 只差接线). TS 红线不破: TS 节点仍只看"本资产的一列", 那列恰好是 CS 广播的.
3. **参数化模板节点**: 620 特征 × ~40 算子 × ~5 窗口 不可能手写 `NODE_`. 需要 `Roll<Op, D>(Up.out(port))` 模板 + 字段表声明式 SRC (对仗 `CS(lvl, src, Tf, Method)`), CMake 展开.
4. **流式/向量式孪生**: 每个算子两份实现同名, 向量版只用于 回测/挖掘 (torch unfold), 流式版用于 落盘/实盘; 对拍是合入门槛. 废仓库的教训: 两堆互不引用的代码 = 没有对照.

## 8. 优先级 (按能拼出的研报因子数)

1. §1.1 Abs/Sign/SignedPow/Relu/TodMask + §2 Add/Sub/Mul/Div/Imb/Share/LogRatio — 广发 94/22、集合竞价 20、遗憾规避、大小单比例 (零状态, 先做)
2. §1.2 Cum{Sum,Mean,Std,Skew,Kurt,Max,ArgMax} + §2 CumCorr/CumWMean — 中金 012、信达 42、广发 048、开源 029、显著效应/草木皆兵
3. §1.3 Ts{Delay,Delta,Mean,Std,Rank,Max,Min,DecayLinear,Slope} + §2 TsCorr — Alpha101 原语、中金 007、华泰 013、波动率的波动率、Fund 同比/趋势
4. §4 CsMean/CsGroupMean + §5 CsResid + §7.2 回流 — Barra CNE6、特质波动、拥挤度
5. §1.2 Concentration/CumPeaks + §4 CsBucket + §5 CsCondRank — 峰岭谷、拥挤度、条件排序挖掘
