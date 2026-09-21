# Factor design for features
_Exported on 9/21/2026 at 22:17:19 GMT+8 from Cursor (3.20.21)_

---

**User**

@README 
@output/doc/checked.md @output/features/smallcap400/features.json 
基于我们找的特征, 如果要实现所有的因子, 我们应该额外设计哪些TS/CS算子?
因子 = 基于特征的非线性算子组合

---

**Cursor**

先说结论: 现有 620 列特征基本是"分钟内原始度量"(累计/采样/日频广播), 研报因子几乎全部落在**特征之上的第二层**: 日内聚合 → 日频序列 → 跨日滚动 → 截面处理. 现在这一层只有 `ts::Tf`(逐值单调变换)、`cs::Method`(单列截面归一) 和一个特例 `Tod`(同分钟跨日 z). 缺的算子按下面分组.

## 一、TS 算子 (资产局部, 因果)

**1. 逐值代数 (Elementwise, 二元/多元)** — 现在 `ts::Tf` 只有一元单调
- `Imb(a,b) = (a−b)/(a+b)`、`Share(a,total) = a/Σ`、`Ratio`、`LogRatio = ln(a/b)`、`Diff`、`SignedPow(x,α)`、`Relu(x) = max(0,x)`、`Sign`、`Where(cond, a, b)`
- 用途: 广发 94/22 大小单全部是 imb/share (`amt_od_bid_bb_p93` vs `amt_od_ask_*`); 集合竞价 20 因子 (`qty_imb_auc2/qty_match_auc2`, `(px_max−px_min)/px_auc2`, `ln(open/px_auc2)`); 遗憾规避的 `Relu(vwap_hist − close)`; 换手 = `vol/float_shares`; 单笔均额 = `amt/n`

**2. 日内累计 (Intraday expanding, 开盘起累计, 日 reset)**
- `CumSum / CumMean / CumStd / CumSkew / CumKurt / CumMax / CumMin / CumArgMax(=分钟位)`
- `CumCorr(x, y)` (Welford 协方差, O(1) 状态) 和 `CumCorrLag(x, y, k)` (x_t vs y_{t+k})
- `CumWMean(x, w)` = 权重为另一列的加权均值 (VWAP 是特例; 显著效应因子的权重 = salience(r))
- `Concentration(x)`: 日内分布形状 — HHI / 熵 / Gini / top-k 占比 / 局部峰个数 (开源 峰岭谷、长江 拥挤度)
- 用途: 信达 42 / 中金 012 / 广发 048 的日内 skew/kurt、量价相关 `corr(Δln close, vol)`、`corr(ret_t, vol_{t+1})`、上下行波动占比 `Σrv_up/Σrv`

**3. 日内时段掩码 (Tod window selector)**
- `TodMask<from,to>` 生成 0/1 门, 与 CumSum 组合 → 开盘 30min / 尾盘 30min / 午后 成交占比、尾盘收益、开盘冲高回落
- 中金 007/012 的"尾盘成交占比"、"开盘 N 分钟收益", 长江 2024 有效因子多在此类

**4. 跨日滚动 (Rolling over D 交易日)** — 把 `Tod` 的 ring+bitmap 泛化成通用状态
- 基本: `Delay(D)`、`Delta(D)`、`Mean/Sum/Std/Min/Max/ArgMax/ArgMin(D)`、`DecayLinear(D)`、`Product`
- 统计: `TsRank(D)` (华泰 013 历史分位数 60/120/250 日; `ts::Rank` 目前只是占位)、`TsZ(D)`、`TsSkew/Kurt(D)`
- 二元: `TsCorr(x,y,D)`、`TsCov`、`TsBeta / TsResid(y on x, D)` (草木皆兵: vol 对 |ret| 回归斜率; 特质波动)
- 二阶: 对日频序列再套一次 → 方正"波动率的波动率" = `Std(Σ_day rv_15s, 20d)`
- Alpha101 / 遗传规划 (华泰 011/023、广发 007 挖掘) 的原语集合就是 1+4 两组, 缺一个都拼不出

**5. 日频→日频的 Fund 派生**
- `QoQ / YoY` = ttm 序列 `Delay(≈250d)` 后比值 (现有 `_q/_ttm/_lyr` 无同比)
- `TrendSlope(8Q)`: 单季序列的回归斜率/标准化斜率 (华泰 004 成长、光大 002)
- `EventAge`: 距最近一次值变动的天数 (财报更新、两融变动)

**6. 事件/条件计数**
- `CountIf(cond, W)`、`FracIf`: |r| > kσ 的分钟数、涨停触板分钟数 (`is_limit_up` 累计)、大单出现频次

## 二、CS 算子 (截面)

**1. 多列截面回归** — 现在 `NeutralRank` 硬编码 行业+log 市值
- `CsResid(y | X₁..Xₖ)`: 任意列集正交化 (Barra CNE6 的非线性规模 = `logmc³ ⊥ logmc`, 风格间正交, 因子相互剥离)
- `CsBeta`: 截面回归系数本身 (= 因子收益, 供 Fama-MacBeth 与因子动量)

**2. 截面聚合 → 广播**
- `CsMean / CsMedian / CsStd / CsQuantile`, 可按 `ind_l1` 分组: 市场收益、行业收益、行业均值
- 这是唯一合法的"跨资产信息入口": TS 红线禁止跨资产, 所以 Barra Beta / 特质波动 / 相对行业换手 / 广义拥挤度 (个股换手 vs 市场换手) 必须 = `CS 聚合` → 回到 TS 做 `TsBeta / Ratio`. **现有 DAG 是 TS→CS 单向, 没有 CS→TS 回流**, 这是结构缺口, 需要在因子层允许 CS 输出作为下一级 TS 输入 (分钟级联可以, L1 已经是 CS 每分钟一刷)

**3. 双列截面**
- `CsRankDiff(a,b)`、`CsCondRank(y | bucket(x))` (量价背离 = rank(Δp) − rank(Δv); 条件分位)
- `CsGroupRank(y, ind_l1)` 行业内 rank

## 三、两个结构性前提 (比单个算子更要紧)

1. **日频层 (L2 / Day)**: `ALL_LEVELS` 只有 L0/L1, 研报因子 80% 是日频, 跨日滚动的状态量 (D=250 × 特征数) 放在分钟节点里不合理. 需要一个 onDay flush 的层: 日内累计算子在 15:00 结算一次, 滚动算子在日频序列上跑.
2. **组合爆炸靠参数化模板而非手写节点**: 特征 620 × 算子 ~30 × 窗口 ~5, 不可能逐个写 `NODE_`. 需要 `Roll<Stat, D>(Up.out(port))` 这种模板节点 + 字段表里声明式 SRC (类似 `CS(lvl, src, Tf, Method)` 那样的 `TS(src, Op, W)`), 让 CMake 展开.

已有可复用的数学件: `math/normalize` (expanding Z/MAD/IQR/Rank, `OSTree`)、`RollingZScore`、`DailyQuantile` (跨日 ring 模板)、`AutoCorrelation`、`RetMoments`、`FracDiff/MADetrend/IIRBandpass`; `ts::Z/RobustZ/…` 占位可以直接接这些实现.

如果要落地, 建议先做 一.1 + 一.2 + 一.4 三组 (覆盖 中金 007/012、信达、广发 94、Alpha101 原语), 二.2 的 CS→TS 回流和日频层需要你先确认架构方向, 我不擅自动.

---

**User**

没有成对的对照。废仓库里流式和向量式是两堆互不引用的代码，没有同一套 TS/CS 算子各写一遍再对拍。

向量式只在 AlphaMining/Mining/Expression/Operator.py。整段序列用 torch.unfold 做滚动窗（RollingOp_1D / RollingOp_2D），一次算出全历史。算子是：

逐值：Abs Sign Log1p Add Sub Mul Div Pow Max Min
时序：TS_Ref TS_Delta TS_Mean TS_Sum TS_Std TS_Var TS_Skew TS_Kurt TS_Max TS_Min TS_Med TS_Mad TS_Rank TS_WMA TS_EMA TS_Cov TS_Corr
截面：只有 CS_Rank（沿股票维 argsort 两次）
BinaryOperator.evaluate 里有一段注释掉的按 period 切片再 unfold 的旧路径，仍是整窗向量计算，不是带状态的流式。

流式是另一套，名字和上面不对齐：

Math/ 里全是 K 线指标的 update()（ema、ma、stddev、rsi 等）。ema 是递推；ma / stddev 每根 K 线把窗口重新求和，不是 O(1) 状态。
Math/util/RecursiveLinReg.py 是固定窗斜率的 O(1) 递推，接近你说的 TrendSlope，没有向量版对照。
CTA/research/7 feature CandleLowTF.ipynb 里有 OnlineEMA、OnlineMean、OnlineMeanVariance（Welford）。只有 expanding 均值/方差，没有偏度、相关、日切，也没有向量孪生。
和你列的缺口对得上的只有：一.1 的加减乘除/Sign/Max 有向量版；一.4 的 Mean/Sum/Std/Skew/Kurt/Rank/Corr/Cov/Delay/Delta 有向量版；一.2 的累计均值/方差有 Welford 流式雏形。没有 Imb/Share/CumCorr/TodMask、没有 CsResid/CsMean、没有日频层，也没有流式对向量式的对拍。


参考一下这里的


然后总结一个最终版的算子表
按照 TS/CS * 1/2/3元 来列一个大表, 作为后面的guideline
@operator.md 

---

**Cursor**

已写入 `operator.md`. 几点与 AlphaMining 的口径差异, 需要你确认:

- 元数按**序列操作数**算, 窗口/阈值/时段是模板参数 (AlphaMining 把 window 算一元, TS_Corr 算三元; 本表 TsCorr 归二元).
- 分钟级跨日滚动 (窗口跨过日界) 不做, 用 `Cum*` (日内 expanding) 与 `Ts*` (日频 D 窗) 两个轴替代, 跨日的分钟级比较只走 `Tod*` 同分钟槽.
- 三元只保留 `Where`, 多元 CS 全部由二元 `CsResid` 链 + `CsGroupMean` 组合, 不另写核.
- 流式/向量式同名孪生 + 逐值对拍作为合入门槛, 写进了 §7.4.

---

**User**

<timestamp>Monday, Sep 21, 2026, 8:58 PM (UTC+8)</timestamp>
<system_notification>
The following task has finished. If you were already aware, ignore this notification and do not restate prior responses.

<task>
kind: shell
status: success
task_id: 9824
title: Locate the referenced AlphaMining repo
tool_call_id: toolu_01XmeYzcMxy36V3sS3YHsXTy
output_path: /home/chuyin/.cursor/projects/home-chuyin-work-stk/terminals/9824.txt
</task>
</system_notification>
<user_query>Briefly inform the user about the task result and perform any follow-up actions (if needed).</user_query>

---

**Cursor**

后台的 `find` 已结束, 结果与之前一致: 参考仓库只在 `/home/chuyin/work/tmp/AlphaMining/Mining/Expression/Operator.py`, 已用于对照, 无需再处理.

---

**User**

流式用来实盘, 接流式计算图, 向量式用来做因子挖掘, 给gpu用
我们还要做一个程序来检测流式和向量式的等价性
1. 给方案
2. 文件结构应该怎么安排

因子的其他基建先不做, 先把算子基建做好

---

**Cursor**

## 1. 方案

**三件东西, 一张真相表**

- 真相表 `OpTable.hpp` (X-macro, 与 `CS_METHODS` 同风格): 每行 = 名 / 元数 / 轴 (Elem·Cum·Roll·Cs) / 参数 (D, k, [a,b)) / 流式 struct 名 / 向量函数名. 流式、向量式、对拍程序三方都从它展开, 任一侧缺实现 = 编译错 / import 错, 不靠人对齐名字 (废仓库的病根).
- 流式 kernel (C++, 实盘): 纯数学 struct, 不依赖 `Series`/DAG, 契约 `push(x[, y]) → value() / reset()`, O(1) 状态. 接计算图的 Node 适配层属因子基建, 后做; kernel 独立可测是对拍的前提.
- 向量式 (torch, 挖掘/GPU): 张量 `[T, A]` (分钟) 或 `[D, A]` (日), 同名函数, `unfold` 整窗一次算完; Cum* 用 `days` 边界做 segment 累计.

**对拍程序 (`check`)**: 枚举 OpTable → 生成输入 → 两侧各算一遍 → 比对 → 报表
- 输入: 合成为主 (高斯 / 厚尾 / NaN 洞 / 常值段 / 零分母 / 日界正好落在窗口内 / 上市首日短样本), 可选真实特征 (C++ 侧 `FeatureRead` dump 几列 npy). 交换格式 npy: `x.npy [T,A] f32` + `days.npy` (日界下标) + `params.json`.
- C++ 侧 `op_stream` 独立可执行: 读 npy, 逐 t (逐日 reset) push, 写 `y.npy`. 按名字分派 = OpTable 展开的 switch.
- Python 侧算 torch 版, 比对: NaN 掩码逐位相等 (预热期 / 样本不足 / 零分母位置必须一致) + 有效位 `|Δ| ≤ atol + rtol·scale`, 容差按算子类 (逐值 1e-6, 一二阶矩 1e-5, 三四阶矩 / corr 1e-4, rank 精确).
- 语义契约先钉死, 否则对不上: NaN 输入 = 跳过不计 n, `n < min_n → NaN`; ddof=1; rank 并列均秩, pct = (r+1)/(n+1); Cum* 在日界 reset; Roll* 预热期 NaN; EMA 首值 = x₀; 方差两遍/Welford (向量式 fp32 单遍 Σx²−nμ² 会先炸, 见 `Tod` 注释, 向量侧也要 `x − mean` 后再平方).
- 输出: 每算子一行 (max|Δ| / NaN 不一致数 / 通过否), 任一失败非零退出. 合入门槛.

## 2. 文件结构

```
cpp/include/factor/                 # 新模块, 与 features/ 平级 (features = 特征 DAG; factor = 特征之上的算子)
  OpTable.hpp                       # 真相表 X-macro
  stream/
    Kernel.hpp                      # 契约 + NaN/min_n 语义 + 公共件 (ring / 单调队列 / Welford 五和)
    Elem.hpp                        # §1.1 §2 逐值 一元/二元/Where (无状态 static apply)
    Cum.hpp                         # §1.2 §2 日内 expanding (CumSum..CumKurt / CumCorr / CumWMean / Concentration)
    Roll.hpp                        # §1.3 §2 D 窗 (TsDelay..TsSlope / TsCorr / TsBeta)
    Cs.hpp                          # §4-6 截面 (复用 cs::, 加 CsMean / CsGroupMean / CsResid)
cpp/src/factor/
  op_stream.cpp                     # 对拍 CLI, 独立 CMake target (不进 app_main)
py/factor/
  optable.py                        # 解析 OpTable.hpp → 注册表 (仿 CMake 扫 NODE_ 的做法)
  ops/ elem.py cum.py roll.py cs.py # torch 向量式, 文件一一对应 stream/
  check/
    cases.py                        # 输入生成 + 边界 case
    tol.py                          # 容差策略
    run.py                          # 主程序: python3 py/factor/check/run.py [--op TsMean] [--real]
    data/                           # 中间 npy (gitignore)
```

加一个算子 = OpTable 一行 + `stream/*.hpp` 一个 struct + `ops/*.py` 一个函数, 跑一次 `check`.

## 待确认

1. 向量侧确定用 Python + torch? 系统 python 没装 torch, 你用哪个环境.
2. 新顶层模块 `cpp/include/factor/` 可以吗, 还是放 `features/Operator/` 下.
3. 交换格式 npy 可以吗 (不用 parquet, 免得两边都拖 arrow).

---

**User**

开始落地
算子要做干净, 整齐, 高度对仗

继续

---

**Cursor**

Now the runner.

