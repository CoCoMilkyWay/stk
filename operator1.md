| 算子         | 元  | 参数 | 形态    | GPU 方案                                                                   | 成本 | 误差 / 问题                                                                     |
| ------------ | --- | ---- | ------- | -------------------------------------------------------------------------- | ---- | ------------------------------------------------------------------------------- |
| Abs          | 1   | —    | MAP     | 纯 ALU                                                                     | ★    | —                                                                               |
| Sign         | 1   | —    | MAP     | 纯 ALU                                                                     | ★    | `Sign(0)=0` 与 fp16 −0 要统一                                                   |
| Log          | 1   | —    | MAP     | `__logf` (SFU)                                                             | ★    | ~2 ulp                                                                          |
| Asinh        | 1   | —    | MAP     | 组合超越函数                                                               | ★    | 稍贵                                                                            |
| Tanh         | 1   | —    | MAP     | 组合超越函数                                                               | ★    | 饱和区信息全丢                                                                  |
| Sqrt         | 1   | —    | MAP     | `sqrtf`                                                                    | ★    | ~2 ulp                                                                          |
| Relu         | 1   | —    | MAP     | 纯 ALU                                                                     | ★    | —                                                                               |
| Recip        | 1   | —    | MAP     | 分母 `clamp(\|x\|,eps)` 保号                                               | ★    | x≈6e-5 时溢出 fp16                                                              |
| SignedPow    | 1   | k    | MAP     | `powf`                                                                     | ★★   | 慢一个量级; `\|x\|^k` 易上下溢                                                  |
| Clip         | 1   | k    | MAP     | `clamp(x,−k,k)`                                                            | ★    | —                                                                               |
| Add          | 2   | —    | MAP     | 纯 ALU                                                                     | ★    | fp16 溢出需 clamp                                                               |
| Sub          | 2   | —    | MAP     | 纯 ALU                                                                     | ★    | fp16 溢出需 clamp                                                               |
| Mul          | 2   | —    | MAP     | 纯 ALU                                                                     | ★    | fp16 溢出需 clamp                                                               |
| Div          | 2   | —    | MAP     | 分母 `clamp(\|y\|,eps)` 保号                                               | ★    | 退化处出巨值而非 NaN                                                            |
| Max          | 2   | —    | MAP     | 纯 ALU                                                                     | ★    | —                                                                               |
| Min          | 2   | —    | MAP     | 纯 ALU                                                                     | ★    | —                                                                               |
| Imb          | 2   | —    | MAP     | 和 `clamp(eps)`                                                            | ★    | 和≈0 时符号不稳                                                                 |
| Share        | 2   | —    | MAP     | 和 `clamp(eps)`                                                            | ★    | 和≈0 时符号不稳                                                                 |
| LogRatio     | 2   | —    | MAP     | `log(clamp(x,eps))−log(clamp(y,eps))`                                      | ★    | 负输入出垃圾值, 靠 valid 掩码标                                                 |
| Where        | 3   | —    | MAP     | `select`, 无分支                                                           | ★    | —                                                                               |
| TodMask      | 0   | k,k2 | MAP     | 纯 t 坐标函数 (不读 x)                                                     | ★    | 现签名假装一元                                                                  |
| TsDelay      | 1   | d    | MAP     | shift/gather, **d 免费**                                                   | ★    | 前 d 行无定义                                                                   |
| TsDelta      | 1   | d    | MAP     | shift/gather, **d 免费**                                                   | ★    | 前 d 行无定义                                                                   |
| CumSum       | 1   | —    | SCAN    | `Σx` 分段 cumsum                                                           | ★    | 长 T 累积误差, 分块重基准                                                       |
| CumMean      | 1   | —    | SCAN    | `Σx, n`                                                                    | ★    | 同上                                                                            |
| CumVar       | 1   | —    | SCAN    | `Σx, Σx²` 两遍标准化                                                       | ★    | 近常值列抵消, 可出负方差需 clamp                                                |
| CumStd       | 1   | —    | SCAN    | 同 CumVar                                                                  | ★    | 同上                                                                            |
| CumSkew      | 1   | —    | SCAN    | `+Σx³`, 标准化后算                                                         | ★★   | 三阶抵消, 必须两遍                                                              |
| CumKurt      | 1   | —    | SCAN    | `+Σx⁴`, 标准化后算                                                         | ★★   | fp32 单遍仅两三位有效, 必须两遍                                                 |
| CumHhi       | 1   | —    | SCAN    | `Σx, Σx²` (与 Var 共享扫描)                                                | ★    | `Σx≈0` 时爆                                                                     |
| CumEntropy   | 1   | —    | SCAN    | `Σx, Σx·lnx`, `where(x>0,·,0)`                                             | ★    | x 有负值时定义失效                                                              |
| CumCountGt   | 1   | k    | SCAN    | 指示量 `Σ1[x>k]` 分段 scan; **k 不免费** (桶版会把落在桶中的 k 算错)        | ★    | 无 (精确整数计数)                                                               |
| CumPeaks     | 1   | k    | SCAN    | gather ±1 谓词 → 分段计数 scan (依赖 expanding mean)                       | ★★   | 实为 rolling 谓词 + expanding 计数, 应拆                                        |
| CumCov       | 2   | —    | SCAN    | `Σx,Σy,Σx²,Σy²,Σxy`                                                        | ★★   | 近常值列失稳                                                                    |
| CumCorr      | 2   | —    | SCAN    | 同上五和                                                                   | ★★   | `\|ρ\|>1` 需 clamp                                                              |
| CumBeta      | 2   | —    | SCAN    | 同上五和                                                                   | ★★   | `var(y)≈0` 时爆                                                                 |
| CumResid     | 2   | —    | SCAN    | 同上五和                                                                   | ★★   | 同上                                                                            |
| CumWMean     | 2   | —    | SCAN    | `Σwx, Σw`                                                                  | ★    | `Σw≈0` 时爆; 负权无意义                                                         |
| TsSum        | 1   | d    | SCAN    | `c[t]−c[t−d]`, **d 免费**                                                  | ★    | 分块局部幂和                                                                    |
| TsMean       | 1   | d    | SCAN    | 同上                                                                       | ★    | 同上                                                                            |
| TsVar        | 1   | d    | SCAN    | `Σx, Σx²` 差分, 两遍标准化                                                 | ★    | 近常值列抵消                                                                    |
| TsStd        | 1   | d    | SCAN    | 同 TsVar                                                                   | ★    | 同上                                                                            |
| TsSkew       | 1   | d    | SCAN    | `+Σx³`, 标准化后算                                                         | ★★   | 必须两遍                                                                        |
| TsKurt       | 1   | d    | SCAN    | `+Σx⁴`, 标准化后算                                                         | ★★   | 必须两遍                                                                        |
| TsZ          | 1   | d    | SCAN    | `Σx, Σx²` + 当前值                                                         | ★    | σ≈0 时爆                                                                        |
| TsWma        | 1   | d    | SCAN    | `Σx` 与 `Σ(s·x)` 线性组合, **不需 unfold, d 免费**                         | ★    | —                                                                               |
| TsProduct    | 1   | d    | SCAN    | `Σlog(1+x)` 差分后 `exp`                                                   | ★    | `1+x≤0` 无定义; 往返误差随 d 累积                                               |
| TsSlope      | 1   | d    | SCAN    | 同 TsWma (期序矩为常数)                                                    | ★    | 同 Var 抵消                                                                     |
| TsCountGt    | 1   | d,k  | SCAN    | 指示量前缀和差分, **d 免费 / k 不免费**                                    | ★    | 无 (精确整数计数)                                                               |
| TsCov        | 2   | d    | SCAN    | 五和差分                                                                   | ★★   | 近常值列失稳                                                                    |
| TsCorr       | 2   | d    | SCAN    | 五和差分                                                                   | ★★   | `\|ρ\|>1` 需 clamp                                                              |
| TsBeta       | 2   | d    | SCAN    | 五和差分                                                                   | ★★   | `var(y)≈0` 时爆                                                                 |
| TsResid      | 2   | d    | SCAN    | 五和差分                                                                   | ★★   | 同上                                                                            |
| TsWMean      | 2   | d    | SCAN    | `Σwx, Σw` 差分                                                             | ★    | `Σw≈0` 时爆                                                                     |
| CumMax       | 1   | —    | EXTREME | 分段 `cummax`                                                              | ★    | 仅 fp16 量化误差                                                                |
| CumMin       | 1   | —    | EXTREME | 分段 `cummin`                                                              | ★    | 同上                                                                            |
| CumArgMax    | 1   | —    | EXTREME | 对 `(值,下标)` pair 做 cummax                                              | ★    | 量化后并列变多, 位置不稳                                                        |
| CumArgMin    | 1   | —    | EXTREME | 同上                                                                       | ★    | 同上                                                                            |
| TsMax        | 1   | d    | EXTREME | doubling sweep (ping-pong 两平面, RMQ 只用 `k=⌊log₂d⌋` 层), 一趟服务所有 d | ★★   | 无数值误差; 层常驻会吃 2.6 GB, 必须 ping-pong                                   |
| TsMin        | 1   | d    | EXTREME | 同上                                                                       | ★★   | 同上                                                                            |
| TsArgMax     | 1   | d    | EXTREME | 同上, pair 版                                                              | ★★   | 并列位置不稳                                                                    |
| TsArgMin     | 1   | d    | EXTREME | 同上, pair 版                                                              | ★★   | 同上                                                                            |
| CumRank      | 1   | —    | ORD     | 分段累计桶直方图前缀和 → 插值                                              | ★★   | 误差 ~1/256; 均秩退化为整桶同秩                                                 |
| CumTopK      | 1   | k    | ORD     | 从高桶向下累计到 k 个                                                      | ★★   | 桶边界处个数不准, 和值有偏                                                      |
| TsRank       | 1   | d    | ORD     | 滑窗桶计数差分, **d 免费**                                                 | ★★   | 精确版需 sort → ★★★★                                                            |
| TsMed        | 1   | d    | ORD     | 找累计 50% 的桶                                                            | ★★   | 分布极偏时误差放大                                                              |
| TsMad        | 1   | d    | ORD     | 两轮直方图 (med → `\|x−med\|` 的 med)                                      | ★★★  | 全表误差最大                                                                    |
| TsEma        | 1   | k    | REC     | 仿射复合 scan (Blelloch)                                                   | ★★   | **参数不免费**; `(1−k)^T` 下溢需分块; 掩码仿射最易写错; 搜索应用 d, `k=2/(d+1)` |
| CsMean       | 1   | —    | CSR     | shfl 归约 + 广播                                                           | ★    | —                                                                               |
| CsStd        | 1   | —    | CSR     | shfl 归约                                                                  | ★    | 抵消同 TsVar                                                                    |
| CsDemean     | 1   | —    | CSR     | 归约 + map                                                                 | ★    | —                                                                               |
| CsZ          | 1   | —    | CSR     | 归约 + map                                                                 | ★    | σ≈0 时爆                                                                        |
| CsRank       | 1   | —    | CSR     | shared-mem 桶计数 (快于排序)                                               | ★★   | 桶宽误差; 现"缺失→均值填充"口径要改                                             |
| CsNormRank   | 1   | —    | CSR     | rank + `Φ⁻¹`                                                               | ★★   | 端点爆, clamp 到 `[1/(N+1), N/(N+1)]`                                           |
| CsWinsorRank | 1   | —    | CSR     | 两轮归约求 MAD → winsor → rank                                             | ★★★  | 两级近似, CS 内最贵                                                             |
| CsWinsorZ    | 1   | —    | CSR     | 两轮归约求 MAD → winsor → z                                                | ★★★  | 同上                                                                            |
| CsMedian     | 1   | —    | CSR     | shared-mem 桶直方图                                                        | ★★   | 桶宽误差                                                                        |
| CsBucket     | 1   | k    | CSR     | rank → `floor(pct·k)`                                                      | ★★   | 桶边界抖动                                                                      |
| CsResid      | 2   | —    | CSR     | 五和归约                                                                   | ★★   | 近共线时爆                                                                      |
| CsBeta       | 2   | —    | CSR     | 五和归约                                                                   | ★★   | `var(y)≈0` 时爆                                                                 |
| CsCorr       | 2   | —    | CSR     | 五和归约                                                                   | ★★   | `\|ρ\|>1` 需 clamp                                                              |
| CsRankDiff   | 2   | —    | CSR     | 两次 rank                                                                  | ★★   | 两份桶误差叠加                                                                  |
| CsGroupMean  | 2   | —    | CSR     | shared-mem 30 槽累加 (无 atomic 争用)                                      | ★★   | 组内样本少时不稳                                                                |
| CsGroupRank  | 2   | —    | CSR     | 组内桶计数                                                                 | ★★   | 组 id 缺失资产需明确归属                                                        |
| CsCondRank   | 2   | k    | CSR     | 先 bucket 再组内 rank                                                      | ★★   | 三级近似                                                                        |
| CsGroupResid | 3   | —    | CSR     | 组内 demean + 全局标量回归 (FWL)                                           | ★★   | 小组 demean 噪声大                                                              |
| Clip3        | 3   | —    | MAP     | 三元 `clamp(x,lo,hi)`                                                      | ★    | 上下界颠倒 (y>z) 判退化                                                         |
| YoY          | 1   | d    | MAP     | `TsDelay + Div` 组合, 不单列核                                             | ★    | 待补; d ≈ 一年分钟数, 远超块 carry 上限, 与分钟块化不兼容                         |
| EventAge     | 1   | d    | SCAN    | 对 `(变动?t:−1)` 做滑窗 max (复用 DOUBLE)                                  | ★    | 变动判据 `|Δ| > kRelEps·(|x_t|+|x_{t−1}|)`; 窗内无变动记 d                       |
| CumGini      | 1   | —    | ORD     | 桶内累计占比 (Lorenz), 只计 x>0                                            | ★★   | 误差 = 桶宽                                                                     |
| CumCorrLag   | 2   | k    | SCAN    | `gather(y,k)` 后走 CumCorr 五和                                            | ★★   | **k 不免费**                                                                    |
| CsQuantile   | 1   | k    | CSR     | 桶直方图找 q 分位                                                          | ★★   | 误差 = 桶宽                                                                     |
| CsWinsor     | 1   | k    | CSR     | 桶找 q/1−q 分位后 clamp                                                    | ★★   | 误差 = 桶宽; WinsorRank/WinsorZ 复用它 (固定 k=0.01)                             |
