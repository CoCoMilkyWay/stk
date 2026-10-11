# Automatic symbol detection removal
_Exported on 10/12/2026 at 03:58:04 GMT+8 from Cursor (3.21.9)_

---

**User**

@README 
我们因子评估那里， 现在会根据期限结构去自动分配不同期限结构下的因子符号， 这个是不合理的
我们还是应该用带冲击的期限结构去找最优期限， 这个你先不管
你先把整个代码里的自动符号检测部分完整删掉， 我们不要这个机制了

---

**Cursor**

我先查找代码里自动符号检测相关的部分。

核心在 `factor/Stat` 下，我看一下那边的实现。

现在开始改。先改核心契约 `Contract.hpp`。

Now the `Accum` accumulator and `finish`.

Contract 改完。接着改 CPU / GPU 后端和对拍。

Now the UI side: `TabInspect.cpp` / `.hpp`.

Grep 结果显示部分旧代码仍在，我重新读一下确认文件状态。

Now `TabFactors.cpp` (sign column), `TabOperators.cpp`, and `StatJson.hpp`.

Layout 是自动生成的，删掉 metric 即可。再改 `TabOperators.cpp` 和 `StatJson.hpp`。

最后全仓库扫一遍残留（含 Python）。

自动符号机制已经全部删掉了，没有编译，需要你自己编一下、跑一下。现在所有统计量都按因子原始方向输出，不再根据 mono 自动翻转。

**核心计算（`factor/Stat/Contract.hpp`）**
- 删掉了 `HoldStat::sign`、翻转阈值 `kSignMonoMin`，以及文件头里讲自动符号的那段说明。
- 删掉了一级 `Row::ls_rev`，以及 `Accum` 里只给翻转用的累加项（`nneg`、`nlrpos`、`slr`、`slr2`、`slmr`）。
- `finish` 改成直接按原值计算 ic、LS、beta、grp、mono 这几组量。

**CPU / GPU 后端和对拍**
- CPU 版 `Cpu.hpp` 去掉了顶组 Σsv 的累加（`sv_top`）；GPU 版 `Gpu.cuh` 也去掉了这一项，`fs` 从 4 项缩成 3 项，同时不再写 `ls_rev`。
- 对拍 `Check.hpp` 不再比较 `ls_rev` 和 `sign`。

**界面**
- Inspect 页（`TabInspect.cpp/.hpp`）：分层累计图、IC 分布、标的轴和 K 线的 R 着色都不再镜像；去掉了 `ax_flip` / `col_flip`，标题和状态行里的 `sign` 也拿掉了。
- 因子表（`TabFactors.cpp`）：删掉了 `sign` 这一列。
- 算子页（`TabOperators.cpp`）：悬停文字里的 `sign` 去掉了。

**旧的落盘结果**
- `StatJson.hpp` 写文件时不再写 `sign` 键。
- 读文件时，如果碰到带 `sign` 键的旧格式就判为无效。旧值里有些是翻转过的，所以 `output/factors/...` 下现有因子文件和 `operators.json` 里的 stat 会显示成没有 stat，要重新评估一遍。

如果你不想要这层旧文件拦截，可以去掉 `load_hold` 里 `o.contains("sign")` 那一行。不过那样的话，旧的翻转值会被当成有效结果读进来。

---

**User**

套和拆这两个字太唐了，用一个上箭头一个X就行

---

**Cursor**

先确认字体里有没有这两个字形，避免显示成问号。

ImGui 版本是 1.92，字形按需加载，再加上 Maple Mono NF 字体本身就带箭头和 ✕，所以 ↑ 和 ✕ 都能显示。

界面提示里也把按钮叫"套 / 拆"，一并改成新符号，免得对不上。

我把 `TabFactors.cpp` 里的两个按钮改了："套"换成 `↑`，"拆"换成 `✕`。构建器帮助提示里写按钮名的那句，还有上面对应的代码注释，也一起改成了新符号。悬停提示的文字没动。

这两个字符应该能正常显示：项目用的 ImGui 1.92 会按需加载字形，不受 `ChineseSimplifiedCommon` 字符范围限制，Maple Mono NF 字体里也有 ↑ 和 ✕。我没编译运行，如果显示成方块或问号告诉我，我再换个字符。

---

**User**

我想给30分钟动量因子改以下符号， 我加tssign但是报错

---

**Cursor**

我先看一下这个因子文件和算子表里有哪些可以取反的算子。

> **30 分钟动量的方向怎么翻？**
> - 用现有算子改写成 TsSub(TsDelayRoll(TsLog(close), d=30), TsLog(close))，不改代码
> - 新增一元算子 TsNeg (−x)，在 CPU / GPU / 流式三个后端都实现
>
> Answer: B， 然后看下是不是还有缺失这种最基本的算子

好，先摸清楚加一个算子要动哪些地方：以 `TsAbs` 为样板全仓库搜一遍。

加算子要动 4 处：`OpTable.hpp` 加一行，`Stream.hpp`、`Cpu.hpp`、`Gpu.cuh` 各加一个同名实现。先把整张表看完，顺便审一下还缺哪些基础算子。

表的排序规则是元数 → 域 → T 窗 → 核类 → 名字字母序，`TsNeg` 应插在 `TsLog` 和 `TsRecip` 之间。我先确认有没有地方硬编码了算子总数。

加好了。还要确认插入新算子后，旧的 `operators.json` 能不能正常读进来（因为后面算子的 idx 都往后移了一位）。

`TsNeg`（时取负，\(-x_t\)）加好了，没编译。30 分钟动量要翻方向，可以写成：

```text
CsRank(TsNeg(TsDeltaRoll(TsLog(close), d=30)))
```

在构建器里选中 `TsDeltaRoll` 那一层，点 ↑，再选 `TsNeg` 就行。

**改了哪些地方**
- 在 `OpTable.hpp` 里加了一行，按排序规则插在 `TsLog` 和 `TsRecip` 之间，输入输出都是 REAL。
- 三个后端各加了一个同名实现：CPU 版 `Cpu.hpp`、GPU 版 `Gpu.cuh`、流式版 `Stream.hpp`。
- 分派、`op_check` 对表、算子页的对拍都按宏自动展开，不用另外注册。注释里的算子计数也顺手改了。
- 现有的 `operators.json` 第一次打开算子页时会因为算子总数对不上，被自动删掉重算一轮，这是正常的。

**还缺不缺这种最基本的算子**

我对着整张表查了一遍，结论是只缺 `TsNeg` 这一个。这张表的原则是能用现有算子拼出来的就不单独收；之前取负拼不出来，是因为没有常数叶子节点。现在有了 `TsNeg`，其他常用的基础运算都能用现有算子精确拼出来：

| 想要的 | 用现有算子写法 |
|---|---|
| 小于 \(1[x<k]\) | `TsGt(TsNeg(x), k=−k)` |
| 平方 | `TsMul(x, x)` |
| 比较 \(x>y\) | `TsGt(TsSub(x, y), k=0)` |
| 与 / 或（0/1 掩码） | `TsMul` / `TsMax` |
| 收益率 | `TsDiv(TsDeltaRoll(x), TsDelayRoll(x))` |
| 中位数 | `TsQuantileRoll(k=0.5)` / `CsQuantile(k=0.5)` |

"小于"和"平方"这两条我补进了表头"不入表的组合"那段注释。截面那边不需要单独的 `CsNeg`，因为 `TsNeg` 是逐点算子，CS 口径里也能直接用。

另外，翻转后这个因子其实是 30 分钟反转。文件名 `mom_30m` 和中文名"三十分钟动量"要不要改，你来定。

---

**User**

我们的mono是mono(t)的均值， 不是end point的mono吧？

---

**Cursor**

两者都不是。现在的 mono 是**先沿 t 池化出 20 个组均值，再对这 20 个数算一次 Spearman**：

```327:329:cpp/include/factor/Stat/Contract.hpp
    double g[kGroups];
    for (int k = 0; k < kGroups; ++k)
      g[k] = sc[k] > 0 ? sg[k] / static_cast<double>(sc[k]) : 0.0; // 沿 t 池化
```

```357:357:cpp/include/factor/Stat/Contract.hpp
    s.mono = static_cast<float>(mono_of(g, kGroups));
```

`Accum` 里根本没有逐 t 的 mono 累加项，一级 `Row` 也没有 `mono` 字段，所以不存在 mean of mono(t)。

它和"end point 的 mono"的关系：
- CS 口径：每 t 各组 cnt 基本相等（A/20 上下差 1），池化均值 ≈ 逐 t 组均值的和 / T，与分层累计图末点成正比，所以 mono 基本就是 end point 的 mono。
- TS 口径：组可空、cnt 逐 t 剧变，池化是 Σgrp / Σcnt（按样本数加权），而 `TabInspect::derive` 画的累计线是逐 t 的 grp/cnt 直接求和（每 t 等权），两者权重不同，mono 和图上末点的单调性可能对不上。

这是设计上写明的（文件头【二级 HoldStat】"grp[k] 各组沿 t 池化均值; mono = Spearman(组号, grp[k])"）。如果你想要的是 mean of mono(t)，需要在 `Row` 里加逐 t mono、`Accum` 加累加项、CPU/GPU `finish_row` 各算一遍——要不要改你定。

---

**User**

所以设计还算合理？ 考虑了中间时间段的分层情况？

---

**Cursor**

合理，但要说清它回答的是哪个问题：

**池化 mono 不看中间时段**。它只回答"整个区间加总之后，20 层的排布是否单调"，等价于分层累计图末点的形状。前半段正向单调、后半段反向，加总后可能相互抵消显示成"不单调"，也可能被几天大行情主导——这些它都分辨不出来。

**但中间时段的分层一致性已经由 IC 系列覆盖了**。逐 t 的 mono(t) 本质上就是把截面秩压成 20 桶之后再算的 Spearman，是一个粗化版的 rank IC；它的均值会和 `ic_mean` 高度相关，它的稳定性对应 `icir` / `ic_pos` / `ic_t`。再加一组 mono(t) 的统计量，信息上基本冗余。

所以现在的分工是：
- `ic_mean / icir / ic_pos / ic_t`：逐 t 看，因子方向在时间上有多稳定
- `mono / grp[]`：加总看，分层形状是否干净（头尾组是否线性拉开，还是只有两端有效、中间乱）——这是 IC 看不出来的，因为 IC 对"中间乱"不敏感
- `ls_*`：只看顶底两组能不能赚

两个真正的瑕疵：
1. TS 口径下池化权重（Σgrp/Σcnt）和 Inspect 累计图的逐 t 等权不一致，mono 和图末点可能对不上。CS 口径无此问题。
2. 和所有加总量一样，mono 会被高波动时段主导。

如果你关心"分层结构是否全程稳定"而不是"方向是否全程稳定"，才有必要加 mono(t) 的分布统计；按现有指标看我不建议加。

---

**User**

现在的IC怎么算的

---

**Cursor**

一级每 (持有期 h, 时刻 t) 一个 IC，二级沿 t 汇总。

**一级 `Row::ic`（`Contract.hpp` / `finish_row`，CPU、GPU 逐位一致）**

```25:32:cpp/include/factor/Stat/Contract.hpp
//   【rank 口径】精确并列均秩 (排序, 不用桶近似 —— 原始因子重尾时一个离群点会把整截面压进同一桶, IC 归零):
//     r16 = round(pct · kRankMax) ∈ [0, kRankMax], 全整数公式 (r16_of), 两后端逐位一致; kRankNone = 无效.
//     行有效样本 n < 2 或全并列 (spread 为假) → 整行 kRankNone.
//     rank(x) 只看 x 的有效集, rank(y) 只看 y 的有效集 (与因子无关才能常驻); 两者的联合集 J 上算统计.
//     TS 的 r16_quant 走 double (float × 65534 在 double 内精确, 与 FMA 无关), 两后端逐位一致; x ∉ [0,1] 断言.
//
//   【一级 Row (每 h, t)】J = {a : rx ≠ None ∧ ry ≠ None}; e = 超额标签 (lv − mkt; 做空侧 sv − mean_J sv)
//     ic     Spearman = Pearson(r16_x, r16_y) over J. 五个和都是整数 (pearson_int), 两后端逐位一致.
```

- `rx`：因子侧秩。CS 口径 = 每 t 沿 A 轴并列均秩；TS 口径 = 根 `TsRankRoll` 输出的自身历史分位直接量化 `round(x·65534)`，不再截面排序。
- `ry`：标签侧秩 = 每 t 做多标签 `lv`（毛口径 LabelReturn，h 分钟后的价格收益）的截面并列均秩，与因子无关，常驻期算一次。
- 在联合有效集 J 上做 Pearson(rx, ry)，五个和 n/Σx/Σy/Σx²/Σy²/Σxy 全是 64 位整数，最后一步才除法开方 (`pearson_int`)。
- 行 `ok` 条件：非段末尾部（分钟档 exit 不能撞收盘集合竞价）∧ n ≥ 2 ∧ 两侧方差 > 0 ∧（CS）20 组都非空。不 ok 的行 ic = 0 且不入二级。

所以 TS 口径下 IC 的含义是"个股处于自身历史高位的程度"与"当刻截面收益排名"的 Spearman，仍是截面相关，不是时序相关。

**二级 `HoldStat`（`Accum::finish`，只用 ok 行，n 行）**
- `ic_mean` = Σic / n
- `ic_std` = 样本标准差 (ddof=1)，`icir` = mean / std
- `ic_t` = icir · √(n / h)：相邻 h 行的标签是同一段收益，有效样本按 n/h 折算，否则 t 虚高 √h 倍
- `ic_pos` = IC > 0 的行占比
- `ic_skew` / `ic_kurt` = 总体三阶 / 四阶中心矩（超额峰度），由幂和 Σic²/Σic³/Σic⁴ 推出

秩都是 16 位整数、量级 ≤ 65534，A ≤ 5120，n·Σx² < 2⁵⁷ 不溢出，所以 CPU 和 GPU 的一级 IC 能对拍到逐位一致；二级幂和是 double，只在 Row 级对拍。

---

**User**

太乱了， 让我简单明白怎么算的

---

**Cursor**

每个时刻 t，取当时所有股票：

1. 因子值排个名次 → rx
2. 未来 h 分钟的收益排个名次 → ry
3. 算这两列名次的相关系数 → 这就是 t 时刻的 IC（rank IC / Spearman）

然后把所有时刻的 IC 放在一起：

- `IC` = 平均值
- `ICIR` = 平均值 / 标准差
- `t` = ICIR × √(时刻数 / h)（相邻 h 分钟的收益重叠，样本数要打折）
- `pos` = IC > 0 的时刻占比

就这些。TS 口径唯一的区别是第 1 步不重新排名，直接用因子自己输出的分位数当名次。

---

**User**

所以如果我要优化因子效果， 通过调整表达式所有参数项， 是不是去优化IC平均值就好？

---

**Cursor**

不够。只盯 IC 均值会出三种偏差：

1. **不稳定也能有高均值**。几天大行情就能把均值抬起来。用 `ICIR` 或 `ic_t` 代替均值，同样的参数搜索，选出来的东西稳得多。
2. **IC 不管换手**。短窗参数往往 IC 高但 `rank_ac` 低、`turn` 爆炸，扣完冲击是负的。你之前说的"带冲击的期限结构找最优期限"，本质就是这个问题——最终目标应该是扣成本后的 LS，不是 IC。
3. **IC 看全截面，交易只用两端**。IC 高但 `mono` 差（中间乱、只有一端有效）的因子，分层做多空效果会打折。

实际操作建议：

- 主目标：`ic_t`（或 `ICIR`），不是 `IC`
- 硬约束：`turn` 不超过某个上限，`mono` 不低于某个下限
- 最后用 `SR`（LS 年化 Sharpe）复核，等冲击模型接进来后换成净 SR
- 参数空间别太大，几个窗长 d 扫一遍就够；每多一个自由参数都是在过拟合 101 天的样本。有条件的话前后半区间分开看，后半段的 `ic_t` 没掉再算数

---

**User**

不会把， 我们看的是超额IC， 应该基本 没有大行情把IC曲解的问题吧
最多就是又没有短暂性失效看不太出来？

---

**Cursor**

对，这点你说得对，我之前的说法不准确。

两点澄清：

1. 对 IC 来说"超额"其实不起作用——排名本身就把当刻的市场水平抹掉了，标签用 lv 还是 lv − mkt，秩一模一样。超额只影响 grp / LS 那些用收益值的量。
2. 真正让 IC 不怕大行情的是 rank + 有界：每个 t 的 IC ∈ [−1, 1]、等权，一天再极端也只是 n 分之一。被几天主导的是 LS、grp、mono 这类直接加收益的量，不是 IC。

剩下的就是你说的：IC 均值看不出**阶段性失效**。比如 10% 的时段 IC 0.1、其余 0.01，均值 0.019 看着不错，但一半贡献来自那 10%。`ICIR` / `ic_t` 就是为这个存在的，所以主目标我还是建议用 `ic_t` 而不是 `IC`。

另外两条（IC 不管换手、IC 看全截面而交易只用两端）不受这个影响，仍然成立。

---

**User**

其他， mono， shaepe那些， 我觉得也不能成为因子和核心梯度下降判据， 这些都会很高
我们最关心的其实应该还是LS 的收益是不是够高和IC是不是比较高？

---

**Cursor**

基本同意。

- `mono` 是过滤器不是目标：任何像样的因子都 0.9+，饱和了没梯度。
- `sharpe` 现在的年化方式（按持有期为一期 × √每年期数）在分钟级会把数字吹到 ±15 这种量级，跨 h 也不可比，确实不适合当目标。

所以核心就是两个：**IC 够不够高、LS 够不够赚**。两点补充：

1. **调参用 IC，验收用 LS**。LS 只用顶底两组（10% 的票）、是收益值、重尾；IC 用全截面、是秩、有界。同样的参数扰动，IC 的变化平滑得多，LS 噪声大，拿 LS 做"梯度"容易跟着噪声走。先按 `ic_t`（或 IC 均值，固定 h 下两者排序差别不大）把参数调出来，再看 LS 是不是跟着上去了；不跟着上去说明两端没有拉开，这才是 LS 该回答的问题。
2. **LS 现在是毛的**。参数一短、换手一高，毛 LS 看着漂亮，扣冲击后排名会完全重排。这正是你要做的带冲击期限结构——冲击接进来之前，LS 对比只在同 h、换手相近的参数之间有意义。

另外 `ls_mean` 随 h 线性放大（你 json 里 1m 是 1e-4，T+5 是 2e-3），跨 h 比要除以 h 或看年化，同 h 内直接比没问题。

---

**User**

对于A股这种杠杆不够的市场， 其实LS收益才是最关键的？ 然后再保证IC还行就可以， 只要分层较好， 哪怕稍微没那么稳定也可以接受？

---

**Cursor**

方向对，但再往前推一步：A 股小票基本融不到券，你能做的只有多头那条腿。所以最关键的不是 LS，是 **G20 的超额（年化 L）**。

LS = 多腿 + 空腿，一个因子的 LS 可能一大半来自 G1 暴跌，多头那边只是平平——这种因子在 A 股没用。Inspect 悬停里已经分开列了"年化 %: L / S / LS"，看 L 那一列。

其余判断我同意：

- **没杠杆 → 收益绝对值比 Sharpe 重要**。高 Sharpe 低收益的信号你放不大，所以 ICIR / Sharpe 退居次要，这正是用绝对收益当主判据的经典理由。
- **不稳定可以接受**，代价是回撤你对冲不掉，只能靠多因子分散。单因子层面先把均值做高没问题。
- **IC 做底线**，防止 G20 的收益是几只票撑出来的巧合。
- `mono` 在这个前提下反而不太重要：你只关心最上面那一组好不好，中间乱不乱无所谓。但要看 G20 和 G19/G18 是不是明显拉开——如果 G20 不是最好的那组，分组边界稍动一下收益就没了。

所以排序：**G20 超额（扣冲击后）> IC > 其他**。冲击模型接上之后这个顺序不变，只是 L 换成净值。

---

**User**

所以如果我们要设计一个loss来去优化因子， 应该怎么设计比较合适？

---

**Cursor**

先说一个前提：表达式参数是窗长 d、阈值 k 这类离散/非光滑量，中间还有 rank，不可微。所以这个"loss"实际上是给网格 / 贝叶斯搜索用的打分函数，不是真能求梯度的东西。按 A 股只能做多、没杠杆的前提，我会这样设计：

**主目标：扣冲击后的多头超额**

\[
L_{\text{net}}(h) = \text{ann}\big(\text{G20 超额}(h)\big) - \text{冲击成本}(h)
\]

- G20 超额 = `grp[K-1]` × `periods_per_year(h)` × 100，Inspect 悬停里"年化 L"已经是这个数
- 冲击成本：Inspect 已经能喂扣冲击后的净标签平面，直接用净口径下的 `grp[K-1]` 更准；没接之前用 `turnover_annual(h)` × 单边冲击近似
- 对 h 取最大：\(\text{score} = \max_h L_{\text{net}}(h)\)，这就是你说的"带冲击的期限结构找最优期限"

**约束，不要做成加权和**

加权和的权重没法定，换成硬门槛，不过线直接淘汰：

- IC 底线：`ic_mean(h*) ≥ 0.02` 左右（防止 G20 收益是几只票撑的）
- 顶组分离：G20 要明显高于 G19、G18。更稳的做法是主目标别只用 G20 一组，用顶部 2–3 组的平均——分组边界稍动一下收益就没了的因子，搜索时会被它骗
- 样本底线：`n ≥ 某值`，不然 T+5 那种 h 几百个样本就能把均值做出来

**过拟合防护，这是最重要的一条**

- 区间对半切，score 取两段的 **min**，或者只认后半段
- 每个表达式最多扫 1–2 个参数；101 天数据扛不住更多自由度
- 搜索完的最优参数，看它邻近参数（d±5）的 score 是不是也差不多；孤峰就是噪声

**最终形式**

\[
\text{score} = \min_{\text{前/后半}} \; \max_h \; L_{\text{net}}(h) \quad \text{s.t.} \; \text{IC}(h) \ge c_1,\; n(h) \ge c_2,\; \text{G20} > \text{G19}
\]

不满足约束的 score = −∞。

如果以后想做真正可微的（比如用神经网络学因子），那是另一套：soft rank 替代 rank，loss = −IC 或 −顶组收益，再加换手正则项。现阶段搜索参数不需要。

---

**User**

先做一个很粗的参数网格， 对于每个参数组合， 我们只算5W冲击下的期限结构， 然后定下来最优期限h
然后找到最优的参数位置， 在它旁边开始梯度下降搜索？

这样是不是比较好？ 参数量我们抗的住的， 注意我们标的多，采样周期是1分钟 ，还在拟合map（因子）而不是单点， 所以搜索应该ok

---

**Cursor**

流程本身对。几个修正和补充：

**"梯度下降"改叫局部搜索**。窗长是整数、中间有 rank，目标函数是阶梯状的，没有梯度。实际做法是 pattern search：在最优点周围每个参数 ±步长试一遍，取最好的移过去，步长逐渐缩小，邻域没改进就停。效果一样，但别用梯度那套思路（比如 Adam、学习率）。

**粗网格阶段定下 h 之后，局部搜索阶段把 h 固定住**。h 本身也是个被拟合的参数，每走一步重选一次 h 会让目标函数更跳、更容易过拟合。局部阶段只动表达式参数，搜完再回头确认 h 还是不是最优。

**样本量这件事，你对了一半**。标的多 × 分钟级确实让截面统计（IC）估得很准，参数多也不怕。但你的主目标是 G20 收益，它的方差主要来自时间维度：同一天内的分钟行高度相关（持有期重叠 + 共同冲击），600 只票的 G20 在同一天也是同涨同跌。有效独立样本数接近 **101 天 × 几**，不是 25755 × 612。所以：
- IC 层面：参数量随便加
- 收益层面：仍然只有 100 天的 regime，这个更多的股票救不了

所以搜索可以放开做，但**最后 30% 天数要留出来一次都不碰**，局部搜索的结果在留出段上 G20 没掉再算数。多拿几个月数据比任何搜索技巧都有用。

**接受改进要有门槛**。局部阶段只接受超过噪声水平的改进（比如前后半段分别算，两段都变好才算），否则会沿着噪声走到孤峰上。搜完看最优点邻域（d±5）是不是平台，不是平台就不信。

**5W 单档够用**。先用一档定结构，最后在 2W / 10W 复核一下最优 h 有没有漂移，漂得厉害说明这个因子的容量很窄。

算力没问题：Inspect 单因子 9 个 h 一次 10ms 级，几百上千个组合随便跑。

---

**User**

不是的， 我的主目标还是LS 收益， 这一步我们先不管空头，后面再去挑多头更好的， 这样是不是就好一点

---

**Cursor**

对，搜索阶段用 LS 更好，理由很实际：LS 用了两个尾部，信号量约是单腿的两倍，目标函数噪声小一半，局部搜索走得稳。多头筛选留到因子池层面做，是标准做法。

只加一条便宜的保险：搜索时把"年化 L"一起记下来，LS 主要靠空腿撑的组合（比如 L < 0.3 × LS）直接丢掉，别在它上面浪费局部搜索。小票空头端天然强（彩票股、动量崩），不设这条很容易搜出一堆 L 平平、S 很猛的参数，到多头筛选那步全军覆没。

其余不变：粗网格定 h → 固定 h 做 pattern search → 留出段验证。

---

**User**

别随便加， 我们一个因子可能要算上万遍， 要准确识别什么才是最重要的核心， 不能贪多

---

**Cursor**

明白。搜索循环里只有一个数：**5W 冲击下、最优 h 的净 LS 年化**。就它。

IC、L、mono 这些都不进循环，等上万遍跑完剩下几个候选，再人工看一眼。它们本来就在 HoldStat 里，不算也在那，但不参与决策。

---

**User**

但是IC也很重要， 这个肯定还是要算的， 不然中间乱了也不行， 我们最多允许里面20层里面乱个3～4层这种

---

**Cursor**

你描述的"20 层里乱 3～4 层"，对应的量是 `mono`，不是 IC。mono 就是 20 个组均值对组号的 Spearman，直接量的是层序乱不乱；IC 量的是全截面秩相关，中间几层对调它几乎不动。

换算一下：20 层里有 3～4 层各错位 2～3 个位置，mono 大约落在 0.95～0.97；只是相邻互换的话 0.99 以上。所以门槛定 **mono ≥ 0.95** 就是你说的容忍度。

IC 如果也要，同样当门槛（比如 ic_mean ≥ 0.02），不当目标。

两个都在同一个 HoldStat 里，一次 finish 全出来，不多算一遍。循环里的决策结构是：

- 目标：净 LS 年化（最优 h）
- 门槛：mono ≥ 0.95、IC ≥ 阈值，不过线 score = −∞

门槛不是"加指标"，是不让搜索往垃圾区走，反而省算力。

---

**User**

我看IC应该更好， 我实测mono还行的有些IC很低

---

**Cursor**

对，你的观察说明问题：mono 只看 20 个池化均值的**顺序**，不看幅度。沿 13000 行平均之后，组间差哪怕只有 1e-6 也能排得整整齐齐，mono 照样 0.95+，但每个 t 的截面关系其实很弱——这就是 IC 低。所以 mono 是个饱和了的量，当门槛拦不住东西。

IC 带幅度信息，而且它低的时候中间层一定乱。门槛就用 IC，mono 不要。

循环里：目标净 LS 年化，门槛 IC ≥ 阈值。两个数。

---

**User**

我不想要IC做阈值， 我想让它成为剃度下降的一部分

---

**Cursor**

那就用乘积，不要加权和：

\[
\text{score} = \text{LS}_{\text{net}}(h^*) \times \text{IC}(h^*)
\]

加权和要给两个单位不同的量配系数，配不出来；乘积只看排序，LS 乘个常数、IC 乘个常数排序都不变，不用对齐量纲。它的含义也直观：一步搜索被接受，当且仅当 LS 的相对提升超过 IC 的相对下降（反之亦然）——两边各让 1% 等价。

两个细节：

- 任一项 ≤ 0 → score = −∞。负 IC 乘负 LS 变正，必须拦。
- 想调偏好就 \(\text{LS} \times \text{IC}^{\lambda}\)，λ 是弹性，只有这一个旋钮，默认 1。λ = 0.5 就是"LS 让 1% 要换 IC 涨 2%"。

h\* 仍由粗网格阶段按净 LS 定，局部阶段固定 h\*，score 在固定 h\* 上算。

---

**User**

还有一个问题， 考虑 5000asset * 10年分钟线 * 10features per 因子的tensor基底
我们是CPU多核， 整个GPU 同时search一个因子好， 还是同时search多个因子好？

---

**Cursor**

先把量算出来，答案就出来了。

**基底放不进 GPU，所以必须沿 T 分块流式**

- T = 10 年 × 242 天 × 255 分钟 ≈ 62 万行，T × A ≈ 3.1e9 格
- 一个特征平面 (float + mask) 5 字节/格 ≈ 15 GB；10 个特征 150 GB
- 标签每个持有期 lv/sv fp16 + mask + ry 秩 ≈ 7 字节/格 ≈ 22 GB；9 个 h 近 200 GB

合计 300+ GB，任何单卡都装不下。只能按天切块（比如 20 天一块 ≈ 5100 行），块跑完换下一块。`Accum` 本来就是为这个设计的——可合并、跨块续攒，不落 Row。

**一旦是流式，瓶颈就是搬数据，不是算**

一块数据搬上去的代价（磁盘 → CPU 解码 → PCIe）远大于在它上面算一个因子。所以原则是**数据不动，候选批着过**：一块装上去，把手头所有待评估的候选全在这块上跑一遍，各自累加 Accum，再换块。候选越多越摊薄搬运成本。

**候选优先选同一因子的参数组合**

同一因子不同 d 的 DAG 前缀是共享的——`TsLog(close)` 对所有 d 只算一次，只有 `TsDeltaRoll(d)` 之后的节点分叉。同一因子的参数网格放一批，既摊数据又摊 DAG；不同因子只摊数据。

**但局部搜索是串行的，一个因子喂不饱 GPU**

pattern search 每步只有 2P 个邻居（P 个参数 ±1 步），几十个候选一批太小。所以同时跑多个因子的搜索，把它们各自当前的邻域拼成一批。

结论：**两者都要**——批内以同一因子的参数组合为主，多个因子的批拼在一起把 GPU 填满。粗网格阶段天然是单因子大批（几百个组合）；局部阶段靠多因子并行凑批。

**CPU 多核干什么**

- 下一块的 IO + 解码 + fp16 打包 + 预取（与 GPU 计算重叠，这才是真正决定吞吐的）
- 搜索驱动：每轮收 Accum、算 score、决定下一批候选
- 不要让 CPU 算因子本身，CPU 后端只留着对拍

**分块的两个坑**

- ROLL 窗要跨块 carry：块首要带上 max(d) 行的预热（TsRankRoll 的 d = 1275 最长），或者沿用 `Gpu.cuh` 现有的 chunk carry 机制
- rank-AC 的 lag = h，块首还要保留 h 行上一块的秩

块长选 ≥ 5 天 (1275 行) 的整数倍，预热开销就 < 一趟。

---

**User**

那局部也用网格呢？ 大网格， 中网格， 小网格

---

**Cursor**

更好，而且是这套硬件下的正解：每一层网格都是一个无依赖的大批，一次流式扫完，GPU 填满、CPU 预取不断流。pattern search 那种"算完这步才知道下一步"的串行性对流式架构是毒药。

设计上几点：

- **粗网格对数间隔**。窗长 d 的效应是对数的：5/10/20/40/80/160 比 10/20/30/40 覆盖得合理。
- **中网格别只围着 top-1 开**。粗阶段的最优很可能是噪声尖峰，取 top-3～5 各开一片中网格（±1 个粗步长，再切 5 点），一批跑完。谁的邻域是平台谁才是真的——这个平台判断在网格里是免费的，pattern search 还要另外做。
- **细网格到整数分辨率**，只围真平台开。
- **h 在粗阶段定下，中细阶段固定**。
- 参数维度 P ≤ 3 时每层 5^P ≤ 125 个点，三层加起来几百次评估，一个因子几分钟级。P = 4 开始 625/层，还能跑，但该先想想这个表达式是不是参数太多了。

成本对比：pattern search 总评估次数更少但串行几十轮；三层网格评估次数多几倍但只有 3 轮流式扫描。数据搬运是瓶颈时，后者快得多。

---

**User**

那就先做这个方案， 这样就能把CPU/GPU 一个因子跑满， 而不用考虑各种复杂的复用
那么算子层面是和stat共享比较好， 还是设计一个新的search算子比较好？

---

**Cursor**

共享 Stat，不另造 search 算子。理由只有一条但是致命的：搜索优化的数必须和 Inspect 里看到的数逐位同一口径，否则搜出来的最优在面板上对不上，你没法信任任何结果。`Contract.hpp` 里 rank → `finish_row` → `Accum::finish` 这条语义链一字不改。

要新增的不是算子，是**入口和驱动**，现有路径有三处不适合上万遍：

**1. `stat_eval` 每因子把 `Row[H][T]` 拷回宿主**

```78:81:cpp/include/factor/GpuRun.hpp
struct StatSession;
StatSession *stat_open(int T, int A, const factor::stat::Holds &hd, const StatLabelHost *lab, double *prep_ms = nullptr);
void stat_eval(StatSession *s, const DevPlane *x, const DevPlane *g, factor::stat::Frame f, factor::stat::Row *rows,
               double *eval_ms = nullptr);
```

Row 136 字节/行，一块 5100 行 × 9 h ≈ 6 MB，500 个候选一块就是 3 GB PCIe，比算还慢。加一个 `stat_eval_batch`：`label_rows` 的输出留在设备 scratch，紧接一个归约 kernel 直接出 `Accum[cand][h]`（Contract 里写明了 Accum 是 POD 可搬上设备），只拷 Accum 回来（每候选每 h 几百字节）。宿主跨块 `merge`，最后 `finish`。Inspect 需要 Row 画图，继续走老入口，两个入口共用同一组 kernel。

**2. DAG 按单因子评估，没有批内前缀共享**

现有 `run_ts_dev` / `plane_new` 已经是设备常驻中间量，够用。上面加一层：一个表达式 + 参数网格 → 按节点规范串（含参数）去重 → 拓扑序每个唯一节点算一次 → 各根喂 stat batch → 引用计数释放平面。一块 5100 × 5000 的平面 128 MB，80 GB 卡扣掉标签（9 h × 7 B/格 ≈ 1.6 GB）能同时活几百个平面，够一层网格。

**3. 没有沿 T 分块的驱动**

Inspect 是整个作用域一次装进来。搜索要按天切块流式，每块带 **1275 行 overlap**（= `kTsNormD`，也 ≥ 最长 hold T+5 的 lag），ROLL 预热和 rank-AC 的 lag 一并解决，Accum 只累加 overlap 之后的行。40 天一块 overlap 开销 12%。这比改 `Gpu.cuh` 里的 chunk carry 机制简单得多，也不碰现有算子。

5W 冲击的净标签平面每块算一次（`FactorsLoad` 的 CostPlane 已有），候选之间共用。

所以代码上是：Stat kernel 复用、Contract 复用、DAG 设备评估复用；新写 批量 stat 入口 + 设备端 Accum 归约 + 网格 DAG 去重 + 分块流式驱动。

---

**User**

这个共享方案真的是最干净， 最极致高效的嘛？
如果只是共享代码， 可以做一个header， 两个算子一起引用， 我需要最极致实现的search算子， 毕竟stat只是UI交互， search比它重上万倍

---

**Cursor**

你说得对，我之前把"共享语义"和"共享 kernel"混在一起了。

`Contract.hpp` 本来就是那个 header——文件头写的就是"CPU / GPU 两后端唯一共享件"，里面是 `r16_of` / `grp_of` / `pearson_int` / `finish_row` / `Accum` 这些纯语义。`Stat/Gpu.cuh` 里的 `rank_rows` / `label_rows` / `eval` 是按 UI 形状写的：一个因子、写 `Row[H][T]`、宿主汇总。Search 只依赖 Contract，kernel 从零按自己的访存模式写，最后用一个 search-vs-stat 的对拍（小张量、逐位一致，像现在的 op_check）保证两边是同一个数。这比复用 kernel 干净，依赖方向也对。

极致的 search 算子和 stat 的差别在哪，按收益排：

**1. 候选批 × 标签只装一次**
stat 每因子每 h 读一遍标签 (ry/lv/sv/m ≈ 7 B/格 × 9 h = 63 B/格)。search kernel 按 t 行组织：一行的 9 组标签装进 shared 一次，然后循环 C 个候选，每个只读自己的 rx (2 B/格)。标签流量被 C 摊薄，stat 侧访存降一个数量级。这是 stat 的 kernel 形状做不到的，也是单独写的核心理由。

**2. 根是 CsRank 时跳过 DAG 的 CsRank 节点**
现在每候选每行排两次序：DAG 里 CsRank 一次，stat 的 `rank_x` 在池上又一次。CsRank 单调，rank-over-pool(CsRank(x)) 和 rank-over-pool(x) 只差 CsRank 的桶近似造成的并列。search 直接在池上对 x 精确排一次。排序是 A=5000 时每行最贵的事，这一刀砍一半。代价：和 UI 的数有并列级别的微小差异，比 UI 更精确——你决定要不要接受。

**3. Accum 设备端归约，Row 不落**
每候选每 h 只回传几百字节。

**4. POINT 链融合**
TsLog → TsDeltaRoll 这类，逐点算子融进消费者的 load，省一次 128 MB 平面的读写。网格的共享前缀只算一次所以不急，但每候选专属的那段（参数节点之后到根）值得融。

**5. 双流流水**
下一块 H2D 和本块计算重叠；CPU 侧提前解码特征、算 5W 净标签、钉页内存。这是决定整体吞吐的,GPU 算得再快 PCIe 不重叠也白搭。

**6. rx 只为 rank-AC 落全局**
u16，lag 最长 1275 行；其余统计量在 shared 里当行算完就扔。

共享的部分：`Contract.hpp` 全部 + `Gpu.cuh` 里那组 `dev::` 的 `__device__` 版（`pearson_int` / `finish_row` / `ls_of`），这些现在是 stat 私有的，抽成一个 `Stat/Dev.cuh` 两边 include。

---

**User**

可以的， 两边都干净实现， 尤其是search， 要极致实现
现在开始做， 在UI上选定一个因子后， 点optimize， 就能针对这个因子， 在对应的后端（CPU/GPU）跑满用最快的速度找到最优参数并且更新到因子表达式里面

---

**Cursor**

这是个大活，先把现有架构摸透再定方案。我先读一圈相关代码。

---

**User**

你先写算子把， 剩下的业务代码在算子做干净之后再做

---

**Cursor**

Now I have what I need. First, extract the device-side Stat contract replicas into `Stat/Dev.cuh` so Search can share them without pulling in Stat's kernels.

Now remove the moved block from `Stat/Gpu.cuh` in place.

Now delete the duplicated `dev` block (lines 144–234) from `Stat/Gpu.cuh`.

Now the Search contract header.

Now the CPU backend.

Now the GPU backend.

Now the host API: `GpuRun.hpp` declarations, the `.cu` implementation and the stub.

Now the `.cu` implementation.

Now the parity-check header and the `op_check` section.

算子写完了，没编译（按你的规矩）。改动一览：

**共享契约**
- `factor/Stat/Dev.cuh`（新）：从 `Stat/Gpu.cuh` 原地抽出的 `__device__` 契约公式（ord / r16 / grp_of / pearson_int / finish_row / 二分），Stat 与 Search 两个 GPU 算子只经它与 `Stat/Contract.hpp` 相见，核互不可见。
- `factor/Search/Contract.hpp`（新）：`CostAcc`（顶组买 / 底组卖两格池化，与 Inspect `buy[K-1]/sell[0]` 同口径）、`Result{Accum[H], cost}` 可 merge、分块流式语义（`t0_abs` / `t_lo` / carry ≥ `max_lag`）、`score_of`：h* = argmax 净 LS，score = ls_net·ic^ic_pow，任一 ≤ 0 → −∞。

**CPU** `factor/Search/Cpu.hpp`：候选级并行（`Exec::tasks` 抢单，算子内单线程）。每行一次排名产出 perm / xr / 组边界，组和是连续区间顺序累加（无 20 路散射），IC / AC / 成本全是 perm 上的 gather；lag 行走 `max_lag+1` 行的环，不留 `[T][A]`。收尾 `finish_row → Row → Accum::add`，公式同一份。

**GPU** `factor/Search/Gpu.cuh`：`rank_batch` grid (T, C)；`stat_batch` grid (⌈C/8⌉, NB, H) 一 warp 一候选、同 block 8 个候选共用同一 (t,h) 的标签行，Row 直接折进 warp 私有分片 `Part[c][h][tb]`（lane 0 标量，lane k 组 k），成本只在 h 序 0 的块攒；`reduce_parts` 沿 tb 固定序归约，无原子。`Work/plan` 给工作区尺寸，`to_result` 搬成 Result。

**宿主接口** `GpuRun.hpp` + `op_check_gpu.cu` + stub：`search_open / search_load(每块标签+成本, ry 预处理) / search_eval(DAG 根槽指针批) / search_close`，和一次性 `run_search`。

**对拍** `factor/Search/Check.hpp` + `op_check.cpp` 新节 `--op Search`：参考 = Stat CPU 的 HoldStat + 朴素成本池化；CS/TS × 6 profile × 3 候选（x, −x|1−x, x²），cpu / gpu 各比"整块 vs 参考"与"两块带 carry merge ≡ 整块"。

跑法：`op_check --op Search [-v]`（-v 顺带打每候选的 score / h* / 成本两格）。

