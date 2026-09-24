# Factor display terminal design
_Exported on 9/24/2026 at 18:10:26 GMT+8 from Cursor (3.21.18)_

---

**User**

@README 
现在我们要设计新的因子展示终端了
1. 先把factor那里换成更清楚的方法, 再说后面的
每个 factor的编号换成可以打对钩的小方框, 
打钩 (同一时间只有一个)= 选中因子来做之前的编辑/add模式

之前的点选行(高亮) = 给后面的单因子展示用
factors后面新增一页, 叫啥你觉得合适, 用来画单因子的分层等画, 先只建立空白页

---

**Cursor**

Now the table in `TabFactors.cpp`.

Header/tooltips for column 0 and 1:

Now `TaskFactors.cpp` and CMake.

改完了, 请编译看效果:

- `TabFactors.cpp` 表格: 首列 `#` 编号 → `edit` 勾选框 (同时只勾一个; 勾 = 载入构建器进编辑模式, 取消勾 / 勾另一行 = 退出 / 切换), 编辑中的行 file 名标橙; 点行高光 → 新状态 `ui.view_file` (与编辑模式互不干涉, 再点取消)
- `TabFactors.hpp`: `FactorsUIState` 加 `view_file`
- 新页 **Inspect** (`TabInspect.hpp/.cpp`, 已挂进 `TaskFactors.cpp` 三个 tab + CMake 两处): 目前只显示高光因子的 文件 / 规范串 / note, 没选则提示; 与 Factors 页共用同一个 service (进 Inspect 也会触发首次扫描)

README 文件树里未加 `TabInspect.hpp` 一行, 按你的规则没动文档, 需要的话你自己补.

---

**User**

对于单标的的时序(择时)因子, 我们也能做分层嘛? 也去优化LS return?

---

**Cursor**

可以, 但"分层"的轴要换, 而且 LS 的含义会变. 先说现状: `factor::stat` 全部是截面口径 —— 每分钟 t 沿 A 轴 rank x → 20 等分组 → `grp[k]` = 该组做多标签均值, `ls` = 最高组做多 + 最低组做空. 单标的 (A=1) 每行 n<2 直接 `kRankNone`, 整个 Stat 是空的; 就算多标的, 一个纯 TS 因子 (根是 `TsRank`/`TsZ` 而非 `CsRank`) 被截面 rank 一遍后, 时序信息大半被扔掉, 现有的 IC/LS 其实是在评"这个 TS 值在截面里的相对高低", 不是择时能力.

**TS 因子的分层怎么做**

- 分组轴从 A 换成 T: 每个标的自己的历史分位. 最省事的路径是不在 Stat 里 rank, 而是要求 TS 因子的根自己归一到 [0,1] (`TsRank(…, d)` 之类, 滚动窗内分位, 无前视), Stat 直接 `grp_of(x)`, 然后把所有 (t, a) 池化到 20 组 → `grp[k]` = "因子处于第 k 分位时的条件前向收益". 分层图就是这 20 根柱 + 单调性, 和 CS 版长得一样, 只是含义变成条件期望.
- 全样本 rank (expanding / 用未来分布) 只能作 in-sample 诊断, 不能进可交易口径; 滚动 `d` 是必须的, 而且 `d` 本身成了择时因子的核心参数.
- TS-IC = 每个标的沿 T 的 Spearman(x_t, y_{t+h}), 再跨标的平均 (或池化). 自相关很重, `n/h` 折算照样要, 有效样本会比 CS 少一到两个量级 —— t 值会很难看, 这是择时的本性, 不是 bug.

**LS 能不能优化 —— 能, 但要改定义**

- CS 的 LS 是多空对冲, 市场中性; TS 的"最高组做多 + 最低组做空"是方向性仓位, PnL 被 β 主导. 现有的 `mkt` / `beta` 列正好是解药: TS 口径的目标应是超额 (`grp[k] − mkt`, 即择时相对于一直持有的增益) 或 long/flat 权益曲线的 Sharpe, 而不是裸 LS 均值. 否则优化器只会学一个"永远看多"的因子.
- A 股单票不能融券, 所以 TS 择时实际能做的是 long/flat (top 组做多, 其余空仓) 或 long/short 对 `mkt`. 我建议报两个: `ls_ts` = top 组 − bottom 组 (对称, 看信号强度) 和 `excess` = top 组 − mkt (可交易).
- 因子方向在 TS 里是有意义的 (CS 里翻个号就行), 优化时不能取 |IC|.

**需要改的东西 (若要做)**

1. Stat 加一个模式 `CS | TS`: TS 模式跳过 `rank_cs(x)`, 直接 `grp_of(x)` (断言 x ∈ [0,1] 且有效), Row 沿 (t, a) 池化而不是每 t 一行; `ac` 改成 lag h 的时序自相关 (含义反而更直接). CPU/GPU 两后端 + 对拍都要跟.
2. 因子文件多一个键标明口径 (或按根算子自动判: 根是 CS 域 → CS, TS 域 → TS).
3. 标签侧不用动: `lv/sv/m/mkt` 都可复用.
4. Inspect 页两种口径画法不同: CS 画 20 组累计净值 + LS 曲线; TS 画条件收益柱 + long/flat vs buy&hold 权益曲线.

结论: 分层能做且很有用 (它就是"因子处在什么区间时该不该持仓"的直观图); LS 优化能做但目标要换成超额/long-flat Sharpe, 直接搬 CS 的 LS 会被 β 绑架. 要不要现在先把 Stat 的 TS 模式设计出来, 还是先把 Inspect 页 CS 分层图做完再说?

---

**User**

t值的定义是啥, t值和这个择时分层, 你觉得哪个作为主力指标更合适

---

**Cursor**

**t 值是什么**

代码里两处 (`Contract.hpp` 二级汇总):
- `ic_t = icir · √(n/h)` = `ic_mean / (ic_std / √n_eff)`, 即"IC 的均值离 0 有几个标准误". 
- `ls_t` 同式, 对象换成多空收益序列.

统计含义: 原假设"这因子平均没信号", t 越大越难用运气解释, |t| > 2 ≈ 5% 显著. `n/h` 折算是因为相邻 h 行的标签是同一段收益 (重叠持有期), 独立样本数不是 n 而约为 n/h, 不折算 t 会虚高 √h 倍. 注意它回答的是**有没有**, 不回答**有多少**: 一个 IC 均值 0.003 但极稳的因子 t 可以很高, 却覆盖不了成本.

**择时场景下两者的问题**

t 值的弱点:
- 有效样本比 CS 少一到两个量级 (每个 t 不再有 A 个独立截面样本), 且 `n/h` 只修正持有期重叠, 修不了因子本身的低频自相关和 regime 聚集 —— 择时因子往往几段行情贡献全部收益, 真实 n_eff 比 n/h 还小, t 系统性偏乐观.
- IC 是全分布的单调关联. 择时信号通常只在尾部有用 (极端分位才该动), 中间 80% 是噪声, IC 被稀释, t 跟着弱, 但这不代表不能用.
- 作为搜索目标会偏好"小而稳"的效应, 对择时来说恰好选错方向 (要付成本, 要的是大而不太脏).

分层的弱点:
- 也是池化, 同样受 regime 聚集影响: top 组均值可能被两三天撑起. 单看柱状图没有置信度, 容易被漂亮的单调形状骗.
- 对 `d` (滚动分位窗) 和分组数敏感.

**我的建议: 分层为主, t 为闸**

择时的主力指标应从分层派生, 因为它直接对应可交易规则:
1. **top 组超额** = `grp[K−1] − mkt` (或 long/flat 相对买入持有的 Sharpe): 有量纲, 能对成本, 能画权益曲线.
2. **mono** (组均值对组号的 Spearman): 保证不是靠一根柱子, 形状是可信的.
3. 对这个"超额"序列再算 t (按日聚簇 + n/h), 作为**准入门槛**而不是目标: t < 2 的直接不看, 过了闸再按 1、2 排序.

也就是: t 值继续算, 但从"排名依据"降级为"过滤器"; IC/ICIR 在择时里退居诊断. CS 那边不用动, `ic_t / ls_t` 仍适合做主排序, 因为截面样本充足、假设也大致满足.

另外一个实操点: 分层稳健性可以用"按日分块的 top 组超额 > 0 的天数占比" (对仗现有的 `ic_pos`) 补一列, 比 t 更抗 regime 聚集, 建议在 TS 模式的 HoldStat 里一并加.

---

**User**

1. 需要怎么判断一个因子是时序因子, 还是截面因子? 是不是看最后一个算子的类型?
2 截面的算法, 可以很自然映射到仓位上, 本身评估也很合理, 有很好的抗过拟合效果 所以时序是不是也应该有类似效果, 
因为时序可能是要给出暂时多/少配置百分之多少的仓位, 是不是也应该做成类似时序分层的东西来更好的映射仓位, 同时以抗过拟的方法来构造因子, 但是择时因子只对自己(单标的)有效, 是不是应该对自己标准化因子值到 -1~1, 然后我们预期因子值能够排序未来收益, 因为标的很多, 可以平均化, 这样也可以做出来有意义的IC分层, 效果和截面类似, 里面一个关键参数是标准化的window长度

均线类的过拟是不是本质上也是这类原因, 我们要拟合的是map而不是单点

这么定义后, 不管是截面还是时序, 因子的核心任务, 其实就是把分层累计IC撑开, 但是区别是截面因子随着撑开可以同时把票池beta隔离出来, 时序的则做不到, 哪怕做出来干净的map, 也没办法保证仓位的平稳变动, 这个其实也是没问题的?

---

**Cursor**

**1. 怎么判断 TS / CS**

看根算子的 `A` 轴基本对, 但要更严一点: 决定"坐标系"的是**最外层的归一算子**, 不是任意根. `CsRank(TsMean(x))` 是 CS; `TsRank(CsRank(x), d)` 是 TS (每票自己历史里的相对位置); 而 `TsMean(CsRank(x), d)` 根是 `A::SELF` 却仍是 CS 尺度 (值域 [0,1], 跨票可比), 按根判会错. MAP 核 (log / abs / 加减) 不改坐标系, MOMENT / ORDER 核才定.

建议直接把规则做成约束而不是推断: 因子文件的根必须是归一算子 (CS: `CsRank/CsZ…`, TS: `TsRank/TsZ/TsPct…`), 解析时断言, 口径 = 根的 `A`. 这样 TS 模式的 Stat 才能拿到已知值域的 x 直接 `grp_of(x)`, 不用再 rank 一遍; 现在 CS 那边 Stat 本来就重排, 加这个约束对它零影响.

**2. TS 分层 = 自身滚动标准化后池化 —— 对, 而且可以说得更清楚**

把 x_ta 拆成 x̄_t (当天所有票的共同分量) + (x_ta − x̄_t) (截面分量). 现在的 CS Stat 每 t 重排, 恰好把 x̄_t 全部扔掉, 只评截面分量, 所以 β 被隔离是副产品. TS 模式不重排、按自身历史分位池化 (t, a), 保留了 x̄_t, 评的是"市场择时 + 个股自身状态择时"之和. 两者是互补的分解, 不是谁替代谁; 同一个表达式换根算子就能分别评, 值得做成两个口径并列.

"标的多可以平均化"要打个折: 择时的共同分量让同一 t 上 A 个样本高度相关, 有效样本数接近 T/h 而不是 T·A/h. 池化能把分层柱子画得很平滑, 但 t 值必须按 t (按日) 聚簇算, 否则会被 √A 虚高. 这是 TS 与 CS 在统计上的本质差别, 比 β 那条更要命.

window 是核心参数没错, 它就是"状态"的定义 (相对最近 d 分钟还是最近 20 天), 和 h 要拉开 (d ≫ h), 不然分位本身就在追随标签.

**均线过拟的根源**

是的, 而且有两层: (a) 均线策略拟合的是离散事件 (金叉/死叉那几个时刻、阈值), 每个参数组合的有效样本 = 交叉次数, 几十个; 分层 map 用全部样本, 且单调性是很强的正则. (b) 均线差没有归一, 是绝对量纲, 跨票跨时期不可比, 参数隐含在适应某一段波动率. 先 `TsRank/TsZ` 再看 map, 两个问题一起消掉. 你说的"拟合 map 而不是单点"就是这个.

**3. 核心任务 = 撑开分层; TS 隔不开 β, 仓位也不平稳 —— 可以接受吗**

可以, 但要配三件事:
- 评估基准换成超额: `grp[k] − mkt`, 或 top 组 long/flat 对 buy&hold. 不然趋势样本里"x 越高越看多"会被 β 白送一个漂亮的单调 map.
- 仓位平稳不是有效性问题, 是成本问题, 现有 `rank_ac` (lag = h) 就是它的度量; TS 模式下它变成自身时序自相关, 含义更直接. 用 window / EMA 调平滑度, 拿 ac 定成本, 在 Stat 里给出即可, 不需要在因子定义里强求.
- 仓位映射: 分层 map 给的是"状态 → 期望超额", 从它到仓位是单调变换 (比如 20 组 → −1..1 线性, 或只在尾部 ±), 这一步放在组合层, 因子只负责把 map 撑开撑直. 和 CS 完全对仗.

一句话: TS 因子 = 自身滚动归一 + 池化分层 + 相对 mkt 评估 + 按日聚簇的显著性; 做不到 β 隔离是定义上的, 不用补救, 但样本量的假象一定要修.

---

**User**

1. 如果有些TS算子包着CS, 那我们的因子值=仓位的原则还能用嘛,还能干净剥离beta嘛, 是不是还是应该严格
2. 对于TS, 确实应该直接减去均值, 虽然信号还作用在单标的上, 但是已经剥离了beta, 直接预测超额
3. 是不是对于截面是不是也应该直接减去平均收益, 也只预测超额, 这样更统一,干净
4. TS的滚动周期应该怎么定, 应该和预测周期有关?

---

**Cursor**

**1. TS 包 CS: 原则失效, 应严格**

"因子值 = 仓位"在 CS 下成立的前提是根的输出每个 t 截面居中 (rank 和为常数 → 多空金额对冲 → β 隔离). `TsZ(CsRank(x), d)` 或 `TsMean(CsRank(x), d)` 的输出每个 t 截面不再居中: 每票对自身历史归一, 整个截面会一起偏高或偏低, 这个共同分量就是 β 漏点. 现在 Stat 每 t 重排相当于在根上再套一层隐式 `CsRank`, 评估结果和"按因子值直接下仓"就不是一回事了 —— 评的是一个东西, 交易的是另一个.

所以严格: CS 因子的根必须是 `A::ALL` 归一算子. 想对 CS 信号做时序平滑, 平滑放里面 (`CsRank(TsMean(x, d))`) 或外面再套一层 (`CsRank(TsZ(CsRank(x), d))`). 与第 2 点合起来, 规则就是"最外层归一算子定口径, 且必须是最外层".

**2. TS 减均值预测超额: 对, 但要认清代价**

标签换成 y − ȳ_t 后, 每个 t 上 Σ(y − ȳ) = 0, 因子的共同分量 x̄_t 对超额的贡献恒为零 —— 池化回归等价于对 (x − x̄_t) 做. 也就是说**市场择时分量被完全扔掉了**, 剩下的是"个股处于自身历史高位时, 是否跑赢市场". 这是好事: β 干净, 样本量恢复到 T·A/h (共同分量正是让样本相关的东西), 不再需要按日聚簇. 但它其实已经是"用自身历史归一的 CS 因子", 只是评的是绝对水平而非截面名次.

一致性要求仓位也居中: w_ta = x_ta − x̄_t. 否则实际 PnL = Σ x(y−ȳ) + x̄·A·ȳ, 评估丢了第二项. 真正的指数择时 (x̄_t → mkt_t) 是单序列问题, T/h 个样本, 应该单独做 (对指数 / 池均值), 不要混进个股因子评估里.

**3. CS 也减均值: 该做, 且几乎免费**

rank 类统计对 y 平移不变: IC / mono 不变, LS = top − bottom 本来就消掉了 mkt, beta 的回归量也不变. 唯一变化是 `grp[k]` 从绝对收益变成超额, 20 根柱子围着 0 —— 这正是 Inspect 页想看的. 建议 Stat 一级 Row 里 `grp[k]` 统一存超额 (减去该 t 上 J 集的 lv 均值), 两口径同一份代码, 分层图同一种画法.

一个细节: 多空标签成本不同 (lv / sv), 减的是 lv 的均值; bottom 组用 sv 时 LS 里保留了这个不对称, 是对的 (实盘口径).

**4. 滚动窗 d 与预测期 h**

三个约束夹出来:
- 下界 d ≫ h: 窗内要有足够多相互独立的 h 段, 不然分位被正在预测的那一段主导 (刚涨完必然在高分位 → 伪均值回归). 经验 d/h ≥ 20; h = 30m 就至少 2–3 个交易日.
- 下界二 d ≥ 原始特征的自身周期: 否则 rank 永远在中间, map 撑不开.
- 上界: 平稳性. d 太长 rank 退化成绝对水平, 跟着慢趋势跑, ac 极高, 换手看似低但暴露成了 β 的近亲.

实操: d 做成搜索参数, 对数网格 (2h … 200h), 断言 d/h 下限; 选 d 看 mono / 超额随 d 的曲线取**平台**不取峰值 (峰值 = 过拟). 另外日内数据跨日滚动会混进日内季节性, d 最好以整交易日计 (kSegLen 的倍数), 或者归一前先过 `TsTodMask` 同时段.

---

**User**

先简化一下, 我们主要是分钟级别, 为了deseason, 我们win应该选整数天数, 就用5天作为rolling window好了, 后面有问题再说

基于上面的思路, 重新干净, 准确, 对仗实现stat等代码

我们因子的类型: 应该是alpha类(上述)和beta类, beta类先不实现, 留格式空位
alpha类里面再分TS和CS类, 然后按照类型计算对应的评估参数, 用来给inspect模块以后用

---

**Cursor**

**Stat 重构设计确认**

> **TS 口径的每 t IC 怎么定? (二级 ic_mean/icir/ic_t 需要时间序列)**
> - Pearson(量化后的自身分位 r16_x, 截面秩 r16_y) —— 整数五和路径完全复用, 与 CS 只差 x 不排序 (Recommended) ✔
> - 不算每 t IC, 只算全样本池化的 Pearson(x, 超额) 一个数 (丢 icir / ic_t)

> **TS 口径每 t 的 ls 定义?**
> - top 组 lv 超额 + bottom 组 sv 超额, 空组项取 0 (与 CS 的 top(lv)+bottom(sv) 逐字对仗) (Recommended) ✔
> - 只用 top 组 lv 超额 (纯 long/flat, 空 → 0)

> **CS 的 grp[k] 也改成沿 t 池化 (Σ和/Σ计数), 与 TS 共用一份 summarize?**
> - 是, 两口径同一份代码 (与现状只差按每 t 有效数加权) (Recommended) ✔
> - 否, CS 保持组均值再时间平均, summarize 按 Frame 分叉

> **因子文件 type 键缺省处理?**
> - 缺省 = alpha, Add/Save 一律写 "type": "alpha" (老文件不破) (Recommended)
> - 必填, 缺 → BROKEN (老文件需手补)
>
> Answer: B, 你帮忙处理一下

> **Operators 页首行 Stat 与 op_check 的对拍是否两口径都跑?**
> - 都跑: CS + TS 各一遍, Diff 合并到同一格, tooltip 分两段 (Recommended) ✔
> - 只跑 CS (TS 路径只靠 op_check 覆盖)

Design confirmed. Starting with the contract.

Now constants, Frame, Row/HoldStat, quantizer, summarize.

Now `summarize` (pooled grp + ls_pos):

Now `Cpu.hpp`.

Now the kernels: quant_rows + label_rows.

Now `StatJson.hpp` (+`ls_pos`) and `Expr.hpp` (`root_frame`).

Also update the builder tooltip text, then the table columns.

Now the sort keys and row cells (shift by one, add frame column).

改完, 请编译 + 跑 `op_check --op Stat -v` 对拍 (CS / TS 各一遍), 再进 Factors 页 Run.

改动要点:
- `Stat/Contract.hpp`: `Frame {CS, TS}`, `kTsNormDays = 5` / `kTsNormD = 1275`; `r16_quant` (double 内量化, 与 FMA 无关); `Row.grp` 改为组**和** + `cnt[]`; 收尾统一进 `finish_row(Frame, …)` (TS 减截面均值 / 允许空组); `summarize` 组沿 t 池化, 加 `ls_pos`.
- `Stat/Cpu.hpp` / `Gpu.cuh`: `eval(…, Frame, …)`; 阶段 1 分叉 (排序 / `quant_rows`), 阶段 2 多累一个 Σsv, 收尾调 `finish_row` (GPU 逐字 `__device__` 版). `GpuRun.hpp` / `.cu` / stub: `run_stat` / `stat_eval` 带 Frame.
- `Stat/Check.hpp`: `Data.frame`, `make(d, frame, …)` (TS x = Φ(z)), `compare_rows` 比 cnt 逐位; `op_check` 与 Operators 页 Stat 行两口径都跑 (Diff 合并, tooltip / operators.json 分 `holds_CS` / `holds_TS`).
- `Expr.hpp`: `root_frame()` 严格判根 (CsRank/CsNormRank/CsZ → CS; TsRankRoll(d=1275) → TS; 其他报错), `static_assert(kTsNormD == kMaxD)`.
- `FactorsService`: 文件 `type` 必填 (alpha | beta; beta 标 error 留位), 扫描 `root_frame` → `FactorRow.frame`; stat 落盘带 `frame`, 载入口径不符则忽略; Add/Save 写 `"type": "alpha"`; 评估按根的口径调 Stat.
- `TabFactors`: 表加 `frame` 列, 构建器 expr 行显示 `[CS]/[TS]` 并即时校验根; 根非归一的 BROKEN 行也能勾选载入 (套个根即可).
- 7 个现有因子文件已补 `"type": "alpha"`. 其中 `maker_bid` / `open_imb_cum` / `tod_mask` 根不是归一算子, 会显示 BROKEN "根须是归一算子", 需要你决定套 `CsRank` 还是删; 旧 stat 块因缺 `frame` / `ls_pos` 会被忽略, Run 后重写. 旧 `operators.json` 同理会被判废重算.

---

**User**

还是有3个因子是broken的
你现在保证CS和TS的评估都符合我们前面的商量嘛? 都是超额语义的是吧

---

**Cursor**

CS 那边**不是**超额 —— 我在 `finish_row` 里只给 TS 减了均值, CS 组和 / ls 还是原值, 与之前商量的"两口径统一超额"不符. 现在改成统一: grp[k] = Σ(lv − mean_J lv), ls = top(lv − l̄) + bottom(sv − s̄), 两口径同一式; 唯一分叉只剩空组规则 (CS 任一组空 → 行无效; TS 允许空, 空项 0).

Now the contract text and UI tooltips that still say "CS 标签原值".

现在两口径全部超额: `grp[k]` = Σ(lv − mkt), `ls` = top(lv − l̄) + bottom(sv − s̄), CS / TS 同一式 (`finish_row` 主机 + device 版都改了); 无信号时 ls ≈ 0 (原值口径会 ≈ −市场往返成本). IC / mono / beta 对平移不变, 不受影响. 请重编 + 重跑 `op_check --op Stat`.

3 个 BROKEN:
- `open_imb_cum`: 已套 `CsRank(...)` (params 前补一项, note 加"截面秩", 旧 stat 删), 重扫即 CS 有效.
- `maker_bid` = `TsGt(vol4_maker_bid, k=2)` 是 0/1 掩码, `tod_mask` = `TsRelu(TsTodMask(...))` 每 t 截面全相同 —— 都不是 alpha 因子, 套 CsRank 也只会永远 "no stat" (全并列 / 20 组填不满). 建议删掉这两个文件; 删除不可逆, 你确认我再删 (或你在表里勾选 → 删除).

---

**User**

那这里的表达式再提交, 能不能被parser直接拦住

---

**Cursor**

能, 而且不用碰语法层 —— 加在 `root_frame` (因子文件 / 构建器共用的那一个判据) 里就行. 这三个文件的退化各有静态特征:

- `TsGt(...)` 根输入是 0/1 列 (`Kind::INT`): 分位只有两档, 20 组永远填不满
- `TsRelu(TsTodMask())` 整个表达式不含任何特征叶: 每 t 截面全相同
- 同类还有 `CsRank(CsMean(x))` 这种根输入是截面广播量 (CsMean / CsStd / CsQuantile) 的, 也是截面常数

`root_frame` 现在多拦三种退化, 构建器即时校验、Add / Save、文件扫描同一判据:

- 不含特征叶 → `tod_mask` 这类 (每 t 截面全相同)
- 根输入是 `Kind::INT` 列 (TsGt / TsTodMask / CsBucket) → `CsRank(TsGt(...))` 这类 (提示先套窗口统计再归一)
- 根输入是截面广播量 (CsMean / CsStd / CsQuantile)

构建器里选出这样的树, expr 行直接红字报原因, Add 按钮不亮. 那两个旧文件重扫后仍是 BROKEN, 原因会变成上面的提示, 删不删你定.

---

**User**

你现在的检查是做的干净的类似公式的general的检查是吧
其实我们每个算子的自变量和因变量都已经定义了取值范围, 那么是不是直接靠取值检查就能干净高效查出来?

---

**Cursor**

不完全干净: 现在是硬编码名字列表 (三个根名 / 三个广播名 / `kind_of` 里三个整数算子), 不是从算子定义推出来的. 你说的方向对, 但只靠取值范围抓不全, 因为两类退化本质不同:

1. **取值退化** (分位只有几档): `TsGt` / `TsTodMask` 输出 {0,1}, `CsBucket` 输出 0..k−1 —— 这是值域属性, 用输出域检查能干净判.
2. **变异退化** (每 t 截面全相同): `TsRelu(TsTodMask())` 值域是 ℝ≥0 没毛病, 问题是它沿 A 轴不变; `CsMean(x)` 值域 ℝ 也没毛病, 但它把 A 轴变异抹掉了. 这是结构属性 (输出是否随资产变), 值域看不出来.

所以干净的做法是给每个节点推一个小"类型" = (输出域, 沿 A 是否变异), 两者都从算子表推, 不再列名字:

- OpTable 加一列机器可读的输出域 `Out ∈ {REAL, NONNEG, UNIT[0,1], SIGNED[−1,1], BIN{0,1}, INT, BCAST}` (现在 operand / op 列是 LaTeX 只给人看; BCAST = 截面广播量 CsMean / CsStd / CsQuantile). `kind_of` 那个手写函数随之删掉, INT/BIN 直接查表.
- 沿 A 变异按结构推: 特征叶变; 算子节点 = 非 BCAST ∧ 任一输入变. 不用加列.
- 输入域要求只有少数算子真有 (TsLogRatio 要 >0, TsProductRoll 要 >−1, TsEntropyCum / TsHhiCum 要 ≥0…), 像 `kKRules` 那样一张小表按名声明, parse 时对输出域做包含检查. 特征叶的域现在 `FeatureLookup` 不返回, 只能当 ℝ —— 所以这层是"能判则判", 主要抓 `TsLogRatio(TsSub(…))` 这种明显错.
- `root_frame` 的判据就变成三句话: 根是归一算子 (CS / TS 那四个仍按名, 这是口径定义不是退化) ∧ 根输入沿 A 变异 ∧ 根输入域 ∉ {BIN, INT}. 名字列表全部消失.

代价: X 宏多一列, 所有展开点 (`FACTOR_EXPR_OPINFO` / OperatorsService 的 RUN_* / TabOperators 静态列 / op_check) 都要跟着补一个占位参数, 大约六七处; 收益是以后加算子不会漏检. 要不要按这个改? 输入域那张小表可以先不做 (纯加分项), 只上 `Out` 列 + 变异推导.

---

**User**

时代, 按照这个改最干净, 既然已经定义了自变量的范围, 紧接着定义因变量范围
我希望parser能最大程度和算子本身解耦, 而是用一个干净的flow解决

---

**Cursor**

Start: `Dom` in the contract, then the table.

Now the consumers. Expr.hpp first — read the relevant parts.

Now edit Expr.hpp: OpInfo, remove `Kind`, table-driven checks.

Check `Node` field names (`feat`, `args`) and the TabFactors usage.

---

**User**

那你因变量是不是也直接处理成Dom, 反正都已经struct化了
UI里tick后面就应该先显示因子type, 是alpha还是beta
现在啊alpha例子都是CS的, 你再补充几个TS的

---

**Cursor**

Now OperatorsService (row struct + ROW) and the Operators table (column + json).

**自变量也 Dom 化?**

> **因变量 (out) 已经是 Dom 列了 (BIN / SIGN3 / BCAST … 就是它). 你说的"也处理成 Dom"是指自变量 (operand 的值域) 也加一列机器可读的 Dom, 让 parser 顺带查 子节点 out ⊆ 本算子 in? 若是, 我建议:**
> - 加一列 in (单个 Dom, 对全部序列元; 组 id 元由 A=GROUP 隐含须 BIN/INT); Dom 补 POS (>0). 特征叶未知 → 放行; WMean 权重列妥协写 REAL (Recommended)
> - 每元一列 (in_x / in_y / in_z), 严格逐元, 表宽 +3 列
> - 不做, out 列够用; operand 列 LaTeX 继续只给人看
>
> Answer: 对呀, operand就是算子输入, range就是算子输出, 你都设计成struct形式的来统一定义, 统一检查, 因为前一个算子的range可能就是后一个算子的operand, 然后算子看板的地方都统一处理变成可渲染的公式, 这样最大化做干净做对仗, 代码里不应该有算子耦合的代码, 应该都是通用的统一处理

Now the OpTable header doc.

Now `group_arg` removal and TabFactors slot filtering by `in` domain.

完成. 摘要:

**OpTable 行格式** → `X(e_name, c_name, 元数, T, A, 核类, k域, in, out, operator, 备注)`
- 删掉手写的 `"参数"` 串和 operand LaTeX; 参数集合由 `(T, k域)` 推出 (`d ⇔ ROLL`, `k ⇔ k域≠NONE`, `k2 ⇔ TOD`), `kKRules` 名字表删除, k 值域进表 (`KDom` 列).
- `in` 逐元 `OP_IN0..3(Dom…)`, `out` 单个 `Dom`; 同一 enum (`REAL/NONNEG/POS/UNIT/SIGNED/BIN/SIGN3/INT/BCAST`, Contract.hpp), 带 `dom_sub / dom_name / dom_tex / dom_holds`.
- 签名 LaTeX 由 `expr::operand_tex(OpInfo)` 生成 (Operators 表 operand 列 / operators.json), out 列按 `dom_tex` 渲染; 旧 operators.json 因静态列不符会自动判废重算.

**parser 统一 flow** (Expr.hpp 不再点任何算子名, 只剩根归一算子集 = 口径定义)
- 每算子节点逐元: `in` 为严格域 (`INT`, 越界 = 算子内 assert) 时要求子算子 `out ⊆ in`, 特征叶留到 eval 前按数据 `dom_holds` 查 (FactorsService `check_leaf_domains`, 取代原组 id 专用检查). 非严格域 (LogRatio 的 ℝ>0 等) 是语义声明: 越界格算子本就按【退化】置无效, 静态否决会把 `TsLogRatio(TsSumRoll(…))` 这类正常表达式误杀, 所以只渲染不否决 — 这是我做的一个取舍, 你看是否认可.
- `root_frame` 退化判定 = `dom_discrete(根输入.out)` + 结构推 `varies_a` (BCAST 抹平 / 无特征叶).
- 编译期 `table_consistent()` 核对 GROUP 末元 `in=INT`、TOD 只在 0 元 POINT.
- 构建器槽位过滤同规则 (`dom_strict(need) → out ⊆ need`), 槽名悬停显示该元值域.

**UI**: Factors 表 edit 后新增 `type` 列 (alpha / beta, 可排序); 新增 4 个 TS 因子文件 `ts_mom_30m / ts_taker_imb_30m / ts_amt_burst_10m / ts_vwap_dev_60m` (根 `TsRankRoll(d=1275)`).

未动: Check.hpp 的 `kDParams / kKParams` (op_check 每算子默认测试参数, 按名索引) — 那是测试数据不是逻辑, 要不要也进表你定. 请编译并跑 `op_check` + Operators / Factors 页.

---

**User**

@/home/chuyin/.cursor/projects/home-chuyin-work-stk/terminals/7.txt:48-79 

---

**Cursor**

宏形参名 `out` 与 `G_TS/G_CS` 宏体里的变量 `DevPlane *out` 撞了, 被替换成了 Dom 名. 已把 `op_check_gpu.cu` 六处宏的形参改为 `in_dom / out_dom`. 其余展开点 (Expr / EvalCpu / OperatorsService / op_check.cpp) 宏体不含同名变量, 不受影响. 请重新编译.

