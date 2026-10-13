# Search operator implementation issue
_Exported on 10/13/2026 at 08:08:10 GMT+8 from Cursor (3.21.18)_

---

**User**

@README 
这个search算子和实现到底有什么问题

---

**Cursor**

看完 `Search/{Contract,Grid,Cpu,Gpu,Check}` 和 `SearchService`, 工程实现 (分块 / 对拍 / 并行 / 显存预算) 基本是对的, 问题集中在**目标函数和搜索语义**上, 实现层只有几处小的. 按严重程度:

## 一、目标函数 (Contract.hpp【目标】) — 最核心的问题

**1. h\* 选的是"每期净 LS", 不是单位时间的**
`net_ls = ls_mean(h) − buy − sell − 2·sell_impact`, 成本是固定的一次往返, 毛 LS 随 h 增长. 结果是分钟档 (1m/5m/15m…) 的净 LS 几乎必然 ≤ 0 → `−∞` 出局, argmax 机械地落到 close/t1/t3/t5. 搜索实际上只在为**最长持有期**调参. 而 Inspect 的期限结构图是**年化**的 (`value(...) * ann`), 用户看到的和搜索优化的不是同一条线.

**2. 成本按 100% 换手扣, 完全不看 rank_ac**
每个持有期都扣整笔建仓 + 平仓冲击, 跟 `rank_ac` 无关. 粘性高的因子 (rank_ac 0.9+) 实际换手只有 10%, 被多扣 10 倍. 这又进一步把 h\* 推向长持有期. 系统里已经有 `turnover_annual`, 但目标里没用.

**3. 没有任何显著性 / 样本量约束**
唯一的门槛是 `n ≥ 3`. `ls_t`、`icir`、`sharpe`、valid% 都不进目标. 搜索是对几百到几千个候选取 max —— 这是经典的 max-of-noise: 大 d 窗把有效行砍掉一大半, 均值方差变大, 更容易"赢". 结果写进文件的 params 很可能是噪声最优.

**4. 全程 in-sample**
搜索区间 = 展示区间 = config 的 start/end. 选完 params 删 stat 再 Run, 面板上显示的 stat 就是被选择偏差抬高过的, 没有 holdout, 看不出过拟合程度.

**5. 目标与你定的优先级相反**
你之前说"形态和换手比 LS 重要, IC 最不重要", 但 `score = ls_net · ic`: mono、rank_ac 不在目标里, IC 反而是乘数. `ic_pow` 在 service 里硬编码 1.0, 是死参数.

**6. 小问题**: h\* 按 net 选, 但再要求 ic(h\*) > 0; 若 h\* 处 IC 为负而别的 h 两项都正, 直接 −∞ 而不是换 h.

## 二、Grid.hpp — 坐标系对阈值类参数失效

**7. ANY / GE0 槽没有尺度先验**
`TsGt.k` (ANY, asinh) 和 `TsClip.k` (GE0, log2) 的 k 是原始特征量纲. 以 k=0 为基线, 粗网格点是 sinh(±1..±4) = ±1.2, ±3.6, ±10, ±27 —— 如果特征是收益 (1e-3 量级), 整个粗网格都是废点; 细化步长从 0.5 减半要 7–8 层才到 1e-3 分辨率, 而每层无增益就停. 这两类参数实际上搜不动. GE0 的 k=0 → `2^-10` 更是拍脑袋.

**8. 组合爆炸没有预算上限**
d 槽粗网格 ~10 点, k 槽 9 点, 笛卡尔积: P=3 就 ~1000 个候选跑全量数据, 再加 3 支 × 5^P × 最多 8 层. 没有任何候选总数 / 时间上限.

## 三、实现层 (较小)

- `gpu_chunk` 每块每趟都 `session_open → upload → close`, `cache_all` 时宿主缓存了但显存没缓存, 每趟重传全部特征平面.
- `gpu_chunk` 里 `C` 一旦因显存减半就不恢复, 后续批都用小 C.
- 成本池化样本 (rx 有效格, 全时刻) 与 LS 样本 (J ∧ ok 行) 不重合 —— 注释里承认了, 但它也是 1/2 的一部分.
- `TsMeanEma` 块首重启的误差 (注释已说明, 可忽略).
- `max_lag(t5) = 1275 = kMaxD` 刚好卡在断言边界, 再加 T+N (N>5) 的标签就会 assert.

## 结论

Search 算子本身 (CPU/GPU 的 Accum 攒法、分块、对拍) 和 Stat 一致, 没发现正确性 bug. 真正的问题是**它在优化一个不该优化的量**: 未年化、按满换手扣成本、无显著性约束、in-sample 的 `ls_net · ic`, 结果就是系统性偏向长持有期 + 挑噪声. 要改的是 Contract 的【目标】和 Grid 的尺度, 不是后端.

---

**User**

你给我提问题, 我来选择

---

**Cursor**

**Search 目标与搜索语义的设计选择**

> **1. 选 h* 和打分用什么时间轴? (现在是每期净 LS, 机械偏向最长持有期)**
> - 年化净 LS (与 Inspect 期限结构图同一条线) (Recommended)
> - 年化净 Sharpe (均值/波动, 自带显著性)
> - 保持每期净 LS 不变
>
> Answer: 年化不就是等比放大嘛? 和A区别是什么

> **2. 建仓/平仓成本怎么扣? (现在每期按 100% 换手扣整笔往返)**
> - 按实际换手折算: 成本 × (1 − rank_ac) (Recommended)
> - 保持满换手扣法 (保守)
> - 目标只看毛 LS, 成本作事后展示
>
> Answer: 我们交易的只是L,S两个档位的标的, 里面的标的也没有全部换手, 得看内部换了哪些

> **3. 显著性 / 样本量约束 (现在只有 n ≥ 3)**
> - 硬门槛: ls_t ≥ 阈值 (如 2) 才有资格 (Recommended)
> - 硬门槛: valid% 不低于基线的某比例 (如 80%)
> - 硬门槛: 有效行数 n 不低于基线的某比例
> - 不加, 只靠目标函数
>
> Answer: 这些不用管, IC本来就是类似均值的情况, 我们是分钟频, IC本身就有显著性, 我们search只看两个: LS和IC

> **4. 形态 (mono) 和换手是否进目标?**
> - 硬门槛: mono ≥ 阈值 且 年化换手 ≤ 阈值, 过了再比收益 (Recommended)
> - 作乘数进 score (如 score = 净LS · mono)
> - 不进目标, 只展示
>
> Answer: C, 不要问这些了, search要特别快, 在代码注释里把观念改了, 只看LS和IC, 以后也不要问了

> **5. IC 在目标里的角色 (现在是乘数 ls_net · ic)**
> - 只作硬门槛 ic > 0, 不进乘积 (Recommended)
> - 保持乘数, 但 ic_pow 做成可配
> - 完全去掉
>
> Answer: 我觉得合理, 我就是要双高的, 为什么要改

> **6. 过拟合控制 (现在选参和展示都在同一区间)**
> - 区间前后切分: 前段搜参, 后段评分并把 OOS stat 一并写文件 (Recommended)
> - 滚动/交叉验证: 多段轮流做 OOS, 取平均
> - 不切, 保持 in-sample
>
> Answer: 我说了IC有稳定性, 先不考虑过拟, 不要再问, 注释写好

> **7. 持有期 h 怎么定?**
> - 用户在 UI 选定一个 hold, 搜索固定它 (Recommended)
> - 保持每候选自动 argmax (但用新目标)
> - 用户多选 hold, 目标取平均/最小
>
> Answer: 还是现在这样, 有什么问题, fix好cost应该就可以了吧

> **8. ANY/GE0 阈值槽 (TsGt.k / TsClip.k) 的坐标尺度怎么定? (现在无尺度, 搜不动)**
> - 装载时按输入节点的分布定尺度 (如中位 |x| 或分位), 坐标 = k/尺度 (Recommended)
> - 按基线 |k| 定尺度 (k = 0 时退化, 不搜)
> - ANY/GE0 槽不搜, 只搜 d / 分位 / EMA 系数
>
> Answer: 没懂这是什么

> **9. 搜索预算 (现在粗网格笛卡尔积无上限)**
> - 候选总数上限, 超了粗网格按槽收窄 (Recommended) ✔
> - UI 可勾选哪些槽参与搜索
> - 不限
>
> Answer: 直接按照预算来确定每一个参数维度的对数系数就好, 也就是每一层用一个max params来先确定好网格精度

**剩余 3 个决定**

> **1. 跨 h 选 h* 时用哪个口径 (成本按 Q2 改成真实换手后)**
> - 年化净 LS: 单位资金年收益, 短持有期靠周转补回来, 与 Inspect 图同线 (Recommended)
> - 每期净 LS: 一次持有赚多少, 长持有期天然占优
>
> Answer: A不会增加计算量吧, 不影响search就A

> **2. 成本按真实换手: 每 (t, h) 行, 顶组里 t−h 时不在顶组的标的 = 新进者, 扣 (buy[a] + sell_impact); 底组同理扣 (sell[a] + sell_impact); 除以组内样本数, 沿 t 池化. 成本从此随 h 变, 不再是两格**
> - 就这么定 (Recommended)
> - 平仓不用常数 sell_impact, 也用冲击平面 (做多退出扣 sell[a], 做空退出扣 buy[a])
>
> Answer: A. 注意search的计算效率

> **3. TsGt.k / TsClip.k 这类原始量纲阈值的"一个单位"从哪来**
> - 装载时算输入节点的标准差, k 的坐标 = k/标准差 (Recommended) ✔
> - 这两类槽不搜, 只搜 d / 分位 / EMA / 时段
> - 按基线 |k| 定尺度 (k=0 则不搜这个槽)

Start editing. First `Search/Contract.hpp` (semantics).

Now `Search/Cpu.hpp`.

Now `Search/Gpu.cuh`.

Now `Search/Check.hpp`.

Good. Now `op_check.cpp` verbose print.

Now `Dag.hpp` (keys) and `Grid.hpp`.

Now `Grid.hpp`.

Now the lattice functions.

Now `SearchService.hpp` header comment.

Now `SearchService.cpp`.

Now the UI texts in `TabFactors.cpp`.

改完了, clangd 无诊断, 已 clang-format. 改动点:

**语义 (`Search/Contract.hpp`)**
- `CostAcc` 变成每持有期一份: 顶/底组成员数 `n_hi/n_lo`、新进者数 `e_hi/e_lo`、新进者 Σ建仓冲击; 均摊成本 = (Σ冲击 + 新进者数·sell_impact) / 组内样本数. 留仓成员不付.
- `net_ls` 年化: `(ls_mean − 顶组均摊 − 底组均摊) · periods_per_year(h)`. h\* 仍 argmax, score 仍 `ls_net · ic`.
- 头注释写明: 只看 LS 与 IC, mono/换手/显著性/样本外有意不进.

**后端**
- `Search/Cpu.hpp`: `cost_row` (本行 rank + t−h 行 rank 逐格判新进者) + `eval_rows` (行统计与成本一遍, 不再走 Stat `accum_rows`).
- `Search/Gpu.cuh`: 成本紧跟 `warp_row` 在同一行用 `xl` 算, 每个 h 的块都攒, 去掉 `with_cost`; 只有顶/底组成员才读 lag/成本.
- `Search/Check.hpp`: 参考改为逐行朴素换手成本, compare 四计数逐位 + 两均值容差.

**网格 (`Grid.hpp`)**
- ANY/GE0 槽坐标 = k/σ (`needs_scale` / `set_scale`), GE0 下界 0; 去掉 asinh 和 2^-10 hack.
- `kLayerMax = 256`: 粗网格步长从 1 按 √2 放大到笛卡尔积 ≤ 256; 细化半径 r 满足 (2r+1)^P ≤ 256 (≤ 2), 3^P 超则退化为坐标轴 ±1.

**服务 (`SearchService.cpp`)**
- 先装块 0, 基线 DAG 在 CPU 走到各阈值槽的输入节点, 取池内有效格 σ; σ 非正有限的槽剔除. 为此 `Dag` 加了 `keys` (节点子树规范串).
- UI 候选表 "LS net/yr" 以百分比显示; 提示文字、README 对应行同步.

编译和 `op_check --op Search` 对拍留给你跑.

---

**User**

可以, 但是比较关键的design choice也是需要提一下的, 这样agent以后可以跳过一些查证

stat里的成本计算方法更新了吗? 应该和search里的成本一样算才对
可以考虑把这部分代码提取出来共享保证一致性, 其他相同的代码也可以共享

---

**Cursor**

Now the Inspect header and Check.hpp reference, and the Stat Cpu `eval_rows`.

`grp_of` is declared later in the file; move the block below it.

Now `cost_row` in `Stat/Host.hpp`:

Now `Search/Contract.hpp`: replace the definitions with aliases.

Now `Search/Cpu.hpp`: drop the local `cost_row`, use the Stat one.

Now GPU warp loop → `cost_cell`:

Check.hpp reference → `cost_row` (whole tensor, naive loop over t ≥ h):

Now Inspect. Header first: `CostTier` becomes per-h `CostAcc`.

Now the service's C step:

Now `plot_term` in `TabInspect.cpp`:

Now TabInspect.hpp doc lines and README entries.

Now README lines 164/169/171:

已改完。核心是：**Stat 本身没有成本，不一致的是 Inspect**（期限结构净线还是老模型：全组均冲击、100% 换手、与 h 无关），而且新进者规则在 Search Cpu / Gpu / Check 复制了三份。

现在成本定义只有一份：

- `Stat/Contract.hpp`：新增【换手成本】段，`Cost` / `CostAcc` / `cost_cell`（FACTOR_HD 逐格规则：顶组 +1 / 底组 −1 / 不交易 0，`ent` = t−h 不在该组）/ `net_ls_period` / `net_ls`。
- `Stat/Host.hpp`：`cost_row`（紧挨 `ac_row`，调 `cost_cell`）。
- `Search/Contract.hpp`：删掉本地定义，只留 `using factor::stat::Cost/CostAcc/net_ls`，其他调用点（SearchService / OperatorsService / op_check / GpuRun）不用动。
- `Search/Cpu.hpp` → 调 `factor::cpu::stat::cost_row`；`Search/Gpu.cuh` warp 循环 → 调 `cost_cell`；`Search/Check.hpp` 参考 → 整张量顺序 `cost_row`。
- Inspect：`CostTier` 改为 `{amt, CostAcc acc[kMaxHold], cover}`，C 步每 (t ≥ h, h, c) 跑同一个 `cost_row`；`plot_term` 净线 L/S 用 `long_cost/short_cost`，LS 用 `net_ls_period`（与 Search 打分同式），悬停显示换手 % / 新进者冲击 / 均摊。

GPU 侧一个小差别：`cost_cell` 按值取 `xl[a]`，每格都会读 lag 行（之前只有顶底成员才读），但该行刚被 `warp_row` 的 AC 读过、在 L1，影响可忽略。

lint 对所有改动的 .hpp/.cpp 为 0 诊断；`Gpu.cuh` 没编译，请你跑一下 Operators 对拍（Search CPU/GPU vs 参考）确认。另外 `output/factors/smallcap400/*.json` 的改动是你那边搜索写回的，我没动。

---

**User**

你看下stat现在算得不同cost下的期限结构, 为什么还是日内操作基本还都是暴亏, 已经是很保守的数字了
我们应该dump了这些不同的期限结构数据?

---

**Cursor**

不是模型保守/悲观的问题，是信号量级本身远低于往返成本。我用 `output/factors/smallcap400/*.json` 里的毛 `ls_mean` 算了一下（只假设 20bp 平仓冲击、建仓冲击 = 0、买卖两腿）：

每期净 LS = `ls_mean − (τ_L + τ_S) × 20bp`，盈亏平衡要求两腿换手之和 < `ls_mean / 20bp`。以 `mom_30m` 为例：

| hold  | 毛 LS/期 | 毛年化 | 允许的两腿换手和 | 实际大致换手                         |
| ----- | -------- | ------ | ---------------- | ------------------------------------ |
| 1m    | 0.89bp   | 551%   | < 4%             | rank_ac 0.86 → 顶底组每分钟换 15–25% |
| 5m    | 3.0bp    | 370%   | < 15%            | rank_ac 0.68 → ~40–50%               |
| 30m   | 6.0bp    | 124%   | < 30%            | rank_ac −0.11 → ~100%                |
| close | 10.9bp   | 26%    | < 55%            | ~100%                                |
| T+3   | 18.5bp   | 15%    | < 92%            | ~100%                                |

其他 8 个因子更弱（多数日内 LS 只有 0.2–2.5bp/期）。也就是说：**仅 20bp 平仓这一项**（还没加 5w 建仓冲击 ≈ 半价差 + 吃深度，小票 10–20bp）就等于 LS 两腿 40bp 往返，而这批因子任何持有期的 LS 都不到 40bp。20bp 对小票日内不算保守，它是信号的 5–40 倍。

为什么看起来是"暴亏"而不是"小亏"：年化系数在 1m 是 61710，−10bp/期 → −6000%/年，图被 −100% 截断了，所以视觉上全砸穿。符号是对的，幅度是年化放大的。

结论：日内要活下来，要么 LS/期 ≥ 几十 bp（这批没有），要么换手极低（分钟因子做不到），要么成本参数（`sell_impact`）真的只有 2–3bp（小票不现实）。短持有期靠周转补回的前提是每次周转不付固定成本，而这里每个新进者都付 20bp 固定平仓冲击。

关于 dump：**没有**。因子 JSON 的 `stat.holds[]` 只有毛口径 `HoldStat`（Factors Run 的 `load_planes` 默认 `with_costs=false`，根本没载成本）；Inspect 的 `CostTier`（各档 × 各 h 的换手/冲击/净线）只在内存；Search 只回写 params。如果要留痕，可以在 Inspect 发布时把每档每 h 的 `turn_hi/turn_lo/buy_mean/sell_mean/ls_net_yr` 写进该因子 JSON 的 `stat.holds[*].net[<amt>w]`，或者让 Factors Run 也载成本顺带算一遍（多一遍 [T][A]×H×C，全表跑会慢一点）。要做的话告诉我选哪种。

---

**User**

日内因子必须进一步降低换手, 我们逆向考虑一下, 拿到这些因子后, 要用什么办法降低触发才能实盘?

---

**Cursor**

先把问题换成正确的量纲：标准不是 LS/期，而是 **每次交易赚多少 bp = LS / (τ_L + τ_S)**，它必须大于往返成本（20bp 平仓 + 10–20bp 建仓 ≈ 30–40bp/腿）。现在 `mom_30m` 在 30m 是 6bp / 1.0 ≈ 6bp/笔，`ts_mom_30m` 在 60m 是 11.5bp/笔，T+3 也只有 ~20bp/笔。所以要么每笔 alpha 提 5 倍，要么同样 alpha 下换手降 5 倍，缺一不可。按这个标准逆推，可行的手段分三档：

**一、改"成员"定义（在现有评估框架里可直接量化，最该先做）**

1. 迟滞带 (hysteresis)：进场看 top 5%，出场只在跌出 top 25–30% 时触发。顶组成员变成一个按 t 顺序的状态机：`in[t] = rank ≥ enter ∨ (in[t−1] ∧ rank ≥ exit)`。换手主要来自"跨出宽带"而不是"5% 边界抖动"，30m 级别能降 3–5 倍；代价是持仓平均分位从 97.5% 滑到 ~85%，按 mono≈1 的线性组收益估 LS 保留 70% 左右。每笔 alpha 从 6bp → 20–30bp。
2. 最短持有 (min-hold)：进场后至少持 h_min，期间不看信号。换手上界 = 1/h_min，等价于你左上图已有的"h 个相位非重叠链"。
3. 确认 / 去抖：信号连续 k 根都在进场区才进。砍掉的是单根噪声触发，对真趋势几乎不损失。
4. 信号平滑：rank 前先 `TsEma / TsMean`，再 CsRank。效果与 3 类似但会引入滞后，对 30m 反转这类快衰减信号损失大。

这四个都可以作为 Stat/Search 的参数维度（enter/exit 分位、h_min、k），让 Search 直接在 `net_ls` 上找换手最优点 —— 目标函数不用改，因为成本模型已经按新进者算。实现上只需要一个顺序预处理：沿 t 逐资产跑状态机，产出 [T][A] 的腿归属平面 (+1/−1/0)，后面的行核照旧并行；`cost_cell` 的 (x, y) 换成 (member[t], member[t−h]) 即可。

**二、改交易方式（成本侧，评估框架要加东西）**

5. 被动成交：进场挂 bid、出场挂 ask，吃掉的那 10–20bp 半价差 + 冲击就变成 0 甚至为正。1m–5m 的信号基本只能靠这个活。代价是成交率和逆向选择，要用 L1 盘口回放来评成交概率，当前标签体系 (mid 进 / VWAP 出) 不覆盖。
6. 冲击分摊：建仓冲击 `lb_cost_buy_5w` 是一口吃 5 万；拆单到多个分钟会更低，但 h 很短时拆单本身就吃掉持有期。

**三、改用途（最现实）**

7. 不独立交易，做"执行 alpha"：给已有的日频 / T+N 组合的调仓做进出场择时。调仓本来就要付那 20bp，日内信号只决定今天的买在哪一分钟 —— 6–10bp/笔的信号在这里是净赚的，因为固定成本已经沉没。你那批因子 `ts_vwap_dev_60m`、`ts_mom_30m` 在 15–60m 的 IC 0.05+ 正适合干这个。
8. 多因子合成后再做 1：几个低相关的 30m 级因子平均后噪声下降，同样迟滞带下换手更低、LS 更稳。

诚实的判断：对现在这批信号强度（任一持有期 ≤ 20bp/笔），光靠一类降换手手段到不了 40bp/笔，第一档 + 被动进场 叠加才有可能；独立日内策略的门槛比"执行 alpha"高得多。

如果要动手，我建议先做 1 + 2（迟滞带 enter/exit 分位 + h_min）进 Stat 契约与 Search 参数，因为它能在当前成本模型下直接看到 LS/笔 的变化，而且 Search 会自动给出最优带宽。要做的话告诉我：这个"成员平面"是放在 Stat 契约里（Inspect / Search 都生效），还是先只给 Search 加参数试水。

---

**User**

我理解:
如果我们要求因子的高频排序能力, 那么几乎肯定和换手是冲突的, 所以因子在评估的时候, 是不是完全不应该考虑成本

对于迟滞带和最短持有, 感觉不本质, 是不是意思就是不要看1min预测, 而是看30m预测, 和这个是等效的?
对于确认 / 去抖和信号平滑, 这类本质上是不是TS算子, 是不是意思大部分时候其实根本不值得开仓?
我们TS算子和CS算子都是按照因子值分档位的, 只不过CS因为是rank所以相当于时时刻刻都要做多做空有一部分
而TS算子的因子值不保证有对应比例的标的有仓位, 所以其实G20和G1两档触发的机会其实可以很少?

---

**Cursor**

四点逐个说，先给结论：你的理解大体对，但有两处要修正。

**1. 评估因子要不要看成本**

分两层。因子层（Factors Run / Inspect 的 IC、mono、分层、毛 LS 期限结构）是在量"信息"，不该掺成本常数，现在就是这样。但"信息衰减有多快"是因子的内禀属性，它决定了每笔交易最多能赚多少 bp，这个量**不需要成本常数**：`LS(h) / (τ_L(h) + τ_S(h))` = 每次换仓的 bp，CostAcc 里的 `turn_hi / turn_lo` 已经是它的分母。所以我建议的口径是：因子层加这一个无成本的"bp/笔"期限结构；成本常数 (sell_impact、金额档) 只在 Search / 组合层作为旋钮进来 —— Search 现在的 `net_ls = ppy × (LS − τ·c)` 其实就是把 bp/笔 和年化周转合在一起、用 c 当阈值，c 是策略参数不是因子属性，这个分工是对的。完全去掉成本不行：Search 没有任何换手惩罚就会一律选最短 h（毛年化最高），选出来的是不可交易的东西。

**2. 迟滞带 / 最短持有 ≡ 看 30m 预测？**

最短持有 ≈ 是：按 h=30 评估、30 个相位错开，就是 min-hold 30。
迟滞带 ≠ ：它是非对称的 —— 进场用 1m 的尖锐信息（择时），出场用慢条件，持有期是变量而不是常数。区别在信号的衰减形态：如果信号是"淡出到 0"，迟滞和固定持有差不多；如果信号会"反转"（`mom_30m` 在 30m 的 rank_ac = −0.1，就是反转型），迟滞会在反转前把你带出来，固定持有不会。对你这批反转型因子，两者确实接近等效，你的直觉在这批上成立；对慢衰减因子就不是。

**3. 确认 / 去抖 / 平滑是 TS 算子？**

是，就是作用在因子值上的 `TsEma / TsMin(k) / TsCount` 一类，表达式里现在就能写，评估侧不用加任何机制。其含义也正是你说的：极端分位大多数时候是瞬态噪声，只有"持续处于极端"才值得付一次固定成本 —— 本质是把"开仓"从高频事件改成稀疏事件。

**4. TS 口径天然稀疏触发？**

对，而且这是 TS 口径和 CS 口径最本质的差别：CS 的 G20/G1 每时每刻必有 5% 的票在里面，宽度被 kGroups 钉死，换手由 rank 抖动决定；TS 的 G20 = 自身 5 日历史的 top 5%，某一刻有几只票在里面由数据说了算，可以是 0，也可以在全市场异动时是几百只（共同分量被超额口径吃掉了）。更关键的是 TS 有一个 CS 没有的旋钮：**把阈值收紧**（top 1% 而不是 5%，或 `TsGt.k` 的 σ 倍数）直接减少触发次数、抬高每笔 alpha，这正是 Search 的参数维度，而 CS 里没有对应的东西可以调。

两个要注意的副作用：(a) 稀疏触发意味着资金大部分时间闲置，`net_ls` 是"每期每成员"的口径，不反映资金利用率，年化到总资金上要再乘触发占比；(b) 单名触发占比 5% 的分钟约 13 分钟/天，这些分钟是成段的，换手其实由"段长"决定，不一定比 CS 低，阈值收紧后段会变短 —— 所以 bp/笔 和触发次数要一起看，还是回到第 1 点那个量。

落到动作上：因子层只需要加"bp/笔"这一列/一条线（无成本常数，分母已有），其余照旧；降换手的手段全部在表达式层（TS 门控、阈值）和 Search 的参数里，评估契约不用再动。

---

**User**

1. 迟滞带 这个就不太对了, 相当于不固定持仓周期, 但是肯定是慢得多, 首先是相当于拉高最短持有, 其次还是事件驱动, 具体的持有周期也不确定?
2. 所以流动性和因子极值都要拿去做成TS算子, 过滤大部分交易, 日内的超额才有可能能做出来, 而且也不影响因子本身的排序优化? 所以日内因子基本全部都要做成TS算子, 强行用CS算子, 然后吧G20 G1的换手压到极低是不现实的, 基本一定会破坏排序能力?

---

**Cursor**

**1. 迟滞带** —— 你说得对，我前面把它抬高了。它两端都是事件驱动：进场是"进入 top 5%"事件，出场是"跌出 top 30%"事件，持有期 = 首次穿越时间，是个随机变量，期望值由因子自相关决定（30m 反转因子大约就是几十分钟），且不能由你指定。它不等于"拉高最短持有"（信号快速反转时会提前出），但效果上确实是"慢得多 + 不可控"。更要命的是评估侧：标签是固定 h 的，变持有期没法用现有 Row/HoldStat 口径算，得走逐笔回放。这个手段可以从清单里划掉。

**2. 日内 = TS 门控** —— 结论对，但二分法要修一下：真正的对立不是"CS 算子 vs TS 算子"，而是 **"宽度固定、时刻满仓" vs "事件触发、大部分时间空仓"**。

- CS 根的问题是结构性的：不管池子里剩几只，G20/G1 永远是有效集的 5%。你就算在 CS 根里面套 TS 门控把大部分票置 NaN，剩下的票还是会被强制分出 5% 去交易；而把换手压低的唯一办法是平滑因子值，平滑 = 滞后 = 短 h 的 IC 直接没了。所以"CS 根 + 极低换手 + 日内排序能力"三者确实不可兼得，你的判断成立。
- TS 根（自身 5 日历史分位）天然是事件型：某刻 G20 里有几只由数据决定，可以为 0；收紧阈值（top 1%、`TsGt.k` 的 σ 倍数）直接减少触发、抬高每笔 bp，这个旋钮 CS 没有。所以独立做日内，根必须是 TS。
- "不影响排序优化"：门控改变的是总体，不是排序。TS 口径的 IC 本来就是每 t 在有效（= 触发）的票之间算的，所以你优化的就是"触发集合内的排序能力"，这正是你要的那个量；代价只是每行截面小、单行 IC 噪声大，池化后无妨。
- 流动性同理做成 TS 门控（自身价差 / 深度 / 冲击处于低位时才允许触发），这是动成本分子的手段，和动换手的手段正交，两者叠加才够。

两个补充：

- 有一个混合形态能保留截面信息又是事件型：`TsRankRoll(CsRank(x))` —— "这只票的截面排名处于自身历史高位"。根是 TS，内部是 CS，符合现有 root_frame 规则，不需要新机制。
- TS 稀疏触发后，`net_ls` 是"每期每成员"口径，不含资金闲置；年化到总资金要再乘触发占比。Search 的目标是在触发集合内比较参数，这个口径没问题；但跨因子比"能赚多少钱"时要记得这一层。

所以你现有的 `ts_*` 那组因子方向是对的，差的是阈值没收紧（现在用的 kGroups 等分 5%，触发还是太密）；CS 那组留给执行 alpha 或作为 TS 根的内层。

---

**User**

你在现有的里面挑一个已经比较好的去优化, 做一个新算子
根是TS, 内部有原本的排序信息, 同时考虑冲击特征和因子极值来过滤, 预期cost后优化后效果比原本因子强

---

**Cursor**

挑了 `ts_vwap_dev_60m`（IC 0.065@15m / 0.063@30m，这批里最强），新文件 `output/factors/smallcap400/ts_vwap_dev_60m_gated.json`：

```
TsRankRoll(
  TsMask(
    CsRank(TsNeg(TsLogRatio(close, TsMeanRoll(vwap, d=60)))),              ← 原排序信息 (截面秩)
    TsMul(
      TsGt(TsAbs(TsZRoll(TsNeg(TsLogRatio(close, TsMeanRoll(vwap, d=60))), d=1275)), k=1.5),  ← 因子极值: 偏离 ≥ 自身 5 日 1.5σ
      TsGt(TsNeg(TsMax(cost_buy_10w, cost_sell_10w)), k=-20))),            ← 冲击: 吃 10 万两侧都 < 20bp
  d=1275)
```

机制：

- 两个门控相乘，任一不满足 → `TsMask` 置无效 → 该格不进 rank、不进成员、不算 IC、不算成本。流动性差的票会整段无效，自动出池。
- 根 `TsRankRoll` 只在存活分钟里对自身存活史分位，G20/G1 = "截面极端 ∧ 自身极端 ∧ 当下便宜"的时刻。内层 `CsRank` 保住相对市场的排序，所以 IC 评的是触发集合内的排序能力。
- 可搜槽 4 个：`TsMeanRoll d`、`TsZRoll d`、极值阈 `k=1.5`（ANY，σ 单位，Search 自动定尺度）、冲击阈 `k=-20`（ANY，按 cost 特征的 σ 定尺度）。3⁴ = 81 < 256，一层够。子树 `TsNeg(TsLogRatio(...))` 出现两次，DAG 去重只算一次。

预期与保留意见：

- 成本后变强的来源是两条：极值门控抬高每期 LS（大偏离后的回归幅度大于普通分位），冲击门控直接压低 `buy_mean / sell_mean`。
- 事件型交易的换手比 τ 在 h ≥ 事件时长后会接近 1（每个成员都是新进者），这是本质，不是缺陷；所以看 Inspect 时直接看 "每期 LS − 两腿均摊" 的符号，不要期望 τ 下降。
- 平仓 `sell_impact` 20bp 是全局常数，对门控后的流动性好的时刻它是偏悲观的；如果净线仍差一点，那是这个常数在压，不一定是因子问题。

你跑：Factors Run（先确认 `cost_buy_10w / cost_sell_10w` 在你的特征库里有，没有的话换 `spread_l1`）→ Inspect 看期限结构净线 → Search 选 5w 档。阈值 1.5σ / 20bp 是起点，Search 会自己挪。

---

**User**

1. 所以所有的期限结构里, 我们只应该优化 30m~T+1的, 两边额外的只作为期限结构的延伸, 而不是search的目标
2. 我发现我们现在的目标函数LS*IC, 配合TS因子, search算法会把G20 和 G0做的特别稀疏, 所以还是得鼓励相对稳定的周期性换手, 比如平均每天都要触发几次,
但是好像是分层整个都不动了, 这个绝对不合理的 , 我们的分层应该是超级活跃的, 完全不应该去gate, 但是好像也没办法在因子层面去单独gate L/S两档?
3. 我们要保证如果search出来的结果, 还不如原来的, 那就不要覆盖
4. 我们在因子看板也要标出来这个因子优化后的最优周期 (in 30m~T+1)

---

**Cursor**

先说现状证据：你跑的 `ts_vwap_dev_60m_gated` 已经被 Search 写成 极值阈 k=4.65σ、冲击阈 k=530，结果 `valid_pct = 0.003%`，每个持有期只剩 **n = 10 行**，h=1 的 rIC 0.64 纯属噪声。这就是你第 2 点说的退化：目标函数 `net_ls × ic` 没有任何"活跃度下限"，稀疏极值几乎必然赢（10 行里挑得出任何数）。

逐点回应：

**1. 只在 30m ~ T+1 里选 h\***：同意。`Objective` 加 hold 窗 `[30, T+1]`，`score_of` 的 argmax 只在窗内；窗外持有期照算、照画期限结构、不进目标。改动小。

**2. 分层活跃 vs L/S 稀疏**：你的判断对 —— 因子层面做不到只 gate 两档，`TsMask` 一上，20 组全部冻结，IC / mono 评的就不再是这个因子。而且不 gate 的 TS 根本身就有你要的"周期性换手"：每只票 5% 的分钟在 G20（≈ 13 分钟/天），全市场每分钟 ~35 只 —— 这就是"每天触发几次"。所以我做的那个门控版方向错了，建议回退成不门控。

真正能"分层全活、只 L/S 稀疏"的位置在**评估层**：因子文件多一个 `gate` 表达式（流动性 / 极值条件），只作用于顶 / 底组的成员资格（ls 与成本按 `top ∧ gate` 算），IC / grp / mono 仍按全部格算。这要动 Stat 契约（Row.ls / CostAcc 的成员定义）+ Host/Dev/Gpu + 装载 + UI，是中等规模的架构改动。

不管那条做不做，目标函数都必须加硬下限（否则 3 永远防不住）：候选在 h\* 上 `n`（ok 行）和顶 / 底组平均成员数 `n_hi / 行数`、`n_lo / 行数` 低于阈值 → −∞。

**3. 不如原来就不覆盖**：基线本来就在粗网格里，按 score 比最优 ≥ 基线是构造保证的；之前"更差却覆盖"是 score 被稀疏解骗了，2 的硬下限就是这条的修法。另外建议把"没好多少"也算不如：`best > base × (1 + kMinGain)` 才写。

**4. 看板标最优周期**：Search 落盘时在因子 JSON 加 `search` 块（h\*、净年化、rIC、金额档、规范串、时间），`write_back` 已保留其他键所以 Run 不会冲掉；表加固定列 `h*`，规范串与 `search.expr` 不一致（之后手改过参数）就显示为失效。

需要你定的：

**Search 目标与门控的设计决定**

> **"只 gate L/S 两档、分层全活"的评估层门控 (因子文件加 gate 表达式, 改 Stat 契约) 这次做不做?**
> - 这次不做: 先把目标函数硬下限 + hold 窗 + 不覆盖规则 + 看板 h* 做完, 门控另开一轮 ✔
> - 一起做: Stat 契约加成员门控平面, Search / Inspect 都生效

> **活跃度硬下限怎么定 (低于 → 该候选 −∞)? 基于 h* 上的 CostAcc / HoldStat**
> - 顶/底组平均成员数 ≥ 5 只/分钟 且 ok 行数 ≥ 基线的 50%
> - 顶/底组平均成员数 ≥ 10 只/分钟 且 ok 行数 ≥ 基线的 80%
> - 只卡根平面 valid% ≥ 基线的 50% (最简单, 不分腿)
>
> Answer: 这个你再跟我聊一下, 我觉得上面的都不本质

> **落盘规则 (第 3 点)**
> - best > base × (1 + kMinGain) 才写 (没好多少也不写)
> - best > base 就写 (只要不更差) ✔

> **ts_vwap_dev_60m_gated.json 怎么处理?**
> - 删掉 (方向错了, 原 ts_vwap_dev_60m 保留) ✔
> - 保留, 等评估层门控做出来再改成 gate 形式

你说得对，那三个都是在给症状打补丁。退一步看退化解为什么能赢：`net_ls` 是**每期每成员**的收益，10 行 × 1 只票赚 100bp 和 3 万行 × 35 只票赚 6bp 在这个口径下前者完胜 —— 但前者一年只动用了几次资金，其余时间钱是闲着的。目标函数量的是 bp，不是钱。

本质的修法是让目标 = **固定资金上的年化收益**，闲置资金记 0：

```
u(h)   = min(1, 平均每分钟顶/底组可交易成员数 / N_full)      ← 资金利用率
score  = net_ls(h) × u(h) × rIC(h)
```

- `N_full` = 满仓时一腿该有几只票。不另立参数，直接取 **池内平均有效票数 / 20**（= CS 口径天然的 5% 宽度）。CS 因子永远 u = 1，不受影响；不门控的 TS 因子每只票 5% 时间在 G20，平均成员数 ≈ 池/20，u ≈ 1；门控越狠 u 越小，退化解 u ≈ 10 行 / 3 万行 → score ≈ 0，自动出局，不需要任何硬阈值。
- 分母用池 `g`（cs_valid）而不是因子有效格：用因子有效格的话 `TsMask` 把格子砍掉分母也跟着缩，u 又回到 1，等于没约束。
- 这同时保住了"分钟 IC 自带显著性"那个前提：u 小 ⇒ n 小 ⇒ 分数小，样本不够的解不可能靠噪声胜出，不用再引入 t 值。
- 它不"鼓励"周期性换手，而是取消了稀疏的奖励：频繁触发但每笔净为正 → 钱多；稀疏触发 → 钱少。换手的平衡点由 `net_ls` 里的成本自己定，这正是你第 1 轮定的框架，不用改。

实现上只要 `CostAcc` 多记一个行数（`rows`，有 lag 的行计一次，GPU lane 0 加），`u = (n_hi + n_lo) / (2·rows) / N_full`；`N_full` 每次 Search 从池平面算一次进 `Objective`。Inspect 不受影响（它不算 score）。

一并做的：hold 窗 `[30m, T+1]`、`best > base` 才写、删 `ts_vwap_dev_60m_gated.json`、看板 `h*` 列（Search 落盘 `search` 块）。

这个口径可以的话我就开始；如果你觉得 `N_full` 该是别的定义（比如固定只数），说一下。

---

**User**

可以的, 开始做, 做干净做对仗, 我们的search也要极端高效

---

**Cursor**

Now let me look at the remaining pieces I need to touch: `SearchService.hpp`, `StatJson.hpp`, the GPU cost block, Check compare, and the opt panel in TabFactors.

Now the GPU block, Check compare, and the opt panel in TabFactors.

Let me start with the contract: `CostAcc.rows` + `util`.

Now `cost_row` counts the row:

GPU side:

Check compare adds `rows`:

Now the Search contract: `Objective` (hold window + `n_full`), `Score` (util), `score_of` / `score_at`.

Now the header comment 【目标】:

Window holds exist in the check set. Fix the two `Objective` call sites with designated initializers (C++23):

Now edit SearchService.cpp: objective construction, `n_full` from pool, cand util, overwrite rule, search block write.

Now `n_full` after chunk 0 loads:

Now `SearchService.hpp`:

Now `FactorsService.hpp`: `SearchMark`, `FactorRow::search`, `WriteFactorParams` signature.

Now `FactorsService.cpp`: write/load the `search` block.

Add a `load_search` next to `load_stat`, and store `hold` as the int key (same as stat holds) plus a readable name.

Check that `write_back` (post-evaluate) copies rows in a way that preserves `search` (the row in memory vs. file keys), and check `#include <cmath>` for `std::isfinite`.

Reuse the existing one rather than duplicating: expose it from `FactorsService.hpp`.

Now check SearchService.cpp's `seen` type, `hd`, `feats_`, `fs` alias, and `cmath` include.

Good. Now TabFactors: view the Col enum, kFixedCols, fixed_cell, sort key, the opt_cands tooltip, and Amt tip.

Now add the `Opt` column: enum, kFixedCols, kStatInsertAfter, fixed_cell, sort key.

Now the Optimize section (status text, tooltips, opt_cands table).

Check the TabFactors header comment (column list) and the Factors UI header for column mentions; then delete the gated file and update README.

The gated file is already gone (user must have deleted it). Update README lines:

完成, lint 全过 (未编译)。本轮收尾改动:

- `SearchService.cpp`: 块 0 装完后 `obj.n_full = Σ cs.m / T / kGroups` (池宽, 与因子无关; 发布到 `res.n_full`); `SearchCand.util`; 终选改为 **`best.score > base.score` 严格才写**, 否则 "文件不动"; 写盘时附 `SearchMark` (h*/amt/sell_impact/ls_net/util/ric/score/base_score/time)。
- `FactorsService.hpp/.cpp`: 新 `SearchMark` (`valid(row_expr)` = 规范串仍匹配), `FactorRow::search`; `WriteFactorParams(..., mark)` 写 `search` 键 (hold 存整数键 + `hold_name` 供人读), 扫描 `load_search`; `now_string` 提为导出函数, Search/Inspect 复用, 删掉两处重复定义。
- `TabFactors.cpp`: 新固定列 `h*` (Valid 之后, Stat 列之前; 失效灰显带 `?`, 排序按持有分钟); 行悬停加 search 详情行; 候选榜加 `util` 列 + 顶部显示 `n_full`; Optimize / Amt 提示改为新目标 (util、hold 窗、严格优于基线才写)。
- README 两行同步。
- `ts_vwap_dev_60m_gated.json` 已不在磁盘 (你已删)。

未做 (按约定推后): 评估层 L/S 门控。

---

**User**

这个是不是应该和最有周期一样放到因子看板里

---

**Cursor**

**看板加列**

> **「这个」指哪个? 目前 search 块里除 h* 之外的字段 (ls_net / util / rIC / score) 只在行悬停里显示, 要把哪些提成看板固定列 (与 h* 一样, 规范串不匹配时灰显)?**
> - util (资金利用率) 一列 ✔
> - util + score 两列
> - ls_net / util / rIC / score 四列 (h* 处的值; 与 Stat 列区分: Stat 列是毛收益、按当前选的 h)
> - n_full (满仓只数): 它是池级常量, 放 Optimize 状态行旁即可, 不进表

已加 `util` 固定列 (紧跟 `h*`, Stat 列之前): 显示 search 块里 h* 处的资金利用率, 规范串不匹配时灰显带 `?`, 可排序; 列注释写明目标式与含义。lint 通过。

---

**User**

stat好像不会更新这些, 我希望stats也能算/更新这些, 
事实上, search应该只内部用这些, 因子看板的所有数据都应该通过stat来更新

注意和其他一样, 这部分代码给stat和search做对仗, 最好做好代码复用不要重写

---

**Cursor**

Good, `FactorsLoad.cpp` is a precise-math TU. Now implementing. Start with the contract split in `Search/Contract.hpp`:

Now `FactorsService.hpp`: move `CostTier` in, drop `SearchMark`, add `costs`/`n_full`/`max_impact`, `RowScore`.

Now `FactorsLoad.hpp/.cpp`: shared `n_full_of` and `cost_tiers`.

Now `InspectService`: remove `CostTier` def, Flow scratch, use `cost_tiers`.

Now GPU `stat_eval`: add optional host `rx` out. Update `GpuRun.hpp`, `.cu`, stub.

Now `FactorsService.cpp`: request `max_impact`, load costs, cost_tiers in both backends, fill/stat JSON, remove search block, `RowScore`.

Now `cost_json`/`load_cost` helpers (next to `hold_json`/`load_hold` in StatJson.hpp, same style) and `RowScore`.

Now `cost_json`/`load_cost` for `CostTier` and `RowScore` in FactorsService.cpp:

Update header comments in FactorsService.hpp (file header / `costs` comment "Run 不读") and FeatureTable:

Now `SearchService.cpp`: use `n_full_of`, drop `SearchMark`.

Now rewrite the TabFactors pieces: `OptCell`, columns, tooltip, sort.

Now the Factors Run / file stat also has costs — Inspect's fallback can show net lines too (same `CostTier`):

`load_cost` is defined after `load_stat`; move it before.

完成, lint 全过 (未编译)。要点:

- **Stat (Run) 现在也攒成本**: `load_planes` 带 `with_costs`, `L.max_impact` 来自 `FactorsRequest.max_impact`; 每个根算完 → `cost_tiers(ex, rx, L, feats_, costs)` 攒全部金额档 × 持有期的 `CostAcc`; 连同 `n_full` / `max_impact` 落 `stat` 块 (`stat.costs[]`, 每档 `{amt, cover, acc[]}`; `StatJson` 新增 `cost_acc_json` / `load_cost_acc`)。
- **对仗 + 复用**: 成本代码从 Inspect 抽成 `FactorsLoad::cost_tiers` (Inspect 改调它, Flow 私有累加器删掉); `n_full_of` 同样抽到 FactorsLoad, Search 块 0 也用它。GPU 后端 `stat_eval` 多一个可选 `rx` 出参把 r16(x) 拷回宿主, 两后端喂同一份宿主成本代码 (秩是整数, 逐位同)。
- **目标同一份**: `score_of` 拆成 `HoldStat[] + CostAcc[]` 入口 (Stat / 看板用) 与 `Result` 入口 (Search 先 finish 再走前者)。`FactorsService::RowScore(row, amt, sell_impact)` 给看板。
- **看板**: `h*` / `util` 列和行悬停"目标"行全部由 stat 现算 (按 Optimize 的 Amt 档 + `ctx.sell_impact`), `SearchMark` / `search` JSON 块 / `load_search` 全删; Search 只写 `params`。`CostTier` 迁到 FactorsService.hpp, Inspect 页回退显示 Factors stat 时也带净线了。

注意: 旧因子文件的 `stat` 没有 `costs` / `n_full`, `load_stat` 会整块忽略 (显示 no stat), 需要 Run 一遍重写。

---

**User**

<timestamp>Monday, Oct 12, 2026, 5:58 PM (UTC+8)</timestamp>
<system_notification>
The following task has finished. If you were already aware, ignore this notification and do not restate prior responses.

<task>
kind: shell
status: aborted
task_id: 153470
title: lint 脚本如何输出诊断详情
tool_call_id: toolu_01SuwSVPydMjtJLnV2VcN4vX
output_path: /home/chuyin/.cursor/projects/home-chuyin-work-stk/terminals/153470.txt
</task>
</system_notification>
<user_query>Briefly inform the user about the task result and perform any follow-up actions (if needed).</user_query>

---

**Cursor**

那是我误触发的全仓 lint (无参数时跑全部 311 个文件), 已主动中止, 无需处理; 相关诊断已改用 ReadLints 查过, 全部改动文件均无错误。

# @cursor_search_operator_implementation_i.md 
_Exported on 10/13/2026 at 09:06:35 GMT+8 from Cursor (3.21.18)_

---

**User**

@cursor_search_operator_implementation_i.md 
现在所有的因子日内5W冲击的收益根本就没法拉正
所以日内因子基本必须是TS根的, 而且每个因子都必须要 流动性过滤和额外的因子过滤
因子本身负责分层和超额, 额外的触发过滤负责进一步提高胜率和盈亏比, 流动性过滤负责优化成本
我理解这3个是相乘的关系
问题是, 应该分别分开优化, 还是分别分开优化?

---

**Cursor**

你这句我按"分开优化 vs 联合优化"来理解。

**结论：两阶段。因子单独优化；两个门控联合优化。不要三者全联合，也不要三者全分开。**

**为什么因子要单独优化（不带门控）**

- "因子负责分层和超额"这个分工，只有在因子**不带门控**评估时才成立。门控一套上，IC / mono / 分层算的都是触发子集内的量，Search 会直接通过缩子集来抬 IC —— 上一轮 `valid_pct = 0.003%` 就是这个。`u` 只堵了稀疏，没有改变"IC 的含义被门控改掉"这件事。
- 极值门控的阈值是对因子自身 z 值取的，它不是独立于因子参数的：`d` 一变，z 的分布变，同一个 `k` 对应的触发率就变。放一起搜，`k` 和 `d` 会互相补偿，搜出来的 `d` 不再是"排序最好的 d"，而是"配这个 k 触发率最合适的 d"。
- 流动性门控根本不应该是因子的参数：它取决于金额档（5w 和 50w 要的阈值不同），是策略 / 资金层的量。写进因子表达式等于每换一个金额档就得重搜因子。
- 因子阶段的目标不能含成本常数（不门控的 TS 因子日内净线全负，score 全 −∞，等于没搜）。该用无成本的量：窗内 `bp/笔 = LS/(τ_L+τ_S)` × rIC，或毛 LS × rIC。

**为什么两个门控要联合**

- 它们共用同一个预算 `u`：都只会把触发集合砍小。极值收紧一档和流动性收紧一档，各自"花"多少 u、换回多少净收益，是一个权衡，分开搜会各自把 u 花光。
- 流动性门控改的是样本：流动性好的时刻 / 票，极值后的回归幅度未必一样（流动性差的票偏离更大也回归更猛），所以极值阈的最优点依赖流动性阈。
- 代价小：两个阈 9×9 = 81 格，一层搞定，比现在 3⁴ 还少。

**"相乘"要修正一点**

相乘的只是**掩码**（三个指示函数的积决定谁进腿）。收益上不是乘法：极值门控抬的是 `LS_gross/笔`，流动性门控减的是 `cost/笔`，二者是**相减**关系进 `net_ls`，然后才乘 `u`。所以不存在"各自优化到最优再乘起来就是全局最优"的分解 —— 这正是两个门控必须联合的数学原因。

**对架构的含义**

现在用 `TsMask` 把门控写进表达式，因子和门控是同一棵树，评估上无法分开。要让两阶段真正成立，需要的就是上轮推后的那个**评估层门控**：因子文件 = `expr`（排序）+ `gate`（腿成员资格），IC / mono / 分层按 `expr` 全集算，`ls / cost / u` 按 `top ∧ gate` 算。Stat 一次跑出两套量，Search 分两阶段：先搜 `expr` 槽（无成本目标），再固定 `expr` 搜 `gate` 槽（`net_ls × u`）。并且 `gate` 的流动性阈可以按金额档各存一份，不污染因子。

要不要这轮就把评估层门控做起来，你定。

---

**User**

那些过滤类门控到底应该单独优化, 还是做到因子里? 因为TS因子本身就有过滤类效果
比如像微观探底回升, 支撑位反弹之类的过滤, 这种因子加入会打乱原本的因子排序能力嘛? 还是其实还是可以联合优化的?

---

**Cursor**

先把"过滤"拆开，因为你举的两类东西性质不同：

**1. 探底回升 / 支撑位反弹 —— 这不是过滤，是另一个因子**

判据只有一条：**这个条件自己有没有方向性预测力**。"探底回升"本身就在说"接下来要涨"，它单独做因子就有 IC。这种东西做成 0/1 门控，等于把一个连续信号二值化后再和主因子相乘 —— 信息损失最大的用法。它应该进因子内部，作为连续分量合成（`TsRankRoll(CsRank(a) + CsRank(b))`、秩乘积、或你现有的任何合成写法），然后作为**一个因子**整体优化。这时候"联合优化"是自然的，因为它就是一棵树。

会不会打乱原排序？一定会改，改了之后是好是坏是经验问题。但有一个几乎必然的现象要先说清：**任何门控都会压低子集内 IC**。门控后留下的票在主因子上的离散度变小（range restriction），即使腿的平均收益更高，子集内相关系数也会掉。所以"加了门控 IC 掉了"不是伤害的证据，评门控只能看腿的 `bp/笔` 和 `net_ls × u`，不能看 IC。

**2. 极值门控 —— 和 TS 根重复，应该删掉**

你说"TS 因子本身就有过滤效果"是对的，而且比你说的更强：`TsRankRoll` 的 G20 = 自身历史 top 5%，`|z| ≥ k` 也是自身历史的尾部，两者是同一个量的两种写法。它真正对应的参数是**腿宽**（top 5% 还是 top 1%），这是策略参数，不是因子属性。现在腿宽被 `kGroups = 20` 钉死，所以才需要再套一个 `TsGt` 去收紧。正确的位置是评估层的腿宽参数，不是因子表达式。

**3. 流动性 —— 真正的过滤，严格放因子外**

没有方向性，只动成本，且依赖金额档。这类不进表达式，进评估层门控，按金额档各存一份。

**4. 中间情形：状态门控（时段、波动率状态、趋势状态）**

自己无方向，但主因子在不同状态下效力不同（交互效应）。这类放 `TsMask` 进因子是**合法**的，因为它的主张就是"因子只在这个状态下成立"，子集内 IC 就是要评的量。但 Search 会利用选择效应，所以先要人工验一次，再决定要不要给 Search 搜。

**判别用现有工具就够，不用新机制**：把候选条件单独写成因子跑 Run 看它的 IC；再写 `TsMask(f, g)` 和 `TsMask(f, 1−g)` 两个因子看条件 IC。
- 条件自身 IC ≠ 0 → 类型 1，合成进因子
- 自身 IC ≈ 0、条件内外因子 IC 差异大 → 类型 4，状态门控
- 两者都 ≈ 0 → 类型 3，只影响成本/腿宽，评估层处理

所以回到上一轮的两阶段：**所有有方向性的东西进因子、阶段 A 联合搜；腿宽和流动性阶段 B 联合搜**。"探底回升"属于前者，极值门控被腿宽替代，流动性属于后者。

---

**User**

所以我们还是不应该在因子外额外设计触发机制, 还是应该用TS算子截取排序本身的极值来提高盈亏比从而克服cost, 因子外只加一层流动性过滤, 比如要求冲击成本在某个值以下?

---

**Cursor**

是，这个框架我同意，而且数据支持它。补两点精确化，一点证据。

**1. "截取极值"不是再套一个算子，而是腿宽参数**

TS 根输出的就是自身历史分位（分辨率 1/1275），"极值"= 分位阈 `q`：G20 现在是 top 5%，收紧就是 top 1%。它应该是评估层的腿成员阈（`pct ≥ 1−q_long`、`pct ≤ q_short`），IC / mono / 20 组照全集算。如果写成树里的 `TsGt(TsRankRoll(...), 0.99)`，20 组就全冻结了，回到上轮的问题。

**2. 收紧只有在尾部凸的时候才划算**

如果组收益对分位是线性的，top 5% 收紧到 top 1%，每笔只多赚几个百分点（分位中心 97.5 → 99.5），而 `u` 掉 5 倍 —— 净亏。所以我查了现有 stat 的 20 组曲线（bp/期）：

| 因子            | h   | 内部斜率/组 | G20−G19  | G2−G1 |
| --------------- | --- | ----------- | -------- | ----- |
| ts_vwap_dev_60m | 60  | 0.21        | **2.49** | 0.10  |
| ts_vwap_dev_60m | 30  | 0.21        | **1.84** | 0.21  |
| ts_mom_30m      | 60  | 0.19        | **2.21** | 0.51  |
| ts_mom_30m      | 30  | 0.20        | **1.38** | 0.04  |
| mom_30m (CS)    | 60  | 0.35        | 1.63     | 0.99  |

两个最强 TS 因子的**多头尾部是强凸的**（最后一格的增量是内部斜率的 10 倍），**空头尾部是线性的**（G2−G1 ≈ 内部斜率）。结论：

- 多头腿收紧有真实回报，top 1% 内很可能还在加速，值得搜 `q_long`。
- 空头腿收紧没有回报，只会损失 `u`；`q_short` 留 5% 或干脆不交易空头。腿宽必须**分腿**设。
- 20 组看不进 top 5% 内部。要定 1% 还是 0.5%，要么评估层直接吃 `q`，要么先临时把 `kGroups` 调到 100 跑一遍 Run 看曲线形状，这是现有工具就能做的检查。

**3. 流动性过滤：一层阈值，评估层，按金额档**

`cost_buy[amt] ≤ c` 作为进腿条件。两个注意：只在进场时刻判（平仓现在是常数 `sell_impact`，不随门控变）；它有选择效应，流动性好的时刻回归幅度通常小一点，会削一点 LS，所以 `c` 要和 `q_long` 一起搜，不能单独定。

所以策略层就三个量：`q_long`、`q_short`、`c`，按金额档各一份，阶段 B 联合搜；因子树保持纯净，阶段 A 只搜排序。这需要先做评估层的腿成员门控，是不是现在开始做，你定。

# Cost structure analysis issue
_Exported on 10/13/2026 at 10:16:35 GMT+8 from Cursor (3.21.18)_

---

**User**

@README 
inspect下, 我感觉我们带成本的期限结构算得是不是有点问题
比如30分钟动量反转这个因子, T+1的超额年化还是有差不多10几个点的
但是配合5W的冲击, 收益就到-100%以下了, 我们是不是把冲击算多了, 既然是年化收益
对于T+1, 我们哪怕是分钟线, 最多也就做250次交易, 我们是不是按全年250*4*60(分钟)次交易算了?

但是L/S那么多的预期收益, 哪怕每分钟都做, 预期收益也不应该低于-100%啊 , 感觉还是哪有问题

继续

---

**Cursor**

结论先说：期数没算错，T+1 用的是 242 期/年，不是 242×255。但 "<−100%" 也不是 bug，是**线性年化**（每期均值 × 每年期数）的必然结果，用 mom_30m 的真实数据对一遍就清楚了。

**1. 年化期数**

```205:205:cpp/include/factor/Stat/Contract.hpp
inline constexpr double periods_per_year(int hold) { return static_cast<double>(kDaysPerYear) * kSegLen / hold_minutes(hold); }
```

`hold_minutes(T+1) = kSegLen = 255`，所以 periods_per_year = 242。`plot_term` 里 `ann = 100 × periods_per_year(h)`，毛线和净线乘的是同一个 242。

**2. 拿 `output/factors/smallcap400/mom_30m.json` 的 T+1 (hold=1001) 手算，5w 档**

- 毛：`ls_mean = 0.00093`/期 → × 242 = **+22.5%**（就是你看到的"十几个点"）
- 成本（`CostAcc` 原始量）：`buy_hi=987.6, e_hi=696262, n_hi=768489`
  - 新进者占比（换手）= 696262/768489 ≈ 90.6%
  - 新进者平均买入冲击 = 987.6/696262 ≈ **0.142%**
  - `long_cost = (987.6 + 696262×0.002)/768489 ≈ 0.0031`/期
  - `short_cost ≈ 0.00317`/期（同理）
  - 两腿合计 ≈ **0.63%/期**
- 净：0.00093 − 0.00627 = **−0.0053/期** → × 242 = **−129%**

所以问题不在"交易次数算多了"，而在每一笔的量级对比：这个因子 T+1 一笔 LS 的毛边际只有 **9.3 bp**，而一腿往返成本约 31 bp（0.14% 买入冲击 + 0.2% 固定 `sell_impact`），两腿 63 bp。一天亏 0.53%，线性加 242 天就是 −129%。

**3. 为什么会"低于 −100%"**

这里的年化是 `mean × N`（算术和），不是复利 `(1+mean)^N − 1`。线性年化没有 −100% 的下界；按复利算 (1−0.0053)^242 − 1 ≈ −73%。1m 档更极端：毛 +715%/年、净 −10000%/年，所以代码里才把 Y 轴钳在 −100：

```761:761:cpp/src/gui/task_factors/ui/TabInspect.cpp
  ImPlot::SetupAxisLimitsConstraints(ImAxis_Y1, -100.0, INFINITY); // 年化 < −100% 无意义 (高冲击档净线会砸穿): 低于的点不参与 fit, 轴底最低到 −100, 线出界即裁
```

**4. 顺带两点观察（不是错误）**

- 成本里的大头其实是 `Config::sell_impact = 0.002` 这个固定平仓常数（占 long_cost 的 58%），而不是数据算出来的 5w 买入冲击（0.14%）。这个数对 smallcap 5w 单是否合理，由你定；它还没含印花税和佣金，真要加只会更差。
- T+1 的"留仓"定义是 lag 255 行（昨天同一分钟还在同组就不付成本），但 T+1 实际上次日开盘已经平掉了，严格说应该 100% 换手。现在 90.6% 比真实略乐观，方向上只会让净线更好看，不是导致 −100% 的原因。

如果你觉得年化线性值看着误导，可以把 tooltip 里加一列"每期净 bp"（毛 9.3 bp − 成本 63 bp）或者改成复利年化；是否改、怎么改你定。

---

**User**

不要算双边, 算平均单边
毛 = 5bp
buy cost 3bp, sell cost 20bp?
这个3bp怎么算得?

---

**Cursor**

先对一下数字：文件里 5w 档的买入冲击均值不是 3bp，是 **14bp**。按单边算（T+1, mom_30m, 5w）：

- 毛单边 ≈ (L 3.3bp + S 5.9bp)/2 ≈ **4.6bp**，和你说的 5bp 一致
- 新进者平均买入冲击 `buy_mean = buy_hi/e_hi = 987.6/696262` = **14.2bp**
- 平仓 `sell_impact` = 20bp（Config 固定值）
- 换手 90.6% → 单边均摊 = 0.906 × (14.2 + 20) ≈ **31bp**

所以单边是 4.6bp 毛 vs 31bp 成本，其中 20bp 是你配的常数，14bp 是数据算出来的。

**buy cost 的算法**（`LabelReturn.hpp`）：

```13:14:cpp/include/features/Operator/TS/Label/LabelReturn.hpp
//   冲击成本特征 (lb_cost_{buy,sell}_<a>w) = entry 同一盘口吃 a 万元的 VWAP 相对中间价的偏离 (比例, ≥0):
//     cost_buy = VWAP^A(a)/mid − 1,  cost_sell = 1 − VWAP^B(a)/mid.  "mid − 成交价" 单拆出来, 标签 − 成本 = 精确的吃单成交收益;
```

每格：entry 锚点（分钟末 +3s）的盘口，从 ask1 往上吃满 5 万元，成交 VWAP 相对 mid 的溢价；全簿吃不完余量按涨停价（`calc_vwap`）；装载时 `min(·, max_impact=1%)` 封顶。然后 `CostAcc` 只对顶组**新进者**累加这个值，`buy_mean` 就是它们的算术平均。

**3bp 从哪来**：你说的 3bp 大概是"典型值"——价格 10~30 元的票 1 tick 半价差就是 2~5bp，5 万元在 ask1 吃得完时冲击就等于半价差。但均值被尾巴拉到 14bp 有两个原因，都不是公式错：

1. 封顶是 1%，吃穿簿子 / 封板的格记 100bp，这些格把均值拉高
2. 选择效应：mom_30m 的顶组恰好是刚被拉起来的票，ask 侧刚被扫过、簿子薄，这一组新进者的冲击天然比全市场均值高

要确认是不是 1、2 导致的，可以看一下 5w 档顶组新进者冲击的分布（中位数 vs 均值），或者看 30w 档：`buy_mean(30w) = 2802/696262 ≈ 40bp`，5w→30w 只涨到 2.8 倍，说明 5w 大部分格确实还在前一两档内，尾部靠封顶兜着。

如果你的 3bp 是在 UI 的 tooltip 看到的，告诉我是哪一行哪一档，我再对。

---

**User**

这几个bp是不是应该在hover里显示
不再hover显示现在这些
而是每个点highlight然后hover
里面写清楚 单边 bp(L+S/2)
开仓BP (buy+sell/2)
关仓bp (xx)
考虑换手xx后的年化 = xx

---

**Cursor**

Now the edit to `plot_term`.

改好了 `plot_term` 的悬停（`TabInspect.cpp`）：

- 不再按整列显示那一大行 stat；改为就近命中一个**可见**节点（组 × 档 × 持有期，≤10 px，隐藏的图例项不参与），命中点画高亮圈。
- tooltip 全按单边 bp/期：
  - `毛 单边 x bp`（LS 点 = ls_mean/2，括号里给 G20 多 / G1 空；L/S 点就是该腿）
  - `成本 开仓 x bp (buy/sell)  关仓 x bp  换手 x%  → 均摊 x bp/期`（毛档不显示这行；L/S 点注明 均摊 = 换手 × (开仓 + 关仓)）
  - `净 单边 x bp/期 × 2 腿 × N 期/年 = 年化 x%`，这个数和画出来的 y 严格相等（LS 乘 2 腿，L/S 不乘）。

你编译跑一下看效果。

---

**User**

bullet多分几行, 还是不够清楚

---

**Cursor**

改成一行一项、三列对齐（名目 | 数值 | 灰色注释），LS 5w @ T+1 会长这样：

```
LS 5w  h=T+1
──────────────
毛 (单边)    +4.65 bp/期    G20 多 +3.35 / G1 空 +5.87 的平均

开仓冲击     17.10 bp       新进者建仓冲击 5w: buy 14.18 / sell 20.02 的平均
关仓冲击     20.00 bp       Config::sell_impact, 固定
换手         91 %           新进者占比 (t−h 不在该组): L 91% / S 91% 的平均
均摊成本     31.38 bp/期    = 换手 × (开仓 + 关仓), 两腿各算再平均 (L 30.97 / S 31.66)

净 (单边)    -26.73 bp/期   = 毛 − 均摊成本
年化         -129.4 %       = 净 × 2 腿 × 242 期/年
```

毛档只有 毛 / 年化 两行；L、S 点去掉"平均"的注释。列位用 `SameLine` 绝对 x 定位，不靠 printf 补空格（中文字节宽和字形宽不一致会错位）。

# Factor adjustment with filters
_Exported on 10/13/2026 at 11:35:18 GMT+8 from Cursor (3.21.18)_

---

**User**

@README 
@cursor_search_operator_implementation_i.md 
你来尝试改造, 试试加上流动性过滤, 和TS算子的更凸位置的过滤, 有没有办法把我们的因子, 带着5W的冲击成本, 把30~60m的收益做正

你觉得是直接改代码好, 还是你自己先做一个脚本试
如果真的能变正, 说明我们的思路应该是可行的

---

**Cursor**

先看一下上下文。

Now the implementation in `FactorsLoad.cpp`.

Now wire it into Inspect: `sell_impact` into the request, and the call after the C step.

Now format and lint the touched files.

改完了，走的是「先探针、不动架构」的路线。

**为什么不直接改代码**：评估层腿门控要动 Stat 契约 + Cpu/Gpu/Check + Search + Inspect + 看板，是中等规模改动；而先验数字并不乐观——`ts_vwap_dev_60m` 在 60m 的 G20 毛超额只有 3.4bp/期、LS 9.6bp，而 5w 一腿一次进场就要付 14bp 建仓 + 20bp 固定平仓。要做正，尾部收紧后每笔毛收益得翻 5–10 倍，这事得先看数据。Python 脚本不可行（没有特征库的 Python 读端，codec 得重写），所以探针直接挂在 Inspect 的现有数据上。

**探针 `gate_probe`**（`FactorsLoad.cpp`，Inspect 的 C 步之后调用，结果打到 stdout）：
- 只换 L/S 两腿的成员定义，IC/分组不动：成员 = 尾部分位 q ∈ {5%, 2%, 1%, 0.5%, 0.2%}（做多 rx ≥ 1−q，做空 rx ≤ q；5% ≈ 现在的 G20/G1）∧ 该金额档建仓冲击 ≤ c ∈ {∞, 30, 20, 10, 5bp}（做多看 buy，做空看 sell）。
- 成本规则与契约【换手成本】完全一致：t−h 时不是成员 = 新进者，付建仓冲击 + `sell_impact`，留仓不付。超额 = 标签 − 该行 J 均值，与 Stat 同口径。
- 一遍 [T][A] 攒 4D 直方（本行/滞后行 × 尾部深度/冲击深度），25 个 (q, c) 组合全部由后缀和得出，不重复扫数据。
- 输出：每 (hold, 金额档, 腿) 一张 5×5 表，每格 `净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util`。

`InspectRequest` 加了 `sell_impact`（只给探针用，不进缓存键）。

**你跑**：编译 → Factors 页选 `ts_vwap_dev_60m`（或 `ts_mom_30m`）进 Inspect，终端看 `==== gate_probe ...` 段，重点看 `hold=30m / 60m, amt=5w` 的 LONG 表：沿行往下看毛 bp 是否随 q 收紧明显抬升（凸尾），沿列往右看新进者冲击 bp 是否降到个位数，净那一列有没有变正。SHORT 表按之前的分析大概率仍是线性尾部。

读数时的两个判断点：如果净做不正但 `毛 − 冲击` 已经 > 0，说明卡的是 20bp 的固定 `sell_impact`，那是成本参数问题不是因子问题；如果 util 在变正的格里已经 < 5%，说明年化到资金上没有意义。结论出来再决定是把门控正式进 Stat 契约，还是删掉探针。

---

**User**

好了跑完了, 你看

==== gate_probe mom_30m.json  sell_impact=10bp  n_full=16.6  (q = 腿宽尾部分位, c = 建仓冲击阈; 多腿看 buy, 空腿看 sell) ====
-- hold=1m  amt=5w  rows=45249  periods/yr=61710
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -4.6|  0.7|  8.1| 29|100|  -2844     -4.6|  0.7|  8.1| 29|100|  -2844     -4.6|  0.7|  8.1| 29|100|  -2844     -7.2| -0.1|  6.1| 44| 53|  -2342     -8.2| -0.3|  3.4| 59| 22|  -1095  
  q 2.0%     -4.6|  0.7|  7.9| 30| 41|  -1169     -4.6|  0.7|  7.9| 30| 41|  -1169     -4.6|  0.7|  7.9| 30| 41|  -1169     -7.3| -0.1|  5.9| 45| 22|   -988     -8.4| -0.3|  3.3| 60|  9|   -487  
  q 1.0%     -4.7|  0.8|  7.8| 31| 21|   -602     -4.7|  0.8|  7.8| 31| 21|   -602     -4.7|  0.8|  7.8| 31| 21|   -602     -7.5| -0.1|  5.8| 47| 11|   -517     -8.5| -0.3|  3.3| 61|  5|   -262  
  q 0.5%     -4.9|  0.9|  7.7| 33| 10|   -315     -4.9|  0.9|  7.7| 33| 10|   -315     -4.9|  0.9|  7.7| 33| 10|   -315     -7.7| -0.0|  5.7| 49|  6|   -271     -8.5| -0.3|  3.3| 62|  3|   -137  
  q 0.2%     -4.9|  1.2|  7.5| 35|  5|   -160     -4.9|  1.2|  7.5| 35|  5|   -160     -4.9|  1.2|  7.5| 35|  5|   -160     -7.7|  0.2|  5.7| 50|  3|   -140     -8.5| -0.1|  3.3| 63|  1|    -73  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -5.6|  0.4| 16.3| 23|100|  -3439     -5.5|  0.1| 12.0| 26| 92|  -3146     -6.0| -0.0| 10.0| 30| 82|  -3019     -7.1| -0.2|  6.1| 43| 51|  -2237     -7.8| -0.2|  3.3| 57| 21|  -1016  
  q 2.0%     -5.2|  0.3| 16.6| 21| 41|  -1301     -5.3| -0.0| 11.9| 24| 37|  -1205     -5.9| -0.1|  9.8| 29| 33|  -1183     -7.0| -0.2|  5.9| 43| 21|   -904     -7.2| -0.0|  3.3| 54|  9|   -420  
  q 1.0%     -5.1|  0.4| 16.8| 20| 20|   -637     -5.2| -0.0| 11.6| 24| 18|   -593     -5.8| -0.1|  9.5| 29| 16|   -587     -6.7| -0.0|  5.8| 42| 11|   -445     -6.2|  0.4|  3.2| 50|  5|   -202  
  q 0.5%     -4.9|  0.7| 16.4| 21| 10|   -309     -5.1|  0.3| 11.4| 25|  9|   -292     -5.6|  0.2|  9.3| 30|  8|   -286     -6.1|  0.4|  5.6| 42|  6|   -207     -4.9|  1.0|  3.0| 46|  3|    -91  
  q 0.2%     -4.4|  1.1| 15.9| 21|  5|   -141     -4.8|  0.6| 11.1| 26|  5|   -139     -5.2|  0.6|  9.0| 31|  4|   -135     -5.5|  0.7|  5.4| 41|  3|    -98     -4.0|  1.5|  2.8| 43|  2|    -41  
-- hold=1m  amt=30w  rows=45249  periods/yr=61710
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -5.1|  0.7|  9.6| 29|100|  -3118     -5.1|  0.7|  9.6| 29|100|  -3118     -5.1|  0.7|  9.6| 29|100|  -3118    -10.4| -0.6|  6.6| 59| 14|   -926     -9.4| -0.5|  3.4| 67|  4|   -253  
  q 2.0%     -5.1|  0.7|  9.5| 30| 41|  -1291     -5.1|  0.7|  9.5| 30| 41|  -1291     -5.1|  0.7|  9.5| 30| 41|  -1291    -10.8| -0.6|  6.5| 62|  6|   -415     -9.6| -0.5|  3.3| 69|  2|   -118  
  q 1.0%     -5.3|  0.8|  9.5| 31| 21|   -669     -5.3|  0.8|  9.5| 31| 21|   -669     -5.3|  0.8|  9.5| 31| 21|   -669    -10.9| -0.7|  6.4| 62|  3|   -224     -9.5| -0.4|  3.3| 68|  1|    -65  
  q 0.5%     -5.5|  0.9|  9.4| 33| 10|   -352     -5.5|  0.9|  9.4| 33| 10|   -352     -5.5|  0.9|  9.4| 33| 10|   -352    -10.9| -0.5|  6.4| 63|  2|   -119     -9.1| -0.2|  3.3| 67|  1|    -35  
  q 0.2%     -5.5|  1.2|  9.4| 35|  5|   -181     -5.5|  1.2|  9.4| 35|  5|   -181     -5.5|  1.2|  9.4| 35|  5|   -181    -10.8| -0.4|  6.3| 64|  1|    -63     -8.6| -0.1|  3.2| 64|  0|    -18  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -12.7|  0.4| 47.5| 23|100|  -7811    -10.5| -0.4| 17.8| 36| 56|  -3619    -10.7| -0.6| 12.7| 44| 38|  -2484     -9.8| -0.6|  6.7| 55| 15|   -923     -6.4|  0.3|  3.3| 50|  5|   -215  
  q 2.0%    -11.5|  0.3| 47.2| 21| 41|  -2889    -10.4| -0.6| 17.4| 36| 23|  -1486    -10.6| -0.7| 12.4| 44| 16|  -1042     -9.0| -0.3|  6.5| 52|  7|   -392     -4.6|  0.9|  3.1| 42|  3|    -89  
  q 1.0%    -10.9|  0.4| 45.4| 20| 20|  -1369    -10.2| -0.5| 17.0| 36| 12|   -751    -10.1| -0.6| 12.0| 43|  8|   -531     -7.6|  0.1|  6.2| 48|  4|   -196     -3.0|  1.4|  2.9| 34|  2|    -41  
  q 0.5%    -10.5|  0.7| 42.8| 21| 10|   -660     -9.7| -0.3| 16.4| 36|  6|   -372     -9.2| -0.2| 11.5| 42|  5|   -258     -5.9|  0.9|  5.8| 43|  2|    -89     -1.5|  2.2|  2.5| 30|  2|    -14  
  q 0.2%     -9.6|  1.1| 40.5| 21|  5|   -308     -9.2|  0.1| 15.9| 36|  3|   -183     -8.2|  0.3| 11.1| 41|  2|   -123     -4.6|  1.4|  5.4| 40|  1|    -41     -0.6|  2.7|  2.2| 27|  1|     -4  
-- hold=5m  amt=5w  rows=44521  periods/yr=12342
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -6.8|  2.1|  7.9| 50|100|   -841     -6.8|  2.1|  7.9| 50|100|   -841     -6.8|  2.1|  7.9| 50|100|   -841     -9.1|  0.7|  5.9| 62| 53|   -595     -9.2|  0.6|  3.3| 74| 22|   -247  
  q 2.0%     -6.9|  2.4|  7.8| 52| 41|   -346     -6.9|  2.4|  7.8| 52| 41|   -346     -6.9|  2.4|  7.8| 52| 41|   -346     -9.3|  0.8|  5.7| 65| 22|   -251     -9.2|  0.9|  3.3| 76|  9|   -106  
  q 1.0%     -6.9|  2.7|  7.7| 54| 21|   -175     -6.9|  2.7|  7.7| 54| 21|   -175     -6.9|  2.7|  7.7| 54| 21|   -175     -9.4|  1.0|  5.6| 66| 11|   -129     -9.0|  1.1|  3.3| 77|  5|    -55  
  q 0.5%     -6.8|  3.3|  7.6| 57| 10|    -86     -6.8|  3.3|  7.6| 57| 10|    -86     -6.8|  3.3|  7.6| 57| 10|    -86     -9.1|  1.7|  5.6| 69|  6|    -64     -8.5|  1.8|  3.2| 78|  3|    -27  
  q 0.2%     -6.3|  4.3|  7.5| 60|  5|    -41     -6.3|  4.3|  7.5| 60|  5|    -41     -6.3|  4.3|  7.5| 60|  5|    -41     -8.9|  2.2|  5.5| 72|  3|    -32     -8.1|  2.4|  3.2| 79|  1|    -14  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -9.7|  1.5| 17.2| 41|100|  -1202     -8.7|  0.5| 11.4| 43| 92|   -992     -8.8|  0.3|  9.5| 47| 82|   -889     -9.2|  0.1|  5.9| 58| 51|   -577     -8.8|  0.4|  3.3| 70| 21|   -227  
  q 2.0%     -9.5|  1.6| 17.6| 40| 41|   -476     -8.6|  0.4| 11.2| 42| 37|   -391     -8.7|  0.3|  9.2| 47| 33|   -351     -8.9|  0.2|  5.7| 58| 21|   -230     -7.5|  1.2|  3.1| 67|  9|    -87  
  q 1.0%     -9.2|  2.1| 17.6| 41| 20|   -230     -8.3|  0.8| 11.0| 43| 18|   -189     -8.2|  0.8|  9.0| 48| 16|   -166     -8.0|  1.0|  5.5| 58| 11|   -105     -5.3|  2.9|  2.9| 63|  5|    -34  
  q 0.5%     -8.2|  3.5| 17.2| 43| 10|   -104     -7.3|  2.2| 10.7| 46|  9|    -83     -7.3|  2.1|  8.7| 50|  8|    -73     -5.9|  2.9|  5.2| 58|  5|    -40     -2.2|  5.5|  2.7| 60|  3|     -8  
  q 0.2%     -7.0|  5.1| 16.6| 46|  5|    -45     -6.3|  3.6| 10.3| 49|  5|    -36     -6.0|  3.6|  8.3| 52|  4|    -31     -4.1|  4.7|  4.9| 59|  3|    -14     +0.8|  8.1|  2.4| 59|  2|     +2  
-- hold=5m  amt=30w  rows=44521  periods/yr=12342
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -7.6|  2.1|  9.5| 50|100|   -940     -7.6|  2.1|  9.5| 50|100|   -940     -7.6|  2.1|  9.5| 50|100|   -940    -12.3|  0.6|  6.4| 78| 14|   -216     -9.9|  1.0|  3.2| 83|  4|    -52  
  q 2.0%     -7.8|  2.4|  9.5| 52| 41|   -391     -7.8|  2.4|  9.5| 52| 41|   -391     -7.8|  2.4|  9.5| 52| 41|   -391    -12.3|  0.7|  6.4| 80|  6|    -94     -9.0|  2.0|  3.2| 83|  2|    -22  
  q 1.0%     -7.8|  2.7|  9.4| 54| 21|   -199     -7.8|  2.7|  9.4| 54| 21|   -199     -7.8|  2.7|  9.4| 54| 21|   -199    -12.0|  1.1|  6.3| 80|  3|    -49     -8.1|  2.7|  3.2| 82|  1|    -11  
  q 0.5%     -7.8|  3.3|  9.4| 57| 10|    -99     -7.8|  3.3|  9.4| 57| 10|    -99     -7.8|  3.3|  9.4| 57| 10|    -99    -10.9|  2.3|  6.3| 81|  2|    -24     -6.4|  4.1|  3.2| 80|  1|     -5  
  q 0.2%     -7.4|  4.3|  9.3| 60|  5|    -48     -7.4|  4.3|  9.3| 60|  5|    -48     -7.4|  4.3|  9.3| 60|  5|    -48    -10.7|  2.6|  6.2| 82|  1|    -12     -3.8|  6.5|  3.1| 78|  0|     -2  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -23.8|  1.5| 51.4| 41|100|  -2938    -15.0| -0.3| 16.9| 55| 56|  -1027    -14.0| -0.2| 12.1| 63| 37|   -648    -11.5|  0.3|  6.4| 72| 15|   -213     -5.1|  3.2|  2.9| 64|  5|    -34  
  q 2.0%    -22.7|  1.6| 50.6| 40| 41|  -1140    -14.6| -0.3| 16.4| 54| 23|   -416    -13.5| -0.1| 11.7| 62| 16|   -264     -9.6|  1.2|  6.1| 67|  7|    -83     -1.5|  5.5|  2.6| 55|  3|     -6  
  q 1.0%    -21.8|  2.1| 48.8| 41| 20|   -548    -13.8|  0.1| 15.9| 54| 12|   -204    -12.1|  0.7| 11.2| 60|  8|   -126     -7.1|  2.6|  5.6| 62|  4|    -36     +1.6|  7.5|  2.2| 48|  2|     +4  
  q 0.5%    -20.4|  3.5| 45.4| 43| 10|   -257    -12.1|  1.7| 15.1| 55|  6|    -92     -9.7|  2.5| 10.5| 59|  5|    -54     -3.1|  5.6|  5.0| 58|  2|     -9     +4.8| 10.2|  1.7| 46|  2|     +9  
  q 0.2%    -19.3|  5.1| 43.4| 46|  5|   -123    -10.3|  3.4| 14.3| 56|  3|    -41     -7.1|  4.6|  9.8| 59|  2|    -21     -0.8|  7.3|  4.3| 57|  1|     -1     +6.2| 11.5|  1.4| 46|  1|     +7  
-- hold=15m  amt=5w  rows=42701  periods/yr=4114
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -9.6|  3.3|  7.9| 72|100|   -395     -9.6|  3.3|  7.9| 72|100|   -395     -9.6|  3.3|  7.9| 72|100|   -395    -11.0|  1.6|  5.8| 80| 52|   -238     -9.9|  1.6|  3.3| 86| 21|    -87  
  q 2.0%     -9.3|  4.1|  7.7| 76| 41|   -156     -9.3|  4.1|  7.7| 76| 41|   -156     -9.3|  4.1|  7.7| 76| 41|   -156    -10.8|  2.1|  5.7| 82| 22|    -96     -9.0|  2.7|  3.3| 88|  9|    -34  
  q 1.0%     -8.9|  4.8|  7.7| 78| 21|    -75     -8.9|  4.8|  7.7| 78| 21|    -75     -8.9|  4.8|  7.7| 78| 21|    -75    -10.6|  2.5|  5.6| 84| 11|    -48     -8.6|  3.1|  3.2| 89|  5|    -18  
  q 0.5%     -7.5|  6.5|  7.6| 80| 10|    -32     -7.5|  6.5|  7.6| 80| 10|    -32     -7.5|  6.5|  7.6| 80| 10|    -32     -9.7|  3.6|  5.5| 86|  6|    -23     -7.9|  4.0|  3.2| 90|  3|     -8  
  q 0.2%     -5.9|  8.5|  7.5| 83|  5|    -13     -5.9|  8.5|  7.5| 83|  5|    -13     -5.9|  8.5|  7.5| 83|  5|    -13     -8.6|  5.0|  5.4| 88|  3|    -10     -6.8|  5.2|  3.2| 91|  1|     -4  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -14.9|  2.3| 16.5| 65|100|   -613    -13.0|  0.9| 11.0| 66| 92|   -492    -12.6|  0.6|  9.2| 69| 81|   -421    -11.5|  0.5|  5.8| 76| 50|   -238     -9.8|  1.1|  3.2| 83| 21|    -83  
  q 2.0%    -14.6|  2.9| 16.6| 66| 41|   -244    -12.7|  1.3| 10.8| 67| 37|   -192    -12.2|  1.0|  8.9| 70| 33|   -164    -10.7|  1.3|  5.5| 77| 21|    -91     -7.6|  3.0|  2.9| 82|  9|    -29  
  q 1.0%    -14.0|  3.7| 16.4| 67| 20|   -118    -12.0|  2.1| 10.6| 69| 18|    -91    -11.4|  1.9|  8.6| 71| 16|    -76     -9.3|  2.5|  5.2| 77| 11|    -41     -4.9|  5.2|  2.6| 80|  5|    -11  
  q 0.5%    -11.8|  6.4| 16.0| 70| 10|    -50     -9.5|  5.0| 10.2| 72|  9|    -36     -9.0|  4.5|  8.2| 74|  8|    -30     -6.1|  5.5|  4.8| 79|  5|    -14     -0.2|  9.6|  2.3| 80|  3|     -0  
  q 0.2%     -9.9|  8.8| 15.4| 74|  5|    -21     -7.9|  7.0|  9.7| 76|  5|    -15     -6.7|  7.0|  7.7| 78|  4|    -11     -3.7|  7.9|  4.4| 81|  3|     -4     +3.5| 13.2|  1.9| 81|  2|     +2  
-- hold=15m  amt=30w  rows=42701  periods/yr=4114
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -10.8|  3.3|  9.5| 72|100|   -444    -10.8|  3.3|  9.5| 72|100|   -444    -10.8|  3.3|  9.5| 72|100|   -444    -12.9|  1.8|  6.4| 90| 14|    -73     -9.1|  3.0|  3.1| 92|  4|    -15  
  q 2.0%    -10.6|  4.1|  9.5| 76| 41|   -178    -10.6|  4.1|  9.5| 76| 41|   -178    -10.6|  4.1|  9.5| 76| 41|   -178    -11.8|  2.9|  6.3| 91|  6|    -29     -6.8|  5.3|  3.1| 92|  2|     -5  
  q 1.0%    -10.3|  4.8|  9.4| 78| 21|    -87    -10.3|  4.8|  9.4| 78| 21|    -87    -10.3|  4.8|  9.4| 78| 21|    -87    -11.3|  3.4|  6.2| 91|  3|    -15     -6.9|  5.0|  3.1| 91|  1|     -3  
  q 0.5%     -9.0|  6.5|  9.4| 80| 10|    -38     -9.0|  6.5|  9.4| 80| 10|    -38     -9.0|  6.5|  9.4| 80| 10|    -38     -9.6|  5.1|  6.1| 91|  2|     -7     -5.9|  5.7|  3.0| 90|  1|     -1  
  q 0.2%     -7.5|  8.5|  9.3| 83|  5|    -16     -7.5|  8.5|  9.3| 83|  5|    -16     -7.5|  8.5|  9.3| 83|  5|    -16     -7.4|  7.3|  6.0| 92|  1|     -3     -3.0|  8.4|  2.8| 89|  0|     -0  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -36.6|  2.3| 50.0| 65|100|  -1507    -19.6| -0.1| 16.4| 74| 55|   -444    -17.1|  0.0| 11.7| 79| 37|   -259    -12.3|  1.3|  6.1| 84| 15|    -73     -3.4|  6.5|  2.5| 79|  5|     -7  
  q 2.0%    -35.2|  2.9| 48.1| 66| 41|   -590    -18.6|  0.6| 15.8| 74| 23|   -174    -15.5|  1.2| 11.2| 79| 16|   -100     -8.8|  3.9|  5.5| 82|  7|    -25     +1.9| 10.8|  2.0| 74|  3|     +2  
  q 1.0%    -33.5|  3.7| 45.5| 67| 20|   -281    -17.1|  1.6| 15.1| 75| 12|    -83    -13.4|  2.6| 10.5| 78|  8|    -46     -5.6|  6.1|  4.8| 79|  4|     -9     +6.1| 14.4|  1.5| 72|  2|     +6  
  q 0.5%    -30.4|  6.4| 42.5| 70| 10|   -128    -13.7|  4.6| 14.0| 77|  6|    -35     -9.6|  5.8|  9.5| 79|  4|    -18     +0.0| 11.0|  4.0| 79|  2|     +0    +10.5| 18.6|  1.1| 73|  2|     +7  
  q 0.2%    -28.5|  8.8| 40.5| 74|  5|    -61    -11.1|  7.1| 12.9| 79|  3|    -15     -6.4|  8.6|  8.6| 81|  2|     -6     +2.8| 13.4|  3.3| 80|  1|     +2    +12.3| 20.5|  0.9| 75|  1|     +5  
-- hold=30m  amt=5w  rows=39971  periods/yr=2057
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -12.1|  4.0|  7.8| 91|100|   -249    -12.1|  4.0|  7.8| 91|100|   -249    -12.1|  4.0|  7.8| 91|100|   -249    -12.6|  2.1|  5.7| 93| 52|   -134    -10.4|  2.2|  3.3| 95| 21|    -45  
  q 2.0%    -11.5|  4.9|  7.7| 93| 41|    -97    -11.5|  4.9|  7.7| 93| 41|    -97    -11.5|  4.9|  7.7| 93| 41|    -97    -12.1|  2.7|  5.6| 95| 22|    -53     -9.2|  3.6|  3.2| 96|  9|    -17  
  q 1.0%    -11.0|  5.6|  7.6| 94| 21|    -46    -11.0|  5.6|  7.6| 94| 21|    -46    -11.0|  5.6|  7.6| 94| 21|    -46    -11.7|  3.1|  5.5| 96| 11|    -27     -8.8|  4.0|  3.2| 97|  5|     -9  
  q 0.5%     -9.2|  7.5|  7.5| 95| 10|    -20     -9.2|  7.5|  7.5| 95| 10|    -20     -9.2|  7.5|  7.5| 95| 10|    -20    -10.6|  4.3|  5.5| 97|  6|    -12     -8.2|  4.6|  3.2| 98|  3|     -4  
  q 0.2%     -7.7|  9.1|  7.5| 96|  5|     -8     -7.7|  9.1|  7.5| 96|  5|     -8     -7.7|  9.1|  7.5| 96|  5|     -8     -9.3|  5.7|  5.4| 98|  3|     -6     -7.5|  5.4|  3.1| 98|  1|     -2  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -20.0|  2.6| 15.9| 87|100|   -411    -17.2|  1.0| 10.8| 88| 92|   -324    -16.3|  0.6|  9.0| 89| 81|   -271    -13.8|  0.5|  5.7| 91| 50|   -142    -11.0|  1.3|  3.0| 94| 20|    -46  
  q 2.0%    -19.3|  3.8| 15.8| 89| 41|   -161    -16.4|  2.1| 10.5| 90| 37|   -124    -15.4|  1.5|  8.7| 91| 32|   -103    -12.4|  1.9|  5.4| 93| 20|    -52     -8.5|  3.7|  2.7| 96|  9|    -16  
  q 1.0%    -17.8|  5.6| 15.6| 91| 20|    -75    -14.7|  3.8| 10.2| 92| 18|    -56    -13.6|  3.4|  8.3| 93| 16|    -45    -10.2|  4.0|  4.9| 95| 10|    -22     -5.6|  6.3|  2.3| 97|  5|     -6  
  q 0.5%    -14.0|  9.6| 15.2| 94| 10|    -29    -10.3|  8.2|  9.7| 94|  9|    -20     -9.3|  7.5|  7.8| 95|  8|    -16     -5.1|  8.9|  4.5| 96|  5|     -6     +0.2| 11.9|  1.9| 98|  3|     +0  
  q 0.2%     -9.3| 14.6| 14.9| 96|  5|    -10     -5.4| 13.0|  9.2| 96|  5|     -5     -3.6| 13.0|  7.2| 96|  4|     -3     +0.7| 14.4|  4.0| 98|  3|     +0     +7.6| 19.1|  1.6| 99|  2|     +3  
-- hold=30m  amt=30w  rows=39971  periods/yr=2057
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -13.6|  4.0|  9.5| 91|100|   -281    -13.6|  4.0|  9.5| 91|100|   -281    -13.6|  4.0|  9.5| 91|100|   -281    -12.9|  2.8|  6.3| 97| 13|    -36     -8.1|  4.7|  3.0| 98|  4|     -7  
  q 2.0%    -13.2|  4.9|  9.5| 93| 41|   -111    -13.2|  4.9|  9.5| 93| 41|   -111    -13.2|  4.9|  9.5| 93| 41|   -111    -11.2|  4.6|  6.2| 97|  6|    -14     -4.3|  8.5|  3.0| 99|  2|     -2  
  q 1.0%    -12.7|  5.6|  9.4| 94| 21|    -54    -12.7|  5.6|  9.4| 94| 21|    -54    -12.7|  5.6|  9.4| 94| 21|    -54    -10.3|  5.5|  6.1| 98|  3|     -7     -3.3|  9.5|  2.9| 99|  1|     -1  
  q 0.5%    -10.9|  7.5|  9.3| 95| 10|    -23    -10.9|  7.5|  9.3| 95| 10|    -23    -10.9|  7.5|  9.3| 95| 10|    -23     -7.6|  8.1|  6.0| 98|  2|     -3     -2.4| 10.2|  2.8| 99|  1|     -0  
  q 0.2%     -9.4|  9.1|  9.3| 96|  5|    -10     -9.4|  9.1|  9.3| 96|  5|    -10     -9.4|  9.1|  9.3| 96|  5|    -10     -4.6| 11.0|  5.8| 99|  1|     -1     +1.2| 13.6|  2.5| 99|  0|     +0  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -48.0|  2.6| 48.0| 87|100|   -987    -23.9| -0.3| 16.1| 91| 54|   -268    -20.1| -0.3| 11.5| 92| 36|   -149    -13.5|  1.5|  5.7| 95| 14|    -39     -3.6|  7.9|  2.1| 96|  5|     -4  
  q 2.0%    -45.7|  3.8| 45.4| 89| 41|   -382    -22.3|  1.0| 15.3| 92| 23|   -104    -18.1|  1.3| 10.7| 94| 15|    -57     -9.9|  4.4|  4.9| 96|  7|    -14     +1.5| 12.7|  1.5| 98|  3|     +1  
  q 1.0%    -42.6|  5.6| 42.7| 91| 20|   -178    -19.7|  3.1| 14.3| 94| 12|    -47    -14.9|  3.9|  9.7| 95|  8|    -25     -5.6|  8.0|  4.0| 97|  4|     -5     +6.6| 17.4|  1.1| 98|  2|     +3  
  q 0.5%    -37.1|  9.6| 39.9| 94| 10|    -78    -14.5|  7.5| 13.0| 96|  6|    -18     -9.4|  8.6|  8.6| 97|  4|     -9     +0.9| 14.0|  3.3| 98|  2|     +0    +10.9| 21.6|  0.8| 99|  2|     +4  
  q 0.2%    -31.8| 14.6| 38.5| 96|  5|    -34     -8.4| 12.9| 11.9| 97|  3|     -6     -3.0| 14.3|  7.6| 98|  2|     -1     +7.0| 19.5|  2.7| 99|  1|     +2    +15.9| 26.4|  0.6|100|  1|     +3  
-- hold=60m  amt=5w  rows=34511  periods/yr=1028
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -11.2|  4.9|  7.9| 90|100|   -115    -11.2|  4.9|  7.9| 90|100|   -115    -11.2|  4.9|  7.9| 90|100|   -115    -12.1|  2.6|  5.8| 93| 51|    -63     -9.8|  2.9|  3.3| 95| 20|    -20  
  q 2.0%     -9.5|  7.0|  7.8| 93| 41|    -40     -9.5|  7.0|  7.8| 93| 41|    -40     -9.5|  7.0|  7.8| 93| 41|    -40    -10.8|  4.1|  5.7| 95| 21|    -23     -8.0|  4.9|  3.3| 97|  9|     -7  
  q 1.0%     -8.3|  8.5|  7.7| 95| 21|    -18     -8.3|  8.5|  7.7| 95| 21|    -18     -8.3|  8.5|  7.7| 95| 21|    -18     -9.7|  5.2|  5.6| 96| 11|    -11     -6.6|  6.3|  3.2| 97|  5|     -3  
  q 0.5%     -5.5| 11.4|  7.6| 96| 10|     -6     -5.5| 11.4|  7.6| 96| 10|     -6     -5.5| 11.4|  7.6| 96| 10|     -6     -7.9|  7.1|  5.5| 97|  6|     -5     -4.6|  8.3|  3.1| 98|  3|     -1  
  q 0.2%     -4.3| 12.7|  7.5| 97|  5|     -2     -4.3| 12.7|  7.5| 97|  5|     -2     -4.3| 12.7|  7.5| 97|  5|     -2     -6.6|  8.5|  5.4| 98|  3|     -2     -3.7|  9.2|  3.1| 99|  1|     -1  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -20.9|  2.6| 16.4| 89|100|   -215    -18.1|  0.6| 10.8| 90| 91|   -169    -17.2|  0.0|  9.0| 91| 80|   -142    -14.9| -0.4|  5.7| 93| 49|    -75    -12.1|  0.3|  3.0| 95| 20|    -25  
  q 2.0%    -20.2|  4.1| 16.2| 92| 41|    -84    -17.3|  1.8| 10.5| 93| 36|    -65    -16.3|  1.2|  8.7| 94| 32|    -54    -13.3|  1.3|  5.3| 95| 20|    -28     -8.9|  3.3|  2.6| 97|  9|     -8  
  q 1.0%    -17.8|  6.8| 16.0| 95| 20|    -37    -14.6|  4.5| 10.1| 95| 18|    -28    -13.4|  4.1|  8.2| 96| 16|    -22     -9.2|  5.2|  4.9| 97| 10|    -10     -4.6|  7.3|  2.2| 98|  5|     -2  
  q 0.5%    -13.1| 11.5| 15.6| 96| 10|    -14     -9.4|  9.6|  9.6| 97|  9|     -9     -8.2|  9.0|  7.7| 97|  8|     -7     -3.1| 11.0|  4.4| 98|  5|     -2     +2.2| 13.9|  1.8| 99|  3|     +1  
  q 0.2%     -7.9| 16.7| 15.2| 97|  5|     -4     -4.1| 14.5|  9.1| 98|  5|     -2     -1.7| 15.1|  7.1| 98|  4|     -1     +4.5| 18.2|  3.9| 99|  3|     +1    +10.1| 21.6|  1.5| 99|  2|     +2  
-- hold=60m  amt=30w  rows=34511  periods/yr=1028
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -12.7|  4.9|  9.5| 90|100|   -130    -12.7|  4.9|  9.5| 90|100|   -130    -12.7|  4.9|  9.5| 90|100|   -130    -12.3|  3.5|  6.3| 97| 13|    -16     -6.6|  6.2|  3.0| 98|  4|     -3  
  q 2.0%    -11.1|  7.0|  9.5| 93| 41|    -47    -11.1|  7.0|  9.5| 93| 41|    -47    -11.1|  7.0|  9.5| 93| 41|    -47     -9.1|  6.8|  6.2| 97|  6|     -5     -1.2| 11.6|  3.0| 99|  2|     -0  
  q 1.0%     -9.9|  8.5|  9.4| 95| 21|    -21     -9.9|  8.5|  9.4| 95| 21|    -21     -9.9|  8.5|  9.4| 95| 21|    -21     -6.4|  9.4|  6.1| 98|  3|     -2     +2.5| 15.3|  2.9| 99|  1|     +0  
  q 0.5%     -7.2| 11.4|  9.3| 96| 10|     -8     -7.2| 11.4|  9.3| 96| 10|     -8     -7.2| 11.4|  9.3| 96| 10|     -8     -2.6| 13.1|  5.9| 99|  2|     -0     +5.1| 17.7|  2.7| 99|  1|     +0  
  q 0.2%     -6.0| 12.7|  9.3| 97|  5|     -3     -6.0| 12.7|  9.3| 97|  5|     -3     -6.0| 12.7|  9.3| 97|  5|     -3     +0.6| 16.2|  5.7| 99|  1|     +0     +8.4| 20.8|  2.4|100|  0|     +0  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -50.7|  2.6| 49.7| 89|100|   -521    -25.3| -1.3| 16.1| 92| 53|   -139    -21.4| -1.3| 11.4| 94| 35|    -77    -14.8|  0.2|  5.7| 96| 14|    -21     -2.3|  9.2|  2.0| 96|  5|     -1  
  q 2.0%    -48.4|  4.1| 46.7| 92| 41|   -202    -23.5|  0.4| 15.2| 95| 22|    -54    -18.9|  0.7| 10.5| 96| 15|    -29    -10.5|  3.8|  4.7| 97|  7|     -7     +3.8| 14.9|  1.4| 98|  3|     +1  
  q 1.0%    -44.1|  6.8| 43.8| 95| 20|    -92    -19.3|  3.8| 14.0| 96| 12|    -23    -14.1|  4.8|  9.4| 97|  8|    -12     -4.7|  8.9|  3.8| 98|  4|     -2     +9.3| 20.1|  1.0| 98|  2|     +2  
  q 0.5%    -37.4| 11.5| 40.8| 96| 10|    -39    -12.6|  9.6| 12.7| 98|  6|     -8     -7.5| 10.4|  8.2| 98|  4|     -3     +3.4| 16.3|  3.0| 99|  2|     +1    +14.3| 24.9|  0.7| 99|  2|     +2  
  q 0.2%    -31.3| 16.7| 39.3| 97|  5|    -17     -5.6| 15.5| 11.5| 98|  3|     -2     +0.1| 17.1|  7.3| 99|  2|     +0    +11.3| 23.7|  2.5| 99|  1|     +2    +21.0| 31.5|  0.6|100|  1|     +2  
-- hold=close  amt=5w  rows=45900  periods/yr=242
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -8.1|  7.9|  7.6| 91|100|    -20     -8.1|  7.9|  7.6| 91|100|    -20     -8.1|  7.9|  7.6| 91|100|    -20     -8.8|  5.7|  5.5| 93| 53|    -11     -7.3|  5.2|  3.0| 95| 23|     -4  
  q 2.0%     -4.8| 11.6|  7.5| 94| 41|     -5     -4.8| 11.6|  7.5| 94| 41|     -5     -4.8| 11.6|  7.5| 94| 41|     -5     -5.4|  9.3|  5.4| 95| 22|     -3     -3.9|  8.7|  3.0| 97| 10|     -1  
  q 1.0%     -2.4| 14.2|  7.5| 95| 21|     -1     -2.4| 14.2|  7.5| 95| 21|     -1     -2.4| 14.2|  7.5| 95| 21|     -1     -3.0| 11.9|  5.4| 97| 11|     -1     -1.5| 11.1|  3.0| 97|  5|     -0  
  q 0.5%     +1.4| 18.2|  7.4| 96| 10|     +0     +1.4| 18.2|  7.4| 96| 10|     +0     +1.4| 18.2|  7.4| 96| 10|     +0     +0.1| 15.0|  5.3| 97|  6|     +0     +2.2| 14.9|  3.0| 98|  3|     +0  
  q 0.2%     +4.1| 21.0|  7.3| 97|  5|     +1     +4.1| 21.0|  7.3| 97|  5|     +1     +4.1| 21.0|  7.3| 97|  5|     +1     +3.3| 18.2|  5.2| 98|  3|     +0     +4.3| 16.9|  2.9| 98|  1|     +0  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -21.4|  1.1| 14.8| 91|100|    -52    -18.6|  0.1| 10.4| 92| 92|    -41    -17.6| -0.3|  8.7| 92| 82|    -35    -15.0| -0.5|  5.5| 94| 52|    -19    -12.2|  0.1|  2.8| 96| 22|     -7  
  q 2.0%    -21.9|  1.4| 14.7| 94| 41|    -21    -18.8|  0.2| 10.1| 95| 37|    -17    -17.5| -0.0|  8.4| 95| 33|    -14    -14.0|  0.5|  5.2| 96| 21|     -7     -8.7|  3.5|  2.6| 97| 10|     -2  
  q 1.0%    -19.5|  4.1| 14.6| 96| 20|    -10    -16.3|  2.8|  9.8| 96| 19|     -7    -14.4|  3.0|  8.0| 97| 16|     -6     -9.6|  4.8|  4.8| 97| 11|     -3     -3.2|  8.8|  2.3| 98|  6|     -0  
  q 0.5%    -15.1|  8.6| 14.4| 97| 10|     -4    -11.2|  7.8|  9.4| 98|  9|     -3     -9.2|  8.0|  7.6| 98|  8|     -2     -3.1| 11.0|  4.4| 98|  6|     -0     +4.1| 15.9|  2.0| 98|  3|     +0  
  q 0.2%     -8.6| 15.2| 14.2| 98|  5|     -1     -4.6| 14.2|  9.0| 98|  5|     -1     -1.4| 15.6|  7.2| 99|  4|     -0     +5.8| 19.7|  4.0| 99|  3|     +0    +14.9| 26.5|  1.7| 99|  2|     +1  
-- hold=close  amt=30w  rows=45900  periods/yr=242
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -9.6|  7.9|  9.3| 91|100|    -23     -9.6|  7.9|  9.3| 91|100|    -23     -9.6|  7.9|  9.3| 91|100|    -23     -8.9|  6.2|  5.7| 96| 16|     -3     -6.7|  5.1|  2.2| 96|  6|     -1  
  q 2.0%     -6.5| 11.6|  9.3| 94| 41|     -6     -6.5| 11.6|  9.3| 94| 41|     -6     -6.5| 11.6|  9.3| 94| 41|     -6     -4.0| 11.2|  5.6| 97|  7|     -1     -1.6| 10.4|  2.3| 97|  3|     -0  
  q 1.0%     -4.1| 14.2|  9.2| 95| 21|     -2     -4.1| 14.2|  9.2| 95| 21|     -2     -4.1| 14.2|  9.2| 95| 21|     -2     -0.6| 14.6|  5.6| 98|  4|     -0     +1.5| 13.5|  2.3| 97|  1|     +0  
  q 0.5%     -0.3| 18.2|  9.2| 96| 10|     -0     -0.3| 18.2|  9.2| 96| 10|     -0     -0.3| 18.2|  9.2| 96| 10|     -0     +3.6| 18.7|  5.5| 98|  2|     +0     +4.3| 16.1|  2.3| 97|  1|     +0  
  q 0.2%     +2.4| 21.0|  9.1| 97|  5|     +0     +2.4| 21.0|  9.1| 97|  5|     +0     +2.4| 21.0|  9.1| 97|  5|     +0     +8.8| 23.8|  5.4| 97|  1|     +0     +3.9| 15.6|  2.2| 96|  0|     +0  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -48.0|  1.1| 44.0| 91|100|   -116    -25.5| -1.8| 15.3| 94| 57|    -35    -21.9| -2.2| 10.8| 95| 39|    -20    -14.8| -0.1|  5.2| 96| 17|     -6     -1.9|  9.3|  1.7| 96|  7|     -0  
  q 2.0%    -47.4|  1.4| 41.9| 94| 41|    -47    -24.0| -0.5| 14.6| 96| 23|    -14    -19.9| -0.4| 10.2| 96| 16|     -8    -10.2|  4.1|  4.7| 97|  8|     -2     +7.5| 18.5|  1.4| 97|  4|     +1  
  q 1.0%    -43.6|  4.1| 39.7| 96| 20|    -21    -19.9|  3.2| 13.8| 97| 12|     -6    -15.0|  3.9|  9.4| 97|  9|     -3     -2.7| 10.9|  4.0| 98|  4|     -0    +16.3| 27.1|  1.1| 97|  2|     +1  
  q 0.5%    -37.7|  8.6| 37.7| 97| 10|     -9    -13.6|  8.7| 12.8| 98|  6|     -2     -8.3|  9.8|  8.5| 98|  5|     -1     +6.8| 19.9|  3.4| 98|  3|     +0    +23.2| 33.8|  0.9| 97|  2|     +1  
  q 0.2%    -30.7| 15.2| 36.7| 98|  5|     -4     -5.8| 15.7| 11.8| 99|  3|     -0     -0.0| 17.5|  7.7| 99|  2|     -0    +15.8| 28.5|  2.8| 99|  1|     +1    +32.4| 43.0|  0.7| 98|  1|     +1  
-- hold=T+1  amt=5w  rows=45900  periods/yr=242
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -13.4|  2.6|  7.6| 91|100|    -32    -13.4|  2.6|  7.6| 91|100|    -32    -13.4|  2.6|  7.6| 91|100|    -32    -15.6| -1.1|  5.5| 93| 53|    -20    -15.6| -3.1|  3.0| 95| 23|     -9  
  q 2.0%    -17.4| -1.0|  7.5| 94| 41|    -17    -17.4| -1.0|  7.5| 94| 41|    -17    -17.4| -1.0|  7.5| 94| 41|    -17    -20.6| -5.8|  5.4| 95| 22|    -11    -24.2|-11.5|  3.0| 97| 10|     -6  
  q 1.0%    -23.7| -7.1|  7.5| 95| 21|    -12    -23.7| -7.1|  7.5| 95| 21|    -12    -23.7| -7.1|  7.5| 95| 21|    -12    -28.5|-13.7|  5.4| 97| 11|     -8    -34.8|-22.2|  3.0| 97|  5|     -4  
  q 0.5%    -31.2|-14.4|  7.4| 96| 10|     -8    -31.2|-14.4|  7.4| 96| 10|     -8    -31.2|-14.4|  7.4| 96| 10|     -8    -39.1|-24.2|  5.3| 97|  6|     -5    -50.7|-38.1|  3.0| 98|  3|     -3  
  q 0.2%    -41.9|-25.0|  7.3| 97|  5|     -5    -41.9|-25.0|  7.3| 97|  5|     -5    -41.9|-25.0|  7.3| 97|  5|     -5    -52.5|-37.6|  5.2| 98|  3|     -4    -70.9|-58.3|  2.9| 98|  1|     -2  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -14.7|  7.9| 14.8| 91|100|    -36    -11.6|  7.1| 10.4| 92| 92|    -26    -10.5|  6.7|  8.7| 92| 82|    -21     -8.1|  6.4|  5.5| 94| 52|    -10     -9.7|  2.6|  2.8| 96| 22|     -5  
  q 2.0%    -12.1| 11.2| 14.7| 94| 41|    -12     -8.9| 10.2| 10.1| 95| 37|     -8     -7.8|  9.7|  8.4| 95| 33|     -6     -5.7|  8.9|  5.2| 96| 21|     -3    -10.9|  1.3|  2.6| 97| 10|     -3  
  q 1.0%     -8.3| 15.3| 14.6| 96| 20|     -4     -5.3| 13.8|  9.8| 96| 19|     -2     -4.1| 13.3|  8.0| 97| 16|     -2     -3.2| 11.3|  4.8| 97| 11|     -1    -15.9| -3.9|  2.3| 98|  6|     -2  
  q 0.5%     -6.2| 17.6| 14.4| 97| 10|     -2     -3.2| 15.7|  9.4| 98|  9|     -1     -2.4| 14.8|  7.6| 98|  8|     -0     -4.2| 10.0|  4.4| 98|  6|     -1    -25.8|-14.0|  2.0| 98|  3|     -2  
  q 0.2%     -2.6| 21.3| 14.2| 98|  5|     -0     -0.1| 18.6|  9.0| 98|  5|     -0     +1.2| 18.1|  7.2| 99|  4|     +0     -2.6| 11.2|  4.0| 99|  3|     -0    -27.3|-15.8|  1.7| 99|  2|     -1  
-- hold=T+1  amt=30w  rows=45900  periods/yr=242
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -14.9|  2.6|  9.3| 91|100|    -36    -14.9|  2.6|  9.3| 91|100|    -36    -14.9|  2.6|  9.3| 91|100|    -36    -19.2| -4.1|  5.7| 96| 16|     -7    -16.5| -4.7|  2.2| 96|  6|     -2  
  q 2.0%    -19.1| -1.0|  9.3| 94| 41|    -19    -19.1| -1.0|  9.3| 94| 41|    -19    -19.1| -1.0|  9.3| 94| 41|    -19    -28.6|-13.4|  5.6| 97|  7|     -5    -35.4|-23.5|  2.3| 97|  3|     -2  
  q 1.0%    -25.4| -7.1|  9.2| 95| 21|    -13    -25.4| -7.1|  9.2| 95| 21|    -13    -25.4| -7.1|  9.2| 95| 21|    -13    -41.5|-26.3|  5.6| 98|  4|     -4    -58.6|-46.6|  2.3| 97|  1|     -2  
  q 0.5%    -32.9|-14.4|  9.2| 96| 10|     -8    -32.9|-14.4|  9.2| 96| 10|     -8    -32.9|-14.4|  9.2| 96| 10|     -8    -59.1|-43.9|  5.5| 98|  2|     -3    -96.3|-84.4|  2.3| 97|  1|     -2  
  q 0.2%    -43.7|-25.0|  9.1| 97|  5|     -6    -43.7|-25.0|  9.1| 97|  5|     -6    -43.7|-25.0|  9.1| 97|  5|     -6    -82.4|-67.4|  5.4| 97|  1|     -2   -145.5|-133.8|  2.2| 96|  0|     -1  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -41.3|  7.9| 44.0| 91|100|   -100    -17.3|  6.4| 15.3| 94| 57|    -24    -13.8|  6.0| 10.8| 95| 39|    -13    -11.6|  3.0|  5.2| 96| 17|     -5    -21.2| -9.9|  1.7| 96|  7|     -4  
  q 2.0%    -37.6| 11.2| 41.9| 94| 41|    -37    -13.9|  9.7| 14.6| 96| 23|     -8    -11.3|  8.2| 10.2| 96| 16|     -4    -13.4|  0.8|  4.7| 97|  8|     -2    -32.6|-21.6|  1.4| 97|  4|     -3  
  q 1.0%    -32.3| 15.3| 39.7| 96| 20|    -16    -10.4| 12.7| 13.8| 97| 12|     -3     -9.8|  9.1|  9.4| 97|  9|     -2    -19.2| -5.6|  4.0| 98|  4|     -2    -44.7|-33.9|  1.1| 97|  2|     -3  
  q 0.5%    -28.8| 17.6| 37.7| 97| 10|     -7    -10.1| 12.1| 12.8| 98|  6|     -2    -12.7|  5.4|  8.5| 98|  5|     -1    -28.7|-15.6|  3.4| 98|  3|     -2    -62.1|-51.5|  0.9| 97|  2|     -2  
  q 0.2%    -24.6| 21.3| 36.7| 98|  5|     -3     -9.0| 12.5| 11.8| 99|  3|     -1    -12.7|  4.8|  7.7| 99|  2|     -1    -34.7|-22.1|  2.8| 99|  1|     -1    -65.8|-55.3|  0.7| 98|  1|     -2  
-- hold=T+3  amt=5w  rows=45390  periods/yr=81
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -10.6|  5.6|  7.7| 92|100|     -9    -10.6|  5.6|  7.7| 92|100|     -9    -10.6|  5.6|  7.7| 92|100|     -9    -12.8|  1.9|  5.5| 94| 53|     -5    -12.6| -0.0|  3.0| 96| 23|     -2  
  q 2.0%    -15.7|  1.0|  7.5| 95| 41|     -5    -15.7|  1.0|  7.5| 95| 41|     -5    -15.7|  1.0|  7.5| 95| 41|     -5    -18.3| -3.3|  5.4| 97| 22|     -3    -18.7| -6.0|  3.0| 98| 10|     -1  
  q 1.0%    -24.2| -7.3|  7.5| 97| 20|     -4    -24.2| -7.3|  7.5| 97| 20|     -4    -24.2| -7.3|  7.5| 97| 20|     -4    -27.5|-12.5|  5.3| 98| 11|     -2    -25.8|-13.0|  3.0| 98|  5|     -1  
  q 0.5%    -35.3|-18.3|  7.4| 98| 10|     -3    -35.3|-18.3|  7.4| 98| 10|     -3    -35.3|-18.3|  7.4| 98| 10|     -3    -40.6|-25.5|  5.3| 99|  6|     -2    -43.2|-30.4|  3.0| 99|  3|     -1  
  q 0.2%    -44.5|-27.4|  7.3| 99|  5|     -2    -44.5|-27.4|  7.3| 99|  5|     -2    -44.5|-27.4|  7.3| 99|  5|     -2    -56.8|-41.7|  5.2| 99|  3|     -1    -68.1|-55.3|  2.9| 99|  1|     -1  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -11.7| 11.3| 14.9| 93|100|     -9     -7.7| 11.4| 10.4| 93| 92|     -6     -5.6| 11.9|  8.7| 94| 82|     -4     -0.3| 14.4|  5.5| 95| 51|     -0     +9.5| 21.9|  2.8| 97| 22|     +2  
  q 2.0%     -5.1| 18.7| 14.8| 96| 41|     -2     -0.5| 18.9| 10.1| 96| 37|     -0     +1.8| 19.5|  8.4| 97| 33|     +0    +10.0| 24.7|  5.2| 97| 21|     +2    +22.4| 34.8|  2.6| 98| 10|     +2  
  q 1.0%     +6.4| 30.6| 14.7| 98| 20|     +1    +11.0| 30.4|  9.8| 98| 18|     +2    +14.2| 31.8|  8.0| 98| 16|     +2    +26.5| 41.1|  4.8| 99| 11|     +2    +36.8| 49.0|  2.3| 99|  5|     +2  
  q 0.5%    +20.4| 44.5| 14.5| 99| 10|     +2    +24.3| 43.5|  9.4| 99|  9|     +2    +28.2| 45.6|  7.6| 99|  8|     +2    +41.1| 55.4|  4.4| 99|  6|     +2    +49.6| 61.5|  2.0| 99|  3|     +1  
  q 0.2%    +31.7| 55.7| 14.3| 99|  5|     +1    +35.2| 54.1|  9.0| 99|  5|     +1    +38.3| 55.4|  7.2| 99|  4|     +1    +47.8| 61.8|  4.0|100|  3|     +1    +51.6| 63.2|  1.7|100|  2|     +1  
-- hold=T+3  amt=30w  rows=45390  periods/yr=81
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -12.2|  5.6|  9.3| 92|100|    -10    -12.2|  5.6|  9.3| 92|100|    -10    -12.2|  5.6|  9.3| 92|100|    -10    -17.7| -2.5|  5.7| 97| 16|     -2    -15.8| -3.9|  2.2| 97|  6|     -1  
  q 2.0%    -17.3|  1.0|  9.3| 95| 41|     -6    -17.3|  1.0|  9.3| 95| 41|     -6    -17.3|  1.0|  9.3| 95| 41|     -6    -26.4|-11.0|  5.6| 98|  7|     -1    -25.4|-13.4|  2.2| 99|  3|     -1  
  q 1.0%    -25.9| -7.3|  9.2| 97| 20|     -4    -25.9| -7.3|  9.2| 97| 20|     -4    -25.9| -7.3|  9.2| 97| 20|     -4    -38.6|-23.3|  5.5| 99|  4|     -1    -36.4|-24.3|  2.3| 99|  1|     -0  
  q 0.5%    -37.1|-18.3|  9.2| 98| 10|     -3    -37.1|-18.3|  9.2| 98| 10|     -3    -37.1|-18.3|  9.2| 98| 10|     -3    -57.4|-42.1|  5.4| 99|  2|     -1    -67.5|-55.4|  2.2| 99|  1|     -0  
  q 0.2%    -46.3|-27.4|  9.1| 99|  5|     -2    -46.3|-27.4|  9.1| 99|  5|     -2    -46.3|-27.4|  9.1| 99|  5|     -2    -78.2|-63.0|  5.3| 99|  1|     -1   -114.3|-102.3|  2.1|100|  0|     -0  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -39.0| 11.3| 44.2| 93|100|    -31     -8.8| 15.2| 15.3| 95| 56|     -4     -0.3| 19.7| 10.8| 96| 38|     -0    +13.8| 28.6|  5.2| 97| 16|     +2    +29.8| 41.2|  1.7| 97|  7|     +2  
  q 2.0%    -31.3| 18.7| 42.1| 96| 41|    -10     +2.4| 26.3| 14.6| 97| 23|     +0    +12.3| 32.0| 10.2| 98| 16|     +2    +27.9| 42.3|  4.6| 99|  8|     +2    +40.9| 52.1|  1.4| 99|  4|     +1  
  q 1.0%    -18.2| 30.6| 39.8| 98| 20|     -3    +16.7| 40.1| 13.7| 98| 12|     +2    +28.1| 47.2|  9.4| 99|  9|     +2    +46.1| 59.9|  3.9| 99|  4|     +2    +54.6| 65.6|  1.1| 99|  2|     +1  
  q 0.5%     -2.6| 44.5| 37.8| 99| 10|     -0    +30.5| 53.0| 12.7| 99|  6|     +2    +44.6| 62.9|  8.5| 99|  5|     +2    +57.7| 70.9|  3.3| 99|  3|     +1    +55.6| 66.4|  0.8| 99|  2|     +1  
  q 0.2%     +9.4| 55.7| 36.7| 99|  5|     +0    +38.3| 60.0| 11.8|100|  3|     +1    +48.5| 66.1|  7.7|100|  2|     +1    +53.8| 66.6|  2.8|100|  1|     +1    +45.3| 55.9|  0.7|100|  1|     +0  
-- hold=T+5  amt=5w  rows=44880  periods/yr=48
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -6.6|  9.7|  7.7| 93|100|     -3     -6.6|  9.7|  7.7| 93|100|     -3     -6.6|  9.7|  7.7| 93|100|     -3     -8.8|  6.0|  5.5| 95| 53|     -2     -9.7|  2.9|  3.0| 97| 22|     -1  
  q 2.0%     -8.8|  8.0|  7.6| 96| 41|     -2     -8.8|  8.0|  7.6| 96| 41|     -2     -8.8|  8.0|  7.6| 96| 41|     -2    -11.9|  3.1|  5.4| 97| 22|     -1    -14.9| -2.2|  3.0| 98| 10|     -1  
  q 1.0%    -16.9|  0.2|  7.5| 97| 20|     -2    -16.9|  0.2|  7.5| 97| 20|     -2    -16.9|  0.2|  7.5| 97| 20|     -2    -22.4| -7.3|  5.4| 98| 11|     -1    -23.3|-10.4|  3.0| 99|  5|     -1  
  q 0.5%    -25.7| -8.6|  7.4| 99| 10|     -1    -25.7| -8.6|  7.4| 99| 10|     -1    -25.7| -8.6|  7.4| 99| 10|     -1    -33.2|-18.1|  5.3| 99|  6|     -1    -36.1|-23.2|  3.0| 99|  3|     -0  
  q 0.2%    -29.5|-12.3|  7.3| 99|  5|     -1    -29.5|-12.3|  7.3| 99|  5|     -1    -29.5|-12.3|  7.3| 99|  5|     -1    -43.6|-28.4|  5.2|100|  3|     -1    -52.9|-40.1|  2.9|100|  1|     -0  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -13.2| 10.1| 15.0| 93|100|     -6     -9.4|  9.8| 10.4| 94| 92|     -4     -6.7| 10.9|  8.7| 94| 81|     -3     +1.3| 16.1|  5.5| 96| 51|     +0    +14.6| 27.1|  2.8| 97| 22|     +2  
  q 2.0%     -6.5| 17.5| 14.9| 96| 40|     -1     -2.2| 17.2| 10.2| 97| 37|     -0     +1.4| 19.2|  8.4| 97| 33|     +0    +12.9| 27.7|  5.2| 98| 21|     +1    +30.5| 42.9|  2.6| 98| 10|     +1  
  q 1.0%     +8.2| 32.5| 14.8| 98| 20|     +1    +13.5| 32.9|  9.8| 98| 18|     +1    +19.2| 36.9|  8.0| 98| 16|     +2    +36.2| 50.8|  4.8| 99| 11|     +2    +57.4| 69.6|  2.3| 99|  5|     +2  
  q 0.5%    +32.1| 56.3| 14.5| 99| 10|     +2    +37.6| 56.9|  9.5| 99|  9|     +2    +44.5| 61.9|  7.6| 99|  8|     +2    +62.9| 77.2|  4.4| 99|  6|     +2    +86.6| 98.5|  2.0| 99|  3|     +1  
  q 0.2%    +57.7| 81.9| 14.4| 99|  5|     +1    +64.8| 83.8|  9.1| 99|  5|     +1    +72.8| 89.9|  7.2| 99|  4|     +1    +93.4|107.4|  4.1|100|  3|     +1   +120.1|131.8|  1.7|100|  2|     +1  
-- hold=T+5  amt=30w  rows=44880  periods/yr=48
  LONG   每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%     -8.2|  9.7|  9.3| 93|100|     -4     -8.2|  9.7|  9.3| 93|100|     -4     -8.2|  9.7|  9.3| 93|100|     -4    -11.0|  4.3|  5.6| 98| 15|     -1    -11.3|  0.6|  2.2| 98|  6|     -0  
  q 2.0%    -10.5|  8.0|  9.3| 96| 41|     -2    -10.5|  8.0|  9.3| 96| 41|     -2    -10.5|  8.0|  9.3| 96| 41|     -2    -17.6| -2.3|  5.6| 99|  7|     -1    -19.2| -7.1|  2.2| 99|  3|     -0  
  q 1.0%    -18.6|  0.2|  9.2| 97| 20|     -2    -18.6|  0.2|  9.2| 97| 20|     -2    -18.6|  0.2|  9.2| 97| 20|     -2    -29.0|-13.7|  5.5| 99|  4|     -0    -27.9|-15.8|  2.3| 99|  1|     -0  
  q 0.5%    -27.5| -8.6|  9.2| 99| 10|     -1    -27.5| -8.6|  9.2| 99| 10|     -1    -27.5| -8.6|  9.2| 99| 10|     -1    -49.6|-34.3|  5.4| 99|  2|     -0    -60.0|-47.8|  2.2| 99|  1|     -0  
  q 0.2%    -31.3|-12.3|  9.1| 99|  5|     -1    -31.3|-12.3|  9.1| 99|  5|     -1    -31.3|-12.3|  9.1| 99|  5|     -1    -60.0|-44.8|  5.3|100|  1|     -0   -100.3|-88.2|  2.1|100|  0|     -0  
  SHORT  每格: 净 bp/期 | 毛 bp | 新进者冲击 bp | 换手% | util% | 年化%×util   (行 = 腿宽 q, 列 = 冲击阈 c)
          c<=inf                               c<=30bp                              c<=20bp                              c<=10bp                              c<=5bp                               
  q 5.0%    -40.7| 10.1| 44.4| 93|100|    -20     -8.5| 15.6| 15.3| 95| 56|     -2     +2.3| 22.4| 10.8| 96| 38|     +0    +24.1| 38.9|  5.2| 98| 16|     +2    +48.5| 59.9|  1.7| 98|  7|     +2  
  q 2.0%    -32.9| 17.5| 42.3| 96| 40|     -6     +4.3| 28.2| 14.6| 97| 23|     +0    +18.7| 38.4| 10.2| 98| 16|     +1    +41.6| 56.0|  4.6| 99|  7|     +1    +69.4| 80.6|  1.4| 99|  4|     +1  
  q 1.0%    -16.5| 32.5| 40.1| 98| 20|     -2    +26.2| 49.6| 13.8| 98| 12|     +2    +42.5| 61.6|  9.4| 99|  9|     +2    +73.7| 87.5|  3.9| 99|  4|     +2   +106.0|117.0|  1.1| 99|  2|     +1  
  q 0.5%     +9.0| 56.3| 37.9| 99| 10|     +0    +52.3| 74.9| 12.8| 99|  6|     +2    +71.0| 89.4|  8.5| 99|  5|     +2   +104.0|117.2|  3.3| 99|  3|     +1   +127.5|138.3|  0.8|100|  2|     +1  
  q 0.2%    +35.1| 81.9| 37.1| 99|  5|     +1    +81.1|102.9| 11.9| 99|  3|     +1   +101.4|119.1|  7.7|100|  2|     +1   +133.0|145.8|  2.8|100|  1|     +1   +153.5|164.2|  0.7|100|  1|     +1

---

**Cursor**

结论先说：在 `mom_30m` 上，30~60m 的 5w 净收益做不正，而且失败的原因是结构性的，不是阈值没调好。

**1. 换手 ≈ 100%，这是最关键的一行数据**

h ≥ 30m 时，所有 q、所有 c 下换手都是 87–99%。尾部没有任何持续性：每个成员都是新进者，每期都付完整一次往返。于是每腿成本下限 = `sell_impact` 10bp + 建仓冲击 (多 7.5 / 空 15bp)，即 17–25bp/期，与 q、c 几乎无关。要正，毛必须 > 这个数。

**2. 凸尾部有，但不够**

60m 多腿毛：5%→0.2% 是 4.9 → 7.0 → 8.5 → 11.4 → 12.7bp，×2.6；空腿 2.6 → 16.7，×6。顶到 13–17bp，刚好摸到成本下限，而此时 util 只剩 2–5%。年化×util 一列全程在 −3% ~ +3% 之间，等于零。

**3. 流动性过滤的结果和直觉相反**

- 多腿：5w 的买入冲击几乎全 ≤ 20bp（均值 7.8），c≤30/20 列完全不动；c≤10bp 把冲击从 7.8 压到 5.7，但毛从 4.0 掉到 2.1（流动性好的票反转幅度小），净反而更差 (−12.1 → −12.6)，util 还砍半。
- 空腿：卖出冲击是多腿两倍 (16bp)，c≤5bp 省下 13bp > 毛的损失，才有改善；30w 档更明显 (48bp → 2bp)。
- 所以流动性过滤只在"冲击本身大于毛"时值得做，它治的是大资金，不是 5w。

**4. 顺带两个观察**

- 多腿尾部到 T+1 变成大幅负毛 (q 0.2%: −25bp, T+3: −27bp)：30m 动量的极端顶在次日反转。这个因子的"凸"是日内的、随后被吐回去。
- 空腿 T+3 / T+5 毛 55–130bp，净全正——但 CS 的 0.2% 尾部每行不到 1 只票，极端动量多半是触板票，sv 标签能算、实际做不了空；不要当信号。

**对思路的判断**

"因子排序 + 尾部收紧 + 流动性过滤"这条路在 5w 上卡在换手，不是卡在信号：既然 30–60m 内尾部成员全换，那每笔就必须独立覆盖一次往返，而 10bp 固定平仓占了这次往返的一半以上。把 60m 多腿最好那格 (毛 12.7, 冲击 7.5) 的平仓成本设成 0，它就是 +5bp——整个胜负在出场方式（被动挂单 / 用真实 `cost_sell`），不在入场过滤。

还差一步再下最终结论：这次跑的是 CS 因子。你再在 Inspect 里点一下 `ts_vwap_dev_60m`（之前算过它 G20−G19 是内部斜率的 10 倍，尾部比 `mom_30m` 凸得多），看 60m LONG 表 q 0.5% / 0.2% 那两行的毛能不能过 25bp。过不了，探针就可以删了；过了，再谈进契约。

