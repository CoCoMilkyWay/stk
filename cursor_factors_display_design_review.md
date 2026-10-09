# Factors display design review
_Exported on 10/9/2026 at 18:59:56 GMT+8 from Cursor (3.21.18)_

---

**User**

@README 
@cursor_factor_display_terminal_design.md 
GUI->FACTORS->INSPECT下面, 你觉得应该怎么设计

---

**Cursor**

先说数据层面的现实约束, 再说画什么.

**现状能给 Inspect 的只有 `HoldStat`**: `evaluate()` 里 `srows` (一级 `Row[H][T]`) 每个根算完 `summarize` 后就被下一个根覆盖, `Loaded` (特征 + 标签平面) 和根平面 x 也随函数结束释放. 所以 Inspect 想画任何沿时间的东西 (累计曲线 / IC 序列 / 日内轮廓 / 分块稳健性), 必须有人把一级 Row 留下来. 两条路:

- FactorsService 对每个因子都留 Row: 一年 ≈ 61710 行 × 12 组合 × ~140B ≈ 100MB/因子, 几十个因子就爆, 不行.
- Inspect 自己一个 `InspectService` (StreamService 骨架, 对仗 Dist/Transform), 只对 `view_file` 这一个因子: 装载它用到的特征 + 全部标签 → 单棵 Dag → eval → Stat, **保留 `Row[H][T]` + 根平面 x[T][A]** (smallcap400 一年 ≈ 120MB, 可接受). 装载是 IO 大头, 服务内缓存标签平面 + 已读特征 (按 scope 键), 换因子只补缺的特征. 这需要把 FactorsService.cpp 匿名空间里的 `Loaded / load_planes / fill_stat` 抬到共享头, 两个服务复用, 不另写一份.

我推荐第二条. 下面按"数据来源分级"给页面设计, 两口径画法完全一样 (标签已统一超额, 20 组都是自身/截面分位组), 只有 TS 多一张"组占用"图.

**层 0 — 只用文件里的 HoldStat, 进页即显示, 不用算**

- 顶栏: 文件 / `[CS|TS]` 规范串 / note / stat 作用域 (与当前 config 不符黄字, 复用 TabFactors 的 `scope_matches`); beta 类直接提示未实现.
- 分层柱: 当前 amt×hold 的 `grp[0..19]` 池化超额, 标 mono / ls_mean / ls_t. 这是主图, 对应"把 map 撑开撑直".
- 分层 × 持有期热图: 行 = hold, 列 = 组, 色 = grp[k]. 一眼看出分层形状随持有期是加强、衰减还是翻号.
- 衰减曲线: ic_mean / icir / ls_mean / sharpe / rank_ac 对 hold 的折线 (多条 amt 叠), 选持有期靠它.
- 全部 amt×hold 的标量小表 (现在只在 status 悬停里, 搬出来).

**层 1 — Inspect 自算后的 Row[H][T] (Compute/Cancel 状态行复用 Common.hpp)**

- 分组累计超额: 20 条线 (按组号 colormap) + LS 粗线. 持有期重叠, 累计要按 h 步长取非重叠子序列 (h 个相位取平均), 否则斜率虚高 h 倍; 横轴用交易日.
- IC: 日均 IC 柱 + 累计 IC 线; 旁边 IC 直方图 (ic_skew/kurt 的可视化).
- 日内轮廓: IC / ls / ac 按 `t % kSegLen` 跨天平均. 分钟级因子这张最关键 —— 开盘半小时有效、午后无效这种事只有它能看出来; 段末 tail_masked 区自然空白.
- 分块稳健性: 按月 (或周/年可选) 切 Row 分别 `summarize` (它本来就吃任意连续切片, 零新代码), 表列 = 块, 行 = ic_t / ls_mean / mono / ls_pos, 着色. 直接回答前面讨论的"regime 聚集"担忧: 收益是不是两三个月撑起来的.
- 换手: rank_ac 的日序列 (成本侧).
- TS 专属: `cnt[k]` 随时间的堆叠图 (组占用). TS 组允许空, 全市场同时挤在 top 组 = 共同分量在动, 这正是 TS 口径扔掉的东西, 要让人看见.

**层 2 — 根平面 x (可后做)**

- 因子值 PDF (复用 Dist 的 KLL 画法) 和 x̄_t 时序 (截面均值, 即被两口径都扔掉的市场择时分量, 单独画出来供以后 beta 类/指数择时用).
- 点资产看单票: x 与标签的散点 / 单票时序.

**布局与交互**

- 顶: 头信息 + 控件 (amt / hold 下拉, Inspect 自己的, 初值取 Factors 页; 分块粒度; Compute/Cancel + 进度).
- 主体 2×3 网格: 左列 分层柱 → 分组累计 → 分块表; 右列 热图/衰减 → IC → 日内轮廓. 层 1 未算时右下三张显示"Compute 后可见", 层 0 永远有.
- hover 分层柱的第 k 组 → 累计图里该组线高亮 (Common.hpp 高亮线); 换 hold 全页联动.
- 换 `view_file` 时若服务空闲自动起算 (缓存命中就很快), 忙则排队覆盖 (StreamService 的 Request 语义).

要确认的一点: 走 `InspectService` 独立装载 (需要把 `Loaded/load_planes/fill_stat` 从 FactorsService.cpp 抬到共享头), 还是你有别的想法保留 Row? 定了我再动手, 先做层 0 + 层 1.

---

**User**

没问题, 就第二条路
一堆stat就简单放到上面,
然后4张图, 左边是分层累计IC(CS/TS)(有个按钮可以选择显示超额累计收益分成还是绝对累计收益分层), 里面还有一个多空IC(图上标注夏普)
右边是期限结构, 1m, 5m,15,30,60,close,t+1,t+2,t+5的超额收益图, 方便看这个因子是预测哪个频段的
左下角是IC(整个universe)时序分布
右下角保留着, 给后面的markoviz CDF仓位映射图用
先实现前3个

---

**Cursor**

Now I have enough context. Starting with the shared load layer.

Now the `.cpp`: move the definitions out of `FactorsService.cpp`.

Now strip the moved pieces from `FactorsService.cpp`.

Now remove the `Loaded`/`load_planes`/`check_leaf_domains`/`valid_pct_of` block (keeping `fill_stat`).

Now update `evaluate()` to use `init_loaded` and `assert_cs_leaves_covered`.

Now the `InspectService` header.

Now the Inspect tab header.

Now the tab implementation.

Fix the stray `(void)fui;` and wire up `TaskFactors.cpp`.

Now CMake (both lists).

改完, 请编译 (CMake 新增了 `FactorsLoad.cpp` / `InspectService.cpp`, 需重新 configure).

改动:
- `FactorsLoad.hpp/.cpp` (新): 从 `FactorsService.cpp` 抬出来的装载层 `Loaded / init_loaded / load_planes(with_labels) / check_leaf_domains / assert_cs_leaves_covered / valid_pct_of`, 两个服务共用; `FactorsService::evaluate` 原地改为调它们, 逻辑不变.
- `InspectService.hpp/.cpp` (新): 单因子 worker, CPU 后端, 保留一级 `Row[H][T]` + 全部 `HoldStat`. 缓存: 同作用域下标签平面 + 标签 rank 常驻, 特征只补当前因子缺的、不用的释放; `reload` / 换作用域整体重读.
- `TabInspect`: 顶部 头信息 + Amt/Hold + `超额|绝对` 切换 + Compute/Cancel/状态 + 作用域行 + 选定持有期的一行 stat; 2×2 图:
  - 左上 分层累计: 20 组 (Jet 色) 沿交易日累计组均收益, 按 1/h 折算重叠持有期; LS 白粗线, 图例标 SR / t; 末端标 G20/G1; 悬停看某日各组值. 绝对模式 = 组均 + mkt (`Row.grp` 存的是超额和, 可还原), LS 两模式同一条 (本就市场中性).
  - 右上 期限结构: 字段表里全部持有期 (现在是 1m/5m/15m/30m/60m/close/T+1/T+3/T+5, 没有 t+2) 的 Top 组 / Bottom 组 / LS 超额均值分组柱, 选定持有期打 tag, 悬停看该档全部 stat. 只用 HoldStat: Inspect 没算过时直接用 Factors 表 (文件 stat), 进页即有.
  - 左下 IC 时序: 日均 rank IC 柱 + 累计 IC (Y2), 标题带 mean / IR / t / pos.
  - 右下 留位.
- 触发: 高光因子 (文件 / 规范串 / universe / 区间) 变且服务空闲 → 自动起算 (取消过的不反复起); Compute = 弃缓存重读. `TaskFactors.cpp` 持有 `InspectService`, 左栏 Inspect 行状态改看它; Destroy 时释放.

已过 clang-format 与 `-fsyntax-only`.

---

**User**

我点factors下的run, done之后, IC 那些指标都没有数据, valid 0%, 只有运行时间是对的
然后去inspect看也都是0

---

**Cursor**

不是 Inspect 的问题, 是 `cs_valid` 列全 0, 而且在我改之前就是了 (18:00 那次提交的因子文件已经 `valid_pct: 0.0`; 动态池提交之前是 70.58%).

原因: `sequential_worker.cpp` 给 `CoreSequential` 传的是 `asset.asset_code` = `"000001"` (裸 6 位), 而 `universe::Pool` 的键是 `"000001.SZ"` → `in_pool()` 对所有资产都返回 false → 每个资产每天 `cs_valid = 0` → CS 根掩码被门控清空, TS 根被 `gate_mask` 清空, 标签也按 cs_valid 全掩掉 → valid 0%, Stat 行全不 ok, 只剩算子耗时是真的.

改动:
- `sequential_worker.cpp`: 传 `asset_code + "." + exchange`
- `CoreSequential.hpp`: 形参/成员改名 `code_ex`, 注释写明是 Pool/AssetAxis 的键
- `Universe.cpp` `in_pool`: 断言键含 `'.'`, 以后再传裸码直接炸

需要: 重新编译 → Features 页 **重算特征** (cs_valid 是落盘列, 旧库里就是 0; 字段表没变, reader 不会自动判废, 必须手动重算) → 再 Factors Run / Inspect.

---

**User**

你觉得对于分层来说, 是分层累计收益好, 分层(每层单独统计)累计IC好

---

**Cursor**

**分层累计收益为主, "每层单独算 IC" 不建议; 真想要 IC 版分层, 应该是"每层的标签均秩"而不是层内 IC.**

为什么层内 IC 没意义:
- 一层只覆盖因子分布的 1/20, 层内 x 的变异极小, 层内 Spearman 衡量的是"在这一窄条里 x 再高一点是否再好一点" —— 局部单调性, 噪声大、系统性接近 0, 且没人按它交易. 20 条线都在 0 附近抖, 看不出东西.
- 分层图的任务是看 map: 状态 → 条件期望超额, 是否单调、信号在尾部还是全身、多空两侧是否对称. 这是 E[y | k] 的事, 不是 corr 的事.

分层累计收益的优点正好对上这个任务: 有量纲 (能对成本), 可交易 (就是 Top 组仓位的权益), 多空不对称一眼可见, 单调性 = 线的排序. 缺点是分钟收益重尾, 几根大 K 线能把一层的累计线顶起来, 看起来像有信号.

补这个缺点的正确办法, 才是"IC 版分层": 把每层的 **标签均秩** 画出来 —— E[rank(y) | k] − 0.5. 它就是 IC 按层的分解 (IC = Σ_k (r̄ₓ,ₖ − ½)·(E[r_y|k] − ½) 的那个第二项), 无量纲、对离群点免疫、跨持有期可比, 20 条累计线如果也是单调排开, 说明收益版的形状不是几个大值撑的. 两张图一致 → 可信; 收益版好看而均秩版平 → 尾部几次事件撑的, 警惕.

所以建议: 左上保持 累计收益 (超额|绝对), 再加第三档 "均秩" 作为稳健性对照; IC 本身的累计仍放左下 (它是全分布的时间稳定性, 一条线够了). 代价是 `Row` 要多存 20 个 Σ r16_y (按组), CPU/GPU 两后端 + 对拍一起加一行, 不大.

---

**User**

你看一下因子的valid率, 为什么还是95多, 不是100%

---

**Cursor**

`valid_pct` 不是"在池率"，是"因子平面在池内的有效率"。它的定义在 `FactorsLoad.cpp`：

```cpp
215:float valid_pct_of(const uint8_t *m, const uint8_t *gate, size_t n) {
  size_t c = 0, g = 0;
  for (size_t i = 0; i < n; ++i) {
    c += m[i] & gate[i];
    g += gate[i];
  }
  return 100.f * float(c) / float(g);
}
```

- `gate` = `cs_valid`（当日是否在 universe 池里，整日常量）
- `m` = 因子根平面的有效掩码，装载时按 `FactorsLoad.cpp:128` 这一行定：

```cpp
128:        const bool ok = fmeta::valid(static_cast<float>(ts_gate[a]), vts[i]) && std::isfinite(x);
```

也就是 `m[a]=1` 要求**两条同时成立**：
1. 该资产该分钟的 `ts_valid` 通过（按列的 `valid_type`：`ALL`/`DEPTH`/`data_valid`）—— 即这一分钟有事件/盘口更新；
2. 特征值 `x` 有限（非 NaN/Inf）。

上次修的 bug 是 `cs_valid` 全 0（裸 code 没拼 `.EX`，`Pool::in_pool` 全 false），所以分母 `g=0`、`valid_pct=0%`。现在分母恢复正常，但分子仍会少于分母，因为：

- **`ts_valid` 是逐分钟逐资产的活动门**，和 `cs_valid`（逐日池籍）相互独立。一只票当天在池里（`cs_valid=1`），但某些分钟可能停牌、集合竞价无成交、盘口无更新 → `ts_valid=0` → `m=0`，`gate=1`，这些格不进分子只进分母。
- 特征算子本身在个别格产生 NaN/Inf（除零、log 负数等）也会让 `m=0`。

所以 95% 多是**预期行为**，不是 bug：大约 5% 的 (资产, 分钟) 格是"在池但该分钟无活动 / 特征非有限"。`MetaFlag.hpp:10` 明确写了 `ts_valid` 与 `cs_valid` 分工、不相与，因子掩码同时卡两者是设计使然。

要不要我帮你加一个区分诊断（比如在 Inspect 里分别显示"池籍率 Σgate/n"和"ts 有效率 Σm/Σgate"），确认这 5% 确实是 ts_valid 而不是特征 NaN？

---

**User**

TS算子应不应该把原本稀疏的特征变成稠密的? 其实95%说明大部分标的已经比较紧实了, 偶尔一点稀疏点

---

**Cursor**

先说现状：TS 算子**已经**在做"稀疏→稠密"，但只对一类做。`TS/Cpu.hpp` 里分两种：

- **归约型**（Sum/Mean/Var/Std/Max/Min/Corr/Ema…）：输出有效只要求窗内 `n ≥ 1`，**不要求 `x_t` 本身有效**。`RollSum` 第 796 行 `ok = full && n[a] >= 1`，`TsMeanEma` 更直接："x 无效时 y 与掩码都不动"——就是前值保持。这些算子天然把稀疏输入变稠密。
- **相对型**（TsRank/TsZ/Pct 这类"把当前点放到自己历史里比"的）：显式写了 `ok = ok && m; // 相对型: x_t 无效则无效`（567、849 行）。Delay/Delta/MAP 同理，没有 `x_t` 就没有东西可比/可变换。

所以 95% 的那 5% 洞不是归约层漏的，是**根节点或叶节点是相对型/点型**：你那批因子里 `ts_*` 根是 `TsRankRoll`，CS 的根是 `CsRank(点特征)`，这一分钟 `ts_valid=0` 就输出无效，归约层再稠密也救不了根。

我的看法：现在这个分界是对的，不该改隐式语义。

- 归约型稠密化合理：一分钟没事件不是"数据缺失"，是"什么都没发生"，窗内均值/方差照旧良定义。
- 相对型不该隐式补：要补只能 ffill，而 ffill 出来的 `x_t` 是陈值，拿陈值在新历史里排名/标准化，结果系统性偏向"上一次有事件时的位置"，而且这类无活动分钟恰好与流动性差相关，和标签有相关的缺失，补了反而引入偏差。还有对拍问题：流式后端要维护"最近有效值+陈旧度"状态，三后端都改，契约里"退化置无效"这条也要开口子。
- 5% 在 Stat 里的代价很小：CS 口径这一分钟只是截面少几只票；TS 口径只是池少几个 (t,a) 样本。

如果真想要 100%，正确的做法是加一个**显式**算子让表达式作者自己选，比如 `TsFillRoll(x, d)`：最近 d 分钟内有有效值就携带，否则无效（d 限制陈旧度，避免停牌一整天还往外吐旧值）。它是 SHIFT 核、SELF 域、ROLL 窗，三后端实现都很简单（CPU 一遍扫、GPU 块内 carry 同 Ema 那套、流式一个状态）。写在因子里就是 `TsRankRoll(TsFillRoll(x, 5), 1275)`，意图可见，而不是算子库偷偷替你补。

要不要我加这个 `TsFillRoll`？目前库里没有任何 fill/carry 类算子，是新增一行 OpTable + 三后端 + op_check 对拍。

---

**User**

现在这几个因子都不是100%, 都能用相对型解释嘛? 你粗看一下

---

**Cursor**

粗看结论：**都能解释，而且基本是同一个数在起作用**。

先把每个因子的"第一个要求 `x_t` 本身有效的节点"找出来（归约型上面没有再套归约的话，它就是瓶颈）：

| 因子 | valid% | 瓶颈节点 | 预期 |
|---|---|---|---|
| taker_imb_10m | 99.34 | `TsMeanRoll(10)` 窗内 ≥1 即可 → 几乎稠密 | 只剩连续 10 分钟无事件（停牌日） |
| open_imb_cum | 94.12 | `TsTodMask(15,45)` 每天前 15 分钟结构性无效 | 1 − 15/255 = **94.12** ✓ 精确 |
| ts_amt_burst_10m | 94.16 | `TsRankRoll(1275)` 前 5 天暖机 (5/125 = 4%) + 竞价段 Σamt=0 → Log 无效 | ≈ 94 ✓ |
| ts_taker_imb_30m | 94.52 | 暖机 4% + 停牌残余 | ≈ 94.5 ✓ |
| pv_corr_60m | 96.02 | `TsCorrRoll(60)` 把里面 Delta(1) 稠密化了 | 剩退化/停牌 ✓ |
| range_pos_60m | 91.53 | `TsSub(close, …)` MAP → 要 `close_t` 有效 | 1 − p |
| ts_vwap_dev_60m | 87.18 | `TsLogRatio(close, …)` 要 `close_t` 有效 × 暖机 | (1−p)·0.96 |
| mom_30m | 83.85 | `TsDeltaRoll(30)` 要 `x_t` **和** `x_{t−30}` 都有效 | (1−p)² |
| ts_mom_30m | 79.49 | 同上 × 暖机 | (1−p)²·0.96 |

设 p = 池内格里 `ts_valid = 0`（该分钟该票无任何事件）的比例，取 **p ≈ 8.5%**：

- range_pos: 1 − 0.085 = 91.5 ✓
- ts_vwap_dev: 0.915 × 0.96 = 87.8 ✓
- mom_30m: 0.915² = 83.7 ✓
- ts_mom_30m: 0.837 × 0.96 = 80.4 ✓

四个互相独立的因子用一个 p 全对上，所以结论是：**这个 smallcap 池里约 8.5% 的 (分钟, 票) 格没有事件**（小票流动性差 + 09:25–09:30 那几分钟空档 + 停牌日），所有 9 个因子的缺口 = 这个 p 经过各自表达式的"穿透次数"（MAP/SHIFT 穿一次 8.5%，Delta 穿两次 16%，归约型吃掉它，TodMask/暖机是另两个独立结构项）。

一个值得注意的点：`close/high/low/vwap` 在字段表里 `valid=DATA`，所以没事件的分钟被门控掉了——但落盘的 `close` 本身 nan%=0，是携带的最新价，那一分钟没成交不代表"最新价不存在"。真正的杠杆不在 TS 算子补洞，而在这几列价格快照的 `valid_type` 是否该是 `DATA`：如果 `close` 的语义是"最新成交价快照"，它在无事件分钟也有定义，门控成 `ALL`（或新增一类"携带型"）比在因子层 ffill 更干净，`mom`/`range_pos`/`vwap_dev` 一下子回到 ~96–100。`vwap`/`amt_taker` 这类"本分钟流量"则确实该保持 `DATA`。

这是特征层的语义决定，要不要改我不替你定——改了需要重算特征。

