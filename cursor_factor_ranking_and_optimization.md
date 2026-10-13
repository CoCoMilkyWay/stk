# Factor ranking and optimization
_Exported on 10/13/2026 at 18:50:56 GMT+8 from Cursor (3.21.18)_

---

**User**

@cursor_search_operator_implementation_i.md 
我试了几个感觉没问题了, 基本好因子都有强的尾部收益, 所以我们只需要 max LS, IC, Util就好, 相当于完全只关心因子的排序能力, 这样就够了? 我们反正在30min~T+1里面去找最优参数, 完全不考虑cost, 只尝试要最强的预测能力就好? 后面具体的执行在优秀因子的基础上独立处理?

---

**Cursor**

基本同意，但有一个地方不能直接"去掉成本"，是 h 的选择。

**同意的部分**

探针数据支持你的判断：h ≥ 30m 时换手 87–99%，5w 建仓冲击多腿恒 ≈ 7.5bp、空腿 ≈ 15bp，几乎不随 q / c / 因子参数变。成本实际上是个**常数**，减一个常数不改变因子参数的 argmax，所以在同一个 h 下，"毛 LS × util × rIC" 和 "净 LS × util × rIC" 选出来的因子是同一个。流动性过滤对 5w 没意义（多腿 c≤30/20 列完全不动），也可以不管。执行（被动出场、真实 `cost_sell`）确实是独立于因子的那一层。

**不能直接去掉的部分：跨 h 比较**

现在 score 是年化：`net_ls × periods_per_year(h)`。成本常数的作用恰恰是让这个年化在 h 上有意义：

- 去掉成本后用年化毛：30m 是 2057 期/年，T+1 是 242 期/年，相差 8.5 倍，而毛 LS 从 30m 到 T+1 通常只涨 1.5–2 倍 → h* 永远 = 30m，`[h_lo, h_hi]` 窗口形同虚设。
- 改成每期 bp（不年化）：LS 随 h 单调涨 → h* 永远 = T+1。

所以"在 30m~T+1 里找最优 h"这件事，本身就必须有一个每笔成本的尺度，否则两端之一恒赢。最省事的做法不是删成本，而是把它**认定为常数门槛**（比如每腿一次往返 k bp，k 对应你认为执行层能做到的水平），数据算出的买入冲击都可以不要。这其实就是现在的式子把 `buy` 换成常数。

**另一个会丢掉的东西：换手定价**

成本进目标 = 给换手定价。在 30m~T+1 里 `mom_30m` 的换手是 ~95%，所以无所谓；但 TS 根 + 长窗口的因子尾部可能有粘性（成员留仓），这类因子在净口径下会被偏好，毛口径下看不出来。你如果接受"执行层自己处理换手"，那这项丢了没问题，但要知道丢的是什么。

**结论**

- 因子参数的排序：毛口径够了，可以不看数据成本。
- h 的选择：需要一个常数门槛，不能真的 cost = 0。
- IC 的角色：rIC 不是目标，是防止 util 小时噪声 LS 胜出的正则项，保留现在的 `ric^pow` 就行。

要改的话最小改动是 `Objective` 里加一个"常数每腿成本"替代 `CostAcc` 的买入冲击，`sell_impact` 保留或并进去；要不要改、k 取多少，你定。

---

**User**

1. 没用, 你加上成本也没办法公平对比不同周期
2. 换手不需要定价, 60m以上的换手会马上到100%附近, 优化这个没什么用

---

**Cursor**

接受这两点。那么推论就只有一个：**h 不能由分数选，必须是 Search 的输入**。

既然不同周期不可公平比较，`argmax_h` 这一步本身就没有意义，不管里面放毛还是净。现在的 `[h_lo, h_hi]` 窗口应该退化为一个固定 h（`h_lo == h_hi`，现有代码已支持），每次 Search 针对一个 h 出一份排序；要看多个 h 就跑多次。固定 h 之后年化系数是常数，也不用再乘。

于是目标就只剩你说的三项：

```
score(h) = ls_mean(h) × util(h) × rIC(h)^pow      ls_mean / rIC ≤ 0 → −∞
```

对代码的改动很小：`Objective` 加一个"不扣成本"的开关（或直接删掉 net 路径），`score_pt` 用 `hs.ls_mean` 替代 `net_ls`；`CostAcc` 仍然攒，只给 Inspect 画净线用，不进分数。看板的 h*/util 列跟着变成"固定 h 下的 util"。

要不要我就按这个改 `Search/Contract.hpp`，还是你想先把 h 固定成哪一个（30m / 60m / T+1）定下来再动？

---

**User**

ok, 那你来改, 每个search都应该针对一个固定的horizon, horizon之间也不需要比较了, cost也在search过程中完全不需要考虑
这样整个search流程应该可以大幅度简化, 加速

但是新问题是一个因子表达式其实有多个不同的optimal weight, 当然这个后面再说, 你先把上面的改好

---

**Cursor**

Now the Search files and their callers.

Grid is unaffected. Now the service and the Check, GpuRun, and the UI callers.

Now the GUI side: FactorsService, FactorsLoad (label subsetting), TabFactors, TaskFactors.

Now TabFactors — the dashboard and Optimize panel.

Let me start with the Stat contract: `leg` on `HoldStat`, `util_of`, and remove `CostAcc::util`.

Now remove `CostAcc::util` and compute `leg` in `Accum::finish`.

Now StatJson and Stat/Check.

Now the Search contract — rewrite in place.

Now `Search/Cpu.hpp`.

Now `Search/Gpu.cuh`.

Search headers are clean. Now `GpuRun.hpp`, the `.cu`, the stub, and `op_check.cpp`.

Now `op_check.cpp`.

Now `SearchService.cpp`.

Now the budget section (no costs) and `load_chunk`.

Now the CPU / GPU chunk evaluators.

Now scoring in `run_pass`, the branches, refinement, and the final section.

Quick comment fixes in the header so they aren't misleading.

Now TabFactors.hpp / TabFactors.cpp / TaskFactors.cpp.

Now TabFactors.cpp: columns `h*` → `score`, OptCell, tooltips, Optimize panel, sort keys.

Now the Hold combo tip and the Optimize panel.

Now the candidates tooltip table.

Now the per-row `s_opt` computation and the sort key.

OperatorsService also runs the Search check. Let me look at it.

Good — only my files changed (the accidental full format aborted on the pipe and was a no-op). Now format just the touched files and run lints.

改完了，没有编译，请你编译跑一下。

要点：
- Search 目标 = 固定 h 的 `ls_mean · util · ric^pow`，h 取 Factors 页 `Hold` 组合框；不装其他标签组，不算成本（CostAcc 只剩 Run / Inspect 净线用）。
- `util` 搬进 Stat：`HoldStat::leg` + `util_of`；`StatJson` 多了 `"leg"` 键，**旧 stat 文件会在 `load_hold` 处断言失败，需重新 Run**。
- 看板 `h*` 列换成 `score`；Optimize 面板去掉 `Amt`。
- 已 clang-format 触动的 25 个文件，`ReadLints` 无报错。

待办（你之前搁置的）：一个表达式多个 optimal weight 的处理。

---

**User**

如果要支持一个因子多个参数怎么办
不同预测周期下的, 相关性低的

---

**Cursor**

先说结论：现有数据模型其实已经能装"一个表达式多组参数"——`dup` 判定按**含参数的规范串**，同 `expr` 不同 `params` 的两个文件不算 dup，各有各的 stat / Inspect，Run 时共享 DAG 的公共子式只算一次。真正的缺口在两处：

**1. Optimize 会互相覆盖。** 文件不记"这组 params 是为哪个 h 调的"，在 Hold=1d 下对一个已为 30m 调好的文件点 Optimize，只要 1d 分数好于当前 params 在 1d 下的基线（几乎必然），就把 30m 那组冲掉。

**2. "相关性低"没有任何地方算。** 换一组参数得到的平面和原平面是不是同一个信号，现在只能靠人看 Inspect 猜。

三种做法：

- **A. 不改代码，纯约定**：手动 Dup 文件 → `mom_30m.json` / `mom_1d.json`，各自在对应 Hold 下 Optimize。缺点就是上面两条：靠人记、容易误覆盖、没有相关性信息。

- **B. 最小改动（我推荐）**：
  - Search 落盘时在文件写 `"opt_hold": h`（stat 旁边，一个键）。
  - Optimize 时若 `req.hold == opt_hold`（或文件无 opt_hold）→ 原地覆盖（现状）；否则写**兄弟文件** `<name>_<hold_name>.json`（expr / name_cn / note 复制，params = 新的，不动原文件）。
  - Search 批里基线就是候选 0，顺手给每个候选多攒一个累加器：逐 t 的 Spearman(rank_c, rank_base) 的时间均值 → `SearchCand::corr_base`，cands tooltip 多一列，最终消息里带上 "与原参数相关 ρ=…"，由人判断值不值得留。CPU / GPU 各加一个 Σ r_c·r_base 项（秩已有），Check 对拍跟着加一列。
  - 改动面：SearchService 落盘分支 + FactorsService 写文件接口加一个目标名参数 + Search Contract/Cpu/Gpu/Check 各一个累加项 + TabFactors tooltip。不动 FactorRow / 扫描 / Inspect。

- **C. 一文件多 variant**：`"variants": [{hold, params, stat}, …]`，FactorRow 变成 (file, variant) 一行。扫描 / 回写 / StatJson / UI 行身份 / Search 回写 / Inspect 全要动，收益只是文件少几份，不值。

另外一个判断留给你：相关性阈值要不要自动化（ρ > 某值直接不另存）？我倾向只展示不自动拒绝，因为高相关但不同 h 的两组参数在执行层仍可能有用。

要做 B 的话告诉我，我再动手；或者你觉得 A 的约定就够，那只需要把 `opt_hold` 那一个键加上防误覆盖即可。

---

**User**

search中要分别 search 不同horizon的, 然后根据第一个网格search去分子tree, 只分这一级, 这里不再限制最多只有n个tree, 然后在子tree里分别去找tree里最优的, 这里每个tree只找一个最优的
得到一大堆最优tree后, 找到里面最好的作为max score, 在score衰减不超过20%的点里, 找rAC最低的<=3个权重

然后对其他horizon也做这个

所有search出来的都存在一个json下
然后页面上可以+展开一个因子, 看下面不同的权重
注意现在hover和看板统计是针对每个权重的了

---

**Cursor**

**多权重 Search 设计确认**

> **「score 衰减 ≤20% 的点里找 rAC 最低的 ≤3 个」—— rAC 是秩自相关, 越高换手越低. 你要的是哪个方向?**
> - rAC 最高 (换手最低) 的 ≤3 个 —— 推测是口误
> - 字面意思: rAC 最低的 ≤3 个
> - 不是看单个 rAC, 是选出的权重之间两两相关性低 (需要新加候选间相关统计)
>
> Answer: 我意思是不同因子截面之间的rAC, 不是单因子RAC, 这样对比的是因子之间持仓的重合程度, 我们要重合程度最低的优质因子, 注意两两算这个比较消耗算力, 要聪明算

> **score 最高的那个权重是否必选 (占 3 个名额之一)?**
> - 必选, 剩下 ≤2 个按 rAC 挑
> - 不必, 3 个全按 rAC 挑 (可能把最高分丢掉) ✔

> **「不再限制最多 n 个 tree」具体是:**
> - 沿用互不相邻 (Chebyshev≥2) 贪心, 只去掉 kBranches 上限 (= 粗网格的全部局部峰) ✔
> - 粗网格每个有限分格点都开一支 (最多 256 支, 评估量大很多)

> **一次 Optimize 跑哪些 horizon?**
> - 字段表里全部持有期, 顺序跑 (Hold 组合框不再影响 Optimize)
> - 只跑 30m ~ T+1 这一段 ✔
> - 仍只跑当前 Hold, 多 horizon 靠多次点

> **落盘: 对某 hold 再次 Optimize 时, 该文件已有的权重怎么处理?**
> - 只替换该 hold 的权重, 其他 hold 的保留 (每权重记 hold)
> - 整个 weights 列表重写 ✔

> **expr 字面参数 (基线) 的地位:**
> - expr 只是模板; weights 为空时整文件就是基线一行; 有 weights 后基线不再单独显示 / 不参与 Run
> - 基线永远作为第 0 个权重保留显示
>
> Answer: expr里的参数就用通用字符表示了, 不再用具体数值, 下面带weights的才用expr带具体数值

> **看板父行 (因子文件那一行) 显示什么统计?**
> - 当前 Hold 下 score 最高的那个权重的统计 (hover 也看它) ✔
> - 父行只显示名字 / 表达式 / 权重数, 统计全在子行

**多权重 Search 设计确认 (第二轮)**

> **模板 expr 里「通用字符」的写法 (解析器要加这一条语法):**
> - d=? / k=? 表示待搜槽; 仍写数字的参数 = 固定不搜 (推荐: 可以钉住部分参数, TS 根 d=240 不动)
> - 直接写键名 d / k, 不带 =; 数字同样表示固定
> - 模板不写任何参数 TsMeanRoll(x), 全部参数都搜 (没法钉住)
>
> Answer: B,  按照算子类型写键名就可以, 不用想办法去重

> **没有基线值, 粗网格以哪里为中心? (有界槽 d / 分位 / 时段: 整段范围都会被枚举到, 中心只影响格点位置)**
> - 有界槽取坐标范围中点; 无界槽取 0 (阈值 = 0σ, 分位 = 0.5) ✔
> - 另议

> **候选间「截面 rAC」的算法 (两后端同一份宿主代码):**
> - 细化结束后对高原集 (score ≥ 0.8·max 的各支最优) 多跑一趟, 每块抽样 ~1/N 的时刻行把候选的秩行拷回宿主, 逐行 Pearson(r_i, r_j) 沿抽样行取均值 → M×M 矩阵; 在 ≤3 的组合里取两两最大相关最小的一组
> - 同上但不抽样 (全部时刻行; GPU 侧算, CPU 侧很慢)
>
> Answer: A, 不要写太复杂, 先做一个, 这部分算法后续还要进一步讨论, 先把别的框架做好

> **文件格式:**
> - {type, name_cn, expr(模板), note, weights: [{hold, expr(具体), stat}]}; 去掉 params / 顶层 stat 键. 没有 weights 的因子 Run 不评 (看板标「未搜索」), Edit 改了模板 expr 就清空 weights ✔
> - 另议

> **看板操作归属:**
> - 父行: Optimize / Edit / Delete / Dup; 子行: hover 统计 / Inspect; 子行标 hold 来源; dup 判定按子行规范串 ✔
> - 另议

Now the Expr edits: header doc, Node free bits, to_string, parser, and removal of `apply_params`.

Now `Node` / `Expr` + a `free_value` helper, placed after `check_params`:

Now the parser: lookahead and the param branch.

Now set `n.free` on the node and remove `apply_params`.

Fix the stale comment in `root_frame` and move to Grid.

Now `Grid.hpp`: slots = free params only, center without baseline, no branch cap.

Now `pick_branches` without the cap:

Grid done. Now the FactorsService layer. Let me read it fully.

Now the header doc at the top and the file-level functions:

Now add `instance_of` to Expr.hpp (template ↔ instance check):

Now `FactorsService.cpp`. First the JSON helpers section (remove params, change write_back):

Now Add / Update / WriteFactorWeights:

Now the scan:

Now `evaluate`: select rows with concrete `expr`, and `write_back` signature.

Now the hold window + a selection helper in `Search/Contract.hpp`. Let me look at its current doc header to place them.

Also fix the stale `score_of` comment, then `FactorsLoad` `select_holds`:

Now the SearchService header rewrite of the request/result types and doc:

Now the `SearchService.cpp` run body. Start with the top-level pieces (Entry/Branch, MakeSearchRequest, worker_loop result init):

Now the `run()` head (holds, expression, candidate table, publish):

Now the correlation sampling. Let me view the current cpu_chunk/gpu_chunk/run_pass region to edit them for rank sampling.

Good: `F.w.st.ws` holds the candidate's r16 after `run`. Now wire sink into cpu_chunk/gpu_chunk:

Now `run_pass` (sink begin/end, per-hold scores) and the whole branch/refine/final section:

Now the scale section's abort and the tail (coarse → branches → refine → corr → write):

Now replace the whole tail from "趟 0" to the end of `run`:

Let me review the scale section and the chunk header comment for consistency.

Now the builder functions in TabFactors.cpp:

Now the param inputs in `render_node`:

Now `fixed_cell`: add a `parent` flag so the Expr column shows the template for parents/file rows, and `NameEn` shows the hold tag for children.

Good. Now the table loop and the Optimize panel. Let me read the current state of those sections.

Now update the Hold tooltip, Run tip, and the Optimize block.

Now the Optimize block:

That nested format trick is ugly; split into separate tip calls.

Now remove the duplicated old `switch` header left above (the Idle/Loading cases were before my insertion). Let me view that region.

Now the builder section: `edit_row`/`view_file` lookups, tips, `conflict`, Save confirm.

Now `conflict` and Save confirm:

Now the table. I'll restructure into file groups with a representative row.

Now the row loop:

Now `TaskFactors.cpp` and `TabInspect.cpp`:

I'll make `InspectRequest::file` carry the row key (`file#widx`) so result lookup is per weight.

Now TabInspect.cpp `view_row`/`probe_key`/defaults:

Now RenderTabInspect's header section:

Check `factor::stat::hold_name` is reachable in TabInspect.cpp and look for other `.file` display spots there:

Good. Now grep leftovers across the codebase:

Now check column tips referencing old semantics, and lints:

Now TaskFactors.cpp comments at 47/125:

Now check the "check" todo items: op_check / OperatorsService / Search/Check.hpp against changed interfaces.

Let me verify the scan side sets `n_ops` etc. and how `error` rows interact with `tmpl`, plus `broken()` semantics:

Let me re-read the final table loop once for correctness after formatting.

UI 部分完成, 全部改动已 clang-format, 残留 grep 干净. 本轮改的:

- `TabFactors.hpp/.cpp`: 构建器每个参数前加勾选框 = 自由参数 (源串打裸键名, 输入禁用), `from_expr` 保留 free 位; 表按文件分组, 父行 = 当前 Hold 下 score 最高的权重, ≥2 权重时名字前有 ▸ 展开子行 (子行标 `#i hold`), 父行 / 单行有编辑勾选, 子行只高光; 排序按父行键, 展开的子行同键排; 状态列新增 "未搜索"; Expr 列父行显示模板; Optimize 按钮要求模板含自由参数, 状态 / 悬停改为逐 hold 的 picks 表 (branch / plateau / max / corr); `conflict` / Save 按 `tmpl` 比, 提示 weights 会丢; 去掉 `opt_hold`.
- `TabInspect.cpp` / `InspectService`: 高光对象改为权重行 `key` (`file#widx`), 请求 / 结果 `file` 字段即 key, 箭头在有效权重行间循环, 标题带 `#widx` 和搜索持有期.
- `TaskFactors.cpp` 调用点、README 四条一行说明同步.

请你编译跑一下. 旧格式因子文件 (`params` / 顶层 `stat`) 现在不读 stat, 下次回写顺手删, 需要 Optimize / Run 重算.

---

**User**

@/home/chuyin/.cursor/projects/home-chuyin-work-stk/terminals/11.txt:1000-1013 

---

**Cursor**

重构行循环时把行末的 `PopID` 丢了.

已补回行末 `PopID` (放在列循环之后, 这样 `##exp` 箭头也在行 ID 作用域内). 请重新编译.

