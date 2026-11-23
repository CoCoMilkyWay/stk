# Agent 挖因子 (外部 agent 说明书)

这是一套 A 股量化因子挖掘系统. 你是外部 agent, 经 `py/agent.py` 调用系统的研究工具: 访问特征库、看数据、提机制假设、回测因子、把好因子提交进正式库. 系统是一个一直开着的 GUI 程序 (`python run.py`); 你的每次调用都在程序里实时执行, 研究员在 GUI 的 Agent → Mine 页看得到你的每批调用与结果.

## 接入

会跑 shell 就行: `python py/agent.py <工具> [参数]`, 打印结果原文.

- 参数 = JSON 对象 (缺省 `{}`) / `@文件` (读该文件的 JSON) / `-` (读标准输入; 长 JSON 用 heredoc 最省事, 不用转义)
- 退出码 0 = 做成, 1 = 工具报错 (原文写原因)
- 结果可能很长 (`doc` 约 7 万字): 重定向进文件再按需读, 省上下文
- 程序没开时脚本报连不上; 地址由程序写在 `output/run/agent/remote.json`

```bash
python py/agent.py doc > /tmp/doc.md
python py/agent.py run - <<'EOF'
{"calls": [{"tool": "list_tried", "args": {}}, {"tool": "view_feature", "args": {"exprs": ["bar.30m.amount_30min"]}}]}
EOF
python py/agent.py wait '{"job": 1}'
python py/agent.py unlock
```

## 锁

- 你的头一个请求即替你锁上 Agent 模块, 结果开头会提醒一次. 锁着时研究员在 GUI 里解不了锁, 也开不了内置会话、用不了工作台, 不会干扰你, 放心做
- 用完调 `unlock` 交还 (有作业在跑时先 `wait` 做完或 `cancel`). 交还后你的下个请求会再替你锁上
- 锁只在内存: 程序重启即解锁. 研究员那时正在 GUI 里用 (有会话开着 / 工作台在跑) 就锁不上, 请求退回原因, 稍后再试
- 本程序进程内, 你的批号与假设板一直往下记 (解锁再锁不清零)

## 接口工具

`py/agent.py` 的第一个参数:

| 工具     | 输入                                                  | 做什么                                                                                                                                                  |
| -------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `doc`    | 无                                                    | 当前作用域的完整说明: 角色与做法 / 评估口径 / 判优 / 研究循环 / 回复协议 / 研究工具表 (参数逐个) / 开放的特征叶 (按族) 与算子. 开工先读, 换作用域后再读 |
| `scope`  | 可选 `pool` / `level` / `hold` / `years` / `lob`      | 不给参数 = 看当前作用域与各项可选值; 给了 = 换作用域 (有批在跑时不能换)                                                                                 |
| `run`    | `calls`: 1..12 个 `{"tool": 研究工具, "args": {...}}` | 交一批研究工具调用, 按序执行; 25 秒内做完即交回全部结果原文 + 假设板, 否则交回作业号与进度                                                              |
| `wait`   | `job`, 可选 `seconds` (≤ 50)                          | 续等一个作业: 做完交回全部结果原文, 否则交回进度                                                                                                        |
| `cancel` | `job`                                                 | 取消在跑的作业: 在跑的调用作废 (落了文件的回测删掉候选), 没开的不执行; 交回已做完的结果                                                                 |
| `unlock` | 无                                                    | 用完交还 Agent 模块给研究员                                                                                                                             |

- 一批 = 一个作业 = 作业号 (从 1 起); 一次只跑一批, 上一批没做完 (先 `wait` 或 `cancel`) 不能交下一批
- 调用编号 `R<批>#<序>` 写在每个结果的标题里, `judge` 的证据引用它
- 参数不合研究工具表 (未知工具 / 参数, 缺必填, 类型 / 范围 / 可选值不对, eval_factor 超 3 个) → 整批退回、一个都不执行, 不合处一次列全; 依赖数据的 (因子 / 特征 / 代码 / 日期 / 假设 / 证据) 不成只在该调用的结果里写原因, 其余照做
- 每个结果至多 6000 字, 超了截断并说明; 结果只交一次, 要用的数自己记下

例:

```json
{"calls": [{"tool": "view_layers", "args": {"factor": "CsRank(TsMeanRoll(TsLog(bar.5m.amt), 20))", "split": "tod"}},
           {"tool": "hypothesize", "args": {"title": "...", "actor": "...", "why": "...", "expect": "...", "refute": "..."}}]}
```

```text
run → "作业 3 还在跑: 第 2/3 个调用 [R3#2] eval_factor(...) (阶段 Search, 进度 40%) ..."
wait {"job": 3} → "# 第 3 批 (作业 3)\n\n## [R3#1] view_layers(...)\n\n…\n# 假设板\n\n…"
```

## 流程

1. **读说明**: `doc`. 下面只是要点, 口径与工具参数以 `doc` 为准
2. **定作用域**: `scope` 看池 × 层 × 持有期 × 评估年数 × lob, 需要就换 (换了再读 `doc`). 一个作用域里的统计只和同作用域、同评估期的比
3. **看现状**: `list_tried` (本作用域已试候选, 中位 rIC 降序) / `list_library` (正式库) / `list_notes` (前人的发现、申请与研究员的处理), 别重走证伪过的路
4. **研究循环** (每一步都是 `run` 里的调用):
   - 观察 (`view_*` / `search_leaves` / 研报 `search_docs` · `search_factors` · `read_doc`) 找现象
   - `hypothesize` 写下机制假设与可证伪的预测 (expect / refute)
   - 观察检验 → `judge` (证据 = 调用编号; 判支持须有观察)
   - 在判为支持的假设上 `eval_factor` 回测变体 (每个变体相对基准只改一处, 检验一个问题)
   - 回测后先诊断 (`view_layers` / `view_trades` / `view_explain` …) 再 `judge`: 变体说明了什么, 不追指标
5. **入库**: 研究透了, `submit_factor` 最好的版本 (权重的中位 rIC 达门槛即真拷进正式因子库)
6. **记录**: 现有叶刻画不了的量 `request_feature`, 缺的观察 `request_tool`, 值得跨会话留下的认识 `note_insight`, 工具问题 / 想问研究员的 `report_issue`; 同类已有的用 `ref` 附议
7. **交还**: 做完 `unlock`

## 口径要点

- **中位 rIC**: 评估期切成滚动窗 (窗长 = 评估期交易日数的 1/10, 10 年即 1 年; 每个交易日收尾一个窗), 每窗 rIC 均值的中位. Search 的目标、因子的 score、判优与入库门槛都是这一个数; 分层 (mono) / 多空 / 分年只作诊断
- **判优**: 达门槛的版本优先; 同档时中位 rIC 更高 (相差超过 5%) 者更好, 相差不到 5% = 持平
- **Search**: 模板的裸参数 (`d` / `k` / `k2` / `w`) 低差异采样一组点一趟评完, 每点按中位 rIC 定方向打分, 最高的一个成为权重, 再全量分层回测; 结果附参数扫描 (各点的中位 rIC)
- **观察**: 观察工具的 `factor` = 候选英文名 (取本持有期的权重) 或具体根式; 分位 / 组号已定向 (高 = 做多端); 收益一律是超额 (毛 − 本行在池均值), 单位 bp

## 名词

| 名词          | 含义                                                                                                                                                                                              |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 作用域        | 池 (`all` 或子池) × 层 (`min` / `5min` / `15min` / `30min` / `60min` / `day`) × 持有期 (`30m` / `60m` / `close` / `T+1C`, 该层能用的) + 特征来源 (bar 恒开, lob 可选) + 评估年数 + 开放的叶与算子 |
| 开放          | 研究员勾给 agent 用的叶与算子 (GUI 里设); `doc` / `search_leaves` / 模板与观察只认开放的; 用了未开放的叶或算子的候选与正式库因子不可见                                                            |
| 模板 / 具体式 | 模板 = 因子表达式, 参数可裸写交给 Search 定值, 例 `CsRank(TsMeanRoll(TsLog(bar.5m.amt), d))`; 具体式 = 不含裸参数的, 观察只接具体式                                                               |
| 评估期        | 标签库与开放的叶所在库落盘日交集的最近 N 年 (其前另装 10% 预热日只给回看); 只有与此刻评估期相同的统计才看得到                                                                                     |
| 候选          | `eval_factor` 交的一个变体: 英文名 / 中文名 / 模板 / 所挂假设 / 思路; 落成候选文件, 跨会话累积 (`list_tried`)                                                                                     |
| 假设板        | `hypothesize` 提的机制假设 (H1, H2 …), `judge` 判 待检验 / 支持 / 落空 / 不清; 每批结果末尾附整张板. 这里不强制挂假设, 但研究循环照样是正确的做法                                                 |
| 全局记录      | 特征申请 (F) / 工具申请 (T) / 发现 (I) / 建议与 bug (R): 跨会话、跨作用域累积; 研究员逐条标处理 (待办 / 已做 / 不做 / 搁置) 并可回一句, `list_notes` 看得到                                       |

## 研究工具

`run` 里能用的工具 (参数逐个见 `doc`). 清单在 `cpp/include/gui/task_agent/Tools.hpp`, 说明表在 `Tools.cpp`.

| 工具                                                         | 做什么                                                                                       |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------- |
| view_feature                                                 | 特征式的分布 / 日内轮廓 / 方差来源 / 自相关 / rIC, 多个时附两两相关                          |
| view_layers                                                  | 因子 20 组分层 + 两端尾部; 可按时段 / 月切开, 或按条件特征三等分各看分层                     |
| view_trades                                                  | 一端真持有过的仓位: 段收益分布、按时段 / 盘面标签的占比与收益、明细                          |
| view_holders                                                 | 最常落在一端的资产                                                                           |
| view_response                                                | 一端开仓前后的平均价格路径 (事件研究)                                                        |
| view_explain                                                 | 因子是不是已知量 (内置基准 + 给的特征式) 的影子: 逐个剥与全剥后的剩余                        |
| view_snapshot                                                | 某日某行截面两端各 n 只                                                                      |
| view_stock_day / view_stock_days                             | 一只票一天逐行 / 多日逐日                                                                    |
| view_book                                                    | 逐笔重放一只票一天里的一段: 逐时盘口与流量、大单事件、某一秒的全簿                           |
| search_leaves                                                | 查特征叶的公式、单位、说明与落盘变换 (同组叶合一行)                                          |
| search_docs / search_factors / read_doc                      | 查研报库: 分面逐层收窄到研报 / 研报里的因子, 读标注卡 / 目录 / 原文                          |
| hypothesize / judge                                          | 提假设 / 判假设 (证据须存在、在提出之后与本调用之前、做成了; 判支持须有观察, 再判须有新证据) |
| eval_factor                                                  | 同式去重 → 落候选文件 → Search → 单文件 Run; 返回结局 (下表) + 参数扫描                      |
| list_tried / list_library                                    | 列同评估期的已试候选 / 正式库, 中位 rIC 降序                                                 |
| list_notes                                                   | 查全局记录                                                                                   |
| submit_factor                                                | 提交一个已评候选入库 (权重的中位 rIC 达门槛才入)                                             |
| request_feature / request_tool / note_insight / report_issue | 提全局记录 (可 ref 附议同类已有的一条)                                                       |

候选的结局:

| 结局     | 含义                                                                                                      |
| -------- | --------------------------------------------------------------------------------------------------------- |
| 未落文件 | 名字 / 模板不合法、重名、与已有候选或正式库同式、算子超上限、用了未开放的叶或算子; 结果里写原因           |
| BROKEN   | 落了文件, 回测算不出                                                                                      |
| 无权重   | Search 没有一个参数点算得出滚动窗 rIC                                                                     |
| 有权重   | 符号 + 权重的中位 rIC (滚动窗) · 每期毛 / 净多空 · 年换手 + 是否达入库门槛; 附分年 rIC、参数扫描与 20 组 |

## 与 GUI 内置会话的差别

GUI 里还有一套内置挖掘会话 (研究员在 Mine 页选 LLM 后端开会话, 系统把 `doc` 同一份说明当系统提示发给模型). 外部 agent 用同一套研究工具与执行路径, 差别:

- 没有会话约束: `eval_factor` 的 `hyp` 可不在假设板上 (也不查 "判为支持 / 变体配额 / 待判先 judge"); 回测后不强制先诊断
- 没有 `ask_human` 与 `finish`; 要问研究员写 `report_issue`
- 上下文 / 研究树不适用: 结果只在 `run` / `wait` 里交一次
- `submit_factor` 达门槛即真入正式库, 全局记录真写 (与内置会话相同; 只有 GUI 的工作台只查不写)

## 附: GUI 内置会话的状态机

外部 agent 锁着时不可用 (Mine 页只读显示它的调用); 没锁时研究员照下图开会话.

- 颜色 = 这一步谁在干活: 黄 = 等人, 紫 = agent, 蓝 = 系统; 菱形 = 系统判断
- 箭头: `按 X` = 人的操作, 其余 = 条件 (成对出现)

```mermaid
---
config:
  layout: elk
---
stateDiagram-v2
    direction TB

    state "开会话" as GOpen {
        state "未开会话" as None
        None : 人 - Mine 页选
        None : 池 · 层 · 持有期 · 特征来源 · 评估年数
        None : 人 - 特征 / 算子页勾开放的
        None : 人 - 过程页写本会话要求 (可空)
        None : 人 - Config 页
        None : 选后端 · 直连 / 代理
        None : 选模型 · CPU / GPU
        None : 设入库门槛
        None : 开 / 关提问
        None : (工具表里有没有 ask_human)
        None : 设每段预算
        None : key 已填 · 模型嗅探成功
        None : 资产轴与标签就绪
        None : 有开放的叶与算子
        None : 才可 New / Test
        None : 人 - 或载入本作用域存过的会话
        state "打包规则" as Pack
        Pack : 定系统提示 (含工具表)
        Pack : 整段对话不变
        Pack : 发出首条消息
        Pack : (Test = 自检的首条; 末尾附本会话要求)

        [*] --> None
        None --> Pack : 按 New / Test
        Pack --> [*]
    }

    state "agent 思考" as Think
    Think : 读上条消息
    Think : (首条 / 结果 / 重发要求)
    Think : 发一串工具调用
    state Format <<choice>>
    state "要求重发" as Retry
    Retry : 发出重发要求 (连续 5 条即停下)
    Retry : 会话约束 -
    Retry : 上条有落了文件的回测
    Retry : 则本条不能回测
    Retry : 待判的假设须各有 judge

    state "逐个执行" as GAct {
        direction LR
        state "取下一个调用" as Next
        state Kind <<choice>>
        state "当场做" as Sync
        Sync : search_leaves 查特征叶
        Sync : list_tried · list_library
        Sync : list_notes 列表
        Sync : hypothesize 记上假设板
        Sync : judge 查证据后改判定
        Sync : submit_factor 达门槛
        Sync : 的拷进正式因子库
        Sync : request_feature
        Sync : request_tool · note_insight
        Sync : report_issue 记进全局记录
        Sync : finish 宣布挖完
        state "后台工作台" as Lab
        Lab : eval_factor 核假设与同式去重
        Lab : → 落候选文件 → Search → 单文件 Run
        Lab : view_* 分块流式全核归约成文
        Lab : view_book 逐笔重放成文
        Lab : search_docs · search_factors · read_doc
        state "人选答案" as Choose
        Choose : 人 - 逐题改选
        Choose : (已按推荐预选)
        Choose : 人 - 或选 "其他"
        Choose : 自己写意见
        Choose : 人 - 可查特征 / 算子表
        Choose : 人 - 可加附言

        [*] --> Next
        Next --> Kind
        Kind --> Sync : 当场工具
        Kind --> Lab : 后台工具
        Kind --> Choose : ask_human
        Sync --> Next
        Lab --> Next
        Choose --> Next : 按交答案
        Next --> [*] : 都做完
    }
    state Fin <<choice>>
    state "交回结果" as Round
    Round : 发出各调用的结果
    Round : (按序, 带调用编号)
    Round : + 假设板 (+ 排队的插话)

    state "停着" as Pause
    Pause : 记停下原因 · 写停下报告与可读日志
    Pause : (入库清单 / 假设板 / 本会话的全局记录)
    Pause : 存会话存档
    Pause : 人 - 按继续接着停下处走
    Pause : 人 - 右键某轮回退
    Pause : (从那轮重来)
    Pause : 人 - 按关闭放下 / 按 New 开下一段

    [*] --> GOpen
    GOpen --> Think
    GOpen --> Pause : 按载入
    Think --> Format
    Format --> Retry : JSON 不合协议 / 违会话约束
    Format --> GAct : 合协议且守约束
    Retry --> Think
    GAct --> Fin
    Fin --> Round : 没调 finish 且预算未满
    Fin --> Pause : 调了 finish / 本段预算用完
    Round --> Think
    Think --> Pause : 按 Stop / 按 Cancel / 回复失败 / 连续 5 条不合协议
    GAct --> Pause : 按 Stop (在做的调用做完) / 按 Cancel
    Pause --> Think : 按继续 (要发消息)
    Pause --> GAct : 按继续 (有余下的调用)

    classDef human fill:#fff3b0,stroke:#b8860b,color:#000
    classDef agent fill:#e4d4f4,stroke:#7b4fa0,color:#000
    classDef sys fill:#d4e8f8,stroke:#3a6ea5,color:#000
    class None, Choose, Pause human
    class Think agent
    class Pack, Retry, Next, Sync, Lab, Round sys
```
