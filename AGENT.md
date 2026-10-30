# Agent 挖因子

人在 GUI 的 Agent → Mine 页操作. 系统 (本程序) 把项目规则打包发给 agent (大模型); agent 消化后提一次问题 (每题带推荐) 由人选答, 或 (Config 页关了澄清) 不问, 按自己的理解定因子与方向; 此后全自动: agent 定下一个因子, 每轮交它的一批实验变体, 系统回测并反馈, agent 据此一处一处改, 做到改不动时把最好的版本提交入库并宣布挖完. 一段会话只做一个因子, 好坏先看分层清晰 (mono), 再看多空收益: 分层做不出来 (mono 不到 Search 门槛) 的因子没有分析价值, 先想尽办法把分层做出来; 过入库门槛的版本优先, 都过门槛时 mono 不明显下降 (≤ 0.01) 而每期毛多空更大 (超出噪声) 者更好; rIC 只作入库底线, 不管风格与相关性. 途中 agent 的疑问攒到最后一起交给人.

## 名词

| 名词       | 含义                                                                                                                                                          |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 池         | 股票池: `all` (全市场) 或某个子池                                                                                                                             |
| 层         | 因子在哪个频率上算: `min` / `5min` / `15min` / `30min` / `60min` / `day`                                                                                      |
| 持有期     | 因子预测多远的收益: `30m` / `60m` / `close` / `T+1O` / `T+1C` (该层能用的才列出)                                                                              |
| 模板       | 因子表达式, 窗长等参数可以不写数字 (裸写 `d` / `k`), 交给 Search 定值. 例: `CsRank(TsMeanRoll(TsLog(bar.5m.amt), d))`                                         |
| 候选       | agent 交的一个因子变体: 英文名 + 中文名 + 模板 + 一句话 (相对哪个版本改了什么、想验证什么)                                                                    |
| 批         | agent 一次回复里的全部新候选 (1..8 个, 都是会话所做那个因子的变体)                                                                                            |
| Search     | 定方向并给模板里没写数字的参数找最好的取值 (没有空参数的只评本身); 只认 mono 过门槛的, 目标 = 多空 × rIC × mono²; 可能一组都找不到 (= 无显著), 也可能找到几组 |
| 评估期     | 统计所在的交易日段 (所用特征库落盘日的交集); 只有与作用域此刻评估期相同的统计才给 agent 看, 不同窗口的数不可比                                                |
| Run        | 对一组定好值的参数做全区间分层回测, 出 score / 多空收益 / rIC / 单调性等统计                                                                                  |
| 入库门槛   | Config 页设的统计下限 (mono / rIC 均值; 多空不设门槛); agent 提交的候选至少有一组参数全部达到才入库                                                           |
| 会话       | 按 New 开始的一段连续对话. 池 / 层 / 持有期 / 后端 / 模型 / 入库门槛 / 澄清开关在会话内不变                                                                   |
| 选项记忆   | Mine 页作用域与 Config 页全部选项改了即存 `config/agent/ui.json` (本机, 不进仓库), 下次启动沿用; 失效的项回落默认                                             |
| 会话目录   | `output/agent/<池>/<层>/<持有期>/`: `factors/` 放候选因子文件 (历次会话累积), `chat/<时间>/` 放会话日志                                                       |
| 会话日志   | 这段对话发收的每条消息 + 带毫秒时间的交互流水 `events.jsonl` (每一步一行) + 结束报告                                                                          |
| 提问面板   | Mine 页左栏 "问题" 页: agent 的问题逐题列出选项, 推荐项标出并预选, 每题末项 "其他" 可自己写意见; 底下一个附言框                                               |
| 参考表     | Mine 页 "特征" / "算子" 页: 发给 agent 的同一份特征表与算子表 (可过滤), 人读题、看模板用; 随作用域, 不必开会话                                                |
| 结果表     | Mine 页 "候选" 页: 会话目录里全部候选及其回测统计, 按 score 降序; 入库的标出; 随作用域, 不必开会话                                                            |
| 正式因子库 | `output/factors/<池>/<层>/`: Factors 页用的因子                                                                                                               |

## 状态图

- 颜色 = 这一步谁在干活: 黄 = 等人, 紫 = agent, 蓝 = 系统; 菱形 = 系统判断
- 箭头: `按 X` = 人的操作, 其余 = 条件 (成对出现)
- 状态: `人 -` = 人在这里能做的, `发出` = 系统发给 agent 的消息, 其余 = 这一步做什么

```mermaid
---
config:
  layout: elk
---
stateDiagram-v2
    direction TB

    state "开会话" as GOpen {
        state "未开会话" as None
        None : 人 - Mine 页选 池 · 层 · 持有期
        None : 人 - Config 页选 后端 · 直连 / 代理 · 模型 · CPU / GPU
        None : 人 - Config 页设入库门槛
        None : 人 - Config 页开 / 关澄清 (首轮让不让 agent 提问)
        None : key 已填 · 模型嗅探成功 · 资产轴与标签就绪才可 New
        state "打包规则" as Pack
        Pack : 定系统提示, 整段对话不变
        Pack : 发出首条消息 (开澄清: 首轮, 让 agent 提问; 关: 开挖, 让它直接交第一批)

        [*] --> None
        None --> Pack : 按 New
        Pack --> [*]
    }

    state "澄清" as GAsk {
        state "agent 消化" as Digest
        Digest : 读规则, 提出要人定的问题
        state QFormat <<choice>>
        state "要求重发" as QRetry
        QRetry : 发出重发要求 (不限次)
        state "人选答案" as Choose
        Choose : 人 - 逐题改选 (已按推荐预选), 或选 "其他" 自己写意见
        Choose : 人 - 可查特征 / 算子表
        Choose : 人 - 可加附言
        Choose : 没有问题就直接按 Start

        [*] --> Digest
        Digest --> QFormat
        QFormat --> QRetry : JSON 不合格
        QFormat --> Choose : JSON 合格
        QRetry --> Digest
        Choose --> [*] : 按 Start
    }

    state "自动挖掘" as GMine {
        state "agent 作答" as Propose
        Propose : 交一批变体 (同一个因子)
        Propose : 改不动时提交最好的版本
        Propose : 疑问写进 issues, 不再提问
        state Format <<choice>>
        state "要求重发" as Retry
        Retry : 发出重发要求 (不限次)
        state "处理提交" as Submit
        Submit : 记下 issues
        Submit : 达门槛的拷进正式因子库 (正式库已有同名则不入)
        Submit : 未入库的连同原因记进会话日志, 下条反馈告诉 agent
        state Fin <<choice>>

        state "逐个回测" as GEval {
            state "写候选文件" as Stage
            Stage : 查名字和模板
            Stage : 合格的写进 factors/
            Stage : 不合格的记下原因
            state "取下一个" as Next
            Next : 按写入顺序, BROKEN 跳过
            Search : 在会话持有期上定方向、搜参数
            Search : 参数组与结局 (评估期 / 无显著时的代表点) 写回因子文件
            Run : 每组参数回测一次
            Run : 统计写回因子文件
            state "写反馈" as Round
            Round : 发出反馈消息

            [*] --> Stage
            Stage --> Next
            Next --> Search : 模板合法
            Search --> Run : 搜完 (找不到参数则 Run 无事可做)
            Search --> Next : 中途放弃 (记 BROKEN)
            Run --> Next : 回测完
            Next --> Round : 都评完
            Round --> [*]
        }

        [*] --> Propose
        Propose --> Format
        Format --> Retry : JSON 不合格
        Format --> Submit : JSON 合格
        Retry --> Propose
        Submit --> Fin
        Fin --> GEval : 未宣布挖完
        Fin --> [*] : 宣布挖完
        GEval --> Propose
    }

    state "结束报告" as Report
    Report : 记结束原因
    Report : 汇总全部 issues 与入库清单
    Report : 写进会话日志, Mine 页显示
    Report : 人 - 可再按 New 开下一段

    [*] --> GOpen
    GOpen --> GAsk : 开澄清
    GOpen --> GMine : 关澄清
    GAsk --> GMine
    GMine --> Report
    GAsk --> Report : 按 Stop / 按 Cancel / 回复失败
    GMine --> Report : 按 Stop / 按 Cancel / 回复失败

    classDef human fill:#fff3b0,stroke:#b8860b,color:#000
    classDef agent fill:#e4d4f4,stroke:#7b4fa0,color:#000
    classDef sys fill:#d4e8f8,stroke:#3a6ea5,color:#000
    class None, Choose human
    class Digest, Propose agent
    class Pack, QRetry, Retry, Submit, Stage, Next, Search, Run, Round, Report sys
```

- 按 Stop: 选答案时立即结束; agent 作答中 → 收到回复后结束 (挖掘阶段先处理完本条的 issues 与 submit, 不再落新候选); 回测中 → 本批评完后结束 (不再发反馈)
- 按 Cancel: 立即结束, 在跑的回复 / 回测作废
- 网络抖动一直重试 (每 30 秒一次, 不限次数), 不算失败; 按 Cancel / 退出程序随即收手
- 回复失败: 请求发出后断网 (Cursor 建 agent / 续对话的 POST 不重发) / 接口报错 (如模型在出口地区不可用) / 代理拒绝 / 回复被截断 / 平台拒答; 原因进结束报告

## 系统发给 agent 的消息

模板都在 `config/agent/prompt/` 下, Config 页可直接改.

| 消息     | 内容                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 系统提示 | 池 / 层 / 持有期 / 评估期; 行与时刻 (每行何时算因子、何时开 / 清仓, 15min 例; 粗层叶的可见时点); 表达式写法; 回测口径 (每行开仓 / 分组 / 每期毛多空 / 超额 / 系统已处理的竞价段·停牌·涨跌停·缺失值 / 复权); 做法 (一个因子、每变体改一处、截面时序同等、先分层后收益、根只看排序、噪声内算持平、入库门槛、何为更好、连续 3 轮无进展收手); 两阶段回复格式 (澄清只问人才能定的事); 该层能用的全部特征; 全部算子; 正式因子库里同评估期且算子不超上限的因子在该持有期的成绩 |
| 首条     | 开澄清时: 会话目录里以前试过、同评估期的候选 (历次会话累积, 可作起点接着做) + "只问人才能定的事, 一次问完"                                                                                                                                                                                                                                                                                                                                                              |
| 开挖     | 关澄清时代替首条: 同样的已试候选 + "自定因子与方向, 直接交第一批; 不得提问, 疑问写进 issues"                                                                                                                                                                                                                                                                                                                                                                            |
| 答复     | 每题人选的答案 (含 "其他" 自写的意见; 没照推荐的附上推荐) + 附言 + "开始挖掘; 此后不得提问, 疑问写进 issues"                                                                                                                                                                                                                                                                                                                                                            |
| 反馈     | 上次提交的每个候选入库与否 (未达的列出差在哪项) + 本批每个候选的结局 (下表) + "对照最好版本判断每处改动"                                                                                                                                                                                                                                                                                                                                                                |
| 重发要求 | 哪里不合格式 + "按格式重发"                                                                                                                                                                                                                                                                                                                                                                                                                                             |

## agent 的回复

每次只回一个 JSON.

澄清 (开澄清才有, 只一次):

```json
{"questions": [{"prompt": "问题", "options": ["选项", ...], "multiple": false, "recommended": [0]}, ...]}
```

- 只问人才能定的事 (从哪个因子起步、往哪个方向挖); 系统提示已定的 (评估口径、数据处理、何为更好、何时收手、怎么提交) 不问也不确认, 没写到的细节按最合理的理解办, 必要时挖掘中写进 issues
- `recommended`: 推荐的选项下标; 单选恰 1 个, 多选至少 1 个
- 每题人还能选 "其他" 自己写 (单选取代选项, 多选并列), 故选项不必穷举, agent 不列兜底项

自动挖掘 (每轮):

```json
{
  "factors": [{"name": "英文名", "name_cn": "中文名", "expr": "模板", "note": "相对哪个版本改了什么、想验证什么"}, ...],
  "submit": ["已评候选的英文名", ...],
  "issues": ["疑问 / 建议", ...],
  "done": false
}
```

- `factors`: 每批 1..8 个, 都是同一个因子的变体; `done` 为 true 时为空
- `submit`: 会话目录里已评过的候选 (含以前会话的), 一般只在最后交最好的版本; 可空
- `issues`: 想问人的、对规则或系统的疑惑, 不阻塞挖掘; 可空
- `done`: true = 挖完, 本次 `submit` 处理完即结束

候选的结局 (反馈消息逐个写, 结果表 "结局" 列同样显示):

| 结局     | 含义                                                                                                                |
| -------- | ------------------------------------------------------------------------------------------------------------------- |
| 未落文件 | 名字或模板不合法, 或与已有的重名; 原因原样告诉 agent, 改正后可以重交                                                |
| BROKEN   | 文件写成了, 但回测时算不出来                                                                                        |
| 无显著   | Search 找不到显著的参数; 给落选原因 + 代表点 (过门槛的峰里估计最高的, 没有峰则 mono 最高的点) 的统计与目标 ± 标准误 |
| 有参数组 | 每组参数给 mono / 每期多空收益 / rIC; 另附 score ± 标准误 (t) / 池内有效率                                          |
| 未评估   | 会话中途结束时还没轮到                                                                                              |

## 交互流水

会话日志里的 `events.jsonl`, 一行一个事件: `time` (毫秒) + `event` + 下表字段 (值为原生类型: 轮次 / 计数 / 耗时是数, done / ask 是布尔, gate 是对象, 其余是字符串).

| event        | 何时               | 字段                                                                                                                                                                                                                             |
| ------------ | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `open`       | 按 New             | pool / level / hold / backend / model / net (`direct` / `proxy 主机:端口`) / eval / gate (`{mono, ric}`) / ask (澄清开关) / tried (已试候选数)                                                                                   |
| `ask`        | 发出一条消息       | round / kind (first / go / answer / round / retry) / chars                                                                                                                                                                       |
| `reply`      | 收齐一条回复       | round / end (ok / cut / error / cancelled) / why (非 ok 的原因) / id (Zhipu 响应 id / Cursor run id) / seconds / think_seconds (发出到首个正文增量) / prompt_tokens / cached_tokens / completion_tokens / chars / thinking_chars |
| `parse_fail` | 回复不合格式       | round / phase (digest / propose) / error                                                                                                                                                                                         |
| `questions`  | 澄清回复合格       | round / n                                                                                                                                                                                                                        |
| `pick`       | 按 Start, 每题一行 | question / picked (含 "其他: 意见") / recommended                                                                                                                                                                                |
| `start`      | 按 Start           | note                                                                                                                                                                                                                             |
| `mine_reply` | 挖掘回复合格       | round / factors / submit / issues / done                                                                                                                                                                                         |
| `issue`      | 每条 issue         | round / text                                                                                                                                                                                                                     |
| `submit`     | 每个提交           | round / name / result (入库 / 未入库原因)                                                                                                                                                                                        |
| `stage`      | 每个新候选         | round / name / expr / note / result (落文件 / 未落原因)                                                                                                                                                                          |
| `eval_begin` | 开始回测本批       | round / files                                                                                                                                                                                                                    |
| `eval`       | 每个文件评完       | name / result (权重数与最佳统计 / 无显著 / BROKEN)                                                                                                                                                                               |
| `eval_end`   | 本批评完           | —                                                                                                                                                                                                                                |
| `stop`       | 按 Stop            | —                                                                                                                                                                                                                                |
| `cancel`     | 按 Cancel          | —                                                                                                                                                                                                                                |
| `finish`     | 进结束报告         | reason / rounds / adopted / issues                                                                                                                                                                                               |
