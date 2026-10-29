# Agent 挖因子

人在 GUI 的 Agent → Mine 页操作. 系统 (本程序) 把项目规则打包发给 agent (大模型); agent 消化后提一次问题由人选答; 此后全自动: agent 交因子模板, 系统回测并反馈, agent 据此改进, 对满意且达门槛的候选提交入库, 直到它宣布挖完. 途中 agent 的疑问攒到最后一起交给人.

## 名词

| 名词       | 含义                                                                                                                  |
| ---------- | --------------------------------------------------------------------------------------------------------------------- |
| 池         | 股票池: `all` (全市场) 或某个子池                                                                                     |
| 层         | 因子在哪个频率上算: `min` / `5min` / `15min` / `30min` / `60min` / `day`                                              |
| 持有期     | 因子预测多远的收益: `30m` / `60m` / `close` / `T+1O` / `T+1C` (该层能用的才列出)                                      |
| 模板       | 因子表达式, 窗长等参数可以不写数字 (裸写 `d` / `k`), 交给 Search 定值. 例: `CsRank(TsMeanRoll(TsLog(bar.5m.amt), d))` |
| 候选       | agent 交的一个因子: 英文名 + 中文名 + 模板 + 一句话假设                                                               |
| 批         | agent 一次回复里的全部新候选 (每次 8 个)                                                                              |
| Search     | 给模板里没写数字的参数找最好的取值; 可能一组都找不到 (= 无显著), 也可能找到几组                                       |
| Run        | 对一组定好值的参数做全区间分层回测, 出 score / 多空收益 / rIC / 单调性等统计                                          |
| 入库门槛   | Config 页设的统计下限 (score / t / 单调性 / 池内有效率); agent 提交的候选至少有一组参数全部达到才入库                 |
| 会话       | 按 New 开始的一段连续对话. 池 / 层 / 持有期 / 后端 / 模型 / 入库门槛在会话内不变                                      |
| 会话目录   | `output/agent/<池>/<层>/<持有期>/`: `factors/` 放候选因子文件 (历次会话累积), `chat/<时间>/` 放会话日志               |
| 会话日志   | 这段对话发收的每条消息 + 每次提交的入库结果 + 结束报告                                                                |
| 提问面板   | Mine 页左上: agent 的问题逐题列出选项, 底下一个附言框                                                                 |
| 结果表     | Mine 页右侧: 会话目录里全部候选及其回测统计, 按 score 降序; 入库的标出                                                |
| 正式因子库 | `output/factors/<池>/<层>/`: Factors 页用的因子                                                                       |

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
        None : 人 - Config 页选 后端 · 模型 · CPU / GPU
        None : 人 - Config 页设入库门槛
        None : 资产轴与标签就绪才可 New
        state "打包规则" as Pack
        Pack : 定系统提示, 整段对话不变
        Pack : 发出首条消息

        [*] --> None
        None --> Pack : 按 New
        Pack --> [*]
    }

    state "澄清" as GAsk {
        state "agent 消化" as Digest
        Digest : 读规则, 提出要人定的问题
        state QFormat <<choice>>
        state "要求重发" as QRetry
        QRetry : 发出重发要求
        state "人选答案" as Choose
        Choose : 人 - 逐题点选
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
        state "agent 作答" as Asking
        Asking : 交一批新候选
        Asking : 提交满意的已评候选
        Asking : 疑问写进 issues, 不再提问
        state Format <<choice>>
        state "要求重发" as Retry
        Retry : 发出重发要求
        state "处理提交" as Submit
        Submit : 达门槛的写进正式因子库
        Submit : 未达的记进会话日志
        Submit : 记下 issues
        state Fin <<choice>>

        state "逐个回测" as GEval {
            state "写候选文件" as Stage
            Stage : 查名字和模板
            Stage : 合格的写进 factors/
            Stage : 不合格的记下原因
            state "取下一个" as Next
            Next : 按写入顺序
            Search : 在会话持有期上搜参数
            Search : 参数组写回因子文件
            Run : 每组参数回测一次
            Run : 统计写回因子文件
            state "写反馈" as Round
            Round : 发出反馈消息

            [*] --> Stage
            Stage --> Next
            Next --> Search : 有空参数
            Next --> Run : 无空参数
            Search --> Run : 找到参数
            Search --> Next : 找不到参数
            Run --> Next : 回测完
            Next --> Round : 都评完
            Round --> [*]
        }

        [*] --> Asking
        Asking --> Format
        Format --> Retry : JSON 不合格
        Format --> Submit : JSON 合格
        Retry --> Asking
        Submit --> Fin
        Fin --> GEval : 未宣布挖完
        Fin --> [*] : 宣布挖完
        GEval --> Asking
    }

    state "结束报告" as Report
    Report : 记结束原因
    Report : 汇总全部 issues 与入库清单
    Report : 写进会话日志, Mine 页显示

    [*] --> GOpen
    GOpen --> GAsk
    GAsk --> GMine
    GMine --> Report
    GAsk --> Report : 按 Cancel / 回复失败
    GMine --> Report : 按 Stop / 按 Cancel / 回复失败

    classDef human fill:#fff3b0,stroke:#b8860b,color:#000
    classDef agent fill:#e4d4f4,stroke:#7b4fa0,color:#000
    classDef sys fill:#d4e8f8,stroke:#3a6ea5,color:#000
    class None, Choose human
    class Digest, Asking agent
    class Pack, QRetry, Retry, Submit, Stage, Next, Search, Run, Round, Report sys
```

- 按 Stop: 当前这步做完再结束; 按 Cancel: 立即结束, 在跑的回复 / 回测作废
- 回复失败: 网络重试用尽 / 回复被截断 / 平台拒答

## 系统发给 agent 的消息

模板都在 `config/agent/prompt/` 下, Config 页可直接改.

| 消息     | 内容                                                                                                                                           |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| 系统提示 | 池 / 层 / 持有期; 表达式写法; 回测口径; 入库门槛; 研究要求; 两阶段回复格式; 该层能用的全部特征; 全部算子; 正式因子库里已有因子在该持有期的成绩 |
| 首条     | 会话目录里以前试过的候选 (历次会话累积, 让 agent 别重复) + "消化规则, 提出要人定的问题"                                                        |
| 答复     | 每题人选的答案 + 附言 + "开始挖掘; 此后不得提问, 疑问写进 issues"                                                                              |
| 反馈     | 上次提交的每个候选入库与否 (未达的列出差在哪项) + 本批每个候选的结局 (下表)                                                                    |
| 重发要求 | 哪里不合格式 + "按格式重发"                                                                                                                    |

## agent 的回复

每次只回一个 JSON.

澄清 (只一次):

```json
{"questions": [{"prompt": "问题", "options": ["选项", ...], "multiple": false}, ...]}
```

自动挖掘 (每轮):

```json
{
  "factors": [{"name": "英文名", "name_cn": "中文名", "expr": "模板", "note": "一句话假设"}, ...],
  "submit": ["已评候选的英文名", ...],
  "issues": ["疑问 / 建议", ...],
  "done": false
}
```

- `factors`: 每批 8 个; `done` 为 true 时为空
- `submit`: 会话目录里已评过的候选 (含以前会话的); 可空
- `issues`: 想问人的、对规则或系统的疑惑, 不阻塞挖掘; 可空
- `done`: true = 挖完, 本次 `submit` 处理完即结束

候选的结局 (反馈消息逐个写, 结果表 "结局" 列同样显示):

| 结局     | 含义                                                                        |
| -------- | --------------------------------------------------------------------------- |
| 未落文件 | 名字或模板不合法, 或与已有的重名; 原因原样告诉 agent, 改正后可以重交        |
| BROKEN   | 文件写成了, 但回测时算不出来                                                |
| 无显著   | Search 找不到显著的参数                                                     |
| 有参数组 | 每组参数给 score / 每期多空收益 / rIC 及其 IR 和 t 值 / 单调性 / 池内有效率 |
| 未评估   | 会话中途结束时还没轮到                                                      |
