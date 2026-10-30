# 工具自检

这一段会话不挖因子, 是**工具自检**: 把 "工具" 节里的每个工具都调一遍 ({{tools}}), 确认它们按说明工作. 成绩好坏无所谓; 候选与全局记录落在测试目录, submit_factor 只查门槛, 不会拷进正式库.

做法:
- 分几轮做完, 每条回复至多 {{max_calls}} 个调用; 要用上一步结果的 (候选名、代码、日期、研报 id、节号、续读位置) 放到下一条.
- 每个工具至少调一次, 参数尽量覆盖各个取值 (view_layers 的 split 与 cond, view_trades 的 side / sort / merge 与 from / to / tod, view_response 的 side / split, view_explain 带与不带 exprs, view_snapshot 带与不带 exprs, view_stock_day 带与不带 rows, view_book 跨午休 / 含开盘或收盘竞价 / 给 step · at · big 与都缺省, 以及非交易日的 view_book, search_leaves 的叶名 / 中文 / 多词 query, search_docs 的 tag / text 与各筛选, search_factors 的 sort, read_doc 的 card / outline / text 及按结果末尾的位置续读, list_notes 的 kind 各取值 / 不给 kind / scope here / query ...).
- 全局记录: request_feature / request_tool / note_insight 各提一条 (标题以 "[自检]" 开头, 内容照实写这次自检里看到的), 再用 ref 附议刚提的一条, 另写一个不存在的 ref 看是否退回并说清原因; report_issue 的 type 三个取值都用到 (问题本身就用它记). 提完用 list_notes 查回来, 对照编号与附议关系.
- 每类数据参数再故意写错一两个 (不存在的候选 / 特征叶 / 代码 / 评估期外的日期 / 写错或超长的 rows 时段 / 流派 / 研报 id / 超出的节号), 看结果是否说清了原因、其余调用是否照做.
- 假设板: hypothesize 提两条 (H1 / H2); 对 H1 先做一个观察, 再 judge 为 supported 并引用它; 再测 judge 的退回: 引用不存在的编号、引用 H2 提出之前的调用、判 supported 却只引用非观察调用、不带新证据再判同一条, 各看是否说清原因且判定没改; H2 判 refuted. 对照每条结果消息末尾的假设板.
- eval_factor 只收本会话 search_leaves 列出过的叶, 且须挂在判为支持的假设上: 交模板前先 search_leaves 查过它用的叶; 另交一个用了没查过的叶的模板、一个挂在 H2 (落空) 上的, 看是否被退回并说清原因.
- eval_factor 交 3 个 (都挂 H1, base 空, parts 按模板拆): 从 list_library 里挑一个有最佳权重的正式因子, 原样抄它的模板 (保证测到有权重的路径; 正式库里没有带本持有期权重的, 改从 list_tried 挑有参数组的, 也没有就换成第二个自己写的简单模板, 并在总评里注明有权重的路径没测到); 一个自己写的简单模板; 一个不合法的 (算子超上限或名字不合法). 有权重的拿去喂观察工具的 factor 再 submit_factor 它; 无显著的也拿去喂一次 factor (应看到它的代表点).
- 回测的下一条回复: 先故意再交一个 eval_factor 且不带 judge (应整条退回, 说明上条有回测、H1 待判), 重发时 judge H1 (引用本批回测与观察). 再下一条测改动归类: 以刚评的一个为 base, 交只改数字的、改了两处的、换平滑的 (base 未达门槛应退回, 达门槛则应落文件并记一个旋钮类配额), 以及 parts 漏掉一个多元算子成分的, 各看原因是否说清.
- {{ask}}.
- 每个结果都对照工具说明查: 该有的部分齐不齐、数是否合理 (分位 0..100、mono ∈ [-1, 1]、超额与收益量级不离谱、计数自洽)、截断与续读是否接得上、出错时原因是否说清. 发现问题立刻 report_issue (type bug): detail 写明 工具 + 参数 + 看到了什么 + 预期是什么.
- 全部调过后, report_issue 写一条总评 (type idea; 每个工具一行: 正常 / 有问题及哪一条的编号), 再 finish.
