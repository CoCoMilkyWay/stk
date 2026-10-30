#pragma once

// =============================================================================
// Stat: 因子评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 无流式后端 —— 评估只在挖掘侧)
// =============================================================================
//   输入  因子平面 x [T][A] (SoA: float + 有效位) + 口径 Frame (见下) + H 组常驻标签 (每组一个持有期键 hold, 见下【持有期键】):
//           v[kLegs] 做多 lv / 做空 sv 价格收益 (features 层 LabelLob, 毛口径; fp16 位, 与落盘同格式, 无精度损失. 冲击不进标签: 超额对每 t 去均值,
//                    成本进来只剩截面相对项 —— 消费端在二级上按组池化成本扣). 分钟型 entry / exit 同为盘口中间价 (行 m 的 exit = 行 m+h 的
//                    entry), 相邻行精确伸缩: 留仓成员逐期标签之和 = 连续持有收益, 【换手成本】的"留仓不付"才成立; 价差只在成本列
//           m        标签有效位 (long / short 同一快照, 共用一张)
//           eo       建仓可成交码 kEoBuy | kEoSell (features lb_entry_ok; 各持有期共用一张): 见【仓位感知】
//           ry       预处理: 做多标签逐行截面 rank (r16), 与因子无关, 常驻期算一次 (prep_label)
//
//   【只做多】一切组合 (组 / 多空两端 / Inspect 尾部 / 池篮子) 都是做多账本: 收益取做多标签 lv, 建仓要价点可买, 冲击取买入冲击.
//     标签两侧 Leg (kLong = lv / kShort = sv, Label::v 按 [kLegs] 下标) 里 sv 只在续持的名义估值出现 (价点卖不出 → 名义值 = −sv, 【仓位感知】).
//   【多空 LS】两端 End = 所评方向的最高组 G20 (kTop) 与最低组 G1 (kBot), 各是一本做多账本 (就是该组的 grp, 同一套建仓 / 续持 / 换手);
//     多空 = ½ (G20 − G1): 资金 1 两本各半, 做多 G20 的收益减做多 G1 的收益, 不做空. 分层与多空同一组账本, 多空只是两端组之差.
//   【方向 Dir】kFwd = 正向 (G20 = 因子最高组); kRev = 反向 (因子取负, 组号倒序). 方向是评估参数 (因子行的 sign), 不改写表达式:
//     Row / Accum 恒按正向攒, 反向由 oriented 改读 —— rIC 变号, 组号倒序 (k ↔ K−1−k), 多空由两端组派生故恰好变号; 同一份数两种读法,
//     Search 定向与 Run / Inspect / Portfolio 按 sign 评一致. 端码 (换手成本) / Inspect 尾部 / alpha 分布由 rx 按所评方向现取 (end_code(x, d)).
//   输出  两级:
//     一级 Row[H][T]  每 (持有期, 时刻) 一行沿 A 轴归约的统计 (两后端各算, 对拍在此级)
//     二级 HoldStat   每持有期 沿 T 轴汇总成十几个标量 (Accum 一遍可合并累加器 → finish; 主机 double, 两后端共用同一份代码)
//
//   【口径 Frame】alpha 因子分两种坐标系, 由因子表达式的根算子决定 (Expr.hpp root_frame). 标签**统一超额语义**:
//     e = lv − mean_J lv, 每 t 的共同分量 (市场水平 / 市场择时) 被扔掉, 评的都是"相对市场";
//     一套代码只在两处分叉:
//     CS  截面: 根是截面归一算子 (CsRank / CsNormRank / CsZ). 每 t 沿 A 轴 rank x (排序) → 20 组 = 截面分位组;
//         因子值 = 仓位 (每 t 居中 → 多空对冲, β 自然隔离; 超额对 rank 类统计是平移不变的). 大块并列会把相邻组挤空.
//     TS  时序: 根是 TsRankRoll(d = ts_norm_d(seg), 整数个交易日), 输出已是每资产对自身滚动历史的分位 ∈ [0,1]
//         (整数天窗跨日 deseason). 不排序, 直接量化 r16 = round(x · kRankMax) (r16_quant) → 20 组 = 自身历史分位组;
//         评的是"个股处于自身高位时是否跑赢市场", β 由超额隔离, 样本间不因共同分量相关.
//     两口径之后逐字共用: grp_of / rIC / 超额 / 持仓口径 / ls / mkt / rAC / 二级汇总. 组允许空 (那一刻没有资产在该组), 空组收益 0.
//
//   【持仓口径】每组资金固定为 1, 满仓 cap = Labels::cap 只 (调用方给 n_full = 池内平均有效资产数 / kGroups, 与因子无关), 每只 1/cap:
//     触发 n 只 → 持仓 min(n, cap) / cap, 超过 cap 满仓在 n 只里均分 → 组收益 = Σe / max(n, cap) (book_den); 未触发 → 0.
//     触发不足 (稀疏 / 并列挤空) 与集中度直接进收益, 不另设活跃度项.
//
//   【rank 口径】精确并列均秩 (排序, 不用桶近似 —— 原始因子重尾时一个离群点会把整截面压进同一桶, IC 归零):
//     r16 = round(pct · kRankMax) ∈ [0, kRankMax], 全整数公式 (r16_of), 两后端逐位一致; kRankNone = 无效.
//     行有效样本 n < 2 或全并列 (spread 为假) → 整行 kRankNone.
//     rank(x) 只看 x 的有效集, rank(y) 只看 y 的有效集 (与因子无关才能常驻); 两者的联合集 J 上算统计.
//     TS 的 r16_quant 走 double (float × 65534 在 double 内精确, 与 FMA 无关), 两后端逐位一致; x ∉ [0,1] 断言.
//
//   【一级 Row (每 h, t)】J = {a : rx ≠ None ∧ ry ≠ None}; e = 超额标签 lv − mkt
//     ric    rank IC = Spearman = Pearson(r16_x, r16_y) over J. 五个和都是整数 (pair_add → pearson_int), 两后端逐位一致.
//     grp[k] 第 k 组 (k = grp_of(r16_x), 等分 kGroups 组) 的**持仓收益** Σe / max(cnt, cap), 代数上 Σlv − cnt·mkt 再除, 一遍 A 轴.
//            cnt[k] = 组真持有只数 (【仓位感知】; 收益按续持取名义值). 二级沿 t 取均值 (Σ / n_all, 见【二级】).
//     ls_row 多空 = ½ (grp[K−1] − grp[0]) (Row 当前读法下的两端, 【多空 LS】) —— 资金 1 两本各半, 无杠杆, 相对市场.
//            二级 HoldStat 的 LS 系 (Moments ls / ls_annual / sharpe_of / beta) 全部基于它. 无信号时 ≈ 0.
//     mkt    J 上做多标签原值均值 = 超额基准 (二级回归 beta 用它).
//     ok     标签侧有效: 非段末尾部 (见下) ∧ ric 可算 (n ≥ 2, 两侧方差 > 0). 组可空 (空组不作废整行).
//     rac    rank AC: Pearson(r16_x(t), r16_x(t−h)) over 两行都有效的资产 —— lag = h 行 (hold_rows) 才对应"一个持有期换多少仓".
//     ok_rac t ≥ h ∧ n ≥ 2 ∧ 两侧方差 > 0. 与标签无关, 段末尾部也算.
//     段末尾部 (分钟型 / 收盘型): 行 t 的标签 = 该行末分钟 m = minute(t_seg) (Contract【层几何】) 的 L_1min 标签, entry 锚点 = 分钟 m+1 起始;
//       最后一个可成交锚点是收盘集合竞价 (14:57 起, 段末 kCloseAuction 分钟) 前一分钟的起始 = L_1min 行 kLastAnchorRow 的锚点.
//       分钟型 exit = 分钟 m+n 的锚点, m + n 越过它就"持有到收盘", 持有期缩短 → 掩掉 minute(t_seg) + n > kLastAnchorRow 的行 (tail_masked);
//       收盘型 m 越过它 = 收盘价建仓即平 (标签恒 0) → 同式 n 取 0. 日级型 (T+N 开盘 / 收盘) exit 在次日之后, 无尾部 (该行按收盘价建仓).
//       尾部行不开仓: 换手成本 / 净值同一判据.
//     ok / ok_rac 为假时对应字段全 0 (与 Contract 的 valid=false ⇒ v=0 同约).
//     J / ric / mkt 与可成交无关 (封板格照进截面); grp / cnt 只收真持有的格 (【仓位感知】). 平仓顺延在标签里 (features LabelHold).
//
//   【仓位感知】一个组合 (组 k / Inspect 尾部 / 池篮子; 成员判据 in 由调用方给) 每行一批 (做多), 格 a 在行 t 的批里:
//     真持有 held: 行 t 价点可买 (eo & kEoBuy), 或 与前一批交接 (hold_nets(t)) 且前一批同组合
//       (m ∧ in) 并真持有 —— 续持者不必成交, 封板也照持. 递归沿链回走到可成交为止, 前一批至多在 t 前 kChainDays 个交易日
//       (= 平仓结清窗; 更早视为断链), 所以每格只看 [t − kChainDays 日 − h, t + h] 的行: 分块带够前后带即与整张量一致 (Search 契约【分块流式】);
//       块外 (评估域首尾) 视为断链.
//     收益 hold_ret: 下一批 (t + h) 交接且同组合 (continues) → 不真平仓, 按名义价点估值: 价点上不可能两向都堵 (封涨停可卖, 封跌停可买,
//       停牌无价点 = 无行), 名义价点可卖 → lv 即名义值, 否则 = −sv (同一 entry / 同一名义 exit 价点); 不续持 → lv (含顺延).
//     非交接持有期 (收盘型 / 开盘型 / 盘中建仓的 T+N 收盘型) 无链, 自然退化为"建仓须可买, 平仓顺延".
//     等权组合天然高抛低吸 (封涨停减仓 = 卖, 封跌停加仓 = 买, 都可成交), 封板的只有新进 / 退出成员; 若要求建仓两向可成交,
//     会把封涨停的续持者整格踢出, 系统性漏掉涨停收益 (过罚). 换手成本同一份 held (【换手成本】).
//
//   【持有期键 hold】int, 与 features 层 LabelHold 的列名同源 (lb_<side>_<name>); 键的单位是**分钟**, 与因子所在层无关.
//     层上合法 (hold_ok): 分钟型 n 是行长 N 的整数倍且首行 minute(0) + n ≤ kLastAnchorRow (尾部不吞整段); close 要求 S > 1
//       (日层的行就在收盘, 即建即平); 日级型各层皆可.
//     独立样本口径的行数 h = hold_rows(hold, geo) (lag / 交接 / 重叠折算 / Sharpe 与换手的年化): 分钟型 = n / N; close 与 T+1 = S
//     (同日各行共一个 exit → 每日一个独立样本); T+N = N · S (相邻 N 日的窗口重叠). L_1min 上 = 分钟数.
//     资金口径的行数 h̄ = hold_span(hold, geo) (净值每批资金 / 成本 / 均值超额的年化): 一日各开仓行的在持行数 (建仓行起, 平仓行
//     hold_exit_row 止) 之和 / S, 尾部行不开仓不计 ⇒ 全日平均仓位恰为 1 (盘中在 1 上下摆); 日层 h̄ = h.
//
//   【协议表 1: 持有期 → 开仓 / 清仓 / 价】(features 标签 / Stat / 净值 / UI hold_desc 同一份; 改口径先改这里, 标签 kLabelSemver +1)
//     价点: E    = 行末分钟价点. Lob 层 (min) = 锚点 (分钟末) + LABEL_DELAY_SECONDS 的可成交盘口中间价;
//                  Bar 层 (5min..60min) = 之后首个有成交连续竞价分钟的 VWAP; 锚点后再无连续竞价 (≥ 14:57) → C(D)
//           C(D) = D 日收盘集合竞价撮合价 (末笔成交);  O(D) = D 日 09:25 开盘撮合价 (无撮合 → 首个有成交连续竞价 bar 的 open)
//     | 显示 (hold_name) | 列名 token | 键            | 适用层            | 开仓                     | 清仓                       | h (hold_rows) | h̄ (hold_span; J = 日内开仓行数) |
//     |------------------|------------|---------------|-------------------|--------------------------|----------------------------|---------------|---------------------------------|
//     | 1m               | 1m         | 1             | min               | C(行) @ E                | C(行)+1m @ E               | n / N         | n / N · J / S                   |
//     | 5m 15m 30m 60m   | <n>m       | n             | 行长 ≤ n 的分钟层 | C(行) @ E                | C(行)+n @ E (过 14:57 掩)  | n / N         | n / N · J / S                   |
//     | close            | close      | kHoldClose    | 非 day            | C(行) @ E                | C(D) @ 收盘撮合            | S             | Σ_{j<J} (S − 1 − j) / S         |
//     | T+1O             | t1o        | hold_open(1)  | 全部              | C(行) @ E; day 层 C(D)   | O(D+1) @ 开盘撮合          | S             | (S + 1) / 2                     |
//     | T+1C T+2C T+3C   | t<N>c      | hold_close(N) | 全部              | C(行) @ E; day 层 C(D)   | C(D+N) @ 收盘撮合          | N · S         | N · S + (S − 1) / 2             |
//
//   【协议表 2: 开关仓策略】(可成交码 / 顺延在 features 标签里定, 建仓 / 续持按【仓位感知】; 消费端只读标签与 lb_entry_ok)
//     | 环节       | 规则                                                                                                      | 落在          |
//     |------------|-----------------------------------------------------------------------------------------------------------|---------------|
//     | 可成交判据 | Lob 盘口: 可买 = 卖一侧有挂单, 可卖 = 买一侧有挂单; Bar 分钟 (Tradable): 可买 = low < 涨停 − ε,           | LabelHold     |
//     |            |   可卖 = high > 跌停 + ε; 开盘 / 收盘撮合价: 价 < 涨停 − ε 可买, 价 > 跌停 + ε 可卖; 涨跌停 NaN → 可成交  |   LabelLob/Bar|
//     | 开仓       | 组合只做多: E 价点可买才新建; 交接的续持者不必成交 (held); 不顺延开仓                                    | 【仓位感知】  |
//     | 续持       | 下一批交接且同组合 → 不真平仓, 名义价点估值 (lv 或 −sv)                                                   | 【仓位感知】  |
//     | 平仓       | 名义价点本侧不可成交 (lv 卖不出 / sv 买不回) 或名义日停牌 → 顺延到之后首个本侧可成交价点                | 标签          |
//     |            |   (分钟级 / 日级同一规则, 可跨日); 行所在日起 8 个交易日 (PEND_DAYS − 1) 仍未平 → 最近收盘价结清          |               |
//     | 尾部       | 分钟型 / 收盘型 minute(t) + n > kLastAnchorRow 的行不开仓 (tail_masked)                                   | Stat          |
//     | 仓位       | 每行一批, 资金 1/h̄, 批内每只 1 / max(只数, cap) (【持仓口径】); 仓位 = 在持各批之和, 全日平均恰为 1       | Stat / 净值   |
//     |            |   (盘中在 1 上下摆; 组内不足 cap / 封板新进 → 少于 1); 不显示, 只给年化 / 净值定满仓口径                  |               |
//     | 换手       | Δw 逐格 (微调照计); 只在行 t 建仓与行 t−h 平仓同一价点时交接 (hold_nets: 只交易差额), 否则整建整平        | 【换手成本】  |
//     | 成本       | 盘口段 (Lob): 增仓 Δw⁺ × 买入冲击 (封顶 max_impact) + 平仓量 × fixed_impact; 固定段 (无冲击列的层,       | 【换手成本】  |
//     |            |   Bar): 增仓量 + 平仓量 × fixed_impact; 各层同一套 CostAcc, 只在扣净线时扣, 标签恒毛                      |               |
//     | 净值       | 行 t 的批收益在 hold_exit_row 实现 (顺延不改实现行) × 1/h̄, 日内求和, 跨日复利; 显示的 LS / SR 取 HoldStat | 【二级】净值  |
//     | 复权       | 标签价 × 当日复权比: 跨日收益不含除权缺口                                                                 | 标签          |
//
//   【二级 HoldStat (每 hold)】只用 ok 行 (n) / ok_rac 行 (n_rac); n_all = 攒过的全部行 (含尾部 / 不 ok, 整张量 = T):
//     ric / rac / ls  逐行量沿 t 的同一套矩 (Moments, 同一个累加器 MomAcc; ric / ls 用 ok 行, rac 用 ok_rac 行):
//       mean / sd (ddof=1) / ir = mean/sd / pos = >0 占比 / skew, kurt (总体矩, 超额峰度)
//       t = ir · √(n/h): 持有期重叠 (相邻 h 行是同一段收益 / 同一段 lag, h = hold_rows), 有效样本按 n/h 折算, 不然 t 虚高 √h 倍
//     真实收益 (派生, 不落 HoldStat): 不 ok 的行记 0 补满 n_all 行 —— ls_period = Σls / n_all, ls_annual = × 每年满仓轮数 (span_per_year, 满仓一年),
//       sharpe_of = 补 0 后的 mean / sd × √每年独立期数 (periods_per_year; 由 n / ls.mean / ls.sd 还原 Σ / Σ², ann_sharpe). 因子表 / Inspect / Portfolio / Search 同一份.
//       一端 end_period(e) = HoldStat::grp[end_group(e)] (Σ grp / n_all, 同分母; 超额: G20 越大越好, G1 越小越好)
//     净值 (Inspect / Portfolio 曲线, 逐行结果按日折叠): 每行一批, 资金 1/h̄ (h̄ = hold_span),
//       行 t 的批收益在其平仓行实现 (hold_exit_row): 日收益 R_D = (1/h̄) Σ_{exit_day(t) = D} r_t (r = Row 的 ls_row / grp, 不 ok 记 0),
//       日内求和, 跨日复利. 沿全期 Σ R_D = Σr / h̄, 算术年化 = ls_annual (同一份收益, 只差落日); 显示的 LS / SR 一律取 HoldStat.
//     beta   ls 对 mkt 的 OLS 斜率 (alpha 略: beta ≈ 0 时 alpha ≈ ls.mean)
//     grp[k] 各组每行持仓收益沿 t 的均值 (Σ grp / n_all, 不 ok 行记 0); mono = Spearman(组号, grp[k])
//     n_trig 触发行数 = ok 行里两端至少一端非空; trigger_of = n_trig / n (诊断)
//     end_n  平均每触发行一端成员数 = Σ (两端 cnt) / (2 n_trig); util_of = end_n / cap, 不封顶 (> 1 = 并列把端组塞过满仓; 诊断)
//     n < 3 → 标签侧 (ric / ls / grp / mono / end_n) 全 0; n_rac < 3 → rac 全 0 (消费端先看对应计数)
//     累加器 Accum = Head (ric / rac / ls 的 MomAcc + beta 幂和 + n_all / n_trig) + Grp[kGroups] (组收益和 / 组计数), 可合并: 行区间并行各攒一份 merge; 超大张量沿 T
//     分块流式时跨块续攒, 不必落 Row. 拆两半是给 GPU 一 warp 一候选: lane 0 攒 Head, lane k 攒 Grp[k]; CPU 整行 add = 两半串起来.
//     oriented(acc, d) = 由正向攒的 acc 改读成方向 d 的 (与逐行 oriented(Row, d) 再攒逐位同), finish 恒出所评方向
//
//   【换手成本 (每 hold)】Row / HoldStat 全是毛口径; 净多空由消费端 (Inspect 期限结构净线) 在二级上扣 (Search 不看成本, 只评排序):
//     交易的是两端两本做多账本 (所评方向的 G20 / G1). 成本只看**端码** ec = end_code(r16, d) ∈ {kEcTop, kEcBot, kEcMid, kEcVoid}
//     (u8 平面, 两后端由 rx 逐格派生; GPU 只回传它, 不回 u16 rx) ∧ 该持有期的标签有效位 m ∧ 真持有 (held, 链上成员 = 同端码;
//     端成员 = 真建了仓的格, 与 Row 的端组同一集合). 增仓 Δ⁺ 要本格可买 (续持者封板加不了仓 → 记 0).
//     行 t 的批 (资金 1, 【持仓口径】) 每只权 w_t(a) = [a ∈ 端] / max(端样本数, 端满仓) (端满仓由调用方给: 两端 = cap, Inspect 尾部另给). 换手 = 目标仓位之差 Δw (无限细粒度, 微调照计):
//     【交接】行 t 建仓与行 t−h 平仓同一价点 (hold_nets: 分钟型恒是; T+N 收盘型只在行 t 按收盘价建仓时 (日层恒是); 收盘型 / 开盘型从不)
//       → 两批在同一时刻对冲, 只交易差额: Δ = w_t − w_{t−h} 逐格, 增仓 Δ⁺ 付建仓冲击, 减仓 Δ⁻ 付平仓固定冲击;
//       不交接 → 本批整建 (w_t 全付建仓冲击), 前一批的平仓由它自己记 (见下).
//     本批的平仓若不与下一批交接 (hold_nets(t + h) 假: 下一批尾部不开仓 / 非同一价点) → 建仓时一并记平仓量 w_t (付固定冲击).
//     金额段 (Cost) 两种, 每层至少一段, 换手量 (Δw) 各段同一份:
//       盘口段 = 买入冲击 imp[a] (已按 Config::max_impact 封顶; 可买的格恒有限, 装载层断言), 平仓量付固定冲击;
//       固定段 = 无冲击列 (imp 空), 增仓量与平仓量都付固定冲击 fixed_impact.
//     每 h 一份 CostAcc 沿 t 求和; 每期成本 cost(端) = (Σ Δ⁺·imp + Σ 固定量 · fixed_impact) / n_all (与 ls_period 同分母). 尾部行不攒.
//     净多空 net_ls_period = ls_period − ½ (cost(kTop) + cost(kBot)) (两本各半资金, 同 ls); 年化 net_ls = × span_per_year (满仓一年).
//
//   【视图】两后端 / 全部调用方同一组参数形状 (宿主或设备指针, 由后端定): Plane (因子平面) · Batch (C 个同形候选, Stat = 1) ·
//     Labels (持有期键 hd 与其标签 l[] 同处, 带满仓 cap) · Span (块形 T × A + 层每段行数 seg + 分块流式 t0_abs / t_lo / t_back; 整张量三者为 0).
//     流程出口统一是二级累加器 Accum[H] (两后端同), HoldStat 由 finish_all 现算; 一级 Row 是可选的 sink.
//
//   【数值】整数和精确 (r16 ≤ 65534, A ≤ kMaxA → n·Σx² < 2^58, 有符号 64 位不溢); 浮点和 (标签值) 两后端
//   求和序不同, 走对拍容差. 全程 branchless 无 NaN: 标签 fp16 → float 是精确转换, 出口不产 inf.
//   【一份公式】契约公式 (ord / r16 / grp_of / tail / pair_add / pearson_int / finish_row / Accum::add, merge) 标 FACTOR_HD: CUDA 编译器下是
//   __host__ __device__, 纯 C++ 下为空 —— CPU / GPU 两后端直接调用这一份, 不各自复刻; 后端只剩访存与并行.
//   【precise-math】依赖受控浮点, 编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Contract.hpp"

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace factor::stat {

// ---- 常量 ----
inline constexpr int kGroups = 20;                                 // 分组数 (等分 pct rank)
inline constexpr int kMaxHold = 12;                                // 同时评估的持有期数上限
inline constexpr int kMaxCost = 8;                                 // 同时攒的换手成本金额段数上限
inline constexpr int kMaxA = 8192;                                 // 资产轴上限 (GPU 一行一 block 的片上排序容量)
inline constexpr int kCloseAuction = 3;                            // 段末收盘集合竞价分钟数 (14:57–15:00), 标签 exit 锚点不能落入
inline constexpr int kLastAnchorRow = kSegMax - kCloseAuction - 2; // 最后一个 entry 锚点可成交的 L_1min 行 (= 分钟号; 锚点 = 下一分钟起始, 须在 14:57 前)
inline constexpr int kDaysPerYear = 242;                           // 年化用交易日数
inline constexpr uint16_t kRankNone = 0xFFFF;                      // r16 无效
inline constexpr int kRankMax = 65534;                             // r16 = round(pct · kRankMax) ∈ [0, kRankMax]
inline constexpr unsigned kNoKey = 0xFFFFFFFFu;                    // 排序键的"无效"哨兵 (有限 float 的保序键 ≤ 0xFF800000, 不冲突)
// TS 口径根 TsRankRoll 的窗长 (行) = ≤ kMaxD 行的最大整交易日数 (整数天窗跨日 deseason; ≤ kMaxD 故分块前带盖得住)
constexpr int ts_norm_d(int seg) { return kMaxD / seg * seg; }

// ---- 标签侧 / 多空两端 / 方向 (见文件头【只做多】【多空 LS】【方向 Dir】) ----
enum Leg : int { kLong = 0,
                 kShort = 1 };
inline constexpr int kLegs = 2;
enum End : int { kTop = 0,
                 kBot = 1 };
inline constexpr int kEnds = 2;
enum Dir : int { kFwd = 0,
                 kRev = 1 };
inline constexpr Dir dir_of(int sign) { return sign < 0 ? kRev : kFwd; } // 权重行的 sign (±1) → 方向
// 端 e 的组号 (已定向的组: G20 / G1); 正向攒的组下 方向 d 的端 e 在 end_group(e ^ d)
FACTOR_HD inline constexpr int end_group(int e) { return e == kTop ? kGroups - 1 : 0; }
// 建仓可成交码 (features lb_entry_ok, 见【仓位感知】): 价点可买 | 可卖. 做多建仓要可买, 名义平仓要可卖
inline constexpr uint8_t kEoBuy = 1, kEoSell = 2;

// ---- 口径 (见文件头【口径 Frame】) ----
enum class Frame : uint8_t { CS,
                             TS };
inline constexpr const char *frame_name(Frame f) { return f == Frame::CS ? "CS" : "TS"; }

// ---- 持有期键 (见文件头【持有期键】) ----
inline constexpr int kHoldDayBase = 1000;                             // ≥ 此值为日级型 (含 close)
inline constexpr int kHoldClose = kHoldDayBase;                       // 持有到当日收盘
inline constexpr int kHoldCloseBase = 2000;                           // T+N 收盘型的基
inline constexpr int kHoldDaysMax = 30;                               // T+N 的 N 上限 (键值域 / 断言用)
inline constexpr int hold_open(int n) { return kHoldDayBase + n; }    // T+n 开盘平仓
inline constexpr int hold_close(int n) { return kHoldCloseBase + n; } // T+n 收盘平仓
FACTOR_HD inline constexpr bool hold_intraday(int hold) { return hold < kHoldDayBase; }
FACTOR_HD inline constexpr bool hold_is_open(int hold) { return hold > kHoldDayBase && hold < kHoldCloseBase; }
FACTOR_HD inline constexpr bool hold_is_close_n(int hold) { return hold > kHoldCloseBase; }
// T+N 的 N (日级型平仓日偏移; 分钟型 / close 为 0)
FACTOR_HD inline constexpr int hold_days(int hold) { return hold_is_open(hold) ? hold - kHoldDayBase : hold_is_close_n(hold) ? hold - kHoldCloseBase
                                                                                                                             : 0; }
// 键在值域内 (与层无关)
inline constexpr bool hold_key_ok(int hold) {
  return (hold >= 1 && hold <= kLastAnchorRow) || hold == kHoldClose ||
         ((hold_is_open(hold) || hold_is_close_n(hold)) && hold_days(hold) <= kHoldDaysMax);
}
// 持有期在层 g 上是否合法 (见文件头【持有期键】)
inline constexpr bool hold_ok(int hold, const Geo &g) {
  if (hold_intraday(hold))
    return hold >= 1 && hold % g.N == 0 && g.minute(0) + hold <= kLastAnchorRow;
  return hold != kHoldClose || g.S > 1;
}
// 独立样本口径的行数 h (层 g 上; lag / 交接 / 重叠折算 / Sharpe 与换手的年化): 分钟型 = n / N; close / T+1 = 一段 S 行; T+N = N 段. 资金口径见 hold_span
FACTOR_HD inline constexpr int hold_rows(int hold, const Geo &g) {
  if (hold_intraday(hold))
    return hold / g.N;
  const int n = hold_days(hold);
  return g.S * (n < 1 ? 1 : n);
}
// 列名 token → 键: "<n>m" / "close" / "t<N>o" / "t<N>c" (LabelGroups LABEL_GROUPS 的 name); 不识别 → 断言
inline int hold_from_name(std::string_view name) {
  assert(!name.empty());
  if (name == "close")
    return kHoldClose;
  const bool day = name.front() == 't';
  const char tail = name.back();
  assert((day ? tail == 'o' || tail == 'c' : tail == 'm') && "持有期 token 不合 <n>m / close / t<N>o / t<N>c");
  const std::string_view digits = day ? name.substr(1, name.size() - 2) : name.substr(0, name.size() - 1);
  assert(!digits.empty());
  int n = 0;
  for (const char c : digits) {
    assert(c >= '0' && c <= '9' && "持有期 token 数字部分非数字");
    n = n * 10 + (c - '0');
  }
  return !day ? n : tail == 'o' ? hold_open(n)
                                : hold_close(n);
}
// 键 → 显示名: "5m" / "close" / "T+1O" / "T+2C"
inline std::string hold_name(int hold) {
  if (hold_intraday(hold))
    return std::to_string(hold) + "m";
  if (hold == kHoldClose)
    return "close";
  return "T+" + std::to_string(hold_days(hold)) + (hold_is_open(hold) ? "O" : "C");
}

// ---- 参数: 键列表 (持有期键 Holds: 每个对应一组标签; 金额段键 Amts: 每个对应一组成本) ----
template <int N>
struct Keys {
  int n = 0;
  int k[N] = {};
};
using Holds = Keys<kMaxHold>;
using Amts = Keys<kMaxCost>;
// key 在 ks 里的下标; 不在 → −1
template <int N>
inline int key_index(const Keys<N> &ks, int key) {
  for (int i = 0; i < ks.n; ++i)
    if (ks.k[i] == key)
      return i;
  return -1;
}
inline int hold_index(const Holds &hd, int hold) { return key_index(hd, hold); }

inline void assert_holds(const Holds &hd, int seg) {
  assert(hd.n >= 1 && hd.n <= kMaxHold);
  for (int i = 0; i < hd.n; ++i) {
    const int h = hd.k[i];
    assert(hold_key_ok(h) && "持有期键越界 (close = kHoldClose, T+N = hold_open(N) / hold_close(N))");
    assert(hold_ok(h, geo_of(seg)) && "持有期在本层不合法 (分钟型须是行长整数倍且尾部不吞整段; 日层无 close)");
    (void)h;
  }
}

// ---- 输入: 一组标签 = 五张 [T][A] 平面的指针 (宿主或设备, 由后端定; 每个 hold 一组, 与 Holds 同序) ----
struct Label {
  const uint16_t *v[kLegs] = {};       // 两侧收益 fp16 位 (毛): 做多 lv / 做空 sv (sv 只给续持名义估值)
  const uint8_t *m = nullptr;          // 有效位 (两侧共用)
  const uint8_t *eo = nullptr;         // 建仓可成交码 kEoBuy | kEoSell (各持有期共用同一张; 【仓位感知】)
  const uint16_t *ry = nullptr;        // 做多标签逐行 r16 (后端 prep_label 填; 与因子无关, 常驻期一次)
  FACTOR_HD Label at(size_t i) const { // 偏到第 i 格 (行首 = t·A)
    assert(ry && "标签未预处理 (prep_label)");
    return {{v[kLong] + i, v[kShort] + i}, m + i, eo + i, ry + i};
  }
};

// ---- 视图 (见文件头【视图】) ----
inline constexpr int kMaxBatch = 128; // 一批候选上限 (GPU 走 kernel 参数区: 128 × 16 B = 2 KB)
struct Plane {                        // 因子平面 [T][A]: 值 + 有效位
  const float *v = nullptr;
  const uint8_t *m = nullptr;
};
struct Batch { // C 个同形候选 (同池 g / 同口径 / 同标签集); Stat = 1
  int C = 0;
  Plane x[kMaxBatch];
};
struct Labels { // l[i] ↔ hd.k[i]
  Holds hd;
  Label l[kMaxHold];
  double cap = 0.0; // 满仓一组只数 (【持仓口径】; 调用方给 n_full)
};
struct Span { // 块形 + 层几何 + 分块流式 (Search 契约【分块流式】); 整张量 t0_abs = t_lo = t_back = 0
  int T = 0, A = 0;
  Geo g; // 因子所在层 (Contract【层几何】): tail_masked / hold_rows 用
  int t0_abs = 0, t_lo = 0, t_back = 0;
  FACTOR_HD size_t n() const { return static_cast<size_t>(T) * A; }
  FACTOR_HD int t_hi() const { return T - t_back; } // 统计行 [t_lo, t_hi)
};

inline void assert_span(const Span &s) {
  assert(s.T >= 1 && s.A >= 1 && s.A <= kMaxA && static_cast<long long>(s.T) * s.A < (1LL << 31));
  assert(is_seg(s.g.S) && geo_of(s.g.S).N == s.g.N && geo_of(s.g.S).M0 == s.g.M0 && "Span::g 须是 kGeos 之一");
  assert(s.t0_abs >= 0 && s.t_lo >= 0 && s.t_back >= 0 && s.t_lo < s.t_hi());
}
// prepped = ry 已由 prep_label 填 (评估入口); 假 = 只要两侧标签 + 掩码 (GPU 会话的宿主输入). seg = 因子所在层 (持有期按层校验)
inline void assert_labels(const Labels &L, int seg, bool prepped) {
  assert_holds(L.hd, seg);
  for (int i = 0; i < L.hd.n; ++i)
    assert(L.l[i].v[kLong] && L.l[i].v[kShort] && L.l[i].m && L.l[i].eo && (!prepped || L.l[i].ry) && "标签不全 / 未预处理 (prep_label)");
  assert(L.cap > 0.0 && "满仓 cap 未给 (Labels::cap = n_full)");
}

// ---- 一级输出: 每 (持有期, 时刻) 一行; 布局 rows[h_idx * T + t] ----
struct Row {
  float ric = 0.f, rac = 0.f;
  float mkt = 0.f;            // J 上做多标签原值均值 (超额基准)
  float grp[kGroups] = {};    // 组的超额持仓收益 Σe / max(cnt, cap)
  uint16_t cnt[kGroups] = {}; // 组真持有只数 (A ≤ kMaxA < 65536)
  uint8_t ok = 0, ok_rac = 0;
};
// 多空 = ½ (G20 − G1) (两本做多各半资金, 无杠杆; Row 当前读法下的两端)
FACTOR_HD inline float ls_row(const Row &r) { return 0.5f * (r.grp[end_group(kTop)] - r.grp[end_group(kBot)]); }
// 正向攒的行改读成方向 d (【方向 Dir】): 反向 = rIC 变号, 组号倒序
inline Row oriented(Row r, int d) {
  if (d == kFwd)
    return r;
  r.ric = -r.ric;
  for (int k = 0; k < kGroups / 2; ++k) {
    std::swap(r.grp[k], r.grp[kGroups - 1 - k]);
    std::swap(r.cnt[k], r.cnt[kGroups - 1 - k]);
  }
  return r;
}

// ---- 二级输出: 每持有期一份 ----
// 二级口径戳: 字段不变但口径变时 +1, 落盘 stat 与之不符读取即作废 (下次 Run 重算)
inline constexpr int kStatSemver = 4;
// 一个逐行量 (rIC / rAC / LS) 沿 t 的矩 (见文件头【二级 HoldStat】)
struct Moments {
  float mean = 0.f, sd = 0.f, ir = 0.f, t = 0.f, pos = 0.f, skew = 0.f, kurt = 0.f;
};
struct HoldStat {
  int seg = 0; // 因子所在层的每段行数 (finish 填; 不落文件, 载入时由因子目录的层给): 年化 / 换手折算用
  int hold = 0, n = 0, n_rac = 0;
  int n_all = 0, n_trig = 0; // 攒过的全部行 (真实收益的分母) / 触发行 (ok 且两端至少一端非空)
  Moments ric, rac, ls;      // 逐行量 (ric / ls: ok 行; rac: ok_rac 行)
  float beta = 0.f, mono = 0.f;
  float end_n = 0.f;       // 平均每触发行一端成员数 (两端只数的平均; util_of 的分子)
  float grp[kGroups] = {}; // 组每行持仓收益的均值 (Σ / n_all)
};
// 每年独立期数 = kDaysPerYear · S / h (一年 242 个交易日, 一天 S 行, h = hold_rows): 独立样本口径的年化 (Sharpe 的 √期数 / 换手) 走这一个
inline constexpr double periods_per_year(int hold, int seg) { return static_cast<double>(kDaysPerYear) * seg / hold_rows(hold, geo_of(seg)); }
inline double periods_per_year(const HoldStat &s) { return periods_per_year(s.hold, s.seg); }
inline double hold_span(int hold, const Geo &g);
// 每年满仓轮数 = kDaysPerYear · S / h̄ (h̄ = hold_span): 金额口径的年化 (均值超额 / 均摊成本 = 满仓一年) 走这一个
inline double span_per_year(int hold, int seg) { return static_cast<double>(kDaysPerYear) * seg / hold_span(hold, geo_of(seg)); }
inline double span_per_year(const HoldStat &s) { return span_per_year(s.hold, s.seg); }
// 真实收益 (见文件头【二级 HoldStat】): m = n 个 ok 行的矩, 其余 n_all − n 行记 0. 年化均值 = Σ / n_all × 每年期数 ppy
inline double ann_mean(const Moments &m, int n, int n_all, double ppy) { return n_all > 0 ? static_cast<double>(m.mean) * n / n_all * ppy : 0.0; }
// 补 0 后的年化 Sharpe: 由 (n, mean, sd) 还原 Σ = n·mean, Σ² = (n−1)·sd² + n·mean², 在 n_all 行上重算 mean / sd (ddof=1)
inline double ann_sharpe(const Moments &m, int n, int n_all, double ppy) {
  if (n < 3 || n_all < 2)
    return 0.0;
  const double nn = n, N = n_all, mu = m.mean, sd = m.sd;
  const double s1 = nn * mu, s2 = (nn - 1.0) * sd * sd + nn * mu * mu;
  const double M = s1 / N, v = (s2 - N * M * M) / (N - 1.0);
  return v > 0.0 ? M / std::sqrt(v) * std::sqrt(ppy) : 0.0;
}
// 净值几何年化: 净值 nav (起点 1) 历经 days 个交易日 → nav^(kDaysPerYear / days) − 1; nav ≤ 0 (亏光) → −1
inline double ann_geo(double nav, double days) {
  assert(days > 0.0);
  return nav > 0.0 ? std::pow(nav, static_cast<double>(kDaysPerYear) / days) - 1.0 : -1.0;
}
// LS 每期真实收益 / 年化 / 年化 Sharpe. 派生量, 不落 HoldStat / 文件
inline double ls_period(const HoldStat &s) { return s.n_all > 0 ? static_cast<double>(s.ls.mean) * s.n / s.n_all : 0.0; }
inline double ls_annual(const HoldStat &s) { return ann_mean(s.ls, s.n, s.n_all, span_per_year(s)); }
inline double sharpe_of(const HoldStat &s) { return ann_sharpe(s.ls, s.n, s.n_all, periods_per_year(s)); }
// 端 e 的每期真实收益 (超额; 与 ls_period 同分母 n_all, ls_period 即两端之差的一半)
inline double end_period(const HoldStat &s, int e) { return s.grp[end_group(e)]; }
// 触发率 = 触发行 / ok 行; n = 0 → 0 (诊断)
inline double trigger_of(const HoldStat &s) { return s.n > 0 ? static_cast<double>(s.n_trig) / s.n : 0.0; }
// 两端利用率 = 平均每触发行一端成员数 / 满仓 cap, 不封顶 (> 1 = 端组塞过满仓; 诊断)
inline double util_of(const HoldStat &s, double cap) {
  assert(cap > 0.0);
  return static_cast<double>(s.end_n) / cap;
}
// ---- 共享公式 (FACTOR_HD: 两后端同一份) ----

// float → 保序无符号键 (a < b ⟺ ord(a) < ord(b)); ±0 先归一为 +0
FACTOR_HD inline unsigned ord(float f) {
  const unsigned u = __builtin_bit_cast(unsigned, f + 0.f);
  return (u & 0x80000000u) ? ~u : (u | 0x80000000u);
}

// 并列均秩 r16: pct = (avg_rank − 1)/(n − 1), avg_rank = less + (eq + 1)/2, r16 = round(pct · kRankMax)
//   全整数: num = (2·less + eq − 1)·kRankMax, den = 2·(n − 1), r16 = ⌊(2·num + den) / (2·den)⌋
FACTOR_HD inline uint16_t r16_of(int less, int eq, int n) {
  assert(n >= 2 && eq >= 1 && less >= 0 && less + eq <= n);
  const long long num = (2LL * less + eq - 1) * kRankMax;
  const long long den = 2LL * (n - 1);
  return static_cast<uint16_t>((2 * num + den) / (2 * den));
}

// TS 口径: 分位 x ∈ [0,1] 直接量化 r16 = round(x · kRankMax). double 内 float × 65534 精确, 与 FMA / 求值序无关
FACTOR_HD inline uint16_t r16_quant(float x) {
  assert(x >= 0.f && x <= 1.f && "TS 口径的因子值须是分位 ∈ [0,1] (根 TsRankRoll)");
  return static_cast<uint16_t>(static_cast<double>(x) * kRankMax + 0.5);
}

// 组号: r16 等分 kGroups 组
FACTOR_HD inline int grp_of(unsigned r16) {
  const int g = static_cast<int>(r16 * kGroups / (kRankMax + 1));
  return g < kGroups - 1 ? g : kGroups - 1;
}

// 段末尾部 (见文件头【Row】段末尾部; 不开仓): 分钟型 exit 锚点越过最后可成交锚点 (持有期缩短) / 收盘型 entry 锚点越过 (收盘价建仓即平);
// 日级型无尾部. t = 绝对行号
FACTOR_HD inline bool tail_masked(int t, int hold, const Geo &g) {
  return hold <= kHoldClose && g.minute(t % g.S) + (hold_intraday(hold) ? hold : 0) > kLastAnchorRow;
}
// 行 t 按收盘价建仓: 行末分钟越过最后可成交锚点, 价点 E 退化为 C(D) (协议表 1; 日层每行都是). t = 绝对行号
FACTOR_HD inline bool close_entry(int t, const Geo &g) { return g.minute(t % g.S) > kLastAnchorRow; }
// 交接 (见文件头【换手成本】): 行 t 建仓与行 t − h 平仓是同一价点 (两批同刻对冲, 只交易差额). t = 绝对行号;
// 分钟型 = 行 t−h 的 exit 就是行 t 的 entry; T+N 收盘型 = 行 t 按收盘价建仓 (close_entry); 收盘型 / 开盘型从不
FACTOR_HD inline bool hold_nets(int t, int hold, const Geo &g) {
  const int h = hold_rows(hold, g);
  if (t < h || tail_masked(t, hold, g) || tail_masked(t - h, hold, g))
    return false;
  return hold_intraday(hold) || (hold_is_close_n(hold) && close_entry(t, g));
}

// ---- 仓位感知 (见文件头【仓位感知】; Row / 换手成本 / Inspect 尾部 / 池指数同一份) ----

// 交接链至多回走的交易日数 (【仓位感知】) = 平仓结清窗 (features LabelHold: PEND_DAYS − 1)
inline constexpr int kChainDays = 8;

// 行 t 的批沿交接链的几何: t = 块内行号, 交接按绝对行 t0_abs + u 判; 前一批越过 t 前 kChainDays 日 / 块外 (u − h < 0 / u ≥ T) 视为断链
struct Chain {
  int t = 0, T = 0, A = 0, h = 0, hold = 0, t0_abs = 0;
  Geo g;
  FACTOR_HD bool nets(int u) const { // 行 u 的批与行 u − h 的批交接
    return u >= h && u < T && t - (u - h) <= kChainDays * g.S && hold_nets(t0_abs + u, hold, g);
  }
};
FACTOR_HD inline Chain chain_of(int t, const Span &s, int hold) { return {t, s.T, s.A, hold_rows(hold, s.g), hold, s.t0_abs, s.g}; }

// 格在行 t 的批里是否真持有: 行 t 价点可买 → 是; 否则须与前一批交接且前一批同一组合 (m ∧ in(code)) 并真持有, 沿链回走.
//   code / m / eo 已偏到行 t 的本格 (行 u 的格 = p[(u − t)·A]); 调用方保证本格在行 t 是成员 (m ∧ in)
template <class Code, class In>
FACTOR_HD inline bool held(const Chain &c, const Code *code, const uint8_t *m, const uint8_t *eo, const In &in) {
  for (int u = c.t;; u -= c.h) {
    const ptrdiff_t o = static_cast<ptrdiff_t>(u - c.t) * c.A;
    if (eo[o] & kEoBuy)
      return true;
    if (!c.nets(u))
      return false;
    const ptrdiff_t p = o - static_cast<ptrdiff_t>(c.h) * c.A;
    if (!m[p] || !in(code[p]))
      return false;
  }
}
// 行 t 的批平仓时, 下一批 (t + h) 交接且同一组合接着持有 → 不真平仓 (持有者无需成交)
template <class Code, class In>
FACTOR_HD inline bool continues(const Chain &c, const Code *code, const uint8_t *m, const In &in) {
  if (!c.nets(c.t + c.h))
    return false;
  const ptrdiff_t o = static_cast<ptrdiff_t>(c.h) * c.A;
  return m[o] && in(code[o]);
}
// 做多一格的收益: 续持 → 名义价点估值 (下一批价点可卖 → lv 就是名义值; 否则可买 (价点不可能两向都堵), 名义值 = −sv);
// 不续持 → lv (含平仓顺延). eo_next = 下一批 (t + h) 的可成交码
FACTOR_HD inline float hold_ret(bool cont, uint8_t eo_next, const float *y) {
  return !cont || (eo_next & kEoSell) ? y[kLong] : -y[kShort];
}
// 一格的交接链视图 (Code = 成员码平面: rx 的 r16 / 端码 ec / 门控 …; in(code) = 是否同一组合, 由调用方给)
template <class Code>
struct Cell {
  const Chain &c;
  const Code *code;      // 已偏到行 t 的本格
  const uint8_t *m, *eo; // 同上
  const float *y;        // 本格两侧标签 [kLegs]
  template <class In>
  FACTOR_HD bool held(const In &in) const { return stat::held(c, code, m, eo, in); }
  template <class In>
  FACTOR_HD bool continues(const In &in) const { return stat::continues(c, code, m, in); }
  FACTOR_HD float ret(bool cont) const { return hold_ret(cont, cont ? eo[static_cast<ptrdiff_t>(c.h) * c.A] : 0, y); }
};
// 平仓行 (见文件头【净值】): 行 t 这一批的收益在哪一行实现. t = 绝对行号; 分钟型 = t + h; 收盘型 = 当日末行;
// T+N 开盘型 = D+N 日首行; T+N 收盘型 = D+N 日末行 (日层 S = 1: 两者都是 t + N). 顺延不改实现行 (按名义平仓日记)
inline int hold_exit_row(int t, int hold, const Geo &g) {
  if (hold_intraday(hold))
    return t + hold_rows(hold, g);
  const int d = t / g.S + hold_days(hold);
  return hold_is_open(hold) ? d * g.S : d * g.S + g.S - 1;
}
// 资金口径行数 h̄ (见文件头【持有期键】): 一日各开仓行的在持行数 (建仓行起, hold_exit_row 止; 尾部行不开仓不计) 之和 / S
inline double hold_span(int hold, const Geo &g) {
  int s = 0;
  for (int j = 0; j < g.S; ++j)
    s += tail_masked(j, hold, g) ? 0 : hold_exit_row(j, hold, g) - j;
  assert(s > 0);
  return static_cast<double>(s) / g.S;
}

// ---- 换手成本 (见文件头【换手成本】; Run (stat.costs) / Inspect 期限结构净线 / Portfolio 逐行成本共用) ----

// 一个金额段: 盘口段 = 买入冲击 [T][A] 平面的指针 (宿主或设备, 由后端定; 可买 (eo) 的格恒有限, 装载层断言); 固定段 = 空
struct Cost {
  const uint16_t *imp = nullptr; // 买入冲击 fp16 位
  FACTOR_HD bool flat() const { return !imp; }
  FACTOR_HD Cost at(size_t i) const { return flat() ? Cost{} : Cost{imp + i}; } // 偏到第 i 格 (行首 = t·A)
};

// 端码 (两后端 / Inspect 同一份): r16 → 方向 d 的高端组 kEcTop / 低端组 kEcBot / 其他组 kEcMid / rank 无效 kEcVoid. 前两个与 End 同值:
// 端成员 = ec ∈ {kTop, kBot} ∧ 该持有期标签有效位 (cost_row 逐格比本批 / 交接前一批的端成员权)
enum EndCode : uint8_t { kEcTop = kTop,
                         kEcBot = kBot,
                         kEcMid,
                         kEcVoid };
FACTOR_HD inline uint8_t end_code(unsigned x, int d) {
  const int g = x != kRankNone ? grp_of(x) : -1;
  return g < 0 ? kEcVoid : g == end_group(kTop ^ d) ? kEcTop
                       : g == end_group(kBot ^ d)   ? kEcBot
                                                    : kEcMid;
}

// 一本做多账本的换手成本和 (沿行求和; 量按【持仓口径】的权, 每批资金 1)
struct CostBook {
  double imp = 0.0;  // Σ Δw⁺ · 买入冲击
  double buy = 0.0;  // Σ Δw⁺ (增仓量)
  double sell = 0.0; // Σ 平仓量 (交接的 Δw⁻ + 不交接批的整批平仓)
  double fix = 0.0;  // Σ 付固定冲击的量 (盘口段 = 平仓量; 固定段 = 增仓量 + 平仓量)
  double w = 0.0;    // Σ w (持仓量)
  long long n = 0;   // 样本数 (成员格数)
};
// 一个持有期的换手成本累加器 (两端各一本); 可合并
struct CostAcc {
  CostBook book[kEnds];
  long long rows = 0; // 攒过的行数 (非尾部行各计一次)
  FACTOR_HD void merge(const CostAcc &o) {
    for (int e = 0; e < kEnds; ++e) {
      book[e].imp += o.book[e].imp, book[e].buy += o.book[e].buy, book[e].sell += o.book[e].sell, book[e].fix += o.book[e].fix;
      book[e].w += o.book[e].w, book[e].n += o.book[e].n;
    }
    rows += o.rows;
  }
  // 每期真实成本 (端 e): (Σ Δw⁺ · 买入冲击 + Σ 固定量 · 固定冲击) / n_all (与 ls_period 同分母)
  double cost(int e, double fixed_impact, int n_all) const {
    const CostBook &c = book[e];
    return n_all > 0 ? (c.imp + c.fix * fixed_impact) / static_cast<double>(n_all) : 0.0;
  }
  // 增仓的平均开仓冲击 (显示用): 盘口段 = 买入冲击均值; 固定段 = fixed_impact (fix − sell = 付固定冲击的增仓量)
  double entry(int e, double fixed_impact) const {
    const CostBook &c = book[e];
    return c.buy > 0.0 ? (c.imp + (c.fix - c.sell) * fixed_impact) / c.buy : 0.0;
  }
};

// 每期净多空 = ls_period − 两端每期成本的平均 (两本各半资金, 同 ls); 年化 = × span_per_year (s.hold 由 finish 填)
inline double net_ls_period(const HoldStat &s, const CostAcc &c, double fixed_impact) {
  return ls_period(s) - 0.5 * (c.cost(kTop, fixed_impact, s.n_all) + c.cost(kBot, fixed_impact, s.n_all));
}
inline double net_ls(const HoldStat &s, const CostAcc &c, double fixed_impact) { return net_ls_period(s, c, fixed_impact) * span_per_year(s); }
// 年化换手 (倍/年, 单边, 每本资金 1; 与成本同一份 Δw): 端 e = Σ Δw⁺ / n_all × span_per_year; 两端 = 两端均. 越低越好
inline double turn_annual(const HoldStat &s, const CostAcc &c, int e) {
  return s.n_all > 0 ? c.book[e].buy / static_cast<double>(s.n_all) * span_per_year(s) : 0.0;
}
inline double turn_annual(const HoldStat &s, const CostAcc &c) { return 0.5 * (turn_annual(s, c, kTop) + turn_annual(s, c, kBot)); }

// 整数五和 + 计数 (rIC / rAC 的 Pearson 原料), 下标 s[kSn..kSxy]; 两后端同一份累加 (r16 ≤ 65534: 积 < 2³², 32 位乘后再加进 64 位)
enum : int { kSn,
             kSx,
             kSy,
             kSxx,
             kSyy,
             kSxy,
             kSums };
FACTOR_HD inline void pair_add(unsigned long long *s, unsigned x, unsigned y, bool ok) {
  s[kSn] += ok;
  s[kSx] += ok ? x : 0u;
  s[kSy] += ok ? y : 0u;
  s[kSxx] += ok ? x * x : 0u;
  s[kSyy] += ok ? y * y : 0u;
  s[kSxy] += ok ? x * y : 0u;
}

// 整数五和的 Pearson: vx = n·Σx² − (Σx)² 等全在 64 位整数内精确, 只有最后一步除法/开方是浮点 (IEEE 正确舍入,
// 两后端逐位一致). ok = n ≥ 2 ∧ vx > 0 ∧ vy > 0
FACTOR_HD inline float pearson_int(const unsigned long long *s, bool &ok) {
  const unsigned long long n = s[kSn];
  if (n < 2) {
    ok = false;
    return 0.f;
  }
  const long long vx = static_cast<long long>(n * s[kSxx]) - static_cast<long long>(s[kSx] * s[kSx]);
  const long long vy = static_cast<long long>(n * s[kSyy]) - static_cast<long long>(s[kSy] * s[kSy]);
  const long long cxy = static_cast<long long>(n * s[kSxy]) - static_cast<long long>(s[kSx] * s[kSy]);
  ok = vx > 0 && vy > 0;
  return ok ? static_cast<float>(static_cast<double>(cxy) / std::sqrt(static_cast<double>(vx) * static_cast<double>(vy))) : 0.f;
}

// 持仓分母 (【持仓口径】) = max(只数, cap): 收益 = Σe / book_den, 不足满仓按只数 / cap 建仓, 超过满仓均分
FACTOR_HD inline double book_den(double n, double cap) { return n > cap ? n : cap; }
// 一本的超额持仓收益 (组 / Inspect 尾部同式) = Σ(lv − 基准) / max(只数, cap) = (Σlv − 只数 · 基准) / 分母; 空 → 0
FACTOR_HD inline double book_excess(double sum, double n, double base, double cap) { return (sum - n * base) / book_den(n, cap); }
// 超额持仓收益 → 绝对持仓收益: excess + 只数 · 基准 / max(只数, cap)
inline double book_abs(double excess, double n, double base, double cap) { return excess + n * base / book_den(n, cap); }

// 一行标签侧的收尾 (两后端同一份): J 上整数五和 s → ric; 组和 / 组计数 → grp (持仓收益) / cnt; 基准 → mkt.
//   sm = J 上 Σlv; gs[k] / gc[k] = 组 k 真持有的 Σ 收益 / 只数; cap = 满仓. 不 ok → r 标签侧字段保持 0
FACTOR_HD inline void finish_row(const unsigned long long *s, double sm, const double *gs, const int *gc, double cap, Row &r) {
  bool icok = false;
  const float ric = pearson_int(s, icok);
  if (!icok)
    return;
  const double mkt = sm / static_cast<double>(s[kSn]);
  r.ok = 1;
  r.ric = ric;
  r.mkt = static_cast<float>(mkt);
  for (int k = 0; k < kGroups; ++k) {
    r.cnt[k] = static_cast<uint16_t>(gc[k]);
    r.grp[k] = static_cast<float>(book_excess(gs[k], gc[k], mkt, cap));
  }
}

// ---- 二级汇总 (主机, double; 两后端共用) ----

// 组号 0..K−1 与 g[k] 的 Spearman (g 并列均秩); g 全并列 → 0
inline double mono_of(const double *g, int K) {
  double rk[kGroups];
  for (int i = 0; i < K; ++i) {
    int less = 0, eq = 0;
    for (int j = 0; j < K; ++j) {
      less += g[j] < g[i];
      eq += g[j] == g[i];
    }
    rk[i] = less + (eq + 1) * 0.5; // 1-based 均秩
  }
  const double mu = (K + 1) * 0.5; // 0..K−1 的秩均值 (1-based) = 组号均值 + 1
  double cxy = 0.0, vx = 0.0, vy = 0.0;
  for (int i = 0; i < K; ++i) {
    const double dx = (i + 1) - mu, dy = rk[i] - mu;
    cxy += dx * dy;
    vx += dx * dx;
    vy += dy * dy;
  }
  return vy > 0.0 ? cxy / std::sqrt(vx * vy) : 0.0;
}

// 二级累加器: 一遍消费 Row, 可合并 (t 切块并行各攒一份再 merge; 沿 T 分块流式跨块续攒), 不留 Row.
//   幂和 double, 原点 0: rIC / rAC ∈ [−1,1], 标签收益 ~1e-3 —— 相消损失 ≈ (μ/σ)^k 量级 (< 1e-2 相对), 远在 float 出口精度内;
//   中心矩相消出负由 fmax 钳 0. 两后端共用同一份 (对拍一致性在 Row 级); add / merge 是 FACTOR_HD, 设备侧也原样攒.

// 一个逐行量 (rIC / rAC / LS 同一份) 的幂和 → Moments; n < 3 → 全 0
struct MomAcc {
  int n = 0, npos = 0, nneg = 0; // 行数 / > 0 行数 / < 0 行数
  double s1 = 0.0, s2 = 0.0, s3 = 0.0, s4 = 0.0;
  FACTOR_HD void add(float v) {
    ++n;
    npos += v > 0.f, nneg += v < 0.f;
    const double x = v, x2 = x * x;
    s1 += x, s2 += x2, s3 += x2 * x, s4 += x2 * x2;
  }
  FACTOR_HD void merge(const MomAcc &o) {
    n += o.n, npos += o.npos, nneg += o.nneg;
    s1 += o.s1, s2 += o.s2, s3 += o.s3, s4 += o.s4;
  }
  // 逐行量取负后的幂和 (奇次变号, 正负计数互换): 与逐行 add(−v) 逐位同
  MomAcc negated() const {
    MomAcc m = *this;
    m.npos = nneg, m.nneg = npos;
    m.s1 = -s1, m.s3 = -s3;
    return m;
  }
  // h = 持有期折成的行数 (hold_rows; t 值的重叠折算 n/h)
  Moments finish(int h) const {
    Moments m;
    if (n < 3)
      return m;
    const double nn = n, mu = s1 / nn;
    const double m2 = std::fmax(s2 - nn * mu * mu, 0.0); // 中心矩
    const double m3 = s3 - 3.0 * mu * s2 + 2.0 * nn * mu * mu * mu;
    const double m4 = std::fmax(s4 - 4.0 * mu * s3 + 6.0 * mu * mu * s2 - 3.0 * nn * mu * mu * mu * mu, 0.0);
    const double sd = std::sqrt(m2 / (nn - 1));
    m.mean = static_cast<float>(mu);
    m.sd = static_cast<float>(sd);
    m.pos = static_cast<float>(npos / nn);
    if (sd > 0.0) {
      m.ir = static_cast<float>(mu / sd);
      m.t = static_cast<float>(mu / sd * std::sqrt(nn / h));
      const double v = m2 / nn;
      m.skew = static_cast<float>((m3 / nn) / (v * std::sqrt(v)));
      m.kurt = static_cast<float>((m4 / nn) / (v * v) - 3.0);
    }
    return m;
  }
};

struct Accum {
  // 标量头: 逐行量的矩 + beta 的幂和 + 行计数 (GPU 上 lane 0 攒)
  struct Head {
    MomAcc ric, rac, ls;                     // ric.n = ls.n = ok 行数 (标签侧全部统计的样本), rac.n = ok_rac 行数
    double smk = 0.0, smk2 = 0.0, slm = 0.0; // mkt 幂和 + ls·mkt 交叉 (beta)
    int n_all = 0, n_trig = 0;               // 全部行 / 触发行 (ok 且两端至少一端非空)
    FACTOR_HD void add(const Row &w) {
      ++n_all;
      if (w.ok) {
        ric.add(w.ric);
        const double mk = w.mkt;
        smk += mk, smk2 += mk * mk;
        n_trig += w.cnt[end_group(kTop)] + w.cnt[end_group(kBot)] > 0;
        const float v = ls_row(w);
        ls.add(v);
        slm += static_cast<double>(v) * mk;
      }
      if (w.ok_rac)
        rac.add(w.rac);
    }
    FACTOR_HD void merge(const Head &o) {
      ric.merge(o.ric), rac.merge(o.rac), ls.merge(o.ls);
      smk += o.smk, smk2 += o.smk2, slm += o.slm;
      n_all += o.n_all, n_trig += o.n_trig;
    }
  };
  // 一个组: 组持仓收益 / 组样本数 沿 t 的和 (GPU 上 lane k 攒组 k; 只收 ok 行, 由调用方判)
  struct Grp {
    double s = 0.0;
    long long c = 0;
    FACTOR_HD void add(float g, unsigned n) { s += g, c += n; }
    FACTOR_HD void merge(const Grp &o) { s += o.s, c += o.c; }
  };
  Head head;
  Grp grp[kGroups];

  FACTOR_HD void add(const Row &w) {
    head.add(w);
    if (w.ok)
      for (int k = 0; k < kGroups; ++k)
        grp[k].add(w.grp[k], w.cnt[k]);
  }
  FACTOR_HD void merge(const Accum &o) {
    head.merge(o.head);
    for (int k = 0; k < kGroups; ++k)
      grp[k].merge(o.grp[k]);
  }
  // hold = 持有期键, seg = 因子所在层的每段行数 (折算 / 年化); n < 3 → 标签侧全 0, rac 照算 (看 n_rac)
  HoldStat finish(int hold, int seg) const {
    assert(hold >= 1);
    const int h = hold_rows(hold, geo_of(seg));
    assert(h >= 1);
    HoldStat s;
    s.seg = seg;
    s.hold = hold;
    s.n = head.ric.n;
    s.n_rac = head.rac.n;
    s.n_all = head.n_all;
    s.n_trig = head.n_trig;
    s.ric = head.ric.finish(h);
    s.rac = head.rac.finish(h);
    s.ls = head.ls.finish(h);
    if (s.n < 3)
      return s;
    double g[kGroups];
    for (int k = 0; k < kGroups; ++k)
      g[k] = grp[k].s / static_cast<double>(s.n_all); // 不 ok 行记 0 (n_all ≥ n ≥ 3)
    const double nn = s.n, mmk = head.smk / nn;
    const double vmk = std::fmax(head.smk2 - nn * mmk * mmk, 0.0);
    if (vmk > 0.0)
      s.beta = static_cast<float>((head.slm - head.ls.s1 * mmk) / vmk);
    for (int k = 0; k < kGroups; ++k)
      s.grp[k] = static_cast<float>(g[k]);
    s.mono = static_cast<float>(mono_of(g, kGroups));
    const long long ne = grp[end_group(kTop)].c + grp[end_group(kBot)].c;
    s.end_n = s.n_trig > 0 ? static_cast<float>(static_cast<double>(ne) / (2.0 * s.n_trig)) : 0.f;
    return s;
  }
};
// 正向攒的累加器改读成方向 d (与逐行 oriented(Row, d) 再攒逐位同): 反向 = rIC / LS 幂和取负, 组倒序
inline Accum oriented(Accum a, int d) {
  if (d == kFwd)
    return a;
  Accum::Head &h = a.head;
  h.ric = h.ric.negated();
  h.ls = h.ls.negated();
  h.slm = -h.slm;
  for (int k = 0; k < kGroups / 2; ++k)
    std::swap(a.grp[k], a.grp[kGroups - 1 - k]);
  return a;
}

// 流程出口 Accum[hd.n] → HoldStat[hd.n] (两后端同一份; 见文件头【视图】)
inline void finish_all(const Accum *acc, const Holds &hd, int seg, HoldStat *out) {
  assert(acc && out);
  for (int i = 0; i < hd.n; ++i)
    out[i] = acc[i].finish(hd.k[i], seg);
}

} // namespace factor::stat
