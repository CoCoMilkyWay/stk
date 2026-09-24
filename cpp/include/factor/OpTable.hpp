#pragma once

// =============================================================================
// 因子算子真相表 (X-macro; 语义契约见 factor/Contract.hpp; 成本与误差见 operator.md)
// =============================================================================
//   命名 = 域_核_窗 (代码里 PascalCase, 文档里 snake_case, 一一对应):
//     域   Ts (A 域 = SELF) / Cs (A 域 = ALL | GROUP) —— 前缀只是 A 域的缩写, 不是独立分类
//     核   统计量本身 (Mean / Var / Rank / Corr …), 与窗正交
//     窗   Cum 段内 expanding / Roll 最近 d 期 / Ema 指数递推; 无窗后缀 = 逐点
//
//   【行序 = 全局 idx】全库唯一算子序, code 与 UI 统一按它 (operators.json 的 idx /
//   op_check 输出序 / GUI Operators 表默认序), 序键 (枚举序见 Contract.hpp):
//     元数 → A 域 (Ts 前 Cs 后) → TS 按 T 窗 / CS 按 ALL → GROUP → 核类 → 名字字母序
//   表按 (元数 × 域) 切成 OP_TS0..3 / OP_CS1..3 七块, 块内按上述序键排;
//   OP_ALL 交错拼出 idx 序, OP_TS / OP_CS 各拼本域 (供 GpuRun 的 run_ts / run_cs 按域分派).
//   加算子按序键插行, idx 顺延, 不另设编号.
//
//   行格式统一, 三个分类列 = 三个正交维度 (语义与类型约束见 Contract.hpp【分类】):
//
//   X(e_name, "c_name", 元数, T, A, 核类, k域, in, out, R"tex(operator)tex", "备注")
//   (列名 = OperatorRow 字段名 = operators.json 键名 = GUI 表头, 全库统一叫法)
//     c_name 与 e_name 逐段对应的固定规则 (无冗余字): 域 时(Ts) / 截(Cs·ALL) / 组(Cs·GROUP)
//            + 窗 累(Cum) / 滚(Roll) / 指(Ema) (逐点无窗字) + 核 (标准术语: 均值 / 方差 / 秩 / 贝塔 …)
//     元数   输入序列数 0..3 (窗长 d / 阈值 k 不算元)
//     T窗    POINT  当前点 (无时间状态)
//            EXPAND 段内 expanding (段 = 交易日 kSegLen 分钟, 段界 reset)
//            ROLL   最近 d 期滚动 (期 = 分钟, 跨段不 reset)
//            EXPO   指数加权全历史 (系数 k, 全程不 reset)
//     A域    SELF 只看本资产 / ALL 同一时刻全截面 / GROUP 同一时刻组内 (整数组 id 由 y 或 z 给)
//     核类   MAP 逐元素 / SHIFT 下标平移 / MOMENT 可和分解 (矩族) / EXTREME 极值及 arg 族 /
//            ORDER 序统计 / RECUR 递推; 复合算子标主导 (最重) 一级
//
//   参数 (Contract.hpp struct Param) 不单列, 由 (T, k域) 推出 (params_str): d ⇔ T = ROLL (窗长 / 滞后, 期 = 分钟);
//     k ⇔ k域 ≠ NONE (阈值 / 桶数 / EMA 系数 / 分位, 含义见各行 operator); k2 ⇔ k域 = TOD (TodMask 上界)
//   k域 列 = k 的值域 (enum KDom): NONE / ANY ℝ / GE0 ≥0 / OPEN01 (0,1) / OPEN0_CLOSED1 (0,1] / OPEN0_HALF (0,1/2) /
//     POSINT_GROUP 正整数 ≤ kMaxGroup / TOD 0 ≤ k < k2 ≤ kSegLen 整数;  parse 按它校验参数值
//
//   in / out 列 = 自变量 / 因变量值域 (同一 enum Dom, Contract.hpp; 全机器可读, 表里不手写值域 LaTeX):
//     REAL ℝ / NONNEG ℝ≥0 / POS ℝ>0 / UNIT [0,1] / SIGNED [−1,1] / BIN {0,1} / SIGN3 {−1,0,1} / INT [0,kMaxGroup) / BCAST 截面广播
//     in  按元数写 OP_IN0..3(逐元 Dom), GROUP 域的组 id 元写 INT; 只写能静态判的约束 (y ≠ 0 / x > −1 判不了 → REAL)
//     out 只按算子自身声明: 透传 (Mask / Where / Delay) 与取大取小写 REAL, 不随输入推导; 计数 (ArgMax 期数) 写 NONNEG
//     parser 逐元查: in 为严格域 (INT) 时 子.out ⊆ in (dom_sub), 特征叶到 eval 前按数据查; 其余 in 是语义声明 (越界格算子自置无效)
//     因子根 (归一算子) 的输入不许离散 (BIN / SIGN3 / INT) / 不许被 BCAST 抹平 (Expr.hpp root_frame)
//     签名 LaTeX (GUI operand 列 / operators.json) 由 Expr.hpp operand_tex 从 (元数, in, T, k域) 生成:
//       x, y, z ∈ dom 在前, 参数 名{=}⟨名⟩ ∈ 值域 在后, ⟨d⟩ ⟨k⟩ ⟨k2⟩ 占位符 GUI 渲染前换成本轮实际值
//
//   备注列 = 用途与选型 (给 agent 检索的一句话, 统一 "量什么; 怎么用 / 配什么"):
//     不写实现 / 近似 / 退化数值细节 (那些在 Contract.hpp 与 operator.md)
//
//   【operator 写法】一律单行 (operand 列同; GUI 表格内联渲染, 行高须齐): 不用 \frac (除法写 X / Y 加括号)、不用大号
//   \sum \prod \sqrt \lfloor (求和写 \Sigma_下标, 乘积 \Pi_下标, 开方写 ^{1/2}, 取整写 \mathrm{floor}),
//   归约算子一律 \mathrm{名}_下标 (\mathrm 不是 mathop, 下标必落右下, 不会顶到符号正下方把行撑高).
//
//   【operator 符号】继承 features/FeaturesDefine.hpp 的规范 (t = 分钟, D = 交易日, 1[·] 指示),
//   算子库专用补充 (GUI Factors→Operators 页按 LaTeX 渲染):
//     x, y, z     输入序列, 按元数取前 1..3 个; 每格 Val{v, m}, 所有 ∑ / 计数 / 极值只计有效 (m = 1) 样本
//     a, b        资产 (截面轴). TS 只看本资产, 省略 a; CS 只看同一 t 的截面, 写 x_a 省略 t
//     t_D         段内分钟位置 = t − 当日首分钟 (0..kSegLen−1)
//     W_t         TS 窗 (由 T 列决定, operator 里统一写 W_t):
//                   EXPAND  W_t = {s : D(s) = D(t), s ≤ t}   ROLL  W_t = {s : t−d < s ≤ t}
//     Σ_{W_t}     窗内求和 ∑_{s∈W_t} (下标只写窗 / 组, 求和变量恒是 s 或 b); Π_{W_t} 同理为乘积
//     n           W_t 内有效样本数 (二元: x, y 同格同时有效); N  截面有效资产数
//     μ_t, σ_t    W_t 内均值 / 样本标准差 (ddof=1); 二元加上标 μ^x, μ^y; 截面去掉下标 t
//     m_k         W_t 内 k 阶中心总体矩 (1/n)·Σ_{W_t}(x_s − μ_t)^k
//     max, min, cov, var, corr, pct, floor, clamp  带下标者下标 = 取值域 (窗 W_t / 截面 a / 组 G(a))
//     pct(v; S)   v 在样本集 S 的并列均秩 pct rank ∈ [0, 1] (Contract pct_of); 省略 S = 全截面
//     Q_p(S)      样本集 S 的 p 分位 (省略 S = 全截面);  Φ⁻¹  标准正态分位;  G(a)  a 所在组 (整数 id 由 y 或 z 给)
//     序统计族 (rank / quantile / winsor / bucket) 三后端同用 kBuckets 桶近似, 桶计数整数, 对拍逐位
//
//   【核 × 窗 为什么不是满格】格子空不空按"这个统计量在这个时间尺度上有没有意义"定, 不为对称而补:
//     Cum (段内 = 当日) 专收**日内分布形状**: Hhi / Entropy 只有 Cum —— 度量"当日成交怎么摊在各分钟上",
//       跨日滚动窗算集中度没有对应的研报语义.
//     Roll (最近 d 分钟, 跨段) 专收**时序动态**: Quantile / Z / Wma / Product / Slope 只有 Roll ——
//       分位 / 标准分 / 衰减权 / 累乘收益 / 趋势斜率都要求窗长固定, 段内 expanding 的窗长随 t_D 变,
//       出来的值前半段和后半段不可比 (自带一条 t_D 的伪趋势).
//     Delay / Delta 是 SHIFT 核, 只看一格, 天生只有 Roll; MeanEma 是 RECUR 核, 与 EXPO 窗一一绑定.
//     其余归约核 (Sum / Mean / Var / Std / Skew / Kurt / Max / Min / ArgMax / ArgMin / Rank /
//     Cov / Corr / Beta / Resid / WMean) Cum 与 Roll **成对齐全**, 缺一即为 bug.
//
//   【不入表的组合】能由现有行拼出来的一律不给独立算子 (表只收不可约的核):
//     计数   Σ1[x > k]     = TsGt + TsSum{Cum,Roll};  占比同理换 TsMean{Cum,Roll}
//     中位   med           = TsQuantileRoll(k=0.5) / CsQuantile(k=0.5)
//     子集统计  只在某时段/某条件上统计 = TsMask(x, 掩码) 再套窗 (掩码由 TsTodMask / TsGt 造)
//     超前相关  corr(x_s, y_{s+k}) = TsCorr{Cum,Roll}(TsDelayRoll(x, k), y) —— 换边即换符号方向
//     TsYoY    = TsDelayRoll + TsDiv, 且 d 需一年分钟数, 超出块 carry 上限
//
//   加算子 = 本表一行 + TS(或 CS) 的 Stream / Cpu / Gpu 各一个同名 struct; 缺任一侧 → op_check 编译错;
//   流式 TS struct::kT 与本表 T 窗列不符 → op_check static_assert 错 (A 域/核类是纯语义列, 无实现侧载荷).
// =============================================================================

// ---- 0 元 × SELF (1) ----
#define OP_TS0(X) \
  X(TsTodMask, "时段掩", 0, POINT, SELF, MAP, TOD, OP_IN0(), BIN, R"tex(\mathbf{1}[k \le t_D < k_2])tex", "选日内时段 (0 = 09:15 竞价首分钟, 15 = 09:30 开盘); 造 0/1 掩码, 配 TsMask 做分时段统计")

// ---- 1 元 × SELF (40 = POINT 8 + EXPAND 13 + ROLL 18 + EXPO 1) ----
#define OP_TS1(X)                                                                                                                                                                                                          \
  X(TsAbs, "时绝对值", 1, POINT, SELF, MAP, NONE, OP_IN1(REAL), NONNEG, R"tex(|x_t|)tex", "取幅度, 弃方向")                                                                                                                \
  X(TsClip, "时截断", 1, POINT, SELF, MAP, GE0, OP_IN1(REAL), REAL, R"tex(\operatorname{clamp}(x_t,\,-k,\,k))tex", "对称截到 ±k; 压极端值")                                                                                \
  X(TsGt, "时大于", 1, POINT, SELF, MAP, ANY, OP_IN1(REAL), BIN, R"tex(\mathbf{1}[x_t > k])tex", "过阈指示 0/1; 套 Sum 窗得计数, 套 Mean 窗得占比")                                                                        \
  X(TsLog, "时对数", 1, POINT, SELF, MAP, NONE, OP_IN1(REAL), REAL, R"tex(\operatorname{sign}(x_t)\,\ln(1+|x_t|))tex", "保号对数压缩; 压长尾")                                                                             \
  X(TsRecip, "时倒数", 1, POINT, SELF, MAP, NONE, OP_IN1(REAL), REAL, R"tex(1/x_t)tex", "取倒数; 比率换向")                                                                                                                \
  X(TsRelu, "时正部", 1, POINT, SELF, MAP, NONE, OP_IN1(REAL), NONNEG, R"tex(\max(0,\,x_t))tex", "取正部; 只留上行")                                                                                                       \
  X(TsSign, "时符号", 1, POINT, SELF, MAP, NONE, OP_IN1(REAL), SIGN3, R"tex(\operatorname{sign}(x_t))tex", "取方向, 弃幅度")                                                                                               \
  X(TsSqrt, "时开方", 1, POINT, SELF, MAP, NONE, OP_IN1(REAL), REAL, R"tex(\operatorname{sign}(x_t)\,|x_t|^{1/2})tex", "保号开方压缩; 比 Log 温和")                                                                        \
  X(TsEntropyCum, "时累熵", 1, EXPAND, SELF, MOMENT, NONE, OP_IN1(POS), NONNEG, R"tex(\ln(\Sigma_{W_t}\,x_s) - (\Sigma_{W_t}\,x_s \ln x_s) \;/\; (\Sigma_{W_t}\,x_s))tex", "当日至今的分布熵; 量摊匀程度, 高 = 均匀")      \
  X(TsHhiCum, "时累集中度", 1, EXPAND, SELF, MOMENT, NONE, OP_IN1(NONNEG), UNIT, R"tex(\Sigma_{W_t}\,x_s^2 \;/\; (\Sigma_{W_t}\,x_s)^2)tex", "当日至今的集中度; 量扎堆程度, 高 = 集中")                                    \
  X(TsKurtCum, "时累峰度", 1, EXPAND, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(m_4 / m_2^{2} - 3)tex", "当日至今的峰度; 量尖峰厚尾")                                                                                  \
  X(TsMeanCum, "时累均值", 1, EXPAND, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(\mu_t = (1/n)\,\Sigma_{W_t}\,x_s)tex", "当日至今的均值; 日内基线")                                                                     \
  X(TsSkewCum, "时累偏度", 1, EXPAND, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(m_3 / m_2^{3/2})tex", "当日至今的偏度; 量不对称")                                                                                      \
  X(TsStdCum, "时累标差", 1, EXPAND, SELF, MOMENT, NONE, OP_IN1(REAL), NONNEG, R"tex(\sigma_t = (\sigma_t^2)^{1/2})tex", "当日至今的标准差; 量离散")                                                                       \
  X(TsSumCum, "时累和", 1, EXPAND, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(\Sigma_{W_t}\,x_s)tex", "当日至今的和; 日内总量")                                                                                         \
  X(TsVarCum, "时累方差", 1, EXPAND, SELF, MOMENT, NONE, OP_IN1(REAL), NONNEG, R"tex(\sigma_t^2 = \Sigma_{W_t}(x_s - \mu_t)^2 / (n-1))tex", "当日至今的方差; 量离散")                                                      \
  X(TsArgMaxCum, "时累最大位", 1, EXPAND, SELF, EXTREME, NONE, OP_IN1(REAL), NONNEG, R"tex(t - \mathrm{min}\{s \in W_t : x_s = \mathrm{max}_{W_t}\,x\})tex", "当日最高点距今期数; 量高点新旧")                             \
  X(TsArgMinCum, "时累最小位", 1, EXPAND, SELF, EXTREME, NONE, OP_IN1(REAL), NONNEG, R"tex(t - \mathrm{min}\{s \in W_t : x_s = \mathrm{min}_{W_t}\,x\})tex", "当日最低点距今期数; 量低点新旧")                             \
  X(TsMaxCum, "时累最大", 1, EXPAND, SELF, EXTREME, NONE, OP_IN1(REAL), REAL, R"tex(\mathrm{max}_{W_t}\,x_s)tex", "当日至今的最高; 日内高点")                                                                              \
  X(TsMinCum, "时累最小", 1, EXPAND, SELF, EXTREME, NONE, OP_IN1(REAL), REAL, R"tex(\mathrm{min}_{W_t}\,x_s)tex", "当日至今的最低; 日内低点")                                                                              \
  X(TsRankCum, "时累秩", 1, EXPAND, SELF, ORDER, NONE, OP_IN1(REAL), UNIT, R"tex(\mathrm{pct}(x_t;\,\{x_s : s \in W_t\}))tex", "本值在当日至今的分位; 日内相对位置")                                                       \
  X(TsDelayRoll, "时滚滞后", 1, ROLL, SELF, SHIFT, NONE, OP_IN1(REAL), REAL, R"tex(x_{t-d})tex", "取 d 期前的值; 造滞后项配二元算子")                                                                                      \
  X(TsDeltaRoll, "时滚差分", 1, ROLL, SELF, SHIFT, NONE, OP_IN1(REAL), REAL, R"tex(x_t - x_{t-d})tex", "本值减 d 期前; 量变化")                                                                                            \
  X(TsKurtRoll, "时滚峰度", 1, ROLL, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(m_4 / m_2^{2} - 3)tex", "近 d 期的峰度; 量尖峰厚尾")                                                                                    \
  X(TsMeanRoll, "时滚均值", 1, ROLL, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(\mu_t = (1/n)\,\Sigma_{W_t}\,x_s)tex", "近 d 期的均值; 平滑基线")                                                                       \
  X(TsProductRoll, "时滚乘积", 1, ROLL, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(\Pi_{W_t}(1 + x_s) - 1)tex", "近 d 期的复利累乘; 聚合收益率")                                                                        \
  X(TsSkewRoll, "时滚偏度", 1, ROLL, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(m_3 / m_2^{3/2})tex", "近 d 期的偏度; 量不对称")                                                                                        \
  X(TsSlopeRoll, "时滚斜率", 1, ROLL, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(\Sigma_i (i - \bar i)(x_{s_i} - \mu_t) \;/\; \Sigma_i (i - \bar i)^2,\; i = 0..d-1)tex", "近 d 期对期序的 OLS 斜率; 量趋势方向与快慢") \
  X(TsStdRoll, "时滚标差", 1, ROLL, SELF, MOMENT, NONE, OP_IN1(REAL), NONNEG, R"tex(\sigma_t = (\sigma_t^2)^{1/2})tex", "近 d 期的标准差; 量近期波动")                                                                     \
  X(TsSumRoll, "时滚和", 1, ROLL, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(\Sigma_{W_t}\,x_s)tex", "近 d 期的和; 近期总量")                                                                                           \
  X(TsVarRoll, "时滚方差", 1, ROLL, SELF, MOMENT, NONE, OP_IN1(REAL), NONNEG, R"tex(\sigma_t^2 = \Sigma_{W_t}(x_s - \mu_t)^2 / (n-1))tex", "近 d 期的方差; 量近期波动")                                                    \
  X(TsWmaRoll, "时滚线权均", 1, ROLL, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(\Sigma_{i=1..d}\,i\,x_{t-d+i} \;/\; \Sigma_{i=1..d}\,i)tex", "近 d 期的线性衰减均值; 越新权越大的平滑")                                \
  X(TsZRoll, "时滚标分", 1, ROLL, SELF, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex((x_t - \mu_t) / \sigma_t)tex", "本值对近 d 期的标准分; 量偏离基线幅度")                                                                    \
  X(TsArgMaxRoll, "时滚最大位", 1, ROLL, SELF, EXTREME, NONE, OP_IN1(REAL), NONNEG, R"tex(t - \mathrm{min}\{s \in W_t : x_s = \mathrm{max}_{W_t}\,x\})tex", "近 d 期最高点距今期数; 量高点新旧")                           \
  X(TsArgMinRoll, "时滚最小位", 1, ROLL, SELF, EXTREME, NONE, OP_IN1(REAL), NONNEG, R"tex(t - \mathrm{min}\{s \in W_t : x_s = \mathrm{min}_{W_t}\,x\})tex", "近 d 期最低点距今期数; 量低点新旧")                           \
  X(TsMaxRoll, "时滚最大", 1, ROLL, SELF, EXTREME, NONE, OP_IN1(REAL), REAL, R"tex(\mathrm{max}_{W_t}\,x_s)tex", "近 d 期的最高; 近期高点")                                                                                \
  X(TsMinRoll, "时滚最小", 1, ROLL, SELF, EXTREME, NONE, OP_IN1(REAL), REAL, R"tex(\mathrm{min}_{W_t}\,x_s)tex", "近 d 期的最低; 近期低点")                                                                                \
  X(TsQuantileRoll, "时滚分位", 1, ROLL, SELF, ORDER, OPEN01, OP_IN1(REAL), REAL, R"tex(Q_k(\{x_s : s \in W_t\}))tex", "近 d 期的 k 分位值; k = 0.5 即中位")                                                               \
  X(TsRankRoll, "时滚秩", 1, ROLL, SELF, ORDER, NONE, OP_IN1(REAL), UNIT, R"tex(\mathrm{pct}(x_t;\,\{x_s : s \in W_t\}))tex", "本值在近 d 期的分位; 近期相对位置")                                                         \
  X(TsMeanEma, "时指均值", 1, EXPO, SELF, RECUR, OPEN0_CLOSED1, OP_IN1(REAL), REAL, R"tex(e_t = k\,x_t + (1-k)\,e_{t-1})tex", "指数衰减均值; 无窗平滑, 对标 d 期取 k = 2/(d+1)")

// ---- 1 元 × 截面 (9 = ALL 9) ----
#define OP_CS1(X)                                                                                                                                                                                          \
  X(CsDemean, "截去均", 1, POINT, ALL, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex(x_a - \mu)tex", "减截面均值; 去市场水平")                                                                                   \
  X(CsMean, "截均值", 1, POINT, ALL, MOMENT, NONE, OP_IN1(REAL), BCAST, R"tex(\mu = (1/N)\,\Sigma_a\,x_a)tex", "截面均值广播; 造市场基准")                                                                 \
  X(CsStd, "截标差", 1, POINT, ALL, MOMENT, NONE, OP_IN1(REAL), BCAST, R"tex(\sigma = (\Sigma_a (x_a - \mu)^2 / (N-1))^{1/2})tex", "截面标准差广播; 量截面分化")                                           \
  X(CsZ, "截标分", 1, POINT, ALL, MOMENT, NONE, OP_IN1(REAL), REAL, R"tex((x_a - \mu) / \sigma)tex", "截面标准分; 去水平去量纲")                                                                           \
  X(CsBucket, "截分桶", 1, POINT, ALL, ORDER, POSINT_GROUP, OP_IN1(REAL), INT, R"tex(\mathrm{floor}(k \cdot \mathrm{pct}(x_a)))tex", "按截面分位分 k 桶, 输出 0..k−1; 造离散分组")                         \
  X(CsNormRank, "截正态秩", 1, POINT, ALL, ORDER, NONE, OP_IN1(REAL), REAL, R"tex(\Phi^{-1}(\operatorname{clamp}(\mathrm{pct}(x_a),\,1/(N+1),\,N/(N+1))))tex", "截面秩映到标准正态; 去量纲且保尾部区分度") \
  X(CsQuantile, "截分位", 1, POINT, ALL, ORDER, OPEN01, OP_IN1(REAL), BCAST, R"tex(Q_k(\{x_b\}))tex", "截面 k 分位广播; k = 0.5 即中位")                                                                   \
  X(CsRank, "截秩", 1, POINT, ALL, ORDER, NONE, OP_IN1(REAL), UNIT, R"tex(\mathrm{pct}(x_a))tex", "截面分位 ∈ [0,1]; 最稳健的去量纲")                                                                      \
  X(CsWinsor, "截缩尾", 1, POINT, ALL, ORDER, OPEN0_HALF, OP_IN1(REAL), REAL, R"tex(\operatorname{clamp}(x_a,\,Q_k,\,Q_{1-k}))tex", "截面双侧缩尾; 去极值, 配 CsZ / CsRank 做预处理")

// ---- 2 元 × SELF (20 = POINT 10 + EXPAND 5 + ROLL 5) ----
#define OP_TS2(X)                                                                                                                                                                                      \
  X(TsAdd, "时加", 2, POINT, SELF, MAP, NONE, OP_IN2(REAL, REAL), REAL, R"tex(x_t + y_t)tex", "逐点和; 合并信号")                                                                                      \
  X(TsDiv, "时除", 2, POINT, SELF, MAP, NONE, OP_IN2(REAL, REAL), REAL, R"tex(x_t / y_t)tex", "逐点比; 造比率")                                                                                        \
  X(TsImb, "时失衡", 2, POINT, SELF, MAP, NONE, OP_IN2(NONNEG, NONNEG), SIGNED, R"tex((x_t - y_t) / (x_t + y_t))tex", "双边失衡度 ∈ [−1,1]; 量买卖力量对比")                                           \
  X(TsLogRatio, "时对数比", 2, POINT, SELF, MAP, NONE, OP_IN2(POS, POS), REAL, R"tex(\ln x_t - \ln y_t)tex", "对数比; 量正数间相对变化")                                                               \
  X(TsMask, "时掩", 2, POINT, SELF, MAP, NONE, OP_IN2(REAL, REAL), REAL, R"tex(x_t \;\mathrm{where}\; y_t > 0)tex", "按 y > 0 保留 x; 下游窗口只统计子集, 掩码由 TsTodMask / TsGt 造")                 \
  X(TsMax, "时最大", 2, POINT, SELF, MAP, NONE, OP_IN2(REAL, REAL), REAL, R"tex(\max(x_t,\,y_t))tex", "逐点取大; 信号取强")                                                                            \
  X(TsMin, "时最小", 2, POINT, SELF, MAP, NONE, OP_IN2(REAL, REAL), REAL, R"tex(\min(x_t,\,y_t))tex", "逐点取小; 信号取弱")                                                                            \
  X(TsMul, "时乘", 2, POINT, SELF, MAP, NONE, OP_IN2(REAL, REAL), REAL, R"tex(x_t \cdot y_t)tex", "逐点积; 信号交互")                                                                                  \
  X(TsShare, "时占比", 2, POINT, SELF, MAP, NONE, OP_IN2(NONNEG, NONNEG), UNIT, R"tex(x_t / (x_t + y_t))tex", "占比 ∈ [0,1]; 量单边份额")                                                              \
  X(TsSub, "时减", 2, POINT, SELF, MAP, NONE, OP_IN2(REAL, REAL), REAL, R"tex(x_t - y_t)tex", "逐点差; 量信号差距")                                                                                    \
  X(TsBetaCum, "时累贝塔", 2, EXPAND, SELF, MOMENT, NONE, OP_IN2(REAL, REAL), REAL, R"tex(\beta_t = \mathrm{cov}_{W_t}(x, y) \;/\; \mathrm{var}_{W_t}(y))tex", "当日至今 x 对 y 的回归斜率; 量敏感度") \
  X(TsCorrCum, "时累相关", 2, EXPAND, SELF, MOMENT, NONE, OP_IN2(REAL, REAL), SIGNED, R"tex(\mathrm{cov}_{W_t}(x, y) \;/\; (\sigma^x_t\,\sigma^y_t))tex", "当日至今的相关系数; 量联动强弱")            \
  X(TsCovCum, "时累协方", 2, EXPAND, SELF, MOMENT, NONE, OP_IN2(REAL, REAL), REAL, R"tex(\Sigma_{W_t}(x_s - \mu^x_t)(y_s - \mu^y_t) \;/\; (n-1))tex", "当日至今的协方差; 量共变")                      \
  X(TsResidCum, "时累残差", 2, EXPAND, SELF, MOMENT, NONE, OP_IN2(REAL, REAL), REAL, R"tex((x_t - \mu^x_t) - \beta_t\,(y_t - \mu^y_t))tex", "当日至今回归的当前残差; 剥离 y 后的特质部分")             \
  X(TsWMeanCum, "时累权均", 2, EXPAND, SELF, MOMENT, NONE, OP_IN2(REAL, NONNEG), REAL, R"tex(\Sigma_{W_t}\,y_s x_s \;/\; \Sigma_{W_t}\,y_s)tex", "当日至今 y 加权的 x 均值; 如量加权价")               \
  X(TsBetaRoll, "时滚贝塔", 2, ROLL, SELF, MOMENT, NONE, OP_IN2(REAL, REAL), REAL, R"tex(\beta_t = \mathrm{cov}_{W_t}(x, y) \;/\; \mathrm{var}_{W_t}(y))tex", "近 d 期 x 对 y 的回归斜率; 量敏感度")   \
  X(TsCorrRoll, "时滚相关", 2, ROLL, SELF, MOMENT, NONE, OP_IN2(REAL, REAL), SIGNED, R"tex(\mathrm{cov}_{W_t}(x, y) \;/\; (\sigma^x_t\,\sigma^y_t))tex", "近 d 期的相关系数; 量联动强弱")              \
  X(TsCovRoll, "时滚协方", 2, ROLL, SELF, MOMENT, NONE, OP_IN2(REAL, REAL), REAL, R"tex(\Sigma_{W_t}(x_s - \mu^x_t)(y_s - \mu^y_t) \;/\; (n-1))tex", "近 d 期的协方差; 量共变")                        \
  X(TsResidRoll, "时滚残差", 2, ROLL, SELF, MOMENT, NONE, OP_IN2(REAL, REAL), REAL, R"tex((x_t - \mu^x_t) - \beta_t\,(y_t - \mu^y_t))tex", "近 d 期回归的当前残差; 剥离 y 后的特质部分")               \
  X(TsWMeanRoll, "时滚权均", 2, ROLL, SELF, MOMENT, NONE, OP_IN2(REAL, NONNEG), REAL, R"tex(\Sigma_{W_t}\,y_s x_s \;/\; \Sigma_{W_t}\,y_s)tex", "近 d 期 y 加权的 x 均值; 如量加权价")

// ---- 2 元 × 截面 (5 = ALL 3 + GROUP 2) ----
#define OP_CS2(X)                                                                                                                                                                                \
  X(CsBeta, "截贝塔", 2, POINT, ALL, MOMENT, NONE, OP_IN2(REAL, REAL), BCAST, R"tex(\hat\beta = \mathrm{cov}_a(x, y) \;/\; \mathrm{var}_a(y))tex", "截面 x 对 y 的回归斜率广播; 量整体敏感度")   \
  X(CsCorr, "截相关", 2, POINT, ALL, MOMENT, NONE, OP_IN2(REAL, REAL), BCAST, R"tex(\mathrm{corr}_a(x, y))tex", "截面相关系数广播; 量因子联动")                                                  \
  X(CsResid, "截残差", 2, POINT, ALL, MOMENT, NONE, OP_IN2(REAL, REAL), REAL, R"tex(x_a - \hat\alpha - \hat\beta\,y_a)tex", "截面回归残差; 对 y 中性化")                                         \
  X(CsGroupMean, "组均值", 2, POINT, GROUP, MOMENT, NONE, OP_IN2(REAL, INT), REAL, R"tex((1/|G(a)|)\,\Sigma_{G(a)}\,x_b,\; G(a) = \{b : y_b = y_a\})tex", "组内均值广播; 造行业基准, y = 组 id") \
  X(CsGroupRank, "组秩", 2, POINT, GROUP, ORDER, NONE, OP_IN2(REAL, INT), UNIT, R"tex(\mathrm{pct}(x_a;\,\{x_b : b \in G(a)\}),\; G(a) = \{b : y_b = y_a\})tex", "组内分位; 组内相对位置, y = 组 id")

// ---- 3 元 × SELF (1) ----
#define OP_TS3(X) \
  X(TsWhere, "时择", 3, POINT, SELF, MAP, NONE, OP_IN3(REAL, REAL, REAL), REAL, R"tex(\mathbf{1}[x_t > 0]\,y_t + \mathbf{1}[x_t \le 0]\,z_t)tex", "按 x > 0 逐点选 y 或 z; 条件拼接")

// ---- 3 元 × 截面 (1) ----
#define OP_CS3(X) \
  X(CsGroupResid, "组残差", 3, POINT, GROUP, MOMENT, NONE, OP_IN3(REAL, REAL, INT), REAL, R"tex(\tilde x_a - \hat\beta\,\tilde y_a,\; \tilde x_a = x_a - \bar x_{G(a)},\; G(a) = \{b : z_b = z_a\})tex", "组内 demean 后 x 对 y 的回归残差; 组与 y 双重中性化, z = 组 id")

// ---- 拼装: OP_ALL 展开序 = 全局 idx 序; OP_TS / OP_CS 按域 (GpuRun 的 run_ts / run_cs 用);
//      需要按 A 域分派时用 X_##域 token 粘贴 (见 op_check / OperatorsService) ----
#define OP_TS(X) OP_TS0(X) OP_TS1(X) OP_TS2(X) OP_TS3(X)
#define OP_CS(X) OP_CS1(X) OP_CS2(X) OP_CS3(X)
#define OP_ALL(X) OP_TS0(X) OP_TS1(X) OP_CS1(X) OP_TS2(X) OP_CS2(X) OP_TS3(X) OP_CS3(X)
