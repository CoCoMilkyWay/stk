#pragma once

#include "codec/L2_DataType.hpp"
#include "features/TimeIndex.hpp"
#include <array>
#include <cstdint>

// 核心理念: 在低信噪比、强竞争的二级市场，端到端的深度模型(数据先验)会先被淘汰, 特征工程因子挖掘(结构性先验)是生存条件，不是选择。

// 在手工原始特征设计阶段:
//  1. 高频特征可以有低频能量(原始特征级别只是数学定义所在的级别), 且不需要手动频域切分(后面会自动化处理)
//  2. 选择合适的归一化, 过滤掉尽可能少的不平稳的最低频信号, 让时序平稳 (保留尽可能多的平稳的低频信息, 这部分是因子主体的主要构成)
//  3. 保证特征序列概率分布在时间/截面上的稳定性(时间上不漂移(因子不衰减), 截面分布一致(可以截面中性化))
// 在符号回归阶段:
//  1. GPU会系统性地对所有手工特征做检测, 得到对应的平稳性, 分布, 频域, 自相关特性
//  2. 用算子池逐个特征测试, 展开得到若干新特征, 然后再次统计新特征的特性, 然后过滤掉不合格的, 自动生成针对原始特征的算子池约束
//  3. 计算展开后的特征池的相关性矩阵(时序相关性(自相关), 截面相关性(交叉相关))
//  4. 根据上面的信息, 符号回归优化Loss(Label), 实现高效因子挖掘和稳健性测试
//
// 完整的因子由两部分构成:
// 频率: 秒 -------------------------------- 分钟 ------------ 小时 ------------- 天
//       <------------------------------------>|<--------------------------->| (更低频信号弃用(不平稳))
//        进出场触发器(高频特征的简单组合)        因子主体(低频特征的复杂组合)
//        非必须. 只是超额                        核心的核心, 盈利主体
//        (为因子主体提供稳定超额, 流动性支持)    (决定因子强度, 周期, 平稳性(分层), 换手)

// ============================================================================
// FORMULA NOTATION CONVENTION (公式符号规范)
// ============================================================================
//
// 【深度 (LOB Depth)】
//   符号结构:  {P, V}_{i,t}^{M,s}
//   下标:
//     i = 档位 (1,2,...,N)
//     t = 时刻
//   上标:
//     M = 固定 (Maker, 与订单流符号一致)
//     s ∈ {B, A}  (B=Bid 买盘, A=Ask 卖盘)
//   例:  P_{1,t}^{M,B} = t 时刻买一价,  V_{5,t}^{M,A} = t 时刻卖五量
//
// 【订单流 (Order Flow)】
//   符号结构:  O_{时间}^{e,s(,c)[\mathrm{cond}]}
//   下标 (时间聚合, 选一):
//     t            = 时刻 (单个 tick)
//     Δt           = 时间分箱 (bin)
//     W            = 滚动窗口
//     ∑_{τ=t₀}^{t} = 开盘累计
//   上标 (按顺序):
//     e ∈ {T, M, C}         事件类型: T=Taker(主动成交), M=Maker(挂单), C=Cancel(撤单)
//     s ∈ {B, A}            侧向: B=Bid 买盘, A=Ask 卖盘
//     c ∈ {XL, L, Mid, S}   大小单分类
//     [\mathrm{cond}]       条件筛选 (如 CA=连续竞价, fleet=闪单)
//   算子:
//     |O|  = 取量 (volume)
//     #O   = 取笔数 (count)
//   例:
//     |O_t^{T,B}|                = t 时刻主动买成交量
//     #O_{\Delta t}^{M,A}        = Δt 内卖方挂单笔数
//     |O_t^{T,B,XL}|             = t 时刻特大单主动买成交量
//     |O_W^{T,\mathrm{CA}}|      = W 窗口连续竞价成交量
//     ∑_{τ=t₀}^{t}|O_τ^{T,B}|    = 开盘到 t 的累计主动买成交量
//

// ============================================================================
// 特征定义: 算子文件自带节点实例 + 落盘列 (Operator/TS/<类别>/<Op>.hpp 末尾), 没有中心表
// ============================================================================
// 算子文件 = 数学 (class) + 文件末尾两种宏 (CMake 扫描汇总, C++ 里不直接展开):
//
//   #define NODE_<Name>(N)  N(<Name>, (OpType), (inputs...), compute_trigger, flush_trigger)
//     Name     节点名 (DAG 成员名), 同一算子可有多个实例 (CI.hpp: Ci_1 / Ci_5 / Ci_10 / Ci_30)
//     OpType   算子类型, 必须加括号 (模板参数里有逗号)
//     inputs   构造参数 (不含输出口), 引用 DAG 成员: tick_data / minute_data / asset_code_ / fund_row_ /
//              上游节点: 单口 Up.out(), 多口 Up.out(Up.port), 源层数组 DepthData.bid_qty 等
//     触发域   Trigger:: 下的名字. compute 在 compute_trigger 域调, flush 在 flush_trigger 域调;
//              采样型算子两者相同, 降频型算子 compute=onTick/flush=onMinute
//     依赖     就是 inputs 里出现的 "Up." — 不需要写别的, 也不需要 #include 上游算子
//
//   #define FIELDS_<LVL>_<Name>(X)  X(code, width, valid_type, cat_l1, cat_l2, norm_method,
//                                     PSD, name_en, name_cn, description, formula, SRC) ...
//     LVL ∈ {L0, L1, DEPTH}: 落盘层. 可无 (纯中间节点), 可多层. 一行 = 一个落盘列.
//     valid_type   ALL / DATA / DEPTH: 该列的有效性看哪个标志 (_data_valid / _depth_valid)
//     SRC 这一列的值从哪来 (基建按它生成写回 / 截面配置 / 广播), 也决定数据类型 (TS/CS/LB/META):
//       OP(Node) / OP(Node, port)  节点输出口 (Node::Out::value / Node::Out::port); 层必须 == 节点 flush 域
//       FUND(field)                当日基本面行 fund::field 盘中广播 (仅 L1)
//       CS(lvl, src, tf, m)        截面: 源层 lvl (0/1) 的字段 src → cs::Transform::tf → cs::Method::m
//       META / LABEL               基建 / 标签回填 手工写
//     非算子列的 <Name> 是任意名字, 放在写它的地方旁边: META → Operator/TS/Meta/Meta.hpp,
//     LABEL → Operator/TS/Label/LabelReturn.hpp, FUND → Fundamental/FundamentalDaily.hpp, CS → Misc/CSMethods.hpp.
//
// CMake (projects/main/CMakeLists.txt) 扫描 features/{Operator,Fundamental,Misc}/**/*.hpp, 按 inputs 引用
// 分层拓扑排序, 生成 build/generated/features/NodesGenerated.hpp: 全部 #include + NODES(N) +
// LEVEL_{0,1}_FIELDS(X) + DEPTH_FIELDS(X). 基建 (ComputeGraph / CoreSequential / CoreCrosssection /
// FeatureStore / Feature.hpp GUI 元数据) 全部由这几张表展开, 不需要手改. 改动后下次 build 自动重新 configure.
//
// 加特征 = 改 (或新建) 一个算子文件. 删特征 = 删几行 / 删文件. 其他地方不动.
// 顺序保证: DAG 按拓扑序声明成一条链, 节点只看得到排在自己前面的节点 → 引用了排后面的节点 = 编译错误.
// 各触发域一个 tick 内的执行顺序: onTaker|onMaker|onCancel → onTick → onDepth; 分钟边界 → onMinute.
// 字段表改动会改变落盘布局: 文件头带表指纹, 读旧文件即断言失败, 需重算.
// 标签 (LabelReturn*) 回填别的时间行, 不走节点表, 在 ComputeGraph.hpp 手工声明.

// ============================================================================
// 字段元数据枚举 (X-macro: 一处定义 → enum + to_string(en/cn) + 全值表)
// ============================================================================
// 值 = 落盘/GUI 稳定编号, 不要改已有值; 加新项追加即可.

struct EnumStr {
  const char *en;
  const char *cn;
};

#define ENUM_ENTRY(name, value, cn) name = value,
#define ENUM_CASE(name, value, cn) \
  case decltype(t)::name:          \
    return {#name, cn};
#define ENUM_VALUE(name, value, cn) E_t::name,
#define DEFINE_ENUM(E, LIST)                   \
  enum class E : uint8_t { LIST(ENUM_ENTRY) }; \
  inline constexpr EnumStr to_string(E t) {    \
    switch (t) { LIST(ENUM_CASE) }             \
    return {"?", "未知"};                      \
  }                                            \
  inline constexpr auto E##_ALL = [] {         \
    using E_t = E;                             \
    return std::array{LIST(ENUM_VALUE)};       \
  }();

// 数据类型 (由字段表 SRC 列推出, 见下 SRC_KIND_*)
#define FEATURE_DATA_TYPES(X) \
  X(TS, 0, "时序")            \
  X(CS, 1, "截面")            \
  X(LB, 2, "标签")            \
  X(META, 3, "元数据")

#define FEATURE_CATEGORY_L1S(X) \
  X(IMBALANCE, 0, "失衡")       \
  X(SHAPE, 1, "形状")           \
  X(ORDER_FLOW, 2, "订单流")    \
  X(BEHAVIORAL, 3, "行为")      \
  X(RESILIENCE, 4, "韧性")      \
  X(LIQUIDITY, 5, "流动性")     \
  X(VOLATILITY, 6, "波动率")    \
  X(BASIC, 7, "基础")           \
  X(LABEL, 8, "标签")           \
  X(META, 9, "元数据")

#define FEATURE_CATEGORY_L2S(X) \
  X(RAW, 0, "原始")             \
  X(NORMALIZED, 1, "标准化")    \
  X(OSCILLATOR, 2, "震荡器")    \
  X(DEVIATION, 3, "偏离")       \
  X(RATIO, 4, "比率")           \
  X(RANK, 5, "排名")            \
  X(FUTURE_RET, 6, "未来收益")  \
  X(SCORE, 7, "评分")           \
  X(UNIVERSE, 8, "全域统计")    \
  X(BENCHMARK, 9, "基准")

// 推荐归一化 (仅元数据提示, 不参与计算)
#define NORM_METHODS(X)                                       \
  X(NONE, 0, "无") /* x */                                    \
  /* scale (linear) */                                        \
  X(ZSCORE, 1, "Z标准化")       /* (x - mean) / std */        \
  X(ROBUST_ZSCORE, 2, "稳健Z")  /* (x - median) / MAD */      \
  X(IQR_ZSCORE, 3, "IQR标准化") /* (x - Q2) / (Q3 - Q1) */    \
  /* order based */                                           \
  X(RANK, 4, "排名")              /* rank / N */              \
  X(RANK_ZSCORE, 5, "排名标准化") /* rank → inverse normal */ \
  /* bounding */                                              \
  X(CLIP, 6, "截断")   /* clip(x, [-k, k]) */                 \
  X(WINSOR, 7, "缩尾") /* winsorize by percentile */          \
  /* nonlinear */                                             \
  X(LOG, 8, "对数")                                           \
  X(POWER, 9, "幂变换")                                       \
  X(ASINH, 10, "反双曲正弦")                                  \
  X(TANH, 11, "双曲正切")                                     \
  X(SINCOS, 12, "正余弦编码") /* x → (sin, cos) */            \
  /* composite pipelines */                                   \
  X(LOG_ZSCORE, 20, "对数+Z")                                 \
  X(POWER_ZSCORE, 21, "幂+Z")                                 \
  X(ASINH_ZSCORE, 22, "asinh+Z")                              \
  X(CLIP_ZSCORE, 23, "Z+截断")                                \
  X(WINSOR_ZSCORE, 24, "缩尾+Z")                              \
  X(CLIP_LOG_ZSCORE, 25, "截断+对数+Z")

DEFINE_ENUM(FeatureDataType, FEATURE_DATA_TYPES)
DEFINE_ENUM(FeatureCategoryL1, FEATURE_CATEGORY_L1S)
DEFINE_ENUM(FeatureCategoryL2, FEATURE_CATEGORY_L2S)
DEFINE_ENUM(NormMethod, NORM_METHODS)

#undef DEFINE_ENUM
#undef ENUM_VALUE
#undef ENUM_CASE
#undef ENUM_ENTRY

// L2::ValidType 定义在 codec, 只补 to_string
inline constexpr EnumStr to_string(L2::ValidType t) {
  switch (t) {
  case L2::ValidType::ALL:
    return {"ALL", "全部"};
  case L2::ValidType::DATA:
    return {"DATA", "数据"};
  case L2::ValidType::DEPTH:
    return {"DEPTH", "深度"};
  }
  return {"?", "未知"};
}

// ----------------------------------------------------------------------------
// SRC 列解析 (字段表消费者共用)
//   SRC_KIND_##src             → FeatureDataType
//   SRC_DISPATCH(P, code, src) → P_OP(code, node[, port]) / P_FUND(code, f) / P_CS(code, lvl, s, tf, m) /
//                                P_LABEL(code) / P_META(code)   (消费者按来源各定义一组 P_*)
// ----------------------------------------------------------------------------
#define SRC_KIND_OP(...) FeatureDataType::TS
#define SRC_KIND_FUND(...) FeatureDataType::TS
#define SRC_KIND_CS(...) FeatureDataType::CS
#define SRC_KIND_LABEL FeatureDataType::LB
#define SRC_KIND_META FeatureDataType::META

#define SRC_ARGS_OP(...) OP, __VA_ARGS__
#define SRC_ARGS_FUND(f) FUND, f
#define SRC_ARGS_CS(l, s, tf, m) CS, l, s, tf, m
#define SRC_ARGS_LABEL LABEL
#define SRC_ARGS_META META
#define SRC_DISPATCH(prefix, code, src) SRC_DISPATCH_I(prefix, code, SRC_ARGS_##src)
#define SRC_DISPATCH_I(prefix, code, ...) SRC_DISPATCH_II(prefix, code, __VA_ARGS__)
#define SRC_DISPATCH_II(prefix, code, kind, ...) prefix##_##kind(code __VA_OPT__(, ) __VA_ARGS__)

// ----------------------------------------------------------------------------
// 落盘层注册: X(level_name, level_index, fields_macro). DEPTH 层单独 (见 FeatureStoreConfig)
// ----------------------------------------------------------------------------
#define ALL_LEVELS(X)      \
  X(L0, 0, LEVEL_0_FIELDS) \
  X(L1, 1, LEVEL_1_FIELDS)
