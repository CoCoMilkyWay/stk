#pragma once

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp" // Trigger
#include <array>
#include <cstdint>

// ============================================================================
// FORMULA NOTATION CONVENTION (公式符号规范)
// ============================================================================
//
// 【索引 (Index)】一个字母只表示一种时间/空间轴, 全表通用:
//   τ = 逐笔事件 (tick, 含成交/委托/撤单)   c = Δ 秒网格格 (Realized)
//   j = 盘口更新 (Book 事件型)              i = 盘口档位 (1..N)
//   t = 分钟                                D = 交易日
//
// 【价格 (Price)】统一大写 P, 语境用上/下标区分, 不用 O/H/L/C 单字母 (O 已归订单流):
//   P_τ / P_c                     逐笔成交价 / 网格格收盘价 (元)
//   P^{open/high/low/close}_t     分钟 OHLC
//   P_{mid} / P^{micro}           中间价 / 量加权中间价
//   P^{up/dn/pre}_D               日频涨停 / 跌停 / 前收
//   VWAP^{s}(A)                   吃 s ∈ {B, A} 侧 A 元的成交均价 (冲击成本 / 标签共用)
//   ε                             价格比较容差 (= kPxEps, 涨跌停判定共用)
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
//   运算符:
//     |O|  = 取量 (volume)
//     #O   = 取笔数 (count)
//   风格: 分钟内逐笔求和一律 ∑_{τ∈Δt} f(O_τ) (τ 显式); 纯笔数计数用 #O_{Δt} (时间挂 O 下标);
//         条件筛选用 1[·] 乘子, 价格容差写 ε.
//   例:
//     |O_τ^{T,B}|                    = τ 时刻主动买成交量
//     ∑_{τ∈Δt} P_τ|O_τ^{T,B}|        = Δt 内主动买成交额
//     #O_{\Delta t}^{M,A}            = Δt 内卖方挂单笔数
//     |O_τ^{T,B,XL}|                 = τ 时刻特大单主动买成交量
//     ∑_{τ=t₀}^{t}|O_τ^{T,B}|        = 开盘到 t 的累计主动买成交量
//

// ============================================================================
// 特征定义: 节点文件自带节点实例 + 落盘列 (Node/<Tier>/{TS,CS}/**/*.hpp 末尾), 没有中心表
// ============================================================================
// 时序节点文件 (Node/<Tier>/TS/<分类>/<Name>.hpp) = 节点类 (class, 纯数学) + 文件末尾两种宏 (CMake 扫描汇总, C++ 里不直接展开):
//   两档都要的节点类: class 放 Node/Common/<分类>/<Name>.hpp, 各档文件 include 它后只写本档的 NODE_ / FIELDS_.
//
//   #define NODE_<Name>(N)  N(<Name>, (NodeType), (inputs...), trigger[, flush_trigger])
//     Name     节点名 (DAG 成员名); 同一节点类可有多个实例 (不同模板参数), 目前每个节点类一个节点, 同族列在节点内用口区分
//     NodeType 节点类, 必须加括号 (模板参数里有逗号)
//     inputs   构造参数 (不含输出口), 引用 DAG 成员: tick_data / minute_data / fund_panel / asset_code_ / asset_id_ / date_ /
//              上游节点: 单口 Up.out(), 多口 Up.out(Up.port), 全口 Up.outs(), 源层数组 Depth.bid_qty 等;
//              盘前即需的日频值直接引 y 槽 Up.y[Up.port] (Fund 的 Series 首个分钟 flush 才有值, 见 Fund.hpp).
//              必须字面写在这一行 (CMake 按 "Up." 抽依赖; Up.out(Up.port) / Up.y[Up.port] 记单口, 其余记整节点), 不要藏进 helper 宏
//     触发域   Trigger:: 下的名字. 采样型只写一个 (compute 与 flush 同域);
//              降频型写两个: compute=onTick, flush=onMinute; 广播型 (日频): compute=onDay, flush=onMinute
//     依赖     就是 inputs 里出现的 "Up." — 不需要写别的, 也不需要 #include 上游节点
//
// 截面列文件 (Node/<Tier>/CS/<分类>/<源节点>.hpp) = 只有 FIELDS_ 宏 (方法在 Method/CS.hpp, 契约见 DataDefine.hpp); 无 NODE_.
//
//   #define FIELDS_<LVL>_<Name>(X, CAT1)  X(code, CAT1, cat_l2, name_en, name_cn, description, formula, SRC) ...
//     LVL ∈ {L0, L1}: 落盘层. 可无 (纯中间节点), 可多层. 一行 = 一个落盘列.
//     cat_l2 缺省写 AUTO (dummy): GUI 按预览值域自动探测 (price/flag/rank/ratio_pos/ratio_neg/ratio/raw, 见 TabFeature detect_cat2);
//       写别的 token = 人类强行赋值, 覆盖探测 (表格绿字). 不参与落盘指纹, 改了不用重算.
//     同族列 (Flow 的 {amt,vol,n}_{taker,maker,cancel}_{bid,ask}, Book 的 qty_{bid,ask}_{1,5,10,all} …) 在文件内用 helper 宏生成行, ## 拼进名字, 字面串拼进 EN/CN/公式.
//     SRC 这一列的值从哪来 (基建按它生成写回 / 截面展开); 数据类型 / 有效性标志 / 归一化 (GUI 两列 TS Norm / CS Norm) 全部由它推出:
//       TS(Node[, port], Tf, Method)  节点输出口. TS; 层必须 == 节点 flush 域; 有效性: onMinute → ALL (L1 稠密), onDepth → DEPTH, 其余 → DATA
//                                  Tf / Method 是 ts:: 下的名字 (Method/TS.hpp): 落盘值 = ts::Tf::apply(节点输出); Method 目前占位 (仅元数据)
//       CS(ns, lvl, src, Tf, Method)  截面: 源档 ns 源层 lvl (0/1) 的字段 src → cs::Tf::apply → cs::Method::apply. CS; 有效性随源层 (L1 ALL / L0 DATA)
//                                  ns = 本档, 或 TIER_SIDES 登记的旁读档 (只许 L1)
//       LABEL                      标签回填 (CoreLob 手工写). LB; ALL (全行写, 不产 NaN)
//       FLAG                       基建标志列 ts_valid / cs_valid (CoreLob 手工写; 编码/状态机见 Meta.hpp fmeta/MetaTracker). META; ALL
//     一行 = 一个标量列 (列下标 = 落盘存储列).
//     非节点列的 <Name> 是任意名字, 放在写它的地方旁边: FLAG → Node/<Tier>/TS/Meta/Meta.hpp,
//     LABEL → Node/<Tier>/TS/Label/*.hpp, CS → Node/<Tier>/CS/<分类>/<源节点>.hpp (owner 名 Cs<源节点>, 与 TS 节点名区分).
//     推荐频谱 (psd) 按层给 (TIER_LEVELS), 不逐列写.
//
// CMake (projects/main/CMakeLists.txt) 逐档扫描 features/Node/<Tier>/{TS,CS}/<分类>/**/*.hpp, 按 inputs 引用
// 分层拓扑排序, 生成 build/generated/features/<Tier>/NodesGenerated.hpp: 本档全部 #include + <Tier>_NODES(N) +
// <Tier>_L0_FIELDS(X) / <Tier>_L1_FIELDS(X). 基建 (LobGraph / Core* / FeatureStore / Feature.hpp GUI 元数据)
// 全部由这几张表展开, 不需要手改. 改动后下次 build 自动重新 configure.
//
// 加特征 = 改 (或新建) 一个节点文件. 删特征 = 删几行 / 删文件. 其他地方不动.
// 顺序保证: DAG 按拓扑序声明成一条链, 节点只看得到排在自己前面的节点 → 引用了排后面的节点 = 编译错误.
// 各触发域执行顺序: 盘前 (节点 reset 后) → onDay; 一个 tick 内 onTaker|onMaker|onCancel → onTick → onDepth; 分钟边界 → onMinute.
// 字段表改动会改变落盘布局: 文件头带表指纹, 读旧文件即断言失败, 需重算.
// 标签 (LabelLob) 回填别的时间行, 不走节点表, 在 LobGraph.hpp 手工声明.

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

// Transform 探索用的时序归一化 (math/normalize/Normalize.hpp, Params::ts_norm). 字段表不再有此列:
// 落盘归一化由 SRC 列声明 (TS(..., Tf, Method) → ts::, CS(..., Tf, Method) → cs::), GUI 从 SRC 推导显示
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
// NODE_ 行触发域: N(name, type, args, trigger[, flush]) → 消费者用 NODE_COMPUTE(__VA_ARGS__) / NODE_FLUSH(__VA_ARGS__)
// ----------------------------------------------------------------------------
#define NODE_COMPUTE(ct, ...) Trigger::ct
#define NODE_FLUSH(ct, ...) NODE_FLUSH_I(__VA_ARGS__ __VA_OPT__(, ) ct)
#define NODE_FLUSH_I(ft, ...) Trigger::ft

// flush 域 → 该节点落盘列的层 / 有效性标志 (TS 列由此推出, 不逐列写)
//   L1 (onMinute) 稠密: 每分钟都写行 (无成交分钟各节点归零 / 延续, 见 ResamplerTick2Min / CoreLob) → ALL, 不受 ts_valid 门控;
//   L0 事件稀疏: 只有事件秒有行 → DATA / DEPTH 按 ts_valid 门控
constexpr int level_of(Trigger flush) { return flush == Trigger::onMinute ? 1 : 0; }
constexpr L2::ValidType valid_of(Trigger flush) {
  return flush == Trigger::onMinute ? L2::ValidType::ALL : flush == Trigger::onDepth ? L2::ValidType::DEPTH
                                                                                     : L2::ValidType::DATA;
}

// ----------------------------------------------------------------------------
// SRC 列解析 (字段表消费者共用)
//   SRC_KIND_##src             → FeatureDataType
//   SRC_VALID_##src            → L2::ValidType (TS 看节点 flush 域: 需要 node_flush::<node>, 见 FeatureStoreConfig)
//   SRC_TS_TF_##src / SRC_TS_METHOD_##src → ts::TfId:: / ts::MethodId:: (TS 行取 Tf / Method, 其余 None; 消费者需 include Method/TS.hpp)
//   SRC_CS_TF_##src / SRC_CS_METHOD_##src → cs::TfId:: / cs::MethodId:: (CS 行取 Tf / Method, 其余 None; 消费者需 include Method/CS.hpp)
//   SRC_DISPATCH(P, code, src) → P_TS(code, node[, port], Tf, Method) / P_CS(code, ns, lvl, s, Tf, Method) /
//                                P_LABEL(code) / P_FLAG(code)   (消费者按来源各定义一组 P_*)
// ----------------------------------------------------------------------------
#define SRC_KIND_TS(...) FeatureDataType::TS
#define SRC_KIND_CS(...) FeatureDataType::CS
#define SRC_KIND_LABEL FeatureDataType::LB
#define SRC_KIND_FLAG FeatureDataType::META

#define SRC_VALID_TS(node, ...) valid_of(node_flush::node)
#define SRC_VALID_CS(ns, l, ...) (l == 1 ? L2::ValidType::ALL : L2::ValidType::DATA) // 截面随源层: L1 稠密 (门控本就只看 cs_valid)
#define SRC_VALID_LABEL L2::ValidType::ALL                                           // 标签全行写 (不产 NaN, 缺口用可成交时刻顶上, 见 LabelLob 文件头), 与 L1 特征同稠密
#define SRC_VALID_FLAG L2::ValidType::ALL

// TS(node, Tf, Method) / TS(node, port, Tf, Method): 按参数个数取 Tf / Method (RowWriter 同法取 node/port)
#define SRC_NODE_PICK(_1, _2, _3, _4, NAME, ...) NAME
#define SRC_NODE_TF_3(node, tf, m) tf
#define SRC_NODE_TF_4(node, port, tf, m) tf
#define SRC_NODE_METHOD_3(node, tf, m) m
#define SRC_NODE_METHOD_4(node, port, tf, m) m
#define SRC_NODE_TF(...) SRC_NODE_PICK(__VA_ARGS__, SRC_NODE_TF_4, SRC_NODE_TF_3, , )(__VA_ARGS__)
#define SRC_NODE_METHOD(...) SRC_NODE_PICK(__VA_ARGS__, SRC_NODE_METHOD_4, SRC_NODE_METHOD_3, , )(__VA_ARGS__)

#define SRC_TS_TF_TS(...) ts::TfId::SRC_NODE_TF(__VA_ARGS__)
#define SRC_TS_TF_CS(...) ts::TfId::None
#define SRC_TS_TF_LABEL ts::TfId::None
#define SRC_TS_TF_FLAG ts::TfId::None

#define SRC_TS_METHOD_TS(...) ts::MethodId::SRC_NODE_METHOD(__VA_ARGS__)
#define SRC_TS_METHOD_CS(...) ts::MethodId::None
#define SRC_TS_METHOD_LABEL ts::MethodId::None
#define SRC_TS_METHOD_FLAG ts::MethodId::None

#define SRC_CS_TF_TS(...) cs::TfId::None
#define SRC_CS_TF_CS(ns, l, s, tf, m) cs::TfId::tf
#define SRC_CS_TF_LABEL cs::TfId::None
#define SRC_CS_TF_FLAG cs::TfId::None

#define SRC_CS_METHOD_TS(...) cs::MethodId::None
#define SRC_CS_METHOD_CS(ns, l, s, tf, m) cs::MethodId::m
#define SRC_CS_METHOD_LABEL cs::MethodId::None
#define SRC_CS_METHOD_FLAG cs::MethodId::None

#define SRC_ARGS_TS(...) TS, __VA_ARGS__
#define SRC_ARGS_CS(ns, l, s, tf, m) CS, ns, l, s, tf, m
#define SRC_ARGS_LABEL LABEL
#define SRC_ARGS_FLAG FLAG
#define SRC_DISPATCH(prefix, code, src) SRC_DISPATCH_I(prefix, code, SRC_ARGS_##src)
#define SRC_DISPATCH_I(prefix, code, ...) SRC_DISPATCH_II(prefix, code, __VA_ARGS__)
#define SRC_DISPATCH_II(prefix, code, kind, ...) prefix##_##kind(code __VA_OPT__(, ) __VA_ARGS__)

// ----------------------------------------------------------------------------
// 特征档 (tier) = 数据源. 一档一套节点表 / 字段表 / 特征库:
//   Lob  Node/Lob/   L2 逐笔 → LOB 还原 (K 线亦由逐笔重采样); 票池内算, 库在 features/<pool>/
//   Bar  Node/Bar/   分钟 K (bar1m, 非 LOB 还原) + 基本面; 全市场算, 库在 features/all/ (票池名单也由它产出)
//   两档 TS / CS 列名全局唯一 (FeatureMeta.cpp 断言; 因子层按名拼接), META / LB 是各档基建列, 同名同语义.
//   档间只许截面旁读 (TIER_SIDES): 时序是资产内纯函数, 不跨档; 同一列只在一档算 (Lob 要基本面就旁读 Bar, 不重算).
//
// 档注册: X(Tier, ns) —— Tier = CMake 生成宏前缀 / TierId 枚举名, ns = C++ 命名空间
#define ALL_TIERS(X) \
  X(Lob, lob)        \
  X(Bar, bar)

// 落盘层注册 (档 × 层, 一格一行): X(Tier, ns, LVL, index, fields_macro, psd); 行数由层号定 (level_rows)
//   index = 语义层号 (0 = L0 秒, 1 = L1 分钟), 全档同一套; 档可以缺层 (Bar 无秒频源, 只有 L1)
//   L0    秒频 (T = L0_ROWS). 预留层: 目前只有 ts_valid / cs_valid 两列, 是"暂时", 不是"无用" ——
//         秒频算力贵 (每笔触发, 每列行数是 L1 的 60 倍), 进 L0 的特征要精挑细选后才加.
//         不要删 Lob 的 L0, 不要把层系统退化成只有 L1, 也不要因 L0 暂无特征就删它的写 / CS 节拍 / 落盘 / 读路径.
//   L1    分钟频 (T = L1_ROWS)
//   每层每天一个日文件, 内带列目录, 按列选读 (见 FeatureDay.hpp; 列是否沿 T 轴 XOR 差分由写端逐列按数据定)
//   psd = 该层特征的推荐频谱 (秒/分/时 能量占比提示, 仅 GUI 元数据)
#define TIER_LEVELS(X)                           \
  X(Lob, lob, L0, 0, Lob_L0_FIELDS, "100/00/00") \
  X(Lob, lob, L1, 1, Lob_L1_FIELDS, "00/100/00") \
  X(Bar, bar, L1, 1, Bar_L1_FIELDS, "00/100/00")

// 档间旁读: X(ns, side) —— ns 档的 L1 截面行可读 side 档的 L1 列 (CS(side, 1, src, ...) / NeutralRank 上下文);
// side 档当日缺库 → 旁读列整日 NaN + 日志 (不等它, 不替它算)
#define TIER_SIDES(X) X(lob, bar)
