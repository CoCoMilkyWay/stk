// Factors→Inspect: 单因子展示页. 对象 = Factors 页表格里点行高光的那一个 (FactorsUIState::view_file); 数据 = InspectService
// 对它算出的一级 Row[H][T] (沿时间) + 二级 HoldStat (每持有期).
//
// 布局: 顶部 2:1 两栏. 左 = 头信息 (◂ ▸ 按表序切相邻有效因子 / 中文名 英文名 / 口径 / 规范串 (按语法着色, 按词折行) / note) + 控件 (◂ Hold ▸ 周期名 hold_name + 开 / 清仓
//   hold_points (时点 / 价分色) / 分层 toggle / Compute / Cancel / 状态; 箭头循环切, 不用下拉) + 作用域行; 右 = 选定持有期的 stat 面板
//   (hold_stat_panel: 按组分列, 定义 / 着色与因子表同一份).
//   整页 (头部文字 + 四图) 画的是**现结果**的因子: 切因子后服务不清旧 result, 旧的继续画, 头部标 "→ 新因子 (计算中…)", 新结果发布时一次性覆盖, 不闪
//   成本只进期限结构 (右下): 分层 / IC / 顶部 stat 全是毛价格收益 (与 Factors Run 同), 拿来看分层 / 拟合形状; 成本与仓位映射另做.
//   服务一次算出毛口径一级 Row[H][T] + 二级 HoldStat[H] + 各金额段 × 各持有期的换手成本 CostTier[C], 换 Hold / 超额|绝对 / 改 fixed_impact 都只是切片
//   (UI 持锁派生 / 现算), 不发请求, 零延时
// 下面 2×2 四图:
//   左上 分层对数累计 (1×2 子图共 y 轴, 无缝相接, 刻度 %): 左 3/4 时序 = 20 组沿交易日的 Σ ln(1 + 日收益) × kDaysPerYear / days
//        (InspectResult::nav 日收益; 全部线同一系数 → 线间差 / 斜率比不变, 匀速 = 直线, 末点 = 全段对数年化;
//        净值引擎 fold_book: 每行开 1/h̄ 一批, 收益在平仓行实现; 可成交 / 顺延在标签里; 控件行 分层 选 超额 e = lv − mkt | 绝对 lv;
//        kGrpSplit 时下半组与 G1 侧尾部按做空侧, 画 −(sv − mkt) | −sv, 与 S 腿同幅度).
//        分层 / LS 同一口径同一 y 轴 (不平移; LS = 两腿平均, 与单组同一资金尺度), y 范围按显示的线定 (
//        图例 (对仗 Portfolio, 逐列定宽): "LS 100%" (顶底 = 整组) / "LS 50%" … + 对数年化 (线末点) + 夏普 + 年换手;
//        夏普 = 日收益年化 Sharpe; 年换手 = 该级两腿 Δw 换手 (InspectResult::ls_turn → turn_annual); LS 随模式: 超额 = 两腿各减基准, 绝对 = 两腿本侧原值 (½ (lv + sv)); 两模式换手同一份). 分层色 = G1 冷蓝 / G20 暖橙, 中心中性灰, 线性底坡 + 两侧反向 tanh 台阶: 边上 2 组近全色, 第 3 组过半, 再往里渐淡到中心灰 (单调)
//        尾部 (InspectService TailRow): G20 / G1 内更极端的 50% / 20% / 5% 再各一条分层线 (饱和顶 / 底色, 越极端越细) + 一条尾部 LS
//        (白, 越极端越细); 相邻尾部分层线之间填淡色带 (G20↔50% / 50%↔20% / 20%↔5%, 越极端越重), 底侧同;
//        一级一个图例项 ("LS 50%" …, 列与 LS 100% 对齐) 管该级分层 + 色带 + LS, 默认隐藏; y 范围只按显示的簇合成, 开关图例即重贴
//        时序上一条可拖光标 (cursor_day, 锚在交易日边界, 双击落点, 默认末日; 与右上 K 线共享, 拖哪边另一边同步; 不做 hover 列表)
//        右 1/4 光标日截面 = 各组左图线在光标日的值 (与左图同一量) 的横柱, 按组号在 y 范围里均匀排
//        (G1 下 … G20 上), x 范围随显示中的柱 (图例 / 光标) 自动贴满;
//        柱色按组号 (与左图同一组色, 两边对得上), 柱长非单调处一眼可见
//   右上 资产 K 线 (因子具体怎么交易的; 对仗 OrderFlow 的图 2): 头行一条资产轴 + 下面选中资产的分钟 K 线.
//        R(t) = r16(x) / kRankMax ∈ [0,1] (CS = 截面秩, TS = 自身分位; 与组号同向: 1 = G20 侧), 色 = lerp(G1 蓝 → G20 橙)
//        资产轴: 光标日池内 (截面门控 g) 资产的日内均 R 降序 (左 = G20 侧), 一资产一格, 格色 = R 组色; 光标 (按下即选, 按住拖动连续换 / ◂ ▸ 步进)
//        选资产, 选中沿全局 id 粘住 (拖光标只换序不换资产, K 线不重载); 未选 / 换池轴 → 光标日 R 最高者 (无池内资产 → 全区间有效刻
//        最多者), 结果一到就有 K 线
//        K 线: 选中资产沿全部交易日的分钟 OHLC (KlineService 逐日流式, 画已发布前缀, 不等全载), 每根色 = 该刻 R(t) 组色; 池外段 (出 CS 池 /
//        当日无成交, 秩不入评估, valid% 也不计) 用不看池的秩 (rx_all) 淡色显示, 因子无输出 = 暗灰; 阴线实心 / 阳线空心.
//        换资产 → x 复位到全段历史 (不按个股上市区间; 双击 fit 同), y 按窗口内蜡烛重贴; 光标 = 与左上图同一个 cursor_day (同色, 锚在日边界), 光标日染淡色竖带
//        叠加 池等权指数线 (InspectResult::pool_index 当前 Hold 那条, 与分层同一净值引擎): 每帧按视野内个股首根蜡烛重锚 (该刻指数 = 该根 close), zoom / 平移跟着对齐; 显示时参与 y 贴合 (开关图例即重贴)
//        (默认隐藏, 点图例显; 带着全区间历史, 看不出近期)
//        再叠加滚动重锚指数线 (青, 默认显), L = 1 天: line_t = EMA_L(P)_{t−L} · exp(EMA_S(log(I_t / EMA_L(I)_{t−L}))),
//        P = 个股 close 前值填充到分钟格, I = 指数, S = 10 分钟; = "个股若跟着指数近期走应在的价", 自发贴住蜡烛, 蜡烛 − 线 = 个股相对指数的近期偏移. UI 现算, 不发请求
//        再叠加累计 rIC (右轴, 默认显): 当前 Hold 的逐行 rIC 沿全部行累加 (非 ok 行持平), 右轴贴合视野内这段; 斜率 = 该段因子有效性, 与蜡烛对着看.
//        线色按回撤渐变: 新高 = 蓝, 回撤越深越红 (√(回撤 / 全段最大回撤))
//   左下 两半对仗 (同一张 plot_dist): 左 rIC 分布 = 逐行 rank IC (ok 行) → KLL → PDF (与顶部 stat 同一组样本); 右 因子分布 = 池内有效格的 alpha
//        → KLL → 截尾 PDF. 各带 PDF 光滑拟合 (λ 自适应 sd) / mean / 时序背景 (rIC(t) | 截面均值 alpha(t)); 右半另按组边界竖切分组 (G1~G20 组色)
//   右下 期限结构: 全部持有期 (1m … T+3C) 的 L (G20 做多超额) / S (G1 做空超额) / LS 均值折线 (PCHIP 圆滑) → 这个因子预测哪个频段.
//        三组 × (毛 + 全部金额段) 一次画全, 图例 = 左上矩阵 (行 = L / S / LS / 池均, 列 = 金额段; 点格 / 行头 / 列头切), 默认只显 LS (固定冲击除外); 另画池均线 ("池均 无冲击"
//        = InspectResult::pool_ann: 池等权做多的绝对算术年化, 随池一代只算一次, 换因子共享). 口径 = 全库全时刻池化 (Σ 组内超额 / Σ 组样本数), 不是逐资产.
//        净线 <amt>w = 毛 − 该腿该持有期的均摊换手成本 (两腿都按本侧超额画, 成本都使线下移; LS = net_ls_period.
//        CostTier[c].acc[h] = Stat 契约 CostAcc, 与 Run 同一份 cost_rows: 两腿按 Δw, 增仓 Δw⁺ 付 lb_cost_{buy,sell}_<amt>w, 平仓量付
//        Config::fixed_impact, 原值扣, 不被截面去均值吃掉), UI 现算. 毛用 HoldStat:
//        Inspect 算过用算的, 没算过用 Factors 表里 (stat: 毛 + 各金额段成本, 同一份 CostTier) 的, 所以进页即有
// 触发: 高光因子 (文件 / 规范串 / 作用域) 变 且 服务空闲 → 自动起算 (InspectAutoRequest, Factors / Inspect 两页都每帧调: 在 Factors 页点行
//   即开算, 不等切页; 缓存命中只补特征); Compute 按钮 = 弃缓存整体重读.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp"
#include "gui/task_factors/services/InspectService.hpp"
#include "gui/task_factors/services/KlineService.hpp"
#include "gui/task_factors/ui/TabFactors.hpp" // FactorsUIState / FactorsUIContext
#include "shared/Analysis.hpp"                // AggPdf (KLL 成品快照)

#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Factors {

// 左上的线表 (= InspectResult::nav 的线序, layer_tail 见 InspectService.hpp). 簇 i 与 LS 线 i 同号: 簇 0 = 20 组, 簇 1 + j = 第 j 级尾部 (两腿)
inline constexpr int kLayerLines = kNavLayers;
inline constexpr int kLsLines = kNavLs;

// 从 result 派生的日级序列 (UI 线程持锁算一次, 选项变才重算)
struct InspectDerived {
  int days = 0;
  std::vector<misc::Ymd> dates;            // [days] 轴刻度用
  std::vector<float> pool_index;           // [T] InspectResult::pool_index 选定 hold 那条复利成的点位 (起点 1, 右上 K 线叠加)
  std::vector<float> x_day;                // [days + 1] 0..days (累计线横轴, 日末)
  std::vector<float> lay_ann[kLayerLines]; // [days + 1], 到第 c 日的 Σ ln(1 + 日收益) × kDaysPerYear / days × 100 (全部线同一系数; c = 0 → 0)
  std::vector<float> ls_ann[kLsLines];     // [days + 1], 同上; [days] = 全段对数年化 (图例)
  float ls_sharpe[kLsLines] = {};          // LS 线的年化 Sharpe: 日收益 mean / sd × √kDaysPerYear
  float ls_turn[kLsLines] = {};            // LS 线的年换手 (倍): turn_annual(hs, InspectResult::ls_turn) (两腿均, 每腿资金 1, 单边)
  // 各簇 / 各 LS 线的 min / max (含起点 0); 左上 y 范围按图例当前显示的簇每帧合成 (layer_range), 开关图例即重贴
  float lay_lo[kLsLines] = {}, lay_hi[kLsLines] = {};
  float ls_lo[kLsLines] = {}, ls_hi[kLsLines] = {};
  analysis::AggPdf ic_pdf;      // = InspectResult::ic_agg 选定 hold (n_pts = 0 → 样本不足不画)
  std::vector<float> ic_series; // 逐行 rank IC (ok 行, 时序) = ic_pdf 的样本; 左下左半背景
  std::vector<float> ic_cum;    // [T] 逐行 rank IC 累计 (非 ok 行持平): 右上 K 线右轴叠加
  std::vector<float> ic_dd;     // [T] ic_cum 距此前最高 (含起点 0) 的回撤 ≥ 0: 累计线着色
  float ic_dd_max = 0.f;        // ic_dd 全段最大值 (着色归一)
  AlphaDist adist;              // = InspectResult::alpha_dist (左下右半, 与持有期无关)
  factor::stat::HoldStat hs;    // 选定 hold 的二级
  bool valid = false;
};

struct InspectUIState {
  int hold_idx = 0;         // FeatureTable labels 下标 (与 Factors 页独立)
  std::string hold_for;     // hold_idx 已按其搜索持有期切过的权重行 key (换权重才切, 之后手动改 Hold 不被覆盖)
  bool absolute = false;    // 左上: 超额 (false) | 绝对 (true)
  int cursor_day = -1;      // 左上时序的光标日 (0 = 起点 … days = 末日); -1 / 越界 → 末日. 右侧截面柱按它取值
  std::string last_req_key; // 上次自动起算的 InspectRequest::key (取消后不反复重起)
  // 本帧请求交接 (TabInspect → TaskFactors): action ≠ 0 时 req_row 有效
  FactorRow req_row;
  bool req_reload = false;
  // 派生缓存 (切片 (epoch, hold) + 口径)
  uint64_t derived_epoch = ~0ull;
  int derived_sel = -1;
  bool derived_abs = false;
  InspectDerived der;
  // ---- 右上 资产 K 线 ----
  uint32_t asset_gid = ~0u; // 选中资产 (全局 id; ~0u = 未选 → 光标日 R 最高者); 跨结果经 read_scope.axis.sub_of 找回, 不在 A 轴内 → 重选
  // 资产轴派生 (键: epoch / 光标日): 光标日各资产日内均 R 降序
  uint64_t ax_epoch = ~0ull;
  int ax_day = -1;
  std::vector<int> ax_order;    // [n] 池轴下标 (R 降序, 左 = G20 侧)
  std::vector<uint32_t> ax_gid; // [n] 对应全局 id
  std::vector<float> ax_r;      // [n] R ∈ [0,1]
  bool ax_scrub = false;        // 在资产条上按下且未松开: 每帧选中鼠标所指格
  // 选中资产的 R(t) 列派生 (键: epoch / 池轴下标)
  uint64_t col_epoch = ~0ull;
  int col_sub = -1;
  std::vector<float> col_r;     // [T] 池内秩 R ∈ [0,1], < 0 = 池外 / 无值
  std::vector<float> col_r_all; // [T] 不看池的秩 (池外段靠它淡色显示), < 0 = 因子无输出
  // K 线请求期望态 (结果 key + 池轴下标 变 → gen++ → KlineService::Request; 与 KlineStream::pub 配对)
  uint32_t kline_gen = 0;
  std::string kline_key;
  int kline_sub = -1;
  int kline_days = 0;                        // 本代日数 (x 域 = days × kline_seg)
  int kline_seg = 0;                         // 本代因子层每日行数 (x = 日下标 × kline_seg + 行)
  uint32_t kline_gen_applied = ~0u;          // x 视野已复位到全段的代 (换代 = 换资产 / 换结果 → 复位)
  size_t kline_last_pub_days = ~0ull;        // 上次 y 贴合时的已发布日数 (换代 / 前缀增长 → 按当前 x 窗口重贴)
  uint8_t kline_shown = 0xFF;                // 上次 y 贴合时的指数线图例显隐位 (开关图例 → 重贴)
  int kline_sel = -1;                        // 上次 y 贴合时的 Hold (derived_sel; 换 Hold → 池指数换线 → 重贴)
  double kline_x_lo = 0.0, kline_x_hi = 0.0; // 上帧 x 视野 (y 贴合只看窗口内的蜡烛)
  // 滚动重锚指数线 (键: 代 / 已发布点数 / 派生 epoch / Hold 变才重算; 只用已发布前缀)
  uint32_t ref_gen = ~0u;
  size_t ref_pts = ~0ull;
  uint64_t ref_epoch = ~0ull;
  int ref_sel = -1;
  int ref_t0 = 0, ref_t1 = 0;  // 定义域 [t0, t1): t0 = 首根有效蜡烛 + L, t1 = 已发布日数 × kline_seg
  std::vector<float> ref_line; // [T], 定义域外无意义
};

// 自动起算 (Factors / Inspect 两页都每帧调, 不管哪页在前: Factors 页点行高光即起算): 高光因子有效 且 服务空闲 且 请求 key (因子 / 作用域)
// 与现结果和上次请求都不同 → 填 ui.req_row / req_reload = false, 返回 true (调用方 MakeInspectRequest + Request)
// 顺带默认高光: 还没高光过 (fui.view_defaulted = false) 且本作用域的表已扫出 → 高光当前 Hold 下 rIC 最高的有效权重 (无 stat → 表序第一个),
// 进 Inspect 即有东西看; 换池 / 换层后重新默认一次
bool InspectAutoRequest(FactorsService &fsvc, InspectService &isvc, FactorsUIState &fui, InspectUIState &ui, const FactorsUIContext &ctx);

// 返回值: 1 = 手动 Compute (ui.req_row / ui.req_reload 已填), -1 = Cancel, 0 = 无. fui 非 const: 页内箭头循环切 view_file.
// ksvc: 右上 K 线流 (页内按期望态直接 Request, 不经返回值)
int RenderTabInspect(FactorsService &fsvc, InspectService &isvc, KlineService &ksvc, FactorsUIState &fui, InspectUIState &ui, const FactorsUIContext &ctx);

} // namespace GUI::Factors
