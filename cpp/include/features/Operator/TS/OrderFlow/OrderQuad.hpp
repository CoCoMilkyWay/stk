#pragma once

// =============================================================================
// OrderQuad - 大小单 · 委托维度 (compute=onTick, flush=onMinute; feature_list.md 1.4)
// =============================================================================
//   每笔成交的买卖双方各是一张委托, 按 "委托大小 (元)" 各归大/小, 落四象限 (首字母 = 买方委托, 次字母 = 卖方委托), 再按主动方向 (bid / ask) 分两口:
//     bb 大买×大卖   bs 大买×小卖 (大单扫散户卖盘)   sb 小买×大卖 (散户接大单抛压)   ss 小买×小卖
//   [买委托大小][卖委托大小][主动方向] 是完全分解, 其余口径皆边缘和 (因子层做):
//     广发 L2-001 四象限 (不分向)      = bid + ask
//     海通 085 大买单主动成交额 (≥thr) = bid_bb + bid_bs;  大卖单主动 = ask_bb + ask_sb
//     智臾 小买单主动成交额 (<P50)     = bid_sb + bid_ss (p50 槽)
//   委托大小 Q (元; 输入面 = LOB_Feature::ord_*, 语义见 LimitOrderBookDefine.hpp, 解码 ord_reliable / ord_filled_before 见 DataDefine.hpp):
//     被动方 (Resting)  = 申报额 ord_orig × ord_price (与 q 轴样本 MAKER 价×量 同口径)
//     主动方            = 截止本笔 (含本笔) 的连续累计成交量 (f + v) × 成交价:  Aggressor (沪市, 不入簿) f = ord_rest;
//                         Resting (深市, 委托记录先到) f = ord_orig − ord_rest.
//                         因果口径, 系统性低估 (广发 L2-001 原文用该委托全日总成交量, 前视); 深市虽有真实申报量也不用 —— 两所统一, 截面可比优先.
//   阈值两组共 9 个 (math/distribution/SizeThresholds.hpp, 与 TradeSize 同件; 每笔各侧只落一个桶 = 满足的阈值个数):
//     B 轴 (固定金额)  4万 / 20万 / 100万 元
//     q 轴 (本股分位)  本股前 5 个交易日 委托申报额 (MAKER 事件 价×量; 市价单价 0 不入) 分位 q ∈ {50,80,84,93,95,98}%; 首日无阈值, q 轴列 0.
//   剔除: 任一侧不可信 —— ord_role==None (乱序, 未见挂单) / 占位单 (orig 不是申报量) / 被动方非 Resting / 过度抵扣 (rest > orig) ——
//         的成交不进四象限; 被剔量 = Flow {amt,vol,n}_taker_bid+ask − 四象限之和 (因子层还原, 不单列).
//   输出 (每分钟增量):
//     {amt,vol,n}_od_{bid,ask}_{bb,bs,sb,ss}_{B|q}   主动买 / 主动卖 × 四象限 成交 额(元) / 量(股) / 笔
//   委托累计成交额的二阶量 (amt2_od_{bid,ask}) 在 Moment.
//   实现: 三维桶 [主动侧][买桶][卖桶] 每笔 O(1); flush 逐非空格按 "格 ≥ 阈值 k" 分派到 4 象限 (纯加法, 不做后缀和相减 → 空象限精确 0).
//   fp16 落盘: 全部 Log Tf.
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "math/distribution/SizeThresholds.hpp"
#include <cstdint>

// 阈值枚举 (输出槽序): 前 3 = B 轴固定金额, 后 6 = q 轴分位; aq = 主动侧_象限 token (bid_bb …)
#define ORDERQUAD_THRS(T, aq) T(aq, 4w) T(aq, 20w) T(aq, 100w) T(aq, p50) T(aq, p80) T(aq, p84) T(aq, p93) T(aq, p95) T(aq, p98)
#define ORDERQUAD_ENUM(aq, b) amt_od_##aq##_##b, vol_od_##aq##_##b, n_od_##aq##_##b,

class OrderQuad {
  static constexpr size_t NFIX = 3, NQ = 6, NB = NFIX + NQ, N_DAYS = 5, ND = 3;
  static constexpr float FIX[NFIX] = {40000.0f, 200000.0f, 1000000.0f}; // 元, 升序
  static constexpr float PROBS[NQ] = {0.50f, 0.80f, 0.84f, 0.93f, 0.95f, 0.98f};

public:
  // 布局: 主动侧 外层 (bid, ask) × 象限 (bb, bs, sb, ss) × 阈值槽 × {amt, vol, n} 内层 —— y[((act·4 + quad)·NB + j)·ND + d]
  enum Out : size_t { ORDERQUAD_THRS(ORDERQUAD_ENUM, bid_bb) ORDERQUAD_THRS(ORDERQUAD_ENUM, bid_bs) ORDERQUAD_THRS(ORDERQUAD_ENUM, bid_sb) ORDERQUAD_THRS(ORDERQUAD_ENUM, bid_ss)
                          ORDERQUAD_THRS(ORDERQUAD_ENUM, ask_bb) ORDERQUAD_THRS(ORDERQUAD_ENUM, ask_bs) ORDERQUAD_THRS(ORDERQUAD_ENUM, ask_sb) ORDERQUAD_THRS(ORDERQUAD_ENUM, ask_ss) kCount };
  static_assert(kCount == 2 * 4 * NB * ND);
  float y[kCount] = {};

  explicit OrderQuad(const TickData &td) : td_(td), thr_(FIX, PROBS) {}

  inline void compute() {
    const auto &lob = td_.lob;
    const float p = lob.price;
    const float v = static_cast<float>(lob.volume);
    const float a = p * v;
    if (a <= 0.0f) [[unlikely]] // 零价 (市价委托 / 深交所零价撤单) / 零量: 无金额可分
      return;
    if (lob.order_type == L2::OrderType::MAKER) {
      thr_.add(a); // 委托申报额样本 → 次日起的 q 阈值
      return;
    }
    if (lob.order_type != L2::OrderType::TAKER)
      return;

    const size_t act = lob.order_dir == L2::OrderDirection::BID ? 0 : 1; // 主动方所在侧 (0 = 买方委托, 1 = 卖方委托)
    const size_t pas = 1 - act;
    uint32_t f[2]; // 两侧此前累计成交量 (两侧都要可信; 被动方还必须在簿: Aggressor 无申报量)
    if (!ord_filled_before(lob, act, f[act]) || !ord_filled_before(lob, pas, f[pas]) || lob.ord_role[pas] != OrderRole::Resting)
      return;
    size_t k[2];
    k[act] = thr_.bucket((static_cast<float>(f[act]) + v) * p);
    k[pas] = thr_.bucket(static_cast<float>(lob.ord_orig[pas]) * lob.ord_price[pas]);
    float (&bk)[ND] = bucket_[act][k[0]][k[1]];
    bk[0] += a, bk[1] += v, bk[2] += 1.0f;
  }

  // 格 (i, j) 对阈值 k (升序第 k 个, 1-based): 买 ≥ ⇔ i ≥ k, 卖 ≥ ⇔ j ≥ k → 象限 {0 bb, 1 bs, 2 sb, 3 ss}; 写到该阈值的输出槽
  inline void flush() {
    thr_.flush_batch();
    const size_t n = thr_.n();
    for (auto &o : y)
      o = 0.0f;
    for (size_t act = 0; act < 2; ++act)
      for (size_t i = 0; i <= n; ++i)
        for (size_t j = 0; j <= n; ++j) {
          const float (&c)[ND] = bucket_[act][i][j];
          if (c[2] == 0.0f)
            continue;
          for (size_t k = 1; k <= n; ++k) {
            const size_t quad = (i >= k ? 0 : 2) + (j >= k ? 0 : 1);
            float *o = &y[((act * 4 + quad) * NB + thr_.slot(k - 1)) * ND];
            for (size_t d = 0; d < ND; ++d)
              o[d] += c[d];
          }
        }
    clear();
  }

  void reset() {
    thr_.roll();
    clear();
  }

private:
  void clear() {
    for (auto &side : bucket_)
      for (auto &row : side)
        for (auto &c : row)
          c[0] = c[1] = c[2] = 0.0f;
  }

  const TickData &td_;
  SizeThresholds<NFIX, NQ, N_DAYS> thr_;
  float bucket_[2][NB + 1][NB + 1][ND] = {}; // [主动侧][买桶][卖桶][amt, vol, n]
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_OrderQuad(N) N(OrderQuad, (OrderQuad), (tick_data), onTick, onMinute)

// 一主动侧 (act token; S 公式侧上标; ACN 中文) × 一象限 (quad token; QEN / QCN 象限英 / 中文名; RB / RS 买 / 卖方关系中文字面 "≥" 或 "<"; FB / FS 对应公式)
// × 一阈值 (b token; TE / TC / TF = 阈值的 英文 / 中文 / 公式 字面) 的 额/量/笔 3 行
// Q^B_τ / Q^A_τ = 买方 / 卖方委托大小 (元): 被动方申报额, 主动方累计成交额 (含本笔)
#define ORDERQUAD_ROWS(X, CAT1, act, S, ACN, quad, QEN, QCN, RB, RS, FB, FS, b, TE, TC, TF)                                                                                                                                                                                                                                                                                                                                                     \
  X(amt_od_##act##_##quad##_##b, CAT1, AUTO, "Taker " S " " QEN " Trade Amount (" TE ")", ACN " " QCN "成交额(" TC ")", "分钟内 " ACN " 且 买方委托" RB TC " 且 卖方委托" RS TC " 的成交额(元; 委托大小: 被动方申报额, 主动方含本笔累计成交额)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^{T,)" S R"(}| \mathbf{1}[Q^B_\tau )" FB " " TF R"(] \mathbf{1}[Q^A_\tau )" FS " " TF R"(])", OP(OrderQuad, amt_od_##act##_##quad##_##b, Log, None)) \
  X(vol_od_##act##_##quad##_##b, CAT1, AUTO, "Taker " S " " QEN " Trade Volume (" TE ")", ACN " " QCN "成交量(" TC ")", "分钟内 " ACN " 且 买方委托" RB TC " 且 卖方委托" RS TC " 的成交量(股)", R"(\sum_{\tau \in \Delta t} |O_\tau^{T,)" S R"(}| \mathbf{1}[Q^B_\tau )" FB " " TF R"(] \mathbf{1}[Q^A_\tau )" FS " " TF R"(])", OP(OrderQuad, vol_od_##act##_##quad##_##b, Log, None))                                                        \
  X(n_od_##act##_##quad##_##b, CAT1, AUTO, "Taker " S " " QEN " Trade Count (" TE ")", ACN " " QCN "成交笔数(" TC ")", "分钟内 " ACN " 且 买方委托" RB TC " 且 卖方委托" RS TC " 的成交笔数", R"(\#O_{\Delta t}^{T,)" S R"(} \mathbf{1}[Q^B_\tau )" FB " " TF R"(] \mathbf{1}[Q^A_\tau )" FS " " TF R"(])", OP(OrderQuad, n_od_##act##_##quad##_##b, Log, None))

// 一主动侧 × 一阈值的 4 象限 × 3 = 12 行
#define ORDERQUAD_ACT_ROWS(X, CAT1, act, S, ACN, b, TE, TC, TF)                                                          \
  ORDERQUAD_ROWS(X, CAT1, act, S, ACN, bb, "BigBuy-BigSell", "大买×大卖", "≥", "≥", R"(\geq)", R"(\geq)", b, TE, TC, TF) \
  ORDERQUAD_ROWS(X, CAT1, act, S, ACN, bs, "BigBuy-SmallSell", "大买×小卖", "≥", "<", R"(\geq)", "<", b, TE, TC, TF)     \
  ORDERQUAD_ROWS(X, CAT1, act, S, ACN, sb, "SmallBuy-BigSell", "小买×大卖", "<", "≥", "<", R"(\geq)", b, TE, TC, TF)     \
  ORDERQUAD_ROWS(X, CAT1, act, S, ACN, ss, "SmallBuy-SmallSell", "小买×小卖", "<", "<", "<", "<", b, TE, TC, TF)

// 一阈值的 主动买 12 + 主动卖 12 = 24 行
#define ORDERQUAD_THR_ROWS(X, CAT1, b, TE, TC, TF)               \
  ORDERQUAD_ACT_ROWS(X, CAT1, bid, "B", "主动买", b, TE, TC, TF) \
  ORDERQUAD_ACT_ROWS(X, CAT1, ask, "A", "主动卖", b, TE, TC, TF)

#define FIELDS_L1_OrderQuad(X, CAT1)                                            \
  ORDERQUAD_THR_ROWS(X, CAT1, 4w, "4w", "4万元", R"(4 \times 10^4)")            \
  ORDERQUAD_THR_ROWS(X, CAT1, 20w, "20w", "20万元", R"(2 \times 10^5)")         \
  ORDERQUAD_THR_ROWS(X, CAT1, 100w, "100w", "100万元", R"(10^6)")               \
  ORDERQUAD_THR_ROWS(X, CAT1, p50, "P50", "前5日委托额P50", R"(Q_{50\%}^{5d})") \
  ORDERQUAD_THR_ROWS(X, CAT1, p80, "P80", "前5日委托额P80", R"(Q_{80\%}^{5d})") \
  ORDERQUAD_THR_ROWS(X, CAT1, p84, "P84", "前5日委托额P84", R"(Q_{84\%}^{5d})") \
  ORDERQUAD_THR_ROWS(X, CAT1, p93, "P93", "前5日委托额P93", R"(Q_{93\%}^{5d})") \
  ORDERQUAD_THR_ROWS(X, CAT1, p95, "P95", "前5日委托额P95", R"(Q_{95\%}^{5d})") \
  ORDERQUAD_THR_ROWS(X, CAT1, p98, "P98", "前5日委托额P98", R"(Q_{98\%}^{5d})")
