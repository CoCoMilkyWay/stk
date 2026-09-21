#pragma once

// =============================================================================
// OrderQuad - 大小单 · 委托维度 (compute=onTick, flush=onMinute; feature_list.md 1.4)
// =============================================================================
//   每笔成交的买卖双方各是一张委托, 按 "委托大小 (元)" 各归大/小, 落四象限 (首字母 = 买方委托, 次字母 = 卖方委托; 与主动方向无关):
//     bb 大买×大卖   bs 大买×小卖 (大单扫散户卖盘)   sb 小买×大卖 (散户接大单抛压)   ss 小买×小卖
//   委托大小 Q (股 → 元 = 股 × 本笔成交价; 输入面 = LOB_Feature::ord_*, 语义见 LimitOrderBookDefine.hpp):
//     被动方 (Resting)  = 申报量 ord_orig
//     主动方            = 截止本笔 (含本笔) 的连续累计成交量 f + v:  Aggressor (沪市, 不入簿) f = ord_rest;
//                         Resting (深市, 委托记录先到) f = ord_orig − ord_rest.
//                         因果口径, 系统性低估 (广发 L2-001 原文用该委托全日总成交量, 前视); 深市虽有真实申报量也不用 —— 两所统一, 截面可比优先.
//   阈值两组共 9 个, 与 TradeSize 同结构 (合并升序, 每笔各侧只落一个桶 = 满足的阈值个数):
//     B 轴 (固定金额)  4万 / 20万 / 100万 元
//     q 轴 (本股分位)  本股前 5 个交易日 委托申报额 (MAKER 事件 价×量; 市价单价 0 不入) 分位 q ∈ {50,80,84,93,95,98}%; 首日无阈值, q 轴列 0.
//   剔除: 任一侧不可信 —— ord_role==None (乱序, 未见挂单) / 占位单 (ord_flag ∈ {OUT_OF_ORDER, ZERO_PRICE}, orig 不是申报量) /
//         被动方非 Resting / 过度抵扣 (rest > orig) —— 的成交不进四象限; 被剔量 = Flow {amt,vol,n}_taker_bid+ask − 四象限之和 (因子层还原, 不单列).
//   输出 (每分钟增量):
//     {amt,vol,n}_od_{bb,bs,sb,ss}_{B|q}   四象限成交 额(元) / 量(股) / 笔
//     od_sq_{bid,ask}                      Σ ((f+v)² − f²)·p²  该侧委托累计成交额平方的增量 (各侧独立判可信; 日 Σ = 各委托成交额平方和 → 买/卖单集中度)
//     amt_od_taker_{bid,ask}_lt_p50        该侧为主动方 且 Q < P50 的成交额 (小单主动成交度; 首日 0)
//   实现: 二维桶 [买桶][卖桶] 每笔 O(1); flush 逐非空格按 "格 ≥ 阈值 k" 分派到 4 象限 (纯加法, 不做后缀和相减 → 空象限精确 0).
//   fp16 落盘: 全部 Log Tf.
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "math/distribution/DailyQuantile.hpp"
#include <cmath>
#include <cstdint>
#include <utility>

// 阈值枚举 (输出槽序): 前 3 = B 轴固定金额, 后 6 = q 轴分位
#define ORDERQUAD_THRS(T) T(4w) T(20w) T(100w) T(p50) T(p80) T(p84) T(p93) T(p95) T(p98)
#define ORDERQUAD_ENUM_BB(b) amt_od_bb_##b, vol_od_bb_##b, n_od_bb_##b,
#define ORDERQUAD_ENUM_BS(b) amt_od_bs_##b, vol_od_bs_##b, n_od_bs_##b,
#define ORDERQUAD_ENUM_SB(b) amt_od_sb_##b, vol_od_sb_##b, n_od_sb_##b,
#define ORDERQUAD_ENUM_SS(b) amt_od_ss_##b, vol_od_ss_##b, n_od_ss_##b,

class OrderQuad {
  static constexpr size_t NFIX = 3, NQ = 6, NB = NFIX + NQ, N_DAYS = 5;
  static constexpr float FIX[NFIX] = {40000.0f, 200000.0f, 1000000.0f}; // 元, 升序
  static constexpr float PROBS[NQ] = {0.50f, 0.80f, 0.84f, 0.93f, 0.95f, 0.98f};

public:
  // 布局: 象限 外层 (bb, bs, sb, ss) × 阈值槽 中层 × {amt, vol, n} 内层 —— y[(quad·NB + j)·3 + d]; 其后 od_sq ×2, lt_p50 ×2
  enum Out : size_t { ORDERQUAD_THRS(ORDERQUAD_ENUM_BB) ORDERQUAD_THRS(ORDERQUAD_ENUM_BS) ORDERQUAD_THRS(ORDERQUAD_ENUM_SB) ORDERQUAD_THRS(ORDERQUAD_ENUM_SS) od_sq_bid,
                      od_sq_ask,
                      amt_od_taker_bid_lt_p50,
                      amt_od_taker_ask_lt_p50,
                      kCount };
  static_assert(od_sq_bid == 4 * NB * 3 && kCount == 4 * NB * 3 + 4);
  float y[kCount] = {};

  explicit OrderQuad(const TickData &td) : td_(td), dq_(PROBS) { rebuild(); }

  inline void compute() {
    const auto &lob = td_.lob;
    const float p = lob.price;
    const float v = static_cast<float>(lob.volume);
    const float a = p * v;
    if (a <= 0.0f) [[unlikely]] // 零价 (市价委托 / 深交所零价撤单) / 零量: 无金额可分
      return;
    if (lob.order_type == L2::OrderType::MAKER) {
      dq_.add(std::log(a)); // 委托申报额样本 → 次日起的 q 阈值
      return;
    }
    if (lob.order_type != L2::OrderType::TAKER)
      return;

    // 两侧此前累计成交量; 集中度各侧独立 (一侧不可信不拖累另一侧)
    uint32_t f[2];
    bool ok[2];
    for (size_t s = 0; s < 2; ++s) {
      ok[s] = filled_before(lob, s, f[s]);
      if (ok[s])
        sq_[s] += (2.0f * static_cast<float>(f[s]) * v + v * v) * p * p;
    }

    const size_t act = lob.order_dir == L2::OrderDirection::BID ? 0 : 1;  // 主动方所在侧 (0 = 买方委托, 1 = 卖方委托)
    if (!(ok[0] && ok[1]) || lob.ord_role[1 - act] != OrderRole::Resting) // 被动方必须在簿 (Aggressor 无申报量)
      return;

    size_t k[2];
    float q_act = 0.0f;
    for (size_t s = 0; s < 2; ++s) {
      const float qty = s == act ? static_cast<float>(f[s]) + v : static_cast<float>(lob.ord_orig[s]);
      const float q = qty * p; // 委托大小 (元)
      size_t b = 0;
      while (b < n_thr_ && q >= thr_[b])
        ++b;
      k[s] = b;
      if (s == act)
        q_act = q;
    }
    float (&bk)[3] = bucket_[k[0]][k[1]];
    bk[0] += a, bk[1] += v, bk[2] += 1.0f;
    if (has_q_ && q_act < thr_q_[0]) // P50 = thr_q_[0] (PROBS 升序)
      lt_[act] += a;
  }

  // 格 (i, j) 对阈值 k (升序第 k 个, 1-based): 买 ≥ ⇔ i ≥ k, 卖 ≥ ⇔ j ≥ k → 象限 {0 bb, 1 bs, 2 sb, 3 ss}; 写到该阈值的输出槽
  inline void flush() {
    dq_.flush_batch();
    for (size_t o = 0; o < od_sq_bid; ++o)
      y[o] = 0.0f;
    for (size_t i = 0; i <= n_thr_; ++i)
      for (size_t j = 0; j <= n_thr_; ++j) {
        const float (&c)[3] = bucket_[i][j];
        if (c[2] == 0.0f)
          continue;
        for (size_t k = 1; k <= n_thr_; ++k) {
          const size_t quad = (i >= k ? 0 : 2) + (j >= k ? 0 : 1);
          float *o = &y[(quad * NB + slot_[k - 1]) * 3];
          o[0] += c[0], o[1] += c[1], o[2] += c[2];
        }
      }
    y[od_sq_bid] = sq_[0];
    y[od_sq_ask] = sq_[1];
    y[amt_od_taker_bid_lt_p50] = lt_[0];
    y[amt_od_taker_ask_lt_p50] = lt_[1];
    clear();
  }

  void reset() {
    float lq[NQ];
    has_q_ = dq_.roll(lq);
    if (has_q_)
      for (size_t j = 0; j < NQ; ++j)
        thr_q_[j] = std::exp(lq[j]);
    rebuild();
    clear();
  }

private:
  // 该侧委托截止本笔之前的累计成交量 (股); false = 该侧不可信 (无 id / 乱序占位单 / 过度抵扣)
  static inline bool filled_before(const LOB_Feature &lob, size_t s, uint32_t &f) {
    switch (lob.ord_role[s]) {
    case OrderRole::Aggressor:
      f = lob.ord_rest[s];
      return true;
    case OrderRole::Resting:
      if (lob.ord_flag[s] == OrderFlags::OUT_OF_ORDER || lob.ord_flag[s] == OrderFlags::ZERO_PRICE || lob.ord_rest[s] > lob.ord_orig[s])
        return false;
      f = lob.ord_orig[s] - lob.ord_rest[s];
      return true;
    default:
      return false;
    }
  }

  // 固定 + 分位 阈值合并升序 (插入排序, 9 个), slot_ 记每个阈值的输出槽 (0..2 固定, 3..8 分位)
  void rebuild() {
    n_thr_ = NFIX + (has_q_ ? NQ : 0);
    for (size_t k = 0; k < n_thr_; ++k)
      thr_[k] = k < NFIX ? FIX[k] : thr_q_[k - NFIX], slot_[k] = k;
    for (size_t k = 1; k < n_thr_; ++k)
      for (size_t m = k; m > 0 && thr_[m] < thr_[m - 1]; --m)
        std::swap(thr_[m], thr_[m - 1]), std::swap(slot_[m], slot_[m - 1]);
  }

  void clear() {
    for (auto &row : bucket_)
      for (auto &c : row)
        c[0] = c[1] = c[2] = 0.0f;
    sq_[0] = sq_[1] = 0.0f;
    lt_[0] = lt_[1] = 0.0f;
  }

  const TickData &td_;
  DailyQuantile<NQ, N_DAYS> dq_;
  float thr_q_[NQ] = {}; // 元, 升序 (分位单调)
  bool has_q_ = false;
  float thr_[NB] = {};   // 当日生效阈值, 升序
  size_t slot_[NB] = {}; // thr_[k] 对应的输出槽
  size_t n_thr_ = 0;
  float bucket_[NB + 1][NB + 1][3] = {}; // [买桶][卖桶][amt, vol, n]
  float sq_[2] = {};                     // [侧] 累计成交额平方增量
  float lt_[2] = {};                     // [侧] 主动小单成交额
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_OrderQuad(N) N(OrderQuad, (OrderQuad), (tick_data), onTick, onMinute)

// 一象限 (quad token; QEN / QCN 象限英 / 中文名; RB / RS 买 / 卖方关系中文字面 "≥" 或 "<"; FB / FS 对应公式) × 一阈值 (b token; TE / TC / TF = 阈值的 英文 / 中文 / 公式 字面) 的 额/量/笔 3 行
// Q^B_τ / Q^A_τ = 买方 / 卖方委托大小 (元): 被动方申报额, 主动方累计成交额 (含本笔)
#define ORDERQUAD_ROWS(X, CAT1, quad, QEN, QCN, RB, RS, FB, FS, b, TE, TC, TF)                                                                                                                                                                                                                                                                                                     \
  X(amt_od_##quad##_##b, CAT1, AUTO, QEN " Trade Amount (" TE ")", QCN "成交额(" TC ")", "分钟内 买方委托" RB TC " 且 卖方委托" RS TC " 的成交额(元; 委托大小: 被动方申报额, 主动方含本笔累计成交额)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^T| \mathbf{1}[Q^B_\tau )" FB " " TF R"(] \mathbf{1}[Q^A_\tau )" FS " " TF R"(])", OP(OrderQuad, amt_od_##quad##_##b, Log, None)) \
  X(vol_od_##quad##_##b, CAT1, AUTO, QEN " Trade Volume (" TE ")", QCN "成交量(" TC ")", "分钟内 买方委托" RB TC " 且 卖方委托" RS TC " 的成交量(股)", R"(\sum_{\tau \in \Delta t} |O_\tau^T| \mathbf{1}[Q^B_\tau )" FB " " TF R"(] \mathbf{1}[Q^A_\tau )" FS " " TF R"(])", OP(OrderQuad, vol_od_##quad##_##b, Log, None))                                                        \
  X(n_od_##quad##_##b, CAT1, AUTO, QEN " Trade Count (" TE ")", QCN "成交笔数(" TC ")", "分钟内 买方委托" RB TC " 且 卖方委托" RS TC " 的成交笔数", R"(\#O_{\Delta t}^T \mathbf{1}[Q^B_\tau )" FB " " TF R"(] \mathbf{1}[Q^A_\tau )" FS " " TF R"(])", OP(OrderQuad, n_od_##quad##_##b, Log, None))

// 一阈值的 4 象限 × 3 = 12 行
#define ORDERQUAD_THR_ROWS(X, CAT1, b, TE, TC, TF)                                                          \
  ORDERQUAD_ROWS(X, CAT1, bb, "BigBuy-BigSell", "大买×大卖", "≥", "≥", R"(\geq)", R"(\geq)", b, TE, TC, TF) \
  ORDERQUAD_ROWS(X, CAT1, bs, "BigBuy-SmallSell", "大买×小卖", "≥", "<", R"(\geq)", "<", b, TE, TC, TF)     \
  ORDERQUAD_ROWS(X, CAT1, sb, "SmallBuy-BigSell", "小买×大卖", "<", "≥", "<", R"(\geq)", b, TE, TC, TF)     \
  ORDERQUAD_ROWS(X, CAT1, ss, "SmallBuy-SmallSell", "小买×小卖", "<", "<", "<", "<", b, TE, TC, TF)

#define FIELDS_L1_OrderQuad(X, CAT1)                                                                                                                                                                                                                                                                                                \
  ORDERQUAD_THR_ROWS(X, CAT1, 4w, "4w", "4万元", R"(4 \times 10^4)")                                                                                                                                                                                                                                                                \
  ORDERQUAD_THR_ROWS(X, CAT1, 20w, "20w", "20万元", R"(2 \times 10^5)")                                                                                                                                                                                                                                                             \
  ORDERQUAD_THR_ROWS(X, CAT1, 100w, "100w", "100万元", R"(10^6)")                                                                                                                                                                                                                                                                   \
  ORDERQUAD_THR_ROWS(X, CAT1, p50, "P50", "前5日委托额P50", R"(Q_{50\%}^{5d})")                                                                                                                                                                                                                                                     \
  ORDERQUAD_THR_ROWS(X, CAT1, p80, "P80", "前5日委托额P80", R"(Q_{80\%}^{5d})")                                                                                                                                                                                                                                                     \
  ORDERQUAD_THR_ROWS(X, CAT1, p84, "P84", "前5日委托额P84", R"(Q_{84\%}^{5d})")                                                                                                                                                                                                                                                     \
  ORDERQUAD_THR_ROWS(X, CAT1, p93, "P93", "前5日委托额P93", R"(Q_{93\%}^{5d})")                                                                                                                                                                                                                                                     \
  ORDERQUAD_THR_ROWS(X, CAT1, p95, "P95", "前5日委托额P95", R"(Q_{95\%}^{5d})")                                                                                                                                                                                                                                                     \
  ORDERQUAD_THR_ROWS(X, CAT1, p98, "P98", "前5日委托额P98", R"(Q_{98\%}^{5d})")                                                                                                                                                                                                                                                     \
  X(od_sq_bid, CAT1, AUTO, "Bid Order Fill Concentration Increment", "买委托成交额平方增量", "分钟内 Σ((f+v)²−f²)·p², f=买方委托此前累计成交量(股), v=本笔量; 日累计=各买委托成交额平方和(元²)", R"(\sum_{\tau \in \Delta t} P_\tau^2 \left[(f^B_\tau + |O_\tau^T|)^2 - (f^B_\tau)^2\right])", OP(OrderQuad, od_sq_bid, Log, None)) \
  X(od_sq_ask, CAT1, AUTO, "Ask Order Fill Concentration Increment", "卖委托成交额平方增量", "分钟内 Σ((f+v)²−f²)·p², f=卖方委托此前累计成交量(股), v=本笔量; 日累计=各卖委托成交额平方和(元²)", R"(\sum_{\tau \in \Delta t} P_\tau^2 \left[(f^A_\tau + |O_\tau^T|)^2 - (f^A_\tau)^2\right])", OP(OrderQuad, od_sq_ask, Log, None)) \
  X(amt_od_taker_bid_lt_p50, CAT1, AUTO, "Small Bid Order Taker Amount (< P50)", "小买单主动成交额", "分钟内 主动买 且 买方委托大小<前5日委托额P50 的成交额(元)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^{T,B}| \mathbf{1}[Q^B_\tau < Q_{50\%}^{5d}])", OP(OrderQuad, amt_od_taker_bid_lt_p50, Log, None))                      \
  X(amt_od_taker_ask_lt_p50, CAT1, AUTO, "Small Ask Order Taker Amount (< P50)", "小卖单主动成交额", "分钟内 主动卖 且 卖方委托大小<前5日委托额P50 的成交额(元)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^{T,A}| \mathbf{1}[Q^A_\tau < Q_{50\%}^{5d}])", OP(OrderQuad, amt_od_taker_ask_lt_p50, Log, None))
