#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/Method/CS.hpp" // cs::<Tf> / cs::<Method> + NeutralRank::Ctx + NEUTRAL_RANK_* 源列
#include "misc/profiler.hpp"
#include <cstring>
#include <vector>

// ============================================================================
// CoreCrosssection: 截面计算 (CS worker), L0 每秒 + L1 分钟边界级联
//   字段表 CS(src_lvl, src, Tf, Method) 行由 CsLevel<LVL>::run 编译期展开 (与 fstore::RowWriter<LVL> 同构),
//   每行 = run_one<...>: gather(valid 子集 dense) → Tf::apply → Method::apply → scatter (无效资产输出 0)
//   算子契约见 DataDefine.hpp; 方法在 Method/CS.hpp (实现 precise TU). NeutralRank 的上下文 (log 市值 + 行业,
//   源列由 Method/CS.hpp 声明) 每分钟按需准备一次, 全部 NeutralRank 行复用; L0 用 NeutralRank = 编译错误.
//
//   一致性锚 (回测 = 实盘): CS 只通过秒网格张量行 (cs_col + _data_valid) 看世界,
//   看不到 tick / LOB / 事件到达顺序. TS 行是资产局部纯函数 (见 CoreSequential.hpp),
//   所以 CS 在 t 的输入对重放调度不变 —— 一致性由这个输入契约保证, 与"CS 是流式
//   伴随还是整日后扫"无关. 回测按日门控 (cs_open 等全部 TS 写完, 见 FeatureStore.hpp),
//   实盘由 feed 层按墙钟逐秒驱动 —— 两种调度下每行的输入相同, 输出逐值相同.
//
//   无回环: 特征层是单向 DAG (逐笔 → TS → 张量 → CS 列 = 终端输出, TS 不回读 CS).
//   任何"用截面结果再加工"的表达放因子层 (离线读落盘特征拼接, 跨资产/跨时间/嵌套
//   均可), 不得进特征层 —— 这是两层的分界, 也是重放可重排的前提.
//
//   实盘固有 gap 在 feed 层: 秒 t 的实盘截面实际打在墙钟 t+ε (推流延迟, 两所不同步),
//   回测张量行是 ε=0 的理想值. 此 gap 与本层的调度/实现形态无关.
// ============================================================================

// ============================================================================
// CsLevel<LVL>::run: 该层字段表里每个 CS(...) 行 → core.run_one<...>(t); 其他来源的行展开为空
// ============================================================================
#define CS_RUN_CS(code, src_lvl, s, tf, m) core.template run_one<kLevel, src_lvl, L##src_lvl##_Field::s, FO::code, cs::tf, cs::m>(t);
#define CS_RUN_OP(code, ...)
#define CS_RUN_LABEL(code)
#define CS_RUN_FLAG(code)
#define CS_RUN_META(code, w)
#define CS_RUN_ONE(code, c1, c2, norm, en, cn, desc, formula, src) SRC_DISPATCH(CS_RUN, code, src)

template <size_t LVL>
struct CsLevel;
#define CS_LEVEL_TRAITS(name, num, fields, rows, psd, columnar)                                             \
  template <>                                                                                               \
  struct CsLevel<num> {                                                                                     \
    static constexpr size_t kLevel = num;                                                                   \
    template <class Core>                                                                                   \
    [[gnu::always_inline]] static inline void run([[maybe_unused]] Core &core, [[maybe_unused]] size_t t) { \
      namespace FO = name##_Field;                                                                          \
      fields(CS_RUN_ONE)                                                                                    \
    }                                                                                                       \
  };
ALL_LEVELS(CS_LEVEL_TRAITS)
#undef CS_LEVEL_TRAITS

class CoreCrosssection {
public:
  explicit CoreCrosssection(GlobalFeatureStore &store)
      : A_(store.query_A()), input_fp32_(A_), logmc_dense_(A_), industry_dense_(A_) {
    valid_indices_.reserve(A_);
  }

  // day = 本日读写句柄 (store.cs_open, 已按日门控: 三层张量整体就绪)
  void set_day(const GlobalFeatureStore::CsDay &day) { day_ = day; }

  void compute_and_store(size_t t) noexcept {
    TraceN("CS");
    TraceColor(C_Magenta);
    {
      TraceN("CS_Tick");
      run<0>(t);
    }
    if (t % 60 == 59) { // 分钟末级联: 分钟 m 在其最后一秒算, 覆盖 m=0..254 (原先 t%60==0 && t>0 漏掉分钟 0、多算哨兵行)
      TraceN("CS_Minute");
      run<1>(L0_to_L1(t));
    }
  }

  // 一行 CS 字段 (CsLevel<LVL>::run 展开调用): 源层 SRC_LVL 的列 SRC → Tf → Method → 本层列 DST
  template <size_t LVL, size_t SRC_LVL, size_t SRC, size_t DST, class Tf, class Method>
  [[gnu::always_inline]] inline void run_one(size_t t) {
    static_assert(LVL <= 1 && SRC_LVL <= 1, "CS fields live in L0/L1 and read L0/L1");
    static_assert(!Method::kNeutral || LVL == 1, "NeutralRank needs L1 context (mcap / industry)");
    const size_t ts = SRC_LVL == LVL ? t : (LVL == 0 ? L0_to_L1(t) : L1_to_L0(t)); // 跨层源列的时间索引 (L1 用分钟起始秒)

    float *y = input_fp32_.data();
    gather_(fstore::cs_col<SRC_LVL>(day_, ts, SRC), y);
    Tf::apply(y, n_);
    if constexpr (Method::kNeutral)
      Method::apply(y, n_, neutral_(t));
    else
      Method::apply(y, n_);
    scatter_<LVL>(t, DST, y);
  }

private:
  template <size_t LVL>
  void run(size_t t) {
    constexpr size_t VALID = LVL == 0 ? size_t(L0_Field::_data_valid) : size_t(L1_Field::_data_valid);
    const _Float16 *valid_flags = fstore::cs_col<LVL>(day_, t, VALID);
    valid_indices_.clear();
    for (size_t a = 0; a < A_; ++a)
      if (static_cast<float>(valid_flags[a]) > 0.5f)
        valid_indices_.push_back(a);
    if (valid_indices_.empty())
      return;
    n_ = valid_indices_.size();
    neutral_ready_ = false;
    CsLevel<LVL>::run(*this, t);
  }

  // NeutralRank 上下文 (L1): 首个 NeutralRank 行触发, 本分钟内复用
  const cs::NeutralRank::Ctx &neutral_(size_t t) {
    if (!neutral_ready_) {
      gather_(fstore::cs_col<1>(day_, t, L1_Field::NEUTRAL_RANK_MCAP), logmc_dense_.data());
      cs::NeutralRank::prepare_logmc(logmc_dense_.data(), n_);
      gather_(fstore::cs_col<1>(day_, t, L1_Field::NEUTRAL_RANK_INDUSTRY), industry_dense_.data());
      neutral_ready_ = true;
    }
    return neutral_ctx_;
  }

  // 源列 valid 子集 → dense fp32
  void gather_(const _Float16 *src, float *dst) const {
    for (size_t i = 0; i < n_; ++i)
      dst[i] = static_cast<float>(src[valid_indices_[i]]);
  }

  // dense fp32 → 目标列就地写 (清零后只写有效资产): 无中转缓冲, 全 A 只扫一遍
  template <size_t LVL>
  void scatter_(size_t t, size_t dst, const float *y) {
    feature_storage_t *col = fstore::cs_col<LVL>(day_, t, dst);
    std::memset(col, 0, A_ * sizeof(feature_storage_t)); // fp16 的 0 是全零位
    for (size_t i = 0; i < n_; ++i)
      col[valid_indices_[i]] = static_cast<feature_storage_t>(y[i]);
  }

  GlobalFeatureStore::CsDay day_{}; // 本日读写句柄, set_day 换入
  size_t A_;

  std::vector<size_t> valid_indices_; // 本 t 有效资产 (dense 下标 → asset id)
  size_t n_ = 0;                      // = valid_indices_.size()
  std::vector<float> input_fp32_;
  std::vector<float> logmc_dense_;    // L1 中性化输入
  std::vector<float> industry_dense_; // L1 中性化输入
  bool neutral_ready_ = false;
  const cs::NeutralRank::Ctx neutral_ctx_{logmc_dense_.data(), industry_dense_.data()}; // 向量在 ctor 定长, 指针稳定
};
