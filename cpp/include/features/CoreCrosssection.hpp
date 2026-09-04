#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/Method/CS.hpp" // cs::<Tf> / cs::<Method> + NeutralRank::Ctx + NEUTRAL_RANK_* 源列
#include "misc/profiler.hpp"
#include <algorithm>
#include <vector>

// ============================================================================
// CoreCrosssection: 截面计算 (CS worker), L0 每秒 + L1 分钟边界级联
//   字段表 CS(src_lvl, src, Tf, Method) 行由 CsLevel<LVL>::run 编译期展开 (与 fstore::Level<LVL>::write_row 同构),
//   每行 = run_one<...>: gather(valid 子集 dense) → Tf::apply → Method::apply → scatter (无效资产输出 0)
//   算子契约见 DataDefine.hpp; 方法在 Method/CS.hpp (实现 precise TU). NeutralRank 的上下文 (log 市值 + 行业,
//   源列由 Method/CS.hpp 声明) 每分钟按需准备一次, 全部 NeutralRank 行复用; L0 用 NeutralRank = 编译错误.
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
      : store_(store), A_(store.query_A()), input_fp32_(A_), output_fp32_(A_), output_fp16_(A_), logmc_dense_(A_), industry_dense_(A_) {
    valid_indices_.reserve(A_);
  }

  void set_date(const std::string &date_str) { date_str_ = date_str; }

  void compute_and_store(size_t t) noexcept {
    TraceN("CS");
    TraceColor(C_Magenta);
    {
      TraceN("CS_Tick");
      run<0>(t);
    }
    if (t % 60 == 0 && t > 0) { // 分钟边界级联
      TraceN("CS_Minute");
      run<1>(t / 60);
    }
  }

  // 一行 CS 字段 (CsLevel<LVL>::run 展开调用): 源层 SRC_LVL 的列 SRC → Tf → Method → 本层列 DST
  template <size_t LVL, size_t SRC_LVL, size_t SRC, size_t DST, class Tf, class Method>
  [[gnu::always_inline]] inline void run_one(size_t t) {
    static_assert(LVL <= 1 && SRC_LVL <= 1, "CS fields live in L0/L1 and read L0/L1");
    static_assert(!Method::kNeutral || LVL == 1, "NeutralRank needs L1 context (mcap / industry)");
    const size_t ts = SRC_LVL == LVL ? t : (LVL == 0 ? L0_to_L1(t) : L1_to_L0(t)); // 跨层源列的时间索引 (L1 用分钟起始秒)

    float *y = input_fp32_.data();
    gather_(fstore::cs_read(store_, date_str_, SRC_LVL, ts, SRC), y);
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
    const _Float16 *valid_flags = fstore::cs_read(store_, date_str_, LVL, t, VALID);
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
      gather_(fstore::cs_read(store_, date_str_, 1, t, L1_Field::NEUTRAL_RANK_MCAP), logmc_dense_.data());
      cs::NeutralRank::prepare_logmc(logmc_dense_.data(), n_);
      gather_(fstore::cs_read(store_, date_str_, 1, t, L1_Field::NEUTRAL_RANK_INDUSTRY), industry_dense_.data());
      neutral_ready_ = true;
    }
    return neutral_ctx_;
  }

  // 源列 valid 子集 → dense fp32
  void gather_(const _Float16 *src, float *dst) const {
    for (size_t i = 0; i < n_; ++i)
      dst[i] = static_cast<float>(src[valid_indices_[i]]);
  }

  // dense fp32 → 全资产列 (无效资产 0) → fp16 写回
  template <size_t LVL>
  void scatter_(size_t t, size_t dst, const float *y) {
    std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
    for (size_t i = 0; i < n_; ++i)
      output_fp32_[valid_indices_[i]] = y[i];
    for (size_t a = 0; a < A_; ++a)
      output_fp16_[a] = static_cast<_Float16>(output_fp32_[a]);
    fstore::cs_write(store_, date_str_, LVL, t, dst, output_fp16_.data(), A_);
  }

  GlobalFeatureStore &store_;
  std::string date_str_;
  size_t A_;

  std::vector<size_t> valid_indices_; // 本 t 有效资产 (dense 下标 → asset id)
  size_t n_ = 0;                      // = valid_indices_.size()
  std::vector<float> input_fp32_;
  std::vector<float> output_fp32_;
  std::vector<_Float16> output_fp16_;
  std::vector<float> logmc_dense_;    // L1 中性化输入
  std::vector<float> industry_dense_; // L1 中性化输入
  bool neutral_ready_ = false;
  const cs::NeutralRank::Ctx neutral_ctx_{logmc_dense_.data(), industry_dense_.data()}; // 向量在 ctor 定长, 指针稳定
};
