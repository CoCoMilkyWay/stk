#pragma once

#include "features/MetaFlag.hpp"  // fmeta::data_valid (之前经 NodesGenerated→Meta.hpp 传递获得)
#include "features/Method/CS.hpp" // cs::<Tf> / cs::<Method> + NeutralRank::Ctx + NEUTRAL_RANK_* 源列
#include "features/Storage/FeatureDay.hpp"
#include "features/Storage/FeatureStore.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"
#include "shared/FeatureLib.hpp"
#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <type_traits>
#include <vector>

// ============================================================================
// CoreCs<T>: 截面计算 (CS worker), 档 T 有 L_sec → L_sec 每秒 + L_min 分钟边界级联; 无 L_sec (Bar) → L_min 每分钟;
//   10 分钟箱末分钟之后级联 L_10min, 末拍之后级联 L_day (收盘行); 档有该层才算
//   与 CoreTs 对仗: begin_day / compute_and_store (节拍层 kBeatLevel, 级联 L_min / L_10min / L_day) / end_day, 逐层 run<LVL>
//   字段表 CS(ns, src_lvl, src, Tf, Method) 行由 CsLevel<T, LVL>::run 编译期展开 (与 fstore::RowWriter<T, LVL> 同构),
//   每行 = run_one<...>: gather(valid 子集 dense) → Tf::apply → Method::apply → scatter (无效资产输出 0); 源层 == 本层
//   节点契约见 DataDefine.hpp; 方法在 Method/CS.hpp (实现 precise TU). NeutralRank 的上下文 (log 市值 + 行业,
//   源列由 Method/CS.hpp 声明, 取本层同名列) 每拍按需准备一次, 全部 NeutralRank 行复用; L_sec 用 NeutralRank = 编译错误.
//
//   旁读 (TIER_SIDES, 只在 L_min): 源档 ns ≠ 本档的行 + 本档没有的 NeutralRank 上下文列 = 旁读列集 (CsSide, 编译期去重).
//   begin_day 从旁读档的库整日读入这些列 (按本档 A 轴取子集); 当日缺库 → 整日 NaN + 日志, 不等不算.
//   旁读列是上游档落盘的张量行, 与本档张量行同属"CS 只看张量行"的输入契约.
//
//   一致性锚 (回测 = 实盘): CS 只通过秒网格张量行 (cs_col + cs_valid) 看世界,
//   看不到 tick / 订单簿 / 事件到达顺序. TS 行是资产局部纯函数 (见 CoreLob.hpp),
//   所以 CS 在 t 的输入对重放调度不变 —— 一致性由这个输入契约保证, 与"CS 是流式
//   伴随还是整日后扫"无关. 回测按日流控 (cs_try_open 等全部 TS 写完, 见 FeatureStore.hpp),
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
// CsLevel<T, LVL>: 档 T 该层字段表里每个 CS(...) 行 → run: core.run_one<...>(t) / kRefs: 源列引用; 其他来源的行展开为空
// (档缺的层无特化, run<LVL> 编译期跳过)
// ============================================================================
#define CS_RUN_CS(code, ns, src_lvl, s, tf, m) core.template run_one<kLevel, ns::Tier, src_lvl, ns::src_lvl##_Field::s, FO::code, cs::tf, cs::m>(t);
#define CS_RUN_TS(code, ...)
#define CS_RUN_LABEL(code)
#define CS_RUN_FLAG(code)
#define CS_RUN_ONE(code, c1, c2, en, cn, desc, formula, src) SRC_DISPATCH(CS_RUN, code, src)

// CS 行的源列引用 (旁读列集按它推导)
struct CsRef {
  TierId tier;
  size_t lvl;
  size_t field;
  bool neutral; // 方法要 NeutralRank 上下文
};
#define CS_REF_CS(code, ns, src_lvl, s, tf, m) CsRef{ns::Tier::kId, src_lvl, ns::src_lvl##_Field::s, cs::m::kNeutral},
#define CS_REF_TS(code, ...)
#define CS_REF_LABEL(code)
#define CS_REF_FLAG(code)
#define CS_REF_ONE(code, c1, c2, en, cn, desc, formula, src) SRC_DISPATCH(CS_REF, code, src)

template <class T, size_t LVL>
struct CsLevel;
#define CS_LEVEL_TRAITS(T, ns, name, num, fields, psd)                                                          \
  template <>                                                                                                   \
  struct CsLevel<ns::Tier, num> {                                                                               \
    static constexpr size_t kLevel = num;                                                                       \
    static constexpr bool kEmpty = count_of_kind(ns::name##_FIELD_INFO, FeatureDataType::CS) == 0;              \
    /* 首项哨兵 (档号越界, 不匹配任何档): 层内无 CS 行时数组也非空 */                                           \
    static constexpr CsRef kRefs[] = {CsRef{static_cast<TierId>(TIER_COUNT), 0, 0, false}, fields(CS_REF_ONE)}; \
    template <class Core>                                                                                       \
    [[gnu::always_inline]] static inline void run([[maybe_unused]] Core &core, [[maybe_unused]] size_t t) {     \
      namespace FO = ns::name##_Field;                                                                          \
      fields(CS_RUN_ONE)                                                                                        \
    }                                                                                                           \
  };
TIER_LEVELS(CS_LEVEL_TRAITS)
#undef CS_LEVEL_TRAITS

#define CS_STR_(x) #x
#define CS_STR(x) CS_STR_(x)

// 档 T 的 LVL 层 NeutralRank 上下文列在不在本档; 不在 → 从旁读档取 (只许 L_min)
template <class T, size_t LVL>
constexpr bool owns_neutral_ctx() {
  return T::has(LVL) && field_of<T>(LVL, CS_STR(NEUTRAL_RANK_MCAP)) < T::LEVELS[LVL].field_count &&
         field_of<T>(LVL, CS_STR(NEUTRAL_RANK_INDUSTRY)) < T::LEVELS[LVL].field_count;
}
template <class T, size_t LVL>
constexpr bool uses_neutral() {
  if constexpr (T::has(LVL)) {
    for (const CsRef &r : CsLevel<T, LVL>::kRefs)
      if (r.neutral)
        return true;
  }
  return false;
}

// 旁读列集: 档 T 的 L_min 截面要读的旁读档 S (= TierSide<T>) L_min 列下标 (升序去重). 无旁读档 → 空
template <class T, class S = typename TierSide<T>::type>
struct CsSide {
  static constexpr size_t N = 0;
  static constexpr std::array<size_t, 0> kFields{};
};
template <class T, class S>
  requires(!std::is_void_v<S>)
struct CsSide<T, S> {
  static constexpr size_t F = S::LEVELS[L_min].field_count;
  static constexpr std::array<bool, F> kMask = [] {
    std::array<bool, F> m{};
    if constexpr (T::has(L_min)) {
      for (const CsRef &r : CsLevel<T, L_min>::kRefs)
        if (r.tier == S::kId)
          m[r.field] = true;
    }
    if (uses_neutral<T, L_min>() && !owns_neutral_ctx<T, L_min>()) {
      m[field_of<S>(L_min, CS_STR(NEUTRAL_RANK_MCAP))] = true;
      m[field_of<S>(L_min, CS_STR(NEUTRAL_RANK_INDUSTRY))] = true;
    }
    return m;
  }();
  static constexpr size_t N = [] {
    size_t n = 0;
    for (const bool b : kMask)
      n += b;
    return n;
  }();
  static constexpr std::array<size_t, N> kFields = [] {
    std::array<size_t, N> f{};
    for (size_t i = 0, n = 0; i < F; ++i)
      if (kMask[i])
        f[n++] = i;
    return f;
  }();
  static constexpr size_t index_of(size_t field) {
    for (size_t i = 0; i < N; ++i)
      if (kFields[i] == field)
        return i;
    return N;
  }
};

template <class T>
class CoreCs {
public:
  using Day = StoreDay<T>;
  using Side = CsSide<T>;
  // 节拍层: 有 L_sec 的档逐秒 (实盘 feed 按墙钟逐秒驱动), 无 L_sec 的档 (Bar, 分钟 K 源) 逐分钟
  static constexpr size_t kBeatLevel = T::kFirstLevel;

  // libs[T::kId] = 本档库 (轴 sub → global); libs[旁读档] = 旁读档的库 (无旁读列时不读)
  CoreCs(FeatureStore<T> &store, const PerTier<FeatureLib> &libs)
      : A_(store.query_A()), libs_(libs), input_fp32_(A_), logmc_dense_(A_), industry_dense_(A_) {
    valid_indices_.reserve(A_);
    if constexpr (Side::N > 0) {
      using S = typename TierSide<T>::type;
      const LibAxis &axis = libs[T::kId].axis;
      const FeatureLib &side = libs[S::kId];
      assert(side.tier == S::kId && axis.size() == A_);
      side_.resize(Side::N * level_rows(L_min) * A_);
      side_full_.resize(Side::N * level_rows(L_min) * side.axis.size());
      side_sub_.resize(A_);
      for (size_t a = 0; a < A_; ++a) {
        side_sub_[a] = side.axis.sub_of(axis.global(a));
        assert(side_sub_[a] < side.axis.size() && "本档资产不在旁读档 A 轴内");
      }
    }
  }

  // day = 本日读写句柄 (store.cs_try_open, 已按日流控: 各层张量整体就绪); end_day 交还
  void begin_day(const Day &day, misc::Ymd date) {
    assert(day && !day_ && "begin_day / end_day 须成对");
    day_ = day;
    if constexpr (Side::N > 0)
      load_side_(date);
  }
  void end_day() {
    assert(day_ && "begin_day / end_day 须成对");
    day_ = {};
  }

  // 每节拍一调 (事件驱动; t = 节拍层 kBeatLevel 的行): 实盘由 feed 层按墙钟逐秒 (无 L_sec 的档逐分钟) 调用,
  // 回测按日流控后逐拍重放同一个调用.
  // 不要改成"整日一次批量扫" —— 那样回测和实盘就成了两条代码路径, 逐值一致无从保证.
  void compute_and_store(size_t t) noexcept {
    TraceN("CS");
    TraceColor(C_Magenta);
    assert(day_ && "compute_and_store: 本日未 begin_day");
    if constexpr (T::has(L_sec)) {
      {
        TraceN("CS_Tick");
        run<L_sec>(t);
      }
      // 分钟末级联: 分钟 m 在其最后一秒算, 覆盖 m=0..254 (原先 t%60==0 && t>0 漏掉分钟 0、多算哨兵行).
      // 读 L_min 行 m 的前提 = 全部资产已结算分钟 m: 回测由按日流控保证; 实盘须先对全部资产 CoreTs::advance(m + 1)
      if (t % 60 == 59)
        run_minute_(L_sec_to_L_min(t));
    } else {
      run_minute_(t);
    }
    // 收盘级联: 末拍之后算 L_day 行 0. 前提 = 全部资产已写收盘行: 回测由按日流控保证; 实盘须先对全部资产 CoreTs::end_day
    if (t + 1 == level_valid_rows(kBeatLevel)) {
      TraceN("CS_Day");
      run<L_day>(0);
    }
  }

  // 一行 CS 字段 (CsLevel<LVL>::run 展开调用): 源档 S 本层的列 SRC → Tf → Method → 本层列 DST
  template <size_t LVL, class S, size_t SRC_LVL, size_t SRC, size_t DST, class Tf, class Method>
  [[gnu::always_inline]] inline void run_one(size_t t) {
    static_assert(SRC_LVL == LVL, "截面只读本层行 (高频读低频由 TS 行在本层节拍取 Series 最新值, 不跨层取张量)");
    static_assert(!Method::kNeutral || LVL != L_sec, "NeutralRank needs L_min / L_10min / L_day context (mcap / industry)");
    static_assert(std::is_same_v<S, T> || LVL == L_min, "旁读只许 L_min 截面行");

    float *y = input_fp32_.data();
    gather_(src_col_<S, LVL, SRC>(t), y);
    Tf::apply(y, n_);
    if constexpr (Method::kNeutral)
      Method::apply(y, n_, neutral_<LVL>(t));
    else
      Method::apply(y, n_);
    scatter_<LVL>(t, DST, y);
  }

private:
  // 分钟 m 的截面 + 箱末级联: m 是 10 分钟箱 k 的末分钟 → 算 L_10min 行 k (前提同 L_min: 全部资产已结算分钟 m, 见 CoreTs::close_10min)
  void run_minute_(size_t m) {
    {
      TraceN("CS_Minute");
      run<L_min>(m);
    }
    if (const size_t k = L_min_to_L_10min(m); L_10min_last_min(k) == m) {
      TraceN("CS_10Min");
      run<L_10min>(k);
    }
  }

  template <size_t LVL>
  void run(size_t t) {
    if constexpr (!T::has(LVL)) { // 档缺该层: 无特化
      return;
    } else if constexpr (CsLevel<T, LVL>::kEmpty) { // 层内暂无 CS 行 (L_sec 目前如此, 预留层): 节拍照走, 有效集也不必扫
      return;
    } else {
      // 截面只看 cs_valid (= 当日在池, 整日常量, 不与 ts_valid 相与, 见 Meta.hpp); 池外资产的 TS 列照常算, 只是不进任何截面.
      // 虽是整日常量仍逐行读第 t 行: CS 在 t 的输入只许来自张量第 t 行 (行自足), 实盘逐秒推进时也成立 —— 别挪成每日向量
      constexpr size_t VALID = field_of<T>(LVL, "cs_valid");
      static_assert(VALID < T::LEVELS[LVL].field_count, "有 CS 行的层必须有 cs_valid 列");
      const _Float16 *valid_flags = fstore::cs_col<LVL>(day_, t, VALID);
      valid_indices_.clear();
      for (size_t a = 0; a < A_; ++a)
        if (fmeta::data_valid(static_cast<float>(valid_flags[a])))
          valid_indices_.push_back(a);
      if (valid_indices_.empty())
        return;
      n_ = valid_indices_.size();
      neutral_ready_ = false;
      CsLevel<T, LVL>::run(*this, t);
    }
  }

  // 源列 t 时刻全部资产的连续段 (本档 A 轴): 本档 → 张量; 旁读档 → 本日旁读缓冲
  template <class S, size_t SRC_LVL, size_t SRC>
  const feature_storage_t *src_col_(size_t t) {
    if constexpr (std::is_same_v<S, T>) {
      return fstore::cs_col<SRC_LVL>(day_, t, SRC);
    } else {
      static_assert(std::is_same_v<S, typename TierSide<T>::type> && SRC_LVL == L_min, "CS 源档须为本档或 TIER_SIDES 登记的旁读档 (只许 L_min)");
      constexpr size_t i = Side::index_of(SRC);
      static_assert(i < Side::N);
      assert(t < level_rows(L_min));
      return side_.data() + (i * level_rows(L_min) + t) * A_;
    }
  }

  // NeutralRank 上下文 (本层同名列): 首个 NeutralRank 行触发, 本拍内复用. 源列按名定址: 本档有就用本档, 否则旁读档 (只许 L_min)
  template <size_t LVL>
  const cs::NeutralRank::Ctx &neutral_(size_t t) {
    using NS = std::conditional_t<owns_neutral_ctx<T, LVL>(), T, typename TierSide<T>::type>;
    static_assert(!std::is_void_v<NS> && (std::is_same_v<NS, T> || LVL == L_min),
                  "用 NeutralRank 的层须有 NEUTRAL_RANK_MCAP / NEUTRAL_RANK_INDUSTRY 源列 (L_min 可由旁读档提供)");
    constexpr size_t MCAP = field_of<NS>(LVL, CS_STR(NEUTRAL_RANK_MCAP));
    constexpr size_t INDUSTRY = field_of<NS>(LVL, CS_STR(NEUTRAL_RANK_INDUSTRY));
    static_assert(MCAP < NS::LEVELS[LVL].field_count && INDUSTRY < NS::LEVELS[LVL].field_count,
                  "NeutralRank 上下文档该层必须有 NEUTRAL_RANK_MCAP / NEUTRAL_RANK_INDUSTRY 源列");
    if (!neutral_ready_) {
      gather_(src_col_<NS, LVL, MCAP>(t), logmc_dense_.data());
      cs::NeutralRank::prepare_logmc(logmc_dense_.data(), n_);
      gather_(src_col_<NS, LVL, INDUSTRY>(t), industry_dense_.data());
      neutral_ready_ = true;
    }
    return neutral_ctx_;
  }

  // 本日旁读列: 旁读档库 [N][T][A_side] → 本档 A 轴 [N][T][A]. 缺库 → 整日 NaN + 日志; 头不符 = 旁读档库过期, 先重算它
  void load_side_(misc::Ymd date) {
    using S = typename TierSide<T>::type;
    const LevelInfo &L = S::LEVELS[L_min];
    const FeatureLib &side = libs_[S::kId];
    const size_t T1 = L.rows, As = side.axis.size();
    const FeatureDay::Status st = FeatureDay::read(feature_file(feature_day_dir(side.dir, date), L_min), L, Side::kFields.data(), Side::N,
                                                   side_full_.data(), As, side.axis.hash, side_scratch_);
    assert((st == FeatureDay::Status::Ok || st == FeatureDay::Status::Missing) && "旁读档库头不符 (轴 / 字段表变了), 先重算旁读档");
    if (st == FeatureDay::Status::Missing) {
      std::fill(side_.begin(), side_.end(), std::bit_cast<feature_storage_t>(fmeta::kNaNBits)); // fast-math TU: 不许浮点 NaN 字面量
      Logger::log("cs", std::string("旁读档 ") + tier_name(S::kId) + " 缺 " + std::to_string(date) + ", " + std::to_string(Side::N) + " 列整日填 NaN");
      return;
    }
    for (size_t i = 0; i < Side::N; ++i)
      for (size_t t = 0; t < T1; ++t) {
        const feature_storage_t *src = side_full_.data() + (i * T1 + t) * As;
        feature_storage_t *dst = side_.data() + (i * T1 + t) * A_;
        for (size_t a = 0; a < A_; ++a)
          dst[a] = src[side_sub_[a]];
      }
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

  Day day_{}; // 本日读写句柄, begin_day 换入 / end_day 交还
  size_t A_;

  const PerTier<FeatureLib> &libs_;          // 本档 + 旁读档库 (目录 + A 轴)
  std::vector<feature_storage_t> side_;      // [N][T][A] 本日旁读列 (本档 A 轴)
  std::vector<feature_storage_t> side_full_; // [N][T][A_side] 读盘暂存
  std::vector<size_t> side_sub_;             // 本档 a → 旁读档 A 轴下标
  FeatureDay::Scratch side_scratch_;

  std::vector<size_t> valid_indices_; // 本 t 有效资产 (dense 下标 → asset id)
  size_t n_ = 0;                      // = valid_indices_.size()
  std::vector<float> input_fp32_;
  std::vector<float> logmc_dense_;    // 中性化输入 (本拍本层)
  std::vector<float> industry_dense_; // 中性化输入 (本拍本层)
  bool neutral_ready_ = false;
  const cs::NeutralRank::Ctx neutral_ctx_{logmc_dense_.data(), industry_dense_.data()}; // 向量在 ctor 定长, 指针稳定
};
