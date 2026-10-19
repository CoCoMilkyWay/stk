#pragma once

#include "features/MetaFlag.hpp"              // fmeta::data_valid (之前经 NodesGenerated→Meta.hpp 传递获得)
#include "features/Method/CS.hpp"             // cs::<Tf> / cs::<Method> + NeutralRank::Ctx + NEUTRAL_RANK_* 源列
#include "features/Storage/DayBatchPlane.hpp" // 上游读: Source / load_source / source_row (与读端同一份)
#include "features/Storage/FeatureStore.hpp"
#include "misc/profiler.hpp"
#include "shared/FeatureLib.hpp"
#include <array>
#include <cmath>
#include <cstring>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

// ============================================================================
// CoreCs<T>: 截面计算 (CS worker), 档 T 有 L_sec → L_sec 每秒一拍; 无 L_sec (Bar) → L_min 每分钟一拍;
//   每拍按收行判据 (for_each_close) 级联本拍末秒收行的各层 (节拍层 → L_min → 分钟箱层 → L_day, 细 → 粗); 档有该层才算
//   与 CoreTs 对仗: begin_day / compute_and_store (节拍层 kBeatLevel, for_each_close 同一级联) / end_day, 逐层 run<LVL>
//   字段表 CS(ns, src_lvl, src, Tf, Method) 行由 CsLevel<T, LVL>::run 编译期展开 (与 fstore::RowWriter<T, LVL> 同构),
//   每行 = run_one<...>: gather(valid 子集 dense) → Tf::apply → Method::apply → scatter (无效资产输出 0); 源层 == 本层
//   节点契约见 DataDefine.hpp; 方法在 Method/CS.hpp (实现 precise TU). NeutralRank 的上下文 (log 市值 + 行业,
//   源列由 Method/CS.hpp 声明, 取本层同名列) 每拍按需准备一次, 全部 NeutralRank 行复用; L_sec 用 NeutralRank = 编译错误.
//
//   旁读 (TIER_SIDES, 两档共有的任意层): 每层源档 ns ≠ 本档的行 + 本档没有的 NeutralRank 上下文列 = 该层旁读列集 (CsSide<T, LVL>, 编译期去重).
//   begin_day 从旁读档的库整日读入各层这些列 (上游读 = 读端同一个 Source, 行 = level_asof(LVL, LVL, t) = t; gather 时经 ids 取本档 A 轴);
//   当日缺库 / 头不符 → 断言 (旁读档库须先算完覆盖本库日期轴, 不等它, 不替它算).
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

// 档 T 的 LVL 层 NeutralRank 上下文列在不在本档; 不在 → 从旁读档同层取
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

// 旁读列集: 档 T 的 LVL 层截面要读的旁读档 S (= TierSide<T>) 同层列下标 (升序去重). 无旁读档 / 该层无旁读列 → 空
template <class T, size_t LVL, class S = typename TierSide<T>::type>
struct CsSide {
  static constexpr size_t N = 0;
  static constexpr std::array<size_t, 0> kFields{};
};
template <class T, size_t LVL, class S>
  requires(!std::is_void_v<S>)
struct CsSide<T, LVL, S> {
  static constexpr size_t F = S::LEVELS[LVL].field_count; // 旁读档缺该层 = 0 (本层还要旁读 → 下标越界, 编译错误)
  static constexpr std::array<bool, F> kMask = [] {
    std::array<bool, F> m{};
    if constexpr (T::has(LVL)) {
      for (const CsRef &r : CsLevel<T, LVL>::kRefs)
        if (r.tier == S::kId)
          m[r.field] = true;
    }
    if (uses_neutral<T, LVL>() && !owns_neutral_ctx<T, LVL>()) {
      m[field_of<S>(LVL, CS_STR(NEUTRAL_RANK_MCAP))] = true;
      m[field_of<S>(LVL, CS_STR(NEUTRAL_RANK_INDUSTRY))] = true;
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
  template <size_t LVL>
  using Side = CsSide<T, LVL>;
  // 节拍层: 有 L_sec 的档逐秒 (实盘 feed 按墙钟逐秒驱动), 无 L_sec 的档 (Bar, 分钟 K 源) 逐分钟
  static constexpr size_t kBeatLevel = T::kFirstLevel;
  // 任一层有旁读列
  static constexpr bool kSides = []<size_t... L>(std::index_sequence<L...>) { return ((Side<L>::N > 0) || ...); }(std::make_index_sequence<LEVEL_COUNT>{});

  // libs[T::kId] = 本档库 (轴 sub → global); libs[旁读档] = 旁读档的库 (无旁读列时不读)
  CoreCs(FeatureStore<T> &store, [[maybe_unused]] const PerTier<FeatureLib> &libs)
      : A_(store.query_A()), input_fp32_(A_), logmc_dense_(A_), industry_dense_(A_) {
    valid_indices_.reserve(A_);
    if constexpr (kSides) {
      using S = typename TierSide<T>::type;
      const LibAxis &axis = libs[T::kId].axis;
      const FeatureLib &side = libs[S::kId];
      assert(side.tier == S::kId && axis.size() == A_);
      const LibId side_lib = lib_id(side.name);
      side_rd_.emplace(side);
      side_readers_.main = lib_id(libs[T::kId].name);
      side_readers_.rd[side_lib] = &*side_rd_;
      [&]<size_t... L>(std::index_sequence<L...>) { (init_side_<L>(side_lib, side.axis.size()), ...); }(std::make_index_sequence<LEVEL_COUNT>{});
      side_ids_.resize(A_);
      for (size_t a = 0; a < A_; ++a) {
        const size_t x = side.axis.sub_of(axis.global(a));
        assert(x < side.axis.size() && "本档资产不在旁读档 A 轴内");
        side_ids_[a] = static_cast<uint32_t>(x);
      }
    }
  }

  // day = 本日读写句柄 (store.cs_try_open, 已按日流控: 各层张量整体就绪); end_day 交还
  void begin_day(const Day &day, misc::Ymd date) {
    assert(day && !day_ && "begin_day / end_day 须成对");
    day_ = day;
    [&]<size_t... L>(std::index_sequence<L...>) { (load_side_<L>(date), ...); }(std::make_index_sequence<LEVEL_COUNT>{});
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
    // 节拍行 t 末秒收行的各层逐层截面 (细 → 粗, 与 CoreTs::close_minute 同一 for_each_close). 读层 LVL 行 r 的前提 = 全部资产已收该行:
    // 回测由按日流控保证; 实盘须先对全部资产 CoreTs::advance(m + 1) (L_min 及以上在分钟 m 末收行, L_day 随末分钟收)
    for_each_close<kBeatLevel>(level_end_sec(kBeatLevel, t) - 1, [&]<size_t LVL>(size_t r) { run<LVL>(r); });
  }

  // 一行 CS 字段 (CsLevel<LVL>::run 展开调用): 源档 S 本层的列 SRC → Tf → Method → 本层列 DST
  template <size_t LVL, class S, size_t SRC_LVL, size_t SRC, size_t DST, class Tf, class Method>
  [[gnu::always_inline]] inline void run_one(size_t t) {
    static_assert(SRC_LVL == LVL, "截面只读本层行 (高频读低频由 TS 行在本层节拍取 Series 最新值, 不跨层取张量)");
    static_assert(!Method::kNeutral || LVL != L_sec, "NeutralRank needs L_min / bin / L_day context (mcap / industry)");

    float *y = input_fp32_.data();
    gather_(src_col_<S, LVL, SRC>(t), ids_<S>(), y);
    Tf::apply(y, n_);
    if constexpr (Method::kNeutral)
      Method::apply(y, n_, neutral_<LVL>(t));
    else
      Method::apply(y, n_);
    scatter_<LVL>(t, DST, y);
  }

private:
  template <size_t LVL>
  void run(size_t t) {
    if constexpr (!T::has(LVL)) { // 档缺该层: 无特化
      return;
    } else if constexpr (CsLevel<T, LVL>::kEmpty) { // 层内暂无 CS 行 (L_sec 目前如此, 预留层): 节拍照走, 有效集也不必扫
      return;
    } else {
      TraceN(level_name(LVL));
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

  // 源列 t 时刻全部资产的连续段 (源档 A 轴, 经 ids_<S> 取本档资产): 本档 → 张量; 旁读档 → 本日该层旁读张量 (source_row 投影)
  template <class S, size_t SRC_LVL, size_t SRC>
  const feature_storage_t *src_col_(size_t t) {
    if constexpr (std::is_same_v<S, T>) {
      return fstore::cs_col<SRC_LVL>(day_, t, SRC);
    } else {
      static_assert(std::is_same_v<S, typename TierSide<T>::type>, "CS 源档须为本档或 TIER_SIDES 登记的旁读档");
      constexpr size_t i = Side<SRC_LVL>::index_of(SRC);
      static_assert(i < Side<SRC_LVL>::N);
      const Source &s = side_src_[SRC_LVL];
      assert(t < s.row.size());
      size_t r = 0;
      const FeatureRead::DayColumns *dc = source_row(s, side_buf_[SRC_LVL], t, r);
      assert(dc && "旁读源本日未读");
      return dc->col(i) + r * dc->A;
    }
  }
  // 源档 A 轴 ← 本档 a: 本档 = 同轴 (nullptr); 旁读档 = Readers::ids 同义
  template <class S>
  const uint32_t *ids_() const {
    if constexpr (std::is_same_v<S, T>)
      return nullptr;
    else
      return side_ids_.data();
  }

  // NeutralRank 上下文 (本层同名列): 首个 NeutralRank 行触发, 本拍内复用. 源列按名定址: 本档有就用本档, 否则旁读档同层
  template <size_t LVL>
  const cs::NeutralRank::Ctx &neutral_(size_t t) {
    using NS = std::conditional_t<owns_neutral_ctx<T, LVL>(), T, typename TierSide<T>::type>;
    static_assert(!std::is_void_v<NS>, "用 NeutralRank 的层须有 NEUTRAL_RANK_MCAP / NEUTRAL_RANK_INDUSTRY 源列 (本档或旁读档同层)");
    constexpr size_t MCAP = field_of<NS>(LVL, CS_STR(NEUTRAL_RANK_MCAP));
    constexpr size_t INDUSTRY = field_of<NS>(LVL, CS_STR(NEUTRAL_RANK_INDUSTRY));
    static_assert(MCAP < NS::LEVELS[LVL].field_count && INDUSTRY < NS::LEVELS[LVL].field_count,
                  "NeutralRank 上下文档该层必须有 NEUTRAL_RANK_MCAP / NEUTRAL_RANK_INDUSTRY 源列");
    if (!neutral_ready_) {
      gather_(src_col_<NS, LVL, MCAP>(t), ids_<NS>(), logmc_dense_.data());
      cs::NeutralRank::prepare_logmc(logmc_dense_.data(), n_);
      gather_(src_col_<NS, LVL, INDUSTRY>(t), ids_<NS>(), industry_dense_.data());
      neutral_ready_ = true;
    }
    return neutral_ctx_;
  }

  // 层 LVL 旁读源 (读端同一个 Source): 值列 = CsSide 列集, 不门控; 行 = level_asof(LVL, LVL, t) = t → 只读当日, 不承接
  template <size_t LVL>
  void init_side_([[maybe_unused]] LibId lib, [[maybe_unused]] size_t As) {
    if constexpr (Side<LVL>::N > 0) {
      Source &s = side_src_[LVL];
      s.lib = lib;
      s.level = LVL;
      s.cs.io.assign(Side<LVL>::kFields.begin(), Side<LVL>::kFields.end());
      s.cs.vts.assign(Side<LVL>::N, L2::ValidType::ALL);
      s.slot.resize(Side<LVL>::N);
      for (size_t i = 0; i < Side<LVL>::N; ++i)
        s.slot[i] = static_cast<uint32_t>(i);
      project_rows(s, LVL, level_valid_rows(LVL));
      assert(s.cur && !s.carry && "同层旁读只读当日");
      side_buf_[LVL].cur.preallocate(As, LVL, Side<LVL>::N);
    }
  }
  // 本日层 LVL 旁读列 (旁读档 A 轴整日张量). 该层无旁读列 → 不读; 缺库 / 头不符 → reader 判废 → 断言
  template <size_t LVL>
  void load_side_(misc::Ymd date) {
    if constexpr (Side<LVL>::N > 0) {
      const Source &s = side_src_[LVL];
      load_source(side_readers_, s, date, 0, s.cs.io, side_buf_[LVL]);
      assert(side_buf_[LVL].has_cur && !side_rd_->stale() && "旁读档库缺这天 / 头不符 (轴 / 字段表变了): 先算 / 重算旁读档");
    }
  }

  // 源列 valid 子集 → dense fp32 (ids = 源档 A 轴 ← 本档 a, nullptr = 同轴)
  void gather_(const _Float16 *src, const uint32_t *ids, float *dst) const {
    if (ids)
      for (size_t i = 0; i < n_; ++i)
        dst[i] = static_cast<float>(src[ids[valid_indices_[i]]]);
    else
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

  std::optional<FeatureRead> side_rd_;              // 旁读档库 reader (无旁读列不建)
  Readers side_readers_;                            // rd[旁读档] = side_rd_
  std::vector<uint32_t> side_ids_;                  // 本档 a → 旁读档 A 轴下标
  std::array<Source, LEVEL_COUNT> side_src_{};      // [lvl] 该层旁读源 (无旁读列的层空)
  std::array<IoSlot::Buf, LEVEL_COUNT> side_buf_{}; // [lvl] 本日该层旁读张量 (旁读档 A 轴)

  std::vector<size_t> valid_indices_; // 本 t 有效资产 (dense 下标 → asset id)
  size_t n_ = 0;                      // = valid_indices_.size()
  std::vector<float> input_fp32_;
  std::vector<float> logmc_dense_;    // 中性化输入 (本拍本层)
  std::vector<float> industry_dense_; // 中性化输入 (本拍本层)
  bool neutral_ready_ = false;
  const cs::NeutralRank::Ctx neutral_ctx_{logmc_dense_.data(), industry_dense_.data()}; // 向量在 ctor 定长, 指针稳定
};
