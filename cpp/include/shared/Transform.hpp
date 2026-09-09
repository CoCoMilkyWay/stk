#pragma once

#include "features/FeaturesDefine.hpp"
#include "features/Method/CS.hpp"
#include "math/Operator.hpp"
#include "math/normalize/Normalize.hpp"
#include "math/spectral/IIRBandpass.hpp"
#include "math/stationary/FracDiff.hpp"
#include "math/stationary/IntDiff.hpp"
#include "math/stationary/MADetrend.hpp"
#include "math/stationary/TodProfile.hpp"
#include "shared/Analysis.hpp"

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

class FeatureRead;

// ============================================================================
// Transform Analysis (特征变换探索, 分批流式; 公共骨架见 shared/Analysis.hpp)
// ============================================================================
// 目的: 在全区间 × 全资产上试一条变换链, 看它对平稳性 / 分布 / 频谱的效果 —— 而且
// 链上每一步都必须是生产算子能逐值复现的东西, 否则结论不可迁移:
//
//   raw ─→ season ─→ stationary ─→ bandpass ─→ ts_norm ─→ CS(Tf → Method)
//          TOD 轮廓   MA/整数/分数差分  IIR 因果    expanding   features/Method/CS.hpp 同一套代码
//
//   TS 段 (前四步) 按天分段: 每 (资产, 天) 一条 VR 长的序列, 滤波/差分状态天首复位,
//   隔夜跳空不进滤波器; 唯一的跨天状态是 TOD 轮廓 (per-asset expanding, 严格因果:
//   今天用截至昨日的轮廓, 再把今天并入). 全部因果, 不用未来值.
//   CS 段: 每个 (天, 分钟) 截面一列, 有效资产子集 dense 化 → cs::column_fn (Tf, Method) 原地.
//
// 流式 (对仗 Dist.hpp): 天是流式维度, 每批 kDaysPerBatch 天, 从前往后:
//   Phase IO:   抢单天并行载入 → DayBatchPlane [列][A][批天][VR] (f16, 无效 = 哨兵 NaN)
//   ── 栅栏 ──
//   Phase TS:   抢资产块; 每 (资产, 天): 上面的 TS 链 → out_ [A][批天][VR] (float, NaN = 缺/预热)
//   ── 栅栏 ──
//   Phase CS:   抢截面列块 (天×分钟); 每列 gather 有效子集 → column_fn → scatter 回 out_
//   ── 栅栏 ──
//   Phase 统计: 抢资产块; 每资产: 全量输出 → 该资产累积 sketch → 导出 AssetLine (PDF/矩);
//               并且逐天 ADF/KPSS (通过率累积)、逐天 PSD 与 ACF/PACF (算术平均累积) ——
//               全资产, 无子集上限; 焦点资产额外攒 全程 原始/输出序列 + TOD 轮廓 (UI 序列视图)
//   ── 栅栏 (completion, 单线程): 短锁 swap 发布 + 进度 + epoch ──
//   首帧 = 一批的 IO + 三段计算 (几十 ms), 与总区间长度无关; 视图逐批收敛.
//
// UI: 每帧持 mutex 读快照; 改任何参数 = 新请求 (取消在跑, 从第一天重来) —— 海量数据下
//     交互仍即时, 因为每次都只等第一批.
// 重算: reset 不清展示字段 (拖动调参时旧图留住), 首批 publish 整体覆盖 —— 与 Dist 同一约定.
// universe: A 轴 = universe 子轴 (与特征库文件列序一致, 见 UniverseAxis) —— 平面 /
//   out / lines / sketch / TOD 都只有子轴大小, 每个槽都是活跃资产, 不存在空列.
//   槽位 = 子轴下标; AssetLine/StatLine 的 .asset 存全局轴下标 (UI 查 items 用).
// ============================================================================

static constexpr size_t kTfMinStatSamples = 20; // 单日 ADF/KPSS 的最少有效样本
static constexpr size_t kTfMaxLag = 40;         // 单日 ACF/PACF 最大滞后 (样本 = 分钟); 需有效样本 ≥ 4×lag
static_assert(4 * kTfMaxLag <= analysis::kVR);

struct Transform : analysis::StreamState {
  using Season = math::stationary::TodProfile::Mode;
  static constexpr size_t kSeasonCount = 3;

  enum class Stationary : uint8_t { None = 0,
                                    MADetrend,
                                    IntDiff,
                                    FracDiff };
  struct StationaryEntry {
    Stationary method;
    const math::OperatorDef *def;
  };
  static inline constexpr math::OperatorDef g_stationary_none = {"无", nullptr, 0};
  static inline constexpr StationaryEntry g_stationary[] = {
      {Stationary::None, &g_stationary_none},
      {Stationary::MADetrend, &math::stationary::MADetrend::def},
      {Stationary::IntDiff, &math::stationary::IntDiff::def},
      {Stationary::FracDiff, &math::stationary::FracDiff::def},
  };
  static inline constexpr size_t g_stationary_count = sizeof(g_stationary) / sizeof(g_stationary[0]);
  static constexpr const math::OperatorDef &stationary_def(Stationary m) {
    return *g_stationary[static_cast<size_t>(m)].def;
  }
  static constexpr const char *IIR_TYPE_NAMES[] = {"Butterworth", "Chebyshev I", "Chebyshev II"};

  // ==========================================================================
  // 参数 (UI 编辑一份, 请求时快照进 Transform; 任何字段变了都是一次新构建)
  // ==========================================================================
  struct Params {
    Season season = Season::None;

    Stationary stationary = Stationary::None;
    math::Operator st; // 按 stationary_def(stationary) 初始化

    // 带通 (IIR 因果, 天首复位). 通带用周期 (样本数 = L1 分钟) 表达, 与 PSD 轴一致: lo < hi
    bool bandpass = false;
    math::spectral::IIRType iir_type = math::spectral::IIRType::Butterworth;
    int iir_order = 2;
    float bp_lo_period = 5.0f;
    float bp_hi_period = 60.0f;
    static constexpr float kMinPeriod = 2.5f;                              // f = 2/period 相对 Nyquist 必须 < 1
    static constexpr float kMaxPeriod = static_cast<float>(analysis::kVR); // 最长看一天

    NormMethod ts_norm = NormMethod::NONE;
    math::Operator ts; // 按 math::normalize::GetMethod(ts_norm) 初始化

    // 构建天数上限: 0 = 全区间; 否则只算区间头 N 天, 不往后算 (快速迭代档: 1/5/30)
    size_t max_days = 0;

    cs::TfId cs_tf = cs::TfId::None;
    cs::MethodId cs_method = cs::MethodId::None;

    Params() {
      reset_stationary();
      reset_ts();
    }
    void reset_stationary() { st.init(stationary_def(stationary)); }
    void reset_ts() { math::normalize::InitOperator(ts, ts_norm); }
    bool cs_enabled() const { return cs_tf != cs::TfId::None || cs_method != cs::MethodId::None; }
    bool cs_neutral() const { return cs::method_neutral(cs_method); }
  };

  // ==========================================================================
  // 发布快照 (worker 批末短锁 swap; UI 持锁只画)
  // ==========================================================================

  using AssetLine = analysis::AssetLine; // 每资产链末输出的累积分布 (槽位 == 子轴下标)

  // 每资产 (槽位 == 子轴下标, 与 lines 对齐): 逐天平稳性检验累积
  struct StatLine {
    uint32_t asset = 0;  // 全局轴下标 (UI 查 items 用)
    uint32_t n_days = 0; // 参与检验的天数 (有效样本 ≥ kTfMinStatSamples)
    uint32_t adf_pass = 0, kpss_pass = 0;
    double adf_sum = 0.0, kpss_sum = 0.0; // 统计量之和 → UI 取均值
    float adf_rate() const { return n_days ? static_cast<float>(adf_pass) / n_days : -1.0f; }
    float kpss_rate() const { return n_days ? static_cast<float>(kpss_pass) / n_days : -1.0f; }
  };

  // 焦点资产 全程 的序列快照 (UI 序列视图: 原始 vs 输出 + TOD 轮廓).
  // 只存一个资产 → 内存 O(总天数 × VR), 与 universe 大小无关; 换焦点 = 重跑构建
  // (与改参数同流程 —— 链末输出经过 CS 截面, 单资产无法离线重放).
  struct SeriesSnap {
    uint32_t asset = 0;                  // 焦点资产全局轴下标 (UI 查 items; 构建起手定)
    size_t n_days = 0;                   // 已处理天数 (逐批递增至全程)
    std::string date_begin, date_end;    // 已处理区间首末日 "YYYYMMDD"
    std::vector<float> raw;              // [总天数 * VR] (f16 → float, NaN = 缺/未到批)
    std::vector<float> out;              // [总天数 * VR] 链末输出 (NaN = 缺/预热/未到批)
    std::vector<float> tod_mean, tod_sd; // [VR] 截至末批的 TOD 轮廓 (season 关时不填)
  };

  // 链路账目 (输入 → 链末输出; 与 analysis::Integrity 的值账目语义不同, 不共用)
  struct Integrity {
    uint64_t n_total = 0;     // 格子数 (A × 天 × VR)
    uint64_t n_in_valid = 0;  // 输入有效 (门控过 且 非 NaN)
    uint64_t n_in_nan = 0;    // 输入真 NaN (门控过但值 NaN)
    uint64_t n_out_valid = 0; // 链末有效输出 (差 = 预热/常数槽/CS 缺失)
    void clear() { *this = Integrity{}; }
  };

  // ==========================================================================
  // State (进度/epoch/mutex 在 StreamState)
  // ==========================================================================

  Params params;                                            // 本次构建的参数快照 (reset 时定, UI 只读)
  std::vector<AssetLine> lines;                             // [A_sub] (槽位 == 子轴下标, .asset = 全局轴下标)
  std::vector<StatLine> stat_lines;                         // [A_sub] 槽位 == 子轴下标 (与 lines 对齐)
  SeriesSnap series;                                        // 焦点资产全程 (逐批增长)
  std::array<float, analysis::DayPSD::N_FREQS> psd_mean{};  // 逐 (资产, 天) 单日谱的算术平均 (逐批收敛)
  uint64_t psd_n = 0;                                       // 参与平均的 (资产, 天) 数
  std::array<float, kTfMaxLag + 1> acf_mean{}, pacf_mean{}; // 逐 (资产, 天) 单日 ACF/PACF 的算术平均 (lag 0 = 1)
  uint64_t acf_n = 0;                                       // 参与平均的 (资产, 天) 数 (有效样本 ≥ 4×kTfMaxLag)
  analysis::AggPdf global;                                  // 全资产全区间链末输出
  Integrity integrity;

  // ==========================================================================
  // Methods (worker 线程调用)
  // ==========================================================================

  Transform();
  ~Transform();

  // 重置构建参数并进入 Building. cols = [特征列 (+ mcap, ind_l1 若 NeutralRank) (+ _meta 门控列)]
  // global_ids = 子轴 → 全局轴映射 (UniverseAxis::ids); series_focus = 序列快照焦点 (子轴下标).
  // 展示字段不清 (旧图留住), universe 变了才整体重建.
  void reset_for_build(const Params &p, std::vector<size_t> cols, bool has_valid,
                       std::vector<std::string> month_keys,
                       std::vector<uint32_t> global_ids, uint32_t series_focus);

  // 全区间构建: 分批流式; 被取消返回 false
  bool build(FeatureRead &reader, const std::atomic<bool> &cancel);

  void clear();

private:
  struct Runtime; // worker 私有 (批平面 / TS 链状态 / 线程 shard), 定义在 Transform.cpp
  std::unique_ptr<Runtime> rt_;
  // 构建参数 (UI 不看): reset 时定好, build 全程只读
  std::vector<size_t> columns_;
  bool has_valid_ = false;
  std::vector<std::string> months_;
  std::vector<uint32_t> global_ids_; // 子轴 → 全局轴映射
  uint32_t series_focus_ = 0;        // 序列快照焦点 (子轴下标)
};
