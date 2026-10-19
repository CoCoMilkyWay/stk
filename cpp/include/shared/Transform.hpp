#pragma once

#include "features/FeaturesDefine.hpp"
#include "features/Method/CS.hpp"
#include "math/Step.hpp"
#include "math/normalize/Normalize.hpp"
#include "math/spectral/IIRBandpass.hpp"
#include "math/stationary/FracDiff.hpp"
#include "math/stationary/IntDiff.hpp"
#include "math/stationary/MADetrend.hpp"
#include "math/stationary/TodProfile.hpp"
#include "shared/Analysis.hpp"
#include "shared/Progress.hpp"

#include <array>
#include <cstdint>
#include <memory>
#include <vector>

class FeatureRead;

// ============================================================================
// Transform Analysis (特征变换探索, 分批流式; 公共骨架见 shared/Analysis.hpp)
// ============================================================================
// 目的: 在全区间 × 全资产上试一条变换链, 看它对平稳性 / 分布 / 频谱的效果 —— 而且
// 链上每一步都必须是生产节点能逐值复现的东西, 否则结论不可迁移:
//
//   raw ─→ season ─→ stationary ─→ bandpass ─→ ts_norm ─→ CS(Tf → Method)
//          TOD 轮廓   MA/整数/分数差分  IIR 因果    expanding   features/Method/CS.hpp 同一套代码
//
//   TS 段 (前四步) 按段分: 每 (资产, 段) 一条 R 长的序列 (网格 = 特征自己的层; 日内层段 = 1 天, L_1day 段 = 连续 255 天),
//   滤波/差分状态段首复位, 隔夜跳空不进滤波器; 唯一的跨段状态是 TOD 轮廓 (per-asset expanding, 严格因果:
//   今天用截至昨日的轮廓, 再把今天并入; L_1day 无日内轮廓). 全部因果, 不用未来值.
//   CS 段: 每个 (段, 行) 截面一列, 有效资产子集 dense 化 → cs::column_fn (Tf, Method) 原地.
//
// 流式 (对仗 Dist.hpp): 段是流式维度, 每批 batch_segs 段, 从前往后 (plan_segs Sequential: TOD 轮廓因果);
// 每相一次 dispatch (Runtime 常驻 ForkJoin), 返回即栅栏:
//   IO (后台预读, 与上一批计算并发): 单段一任务 → TsPlane [列][A][批段][R] (f16, 无效 = 哨兵 NaN; 各列按自己的 vt 门控;
//                   NeutralRank 上下文 [mcap, ind] 是同一读计划里的 Bar 旁读列, 按 as-of 投影到网格)
//   Phase TS:   资产块一任务; 每 (资产, 段): 上面的 TS 链 → out_ [A][批段][R] (float, NaN = 缺/预热)
//   ── 栅栏 ──
//   Phase CS:   截面列块 (段×行) 一任务; 每列 gather 有效子集 → column_fn → scatter 回 out_
//   ── 栅栏 ──
//   Phase 统计: 资产块一任务; 每资产: 全量输出 → 该资产累积 sketch → 导出 AssetLine (PDF/矩);
//               并且逐天 ADF/KPSS (通过率累积)、逐天 PSD 与 ACF/PACF (算术平均累积) ——
//               全资产, 无子集上限; 焦点资产额外攒 全程 原始/输出序列 + TOD 轮廓 (UI 序列视图)
//   ── 栅栏 ──
//   Phase 归约: analysis::reduce_tree → shard 0 = 全程累加器
//   ── 栅栏 ── 批末 (单线程): 短锁 swap 发布 + 进度 + epoch
//   首帧 = 一批的 IO + 三段计算 (几十 ms), 与总区间长度无关; 视图逐批收敛.
//
// UI: 每帧持 mutex 读快照; 改任何参数 = 新请求 (取消在跑, 从第一天重来) —— 海量数据下
//     交互仍即时, 因为每次都只等第一批.
// 重算: reset 不清展示字段 (拖动调参时旧图留住), 首批 publish 整体覆盖 —— 与 Dist 同一约定.
// A 轴 = 库轴 (与特征库文件列序一致, 见 LibAxis) —— 平面 /
//   out / lines / sketch / TOD 都只有库轴大小, 每个槽都是活跃资产, 不存在空列.
//   槽位 = 库轴下标; AssetLine/StatLine 的 .asset 存全局轴下标 (UI 查 items 用).
// ============================================================================

static constexpr size_t kTfMinStatSamples = 20; // 单段 ADF/KPSS 的最少有效样本
static constexpr size_t kTfMaxLag = 40;         // 单段 ACF/PACF 最大滞后 (样本 = 网格行); 需有效样本 ≥ 4×lag (段太短的层不出 ACF)

struct Transform : Published {
  using Season = math::stationary::TodProfile::Mode;
  static constexpr size_t kSeasonCount = 3;

  enum class Stationary : uint8_t { None = 0,
                                    MADetrend,
                                    IntDiff,
                                    FracDiff };
  struct StationaryEntry {
    Stationary method;
    const math::StepDef *def;
  };
  static inline constexpr math::StepDef g_stationary_none = {"无", nullptr, 0};
  static inline constexpr StationaryEntry g_stationary[] = {
      {Stationary::None, &g_stationary_none},
      {Stationary::MADetrend, &math::stationary::MADetrend::def},
      {Stationary::IntDiff, &math::stationary::IntDiff::def},
      {Stationary::FracDiff, &math::stationary::FracDiff::def},
  };
  static inline constexpr size_t g_stationary_count = sizeof(g_stationary) / sizeof(g_stationary[0]);
  static constexpr const math::StepDef &stationary_def(Stationary m) {
    return *g_stationary[static_cast<size_t>(m)].def;
  }
  static constexpr const char *IIR_TYPE_NAMES[] = {"Butterworth", "Chebyshev I", "Chebyshev II"};

  // ==========================================================================
  // 参数 (UI 编辑一份, 请求时快照进 Transform; 任何字段变了都是一次新构建)
  // ==========================================================================
  struct Params {
    Season season = Season::None;

    Stationary stationary = Stationary::None;
    math::Step st; // 按 stationary_def(stationary) 初始化

    // 带通 (IIR 因果, 段首复位). 通带用周期 (样本数 = 网格行) 表达, 与 PSD 轴一致: lo < hi
    bool bandpass = false;
    math::spectral::IIRType iir_type = math::spectral::IIRType::Butterworth;
    int iir_order = 2;
    float bp_lo_period = 5.0f;
    float bp_hi_period = 60.0f;
    static constexpr float kMinPeriod = 2.5f;                                             // f = 2/period 相对 Nyquist 必须 < 1
    static float max_period(size_t level) { return static_cast<float>(seg_rows(level)); } // 最长看一段

    NormMethod ts_norm = NormMethod::NONE;
    math::Step ts; // 按 math::normalize::GetMethod(ts_norm) 初始化

    // 构建段数上限: 0 = 全区间; 否则只算区间头 N 段, 不往后算 (快速迭代预设: 1/5/30)
    size_t max_days = 0;

    cs::TfId cs_tf = cs::TfId::None;
    cs::MethodId cs_method = cs::MethodId::None;

    Params() {
      reset_stationary();
      reset_ts();
    }
    void reset_stationary() { st.init(stationary_def(stationary)); }
    void reset_ts() { math::normalize::InitStep(ts, ts_norm); }
    bool cs_enabled() const { return cs_tf != cs::TfId::None || cs_method != cs::MethodId::None; }
    bool cs_neutral() const { return cs::method_neutral(cs_method); }
  };

  using AssetLine = analysis::AssetLine; // 每资产链末输出的累积分布 (槽位 == 库轴下标)

  // 每资产 (槽位 == 库轴下标, 与 lines 对齐): 逐天平稳性检验累积
  struct StatLine {
    uint32_t asset = 0;  // 全局轴下标 (UI 查 items 用)
    uint32_t n_days = 0; // 参与检验的天数 (有效样本 ≥ kTfMinStatSamples)
    uint32_t adf_pass = 0, kpss_pass = 0;
    double adf_sum = 0.0, kpss_sum = 0.0; // 统计量之和 → UI 取均值
    float adf_rate() const { return n_days ? static_cast<float>(adf_pass) / n_days : -1.0f; }
    float kpss_rate() const { return n_days ? static_cast<float>(kpss_pass) / n_days : -1.0f; }
  };

  // 焦点资产 全程 的序列快照 (UI 序列视图: 原始 vs 输出 + TOD 轮廓).
  // 只存一个资产 → 内存 O(总段数 × R), 与 库轴大小无关; 换焦点 = 重跑构建
  // (与改参数同流程 —— 链末输出经过 CS 截面, 单资产无法离线重放).
  // 前缀发布零拷贝: raw/out 首批发布时整体 swap 进来, 之后 worker 锁外只写 [n_segs·R, ...) 段,
  // 短锁只推进 n_segs / 日期 / TOD 轮廓; UI 只读前缀 [0, n_segs·R)
  struct SeriesSnap {
    uint32_t asset = 0;                     // 焦点资产全局轴下标 (UI 查 items; 构建起手定)
    size_t level = L_1min, R = 0;           // 网格层 / 段行数 (UI 按它铺 X 轴)
    size_t n_segs = 0;                      // 已发布段数 (逐批递增至全程)
    misc::Ymd date_begin = 0, date_end = 0; // 已发布区间首末日
    std::vector<float> raw;                 // [总段数 * R] (f16 → float, NaN = 缺/未到批)
    std::vector<float> out;                 // [总段数 * R] 链末输出 (NaN = 缺/预热/未到批)
    std::vector<float> tod_mean, tod_sd;    // [R] 截至末批的 TOD 轮廓 (season 关时全 NaN)
  };

  struct Spec {
    Params params;
    ReadPlan plan;      // 平面列 [特征] (+ Bar 旁读 [mcap, ind_l1], 仅 NeutralRank); 网格 = 特征自己的层
    uint32_t focus = 0; // 序列快照焦点 (库轴下标)
  };
  // 发布快照 (worker 批末短锁 swap; UI 持锁只画)
  struct Snap {
    std::vector<AssetLine> lines;                             // [A_sub] (槽位 == 库轴下标, .asset = 全局轴下标)
    std::vector<StatLine> stat_lines;                         // [A_sub] 槽位 == 库轴下标 (与 lines 对齐)
    size_t psd_N = 0;                                         // 谱 FFT 长度: bin k ↔ 周期 psd_N / k 网格行
    std::vector<float> psd_mean;                              // [psd_N/2 + 1] 逐 (资产, 段) 单段谱的算术平均 (逐批收敛)
    uint64_t psd_n = 0;                                       // 参与平均的 (资产, 段) 数
    std::array<float, kTfMaxLag + 1> acf_mean{}, pacf_mean{}; // 逐 (资产, 段) 单段 ACF/PACF 的算术平均 (lag 0 = 1)
    uint64_t acf_n = 0;                                       // 参与平均的 (资产, 段) 数 (有效样本 ≥ 4×kTfMaxLag)
    analysis::AggPdf global;                                  // 全资产全区间链末有限输出
    // 链路账目: 输入 = f16 源 (门控不过 / NaN / ±inf / 有限), 输出 = 链末 (NaN = 缺/预热/常数槽, 不分账)
    analysis::Integrity in, out;
  };
  Spec spec;
  Snap snap;
  SeriesSnap series; // 焦点资产全程 (逐批增长, 前缀发布)

  Transform();
  ~Transform();

  // 重置构建参数并进入 Building. 展示字段不清 (旧图留住), 库轴变了才重建资产线
  void reset(analysis::ReadScope scope, Spec spec);
  // 全区间构建: 分批流式; 被取消返回 false
  bool build(const PerLib<const FeatureRead *> &fr, const std::atomic<bool> &cancel);
  void clear();

private:
  struct Runtime; // 网格无关基 (虚析构), 定义在 Transform.cpp
  template <class G>
  struct RuntimeOf; // worker 私有 (批平面 / TS 链状态 / 线程 shard), 按网格实例化
  template <class G>
  bool build_grid(const PerLib<const FeatureRead *> &fr, const std::atomic<bool> &cancel);
  std::unique_ptr<Runtime> rt_;
  analysis::ReadScope scope_; // reset 时定好, build 全程只读
};
