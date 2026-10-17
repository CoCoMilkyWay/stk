#pragma once

#include "shared/Analysis.hpp"
#include "shared/Progress.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

class FeatureRead;

// ============================================================================
// Corr (特征两两相关矩阵, 逐抽样日流式; 公共骨架见 shared/Analysis.hpp)
// ============================================================================
// 口径 (为什么是这个口径, 而不是把全部 (分钟, 资产) 格子倒进一个池子):
//
//   ρ_ij = 平均截面 Spearman = 每个时间切片上先算一次跨资产的秩相关, 再对切片取算术平均
//          (Fama-MacBeth 式). 问的是"同一决策时刻这两列是不是同一份信息" —— 正是选股
//          语境下"冗余"的定义.
//
//   为什么不 pooled: 日内轮廓会主导一切. 成交量族全带 U 型, pooled 相关一律 0.9+,
//     与是否同一份信息无关. 逐切片做则自动消掉 —— 日内轮廓在时刻 t 是所有资产共同的
//     一个因子, 对截面排序完全不起作用. 跨资产的永久水平差 (大盘股两列都偏高) 同理.
//
//   为什么是秩不是 Pearson: 值是 f16 厚尾, 量级跨度极大 (price 族 vs ratio 族),
//     Pearson 会被几个离群点劫持. Spearman 对单调变换不变 —— 一个特征和它的 log /
//     rank 派生版本老实给出 |ρ| = 1, 正是要抓的冗余. 并列取中位秩 (flag 列大片并列).
//
//   为什么没有 horizon / forward 位移: 那是 IC (特征 vs 未来收益) 的概念. 特征↔特征
//     的对齐就是 lag 0; 强行 shift 得到的是 真实同期相关 × 自相关, 两件事搅一起.
//     "B 是不是 A 的延迟副本" 由 CorrPair 的 lead-lag 曲线单独回答 (悬停格子触发),
//     "哪些对是平移关系" 由 CorrLag 全矩阵扫一遍 (勾选触发).
//
// 抽样 (无偏 + 无混叠):
//   天:   固定种子洗牌取前 kCorrDays 个 (不是"每隔 k 天取一天" —— 那会锁死星期几).
//   切片: 日内每 kCorrSliceMinutes 分钟一片 = 对日内做分层抽样, 比随机抽方差更小.
//         相位逐日轮转 (第 d 天起点 = d % stride), 跨天把全部相位均匀覆盖掉.
//   相邻分钟高度自相关, 抽稀几乎不损精度; F² × 样本数 是唯一的成本瓶颈.
//
// 数值: 每切片每特征的秩先标准化到 均值 0 / 单位方差 (在它自己的有效资产集上), 无效槽
//   置 0 并另记位掩码. 标准化之后, "把各切片的叉积累加起来再统一除" 恒等于 "逐切片算 ρ
//   再平均" —— 于是只需一套跨切片累加器, 不必每切片开方.
//   缺失按 pairwise-complete 精确处理 (每对各自的交集均值/方差), 累加六个量:
//     xy = Σ R_i R_j (全资产, 交集外恒 0)       n = |m_i ∩ m_j|
//     x  = Σ_{i∩j} R_i,  xx = Σ_{i∩j} R_i²      y / yy 对称
//   只有 xy 是稠密内积 (寄存器分块 GEMM); 其余四个由行恒等式 Σ_i R = 0, Σ_i R² = k 化成
//   掩码差集上的稀疏和, 掩码相同 (等价类) 的对直接 O(1): x = y = 0, xx = yy = n = k.
//   (秩本身是在各自有效集上排的, 不是在每对的交集上排 —— 逐对重排是 F²·A log A, 不可行;
//    这是这里唯一的近似, 缺失率低时无感.)
// 三个目标 (Corr / CorrLag / CorrPair) 同一个引擎 (Corr.cpp), 只差配置:
//   切片布局 (等距抽稀 / 连续窗 / 取满) × lag 数 (1 / kCorrLags)
//
// 网格: 请求列里最细的层; 粗层列按 level_asof 投影 (前一根已收的粗层值). L_day 网格 = 段内逐日截面, 切片取满 (日 = 切片).
// 生命周期: 进 Corr tab 且过滤行集/层/库/日期区间变了 → 新请求取消在跑重算;
//   切走 tab → 只中断 (矩阵留在内存, 切回不重算); 切出 Features 任务 → clear() 释放.
// ============================================================================

static constexpr size_t kCorrDays = 32;                   // 抽样日上限 (流式单位 = 天, 逐步发布收敛)
static constexpr size_t kCorrSliceMinutes = 10;           // 日内切片间隔 (分钟; L_sec 自动换算成秒步长, L_10min 换算成箱步长)
static constexpr size_t kCorrStepCells = size_t(8) << 20; // 单步 (天批) 切片格子预算 天×S×F×A: 列少 (Pair) 一步读多天, IO 按天并行
static constexpr size_t kCorrMinAssets = 20;              // 单 (切片, 特征) 最少有效资产, 否则该切片该列作废
static constexpr size_t kCorrMinPairN = 2000;             // 单对累计样本下限, 否则 ρ = NaN (UI 画空格)
static constexpr size_t kCorrMaxLag = 10;                 // lead-lag 半宽 (层行数: L_min = 分钟, L_sec = 秒, L_10min = 箱)
static constexpr size_t kCorrLags = 2 * kCorrMaxLag + 1;
static constexpr size_t kCorrLagWindow = kCorrLags; // CorrLag 每日连续窗长 (行数): 每个 lag 至少 kCorrMaxLag+1 对切片

// 该层的切片行步长 (kCorrSliceMinutes 分钟折成层行数; L_min = 10, L_sec = 600, L_10min = 1, L_day = 1)
size_t corr_slice_stride(size_t level);

// 三个目标共用的输入: 读计划 (网格层 + ColRef; 矩阵 / lag: 升序去重 ≥ 2; 单对: {a, b} 按序)
struct CorrSpec {
  ReadPlan plan;
  size_t level() const { return plan.level; }
  size_t n() const { return plan.n(); }
  const ColRef &col(size_t i) const {
    assert(i < n());
    return plan.cols[i];
  }
  bool same(const CorrSpec &o) const { return plan.level == o.plan.level && plan.cols == o.plan.cols; }
};

struct Corr : Published {
  using Spec = CorrSpec;
  // 发布快照 (worker 每步短锁 swap; UI 渲染帧内持锁只读)
  struct Snap {
    std::vector<float> rho;       // [n][n] 平均截面 Spearman; NaN = 样本不足 / 无方差
    std::vector<uint32_t> pair_n; // [n][n] 累计 pairwise 样本数 (切片 × 资产); 对角 = 有效资产数
  };
  Spec spec;
  Snap snap;

  Corr();
  ~Corr();

  // 重置全部状态并进入 Building (列集/层变了旧矩阵下标全错位, 快照整体清)
  void reset(analysis::ReadScope scope, Spec spec);
  // 逐抽样日构建 (日内切片累加, 每步发布); 被取消返回 false
  bool build(FeatureRead &reader, const FeatureRead &all, const std::atomic<bool> &cancel);
  void clear();

private:
  struct Runtime; // worker 私有 (引擎 + 发布 staging), 定义在 Corr.cpp
  std::unique_ptr<Runtime> rt_;
  analysis::ReadScope scope_; // reset 时定好, build 全程只读
};

// ============================================================================
// CorrPair (单对 lead-lag 曲线: ρ(lag) = corr(A(t), B(t + lag)), lag > 0 = A 领先 B)
// ============================================================================
// 独立 worker (只读两列, 比矩阵便宜三个数量级), 故不与矩阵构建互相打断.
// 用途: 主热图只给 lag 0, 一个被平滑/延迟过的副本在 lag 0 上 |ρ| < 1 看着像新信息,
//   扫 ±kCorrMaxLag 取峰值才露馅. 注意慢变特征 (分钟级自相关 ≈ 1) 的曲线天然平坦,
//   对它们这条曲线说明不了什么 —— 只对快变列有判别力.
// 与矩阵同一套抽样日 (同种子同天数) 与同一套秩标准化; 切片取满 (不抽稀, 要分辨相邻期).
// ============================================================================

struct CorrPair : Published {
  using Spec = CorrSpec; // cols = {a, b} (metadata 下标, UI 核对当前选中对)
  struct Snap {
    std::array<float, kCorrLags> rho{}; // rho[kCorrMaxLag] = 同期; NaN = 样本不足
    std::array<uint32_t, kCorrLags> pair_n{};
  };
  Spec spec;
  Snap snap;

  static int lag_of(size_t k) { return static_cast<int>(k) - static_cast<int>(kCorrMaxLag); }

  CorrPair();
  ~CorrPair();

  void reset(analysis::ReadScope scope, Spec spec);
  bool build(FeatureRead &reader, const FeatureRead &all, const std::atomic<bool> &cancel);
  void clear();

private:
  struct Runtime;
  std::unique_ptr<Runtime> rt_;
  analysis::ReadScope scope_;
};

// ============================================================================
// CorrLag (全矩阵 lead-lag 峭点: 每对在 ±kCorrMaxLag 内 |ρ| 最大的 lag)
// ============================================================================
// 回答的问题: 哪些对其实是"平移关系" —— 主热图 lag 0 上 |ρ| 不高, 但挪几行就贴上. 热图
//   按 lag_best 着色 (0 = 中性色, ±K 两端两色渐变), 非 0 的格子值得去 CorrPair 曲线细看.
// 定义: ρ_ij(lag) = corr(F_i(t), F_j(t + lag)), lag > 0 = i 领先 j; ρ_ji(lag) = ρ_ij(-lag),
//   故只算上三角 × 全部 lag, lag_best 反对称.
// 抽样: 与矩阵同一套抽样日; 每日一段连续 kCorrLagWindow 行的窗 (起点逐日轮转), 窗内所有
//   (t, t+lag) 切片对都累加. 连续窗是必要的 —— 抽稀切片分辨不了相邻期.
// 数值: 与矩阵同一引擎同一套累加量 (精确 pairwise-complete ρ, 逐 lag).
// 成本: ≈ 主矩阵的 Σ_lag (W-|lag|) / S 倍, 故只在 UI 勾选时算.
// ============================================================================

struct CorrLag : Published {
  using Spec = CorrSpec; // 必须与 Corr.spec 一致 (same), UI 才能按同一显示序取值
  struct Snap {
    std::vector<int8_t> lag_best; // [n][n] |ρ| 最大的 lag (反对称; 对角 0; 样本不足 0)
    std::vector<float> rho_best;  // [n][n] 该 lag 上的 ρ (对称位为同值); NaN = 样本不足
  };
  Spec spec;
  Snap snap;

  CorrLag();
  ~CorrLag();

  void reset(analysis::ReadScope scope, Spec spec);
  bool build(FeatureRead &reader, const FeatureRead &all, const std::atomic<bool> &cancel);
  void clear();

private:
  struct Runtime;
  std::unique_ptr<Runtime> rt_;
  analysis::ReadScope scope_;
};
