#pragma once
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

/**
 * KLLcache.hpp
 *
 * KLLcache —— 基于 KLL Sketch 的在线概率分布缓存
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * 【核心问题】
 *   给定流式数据 x₁, x₂, ..., x_n,如何用 o(n) 空间回答分位数/CDF 查询?
 *
 * 【解决方案】
 *   KLL Sketch (Karnin-Lang-Liberty, FOCS 2016) 使用分层随机压缩:
 *   - 原始 KLL:空间 O(k · log log n),使用递减容量 capacity[ℓ] = ⌈k·(2/3)^ℓ⌉
 *   - 简化版本:空间 O(k · log(n/k)),所有层统一容量 k
 *   - 误差:对任意秩查询,|r̂ - r| ≤ ε·n,其中 ε = O(1/k)
 *   - 支持合并(mergeable),适用于分布式计算
 *
 * 【数据结构】
 *   多层 Level:level ℓ 的样本隐式权重为 w = 2^ℓ
 *   - Level 0:权重 1,直接存储新样本
 *   - Level ℓ:权重 2^ℓ,由 level ℓ-1 压缩晋升而来
 *
 * 【Compaction 操作】
 *   当某层超过容量时触发:
 *   1. 排序该层样本
 *   2. 随机选择偶数或奇数下标(概率各 1/2)
 *   3. 保留选中的一半,晋升到上一层
 *   数学性质:保持秩估计的无偏性 E[r̂] = r
 *
 * 【重建接口】
 *   四大族(CDF / PDF / ICDF / QDF),使用 PCHIP 重建方法:
 *   PCHIP:单调三次 Hermite 插值(C¹平滑),保单调,快速
 *
 * 【参考文献】
 *   [1] Karnin, Lang, Liberty. "Optimal Quantile Approximation in Streams"
 *       FOCS 2016. https://arxiv.org/abs/1603.05346
 *   [2] Jaynes. "Information Theory and Statistical Mechanics" 1957.
 *   [3] Fritsch, Carlson. "Monotone Piecewise Cubic Interpolation" 1980.
 * ═══════════════════════════════════════════════════════════════════════════
 */

// ═══════════════════════════════════════════════════════════════════════════
// 重建参数
// ═══════════════════════════════════════════════════════════════════════════

// CDF/ICDF高斯滤波参数:半径占总点数的百分比
// - ratio = 0: 关闭滤波
// - ratio = 0.01~0.03: 轻度平滑(1%~3%半径)
// - ratio = 0.05~0.10: 中度平滑(5%~10%半径)
// 滤波后的CDF/ICDF差分会传递到PDF/QDF,实现整体平滑
constexpr float CDF_BLUR_RADIUS_RATIO = 0.05f;

class KLLcache {
public:
  /**
   * LinePtr — 零拷贝导出结果
   *
   * 【设计原则】
   *   数据持久化在 KLLcache 的内部 Cache 中,指针生命期与 KLLcache 绑定。
   *   避免每次导出都动态分配,提高性能。
   *
   * 【注意事项】
   *   - 不要持有指针超过对象生命期
   */
  struct LinePtr {
    const float *x; // x 坐标指针(CDF/PDF: value domain; ICDF/QDF: [0,1])
    const float *y; // y 值指针(CDF/ICDF: function values; PDF/QDF: density)
    size_t n;       // 数组长度
  };

public:
  // ----------------
  // 构造 / 析构 / 移动
  // ----------------

  /**
   * @brief 构造函数
   * @param k  每层容量(越大精度越高,内存/时间开销越大)
   * @param n_recon  重建点数(CDF/ICDF的采样点数,PDF/QDF为n_recon-1)
   *
   * 【数学理论】
   *   基于 KLL Sketch (Karnin-Lang-Liberty, 2016) 的简化版本。
   *   给定容量参数 k,sketch 通过分层随机压缩维持固定空间。
   *
   * 【简化设计】
   *   原始 KLL:capacity[ℓ] = ⌈k · (2/3)^ℓ⌉  (递减)
   *   简化版本:capacity[ℓ] = k              (统一)
   *
   *   优势:
   *   - 参数简单:只有两个参数 k(精度)和 n_recon(分辨率)
   *   - 行为可预测:每层容量相同,重建点数固定
   *   - 实用性强:对于中等规模数据(<10M),统一容量效果更好
   *
   * 【公式定义】
   *   - 每层容量:capacity[ℓ] = k
   *   - 总层数上界:L = ⌈log₂(n/k)⌉
   *   - 总存储项数:m = k · L = O(k · log(n/k))
   *
   * 【参数值域】
   *   k ∈ [2, ∞),推荐 k ≥ 100
   *   典型值:k=256 (快速), k=512 (平衡), k=1024 (高精度)
   *
   *   n_recon ∈ [2, ∞),推荐 n_recon ≥ 100
   *   典型值:n_recon=100 (快速), n_recon=300 (平衡), n_recon=1000 (高分辨率)
   *
   * 【Motivation】
   *   统一参数化使得:
   *   1. API更简洁(不需要每次调用时传参数)
   *   2. 缓存更高效(一次性计算所有重建结果)
   *   3. 行为更可预测(固定的重建质量)
   */
  explicit KLLcache(size_t k = 512, size_t n_recon = 1024);

  ~KLLcache() = default;

  // 支持移动语义
  KLLcache(KLLcache &&) noexcept = default;
  KLLcache &operator=(KLLcache &&) noexcept = default;

  /**
   * @brief 清空所有状态,回到初始(empty)状态。
   *
   * 【语义】重置 n_total=0,清空所有 level buffer,min/max 未定义。
   */
  void clear();

  /**
   * @brief 检查是否为空(未插入任何数据)
   * @return n_total == 0
   */
  [[nodiscard]] bool empty() const noexcept { return count_ == 0; }

  // ----------------
  // 插入 / 合并(流式)
  // ----------------

  /**
   * addBatch
   *  - 输入:样本向量 X = {x₁, x₂, ..., x_m}
   *
   * 【数学理论】
   *   由于 KLL 的 mergeable 性质,插入顺序不影响最终的误差界。
   *
   * 【复杂度】(简化版本:capacity[ℓ] = k)
   *   - 时间:O(m + (m/k)·k log k) = O(m + m log k) 摊销
   *   - 触发 compaction 次数:O(m/k)
   *   - 每次 compaction 排序:O(k log k)
   *
   * 【Motivation】
   *   批量接口允许实现层面优化(如延迟排序、批量晋升),
   *   减少 compaction 的排序开销。
   */
  void addBatch(const std::vector<float> &X);

  /**
   * mergeWith
   *  - 输入:另一个 KLLcache(只读)
   *  - 前提:this.k == other.k
   *
   * 【数学理论】
   *   KLL sketch 具有 mergeable 性质:
   *   - 可交换:merge(A,B) ≈ merge(B,A)(误差界相同)
   *   - 可结合:merge(merge(A,B),C) ≈ merge(A,merge(B,C))
   *
   *   合并算法:
   *   1. 对每层 ℓ:this.buffer[ℓ] ← this.buffer[ℓ] ∪ other.buffer[ℓ]
   *   2. 从 level 0 向上依次执行 compaction 直到所有层满足容量约束
   *
   * 【误差界】
   *   设 this 来自 n₁ 个样本,other 来自 n₂ 个样本。
   *   合并后对任意秩 r 的估计 r̂ 满足:
   *       |r̂ - r| ≤ ε·(n₁ + n₂)
   *
   * 【Motivation】
   *   mergeable 性质支持分布式计算:各节点独立构建 sketch,
   *   最后合并得到全局分位数估计,通信开销仅 O(k)。
   */
  void mergeWith(const KLLcache &other);

  // ═══════════════════════════════════════════════════════════════════════════
  // 四大导出接口(CDF / PDF / ICDF / QDF)
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * ═══════════════════════════════════════════════════════════════════════════
   * 四大导出接口(4-Object System)
   * ═══════════════════════════════════════════════════════════════════════════
   *
   * 【对偶结构】
   *   x 空间:     CDF(x) → PDF(x) = dF/dx
   *   u 空间:     ICDF(u) → QDF(u) = dQ/du
   *
   * 【参数化设计】
   *   - k 和 n_recon 在构造函数统一设置
   *   - build cache 时使用 PCHIP 方法计算
   *   - export 时返回缓存结果的零拷贝视图
   *
   * 【数据规模】
   *   - CDF/ICDF: n_recon 个点
   *   - PDF/QDF:  n_recon-1 个点(区间中点)
   * ═══════════════════════════════════════════════════════════════════════════
   */

  /**
   * exportCDF — 导出累积分布函数 F(x)
   *
   * 【数学定义】F(x) = P(X ≤ x),单调非减,F(-∞)=0, F(+∞)=1
   * 【算法】PCHIP 单调三次 Hermite 插值(C¹平滑)
   * 【输出】{x ∈ [min,max], F ∈ [0,1], n=n_recon}
   * 【前提】!empty()
   */
  [[nodiscard]] LinePtr exportCDF() const;

  /**
   * exportPDF — 导出概率密度函数 f(x) = dF/dx
   *
   * 【数学定义】f(x) ≥ 0,∫ f(x)dx = 1
   * 【算法】从 PCHIP 平滑的 CDF 差分得到
   * 【输出】{x ∈ [min,max], f ≥ 0, n=n_recon-1}
   * 【前提】!empty()
   */
  [[nodiscard]] LinePtr exportPDF() const;

  /**
   * exportICDF — 导出逆累积分布函数 Q(u) = F^{-1}(u)
   *
   * 【数学定义】Q(u) = inf{x: F(x)≥u},单调非减,Q(0)=min, Q(1)=max
   * 【算法】PCHIP 单调三次 Hermite 插值(C¹平滑)
   * 【输出】{u ∈ [0,1], Q ∈ [min,max], n=n_recon}
   * 【前提】!empty()
   */
  [[nodiscard]] LinePtr exportICDF() const;

  /**
   * exportQDF — 导出分位密度函数 ρ(u) = dQ/du = 1/f(Q(u))
   *
   * 【数学定义】ρ(u) ≥ 0,通过 ICDF 导数估计密度(对偶于 PDF)
   * 【算法】从 PCHIP 平滑的 ICDF 差分得到
   * 【输出】{u ∈ [0,1], ρ ≥ 0, n=n_recon-1}
   * 【前提】!empty()
   */
  [[nodiscard]] LinePtr exportQDF() const;

  // ----------------
  // 内部状态导出(用于调试/测试)
  // ----------------

  /**
   * exportWeightedSamples
   *  - 输出:(values, weights),其中 weights[i] = 2^{level(i)}
   *  - 前提:!empty()
   *
   * 【用途】
   *   导出 KLL 内部的加权样本,用于自定义重建算法或调试。
   */
  std::pair<std::vector<float>, std::vector<uint64_t>>
  exportWeightedSamples() const;

  /**
   * dumpLevels — 调试:导出所有层的 buffer
   *  - 输出:levels[ℓ] = 第 ℓ 层的样本数组
   */
  std::vector<std::vector<float>> dumpLevels() const;

  // ----------------
  // 统计信息
  // ----------------

  /**
   * storedSize — 当前存储的样本总数(所有层的 buffer 大小之和)
   */
  [[nodiscard]] size_t storedSize() const noexcept;

  /**
   * totalCount — 插入的总样本数(包括已被压缩的)
   */
  [[nodiscard]] uint64_t totalCount() const noexcept;

  /**
   * range — 返回 (min, max)
   *  - 前提:!empty()
   */
  [[nodiscard]] std::pair<float, float> range() const noexcept;

  // ----------------
  // Moments (lazy computed from weighted samples)
  // ----------------
  // O(stored_size) ≈ O(k * log(n/k)), 只在首次查询时计算

  [[nodiscard]] double mean() const;
  [[nodiscard]] double var() const;
  [[nodiscard]] double skew() const;
  [[nodiscard]] double kurt() const;

  /**
   * getMemoryUsage — 返回 KLL sketch 的实际内存占用(bytes)
   *
   * 【计算内容】
   *   1. Level buffers: Σ_ℓ buffer[ℓ].capacity() * sizeof(float)
   *   2. Metadata: sizeof(Level) * levels_.size() + vector overhead
   *
   * 【简化说明】
   *   - 每层容量统一为 k,总层数 L ≈ log₂(n/k)
   *   - 实际占用 ≈ k * L * sizeof(float) + O(1)
   *   - 包括 vector 的 capacity,不只是 size
   *   - 不包括 cache(懒加载,仅在重建时使用)
   *
   * @return 总内存占用(bytes)
   */
  [[nodiscard]] size_t getMemoryUsage() const noexcept;

  /**
   * getCompressionRatio — 返回准确的压缩率
   *
   * 【定义】
   *   压缩率 = 原始数据大小 / KLL 实际内存占用
   *
   * 【原始数据大小】
   *   n_total * sizeof(float)  (如果要存储所有原始样本)
   *
   * 【KLL 内存占用】
   *   见 getMemoryUsage()
   *
   * @return 压缩率(倍数),例如 10.5 表示压缩了 10.5 倍
   */
  [[nodiscard]] float getCompressionRatio() const noexcept;

private:
  // ----------------
  // 内部数据结构
  // ----------------

  struct Level {
    std::vector<float> buffer; // 存储样本(隐式权重 2^ℓ)
    size_t capacity;           // 容量上界(触发 compaction)
  };

  struct WeightedPoint {
    float value;    // 样本值
    float cum_prob; // 累积概率
    float weight;   // 归一化权重
  };

  struct Cache {
    bool valid = false; // 缓存是否有效

    // ────────────────────────────────────────────────────────────────────────
    // Level 1: 原始加权样本
    // ────────────────────────────────────────────────────────────────────────
    std::vector<float> values;             // 样本值
    std::vector<uint64_t> weights;         // 权重 2^ℓ
    std::vector<WeightedPoint> sorted_pts; // 排序后的点集 {value, cum_prob, weight}

    // ────────────────────────────────────────────────────────────────────────
    // Level 2: CDF/ICDF knots(原始稀疏)
    // ────────────────────────────────────────────────────────────────────────
    std::vector<float> cdf_x, cdf_F;   // CDF knots: {x, F(x)}
    std::vector<float> icdf_u, icdf_Q; // ICDF knots: {u, Q(u)}

    // ────────────────────────────────────────────────────────────────────────
    // Level 3: 重建结果(PCHIP 方法)
    // ────────────────────────────────────────────────────────────────────────
    // PCHIP方法:单调三次 Hermite 插值(C¹平滑)
    std::vector<float> cdf_pchip_x, cdf_pchip_F;   // n 个点
    std::vector<float> pdf_pchip_x, pdf_pchip_f;   // n-1 个点(区间中点)
    std::vector<float> icdf_pchip_u, icdf_pchip_Q; // n 个点
    std::vector<float> qdf_pchip_u, qdf_pchip_rho; // n-1 个点(区间中点)

    void invalidate() noexcept {
      valid = false;
      moments_valid = false;
    }

    // ────────────────────────────────────────────────────────────────────────
    // Moments (lazy computed from weighted samples)
    // ────────────────────────────────────────────────────────────────────────
    bool moments_valid = false;
    double mean = 0.0;
    double var = 0.0;
    double skew = 0.0;
    double kurt = 0.0;
  };

  // ----------------
  // 成员变量
  // ----------------

  size_t capacity_;           // 每层容量 k
  size_t resolution_;         // 重建点数 n(CDF/ICDF: n个点；PDF/QDF: n-1个点)
  uint64_t count_;            // 插入的总样本数
  std::vector<Level> levels_; // 多层 buffer
  float min_, max_;           // 值域范围
  std::mt19937_64 rng_;       // 随机数生成器(用于 compaction)
  mutable Cache cache_;       // 缓存(懒加载)

  // ----------------
  // 内部辅助函数:压缩管理
  // ----------------

  void growLevels(size_t target_idx);
  void compactIfNeeded(size_t idx);
  void compact(size_t idx);

  // ----------------
  // 内部辅助函数:缓存管理
  // ----------------

  void invalidateCache() noexcept { cache_.invalidate(); }
  void validateCache() const;
  void buildCache() const;
  void validateMoments() const;
  void buildMoments() const;

  // ----------------
  // 内部辅助函数:重建(按对称性排列)
  // ----------------

  void buildCDF_PCHIP() const;
  void buildICDF_PCHIP() const;

  void buildPDF_PCHIP() const;
  void buildQDF_PCHIP() const;

  // PDF/QDF 公共逻辑:从累积函数差分得到密度
  static void densityFromCumulative(const float *x, const float *y, size_t n,
                                    float *x_mid, float *density);

  // 高斯滤波
  static void gaussianBlur1D(std::vector<float> &data, float sigma);

  // ----------------
  // 内部辅助函数:算法工具
  // ----------------

  /**
   * sortAndAccumulate — 从加权样本构造排序的 CDF 点列表
   *
   * 【输入】
   *   values[i]:样本值
   *   weights[i]:样本权重(2^ℓ)
   *
   * 【输出】
   *   pts:按 value 排序的 WeightedPoint 列表,每个点包含:
   *     - value:样本值
   *     - cum_prob:累积概率(归一化后)
   *     - weight:归一化权重(sum=1)
   *
   * 【算法】
   *   1. 对 values 排序(使用 indices 存储排序索引)
   *   2. 计算累积权重:W_j = Σ_{i≤j} weights[i]
   *   3. 归一化:cum_prob = W_j / W_total
   */
  static void sortAndAccumulate(const std::vector<float> &values,
                                const std::vector<uint64_t> &weights,
                                std::vector<WeightedPoint> &pts,
                                std::vector<size_t> &indices);

  /**
   * pchipSlopes — Fritsch-Carlson 单调三次 Hermite 斜率计算
   *
   * 【输入】
   *   x, y: knot 点 (x_j, y_j),要求 x 严格递增
   *
   * 【输出】
   *   slopes: 每个 knot 的导数 d_j,满足 PCHIP 单调性约束
   *
   * 【算法】
   *   1. 计算区间斜率:δ_j = (y_{j+1} - y_j) / (x_{j+1} - x_j)
   *   2. 内部点:用加权调和平均计算 d_j(保证单调性)
   *   3. 端点:用外推公式(满足 |d_0| ≤ 3|δ_0|)
   *   4. 全局约束:对每个区间,强制 α²+β² ≤ 9(防止过冲)
   *
   * 【性质】
   *   - 若 y 单调递增,则 PCHIP 插值 ŷ(x) 也单调递增
   *   - C¹ 连续(但不是 C²)
   *
   * 【参考】Fritsch & Carlson, 1980. "Monotone Piecewise Cubic Interpolation"
   */
  static void pchipSlopes(const std::vector<float> &x,
                          const std::vector<float> &y,
                          std::vector<float> &slopes,
                          std::vector<float> &widths,
                          std::vector<float> &deltas);

  /**
   * pchipEval — 批量 PCHIP 求值(向量化)
   *
   * 【输入】
   *   queries: 查询点列表
   *   knots_x, knots_y, slopes: PCHIP knots 和斜率
   *
   * 【输出】
   *   values: ŷ(queries[j])
   *   derivs (可选): dy/dx(queries[j])
   *
   * 【算法】
   *   对每个 x ∈ queries:
   *   1. 二分查找区间 [x_i, x_{i+1}]
   *   2. 计算局部坐标 t = (x - x_i) / h
   *   3. 用 Hermite basis 函数计算:
   *         ŷ(x) = H₀(t)·y_i + h·H₁(t)·d_i + H₂(t)·y_{i+1} + h·H₃(t)·d_{i+1}
   *   4. 若需要导数:dy/dx = (dH₀·y_i + h·dH₁·d_i + ...) / h
   */
  static void pchipEval(const std::vector<float> &queries,
                        const std::vector<float> &knots_x,
                        const std::vector<float> &knots_y,
                        const std::vector<float> &slopes,
                        std::vector<float> &values,
                        std::vector<float> *derivs = nullptr) noexcept;

  // 禁用拷贝
  KLLcache(const KLLcache &) = delete;
  KLLcache &operator=(const KLLcache &) = delete;
};

// ============================================================================
// Implementation
// ============================================================================

inline KLLcache::KLLcache(size_t k, size_t n_recon)
    : capacity_(k), resolution_(n_recon), count_(0), min_(0.0f), max_(0.0f),
      rng_(std::random_device{}()), cache_{} {
  assert(capacity_ >= 2);
  assert(resolution_ >= 2);
  levels_.reserve(32); // 足够存储 2^64 个样本
  growLevels(0);
}

inline void KLLcache::clear() {
  count_ = 0;
  for (auto &lv : levels_) {
    lv.buffer.clear();
  }
  invalidateCache();
}

inline void KLLcache::growLevels(size_t target_idx) {
  assert(target_idx < 32);
  while (levels_.size() <= target_idx) [[unlikely]] {
    Level lv;
    // 简化设计:所有层容量统一为 k
    lv.capacity = capacity_;
    // reserve 2k,容纳本层最多 k+1 个元素 + compact 晋升的 k/2
    // 最坏情况:k + k/2 = 1.5k < 2k,无需 reallocation
    lv.buffer.reserve(capacity_ * 2);
    levels_.push_back(std::move(lv));
  }
}

inline void KLLcache::compactIfNeeded(size_t idx) {
  if (levels_[idx].buffer.size() > levels_[idx].capacity) [[unlikely]] {
    compact(idx);
  }
}

/**
 * compact — 压缩第 idx 层
 *
 * 【算法】
 *   1. 排序 buffer(使用 std::sort,O(n log n))
 *   2. 随机选择偶数或奇数下标(rng() & 1)
 *   3. 选中的一半晋升到 idx+1 层
 *   4. 若有剩余元素(奇数个),保留在本层
 *   5. 递归检查 idx+1 层是否需要压缩
 */
inline void KLLcache::compact(size_t idx) {
  auto &buf = levels_[idx].buffer;
  size_t L = buf.size();

  // Step 1: 排序(std::sort 在现代编译器中已经是高度优化的 pdqsort)
  std::sort(buf.begin(), buf.end());
  growLevels(idx + 1);

  // Step 2: 选择压缩区间(保证长度为偶数)
  // 若 L 为奇数,则随机保留一个端点作为 leftover(避免系统性偏向一侧)
  size_t start = 0;
  size_t end = L;
  bool has_leftover = ((L & 1U) != 0);
  float leftover = 0.0f;
  if (has_leftover) [[unlikely]] {
    bool keep_low = static_cast<bool>(rng_() & 1ULL);
    if (keep_low) {
      leftover = buf[0];
      start = 1;
    } else {
      leftover = buf[L - 1];
      end = L - 1;
    }
  }
  size_t compact_len = end - start;
  assert((compact_len & 1U) == 0);
  size_t half_len = compact_len / 2;

  // Step 3: 随机选择偶/奇下标(一次 coin flip),每对取一个晋升到 idx+1
  auto &next_buf = levels_[idx + 1].buffer;
  size_t old_size = next_buf.size();
  next_buf.resize(old_size + half_len);

  size_t offset = static_cast<size_t>(rng_() & 1ULL);
  for (size_t i = 0; i < compact_len; i += 2) {
    next_buf[old_size + i / 2] = buf[start + i + offset];
  }

  // Step 4: 处理剩余元素
  buf.clear();
  if (has_leftover) [[unlikely]]
    buf.push_back(leftover);

  // Step 5: 递归检查上层
  compactIfNeeded(idx + 1);
}

inline void KLLcache::addBatch(const std::vector<float> &X) {
  if (X.empty()) [[unlikely]]
    return;

  invalidateCache();

  // 批量计算 min/max(向量化友好)
  float batch_min = X[0];
  float batch_max = X[0];
  for (size_t i = 1; i < X.size(); ++i) {
    if (X[i] < batch_min)
      batch_min = X[i];
    if (X[i] > batch_max)
      batch_max = X[i];
  }

  if (count_ == 0) [[unlikely]] {
    min_ = batch_min;
    max_ = batch_max;
  } else {
    if (batch_min < min_)
      min_ = batch_min;
    if (batch_max > max_)
      max_ = batch_max;
  }

  assert(count_ <= ((std::numeric_limits<uint64_t>::max)() - X.size()));
  count_ += X.size();

  // 批量插入到 level 0,保持 size ≤ k+1
  // 每层已 reserve 2k,插入到 k+1 无 reallocation
  // 大 batch 会分批处理:插满 k+1 → compact → 继续
  auto &buf = levels_[0].buffer;
  size_t idx = 0;
  while (idx < X.size()) {
    // 计算可插入数量(插入到 k+1 触发 compact)
    size_t remaining = capacity_ + 1 - buf.size();
    size_t to_insert = (std::min)(remaining, X.size() - idx);

    // 批量插入(无 reallocation,因为 capacity = 2k > k+1)
    buf.insert(buf.end(), X.begin() + idx, X.begin() + idx + to_insert);
    idx += to_insert;

    // 满了就 compact(减半)
    if (buf.size() > capacity_) {
      compact(0);
    }
  }
}

inline void KLLcache::mergeWith(const KLLcache &other) {
  if (other.empty()) [[unlikely]]
    return;

  assert(capacity_ == other.capacity_);

  invalidateCache();

  // 更新 min/max
  if (empty()) [[unlikely]] {
    min_ = other.min_;
    max_ = other.max_;
  } else {
    if (other.min_ < min_)
      min_ = other.min_;
    if (other.max_ > max_)
      max_ = other.max_;
  }

  assert(count_ <= ((std::numeric_limits<uint64_t>::max)() - other.count_));
  count_ += other.count_;

  // 合并各层:每层已 reserve 2k,足够容纳合并
  // 最坏情况:this_buf(k) + other_buf(k) = 2k,正好在容量内
  for (size_t i = 0; i < other.levels_.size(); i++) {
    growLevels(i);
    const auto &other_buf = other.levels_[i].buffer;
    if (other_buf.empty())
      continue;

    auto &this_buf = levels_[i].buffer;
    this_buf.insert(this_buf.end(), other_buf.begin(), other_buf.end());
  }

  // 从 level 0 向上触发 compaction,直到所有层满足容量约束
  for (size_t i = 0; i < levels_.size(); i++)
    compactIfNeeded(i);
}

inline std::pair<std::vector<float>, std::vector<uint64_t>>
KLLcache::exportWeightedSamples() const {
  validateCache();
  return {cache_.values, cache_.weights};
}

// ════════════════════════════════════════════════════════════════════════════
// Export 实现(4个对称接口)
// ════════════════════════════════════════════════════════════════════════════

// ════════════════════════════════════════════════════════════════════════════
// 四大导出接口实现(Export API)
// ════════════════════════════════════════════════════════════════════════════

inline KLLcache::LinePtr KLLcache::exportCDF() const {
  assert(!empty());
  validateCache();
  return {cache_.cdf_pchip_x.data(), cache_.cdf_pchip_F.data(), resolution_};
}

inline KLLcache::LinePtr KLLcache::exportPDF() const {
  assert(!empty());
  validateCache();
  return {cache_.pdf_pchip_x.data(), cache_.pdf_pchip_f.data(), resolution_ - 1};
}

inline KLLcache::LinePtr KLLcache::exportICDF() const {
  assert(!empty());
  validateCache();
  return {cache_.icdf_pchip_u.data(), cache_.icdf_pchip_Q.data(), resolution_};
}

inline KLLcache::LinePtr KLLcache::exportQDF() const {
  assert(!empty());
  validateCache();
  return {cache_.qdf_pchip_u.data(), cache_.qdf_pchip_rho.data(), resolution_ - 1};
}

inline std::vector<std::vector<float>> KLLcache::dumpLevels() const {
  std::vector<std::vector<float>> result;
  result.reserve(levels_.size());
  for (const auto &lv : levels_) {
    result.push_back(lv.buffer);
  }
  return result;
}

inline size_t KLLcache::storedSize() const noexcept {
  size_t m = 0;
  for (const auto &lv : levels_) {
    m += lv.buffer.size();
  }
  return m;
}

inline uint64_t KLLcache::totalCount() const noexcept { return count_; }

inline std::pair<float, float> KLLcache::range() const noexcept {
  assert(!empty());
  return {min_, max_};
}

// ════════════════════════════════════════════════════════════════════════════
// Moments (lazy computed from weighted samples)
// ════════════════════════════════════════════════════════════════════════════

inline void KLLcache::validateMoments() const {
  if (cache_.moments_valid) [[likely]]
    return;
  buildMoments();
}

inline void KLLcache::buildMoments() const {
  // 收集加权样本（不需要完整的 cache rebuild）
  size_t total_size = 0;
  for (const auto &lv : levels_) {
    total_size += lv.buffer.size();
  }

  if (total_size == 0) {
    cache_.mean = cache_.var = cache_.skew = cache_.kurt = 0.0;
    cache_.moments_valid = true;
    return;
  }

  // 单遍扫描计算加权 moments（two-pass for numerical stability）
  // Pass 1: 计算加权均值
  double sum_w = 0.0;
  double sum_wx = 0.0;
  for (size_t i = 0; i < levels_.size(); ++i) {
    double w = static_cast<double>(1ULL << i);
    for (float v : levels_[i].buffer) {
      sum_w += w;
      sum_wx += w * v;
    }
  }
  double mu = sum_wx / sum_w;

  // Pass 2: 计算中心矩 M2, M3, M4
  double M2 = 0.0, M3 = 0.0, M4 = 0.0;
  for (size_t i = 0; i < levels_.size(); ++i) {
    double w = static_cast<double>(1ULL << i);
    for (float v : levels_[i].buffer) {
      double d = v - mu;
      double d2 = d * d;
      M2 += w * d2;
      M3 += w * d2 * d;
      M4 += w * d2 * d2;
    }
  }

  cache_.mean = mu;
  cache_.var = M2 / sum_w;

  double sigma2 = cache_.var;
  if (sigma2 < 1e-14) {
    cache_.skew = 0.0;
    cache_.kurt = 0.0;
  } else {
    double sigma3 = sigma2 * std::sqrt(sigma2);
    double sigma4 = sigma2 * sigma2;
    cache_.skew = (M3 / sum_w) / sigma3;
    cache_.kurt = (M4 / sum_w) / sigma4 - 3.0;
  }

  cache_.moments_valid = true;
}

inline double KLLcache::mean() const {
  assert(!empty());
  validateMoments();
  return cache_.mean;
}

inline double KLLcache::var() const {
  assert(!empty());
  validateMoments();
  return cache_.var;
}

inline double KLLcache::skew() const {
  assert(!empty());
  validateMoments();
  return cache_.skew;
}

inline double KLLcache::kurt() const {
  assert(!empty());
  validateMoments();
  return cache_.kurt;
}

inline size_t KLLcache::getMemoryUsage() const noexcept {
  size_t total_bytes = 0;

  // Level buffers (actual stored samples)
  for (const auto &lv : levels_) {
    total_bytes += lv.buffer.size() * sizeof(float);
  }

  return total_bytes;
}

inline float KLLcache::getCompressionRatio() const noexcept {
  if (count_ == 0)
    return 1.0f;

  size_t original_size = static_cast<size_t>(count_) * sizeof(float);
  size_t kll_size = getMemoryUsage();

  if (kll_size == 0)
    return 1.0f;

  return static_cast<float>(original_size) / static_cast<float>(kll_size);
}

// ----------------------------------------------------------------------------
// Internal Algorithms
// ----------------------------------------------------------------------------

inline void KLLcache::sortAndAccumulate(const std::vector<float> &values,
                                        const std::vector<uint64_t> &weights,
                                        std::vector<WeightedPoint> &pts,
                                        std::vector<size_t> &indices) {
  size_t n = values.size();
  assert(weights.size() == n);

  pts.clear();
  if (n == 0) [[unlikely]]
    return;

  pts.reserve(n);

  // Step 1: 对 values 排序(使用 indices 存储排序索引)
  indices.resize(n);
  std::iota(indices.begin(), indices.end(), size_t{0});
  std::sort(indices.begin(), indices.end(),
            [&](size_t a, size_t b) { return values[a] < values[b]; });

  // Step 2: 计算总权重(使用 std::accumulate 可能被编译器向量化)
  uint64_t total_w =
      std::accumulate(weights.begin(), weights.end(), uint64_t{0});
  assert(total_w > 0);
  float inv_total = 1.0f / static_cast<float>(total_w);

  // Step 3: 构造排序的 CDF 点列表(循环融合优化)
  float cum = 0.0f;
  for (size_t i = 0; i < n; i++) {
    size_t j = indices[i];
    float w_norm = static_cast<float>(weights[j]) * inv_total;
    cum += w_norm;
    pts.push_back({values[j], cum, w_norm});
  }
}

/**
 * pchipSlopes — Fritsch-Carlson 单调三次 Hermite 斜率计算
 *
 * 【参考】Fritsch & Carlson, 1980. "Monotone Piecewise Cubic Interpolation"
 *
 * 【核心思想】
 *   对于单调数据(y 递增),构造单调三次插值的关键是斜率 d_i 的选择。
 *   PCHIP 使用加权调和平均 + 端点外推 + 全局约束(α²+β²≤9)保证单调性。
 */
inline void KLLcache::pchipSlopes(const std::vector<float> &x,
                                  const std::vector<float> &y,
                                  std::vector<float> &slopes,
                                  std::vector<float> &widths,
                                  std::vector<float> &deltas) {
  size_t n = x.size();
  assert(n >= 2);

  slopes.assign(n, 0.0f);

  // 特殊情况:只有 2 个点,用线性斜率
  if (n == 2) [[unlikely]] {
    float slope = (y[1] - y[0]) / (x[1] - x[0]);
    slopes[0] = slopes[1] = slope;
    return;
  }

  // Step 1: 计算区间宽度和区间斜率
  widths.resize(n - 1);
  deltas.resize(n - 1);
  for (size_t i = 0; i < n - 1; i++) {
    widths[i] = x[i + 1] - x[i];
    deltas[i] = (widths[i] > 0) ? (y[i + 1] - y[i]) / widths[i] : 0.0f;
  }

  // Step 2: 内部点斜率 — 加权调和平均
  for (size_t i = 1; i < n - 1; i++) {
    if (deltas[i - 1] * deltas[i] <= 0) [[unlikely]] {
      // 不同号或为零 → 极值点,斜率置零
      slopes[i] = 0.0f;
    } else {
      // 加权调和平均
      float w1 = 2.0f * widths[i] + widths[i - 1];
      float w2 = widths[i] + 2.0f * widths[i - 1];
      slopes[i] = (w1 + w2) / (w1 / deltas[i - 1] + w2 / deltas[i]);
    }
  }

  // Step 3: 端点斜率 — 外推公式
  {
    // 左端点 slopes[0]
    float d0 = deltas[0], d1 = deltas[1];
    if (d0 == 0.0f) {
      slopes[0] = 0.0f;
    } else if (d1 == 0.0f) {
      slopes[0] = d0;
    } else if (d0 * d1 <= 0.0f) {
      slopes[0] = 0.0f; // 不同号
    } else {
      float dx0 = widths[0], dx1 = widths[1];
      float denom = dx0 + dx1;
      float s0 =
          (denom == 0.0f) ? 0.0f : ((2.0f * dx0 + dx1) * d0 - dx0 * d1) / denom;
      if (s0 * d0 < 0.0f)
        s0 = 0.0f;
      if (std::abs(s0) > 3.0f * std::abs(d0))
        s0 = 3.0f * d0;
      slopes[0] = s0;
    }

    // 右端点 slopes[n-1]
    float dn1 = deltas[n - 2], dn2 = deltas[n - 3];
    if (dn1 == 0.0f) {
      slopes[n - 1] = 0.0f;
    } else if (dn2 == 0.0f) {
      slopes[n - 1] = dn1;
    } else if (dn1 * dn2 <= 0.0f) {
      slopes[n - 1] = 0.0f;
    } else {
      float dxn2 = widths[n - 2], dxn3 = widths[n - 3];
      float denom = dxn2 + dxn3;
      float sn = (denom == 0.0f)
                     ? 0.0f
                     : ((2.0f * dxn2 + dxn3) * dn1 - dxn2 * dn2) / denom;
      if (sn * dn1 < 0.0f)
        sn = 0.0f;
      if (std::abs(sn) > 3.0f * std::abs(dn1))
        sn = 3.0f * dn1;
      slopes[n - 1] = sn;
    }
  }

  // Step 4: 强制 α²+β² ≤ 9(防止过冲)
  for (size_t i = 0; i < n - 1; i++) {
    if (deltas[i] == 0.0f) [[unlikely]] {
      slopes[i] = 0.0f;
      slopes[i + 1] = 0.0f;
    } else {
      float alpha = slopes[i] / deltas[i];
      float beta = slopes[i + 1] / deltas[i];
      float r2 = alpha * alpha + beta * beta;
      if (r2 > 9.0f) [[unlikely]] {
        float tau = 3.0f / std::sqrt(r2);
        slopes[i] = tau * alpha * deltas[i];
        slopes[i + 1] = tau * beta * deltas[i];
      }
    }
  }
}

/**
 * pchipEval — 批量 PCHIP 求值(向量化)
 *
 * 【算法】
 *   对每个 x ∈ queries:
 *   1. 二分查找区间 [x_i, x_{i+1}] 使得 x_i ≤ x < x_{i+1}
 *   2. 计算局部坐标 t = (x - x_i) / h,其中 h = x_{i+1} - x_i
 *   3. 用 Hermite basis 函数计算:
 *         ŷ(x) = H₀(t)·y_i + h·H₁(t)·d_i + H₂(t)·y_{i+1} + h·H₃(t)·d_{i+1}
 *      其中:
 *         H₀(t) = 2t³ - 3t² + 1
 *         H₁(t) = t³ - 2t² + t
 *         H₂(t) = -2t³ + 3t²
 *         H₃(t) = t³ - t²
 *   4. 若需要导数:
 *         dy/dx = (dH₀·y_i + h·dH₁·d_i + dH₂·y_{i+1} + h·dH₃·d_{i+1}) / h
 *      其中:
 *         dH₀(t) = 6t² - 6t
 *         dH₁(t) = 3t² - 4t + 1
 *         dH₂(t) = -6t² + 6t
 *         dH₃(t) = 3t² - 2t
 *
 */
inline void KLLcache::pchipEval(const std::vector<float> &queries,
                                const std::vector<float> &knots_x,
                                const std::vector<float> &knots_y,
                                const std::vector<float> &slopes,
                                std::vector<float> &values,
                                std::vector<float> *derivs) noexcept {
  size_t m = queries.size();
  size_t n = knots_x.size();

  values.resize(m);
  if (derivs)
    derivs->resize(m);

  if (n < 2 || m == 0) [[unlikely]]
    return;

  const float *__restrict xk_ptr = knots_x.data();
  const float *__restrict yk_ptr = knots_y.data();
  const float *__restrict dk_ptr = slopes.data();

  size_t cached_interval = 0;

  for (size_t j = 0; j < m; ++j) {
    float x = queries[j];

    // 边界情况
    if (x <= xk_ptr[0]) [[unlikely]] {
      values[j] = yk_ptr[0];
      if (derivs)
        (*derivs)[j] = dk_ptr[0];
      continue;
    }
    if (x >= xk_ptr[n - 1]) [[unlikely]] {
      values[j] = yk_ptr[n - 1];
      if (derivs)
        (*derivs)[j] = dk_ptr[n - 1];
      continue;
    }

    // 二分查找区间(利用缓存加速)
    size_t i = cached_interval;
    // 快速路径:检查是否在缓存区间内
    if (x >= xk_ptr[i] && x < xk_ptr[i + 1]) [[likely]] {
      // 在缓存区间内,直接使用
    } else [[unlikely]] {
      // 不在缓存区间,重新二分查找
      size_t lo = 0, hi = n - 2;
      while (lo < hi) {
        size_t mid = (lo + hi + 1) >> 1;
        if (xk_ptr[mid] <= x)
          lo = mid;
        else
          hi = mid - 1;
      }
      i = lo;
      cached_interval = i;
    }

    // PCHIP 插值
    float h = xk_ptr[i + 1] - xk_ptr[i];
    if (h < 1e-15f) [[unlikely]] {
      values[j] = yk_ptr[i];
      if (derivs)
        (*derivs)[j] = 0.0f;
      continue;
    }

    float t = (x - xk_ptr[i]) / h;
    float t2 = t * t;
    float t3 = t2 * t;

    // Hermite basis functions
    float t2_3 = t2 * 3.0f;
    float t3_2 = t3 * 2.0f;

    float H00 = t3_2 - t2_3 + 1.0f; // 2t³ - 3t² + 1
    float H10 = t3 - t2 * 2.0f + t; // t³ - 2t² + t
    float H01 = t2_3 - t3_2;        // -2t³ + 3t²
    float H11 = t3 - t2;            // t³ - t²

    float yi = yk_ptr[i], yi1 = yk_ptr[i + 1];
    float di = dk_ptr[i], di1 = dk_ptr[i + 1];

    float hdi = h * di, hdi1 = h * di1;
    values[j] = yi * H00 + hdi * H10 + yi1 * H01 + hdi1 * H11;

    // 计算导数(可选)
    if (derivs) {
      float t_6 = t * 6.0f;
      float dH00 = t2 * 6.0f - t_6;             // 6t² - 6t
      float dH10 = t2 * 3.0f - t * 4.0f + 1.0f; // 3t² - 4t + 1
      float dH01 = t_6 - t2 * 6.0f;             // -6t² + 6t
      float dH11 = t2 * 3.0f - t * 2.0f;        // 3t² - 2t
      (*derivs)[j] = (yi * dH00 + hdi * dH10 + yi1 * dH01 + hdi1 * dH11) / h;
    }
  }
}

/**
 * densityFromCumulative — 从累积函数差分得到密度函数
 *
 * 【算法】区间密度 = Δy / Δx
 * 【应用】
 *   - PDF: 从 CDF(x) 差分得到 f(x) = dF/dx
 *   - QDF: 从 ICDF(u) 差分得到 ρ(u) = dQ/du
 */
inline void KLLcache::densityFromCumulative(const float *x, const float *y,
                                            size_t n, float *x_mid, float *density) {
  for (size_t i = 0; i < n - 1; i++) {
    float dx = x[i + 1] - x[i];
    float dy = y[i + 1] - y[i];
    x_mid[i] = (x[i] + x[i + 1]) * 0.5;
    density[i] = (dx > 0) ? (std::max)(0.0f, dy / dx) : 0.0f;
  }
}

/**
 * gaussianBlur1D — 1D高斯滤波平滑
 *
 * 【算法】
 *   使用高斯核 G(x) = exp(-x²/(2σ²)) 进行卷积,radius = ⌈3σ⌉
 *   边界处理:镜像对称
 *
 * 【参数】
 *   data: 输入/输出数组(原地修改)
 *   sigma: 高斯核标准差(控制平滑强度)
 */
inline void KLLcache::gaussianBlur1D(std::vector<float> &data, float sigma) {
  if (sigma <= 0.0f || data.size() < 2)
    return;

  int radius = static_cast<int>(std::ceil(3.0f * sigma));
  int len = static_cast<int>(data.size());
  std::vector<float> kernel(2 * radius + 1);
  std::vector<float> temp(data.size());

  // 计算高斯核(中心对称,和为1)
  float sum = 0.0f;
  float denom = 2.0f * sigma * sigma;
  for (int i = -radius; i <= radius; i++) {
    float val = std::exp(-(i * i) / denom);
    kernel[i + radius] = val;
    sum += val;
  }
  for (auto &v : kernel)
    v /= sum;

  // 卷积,使用镜像边界处理
  for (int i = 0; i < len; i++) {
    float acc = 0.0f;
    for (int k = -radius; k <= radius; k++) {
      int idx = i + k;
      // 镜像边界
      if (idx < 0)
        idx = -idx;
      else if (idx >= len)
        idx = 2 * len - idx - 2;
      acc += data[idx] * kernel[k + radius];
    }
    temp[i] = acc;
  }

  // 赋值回原数组
  std::swap(data, temp);
}

// ----------------------------------------------------------------------------
// Cache Management
// ----------------------------------------------------------------------------

inline void KLLcache::validateCache() const {
  if (cache_.valid) [[likely]]
    return;
  buildCache();
}

/**
 * buildCache — 重建内部 3-level 缓存
 *
 * 【三层结构】
 *   Level 1: 加权样本 (values, weights) + 排序点集 (sorted_pts)
 *   Level 2: CDF/ICDF knots(原始稀疏)
 *   Level 3: 重建结果(PCHIP 方法,四种函数)
 *
 * 【算法流程】
 *   1. 收集所有 levels 的样本 → (values, weights)
 *   2. 排序 + 累积 → sorted_pts {value, cum_prob, weight}
 *   3. 构造 CDF knots: 去重 x 轴 → (cdf_x, cdf_F)
 *   4. 构造 ICDF knots: 去重 u 轴 → (icdf_u, icdf_Q)
 *   5. 一次性重建所有4组数据:CDF/PDF/ICDF/QDF(PCHIP方法)
 */
inline void KLLcache::buildCache() const {
  // ══════════════════════════════════════════════════════════════════════════
  // Level 1: 收集加权样本
  // ══════════════════════════════════════════════════════════════════════════
  size_t total_size = 0;
  for (const auto &lv : levels_) {
    total_size += lv.buffer.size();
  }

  cache_.values.clear();
  cache_.weights.clear();
  cache_.values.reserve(total_size);
  cache_.weights.reserve(total_size);

  for (size_t i = 0; i < levels_.size(); i++) {
    uint64_t w = 1ULL << i; // 权重 2^ℓ
    const auto &buf = levels_[i].buffer;
    for (float v : buf) {
      cache_.values.push_back(v);
      cache_.weights.push_back(w);
    }
  }

  // 排序并计算累积概率
  static thread_local std::vector<size_t> indices;
  sortAndAccumulate(cache_.values, cache_.weights, cache_.sorted_pts, indices);

  // ══════════════════════════════════════════════════════════════════════════
  // Level 2: 构造 CDF knots(去重 x 轴)
  // ══════════════════════════════════════════════════════════════════════════
  size_t estimated_knots = cache_.sorted_pts.size() + 2;
  cache_.cdf_x.clear();
  cache_.cdf_F.clear();
  cache_.cdf_x.reserve(estimated_knots);
  cache_.cdf_F.reserve(estimated_knots);

  // 中间点去重:相同 x → 保留最大 F(处理离散质量点)
  for (const auto &p : cache_.sorted_pts) {
    if (!cache_.cdf_x.empty() && p.value == cache_.cdf_x.back()) {
      cache_.cdf_F.back() = p.cum_prob;
    } else {
      cache_.cdf_x.push_back(p.value);
      cache_.cdf_F.push_back(p.cum_prob);
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // Level 2: 构造 ICDF knots(去重 u 轴)
  // ══════════════════════════════════════════════════════════════════════════
  cache_.icdf_u.clear();
  cache_.icdf_Q.clear();
  cache_.icdf_u.reserve(estimated_knots);
  cache_.icdf_Q.reserve(estimated_knots);

  // 中间点去重:相同 u → 保留最大 Q(处理 CDF 平台)
  float last_cum_prob = -1.0f;
  for (const auto &p : cache_.sorted_pts) {
    if (p.cum_prob == last_cum_prob) {
      cache_.icdf_Q.back() = p.value;
    } else {
      cache_.icdf_u.push_back(p.cum_prob);
      cache_.icdf_Q.push_back(p.value);
      last_cum_prob = p.cum_prob;
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // Level 3: 重建所有4组数据(PCHIP方法)
  // ══════════════════════════════════════════════════════════════════════════
  buildCDF_PCHIP();
  buildICDF_PCHIP();

  buildPDF_PCHIP(); // 依赖 CDF_PCHIP
  buildQDF_PCHIP(); // 依赖 ICDF_PCHIP

  cache_.valid = true;
}

// ════════════════════════════════════════════════════════════════════════════

/**
 * buildCDF_PCHIP — PCHIP方法重建CDF
 *
 * 【算法】从稀疏knots用PCHIP插值到resolution个均匀点
 */
inline void KLLcache::buildCDF_PCHIP() const {
  const size_t n = resolution_;
  cache_.cdf_pchip_x.resize(n);
  cache_.cdf_pchip_F.resize(n);

  // 退化情况
  if (min_ == max_) [[unlikely]] {
    for (size_t i = 0; i < n; i++) {
      cache_.cdf_pchip_x[i] = min_;
      cache_.cdf_pchip_F[i] = 1.0f;
    }
    return;
  }

  // 均匀采样 x 轴
  float dx = (max_ - min_) / static_cast<float>(n - 1);
  for (size_t i = 0; i < n; i++) {
    cache_.cdf_pchip_x[i] = min_ + static_cast<float>(i) * dx;
  }

  // PCHIP 插值
  static thread_local std::vector<float> slopes, widths, deltas;
  pchipSlopes(cache_.cdf_x, cache_.cdf_F, slopes, widths, deltas);
  pchipEval(cache_.cdf_pchip_x, cache_.cdf_x, cache_.cdf_F, slopes,
            cache_.cdf_pchip_F, nullptr);

  // Clamp to [0, 1]
  for (size_t i = 0; i < n; i++) {
    cache_.cdf_pchip_F[i] = std::clamp(cache_.cdf_pchip_F[i], 0.0f, 1.0f);
  }

  // 高斯滤波平滑(自适应sigma)
  float adaptive_sigma = resolution_ * CDF_BLUR_RADIUS_RATIO / 3.0f; // radius ≈ 3*sigma
  gaussianBlur1D(cache_.cdf_pchip_F, adaptive_sigma);
}

/**
 * buildPDF_PCHIP — PCHIP方法重建PDF
 *
 * 【算法】对已重建的CDF_PCHIP差分得到PDF
 */
inline void KLLcache::buildPDF_PCHIP() const {
  const size_t n = resolution_ - 1; // n-1个区间
  cache_.pdf_pchip_x.resize(n);
  cache_.pdf_pchip_f.resize(n);

  // 退化情况
  if (min_ == max_) [[unlikely]] {
    for (size_t i = 0; i < n; i++) {
      cache_.pdf_pchip_x[i] = min_;
      cache_.pdf_pchip_f[i] = 0.0f;
    }
    return;
  }

  // 对CDF差分得到PDF
  densityFromCumulative(cache_.cdf_pchip_x.data(), cache_.cdf_pchip_F.data(),
                        resolution_, cache_.pdf_pchip_x.data(), cache_.pdf_pchip_f.data());
}

/**
 * buildICDF_PCHIP — PCHIP方法重建ICDF
 *
 * 【算法】从稀疏knots用PCHIP插值到resolution个均匀点
 */
inline void KLLcache::buildICDF_PCHIP() const {
  const size_t n = resolution_;
  cache_.icdf_pchip_u.resize(n);
  cache_.icdf_pchip_Q.resize(n);

  if (cache_.icdf_u.empty()) [[unlikely]] {
    return;
  }

  // 均匀采样概率轴 [u_min, u_max]
  float u_min = cache_.icdf_u.front();
  float u_max = cache_.icdf_u.back();
  float du = (u_max - u_min) / static_cast<float>(n - 1);
  for (size_t i = 0; i < n; i++) {
    cache_.icdf_pchip_u[i] = u_min + static_cast<float>(i) * du;
  }

  // PCHIP 插值
  static thread_local std::vector<float> slopes, widths, deltas;
  pchipSlopes(cache_.icdf_u, cache_.icdf_Q, slopes, widths, deltas);
  pchipEval(cache_.icdf_pchip_u, cache_.icdf_u, cache_.icdf_Q, slopes,
            cache_.icdf_pchip_Q, nullptr);

  // Clamp to [min, max]
  for (size_t i = 0; i < n; i++) {
    cache_.icdf_pchip_Q[i] = std::clamp(cache_.icdf_pchip_Q[i], min_, max_);
  }

  // 高斯滤波平滑（自适应sigma)
  float adaptive_sigma = resolution_ * CDF_BLUR_RADIUS_RATIO / 3.0f; // radius ≈ 3*sigma
  gaussianBlur1D(cache_.icdf_pchip_Q, adaptive_sigma);
}

/**
 * buildQDF_PCHIP — PCHIP方法重建QDF
 *
 * 【算法】对已重建的ICDF_PCHIP差分得到QDF
 */
inline void KLLcache::buildQDF_PCHIP() const {
  const size_t n = resolution_ - 1; // n-1个区间
  cache_.qdf_pchip_u.resize(n);
  cache_.qdf_pchip_rho.resize(n);

  // 对ICDF差分得到QDF
  densityFromCumulative(cache_.icdf_pchip_u.data(), cache_.icdf_pchip_Q.data(),
                        resolution_, cache_.qdf_pchip_u.data(), cache_.qdf_pchip_rho.data());
}
