#pragma once
#include <vector>
#include <cstdint>
#include <utility>
#include <random>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <cassert>
#include <limits>

/**
 * ═══════════════════════════════════════════════════════════════════════════
 * Implementation Uncertainty List
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * 1. [MaxEntropy] 当前 MaxEnt 实现是简化版（piecewise constant PDF）
 *    - 数学上正确：给定 CDF 约束 F(x_i)=F_i，区间内均匀分布确实是 MaxEnt 解
 *    - 但不是完整的指数族形式：f*(x) = exp(Σ λ_j φ_j(x)) / Z
 *    - 完整实现需要：Newton 迭代求解 Lagrange 乘子 + 数值积分 + 正则化
 *    - 当前实现：已标记为 "Simplified MaxEnt"，待替换为完整版本
 *
 * ═══════════════════════════════════════════════════════════════════════════
 */

// Forward declarations and core types for kll_detail
namespace kll_detail {

struct WeightedPoint {
    double value;
    double cum_prob;   // cumulative probability at this point (after including this point)
    double weight;     // normalized weight of this point
};

} // namespace kll_detail

/**
 * KLLcache.hpp
 *
 * KLLcache —— 基于 KLL Sketch 的在线概率分布缓存
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * 【核心问题】
 *   给定流式数据 x₁, x₂, ..., x_n，如何用 o(n) 空间回答分位数/CDF 查询？
 *
 * 【解决方案】
 *   KLL Sketch (Karnin-Lang-Liberty, FOCS 2016) 使用分层随机压缩：
 *   - 空间：O(k · log log n)，其中 k 控制精度
 *   - 误差：对任意秩查询，|r̂ - r| ≤ ε·n，其中 ε = O(1/k)
 *   - 支持合并（mergeable），适用于分布式计算
 *
 * 【数据结构】
 *   多层 Level：level ℓ 的样本隐式权重为 w = 2^ℓ
 *   - Level 0：权重 1，直接存储新样本
 *   - Level ℓ：权重 2^ℓ，由 level ℓ-1 压缩晋升而来
 *
 * 【Compaction 操作】
 *   当某层超过容量时触发：
 *   1. 排序该层样本
 *   2. 随机选择偶数或奇数下标（概率各 1/2）
 *   3. 保留选中的一半，晋升到上一层
 *   数学性质：保持秩估计的无偏性 E[r̂] = r
 *
 * 【重建接口】
 *   三大族（CDF / PDF / Quantile），每族支持两种重建方法：
 *   A = PCHIP：单调三次 Hermite 插值，快速，保单调
 *   B = MaxEnt：最大熵密度重建，全局平滑，信息论最优
 *
 * 【参考文献】
 *   [1] Karnin, Lang, Liberty. "Optimal Quantile Approximation in Streams"
 *       FOCS 2016. https://arxiv.org/abs/1603.05346
 *   [2] Jaynes. "Information Theory and Statistical Mechanics" 1957.
 *   [3] Fritsch, Carlson. "Monotone Piecewise Cubic Interpolation" 1980.
 * ═══════════════════════════════════════════════════════════════════════════
 */

class KLLcache {
public:
    // ----------------
    // 重建方法枚举
    // ----------------

    enum class ReconstructionMethod {
        PCHIP,       // A: 单调三次 Hermite 插值（Monotone Cubic）
        MaxEntropy   // B: 最大熵密度重建（Maximum Entropy）
    };

public:
    // ----------------
    // 构造 / 析构 / 移动
    // ----------------

    /**
     * @brief 构造函数
     * @param k  KLL 精度参数（越大精度越高，内存/时间开销越大）
     *
     * 【数学理论】
     *   KLL Sketch (Karnin-Lang-Liberty, 2016) 是一种流式分位数估计数据结构。
     *   给定精度参数 k，sketch 保证对任意查询秩 r，返回估计秩 r̂ 满足：
     *
     *       |r̂ - r| ≤ ε·n,   其中 ε = O(1/k)
     *
     *   空间复杂度为 O(k·log log(n))，优于传统 GK sketch 的 O((1/ε)·log(εn))。
     *
     * 【公式定义】
     *   - 每层容量：capacity[ℓ] = ⌈k · (2/3)^ℓ⌉
     *   - 总层数上界：L = O(log log n)
     *   - 总存储项数：m = Σℓ capacity[ℓ] = O(k)
     *
     * 【值域】
     *   k ∈ [1, ∞)，推荐 k ≥ 200 以获得 ε ≤ 0.5%
     *
     * 【Motivation】
     *   传统精确分位数需要 O(n) 空间；KLL 用随机抽样 + 分层压缩，
     *   以极小空间换取可控误差，适用于无法存储全部数据的流式场景。
     */
    explicit KLLcache(size_t k = 1024);

    ~KLLcache() = default;

    // 支持移动语义
    KLLcache(KLLcache&&) noexcept = default;
    KLLcache& operator=(KLLcache&&) noexcept = default;

    /**
     * @brief 清空所有状态，回到初始（empty）状态。
     *
     * 【语义】重置 n_total=0，清空所有 level buffer，min/max 未定义。
     */
    void clear();

    /**
     * @brief 检查是否为空（未插入任何数据）
     * @return n_total == 0
     */
    [[nodiscard]] bool empty() const noexcept { return n_total_ == 0; }


    // ----------------
    // 插入 / 合并（流式）
    // ----------------

    /**
     * addValue
     *  - 输入：单点样本 x ∈ ℝ
     *
     * 【数学理论】
     *   KLL 采用分层随机压缩：
     *   1. 新样本插入 level 0（权重 w=2^0=1）
     *   2. 若 |buffer[ℓ]| > capacity[ℓ]，触发 compaction：
     *      - 对 buffer 排序：v₁ ≤ v₂ ≤ ... ≤ v_m
     *      - 随机选择奇数或偶数下标子集（概率各 1/2）
     *      - 保留的元素晋升到 level ℓ+1（隐式权重翻倍）
     *
     * 【公式定义】
     *   设 level ℓ 存储样本 S_ℓ，隐式权重 w_ℓ = 2^ℓ。
     *   Compaction 操作：S_ℓ → S'_{ℓ+1}，其中 |S'| = ⌊|S|/2⌋
     *
     * 【Insight】
     *   随机偶/奇选择保证了秩估计的无偏性：E[r̂] = r。
     *   方差随层数增加而累积，但总误差仍为 O(1/k)。
     */
    void addValue(double x);

    /**
     * addBatch
     *  - 输入：样本向量 X = {x₁, x₂, ..., x_m}
     *
     * 【数学理论】
     *   批量插入在数学上等价于顺序调用 addValue(x_i)。
     *   由于 KLL 的 mergeable 性质，插入顺序不影响最终的误差界。
     *
     * 【复杂度】
     *   - 时间：O(m·log k) 摊销
     *   - 触发 compaction 次数：O(m/k)
     *
     * 【Motivation】
     *   批量接口允许实现层面优化（如延迟排序、批量晋升），
     *   减少 compaction 的排序开销。
     */
    void addBatch(const std::vector<double>& X);

    /**
     * mergeWith
     *  - 输入：另一个 KLLcache（只读）
     *
     * 【数学理论】
     *   KLL sketch 具有 mergeable 性质：
     *   - 可交换：merge(A,B) ≈ merge(B,A)（误差界相同）
     *   - 可结合：merge(merge(A,B),C) ≈ merge(A,merge(B,C))
     *
     *   合并算法：
     *   1. 对每层 ℓ：this.buffer[ℓ] ← this.buffer[ℓ] ∪ other.buffer[ℓ]
     *   2. 从 level 0 向上依次执行 compaction 直到所有层满足容量约束
     *
     * 【误差界】
     *   设 this 来自 n₁ 个样本，other 来自 n₂ 个样本。
     *   合并后对任意秩 r 的估计 r̂ 满足：
     *       |r̂ - r| ≤ ε·(n₁ + n₂)
     *
     * 【Motivation】
     *   mergeable 性质支持分布式计算：各节点独立构建 sketch，
     *   最后合并得到全局分位数估计，通信开销仅 O(k)。
     */
    void mergeWith(const KLLcache& other);


    // ----------------
    // 单点快速查询（fast path）
    // ----------------

    /**
     * queryQuantile
     *  - 输入：φ ∈ [0,1]
     *  - 输出：近似分位点 Q̂(φ) ∈ [min, max]
     *  - 前提：!empty()
     *
     * 【数学定义】
     *   真实分位函数：Q(φ) = inf{ x : F(x) ≥ φ }
     *   其中 F(x) = P(X ≤ x) 为累积分布函数。
     *
     * 【算法】
     *   1. 合并所有层的加权样本：{(v_i, w_i)}，其中 w_i = 2^{level(i)}
     *   2. 按 v_i 排序，计算累积权重 W_j = Σ_{i≤j} w_i
     *   3. 找到最小的 j 使得 W_j / W_total ≥ φ
     *   4. 返回 v_j（或在相邻点间线性插值）
     *
     * 【误差界】
     *   |rank(Q̂(φ)) - φ·n| ≤ ε·n，即返回值的真实秩与目标秩相差不超过 ε·n
     *
     * 【边界行为】
     *   - φ = 0 → 返回 min
     *   - φ = 1 → 返回 max
     *
     * 【值域】
     *   输入：φ ∈ [0, 1]
     *   输出：Q̂ ∈ [min_v_, max_v_]
     */
    double queryQuantile(double phi) const;

    /**
     * queryCDF
     *  - 输入：标量 x ∈ ℝ
     *  - 输出：近似 CDF F̂(x) ∈ [0, 1]
     *  - 前提：!empty()
     *
     * 【数学定义】
     *   真实 CDF：F(x) = P(X ≤ x) = ∫_{-∞}^{x} f(t) dt
     *
     * 【算法】
     *   基于加权样本的经验 CDF：
     *
     *       F̂(x) = Σ_i w_i · 𝟙(v_i ≤ x) / Σ_i w_i
     *
     *   其中 𝟙(·) 为指示函数，w_i = 2^{level(i)}。
     *
     * 【误差界】
     *   |F̂(x) - F(x)| ≤ ε，对所有 x 同时成立（uniform error bound）
     *
     * 【值域】
     *   输入：x ∈ ℝ
     *   输出：F̂(x) ∈ [0, 1]
     *   - x < min → 0
     *   - x ≥ max → 1
     */
    double queryCDF(double x) const;

    /**
     * queryPDF
     *  - 输入：x ∈ ℝ，重建方法 method
     *  - 输出：密度估计 f̂(x) ≥ 0
     *  - 前提：!empty()
     *
     * 【数学定义】
     *   概率密度函数 f(x) 满足：F(x) = ∫_{-∞}^{x} f(t) dt
     *   归一化条件：∫_{-∞}^{+∞} f(t) dt = 1
     *
     * 【PCHIP 方法】
     *   1. 构造 CDF 的单调三次 Hermite 插值 F̂_pchip(x)
     *   2. 解析求导：f̂(x) = dF̂_pchip/dx
     *   性质：保证 f̂(x) ≥ 0（因 PCHIP 保单调性）
     *
     * 【MaxEntropy 方法】
     *   求解最大熵优化问题：
     *       max  -∫ f(x) log f(x) dx
     *       s.t. ∫ f(x) dx = 1
     *            F(v_i) = F̂_empirical(v_i)  （矩约束）
     *   解的形式：f*(x) = exp(Σ_j λ_j φ_j(x)) / Z
     *
     * 【Insight】
     *   - PCHIP：局部平滑，计算快，但在数据稀疏处可能欠平滑
     *   - MaxEnt：全局最平滑（熵最大 = 最少假设），但计算较慢
     *
     * 【值域】
     *   输入：x ∈ ℝ
     *   输出：f̂(x) ∈ [0, +∞)，且 ∫ f̂ = 1
     */
    double queryPDF(double x,
                        ReconstructionMethod method = ReconstructionMethod::PCHIP) const;


    // ----------------
    // 三大重建接口（CDF / PDF / Quantile）
    //    —— 每个都支持 method = A (PCHIP) 或 B (MaxEnt)
    // ----------------

    /**
     * reconstructCDF
     *  - 输入：num_points（输出格点数 ≥ 2），method
     *  - 输出：(x_grid, F_grid)，两个等长数组
     *  - 前提：!empty()，num_points ≥ 2
     *
     * 【数学定义】
     *   输出均匀格点上的 CDF 值：
     *       x_grid[i] = min + i·(max - min)/(num_points - 1)
     *       F_grid[i] = F̂(x_grid[i])
     *
     * 【PCHIP 方法 —— Piecewise Cubic Hermite Interpolating Polynomial】
     *   1. 从加权样本构造节点：{(v_j, F_j)} 其中 F_j = Σ_{i≤j} w_i / Σ w
     *   2. 计算每个节点的斜率 d_j，使用 Fritsch-Carlson 方法保证单调性
     *   3. 在每个区间 [v_j, v_{j+1}] 上构造三次多项式：
     *          F̂(x) = a_j + b_j·t + c_j·t² + d_j·t³,  t = (x - v_j)/(v_{j+1} - v_j)
     *   性质：
     *   - 保单调：F̂'(x) ≥ 0（无 overshoot/undershoot）
     *   - C¹ 连续：F̂ 及其一阶导数在节点处连续
     *
     * 【MaxEntropy 方法】
     *   1. 求解最大熵密度 f*(x) = exp(Σ λ_j φ_j(x)) / Z
     *   2. 数值积分得到 CDF：F*(x) = ∫_{min}^{x} f*(t) dt
     *   性质：
     *   - 全局光滑（C^∞）
     *   - 信息论最优：在给定矩约束下熵最大，即引入最少的先验假设
     *
     * 【Motivation】
     *   离散样本 → 连续 CDF 是可视化和下游计算的基础。
     *   两种方法权衡：PCHIP 快且保单调；MaxEnt 平滑但计算量大。
     */
    std::pair<std::vector<double>, std::vector<double>>
    reconstructCDF(size_t num_points,
                   ReconstructionMethod method = ReconstructionMethod::PCHIP) const;

    /**
     * reconstructPDF
     *  - 输入：num_points（≥ 2），method
     *  - 输出：(x_grid, f_grid)，两个等长数组
     *  - 前提：!empty()，num_points ≥ 2
     *
     * 【数学定义】
     *   概率密度函数：f(x) = dF(x)/dx
     *   归一化：∫ f(x) dx = 1，非负性：f(x) ≥ 0
     *
     * 【PCHIP 方法】
     *   对 CDF 的 PCHIP 插值解析求导：
     *       f̂(x) = dF̂_pchip/dx = b_j + 2c_j·t + 3d_j·t²
     *   其中 t = (x - v_j)/(v_{j+1} - v_j)，需除以区间长度。
     *   性质：分片二次多项式，C⁰ 连续，非负（因 CDF 单调递增）
     *
     * 【MaxEntropy 方法 —— 最大熵密度重建】
     *   优化问题：
     *       max  H[f] = -∫ f(x) log f(x) dx      (熵)
     *       s.t. ∫ f(x) dx = 1                   (归一化)
     *            ∫ f(x) 𝟙(x ≤ v_j) dx = F_j     (CDF 矩约束)
     *
     *   Lagrangian 对偶后，最优解形式为：
     *       f*(x) = exp(λ₀ + Σ_j λ_j 𝟙(x ≤ v_j)) / Z
     *
     *   实际实现中常用平滑基函数替代阶跃函数，或转化为矩约束问题。
     *
     * 【Insight】
     *   - 最大熵原则（Jaynes, 1957）：在给定信息下，选择熵最大的分布，
     *     因为它对未知信息做最少假设，是"最诚实"的估计。
     *   - PDF 比 CDF 更易暴露分布特征（峰、谷、多模态）。
     *
     * 【值域】
     *   x_grid ⊆ [min, max]
     *   f_grid ≥ 0，且 Σ f_grid[i]·Δx ≈ 1
     */
    std::pair<std::vector<double>, std::vector<double>>
    reconstructPDF(size_t num_points,
                   ReconstructionMethod method = ReconstructionMethod::PCHIP) const;

    /**
     * reconstructQuantile
     *  - 输入：num_points（≥ 2），method
     *  - 输出：(u_grid, Q_grid)，两个等长数组
     *  - 前提：!empty()，num_points ≥ 2
     *
     * 【数学定义】
     *   分位函数（Quantile Function）是 CDF 的逆：
     *       Q(u) = F⁻¹(u) = inf{ x : F(x) ≥ u }
     *
     *   输出：
     *       u_grid[i] = i / (num_points - 1) ∈ [0, 1]
     *       Q_grid[i] = Q̂(u_grid[i])
     *
     * 【PCHIP 方法】
     *   直接在 (u, Q) 空间构造 PCHIP：
     *   1. 节点：{(F_j, v_j)}，其中 F_j 是加权累积概率
     *   2. 对 u → Q(u) 做单调三次插值
     *   等价于对 CDF 的 PCHIP 求逆，但直接在 Q 空间插值更稳定。
     *
     * 【MaxEntropy 方法】
     *   1. 先求 MaxEnt 密度 f*(x)
     *   2. 积分得 CDF：F*(x) = ∫ f*(t) dt
     *   3. 数值求逆：Q*(u) = F*⁻¹(u)（二分搜索或 Newton 法）
     *
     * 【Insight】
     *   分位函数在以下场景有用：
     *   - 随机数生成：X = Q(U)，其中 U ~ Uniform(0,1)
     *   - 风险度量：VaR_α = Q(α)，CVaR = E[X | X ≥ VaR]
     *   - 分布比较：Q-Q plot
     *
     * 【值域】
     *   u_grid ⊆ [0, 1]
     *   Q_grid ⊆ [min, max]
     */
    std::pair<std::vector<double>, std::vector<double>>
    reconstructQuantile(size_t num_points,
                        ReconstructionMethod method = ReconstructionMethod::PCHIP) const;


    // ----------------
    // 导出 / 调试 / 状态
    // ----------------

    /**
     * exportWeightedSamples
     *  - 输出：(values, weights) 两个并行数组
     *
     * 【数学定义】
     *   返回 sketch 存储的全部加权样本：
     *       values[i]  = v_i（样本值）
     *       weights[i] = 2^{level(i)}（隐式权重）
     *
     *   这些加权样本定义了经验分布：
     *       F̂(x) = Σ_i w_i 𝟙(v_i ≤ x) / Σ_i w_i
     *
     * 【Motivation】
     *   导出加权样本用于：
     *   - 离线分析或自定义重建算法
     *   - 序列化/反序列化
     *   - 与其他统计工具集成
     */
    std::pair<std::vector<double>, std::vector<uint64_t>> exportWeightedSamples() const;

    /**
     * dumpLevels
     *  - 输出：每层的原始 buffer（调试用）
     *
     * 【结构】
     *   result[ℓ] = levels_[ℓ].buffer
     *   层 ℓ 的隐式权重为 2^ℓ
     *
     * 【用途】
     *   调试 compaction 行为，验证各层容量约束。
     */
    std::vector<std::vector<double>> dumpLevels() const;

    /**
     * storedSize
     *  - 输出：当前存储的原始项数 m
     *
     * 【公式】
     *   m = Σ_ℓ |levels_[ℓ].buffer|
     *
     * 【理论界】
     *   KLL 空间复杂度：m = O(k · log log n)
     *   其中 k 是精度参数，n 是已插入样本数。
     *   这优于 GK sketch 的 O((1/ε) · log(εn))。
     */
    [[nodiscard]] size_t storedSize() const noexcept;

    /**
     * totalCount
     *  - 输出：累计接收样本数 n
     *
     * 【说明】
     *   n 精确记录已调用 addValue 的次数（含 addBatch 和 mergeWith）。
     *   用于计算绝对秩误差：|r̂ - r| ≤ ε·n
     */
    [[nodiscard]] uint64_t totalCount() const noexcept;

    /**
     * range
     *  - 输出：(min, max) 数据范围
     *  - 前提：!empty()
     *
     * 【数学定义】
     *   min = min{ v_i : 所有加权样本 }
     *   max = max{ v_i : 所有加权样本 }
     *
     * 【性质】
     *   这是精确的最小/最大值（非近似），因为 KLL 总是保留极值。
     */
    [[nodiscard]] std::pair<double, double> range() const noexcept;

private:
    // ----------------
    // 内部数据结构
    // ----------------

    struct Level {
        std::vector<double> buffer;   // 该层存储的样本值（无显式权重）
        size_t capacity;              // 该层允许的最大元素数（触发 compaction）
    };

    // ----------------
    // 缓存结构（延迟计算，查询时构建）
    // ----------------

    struct Cache {
        bool valid = false;
        
        // Level 1: weighted samples (from exportWeightedSamples)
        std::vector<double> values;
        std::vector<uint64_t> weights;
        
        // Level 2: sorted CDF points
        std::vector<kll_detail::WeightedPoint> sorted_pts;
        
        // Level 3: CDF knots (xk, Fk) and PCHIP slopes
        std::vector<double> xk;
        std::vector<double> Fk;
        std::vector<double> dk_cdf;  // PCHIP slopes for CDF
        
        // Level 4: Quantile knots (Fk_q, Qk) and PCHIP slopes
        std::vector<double> Fk_quantile;
        std::vector<double> Qk;
        std::vector<double> dk_quantile;  // PCHIP slopes for quantile
        
        void invalidate() noexcept { valid = false; }
    };

    // ----------------
    // 内部成员（状态）
    // ----------------

    size_t k_;                         // 精度参数
    uint64_t n_total_;                 // 累计样本数
    std::vector<Level> levels_;        // 多层缓冲
    double min_v_, max_v_;             // 全局最小/最大
    std::mt19937_64 rng_;              // 随机数生成器（用于 compaction）
    mutable Cache cache_;              // 延迟计算缓存（mutable for const methods）

    // ----------------
    // 内部行为函数
    // ----------------

    // 确保 levels_[idx] 存在，并设置 capacity[idx] = ceil(k * (2/3)^idx)
    void ensureLevel(size_t idx);

    // 若 |buffer[idx]| > capacity[idx] 则调用 compactLevel
    void tryCompactLevel(size_t idx);

    // 执行 compaction：排序 → 随机选偶/奇 → 一半晋升到 idx+1
    void compactLevel(size_t idx);

    // ----------------
    // 缓存管理
    // ----------------

    // 使缓存失效（在 mutation 后调用）
    void invalidateCache() noexcept { cache_.invalidate(); }

    // 确保缓存有效（延迟构建）
    void ensureCacheValid() const;

    // 重建完整缓存
    void rebuildCache() const;

    // ----------------
    // 禁止复制
    // ----------------

    KLLcache(const KLLcache&) = delete;
    KLLcache& operator=(const KLLcache&) = delete;
};


// ============================================================================
// Implementation
// ============================================================================

// ----------------------------------------------------------------------------
// Constructor / clear
// ----------------------------------------------------------------------------

inline KLLcache::KLLcache(size_t k)
    : k_(k), n_total_(0), min_v_(0.0), max_v_(0.0),
      rng_(std::random_device{}()), cache_{} {
    assert(k_ >= 1);
    levels_.reserve(32);  // O(log log n) levels typical
    ensureLevel(0);
}

inline void KLLcache::clear() {
    n_total_ = 0;
    for (auto& lv : levels_) {
        lv.buffer.clear();
    }
    invalidateCache();
}

// ----------------------------------------------------------------------------
// Internal: Level management and Compaction
// ----------------------------------------------------------------------------

inline void KLLcache::ensureLevel(size_t idx) {
    assert(idx < 64);
    while (levels_.size() <= idx) [[unlikely]] {
        assert(levels_.size() < 64);
        Level lv;
        // capacity[ℓ] = ⌈k · (2/3)^ℓ⌉
        double factor = std::pow(2.0 / 3.0, static_cast<double>(levels_.size()));
        lv.capacity = static_cast<size_t>(std::ceil(static_cast<double>(k_) * factor));
        if (lv.capacity < 2) lv.capacity = 2;  // minimum capacity for valid compaction
        levels_.push_back(std::move(lv));
    }
}

inline void KLLcache::tryCompactLevel(size_t idx) {
    if (levels_[idx].buffer.size() > levels_[idx].capacity) [[unlikely]] {
        compactLevel(idx);
    }
}

/**
 * compactLevel — KLL compaction 操作（标准实现）
 *
 * 【标准算法】(Karnin-Lang-Liberty, 2016)
 *   1. 排序 buffer
 *   2. 只对成对元素 (偶数个) 进行 compaction
 *   3. 随机选择保留每对中的第一个或第二个 (概率各 1/2)
 *   4. 若原长度为奇数，保留最后一个元素 (leftover) 在本层
 *
 * 【权重守恒】
 *   - compact_len = L & ~1 (向下取偶)
 *   - 晋升 compact_len/2 个元素，每个权重 ×2
 *   - leftover (若存在) 保留在本层，权重不变
 *   - 因此：Σw_i 严格守恒 = n_total
 *
 * 【无偏性】
 *   E[r̂] = r，因为每对中选哪个是等概率的
 *
 * 【误差界】
 *   |r̂ - r| ≤ ε·n，其中 ε = O(1/k)
 */
inline void KLLcache::compactLevel(size_t idx) {
    auto& buf = levels_[idx].buffer;
    size_t L = buf.size();
    
    std::sort(buf.begin(), buf.end());  // O(L log L)
    ensureLevel(idx + 1);
    
    // Only compact pairs; keep leftover if L is odd
    size_t compact_len = L & ~size_t{1};  // Round down to even
    bool has_leftover = (L % 2 == 1);
    
    // Random choice: keep first (0) or second (1) of each pair
    size_t offset = static_cast<size_t>(rng_() & 1ULL);
    
    // Reserve space for promoted elements
    levels_[idx + 1].buffer.reserve(levels_[idx + 1].buffer.size() + compact_len / 2);
    
    // Promote one element from each pair to next level (weight doubles implicitly)
    for (size_t i = 0; i < compact_len; i += 2) {
        levels_[idx + 1].buffer.push_back(buf[i + offset]);
    }
    
    // Keep leftover in current level (if any)
    if (has_leftover) {
        double leftover = buf[L - 1];
        buf.clear();
        buf.push_back(leftover);
    } else {
        buf.clear();
    }
    
    // Cascade compaction to next level if needed
    tryCompactLevel(idx + 1);
}

// ----------------------------------------------------------------------------
// Insertion / Merge
// ----------------------------------------------------------------------------

inline void KLLcache::addValue(double x) {
    assert(n_total_ != std::numeric_limits<uint64_t>::max());
    
    invalidateCache();  // Data mutation
    
    if (n_total_ == 0) [[unlikely]] {
        min_v_ = max_v_ = x;
    } else {
        if (x < min_v_) [[unlikely]] min_v_ = x;
        if (x > max_v_) [[unlikely]] max_v_ = x;
    }
    
    n_total_++;
    levels_[0].buffer.push_back(x);
    tryCompactLevel(0);
}

inline void KLLcache::addBatch(const std::vector<double>& X) {
    if (X.empty()) [[unlikely]] return;
    
    invalidateCache();  // Data mutation
    
    // Pre-scan for min/max to reduce branch mispredictions in hot loop
    double batch_min = X[0];
    double batch_max = X[0];
    for (size_t i = 1; i < X.size(); ++i) {
        if (X[i] < batch_min) batch_min = X[i];
        if (X[i] > batch_max) batch_max = X[i];
    }
    
    if (n_total_ == 0) [[unlikely]] {
        min_v_ = batch_min;
        max_v_ = batch_max;
    } else {
        if (batch_min < min_v_) min_v_ = batch_min;
        if (batch_max > max_v_) max_v_ = batch_max;
    }
    
    // Reserve space for level 0 to avoid reallocations
    levels_[0].buffer.reserve(levels_[0].buffer.size() + X.size());
    
    assert(n_total_ <= (std::numeric_limits<uint64_t>::max() - X.size()));
    n_total_ += X.size();
    
    for (double x : X) {
        levels_[0].buffer.push_back(x);
        tryCompactLevel(0);
    }
}

inline void KLLcache::mergeWith(const KLLcache& other) {
    if (other.empty()) [[unlikely]] return;
    
    // Parameter consistency check: k must match for error guarantees to hold
    assert(k_ == other.k_);
    
    // Level structure consistency: verify capacity at overlapping levels
    for (size_t i = 0; i < std::min(levels_.size(), other.levels_.size()); i++) {
        assert(levels_[i].capacity == other.levels_[i].capacity);
    }
    
    invalidateCache();  // Data mutation
    
    if (empty()) [[unlikely]] {
        min_v_ = other.min_v_;
        max_v_ = other.max_v_;
    } else {
        if (other.min_v_ < min_v_) min_v_ = other.min_v_;
        if (other.max_v_ > max_v_) max_v_ = other.max_v_;
    }
    
    assert(n_total_ <= (std::numeric_limits<uint64_t>::max() - other.n_total_));
    n_total_ += other.n_total_;
    
    // Merge buffers from each level
    for (size_t i = 0; i < other.levels_.size(); i++) {
        ensureLevel(i);
        // Reserve space
        levels_[i].buffer.reserve(levels_[i].buffer.size() + other.levels_[i].buffer.size());
        for (double v : other.levels_[i].buffer) {
            levels_[i].buffer.push_back(v);
        }
    }
    
    // Compact from level 0 upward
    for (size_t i = 0; i < levels_.size(); i++) {
        tryCompactLevel(i);
    }
}

// ----------------------------------------------------------------------------
// Export / Debug / Status
// ----------------------------------------------------------------------------

inline std::pair<std::vector<double>, std::vector<uint64_t>>
KLLcache::exportWeightedSamples() const {
    // Use cache if available to avoid recomputation
    if (cache_.valid) [[likely]] {
        return {cache_.values, cache_.weights};
    }
    
    std::vector<double> values;
    std::vector<uint64_t> weights;
    
    size_t total_size = 0;
    for (const auto& lv : levels_) {
        total_size += lv.buffer.size();
    }
    values.reserve(total_size);
    weights.reserve(total_size);
    
    for (size_t i = 0; i < levels_.size(); i++) {
        assert(i < 64);
        uint64_t w = 1ULL << i;  // weight = 2^i
        for (double v : levels_[i].buffer) {
            values.push_back(v);
            weights.push_back(w);
        }
    }
    
    return {values, weights};
}

inline std::vector<std::vector<double>> KLLcache::dumpLevels() const {
    std::vector<std::vector<double>> result;
    result.reserve(levels_.size());
    for (const auto& lv : levels_) {
        result.push_back(lv.buffer);
    }
    return result;
}

inline size_t KLLcache::storedSize() const noexcept {
    size_t m = 0;
    for (const auto& lv : levels_) {
        m += lv.buffer.size();
    }
    return m;
}

inline uint64_t KLLcache::totalCount() const noexcept {
    return n_total_;
}

inline std::pair<double, double> KLLcache::range() const noexcept {
    assert(!empty());
    return {min_v_, max_v_};
}

// ----------------------------------------------------------------------------
// Helper: Build sorted weighted samples with cumulative CDF
// ----------------------------------------------------------------------------

namespace kll_detail {

/**
 * buildSortedCDF
 *
 * 【功能】将加权样本 {(v_i, w_i)} 转换为排序后的累积 CDF 点集
 *
 * 【算法】
 *   1. 间接排序：构造 idx[] 按 values[idx[i]] 升序
 *   2. 归一化权重：total_w = Σw_i, 各点权重 w_norm = w_i / total_w
 *   3. 累积求和：cum[i] = Σ_{j≤i} w_norm[j]
 *
 * 【输出格式】
 *   pts_out[i] = {value, cum_prob, weight}
 *   - value: 样本值（已排序）
 *   - cum_prob: 累积概率 F(value) = P(X ≤ value)
 *   - weight: 该点的归一化权重
 *
 * 【复杂度】O(n log n) 排序 + O(n) 累积
 *
 * 【注意】idx_buf 作为参数传入以复用内存，避免重复分配
 */
inline void buildSortedCDF(
    const std::vector<double>& values,
    const std::vector<uint64_t>& weights,
    std::vector<WeightedPoint>& pts_out,
    std::vector<size_t>& idx_buf)
{
    size_t n = values.size();
    assert(weights.size() == n);
    
    pts_out.clear();
    if (n == 0) [[unlikely]] return;
    
    pts_out.reserve(n);
    
    // Indirect sort: avoid moving heavy elements
    idx_buf.resize(n);
    std::iota(idx_buf.begin(), idx_buf.end(), size_t{0});
    std::sort(idx_buf.begin(), idx_buf.end(),
              [&](size_t a, size_t b) { return values[a] < values[b]; });
    
    // Compute total weight with overflow check
    uint64_t total_w = 0;
    for (uint64_t w : weights) {
        assert(total_w <= (std::numeric_limits<uint64_t>::max() - w));
        total_w += w;
    }
    assert(total_w > 0);
    double inv_total = 1.0 / static_cast<double>(total_w);
    
    // Build sorted weighted points with cumulative CDF
    double cum = 0.0;
    for (size_t i = 0; i < n; i++) {
        size_t j = idx_buf[i];
        double w_norm = static_cast<double>(weights[j]) * inv_total;
        cum += w_norm;
        pts_out.push_back({values[j], cum, w_norm});
    }
}

/**
 * computePCHIPSlopes — Fritsch-Carlson 单调三次 Hermite 斜率计算
 *
 * 【数学背景】
 *   PCHIP (Piecewise Cubic Hermite Interpolating Polynomial) 在每个区间
 *   [x_i, x_{i+1}] 上构造三次多项式，满足：
 *   - 插值条件：p(x_i) = y_i, p(x_{i+1}) = y_{i+1}
 *   - 斜率条件：p'(x_i) = d_i, p'(x_{i+1}) = d_{i+1}
 *   - 单调性：若 y 单调，则 p 也单调（无 overshoot/undershoot）
 *
 * 【Fritsch-Carlson 算法 (1980)】
 *   Step 1: 计算 secant 斜率 h_i = Δy_i / Δx_i
 *   Step 2: 内点斜率用加权调和平均：
 *           d_i = (w1 + w2) / (w1/h_{i-1} + w2/h_i)
 *           其中 w1 = 2Δx_i + Δx_{i-1}, w2 = Δx_i + 2Δx_{i-1}
 *   Step 3: 端点斜率用外推公式 + 符号检查
 *   Step 4: 强制 α²+β²≤9 以保证单调（α=d_i/h_i, β=d_{i+1}/h_i）
 *
 * 【Insight】
 *   - 调和平均自动处理符号变化：h_{i-1}·h_i ≤ 0 时 d_i = 0
 *   - α²+β²≤9 是充分条件；圆内任意点都保证该区间单调
 *   - 单遍扫描：因为缩放 d_{i+1} 只会让下一区间的 α 更小
 *
 * 【参考】Fritsch & Carlson, "Monotone Piecewise Cubic Interpolation",
 *        SIAM J. Numer. Anal. 17(2), 1980.
 */
inline void computePCHIPSlopes(
    const std::vector<double>& x,
    const std::vector<double>& y,
    std::vector<double>& d_out,
    std::vector<double>& h_buf,
    std::vector<double>& delta_buf)
{
    size_t n = x.size();
    assert(n >= 2);
    
    d_out.assign(n, 0.0);
    
    if (n == 2) [[unlikely]] {
        // Degenerate: linear interpolation
        double slope = (y[1] - y[0]) / (x[1] - x[0]);
        d_out[0] = d_out[1] = slope;
        return;
    }
    
    // Step 1: Secant slopes h_i = (y_{i+1} - y_i) / (x_{i+1} - x_i)
    h_buf.resize(n - 1);
    delta_buf.resize(n - 1);
    for (size_t i = 0; i < n - 1; i++) {
        delta_buf[i] = x[i + 1] - x[i];
        h_buf[i] = (delta_buf[i] > 0) ? (y[i + 1] - y[i]) / delta_buf[i] : 0.0;
    }
    
    // Step 2: Interior slopes — weighted harmonic mean (Fritsch-Carlson)
    // Key insight: harmonic mean naturally gives 0 when signs differ
    for (size_t i = 1; i < n - 1; i++) {
        if (h_buf[i - 1] * h_buf[i] <= 0) [[unlikely]] {
            // Sign change or zero → set slope to 0 for monotonicity
            d_out[i] = 0.0;
        } else {
            // Weighted harmonic mean preserves monotonicity
            double w1 = 2.0 * delta_buf[i] + delta_buf[i - 1];
            double w2 = delta_buf[i] + 2.0 * delta_buf[i - 1];
            d_out[i] = (w1 + w2) / (w1 / h_buf[i - 1] + w2 / h_buf[i]);
        }
    }
    
    // Step 3: Endpoint slopes — extrapolation with sign/magnitude checks
    {
        // Left endpoint d[0]
        double h0 = h_buf[0], h1 = h_buf[1];
        if (h0 == 0.0) {
            d_out[0] = 0.0;
        } else if (h1 == 0.0) {
            d_out[0] = h0;
        } else if (h0 * h1 <= 0.0) {
            d_out[0] = 0.0;  // Opposite signs → 0 for safety
        } else {
            double dx0 = delta_buf[0], dx1 = delta_buf[1];
            double denom = dx0 + dx1;
            double d0 = (denom == 0.0) ? 0.0 
                        : ((2.0 * dx0 + dx1) * h0 - dx0 * h1) / denom;
            if (d0 * h0 < 0.0) d0 = 0.0;              // Enforce same sign
            if (std::abs(d0) > 3.0 * std::abs(h0)) d0 = 3.0 * h0;  // Clamp magnitude
            d_out[0] = d0;
        }
        
        // Right endpoint d[n-1]
        double hn1 = h_buf[n - 2], hn2 = h_buf[n - 3];
        if (hn1 == 0.0) {
            d_out[n - 1] = 0.0;
        } else if (hn2 == 0.0) {
            d_out[n - 1] = hn1;
        } else if (hn1 * hn2 <= 0.0) {
            d_out[n - 1] = 0.0;
        } else {
            double dxn2 = delta_buf[n - 2], dxn3 = delta_buf[n - 3];
            double denom = dxn2 + dxn3;
            double dn = (denom == 0.0) ? 0.0
                        : ((2.0 * dxn2 + dxn3) * hn1 - dxn2 * hn2) / denom;
            if (dn * hn1 < 0.0) dn = 0.0;
            if (std::abs(dn) > 3.0 * std::abs(hn1)) dn = 3.0 * hn1;
            d_out[n - 1] = dn;
        }
    }
    
    // Step 4: Enforce α²+β²≤9 — sufficient condition for monotonicity
    // Single-pass: scaling d_{i+1} down only makes next interval's α smaller
    for (size_t i = 0; i < n - 1; i++) {
        if (h_buf[i] == 0.0) [[unlikely]] {
            d_out[i] = 0.0;
            d_out[i + 1] = 0.0;
        } else {
            double alpha = d_out[i] / h_buf[i];
            double beta = d_out[i + 1] / h_buf[i];
            double r2 = alpha * alpha + beta * beta;
            if (r2 > 9.0) [[unlikely]] {
                // Scale (α,β) to lie on the circle α²+β²=9
                double tau = 3.0 / std::sqrt(r2);
                d_out[i] = tau * alpha * h_buf[i];
                d_out[i + 1] = tau * beta * h_buf[i];
            }
        }
    }
}

/**
 * evalPCHIP — 单点 Hermite 插值求值
 *
 * 【Hermite 基函数】设 t = (x - x_i) / h, h = x_{i+1} - x_i, t ∈ [0,1]
 *   H00(t) = 2t³ - 3t² + 1      （左值插值）
 *   H10(t) = t³ - 2t² + t       （左斜率插值，需乘 h）
 *   H01(t) = -2t³ + 3t²         （右值插值）
 *   H11(t) = t³ - t²            （右斜率插值，需乘 h）
 *
 * 【插值公式】
 *   p(x) = y_i·H00 + h·d_i·H10 + y_{i+1}·H01 + h·d_{i+1}·H11
 *   p'(x) = (y_i·H00' + h·d_i·H10' + y_{i+1}·H01' + h·d_{i+1}·H11') / h
 *
 * @return {F(x), F'(x)} — 函数值和导数
 */
inline std::pair<double, double> evalPCHIP(
    double x,
    const std::vector<double>& xk,
    const std::vector<double>& yk,
    const std::vector<double>& dk) noexcept
{
    size_t n = xk.size();
    const double* __restrict xk_ptr = xk.data();
    const double* __restrict yk_ptr = yk.data();
    const double* __restrict dk_ptr = dk.data();
    
    // Boundary: clamp to endpoint values
    if (x <= xk_ptr[0]) [[unlikely]] return {yk_ptr[0], dk_ptr[0]};
    if (x >= xk_ptr[n - 1]) [[unlikely]] return {yk_ptr[n - 1], dk_ptr[n - 1]};
    
    // Binary search for interval [xk[i], xk[i+1]] containing x
    size_t lo = 0, hi = n - 2;
    while (lo < hi) {
        size_t mid = (lo + hi + 1) / 2;
        if (xk_ptr[mid] <= x) lo = mid;
        else hi = mid - 1;
    }
    size_t i = lo;
    
    double h = xk_ptr[i + 1] - xk_ptr[i];
    if (h < 1e-15) [[unlikely]] return {yk_ptr[i], 0.0};  // Degenerate interval
    
    double t = (x - xk_ptr[i]) / h;
    double t2 = t * t;
    double t3 = t2 * t;
    
    // Hermite basis functions
    double H00 = 2.0 * t3 - 3.0 * t2 + 1.0;
    double H10 = t3 - 2.0 * t2 + t;
    double H01 = -2.0 * t3 + 3.0 * t2;
    double H11 = t3 - t2;
    
    double yi = yk_ptr[i], yi1 = yk_ptr[i + 1];
    double di = dk_ptr[i], di1 = dk_ptr[i + 1];
    double hdi = h * di, hdi1 = h * di1;
    
    // p(x) = y_i·H00 + h·d_i·H10 + y_{i+1}·H01 + h·d_{i+1}·H11
    double F = yi * H00 + hdi * H10 + yi1 * H01 + hdi1 * H11;
    
    // Hermite basis derivatives (d/dt)
    double dH00 = 6.0 * t2 - 6.0 * t;
    double dH10 = 3.0 * t2 - 4.0 * t + 1.0;
    double dH01 = -6.0 * t2 + 6.0 * t;
    double dH11 = 3.0 * t2 - 2.0 * t;
    
    // p'(x) = (1/h) · dp/dt
    double dF = (yi * dH00 + hdi * dH10 + yi1 * dH01 + hdi1 * dH11) / h;
    
    return {F, dF};
}

/**
 * evalPCHIPBatch — 批量 PCHIP 求值（面向顺序访问优化）
 *
 * 【优化策略】
 *   对于均匀格点 x_grid (reconstructCDF/PDF 的典型用法)：
 *   - 记录 current_interval，下次先检查是否仍在该区间
 *   - 若顺序递增，大部分情况 O(1) 命中，少数 O(log n) 二分
 *   - 摊销复杂度：O(m + log n) 而非 O(m log n)
 *
 * @param dF_out  可选，若非空则同时输出导数 F'(x)
 */
inline void evalPCHIPBatch(
    const std::vector<double>& x_grid,
    const std::vector<double>& xk,
    const std::vector<double>& yk,
    const std::vector<double>& dk,
    std::vector<double>& F_out,
    std::vector<double>* dF_out = nullptr) noexcept
{
    size_t m = x_grid.size();
    size_t n = xk.size();
    
    F_out.resize(m);
    if (dF_out) dF_out->resize(m);
    
    if (n < 2 || m == 0) [[unlikely]] return;
    
    const double* __restrict xk_ptr = xk.data();
    const double* __restrict yk_ptr = yk.data();
    const double* __restrict dk_ptr = dk.data();
    
    // Sequential access optimization: track current interval
    size_t current_interval = 0;
    
    for (size_t j = 0; j < m; ++j) {
        double x = x_grid[j];
        
        // Handle boundaries
        if (x <= xk_ptr[0]) [[unlikely]] {
            F_out[j] = yk_ptr[0];
            if (dF_out) (*dF_out)[j] = dk_ptr[0];
            continue;
        }
        if (x >= xk_ptr[n - 1]) [[unlikely]] {
            F_out[j] = yk_ptr[n - 1];
            if (dF_out) (*dF_out)[j] = dk_ptr[n - 1];
            continue;
        }
        
        // Try current interval first (sequential access pattern)
        size_t i = current_interval;
        if (x < xk_ptr[i] || x >= xk_ptr[i + 1]) [[unlikely]] {
            // Binary search
            size_t lo = 0, hi = n - 2;
            while (lo < hi) {
                size_t mid = (lo + hi + 1) / 2;
                if (xk_ptr[mid] <= x) lo = mid;
                else hi = mid - 1;
            }
            i = lo;
            current_interval = i;
        }
        
        double h = xk_ptr[i + 1] - xk_ptr[i];
        if (h < 1e-15) [[unlikely]] {
            F_out[j] = yk_ptr[i];
            if (dF_out) (*dF_out)[j] = 0.0;
            continue;
        }
        
        double t = (x - xk_ptr[i]) / h;
        double t2 = t * t;
        double t3 = t2 * t;
        
        double H00 = 2.0 * t3 - 3.0 * t2 + 1.0;
        double H10 = t3 - 2.0 * t2 + t;
        double H01 = -2.0 * t3 + 3.0 * t2;
        double H11 = t3 - t2;
        
        double yi = yk_ptr[i], yi1 = yk_ptr[i + 1];
        double di = dk_ptr[i], di1 = dk_ptr[i + 1];
        double hdi = h * di, hdi1 = h * di1;
        
        F_out[j] = yi * H00 + hdi * H10 + yi1 * H01 + hdi1 * H11;
        
        if (dF_out) {
            double dH00 = 6.0 * t2 - 6.0 * t;
            double dH10 = 3.0 * t2 - 4.0 * t + 1.0;
            double dH01 = -6.0 * t2 + 6.0 * t;
            double dH11 = 3.0 * t2 - 2.0 * t;
            (*dF_out)[j] = (yi * dH00 + hdi * dH10 + yi1 * dH01 + hdi1 * dH11) / h;
        }
    }
}

/**
 * maxEntPDF_Simplified — 简化版最大熵密度重建
 *
 * 【数学背景】
 *   给定 CDF 约束 F(x_j) = F_j，最大熵原则 (Jaynes, 1957) 要求：
 *     max H[f] = -∫ f(x) log f(x) dx
 *     s.t. ∫_{-∞}^{x_j} f(t)dt = F_j
 *
 *   当约束仅为"区间端点 CDF 值"时，解析解是分片常数：
 *     f(x) = (F_{j+1} - F_j) / (x_{j+1} - x_j),  x ∈ (x_j, x_{j+1})
 *
 * 【Insight】
 *   - 这是"对未知信息做最少假设"的体现：区间内无额外约束 → 均匀分布
 *   - 不是完整的指数族 MaxEnt (见 Uncertainty List)
 *   - 优点：O(n) 计算，无需迭代
 *   - 缺点：密度在 knot 处不连续
 */
inline std::vector<double> maxEntPDF_Simplified(
    const std::vector<double>& x_grid,
    const std::vector<double>& xk,
    const std::vector<double>& Fk)
{
    size_t n_grid = x_grid.size();
    size_t n_knots = xk.size();
    std::vector<double> f(n_grid, 0.0);
    
    if (n_knots < 2) [[unlikely]] return f;
    
    const double* __restrict xk_ptr = xk.data();
    const double* __restrict Fk_ptr = Fk.data();
    
    // Sequential access optimization
    size_t current_interval = 0;
    
    for (size_t i = 0; i < n_grid; i++) {
        double x = x_grid[i];
        
        if (x < xk_ptr[0] || x > xk_ptr[n_knots - 1]) [[unlikely]] {
            f[i] = 0.0;
            continue;
        }
        
        // Try current interval first (O(1) hit for sequential access)
        size_t lo = current_interval;
        if (x < xk_ptr[lo] || x >= xk_ptr[lo + 1]) [[unlikely]] {
            lo = 0;
            size_t hi = n_knots - 2;
            while (lo < hi) {
                size_t mid = (lo + hi + 1) / 2;
                if (xk_ptr[mid] <= x) lo = mid;
                else hi = mid - 1;
            }
            current_interval = lo;
        }
        
        // Piecewise constant: f = ΔF / Δx
        double dx = xk_ptr[lo + 1] - xk_ptr[lo];
        double dF = Fk_ptr[lo + 1] - Fk_ptr[lo];
        f[i] = (dx > 0) ? dF / dx : 0.0;
    }
    
    return f;
}

/**
 * maxEntCDF_Simplified — 简化版 MaxEnt CDF（分片线性）
 *
 * 【说明】对应 maxEntPDF_Simplified 的积分，即 knot 间线性插值
 */
inline double maxEntCDF_Simplified(
    double x,
    const std::vector<double>& xk,
    const std::vector<double>& Fk) noexcept
{
    size_t n = xk.size();

    if (n == 0) [[unlikely]] return 0.0;
    if (n == 1) [[unlikely]] return (x < xk[0]) ? 0.0 : 1.0;
    
    if (x <= xk[0]) [[unlikely]] return 0.0;
    if (x >= xk[n - 1]) [[unlikely]] return 1.0;
    
    const double* __restrict xk_ptr = xk.data();
    const double* __restrict Fk_ptr = Fk.data();
    
    // Binary search for interval
    size_t lo = 0, hi = n - 2;
    while (lo < hi) {
        size_t mid = (lo + hi + 1) / 2;
        if (xk_ptr[mid] <= x) lo = mid;
        else hi = mid - 1;
    }
    
    // Linear interpolation: F(x) = F_lo + t·(F_{lo+1} - F_lo)
    double dx = xk_ptr[lo + 1] - xk_ptr[lo];
    double t = (dx > 0) ? (x - xk_ptr[lo]) / dx : 0.0;
    return Fk_ptr[lo] + t * (Fk_ptr[lo + 1] - Fk_ptr[lo]);
}

} // namespace kll_detail

// ----------------------------------------------------------------------------
// Cache Management
// ----------------------------------------------------------------------------

/**
 * 缓存层级结构（延迟构建，mutation 后失效）：
 *
 *   Level 1: (values, weights)      — 原始加权样本
 *   Level 2: sorted_pts             — 排序后的 CDF 点集
 *   Level 3: (xk, Fk, dk_cdf)       — CDF knots + PCHIP 斜率
 *   Level 4: (Fk_q, Qk, dk_quantile) — Quantile knots + PCHIP 斜率
 *
 * 【Motivation】
 *   - 多次查询时避免重复计算 O(m log m) 的排序和 O(m) 的 PCHIP 斜率
 *   - 缓存失效仅在 addValue/addBatch/mergeWith/clear 时触发
 */

inline void KLLcache::ensureCacheValid() const {
    if (cache_.valid) [[likely]] return;
    rebuildCache();
}

inline void KLLcache::rebuildCache() const {
    // Level 1: weighted samples {(v_i, 2^level(i))}
    cache_.values.clear();
    cache_.weights.clear();
    
    size_t total_size = 0;
    for (const auto& lv : levels_) {
        total_size += lv.buffer.size();
    }
    cache_.values.reserve(total_size);
    cache_.weights.reserve(total_size);
    
    for (size_t i = 0; i < levels_.size(); i++) {
        assert(i < 64);
        uint64_t w = 1ULL << i;
        for (double v : levels_[i].buffer) {
            cache_.values.push_back(v);
            cache_.weights.push_back(w);
        }
    }
    
    // Level 2: Build sorted CDF
    std::vector<size_t> idx_buf;
    kll_detail::buildSortedCDF(cache_.values, cache_.weights, cache_.sorted_pts, idx_buf);
    
    // Level 3: Build CDF knots
    cache_.xk.clear();
    cache_.Fk.clear();
    cache_.xk.reserve(cache_.sorted_pts.size() + 2);
    cache_.Fk.reserve(cache_.sorted_pts.size() + 2);
    
    cache_.xk.push_back(min_v_);
    cache_.Fk.push_back(0.0);
    
    for (const auto& p : cache_.sorted_pts) {
        if (!cache_.xk.empty() && p.value == cache_.xk.back()) {
            if (cache_.xk.size() > 1) {
                cache_.Fk.back() = p.cum_prob;
            }
        } else {
            cache_.xk.push_back(p.value);
            cache_.Fk.push_back(p.cum_prob);
        }
    }
    
    if (cache_.xk.back() < max_v_) {
        cache_.xk.push_back(max_v_);
        cache_.Fk.push_back(1.0);
    } else {
        cache_.Fk.back() = 1.0;
    }
    
    // Compute PCHIP slopes for CDF
    if (cache_.xk.size() >= 2) {
        std::vector<double> h_buf, delta_buf;
        kll_detail::computePCHIPSlopes(cache_.xk, cache_.Fk, cache_.dk_cdf, h_buf, delta_buf);
    }
    
    // Level 4: Build Quantile knots
    cache_.Fk_quantile.clear();
    cache_.Qk.clear();
    cache_.Fk_quantile.reserve(cache_.sorted_pts.size() + 2);
    cache_.Qk.reserve(cache_.sorted_pts.size() + 2);
    
    cache_.Fk_quantile.push_back(0.0);
    cache_.Qk.push_back(min_v_);
    
    for (const auto& p : cache_.sorted_pts) {
        if (!cache_.Fk_quantile.empty() && p.cum_prob == cache_.Fk_quantile.back()) {
            cache_.Qk.back() = p.value;
        } else {
            cache_.Fk_quantile.push_back(p.cum_prob);
            cache_.Qk.push_back(p.value);
        }
    }
    
    if (cache_.Fk_quantile.back() < 1.0) {
        cache_.Fk_quantile.push_back(1.0);
        cache_.Qk.push_back(max_v_);
    } else {
        cache_.Qk.back() = max_v_;
    }
    
    // Compute PCHIP slopes for Quantile
    if (cache_.Fk_quantile.size() >= 2) {
        std::vector<double> h_buf, delta_buf;
        kll_detail::computePCHIPSlopes(cache_.Fk_quantile, cache_.Qk, cache_.dk_quantile, h_buf, delta_buf);
    }
    
    cache_.valid = true;
}

// ----------------------------------------------------------------------------
// Single-point Queries
// ----------------------------------------------------------------------------

inline double KLLcache::queryQuantile(double phi) const {
    assert(!empty());
    
    if (phi <= 0.0) [[unlikely]] return min_v_;
    if (phi >= 1.0) [[unlikely]] return max_v_;
    
    ensureCacheValid();
    const auto& pts = cache_.sorted_pts;
    
    // Binary search for smallest j s.t. pts[j].cum_prob >= phi
    size_t lo = 0, hi = pts.size() - 1;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (pts[mid].cum_prob < phi) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    
    // Linear interpolation between adjacent points
    if (lo == 0) [[unlikely]] {
        double t = phi / pts[0].cum_prob;
        return min_v_ + t * (pts[0].value - min_v_);
    }
    
    double F_lo = pts[lo - 1].cum_prob;
    double F_hi = pts[lo].cum_prob;
    double v_lo = pts[lo - 1].value;
    double v_hi = pts[lo].value;
    
    if (F_hi == F_lo) [[unlikely]] return v_hi;
    
    double t = (phi - F_lo) / (F_hi - F_lo);
    return v_lo + t * (v_hi - v_lo);
}

/**
 * queryCDF — 单点 CDF 查询
 *
 * 【算法】
 *   经验 CDF：F̂(x) = Σ w_i · 𝟙(v_i ≤ x) / Σ w_i
 *   其中 w_i = 2^{level(i)} 是隐式权重
 *
 * 【权重守恒】
 *   标准 KLL compaction 保证 Σw_i = n_total（精确守恒）
 *   因此归一化因子等于总样本数
 */
inline double KLLcache::queryCDF(double x) const {
    assert(!empty());
    
    if (x < min_v_) [[unlikely]] return 0.0;
    if (x >= max_v_) [[unlikely]] return 1.0;
    
    ensureCacheValid();
    const auto& values = cache_.values;
    const auto& weights = cache_.weights;
    
    // Weighted empirical CDF: F̂(x) = Σ w_i·𝟙(v_i ≤ x) / Σ w_i
    uint64_t total_w = 0;
    uint64_t cum_w = 0;
    for (size_t i = 0; i < values.size(); i++) {
        assert(total_w <= (std::numeric_limits<uint64_t>::max() - weights[i]));  // Overflow check
        total_w += weights[i];
        if (values[i] <= x) {
            cum_w += weights[i];
        }
    }
    
    return static_cast<double>(cum_w) / static_cast<double>(total_w);
}

inline double KLLcache::queryPDF(double x, ReconstructionMethod method) const {
    assert(!empty());
    
    if (x < min_v_ || x > max_v_) [[unlikely]] return 0.0;
    if (min_v_ == max_v_) [[unlikely]] return 0.0;
    
    ensureCacheValid();
    
    if (cache_.sorted_pts.size() < 2) [[unlikely]] return 0.0;
    
    const auto& xk = cache_.xk;
    const auto& Fk = cache_.Fk;
    
    if (method == ReconstructionMethod::PCHIP) {
        assert(xk.size() >= 2);
        auto [F, dF] = kll_detail::evalPCHIP(x, xk, Fk, cache_.dk_cdf);
        return std::max(0.0, dF);
    } else {
        // MaxEntropy: piecewise constant
        size_t n = xk.size();
        if (x < xk[0] || x > xk[n - 1]) [[unlikely]] return 0.0;
        
        // Binary search
        size_t lo = 0, hi = n - 2;
        while (lo < hi) {
            size_t mid = (lo + hi + 1) / 2;
            if (xk[mid] <= x) lo = mid;
            else hi = mid - 1;
        }
        
        double dx = xk[lo + 1] - xk[lo];
        double dF = Fk[lo + 1] - Fk[lo];
        return (dx > 0) ? dF / dx : 0.0;
    }
}

// ----------------------------------------------------------------------------
// Reconstruction: CDF
// ----------------------------------------------------------------------------

inline std::pair<std::vector<double>, std::vector<double>>
KLLcache::reconstructCDF(size_t num_points, ReconstructionMethod method) const {
    assert(!empty());
    assert(num_points >= 2);
    
    std::vector<double> x_grid(num_points);
    std::vector<double> F_grid(num_points);
    
    double x_min = min_v_;
    double x_max = max_v_;
    if (x_min == x_max) [[unlikely]] {
        for (size_t i = 0; i < num_points; i++) {
            x_grid[i] = x_min;
            F_grid[i] = 1.0;
        }
        return {x_grid, F_grid};
    }
    
    double dx = (x_max - x_min) / static_cast<double>(num_points - 1);
    for (size_t i = 0; i < num_points; i++) {
        x_grid[i] = x_min + static_cast<double>(i) * dx;
    }
    
    ensureCacheValid();
    const auto& xk = cache_.xk;
    const auto& Fk = cache_.Fk;
    
    if (method == ReconstructionMethod::PCHIP) {
        assert(xk.size() >= 2);
        // Use batch evaluation for better cache locality
        kll_detail::evalPCHIPBatch(x_grid, xk, Fk, cache_.dk_cdf, F_grid, nullptr);
        // Clamp to [0, 1]
        for (size_t i = 0; i < num_points; i++) {
            F_grid[i] = std::clamp(F_grid[i], 0.0, 1.0);
        }
    } else {
        // MaxEntropy (Simplified): piecewise linear CDF
        for (size_t i = 0; i < num_points; i++) {
            F_grid[i] = kll_detail::maxEntCDF_Simplified(x_grid[i], xk, Fk);
        }
    }
    
    return {x_grid, F_grid};
}

// ----------------------------------------------------------------------------
// Reconstruction: PDF
// ----------------------------------------------------------------------------

inline std::pair<std::vector<double>, std::vector<double>>
KLLcache::reconstructPDF(size_t num_points, ReconstructionMethod method) const {
    assert(!empty());
    assert(num_points >= 2);
    
    std::vector<double> x_grid(num_points);
    std::vector<double> f_grid(num_points);
    
    double x_min = min_v_;
    double x_max = max_v_;
    if (x_min == x_max) [[unlikely]] {
        for (size_t i = 0; i < num_points; i++) {
            x_grid[i] = x_min;
            f_grid[i] = 0.0;
        }
        return {x_grid, f_grid};
    }
    
    double dx = (x_max - x_min) / static_cast<double>(num_points - 1);
    for (size_t i = 0; i < num_points; i++) {
        x_grid[i] = x_min + static_cast<double>(i) * dx;
    }
    
    ensureCacheValid();
    const auto& xk = cache_.xk;
    const auto& Fk = cache_.Fk;
    
    if (method == ReconstructionMethod::PCHIP) {
        assert(xk.size() >= 2);
        // Use batch evaluation with derivative computation
        std::vector<double> F_dummy;
        kll_detail::evalPCHIPBatch(x_grid, xk, Fk, cache_.dk_cdf, F_dummy, &f_grid);
        // Ensure non-negative
        for (size_t i = 0; i < num_points; i++) {
            f_grid[i] = std::max(0.0, f_grid[i]);
        }
    } else {
        // MaxEntropy (Simplified): piecewise constant PDF
        f_grid = kll_detail::maxEntPDF_Simplified(x_grid, xk, Fk);
    }
    
    return {x_grid, f_grid};
}

// ----------------------------------------------------------------------------
// Reconstruction: Quantile
// ----------------------------------------------------------------------------

inline std::pair<std::vector<double>, std::vector<double>>
KLLcache::reconstructQuantile(size_t num_points, ReconstructionMethod method) const {
    assert(!empty());
    assert(num_points >= 2);
    
    std::vector<double> u_grid(num_points);
    std::vector<double> Q_grid(num_points);
    
    double du = 1.0 / static_cast<double>(num_points - 1);
    for (size_t i = 0; i < num_points; i++) {
        u_grid[i] = static_cast<double>(i) * du;
    }
    
    ensureCacheValid();
    const auto& Fk = cache_.Fk_quantile;
    const auto& Qk = cache_.Qk;
    
    if (method == ReconstructionMethod::PCHIP) {
        // Use batch evaluation for quantile function
        kll_detail::evalPCHIPBatch(u_grid, Fk, Qk, cache_.dk_quantile, Q_grid, nullptr);
        // Clamp to [min, max]
        for (size_t i = 0; i < num_points; i++) {
            Q_grid[i] = std::clamp(Q_grid[i], min_v_, max_v_);
        }
    } else {
        // MaxEntropy: piecewise linear quantile
        const double* __restrict Fk_ptr = Fk.data();
        const double* __restrict Qk_ptr = Qk.data();
        size_t n_knots = Fk.size();
        
        for (size_t i = 0; i < num_points; i++) {
            double u = u_grid[i];
            
            if (u <= 0.0) [[unlikely]] {
                Q_grid[i] = min_v_;
                continue;
            }
            if (u >= 1.0) [[unlikely]] {
                Q_grid[i] = max_v_;
                continue;
            }
            
            // Binary search for interval [Fk[j], Fk[j+1]] containing u
            size_t lo = 0, hi = n_knots - 2;
            while (lo < hi) {
                size_t mid = (lo + hi + 1) / 2;
                if (Fk_ptr[mid] <= u) lo = mid;
                else hi = mid - 1;
            }
            
            // Linear interpolation
            double dF = Fk_ptr[lo + 1] - Fk_ptr[lo];
            double t = (dF > 0) ? (u - Fk_ptr[lo]) / dF : 0.0;
            Q_grid[i] = Qk_ptr[lo] + t * (Qk_ptr[lo + 1] - Qk_ptr[lo]);
        }
    }
    
    return {u_grid, Q_grid};
}
