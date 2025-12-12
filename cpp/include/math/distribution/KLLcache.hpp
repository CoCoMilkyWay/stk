#pragma once
#include <vector>
#include <cstdint>
#include <utility>
#include <random>

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
    bool empty() const { return n_total_ == 0; }


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
    size_t storedSize() const;

    /**
     * totalCount
     *  - 输出：累计接收样本数 n
     *
     * 【说明】
     *   n 精确记录已调用 addValue 的次数（含 addBatch 和 mergeWith）。
     *   用于计算绝对秩误差：|r̂ - r| ≤ ε·n
     */
    uint64_t totalCount() const;

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
    std::pair<double, double> range() const;

private:
    // ----------------
    // 内部数据结构
    // ----------------

    struct Level {
        std::vector<double> buffer;   // 该层存储的样本值（无显式权重）
        size_t capacity;              // 该层允许的最大元素数（触发 compaction）
    };

    // ----------------
    // 内部成员（状态）
    // ----------------

    size_t k_;                         // 精度参数
    uint64_t n_total_;                 // 累计样本数
    std::vector<Level> levels_;        // 多层缓冲
    double min_v_, max_v_;             // 全局最小/最大
    std::mt19937_64 rng_;              // 随机数生成器（用于 compaction）

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
    // 禁止复制
    // ----------------

    KLLcache(const KLLcache&) = delete;
    KLLcache& operator=(const KLLcache&) = delete;
};
