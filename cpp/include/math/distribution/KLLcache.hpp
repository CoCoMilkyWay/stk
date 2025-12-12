#pragma once
#include <vector>
#include <cstdint>
#include <utility>
#include <random>

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
 * 2. [PCHIP Endpoint Slopes] 端点斜率计算可能不够精确
 *    - 当前：简单使用单侧差分 d[0] = h[0], d[n-1] = h[n-2]
 *    - Fritsch-Carlson 论文原版：有更复杂的端点处理
 *    - 影响：端点附近的 PDF 可能不够平滑
 *
 * 3. [PCHIP α²+β²≤9 Constraint] 约束执行顺序可能影响结果
 *    - 当前：从左到右遍历，逐个区间修正
 *    - 问题：修正 d[i+1] 后会影响下一个区间的 α 值
 *    - 需要验证：是否应该迭代直到收敛？
 *
 * ═══════════════════════════════════════════════════════════════════════════
 */

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


// ============================================================================
// Implementation
// ============================================================================

#include <algorithm>
#include <numeric>
#include <cmath>
#include <cassert>
#include <limits>

// ----------------------------------------------------------------------------
// Constructor / clear
// ----------------------------------------------------------------------------

inline KLLcache::KLLcache(size_t k)
    : k_(k), n_total_(0), min_v_(0.0), max_v_(0.0),
      rng_(std::random_device{}()) {
    assert(k_ >= 1);
    levels_.reserve(32);  // O(log log n) levels typical
    ensureLevel(0);
}

inline void KLLcache::clear() {
    n_total_ = 0;
    for (auto& lv : levels_) {
        lv.buffer.clear();
    }
}

// ----------------------------------------------------------------------------
// Internal: Level management and Compaction
// ----------------------------------------------------------------------------

inline void KLLcache::ensureLevel(size_t idx) {
    assert(idx < 64);
    while (levels_.size() <= idx) {
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
    if (levels_[idx].buffer.size() > levels_[idx].capacity) {
        compactLevel(idx);
    }
}

inline void KLLcache::compactLevel(size_t idx) {
    auto& buf = levels_[idx].buffer;
    
    // Sort the buffer using introsort (std::sort) - O(n log n)
    std::sort(buf.begin(), buf.end());
    
    // Ensure next level exists
    ensureLevel(idx + 1);
    
    // Randomly choose odd (1) or even (0) indices (single RNG draw).
    // Note: for odd buf.size(), offset=0 and offset=1 will promote different counts (ceil/floor).
    // This is intended; do not introduce leftovers, pairing, or extra randomness here.
    size_t offset = static_cast<size_t>(rng_() & 1ULL);
    
    // Promote selected half to next level
    for (size_t i = offset; i < buf.size(); i += 2) {
        levels_[idx + 1].buffer.push_back(buf[i]);
    }
    
    // Clear this level (discard the non-promoted items)
    buf.clear();
    
    // Recursively compact if needed
    tryCompactLevel(idx + 1);
}

// ----------------------------------------------------------------------------
// Insertion / Merge
// ----------------------------------------------------------------------------

inline void KLLcache::addValue(double x) {
    assert(n_total_ != std::numeric_limits<uint64_t>::max());
    if (n_total_ == 0) {
        min_v_ = max_v_ = x;
    } else {
        if (x < min_v_) min_v_ = x;
        if (x > max_v_) max_v_ = x;
    }
    
    n_total_++;
    levels_[0].buffer.push_back(x);
    tryCompactLevel(0);
}

inline void KLLcache::addBatch(const std::vector<double>& X) {
    for (double x : X) {
        addValue(x);
    }
}

inline void KLLcache::mergeWith(const KLLcache& other) {
    if (other.empty()) return;
    
    if (empty()) {
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

inline size_t KLLcache::storedSize() const {
    size_t m = 0;
    for (const auto& lv : levels_) {
        m += lv.buffer.size();
    }
    return m;
}

inline uint64_t KLLcache::totalCount() const {
    return n_total_;
}

inline std::pair<double, double> KLLcache::range() const {
    assert(!empty());
    return {min_v_, max_v_};
}

// ----------------------------------------------------------------------------
// Helper: Build sorted weighted samples with cumulative CDF
// Returns: sorted vector of (value, cumulative_prob_before, weight_normalized)
// ----------------------------------------------------------------------------

namespace kll_detail {

struct WeightedPoint {
    double value;
    double cum_prob;   // cumulative probability at this point (after including this point)
    double weight;     // normalized weight of this point
};

inline std::vector<WeightedPoint> buildSortedCDF(
    const std::vector<double>& values,
    const std::vector<uint64_t>& weights)
{
    size_t n = values.size();
    assert(weights.size() == n);
    if (n == 0) return {};
    
    // Build index array for indirect sort
    std::vector<size_t> idx(n);
    std::iota(idx.begin(), idx.end(), 0);
    std::sort(idx.begin(), idx.end(),
              [&](size_t a, size_t b) { return values[a] < values[b]; });
    
    // Compute total weight
    uint64_t total_w = 0;
    for (uint64_t w : weights) {
        assert(total_w <= (std::numeric_limits<uint64_t>::max() - w));
        total_w += w;
    }
    assert(total_w > 0);
    double inv_total = 1.0 / static_cast<double>(total_w);
    
    // Build sorted weighted points with cumulative CDF
    std::vector<WeightedPoint> pts;
    pts.reserve(n);
    
    double cum = 0.0;
    for (size_t i = 0; i < n; i++) {
        size_t j = idx[i];
        double w_norm = static_cast<double>(weights[j]) * inv_total;
        cum += w_norm;
        pts.push_back({values[j], cum, w_norm});
    }
    
    return pts;
}

// PCHIP: Compute monotone slopes using Fritsch-Carlson method
// Input: x[i], y[i] - knot points (sorted by x, y monotonically increasing)
// Output: d[i] - slope at each knot
inline std::vector<double> computePCHIPSlopes(
    const std::vector<double>& x,
    const std::vector<double>& y)
{
    size_t n = x.size();
    assert(n >= 2);
    
    std::vector<double> d(n, 0.0);
    
    if (n == 2) {
        // Linear case
        double slope = (y[1] - y[0]) / (x[1] - x[0]);
        d[0] = d[1] = slope;
        return d;
    }
    
    // Step 1: Compute secant slopes h[i] = (y[i+1] - y[i]) / (x[i+1] - x[i])
    std::vector<double> h(n - 1);
    std::vector<double> delta(n - 1);  // interval lengths
    for (size_t i = 0; i < n - 1; i++) {
        delta[i] = x[i + 1] - x[i];
        h[i] = (delta[i] > 0) ? (y[i + 1] - y[i]) / delta[i] : 0.0;
    }
    
    // Step 2: Initialize interior slopes using weighted harmonic mean (Fritsch-Carlson)
    for (size_t i = 1; i < n - 1; i++) {
        if (h[i - 1] * h[i] <= 0) {
            // Sign change or zero - set slope to zero for monotonicity
            d[i] = 0.0;
        } else {
            // Weighted harmonic mean: preserves monotonicity
            double w1 = 2.0 * delta[i] + delta[i - 1];
            double w2 = delta[i] + 2.0 * delta[i - 1];
            d[i] = (w1 + w2) / (w1 / h[i - 1] + w2 / h[i]);
        }
    }
    
    // Step 3: Endpoints - use one-sided difference with monotonicity constraint
    d[0] = h[0];
    d[n - 1] = h[n - 2];
    
    // Step 4: Ensure monotonicity constraint: |d[i]| ≤ 3 * min(|h[i-1]|, |h[i]|)
    // For monotone increasing CDF, we ensure d[i] >= 0
    for (size_t i = 0; i < n - 1; i++) {
        if (h[i] == 0) {
            d[i] = 0.0;
            d[i + 1] = 0.0;
        } else {
            double alpha = d[i] / h[i];
            double beta = d[i + 1] / h[i];
            // Fritsch-Carlson condition: alpha^2 + beta^2 <= 9
            double r2 = alpha * alpha + beta * beta;
            if (r2 > 9.0) {
                double tau = 3.0 / std::sqrt(r2);
                d[i] = tau * alpha * h[i];
                d[i + 1] = tau * beta * h[i];
            }
        }
    }
    
    // Ensure non-negativity for monotone increasing CDF
    for (size_t i = 0; i < n; i++) {
        if (d[i] < 0) d[i] = 0.0;
    }
    
    return d;
}

// Evaluate PCHIP at point x, given knots and slopes
// Returns (F(x), F'(x))
inline std::pair<double, double> evalPCHIP(
    double x,
    const std::vector<double>& xk,
    const std::vector<double>& yk,
    const std::vector<double>& dk)
{
    size_t n = xk.size();
    
    // Handle boundaries
    if (x <= xk[0]) return {yk[0], dk[0]};
    if (x >= xk[n - 1]) return {yk[n - 1], dk[n - 1]};
    
    // Binary search for interval [xk[i], xk[i+1]]
    size_t lo = 0, hi = n - 2;
    while (lo < hi) {
        size_t mid = (lo + hi + 1) / 2;
        if (xk[mid] <= x) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    size_t i = lo;
    
    // Hermite basis: t ∈ [0, 1]
    double h = xk[i + 1] - xk[i];
    double t = (x - xk[i]) / h;
    double t2 = t * t;
    double t3 = t2 * t;
    
    // Hermite basis functions:
    // H00(t) = 2t³ - 3t² + 1
    // H10(t) = t³ - 2t² + t
    // H01(t) = -2t³ + 3t²
    // H11(t) = t³ - t²
    double H00 = 2.0 * t3 - 3.0 * t2 + 1.0;
    double H10 = t3 - 2.0 * t2 + t;
    double H01 = -2.0 * t3 + 3.0 * t2;
    double H11 = t3 - t2;
    
    // F(x) = y[i]*H00 + h*d[i]*H10 + y[i+1]*H01 + h*d[i+1]*H11
    double F = yk[i] * H00 + h * dk[i] * H10 + yk[i + 1] * H01 + h * dk[i + 1] * H11;
    
    // Derivatives of Hermite basis:
    // H00'(t) = 6t² - 6t
    // H10'(t) = 3t² - 4t + 1
    // H01'(t) = -6t² + 6t
    // H11'(t) = 3t² - 2t
    double dH00 = 6.0 * t2 - 6.0 * t;
    double dH10 = 3.0 * t2 - 4.0 * t + 1.0;
    double dH01 = -6.0 * t2 + 6.0 * t;
    double dH11 = 3.0 * t2 - 2.0 * t;
    
    // dF/dx = (1/h) * dF/dt
    double dF = (yk[i] * dH00 + h * dk[i] * dH10 + yk[i + 1] * dH01 + h * dk[i + 1] * dH11) / h;
    
    return {F, dF};
}

// ----------------------------------------------------------------------------
// MaxEntropy (Simplified): piecewise-constant density implied by CDF knots.
// In interval (xk[j], xk[j+1]), f(x) = (Fk[j+1] - Fk[j]) / (xk[j+1] - xk[j]).
// ----------------------------------------------------------------------------
inline std::vector<double> maxEntPDF_Simplified(
    const std::vector<double>& x_grid,
    const std::vector<double>& xk,
    const std::vector<double>& Fk)
{
    size_t n_grid = x_grid.size();
    size_t n_knots = xk.size();
    std::vector<double> f(n_grid, 0.0);
    
    if (n_knots < 2) return f;
    
    // For MaxEnt with CDF constraints only:
    // In interval (x_{j}, x_{j+1}), the density is constant:
    // f(x) = (F_{j+1} - F_j) / (x_{j+1} - x_j)
    
    for (size_t i = 0; i < n_grid; i++) {
        double x = x_grid[i];
        
        // Find interval
        if (x <= xk[0] || x >= xk[n_knots - 1]) {
            f[i] = 0.0;
            continue;
        }
        
        // Binary search
        size_t lo = 0, hi = n_knots - 2;
        while (lo < hi) {
            size_t mid = (lo + hi + 1) / 2;
            if (xk[mid] <= x) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        
        double dx = xk[lo + 1] - xk[lo];
        double dF = Fk[lo + 1] - Fk[lo];
        f[i] = (dx > 0) ? dF / dx : 0.0;
    }
    
    return f;
}

// MaxEnt CDF (Simplified): Linear interpolation between knots
inline double maxEntCDF_Simplified(
    double x,
    const std::vector<double>& xk,
    const std::vector<double>& Fk)
{
    size_t n = xk.size();

    if (n == 0) return 0.0;
    if (n == 1) {
        // Degenerate distribution: all mass at xk[0]
        return (x < xk[0]) ? 0.0 : 1.0;
    }
    
    if (x <= xk[0]) return 0.0;
    if (x >= xk[n - 1]) return 1.0;
    
    // Binary search for interval
    size_t lo = 0, hi = n - 2;
    while (lo < hi) {
        size_t mid = (lo + hi + 1) / 2;
        if (xk[mid] <= x) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    
    // Linear interpolation within interval (piecewise uniform)
    double t = (x - xk[lo]) / (xk[lo + 1] - xk[lo]);
    return Fk[lo] + t * (Fk[lo + 1] - Fk[lo]);
}

} // namespace kll_detail

// ----------------------------------------------------------------------------
// Single-point Queries
// ----------------------------------------------------------------------------

inline double KLLcache::queryQuantile(double phi) const {
    assert(!empty());
    
    if (phi <= 0.0) return min_v_;
    if (phi >= 1.0) return max_v_;
    
    auto [values, weights] = exportWeightedSamples();
    auto pts = kll_detail::buildSortedCDF(values, weights);
    
    // Binary search for smallest j s.t. pts[j].cum_prob >= phi
    // Then interpolate for accuracy
    size_t lo = 0, hi = pts.size() - 1;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (pts[mid].cum_prob < phi) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    
    // Linear interpolation between adjacent points for accuracy
    if (lo == 0) {
        // phi is below first cumulative prob
        double t = phi / pts[0].cum_prob;
        return min_v_ + t * (pts[0].value - min_v_);
    }
    
    double F_lo = pts[lo - 1].cum_prob;
    double F_hi = pts[lo].cum_prob;
    double v_lo = pts[lo - 1].value;
    double v_hi = pts[lo].value;
    
    if (F_hi == F_lo) return v_hi;
    
    double t = (phi - F_lo) / (F_hi - F_lo);
    return v_lo + t * (v_hi - v_lo);
}

inline double KLLcache::queryCDF(double x) const {
    assert(!empty());
    
    if (x < min_v_) return 0.0;
    if (x >= max_v_) return 1.0;
    
    auto [values, weights] = exportWeightedSamples();
    
    // Compute weighted sum of 1(v_i <= x)
    uint64_t total_w = 0;
    uint64_t cum_w = 0;
    for (size_t i = 0; i < values.size(); i++) {
        total_w += weights[i];
        if (values[i] <= x) {
            cum_w += weights[i];
        }
    }
    
    return static_cast<double>(cum_w) / static_cast<double>(total_w);
}

inline double KLLcache::queryPDF(double x, ReconstructionMethod method) const {
    assert(!empty());
    
    if (x < min_v_ || x > max_v_) return 0.0;
    if (min_v_ == max_v_) return 0.0;
    
    auto [values, weights] = exportWeightedSamples();
    auto pts = kll_detail::buildSortedCDF(values, weights);
    
    if (pts.size() < 2) return 0.0;
    
    // Build knots: include min and max as boundary
    std::vector<double> xk, Fk;
    xk.reserve(pts.size() + 2);
    Fk.reserve(pts.size() + 2);
    
    xk.push_back(min_v_);
    Fk.push_back(0.0);
    
    for (const auto& p : pts) {
        // Skip duplicates
        if (!xk.empty() && p.value == xk.back()) {
            Fk.back() = p.cum_prob;
        } else {
            xk.push_back(p.value);
            Fk.push_back(p.cum_prob);
        }
    }
    
    if (xk.back() < max_v_) {
        xk.push_back(max_v_);
        Fk.push_back(1.0);
    } else {
        Fk.back() = 1.0;
    }
    
    if (method == ReconstructionMethod::PCHIP) {
        assert(xk.size() >= 2);
        auto dk = kll_detail::computePCHIPSlopes(xk, Fk);
        auto [F, dF] = kll_detail::evalPCHIP(x, xk, Fk, dk);
        return std::max(0.0, dF);  // PDF must be non-negative
    } else {
        // MaxEntropy: piecewise constant
        size_t n = xk.size();
        if (x <= xk[0] || x >= xk[n - 1]) return 0.0;
        
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
    if (x_min == x_max) {
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
    
    // Build knot data
    auto [values, weights] = exportWeightedSamples();
    auto pts = kll_detail::buildSortedCDF(values, weights);
    
    // Build unique knots
    std::vector<double> xk, Fk;
    xk.reserve(pts.size() + 2);
    Fk.reserve(pts.size() + 2);
    
    xk.push_back(x_min);
    Fk.push_back(0.0);
    
    for (const auto& p : pts) {
        if (!xk.empty() && p.value == xk.back()) {
            Fk.back() = p.cum_prob;
        } else {
            xk.push_back(p.value);
            Fk.push_back(p.cum_prob);
        }
    }
    
    if (xk.back() < x_max) {
        xk.push_back(x_max);
        Fk.push_back(1.0);
    } else {
        Fk.back() = 1.0;
    }
    
    if (method == ReconstructionMethod::PCHIP) {
        assert(xk.size() >= 2);
        auto dk = kll_detail::computePCHIPSlopes(xk, Fk);
        for (size_t i = 0; i < num_points; i++) {
            auto [F, dF] = kll_detail::evalPCHIP(x_grid[i], xk, Fk, dk);
            F_grid[i] = std::clamp(F, 0.0, 1.0);
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
    if (x_min == x_max) {
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
    
    // Build knot data
    auto [values, weights] = exportWeightedSamples();
    auto pts = kll_detail::buildSortedCDF(values, weights);
    
    // Build unique knots
    std::vector<double> xk, Fk;
    xk.reserve(pts.size() + 2);
    Fk.reserve(pts.size() + 2);
    
    xk.push_back(x_min);
    Fk.push_back(0.0);
    
    for (const auto& p : pts) {
        if (!xk.empty() && p.value == xk.back()) {
            Fk.back() = p.cum_prob;
        } else {
            xk.push_back(p.value);
            Fk.push_back(p.cum_prob);
        }
    }
    
    if (xk.back() < x_max) {
        xk.push_back(x_max);
        Fk.push_back(1.0);
    } else {
        Fk.back() = 1.0;
    }
    
    if (method == ReconstructionMethod::PCHIP) {
        assert(xk.size() >= 2);
        auto dk = kll_detail::computePCHIPSlopes(xk, Fk);
        for (size_t i = 0; i < num_points; i++) {
            auto [F, dF] = kll_detail::evalPCHIP(x_grid[i], xk, Fk, dk);
            f_grid[i] = std::max(0.0, dF);
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
    
    // Build knot data for inverse CDF (quantile function)
    auto [values, weights] = exportWeightedSamples();
    auto pts = kll_detail::buildSortedCDF(values, weights);
    
    // Build quantile knots: (F_j, v_j) with F as x-axis, v as y-axis
    std::vector<double> Fk, Qk;
    Fk.reserve(pts.size() + 2);
    Qk.reserve(pts.size() + 2);
    
    Fk.push_back(0.0);
    Qk.push_back(min_v_);
    
    for (const auto& p : pts) {
        // For quantile, we need F -> Q, so accumulate
        if (!Fk.empty() && p.cum_prob == Fk.back()) {
            // Same F, take larger Q (right-continuous inverse)
            Qk.back() = p.value;
        } else {
            Fk.push_back(p.cum_prob);
            Qk.push_back(p.value);
        }
    }
    
    // Ensure we end at (1, max)
    if (Fk.back() < 1.0) {
        Fk.push_back(1.0);
        Qk.push_back(max_v_);
    } else {
        Qk.back() = max_v_;
    }
    
    if (method == ReconstructionMethod::PCHIP) {
        // PCHIP on (F, Q) - monotone interpolation of quantile function
        auto dk = kll_detail::computePCHIPSlopes(Fk, Qk);
        for (size_t i = 0; i < num_points; i++) {
            auto [Q, dQ] = kll_detail::evalPCHIP(u_grid[i], Fk, Qk, dk);
            Q_grid[i] = std::clamp(Q, min_v_, max_v_);
        }
    } else {
        // MaxEntropy: piecewise linear quantile (inverse of piecewise linear CDF)
        for (size_t i = 0; i < num_points; i++) {
            double u = u_grid[i];
            
            if (u <= 0.0) {
                Q_grid[i] = min_v_;
                continue;
            }
            if (u >= 1.0) {
                Q_grid[i] = max_v_;
                continue;
            }
            
            // Binary search for interval [Fk[j], Fk[j+1]] containing u
            size_t lo = 0, hi = Fk.size() - 2;
            while (lo < hi) {
                size_t mid = (lo + hi + 1) / 2;
                if (Fk[mid] <= u) lo = mid;
                else hi = mid - 1;
            }
            
            // Linear interpolation
            double t = (Fk[lo + 1] > Fk[lo]) 
                       ? (u - Fk[lo]) / (Fk[lo + 1] - Fk[lo])
                       : 0.0;
            Q_grid[i] = Qk[lo] + t * (Qk[lo + 1] - Qk[lo]);
        }
    }
    
    return {u_grid, Q_grid};
}
