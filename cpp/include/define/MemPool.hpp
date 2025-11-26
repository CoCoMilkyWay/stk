#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <vector>

// ═══════════════════════════════════════════════════════════════════════════
// 控制选项
// ═══════════════════════════════════════════════════════════════════════════
// #define MEMPOOL_WARN_ON_EXPANSION  // 打印扩容日志
// #define MEMPOOL_ENABLE_STATS       // 启用统计功能(零开销)

#ifdef MEMPOOL_WARN_ON_EXPANSION
#include "misc/logging.hpp"
#include <sstream>
#endif

#ifdef MEMPOOL_ENABLE_STATS
#include <iostream>
#endif

/*
═══════════════════════════════════════════════════════════════════════════
高性能内存管理系统
═══════════════════════════════════════════════════════════════════════════

架构层次(从底层到应用层):
  [底层] BumpPool<T>    - 极速分配池(不支持回收)
  [底层] BitmapPool<T>  - 通用分配池(支持O(1)回收)
  [应用] HashMap<K,V,P> - 配置化哈希表(Pool可选)
  [别名] BumpDict<K,V>   - 基于BumpPool的字典
  [别名] BitmapDict<K,V> - 基于BitmapPool的字典

性能指标:
  BumpPool:   1-3 cycles 分配
  BitmapPool: 3-7 cycles 分配 + 3-7 cycles 回收
  HashMap:    +3-10 cycles 哈希开销

内存模型:
  - 分块存储:每块 2^N 个对象(目标 ~1MB)
  - 自动扩容:指针永久有效
  - 缓存友好:块内连续,64字节对齐

═══════════════════════════════════════════════════════════════════════════
*/

namespace MemPool {

// ═══════════════════════════════════════════════════════════════════════════
// 第一层:配置常量
// ═══════════════════════════════════════════════════════════════════════════

namespace Config {
static constexpr size_t CACHE_LINE_SIZE = 64;      // 缓存行大小
static constexpr size_t DEFAULT_CAPACITY = 10000;  // 默认初始容量
static constexpr size_t MIN_BUCKET_COUNT = 16;     // HashMap最小桶数
static constexpr double TARGET_LOAD_FACTOR = 0.50; // HashMap目标负载因子 (降低以减少冲突)

// 自适应块大小:根据对象大小自动选择,目标 ~1MB/块
template <typename T>
struct ChunkConfig {
  static constexpr size_t SHIFT =
      sizeof(T) <= 16 ? 16 : // 16B  x 2^16 = 1.0 MB
          sizeof(T) <= 32 ? 15
                          : // 32B  x 2^15 = 1.0 MB
          sizeof(T) <= 64 ? 14
                          : // 64B  x 2^14 = 1.0 MB
          sizeof(T) <= 128 ? 13
                           : // 128B x 2^13 = 1.0 MB
          12;                // 256B x 2^12 = 1.0 MB

  static constexpr size_t SIZE = size_t{1} << SHIFT; // 块大小(对象数)
  static constexpr size_t MASK = SIZE - 1;           // 块掩码(快速取模)
};
} // namespace Config

// ═══════════════════════════════════════════════════════════════════════════
// 第二层:辅助函数
// ═══════════════════════════════════════════════════════════════════════════

namespace detail {
// 向上对齐到2的幂
[[gnu::const]] inline constexpr size_t round_up_pow2(size_t n) noexcept {
  return n <= 1 ? 1 : size_t{1} << (64 - __builtin_clzll(n - 1));
}

// 计算所需块数
template <typename T>
[[gnu::const]] inline constexpr size_t calc_chunk_count(size_t capacity) noexcept {
  using Cfg = Config::ChunkConfig<T>;
  return std::max(size_t{1}, (capacity + Cfg::SIZE - 1) >> Cfg::SHIFT);
}
} // namespace detail

// ═══════════════════════════════════════════════════════════════════════════
// 第三层:BumpPool(极简分配器,渐进式引入概念)
// ═══════════════════════════════════════════════════════════════════════════

template <typename T>
class alignas(Config::CACHE_LINE_SIZE) BumpPool {
public:
  using value_type = T;
  using Chunk = Config::ChunkConfig<T>;

  // ─────────────────────────────────────────────────────────────────────────
  // 生命周期
  // ─────────────────────────────────────────────────────────────────────────

  explicit BumpPool(size_t initial_capacity = Config::DEFAULT_CAPACITY)
      : num_allocated_(0),
        num_initial_chunks_(detail::calc_chunk_count<T>(initial_capacity)),
        cache_chunk_(nullptr),
        cache_limit_(0) {
    chunks_.reserve(num_initial_chunks_);
    for (size_t i = 0; i < num_initial_chunks_; ++i) {
      expand_storage();
    }
    if (!chunks_.empty()) {
      cache_chunk_ = chunks_[0];
      cache_limit_ = Chunk::SIZE;
    }
  }

  ~BumpPool() {
    reset();
    for (T *chunk : chunks_) {
      operator delete[](chunk, std::align_val_t{Config::CACHE_LINE_SIZE});
    }
  }

  BumpPool(const BumpPool &) = delete;
  BumpPool &operator=(const BumpPool &) = delete;
  BumpPool(BumpPool &&) = delete;
  BumpPool &operator=(BumpPool &&) = delete;

  // ─────────────────────────────────────────────────────────────────────────
  // 核心操作(按使用频率排序)
  // ─────────────────────────────────────────────────────────────────────────

  // 1. 分配:返回未初始化的内存
  [[nodiscard, gnu::hot, gnu::always_inline]]
  inline T *allocate() {
    size_t slot_idx = num_allocated_++;

    // 快速路径:当前块内有空间
    if (slot_idx < cache_limit_) [[likely]] {
      return &cache_chunk_[slot_idx & Chunk::MASK];
    }

    // 慢速路径:切换到新块
    return allocate_slow_path(slot_idx);
  }

  // 2. 回收:Bump语义不支持
  [[gnu::always_inline]]
  inline void deallocate(T *) noexcept {
    // Bump Pool不回收单个对象
  }

  // 3. 构造:分配 + 初始化
  template <typename... Args>
  [[nodiscard, gnu::hot, gnu::always_inline]]
  inline T *construct(Args &&...args) {
    T *ptr = allocate();
    new (ptr) T(std::forward<Args>(args)...);
#ifdef MEMPOOL_ENABLE_STATS
    ++num_constructed_;
#endif
    return ptr;
  }

  // 4. 重置:析构所有对象,重置状态
  void reset(bool shrink = false) {
    // 析构所有已分配对象
    if constexpr (!std::is_trivially_destructible_v<T>) {
      for (size_t i = 0; i < num_allocated_; ++i) {
        size_t chunk_idx = i >> Chunk::SHIFT;
        size_t local_idx = i & Chunk::MASK;
        chunks_[chunk_idx][local_idx].~T();
      }
    }

    // 重置分配计数
    num_allocated_ = 0;

    // 可选:释放多余内存
    if (shrink && chunks_.size() > num_initial_chunks_) {
      for (size_t i = num_initial_chunks_; i < chunks_.size(); ++i) {
        operator delete[](chunks_[i], std::align_val_t{Config::CACHE_LINE_SIZE});
      }
      chunks_.resize(num_initial_chunks_);
    }

    // 重置快速路径缓存
    if (!chunks_.empty()) {
      cache_chunk_ = chunks_[0];
      cache_limit_ = Chunk::SIZE;
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 查询接口
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] size_t size() const noexcept {
    return num_allocated_;
  }

  [[nodiscard]] size_t capacity() const noexcept {
    return chunks_.size() * Chunk::SIZE;
  }

  [[nodiscard]] double utilization() const noexcept {
    size_t cap = capacity();
    return cap > 0 ? static_cast<double>(num_allocated_) / cap : 0.0;
  }

#ifdef MEMPOOL_ENABLE_STATS
  [[nodiscard]] size_t num_constructed() const noexcept {
    return num_constructed_;
  }

  void reset_stats() noexcept {
    num_constructed_ = 0;
  }
#endif

private:
  // ─────────────────────────────────────────────────────────────────────────
  // 内部状态
  // ─────────────────────────────────────────────────────────────────────────

  std::vector<T *> chunks_;   // 内存块数组
  size_t num_allocated_;      // 已分配对象数(单调递增)
  size_t num_initial_chunks_; // 初始块数(用于shrink)

  // 快速路径缓存(避免重复计算)
  T *cache_chunk_;     // 当前块指针
  size_t cache_limit_; // 当前块上限(全局索引)

#ifdef MEMPOOL_ENABLE_STATS
  size_t num_constructed_ = 0; // 统计:构造次数
#endif

  // ─────────────────────────────────────────────────────────────────────────
  // 内部辅助
  // ─────────────────────────────────────────────────────────────────────────

  [[gnu::noinline]]
  T *allocate_slow_path(size_t slot_idx) {
    size_t chunk_idx = slot_idx >> Chunk::SHIFT;

    // 需要新块时扩容
    if (chunk_idx >= chunks_.size()) [[unlikely]] {
      expand_storage();
    }

    // 更新缓存
    cache_chunk_ = chunks_[chunk_idx];
    cache_limit_ = (chunk_idx + 1) << Chunk::SHIFT;

    return &cache_chunk_[slot_idx & Chunk::MASK];
  }

  void expand_storage() {
    size_t bytes = Chunk::SIZE * sizeof(T);
    void *raw = operator new[](bytes, std::align_val_t{Config::CACHE_LINE_SIZE});
    chunks_.push_back(static_cast<T *>(raw));

#ifdef MEMPOOL_WARN_ON_EXPANSION
    if (chunks_.size() > num_initial_chunks_ && Logger::is_initialized()) {
      std::ostringstream oss;
      oss << "[BumpPool] Expansion #" << chunks_.size()
          << " (" << (bytes / 1024.0 / 1024.0) << " MB)";
      Logger::log("analyzing", oss.str());
    }
#endif
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// 第四层:BitmapPool(引入回收机制)
// ═══════════════════════════════════════════════════════════════════════════

template <typename T>
class alignas(Config::CACHE_LINE_SIZE) BitmapPool {
public:
  using value_type = T;
  using Chunk = Config::ChunkConfig<T>;

  // ─────────────────────────────────────────────────────────────────────────
  // 生命周期
  // ─────────────────────────────────────────────────────────────────────────

  explicit BitmapPool(size_t initial_capacity = Config::DEFAULT_CAPACITY)
      : num_alive_(0),
        peak_allocated_(0),
        num_initial_chunks_(detail::calc_chunk_count<T>(initial_capacity)) {
    chunks_.reserve(num_initial_chunks_);
    freelist_.reserve(num_initial_chunks_ * (Chunk::SIZE >> 6));
    sorted_chunk_indices_.reserve(num_initial_chunks_);

    for (size_t i = 0; i < num_initial_chunks_; ++i) {
      expand_storage();
    }
  }

  ~BitmapPool() {
    reset();
    for (T *chunk : chunks_) {
      operator delete[](chunk, std::align_val_t{Config::CACHE_LINE_SIZE});
    }
  }

  BitmapPool(const BitmapPool &) = delete;
  BitmapPool &operator=(const BitmapPool &) = delete;
  BitmapPool(BitmapPool &&) = delete;
  BitmapPool &operator=(BitmapPool &&) = delete;

  // ─────────────────────────────────────────────────────────────────────────
  // 核心操作(按使用频率排序)
  // ─────────────────────────────────────────────────────────────────────────

  // 1. 分配:优先复用空闲slot,否则扩展
  [[nodiscard, gnu::hot]]
  T *allocate() {
    // 策略1:从已用区域找空闲slot(热路径,cache友好)
    size_t search_limit = (peak_allocated_ + 63) >> 6; // 转换为word数
    for (size_t word_idx = 0; word_idx < search_limit; ++word_idx) {
      uint64_t free_bits = freelist_[word_idx];
      if (free_bits != 0) [[likely]] {
        size_t bit_idx = __builtin_ctzll(free_bits);
        size_t slot_idx = (word_idx << 6) + bit_idx;

        // 标记为占用
        freelist_[word_idx] &= ~(1ULL << bit_idx);
        ++num_alive_;

        // 更新峰值
        if (slot_idx >= peak_allocated_) [[unlikely]] {
          peak_allocated_ = slot_idx + 1;
        }

        // 转换为实际地址
        size_t chunk_idx = slot_idx >> Chunk::SHIFT;
        size_t local_idx = slot_idx & Chunk::MASK;
        return &chunks_[chunk_idx][local_idx];
      }
    }

    // 策略2:扩展到新区域(冷路径)
    size_t slot_idx = peak_allocated_++;
    size_t chunk_idx = slot_idx >> Chunk::SHIFT;

    // 需要新块时扩容
    if (chunk_idx >= chunks_.size()) [[unlikely]] {
      expand_storage();
    }

    // 标记为占用
    size_t word_idx = slot_idx >> 6;
    size_t bit_idx = slot_idx & 63;
    freelist_[word_idx] &= ~(1ULL << bit_idx);
    ++num_alive_;

    // 转换为实际地址
    size_t local_idx = slot_idx & Chunk::MASK;
    return &chunks_[chunk_idx][local_idx];
  }

  // 2. 回收:析构 + 标记空闲
  [[gnu::hot, gnu::always_inline]]
  inline void deallocate(T *ptr) {
    if (!ptr) [[unlikely]]
      return;

#ifdef MEMPOOL_ENABLE_STATS
    ++num_deallocated_;
#endif

    // 析构对象
    if constexpr (!std::is_trivially_destructible_v<T>) {
      ptr->~T();
    }

    // 二分查找所属chunk(O(log N))
    size_t chunk_idx = find_chunk_index(ptr);
    if (chunk_idx == SIZE_MAX) [[unlikely]] {
      assert(false && "BitmapPool::deallocate: pointer not owned");
      return;
    }

    // 计算slot索引
    T *chunk_base = chunks_[chunk_idx];
    size_t offset_in_chunk = static_cast<size_t>(ptr - chunk_base);
    size_t slot_idx = (chunk_idx << Chunk::SHIFT) + offset_in_chunk;

    // 标记为空闲
    size_t word_idx = slot_idx >> 6;
    size_t bit_idx = slot_idx & 63;
    freelist_[word_idx] |= (1ULL << bit_idx);
    --num_alive_;
  }

  // 3. 构造:分配 + 初始化
  template <typename... Args>
  [[nodiscard, gnu::hot]]
  T *construct(Args &&...args) {
    T *ptr = allocate();
    new (ptr) T(std::forward<Args>(args)...);
#ifdef MEMPOOL_ENABLE_STATS
    ++num_constructed_;
#endif
    return ptr;
  }

  // 4. 重置:析构所有对象,重置状态
  void reset(bool shrink = false) {
#ifdef MEMPOOL_ENABLE_STATS
    if (num_constructed_ > 0) {
      print_stats();
    }
#endif

    // 析构所有存活对象
    if constexpr (!std::is_trivially_destructible_v<T>) {
      for (size_t i = 0; i < peak_allocated_; ++i) {
        size_t word_idx = i >> 6;
        size_t bit_idx = i & 63;

        // 检查是否被占用(bit为0表示占用)
        if ((freelist_[word_idx] & (1ULL << bit_idx)) == 0) {
          size_t chunk_idx = i >> Chunk::SHIFT;
          size_t local_idx = i & Chunk::MASK;
          chunks_[chunk_idx][local_idx].~T();
        }
      }
    }

    // 重置状态(所有bit设为1表示空闲)
    std::fill(freelist_.begin(), freelist_.end(), ~0ULL);
    num_alive_ = 0;
    peak_allocated_ = 0;

    // 可选:释放多余内存
    if (shrink && chunks_.size() > num_initial_chunks_) {
      for (size_t i = num_initial_chunks_; i < chunks_.size(); ++i) {
        operator delete[](chunks_[i], std::align_val_t{Config::CACHE_LINE_SIZE});
      }
      chunks_.resize(num_initial_chunks_);

      // 同步缩减bitmap
      size_t bitmap_words = (num_initial_chunks_ * Chunk::SIZE) >> 6;
      freelist_.resize(bitmap_words);
      std::fill(freelist_.begin(), freelist_.end(), ~0ULL);

      // 重建地址索引
      rebuild_chunk_index();
    }

#ifdef MEMPOOL_ENABLE_STATS
    reset_stats();
#endif
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 查询接口
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] size_t size() const noexcept {
    return num_alive_;
  }

  [[nodiscard]] size_t capacity() const noexcept {
    return chunks_.size() * Chunk::SIZE;
  }

  [[nodiscard]] double utilization() const noexcept {
    return peak_allocated_ > 0 ? static_cast<double>(num_alive_) / peak_allocated_ : 1.0;
  }

#ifdef MEMPOOL_ENABLE_STATS
  [[nodiscard]] size_t num_constructed() const noexcept {
    return num_constructed_;
  }

  [[nodiscard]] size_t num_deallocated() const noexcept {
    return num_deallocated_;
  }

  void reset_stats() noexcept {
    num_constructed_ = 0;
    num_deallocated_ = 0;
  }

  void print_stats(const char *name = "BitmapPool") const noexcept {
    size_t cap = capacity();
    double turnover = num_constructed_ > 0
                          ? num_deallocated_ * 100.0 / num_constructed_
                          : 0.0;
    double reclaimed = num_constructed_ > 0
                           ? (1.0 - static_cast<double>(num_alive_) / num_constructed_) * 100.0
                           : 0.0;

    std::cout << "\n[" << name << " Stats]\n"
              << "  Constructed:  " << num_constructed_ << "\n"
              << "  Deallocated:  " << num_deallocated_ << "\n"
              << "  Alive/Peak:   " << num_alive_ << " / " << peak_allocated_ << "\n"
              << "  Capacity:     " << cap << " (" << chunks_.size() << " chunks)\n"
              << "  Memory:       " << (cap * sizeof(T) / 1024.0 / 1024.0) << " MB\n"
              << "  Turnover:     " << turnover << "%\n"
              << "  Reclaimed:    " << reclaimed << "%\n";
  }
#endif

private:
  // ─────────────────────────────────────────────────────────────────────────
  // 内部状态
  // ─────────────────────────────────────────────────────────────────────────

  std::vector<T *> chunks_;                  // 内存块数组
  std::vector<uint64_t> freelist_;           // 空闲位图(1=空闲,0=占用)
  std::vector<size_t> sorted_chunk_indices_; // 按地址排序的chunk索引

  size_t num_alive_;          // 当前存活对象数
  size_t peak_allocated_;     // 历史峰值(水位线)
  size_t num_initial_chunks_; // 初始块数(用于shrink)

#ifdef MEMPOOL_ENABLE_STATS
  size_t num_constructed_ = 0; // 统计:构造次数
  size_t num_deallocated_ = 0; // 统计:析构次数
#endif

  // ─────────────────────────────────────────────────────────────────────────
  // 内部辅助
  // ─────────────────────────────────────────────────────────────────────────

  // 二分查找指针所属的chunk(O(log N))
  [[gnu::hot]]
  size_t find_chunk_index(T *ptr) const noexcept {
    size_t left = 0, right = sorted_chunk_indices_.size();

    while (left < right) {
      size_t mid = (left + right) >> 1;
      size_t idx = sorted_chunk_indices_[mid];
      T *base = chunks_[idx];

      if (ptr < base) {
        right = mid;
      } else if (ptr >= base + Chunk::SIZE) {
        left = mid + 1;
      } else {
        return idx; // 找到了
      }
    }

    return SIZE_MAX; // 未找到
  }

  void expand_storage() {
    size_t bytes = Chunk::SIZE * sizeof(T);
    void *raw = operator new[](bytes, std::align_val_t{Config::CACHE_LINE_SIZE});
    chunks_.push_back(static_cast<T *>(raw));

    // 扩展freelist(每个chunk需要 SIZE/64 个word)
    size_t bitmap_words = Chunk::SIZE >> 6;
    freelist_.insert(freelist_.end(), bitmap_words, ~0ULL);

    // 维护地址排序索引(用于快速查找)
    size_t new_idx = chunks_.size() - 1;
    T *new_base = chunks_.back();
    auto it = std::lower_bound(
        sorted_chunk_indices_.begin(), sorted_chunk_indices_.end(), new_base,
        [this](size_t idx, T *ptr) { return chunks_[idx] < ptr; });
    sorted_chunk_indices_.insert(it, new_idx);

#ifdef MEMPOOL_WARN_ON_EXPANSION
    if (chunks_.size() > num_initial_chunks_ && Logger::is_initialized()) {
      std::ostringstream oss;
      oss << "[BitmapPool] Expansion #" << chunks_.size()
          << " (" << (bytes / 1024.0 / 1024.0) << " MB)";
      Logger::log("analyzing", oss.str());
    }
#endif
  }

  void rebuild_chunk_index() {
    sorted_chunk_indices_.clear();
    sorted_chunk_indices_.reserve(chunks_.size());
    for (size_t i = 0; i < chunks_.size(); ++i) {
      sorted_chunk_indices_.push_back(i);
    }
    std::sort(sorted_chunk_indices_.begin(), sorted_chunk_indices_.end(),
              [this](size_t a, size_t b) { return chunks_[a] < chunks_[b]; });
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// 第五层:HashMap(应用层,基于Pool构建)
// ═══════════════════════════════════════════════════════════════════════════

template <typename Key, typename Value, template <typename> class Pool = BitmapPool,
          typename Hash = std::hash<Key>>
class HashMap {
private:
  // 链表节点
  struct Node {
    Key key;
    Value value;
    Node *next;

    Node(const Key &k, const Value &v) : key(k), value(v), next(nullptr) {}
    Node(Key &&k, Value &&v) : key(std::move(k)), value(std::move(v)), next(nullptr) {}
  };

  Pool<Node> node_pool_;        // 节点内存池
  std::vector<Node *> buckets_; // 哈希桶数组
  Hash hasher_;                 // 哈希函数
  size_t num_entries_;          // 当前条目数
  size_t bucket_mask_;          // 桶掩码(count-1,用于快速取模)

#ifdef MEMPOOL_ENABLE_STATS
  size_t num_erased_ = 0; // 统计:删除次数
#endif

public:
  // ─────────────────────────────────────────────────────────────────────────
  // 生命周期
  // ─────────────────────────────────────────────────────────────────────────

  explicit HashMap(size_t expected_size = Config::DEFAULT_CAPACITY)
      : node_pool_(expected_size), num_entries_(0) {
    // 计算桶数:目标负载因子 ~0.67,向上对齐到2的幂
    size_t target = std::max(Config::MIN_BUCKET_COUNT,
                             static_cast<size_t>(expected_size / Config::TARGET_LOAD_FACTOR));
    size_t bucket_count = detail::round_up_pow2(target);
    bucket_mask_ = bucket_count - 1;
    buckets_.resize(bucket_count, nullptr);
  }

  ~HashMap() {
    clear();
  }

  HashMap(const HashMap &) = delete;
  HashMap &operator=(const HashMap &) = delete;
  HashMap(HashMap &&) = delete;
  HashMap &operator=(HashMap &&) = delete;

  // ─────────────────────────────────────────────────────────────────────────
  // 核心操作(按使用频率排序)
  // ─────────────────────────────────────────────────────────────────────────

  // 1. 查找:返回值指针(nullptr表示不存在)
  [[nodiscard, gnu::hot, gnu::always_inline]]
  inline Value *find(const Key &key) {
    size_t bucket_idx = hasher_(key) & bucket_mask_;
    Node *node = buckets_[bucket_idx];
    
    // Unroll first iteration (most common case: empty or single node)
    if (node == nullptr) [[unlikely]]
      return nullptr;
    if (node->key == key) [[likely]]
      return &node->value;
    
    node = node->next;
    while (node) {
      if (node->key == key)
        return &node->value;
      node = node->next;
    }
    return nullptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline]]
  inline const Value *find(const Key &key) const {
    return const_cast<HashMap *>(this)->find(key);
  }

  // 2. 插入/更新:存在则更新,不存在则插入
  [[gnu::hot]]
  bool insert(const Key &key, const Value &value) {
    size_t bucket_idx = hasher_(key) & bucket_mask_;
    Node *node = buckets_[bucket_idx];

    // 查找是否已存在
    while (node) {
      if (node->key == key) {
        node->value = value; // 更新
        return true;
      }
      node = node->next;
    }

    // 插入新节点(头插法)
    Node *new_node = node_pool_.construct(key, value);
    new_node->next = buckets_[bucket_idx];
    buckets_[bucket_idx] = new_node;
    ++num_entries_;
    return true;
  }

  // 3. 尝试插入:存在则返回现有值,不存在则插入
  [[gnu::hot, gnu::always_inline]]
  inline std::pair<Value *, bool> try_emplace(const Key &key, const Value &value) {
    size_t bucket_idx = hasher_(key) & bucket_mask_;
    Node *node = buckets_[bucket_idx];

    // Unroll first iteration (most common: empty bucket or first node match)
    if (node == nullptr) [[unlikely]] {
      goto insert_new;
    }
    if (node->key == key) [[likely]] {
      return {&node->value, false}; // 已存在
    }

    // 查找是否已存在(继续遍历链表)
    node = node->next;
    while (node) {
      if (node->key == key) {
        return {&node->value, false}; // 已存在
      }
      node = node->next;
    }

  insert_new:
    // 插入新节点(头插法)
    Node *new_node = node_pool_.construct(key, value);
    new_node->next = buckets_[bucket_idx];
    buckets_[bucket_idx] = new_node;
    ++num_entries_;
    return {&new_node->value, true}; // 新插入
  }

  // 4. 删除:返回是否成功
  [[gnu::hot]]
  bool erase(const Key &key) {
    size_t bucket_idx = hasher_(key) & bucket_mask_;
    Node **prev_ptr = &buckets_[bucket_idx];
    Node *node = *prev_ptr;

    while (node) {
      if (node->key == key) {
        *prev_ptr = node->next;      // 从链表移除
        node_pool_.deallocate(node); // 回收内存
#ifdef MEMPOOL_ENABLE_STATS
        ++num_erased_;
#endif
        --num_entries_;
        return true;
      }
      prev_ptr = &node->next;
      node = node->next;
    }
    return false;
  }

  // 5. 清空:删除所有条目
  void clear() {
#ifdef MEMPOOL_ENABLE_STATS
    if (num_entries_ > 0) {
      print_stats();
    }
#endif
    std::fill(buckets_.begin(), buckets_.end(), nullptr);
    node_pool_.reset();
    num_entries_ = 0;
#ifdef MEMPOOL_ENABLE_STATS
    reset_stats();
#endif
  }

  // ─────────────────────────────────────────────────────────────────────────
  // 查询接口
  // ─────────────────────────────────────────────────────────────────────────

  [[nodiscard]] size_t size() const noexcept {
    return num_entries_;
  }

  [[nodiscard]] bool empty() const noexcept {
    return num_entries_ == 0;
  }

  // 遍历所有条目
  template <typename Func>
  void for_each(Func &&func) const {
    for (Node *head : buckets_) {
      for (Node *node = head; node; node = node->next) {
        func(node->key, node->value);
      }
    }
  }

#ifdef MEMPOOL_ENABLE_STATS
  [[nodiscard]] size_t num_erased() const noexcept {
    return num_erased_;
  }

  void reset_stats() noexcept {
    num_erased_ = 0;
  }

  void print_stats(const char *name = "HashMap") const noexcept {
    double load_factor = static_cast<double>(num_entries_) / (bucket_mask_ + 1);
    std::cout << "\n[" << name << " Stats]\n"
              << "  Entries:      " << num_entries_ << "\n"
              << "  Erased:       " << num_erased_ << "\n"
              << "  Buckets:      " << (bucket_mask_ + 1) << "\n"
              << "  Load Factor:  " << load_factor << "\n";
  }
#endif
};

// ═══════════════════════════════════════════════════════════════════════════
// 第六层:类型别名(便利层)
// ═══════════════════════════════════════════════════════════════════════════

template <typename Key, typename Value, typename Hash = std::hash<Key>>
using BumpDict = HashMap<Key, Value, BumpPool, Hash>;

template <typename Key, typename Value, typename Hash = std::hash<Key>>
using BitmapDict = HashMap<Key, Value, BitmapPool, Hash>;

} // namespace MemPool
