// OrderFlow — GUI OrderFlow tab 数据面 (双图对仗; 单写 worker 线程 / 单读 GUI 线程, 免锁)
//
// OrderFlow
// ├── Kline  (图2) 单资产全回测区间: K线 + 多特征 overlay (L1 分钟频)
// │            worker 逐日流式 (从前往后, 每日一次选列读), GUI 画已发布前缀
// ├── Depth  (图1) 单 (day, asset) 秒级盘口: orders/*.bin 逐笔重放 LOB
// │            + 多特征 overlay (L0 逐列选读); 双槽 ping-pong 整体发布
// ├── HeatmapColored  GUI 线程私有渲染缓存 (front Depth 槽 + 阈值派生)
// └── UI     用户态: 选择 / 锚点 / 请求代 (gen) / 坐标轴缓存
//
// 发布协议 (无锁 SPSC):
//   Kline: 每日尾 feat_n[i] / y range (release) → pub word [gen|days|points] (release);
//          GUI acquire 读 pub, 只画已发布前缀; gen != 请求代 = 正在重建, 不画.
//          数组 reserve 满容量后只 push_back → data() 恒稳定, 前缀读安全.
//   Depth: worker 写背槽 → depth_pending = true; GUI 帧首 ack (pending=false, front^=1);
//          pending 未 ack 期间 worker 不碰任何槽.
#pragma once

#include "features/Backend/FeatureStoreConfig.hpp"

#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

// ============================================================================
// Constants
// ============================================================================

namespace OrderFlowConst {
// Data Capacity
constexpr size_t L0_CAPACITY = LEVELS[0].rows; // ~15300 秒/日 (X 轴跨度)
constexpr size_t L1_CAPACITY = LEVELS[1].rows; // ~255 分钟/日 (X 轴跨度)
constexpr size_t LOB_DEPTH = L2::LOB_DEPTH;    // 30 levels
constexpr size_t MAX_FEATURES = 8;             // overlay 特征多选上限 (两图同限)

// Price and Volume Conversion
constexpr float TICK_SIZE = 0.01f;            // Minimum price step (RMB)
constexpr float SHARES_PER_LOT = 100.0f;      // 1 lot = N shares
constexpr float PRICE_SCALE = 100.0f;         // Price stored as integer * N
constexpr float ROUNDING_OFFSET = 0.5f;       // For float to int conversion
constexpr int32_t AMOUNT_ROUND_TO_RMB = 1000; // Round amount to nearest N RMB

// Cache Reserve Sizes
constexpr size_t ESTIMATED_PRICE_LEVELS = 512; // 全簿唯一价位初始预留 (不够则增长)

// Amount Thresholds (RMB)
constexpr float AMOUNT_MIN_VISIBLE = 1000.0f;      // 1K RMB (transparent in heatmap)
constexpr float AMOUNT_MAX_VISIBLE = 10000000.0f;  // 10M RMB (solid in heatmap)
constexpr float DEPTH_BAR_MAX_AMOUNT = 1000000.0f; // 100W RMB (full bar in depth panel)

// GUI Layout Parameters
constexpr float DEPTH_PANEL_WIDTH = 160.0f; // Width of depth panel (pixels)
constexpr float TOP_VIEW_RATIO = 0.55f;     // Top view height ratio (55%)
constexpr float Y_MARGIN_RATIO = 0.20f;     // Y-axis margin for plots (20%)

// GUI Rendering Parameters
constexpr float MIN_CANDLESTICK_BODY_HEIGHT = 1.0f; // Minimum visible body (pixels)
constexpr double CANDLESTICK_HALF_WIDTH = 0.5;      // Half width of candlestick bar
} // namespace OrderFlowConst

// ============================================================================
// Main OrderFlow Structure
// ============================================================================

struct OrderFlow {
  // ==========================================================================
  // FeatLine — overlay 一条特征线 (x/y 对齐, 只含有效点; 两图同构)
  // ==========================================================================
  struct FeatLine {
    std::vector<double> x, y;
    void clear() {
      x.clear();
      y.clear();
    }
  };

  // ==========================================================================
  // Kline (图2) — 单资产全回测: K线 + 多特征, 逐日流式, 单调前缀发布
  // ==========================================================================
  struct Kline {
    // pub word: [gen:16][days:16][points:32]; gen 换代时 days/points 归零
    static constexpr uint64_t pack(uint32_t gen, size_t days, size_t points) {
      return (static_cast<uint64_t>(gen & 0xFFFF) << 48) |
             (static_cast<uint64_t>(days & 0xFFFF) << 32) |
             static_cast<uint64_t>(points & 0xFFFFFFFF);
    }
    static constexpr void unpack(uint64_t w, uint32_t &gen, size_t &days, size_t &points) {
      gen = static_cast<uint32_t>(w >> 48);
      days = static_cast<size_t>((w >> 32) & 0xFFFF);
      points = static_cast<size_t>(w & 0xFFFFFFFF);
    }

    // ---- worker 写 / GUI 前缀读 ----
    std::vector<std::string> dates; // 回测区间全部特征日 (换代时随 rescan 重扫; gen 匹配期间稳定)
    size_t asset_idx = SIZE_MAX;

    // K线点集: data_valid 分钟, x = day_idx * L1_CAPACITY + minute
    std::vector<double> x, open, high, low, close;
    std::array<FeatLine, OrderFlowConst::MAX_FEATURES> feat; // 与 K线同 X 网格, 每特征独立点集 (跳过 NaN)
    size_t n_feat = 0;

    // 发布面 (worker release / GUI acquire)
    std::atomic<uint64_t> pub{0};
    std::array<std::atomic<size_t>, OrderFlowConst::MAX_FEATURES> feat_n{}; // 每特征已发布点数
    std::array<std::atomic<float>, OrderFlowConst::MAX_FEATURES> feat_y_min{}, feat_y_max{};
    std::atomic<double> y_min{0.0}, y_max{0.0}; // OHLC 已发布范围

    // 坐标换算 (纯算术; dates 读取需 gen 匹配)
    size_t day_idx_from_x(double global_x) const {
      return static_cast<size_t>(global_x) / OrderFlowConst::L1_CAPACITY;
    }
    double snap_to_day_start(double global_x) const {
      return static_cast<double>(day_idx_from_x(global_x) * OrderFlowConst::L1_CAPACITY);
    }
    const std::string &date_from_x(double global_x) const {
      static const std::string empty;
      const size_t d = day_idx_from_x(global_x);
      return d < dates.size() ? dates[d] : empty;
    }

    // worker: 换代 — 先零计数再置 gen (GUI 见到新 gen 时旧计数必已归零)
    void begin_generation(uint32_t gen, size_t asset, size_t n_feats);
    // worker: 满容量预留 (dates 定下后一次; 之后只 push_back, data() 稳定)
    void reserve_capacity();

    void clear();
  } kline;

  // ==========================================================================
  // Depth (图1) — 单 (day, asset) 秒级盘口重放结果 (双槽 ping-pong)
  // ==========================================================================
  struct Depth {
    // 单秒盘口快照 (秒末终值; 稀疏, 只存重放出有效盘口的秒, 按 tick_idx 升序)
    struct Tick {
      size_t tick_idx; // 交易秒下标 [0, 15300)
      float mid_price;
      std::array<float, OrderFlowConst::LOB_DEPTH> bid_price, ask_price; // 元, NaN = 笼外/哨兵
      std::array<float, OrderFlowConst::LOB_DEPTH> bid_volume;           // 手, SIGNED: > 0
      std::array<float, OrderFlowConst::LOB_DEPTH> ask_volume;           // 手, SIGNED: < 0
    };

    // 线图: 与 ticks 1:1 (plot_idx == ticks 下标)
    struct Plot {
      std::vector<double> x, mid_price, best_bid, best_ask;
      std::vector<size_t> tick_idx_map; // 秒下标 -> plot_idx (SIZE_MAX = 无), O(1) snap
      double y_min = 0.0, y_max = 0.0;
      double y_min_with_margin = 0.0, y_max_with_margin = 0.0;
      void clear();
    };

    // 热力图 (全簿): 逐有效秒喂 LOB 全部可见档位 → 按价位合并矩形 (amount 不变则延长)
    struct HeatmapMerged {
      struct Rect {
        size_t tick_start, tick_end;
        float price_high, price_low; // high >= low always
        int32_t amount_rmb;          // SIGNED: +bid, -ask
      };
      struct Level {
        float price;
        std::vector<Rect> rects;
      };
      std::vector<Level> levels;
      size_t rect_count = 0;
      void clear();
    };

    // 深度面板查询结果
    struct Snapshot {
      float mid_price = 0;
      const std::array<float, OrderFlowConst::LOB_DEPTH> *bid_price = nullptr;
      const std::array<float, OrderFlowConst::LOB_DEPTH> *ask_price = nullptr;
      const std::array<float, OrderFlowConst::LOB_DEPTH> *bid_volume = nullptr;
      const std::array<float, OrderFlowConst::LOB_DEPTH> *ask_volume = nullptr;
      size_t tick_idx = 0;
      struct {
        uint8_t hour, minute, second;
      } time;
      bool valid = false;
    };

    // ---- 数据 (worker 写背槽, 发布后只读) ----
    uint32_t gen = 0; // 请求代 (GUI 配对; 兼作 HeatmapColored 失效键)
    std::string date;
    size_t asset_idx = SIZE_MAX;
    bool has_data = false; // .bin 存在且重放出至少一个有效快照
    size_t order_count = 0;
    size_t data_valid_count = 0; // 有逐笔的秒数

    std::vector<Tick> ticks; // 按 tick_idx 升序
    Plot plot;
    HeatmapMerged merged;
    std::array<FeatLine, OrderFlowConst::MAX_FEATURES> feat; // L0 特征线 (data_valid 秒)
    std::array<float, OrderFlowConst::MAX_FEATURES> feat_y_min{}, feat_y_max{};
    size_t n_feat = 0;

    // ---- 查询 (GUI, front 槽; X = 秒下标) ----
    size_t plot_idx_from_x(double x) const;
    size_t snap_to_valid_plot_idx(double x) const;
    Snapshot query_depth(size_t plot_idx) const;

    // ---- 构建 (worker, 背槽) ----
    struct HeatmapScratch {
      std::map<int, size_t> price_to_level;              // price_key -> merged.levels 下标 (整代累积, 有序)
      std::vector<std::pair<int, int32_t>> current_tick; // 当前秒全簿: (price_key 升序, SIGNED amount_rmb)
      void clear();
    };
    void build_plot();
    // 热力图增量构建: begin 一次 → 重放中逐有效秒 (caller 填好 current_tick) commit
    void heatmap_begin(HeatmapScratch &scratch);
    void heatmap_commit_tick(HeatmapScratch &scratch, size_t tick_idx);

    void clear();
  };

  Depth depth[2];
  std::atomic<int> depth_front{0};        // GUI ack 时翻转
  std::atomic<bool> depth_pending{false}; // worker 置位 / GUI ack
  const Depth &depth_front_slot() const { return depth[depth_front.load(std::memory_order_acquire)]; }

  // ==========================================================================
  // HeatmapColored — GUI 线程私有: front Depth 槽 + 阈值 → 着色矩形
  // ==========================================================================
  struct HeatmapColored {
    struct Rect {
      double x1, y1, x2, y2;
      uint32_t color;
    };
    struct Metadata { // tooltip (与 rects 1:1)
      int32_t amount_rmb;
      float price; // bid: price_high, ask: price_low
      size_t tick_start, tick_end;
    };
    std::vector<Rect> rects;
    std::vector<Metadata> metadata;

    uint32_t gen = UINT32_MAX; // 绑定的 Depth 槽 gen
    float threshold = -1.0f;   // 绑定的 log_amount_threshold
    bool matches(uint32_t g, float thr) const { return gen == g && threshold == thr; }

    void build(const Depth &src, float log_threshold); // GUI 线程
    void clear();
  } heatmap_colored;

  // ==========================================================================
  // UI State — GUI 线程私有 (请求代 gen 与发布面配对)
  // ==========================================================================
  struct UI {
    // Selection state
    int selected_asset_idx = 0;
    double l1_anchor_x = 0;
    std::string l1_anchor_date;       // GUI 自持副本 (dates 重建期间仍可用)
    size_t l0_anchor_tick = SIZE_MAX; // 图1 锚点 = 秒下标 (跨日/换资产稳定; SIZE_MAX = 未设)

    // Rendering parameters
    bool show_heatmap = true;
    float log_amount_threshold = 5.0f; // log10(amount) threshold [3.0, 7.0]

    // 请求快照 (期望态; 变化 → gen++ → Request*)
    uint32_t kline_gen = 0, depth_gen = 0;
    size_t kline_asset = SIZE_MAX, depth_asset = SIZE_MAX;
    std::vector<int> kline_feats, depth_feats; // 选中特征列 (L1 / L0)
    std::string depth_date;

    // Y 轴管理 (流式期间跟随发布范围)
    size_t l1_last_pub_days = SIZE_MAX; // 上次应用 Y1 范围时的已发布日数
    uint32_t l0_last_gen = UINT32_MAX;  // 上次 L0 视图重置时的槽 gen

    void clear();
  } ui;

  // 特征重算完成 → 重扫日期 + 整体重拉 (TaskFeatures 置位, tab 消费)
  std::atomic<bool> needs_rescan{false};

  void clear();
};

// ============================================================================
// Helper Functions
// ============================================================================

// Convert volume and price to amount (RMB)
// NOTE: volume is SIGNED (bid > 0, ask < 0), so amount preserves the sign
inline float volume_to_amount(float volume, float price) {
  return volume * price * OrderFlowConst::SHARES_PER_LOT;
}

// Convert price to integer key for heatmap
inline int price_to_key(float price) {
  return static_cast<int>(price * OrderFlowConst::PRICE_SCALE + OrderFlowConst::ROUNDING_OFFSET);
}

// Round amount to nearest AMOUNT_ROUND_TO_RMB
inline int32_t round_amount_to_rmb(float amount) {
  return static_cast<int32_t>(std::round(amount / static_cast<float>(OrderFlowConst::AMOUNT_ROUND_TO_RMB))) * OrderFlowConst::AMOUNT_ROUND_TO_RMB;
}

// Convert amount (RMB) to 万元 (10K RMB)
inline float amount_to_wan(float amount) {
  return amount / 10000.0f;
}
