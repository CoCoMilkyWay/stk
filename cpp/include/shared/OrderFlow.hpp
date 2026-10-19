// OrderFlow — GUI OrderFlow tab 数据面 (双图对仗; 每块单写 worker 线程 / 单读 GUI 线程, 免锁)
//
// OrderFlow (主库 = K 线 OHLC 所在库, Lob 或 Bar 档皆可; 日轴 = 主库 + overlay 各库落盘日的并集, 各库画各自有的日)
// ├── Kline  (图2) 单资产全日轴: K线 + 多特征 overlay (≥ L_min 的任意 ColRef, 分钟网格)
// │            worker 分批流式 (从前往后, 每日每源一次选列读, 下一批后台预读), GUI 画已发布前缀
// ├── Depth  (图1) 单 (day, asset) 秒级盘口: 当天日文件里的逐笔帧重放订单簿
// │            + 多特征 overlay (任意 ColRef, 秒网格); 双槽 ping-pong 整体发布
// overlay 两图同构 (Overlay): 点 = 尾部对齐 (行 emit 时刻), x = 日起点 + 网格行 (行末秒 − 1), 网格: 图1 L_sec / 图2 L_min
//   (细于网格的源每格留最后一个有效行). 各源按自己的 (库, 层) 读、按 column_set 门控 (与分析页同一 IO 契约)
// ├── HeatmapColored  GUI 线程私有渲染缓存 (front Depth 槽 + 阈值派生)
// └── UI     用户态: 选择 / 锚点 / 请求代 (gen) / 坐标轴缓存
//
// 发布协议 (无锁 SPSC):
//   Kline: 每批尾 feat_n[i] / y range (release) → pub word [gen|days|points] (release, shared/Progress.hpp PrefixPub);
//          GUI acquire 读 pub, 只画已发布前缀; gen != 请求代 = 正在重建, 不画.
//          数组 reserve 满容量后只 push_back → data() 恒稳定, 前缀读安全.
//   Depth / Pick: PingPong (shared/Progress.hpp) — worker 写背槽 → commit; GUI 帧首 ack 翻面;
//          pending 未 ack 期间 worker 不碰任何槽.
// 三块各带进度面 (Kline 自身 / PingPong), 是各自 worker 的 board.
#pragma once

#include "codec/L2_DataType.hpp"              // L2::LOB_DEPTH
#include "features/Storage/FeatureLevels.hpp" // ColRef / level_end_sec (稳定层, 不依赖字段表)
#include "features/TimeIndex.hpp"             // L_SEC_ROWS / L_MIN_ROWS (稳定形状常量, 不依赖字段表)
#include "misc/date.hpp"
#include "shared/Progress.hpp" // PrefixPub / PingPong

#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

// ============================================================================
// Constants
// ============================================================================

namespace OrderFlowConst {
// Data Capacity
constexpr size_t SEC_CAPACITY = L_SEC_ROWS; // ~15300 秒/日 (X 轴跨度)
constexpr size_t MIN_CAPACITY = L_MIN_ROWS; // ~255 分钟/日 (X 轴跨度)
constexpr size_t LOB_DEPTH = L2::LOB_DEPTH; // 30 levels
constexpr size_t MAX_FEATURES = 8;          // overlay 特征多选上限 (两图同限)

// Price and Volume Conversion
constexpr float TICK_SIZE = 0.01f;            // Minimum price step (RMB)
constexpr float PRICE_SCALE = 100.0f;         // Price stored as integer * N
constexpr float ROUNDING_OFFSET = 0.5f;       // For float to int conversion
constexpr int32_t AMOUNT_ROUND_TO_RMB = 1000; // Round amount to nearest N RMB

// Cache Reserve Sizes
constexpr size_t ESTIMATED_PRICE_LEVELS = 512; // 全簿唯一价位初始预留 (不够则增长)

// Amount Thresholds (RMB)
constexpr float AMOUNT_MAX_VISIBLE = 10000000.0f;  // 10M RMB (solid in heatmap)
constexpr float DEPTH_BAR_MAX_AMOUNT = 1000000.0f; // 100W RMB (full bar in depth panel)

// 热力图阈值 (log10 金额): 滑条区间 / 显示精度 / 自动初值目标浓度
constexpr float HEATMAP_LOG_THR_MIN = 3.0f;     // 1千元
constexpr float HEATMAP_LOG_THR_MAX = 7.0f;     // 1千万元 (= AMOUNT_MAX_VISIBLE)
constexpr float HEATMAP_LOG_THR_STEP = 0.1f;    // 自动阈值直方图分辨率 = 滑条显示精度
constexpr float HEATMAP_LOG_THR_DEFAULT = 5.0f; // 无数据兜底 (10万元)
constexpr float HEATMAP_AUTO_INK_RATIO = 0.60f; // 自动阈值目标: 着色价位秒 / 有量价位秒 (越大越密)

// 日内时段边界 (L_sec 秒下标): 开盘竞价 [0,600) / 盘前5分钟 [600,900)
// / 连续竞价 [900,15120) / 收盘竞价 [15120,15300)
constexpr size_t SEG_AUCTION_OPEN_END = Clock_to_L_sec(9, 25, 0);     // 600
constexpr size_t SEG_PREOPEN_END = Clock_to_L_sec(9, 30, 0);          // 900
constexpr size_t SEG_AUCTION_CLOSE_BEGIN = Clock_to_L_sec(14, 57, 0); // 15120

// GUI Layout Parameters
constexpr float DEPTH_PANEL_WIDTH = 160.0f; // Width of depth panel (pixels; 纵向深度图)
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
  // Overlay — 一图的特征 overlay (两图同构): 各线的列 + 点集 + 值域
  //   V = float (Depth: 整槽发布) / std::atomic<float> (Kline: 前缀发布, worker 逐日更新值域)
  // ==========================================================================
  template <class V>
  struct Overlay {
    std::vector<ColRef> refs;                                // [n] 各线的列 (legend / 轴分流按此取元数据, 与槽 / 代配对)
    std::array<FeatLine, OrderFlowConst::MAX_FEATURES> line; // 只含有效点 (哨兵 / NaN 跳过)
    std::array<V, OrderFlowConst::MAX_FEATURES> y_min{}, y_max{};
    size_t n() const { return refs.size(); }
    void clear() {
      refs.clear();
      for (size_t i = 0; i < OrderFlowConst::MAX_FEATURES; ++i) {
        line[i].clear();
        y_min[i] = 0.0f;
        y_max[i] = 0.0f;
      }
    }
  };

  // ==========================================================================
  // Kline (图2) — 单资产全日轴: K线 + 多特征, 分批流式, 单调前缀发布
  // ==========================================================================
  struct Kline : Progress, PrefixPub { // 进度 done/total = 天
    // ---- worker 写 / GUI 前缀读 ----
    std::vector<misc::Ymd> dates; // 日轴: 库集各库落盘日的并集 (换代时随 rescan 重扫; gen 匹配期间稳定)

    // K线点集: data_valid 分钟, x = day_idx * MIN_CAPACITY + minute
    std::vector<double> x, open, high, low, close;
    Overlay<std::atomic<float>> feat; // 与 K线同 X 网格 (refs 换代时定, gen 匹配期间稳定)

    // 发布面 (worker release / GUI acquire; pub word 在 PrefixPub)
    std::array<std::atomic<size_t>, OrderFlowConst::MAX_FEATURES> feat_n{}; // 每特征已发布点数
    std::atomic<double> y_min{0.0}, y_max{0.0};                             // OHLC 已发布范围

    // 坐标换算 (纯算术; dates 读取需 gen 匹配)
    size_t day_idx_from_x(double global_x) const {
      return static_cast<size_t>(global_x) / OrderFlowConst::MIN_CAPACITY;
    }
    double snap_to_day_start(double global_x) const {
      return static_cast<double>(day_idx_from_x(global_x) * OrderFlowConst::MIN_CAPACITY);
    }
    misc::Ymd date_from_x(double global_x) const {
      const size_t d = day_idx_from_x(global_x);
      return d < dates.size() ? dates[d] : 0;
    }

    // worker: 换代 — 先零计数 / 清数组 / 定 overlay 列, 后置 gen (GUI 见到新 gen 时一切已就位)
    void begin_generation(uint32_t gen, std::vector<ColRef> refs);
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
      float mid_price; // 竞价交叉秒 = 预撮合参考价, 其余 = (bid1+ask1)/2
      // 集合竞价预撮合 (订单簿 update_depth 竞价分支): ref > 0 = 竞价且簿交叉, 其余恒 0
      float ref_price;                                                   // 元
      float matched_amount;                                              // 元, 参考价位虚拟匹配额
      float imbalance_amount;                                            // 元, SIGNED: + 买剩, - 卖剩
      std::array<float, OrderFlowConst::LOB_DEPTH> bid_price, ask_price; // 元, NaN = 笼外/哨兵
      std::array<float, OrderFlowConst::LOB_DEPTH> bid_volume;           // 手, SIGNED: > 0
      std::array<float, OrderFlowConst::LOB_DEPTH> ask_volume;           // 手, SIGNED: < 0
    };

    // 线图: 与 ticks 1:1 (plot_idx == ticks 下标)
    struct Plot {
      std::vector<double> x, mid_price, best_bid, best_ask;
      std::vector<size_t> tick_idx_map; // 秒下标 -> plot_idx (SIZE_MAX = 无), O(1) snap
      // 分钟 VWAP 阶梯 (L_min 特征列 vwap, 与标签 exit 同源; 只有库轴内且当日已算特征的资产才有):
      // 逐有效分钟一点, X = 分钟起始秒, 末尾补一点 (末分钟末秒) 收口; 与 ticks 无关
      std::vector<double> vwap_x, vwap_y;
      double y_min = 0.0, y_max = 0.0;
      double y_min_with_margin = 0.0, y_max_with_margin = 0.0;
      void clear();
    };

    // 热力图 (全簿): 逐有效秒喂订单簿全部可见价位 → 按价位合并矩形 (amount 不变则延长)
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
      float ref_price = 0; // 预撮合 (> 0 = 竞价交叉秒), 语义同 Tick
      float matched_amount = 0;
      float imbalance_amount = 0;
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
    misc::Ymd date = 0;
    size_t asset_idx = SIZE_MAX;
    bool has_data = false; // 帧存在且重放出至少一个有效快照
    size_t order_count = 0;

    std::vector<Tick> ticks; // 按 tick_idx 升序
    Plot plot;
    HeatmapMerged merged;
    // 当日自动热力图阈值 (log10 元): 载入新槽时 GUI 取作初值 → 色块浓度跨日/跨资产一致
    float auto_log_threshold = OrderFlowConst::HEATMAP_LOG_THR_DEFAULT;
    Overlay<float> feat; // 与图2 同源, X = 行末秒 − 1

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
    // 自动阈值: 合并矩形按持续秒加权的 log10(净额) 上分位 (依赖 build_plot 的初始 Y 视野)
    void build_auto_threshold();
    // 热力图增量构建: begin 一次 → 重放中逐有效秒 (caller 填好 current_tick) commit
    void heatmap_begin(HeatmapScratch &scratch);
    void heatmap_commit_tick(HeatmapScratch &scratch, size_t tick_idx);

    void clear();
  };

  PingPong<Depth> depth;

  // ==========================================================================
  // Pick (资产筛选) — 锚点日的逐日 PIT 状态 (worker) + 筛选/候选 (GUI)
  //   状态源 = L_min 落盘列 (Fund 节点日频广播): risk_warn / list_age / delist_age
  //   / industry_l1, 配合同层 ts_valid 判有效 —— 当日无有效分钟的资产读不到状态
  //   (落盘缓冲清零, 值全 0 会被误读成"正常在市"), 一律不进候选.
  // ==========================================================================
  struct Pick {
    // 单资产当日状态 (worker 写背槽, 发布后只读)
    struct Meta {
      uint8_t industry_l1 = 0; // SW2021 一级行业 ID (0 = 未知), 见 fund::SW2021_L1_NAMES
      uint8_t risk_warn = 0;   // 0=正常 1=ST 2=*ST 3=退市整理期
      bool has_data = false;   // 当日有有效分钟 (false = 其余字段无意义)
      bool listed = false;     // 当日在市 (已上市 且 未退市)
      bool delisted = false;   // 当日已退市
    };

    // ---- 逐日状态槽 (双槽 ping-pong, 对仗 Depth) ----
    struct Slot {
      uint32_t gen = 0;
      misc::Ymd date = 0;
      std::vector<Meta> meta; // [A] 与 AssetAxis 同序 (asset_idx 直接下标)
      void clear();
    };
    PingPong<Slot> slots;

    // ---- 筛选条件 (GUI; 空集 = 该维不筛, 对仗 DATABASE/TABLE) ----
    std::set<int> st_filter;        // risk_warn 取值 0..3
    std::set<int> listed_filter{0}; // 0=在市 1=退市 (默认在市)
    std::set<int> board_filter;     // GUI::Database::BoardType 的底层值
    std::set<int> industry_filter;  // SW2021 一级行业 ID

    // ---- 候选列表 (GUI 派生缓存: 过滤 + 排序) ----
    std::vector<size_t> candidates;    // 通过筛选的 asset_idx (市场 → 代码序)
    std::vector<size_t> display_order; // 全部 asset_idx 按 市场 → 代码 排序 (静态, 建一次)
    uint32_t cached_gen = UINT32_MAX;  // 上次 rebuild 绑定的槽 gen
    uint64_t filter_epoch = 0;         // 筛选条件版本 (改一次 +1)
    uint64_t cached_epoch = UINT64_MAX;
    bool matches(uint32_t g) const { return cached_gen == g && cached_epoch == filter_epoch; }

    // 请求快照 (GUI; 锚点日变 → gen++ → RequestPick)
    uint32_t gen = 0;
    misc::Ymd req_date = 0;

    // GUI: 单资产是否通过筛选 (has_data 是前置条件)
    bool passes(const Meta &m, int board) const;
    // GUI: display_order 建一次 (市场 → 代码序); codes/exchanges 与 AssetAxis 同序
    void build_display_order(const std::vector<std::pair<std::string, std::string>> &exch_code);
    // GUI: 重建候选 (front 槽 + 筛选条件); boards[asset_idx] 由调用方按代码推出
    void rebuild_candidates(const Slot &s, const std::vector<int> &boards);

    void clear();
  } pick;

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
  // DepthProfile — GUI 线程私有: 锚点秒的全簿截面 → 纵向深度图 (图1右)
  //   数据源 = 热力图合并矩形 (全簿, 不受 30 价位限制): 每价位二分找覆盖锚点秒的矩形.
  //   竞价交叉簿三态: 纯买区 (< 最低卖价位) / 纯卖区 (> 最高买价位) / 重合区 [ask_low, bid_top]
  // ==========================================================================
  struct DepthProfile {
    // 逐价位净额 (价升序; amount SIGNED 元: + 买 - 卖)
    std::vector<double> price, amount;
    // 累计曲线 (万元): 买自最高买价向下累计, 卖自最低卖价向上累计 (各自独立点集)
    std::vector<double> bid_cum_x, bid_cum_y; // y = 价格降序
    std::vector<double> ask_cum_x, ask_cum_y; // y = 价格升序
    double bid_top = 0, ask_low = 0;          // 最高买价位价 / 最低卖价位价 (交叉: ask_low < bid_top)
    double cum_max = 0;                       // 两侧累计额最大值 (万元, X 轴范围)

    uint32_t gen = UINT32_MAX; // 绑定的 Depth 槽 gen
    size_t tick = SIZE_MAX;    // 绑定的锚点秒
    bool matches(uint32_t g, size_t t) const { return gen == g && tick == t; }

    void build(const Depth &src, size_t tick_idx); // GUI 线程
    void clear();
  } depth_profile;

  // ==========================================================================
  // UI State — GUI 线程私有 (请求代 gen 与发布面配对)
  // ==========================================================================
  struct UI {
    // Selection state
    int selected_asset_idx = 0;
    bool asset_initialized = false; // 首个候选就绪时自动选一次 (之后尊重用户选择)
    double min_anchor_x = 0;
    misc::Ymd min_anchor_date = 0;     // GUI 自持副本 (dates 重建期间仍可用; 0 = 未设)
    size_t sec_anchor_tick = SIZE_MAX; // 图1 锚点 = 秒下标 (跨日/换资产稳定; SIZE_MAX = 未设)

    // Rendering parameters
    bool show_heatmap = true;
    // log10(amount) 阈值, 区间 [HEATMAP_LOG_THR_MIN, MAX]; 换槽时取该槽 auto_log_threshold
    float log_amount_threshold = OrderFlowConst::HEATMAP_LOG_THR_DEFAULT;

    // 请求快照 (期望态; 变化 → gen++ → Request*)
    uint32_t kline_gen = 0, depth_gen = 0;
    size_t kline_asset = SIZE_MAX, depth_asset = SIZE_MAX;
    std::vector<ColRef> kline_feats, depth_feats; // 选中特征列 (同一组选中, 各截到图的网格: 图2 ≥ L_min / 图1 ≥ L_sec; 截断 MAX_FEATURES)
    LibMask kline_libs = 0;                       // 图2 库集 (主库 + overlay 各库) = 日轴并集的库; 变了 → 重扫日轴
    misc::Ymd depth_date = 0;

    // 图2 X 域 (= 特征日数 × MIN_CAPACITY): GUI 自持副本, 换代在途时照画空图不撤轴;
    // 只在域本身变了 (重扫日期) 才复位视野 —— 换资产不动 X
    double min_x_max = 0.0, min_x_applied = 0.0;
    bool kline_rescan_pending = false; // 请求带 rescan (dates 正在重扫): 期间 dates 不可读

    // Y 轴管理 (流式期间跟随发布范围)
    size_t min_last_pub_days = SIZE_MAX;     // 上次应用 Y1 范围时的已发布日数
    uint32_t sec_last_gen = UINT32_MAX;      // 上次 L_sec 视图重置时的槽 gen
    double sec_y_min = 0.0, sec_y_max = 0.0; // 图1 当前 Y 视野 (每帧快照; 右侧深度面板同步)

    // 图2 缺失日标记缓存: 已发布前缀中该资产无 data_valid 分钟的 day_idx (只在单库日轴时画: 多库叠加时日轴是并集, 不画)
    // (kline_gen, pub_days) 变才重算 — 流式追加 / 换资产 / 重扫日期 都被 gen 覆盖
    std::vector<size_t> min_missing_days;
    uint32_t min_missing_days_gen = UINT32_MAX;
    size_t min_missing_days_pub_days = 0;

    void clear();
  } ui;

  // 特征重算完成 → 重扫日期 + 整体重拉 (TaskFeatures 置位, tab 消费)
  std::atomic<bool> needs_rescan{false};

  void clear();
};

// ============================================================================
// Helper Functions
// ============================================================================

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
