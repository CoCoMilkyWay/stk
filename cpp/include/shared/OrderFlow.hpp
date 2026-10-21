// OrderFlow — GUI OrderFlow tab 数据面 (两图对仗; 每块单写 worker 线程 / 单读 GUI 线程, 免锁)
//
// OrderFlow (主池 = 图1 VWAP / 候选轴所在池, Lob 或 Bar 档皆可; 图2 日轴 = all + overlay 各库落盘日的并集, 各池画各自有的日)
// ├── Sec   (图1, 网格 L_sec)  单 (day, asset) 秒级盘口: 当天日文件里的逐笔帧重放订单簿
// │            + 多特征 overlay (任意 ColRef; 尾挂主池 vwap); 双槽 ping-pong 整体发布
// ├── Min   (图2, 网格 L_1min) 单资产全日轴: all 池日 K 底图 + 多特征 overlay (≥ L_1min 的任意 ColRef; 分钟 K 也是 overlay)
// │            worker 分批流式 (从前往后, 每日每源一次选列读, 下一批后台预读), GUI 画已发布前缀
// ├── Pick  资产筛选: 锚点日逐日 PIT 状态 (双槽 ping-pong) + 筛选 / 候选 (GUI)
// overlay 两图同构 (Overlay): 点 = 尾部对齐 (行 emit 时刻), x = 日起点 + 网格行 (行末秒 − 1)
//   (细于网格的源每格留最后一个有效行). 各源按自己的 (池, 层) 读、按 column_set 门控 (与分析页同一 IO 契约)
// ├── HeatColor / Profile / EventMarks / MissingDays  GUI 线程私有渲染缓存, 键 = Memo (键变才重建)
// └── UI    用户态: 选择 / 锚点 / 三路期望态 Want (键变 → gen++ → Request) / 坐标轴缓存
//
// 两侧对仗 = 下标 Side (Bid / Ask), 不写两份: 一档价 best[s] / 深度累计 cum[s] / 内沿 edge[s]
//
// 发布协议 (无锁 SPSC), 三路同一判据 = 发布代追上请求代 (Min::prefix(gen).ready / Board::ready(gen)):
//   Min: 每批尾 overlay 计数 / 值域 (release) → pub word [gen|days|points] (release, shared/Progress.hpp PrefixPub);
//        GUI acquire 读 pub, 只画已发布前缀; 数组 reserve 满容量后只 push_back → data() 恒稳定, 前缀读安全.
//   Sec / Pick: PingPong (shared/Progress.hpp) — worker 写背槽 → commit; GUI 帧首 ack 翻面;
//        pending 未 ack 期间 worker 不碰任何槽.
// 三块各带进度面 (Min 自身 / PingPong), 是各自 worker 的 board.
#pragma once

#include "codec/L2_DataType.hpp"              // L2::LOB_DEPTH
#include "features/Storage/FeatureLevels.hpp" // ColRef / level_end_sec (稳定层, 不依赖字段表)
#include "features/TimeIndex.hpp"             // L_SEC_ROWS / L_1MIN_ROWS (稳定形状常量, 不依赖字段表)
#include "misc/date.hpp"
#include "shared/AssetEvent.hpp" // EventMarks
#include "shared/Progress.hpp"   // PrefixPub / PingPong

#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

// ============================================================================
// Constants
// ============================================================================

namespace OrderFlowConst {
// Data Capacity
constexpr size_t SEC_CAPACITY = L_SEC_ROWS;    // ~15300 秒/日 (X 轴跨度)
constexpr size_t MIN_CAPACITY = L_1MIN_ROWS;   // ~255 分钟/日 (X 轴跨度)
constexpr size_t LOB_DEPTH = L2::LOB_DEPTH;    // 30 levels
constexpr size_t MAX_FEATURES = 8;             // overlay 特征多选上限 (两图同限)
constexpr size_t MAX_LINES = MAX_FEATURES + 1; // overlay 线上限 (选中 + 图1 尾挂 vwap)
constexpr size_t PRICE_SLOTS = 65536;          // 订单簿价位下标空间 (= lob PRICE_RANGE_SIZE, OrderFlowService.cpp 断言)

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
constexpr std::array<size_t, 3> SEG_BOUNDS = {SEG_AUCTION_OPEN_END, SEG_PREOPEN_END, SEG_AUCTION_CLOSE_BEGIN};

// GUI Layout Parameters
constexpr float DEPTH_PANEL_WIDTH = 160.0f; // Width of depth panel (pixels; 纵向深度图)
constexpr float TOP_VIEW_RATIO = 0.55f;     // Top view height ratio (55%)
constexpr float Y_MARGIN_RATIO = 0.20f;     // Y-axis margin for plots (20%)

// GUI Rendering Parameters
constexpr float MIN_CANDLESTICK_BODY_HEIGHT = 1.0f; // Minimum visible body (pixels)
constexpr double CANDLESTICK_HALF_WIDTH = 0.5;      // Half width of candlestick bar
} // namespace OrderFlowConst

// ============================================================================
// Memo / Want — GUI 线程的两种键 (缓存失效 / 请求换代), 全 tab 只此两份
// ============================================================================

// Memo — 渲染缓存的失效键: 键变 (或未建) → stale 记新键并返回 true, 调用方随即重建
template <class... K>
struct Memo {
  std::optional<std::tuple<K...>> key;
  bool stale(K... k) {
    std::tuple<K...> t{k...};
    if (key == t)
      return false;
    key = std::move(t);
    return true;
  }
  void reset() { key.reset(); }
};

// Want — 期望态 → 请求代: 键变 (或 force / 未发过) → gen++, 调用方随即 Request; 发布面以 gen 配对
template <class K>
struct Want {
  std::optional<K> key;
  uint32_t gen = 0;
  bool set(const K &k, bool force = false) {
    if (!force && key == k)
      return false;
    key = k;
    ++gen;
    return true;
  }
  void reset() { key.reset(); } // 下次 set 必换代
};

// ============================================================================
// Main OrderFlow Structure
// ============================================================================

struct OrderFlow {
  // 两侧下标 (买卖对仗): sign = +1 / −1
  enum Side : size_t { Bid = 0,
                       Ask = 1 };
  template <class T>
  using PerSide = std::array<T, 2>;

  // 双槽发布面 + 配对判据 (Sec / Pick 同一份)
  template <class Slot>
  struct Board : PingPong<Slot> {
    bool ready(uint32_t g) const { return this->front_slot().gen == g; }
  };

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
  // Overlay — 一图的特征 overlay (两图同构): 各线的列 + 点集 + 已发布点数 / 值域
  //   worker: begin (换代: 清点集 / 零计数 / 定列) → 追加点 → publish (值域 relaxed, 点数 release)
  //   GUI:    count (acquire) 之内的点可读; y_lo / y_hi 随之
  // ==========================================================================
  struct Overlay {
    std::vector<ColRef> refs;                             // [size()] 各线的列 (legend / 轴分流 / 配色按此取, 与槽 / 代配对)
    std::array<FeatLine, OrderFlowConst::MAX_LINES> line; // 只含有效点 (哨兵 / NaN 跳过)
    std::array<std::atomic<size_t>, OrderFlowConst::MAX_LINES> n{};
    std::array<std::atomic<float>, OrderFlowConst::MAX_LINES> lo{}, hi{};

    size_t size() const { return refs.size(); }
    size_t count(size_t i) const { return n[i].load(std::memory_order_acquire); }
    float y_lo(size_t i) const { return lo[i].load(std::memory_order_relaxed); }
    float y_hi(size_t i) const { return hi[i].load(std::memory_order_relaxed); }

    void begin(std::vector<ColRef> cols);
    void publish(const float *lo_, const float *hi_); // [size()] 本代至今的值域
    void clear();
  };

  // ==========================================================================
  // Min (图2, 网格 L_1min) — 单资产全日轴: K线 + 多特征, 分批流式, 单调前缀发布
  // ==========================================================================
  struct Min : Progress, PrefixPub { // 进度 done/total = 天
    enum Ohlc : size_t { kOpen = 0,
                         kHigh,
                         kLow,
                         kClose }; // 与读计划固定列 [0, 4) 同序

    // ---- worker 写 / GUI 前缀读 ----
    std::vector<misc::Ymd> dates; // 日轴: 池集各库落盘日的并集 (换代时随 rescan 重扫; gen 匹配期间稳定)

    // K线点集 (all 池日 K): data_valid 行, x = day_idx * MIN_CAPACITY + 该行所跨分钟的中点 (日 K = 日中)
    std::vector<double> x;
    std::array<std::vector<double>, 4> ohlc;                    // [Ohlc] 与 x 1:1
    double half_width = OrderFlowConst::CANDLESTICK_HALF_WIDTH; // 一根 K 线半宽 (X 单位; 换代时随 OHLC 层定)
    Overlay feat;                                               // 与 K线同 X 网格 (refs 换代时定, gen 匹配期间稳定)

    // GUI: 已发布前缀 (发布代 ≠ 请求代 → 未就绪, 计数归零: 数组正在重建)
    struct Prefix {
      bool ready = false;
      size_t days = 0, points = 0;
    };
    Prefix prefix(uint32_t gen) const;

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
    void begin_generation(uint32_t gen, std::vector<ColRef> refs, double half_width);
    // worker: 满容量预留 (dates 定下后一次; 之后只 push_back, data() 稳定); ohlc_rows = OHLC 层每日有效行
    void reserve_capacity(size_t ohlc_rows);

    void clear();
  } min;

  // ==========================================================================
  // Sec (图1, 网格 L_sec) — 单 (day, asset) 秒级盘口重放结果 (双槽 ping-pong)
  // ==========================================================================
  struct Sec {
    static constexpr uint32_t kNoRow = UINT32_MAX;

    // 热力图 (全簿): 逐有效秒喂订单簿全部可见价位 → 按价位合并矩形 (额不变则延长; 时段边界处断开, 矩形不跨段)
    struct Heat {
      struct Rect {
        uint32_t t0, t1; // [t0, t1) 秒
        int32_t amount;  // 元, SIGNED 恒非 0: + bid (矩形 [价 − tick, 价]) / − ask ([价, 价 + tick])
      };
      struct Level {
        float price;
        size_t off;                     // 首个矩形在全体矩形拼接序中的下标 (GUI 着色数组按此对齐)
        std::vector<Rect> rects;        // t0 升序, 首尾不重叠
        const Rect *at(size_t t) const; // 覆盖秒 t 的矩形 (无 = nullptr)
      };
      std::vector<Level> levels; // 价升序
      size_t rect_count = 0;

      // 价落在 [lo, hi] 的价位下标区间 [first, last) (价升序二分)
      std::pair<size_t, size_t> span(double lo, double hi) const;
      static std::pair<double, double> rect_y(float price, int32_t amount) { // 矩形价区间 [y0, y1]
        const double p = static_cast<double>(price), tick = static_cast<double>(OrderFlowConst::TICK_SIZE);
        return amount > 0 ? std::pair{p - tick, p} : std::pair{p, p + tick};
      }
      void clear();
    };

    // 热力图构建暂存 (worker 私有常驻): 价位下标 = key − base, 稠密 O(1)
    struct HeatScratch {
      static constexpr uint32_t kNoLevel = UINT32_MAX;
      uint32_t base = 0;                              // 当日价位基准 (key = base + 价位下标)
      std::vector<uint32_t> level_of;                 // [PRICE_SLOTS] 价位下标 → levels 下标 (只在重放期有效)
      std::vector<std::pair<int, int32_t>> prev, cur; // 上一提交秒 / 本秒全簿: (price_key 升序, SIGNED 元, 非 0)
      size_t t_prev = 0;                              // 上一提交秒
    };

    // ---- 数据 (worker 写背槽, 发布后只读) ----
    uint32_t gen = 0; // 请求代 (GUI 配对; 兼作 HeatColor / Profile 失效键)
    misc::Ymd date = 0;
    size_t asset_idx = SIZE_MAX;
    bool has_data = false; // 帧存在且重放出至少一个有效快照
    size_t order_count = 0;

    // 秒序列 (秒末终值; 稀疏, 只存重放出有效盘口的秒, x 升序; 各列 1:1, 行 = 下标)
    std::vector<double> x;             // 交易秒下标 [0, 15300)
    std::vector<double> mid;           // 竞价交叉秒 = 预撮合参考价, 其余 = (bid1 + ask1) / 2
    PerSide<std::vector<double>> best; // 一档价 (元); 竞价交叉秒 NaN (断线不画)
    std::vector<double> ref;           // 预撮合参考价 (订单簿竞价分支): > 0 = 竞价且簿交叉, 其余恒 0
    std::vector<double> matched;       // 元, 参考价位虚拟匹配额
    std::vector<double> imbalance;     // 元, SIGNED: + 买剩, − 卖剩
    std::vector<uint32_t> row_of;      // [SEC_CAPACITY] 秒 → 行 (kNoRow = 无), O(1) snap
    double y_lo = 0.0, y_hi = 0.0;     // 初始 Y 视野 (连续竞价秒 bid1..ask1 极值 ± 余量; 双击复位同口径)

    Heat heat;
    // 当日自动热力图阈值 (log10 元): 载入新槽时 GUI 取作初值 → 色块浓度跨日/跨资产一致
    float auto_log_threshold = OrderFlowConst::HEATMAP_LOG_THR_DEFAULT;
    Overlay feat; // 与图2 同源, X = 行末秒 − 1

    // ---- 查询 (GUI, front 槽; X = 秒下标) ----
    size_t size() const { return x.size(); }
    size_t row_at(double gx) const; // 秒有快照 O(1), 否则右邻有效秒 (越末取末行); 空 = SIZE_MAX

    // ---- 构建 (worker, 背槽): begin → 重放中逐快照 put (秒内覆盖写) + 逐有效秒 heat_commit (调用方填好 s.cur) → end ----
    void begin(HeatScratch &s, uint32_t base);
    void put(size_t t, float bid1, float ask1, float ref_price, float matched_amount, float imbalance_amount);
    void heat_commit(HeatScratch &s, size_t t);
    void end(); // 热力图价升序 + 拼接下标 → Y 视野 → 自动阈值

    void clear();

  private:
    void build_view();
    // 自动阈值: 合并矩形按持续秒加权的 log10(净额) 上分位 (依赖 build_view 的初始 Y 视野)
    void build_auto_threshold();
  };

  Board<Sec> sec;

  // ==========================================================================
  // Pick (资产筛选) — 锚点日的逐日 PIT 状态 (worker) + 筛选/候选 (GUI)
  //   状态源 = all 池承接日 (日轴前一日) L_1day 行 (Fund 节点): risk_warn / list_age / delist_age
  //   / industry_l1 —— 承接格无源 (轴首 / 不在 all 池) 读不到状态, 不进候选; 当日有无分钟不论 (停牌照列).
  //   候选轴 = 主池轴 (GUI 按选中列定: 无选中 = all, 单池 = 该池)
  // ==========================================================================
  struct Pick {
    // 单资产当日状态 (worker 写背槽, 发布后只读)
    struct Meta {
      uint8_t industry_l1 = 0; // SW2021 一级行业 ID (0 = 未知), 见 fund::SW2021_L1_NAMES
      uint8_t risk_warn = 0;   // 0=正常 1=ST 2=*ST 3=退市整理期
      bool has_data = false;   // 状态可判读 (承接日 L_1day 行有源; false = 其余字段无意义)
      bool listed = false;     // 当日在市 (已上市 且 未退市)
      bool delisted = false;   // 当日已退市
    };

    // ---- 逐日状态槽 (双槽 ping-pong, 对仗 Sec) ----
    struct Slot {
      uint32_t gen = 0;
      misc::Ymd date = 0;
      std::vector<Meta> meta; // [A] 与 AssetAxis 同序 (asset_idx 直接下标)
      void clear();
    };
    Board<Slot> slots;

    // ---- 筛选条件 (GUI; 空集 = 该维不筛, 对仗 DATABASE/TABLE) ----
    std::set<int> st_filter;        // risk_warn 取值 0..3
    std::set<int> listed_filter{0}; // 0=在市 1=退市 (默认在市)
    std::set<int> board_filter;     // GUI::Database::BoardType 的底层值
    std::set<int> industry_filter;  // SW2021 一级行业 ID

    // ---- 候选列表 (GUI 派生缓存: 过滤 + 排序) ----
    std::vector<size_t> candidates;    // 通过筛选的 asset_idx (市场 → 代码序)
    std::vector<size_t> display_order; // 全部 asset_idx 按 市场 → 代码 排序 (静态, 建一次)
    uint64_t filter_epoch = 0;         // 筛选条件版本 (改一次 +1)
    Memo<uint32_t, uint64_t> key;      // (槽 gen, 筛选版本)

    // GUI: 单资产是否通过筛选 (has_data 是前置条件)
    bool passes(const Meta &m, int board) const;
    // GUI: display_order 建一次 (市场 → 代码序); codes/exchanges 与 AssetAxis 同序
    void build_display_order(const std::vector<std::pair<std::string, std::string>> &exch_code);
    // GUI: 重建候选 (front 槽 + 筛选条件); boards[asset_idx] 由调用方按代码推出
    void rebuild_candidates(const Slot &s, const std::vector<int> &boards);

    void clear();
  } pick;

  // ==========================================================================
  // GUI 线程私有渲染缓存 (四块同形: 产物 + Memo 键 + build / clear; 调用方 key.stale(...) 即 build)
  // ==========================================================================

  // HeatColor — front Sec 槽 + 阈值 → 每矩形颜色 (与 Sec::Heat 矩形拼接序 1:1, 下标 = Level::off + k; 0 = 透明)
  struct HeatColor {
    std::vector<uint32_t> color;
    Memo<uint32_t, float> key; // (Sec 槽 gen, log 阈值)
    void build(const Sec &src, float log_threshold);
    void clear();
  } heat_color;

  // Profile — 锚点秒的全簿截面 → 纵向深度图 (图1右)
  //   数据源 = 热力图合并矩形 (全簿, 不受 30 价位限制): 每价位二分找覆盖锚点秒的矩形.
  //   竞价交叉簿三态: 纯买区 (< edge[Ask]) / 纯卖区 (> edge[Bid]) / 重合区 [edge[Ask], edge[Bid]]
  struct Profile {
    struct Curve {
      std::vector<double> x, y; // x = 累计额 (万元), y = 价
    };
    std::vector<double> price, amount; // 逐价位净额 (价升序; amount SIGNED 元: + 买 − 卖)
    PerSide<Curve> cum;                // 累计曲线: 自内沿向外 (Bid 价降序 / Ask 价升序), 各自独立点集
    PerSide<double> edge{};            // 内沿价: 最高买价位 / 最低卖价位 (交叉: edge[Ask] < edge[Bid]; 0 = 该侧空)
    double cum_max = 0;                // 两侧累计额最大值 (万元, X 轴范围)
    Memo<uint32_t, size_t> key;        // (Sec 槽 gen, 锚点秒)
    void build(const Sec &src, size_t t);
    void clear();
  } profile;

  // EventMarks — 选中资产的历史事件 (AssetInfo::events) → 图2 日轴 X, 按组分行
  //   日 → 日轴: 单点落在 d0 起首个日轴日 (非交易日公告顺延), 早于轴首 / 晚于轴末丢; 区间裁到日轴, 与日轴无交丢
  struct EventMarks {
    struct Mark {
      double x0, x1; // [首日起点, 末日终点) (图2 X)
      uint32_t ev;   // events 下标
    };
    const std::vector<AssetEvent> *events = nullptr;  // 选中资产的事件 (AssetInfo 内; seq 配对期间稳定)
    std::array<std::vector<Mark>, kEventGroups> lane; // 按组, 各行 x0 升序
    Memo<uint32_t, uint64_t> key;                     // (Min 请求代, AssetEvents::seq): 换资产 / 重扫日轴 / 基本面重建都失效
    // dates = Min 日轴 (gen 配对期间稳定), evs = 选中资产的事件 (nullptr = 无)
    void build(const std::vector<misc::Ymd> &dates, const std::vector<AssetEvent> *evs);
    void clear();
  } event_marks;

  // MissingDays — 已发布前缀中该资产无 K 线的 day_idx → 图2 淡黄竖条 (只在单池日轴画: 多池叠加 = 并集日轴, 不画)
  struct MissingDays {
    std::vector<size_t> days;
    Memo<uint32_t, size_t> key; // (Min 请求代, 已发布日数): 流式追加 / 换资产 / 重扫日期 都失效
    void build(const Min &src, size_t pub_days, size_t pub_points);
    void clear();
  } missing_days;

  // ==========================================================================
  // 三路期望态键 (GUI 快照, 服务按键发请求; 键变 → 换代)
  //   feats = 同一组选中列各截到图的网格 (图2 ≥ L_1min / 图1 ≥ L_sec; 截断 MAX_FEATURES)
  //   main  = 主池 (选中列所在的唯一池, 无选中 = all): 图1 VWAP / 候选轴
  // ==========================================================================
  struct MinKey {
    size_t asset = SIZE_MAX;
    std::vector<ColRef> feats;
    PoolMask pools = 0; // 日轴池集 (all + overlay 各池); 变了 → 重扫日轴
    bool operator==(const MinKey &) const = default;
  };
  struct SecKey {
    PoolId main = kAllPool;
    misc::Ymd date = 0;
    size_t asset = SIZE_MAX;
    std::vector<ColRef> feats;
    bool operator==(const SecKey &) const = default;
  };
  struct PickKey {
    PoolId main = kAllPool;
    misc::Ymd date = 0;
    bool operator==(const PickKey &) const = default;
  };

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
    unsigned event_groups = (1u << kEventGroups) - 1; // 图2 显示的事件组 (位 = EventGroup)
    // log10(amount) 阈值, 区间 [HEATMAP_LOG_THR_MIN, MAX]; 换槽时取该槽 auto_log_threshold
    float log_amount_threshold = OrderFlowConst::HEATMAP_LOG_THR_DEFAULT;

    // 三路期望态 (键变 → gen++ → Request)
    Want<MinKey> min;
    Want<SecKey> sec;
    Want<PickKey> pick;

    // 图2 X 域 (= 特征日数 × MIN_CAPACITY): GUI 自持副本, 换代在途时照画空图不撤轴;
    // 只在域本身变了 (重扫日期) 才复位视野 —— 换资产不动 X
    double min_x_max = 0.0, min_x_applied = 0.0;
    bool min_rescan_pending = false; // 请求带 rescan (dates 正在重扫): 期间 dates 不可读

    // 图2 Y1 = 可见 X 区间内 K 线贴合 (RangeFit): 发布变了 / X 视野变了 → 重贴; 只动 Y 轴不重贴 (手动 Y 缩放保留)
    size_t min_last_pub_days = SIZE_MAX;               // 上次贴合 Y1 时的已发布日数
    bool min_fit_y = false;                            // 上帧 X 视野变了 → 本帧贴合 Y1
    double min_view_x_min = 0.0, min_view_x_max = 0.0; // 上帧 X 视野
    uint32_t sec_last_gen = UINT32_MAX;                // 上次 L_sec 视图重置时的槽 gen
    double sec_y_min = 0.0, sec_y_max = 0.0;           // 图1 当前 Y 视野 (每帧快照; 右侧深度面板同步)

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
