#pragma once

// =============================================================================
// 观察 (view_*) 的结果: 每个工具一个 Res; 计算与原文在 tools/look/<工具>.cpp (计算核 tools/look/Kernel.hpp), 展示在 ui/look/<工具>.cpp
// =============================================================================
//   全部后台 (LabFn): 读台面 Bay 的观察口径, 在本件的计算流上分块流式全核归约; view_book 逐笔重放进 worker 线程常驻的订单簿
//   收益一律是超额 (毛 − 本行在池均值); 分位 / 组号已定向 (高 = 做多端 G20); period = 观察的日段与评估期不同时的提示 (同则空)
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <array>
#include <cmath>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Engine {
struct FactorFlow;
}

namespace GUI::Agent {

struct ViewEnv;

inline constexpr int kGroupN = 20; // 分层组数
inline constexpr const char *kBookKinds[] = {"新挂", "撤单", "被吃完", "扫单"};

// 一行分层 (分时段 / 分月 / 条件段 / 剥离对照同一口径)
struct LayerRow {
  std::string name;
  int rows = 0, ok = 0;
  double mono = NAN, ls = NAN, ric = NAN;
  std::vector<double> grp; // [kGroupN] 每期超额 G1 → G20
};
void DrawLayerTable(const char *id, const std::vector<LayerRow> &rows); // ui/look/ViewLayers.cpp

struct FeatureRes : Res {
  struct Feat {
    std::string raw, err;  // err = 没算成的原因
    size_t in = 0, ok = 0; // 池内格 / 有效格 (不到 100 只报个数)
    double mean = NAN, sd = NAN;
    std::array<double, 7> q{}; // 分位 1/5/25/50/75/95/99%
    size_t levels = 0;         // 抽样里的取值种数 (≤ 20 才记)
    double zero = 0.0;         // 零值占比
    bool var = false;          // 方差可分解 (总方差 > 0)
    double v_asset = NAN, v_day = NAN, v_intra = NAN;
    std::array<double, 3> lag{}; // 三档滞后的同一资产自相关 (名见 lags)
    double day1 = NAN;           // 隔 1 日自相关
    bool slow = false;           // 日内方差太小 (日频慢变量), 不列日内轮廓
    std::vector<double> prof;    // 日内层: 日内轮廓 [seg]; 日层: 截面中位数逐日
    double ric = NAN, ric_t = NAN, ric_up = NAN;
    std::vector<double> ric_part; // 分时段 rIC (日内层)
    double h_lo = NAN, h_hi = NAN;
    std::vector<double> hist; // 1~99% 分位间等宽分箱的抽样频数
  };
  std::string period;
  int seg = 1;
  std::vector<std::string> clock, parts; // 本层行末时刻 / 时段名
  std::vector<int> part_end;             // 各时段末行
  std::vector<std::string> lags;
  std::vector<Feat> feats;
  std::vector<size_t> good; // 有统计的式子
  std::vector<double> corr; // [good × good] 两两截面秩相关
  std::string text() const override;
  std::string gist() const override;
  void draw() const override;
};

struct LayersRes : Res {
  std::string period, label, split, cond;
  int seg = 1;
  LayerRow all;
  double ric_t = NAN, ric_up = NAN;
  std::vector<double> held; // [kGroupN] 各组行均真持有只数
  struct Tail {
    double frac = 0.0; // 占组比例
    double top = NAN, top_n = NAN, bot = NAN, bot_n = NAN;
  };
  std::vector<Tail> tails;
  std::vector<LayerRow> splits; // 切分的各段 (tod / month)
  std::string cond_err;         // 条件没算成
  bool has_cond = false;        // 条件三段有可用的行
  double cond_rho = NAN, cond_ric = NAN;
  std::vector<LayerRow> conds; // 全体 (同口径) / 低 / 中 / 高
  std::string text() const override;
  std::string gist() const override;
  void draw() const override;
};

struct TradesRes : Res {
  std::string period, label, side, merge, sort, win; // win = " · 只看 … 的行 (N 行)" 或空
  int seg = 1, win_rows = 0;
  size_t cells = 0, segs = 0;
  double cell_mean = NAN, cell_up = NAN, seg_mean = NAN, seg_med = NAN, seg_p10 = NAN, seg_p90 = NAN, seg_up = NAN, worst = NAN, best = NAN;
  double h_lo = NAN, h_hi = NAN;
  std::vector<double> hist; // 段超额 p1~p99 分箱的段数
  struct Part {
    std::string name;
    size_t n = 0;
    double share = NAN, mean = NAN, up = NAN;
  };
  std::vector<Part> parts;
  struct Tag {
    std::string name;
    double held = NAN, base = NAN, with = NAN, without = NAN;
  };
  std::vector<Tag> tags;
  struct Seg {
    std::string code, date, time, tags;
    int rows = 0;
    double rank = NAN, ex = NAN, chg = NAN, pos = NAN;
  };
  std::vector<Seg> list;
  std::string text() const override;
  void draw() const override;
};

struct HoldersRes : Res {
  std::string period, label, side;
  size_t total = 0, any = 0, half = 0;
  int active = 0;
  double top10 = NAN;
  struct Row {
    std::string code;
    int side = 0, in = 0, days = 0;
    double share = NAN, run = NAN;
    size_t held = 0;
    double ex = NAN;
  };
  std::vector<Row> rows;
  std::string text() const override;
  void draw() const override;
};

struct ResponseRes : Res {
  std::string period, label, side;
  size_t events = 0, kept = 0;
  struct Pt {
    std::string name;
    bool zero = false;
    size_t n = 0;
    double ex = NAN, se = NAN, up = NAN, rank = NAN;
  };
  std::vector<Pt> pts;
  std::vector<std::pair<std::string, double>> lags; // 开仓行因子分位与此前收益的截面秩相关
  bool tod = false;
  struct Part {
    std::string name;
    size_t n = 0;
    std::vector<double> ex; // [pts]
  };
  std::vector<Part> parts;
  std::string text() const override;
  void draw() const override;
};

struct ExplainRes : Res {
  std::string period, label;
  std::vector<std::string> exprs; // E 行 (没算成的写原因)
  size_t rows = 0;
  double fric = NAN, r2 = NAN, rric = NAN;
  struct Base {
    std::string name;
    double corr = NAN, ric = NAN, rest = NAN;
  };
  std::vector<Base> bases;
  LayerRow orig, rest;
  std::string head() const; // 标题 + E 行
  std::string text() const override;
  std::string gist() const override;
  void draw() const override;
};

struct SnapshotRes : Res {
  std::string period, label, when, level;
  int seg = 1;
  size_t n = 0;
  bool row_ok = false;
  double ric = NAN, g20 = NAN, g1 = NAN;
  int c20 = 0, c1 = 0;
  std::vector<std::string> exprs;
  std::vector<std::pair<std::string, double>> corr; // 与因子分位的截面秩相关
  struct Cell {
    std::string end, code, tags;
    bool mid = false; // 池中位那行
    double rank = NAN;
    int grp = -1, held = 0;
    double chg = NAN, step = NAN, pos = NAN, amt = NAN, ex = NAN;
    std::vector<double> fv;
  };
  std::vector<Cell> cells; // 高 n 只 + 池中位 + 低 n 只
  std::string text() const override;
  void draw() const override;
};

struct StockDayRes : Res {
  std::string period, code, date, level;
  int seg = 1;
  std::vector<std::string> clock;
  double pre = NAN, hi = NAN, lo = NAN, last = NAN, lim_up = NAN, lim_dn = NAN;
  int first = -1, in = 0;
  double amt_sum = 0.0;
  size_t amt_n = 0;
  std::vector<std::string> exprs;
  bool has_fac = false;
  std::string fac;
  std::vector<double> close, high, low, vol, amt, ex, rank; // [seg]
  std::vector<int> grp;
  std::vector<std::vector<double>> fv; // [式子][seg]
  struct Part {
    std::string name;
    double chg = NAN, hi = NAN, lo = NAN, vol = NAN, rank = NAN, ex = NAN;
    int up = 0, dn = 0;
    std::vector<double> end; // 各式子段末值
  };
  std::vector<Part> parts;
  std::string rows; // 逐行时段 (空 = 不附逐行表)
  int r0 = 0, r1 = 0;
  std::string text() const override;
  void draw() const override;
};

struct StockDaysRes : Res {
  std::string period, code, level;
  std::vector<std::string> exprs;
  bool has_fac = false;
  std::string fac;
  struct Day {
    std::string date;
    double close = NAN, chg = NAN, amp = NAN, amt = NAN;
    int in = 0, up = 0, dn = 0;
    double rank = NAN, rank_close = NAN, ex = NAN;
    std::vector<double> fv;
  };
  std::vector<Day> days;
  size_t n_feats = 0;
  std::string text() const override;
  void draw() const override;
};

struct BookRes : Res {
  std::string head;
  size_t orders = 0;
  bool traded = false;
  double open = 0, high = 0, low = 0, close = 0, amt = 0;
  double big = 0;
  bool big_given = false;
  double taker[2]{}, maker[2]{}, cancel[2]{};
  double first = 0, last = 0, spread = NAN;
  int from = 0;                   // 窗口起 (日内秒)
  std::vector<double> price, net; // 逐秒成交价 (前值延续) / 累计主动买 − 主动卖 (万)
  int step = 1;
  bool call = false; // 窗口含集合竞价段
  struct Row {
    int clock = 0;
    double last = NAN, bid = 0, ask = 0;
    double taker[2]{}, maker[2]{}, cancel[2]{}, depth[2]{};
    double ref = 0, matched = 0, imbalance = 0;
  };
  std::vector<Row> rows;
  int count[4][2]{};
  double amount[4][2]{}, levels[2]{};
  double life[2] = {NAN, NAN}; // 撤单寿命中位 (买 / 卖)
  size_t events = 0;
  struct Event {
    int clock = 0, cs = 0, kind = 0, side = 0, levels = 0;
    double price = 0, first = 0, amount = 0, life = -1, best = 0; // best = 上一秒末同侧最优价
  };
  std::vector<Event> top; // 额最大的若干起 (按时序)
  int at = 0;
  double book_bid = 0, book_ask = 0;
  struct Level {
    bool bid = false;
    int rank = 0;
    double price = 0, amount = 0;
  };
  std::vector<Level> profile; // 卖 (远 → 近) 再买 (近 → 远)
  std::string text() const override;
  void draw() const override;
};

ToolOut ViewFeature(const Call &c, Bay &bay);
ToolOut ViewLayers(const Call &c, Bay &bay);
ToolOut ViewTrades(const Call &c, Bay &bay);
ToolOut ViewHolders(const Call &c, Bay &bay);
ToolOut ViewResponse(const Call &c, Bay &bay);
ToolOut ViewExplain(const Call &c, Bay &bay);
ToolOut ViewSnapshot(const Call &c, Bay &bay);
ToolOut ViewStockDay(const Call &c, Bay &bay);
ToolOut ViewStockDays(const Call &c, Bay &bay);
ToolOut ViewBook(const Call &c, Bay &bay);

nlohmann::ordered_json SampleViewFeature(Feed &p);
nlohmann::ordered_json SampleViewLayers(Feed &p);
nlohmann::ordered_json SampleViewTrades(Feed &p);
nlohmann::ordered_json SampleViewHolders(Feed &p);
nlohmann::ordered_json SampleViewResponse(Feed &p);
nlohmann::ordered_json SampleViewExplain(Feed &p);
nlohmann::ordered_json SampleViewSnapshot(Feed &p);
nlohmann::ordered_json SampleViewStockDay(Feed &p);
nlohmann::ordered_json SampleViewStockDays(Feed &p);
nlohmann::ordered_json SampleViewBook(Feed &p);

} // namespace GUI::Agent
