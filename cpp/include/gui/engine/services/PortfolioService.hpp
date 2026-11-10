// PortfolioService — Market 各页 (NAV / VAL / FLOW / CTA) 的单 worker: 因子组合 (一组合一文件) 里每个因子**独立**回测 + 全部篮子的可成交指数 / 增仓 / 估值.
//   两者同一种成品 = 账 PortfolioBook (线 × 行的逐行增量, 线键取线, 列 = 毛收益 / 建仓冲击 / 固定份数 / 增仓量, 另带估值):
//   因子一本一线; 行情两侧 (Quote::Side) 各一本: 篮子一本 basket_count 线 (不扣成本 = 无冲击两列), 个股一本每只一线 (只毛收益一列).
//
// 组合文件 output/portfolios/<name>.json (name = 主干, ValidFactorName):
//   { "factors": [ { "pool": "all", "level": "5min", "file": "mom30.json", "hold": 30, "sign": -1, "expr": "<具体规范串>" }, … ] }
//   条目 = 权重粒度: (池, 因子层, 因子文件, 具体规范串, 方向) 定位 Factors 表里的一个权重 (FactorRow::expr / sign; sign 缺 = +1); hold = 加入时 Factors 页的 Hold.
//   因子文件里找不到该规范串 + 方向 (被 Search 重写 / 删了) → 条目失效 (UI 红, 不算). 组合内可混池混层.
//
// 逐因子独立 (各用自己的层 / 池 / 评估期, 不统一网格): 与 Run / Inspect 同一条管线 (gui/engine/Engine eval_job: 装载 → 森林 → 探针).
//   同 (层, 池) 的待算条目一组一张森林 (同 Y = 各根 hd 之并, 公共子式只算一次): 根 = 去重的 (具体规范串, 口径, 方向), 根的 hd = 其在组内的全部 hold;
//   落账探针 BookP (accum_rows 一级 Row 的 G20 绝对收益 + cost_rows 逐行换手, 与 Run 同口径) → 净值引擎 fold_book (gui/engine/Book; Stat 契约【净值】:
//   G20 多头, 每批 1/h̄, 平仓行实现; 可成交 / 顺延在标签里) → 一线的账.
//   成本按 Δw (契约【换手成本】), 取主段 (y_spec Segs::First: 首个盘口段, 无则固定段): 盘口段 = 增仓 Δw⁺ × 买入冲击 (封顶 max_impact) + 平仓量 × Config::fixed_impact; 固定段 =
//   增仓量 / 平仓量各付 fixed_impact (固定量 UI 乘). 净值 / 聚合 (等权, 日并集轴) / 年化换手在 UI 现算.
// 篮子: 全部篮子一个作业 (gui/engine/Engine eval_baskets, 与 eval_job 平级), 在 kBasketLevel 的 all 池评估域 (全轴; 不走森林): load_b 一趟读齐各池成员位 (池列) + T+1C 标签 + 原料 → baskets 一趟扫完
//   (gui/engine/Book) → 一本 basket_count 线的账 (线 = basket_line, 与行情侧价格指数同一张篮子表): 两权重可成交指数日收益 (净值引擎 fold_book) + 逐日增仓量
//   + 估值 EP / BP / SP / CP / DY (等权 = 个股收益率均值, 加权 = 整体法收益率; 滚动分位 band 在 UI 按可调窗长现算);
//   同一趟另出一本个股账 (线 = all 池轴的每只: 后复权日收益 + 本只收益率); 两本按 Side 并列 (PortfolioResult::quotes), UI 的两侧同一套.
// 缓存: 因子按 FactorTask::key 存, 账记算时的 sig (变了重算), 只算缺的 (加一个因子只算它); 请求外的 (换组合 / 删条目) 按上次被请求的先后留到总量上限
//   (切回不重算); 篮子整本存, reload 才重算.
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑 (缓存已算的不丢); UI 持 mutex 读 result
// (shared_ptr 拷出即放锁). 进度: Loading 期 done/total = 天; Running 期 = 算子节点 + 收尾; 第几个因子走 jobs_done / jobs_total.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/engine/Book.hpp"   // BookLines / compound
#include "gui/engine/Table.hpp"  // FactorScope / FactorRef / kPoolVals
#include "gui/engine/Worker.hpp" // FactorWorker
#include "gui/quote/Quote.hpp"   // PerSide (行情两侧的账)

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Engine {

// 组合的账走因子引擎 (gui/engine: 字段表 / 作用域 / 净值引擎同一份)

// ---- 组合文件 (GUI 线程读写) ----
struct PortfolioEntry {
  std::string pool;
  FactorLevelId lv = 0; // 因子层 (文件里存层名)
  std::string file;     // 因子文件名 (含 .json)
  std::string expr;     // 具体规范串 (FactorRow::expr)
  int hold = 0;         // 持有期键
  int sign = 1;         // 方向 (FactorRow::sign)
  std::string key() const {
    return pool + "|" + factor_level(lv).name + "|" + file + "|" + expr + "|" + std::to_string(sign) + "|" + std::to_string(hold);
  }
};
std::vector<std::string> ListPortfolios();                                          // 主干名, 字典序
std::vector<PortfolioEntry> LoadPortfolio(const std::string &name);                 // 格式不对 → assert (只本页写)
void SavePortfolio(const std::string &name, const std::vector<PortfolioEntry> &es); // 原子替换
bool RenamePortfolio(const std::string &from, const std::string &to);               // to 已存在 → false
void DeletePortfolio(const std::string &name);

// ---- 解析 (GUI 线程): 条目 → 该 (池, 层) 目录下 ScanFactorFile 找回的权重行 (字段表取全页共享的 FeatureTables) ----
struct PortfolioItem {
  PortfolioEntry e;
  std::string name_en, name_cn;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  std::string missing; // 非空 = 失效原因 (文件 / 规范串找不到, 权重 BROKEN, 持有期不在该层, 标签库未建), 不算
};
void ResolvePortfolio(const SharedData &data, FeatureTables &tables, const std::vector<PortfolioEntry> &es, std::vector<PortfolioItem> &out);

// ---- 请求 ----
struct FactorTask {  // 一个有效条目 = 一个因子 (FactorRef, 与 Inspect 请求同一份) + 持有期
  std::string key;   // PortfolioEntry::key
  FactorScope scope; // 条目的 (池, 层)
  FactorRef f;
  int hold = 0;
  std::string sig() const { return key + "|" + std::to_string(scope.max_impact); } // 买入冲击封顶变了重算
};
inline constexpr FactorLevelId kBasketLevel = factor_level_of(L_1day); // 篮子的层 (篮子账按日)
struct BasketTask {                                                    // 全部篮子 (一个作业)
  FactorScope scope;                                                   // (all, kBasketLevel)
  std::string sig() const { return "baskets"; }
};
struct PortfolioRequest {
  std::vector<FactorTask> factors;
  std::optional<BasketTask> baskets; // all 标签库已建才带 (与有无因子无关: Market 各页的篮子账)
  bool reload = false;               // 弃缓存全部重算 (特征库重算过 / 手动 Recompute)
  std::string sig() const;           // 内容签名 (不含 reload): 没变就不发请求, 不打断在跑的
};
// 资产轴须已就绪 (断言). 作用域建不成 (标签库未建) 的条目 → items[i].missing 置上, 不进请求. gpu = 因子线的后端 (篮子无森林, 恒宿主)
void MakePortfolioRequest(const SharedData &data, FeatureTables &tables, std::vector<PortfolioItem> &items, bool reload, bool gpu, PortfolioRequest &req);

// ---- 结果 ----
// 账的列 (fold_book stride 1 = 逐行; 增量占净值): 收益记在平仓行 (hold_exit_row), 成本 / 增仓记在建仓行
enum BookCol : int { kBookRet,   // 毛收益 (因子 = G20 多头; 篮子 = 可成交指数)
                     kBookImp,   // 建仓冲击 (篮子无此列)
                     kBookFix,   // 固定冲击份数 (UI × Config::fixed_impact; 篮子无此列); 净 = ret − imp − fix · fixed_impact
                     kBookBuy }; // 增仓量 Δw⁺ (UI 折年化单边换手)
inline constexpr int kBookCols = 4;
// 一本账 (因子 / 篮子 / 个股同形): 线 × 行 = [线][T = 日 · S], 线键 keys 升序 (line_of 统一取线): 因子一线 (键 0, 该 hold),
// 篮子 basket_count 线 (键 = basket_line, shared/Basket.hpp), 个股 = all 池轴的每只一线 (键 = 全局 id; 只毛收益一列 + 估值);
// 行情两侧的线键 = 该侧的行情键 (Quote::Side 路的键)
struct PortfolioBook {
  std::string sig;              // 算时的任务签名 (FactorTask / BasketTask::sig)
  std::vector<misc::Ymd> dates; // [D] 评估期
  FactorLevelId lv = 0;         // 层 (每日行数 S)
  int amt = kAmtFixed;          // 冲击段 (amt_name): 因子 = 主段; 篮子不扣成本, 恒 kAmtFixed
  std::vector<uint32_t> keys;   // [线] 线键 (升序)
  BookLines col[kBookCols];     // [BookCol][线][T]; 篮子不扣成本: kBookImp / kBookFix 无列 (n = 0)
  BookLines val[kPoolVals];     // [PoolVal][线][D] 估值 EP / BP / SP / CP / DY (篮子: 等权 = 个股收益率均值 / 加权 = 整体法收益率; 个股: 本只收益率;
                                //   NaN = 当日不可算); 因子无列 (n = 0)
  std::string error;            // 非空 = 没算成 (作用域无数据 / 叶值域越界), 其余空
  size_t lines() const { return keys.size(); }
  size_t line_of(uint32_t key) const { // 线键 → 线; 不在 → lines()
    const auto it = std::lower_bound(keys.begin(), keys.end(), key);
    return it != keys.end() && *it == key ? static_cast<size_t>(it - keys.begin()) : lines();
  }
  const float *at(int k, size_t line) const { // [T]; 无列 → nullptr
    assert(line < lines());
    return col[k].n ? col[k].at(static_cast<int>(line)) : nullptr;
  }
  const float *val_of(size_t line, int m) const { // [D]
    assert(line < lines() && val[m].n);
    return val[m].at(static_cast<int>(line));
  }
};
struct PortfolioResult {
  std::map<std::string, std::shared_ptr<const PortfolioBook>> factors; // FactorTask::key →
  Quote::PerSide<std::shared_ptr<const PortfolioBook>> quotes;         // [Side] 同一 BasketTask 的两本 (同有同无, 同日轴); 空 = 未算
};

class PortfolioService : public FactorWorker<PortfolioService, PortfolioRequest> {
  friend Service<PortfolioService, PortfolioRequest>;

public:
  static constexpr const char *kName = "PortfolioWorker";
  PortfolioService(); // 两者都在 .cpp: FactorFlow 不完整
  ~PortfolioService();

  // UI 持 mutex 读
  PortfolioResult result;
  uint64_t result_gen = 0;                      // result 每变一次 +1 (UI 派生缓存键)
  std::atomic<int> jobs_done{0}, jobs_total{0}; // 本轮要算的 (因子 + 篮子一个)

private:
  static constexpr Phase kStart = Phase::Loading;
  bool run(PortfolioRequest &req);
  // 一张森林的待算条目 (同层同池, 按 (串, 框架) 排好, 同串必同框架): 根评完逐条发布 / 篮子: 全部一次发布. false = 取消 / 判废
  bool eval(const std::vector<const FactorTask *> &ts, FactorFlow &F);
  bool eval(const BasketTask &t, FactorFlow &F);
  // 发布一本账 (两种同一路): 持锁 set(result) + 代 +1, 计一个作业
  template <class Set>
  void put(Set &&set);

  // worker 私有: 因子缓存的新旧 (key → 上次被请求的 tick_)
  std::map<std::string, uint64_t> used_;
  uint64_t tick_ = 0;
};

} // namespace GUI::Engine