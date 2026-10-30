// PortfolioService — Factors→Portfolio 页的单 worker: 因子组合 (一组合一文件) 里每个因子**独立**回测 + 各池等权指数.
//
// 组合文件 <config.portfolio_dir>/<name>.json (name = 主干, ValidFactorName):
//   { "factors": [ { "pool": "all", "level": "5min", "file": "mom30.json", "hold": 30, "sign": -1, "expr": "<具体规范串>" }, … ] }
//   条目 = 权重粒度: (池, 因子层, 因子文件, 具体规范串, 方向) 定位 Factors 表里的一个权重 (FactorRow::expr / sign; sign 缺 = +1); hold = 加入时 Factors 页的 Hold.
//   因子文件里找不到该规范串 + 方向 (被 Search 重写 / 删了) → 条目失效 (UI 红, 不算). 组合内可混池混层.
//
// 逐因子独立 (各用自己的层 / 池 / 评估期, 不统一网格): 与 Run / Inspect 同一条管线 (FactorsEngine eval_job: 装载 → 森林 → 探针).
//   评估期随标签库 (FactorScope::pool_span = false): Lob 池的 Bar 层因子只读分钟线, 与 all 池同期 (Bar 库首日起), 不限于池库落盘日.
//   同 (层, 池) 的待算条目一组一张森林 (同 Y = 各根 hd 之并, 公共子式只算一次): 根 = 去重的 (具体规范串, 口径, 方向), 根的 hd = 其在组内的全部 hold;
//   落线探针 CurveP (accum_rows 一级 Row 的 G20 绝对收益 + cost_rows 逐行换手, 与 Run 同口径) → 净值引擎 fold_book (FactorsBook; Stat 契约【净值】:
//   G20 多头, 每批 1/h̄, 平仓行实现; 可成交 / 顺延在标签里) → FactorCurve 逐行出参线.
//   成本按 Δw (契约【换手成本】), 取该层首个金额段: 盘口段 = 增仓 Δw⁺ × 买入冲击 (封顶 max_impact) + 平仓量 × Config::fixed_impact; 固定段 =
//   增仓量 / 平仓量各付 fixed_impact (固定量 UI 乘). 净值 / 聚合 (等权, 日并集轴) 在 UI 现算.
// 池篮子: 全部池一个作业 (FactorsEngine eval_baskets, 与 eval_job 平级), 在 kBasketLevel 的 all 池评估域 (全轴; 不走森林): load_b 一趟读齐各池成员位 (池列) + T+1C 标签 + 原料 → baskets 一趟扫完
//   (FactorsBook) → 按池切进 PoolCurve: 各池整体 + 申万一级行业 (all 内切) 两权重指数日收益 (净值引擎 fold_book) + 两权重估值 EP / BP / SP / CP / DY
//   (等权 = 个股收益率均值, 加权 = 整体法收益率; 滚动分位 band 在 UI 按可调窗长现算) + 池整体年化单边换手.
// 缓存: 因子按 FactorTask::key 存, 曲线记算时的 sig (变了重算), 请求里没有的丢掉 (加一个因子只算它); 池篮子整份存, reload 才重算.
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑 (缓存已算的不丢); UI 持 mutex 读 result
// (shared_ptr 拷出即放锁). 进度: Loading 期 done/total = 天; Running 期 = 算子节点 + 收尾; 第几个因子走 jobs_done / jobs_total.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsBook.hpp"   // BookLines / Baskets::index_line / compound
#include "gui/task_factors/services/FactorsTable.hpp"  // FactorScope / FactorRef / kWeights
#include "gui/task_factors/services/FactorsWorker.hpp" // FactorWorker

#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Factors {

// ---- 组合文件 (GUI 线程读写) ----
struct PortfolioEntry {
  std::string pool;
  LevelId lv = 0;   // 因子层 (文件里存层名)
  std::string file; // 因子文件名 (含 .json)
  std::string expr; // 具体规范串 (FactorRow::expr)
  int hold = 0;     // 持有期键
  int sign = 1;     // 方向 (FactorRow::sign)
  std::string key() const {
    return pool + "|" + factor_level(lv).name + "|" + file + "|" + expr + "|" + std::to_string(sign) + "|" + std::to_string(hold);
  }
};
std::vector<std::string> ListPortfolios(const std::string &dir);                                            // 主干名, 字典序; 目录不存在 = 空
std::vector<PortfolioEntry> LoadPortfolio(const std::string &dir, const std::string &name);                 // 格式不对 → assert (只本页写)
void SavePortfolio(const std::string &dir, const std::string &name, const std::vector<PortfolioEntry> &es); // tmp + rename
bool RenamePortfolio(const std::string &dir, const std::string &from, const std::string &to);               // to 已存在 → false
void DeletePortfolio(const std::string &dir, const std::string &name);

// ---- 解析 (GUI 线程): 条目 → 该 (池, 层) 目录下 ScanFactorFile 找回的权重行 (字段表取全页共享的 FeatureTables) ----
struct PortfolioItem {
  PortfolioEntry e;
  std::string name_en, name_cn;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  std::string missing; // 非空 = 失效原因 (文件 / 规范串找不到, 权重 BROKEN, 持有期不在该层, 标签库未建), 不算
};
void ResolvePortfolio(const SharedData &data, FeatureTables &tables, const std::vector<PortfolioEntry> &es, std::vector<PortfolioItem> &out);

// ---- 线的轴: 组合线 (G20 多头, 扣其换手成本) = 冲击 Impact; 池线 = 权重 Weight (FactorsTable.hpp; Service 出毛序列, UI 组线) ----
enum Impact : int { kImpactNet,     // 有冲击
                    kImpactGross }; // 无冲击
inline constexpr int kImpacts = 2;

// ---- 请求 ----
struct FactorTask {  // 一个有效条目 = 一个因子 (FactorRef, 与 Inspect 请求同一份) + 持有期
  std::string key;   // PortfolioEntry::key
  FactorScope scope; // 条目的 (池, 层)
  FactorRef f;
  int hold = 0;
  std::string sig() const { return key + "|" + std::to_string(scope.max_impact); } // 买入冲击封顶变了重算
};
inline constexpr LevelId kBasketLevel = level_of(L_1day); // 池篮子的层 (PoolCurve 按日)
struct PoolTask {                                         // 全部池的篮子 (一个作业)
  FactorScope scope;                                      // (all, kBasketLevel)
};
struct PortfolioRequest {
  std::vector<FactorTask> factors;
  std::optional<PoolTask> pools; // 有因子才带 (all 标签库已建)
  bool reload = false;           // 弃缓存全部重算 (特征库重算过 / 手动 Recompute)
  std::string sig() const;       // 内容签名 (不含 reload): 没变就不发请求, 不打断在跑的
};
// 资产轴须已就绪 (断言). 作用域建不成 (标签库未建) 的条目 → items[i].missing 置上, 不进请求. gpu = 因子线的后端 (池篮子无森林, 恒宿主)
void MakePortfolioRequest(const SharedData &data, FeatureTables &tables, std::vector<PortfolioItem> &items, bool reload, bool gpu, PortfolioRequest &req);

// ---- 结果 ----
// 一个因子 (该 hold) 的 G20 多头出参线 (fold_book stride 1 = 逐行; 线序 = CurveLine, 与 InspectResult::nav 同形):
// 增量占净值; 收益记在平仓行 (hold_exit_row), 成本记在建仓行
enum CurveLine : int { kCurvePnl, // 毛收益
                       kCurveImp, // 建仓冲击
                       kCurveFix, // 固定冲击份数 (UI × Config::fixed_impact); 净 = pnl − imp − fix · fixed_impact
                       kCurveBuy, // 增仓量 Δw⁺ (建仓行; UI 折年化单边换手, 同 PoolCurve::turn)
                       kCurveLines };
struct FactorCurve {
  std::string sig;              // 算时的 FactorTask::sig
  std::vector<misc::Ymd> dates; // [days]
  LevelId lv = 0;
  int hold = 0;
  int amt = 0;       // 金额段 = 该层首段 (amt_name; kAmtFixed = 固定段, 增仓量也付 fixed_impact)
  BookLines nav;     // [kCurveLines][T = days · seg]
  std::string error; // 非空 = 没算成 (作用域无数据 / 叶值域越界), 其余空
  const float *nav_at(int line) const { return nav.at(line); }
};
// 一个池的篮子 (从 Baskets 按池切出): 篮子 0 = 池整体; all 池另有 k = 1..Baskets::kAllBaskets−1 = SW2021 一级行业 (全A 内切)
struct PoolCurve {
  std::vector<misc::Ymd> dates;                // [nd] 评估期
  BookLines index;                             // [nk · kWeights][nd] 篮子指数的日收益 (nk = baskets(); 线序 Baskets::index_line; UI compound 成点位)
  BookLines index_px;                          // 同形, 收盘价指数 (Baskets::index_px, 不看可成交)
  std::vector<float> val[kWeights][kPoolVals]; // [Weight][PoolVal][nk · nd] 篮子估值 EP / BP / SP / CP / DY (等权 = 个股收益率均值 / 加权 = 整体法收益率; NaN = 当日不可算)
  double turn[kWeights] = {};                  // [Weight] 篮子 0 年化单边换手 (倍; Baskets::turn × 每年行数; 再平衡 + 成分进出, 停牌 = 移出再移回)
  std::string error;
  size_t baskets() const { return static_cast<size_t>(index.n / kWeights); } // all 池 = Baskets::kAllBaskets, 其余池 = 1
  const float *index_of(size_t k, int w, bool px) const {                    // 篮子 k 的日收益 [nd]
    assert(k < baskets());
    return (px ? index_px : index).at(Baskets::index_line(k, w));
  }
  const float *val_of(size_t k, int w, int m) const {
    assert(k < baskets());
    return val[w][m].data() + k * dates.size();
  }
};
struct PortfolioResult {
  std::map<std::string, std::shared_ptr<const FactorCurve>> factors; // FactorTask::key →
  std::map<std::string, std::shared_ptr<const PoolCurve>> pools;     // 池名 → (全部池同一趟算出)
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
  std::atomic<int> jobs_done{0}, jobs_total{0}; // 本轮要算的 (因子 + 池篮子一个)

private:
  static constexpr Phase kStart = Phase::Loading;
  bool run(PortfolioRequest &req);
  // 一张森林的待算条目 (同层同池, 按 (串, 框架) 排好, 同串必同框架): 根评完逐条发布 / 池篮子: 全部池一次发布. false = 取消 / 判废
  bool eval(const std::vector<const FactorTask *> &ts, FactorFlow &F);
  bool eval(const PoolTask &t, FactorFlow &F);
  void put(const std::string &key, std::shared_ptr<FactorCurve> c);
};

} // namespace GUI::Factors
