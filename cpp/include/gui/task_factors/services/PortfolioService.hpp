// PortfolioService — Factors→Portfolio 页的单 worker: 因子组合 (一组合一文件) 里每个因子**独立**回测 + 各池等权指数.
//
// 组合文件 <config.portfolio_dir>/<name>.json (name = 主干, ValidFactorName):
//   { "factors": [ { "pool": "all", "level": "5min", "file": "mom30.json", "expr": "<具体规范串, 已含 sign>", "hold": 30 }, … ] }
//   条目 = 权重粒度: (池, 因子层, 因子文件, 具体规范串) 定位 Factors 表里的一个权重 (FactorRow::expr); hold = 加入时 Factors 页的 Hold.
//   因子文件里找不到该规范串 (被 Search 重写 / 删了) → 条目失效 (UI 红, 不算). 组合内可混池混层.
//
// 逐因子独立 (各用自己的层 / 池 / 评估期, 不统一网格): 与 Run / Inspect 同一条管线 (FactorsEngine eval_job: 装载 → 森林 → 探针), 探针只评该 hold:
//   Stat 逐行 (accum_rows 一级 Row + cost_rows 逐行换手, 与 Run 同口径) → 净值引擎 fold_exit (Stat 契约【净值】: G20 多头, 每批 1/h̄, 平仓行实现;
//   可成交 / 顺延在标签里) → FactorCurve 逐行出参线.
//   成本按 Δw (契约【换手成本】): 有金额段 (Lob) = 增仓 Δw⁺ × 最小段买入冲击 (封顶 max_impact) + 平仓量 × Config::fixed_impact; 无段 (Bar) =
//   增仓量 / 平仓量各付 fixed_impact (UI 乘). 净值 / 聚合 (等权, 日并集轴) 在 UI 现算.
// 池篮子: 每池日层一份 (日层 Bar 标签库 + 池成员, YPlanes 篮子 → PoolCurve): 篮子 (池整体 + 行业) 两权重指数日收益 (净值引擎 fold_exit, T+1C 标签) + 两权重估值 EP / BP / SP / CP / DY (等权 = 个股收益率均值, 加权 = 整体法收益率;
//   滚动分位 band 在 UI 按可调窗长现算) + 池整体日均单边换手.
// 因子 ↔ 池 同一套缓存: 按 Job::key 存, 曲线记算时的 Job::sig (变了重算); 请求里没有的丢掉 (加一个因子只算它).
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑 (缓存已算的不丢); UI 持 mutex 读 result
// (shared_ptr 拷出即放锁). 进度: Loading 期 done/total = 天; Running 期 = 算子节点 + 收尾; 第几个因子走 jobs_done / jobs_total.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsLoad.hpp"    // BookLines / YPlanes::index_line / compound
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable / FactorScope / Request
#include "gui/util/Service.hpp"

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Factors {

// ---- 组合文件 (GUI 线程读写) ----
struct PortfolioEntry {
  std::string pool;
  int seg = 0;      // 因子层 (FactorLevel::seg; 文件里存层名)
  std::string file; // 因子文件名 (含 .json)
  std::string expr; // 具体规范串 (FactorRow::expr, 已含 sign)
  int hold = 0;     // 持有期键
  std::string key() const { return pool + "|" + std::to_string(seg) + "|" + file + "|" + expr + "|" + std::to_string(hold); }
};
std::vector<std::string> ListPortfolios(const std::string &dir);                                            // 主干名, 字典序; 目录不存在 = 空
std::vector<PortfolioEntry> LoadPortfolio(const std::string &dir, const std::string &name);                 // 格式不对 → assert (只本页写)
void SavePortfolio(const std::string &dir, const std::string &name, const std::vector<PortfolioEntry> &es); // tmp + rename
bool RenamePortfolio(const std::string &dir, const std::string &from, const std::string &to);               // to 已存在 → false
void DeletePortfolio(const std::string &dir, const std::string &name);

// ---- 解析 (GUI 线程): 条目 → 该 (池, 层) 目录下 ScanFactorFile 找回的权重行 ----
using FeatureTables = std::map<int, std::shared_ptr<const FeatureTable>>; // seg → 字段表 (按需建一次)
std::shared_ptr<const FeatureTable> table_of(FeatureTables &tables, const Feature &feature, int seg);
struct PortfolioItem {
  PortfolioEntry e;
  std::string name_en, name_cn;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  std::string missing; // 非空 = 失效原因 (文件 / 规范串找不到, 权重 BROKEN, 持有期不在该层, 标签库未建), 不算
};
void ResolvePortfolio(const SharedData &data, FeatureTables &tables, const std::vector<PortfolioEntry> &es, std::vector<PortfolioItem> &out);

// ---- 线的轴: 组合线 (G20 多头, 扣多腿成本) = 冲击 Impact; 池线 = 权重 Weight (FactorsService.hpp; Service 出毛序列, UI 组线) ----
enum Impact : int { kImpactNet,     // 有冲击
                    kImpactGross }; // 无冲击
inline constexpr int kImpacts = 2;

// ---- 请求 ----
struct HoldJob {   // 一个有效条目 = 一个因子 (FactorJob, 与 Inspect 请求同一份; scope = 条目的 (池, 层)) + 持有期
  std::string key; // PortfolioEntry::key
  FactorJob f;
  int hold = 0;
  std::string sig() const { return key + "|" + std::to_string(f.scope.max_impact); } // 买入冲击封顶变了重算
};
struct PoolJob {     // 一个池的日层指数
  std::string key;   // 池名
  FactorScope scope; // (池, 日层)
  std::shared_ptr<const FeatureTable> ft;
  std::string sig() const { return key; }
};
struct PortfolioRequest {
  std::vector<HoldJob> factors;
  std::vector<PoolJob> pools; // 有因子才带 (标签库已建的全部池)
  bool reload = false;        // 弃缓存全部重算 (特征库重算过 / 手动 Recompute)
  std::string sig() const;    // 内容签名 (不含 reload): 没变就不发请求, 不打断在跑的
};
// 资产轴须已就绪 (断言). 作用域建不成 (标签库未建) 的条目 → items[i].missing 置上, 不进请求
void MakePortfolioRequest(const SharedData &data, FeatureTables &tables, std::vector<PortfolioItem> &items, bool reload, PortfolioRequest &req);

// ---- 结果 ----
// 一个因子 (该 hold) 的 G20 多头出参线 (fold_exit stride 1 = 逐行; 线序 = CurveLine, 与 InspectResult::nav 同形):
// 增量占净值; 收益记在平仓行 (hold_exit_row), 成本记在建仓行
enum CurveLine : int { kCurvePnl, // 毛收益
                       kCurveImp, // 建仓冲击
                       kCurveFix, // 固定冲击份数 (UI × Config::fixed_impact); 净 = pnl − imp − fix · fixed_impact
                       kCurveLines };
struct FactorCurve {
  std::string sig;              // 算时的 HoldJob::sig
  std::vector<misc::Ymd> dates; // [days]
  int seg = 0, hold = 0;
  int amt = 0;       // 建仓冲击金额段 (万) = 该层最小段; 0 = 无段 (Bar 层) → 增仓量也付 fixed_impact
  BookLines nav;     // [kCurveLines][T = days · seg]
  std::string error; // 非空 = 没算成 (作用域无数据 / 叶值域越界), 其余空
  const float *nav_at(int line) const { return nav.at(line); }
};
// 一个池的日层篮子 (YPlanes 篮子原样搬来): 篮子 0 = 池整体, k = 1..kBaskets−1 = 本池按 SW2021 一级切的行业 (UI 只用 all 池的 = 申万一级)
struct PoolCurve {
  std::string sig;                             // 算时的 PoolJob::sig
  std::vector<misc::Ymd> dates;                // [nd] 日层评估期
  BookLines index;                             // [kBaskets · kWeights][nd] 篮子指数的日收益 (线序 YPlanes::index_line; UI compound 成点位)
  std::vector<float> val[kWeights][kPoolVals]; // [Weight][PoolVal][kBaskets · nd] 篮子估值 EP / BP / SP / CP / DY (等权 = 个股收益率均值 / 加权 = 整体法收益率; NaN = 当日不可算)
  double turn[kWeights] = {};                  // [Weight] 篮子 0 日均单边换手 (YPlanes::turn; 再平衡 + 成分进出, 停牌 = 移出再移回)
  std::string error;
  const float *index_of(size_t k, int w) const { return index.at(YPlanes::index_line(k, w)); } // 篮子 k 的日收益 [nd]
  const float *val_of(size_t k, int w, int m) const { return val[w][m].data() + k * dates.size(); }
};
struct PortfolioResult {
  std::map<std::string, std::shared_ptr<const FactorCurve>> factors; // HoldJob::key →
  std::map<std::string, std::shared_ptr<const PoolCurve>> pools;     // PoolJob::key →
};

class PortfolioService : public Service<PortfolioService, PortfolioRequest>, public Published {
  friend Service<PortfolioService, PortfolioRequest>;

public:
  static constexpr const char *kName = "PortfolioWorker";
  PortfolioService(); // 两者都在 .cpp: Flow 不完整
  ~PortfolioService();

  // UI 持 mutex 读
  PortfolioResult result;
  uint64_t result_gen = 0;                      // result 每变一次 +1 (UI 派生缓存键)
  std::string message;                          // 本轮说明 (如 "特征库判废"), 空 = 无
  std::atomic<int> jobs_done{0}, jobs_total{0}; // 本轮要算的 (因子 + 池)

private:
  struct Flow;
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return *this; }
  bool run(PortfolioRequest &req);
  template <class Job, class Curve>
  bool run_jobs(const std::vector<const Job *> &todo, std::map<std::string, std::shared_ptr<const Curve>> &out, Flow &F); // false = 取消 / 判废
  bool eval(const HoldJob &j, Flow &F, FactorCurve &out);                                                                 // false = 取消 / 判废
  bool eval(const PoolJob &j, Flow &F, PoolCurve &out);                                                                   // false = 取消 / 判废
  bool stale_out(Flow &F);                                                                                                // 库判废: 弃缓存 + 置 message; 恒 false

  std::unique_ptr<Flow> flow_; // 首个请求懒建, 随 worker 活
};

} // namespace GUI::Factors
