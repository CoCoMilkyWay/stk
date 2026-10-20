// PortfolioService — Factors→Portfolio 页的单 worker: 因子组合 (一组合一文件) 里每个因子**独立**回测 + 各池等权指数.
//
// 组合文件 <config.portfolio_dir>/<name>.json (name = 主干, ValidFactorName):
//   { "factors": [ { "pool": "all", "level": "5min", "file": "mom30.json", "expr": "<具体规范串, 已含 sign>", "hold": 30 }, … ] }
//   条目 = 权重粒度: (池, 因子层, 因子文件, 具体规范串) 定位 Factors 表里的一个权重 (FactorRow::expr); hold = 加入时 Factors 页的 Hold.
//   因子文件里找不到该规范串 (被 Search 重写 / 删了) → 条目失效 (UI 红, 不算). 组合内可混池混层.
//
// 逐因子独立 (各用自己的层 / 池 / 评估期, 不统一网格): 与 Inspect 同一条 flow (FactorsLoad 装载 → Engine<Cpu> walk → rank_x →
//   accum_rows / leg_codes), 只留该 hold 的逐行序列 (FactorCurve): G20 做多绝对收益 / LS (毛) + 最小金额段两腿的逐行换手成本
//   (Stat 契约【换手成本】, cost_row 每行一份 CostAcc; 与 cost_rows 逐行同式, 行和 = Inspect 的 CostTier). Bar 层无金额段: 新进者计数不看成本有效
//   (建仓 / 平仓都按 Config::fixed_impact, UI 扣). 净值 / 聚合 (等权, 日并集轴) 在 UI 现算.
//   结果按条目键缓存: 加一个因子只算它; 请求里没有的条目丢掉.
// 池等权指数: 每池日层一份 (日层 Bar 标签库 + 池成员, YPlanes::index) + 成员日均换手, 按池缓存 (Recompute 才重算).
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑 (缓存已算的不丢); UI 持 mutex 读 result
// (shared_ptr 拷出即放锁). 进度: Loading 期 done/total = 天; Running 期 = 算子节点 + 收尾; 第几个因子走 jobs_done / jobs_total.
#pragma once

#include "factor/Stat/Contract.hpp"
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

// ---- 请求 ----
struct PortfolioJob { // 一个有效条目
  std::string key;    // PortfolioEntry::key
  FactorScope scope;  // 条目的 (池, 层)
  std::shared_ptr<const FeatureTable> ft;
  std::string expr;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  int hold = 0;
};
struct PoolJob {     // 一个池的日层指数
  FactorScope scope; // (池, 日层)
  std::shared_ptr<const FeatureTable> ft;
};
struct PortfolioRequest {
  std::vector<PortfolioJob> factors;
  std::vector<PoolJob> pools; // 有因子才带 (标签库已建的全部池)
  bool reload = false;        // 弃缓存全部重算 (特征库重算过 / 手动 Recompute)
  std::string sig() const;    // 内容签名 (不含 reload): 没变就不发请求, 不打断在跑的
};
// 资产轴须已就绪 (断言). 作用域建不成 (标签库未建) 的条目 → items[i].missing 置上, 不进请求
void MakePortfolioRequest(const SharedData &data, FeatureTables &tables, std::vector<PortfolioItem> &items, bool reload, PortfolioRequest &req);

// ---- 结果 ----
// 一个因子 (该 hold) 的逐行序列 [T = days · seg], 毛口径; 不 ok / 段末尾部行 = 0 (真实收益口径: 不触发记 0)
struct FactorCurve {
  std::string key;
  int seg = 0, hold = 0;
  double max_impact = 0.0;      // 算时的买入冲击封顶 (变了重算)
  std::vector<misc::Ymd> dates; // [days]
  std::vector<float> lng;       // G20 做多绝对收益 = grp[G20] + mkt[多] · cnt / max(cnt, cap) (Inspect 分层 绝对 口径)
  std::vector<float> ls;        // Row::ls
  int amt = 0;                  // 成本金额段 (万) = 该层最小段; 0 = 无段 (Bar 层) → 建仓也按 fixed_impact
  std::vector<float> wimp, we;  // [kLegs][T] 逐行 CostLeg::wimp / we (t < h 的行 0); 该腿行成本 = wimp + we · fixed_impact · (amt > 0 ? 1 : 2)
  std::string error;            // 非空 = 没算成 (作用域无数据 / 叶值域越界), 其余空
  const float *wimp_at(int l) const { return wimp.data() + static_cast<size_t>(l) * lng.size(); }
  const float *we_at(int l) const { return we.data() + static_cast<size_t>(l) * lng.size(); }
};
struct PoolCurve {
  std::string pool;
  std::vector<misc::Ymd> dates; // [days] 日层评估期
  std::vector<float> index;     // [days] 池等权指数 (起点 1)
  double turnover = -1.0;       // 日均换手 = 沿日均值 (1 − |S_t ∩ S_{t−1}| / |S_t|), S = 池成员位 (日层: D 收盘定的池); all 池无成员 → < 0
  std::string error;
};
struct PortfolioResult {
  std::map<std::string, std::shared_ptr<const FactorCurve>> factors; // PortfolioEntry::key →
  std::map<std::string, std::shared_ptr<const PoolCurve>> pools;     // 池名 →
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
  bool eval_factor(const PortfolioJob &j, Flow &F, FactorCurve &out); // false = 取消 / 判废
  bool eval_pool(const PoolJob &j, Flow &F, PoolCurve &out);          // false = 取消 / 判废

  std::unique_ptr<Flow> flow_; // 首个请求懒建, 随 worker 活
};

} // namespace GUI::Factors
