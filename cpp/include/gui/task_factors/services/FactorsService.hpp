// FactorsService — Factors→Factors 表的单 worker: 扫 <factor_dir>/<pool>/*.json 因子文件 → 解析 / 校验模板与权重表达式
// (factor/Expr.hpp) → (Run 时) 读特征库把因子输入 + 标签装成 [T = days·seg][A] 平面 (seg = 因子层, 见 FactorLevel) → 每权重 Expr → Dag →
// CPU / GPU evaluator 算根平面 → Stat 评估 → 发布一行; 一个文件的森林走完 (未取消) 一次回写它全部权重的 stat (FactorsFile WriteFactorStats).
//
// 因子文件格式 / 行 FactorRow / 文件增删改: FactorsFile.hpp.
//   同一具体规范串多行 (跨文件 / 同文件) → 后者标 dup (黄), 仍算 (同文件的在共享 DAG 里是同一个根, Stat 也只算一次).
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑; UI 持 mutex 读 rows;
// 进度走原子. evaluate = false 只扫描 (进页 / Add / Rescan), = true 扫描 + 装载 + 评估.
//
// 装载: 逐因子文件 (各文件评估期 / A 轴不同, 见 FactorLevel) 把该文件用到的特征 (去重, 跨库跨层 level_asof 投影) + 标签库 ts_valid / cs_valid
// + 全部持有期标签列读进宿主 (逐天并行, 门控 fmeta::valid ∧ isfinite → 值 + 掩码); 标签存 fp16 位 (与落盘同格式, Stat 直接吃).
// 装载前按宿主内存预算断言放得下 (FactorsLoad assert_fits; 放不下 = 该因子层太细 / 评估期太长).
// 两张门控列的分工 (features/MetaFlag.hpp): 特征输入按 ts_valid 门控 (TS 节点要跨进出池的历史); 标签按 cs_valid 门控 (只被 Stat 吃,
// rank(y) 得在池内排); cs_valid 的有效位另成一张截面池掩码平面 g, 原样递给 CS 节点的算子与 Stat (factor/Contract.hpp【截面池 g】:
// 池内统计 / 池外就近取值全在算子内部, 评估器不做二次门控).
// 评估 (Run 的并行方案, 与 search 的解耦): 一个文件的全部权重合成一张共享 DAG (factor::build_forest, 公共子式只算一次), 按拓扑序
// 顺序走节点; CPU 每个节点内部切满所有核 (factor::cpu::run_node + 常驻 ForkJoin, 结果与单线程逐位一致), 根算完立刻全核 Stat, 再按槽计划
// 释放 (内存 = 峰值活槽 × T·A × 5B, 确定); GPU 同一张 DAG, 输入上传一次, 中间量常驻显存 (DevPool), Stat 走常驻会话.
// Stat 的持有期维 hd.n = n_hold ≤ kMaxHold, x 的 rank 只算一次. 标签是毛价格收益 (不含冲击 / 税佣), HoldStat 是毛口径;
// 换手成本 (每金额段: lb_cost_* 盘口段, 买入按 Config::max_impact 封顶; 无冲击列的层 = 一个固定段) 用同一份秩再攒一遍 CostAcc (FactorsProbe CostP →
// Stat cost_rows, 两后端同一份宿主代码: CPU 用刚算的 rx, GPU 把 r16(x) 拷回), 落 stat.costs (年换手取段 0); 净线由 Inspect plot_term 在二级上现算,
// 因子表 score (排序 / 选代表权重) 由 RowScore 从该 h 的 HoldStat 算,
// Config::fixed_impact 只进公式不进文件. 因子表的全部数据都来自 stat —— Search 只内部用同一份目标挑参数, 不往文件写结论.
// time 列 (eval_ms) = 该因子子树全部节点 wall 之和 (共享节点算给每个用它的因子 = 单独算它要多久).
// 出错策略: 因子文件 / 表达式不合法是正常输入 → 行 error; 特征库 / 契约不一致 → assert.
#pragma once

#include "gui/task_factors/services/FactorsFile.hpp"  // FactorRow (+ 文件增删改, 调用方经本头一并可见)
#include "gui/task_factors/services/FactorsTable.hpp" // FeatureTable / FactorScope
#include "gui/util/Service.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Factors {

enum class Rc : uint8_t; // FactorsScope.hpp
struct FactorFlow;       // FactorsEngine.hpp

// 因子服务骨架 (Run / Search / Inspect / Portfolio 同一份): 进度面 = 自身 + 本轮说明 + worker 常驻件 FactorFlow (首个请求懒建, 随 worker 活)
// + 结局收尾. 成员函数定义在 FactorsEngine.hpp (FactorFlow 完整处); 派生类的 ctor / dtor 须在 .cpp
template <class D, class Req>
class FactorWorker : public Service<D, Req>, public Published {
public:
  std::string message; // 本轮说明 (如 "特征库无数据"), 空 = 无; UI 持 mutex 读

protected:
  Progress &board() { return *this; }
  FactorFlow &flow();
  bool abort_out(Rc rc); // 中断收尾 (rc 须 aborted): Stale → 弃块 + 置 message; 恒 false
  // 作业结局 (四服务同一份): 中断 (Cancel / Stale) → abort_out (false); Empty / Broken (正常输入) → fail(why) (true); Ok → true
  template <class Fail>
  bool settle(Rc rc, const std::string &why, Fail &&fail);

private:
  std::unique_ptr<FactorFlow> flow_;
};

struct RunSpec {
  bool evaluate = false; // false = 只扫描解析 (不读库)
  std::string only_file; // 非空 = 只评这个因子文件的权重 (Run / Search 写完后), 其余行保留文件里的 stat; 空 = Run all
};
using FactorsRequest = Request<RunSpec>;
// 只扫描恒成; evaluate 要作用域可用 (MakeFactorScope). ft = 该层字段表
bool MakeFactorsRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, bool evaluate, bool gpu,
                        FactorsRequest &req);

// 进度面 = 自身 (FactorWorker): Scanning → (evaluate) Loading 期 done/total = 天 → Running 期 = 共享 DAG 算子节点
class FactorsService : public FactorWorker<FactorsService, FactorsRequest> {
  friend Service<FactorsService, FactorsRequest>;

public:
  static constexpr const char *kName = "FactorsWorker";
  FactorsService(); // 两者都在 .cpp: FactorFlow 不完整
  ~FactorsService();

  int broken() const { return broken_.load(std::memory_order_relaxed); }

  // UI 持 mutex 读
  std::vector<FactorRow> rows;
  FactorsRequest current;

private:
  static constexpr Phase kStart = Phase::Scanning;
  bool run(FactorsRequest &req);
  void scan(const FactorsRequest &req, std::vector<FactorRow> &out);
  bool eval(const FactorsRequest &req, FactorFlow &F); // false = 取消 / 判废
  // 一个因子文件的全部待评权重 (idx = rows 下标, 同文件同模板 → 同叶 → 同评估期 / A 轴): 根集 → 管线 eval_job (共享森林, 块 F.c 补装)
  // 走引擎 E (FactorsEngine Engine<Cpu | Gpu>, 本轮一个). false = 取消 / 判废 (该文件 Empty / Broken: 行已标, true)
  template <class E>
  bool eval_file(const FactorsRequest &req, FactorFlow &F, E &eng, const std::vector<size_t> &idx);
  void mark(size_t i, const std::string &why); // 行 i 收尾为 BROKEN (调用方持锁; 已收尾的行不动)

  std::atomic<int> broken_{0};
};

} // namespace GUI::Factors
