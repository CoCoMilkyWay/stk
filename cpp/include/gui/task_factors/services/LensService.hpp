// LensService — agent 观察工具的单 worker: 一组因子根 + 一组特征式, 在作用域评估期上整期一块评一次 (eval_job, 与 Inspect 同一条管线), 落地原料不做汇总:
//   每个因子根: 已定向的池内秩 [T][A'] / 一级 Row [T] (单持有期) / 两端组真持有的格 (行 / 列 / 端 / 持有毛收益, 与 Row::grp 同一份判据: label_row 的逐格扩展);
//   每个特征式: 包一层 CsRank 进森林, 取根的输入 (kNeedAlpha) = 特征原值 [T][A'];
//   块上: 截面门控 / 该持有期毛标签 [T][A'] + 逐行池内标签均值 [T] (超额基准).
// A' = 落地列: 整截面 = 评估期内进过池的资产; 给了资产 = 只那一列 (个股视图, 不看池也落).
// 汇总与成文全在调用方 (agent/Views): 本服务只出平面, 不写文件, 结果只在内存.
// 计算 flow 与 Inspect 同构: X 评估域内读过的全留 (Life::Worker), 每次请求整条重算; 逐格扩展 → 恒 CPU.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsTable.hpp"  // FactorRef / Request
#include "gui/task_factors/services/FactorsWorker.hpp" // FactorWorker

#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Factors {

// 请求: 因子根 (具体规范串 + 口径 + 方向) / 特征根 (CsRank(特征式) 的规范串, CS 正向) 都须已校验; hold = 字段表里有的持有期键
struct LensSpec {
  std::vector<FactorRef> roots, feats;
  int hold = 0;
  uint32_t asset = UINT32_MAX; // 全局资产下标; UINT32_MAX = 整截面
};
using LensRequest = Request<LensSpec>;

// 两端组真持有的一格: e = 端 (kTop / kBot, 已按根的方向); ret = 持有毛收益 (续持取名义值; 超额 = ret − Row::mkt)
struct LensCell {
  int t = 0, a = 0; // 行 / 落地列
  uint8_t e = 0;
  float ret = 0.f;
};
struct LensRoot {
  std::string error;                   // 非空 = 该根没算成 (叶值域越界)
  std::vector<uint16_t> rx;            // [T][A'] r16 (已定向: 高 = 做多端; kRankNone 无效)
  std::vector<factor::stat::Row> rows; // [T] (已定向)
  std::vector<LensCell> cells;         // 行序
};
struct LensFeat {
  std::string error;
  std::vector<float> v; // [T][A'] 原值 (无效 NaN)
};
struct LensResult {
  std::string error; // 非空 = 没算成 (评估域不成 / 资产不在池轴 / 全部根坏), 其余字段空
  std::vector<misc::Ymd> dates;
  std::vector<uint32_t> ids; // [A'] 全局资产下标
  std::vector<uint8_t> gate; // [T][A'] 截面门控
  std::vector<float> label;  // [T][A'] 持有毛收益 (无效 NaN)
  std::vector<float> mkt;    // [T] 门控 ∧ 标签有效格的均值 (整截面; 无 → NaN)
  std::vector<LensRoot> roots;
  std::vector<LensFeat> feats;
};

// 进度面 = 自身 (FactorWorker): Loading 期 done/total = 天; Running 期 = 算子节点 + 根
class LensService : public FactorWorker<LensService, LensRequest> {
  friend Service<LensService, LensRequest>;

public:
  static constexpr const char *kName = "LensWorker";
  LensService(); // 两者都在 .cpp: FactorFlow 不完整
  ~LensService();

  // UI 持 mutex 取走 (std::move)
  LensResult result;

private:
  static constexpr Phase kStart = Phase::Loading;
  bool run(LensRequest &req);
  bool eval(const LensRequest &req, FactorFlow &F, LensResult &res); // false = 取消 / 判废
};

} // namespace GUI::Factors
