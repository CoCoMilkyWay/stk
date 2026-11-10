// StyleService — Beta→Style 页的单 worker: 一个风格的参数网格 (features/Panel/PoolSpec.hpp Grid) 逐组选池 → 篮子净值 / 绩效 (对全A)
//   原料带 = 进程内一份 (pool::tape, 与选股表共用; 新落盘日增量补); 风格键的逐行次序按风格留最近 kKeep 份 (随原料带补行);
//   篮子账按参数留 (全选池不分风格常留, 其余只留当前风格; 原料带换代清空): 改网格只算新出现的组
//   全A = 行情篮子同一条 (全A 成员 = cs_valid ∧ 收盘 > 0, 日频), 两权重
//   请求 = 网格 (风格维只勾打开的那一个); 新请求覆盖旧的. 进度: Loading = 原料 / 键的行数, Running = 待算的篮子数
// 线程模型 (gui/util/Service.hpp, 随对象): UI 持 mutex 读 result (shared_ptr 拷出即放锁)
#pragma once

#include "features/Panel/PoolSelect.hpp"
#include "gui/util/Service.hpp"
#include "misc/exec.hpp"
#include "shared/Basket.hpp" // Weight
#include "shared/Progress.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Beta {

struct StyleRequest {
  uint32_t gen = 0;
  pool::Grid grid;
};

// 一条篮子线 (一组参数 × 一种权重) 自首个有仓日起的净值与绩效
struct StyleLine {
  pool::Spec spec;
  Weight weight = kWeightEq;
  size_t first = 0;       // 首个有仓日 (无 → 日数, 绩效全 NaN)
  std::vector<float> nav; // [日] 净值 (首个有仓日前 = 1)
  float ann = 0;          // 年化收益
  float sharpe = 0;       // 夏普 (无风险 0)
  float mdd = 0;          // 最大回撤 (≥ 0)
  float exc = 0;          // 相对全A (同权重) 年化超额: 相对净值 Π (1+r)/(1+r_b) 年化
  float ir = 0;           // 信息比: 相对日收益 均值 / 标准差 × √年交易日
  float turn = 0;         // 年化单边换手 (累计增仓量 / 年)
};

struct StyleResult {
  uint32_t gen = 0; // = 请求的
  pool::Grid grid;
  std::string lack;             // 算不了的原因 (空 = 有结果)
  std::vector<misc::Ymd> dates; // 日轴
  std::vector<StyleLine> lines; // 网格序 (specs_of) × 网格勾选的权重; 全程切片为空的组不列
  size_t empty = 0;             // 不列的组数
  StyleLine bench[kWeights];    // [Weight] 全A (spec 不用)
};

class StyleService : public Service<StyleService, StyleRequest>, public Published {
  friend Service<StyleService, StyleRequest>;

public:
  static constexpr const char *kName = "StyleWorker";
  StyleService() { Start(); }
  ~StyleService() { Stop(); }

  std::shared_ptr<const StyleResult> result; // UI 持 mutex 读; 空 = 未算

private:
  static constexpr Phase kStart = Phase::Loading;
  static constexpr size_t kKeep = 3; // 留在内存的风格次序数 (每份 ≈ [日 · A] uint32)
  static constexpr int kThreads = 8; // 键: 每线程一行的列读 + 排序; 篮子: 每线程一组参数的成员 / 权
  Progress &board() { return *this; }
  bool run(StyleRequest &req);
  bool publish(const StyleRequest &req, std::string lack, std::shared_ptr<StyleResult> out = nullptr);
  // 风格 st 补到 tape_ 末行的次序 (提到最新); 取消 / 库判废 → nullptr
  std::shared_ptr<const pool::Ranks> ranks(const FeatureLib &lib, pool::Style st);

  struct Key {
    pool::Style style;
    std::shared_ptr<const pool::Ranks> rk;
  };

  misc::exec::ForkJoin pool_{misc::exec::fit(kThreads)};
  std::shared_ptr<const pool::Tape> tape_;                        // 上次用的原料带 (空 = 未装)
  std::vector<Key> keys_;                                         // 新 → 旧 (原料带重建清空, 补行随用随补)
  std::map<pool::Spec, std::shared_ptr<const pool::Book>> books_; // 篮子账 (随 tape_ 换代清空)
  std::shared_ptr<const pool::Book> bench_;                       // 全A 的账 (随 tape_ 换代清空)
};

} // namespace GUI::Beta