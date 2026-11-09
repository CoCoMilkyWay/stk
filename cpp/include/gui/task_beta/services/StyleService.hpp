// StyleService — Beta→Style 页的单 worker: 一个风格的参数网格 (gui/task_beta/services/PoolSpec.hpp Grid) 逐组选池 → 篮子净值 / 绩效 (对全A)
//   原料带 = all.bar 已落盘日 (库轴 / 日轴不变即常驻); 风格键的逐行次序按风格留最近 kKeep 份; 篮子账按参数留 (全选池不分风格常留,
//   其余只留当前风格): 改网格只算新出现的组
//   请求 = 风格 + 网格; 新请求覆盖旧的. 进度: Loading = 原料 / 键读 / 排序日数, Running = 待算的篮子数
// 线程模型 (gui/util/Service.hpp, 随对象): UI 持 mutex 读 result (shared_ptr 拷出即放锁)
#pragma once

#include "gui/util/Service.hpp"
#include "misc/exec.hpp"
#include "shared/Basket.hpp" // Weight
#include "gui/task_beta/services/PoolSelect.hpp"
#include "shared/Progress.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

struct Config;

namespace GUI::Beta {

struct StyleRequest {
  uint32_t gen = 0;
  pool::Style style = pool::kStyleSize;
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
  pool::Style style = pool::kStyleSize;
  pool::Grid grid;
  std::string lack;             // 算不了的原因 (空 = 有结果)
  std::vector<misc::Ymd> dates; // 日轴
  std::vector<StyleLine> lines; // 网格序 (specs_of) × 网格勾选的权重; 全程切片为空的组不列
  size_t empty = 0;             // 不列的组数
  StyleLine bench[kWeights];    // [Weight] 全A (在市全选, 不切不剔, 日频)
};

class StyleService : public Service<StyleService, StyleRequest>, public Published {
  friend Service<StyleService, StyleRequest>;

public:
  static constexpr const char *kName = "StyleWorker";
  explicit StyleService(const Config &cfg) : cfg_(cfg) { Start(); }
  ~StyleService() { Stop(); }

  std::shared_ptr<const StyleResult> result; // UI 持 mutex 读; 空 = 未算

private:
  static constexpr Phase kStart = Phase::Loading;
  static constexpr size_t kKeep = 3; // 留在内存的风格次序数 (每份 ≈ [日 · A] uint32)
  static constexpr int kThreads = 8; // 原料 / 键: 每线程一日的列读; 排序: 每线程一行; 篮子: 每线程一组参数的成员 / 权
  Progress &board() { return *this; }
  bool run(StyleRequest &req);
  bool publish(const StyleRequest &req, std::string lack, std::shared_ptr<StyleResult> out = nullptr);
  // 风格 st 的次序 (留着的 → 提到最新; 否则读键排序); 取消 → nullptr
  std::shared_ptr<const pool::Ranks> ranks(const FeatureRead &rd, pool::Style st);

  struct Key {
    pool::Style style;
    std::shared_ptr<const pool::Ranks> rk;
  };

  const Config &cfg_;
  misc::exec::ForkJoin pool_{misc::exec::fit(kThreads)};
  pool::Tape tape_;                                               // 空 (A = 0) = 未装 / 装到一半被取消
  std::vector<Key> keys_;                                         // 新 → 旧 (随 tape_ 换代清空)
  std::map<pool::Spec, std::shared_ptr<const pool::Book>> books_; // 篮子账 (随 tape_ 换代清空)
};

} // namespace GUI::Beta