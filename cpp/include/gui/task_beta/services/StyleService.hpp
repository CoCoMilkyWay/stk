// StyleService — Beta→Style 页的单 worker: 一个风格的参数网格 (features/Panel/PoolSpec.hpp Grid) 逐组选池 → 篮子净值 / 各窗 (末 W 年) 绩效 (对全A)
//   原料带 = 进程内一份 (pool::tape, 与选股表共用; 新落盘日增量补); 风格键的逐行次序按风格留最近 kKeep 份 (随原料带补行);
//   篮子账按参数留 (全选池不分风格常留, 其余只留当前风格; 原料带换代清空): 改网格只算新出现的组
//   全A = 行情篮子同一条 (全A 成员 = cs_valid ∧ 收盘 > 0, 日频), 两权重
//   请求 = 网格 (风格维只勾打开的那一个); 新请求覆盖旧的. 进度: Loading = 原料 / 键的行数, Running = 待算的篮子数
// 线程模型 (gui/util/Service.hpp, 随对象): UI 持 mutex 读 result (shared_ptr 拷出即放锁)
#pragma once

#include "features/Panel/PoolSelect.hpp"
#include "gui/util/Service.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "shared/Basket.hpp" // Weight
#include "shared/Progress.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace GUI::Beta {

struct StyleRequest {
  uint32_t gen = 0;
  pool::Grid grid;
};

// 窗 = 末 W 年 (W = 1 … win_years; W = win_years 盖住全程): 以日 t 收尾的窗起 = 首个晚于 dates[t] 前推 W 年的日 (窗 = [起, t], 基日 = 起 − 1)
inline size_t win_start(std::span<const misc::Ymd> dates, size_t t, int W) {
  return static_cast<size_t>(std::upper_bound(dates.begin(), dates.begin() + static_cast<std::ptrdiff_t>(t) + 1, dates[t] - 10000 * W) - dates.begin());
}
// 盖住全程的最小 W
inline int win_years(std::span<const misc::Ymd> dates) {
  int W = 1;
  while (dates.back() - 10000 * W >= dates.front())
    ++W;
  return W;
}

// 一条线在一个窗内的绩效: 窗 = [max(窗起, 首个有仓日), 末日]; 年 = 基日到末日的日历跨度
struct StyleStat {
  float ann = 0;    // 年化收益
  float sharpe = 0; // 夏普 (无风险 0)
  float mdd = 0;    // 最大回撤 (≥ 0; 峰自基日起)
  float exc = 0;    // 相对全A (同权重) 年化超额: 相对净值 Π (1+r)/(1+r_b) 年化
  float ir = 0;     // 信息比: 相对日收益 均值 / 标准差 × √年交易日
  float turn = 0;   // 年化单边换手 (窗内增仓量 / 年)
};

// 一条篮子线 (一组参数 × 一种权重) 自首个有仓日起的净值与各窗绩效
struct StyleLine {
  pool::Spec spec;
  Weight weight = kWeightEq;
  size_t first = 0;           // 首个有仓日 (无 → 日数, 绩效全 NaN)
  std::vector<float> nav;     // [日] 净值 (首个有仓日前 = 1)
  std::vector<StyleStat> win; // [W − 1] 末 W 年窗的绩效 (W = 1 … StyleResult::years)
};

struct StyleResult {
  uint32_t gen = 0; // = 请求的
  pool::Grid grid;
  std::string lack;             // 算不了的原因 (空 = 有结果)
  std::vector<misc::Ymd> dates; // 日轴
  int years = 0;                // 窗的最大年数 (= win_years(dates), 盖住全程)
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
  static constexpr size_t kKeep = 3; // 留在内存的风格次序数 (每份 ≈ [日 · A] uint32 + uint16)
  Progress &board() { return *this; }
  bool run(StyleRequest &req);
  bool publish(const StyleRequest &req, std::string lack, std::shared_ptr<StyleResult> out = nullptr);
  // 风格 st 补到 tape_ 末行的次序 (提到最新); 取消 / 库判废 → nullptr
  std::shared_ptr<const pool::Ranks> ranks(const FeatureLib &lib, pool::Style st);

  struct Key {
    pool::Style style;
    std::shared_ptr<const pool::Ranks> rk;
  };

  misc::exec::ForkJoin pool_{misc::exec::hw()};                   // 键: 每线程一行的列读 + 排序; 篮子: 每线程一组参数的成员 / 权
  std::shared_ptr<const pool::Tape> tape_;                        // 上次用的原料带 (空 = 未装)
  std::vector<Key> keys_;                                         // 新 → 旧 (原料带重建清空, 补行随用随补)
  std::map<pool::Spec, std::shared_ptr<const pool::Book>> books_; // 篮子账 (随 tape_ 换代清空)
  std::shared_ptr<const pool::Book> bench_;                       // 全A 的账 (随 tape_ 换代清空)
};

} // namespace GUI::Beta