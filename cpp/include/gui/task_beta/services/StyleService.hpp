// StyleService — Beta→Style 页的单 worker: 一个风格的参数网格 (features/Panel/PoolSpec.hpp Grid) 逐组选池 → 篮子的滚动一年绩效 (平均 / 中位) 与累计中位夏普
//   原料带 = 每进程代一份 (pool::tape, 与选股表共用; 新落盘日增量补); 风格的键序 (pool::rank) 按风格留最近 kKeep 份 (随原料带补行);
//   篮子账按成员 (权重归 0, 两权重同一趟) 留 (全选池不分风格常留, 其余只留当前风格; 原料带换代清空): 改网格只算新出现的成员
//   全A = 行情篮子同一条 (全A 成员 = cs_valid ∧ 收盘 > 0, 日频), 两权重
//   请求 = 网格 (风格维只勾打开的那一个) + 回测区间 (只改区间 = 账全在, 只重算绩效); 新请求覆盖旧的. 进度: Loading = 原料 / 键的行数, Running = 待算的篮子数
// 线程模型 (gui/util/Service.hpp, 随对象): UI 持 mutex 读 result (shared_ptr 拷出即放锁)
#pragma once

#include "features/Panel/PoolSelect.hpp"
#include "gui/util/Service.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "shared/Progress.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Beta {

struct StyleRequest {
  uint32_t gen = 0;
  pool::Grid grid;
  misc::Ymd lo = 0, hi = 0; // 回测区间 [lo, hi] (0 = 不限: 自首日 / 到末日)
};

// 区间 [d0, d1) 内: 起点 = max(首个有仓日, d0 + 1) (基日 d0 = 区间首日, 净值 1);
// 滚动窗: 以日 t 收尾 = [首个晚于 dates[t] 前推一年的日, t], 基日 = 窗起 − 1; 只计满一年的窗 (基日不早于起点 − 1).
// 年 = 基日到 t 的日历跨度 / 365.25
// 一条线在区间内的绩效: 年化 / 夏普 = 各滚动窗的平均与中位; 回撤 / 换手 = 整段 (自起点)
struct StyleStat {
  float ann = std::numeric_limits<float>::quiet_NaN();        // 各窗对数年化的平均, 换回简单收益 (≈ 区间年化复合)
  float ann_med = std::numeric_limits<float>::quiet_NaN();    // 各窗年化收益的中位
  float sharpe = std::numeric_limits<float>::quiet_NaN();     // 各窗夏普 (无风险 0) 的平均
  float sharpe_med = std::numeric_limits<float>::quiet_NaN(); // 各窗夏普的中位 (= 累计中位夏普的末日值; 打分)
  float mdd = std::numeric_limits<float>::quiet_NaN();        // 最大回撤 (≥ 0; 峰自基日起)
  float turn = std::numeric_limits<float>::quiet_NaN();       // 年化单边换手 (增仓量 / 年)
};

// 一条篮子线 (一组参数): 累计中位夏普与绩效
struct StyleLine {
  pool::Spec spec;
  size_t first = 0;       // 首个有仓日 (全程; 无 → 日数)
  size_t from = 0;        // 首个满一年的窗的收尾日 (无 → d1, 年化 / 夏普 NaN)
  std::vector<float> med; // [日] 累计中位夏普: 收尾日 from … 该日 各窗夏普的中位 (from 前 / 区间外 NaN)
  StyleStat stat;
};

struct StyleResult {
  uint32_t gen = 0; // = 请求的
  pool::Grid grid;
  std::string lack;                // 算不了的原因 (空 = 有结果)
  std::vector<misc::Ymd> dates;    // 日轴 (全程)
  size_t d0 = 0, d1 = 0;           // 回测区间 [d0, d1) (dates 下标; = 请求的 [lo, hi])
  std::vector<float> nav;          // [日] 全A 等权全程净值 (区间条底线)
  std::vector<StyleLine> lines;    // 网格序 (specs_of); 全程切片为空的组不列
  size_t empty = 0;                // 不列的组数
  StyleLine bench[pool::kWeights]; // [Weight] 全A (spec 不用)
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
  static constexpr size_t kKeep = 3; // 留在内存的风格键序数 (每份 ≈ [日 · A] 两份 uint16)
  Progress &board() { return *this; }
  bool run(StyleRequest &req);
  bool publish(const StyleRequest &req, std::string lack, std::shared_ptr<StyleResult> out = nullptr);
  // 风格 st 补到 tape_ 末行的键序 (提到最新); 取消 / 库判废 → nullptr
  std::shared_ptr<const pool::Orders> ranks(const FeatureLib &lib, pool::Style st, const pool::Tick &tick);

  struct Key {
    pool::Style style;
    std::shared_ptr<const pool::Orders> rk;
  };

  misc::exec::ForkJoin pool_{misc::exec::hw()};                   // 键: 每线程一行的列读 + 排序; 篮子: 每线程一组参数的成员 / 权
  std::shared_ptr<const pool::Tape> tape_;                        // 上次用的原料带 (空 = 未装)
  std::vector<Key> keys_;                                         // 新 → 旧 (原料带重建清空, 补行随用随补)
  std::map<pool::Spec, std::shared_ptr<const pool::Book>> books_; // 篮子账 [成员 (权重归 0)] (随 tape_ 换代清空)
  std::shared_ptr<const pool::Book> bench_;                       // 全A 的账 (随 tape_ 换代清空)
};

} // namespace GUI::Beta