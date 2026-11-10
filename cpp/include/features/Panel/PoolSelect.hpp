#pragma once
// 选池引擎: all.bar L_1day 日行 → 原料带 Tape (进程内一份) → 逐行成员 (features/Panel/PoolSpec.hpp 的一组参数) → 篮子净值账 (Style 页) / 选股表 Picks (池表各子池)
//   行 r = all.bar 第 r 个已落盘日; 行 r 的键 / 资产态 = L_1day 行 r (= r+1 盘前态); 交易日 d 的成员 = 源行 src_of(周期, d) 选出的, 无前视
//   持仓权 (篮子账 / 行情篮子同一份, Tape::omega): 源行的目标权 (等权 1 / 加权 收盘总市值) 随其后各日收益漂移; 日频 = 目标权
//   交易日 d 的收益 = close_d / 前收 − 1 (前收 = 行 d−1 的 pre_close, 已除权; d 无成交 → 0): 后复权收盘价, 不看可成交 (与行情侧价格指数同口径)
#include "features/Panel/PoolSpec.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "shared/Basket.hpp" // kWeights / kAllCuts

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace pool {

// all.bar 层 level 里 code 的列下标 (落盘变换须是 tf)
size_t bar_col(size_t level, std::string_view code, ts::TfId tf = ts::TfId::None);

// all.bar L_1day 日行 (原料带 / 行情篮子合成同一张列表): 列序 = DayCol; 行情篮子的日根只读前 kQuoteCols 列; 切片维 k 的列 = kColSlice0 + k
enum DayCol : size_t { kColTs,
                       kColOpen,
                       kColHigh,
                       kColLow,
                       kColClose,
                       kColAmount, // 落盘 Log
                       kColPre,
                       kQuoteCols,
                       kColCs = kQuoteCols,
                       kColShares,
                       kColInd,
                       kColListAge,
                       kColDelistAge,
                       kColSlice0,
                       kDayCols = kColSlice0 + kSlices };
std::vector<size_t> day_cols(size_t n = kDayCols); // 前 n 列的库列下标

// 周序号 (周一起)
int64_t week_of(misc::Ymd d);
// 调仓日历 (选股表 / 篮子账 / 行情篮子 / 因子引擎池篮子同一份): 交易日 d 的成员源行 = rows (升序) 里 d 之前的末行 (日频) / d 所在周之前的末行 (周频);
//   无 → SIZE_MAX
size_t src_of(Rebal rb, std::span<const misc::Ymd> rows, misc::Ymd d);
// 相邻交易日 prev → d 换源 (d 起回到新源的目标权)
bool rebal_at(Rebal rb, misc::Ymd prev, misc::Ymd d);

// all.bar 库的已落盘日 (清单日期轴可能还在算)
std::vector<misc::Ymd> landed(const FeatureLib &lib);
// 续算判据: 已做的日 done 是此刻日轴 now 的非空前缀
bool extends(std::span<const misc::Ymd> done, std::span<const misc::Ymd> now);
// all.bar 库 + 已落盘日 (Style 页 / 行情篮子 / CTA 同一段开场): lack 非空 = 用不了 (未建 / 落盘不足 2 日), 以用处 what 开头
struct AllBar {
  FeatureLib lib;
  std::vector<misc::Ymd> dates;
  std::string lack;
};
AllBar all_bar(std::string_view what);

// 资产类: 行 r 的在市资产 (已上市 ∧ 未退市 ∧ 收盘总市值 > 0 ∧ 各切片列有类) 按各切片维的类归一类 (混合进制, 末维最低位); 切片 = 若干类的并
inline constexpr size_t slice_radix(int k) { return static_cast<size_t>(dim_size(kDimSt + k) - 1); }
inline constexpr size_t kClasses = [] {
  size_t n = 1;
  for (int k = 0; k < kSlices; ++k)
    n *= slice_radix(k);
  return n;
}();
inline constexpr uint16_t kOut = static_cast<uint16_t>(kClasses); // 不在市
bool covers(const Spec &s, size_t c);                             // 类 c 在 s 的切片内

using Cancel = std::atomic<bool>;
using Tick = std::function<void()>; // 每做完一行调一次 (进度)

// 原料带: all.bar 全轴 × 已落盘日
struct Tape {
  std::vector<misc::Ymd> dates;   // 行 = 已落盘日 (升序)
  size_t A = 0;                   // all.bar 轴长
  uint64_t axis = 0;              // 轴指纹
  std::vector<uint16_t> cls;      // [行 · A] 类号 (不在市 = kOut)
  std::vector<uint16_t> cnt;      // [行 · kClasses] 类内资产数
  std::vector<uint16_t> byc;      // [行 · A] 在市资产按 (类, 下标) 升序 (类段 = cnt 前缀和; 行尾在市数之后不用)
  std::vector<uint8_t> live, ind; // [行 · A] 全A 成员 (cs_valid ∧ 收盘 > 0) / 申万一级 (0 = 未知)
  std::vector<float> mcap;        // [行 · A] 收盘总市值 = close_day × total_shares (缺 → 0)
  std::vector<float> ret;         // [行 · A] 日收益 (首行 0)
  std::vector<float> pre;         // [A] 末行前收 (续行的收益)
  size_t days() const { return dates.size(); }
  // 资产 a 在行 q 收盘的持仓权 [Weight] = 源行 r 的目标权 (等权 1 / 加权 mcap) × Π_{r < k ≤ q} (1 + ret) (r ≤ q; r = q → 目标权)
  void omega(size_t r, size_t q, size_t a, float om[kWeights]) const;
};
// all.bar 库 lib 的原料带: 进程内一份, 已落盘日多出的行增量补 (轴变 / 旧行不再是前缀 → 重建), 每读完一行调 tick; 取消 / 库判废 → nullptr
std::shared_ptr<const Tape> tape(const FeatureLib &lib, const Cancel *cancel = nullptr, const Tick &tick = {});
// 行 r 的全A 成员 (资产下标升序)
void live_at(const Tape &t, size_t r, std::vector<uint32_t> &out);

// 键的逐行次序 (选股只看次序): 行 r 的有限键资产按 (键, 下标) 升序 + 各类的有限键数
struct Ranks {
  std::vector<uint32_t> ord;  // 逐行接排: 行 r = ord[off[r], off[r + 1])
  std::vector<uint16_t> rbc;  // 逐行接排 (同 off): 在市有限键资产在本行 ord 里的位置, 按 (类, 位置) 升序 (类段 = fin 前缀和; 行尾在市数之后不用)
  std::vector<size_t> off{0}; // [行 + 1]
  std::vector<uint16_t> fin;  // [行 · kClasses] 类内有限键数
  size_t rows() const { return off.size() - 1; }
};
// 风格 st 的次序补到 t 的末行 (rk 已排的行 = t 的前缀行; 只用原料带的风格不读库), 每排完一行调 tick; 取消 / 库判废 → false
bool rank_rows(const FeatureLib &lib, const Tape &t, Style st, misc::exec::ForkJoin &ex, const Cancel &cancel, const Tick &tick, Ranks &rk);

// 一行的选股原料: 类号 [A] / 类内资产数 [kClasses] / 按类分段的在市资产 byc; 风格池另要有限键的次序 [o, e)、类内有限键数 fin [kClasses]
//   与按类分段的次序位置 rbc
struct RowView {
  size_t A = 0;
  const uint16_t *cls = nullptr, *cnt = nullptr, *byc = nullptr;
  const uint32_t *o = nullptr, *e = nullptr;
  const uint16_t *fin = nullptr, *rbc = nullptr;
};
RowView row_view(const Tape &t, const Ranks *rk, size_t r); // rk 可空 (只给全选池用)

// 选股: 行内切片; 风格池: 键覆盖率 (有限 / 切片内) < kCover → 切片内全选 (键未到齐时不按残缺截面截两端),
//   否则有限键取低端 / 高端前 N (次序两头往里取: 并列时低端取下标小的, 高端取下标大的; 有限不足 N 则全取); 全选池 = 切片内全选
//   取法按代价二选一 (结果相同): 宽切片沿 A 轴 / 次序扫, 窄切片按类段收拢 (byc / rbc)
inline constexpr float kCover = 0.8f;
class Selector {
public:
  explicit Selector(const Spec &s);
  void at(const RowView &r, std::vector<uint32_t> &out) const; // out = 成员 (资产下标升序)

private:
  Spec s_;
  std::vector<uint8_t> in_;   // [类号 (含 kOut)] 在切片内
  std::vector<uint16_t> cls_; // 切片内的类
};

// 篮子净值账 (两种权重同一趟): 交易日 d 的成员 = pick(源行 r, out) (r = src_of(rb, 行, d); 换源才重选), 权 = Tape::omega 归一
struct Book {
  std::vector<float> ret[kWeights]; // [Weight][日] 日收益 (首日 / 未建仓 0)
  std::vector<float> buy[kWeights]; // [Weight][日] 增仓量 Σ (w − w̃)⁺ (w = 目标权, w̃ = 漂移后的权; 建仓 = 1; 不调仓 0)
  size_t first = 0;                 // 首个有仓日 (无 → days)
};
using Pick = std::function<void(size_t r, std::vector<uint32_t> &out)>;
void run_book(const Tape &t, Rebal rb, const Pick &pick, Book &b);

// 选股表 (池表全部子池同一份): 原料带的行 r → 子池参数在行 r 选出的成员 (all.bar 轴下标升序; 周频池也逐行选)
//   子池库成员 (features/Panel/Pool.hpp) / 行情篮子 (gui/quote/BasketRow.hpp) / 因子引擎池篮子 (gui/engine/Load.hpp) 同读它
struct Picks {
  std::shared_ptr<const Tape> tape;       // 行 = tape->dates
  std::vector<std::vector<uint32_t>> off; // [p − 1][行 + 1]
  std::vector<std::vector<uint16_t>> ids; // [p − 1] 逐行接排
  // 子池 p (≥ 1) 在交易日 d 的源行 (按池的周期; 无 → SIZE_MAX) / 行 r 选出的成员 / d 的成员 (无源 = 空)
  size_t src(PoolId p, misc::Ymd d) const;
  std::span<const uint16_t> row(PoolId p, size_t r) const;
  std::span<const uint16_t> of(PoolId p, misc::Ymd d) const;
};
// all.bar 库 lib 的选股表: 进程内一份, 随原料带增量补行 (原料带重建 → 重建); 取消 / 库判废 → nullptr
std::shared_ptr<const Picks> picks(const FeatureLib &lib, const Cancel *cancel = nullptr, const Tick &tick = {});

} // namespace pool
