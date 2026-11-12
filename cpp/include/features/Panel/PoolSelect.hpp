#pragma once
// 选池引擎: all.bar L_1day 日行 → 原料带 Tape (每进程代一份) → 次序 Order → 逐行成员 (features/Panel/PoolSpec.hpp 的一组参数) → 篮子净值账 (Style 页) / 选股表 Picks (池表各子池)
//   行 r = all.bar 第 r 个已落盘日; 行 r 的键 / 资产态 = L_1day 行 r (= r+1 盘前态); 交易日 d 的成员 = 源行 src_of(周期, d) 选出的, 无前视
//   次序 (选股只看次序) = 风格键升序 (现排, 同风格的池共用); 调优 (Style 页) 与落地 (选股表) 同一条行扫: 调优次序整列留存 (rank), 落地当行即选 (picks)
//   持仓权 (篮子账 / 行情篮子同一份, Tape::omega): 源行的目标权 (等权 1 / 加权 收盘总市值) 随其后各日收益漂移; 日频 = 目标权
//   交易日 d 的收益 = close_d / 前收 − 1 (前收 = 行 d−1 的 pre_close, 已除权; d 无成交 → 0): 后复权收盘价, 不看可成交 (与行情侧价格指数同口径)
#include "define/Csr.hpp"
#include "features/Panel/PoolSpec.hpp"
#include "features/Storage/FeatureLib.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"

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

// all.bar L_1day 日行 (原料带 / 行情篮子合成同一张列表): 列序 = DayCol; 行情篮子的日根只读前 kQuoteCols 列; 过滤项 f 的列 = kColFilter0 + f
enum DayCol : size_t { kColTs,
                       kColOpen,
                       kColHigh,
                       kColLow,
                       kColClose,
                       kColAmount, // 落盘 Log
                       kColPre,
                       kQuoteCols,
                       kColShares = kQuoteCols,
                       kColInd,
                       kColListAge,
                       kColDelistAge,
                       kColSusp,
                       kColSt,
                       kColBoard,
                       kColMargin,
                       kColFilter0,
                       kDayCols = kColFilter0 + kFilters };
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

// 资产类: 行 r 的在市资产 (见 PoolSpec.hpp; 各划分列有类) 按各切片维的类归一类 (混合进制, 末维最低位; 过滤维的类 = 中的项的位集); 切片 = 若干类的并
inline constexpr size_t slice_radix(int k) { return kDimDefs[kDimSt + k].kind == kKindFilter ? size_t{1} << dim_size(kDimSt + k) : static_cast<size_t>(dim_size(kDimSt + k)); }
inline constexpr size_t kClasses = [] {
  size_t n = 1;
  for (int k = 0; k < kSlices; ++k)
    n *= slice_radix(k);
  return n;
}();
inline constexpr uint16_t kOut = static_cast<uint16_t>(kClasses); // 不在市
bool covers(const Spec &s, size_t c);                             // 类 c 在 s 的切片内

using Cancel = std::atomic<bool>;
// 进度: 开工报待做行数 / 每做完一行报一次 (都可空)
struct Tick {
  std::function<void(size_t)> begin;
  std::function<void()> step;
};

using Aid = uint16_t; // all.bar 轴下标

// 一行的次序: seq [len] = 有限键资产按 (键, 下标) 升序 + 在市者在 seq 里的位置 pos 按 (类, 位置) 升序 (类段长 n [kClasses])
//   cnt = 类内在市数 (原料带的, 覆盖率的分母); 全市场只读 cls (Tape::row)
struct Order {
  const uint16_t *cls = nullptr; // [A] 类号
  const uint16_t *cnt = nullptr; // [kClasses]
  const Aid *seq = nullptr;      // [len]
  size_t len = 0;
  const uint16_t *n = nullptr; // [kClasses]
  const Aid *pos = nullptr;    // [Σ n]
  size_t A = 0;
};

struct Tape;
// 一个风格逐行的次序 (行宽 A)
struct Orders {
  std::vector<Aid> seq;      // [行 · A]
  std::vector<uint16_t> len; // [行] seq 长
  std::vector<uint16_t> n;   // [行 · kClasses]
  std::vector<Aid> pos;      // [行 · A] (行尾 Σ n 之后不用)
  size_t rows() const { return len.size(); }
  void resize(size_t R, size_t A);
  Order row(const Tape &t, size_t r) const;
};

// 原料带: all.bar 全轴 × 已落盘日
struct Tape {
  std::vector<misc::Ymd> dates; // 行 = 已落盘日 (升序)
  size_t A = 0;                 // all.bar 轴长
  uint64_t axis = 0;            // 轴指纹
  uint64_t line = 0;            // 谱系: 同谱系 = 同轴且日轴互为前缀 (增量补行沿用, 重建换新)
  std::vector<uint16_t> cls;    // [行 · A] 类号 (不在市 = kOut)
  std::vector<uint16_t> cnt;    // [行 · kClasses] 类内在市数
  std::vector<uint8_t> ind;     // [行 · A] 申万一级 (0 = 未知; 在市者已知)
  std::vector<float> mcap;      // [行 · A] 收盘总市值 = close_day × total_shares (缺 → 0)
  std::vector<float> grow;      // [行 · A] 累计增长 Π_{0 < k ≤ 行} (1 + 日收益) (首行 1)
  std::vector<float> pre;       // [A] 末行前收 (续行的收益)
  size_t days() const { return dates.size(); }
  Order row(size_t r) const; // 行 r 的类号 (无次序: 只够全市场选)
  // 资产 a 在行 q 收盘的持仓权 [Weight] = 源行 r 的目标权 (等权 1 / 加权 mcap) × grow_q / grow_r (r ≤ q; r = q → 目标权)
  void omega(size_t r, size_t q, size_t a, float om[kWeights]) const;
};
// all.bar 库 lib 的原料带: 每进程代一份, 已落盘日多出的行增量补 (轴变 / 旧行不再是前缀 → 重建); 取消 / 库判废 → nullptr
std::shared_ptr<const Tape> tape(const FeatureLib &lib, const Cancel *cancel = nullptr, const Tick &tick = {});
// 风格 st 的次序 rk 补到 t 的末行 (rk 已排的行 = t 的同谱系前缀行; 只用原料带的风格不读库); 取消 / 库判废 → false (rk 不变)
bool rank(const FeatureLib &lib, const Tape &t, Style st, misc::exec::ForkJoin &ex, const Cancel *cancel, const Tick &tick, Orders &rk);

// 选股: 次序 = s 的风格的; 行内切片, 有限键覆盖率 (次序内 / 切片内在市) < kCover → 空 (键未到齐时不按残缺截面截两端, 不建仓);
//   否则自次序的一端往里取前 N: 低端 / 高端 (并列时低端取下标小的, 高端取下标大的; 不足 N 则全取)
//   取法按代价二选一 (结果相同): 沿次序扫 / 按类段收拢 (pos); 全市场 = 切片内在市全取 (不看次序)
inline constexpr float kCover = 0.8f;
class Selector {
public:
  explicit Selector(const Spec &s);
  Style style() const { return s_.style(); }
  void at(const Order &o, std::vector<Aid> &out) const; // out = 成员 (资产下标升序)

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
using Pick = std::function<void(size_t r, std::vector<Aid> &out)>;
void run_book(const Tape &t, Rebal rb, const Pick &pick, Book &b);

// 选股表 (池表全部子池同一份): 原料带的行 r → 子池参数在行 r 选出的成员 (all.bar 轴下标升序; 周频池也逐行选)
//   子池库成员 (features/Panel/Pool.hpp) / 行情篮子 (gui/quote/BasketRow.hpp) / 因子引擎池篮子 (gui/engine/Load.hpp) 同读它
struct Picks {
  std::shared_ptr<const Tape> tape; // 行 = tape->dates
  std::vector<Csr<Aid>> rows;       // [p − 1] 行 → 成员
  // 子池 p (≥ 1) 在交易日 d 的源行 (按池的周期; 无 → SIZE_MAX) / 行 r 选出的成员 / d 的成员 (无源 = 空)
  size_t src(PoolId p, misc::Ymd d) const;
  std::span<const Aid> row(PoolId p, size_t r) const;
  std::span<const Aid> of(PoolId p, misc::Ymd d) const;
};
// all.bar 库 lib 的选股表: 每进程代一份, 随原料带增量补行 (原料带重建 → 重建); 取消 / 库判废 → nullptr
std::shared_ptr<const Picks> picks(const FeatureLib &lib, const Cancel *cancel = nullptr, const Tick &tick = {});

} // namespace pool
