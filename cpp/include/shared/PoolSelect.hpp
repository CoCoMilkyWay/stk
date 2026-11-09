#pragma once
// 选池引擎: all.bar L_1day 基础列 → 逐行成员 (shared/PoolSpec.hpp 的一组参数) → 篮子净值账
//   原料带 Tape = all.bar 全轴 × 已落盘日: 资产类 / 收益 / 市值常驻, 风格键按需另装 (load_key) 并排成逐行次序 (rank_key, 同风格的池共用一份)
//   行 r 的键 / 资产态 = L_1day 行 r (= r+1 盘前态); 调仓行 r 选出的成员持有于其后的交易日
//     (日频 = r+1 日; 周频 = 到下一调仓行的次日), 无前视
//   交易日 d 的收益 = close_d / 前收 − 1 (前收 = 行 d−1 的 pre_close, 已除权; d 无成交 → 0): 后复权收盘价, 不看可成交 (与行情侧价格指数同口径)
#include "features/Storage/FeatureRead.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "shared/Basket.hpp" // kWeights
#include "shared/PoolSpec.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <vector>

namespace pool {

// 资产类: 行 r 的在市资产 (已上市 ∧ 未退市 ∧ 板块已知 ∧ 收盘总市值 > 0) 按 (ST 等级, 板块, 两融, 过滤位) 归一类; 切片 = 若干类的并
//   类号 = ((st_level · 5 + 板块 − 1) · 2 + 非两融) · 2^kFilters + 过滤位
inline constexpr size_t kClasses = static_cast<size_t>(kSts - 1) * (kBoards - 1) * (kMargins - 1) << kFilters;
inline constexpr uint16_t kOut = static_cast<uint16_t>(kClasses); // 不在市
bool covers(const Spec &s, size_t c);                             // 类 c 在 s 的切片内

struct Tape {
  std::vector<misc::Ymd> dates;        // 已落盘日 (升序)
  size_t A = 0;                        // all.bar 轴长
  uint64_t axis = 0;                   // 轴指纹
  std::vector<uint16_t> cls;           // [行 · A] 类号 (不在市 = kOut)
  std::vector<uint16_t> cnt;           // [行 · kClasses] 类内资产数
  std::vector<float> ret;              // [日 · A] 日收益 (首日 0)
  std::vector<float> mcap;             // [行 · A] 收盘总市值 = close_day × total_shares (缺 → 0)
  std::vector<uint8_t> rebal[kRebals]; // [Rebal][行] 调仓行 (日 = 每行; 周 = 所在周的末个交易日, 末行 = 周五)
  size_t days() const { return dates.size(); }
};

using Cancel = std::atomic<bool>;
using Tick = std::function<void()>; // 每做完一日调一次 (进度)
// all.bar 库 lib 在 dates (已落盘日) 上的原料; 取消 / 库判废 → false
bool load_tape(const FeatureLib &lib, const FeatureRead &rd, std::span<const misc::Ymd> dates, misc::exec::ForkJoin &ex, const Cancel &cancel,
               const Tick &tick, Tape &t);
// 风格 st 的键 [行 · A] (NaN = 缺; 只用 t 的原料的风格不读库, 不调 tick); 取消 / 库判废 → false
bool load_key(const FeatureRead &rd, const Tape &t, Style st, misc::exec::ForkJoin &ex, const Cancel &cancel, const Tick &tick, std::vector<float> &key);
// 风格 st 的键要读库 (load_key 逐日调 tick)
bool key_reads(Style st);

// 键的逐行次序 (选股只看次序): 行 r 的有限键资产按 (键, 下标) 升序 + 各类的有限键数
struct Ranks {
  std::vector<uint32_t> ord; // 逐行接排: 行 r = ord[off[r], off[r + 1])
  std::vector<size_t> off;   // [行 + 1]
  std::vector<uint16_t> fin; // [行 · kClasses] 类内有限键数
};
// 逐行调 tick; 取消 → false
bool rank_key(const Tape &t, const std::vector<float> &key, misc::exec::ForkJoin &ex, const Cancel &cancel, const Tick &tick, Ranks &rk);

// 选股: 行 r 的切片内; N > 0: 键覆盖率 (有限 / 切片内) < kCover → 切片内全选 (键未到齐时不按残缺截面截两端),
//   否则有限键取低端 / 高端前 N (次序两头往里取: 并列时低端取下标小的, 高端取下标大的; 有限不足 N 则全取); N = 0 全选 (rk 可空)
inline constexpr float kCover = 0.8f;
class Selector {
public:
  Selector(const Tape &t, const Ranks *rk, const Spec &s);
  void at(size_t r, std::vector<uint32_t> &out) const; // out = 成员 (资产下标升序)

private:
  const Tape &t_;
  const Ranks *rk_;
  Spec s_;
  std::vector<uint8_t> in_;   // [类号 (含 kOut)] 在切片内
  std::vector<uint16_t> cls_; // 切片内的类
};

// 篮子净值账 (一组参数两种权重同一趟): 调仓行 r 选成员 → 目标权 (等权 1/n; 加权 ∝ 行 r 收盘总市值) → 持有至下一调仓行, 权随价漂移
struct Book {
  std::vector<float> ret[kWeights]; // [Weight][日] 日收益 (首日 / 未建仓 0)
  double buy[kWeights] = {};        // [Weight] 累计增仓量 Σ (w − w̃)⁺ (w = 目标权, w̃ = 漂移后的权; 建仓 = 1)
  size_t first = 0;                 // 首个有仓日 (无 → days)
};
void run_book(const Tape &t, const Ranks *rk, const Spec &s, Book &b);

} // namespace pool
