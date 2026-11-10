// CtaLib — CTA 页的分钟价格库 (CtaService worker 私有): 两侧 (个股 = 库轴全部个股 / 篮子 = 指数 / 行业 / 概念三组线) 一趟过日合成落盘, 之后只补新日
//   源 = all.bar 特征库: 交易日 D 的成员 / 权 = 选股表 (Quote::members), 基准 = D−1 行前收, D 的日根 + L_1min 分钟根; 一日一件 Quote::Lane (与 BasketHistory 同一份), 两侧共用
//   两侧同一种记录 (一行一日 = 因子 + 分钟价), 有效根 = 价 × 因子:
//     个股: 价 = 库存原价 (f16, 与库逐位一致), 前收 = D−1 行的前收
//     篮子: 价 = Lane 的日内点位 (前日收 = 1, f32), 前收 = 1
//     因子同一条链: 自首个有根日 1 起, 逢前收 ≠ 参考价乘 参考价 / 前收, 参考价 = 末个收盘 (按前收滚动) ⇒ 个股 = 后复权, 篮子 = 点位逐日接续
//   时间格: 有效根 t = 日下标 · kSlots + 槽, 日下标 = 库落盘日序 (首日只定成员, 无根); 无根 (无成员 / 无成交 / 末个有根的槽之后) 不收
// 落盘 <库>/cache/cta/ (日主序, 续算只追加; 库全量重算连目录一起删):
//   head.bin   = Head + 两侧键表 [N] + dates [D] + 两侧链 [N] (末日因子 / 参考价)
//   stock.bin  = [D][A] { f64 因子 (0 = 当日无根), f16 [kSlots][kFields] 原价 高 低 收 (无根 = NaN) }
//   basket.bin = [D][B] { f64 因子 (0 = 当日无根), f32 [kSlots][kFields] 点位 高 低 收 (无根 = NaN) }
//   Head 与键表对得上、两文件够长且已合成日是选股表日轴的前缀 (pool::extends) → 只合成其后的新日, 否则清目录从头; 每批落一次头 (取消不丢已合成的)
#pragma once

#include "cta/Model.hpp" // Series
#include "disk/Disk.hpp"
#include "gui/quote/Quote.hpp" // Side / PerSide
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "shared/Progress.hpp"

#include <atomic>
#include <cstdint>
#include <vector>

struct FeatureLib;
class FeatureRead;
namespace pool {
struct Picks;
}

namespace GUI::Market {

using namespace Quote;

class CtaLib {
public:
  enum Field : int { kHigh,
                     kLow,
                     kClose,
                     kFields };

  // cancel / x / pool = 宿主服务的 (取消 / 进度面 Loading 天 / 线程池)
  CtaLib(const std::atomic<bool> &cancel, Progress &x, misc::exec::ForkJoin &pool) : cancel_(cancel), x_(x), pool_(pool) {}

  // 价格库补到选股表 pk 的日轴 (= 库已落盘日, 升序, ≥ 2 日); false = 取消 / 判废
  bool sync(const FeatureLib &lib, const FeatureRead &rd, const pool::Picks &pk);
  // 标的 (个股 = 全局 id / 篮子 = basket_line) 的有效根 (sync 之后); 不在该侧键表 (库轴外 / 三组线外) → false
  bool series(Quote::Side side, uint32_t key, cta::Series &se) const;

private:
  struct Head {
    uint64_t magic = 0, concepts = 0, pools = 0; // 概念表指纹 / 池表指纹
    misc::Ymd first = 0;                         // 库首个落盘日
    bool operator==(const Head &) const = default;
  };
  // 一行的因子链: 末日因子 (0 = 尚无根) / 参考价 (末个收盘, 按前收滚动; 0 = 尚无)
  struct Link {
    double f = 0;
    float ref = 0;
  };
  template <class Ar>
  void snap(Ar &ar);
  void save_head();

  const std::atomic<bool> &cancel_;
  Progress &x_;
  misc::exec::ForkJoin &pool_;
  disk::Key lib_; // 所在特征库
  Head head_;
  Quote::PerSide<std::vector<uint32_t>> keys_; // [侧][行] → 键 (个股 = 全局 id, 篮子 = basket_line)
  std::vector<misc::Ymd> dates_;               // [D] 已合成日 (首日只定成员, 其块无根)
  Quote::PerSide<std::vector<Link>> link_;     // [侧][行]
};

} // namespace GUI::Market