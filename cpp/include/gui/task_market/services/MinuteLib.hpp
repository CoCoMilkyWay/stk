// MinuteLib — Market 的分钟价格库 (CtaService / MinuteService 共用一份, 宿主 = Market 任务): 两侧 (个股 = 库轴全部个股 / 篮子 = 篮子表全部, 行 = 篮子下标)
//   一趟过日合成落盘, 之后只补新日
//   源 = all.bar 特征库: 交易日 D 的成员 / 权 = 选股表 (Quote::members), 源 = Quote::LibDay (基准 = D−1 行前收, D 的日根 + L_1min 分钟根);
//   库段一趟 Quote::lib_pass (与 BasketHistory 同一趟, 一日一件 Lane), 两侧同出
//   两侧同一种记录 (一行一日 = 因子 + 分钟价), 有效根 = 价 × 因子:
//     个股: 价 = 库存原价 (f16, 与库逐位一致), 前收 = D−1 行的前收
//     篮子: 价 = Lane 的日内比 (前日 = 1, f32), 前收 = 1
//     因子同一条链: 自首个有根日 1 起, 逢前收 ≠ 参考价乘 参考价 / 前收, 参考价 = 末个收盘 (按前收滚动) ⇒ 个股 = 后复权, 篮子 = 点位逐日接续
//   时间格: 有效根 t = 日下标 · kSlots + 槽, 日下标 = 库落盘日序 (首日只定成员, 无根); 无根 (无成员 / 无成交 / 末个有根的槽之后) 不收
//   宿主服务同一段开场 open (库 + 选股表 + 补到末日) → 戳 Stamp; 读一种 each = 一行逐个有根日的块 (宿主各取所需: CTA = 有效根, NAV = 逐日收盘)
//   宿主按 (标的, 戳) 沿用读出的序列 (Kept)
//   线程: 补与读同一把锁, 两个宿主服务各自的 worker 先 open 再读
// 落盘 <库>/cache/minute/ (日主序, 续算只追加; 库全量重算连目录一起删):
//   head.bin   = Head + 两侧键表 [N] + dates [D] + 两侧链 [N] (末日因子 / 参考价)
//   stock.bin  = [D][A] { f64 因子 (0 = 当日无根), f16 [kSlots][kFields] 原价 高 低 收 (无根 = NaN) }
//   basket.bin = [D][B] { f64 因子 (0 = 当日无根), f32 [kSlots][kFields] 点位 高 低 收 (无根 = NaN) }
//   Head 与键表对得上、两文件够长且已合成日是选股表日轴的前缀 (pool::extends) → 只合成其后的新日, 否则清目录从头; 每批落一次头 (取消不丢已合成的)
#pragma once

#include "disk/Disk.hpp"
#include "gui/quote/Quote.hpp" // Side / PerSide
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "shared/Progress.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

struct FeatureLib;
class FeatureRead;
namespace pool {
struct Picks;
}

namespace GUI::Market {

using namespace Quote;

// 库的戳: 库轴指纹 + 日轴末日 / 日数 (对得上 = 读出的序列可沿用)
struct MinuteStamp {
  uint64_t axis = 0;
  misc::Ymd last = 0;
  size_t days = 0;
  bool operator==(const MinuteStamp &) const = default;
};

// 一次开库: lack 非空 = 用不了 (以用处开头; dates / stamp 不填)
struct MinuteOpen {
  std::string lack;
  std::vector<misc::Ymd> dates; // 库落盘日 (升序, ≥ 2 日; 日下标 = each 的 i)
  MinuteStamp stamp;
};

class MinuteLib {
public:
  enum Field : int { kHigh,
                     kLow,
                     kClose,
                     kFields };
  // 一行一日的有根块: i = 日下标, f = 因子, px = [槽][高 低 收] 价 (无根 = NaN)
  using Each = std::function<void(size_t i, double f, const float *px)>;

  // all.bar 库 + 选股表 + 补到其日轴末日; what = 用处 (lack 开头); cancel / x / pool = 宿主服务的 (取消 / 进度面 Loading 天 / 线程池)
  //   nullopt = 取消 / 判废
  std::optional<MinuteOpen> open(std::string_view what, const std::atomic<bool> &cancel, Progress &x, misc::exec::ForkJoin &pool);
  // 读 open 之后的库: ref 逐个有根日 f(…); 不在该侧键表 (库轴外) → false
  bool each(Quote::Ref ref, const Each &f) const;

private:
  struct Head {
    uint64_t magic = 0, table = 0; // 篮子表指纹 (BasketTable::hash)
    misc::Ymd first = 0;           // 库首个落盘日
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
  // 价格库补到选股表 pk 的日轴 (= 库已落盘日); false = 取消 / 判废
  bool sync(const FeatureLib &lib, const FeatureRead &rd, const pool::Picks &pk, const std::atomic<bool> &cancel, Progress &x, misc::exec::ForkJoin &pool);
  size_t row_of(Quote::Ref ref) const; // 不在 → 该侧键数

  mutable std::mutex mu_;
  disk::Key lib_; // 所在特征库
  Head head_;
  Quote::PerSide<std::vector<uint32_t>> keys_; // [侧][行] → 键 (个股 = 全局 id, 篮子 = 篮子下标)
  std::vector<misc::Ymd> dates_;               // [D] 已合成日 (首日只定成员, 其块无根)
  Quote::PerSide<std::vector<Link>> link_;     // [侧][行]
};

// 宿主的读出序列: 按标的留最近 cap 个, 戳对得上才沿用
template <class T>
class Kept {
public:
  explicit Kept(size_t cap) : cap_(cap) {}
  // ref 在戳 st 下的序列: 留着 → 沿用 (提到最新); 否则 make(T &) 读 (false = 读不到 → 不留, nullptr)
  template <class F>
  std::shared_ptr<const T> get(Quote::Ref ref, const MinuteStamp &st, F &&make) {
    const auto hit = std::ranges::find_if(items_, [&](const Item &k) { return k.ref == ref && k.st == st; });
    std::shared_ptr<const T> v;
    if (hit != items_.end()) {
      v = hit->v;
      items_.erase(hit);
    } else if (auto t = std::make_shared<T>(); make(*t)) {
      v = std::move(t);
    } else {
      return nullptr;
    }
    items_.insert(items_.begin(), Item{ref, st, v});
    if (items_.size() > cap_)
      items_.pop_back();
    return v;
  }

private:
  struct Item {
    Quote::Ref ref;
    MinuteStamp st;
    std::shared_ptr<const T> v;
  };
  size_t cap_;
  std::vector<Item> items_; // 新 → 旧
};

} // namespace GUI::Market
