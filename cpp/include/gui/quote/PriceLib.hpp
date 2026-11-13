// PriceLib — 行情的历史价格库 (QuoteHub 持有; 两路 History / MinuteService / CtaService 同一份): 两侧 (个股 = 库轴全部个股 / 篮子 = 篮子表全部, 行 = 篮子下标)
//   × 两种根 (日根 / 分钟根), 一趟合成落盘, 之后只补新日.
// 日脊 = 历史日 [D] (两侧同一条, 日下标 = each 的 i):
//   库段 [0, L) = all.bar 落盘日 (首日只定成员, 无根): 交易日 D 的成员 / 权 = 选股表 (members), 源 = LibDay (基准 = D−1 行前收, 日根 + L_1min 分钟根);
//     一趟 lib_pass (BasketRow.hpp, 一日一件 Lane), 两侧同出
//   补段 [L, D) = 库末日之后至基本面末日 (fund_last) 的交易日: 成员 = 库末日之后冻结, 逐日过 drift; 源 = FundDay (bar1d 行 + bar1m 分钟根);
//     库段 / 补段日变才重算
//   两段同一个 Lane, 同一条因子链; 末了留交接 (handoff): 末个历史日之后那个交易日的成员表 (库轴) + 各篮子末日点位, 当日合成接着它;
//   交接一变即交给当日 (构造时给的 bind, 在 open 返回之前 ⇒ 读端拿到新库时当日已换上新交接)
// 有效价 = 存价 × 因子 (因子 > 0 ⇔ 当日有日根):
//   个股: 存价 = 原价 (库段 = 库存 f16, 与特征 / 标签逐位一致; 补段 = bar1d / bar1m 落成 f16), 因子 = 后复权 (自首个有根日 1 起, 逢前收 ≠ 参考价乘 参考价 / 前收)
//   篮子: 存价 = Lane 的比 (前日 = 1, f32), 因子 = 前日点位 (自 kBase 起, 逐日 × 前日比收)
//   分钟根: 个股 = 高 低 收 (f16) + log1p(额) (f16, 库的额口径); 篮子 = 高 低 收 额 净流入 (f32); 无根 = NaN. 无量 (库不存)
// 落盘 <库>/cache/price/ (块主序: 每 kBlock 日一块, 块内按行连续 ⇒ 一行全史 = 每块一段连续读; 续算只写新日, 库全量重算连目录一起删):
//   head.bin = Head + 两侧键表 + 日脊 + L + 两侧库段末的链 (末日因子 / 参考价)
//   <侧>_day.bin = [块][行][kBlock] DayRec, <侧>_min.bin = [块][行][kBlock] 分钟根
//   头对得上、四文件够长且库段是选股表日轴的前缀 (pool::extends) → 先截去补段, 只合成库段新日再重算补段; 否则清目录从头. 每批落一次头 (取消不丢已合成的)
// 读: each = 一行逐个有分钟根的日 (CTA = 有效分钟价, NAV = 逐日收盘); kline = 一行日脊末 n 日的日根 + 末 m 日的分钟根 (有效价; 带净流入)
// 线程: 补与读同一把锁; 合成线程池随对象
#pragma once

#include "define/Members.hpp"
#include "disk/Disk.hpp"
#include "fundamental/AssetAxis.hpp" // PoolAxis
#include "gui/quote/Quote.hpp"       // Side / PerSide / Kline
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
namespace pool {
struct Picks;
}

namespace GUI::Quote {

// 库的戳: 库轴指纹 + 日脊末日 / 日数 / 库段日数 (对得上 = 读出的序列可沿用)
struct PriceStamp {
  uint64_t axis = 0;
  misc::Ymd last = 0;
  size_t days = 0, lib = 0;
  bool operator==(const PriceStamp &) const = default;
};

// 一次开库: lack 非空 = 用不了 (以用处开头; 其余不填)
struct PriceOpen {
  std::string lack;
  std::vector<misc::Ymd> dates; // 日脊 (升序, ≥ 2 日)
  size_t lib = 0;               // 库段日数 (dates[lib − 1] = 库末日, 其后为补段)
  PriceStamp stamp;
};

// 当日合成的交接 (库轴): 末个历史日之后那个交易日的成员表 + 各篮子末日点位
struct Handoff {
  PoolAxis axis; // 库轴
  Members mem;
  std::vector<double> level; // [篮子]
};

struct PriceFiles; // 四个块文件的映射

class PriceLib {
public:
  static constexpr size_t kBlock = 64;
  static constexpr int kThreads = 8; // 合成线程 (每线程一日的日根 / 分钟读 + 篮子累加, 数十 MB)
  static constexpr double kBase = 1000;
  enum Field : size_t { kHigh,
                        kLow,
                        kClose,
                        kFields };
  // 一行一个有分钟根的日: i = 日下标, f = 因子, px = [槽][高 低 收] 存价 (无根 = NaN)
  using Each = std::function<void(size_t i, double f, const float *px)>;
  // 一行的因子链: 末日因子 (0 = 尚无根) / 参考价 (末个收盘, 按前收滚动; 0 = 尚无)
  struct Link {
    double f = 0;
    float ref = 0;
  };

  // bind = 交接的去处 (QuoteService::bind; 持 PriceLib 的锁调)
  PriceLib(std::vector<misc::Ymd> trading, std::function<void(const Handoff &)> bind);
  ~PriceLib();
  PriceLib(const PriceLib &) = delete;
  PriceLib &operator=(const PriceLib &) = delete;

  // 补到 all.bar 落盘末日 + 基本面末日; what = 用处 (lack 开头); cancel / x = 调用方服务的 (取消: 库判废时读端一并拉起 / 进度面 Loading 天)
  //   nullopt = 取消 / 判废
  std::optional<PriceOpen> open(std::string_view what, std::atomic<bool> &cancel, Progress &x);
  // 以下读 open 之后的库; ref 不在该侧键表 (库轴外) → false
  bool each(Ref ref, const Each &f) const;
  // 日脊末 daily 日的日根 + 末 minutes 日的分钟根 (有效价; 无根的日不收)
  bool kline(Ref ref, size_t daily, size_t minutes, Kline &k) const;
  // 戳 st 那次开库的交接; 其后库又变了 → nullopt
  std::optional<Handoff> handoff(const PriceStamp &st) const;

private:
  struct Head {
    uint64_t magic = 0, table = 0; // 篮子表指纹 (BasketTable::hash)
    misc::Ymd first = 0;           // 库首个落盘日
    bool operator==(const Head &) const = default;
  };

  template <class Ar>
  void snap(Ar &ar);
  void save_head();
  PriceStamp stamp() const;
  misc::Ymd next(misc::Ymd d) const; // d 的次交易日
  // 头 / 文件对不上 → 清目录从头 (只留库首日)
  void reopen(const FeatureLib &lib, const std::vector<misc::Ymd> &dates);
  // 库段补到 dates, 补段补到基本面末日, 再定交接; false = 取消 / 判废
  bool sync(const FeatureLib &lib, const std::vector<misc::Ymd> &dates, std::atomic<bool> &cancel, Progress &x);
  // 补段 tail (write = 重算其根, 否则只读已落盘的) 逐日过成员 → 交接; false = 取消
  bool fill(const pool::Picks &pk, const fundamental::Catalog &cat, const std::vector<misc::Ymd> &tail, bool write, std::atomic<bool> &cancel, Progress &x);
  // 第 i 日 (已写好日根) 各行过链 z (就地推进), 填因子
  void chain(size_t i, PerSide<std::vector<Link>> &z);
  size_t row_of(Ref ref) const; // 不在 → 该侧键数

  const std::vector<misc::Ymd> trading_; // 交易日升序
  const std::function<void(const Handoff &)> bind_;
  mutable std::mutex mu_;
  disk::Key lib_; // 所在特征库
  PoolAxis axis_; // 库轴
  Head head_;
  PerSide<std::vector<uint32_t>> keys_; // [侧][行] → 键 (个股 = 全局 id, 升序; 篮子 = 篮子下标)
  std::vector<misc::Ymd> dates_;        // 日脊
  size_t lib_days_ = 0;                 // 库段日数 L
  PerSide<std::vector<Link>> link_;     // [侧][行] 库段末的链
  std::unique_ptr<PriceFiles> files_;
  PriceStamp at_;             // 交接所对的戳 (空 = 未定)
  Members mem_;               // 交接: 成员表 (库轴)
  std::vector<double> level_; // 交接: [篮子] 末日点位
  misc::exec::ForkJoin pool_{misc::exec::fit(kThreads)};
};

// 读出序列的沿用: 按标的留最近 cap 个, 戳对得上才沿用
template <class T>
class Kept {
public:
  explicit Kept(size_t cap) : cap_(cap) {}
  // ref 在戳 st 下的序列: 留着 → 沿用 (提到最新); 否则 make(T &) 读 (false = 读不到 → 不留, nullptr)
  template <class F>
  std::shared_ptr<const T> get(Ref ref, const PriceStamp &st, F &&make) {
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
    Ref ref;
    PriceStamp st;
    std::shared_ptr<const T> v;
  };
  size_t cap_;
  std::vector<Item> items_; // 新 → 旧
};

} // namespace GUI::Quote
