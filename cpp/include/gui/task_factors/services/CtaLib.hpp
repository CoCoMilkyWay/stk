// CtaLib — CTA 页的分钟价格库 (CtaService worker 私有): 篮子 (指数 / 行业 / 概念三组线) 与库轴全部个股, 一趟过日合成落盘, 之后只补新日
//   源 = all.bar 特征库: 交易日 D 的成员 / 权重 / 基准取 D−1 收盘行, D 的日根 + L_1min 分钟根 (口径 = BasketRow), 一日一读两者共用
//   篮子 = basket_group 三组的线, 合成核 = Database::Fold (成员表滤到这些线); 点位自库首日 1 起逐日接续 (只看相对涨跌):
//     一日按线程合成到 "前日点位 = 1", 再按日序乘前日点位
//   个股 = 库存原价 (f16, 与库逐位一致) × 后复权因子; 因子自首个有根日 1 起, 逢前收 ≠ 参考价 (除权) 乘 参考价 / 前收, 参考价 = 末个收盘 (按前收滚动)
//   时间格: 有效根 t = 日下标 · kSlots + 槽, 日下标 = 库落盘日序 (首日只定成员, 无根); 无根 (无成员 / 无成交 / 末个有根的槽之后) 不收
// 落盘 <库目录>/cta/ (日主序, 续算只追加; 库全量重算连目录一起删):
//   head.bin = Head + 线表 [B] + dates [D] + level [B] (篮子末日点位) + adj [A] / ref [A] (个股因子 / 参考价)
//   1m.bin    = f32 [D][B][kSlots][kFields] (篮子点位 高 低 收; 无根 = NaN)
//   stock.bin = [D][A] { f64 因子 (0 = 当日无根), f16 [kSlots][kFields] 原价 高 低 收 (无根 = NaN) }
//   Head 与线表对得上、两文件够长且已合成日是库落盘日的前缀 → 只合成其后的新日, 否则从头; 每批落一次头 (取消不丢已合成的)
#pragma once

#include "cta/Model.hpp" // Series
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "shared/Progress.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

struct FeatureLib;
class FeatureRead;

namespace GUI::Factors {

class CtaLib {
public:
  enum Field : int { kHigh,
                     kLow,
                     kClose,
                     kFields };

  // cancel / x / pool = 宿主服务的 (取消 / 进度面 Loading 天 / 线程池)
  CtaLib(const std::atomic<bool> &cancel, Progress &x, misc::exec::ForkJoin &pool) : cancel_(cancel), x_(x), pool_(pool) {}

  // 价格库补到 dates (= 库已落盘日, 升序, ≥ 2 日); false = 取消 / 判废
  bool sync(const FeatureLib &lib, const FeatureRead &rd, const std::vector<misc::Ymd> &dates);
  // 篮子线 line 的有效根 (sync 之后); 不在三组 → false
  bool basket(size_t line, cta::Series &se) const;
  // 个股 (库轴下标 a) 的后复权有效根 (sync 之后)
  void stock(size_t a, cta::Series &se) const;

private:
  struct Head {
    uint64_t magic = 0, axis = 0, concepts = 0; // 库 A 轴 hash / 概念表指纹
    uint32_t A = 0, B = 0;
    misc::Ymd first = 0; // 库首个落盘日
    bool operator==(const Head &) const = default;
  };
  void save_head();
  size_t A() const { return head_.A; }
  size_t B() const { return lines_.size(); }

  const std::atomic<bool> &cancel_;
  Progress &x_;
  misc::exec::ForkJoin &pool_;
  std::string dir_;
  Head head_;
  std::vector<uint32_t> lines_;  // [B] 线 → basket_line
  std::vector<uint32_t> row_;    // [basket_count] basket_line → 线 (B = 不在)
  std::vector<misc::Ymd> dates_; // [D] 已合成日 (首日只定成员, 其块无根)
  std::vector<double> level_;    // [B] 篮子末日点位
  std::vector<double> adj_;      // [A] 个股末日后复权因子 (0 = 尚无根)
  std::vector<float> ref_;       // [A] 个股参考价 (末个收盘, 按前收滚动; 0 = 尚无)
};

} // namespace GUI::Factors
