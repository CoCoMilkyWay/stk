// 篮子 (指数 / 行业 / 概念) 的历史一路 (见 Quote.hpp): 源 = all Bar 特征库 (请求即重读库清单, 见 features/Storage/FeatureLib.hpp); 与 StockHistory 对仗.
//
// 篮子 = shared/Basket.hpp 的篮子表 (切片 × 该切片的权重, 与 PortfolioService 可成交指数同一张): 切片 = 全A / 申万一级行业 (全A 内按 ind_l1 切) / 各子池 (池表 feature_pools)
//   / 概念 (全A 内按概念表切, shared/Concept.hpp). 这里是价格指数 (不看可成交): NAV 日内栏 / 理想线 / 近况同出于此.
// 交易日 D 的成员 / 权重出自选股表与其原料带 (gui/quote/BasketRow.hpp members, 与 Style 页篮子账同一份), 基准取库内前一交易日 (D−1) 的 L_1day 行:
//   成员: 全A = D−1 行 cs_valid ∧ 收盘价 > 0, 行业 = 全A ∧ ind_l1 = k, 概念 = 全A ∧ 在概念表该概念成分里, 子池 = 选股表 D 的成员 (features/Panel/PoolSelect.hpp Picks);
//   权 = 源行目标权 (等权 1 / 加权 收盘价 × total_shares; 缺股本 → 只进等权) 随价漂移到 D−1 收盘 (日频 = D−1 的目标权; 周频子池一周内漂移).
//   基准 = D−1 行 pre_close (该列行值 = 次日除权后前收, 与新浪昨收同一个量).
// 日根 = L_1day open_day / high_day / low_day / close_day / amount_day; 点位自库首个落盘日 kBase 起逐日接续 (一日合成 = BasketRow Lane, 按日序串行);
//   分钟根 = 末 kMinuteDays 个落盘日的 L_1min, 按行 row_slot 并根 (与个股读 bar1m 同一路).
//   各日文件互不相干 ⇒ 日根行每批 2 × 线程数日在常驻线程池上一日一任务并行读.
// 窗: 只留末 kDailyDays 个落盘日的日根 / 末 kMinuteDays 个落盘日的分钟根 (按日期截, 不按根数).
// 续算: 全表 Book 常驻 worker, 落盘 <库>/cache/baskets.bin (头 = 库 A 轴 + 概念表 / 池表指纹 + 篮子数 + 根布局; 随库清库而没).
//   Book 的已合成日是选股表日轴的前缀 (pool::extends) 且头对得上 → 只合成其后的新日 (重读其末日的行取基准), 否则从库首日重合成;
//   续算 ≡ 全量 (逐日同一份 Lane, 窗按日期截). 合成了新日才落盘.
// 补日 (库末日之后至基本面 bar1d / bar1m 末日, 不落盘, 库末日 / 补日 / 点位不变即沿用): 成员 / 池 / 行业 / 股本冻结在库末日,
//   日根 / 基准 = bar1d open … close / amount / pre_close, 分钟根 = bar1m; 权逐日过 BasketRow drift (加权随价, 等权逢换源回 1, 否则随价).
// 历史末日 = 末个补日 (无补日 = 库末个落盘日). 当日 (之后的行情日): 跑完把末日的成员表 (下标 = QuoteService 全市场) 与各篮子末日点位
//   bind 给 QuoteService, 由它每轮快照合成 (基准 = 新浪昨收); 先 bind 后发布. 发布 = 请求键集 (basket_line) 的子表 (Book + 补日)
//   + 该成员表各篮子的只数 / Σ 权 / 成员总市值 (Tape::members / weight / cap) / 成分 (Tape::cons, NAV 右表展开) + 键集两两的成员重合 (Tape::overlap).
//   无补日 → 子表与 Book 共享各篮子的 Kline (不拷; Book 改前先独占, 见 own); 有补日 → 逐篮子拼一份.
#pragma once

#include "gui/quote/Quote.hpp"
#include "gui/quote/services/QuoteService.hpp"
#include "gui/util/Service.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

struct FeatureLib;
class FeatureRead;
namespace pool {
struct Picks;
}

namespace GUI::Quote {

class BasketHistory : public Service<BasketHistory, HistoryRequest> {
  friend Service<BasketHistory, HistoryRequest>;

public:
  static constexpr const char *kName = "BasketHistory";
  static constexpr double kBase = 1000;

  explicit BasketHistory(QuoteService &live) : live_(live) { Start(); }
  ~BasketHistory() { Stop(); }

  const HistoryPub &published() const { return x_; }

private:
  // 篮子全表的合成进度 (续算点)
  struct Book {
    struct Head {
      uint64_t magic = 0, hash = 0, concepts = 0, pools = 0; // 库 A 轴 hash / 概念表指纹 / 池表指纹
      uint32_t A = 0, baskets = 0;
      uint32_t candle = 0, bars = 0; // 根布局
      bool operator==(const Head &) const = default;
    };
    Head head;
    std::vector<misc::Ymd> dates;             // 已合成的落盘日 (首日只定成员 / 基准, 无根)
    std::vector<double> level;                // [篮子] 末日点位
    std::vector<std::shared_ptr<Kline>> list; // [篮子] (可能与发布共享)
    template <class Ar>
    void snap(Ar &ar); // 头之后的部分
  };
  // Book 末日之后的补日
  struct Tail {
    misc::Ymd from = 0;           // Book 末日
    std::vector<double> base;     // [篮子] Book 末日点位
    std::vector<misc::Ymd> dates; // 补日 (升序)
    std::vector<double> level;    // [篮子] 末补日点位
    std::vector<Kline> list;      // [篮子] 补日的根
    Members mem;                  // 末补日之后那个交易日的成员表 (库轴)
  };

  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return x_; }
  bool run(HistoryRequest &req);
  // book_ 合成到选股表日轴末日; mem = 末日之后那个交易日的成员表 (库轴). false = 取消 / 判废
  bool extend(const FeatureLib &lib, const FeatureRead &rd, const pool::Picks &pk, Members &mem);
  // tail_ 补到基本面末日; mem: 入 = Book 末日之后那个交易日的成员表, 出 = 末补日之后那个交易日的. false = 取消
  bool fill(const FeatureLib &lib, Members &mem);
  // lack 空 → axis = 成员表的库轴 (成分的全局 id)
  bool publish(const HistoryRequest &req, const std::string &lack, const PoolAxis *axis = nullptr);

  QuoteService &live_;
  HistoryPub x_;
  Book book_; // worker 私有
  Tail tail_; // worker 私有
  misc::exec::ForkJoin pool_{static_cast<int>(std::max(1u, std::thread::hardware_concurrency()))};
};

} // namespace GUI::Quote