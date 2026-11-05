// 篮子 (指数 / 行业 / 概念) 的历史一路 (见 Kline.hpp): 源 = all Bar 特征库 (请求即重读库清单, 见 shared/FeatureLib.hpp); 与 StockHistory 对仗.
//
// 篮子 = shared/Basket.hpp 的篮子表 (切片 × 权重, 与 Factors 可成交指数同一张): 切片 = 全A / 申万一级行业 (全A 内按 ind_l1 切) / 各子池 (池表 feature_pools)
//   / 概念 (全A 内按概念表切, shared/Concept.hpp). 这里是价格指数 (不看可成交): Portfolio 日内栏 / 理想线 / 近况同出于此.
// 交易日 D 的成员 / 权重 / 基准一律取库内前一交易日 (D−1) 的 L_1day 收盘行:
//   成员: 全A = cs_valid, 子池 = pool_<池> > 0.5, 行业 = 全A ∧ ind_l1 = k, 概念 = 全A ∧ 在概念表该概念成分里, 均须收盘价 > 0;
//   等权 = 1, 加权 = D−1 收盘价 × total_shares (该行 = D 的股本; 缺 → 只进等权).
//   基准 = D−1 行 pre_close (该列行值 = 次日除权后前收, 与新浪昨收同一个量).
// 日根 = L_1day open_day / high_day / low_day / close_day / amount_day; 点位自库首个落盘日 kBase 起逐日接续 (Fold 按日序串行);
//   分钟根 = 末 kMinuteDays 个落盘日的 L_1min, 按行 row_slot 并根 (与个股读 bar1m 同一路).
//   各日文件互不相干 ⇒ 收盘行每批 2 × 线程数日在常驻线程池上一日一任务并行读.
// 窗: 只留末 kDailyDays 个落盘日的日根 / 末 kMinuteDays 个落盘日的分钟根 (按日期截, 不按根数).
// 续算: 全表 Book 常驻 worker, 落盘 <库目录>/baskets.bin (头 = 库 A 轴 + 概念表指纹 + 篮子数 + 根布局; 随库清库而没).
//   Book 的已合成日是此刻落盘日的前缀且头对得上 → 只合成其后的新日 (重读其末日收盘行定成员), 否则从库首日重合成;
//   续算 ≡ 全量 (逐日同一份 Fold, 窗按日期截). 合成了新日才落盘.
// 补日 (库末日之后至基本面 bar1d / bar1m 末日, 不落盘, 库末日 / 补日 / 点位不变即沿用): 成员 / 池 / 行业 / 股本冻结在库末日收盘行,
//   日根 / 基准 = bar1d open … close / amount / pre_close, 分钟根 = bar1m; 加权的权重逐日 × 收 / 前收 (市值随价漂移).
// 历史末日 = 末个补日 (无补日 = 库末个落盘日). 当日 (之后的行情日): 跑完把末日的成员表 (下标 = QuoteService 全市场) 与各篮子末日点位
//   bind 给 QuoteService, 由它每轮快照合成 (基准 = 新浪昨收); 先 bind 后发布. 发布 = 请求键集 (basket_line) 的子表 (Book + 补日).
#pragma once

#include "gui/task_database/services/Kline.hpp"
#include "gui/task_database/services/QuoteService.hpp"
#include "gui/util/Service.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <cstdint>
#include <thread>
#include <vector>

struct Config;
struct FeatureLib;
class FeatureRead;

namespace GUI::Database {

class BasketHistory : public Service<BasketHistory, HistoryRequest> {
  friend Service<BasketHistory, HistoryRequest>;

public:
  static constexpr const char *kName = "BasketHistory";
  static constexpr double kBase = 1000;

  BasketHistory(const Config &cfg, QuoteService &live) : cfg_(cfg), live_(live) { Start(); }
  ~BasketHistory() { Stop(); }

  const HistoryPub &published() const { return x_; }

private:
  // 篮子全表的合成进度 (续算点)
  struct Book {
    struct Head {
      uint64_t magic = 0, hash = 0, concepts = 0; // 库 A 轴 hash / 概念表指纹
      uint32_t A = 0, baskets = 0;
      uint32_t candle = 0, bars = 0; // 根布局
      bool operator==(const Head &) const = default;
    };
    Head head;
    std::vector<misc::Ymd> dates; // 已合成的落盘日 (首日只定成员 / 基准, 无根)
    std::vector<double> level;    // [篮子] 末日点位
    std::vector<Kline> list;      // [篮子]
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
    Members mem;                  // 末补日收盘定的成员表 (库轴)
  };

  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return x_; }
  bool run(HistoryRequest &req);
  // book_ 合成到 dates 末日; mem = 末日收盘行定的成员表 (库轴). false = 取消 / 判废
  bool extend(const FeatureLib &lib, const FeatureRead &rd, const std::vector<misc::Ymd> &dates, Members &mem);
  // tail_ 补到基本面末日; mem: 入 = Book 末日的成员表, 出 = 末补日的. false = 取消
  bool fill(const FeatureLib &lib, Members &mem);
  bool publish(const HistoryRequest &req, const char *lack);

  const Config &cfg_;
  QuoteService &live_;
  HistoryPub x_;
  Book book_; // worker 私有
  Tail tail_; // worker 私有
  misc::exec::ForkJoin pool_{static_cast<int>(std::max(1u, std::thread::hardware_concurrency()))};
};

} // namespace GUI::Database
