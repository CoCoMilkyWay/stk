// Assets 篮子 (指数 / 行业) 的历史一路 (见 Kline.hpp): 源 = all Bar 特征库 (须已算好; 读端只读库清单, 见 shared/FeatureLib.hpp); 与 StockHistory 对仗.
//
// 篮子 = shared/Basket.hpp 的篮子表 (切片 × 权重, 与 Factors 可成交指数同一张): 切片 = 全A / 申万一级行业 (全A 内按 ind_l1 切) / 各子池 (池表 feature_pools).
//   这里是价格指数 (不看可成交): Assets 篮子行情 / Portfolio 理想线 / 近况同出于此.
// 交易日 D 的成员 / 权重 / 基准一律取库内前一交易日 (D−1) 的 L_1day 收盘行:
//   成员: 全A = cs_valid, 子池 = pool_<池> > 0.5, 行业 = 全A ∧ ind_l1 = k, 均须收盘价 > 0; 等权 = 1, 加权 = D−1 收盘价 × total_shares (该行 = D 的股本; 缺 → 只进等权).
//   基准 = D−1 行 pre_close (该列行值 = 次日除权后前收, 与新浪昨收同一个量).
// 日根 = L_1day open_day / high_day / low_day / close_day / amount_day, 最近 kDailyDays 个已落盘日, 首日前收 = kBase;
//   分钟根 = 最末 kMinuteDays 日的 L_1min, 按行 row_slot 并根 (与个股读 bar1m 同一路).
//   各日文件互不相干 ⇒ 收盘行每批 2 × 线程数日在常驻线程池上一日一任务并行读; 点位逐日接续 ⇒ Fold 按日序串行.
// 历史末日 = 库末日. 当日 (库末日之后的行情日): 跑完把库末日收盘行定的成员表 (下标 = QuoteService 全市场) 与各篮子末日点位 bind 给 QuoteService,
//   由它每轮快照合成 (基准 = 新浪昨收); 先 bind 后发布.
#pragma once

#include "gui/task_database/services/Kline.hpp"
#include "gui/task_database/services/QuoteService.hpp"
#include "gui/util/Service.hpp"
#include "misc/exec.hpp"
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <string>
#include <thread>
#include <vector>

namespace GUI::Database {

struct Basket {
  std::string name; // basket_name
  Kline kline;
  size_t size = 0; // 当日成员数
};

struct Baskets {
  std::vector<Basket> list; // [basket_line(c, w)]; 落盘不足 2 日 → 空
};

struct BasketRequest {
  uint32_t gen = 0;
  FeatureLib lib; // all Bar 库清单 (ready)
};

using BasketKline = HistoryPub<Baskets>;

class BasketHistory : public Service<BasketHistory, BasketRequest> {
  friend Service<BasketHistory, BasketRequest>;

public:
  static constexpr const char *kName = "BasketHistory";
  static constexpr double kBase = 1000;

  explicit BasketHistory(QuoteService &live) : live_(live) { Start(); }
  ~BasketHistory() { Stop(); }

  const BasketKline &published() const { return x_; }

private:
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return x_; }
  bool run(BasketRequest &req);

  QuoteService &live_;
  BasketKline x_;
  misc::exec::ForkJoin pool_{static_cast<int>(std::max(1u, std::thread::hardware_concurrency()))};
};

} // namespace GUI::Database
