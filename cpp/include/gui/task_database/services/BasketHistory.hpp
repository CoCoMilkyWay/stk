// Assets 篮子 (指数 / 行业) 的历史一路 (见 Kline.hpp): 源 = all Bar 特征库 (须已算好; 读端只读库清单, 见 shared/FeatureLib.hpp); 与 StockHistory 对仗.
//
// 篮子: 指数 = 全A 等权 / 全A 市值加权 / 各子池等权 (池表 feature_pools); 行业 = 全A 内按申万一级 (ind_l1) 切, 市值加权.
// 交易日 D 的成员 / 权重 / 基准一律取库内前一交易日 (D−1) 的 L_1day 收盘行:
//   成员: 全A = cs_valid, 子池 = pool_<池> > 0.5, 行业 = 全A ∧ ind_l1 = k; 市值权 = D−1 收盘价 × total_shares (该行 = D 的股本).
//   基准 = D−1 收盘价折到 D 的除权口径 (× adj_factor 两行之比; 该列行值 = 次日因子).
// 逐日一遍 Fold: 日根 = L_1day open_day / high_day / low_day / close_day / amount_day, 最近 kDailyDays 个已落盘日, 首日前收 = kBase;
//   分钟根 = 最末 kMinuteDays 日的 L_1min (竞价段并入首根).
// 历史末日 = 库末日. 当日 (库末日之后的行情日) 由 QuoteService::today 按 Baskets::live 合成: 成员 / 权重 = 库末日收盘行, 基准 = 新浪昨收.
#pragma once

#include "gui/task_database/services/Kline.hpp"
#include "gui/task_database/services/QuoteService.hpp"
#include "gui/util/Service.hpp"
#include "shared/FeatureLib.hpp"

#include <memory>
#include <string>
#include <vector>

namespace GUI::Database {

struct Basket {
  std::string name;
  bool industry = false;
  Kline kline;
  size_t size = 0; // 当日成员数
};

struct Baskets {
  std::vector<Basket> list; // 指数在前, 行业在后; 落盘不足 3 日 → 空
  Members live;             // 当日成员表 (下标 = 新浪全市场)
};

struct BasketRequest {
  uint32_t gen = 0;
  FeatureLib lib; // all Bar 库清单 (ready)
};

struct BasketKline : Published {
  uint32_t gen = 0;
  std::shared_ptr<const Baskets> data;
};

class BasketHistory : public Service<BasketHistory, BasketRequest> {
  friend Service<BasketHistory, BasketRequest>;

public:
  static constexpr const char *kName = "BasketHistory";
  static constexpr double kBase = 1000;

  explicit BasketHistory(const QuoteService &live) : live_(live) { Start(); }
  ~BasketHistory() { Stop(); }

  const BasketKline &published() const { return x_; }

private:
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return x_; }
  bool run(BasketRequest &req);

  const QuoteService &live_;
  BasketKline x_;
};

} // namespace GUI::Database
