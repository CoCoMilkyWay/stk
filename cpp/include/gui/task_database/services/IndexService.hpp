// Assets → 指数 / 行业: 各池与申万一级行业的合成指数, 源 = all Bar 特征库 (须已算好; 读端只读库清单, 见 shared/FeatureLib.hpp)
//
// 篮子: 指数 = 全A 等权 / 全A 市值加权 / 各子池等权 (池表 feature_pools); 行业 = 全A 内按申万一级 (ind_l1) 切, 市值加权.
// 交易日 D 的成员 / 权重 / 基准一律取库内前一交易日 (D−1) 的 L_1day 收盘行:
//   成员: 全A = cs_valid, 子池 = pool_<池> > 0.5, 行业 = 全A ∧ ind_l1 = k; 市值权 = D−1 收盘价 × total_shares (该行 = D 的股本).
//   基准 = D−1 收盘价折到 D 的除权口径 (× adj_factor 两行之比; 该列行值 = 次日因子).
//   点位 = 前日点位 × Σ w · 价 / 基准 / Σ w; 当日无成交的成员比 1. 开 / 高 / 低 / 收 各自如此合成
//   (高 / 低 = 成员高低的加权, 是真实路径的外包); 额 = 成员之和.
// 日线 = L_1day open_day / high_day / low_day / close_day / amount_day, 最近 kDailyDays 个已落盘日, 首日前收 = kBase;
// 日内 = 最末 kMinuteDays 日的 L_1min (竞价段并入首根, 成员缺根沿用其上根收盘), 槽同 QuoteService.
// 库末日之后的行情日 (当日) 由 QuoteService::compose 现合成: 成员 / 权重 = 库末日收盘行 (Basket::live), 基准 = 新浪昨收.
#pragma once

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
  std::vector<Candle> daily;                // 日升序
  std::vector<std::vector<Candle>> minutes; // 最末 kMinuteDays 日, 每日槽升序
  std::vector<QuoteService::Weight> live;   // 库末日之后那天的成员 / 权重
};

struct IndexData {
  misc::Ymd last = 0;          // 库末日; 0 = 落盘不足 3 日 (baskets 空)
  std::vector<Basket> baskets; // 指数在前, 行业在后
};

struct IndexRequest {
  uint32_t gen = 0;
  FeatureLib lib; // all Bar 库清单 (ready)
};

struct Indexes : Published {
  uint32_t gen = 0;
  std::shared_ptr<const IndexData> data;
};

class IndexService : public Service<IndexService, IndexRequest> {
  friend Service<IndexService, IndexRequest>;

public:
  static constexpr const char *kName = "QuoteIndex";
  static constexpr size_t kDailyDays = 270, kMinuteDays = HistoryService::kMinuteDays;
  static constexpr double kBase = 1000;

  explicit IndexService(const QuoteService &live) : live_(live) { Start(); }
  ~IndexService() { Stop(); }

  const Indexes &indexes() const { return x_; }

private:
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return x_; }
  bool run(IndexRequest &req);

  const QuoteService &live_;
  Indexes x_;
};

} // namespace GUI::Database
