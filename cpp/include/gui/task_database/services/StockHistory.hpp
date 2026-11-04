// Assets 个股的历史一路 (见 Kline.hpp): 选中标的的基本面历史, 最新请求覆盖旧的 (gui/util/Service.hpp); 与 BasketHistory 对仗.
//   日线 = cn_stock_real_bar1d 最近 kDailyDays 个交易日 (月分片从末往前读到够), ref = pre_close;
//   分钟 = bar1m 最近 kMinuteDays 个交易日文件, 经 Bar1mDay 落到 L_1min 行后按行 row_slot 并根 (与篮子读 L_1min 同一路).
//   不复权, 无成交的行 / 根不收.
//   历史末日 = 两表落盘水位之小 (全市场口径, 不看本只有无成交).
#pragma once

#include "gui/task_database/services/Kline.hpp"
#include "gui/util/Service.hpp"

#include <string>

namespace GUI::Database {

struct StockRequest {
  uint32_t gen = 0;
  std::string code; // "600000.SH"
};

using StockKline = HistoryPub<Kline>;

class StockHistory : public Service<StockHistory, StockRequest> {
  friend Service<StockHistory, StockRequest>;

public:
  static constexpr const char *kName = "StockHistory";

  StockHistory() { Start(); }
  ~StockHistory() { Stop(); }

  const StockKline &published() const { return x_; }

private:
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return x_; }
  bool run(StockRequest &req);

  StockKline x_;
};

} // namespace GUI::Database
