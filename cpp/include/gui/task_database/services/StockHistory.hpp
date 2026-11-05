// 个股的历史一路 (见 Kline.hpp): 键集 (全局 id) 的基本面历史, 最新请求覆盖旧的 (gui/util/Service.hpp); 与 BasketHistory 对仗.
//   日线 = cn_stock_real_bar1d 最近 kDailyDays 个交易日 (月分片自末往前成批并行读到够), ref = pre_close;
//   分钟 = bar1m 最近 kMinuteDays 个交易日文件, 经 Bar1mDay 落到 L_1min 行后按行 row_slot 并根 (与篮子读 L_1min 同一路).
//   分片单 row group 无从下推, 读一只 = 解全市场的片; 耗时全在解码, 故全部分片在常驻线程池上一片一任务并行, 缺的键一趟读齐.
//   不复权, 无成交的行 / 根不收.
//   历史末日 = 两表落盘水位之小 (全市场口径, 不看本只有无成交).
// 缓存: 内存留最近请求过的 kKeep 只, 命中 ⇔ 其历史末日 = 此刻水位 (基本面同步过即整体失效); 新近在后, 满了逐出最久的.
#pragma once

#include "gui/task_database/services/Kline.hpp"
#include "gui/util/Service.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <thread>
#include <utility>
#include <vector>

namespace GUI::Database {

class StockHistory : public Service<StockHistory, HistoryRequest> {
  friend Service<StockHistory, HistoryRequest>;

public:
  static constexpr const char *kName = "StockHistory";
  static constexpr size_t kKeep = 64;

  StockHistory() { Start(); }
  ~StockHistory() { Stop(); }

  const HistoryPub &published() const { return x_; }

private:
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return x_; }
  bool run(HistoryRequest &req);

  HistoryPub x_;
  std::vector<std::pair<uint32_t, Kline>> keep_; // worker 私有: (全局 id, 历史), 新近在后
  misc::exec::ForkJoin pool_{static_cast<int>(std::max(1u, std::thread::hardware_concurrency()))};
};

} // namespace GUI::Database
