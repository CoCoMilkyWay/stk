// 行情的历史一路 (见 Quote.hpp): 一侧一个 (个股 / 篮子同一骨架), 键集 → 各键的历史 (Tape), 最新请求覆盖旧的 (gui/util/Service.hpp).
//   源 = PriceLib (日脊 = 库段 + 补段, 两侧同一条): 每键 = 日脊末 kDailyDays 日的日根 + 末 kMinuteDays 日的分钟根 (有效价, 带净流入);
//     按 (键, 库戳) 沿用 (Kept), 库变了 (新日 / 补段 / 重算) 整体失效.
//   个股: 键 = 全局 id; 库轴外 (库建成之后上市 / 进池) 无历史 (nullptr, 只画当日), all.bar 全量重算之后才有. 至多 kKeep 只.
//   篮子: 键 = 篮子下标 (shared/Basket.hpp 的篮子表: 各子池 / 申万一级行业 / 概念 × 该切片的权重; 价格指数, 不看可成交).
//     另带交接 (PriceLib::handoff: 末个历史日之后那个交易日的成员表) 在键集上的普查 (BasketRow census: 只数 / Σ 权 / 成员总市值 / 成分 / 两两的成员重合),
//     并把它 (下标换成 QuoteService 全市场) 与各篮子末日点位 bind 给 QuoteService, 由它每轮快照合成当日 (基准 = 新浪昨收); 先 bind 后发布.
//   发布 = Tape (键集同序) + 日脊; 源未就绪 → lack (各键 nullptr).
#pragma once

#include "gui/quote/PriceLib.hpp"
#include "gui/quote/Quote.hpp"
#include "gui/quote/services/QuoteService.hpp"
#include "gui/util/Service.hpp"

#include <cstddef>

namespace GUI::Quote {

class History : public Service<History, HistoryRequest> {
  friend Service<History, HistoryRequest>;

public:
  static constexpr const char *kName = "History";
  static constexpr size_t kKeep = 64; // 个股键集的上限 (= 留在内存的序列数)

  History(Side s, PriceLib &lib, QuoteService &live);
  ~History() { Stop(); }

  const HistoryPub &published() const { return x_; }

private:
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return x_; }
  bool run(HistoryRequest &req);
  void bind(const Handoff &h); // 交接 → QuoteService (成员下标: 库轴 → 全市场)

  const Side side_;
  PriceLib &lib_;
  QuoteService &live_;
  HistoryPub x_;
  Kept<Kline> kept_; // worker 私有
};

} // namespace GUI::Quote
