// MinuteService — Market→NAV 主图的分钟价 worker: NAV 选中的有行情键的行 (篮子 / 成分股) → 各自的全史逐日分钟收盘 (MinuteCloses)
//   价格源 = MinuteLib (与 CtaService 同一段开场 open, 同一种沿用 Kept); 序列按标的留最近 kKeep 个
//   请求 = 标的集 (空 = 只备价格库); 新请求覆盖旧的. 进度: Loading = 合成天
// 线程模型 (gui/util/Service.hpp, 随对象): UI 持 mutex 读 result (latest)
#pragma once

#include "gui/quote/Quote.hpp" // Ref
#include "gui/task_market/services/MinuteLib.hpp"
#include "gui/util/Service.hpp"
#include "misc/exec.hpp"
#include "shared/Progress.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Market {

// 一个标的的逐日分钟收盘 (有根的日; 价 = 库存原价 / 日内比, 只看同日之比, 不乘因子)
struct MinuteCloses {
  std::vector<misc::Ymd> dates; // 升序
  std::vector<float> close;     // [dates][kSlots] (无根 = NaN)
  // 日 d 的 kSlots 格; 无 → nullptr
  const float *on(misc::Ymd d) const;
};

struct MinuteRequest {
  uint32_t gen = 0;
  std::vector<Quote::Ref> refs;
  bool operator==(const MinuteRequest &) const = default;
};

struct MinuteResult {
  uint32_t gen = 0;                                      // = 请求的
  std::vector<Quote::Ref> refs;                          // = 请求的
  std::vector<std::shared_ptr<const MinuteCloses>> list; // [refs]; 空 = 不在库轴 (库建成之后上市 / 进池) 或库未就绪
  std::string lack;                                      // 库未就绪的原因 (空 = 就绪)
  // ref 的分钟收盘; 无 → nullptr
  const MinuteCloses *of(Quote::Ref ref) const;
};

class MinuteService : public Service<MinuteService, MinuteRequest>, public Published {
  friend Service<MinuteService, MinuteRequest>;

public:
  static constexpr const char *kName = "MinuteWorker";
  explicit MinuteService(MinuteLib &lib) : lib_(lib) { Start(); }
  ~MinuteService() { Stop(); }

  std::shared_ptr<const MinuteResult> result; // UI 持 mutex 读; 空 = 未发布

private:
  static constexpr Phase kStart = Phase::Loading;
  static constexpr size_t kKeep = 32; // 留在内存的标的序列数
  static constexpr int kThreads = 8;  // 合成线程 (同 CtaService)
  Progress &board() { return *this; }
  bool run(MinuteRequest &req);
  bool publish(const MinuteRequest &req, std::shared_ptr<MinuteResult> out);

  MinuteLib &lib_;
  misc::exec::ForkJoin pool_{misc::exec::fit(kThreads)};
  Kept<MinuteCloses> kept_{kKeep};
};

} // namespace GUI::Market
