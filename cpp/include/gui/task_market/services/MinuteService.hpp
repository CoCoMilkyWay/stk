// MinuteService — Market→NAV 主图的分钟价 worker: NAV 选中的有行情键的行 (篮子 / 成分股) → 各自的全史逐日分钟收盘 (MinuteLib::closes)
//   价格源 = MinuteLib (与 CtaService 共用; 每次请求先补到库末日); 序列按标的沿用上一份 (库轴 hash + 日轴末日 / 日数对得上), 只读新加的标的
//   请求 = 标的集; 新请求覆盖旧的. 进度: Loading = 合成天
// 线程模型 (gui/util/Service.hpp, 随对象): UI 持 mutex 读 result (shared_ptr 拷出即放锁)
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

struct MinuteRequest {
  uint32_t gen = 0;
  std::vector<Quote::Ref> refs;
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
  static constexpr int kThreads = 8; // 合成线程 (同 CtaService)
  Progress &board() { return *this; }
  bool run(MinuteRequest &req);
  bool publish(std::shared_ptr<MinuteResult> out);

  // 一个标的的分钟收盘 (库轴 hash + 日轴末日 / 日数对得上才沿用)
  struct Kept {
    Quote::Ref ref;
    uint64_t axis;
    misc::Ymd last;
    size_t days;
    std::shared_ptr<const MinuteCloses> mc;
  };

  MinuteLib &lib_;
  misc::exec::ForkJoin pool_{misc::exec::fit(kThreads)};
  std::vector<Kept> kept_; // = 上一份发布的
};

} // namespace GUI::Market
