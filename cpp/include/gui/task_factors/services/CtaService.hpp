// CtaService — Factors→Portfolio CTA 页的单 worker: 选中的一个标的 (篮子线 / 个股) 的 1 分钟后复权价 → 单资产多空 band 模型的滚动调参全期轨迹 (cta/Trace.hpp)
//   价格源 = CtaLib (篮子与个股一趟合成落盘, 每次请求先补到库末日); 有效根序列按标的留最近 kKeep 个 (只改设定不重读)
//   请求 = 标的 + CtaSpec (或 kCtaNone = 只备价格库); 新请求覆盖旧的 (换标的 / 改设定即重算). 进度: Loading = 合成天, Running = 网格配置数
// 线程模型 (gui/util/Service.hpp, 随对象): UI 持 mutex 读 result (shared_ptr 拷出即放锁)
#pragma once

#include "cta/Trace.hpp"
#include "gui/task_database/services/Kline.hpp" // Side
#include "gui/task_factors/services/CtaLib.hpp"
#include "gui/util/Service.hpp"
#include "misc/exec.hpp"
#include "shared/Progress.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct Config;

namespace GUI::Factors {

// 模型与回测设定 (UI 编辑, 进请求)
struct CtaSpec {
  size_t goal = cta::kSplit;                  // 优化目标 (cta::kGoals 下标)
  cta::Kinds kind{};                          // [部件] 所选模型 (cta::kParts[j].kinds 下标)
  int roll = static_cast<int>(cta::kMinRoll); // 滚动窗 (日, ≥ kMinRoll)
  float commission_bp = 1;                    // 佣金率 [bp] (买卖各一次)
  float stamp_bp = 5;                         // 印花税率 [bp] (卖出单边)
  float impact_bp = 5;                        // 冲击率 [bp] (买卖各一次)
  float vol = 0;                              // 目标年化波动 (0 = 不调)
  float buffer = 0;                           // 不交易带宽
  bool operator==(const CtaSpec &) const = default;
};

inline constexpr uint32_t kCtaNone = UINT32_MAX; // 请求的 key: 无标的, 只备价格库

struct CtaRequest {
  uint32_t gen = 0;
  Database::Side side = Database::kBasket;
  uint32_t key = 0; // 篮子 = basket_line, 个股 = 全局 id; kCtaNone = 只备价格库
  CtaSpec spec;
};

struct CtaResult {
  uint32_t gen = 0; // = 请求的
  Database::Side side = Database::kBasket;
  uint32_t key = 0;
  CtaSpec spec;
  std::string lack;                      // 算不了的原因 (空 = 有轨迹)
  std::vector<misc::Ymd> dates;          // 日轴: 有效根的日下标 = t / kSlots
  std::shared_ptr<const cta::Series> se; // 有效根
  cta::Track tr;                         // [有效根] 轨迹 (策略 / 持有两本账)
};

class CtaService : public Service<CtaService, CtaRequest>, public Published {
  friend Service<CtaService, CtaRequest>;

public:
  static constexpr const char *kName = "CtaWorker";
  explicit CtaService(const Config &cfg) : cfg_(cfg) { Start(); }
  ~CtaService() { Stop(); }

  std::shared_ptr<const CtaResult> result; // UI 持 mutex 读; 空 = 未算

private:
  static constexpr Phase kStart = Phase::Loading;
  static constexpr size_t kKeep = 4; // 留在内存的标的序列数
  static constexpr int kThreads = 8; // 合成 / 网格线程 (合成: 每线程一日的分钟读 + 篮子累加 + 个股原价, 数十 MB; 网格: 每线程一个配置的几条根列)
  Progress &board() { return *this; }
  bool run(CtaRequest &req);
  bool publish(const CtaRequest &req, std::string lack, std::shared_ptr<CtaResult> out = nullptr);

  // 一个标的的有效根 (库轴 hash + 日轴末日 / 日数对得上才用)
  struct Kept {
    Database::Side side;
    uint32_t key;
    uint64_t axis;
    misc::Ymd last;
    size_t days;
    std::shared_ptr<const cta::Series> se;
  };

  const Config &cfg_;
  misc::exec::ForkJoin pool_{misc::exec::fit(kThreads)};
  CtaLib lib_{cancel_, *this, pool_};
  std::vector<Kept> kept_; // 新 → 旧
};

} // namespace GUI::Factors
