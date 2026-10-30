// FactorsWorker — 因子服务骨架 FactorWorker (Run / Search / Inspect / Portfolio 同一份): 进度面 = 自身 + 本轮说明 + worker 常驻件 FactorFlow
//   (首个请求懒建, 随 worker 活) + 结局收尾 settle. 成员函数定义在 FactorsEngine.hpp (FactorFlow 完整处); 派生类的 ctor / dtor 须在 .cpp
#pragma once

#include "gui/util/Service.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace GUI::Factors {

enum class Rc : uint8_t; // FactorsScope.hpp
struct FactorFlow;       // FactorsEngine.hpp

template <class D, class Req>
class FactorWorker : public Service<D, Req>, public Published {
public:
  std::string message; // 本轮说明 (如 "特征库无数据"), 空 = 无; UI 持 mutex 读

protected:
  Progress &board() { return *this; }
  FactorFlow &flow();
  // 作业结局 (四服务同一份): 中断 (Cancel / Stale) → false (Stale: 弃块 + 置 message); Ok / Empty / Broken → true
  // (Empty / Broken 是正常输入, 原因已由管线交给条目, 或由调用方取 Job::why)
  bool settle(Rc rc);

private:
  std::unique_ptr<FactorFlow> flow_;
};

} // namespace GUI::Factors
