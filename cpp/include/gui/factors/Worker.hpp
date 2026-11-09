// Worker — 因子服务骨架 FactorWorker (Run / Search / Inspect / Portfolio / Lens 同一份): 进度面 = 自身 + 本轮说明 + 计算句柄 FactorFlow
//   (首个请求懒建, 随 worker 活; 计算面是进程唯一的 Store, 按 prio 排队取计算权) + 结局收尾 settle.
//   成员函数定义在 Engine.hpp (FactorFlow 完整处); 派生类的 ctor / dtor 须在 .cpp
#pragma once

#include "gui/util/Service.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace GUI::Factors {

enum class Rc : uint8_t;   // Scope.hpp
enum class Prio : uint8_t; // Load.hpp
struct FactorFlow;         // Engine.hpp

template <class D, class Req>
class FactorWorker : public Service<D, Req>, public Published {
public:
  std::string message; // 本轮说明 (如 "特征库无数据"), 空 = 无; UI 持 mutex 读

protected:
  explicit FactorWorker(Prio prio) : prio_(prio) {}
  Progress &board() { return *this; }
  FactorFlow &flow();
  // 作业结局 (全部服务同一份; 须持租约): 中断 (Cancel / Stale) → false (Stale: 弃平面缓存 + 置 message); Ok / Empty / Broken → true
  // (Empty / Broken 是正常输入, 原因已由管线交给条目, 或由调用方取 Job::why)
  bool settle(Rc rc);

private:
  const Prio prio_; // 计算权优先级 (交互服务 Interactive, 批服务 Batch)
  std::unique_ptr<FactorFlow> flow_;
};

} // namespace GUI::Factors