#include "live/tdx/Tdx.hpp"

#include "live/Trace.hpp"
#include "live/tdx/Credit.hpp"
#include "live/tdx/Flash.hpp"
#include "live/tdx/Flow.hpp"
#include "live/tdx/Heap.hpp"
#include "live/tdx/Req.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Tables.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Win.hpp"

#include <chrono>
#include <mutex>
#include <string_view>
#include <tuple>

namespace live::tdx {

namespace {

class Tdx final : public Driver {
public:
  std::string connect() override {
    int pid;
    {
      std::lock_guard wk(work_);
      if (!heap_ || heap_->pid() != mem::find_pid())
        heap_ = mem::attach();
      if (!shim::online())
        win::attach();
      req::arm(heap_);
      pid = heap_->pid();
    }
    prime(tables::kReads);
    return strf("tdxw.exe pid %d", pid);
  }

  Fills fills() override { return tables::read<tables::Fills>(); }
  Keyed<Position> pos() override { return tables::read<tables::Positions>(); }
  Keyed<Pending> pends() override { return tables::read<tables::Pendings>(); }
  Account acct() override { return credit::acct(); }

  Terms terms(const Report &r) override {
    prime(tables::kTerms);
    return credit::terms(r);
  }
  std::vector<CashFlow> flows(misc::Ymd from, const Report &r) override {
    prime(tables::kFlows);
    return flow::flows(from, r);
  }
  Placed place(const OrderReq &o, const Report &r) override {
    const auto g = gate();
    return flash::place(o, r);
  }

  const std::vector<std::string> &steps() const override { return flash::steps(); }

private:
  // 占界面锁; 被占着即等, 记下等了多久
  std::unique_lock<std::mutex> gate() {
    std::unique_lock g(gate_, std::try_to_lock);
    if (!g) {
      const auto t0 = std::chrono::steady_clock::now();
      g.lock();
      trace::ev("gate", {{"ms", trace::ms_since(t0)}});
    }
    return g;
  }

  // 引导一组表 S…: 插件还没发过的各经界面查一次 (其引导页), 末了回持仓页 (持仓缺也由它补)
  template <class... S>
  void prime(std::tuple<S...>) {
    std::vector<ui::Open> miss;
    ((tables::seen<S>() ? void() : miss.push_back(S::kOpen)), ...);
    if (miss.empty())
      return;
    std::erase_if(miss, [](const ui::Open &o) { return std::string_view(o.page) == tables::Positions::kOpen.page; });
    miss.push_back(tables::Positions::kOpen);
    const auto g = gate();
    trace::Span span("引导");
    ui::show_bar();
    for (const ui::Open &o : miss)
      std::ignore = ui::open(o);
    (need(tables::seen<S>(), strf("经界面查过「%s」, 插件仍没发过其功能", S::kName)), ...);
  }

  std::mutex gate_, work_; // 界面 / 进程堆与接入; 两把不嵌套
  mem::HeapP heap_;
};

} // namespace

std::unique_ptr<Driver> make() { return std::make_unique<Tdx>(); }

} // namespace live::tdx
