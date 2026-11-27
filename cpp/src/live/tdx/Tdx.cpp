#include "live/tdx/Tdx.hpp"

#include "live/Trace.hpp"
#include "live/tdx/Flash.hpp"
#include "live/tdx/Heap.hpp"
#include "live/tdx/Repay.hpp"
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

// 接上客户端 (Driver 与画面复现同一路, 一次一路): 进程换了即重连 shim 通道、shim 不在线即注入、请求层钩上 → pid
int attach() {
  static std::mutex mu;
  static int pid = 0;
  std::lock_guard lk(mu);
  const int p = mem::find_pid();
  need(p != 0, "找不到 tdxw.exe 进程");
  if (p != pid) {
    shim::reset();
    pid = p;
  }
  if (!shim::online())
    win::attach();
  req::arm(pid);
  return pid;
}

// 画面复现: 窗口树 / 绘制层 / 控件子项 / 输入透传, 各经 shim (gdiwatch 走独立推流连接)
class TdxMirror final : public Mirror {
public:
  void ensure() override { attach(); }
  std::vector<WinNode> tree() override { return win::tree(); }
  std::vector<DrawLayer> gdi_dump() override { return win::gdidump(); }
  DrawLayer detail(const std::string &hwnd) override { return win::detail(hwnd); }
  int64_t tree_gen() override { return win::treegen(); }
  void watch_gdi(const std::function<bool(DrawLayer)> &cb) override {
    shim::Stream st("gdiwatch");
    nlohmann::json j;
    while (true) {
      if (!st.next(j, 0.25)) {
        if (!cb(DrawLayer{})) // 心跳让调用方检查停标志
          return;
        continue;
      }
      if (!cb(win::layer_of(j)))
        return;
    }
  }
  void mouse(const char *kind, int x, int y, unsigned mk, int delta) override { win::mouse(kind, x, y, mk, delta); }
  void key(unsigned vk) override { win::key(vk); }
  void ch(unsigned cp) override { win::ch(cp); }
};

// 通达信的 Driver (见 Tdx.hpp)
class Tdx final : public Driver {
public:
  std::string connect() override {
    const int pid = attach();
    prime(tables::kReads);
    return strf("tdxw.exe pid %d", pid);
  }

  Snapshot read() override { return tables::reads(); }

  Terms terms() override {
    prime(tables::kTerms);
    return tables::terms();
  }
  Ledger ledger(misc::Ymd from) override {
    prime(tables::kLedger);
    return tables::ledger(from);
  }
  Placed place(const OrderReq &o) override {
    const auto g = gate();
    return flash::place(o);
  }
  std::vector<Repaid> repay(const RepayReq &r) override {
    const auto g = gate();
    ui::show_bar();
    return tdx::repay::run(r);
  }
  std::vector<std::string> steps() const override { return flash::steps(); }
  std::unique_ptr<Mirror> mirror() override { return std::make_unique<TdxMirror>(); }

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
    ((tables::seen_func<S>() ? void() : miss.push_back(S::kOpen)), ...);
    if (miss.empty())
      return;
    std::erase_if(miss, [](const ui::Open &o) { return std::string_view(o.page) == tables::Positions::kOpen.page; });
    miss.push_back(tables::Positions::kOpen);
    const auto g = gate();
    trace::Span span("引导");
    ui::show_bar();
    for (const ui::Open &o : miss)
      std::ignore = ui::open(o);
    (need(tables::seen_func<S>() != 0, "经界面查过「%s」, 插件仍没发过其功能", S::kName), ...);
  }

  std::mutex gate_; // 界面锁; 与接入的锁不嵌套
};

} // namespace

std::unique_ptr<Driver> make() { return std::make_unique<Tdx>(); }

} // namespace live::tdx
