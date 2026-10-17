#include "worker/feature_workers.hpp"

#include "features/Backend/FeatureStore.hpp"
#include "features/Bar1mDay.hpp"
#include "features/CoreBar.hpp"
#include "misc/affinity.hpp"
#include "misc/exec.hpp"
#include "misc/logging.hpp"
#include "shared/AssetAxis.hpp"

#include <cassert>
#include <cstdio>
#include <future>
#include <memory>
#include <string>
#include <vector>

// 本 TU precise-math (CMakeLists PRECISE 表): Fund 缺失值 / PoolKey 母集判定 / cs_valid 依赖 NaN 比较语义.

void bar_worker(BarWorkerCtx ctx) {
  AllFeatureStore &store = ctx.store;
  const size_t A = store.query_A();
  const size_t D = ctx.dates.size();
  const int n_threads = static_cast<int>(ctx.ts_cpus.size());
  assert(n_threads >= 1 && D >= 1 && A == asset_axis().size());

  misc::exec::ForkJoin ex(n_threads);
  // 每 tid pin 一个逻辑 CPU (rows(n) 静态条带: tid 恰得 [tid, tid + 1))
  if (misc::Affinity::supported())
    ex.rows(n_threads, [&](int, int, int tid) {
      const bool pinned = misc::Affinity::pin_to_core(static_cast<unsigned int>(ctx.ts_cpus[static_cast<size_t>(tid)]));
      assert(pinned && "All TS 线程 pin 失败");
      (void)pinned;
    });

  // per-asset 跨日状态: 与日循环同一静态条带构造 → first-touch 在其后每日处理它的 tid 上
  std::vector<std::unique_ptr<CoreBar>> cores(A);
  ex.rows(static_cast<int>(A), [&](int a0, int a1, int) {
    for (int a = a0; a < a1; ++a)
      cores[static_cast<size_t>(a)] = std::make_unique<CoreBar>(ctx.fund_pool, asset_axis().code(static_cast<size_t>(a)), static_cast<size_t>(a));
  });

  // bar1m 解码领先一日 (双缓冲: 日 d 用 bufs[d & 1], 解码线程同时写 bufs[(d + 1) & 1])
  Bar1mDay bufs[2];
  auto decode = [&](size_t d) {
    return std::async(std::launch::async, [&, d] {
      if (misc::Affinity::supported())
        misc::Affinity::pin_to_core(static_cast<unsigned int>(ctx.decode_cpu));
      bufs[d & 1].load(ctx.catalog, ctx.dates[d], A);
    });
  };
  std::future<void> next = decode(0);

  ComputeStats::Core stat; // 干等记账 (All 档无仪表盘, 只占位)
  size_t missing = 0, done = 0;
  for (size_t d = 0; d < D; ++d) {
    if (ctx.cancel.load(std::memory_order_relaxed))
      break;
    next.get();
    const Bar1mDay &bars = bufs[d & 1];
    if (d + 1 < D)
      next = decode(d + 1);
    missing += !bars.present;

    const AllFeatureStore::Day day = wait_open(ctx.cancel, stat, [&] { return store.ts_try_open(d); });
    if (!day)
      break;
    const misc::Ymd date = ctx.dates[d];
    ex.rows(static_cast<int>(A), [&](int a0, int a1, int) {
      for (int a = a0; a < a1; ++a)
        cores[static_cast<size_t>(a)]->run_day(date, day, bars.asset(static_cast<size_t>(a)), store);
    });
    done = d + 1;

    std::printf("\r[All] TS %zu/%zu (%d) bar1m %zu 资产%s | CS %zu | IO %zu   ", done, D, date, bars.n_assets,
                bars.present ? "" : " (缺)", store.query_cs_done(), store.query_io_done());
    std::fflush(stdout);
  }

  // 区间末: 悬挂日 (开盘档标签) 按最近收盘价结清, 归还句柄 → 尾部各日计满, CS / IO 收尾
  if (done == D)
    ex.rows(static_cast<int>(A), [&](int a0, int a1, int) {
      for (int a = a0; a < a1; ++a)
        cores[static_cast<size_t>(a)]->finish_all(store);
    });

  std::printf("\n[All] TS %s: %zu/%zu 日, 缺 bar1m %zu 日\n", done == D ? "完成" : "取消", done, D, missing);
  Logger::log("bar_worker", "TS " + std::to_string(done) + "/" + std::to_string(D) + " days, missing bar1m " + std::to_string(missing));
}
