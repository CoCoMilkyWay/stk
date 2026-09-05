#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/binary_decoder_L2.hpp"
#include "features/Backend/FeatureStore.hpp"
#include "lob/LimitOrderBook.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

// cores 的 unique_ptr 需要 CoreSequential 完整类型, 只在本 TU 实例化
TsSchedule::TsSchedule(size_t num_assets, size_t num_workers)
    : owner(num_assets), claimed(num_assets), done(num_assets),
      weight(num_assets, 0), cores(num_assets), adopt_window(num_workers) {
  for (size_t i = 0; i < num_assets; ++i) {
    owner[i].store(-1, std::memory_order_relaxed);
    claimed[i].store(-1, std::memory_order_relaxed);
    done[i].store(-1, std::memory_order_relaxed);
  }
  for (size_t w = 0; w < num_workers; ++w)
    adopt_window[w].store(0, std::memory_order_relaxed);
}

TsSchedule::~TsSchedule() = default;

// 领养节流: 单一阈值 N (%) = sched.adopt_pct (UI 可调, 0 = 关闭领养).
// 触发时机 = 领跑者在池边干等 slot (它已领先, 等待时间白白浪费, 正好拿来
// 帮最慢者).
//   victim 侧: 每天 (预算窗口按其前沿滚动) 最多让出自己持仓的 N% ——
//              让完这波它下一轮几乎不再是最慢者, 也不会被过度掏空;
//   leader 侧: 每天最多领养 (全市场标的数 / worker 数) 的 N% —— 单核不会
//              一天暴涨吃成新的 lagger.

void sequential_worker(WorkerCtx ctx) {
  const int worker_id = ctx.worker_id;
  SharedData &data = ctx.data;
  GlobalFeatureStore &store = ctx.store;
  TsSchedule &sched = ctx.sched;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  const misc::ProgressHandle progress_handle = std::move(ctx.progress);

  TraceNS("TSWorker", 5);
  TraceValue(worker_id);
  TraceThread(("ts_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = data.asset.all_dates.size();

  // Initialize as idle (will be updated if assets are assigned)
  progress_handle.set_label("Idle");
  progress_handle.update(1, 1, "");

  // Find assets initially assigned to this worker
  std::vector<size_t> my_asset_ids;
  for (size_t i = 0; i < sched.owner.size(); ++i) {
    if (sched.owner[i].load(std::memory_order_relaxed) == worker_id) {
      my_asset_ids.push_back(i);
    }
  }

  if (my_asset_ids.empty()) {
    // 无资产 (资产数 < worker 数才会发生): 前沿报到底, 不挡任何人
    store.ts_report_frontier(worker_id, total_dates);
    return;
  }

  // LOB 工作区: 每 worker 一个 (非每资产). 簿状态全部日内瞬态 (每天 clear()),
  // worker 内资产串行, 复用同一套页面 —— 跨资产 cache/TLB 常驻, 稳态零扩容;
  // per-asset 常驻簿的 ~3MB/资产地板 (档位数组 + 池块粒度 + 桶表) 由此消失.
  // 容量按最忙资产奢侈预留 (见 L2::LOB_ORDER_CAPACITY 注释).
  LimitOrderBook lob(L2::LOB_ORDER_CAPACITY);

  // Per-asset 跨日状态 (DAG 暖历史 + Fund 状态机 + 分钟缓冲 + TickData),
  // ~百 KB/资产. 初始 owner 在本核构造 (first-touch); 处置权转移时对象不动,
  // 只换驱动它的 LOB (bind 换绑写出口, 见 LimitOrderBook).
  {
    TraceN("InitCores");
    for (const size_t asset_id : my_asset_ids) {
      const auto &asset = data.asset.items[asset_id];
      sched.cores[asset_id] = std::make_unique<CoreSequential>(data.fund_pool, asset.asset_code, asset.asset_id, worker_id);
    }
  }

  // 每 worker 一个 decoder (非每资产): decode 是按 (asset, date) 文件的无状态操作,
  // 且 worker 内资产串行 —— 解码结果在下一次 decode 前已被 process_batch 消费完,
  // 零拷贝契约天然满足. 缓冲跨资产/跨日复用, 自动长到本 worker 最忙资产的单日
  // 条数后稳定 (零分配稳态);
  L2::BinaryDecoder_L2 decoder(L2::DEFAULT_ENCODER_ORDER_SIZE);

  Logger::log("worker_" + std::to_string(worker_id), "Started: " + std::to_string(my_asset_ids.size()) + " assets, " +
                                                         std::to_string(data.asset.all_dates.size()) + " dates");

  // Progress label
  char label_buf[128];
  snprintf(label_buf, sizeof(label_buf), "时序核心%2d:", worker_id);
  progress_handle.set_label(label_buf);

  size_t cumulative_orders = 0;
  size_t completed_dates = 0;
  size_t date_orders = 0;
  size_t date_assets_processed = 0;
  auto start_time = std::chrono::steady_clock::now();

  // 认领 (asset, d): CAS d-1→d 唯一裁决谁处理这个 asset-day —— 处置权转移
  // 瞬间的双写/漏写由它兜底, owner 只是"该不该去试"的路标.
  auto claim = [&sched](size_t asset_id, int32_t d) {
    int32_t expected = d - 1;
    return sched.claimed[asset_id].compare_exchange_strong(expected, d, std::memory_order_acq_rel);
  };

  // 进度: 日数为主刻度, 日内排 [已完成/持有] 紧凑计数; 行色按与领跑者的
  // 差距 (slot 粒度) 白→红 (满红 = 差距吃满整个池子, 即本核快把全员拖死).
  auto update_progress = [&](size_t days_done, const std::string &date_str, size_t done_today) {
    size_t max_days = days_done;
    for (int w = 0; w < static_cast<int>(store.query_ts_workers()); ++w)
      max_days = std::max(max_days, store.ts_frontier(w));
    const size_t gap = max_days - days_done;
    const float t = std::min(1.0f, static_cast<float>(gap) / static_cast<float>(store.query_slots()));
    const int gb = static_cast<int>(255.0f * (1.0f - t));
    char color_buf[24];
    snprintf(color_buf, sizeof(color_buf), "\033[38;2;255;%d;%dm", gb, gb);
    progress_handle.set_color(color_buf);

    const float elapsed_seconds = std::chrono::duration<float>(std::chrono::steady_clock::now() - start_time).count();
    const float speed_M_per_sec = (elapsed_seconds > 0) ? (cumulative_orders / 1e6) / elapsed_seconds : 0.0;

    char msg_buf[128];
    snprintf(msg_buf, sizeof(msg_buf), "%s [%3zu/%3zu] [%.1fM/s (%.1fM)]",
             date_str.c_str(), done_today, my_asset_ids.size(), speed_M_per_sec, cumulative_orders / 1e6);
    progress_handle.update(days_done, total_dates, msg_buf);
  };

  // 处理一个 asset-day (缺 binary 则空过, 张量保持默认值), 返回订单数.
  // 主循环与领养回填共用 —— 两者只差句柄来自哪一天.
  auto process_asset_day = [&](size_t asset_id, size_t didx, const std::string &date_str, const GlobalFeatureStore::Day &day) -> size_t {
    const auto &asset = data.asset.items[asset_id];
    if (!asset.date_at(didx).has_binaries())
      return 0; // 缺二进制: 当天张量保持默认值, warm 状态不推进.

    lob.bind(sched.cores[asset_id].get(), asset_id, asset.exchange_type); // 工作区换绑本资产 (簿此刻是干净的)
    lob.tick_data().core_id = static_cast<uint32_t>(worker_id);           // 追踪: 当前驱动核
    lob.begin_day(date_str, day);                                         // 盘前: DAG reset + onDay (Fund 状态机推进到当日)

    // 路径由 (date, code, exchange) 现算 — DateInfo 不再为五百万条记录
    // 各存一份字符串
    const std::string orders_file = Utils::generate_orders_path(
        data.config.orders_dir, date_str, asset.asset_code, asset.exchange,
        config::BINARY_EXTENSION);

    size_t order_num = 0;
    const L2::Order *orders = nullptr;
    {
      TraceN("DecodeOrders");
      orders = decoder.decode_orders_stream(orders_file, order_num);
    }

    if (orders != nullptr) [[likely]] {
      // 档位索引基准来自这一天的文件头, 必须先于第一条订单设进去 —— 绝对价
      // 要减去它才是档位下标 (见 L2_DataType.hpp 的 kPriceIndexRange).
      lob.set_price_base(decoder.last_price_base());

      // Batch processing: zero-overhead inlined loop (process_impl inlined into process_batch)
      size_t order_invalid_cnt = 0;
      {
        TraceN("ProcessLobs");
        TraceValue(order_num);
        order_invalid_cnt = lob.process_batch(orders, order_num);
      }

      if (order_invalid_cnt > 100) {
        Logger::log("worker_" + std::to_string(worker_id), "ERROR: " + date_str + " asset_id=" + std::to_string(asset_id) + " order_invalid=" + std::to_string(order_invalid_cnt));
        std::exit(1);
      }

      if (order_num > 0) {
        Logger::log("worker_" + std::to_string(worker_id),
                    date_str + " asset:" + std::to_string(asset_id) + " " + asset.asset_code + "." + asset.exchange + " " + asset.asset_name +
                        " decoded=" + std::to_string(order_num) +
                        " order_invalid=" + std::to_string(order_invalid_cnt) +
                        " tob_invalid=" + std::to_string(lob.get_tob_invalid_count()) +
                        " tob_refresh=" + std::to_string(lob.get_tob_refresh_count()));
      }

      lob.end_day();
      date_assets_processed++;
    } else {
      Logger::log("worker_" + std::to_string(worker_id), "WARNING: " + date_str + " failed to decode " + orders_file);
      order_num = 0;
    }

    // 归还工作区: 换绑下一个资产前簿必须干净 (bind 断言 order_lookup_ 为空).
    lob.clear();
    return order_num;
  };

  // Leader 侧记账: 本 worker 今天已领养数 (每日在日循环头重置).
  // 上限 = 平均每核持仓 (全市场 / worker 数) 的 adopt_pct%; 0 = 关闭领养.
  size_t adopted_today = 0;
  const size_t leader_cap =
      (sched.adopt_pct == 0) ? 0 : std::max<size_t>(1, sched.owner.size() / store.query_ts_workers() * sched.adopt_pct / 100);

  // 负载再平衡: 在池边干等 slot 时调用 (本 worker 已领先, 等待时间正好拿来
  // 帮落后者). 接手 victim 的最轻资产, 立刻回填落下的旧日期 —— 含 victim
  // 的"当天" (claim CAS 唯一裁决: victim 日循环还没轮到它就被本 worker 先
  // claim 走, victim 见 owner 已变直接跳过, 当天即刻变轻). 旧日 slot 未计
  // 满必为 BUSY, ts_open 直接命中; 回填后并入本 worker 日循环.
  // 两侧 N% 预算限量, 不会一下子掏空/吃撑.
  auto try_adopt = [&](size_t my_days_done) {
    if (adopted_today >= leader_cap)
      return;

    // 候选 victim: 所有落后 ≥ 2 天者 (1 天差是相位噪声), 按前沿升序逐个试
    // —— 最慢者预算耗尽/撞车时顺延帮第二慢者, 而不是空转干等.
    // f ≥ 1 兼保证 victim 的 core 构造已通过前沿计数的 release/acquire 发布.
    std::vector<std::pair<size_t, int>> laggers; // (前沿, worker)
    for (int w = 0; w < static_cast<int>(store.query_ts_workers()); ++w) {
      const size_t f = store.ts_frontier(w);
      if (w != worker_id && f >= 1 && my_days_done >= f + 2)
        laggers.emplace_back(f, w);
    }
    std::sort(laggers.begin(), laggers.end());

    for (const auto &[victim_days, victim] : laggers) {
      // victim 手里最轻的资产 (顺便数持仓; 至少给它留 2 个)
      size_t victim_count = 0, pick = SIZE_MAX, pick_weight = SIZE_MAX;
      for (size_t i = 0; i < sched.owner.size(); ++i) {
        if (sched.owner[i].load(std::memory_order_relaxed) != victim)
          continue;
        ++victim_count;
        if (sched.weight[i] < pick_weight) {
          pick_weight = sched.weight[i];
          pick = i;
        }
      }
      if (victim_count < 2 || pick == SIZE_MAX)
        continue;

      // victim 侧预算 CAS: 本窗 (其前沿推进即换窗) 最多让出持仓的 adopt_pct%.
      // 编码 (前沿 << 16) | 已让出数.
      const uint64_t victim_budget = std::max<uint64_t>(1, victim_count * sched.adopt_pct / 100);
      bool budget_ok = false;
      {
        std::atomic<uint64_t> &win = sched.adopt_window[victim];
        uint64_t w = win.load(std::memory_order_relaxed);
        while (true) {
          if ((w >> 16) != victim_days) {
            // 新窗口: 重置并领第 1 个
            if (win.compare_exchange_weak(w, (static_cast<uint64_t>(victim_days) << 16) | 1, std::memory_order_relaxed)) {
              budget_ok = true;
              break;
            }
          } else if ((w & 0xFFFF) >= victim_budget) {
            break; // 本窗预算已耗尽, 试下一个 victim
          } else if (win.compare_exchange_weak(w, w + 1, std::memory_order_relaxed)) {
            budget_ok = true;
            break;
          }
        }
      }
      if (!budget_ok)
        continue;

      // 处置权 CAS: 路标改指本 worker, victim 下一天起直接跳过 pick.
      // 失败 = 与其他领养方撞车 (预算多扣一格, 无妨, 试下一个 victim).
      int32_t expect = victim;
      if (!sched.owner[pick].compare_exchange_strong(expect, worker_id, std::memory_order_acq_rel))
        continue;
      ++adopted_today;

      // victim 可能正在算 pick 的在飞一天: 等它交割 (窗口 = 单个 asset-day)
      while (sched.claimed[pick].load(std::memory_order_acquire) != sched.done[pick].load(std::memory_order_acquire)) {
        if (cancel_requested.load(std::memory_order_relaxed))
          return;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }

      const int32_t from = sched.done[pick].load(std::memory_order_acquire) + 1;
      const int32_t upto = static_cast<int32_t>(my_days_done) - 1; // 回填到本 worker 前沿
      Logger::log("worker_" + std::to_string(worker_id),
                  "adopt: asset " + std::to_string(pick) + " from worker " + std::to_string(victim) +
                      " (gap " + std::to_string(my_days_done - victim_days) + "d), backfill didx " +
                      std::to_string(from) + ".." + std::to_string(upto));

      for (int32_t d = from; d <= upto; ++d) {
        if (cancel_requested.load(std::memory_order_relaxed))
          return;
        TraceN("AdoptBackfill");
        [[maybe_unused]] const bool claimed_ok = claim(pick, d);
        assert(claimed_ok && "adopt backfill: claim 竞争 (处置权已归本 worker, 不应有对手)");
        const std::string &bdate = data.asset.all_dates[static_cast<size_t>(d)];
        const auto bday = store.ts_open(bdate, worker_id, cancel_requested);
        if (!bday)
          return;
        cumulative_orders += process_asset_day(pick, data.asset.date_idx(bdate), bdate, bday);
        sched.done[pick].store(d, std::memory_order_release);
        store.ts_close(bday);
      }
      my_asset_ids.push_back(pick);
      return; // 一次领养一个; 还在等 slot 的话下轮再来
    }
  };

  // Zero-copy streaming: decoder maintains internal buffer, worker receives const pointer
  // No memory allocation in worker - decoder reuses buffer across all decode calls

  for (size_t date_idx = 0; date_idx < total_dates; ++date_idx) {
    if (cancel_requested.load(std::memory_order_relaxed))
      break;

    TraceN("DateLoop");
    const std::string &date_str = data.asset.all_dates[date_idx];
    TraceTextS(date_str.c_str());
    date_orders = 0;
    date_assets_processed = 0;
    adopted_today = 0; // leader 侧领养预算按日重置

    // 日期 → 日期轴下标, 一天查一次; 资产内循环 O(1) 定址
    const size_t didx = data.asset.date_idx(date_str);

    // 本日写句柄: 每 worker 每日 open 一次, 之后所有写回是纯指针算术.
    // 池满 = 本 worker 领先到把池子吃满 —— 干等的时间拿去领养最慢者的资产
    // 并回填 (回填日必为 BUSY, 不占新 slot), 等 IO 释放后再继续本日.
    auto day = store.ts_try_open(date_str, worker_id);
    while (!day && !cancel_requested.load(std::memory_order_relaxed)) {
      TraceN("PoolWait");
      TraceColor(C_Orange);
      try_adopt(date_idx); // date_idx = 本 worker 已完成日数
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      day = store.ts_try_open(date_str, worker_id);
    }
    if (!day)
      break;

    // Process each asset at this date
    size_t done_today = 0;
    for (size_t i = 0; i < my_asset_ids.size();) {
      const size_t asset_id = my_asset_ids[i];
      // 处置权已转走 (或 claim 输给领养方的回填): 移除, 计数由新 owner 负责
      if (sched.owner[asset_id].load(std::memory_order_relaxed) != worker_id ||
          !claim(asset_id, static_cast<int32_t>(date_idx))) {
        my_asset_ids[i] = my_asset_ids.back();
        my_asset_ids.pop_back();
        continue;
      }
      const size_t order_num = process_asset_day(asset_id, didx, date_str, day);
      date_orders += order_num;
      cumulative_orders += order_num;
      sched.done[asset_id].store(static_cast<int32_t>(date_idx), std::memory_order_release);
      store.ts_close(day);
      ++i;
      ++done_today;
      update_progress(date_idx, date_str, done_today);
    }

    if (date_assets_processed > 0) {
      Logger::log("worker_" + std::to_string(worker_id), date_str + " completed: " + std::to_string(date_assets_processed) + " assets, " + std::to_string(date_orders) + " orders");
    }

    // 本 worker 本日计完; 自报前沿 (release 发布本日全部 core 状态)
    {
      TraceN("StoreDone");
      store.ts_report_frontier(worker_id, date_idx + 1);
    }

    completed_dates = date_idx + 1;
    update_progress(completed_dates, date_str, done_today);

    TraceFrame; // Mark frame boundary for timeline
  }

  progress_handle.set_color("");
  if (cancel_requested.load(std::memory_order_relaxed)) {
    progress_handle.update(completed_dates, data.asset.all_dates.size(), "Cancelled");
    Logger::log("worker_" + std::to_string(worker_id), "Cancelled: processed " + std::to_string(cumulative_orders) + " orders across " + std::to_string(completed_dates) + " dates");
  } else {
    Logger::log("worker_" + std::to_string(worker_id), "Completed: processed " + std::to_string(cumulative_orders) + " orders across " + std::to_string(data.asset.all_dates.size()) + " dates");
  }
}
