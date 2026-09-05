#include "worker/sequential_worker.hpp"
#include "shared/SharedData.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/binary_decoder_L2.hpp"
#include "features/Backend/FeatureStore.hpp"
#include "lob/LimitOrderBook.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <memory>
#include <thread>
#include <vector>

// cores 的 unique_ptr 需要 CoreSequential 完整类型, 只在本 TU 实例化
TsSchedule::TsSchedule(size_t num_assets)
    : owner(num_assets), claimed(num_assets), done(num_assets),
      weight(num_assets, 0), cores(num_assets) {
  for (size_t i = 0; i < num_assets; ++i) {
    owner[i].store(-1, std::memory_order_relaxed);
    claimed[i].store(-1, std::memory_order_relaxed);
    done[i].store(-1, std::memory_order_relaxed);
  }
}

TsSchedule::~TsSchedule() = default;

// 领养阈值/冷却: 领先最慢者 ≥ kAdoptGapDays 才伸手 (赶在池满卡死之前);
// 全局每 kAdoptCooldownDays 天至多一笔转移 —— 反馈是即时的 (落后者下一天
// 立刻变轻), 小步慢走就能收敛, 不会一下子接多.
inline constexpr size_t kAdoptGapDays = 3;
inline constexpr int64_t kAdoptCooldownDays = 10;

void sequential_worker(int worker_id,
                       SharedData &data,
                       GlobalFeatureStore &store,
                       TsSchedule &sched,
                       const std::atomic<bool> &cancel_requested,
                       misc::ProgressHandle progress_handle) {
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

  // 处理一个 asset-day (缺 binary 则空过, 张量保持默认值), 返回订单数.
  // 主循环与领养回填共用 —— 两者只差句柄来自哪一天.
  auto process_asset_day = [&](size_t asset_id, size_t didx, const std::string &date_str, const GlobalFeatureStore::TsDay &day) -> size_t {
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

  // 负载再平衡: 领先最慢 worker 超阈值 → 接手其最轻资产, 立刻回填落下的旧
  // 日期 (旧日 slot 未计满必为 BUSY, ts_open 直接命中), 之后并入本 worker
  // 日循环. 落后者下一天马上变轻 —— 反馈即时, 全局冷却限速, 一次一个.
  auto try_adopt = [&](size_t my_days_done) {
    // 最慢 worker (排除自己). min_days ≥ 1 兼保证 victim 的 core 构造已
    // 通过前沿计数的 release/acquire 发布.
    size_t min_days = SIZE_MAX;
    int victim = -1;
    for (int w = 0; w < static_cast<int>(store.query_ts_workers()); ++w) {
      if (w == worker_id)
        continue;
      const size_t f = store.ts_frontier(w);
      if (f < min_days) {
        min_days = f;
        victim = w;
      }
    }
    if (victim < 0 || min_days < 1 || my_days_done < min_days + kAdoptGapDays)
      return;

    // 全局冷却 CAS: 每 kAdoptCooldownDays 天至多一笔 (失败 = 别人刚领过)
    int64_t last = sched.last_adopt_didx.load(std::memory_order_relaxed);
    const int64_t now = static_cast<int64_t>(my_days_done);
    if (now < last + kAdoptCooldownDays)
      return;
    if (!sched.last_adopt_didx.compare_exchange_strong(last, now, std::memory_order_relaxed))
      return;

    // victim 手里最轻的资产 (至少给它留 2 个; 冷却 CAS 已串行化领养方,
    // 计数窗口内不会有并发转移把它掏空)
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
      return;

    // 处置权 CAS: 路标改指本 worker, victim 下一天起直接跳过 pick
    int32_t expect = victim;
    if (!sched.owner[pick].compare_exchange_strong(expect, worker_id, std::memory_order_acq_rel))
      return;

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
                    " (gap " + std::to_string(my_days_done - min_days) + "d), backfill didx " +
                    std::to_string(from) + ".." + std::to_string(upto));

    for (int32_t d = from; d <= upto; ++d) {
      if (cancel_requested.load(std::memory_order_relaxed))
        return;
      TraceN("AdoptBackfill");
      [[maybe_unused]] const bool claimed_ok = claim(pick, d);
      assert(claimed_ok && "adopt backfill: claim 竞争 (处置权已归本 worker, 不应有对手)");
      const std::string &bdate = data.asset.all_dates[static_cast<size_t>(d)];
      const auto bday = store.ts_open(bdate, worker_id, cancel_requested);
      if (!bday.slot)
        return;
      cumulative_orders += process_asset_day(pick, data.asset.date_idx(bdate), bdate, bday);
      sched.done[pick].store(d, std::memory_order_release);
      store.ts_asset_done(bday);
    }
    my_asset_ids.push_back(pick);
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

    // 日期 → 日期轴下标, 一天查一次; 资产内循环 O(1) 定址
    const size_t didx = data.asset.date_idx(date_str);

    // 本日写句柄: 每 worker 每日 open 一次, 之后所有写回是纯指针算术
    const auto day = store.ts_open(date_str, worker_id, cancel_requested);
    if (!day.slot)
      break;

    // Process each asset at this date
    for (size_t i = 0; i < my_asset_ids.size();) {
      const size_t asset_id = my_asset_ids[i];
      // 处置权已转走 (或 claim 输给领养方的回填): 移除, 计数由新 owner 负责
      if (sched.owner[asset_id].load(std::memory_order_relaxed) != worker_id ||
          !claim(asset_id, static_cast<int32_t>(date_idx))) {
        my_asset_ids[i] = my_asset_ids.back();
        my_asset_ids.pop_back();
        continue;
      }
      date_orders += process_asset_day(asset_id, didx, date_str, day);
      sched.done[asset_id].store(static_cast<int32_t>(date_idx), std::memory_order_release);
      store.ts_asset_done(day);
      ++i;
    }
    cumulative_orders += date_orders;

    if (date_assets_processed > 0) {
      Logger::log("worker_" + std::to_string(worker_id), date_str + " completed: " + std::to_string(date_assets_processed) + " assets, " + std::to_string(date_orders) + " orders");
    }

    // 本 worker 本日计完; 自报前沿 (release 发布本日全部 core 状态)
    {
      TraceN("StoreDone");
      store.ts_report_frontier(worker_id, date_idx + 1);
    }

    // Update progress
    auto current_time = std::chrono::steady_clock::now();
    float elapsed_seconds = std::chrono::duration<float>(current_time - start_time).count();
    float speed_M_per_sec = (elapsed_seconds > 0) ? (cumulative_orders / 1e6) / elapsed_seconds : 0.0;

    char msg_buf[128];
    snprintf(msg_buf, sizeof(msg_buf), "%2zu Assets: %s [%.1fM/s (%.1fM)]",
             my_asset_ids.size(), date_str.c_str(), speed_M_per_sec, cumulative_orders / 1e6);
    completed_dates = date_idx + 1;
    progress_handle.update(completed_dates, total_dates, msg_buf);

    // 负载再平衡检查 (本日已计完, 正是接活的空当)
    try_adopt(date_idx + 1);

    TraceFrame; // Mark frame boundary for timeline
  }

  if (cancel_requested.load(std::memory_order_relaxed)) {
    progress_handle.update(completed_dates, data.asset.all_dates.size(), "Cancelled");
    Logger::log("worker_" + std::to_string(worker_id), "Cancelled: processed " + std::to_string(cumulative_orders) + " orders across " + std::to_string(completed_dates) + " dates");
  } else {
    Logger::log("worker_" + std::to_string(worker_id), "Completed: processed " + std::to_string(cumulative_orders) + " orders across " + std::to_string(data.asset.all_dates.size()) + " dates");
  }
}
