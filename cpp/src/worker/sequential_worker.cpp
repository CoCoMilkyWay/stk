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
#include <vector>

void sequential_worker(int worker_id,
                       SharedData &data,
                       GlobalFeatureStore &store,
                       misc::ProgressHandle progress_handle) {
  TraceNS("TSWorker", 5);
  TraceValue(worker_id);
  TraceThread(("ts_worker_" + std::to_string(worker_id)).c_str());

  // Initialize as idle (will be updated if assets are assigned)
  progress_handle.set_label("Idle");
  progress_handle.update(1, 1, "");

  // Find assets assigned to this worker
  std::vector<size_t> my_asset_ids;
  for (size_t i = 0; i < data.asset.items.size(); ++i) {
    if (data.asset.items[i].assigned_worker_id == worker_id) {
      my_asset_ids.push_back(i);
    }
  }

  // LOB 工作区: 每 worker 一个 (非每资产). 簿状态全部日内瞬态 (每天 clear()),
  // worker 内资产串行, 复用同一套页面 —— 跨资产 cache/TLB 常驻, 稳态零扩容;
  // per-asset 常驻簿的 ~3MB/资产地板 (档位数组 + 池块粒度 + 桶表) 由此消失.
  // 容量按最忙资产奢侈预留 (见 L2::LOB_ORDER_CAPACITY 注释).
  LimitOrderBook lob(L2::LOB_ORDER_CAPACITY);
  lob.tick_data().core_id = static_cast<uint32_t>(worker_id);

  // Per-asset 跨日状态 (DAG 暖历史 + Fund 状态机 + 分钟缓冲), ~百 KB/资产.
  // DAG_Root 构造时引用本 worker 工作区的 TickData —— 资产归属 worker 静态
  // 独占, 引用终身有效. 处理某资产前 lob.bind() 换绑.
  std::vector<std::unique_ptr<CoreSequential>> cores;
  {
    TraceN("InitCores");
    for (size_t i = 0; i < my_asset_ids.size(); ++i) {
      const size_t asset_id = my_asset_ids[i];
      const auto &asset = data.asset.items[asset_id];
      cores.push_back(std::make_unique<CoreSequential>(lob.tick_data(), data.fund_pool, asset.asset_code, asset.asset_id, worker_id));
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
  if (!my_asset_ids.empty()) {
    snprintf(label_buf, sizeof(label_buf), "时序核心%2d: %2zu Assets: %s(%s)",
             worker_id,
             my_asset_ids.size(),
             data.asset.items[my_asset_ids[0]].asset_code.c_str(),
             data.asset.items[my_asset_ids[0]].asset_name.c_str());
  } else {
    snprintf(label_buf, sizeof(label_buf), "时序核心%2d: Idle", worker_id);
  }
  progress_handle.set_label(label_buf);

  size_t cumulative_orders = 0;
  auto start_time = std::chrono::steady_clock::now();

  // Zero-copy streaming: decoder maintains internal buffer, worker receives const pointer
  // No memory allocation in worker - decoder reuses buffer across all decode calls

  for (size_t date_idx = 0; date_idx < data.asset.all_dates.size(); ++date_idx) {
    TraceN("DateLoop");
    const std::string &date_str = data.asset.all_dates[date_idx];
    TraceTextS(date_str.c_str());
    size_t date_orders = 0;
    size_t date_assets_processed = 0;

    // 日期 → 日期轴下标, 一天查一次; 资产内循环 O(1) 定址
    const size_t didx = data.asset.date_idx(date_str);

    // 本日写句柄: 每 worker 每日 open 一次, 之后所有写回是纯指针算术
    const auto day = store.ts_open(date_str, worker_id);

    // Process each asset at this date
    for (size_t i = 0; i < my_asset_ids.size(); ++i) {
      const size_t asset_id = my_asset_ids[i];
      const auto &asset = data.asset.items[asset_id];
      lob.bind(cores[i].get(), asset_id, asset.exchange_type); // 工作区换绑本资产 (簿此刻是干净的)
      lob.begin_day(date_str, day);                            // 盘前: DAG reset + onDay (Fund 状态机推进到当日)
      // Hot path: has data and binaries
      if (asset.date_at(didx).has_binaries()) [[likely]] {

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
          date_orders += order_num;
          date_assets_processed++;
          cumulative_orders += order_num;
        } else {
          Logger::log("worker_" + std::to_string(worker_id), "WARNING: " + date_str + " failed to decode " + orders_file);
        }
      }

      // 归还工作区: 无论有无数据/解码成败, 换绑下一个资产前簿必须干净
      // (bind 断言 order_lookup_ 为空). clear() 同时 reset 本资产的日内特征态.
      lob.clear();
    }

    if (date_assets_processed > 0) {
      Logger::log("worker_" + std::to_string(worker_id), date_str + " completed: " + std::to_string(date_assets_processed) + " assets, " + std::to_string(date_orders) + " orders");
    }

    // 本 worker 本日写完; 全部 worker close 后 CS 放行 (按日门控)
    {
      TraceN("StoreDone");
      store.ts_close(day, worker_id);
    }

    // Update progress
    auto current_time = std::chrono::steady_clock::now();
    float elapsed_seconds = std::chrono::duration<float>(current_time - start_time).count();
    float speed_M_per_sec = (elapsed_seconds > 0) ? (cumulative_orders / 1e6) / elapsed_seconds : 0.0;

    char msg_buf[128];
    snprintf(msg_buf, sizeof(msg_buf), "%s [%.1fM/s (%.1fM)]", date_str.c_str(), speed_M_per_sec, cumulative_orders / 1e6);
    progress_handle.update(date_idx + 1, data.asset.all_dates.size(), msg_buf);

    TraceFrame; // Mark frame boundary for timeline
  }

  Logger::log("worker_" + std::to_string(worker_id), "Completed: processed " + std::to_string(cumulative_orders) + " orders across " + std::to_string(data.asset.all_dates.size()) + " dates");
}
