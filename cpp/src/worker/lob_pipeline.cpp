// Lob 档管线 (逐笔 → 订单簿源, 池轴): 档的源 TierFeed<lob::Tier> + pipeline<lob::Tier> 实例化. 本 TU fast-math.
#include "worker/feature_pipeline.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/OrderFrame.hpp"
#include "features/CoreLob.hpp"
#include "lob/LimitOrderBook.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

// 软源: 预取只把当天日文件里本档资产的帧读进 page cache, TS 的 decode 只吃缓存不等磁盘 (TS 不等预取)
template <>
struct TierFeed<lob::Tier> {
  using Core = CoreLob;
  static constexpr bool kHardFeed = false;
  static constexpr const char *kUnit = "单";

  SharedData &data;
  const PoolAxis &axis;
  std::span<const misc::Ymd> dates;
  // 帧目录切成 日期 × 池轴 (行 = dates 下标, 列 = 池轴下标), 持有到全部 worker 退出,
  // 不碰 Asset 上随重扫描换代的那一份; orders.at(date_idx, sub) 直接定址.
  const OrdersCatalog orders;
  std::vector<size_t> weights; // 区间内逐笔条数 (扫描时随日文件目录读好, 不碰文件系统)

  TierFeed(const PipelineArgs &args, const StoreRing &)
      : data(args.data), axis(args.libs[TierId::Lob].axis), dates(args.dates),
        orders(args.data.asset.orders()->slice(args.dates, axis.ids)), weights(axis.size(), 0) {
    // 池逐日成员 (ComputeService 规划本池时 build: 并集 = 本池 A 轴, 逐日成员位 → Meta cs_valid)
    assert(data.pool_panel.axis().hash == axis.hash && std::ranges::equal(data.pool_panel.dates(), dates) && "池与本池 A 轴 / 日期轴不符");
    for (size_t d = 0; d < dates.size(); ++d) {
      const std::span<const FrameRef> row = orders.row(d);
      for (size_t sub = 0; sub < weights.size(); ++sub)
        weights[sub] += row[sub].head.count;
    }
  }

  size_t weight(size_t sub) const { return weights[sub]; }

  std::unique_ptr<CoreLob> make_core(size_t sub, int worker_id) const {
    const auto &asset = data.asset.items[axis.global(sub)];
    return std::make_unique<CoreLob>(data.fund_panel, data.pool_panel, asset.code_ex(), sub, static_cast<size_t>(worker_id));
  }

  // reader 按偏移升序交给内核预读 (空格子跳过), 字节不进用户态 (decode 在 TS worker 内联)
  size_t prefetch(size_t d) const {
    const std::span<const FrameRef> refs = orders.row(d);
    if (std::none_of(refs.begin(), refs.end(), [](const FrameRef &ref) { return !ref.empty(); }))
      return 0;
    return DayReader(data.config.orders_dir, dates[d]).prefetch(refs);
  }

  // 每 TS worker 一份工作区
  class Runner {
  public:
    Runner(TierFeed &feed, int worker_id) : feed_(feed), worker_id_(worker_id), lob_(L2::LOB_ORDER_CAPACITY) {}

    // 一个 asset-day (缺 binary 则按零事件日走同一路径), 返回订单数
    size_t run(CoreLob &core, size_t asset_id, size_t date_idx, const LobFeatureStore::Day &day, LobFeatureStore &store) {
      const misc::Ymd date = feed_.dates[date_idx];
      const auto &asset = feed_.data.asset.items[feed_.axis.global(asset_id)];
      const FrameRef &ref = feed_.orders.at(date_idx, asset_id);
      if (ref.empty()) {
        core.empty_day(date, day, store); // 缺二进制 = 零事件日: 与正常日同路径, 不绑订单簿
        return 0;
      }

      lob_.bind(&core, asset_id, asset.exchange_type);              // 工作区换绑本资产 (簿此刻是干净的)
      lob_.tick_data().core_id = static_cast<uint32_t>(worker_id_); // 追踪: 当前驱动核
      lob_.begin_day(date, day);                                    // 盘前: DAG reset / 日频节点承接 + onDay

      // 帧的位置/条数/价位基准都在扫描灌好的帧目录里, 不再读任何头; 同一天的
      // 资产共用 reader 里那一个 fd.
      L2::FrameView view;
      {
        TraceN("DecodeOrders");
        if (!reader_ || reader_->date() != date)
          reader_.emplace(feed_.data.config.orders_dir, date);
        view = reader_->get(ref, decoder_);
      }
      const size_t order_num = view.head.count;

      // Batch processing: zero-overhead inlined loop (process_impl inlined into run_frame)
      size_t order_invalid_cnt = 0;
      {
        TraceN("ProcessLobs");
        TraceValue(order_num);
        order_invalid_cnt = lob_.run_frame(view);
      }

      // 编码期准入校验 (L2::LobUnusable) 已拦下订单簿抵扣不动的记录, 走到这里即校验漏判
      assert(order_invalid_cnt <= 100 && "Lob TS: 订单簿无效条数超阈值 (准入校验漏判?)");

      Logger::log("worker_" + std::to_string(worker_id_),
                  std::to_string(date) + " asset:" + std::to_string(asset_id) + " " + asset.code_ex() + " " + asset.asset_name +
                      " decoded=" + std::to_string(order_num) +
                      " order_invalid=" + std::to_string(order_invalid_cnt) +
                      " tob_invalid=" + std::to_string(lob_.get_tob_invalid_count()) +
                      " tob_refresh=" + std::to_string(lob_.get_tob_refresh_count()) +
                      " depth_crossed=" + std::to_string(lob_.get_depth_crossed_count()) + "/" + std::to_string(lob_.get_depth_seen_count()));

      lob_.end_day(store); // begin_day 已推日环, 收盘结算 / 归还句柄不能少
      lob_.unbind();       // 归还干净工作区 (下一个资产的 bind 断言未绑定)
      return order_num;
    }

  private:
    TierFeed &feed_;
    int worker_id_;
    // 订单簿工作区: 每 worker 一个 (非每资产). 簿状态全部日内瞬态 (每个 asset-day bind…unbind),
    // worker 内资产串行, 复用同一套页面 —— 跨资产 cache/TLB 常驻, 稳态零扩容;
    // per-asset 常驻簿的 ~3MB/资产地板 (价位数组 + 池块粒度 + 桶表) 由此消失.
    // 容量按最忙资产奢侈预留 (见 L2::LOB_ORDER_CAPACITY 注释).
    LimitOrderBook lob_;
    // 每 worker 一个 decoder (非每资产): get 是按 (asset, date) 一帧的无状态操作,
    // 且 worker 内资产串行 —— 解码结果在下一次 get 前已被 run_frame 消费完, 零拷贝
    // 契约天然满足. 帧字节与逐笔两块缓冲都在 decoder 里, 跨资产/跨日复用, 自动长到
    // 本 worker 最忙资产的单日条数后稳定 (零分配稳态). reader 只是当天日文件的 fd,
    // 换天才重建 (领养回填会切到旧日, 回来再切回).
    std::optional<DayReader> reader_;
    L2::FrameDecoder decoder_;
  };
};

template bool pipeline<lob::Tier>(const PipelineArgs &);
