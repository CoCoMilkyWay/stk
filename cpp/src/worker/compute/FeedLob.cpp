// Lob 档管线 (逐笔 → 订单簿源): 档的源 TierFeed<lob::Tier> + 档入口表 lob_ops. 本 TU fast-math.
#include "worker/compute/Pipeline.hpp"

#include "codec/L2DataType.hpp"
#include "codec/OrderFrame.hpp"
#include "features/CoreLob.hpp"
#include "lob/LimitOrderBook.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

// 软源: 预取只把当天日文件里本档资产的帧读进 page cache, TS 的 decode 只吃缓存不等磁盘 (TS 不等预取)
template <>
struct TierFeed<lob::Tier> {
  using Core = CoreLob;
  static constexpr bool kHardFeed = false;
  static constexpr const char *kUnit = "单";

  // 源日历: 回测日历 ≤ 末个已编码日, 止于首个待编码日之前 (有归档未编码 ⇒ 数据将至, 不落默认张量;
  //   raw 缺口日 = 无源可编, 在轴内落默认张量)
  static std::vector<misc::Ymd> source_dates(const SharedData &data) {
    assert(data.asset.backtest.start == data.config.start_date && "回测日历与配置起点不符 (改配置后没重算覆盖?)");
    std::vector<misc::Ymd> dates;
    for (const Asset::BacktestDay &day : data.asset.backtest.days) {
      if (day.date > data.asset.binary.max_date || day.pending())
        break;
      dates.push_back(day.date);
    }
    return dates;
  }

  SharedData &data;
  const PoolAxis &axis;
  std::span<const misc::Ymd> dates;
  // 帧目录切成 日期 × 池轴 (行 = dates 下标, 列 = 池轴下标), 持有到全部 worker 退出,
  // 不碰 Asset 上随重扫描换代的那一份; orders.at(date_idx, sub) 直接定址.
  const OrdersCatalog orders;

  static size_t cell_bytes(size_t) { return 0; } // 软源无源格

  explicit TierFeed(const PipelineArgs &args)
      : data(args.data), axis(args.libs[TierId::Lob].axis), dates(args.dates),
        orders(args.data.asset.orders()->slice(args.dates, axis.ids)) {}

  void bind(const StoreRing &) {}

  // 逐笔条数 (扫描时随日文件目录读好, 不碰文件系统)
  size_t weight(size_t sub, size_t d) const { return orders.at(d, sub).head.count; }

  // reader 按偏移升序交给内核预读 (空格子跳过), 字节不进用户态 (decode 在 TS worker 内联)
  size_t prefetch(size_t d) const {
    const std::span<const FrameRef> refs = orders.row(d);
    if (std::none_of(refs.begin(), refs.end(), [](const FrameRef &ref) { return !ref.empty(); }))
      return 0;
    return DayReader(data.config.orders_dir, dates[d]).prefetch(refs);
  }

  // 源格 = 日文件里本资产当日的帧引用 (空 = 缺二进制)
  const FrameRef &cell(size_t sub, size_t d) const { return orders.at(d, sub); }

  // 每 TS worker 一份工作区
  class Runner {
  public:
    Runner(TierFeed &f, int worker_id) : feed_(f), worker_id_(worker_id), lob_(L2::LOB_ORDER_CAPACITY) {}

    // 源格的逐笔经订单簿进 core, 返回订单数; 缺源 = 不绑订单簿 (零事件日)
    size_t feed(CoreLob &core, size_t sub, size_t d) {
      const FrameRef &ref = feed_.cell(sub, d);
      if (ref.empty())
        return 0;
      const misc::Ymd date = feed_.dates[d];

      lob_.bind(&core, sub, feed_.data.asset.items[feed_.axis.global(sub)].exchange_type); // 工作区换绑本资产 (簿此刻是干净的)
      lob_.tick_data().core_id = static_cast<uint32_t>(worker_id_);                        // 追踪: 当前驱动核

      // 帧的位置/条数/价位基准都在扫描灌好的帧目录里, 不读任何头; 同一天的
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

      diag_.order_invalid += order_invalid_cnt;
      diag_.tob_invalid += lob_.get_tob_invalid_count();
      diag_.tob_refresh += lob_.get_tob_refresh_count();
      diag_.depth_crossed += lob_.get_depth_crossed_count();
      diag_.depth_seen += lob_.get_depth_seen_count();
      return order_num;
    }

    // core 收盘之后归还干净工作区 (下一个资产的 bind 断言未绑定; 收盘结算还读簿, 见 LimitOrderBook::bind)
    void release() {
      if (lob_.bound())
        lob_.unbind();
    }

    // 订单簿诊断累计 (交叉簿率 = crossed / seen: 定位报单乱序 / TOB 失效的量级)
    std::string day_log() {
      const Diag g = std::exchange(diag_, {});
      return " | order_invalid=" + std::to_string(g.order_invalid) + " tob_invalid=" + std::to_string(g.tob_invalid) +
             " tob_refresh=" + std::to_string(g.tob_refresh) + " depth_crossed=" + std::to_string(g.depth_crossed) + "/" + std::to_string(g.depth_seen);
    }

  private:
    struct Diag {
      size_t order_invalid = 0, tob_invalid = 0, tob_refresh = 0, depth_crossed = 0, depth_seen = 0;
    };

    TierFeed &feed_;
    int worker_id_;
    Diag diag_; // 自上次 day_log 以来
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

const TierOps lob_ops = tier_ops_of<lob::Tier>();
