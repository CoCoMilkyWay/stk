#pragma once

#include "misc/progress_parallel.hpp"
#include "shared/EncodeDayRecord.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

// Forward declarations
struct SharedData;

// ============================================================================
// PHASE 1: ENCODING PIPELINE (单 producer 列举 → N worker 并行解压解码)
// ============================================================================
//
// 解压必须并行: 单条 unrar 流单核只有几十 (资产, 日期)/s, 喂不满几十个解码核
// — 所以每个 worker 对自己的批各开一次 unrar (实测能跑满全部核心). 归档目录
// 落在 NVMe 上, 多路并发随机读没有寻道代价 (实测 O_DIRECT 下 64 路并发随机读
// 吞吐与单流顺序读同级, 甚至更高), 不需要为并发直读盘做额外的预读/串行化.
//
// 天与天自然流水: worker 解 day N 时, producer 在列举 day N+1.
//
// 批内内存不随批大小增长: 一次只持有一个文件的字节 (见 misc/archive.hpp 的
// stream_archive_files), 到达即解析成中间结构.

// 一天的元数据全在 orders/YYYY/MM/DD/.stat 里 (齐备标记 + 处置分类 + 每个
// 资产的条数/体积 + 墓碑), 定义见 shared/EncodeDayRecord.hpp.
//
// 它的 complete 项不老于当天归档 ⇒ 这一天的全部 (资产, 日期) 都有了结局.
// 增量重跑靠它整天跳过, 省掉一次 unrar l (机械盘上 0.2~2.5s) 与当天几千次
// 产物 stat —— 没有它, "确认无事可做"本身就要几秒一天.
// 扫描端按 .bin 后缀过滤, 天然忽略它.
//
// 手工删掉损坏的 .bin 不需要连带删 .stat: 快路径在信 complete 之前会拿一次
// readdir 跟明细核对 (见 day_products_match), 扫描那侧发现不符也会把齐备标记
// 作废 —— 两边都能自己发现盘上少了东西, 那天于是回到重编队列.

// 一个 (资产, 日期) 的待编码任务.
//
// 只存下标与尺寸, 不存路径字符串: 包内路径由 (批的日期, 资产代码) 现场拼出来.
struct EncodeTask {
  size_t asset_id;
  size_t order_index;  // 逐笔委托在归档内的序号 (排序键)
  size_t trade_index;  // 逐笔成交在归档内的序号
  size_t market_index; // 行情 (三秒快照) 在归档内的序号
  size_t order_size;   // 解压后字节数
  size_t trade_size;   // 0 表示该资产当日没有成交文件
  size_t market_size;  // 0 表示该资产当日没有行情文件 (准入校验将判 MarketAbsent)
};

// 一批共享同一个归档的任务 (纯元数据, 很小) — worker 的调度粒度,
// 也是一次 unrar p 调用的粒度 (摊薄进程启动 + 包头扫描的固定开销).
struct EncodeBatch {
  std::string date; // "20260803"
  std::string archive_path;
  std::vector<EncodeTask> tasks;
};

// 有界批队列 — producer (列举) 与 worker (解压+解码) 之间的交接.
// 容量即流水深度: 大致对应 producer 领先几天, 见 EncodingService 的容量常量.
class BatchQueue {
public:
  explicit BatchQueue(size_t capacity) : capacity_(capacity) {}

  // 队列满则阻塞. 已 close 返回 false.
  bool push(EncodeBatch batch);

  // 队列空则阻塞. 已 close 且排空返回 false.
  bool pop(EncodeBatch &out);

  // 生产端结束: 唤醒所有等待者
  void close();

private:
  std::deque<EncodeBatch> queue_;
  std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  size_t capacity_;
  bool closed_ = false;
};

// 列举/编码的全局计数 — 汇总进度与收尾统计用
struct EncodeStats {
  std::atomic<size_t> pairs_listed{0};  // 待编码 (asset, date) 对
  std::atomic<size_t> pairs_skipped{0}; // 产物新鲜, 逐个跳过
  std::atomic<size_t> days_skipped{0};  // 整天完成标记命中, 连列举都免了
  std::atomic<size_t> pairs_corrupt{0}; // 源 CSV 损坏, 跳过待人工修复
  std::atomic<size_t> pairs_invalid{0}; // 准入校验未过, 跳过待人工核查数据
  std::atomic<size_t> days_corrupt{0};  // 包头损坏, 整天列举不出来
  std::mutex assets_mutex;
  std::unordered_set<size_t> assets_with_work;

  // 天粒度进度账本: date → 该天的完成情况.
  // producer 列举完一天就注册, worker 每落盘一对就推进; 一天清零即从账本
  // 移除、把汇总行 (单位: days) +1, 并把 rec 落成整天账目文件 (见
  // shared/EncodeDayRecord.hpp). 汇总附注始终显示最老在编天的资产进度.
  struct DayProgress {
    size_t done = 0;
    size_t total = 0;
    size_t errors = 0; // 没留下产物的对数; 非零则 rec.complete = false

    // 本天的处置分类账. producer 先填上分母与"产物已新鲜"的那部分,
    // worker 每销一个账就往对应的桶里加一笔.
    EncodeDayRecord rec;

    // 本天逐资产的计量与墓碑 (见 shared/EncodeDayRecord.hpp). 与 rec 同步
    // 积累: producer 先放进"产物已新鲜"的那些 (增量跑里那是绝大多数, 少了它们
    // 明细就配不上当天 readdir 的名单), worker 每落一个结局追加一条.
    // 收工时搬进 rec.assets 一起落盘.
    std::vector<EncodeDayIndexEntry> index;
  };
  std::mutex days_mutex;
  std::map<std::string, DayProgress> days_inflight;

  // 本轮真正动过的天 (producer 列举出活儿就记, 整天跳过的不记).
  // 收工后交给增量扫描做定向重扫, 见 Asset::binary.dirty_dates.
  std::set<std::string> days_touched;
};

// producer: 逐天 [列举 → 增量过滤 → 切批 → 推批].
// progress 可空; 汇总行以天为单位 (总量 = 全部日期数, 开跑即精确),
// 整天跳过 (产物全部新鲜) 的天由 producer 直接推进.
void encoding_producer(SharedData &data,
                       BatchQueue &queue,
                       std::atomic<bool> *cancel_flag,
                       bool skip_existing,
                       EncodeStats &stats,
                       size_t worker_count,
                       misc::ParallelProgress *progress);

void encoding_worker(SharedData &data,
                     BatchQueue &queue,
                     std::atomic<bool> *cancel_flag,
                     EncodeStats &stats,
                     unsigned int worker_id,
                     misc::ProgressHandle progress_handle);
