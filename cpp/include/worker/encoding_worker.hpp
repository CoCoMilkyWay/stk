#pragma once

#include "codec/OrderEncoder.hpp" // L2::Member
#include "misc/date.hpp"
#include "misc/progress_parallel.hpp"
#include "shared/OrdersDay.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
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

// 一天的产物就是一个日文件 orders/YYYY/MM/YYYYMMDD.bin (帧 + 齐备标记 + 处置
// 分类 + 目录), 定义见 shared/OrdersDay.hpp.
//
// 它不老于当天归档且 complete ⇒ 这一天的全部 (资产, 日期) 都有了结局.
// 增量重跑靠它整天跳过, 省掉一次 unrar l (机械盘上 0.2~2.5s) —— 没有它,
// "确认无事可做"本身就要几秒一天.
//
// 不齐备的天增量补编: 旧日文件里已有结局的帧原样搬进新日文件 (免解压免重编),
// 只编缺的; 整天写完一次 rename 换掉旧文件. 取消则丢掉写了一半的 .tmp, 旧
// 文件原样不动.

// 一个归档成员 (CSV) 在包里的位置.
struct ArchiveMember {
  size_t index = 0; // 在归档内的序号
  size_t size = 0;  // 解压后字节数; 0 = 该资产当日没有这个文件
};

// 一个 (资产, 日期) 的待编码任务.
//
// 只存下标与尺寸, 不存路径字符串: 包内路径由 (批的日期, 资产代码, 成员) 现场
// 拼出来. 委托必有 (producer 只收有委托的资产); 缺行情的准入校验将判 MarketAbsent.
struct EncodeTask {
  size_t asset_id = 0;
  ArchiveMember member[L2::kMemberCount]; // 下标即 L2::Member
};

// 一天在飞的编码: 日文件写端 + 当天的账目与目录 + 销账进度.
//
// producer 先填上分母与"旧日文件里已有结局"的那部分 (帧已 adopt 进 writer),
// worker 每销一个账就记一笔结局 —— 两边同一个 DayIndex::settle. 最后一个
// 销账的 worker commit.
struct DayJob {
  DayJob(const std::string &dir, misc::Ymd date) : writer(dir, date) {}

  DayWriter writer;
  DayIndex index;
  size_t done = 0;  // 已销账的任务数 (仅本轮要编的)
  size_t total = 0; // 本轮要编的任务数
};

// 一批共享同一个归档的任务 (纯元数据, 很小) — worker 的调度粒度,
// 也是一次 unrar p 调用的粒度 (摊薄进程启动 + 包头扫描的固定开销).
struct EncodeBatch {
  misc::Ymd date = 0;
  std::string archive_path;
  std::vector<EncodeTask> tasks;
  std::shared_ptr<DayJob> day; // 压好的帧往 day->writer 追加
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
  std::atomic<size_t> days_corrupt{0};  // 包头损坏, 整天列举不出来
  std::mutex assets_mutex;
  std::unordered_set<size_t> assets_with_work;

  // 天粒度进度账本: date → 该天在飞的编码.
  // producer 列举完一天就注册, worker 每销账一对就推进; 一天清零即从账本
  // 移除、把汇总行 (单位: days) +1, 并 commit 当天日文件. 汇总附注始终显示
  // 最老在编天的资产进度.
  std::mutex days_mutex;
  std::map<misc::Ymd, std::shared_ptr<DayJob>> days_inflight;

  // 本轮销账的结局 + 判据, 与当天账目同一笔 tally (settle_in_day 里成对记);
  // days_mutex 保护, 全部 worker 退出后才读.
  DayRecord run;
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
