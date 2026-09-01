#pragma once

#include "misc/progress_parallel.hpp"

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
// PHASE 1: ENCODING PIPELINE (单 producer 预热页缓存 → N worker 并行解压解码)
// ============================================================================
//
// 两个硬约束决定了架构:
//   - 解压必须并行: 单条 unrar 流单核只有几十 (资产, 日期)/s, 喂不满几十个
//     解码核 — 所以每个 worker 对自己的批各开一次 unrar (实测能跑满全部核心).
//   - HDD 只能吃顺序读: 几十路 unrar 并发直读盘会把顺序读打碎成寻道.
//
// 解法: producer 按天把整个 .rar 顺序读一遍丢进 OS 页缓存, 然后才把该天的
// 元数据批推给 worker — worker 的多路 unrar 全部命中内存, 盘上只有 producer
// 一条顺序流. 页缓存由内核管理, 不需要自己的内存记账 (机器必须是大内存,
// EncodingService 直接 assert, 不为小机器妥协).
//
// 天与天自然流水: worker 解 day N 时, producer 在列举/预读 day N+1.
//
// 批内内存不随批大小增长: 一次只持有一个文件的字节 (见 misc/archive.hpp 的
// stream_archive_files), 到达即解析成中间结构.

// 墓碑扩展名: "该 (资产, 日期) 的源数据不足以编码 (停牌/无深度)" 的持久否定
// 缓存 — 没有它, 增量重跑会对这些对反复开归档解码再失败一遍.
// 空文件, 与 .bin 同目录; 新鲜度规则与 .bin 相同 (归档更新则失效重试).
// 扫描端按 .bin 后缀过滤, 天然忽略它.
inline constexpr const char *kEncodeTombstoneExt = ".skip";

// 整天完成标记的文件名 (orders/YYYY/MM/DD/ 下的空文件).
//
// 存在且不老于当天归档 ⇒ 这一天的全部 (资产, 日期) 都已产出 .bin 或 .skip.
// 增量重跑靠它整天跳过, 省掉一次 unrar l (机械盘上 0.2~2.5s) 与当天几千次
// 产物 stat —— 没有它, "确认无事可做"本身就要几秒一天.
// 扫描端按 .bin 后缀过滤, 天然忽略它.
//
// 注意: 手工删除损坏的 .bin 时必须同时删掉当天的这个标记, 否则增量重跑会
// 整天跳过, 刚删掉的文件永远补不回来.
inline constexpr const char *kEncodeDayDoneName = ".day_complete";

// 一个 (资产, 日期) 的待编码任务.
//
// 只存下标与尺寸, 不存路径字符串: 包内路径由 (批的日期, 资产代码) 现场拼出来.
struct EncodeTask {
  size_t asset_id;
  size_t order_index; // 逐笔委托在归档内的序号 (排序键)
  size_t trade_index; // 逐笔成交在归档内的序号
  size_t order_size;  // 解压后字节数
  size_t trade_size;  // 0 表示该资产当日没有成交文件
};

// 一批共享同一个归档的任务 (纯元数据, 很小) — worker 的调度粒度,
// 也是一次 unrar p 调用的粒度 (摊薄进程启动 + 包头扫描的固定开销).
struct EncodeBatch {
  std::string date; // "20260803"
  std::string archive_path;
  std::vector<EncodeTask> tasks;
};

// 有界批队列 — producer (列举+预读) 与 worker (解压+解码) 之间的交接.
// 容量即流水深度: 大致对应"预读领先几天", 见 EncodingService 的容量常量.
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
  std::atomic<size_t> days_corrupt{0};  // 包头损坏, 整天列举不出来
  std::mutex assets_mutex;
  std::unordered_set<size_t> assets_with_work;

  // 天粒度进度账本: date → 该天的完成情况.
  // producer 列举完一天就注册, worker 每落盘一对就推进; 一天清零即从账本
  // 移除、把汇总行 (单位: days) +1, 并落下整天完成标记 (见
  // kEncodeDayDoneName). 汇总附注始终显示最老在编天的资产进度.
  struct DayProgress {
    size_t done = 0;
    size_t total = 0;
    size_t errors = 0; // 环境错误 (没留下产物) 的对数; 非零则不落完成标记
  };
  std::mutex days_mutex;
  std::map<std::string, DayProgress> days_inflight;

  // 本轮真正动过的天 (producer 列举出活儿就记, 整天跳过的不记).
  // 收工后交给扫描做定向重扫: 重编会覆盖同名 .bin, 目录条目没增删,
  // 光看目录 mtime 发现不了.
  std::set<std::string> days_touched;
};

// producer: 逐天 [列举 → 增量过滤 → 切批 → 顺序预读 .rar 进页缓存 → 推批].
// progress 可空; 汇总行以天为单位 (总量 = 全部日期数, 开跑即精确),
// 整天跳过 (产物全部新鲜) 的天由 producer 直接推进.
void encoding_producer(SharedData &data,
                       BatchQueue &queue,
                       std::atomic<bool> *cancel_flag,
                       bool skip_existing,
                       EncodeStats &stats,
                       misc::ParallelProgress *progress);

void encoding_worker(SharedData &data,
                     BatchQueue &queue,
                     std::atomic<bool> *cancel_flag,
                     EncodeStats &stats,
                     unsigned int worker_id,
                     misc::ProgressHandle progress_handle);
