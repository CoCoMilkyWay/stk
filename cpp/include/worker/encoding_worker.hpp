#pragma once

#include "misc/progress_parallel.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

// Forward declarations
struct SharedData;

// ============================================================================
// PHASE 1: ENCODING WORKER
// ============================================================================
//
// 工作单元是"批", 而不是单个 (资产, 日期):
//
// unrar 的固定开销 (进程启动 + 三万条目包头扫描) 是单资产提取的主要成本 ——
// 实测 20 个资产一次调用 0.390s, 而 20 次单资产调用 1.292s (3.3x). 所以一批
// 里的任务共享同一个归档、由一次 `unrar p` 调用流式取完.
//
// 批内内存不随批大小增长: 一次只持有一个文件的字节 (见 misc/archive.hpp 的
// stream_archive_files), 到达即解析成中间结构.

// 一个 (资产, 日期) 的待编码任务.
//
// 只存下标与尺寸, 不存路径字符串: 4 TB 压缩数据 ≈ 690 个交易日 × ~5000 资产,
// 任务表要全量驻留内存才能算出准确总量, 每条多带几个 std::string 就是几百 MB.
// 包内路径由 (批的日期, 资产代码) 现场拼出来.
struct EncodeTask {
  size_t asset_id;
  size_t order_index; // 逐笔委托在归档内的序号 (排序键, 见下)
  size_t trade_index; // 逐笔成交在归档内的序号
  size_t order_size;  // 解压后字节数
  size_t trade_size;  // 0 表示该资产当日没有成交文件

  // 该资产最后一个待编码日期 —— 编完它就意味着这个资产整体完成, 用来推进
  // 进度条上的资产计数. 批按日期升序入队且 FIFO 消费, 所以一个资产的末日
  // 任务必然最后被取走, 这个计数是准的.
  bool last_for_asset;
};

// 一批共享同一个归档的任务
struct EncodeBatch {
  std::string date; // "20260803"
  std::string archive_path;
  std::vector<EncodeTask> tasks;
};

// 有界批队列 — 生产端 (列举归档) 与消费端 (编码) 之间的交接.
// 有界是为了背压: 批表本身很小, 但没有上限的话生产端会把整个任务表一次性
// 灌进来, 失去"边列举边编码"的重叠.
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

void encoding_worker(SharedData &data,
                     BatchQueue &queue,
                     std::atomic<bool> *cancel_flag,
                     unsigned int worker_id,
                     misc::ProgressHandle progress_handle);
