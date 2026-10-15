#include "worker/encoding_worker.hpp"
#include "shared/SharedData.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/OrderEncoder.hpp"
#include "codec/OrderFrame.hpp"
#include "misc/affinity.hpp"
#include "misc/archive.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"
#include "shared/AssetAxis.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

// ============================================================================
// BATCH QUEUE
// ============================================================================

bool BatchQueue::push(EncodeBatch batch) {
  std::unique_lock<std::mutex> lock(mutex_);
  not_full_.wait(lock, [this] { return closed_ || queue_.size() < capacity_; });
  if (closed_)
    return false;
  queue_.push_back(std::move(batch));
  lock.unlock();
  not_empty_.notify_one();
  return true;
}

bool BatchQueue::pop(EncodeBatch &out) {
  std::unique_lock<std::mutex> lock(mutex_);
  not_empty_.wait(lock, [this] { return closed_ || !queue_.empty(); });
  if (queue_.empty())
    return false; // 已 close 且排空
  out = std::move(queue_.front());
  queue_.pop_front();
  lock.unlock();
  not_full_.notify_one();
  return true;
}

void BatchQueue::close() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
  }
  not_empty_.notify_all();
  not_full_.notify_all();
}

// ============================================================================
// ENCODING PRODUCER — 列举 + 推元数据批
// ============================================================================

namespace {

// 一批最多多少个资产 — 权衡两件事:
//   摊薄 unrar 固定开销 (进程启动 + 三万条目包头扫描): 批越大越省, 实测 20 个
//   资产一次调用比 20 次单独调用快 3.3x;
//   负载均衡: 批是 worker 的调度粒度, 批太大会在收尾时让部分 worker 空转.
constexpr size_t kAssetsPerBatch = 64;

// 包内文件名, 下标即 L2::Member
using MemberFiles = std::array<const std::string *, L2::kMemberCount>;
MemberFiles member_files(const Config &config) {
  return {&config.csv_market_order, &config.csv_market_trade, &config.csv_market_data};
}

// 增量新鲜度规则: 日文件存在且不老于归档 → 它的结局可信.
//   - 原子落盘保证"存在即完整", 一次 stat 就够, 不需要读帧;
//   - 归档被重下/修复过 → mtime 变新 → 日文件判过期, 整天重编覆盖.
bool day_file_fresh(const std::string &orders_dir, const std::string &archive_path, misc::Ymd date) {
  std::error_code ec;
  std::filesystem::file_time_type archive_mtime = std::filesystem::last_write_time(archive_path, ec);
  if (ec)
    archive_mtime = {};
  const auto day_mtime = std::filesystem::last_write_time(orders_day_path(orders_dir, date), ec);
  return !ec && day_mtime >= archive_mtime;
}

// 列举结果按资产归并: 同一资产的委托/成交/行情条目在包里是分开的三条.
// 全市场下不能拿 ipo/退市区间去盲试 (那会对当天包里没有的资产各发一次 unrar,
// 每次重走三万条目的包头), 列举拿到的就是当天真实 universe, 再按 A 轴过滤掉
// ETF/基金 (轴只含股票).
std::unordered_map<size_t, EncodeTask> group_by_asset(const std::vector<misc::ArchiveEntry> &entries,
                                                      const MemberFiles &files, const AssetAxis &axis, misc::Ymd date) {
  std::unordered_map<size_t, EncodeTask> by_asset;
  for (const auto &entry : entries) {
    // "20260803/000001.SZ/逐笔委托.csv" → code_ex, filename
    const size_t first = entry.path.find('/');
    if (first == std::string::npos)
      continue;
    const size_t second = entry.path.find('/', first + 1);
    if (second == std::string::npos)
      continue;

    const std::string code_ex = entry.path.substr(first + 1, second - first - 1);
    const std::string filename = entry.path.substr(second + 1);

    // 行情.csv 不编码落盘, 但要读进来做准入校验 (见 L2_Validator.hpp)
    size_t m = 0;
    while (m < L2::kMemberCount && filename != *files[m])
      ++m;
    if (m == L2::kMemberCount)
      continue; // 委托队列.csv 等历史遗留文件

    // 代码变更前的日子, 归档里是老代码, 轴上只有新代码 — 换回来才认得出
    // (见 config::CODE_CHANGES). 无变更记录时 current_code 原样返回.
    const size_t asset_id = axis.find(config::current_code(code_ex, date));
    if (asset_id == axis.size())
      continue; // 非股票 (ETF/基金) 或轴外代码

    EncodeTask &task = by_asset[asset_id];
    task.asset_id = asset_id;
    task.member[m] = {entry.index, entry.size};
  }
  return by_asset;
}

} // namespace

void encoding_producer(SharedData &data,
                       BatchQueue &queue,
                       std::atomic<bool> *cancel_flag,
                       bool skip_existing,
                       EncodeStats &stats,
                       size_t worker_count,
                       misc::ParallelProgress *progress) {
  TraceNS("EncodingProducer", 5);

  const AssetAxis &axis = asset_axis();
  assert(data.asset.items.size() == axis.size() &&
         "items 未与 A 轴对齐 (AssetLoader::load 没跑?)");
  // 空轴下每一天都会退化成"零分母" (包里认不出任何资产), 编出来的只有一堆
  // 空账目 — 与其跑完再排查, 不如开跑前就断在这里
  assert(!axis.empty() && "A 轴为空 (基本面未就绪?), 编码无从进行");
  assert(worker_count > 0 && "encoding_producer: worker_count 必须为正");
  const MemberFiles files = member_files(data.config);

  // 编码日期轴 = 回测日历 (交易日历 ∩ 回测区间), 与特征计算同一条轴; 进度条总量
  // (EncodingService) 也是它. 归档缺的天在列举处按 Missing 跳过.
  const std::vector<misc::Ymd> dates = data.asset.backtest_dates();
  assert(data.asset.backtest.start == data.config.start_date && data.asset.backtest.end == data.config.end_date &&
         "回测日历与配置区间不符 (改配置后没重算覆盖?)");

  const size_t total_days = dates.size();

  for (size_t idx = 0; idx < total_days && !cancel_flag->load(); ++idx) {
    const misc::Ymd date = dates[idx];
    TraceN("ProducerDay");
    TraceTextS(misc::dense(date).c_str());

    const std::string archive_path = archive_day_path(data.config.archive_dir, date);

    const std::string &orders_dir = data.config.orders_dir;

    // 上一轮的日文件 (见 shared/OrdersDay.hpp), 新鲜才认 (新鲜 ⇒ 存在). 三处用它:
    // 整天快路径看 complete, 下面的逐资产过滤看目录里的帧与墓碑, 新日文件从它
    // adopt 旧帧.
    std::optional<DayReader> prev;
    if (skip_existing && day_file_fresh(orders_dir, archive_path, date))
      prev.emplace(orders_dir, date);

    // 整天快路径 — 必须在列举之前: 下面那次 unrar l 要在机械盘上走完整条包头
    // 链 (实测 0.2~2.5s), 光"确认无事可做"就是几秒一天.
    //
    // 出错的天同样会留下日文件 (供界面按日拆解原因), 所以判据是 complete 项
    // 而不是文件在不在 —— 只有整天齐备才能跳过.
    if (prev && prev->index().rec.complete) {
      stats.days_skipped.fetch_add(1);
      if (progress)
        progress->bump_summary(1, false); // 秒回的一天, 不参与 ETA 速率
      continue;
    }

    // ------------------------------------------------------------------
    // 列举 — 当日包里"实际有哪些资产、每个文件多大". 尺寸在这一步就要
    // 拿到 —— worker 靠它在 unrar p 的输出流上切分边界.
    // ------------------------------------------------------------------
    std::vector<misc::ArchiveEntry> entries;
    const misc::ArchiveListStatus list_status =
        misc::list_archive(archive_path, config::ARCHIVE_TOOL, entries);

    if (list_status == misc::ArchiveListStatus::Corrupt) {
      // 包头链断了, 这天连有哪些资产都问不出来 — 留日志跳过, 不落完成标记,
      // 人工修好源文件后靠增量自动补齐 (绝不 abort, 见 archive.hpp)
      Logger::log("encoding", "[CORRUPT ARCHIVE] " + archive_path +
                                  " — cannot list, day skipped, source needs repair");
      stats.days_corrupt.fetch_add(1);
      if (progress)
        progress->bump_summary(1, false);
      continue;
    }

    // 没有源: 这天既无从编码, 也无从判断齐备 — 直接走, 绝不落完成标记.
    // 盘上很可能还留着上一轮的半成品 (归档事后被移走/还没下回来), 一旦标成
    // complete, 增量的快路径就再也不会回来补这一天.
    if (list_status == misc::ArchiveListStatus::Missing) {
      if (progress)
        progress->bump_summary(1, false);
      continue;
    }

    const std::unordered_map<size_t, EncodeTask> by_asset = group_by_asset(entries, files, axis, date);

    // 断点续跑: 旧日文件 (新鲜的话) 里已有结局的资产不重编 —— 帧或墓碑原样
    // 搬进新日文件.
    //
    // 顺带填当天账目: 分母是这里认下的资产数, 搬过来的按结局归到 Ok /
    // Tombstone —— 增量跑只重编缺结局的那些, 光靠 worker 的销账凑不齐全天的
    // 分类. 不在当天包里的旧结局不搬 (分母里没有它).
    DayIndex day;
    std::vector<EncodeTask> tasks;
    tasks.reserve(by_asset.size());
    for (const auto &[asset_id, task] : by_asset) {
      if (task.member[size_t(L2::Member::Order)].size == 0)
        continue; // 没有委托文件, 无从重建盘口

      ++day.rec.total;

      if (prev) {
        if (const FrameRef *old = prev->index().find(asset_id)) {
          stats.pairs_skipped.fetch_add(1);
          day.settle(old->tombstone() ? L2::Outcome::Tombstone : L2::Outcome::Ok, 0, *old);
          continue;
        }
      }
      tasks.push_back(task);
    }

    // 收尾统计计数
    if (!tasks.empty()) {
      std::lock_guard<std::mutex> lock(stats.assets_mutex);
      for (const auto &task : tasks)
        stats.assets_with_work.insert(task.asset_id);
    }
    stats.pairs_listed.fetch_add(tasks.size());

    // 零分母不算"齐备": total == 0 意味着包里一个 A 轴资产都没认出来
    // (包内结构不符 / 轴与归档口径错位), 那是"什么都没看见"而不是"什么都齐
    // 了". 真齐备的天分母是几千. 标了 complete 增量就再也不来, 半成品会被
    // 永久冻在盘上 — 所以这种天留日志跳过, 不写日文件.
    if (day.rec.total == 0) {
      Logger::log("encoding", "[NO ASSETS] " + archive_path + " — " +
                                  std::to_string(entries.size()) +
                                  " entries listed but none on the asset axis, day skipped, "
                                  "no day file written");
      if (progress)
        progress->bump_summary(1, false);
      continue;
    }

    // 新日文件: 先把旧日文件里搬得过来的帧整段拷进去 (就地改写目录项的
    // 偏移), worker 再往后追加新编的帧.
    auto job = std::make_shared<DayJob>(orders_dir, date);
    job->index = std::move(day);
    if (prev)
      job->writer.adopt(*prev, job->index.refs);
    prev.reset();

    if (tasks.empty()) {
      // 包里没活 (旧日文件不齐备, 但缺的那些这回不在包里了) 也是完成了一天 —
      // 走到这里说明每个资产都有了结局, 一个错误都没剩 (出错的对既没有帧也
      // 没有墓碑, 必然被上面重新列成任务). 换上标了 complete 的日文件, 下次
      // 连列举都省了.
      job->index.rec.complete = 1;
      job->writer.commit(std::move(job->index));
      if (progress)
        progress->bump_summary(1, false); // 只列举没真编, 不参与 ETA 速率
      continue;
    }

    // 按归档序排, 再切成批 — unrar p 的输出顺序是归档顺序 (已实测),
    // worker 按这个顺序在流上切分.
    constexpr size_t kOrder = size_t(L2::Member::Order);
    std::sort(tasks.begin(), tasks.end(), [](const EncodeTask &a, const EncodeTask &b) {
      return a.member[kOrder].index < b.member[kOrder].index;
    });
    job->total = tasks.size();

    // 注册天粒度账本 — 必须在推批之前 (worker 销账时要查账).
    // 若这是当前最老的在编天, 顺手把附注立起来 (否则第一天列举/预读的
    // 几十秒里汇总行是空的).
    {
      std::lock_guard<std::mutex> lock(stats.days_mutex);
      const bool oldest = stats.days_inflight.empty();
      stats.days_inflight.emplace(date, job);
      if (oldest && progress)
        progress->set_summary_note(std::to_string(date) + ": 0/" + std::to_string(tasks.size()) +
                                   " assets");
    }

    const size_t even_batch_size = (tasks.size() + worker_count - 1) / worker_count;
    const size_t batch_size = std::min(kAssetsPerBatch, even_batch_size);
    assert(batch_size > 0 && "encoding_producer: batch_size 必须为正");

    for (size_t i = 0; i < tasks.size() && !cancel_flag->load(); i += batch_size) {
      EncodeBatch batch;
      batch.date = date;
      batch.archive_path = archive_path;
      batch.tasks.assign(tasks.begin() + static_cast<long>(i), tasks.begin() + static_cast<long>(std::min(i + batch_size, tasks.size())));
      batch.day = job;
      if (!queue.push(std::move(batch)))
        return; // 队列已关 = 取消
    }

    TraceFrame;
  }
}

// ============================================================================
// ENCODING WORKER — 每批自己 unrar + 解码 + 往当天日文件追加帧
// ============================================================================
//
// worker 只写日文件, 不回填帧目录 (Asset::orders()).
//
// 同一天的不同资产落在不同批上, 由不同 worker 并发处理 —— 再去写同一行帧
// 目录就要加锁. 编码产物的真相由编码后的重新扫描统一读取 (ScanService).

namespace {

// 批内一个待读成员在流上的位置
struct StreamSlot {
  size_t task_idx;
  L2::Member member;
};

// 一次 unrar p 的读取计划: 成员按归档序排好, 同一资产的几块连成一段
struct ReadPlan {
  std::vector<std::string> paths;
  std::vector<size_t> sizes;
  std::vector<StreamSlot> slots;
};

// 一批的读取计划, 跨批复用容量. plans[0] 是连成一段的大流, 其后每条是一个
// 不相邻资产的小流.
//
// 按归档序排是必须的: unrar p 的输出顺序是归档顺序, 与名单顺序无关 (已实测).
// 排错了就会拿 A 的尺寸去切 B 的字节, 静默产出垃圾.
//
// 流式切分假定"同一资产的几块在归档序上连成一段" — 包按 date/ASSET/ 目录
// 逐个写入时天然成立, 但修补过的归档会把补的文件追加到包尾, 于是同一资产的
// 几块在条目序上隔了上万条. 连成一段的资产共用 plans[0] 一条大流; 不相邻的
// 每个单独一条小流 (plans[1..]), 多付一次 unrar 固定开销, 但这种资产一天
// 最多个位数, 无关紧要. 判据是"首尾跨度等于块数": 一个资产有 1~3 块, 连成
// 一段当且仅当 last - first + 1 == count.
struct BatchPlan {
  std::vector<ReadPlan> plans;

  void build(const SharedData &data, const EncodeBatch &batch, const MemberFiles &files) {
    order_.clear();
    for (size_t t = 0; t < batch.tasks.size(); ++t)
      for (size_t m = 0; m < L2::kMemberCount; ++m)
        if (batch.tasks[t].member[m].size > 0)
          order_.emplace_back(batch.tasks[t].member[m].index, StreamSlot{t, static_cast<L2::Member>(m)});
    std::sort(order_.begin(), order_.end(),
              [](const auto &a, const auto &b) { return a.first < b.first; });

    constexpr size_t kNone = static_cast<size_t>(-1);
    std::vector<size_t> first_pos(batch.tasks.size(), kNone);
    std::vector<size_t> last_pos(batch.tasks.size(), kNone);
    std::vector<size_t> slot_count(batch.tasks.size(), 0);
    for (size_t i = 0; i < order_.size(); ++i) {
      const size_t t = order_[i].second.task_idx;
      if (first_pos[t] == kNone)
        first_pos[t] = i;
      last_pos[t] = i;
      ++slot_count[t];
    }
    size_t n_plans = 1;
    plan_of_.assign(batch.tasks.size(), 0);
    for (size_t t = 0; t < batch.tasks.size(); ++t) {
      assert(slot_count[t] > 0 && "encoding_worker: 任务没有委托成员 (producer 应已滤掉)");
      if (last_pos[t] - first_pos[t] + 1 != slot_count[t])
        plan_of_[t] = n_plans++;
    }

    plans.resize(n_plans);
    for (ReadPlan &plan : plans) {
      plan.paths.clear();
      plan.sizes.clear();
      plan.slots.clear();
    }
    for (const auto &[archive_index, slot] : order_) {
      const EncodeTask &task = batch.tasks[slot.task_idx];
      const AssetItem &asset = data.asset.items[task.asset_id];
      // 归档里是当时的代码 (见 config::CODE_CHANGES), 变更前的日子要换回老的
      const std::string asset_full = config::archive_code(asset.code_ex(), batch.date);
      const size_t m = static_cast<size_t>(slot.member);
      ReadPlan &plan = plans[plan_of_[slot.task_idx]];
      plan.paths.push_back(misc::dense(batch.date) + "/" + asset_full + "/" + *files[m]);
      plan.sizes.push_back(task.member[m].size);
      plan.slots.push_back(slot);
    }

#ifndef NDEBUG
    // 剔除不相邻者之后, 相邻性在每条计划里是按构造保证的 (剔除只会拉近剩余
    // 元素, 不会往中间插新东西). 万一还破 — 一个资产会被 begin/finish 两次,
    // 第二次把第一次的产物覆盖成半截数据, 完全无声, 与其信任不如当场炸.
    std::vector<bool> closed(batch.tasks.size(), false);
    for (const ReadPlan &plan : plans) {
      for (size_t i = 0; i < plan.slots.size(); ++i) {
        const size_t t = plan.slots[i].task_idx;
        assert(!closed[t] && "encoding_worker: 读取计划里仍有不相邻的资产, 切分会错位");
        if (i + 1 == plan.slots.size() || plan.slots[i + 1].task_idx != t)
          closed[t] = true;
      }
    }
#endif
  }

private:
  std::vector<std::pair<size_t, StreamSlot>> order_; // (归档序, slot)
  std::vector<size_t> plan_of_;                      // 批内下标 → 计划下标
};

// 天粒度账本记一笔结局 (DayIndex::settle), 本轮总账 (EncodeStats::run) 同一笔. 当天最后一笔 → rec.complete 定案,
// 出账本, 返回该天 (调用方锁外 commit); 否则返回空. 没留下结局的 ⇒
// rec.complete = 0, 下次增量必须重新列举补齐.
std::shared_ptr<DayJob> settle_in_day(EncodeStats &stats, const EncodeBatch &batch, misc::ProgressHandle &progress_handle,
                                      L2::Outcome outcome, uint32_t check_flags, const FrameRef &ref) {
  std::lock_guard<std::mutex> lock(stats.days_mutex);
  DayJob &job = *batch.day;
  job.index.settle(outcome, check_flags, ref);
  stats.run.tally(outcome, check_flags);

  std::shared_ptr<DayJob> finished;
  if (++job.done == job.total) {
    job.index.rec.complete = job.index.rec.errors() == 0 ? 1 : 0;
    const size_t erased = stats.days_inflight.erase(batch.date);
    assert(erased == 1 && "settle_in_day: 本天未在账本注册");
    (void)erased;
    finished = batch.day;
    progress_handle.bump_summary();
  }
  // 附注跟着最老的在编天走 (多天在飞时以它为准)
  if (!stats.days_inflight.empty()) {
    const auto &[day, oldest] = *stats.days_inflight.begin();
    progress_handle.set_summary_note(std::to_string(day) + ": " + std::to_string(oldest->done) + "/" +
                                     std::to_string(oldest->total) + " assets");
  }
  return finished;
}

} // namespace

void encoding_worker(SharedData &data,
                     BatchQueue &queue,
                     std::atomic<bool> *cancel_flag,
                     EncodeStats &stats,
                     unsigned int worker_id,
                     misc::ProgressHandle progress_handle) {
  TraceNS("EncodingWorker", 5);
  TraceValue(worker_id);
  TraceThread(("encoding_worker_" + std::to_string(worker_id)).c_str());

  static thread_local bool affinity_set = false;
  if (!affinity_set && misc::Affinity::supported()) {
    const unsigned int core_count = misc::Affinity::core_count();
    affinity_set = misc::Affinity::pin_to_core(core_count > 0 ? worker_id % core_count : 0);
  }

  L2::OrderEncoder encoder(L2::DEFAULT_ENCODER_ORDER_SIZE);
  L2::FrameEncoder framer;
  progress_handle.set_label("Idle");
  progress_handle.update(1, 1, "");

  Logger::log("encoding", "[Worker " + std::to_string(worker_id) + "] Started");

  const MemberFiles files = member_files(data.config);
  BatchPlan batch_plan;

  EncodeBatch batch;
  while (!cancel_flag->load() && queue.pop(batch)) {
    TraceN("BatchLoop");
    TraceTextS(misc::dense(batch.date).c_str());

    if (batch.tasks.empty())
      continue;

    batch_plan.build(data, batch, files);

    // 流式消费: 每个文件到达就地解析, 一个资产的几块都喂完就落盘.
    // 一次只持有一个文件的字节 — 批可以开得很大而内存不涨.
    size_t current_task = 0;
    bool fed_any = false;
    size_t done_in_batch = 0;

    // 批内任务的销账表. 源损坏会让一部分任务永远落不了盘, 但它们仍要在天
    // 账本里销账 —— 否则这天永远凑不齐 total, 进度会一直卡在它上面, 且后面
    // 的天在附注里永远排不到前面.
    std::vector<char> retired(batch.tasks.size(), 0);

    // 销账: 批内记一笔, 天粒度账本记一笔 (settle_in_day), 当天清零则 commit 日文件.
    auto retire_task = [&](size_t task_idx, L2::Outcome outcome, uint32_t check_flags, const FrameRef &ref) {
      assert(!retired[task_idx] && "retire_task: 同一任务销账两次");
      retired[task_idx] = 1;
      ++done_in_batch;
      progress_handle.update(done_in_batch, batch.tasks.size(), std::to_string(batch.date));

      const std::shared_ptr<DayJob> finished = settle_in_day(stats, batch, progress_handle, outcome, check_flags, ref);
      // 出错的天照样 commit —— complete=0 让增量重来 (已有结局的帧那时原样
      // 搬过去), 而界面在重跑之前就能说清楚那天错在哪. fdatasync 几 GB 不该
      // 压在锁里: 此刻当天全部任务已销账, 不会再有人往这个 writer 追加.
      if (finished)
        finished->writer.commit(std::move(finished->index));
    };

    auto flush_task = [&](size_t task_idx) {
      if (!fed_any)
        return;
      fed_any = false;
      const EncodeTask &task = batch.tasks[task_idx];
      const AssetItem &asset = data.asset.items[task.asset_id];

      uint32_t check_flags = 0;
      FrameRef ref{};
      const L2::Outcome outcome = encoder.finish(std::to_string(batch.date) + " " + asset.code_ex());
      switch (outcome) {
      case L2::Outcome::Ok:
        ref = batch.day->writer.put(task.asset_id, framer, encoder.orders());
        break;
      case L2::Outcome::Tombstone:
        // 持久否定缓存: 目录里记一条墓碑 (其余字段皆 0), 增量重跑不再碰它.
        // 墓碑的新鲜度随整个日文件走 —— 归档重下后日文件判过期, 这些资产
        // 照样重解一遍.
        ref = make_tombstone(task.asset_id);
        break;
      case L2::Outcome::Corrupt:
        // 源 CSV 坏行 (归档成员位腐烂). 什么都不写 — 不落帧 (半真半假会
        // 静默污染下游), 也不落墓碑 (那是"确实无数据"的语义). 修好源文件后
        // 增量自动重来.
        break;
      case L2::Outcome::Invalid:
        // 准入校验未过 (逐笔流不自洽或与快照对不上). 处置与 Corrupt 相同 ——
        // 什么都不写, 详情已由 finish 记入日志, 等人核查数据. 命中的判据同时
        // 进当天账目, 供 Encode 页按日拆解原因.
        check_flags = encoder.report().flags;
        break;
      }
      retire_task(task_idx, outcome, check_flags, ref);
    };

    // plans[0] 是连成一段的大流, 其后每条是一个不相邻资产的小流 —— 同一个循环.
    // 到达先后在资产内无所谓 (finish 统一合并排序).
    for (const ReadPlan &plan : batch_plan.plans) {
      if (plan.slots.empty())
        continue;
      TraceN("StreamPlan");
      const bool stream_ok = misc::stream_archive_files(
          batch.archive_path, config::ARCHIVE_TOOL, plan.paths, plan.sizes,
          [&](size_t i, const char *csv, size_t len) {
            const StreamSlot &slot = plan.slots[i];

            // 换资产了 → 先把上一个收尾落盘
            if (slot.task_idx != current_task) {
              flush_task(current_task);
              current_task = slot.task_idx;
            }
            if (!fed_any) {
              const AssetItem &asset = data.asset.items[batch.tasks[slot.task_idx].asset_id];
              encoder.begin(asset.code_ex());
              progress_handle.set_label(asset.asset_code + " " + asset.asset_name);
              fed_any = true;
            }

            encoder.feed(slot.member, csv, len);
          },
          cancel_flag);

      // 取消时流是中途断的, 手上可能只喂了半个资产 — 直接丢弃, 不落帧. 当天
      // 凑不齐销账, 日文件不会 commit, .tmp 随最后一个持有者析构删掉.
      if (cancel_flag->load())
        break;

      // 流断了 (成员 CRC 失败 / 长度对不上): 手上这个资产的字节不完整, 丢掉;
      // 已经落盘的那些是完整读出来的, 留着. 这条流剩下没销账的任务在批尾统一
      // 记为 Corrupt ⇒ 这天不落完成标记, 等人修好源包后增量重来.
      if (!stream_ok) {
        fed_any = false;
        Logger::log("encoding", "[CORRUPT ARCHIVE] " + batch.archive_path + " — stream broke at " +
                                    std::to_string(batch.date) + " (worker " + std::to_string(worker_id) +
                                    "), remaining assets in stream skipped");
      }

      flush_task(current_task); // 这条流的最后一个资产
    }
    if (cancel_flag->load())
      break;

    // 没能落盘的任务在这里销账 (归档流断的路径 — 手上的字节不完整, 那条流
    // 剩下的都编不出来). 正常批走不到这个循环体.
    for (size_t t = 0; t < batch.tasks.size(); ++t)
      if (!retired[t])
        retire_task(t, L2::Outcome::Corrupt, 0, FrameRef{});

    TraceFrame;
  }

  progress_handle.set_label("Idle");
  Logger::log("encoding", "[Worker " + std::to_string(worker_id) + "] Finished");
}
