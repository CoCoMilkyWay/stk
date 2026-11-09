#include "shared/Asset.hpp"
#include "fundamental/AssetAxis.hpp"
#include "misc/exec.hpp"
#include "shared/AssetInfo.hpp"
#include "shared/Progress.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

// ============================================================================
// AssetItem Implementation
// ============================================================================

AssetItem::AssetItem(size_t id, std::string code, std::string name, std::string exch, misc::Ymd start, misc::Ymd end)
    : asset_id(id),
      asset_code(std::move(code)),
      asset_name(std::move(name)),
      exchange(std::move(exch)),
      exchange_type(L2::infer_exchange_type(asset_code)),
      start_date(start),
      end_date(end) {}

// ============================================================================
// Asset Implementation
// ============================================================================

std::shared_ptr<const OrdersCatalog> Asset::catalog() const {
  std::lock_guard<std::mutex> lock(catalog_mutex_);
  return catalog_;
}

void Asset::publish_catalog(std::shared_ptr<const OrdersCatalog> catalog) {
  assert(catalog && "publish_catalog: 空快照用默认构造的 OrdersCatalog, 不用 nullptr");
  std::lock_guard<std::mutex> lock(catalog_mutex_);
  catalog_ = std::move(catalog);
}

void Asset::adopt(AssetScan &&scan) {
  orders = std::move(scan.orders);
  archive = std::move(scan.archive);
  backtest = std::move(scan.backtest);
  publish_catalog(std::move(scan.catalog));
  date_stats = std::move(scan.date_stats);
  date_gaps = std::move(scan.date_gaps);
  asset_stats = std::move(scan.asset_stats);
  ++generation;
}

// ============================================================================
// Orders Scan
// ============================================================================

void Asset::scan_orders(AssetScan &out, const std::string &orders_dir, misc::exec::ForkJoin &ex, Progress &p) const {
  namespace fs = std::filesystem;

  // 整体重建帧目录, 不做增量 (见 OrdersCatalog); encoded_assets 由 compute_coverage_statistics 填
  Orders &ord = out.orders;
  ord = Orders{};
  ord.scanned = true;
  ord.path = orders_dir;
  ord.exists = fs::exists(orders_dir) && fs::is_directory(orders_dir);
  out.catalog = std::make_shared<const OrdersCatalog>();
  if (!ord.exists)
    return;

  // ------------------------------------------------------------------
  // 1. 行 = 全部日文件 (升序), 列 = items; 整块一次定形, 之后各天的任务只往
  //    自己那一行写 (不同天 = 不相交的连续段, 免锁免合并).
  // ------------------------------------------------------------------
  assert(items.size() == asset_axis().size() && "items 未与 A 轴对齐 (AssetLoader::load 没跑?)");
  auto cat = std::make_shared<OrdersCatalog>(orders_days(orders_dir), items.size());
  const size_t n_days = cat->days.size();

  // ------------------------------------------------------------------
  // 2. 每天一个任务: 一次 open, pread 头 + 尾部目录, 灌进当天那一行.
  // ------------------------------------------------------------------
  struct DayOut {
    size_t orders = 0;  // 当天条数
    double bytes = 0.0; // 当天帧体积
    size_t frames = 0;  // 当天有帧的资产数
  };
  std::vector<DayOut> outs(n_days);

  p.begin(Phase::Scanning, n_days);
  ex.tasks(static_cast<int>(n_days), [&](int t, int) {
    const size_t d = static_cast<size_t>(t);
    cat->fill_day(d, orders_dir);
    DayOut &o = outs[d];
    for (const FrameRef &ref : cat->row(d)) {
      if (ref.empty())
        continue;
      o.orders += ref.head.count;
      o.bytes += static_cast<double>(ref.size);
      ++o.frames;
    }
    p.publish(1);
  });

  // ------------------------------------------------------------------
  // 3. 汇总 (按天, 几百项; 全库帧目录那趟遍历归 coverage 阶段)
  // ------------------------------------------------------------------
  size_t total_orders = 0;
  double total_bytes = 0.0;
  for (size_t d = 0; d < n_days; ++d) {
    const DayOut &o = outs[d];
    total_orders += o.orders;
    total_bytes += o.bytes;
    // 一帧都没有的日文件 (全是墓碑) 不算"有这天", 否则会把 min/max 区间往外撑
    if (o.frames > 0)
      ord.dates.insert(cat->days[d]);
  }
  out.catalog = std::move(cat);

  ord.total_orders = total_orders;
  ord.orders_size_gb = static_cast<float>(total_bytes / (1024.0 * 1024.0 * 1024.0));

  if (!ord.dates.empty()) {
    ord.min_date = *ord.dates.begin();
    ord.max_date = *ord.dates.rbegin();
  }
}

// ============================================================================
// Archive Database Scan
// ============================================================================

void Asset::scan_archive(AssetScan &out, const std::string &archive_dir, const std::string &archive_extension,
                         misc::exec::ForkJoin &ex, Progress &p) {
  namespace fs = std::filesystem;

  Archive &arc = out.archive;
  arc = Archive{};
  arc.scanned = true;
  arc.path = archive_dir;
  arc.exists = fs::exists(archive_dir) && fs::is_directory(archive_dir);
  if (!arc.exists)
    return;

  // Collect all month paths (archive_dir/YYYY/YYYYMM/).
  // 与 orders 扫描同理: 按年切只有十来个任务, 按月切才喂得满执行器.
  std::vector<std::string> month_paths;
  for (const auto &year_entry : fs::directory_iterator(archive_dir)) {
    if (!year_entry.is_directory())
      continue;
    for (const auto &month_entry : fs::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory())
        continue;
      month_paths.push_back(month_entry.path().string());
    }
  }

  // 一月一格 (task 下标即格), 段间不相交免锁; 收尾按月序合并
  struct MonthOut {
    std::set<misc::Ymd> dates;
    size_t files = 0;
    double bytes = 0.0;
  };
  std::vector<MonthOut> outs(month_paths.size());

  p.begin(Phase::Scanning, month_paths.size());
  ex.tasks(static_cast<int>(month_paths.size()), [&](int m, int) {
    MonthOut &o = outs[static_cast<size_t>(m)];
    for (const auto &file_entry : fs::directory_iterator(month_paths[static_cast<size_t>(m)])) {
      if (!file_entry.is_regular_file() || file_entry.path().extension().string() != archive_extension)
        continue;
      const std::string filename = file_entry.path().stem().string();
      if (const misc::Ymd date = filename.size() == 8 ? misc::parse_ymd(filename) : 0) {
        o.dates.insert(date);
        ++o.files;
        o.bytes += static_cast<double>(file_entry.file_size());
      }
    }
    p.publish(1);
  });

  double total_bytes = 0.0;
  for (MonthOut &o : outs) {
    arc.dates.merge(o.dates);
    arc.total_files += o.files;
    total_bytes += o.bytes;
  }
  arc.total_size_gb = static_cast<float>(total_bytes / (1024.0 * 1024.0 * 1024.0));

  if (!arc.dates.empty()) {
    arc.min_date = *arc.dates.begin();
    arc.max_date = *arc.dates.rbegin();
  }
}

// ============================================================================
// Backtest Coverage Analysis
// ============================================================================

void Asset::compute_backtest_coverage(AssetScan &out, misc::Ymd start, const AssetInfo &asset_info) {
  Backtest &bt = out.backtest;
  bt = Backtest{};
  bt.start = start;
  bt.end = std::max(out.orders.max_date, out.archive.max_date);

  // 行 = 基本面交易日历 (权威, archive 自身缺日也能发现); 调用方保证基本面 Ready (ScanService assert).
  // 两源未扫描 / 不存在时其日期集为空, 行上自然标"无".
  const std::vector<CalendarDay> &stock_days = asset_info.days;
  assert(!stock_days.empty() && "基本面交易日历未就绪");
  for (const CalendarDay &day : stock_days)
    if (day.trading && day.date >= bt.start && day.date <= bt.end)
      bt.days.push_back({day.date, out.orders.dates.count(day.date) > 0, out.archive.dates.count(day.date) > 0});
  assert(std::is_sorted(bt.days.begin(), bt.days.end(),
                        [](const BacktestDay &a, const BacktestDay &b) { return a.date < b.date; }) &&
         "基本面交易日历须升序");

  // 区间内有数据的天数 (区间内条数/体积要遍历帧目录, 归 compute_coverage_statistics 那趟)
  out.orders.backtest_order_days = bt.count([](const BacktestDay &d) { return d.orders; });
}

std::vector<misc::Ymd> Asset::backtest_dates() const {
  std::vector<misc::Ymd> dates;
  dates.reserve(backtest.days.size());
  for (const BacktestDay &day : backtest.days)
    dates.push_back(day.date);
  return dates;
}

// ============================================================================
// Coverage Statistics (Calendar / ASSET / Encode 共用的一份统计)
// ============================================================================

void Asset::compute_coverage_statistics(AssetScan &out, const AssetInfo &asset_info, misc::exec::ForkJoin &ex,
                                        Progress &p) const {
  const std::vector<CalendarDay> &stock_days = asset_info.days;
  const Backtest &bt = out.backtest;
  assert(asset_info.stocks.size() == items.size() && "AssetInfo 未与 items 对齐 (AssetLoader::load 没跑?)");

  std::unordered_map<misc::Ymd, DateStats> local_date_stats;
  std::vector<AssetStats> local_asset_stats(items.size());

  const size_t n_workers = static_cast<size_t>(ex.threads());

  // 全库帧目录唯一的一趟遍历, 按连续的天段切 (一段一个 task; 日主序, 每段顺扫一块
  // 连续内存): 每段一份每资产的全库口径两列 (Table 的 Days / Orders) 部分和,
  // 顺路把回测区间内的条数/体积按段累加. 结束后逐资产合并, 再数"至少有一天
  // 有数据"的资产.
  struct RangeSum {
    size_t orders = 0;
    double bytes = 0.0;
    std::vector<size_t> days_by_asset;   // 有帧的天数
    std::vector<size_t> orders_by_asset; // 逐笔条数
  };
  const std::shared_ptr<const OrdersCatalog> &cat = out.catalog; // 本趟扫出 (或沿用) 的那份快照
  assert(cat->width <= items.size() && "帧目录宽于 items");
  const size_t n_days = cat->days.size();
  const size_t per_task = std::max<size_t>(1, (n_days + n_workers - 1) / n_workers);
  std::vector<RangeSum> range_sums((n_days + per_task - 1) / per_task);
  ex.tasks(static_cast<int>(range_sums.size()), [&](int t, int) {
    const size_t begin = static_cast<size_t>(t) * per_task;
    const size_t end = std::min(n_days, begin + per_task);
    RangeSum &sum = range_sums[static_cast<size_t>(t)];
    sum.days_by_asset.assign(cat->width, 0);
    sum.orders_by_asset.assign(cat->width, 0);
    for (size_t d = begin; d < end; ++d) {
      const bool in_range = cat->days[d] >= bt.start && cat->days[d] <= bt.end;
      const std::span<const FrameRef> row = cat->row(d);
      for (size_t i = 0; i < row.size(); ++i) {
        if (row[i].empty())
          continue; // 密集: 空格子不是"有数据的天"
        ++sum.days_by_asset[i];
        sum.orders_by_asset[i] += row[i].head.count;
        if (in_range) {
          sum.orders += row[i].head.count;
          sum.bytes += static_cast<double>(row[i].size);
        }
      }
    }
  });

  // 每资产的常量先摊平: 上市 / 退市 / 是否排除, 日期×资产的内循环只读这一张表.
  struct AssetKey {
    misc::Ymd list_date = 0;   // 0 = 不限
    misc::Ymd delist_date = 0; // 0 = 不限
    bool excluded = false;     // 北交所: L2 archive 从不覆盖
  };
  std::vector<AssetKey> keys(items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    const StockInfo &s = asset_info.stocks[i];
    keys[i] = {s.ipo_date, s.out_date, items[i].exchange == "BJ"};
  }

  const Orders &ord = out.orders;
  const bool has_db_range = ord.min_date && ord.max_date;
  const bool has_bt_range = bt.start && bt.end;

  // 要算的交易日先挑出来 (区间外的不进任务, 进度分母也只数这些).
  // per-date 统计沿用全库范围; per-asset 缺口只看回测区间 —— 区间外没编
  // 码不算缺, 那不是要跑的行情. 回测区间内的交易日即回测日历的行 (同源于
  // stock_days, 同序), bidx 逐个对上.
  struct DayJob {
    misc::Ymd date;
    size_t cal;  // 全日历下标 (AssetInfo::days / suspended 的行)
    size_t didx; // 日期轴下标, 内循环 O(1) 定址
    size_t bidx; // 回测日历行 (in_backtest 时有效)
    bool in_db_range;
    bool in_backtest;
    bool archive_has_day;
  };
  std::vector<DayJob> jobs;
  jobs.reserve(stock_days.size());
  size_t next_bidx = 0;
  for (size_t cal = 0; cal < stock_days.size(); ++cal) {
    const CalendarDay &day = stock_days[cal];
    const bool in_db_range =
        !has_db_range || (day.date >= ord.min_date && day.date <= ord.max_date);
    const bool in_backtest = has_bt_range && day.trading &&
                             day.date >= bt.start && day.date <= bt.end;
    if (!in_db_range && !in_backtest)
      continue;

    DayJob job;
    job.date = day.date;
    job.cal = cal;
    job.didx = cat->day_idx(day.date);
    job.bidx = in_backtest ? next_bidx++ : 0;
    job.in_db_range = in_db_range;
    job.in_backtest = in_backtest;
    job.archive_has_day = in_backtest && bt.days[job.bidx].archive;
    assert((!in_backtest || bt.days[job.bidx].date == day.date) && "回测日历与交易日历错位 (compute_backtest_coverage 没先跑?)");
    jobs.push_back(job);
  }
  assert(next_bidx == bt.days.size() && "回测日历与交易日历错位");
  std::vector<DateGap> local_date_gaps(bt.days.size()); // 一天一格, 段间不相交, 免合并

  // 这个双重循环 (交易日 × 全部资产) 是整个 coverage 阶段唯一要等的部分,
  // 进度就报它. 上面那几趟 items 遍历相比之下可以忽略.
  p.begin(Phase::Running, jobs.size());

  // 按连续的交易日段切任务: 每段自己的 date_stats (按天, 段间不相交), date_gaps
  // 直接写各自那几格, 再加一份 per-asset 部分和, 结束后按段序合并 —— 缺失日期样本取的
  // 是"最早的 kMissingSample 个", 段序 = 时间序才能保住这一点.
  // 段数取线程数的两倍摊平尾部; 每段一份 AssetStats 向量是几百 KB.
  struct Chunk {
    std::unordered_map<misc::Ymd, DateStats> date_stats;
    std::vector<AssetStats> asset_stats; // 只填回测口径 (expected/missing/样本)
  };
  const size_t n_chunks = std::max<size_t>(1, std::min(jobs.size(), n_workers * 2));
  std::vector<Chunk> chunks(n_chunks);
  const size_t per_chunk = jobs.empty() ? 1 : (jobs.size() + n_chunks - 1) / n_chunks;

  ex.tasks(static_cast<int>(n_chunks), [&](int c, int) {
    const size_t begin = static_cast<size_t>(c) * per_chunk;
    const size_t end = std::min(jobs.size(), begin + per_chunk);
    if (begin >= end)
      return;
    Chunk &ck = chunks[static_cast<size_t>(c)];
    ck.asset_stats.assign(items.size(), AssetStats{});
    std::vector<uint8_t> susp(items.size(), 0); // 当日停牌位图, 按资产下标

    for (size_t j = begin; j < end; ++j) {
      const DayJob &job = jobs[j];

      // 当日停牌名单 → 位图 (资产下标 = 全局 id)
      std::fill(susp.begin(), susp.end(), 0);
      for (const uint32_t id : asset_info.suspended[job.cal])
        susp[id] = 1;

      DateStats *ds = job.in_db_range ? &ck.date_stats[job.date] : nullptr;
      // 一天一个条目, 哪怕零缺口 —— By Date 表要能说"这天检查过, 没事"
      DateGap *dg = job.in_backtest ? &local_date_gaps[job.bidx] : nullptr;

      for (size_t i = 0; i < items.size(); ++i) {
        const AssetKey &k = keys[i];
        if (k.excluded || susp[i])
          continue;
        if (k.list_date && job.date < k.list_date)
          continue;
        // 退市日当天已经不交易了 (最后交易日是它之前那个交易日), 用 > 的话
        // 每只退市股都会平白多出一天缺口 —— 实测 145 只退市股各缺 1 天, 缺
        // 的正是各自的 delist_date.
        if (k.delist_date && job.date >= k.delist_date)
          continue;

        // 墓碑 = 编码器判定当天源数据为空, 与基本面停牌同义: 不在分母里. 缺失因此
        // 只剩缺席 (没编过 / Corrupt / Invalid), 即重跑增量能补回来的那部分.
        const FrameRef &cell = cat->at(job.didx, i);
        if (cell.tombstone())
          continue;
        const bool has_orders = !cell.empty();

        if (ds) {
          ds->total_assets++;
          if (has_orders)
            ds->assets_with_orders++;
        }

        if (job.in_backtest) {
          AssetStats &st = ck.asset_stats[i];
          st.expected_days++;
          dg->expected++;
          if (!has_orders) {
            st.orders_missing++;
            dg->orders_missing++;
            if (st.orders_missing_sample.size() < kMissingSample)
              st.orders_missing_sample.push_back(job.date);
          }
          if (!job.archive_has_day) {
            st.archive_missing++;
            dg->archive_missing++;
            if (st.archive_missing_sample.size() < kMissingSample)
              st.archive_missing_sample.push_back(job.date);
          }
        }
      }

      p.publish(1);
    }
  });

  {
    size_t backtest_orders = 0;
    double backtest_bytes = 0.0;
    for (const RangeSum &s : range_sums) {
      backtest_orders += s.orders;
      backtest_bytes += s.bytes;
      for (size_t i = 0; i < s.days_by_asset.size(); ++i) {
        local_asset_stats[i].total_days += s.days_by_asset[i];
        local_asset_stats[i].total_orders += s.orders_by_asset[i];
      }
    }
    size_t encoded_assets = 0;
    for (const AssetStats &st : local_asset_stats)
      if (st.total_days > 0)
        ++encoded_assets;
    out.orders.backtest_orders = backtest_orders;
    out.orders.backtest_orders_size_gb = static_cast<float>(backtest_bytes / (1024.0 * 1024.0 * 1024.0));
    out.orders.encoded_assets = encoded_assets;
  }

  // 按段序合并. 段的 per-asset 部分和只有回测口径那几列, 全库口径两列已由
  // 上面的天段部分和合并进 local_asset_stats.
  for (Chunk &chunk : chunks) {
    local_date_stats.merge(chunk.date_stats);
    for (size_t i = 0; i < chunk.asset_stats.size(); ++i) {
      AssetStats &st = local_asset_stats[i];
      AssetStats &cs = chunk.asset_stats[i];
      st.expected_days += cs.expected_days;
      st.orders_missing += cs.orders_missing;
      st.archive_missing += cs.archive_missing;
      for (const misc::Ymd d : cs.orders_missing_sample)
        if (st.orders_missing_sample.size() < kMissingSample)
          st.orders_missing_sample.push_back(d);
      for (const misc::Ymd d : cs.archive_missing_sample)
        if (st.archive_missing_sample.size() < kMissingSample)
          st.archive_missing_sample.push_back(d);
    }
  }

  out.date_stats = std::move(local_date_stats);
  out.date_gaps = std::move(local_date_gaps);
  out.asset_stats = std::move(local_asset_stats);
}
