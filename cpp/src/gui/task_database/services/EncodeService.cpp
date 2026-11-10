// EncodeService Implementation
#include "gui/task_database/services/EncodeService.hpp"
#include "disk/Disk.hpp"
#include "misc/cross_platform.hpp"
#include "misc/logging.hpp"
#include "shared/SharedData.hpp"
#include "worker/encode/Pipeline.hpp"

#include <cassert>
#include <iostream>
#include <memory>

namespace GUI::Database {

EncodeService::EncodeService(SharedData &data)
    : data_(data) {
  Start();
}

// 异常不捕获 (fail fast). 取消也 post 交接: 日文件可能已被 rename 换掉, 须重扫
bool EncodeService::run(EncodeRequest &req) {
  const size_t target_dates = data_.asset.backtest.days.size();
  assert(target_dates > 0 && "编码前必须先完成回测区间覆盖扫描");
  // 大内存假设 (不为小机器妥协): 各 worker 并发解压+解码的工作集要装得下.
  assert(physical_ram_bytes() >= (size_t(16) << 30) &&
         "encoding 需要 ≥16GB 物理内存");

  // Enable High Performance Mode: GUI low-refresh, workers own cores
  data_.high_performance_mode = true;
  std::cout << "[High Performance Mode] Enabled - GUI low-refresh\n"
            << std::endl;

  // 编码会 rename 换掉日文件, 帧目录快照里的偏移随之作废: 先撤下, 收尾交接后的重扫描再发布新的.
  data_.asset.publish_catalog(std::make_shared<const OrdersCatalog>());

  begin(Phase::Running, target_dates);

  // Initialize logger for all encoding workers (shared log file)
  Logger::init(disk::ensure(disk::RunOrdersLog).string());
  Logger::reg("encoding");

  // 已完成天数镜像进进度面
  const EncodeArgs args{data_, req.workers, req.skip_existing, cancel_,
                        [this](size_t days_done) { publish(days_done - done.load(std::memory_order_relaxed)); }};
  const bool completed = encode_pipeline(args);

  // Disable High Performance Mode: GUI full-refresh
  data_.high_performance_mode = false;
  std::cout << "\n[High Performance Mode] Disabled - GUI full-refresh\n"
            << std::endl;

  // 交接 → StateManager 重扫 (扫描整体重建, 每天重读一遍日文件目录, 不用在这里告诉它动过哪些天)
  handoff.post(req.seq);
  return completed;
}

} // namespace GUI::Database
