// State Manager - Database 流水线编排 (GUI 线程) + 解锁进度
//
// 三个 worker 服务 (数据集 Sync / 扫描 Scan / 编码 Encode) 各一条通道: 发请求带递增请求代, 交接件原样带回.
//   在飞 = 发出代 != 收到代; 收到的不是最新代 = 陈旧件, 丢弃.
// 共享状态 (asset_info / asset.items / Asset 扫描产物) 只在 tick 里换进 (GUI 线程), 换进时机保证无 worker 正在读:
//   冻结:     特征计算在飞 (task_state.compute.computing) → 件留在邮箱, 不发编码; 反向: 编码在飞时特征计算不开跑
//   数据集件: 扫描 / 编码都不在飞才收 → rebuilt 为真时换 asset_info → Asset::materialize → 请求覆盖扫描
//   扫描件:   adopt 扫描产物 + 留存覆盖结论
//   编码件:   请求 RescanStorage (日文件被 rename 换掉)
#pragma once

#include "gui/task_database/services/ScanService.hpp"

#include <cstdint>
#include <string>

struct SharedData;

namespace GUI::Database {

class SyncService;
class EncodeService;

struct TabAccessState {
  bool can_access_sync = true;      // 数据源同步, 流水线第一步, 永远可进
  bool can_access_encode = false;   // 基本面 Ready 后解锁 (覆盖检查依赖日历)
  bool can_access_calendar = false; // 基本面 Ready 且已扫描过一遍
};

struct AggregatedState {
  TabAccessState tabs;
  bool sync_taken = false;       // 收到过同步构建交接件 (否 = 首轮在跑)
  bool asset_info_ready = false; // 最近一件构建成功 (AssetInfo 已换进)
  CoverageCheck coverage;        // 最近一件扫描交接的覆盖结论
};

class StateManager {
public:
  StateManager(SharedData &data, SyncService &sync, ScanService &scan, EncodeService &enc);

  void tick();

  // 唯一刷新链入口: 数据集同步 (online 时按 config.sync_fundamental / sync_bar1m) → AssetInfo 重建 → L2 覆盖扫描
  void refresh(bool online = true);
  void audit(std::string table = {}, bool incremental = false);
  void repair(std::string table);
  void encode(int workers, bool skip_existing);

  bool syncing() const { return sync_ch_.inflight(); }
  bool scanning() const { return scan_ch_.inflight(); }
  bool encoding() const { return enc_ch_.inflight(); }
  bool frozen() const;
  bool online() const;

  bool can_sync() const { return !syncing() && !scanning() && !encoding(); }
  bool can_audit() const { return can_sync() && online(); }
  bool can_encode() const { return can_sync() && !frozen(); }

  const AggregatedState &get_state() const { return state_; }
  const TabAccessState &get_tab_access() const { return state_.tabs; }

private:
  struct Channel {
    std::uint64_t sent = 0, taken = 0;
    bool inflight() const { return sent != taken; }
    std::uint64_t next() { return ++sent; }
    bool latest(std::uint64_t seq) const { return seq == sent; }
  };

  void scan(ScanMode mode);
  void publish_task_state();

  SharedData &data_;
  SyncService &sync_;
  ScanService &scan_;
  EncodeService &enc_;
  Channel sync_ch_, scan_ch_, enc_ch_;
  AggregatedState state_;
};

} // namespace GUI::Database
