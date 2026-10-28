// State Manager - Database 流水线编排 (GUI 线程) + 解锁进度
//
// 四个 worker 服务 (基本面 / 扫描 / 编码 / 核对) 各自一条通道: 发请求带递增请求代, 交接件原样带回.
//   在飞 = 发出代 != 收到代; 收到的不是最新代 = 陈旧件, 丢弃 (最新请求已在路上)
// 共享状态 (assetinfo / asset.items / Asset 扫描产物) 只在 tick 里换进 (GUI 线程), 换进时机保证无 worker 正在读:
//   冻结:     特征计算在飞 (taskstate.features.computing) → 件都留在邮箱里, 不发编码 (Lob 按帧目录快照读日文件, 编码 rename 换掉它们);
//             同步 / 扫描照跑, 收尾后再换进. 反向: 编码在飞时特征计算不开跑 (taskstate.database.encode_inflight)
//   基本面件: 扫描 / 编码都不在飞才收 (二者读 assetinfo / items) → 换 assetinfo → AssetLoader 重建 items → 请求覆盖扫描
//   扫描件:   adopt 扫描产物 + 留存覆盖结论 (扫描只在基本面件收下后 / 编码收尾后发, 编码只在扫描不在飞时发)
//   编码件:   请求 RescanStorage (日文件被 rename 换掉)
//   核对件:   Repair 收尾 → refresh (改了分片, 重建 AssetInfo)
#pragma once

#include "gui/task_database/services/AuditService.hpp"
#include "gui/task_database/services/ScanService.hpp"

#include <cstdint>
#include <string>

// Forward declaration
struct SharedData;

namespace GUI::Database {

class FundamentalService;
class EncodingService;

// ============================================================================
// Tab Access Control State
// ============================================================================

struct TabAccessState {
  // Tab unlock progression (基本面 → L2 → 消费端)
  bool can_access_overview = true; // 基本面面板, 流水线第一步, 永远可进
  bool can_access_encode = false;  // 基本面 Ready 后解锁 (覆盖检查依赖日历)

  // Table 主要展示 Overview 的日频基本面, Browser 本身就是在展示 L2 数据
  // 完整性 (缺口正是它要呈现的东西) —— 两者都不需要 L2 覆盖 100% Pass,
  // 但仍要等 Encode 页跑过一遍覆盖扫描 (数据才建好).
  bool can_access_table = false;   // 基本面 Ready 且已扫描过一遍
  bool can_access_browser = false; // 基本面 Ready 且已扫描过一遍
};

// ============================================================================
// Aggregated State (lightweight, view-only)
// ============================================================================

struct AggregatedState {
  // Tab access control
  TabAccessState tabs;

  bool fundamental_taken = false; // 收到过基本面交接件 (之前 = 首轮在跑)
  bool fundamental_ok = false;    // 最近一件 ok (AssetInfo 已从 parquet 构建并换进)
  DatabaseCheckResult check;      // 最近一件扫描交接的覆盖结论

  // Helper: fundamental data ready (AssetInfo built from parquet)
  bool all_json_ready() const { return fundamental_ok; }
};

// ============================================================================
// State Manager - Coordinates all services
// ============================================================================

class StateManager {
public:
  // 构造即发首批请求: 核对 Load + refresh
  StateManager(SharedData &data, FundamentalService &fund, ScanService &scan, EncodingService &enc,
               AuditService &audit);

  // 每帧帧首 (GUI 线程): 收交接 → 换进共享状态 → 推下一站 → 刷解锁 + taskstate.database
  void tick();

  // 唯一刷新链入口: 基本面 / 分钟线同步 (联网按 config.sync_fundamental / sync_bar1m) → AssetInfo/items 重建 → L2 覆盖扫描.
  // GUI 启动、Overview Update、修复收尾都走这里, 不各写一份流程.
  void refresh();
  // 编码 / 核对请求 (调用方按 can_encode / auditing 使能)
  void encode(int workers, bool skip_existing);
  void audit(AuditOp op, std::string table, bool incremental);

  bool syncing() const { return fund_ch_.inflight(); }
  bool scanning() const { return scan_ch_.inflight(); }
  bool encoding() const { return enc_ch_.inflight(); }
  bool auditing() const { return audit_ch_.inflight(); }
  bool frozen() const; // 共享数据冻结 (特征计算在飞)
  bool can_encode() const { return !syncing() && !scanning() && !encoding() && !frozen(); }

  const AggregatedState &get_state() const { return state_; }
  const TabAccessState &get_tab_access() const { return state_.tabs; }

private:
  struct Channel {
    uint64_t sent = 0, taken = 0;
    bool inflight() const { return sent != taken; }
    uint64_t next() { return ++sent; }
    bool latest(uint64_t seq) const { return seq == sent; } // 收件后: 是最新代
  };

  // 覆盖扫描请求; RecomputeCoverage 但扫描缓存与 config 路径不符 → 降级 RescanStorage
  void scan(ScanMode mode);
  void publish_task_state();

  SharedData &data_;
  FundamentalService &fund_;
  ScanService &scan_;
  EncodingService &enc_;
  AuditService &audit_;
  Channel fund_ch_, scan_ch_, enc_ch_, audit_ch_;
  AuditOp audit_op_ = AuditOp::Load; // 最近一次核对请求的 op (收尾分流)
  AggregatedState state_;
};

} // namespace GUI::Database
