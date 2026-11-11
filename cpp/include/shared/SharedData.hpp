#pragma once

#include "./Asset.hpp"
#include "./AssetInfo.hpp"
#include "./Config.hpp"
#include "./Feature.hpp"
#include "./TaskState.hpp"

#include <atomic>

// 进程内重启 (主循环帧首 GUI::ReinitAllTasks): 任务 / 服务 / SharedData / 进程级缓存 (misc::Epochal) 全部按盘重建.
//   起因 = 盘上状态变了: 配置保存 / 子池表落地 / 池表调序 / 一轮特征计算收尾 / 删库. online / append 随重建带过去, 新一代首次用掉即复位
struct Reinit {
  bool requested = false;
  bool on_leave = false; // 盘上已变, 左栏换页时才重启 (池表调序: 连拖不打断, 本代内各池列表按 GUI::pool_order 画)
  bool online = true;    // 重建后首轮基本面同步按配置联网 (false = 只本地构建)
  bool append = true;    // 重建后数据就绪即自动续算 (false = 上一轮被取消)
};

struct SharedData {
  Config config;
  TaskState task_state;
  Asset asset;
  AssetInfo asset_info;
  Feature feature;

  Reinit reinit;
  std::atomic<bool> high_performance_mode{false}; // 编码 / 特征计算在飞 (各服务 run 首尾写), 渲染循环读以降帧率
};
