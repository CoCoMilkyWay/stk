// Encoding Service - CSV 归档 → L2 逐笔二进制 (按天流水: producer 列举 → worker 并行解码)
//   + FileCheckService - 归档格式 / 结构 / 完整性核查 (顺序探测, 单盘)
// 线程模型 (gui/util/Worker.hpp, 随对象): 进度面 = 自身 (done/total = 回测交易日 / 归档数);
//   收尾 handoff.post 一件 (编码: 请求代, StateManager 收到即重扫; 核查: 结论, Encode 页帧首收)
// 编码期间读 data_.asset (回测日历 / 资产表): StateManager 只在无扫描 / 同步在飞时发请求, 编码在飞时不发扫描
#pragma once

#include "gui/util/Worker.hpp"
#include "misc/file_check.hpp"

#include <cstdint>
#include <string>

struct SharedData;

namespace GUI::Database {

struct EncodeRequest {
  uint64_t seq = 0; // 请求代 (StateManager 发), 交接原样带回
  int workers = 0;
  bool skip_existing = true;
};

class EncodingService : public Worker<EncodingService, EncodeRequest>, public Progress {
  friend Worker<EncodingService, EncodeRequest>;

public:
  static constexpr const char *kName = "EncodeWorker";
  explicit EncodingService(SharedData &data); // 末尾 Start
  ~EncodingService() { Stop(); }

  bool cancelling() const { return busy() && cancel_.load(std::memory_order_relaxed); }

  Mailbox<uint64_t> handoff;

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return *this; }
  bool run(EncodeRequest &req);

  SharedData &data_;
};

class FileCheckService : public Worker<FileCheckService, std::string>, public Progress {
  friend Worker<FileCheckService, std::string>;

public:
  static constexpr const char *kName = "FileCheckWorker";
  FileCheckService() { Start(); }
  ~FileCheckService() { Stop(); }

  Mailbox<FileCheck::FileCheckResult> handoff;

private:
  static constexpr Phase kStart = Phase::Scanning;
  Progress &board() { return *this; }
  bool run(std::string &archive_base_dir); // 不可取消 (unrar 探测逐个跑完)
};

} // namespace GUI::Database
