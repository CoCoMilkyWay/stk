// EncodeService - CSV 归档 → orders 日文件 (按天流水: producer 列举 → worker 并行解码)
// 线程模型 (gui/util/Service.hpp, 随对象): 进度面 = 自身 (done/total = 回测交易日);
//   收尾 handoff.post 请求代, StateManager 收到即重扫
// 编码期间读 data_.asset (回测日历 / 资产表): StateManager 只在无扫描 / 同步在飞时发请求, 编码在飞时不发扫描
#pragma once

#include "gui/util/Service.hpp"

#include <cstdint>

struct SharedData;

namespace GUI::Database {

struct EncodeRequest {
  uint64_t seq = 0; // 请求代 (StateManager 发), 交接原样带回
  int workers = 0;
  bool skip_existing = true;
};

class EncodeService : public Service<EncodeService, EncodeRequest>, public Progress {
  friend Service<EncodeService, EncodeRequest>;

public:
  static constexpr const char *kName = "EncodeWorker";
  explicit EncodeService(SharedData &data); // 末尾 Start
  ~EncodeService() { Stop(); }

  bool cancelling() const { return busy() && cancel_.load(std::memory_order_relaxed); }

  Mailbox<uint64_t> handoff;

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return *this; }
  bool run(EncodeRequest &req);

  SharedData &data_;
};

} // namespace GUI::Database
