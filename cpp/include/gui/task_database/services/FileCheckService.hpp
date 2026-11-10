// FileCheckService - 归档核查 / 修复 (顺序逐个, 单盘). 请求两种 (FileCheckOp), 同一 worker 串行:
//   Check  : 核查 (misc/file_check.hpp), 进度阶段 Scanning
//   Repair : 修复请求里那份核查的可修项 (misc/file_repair.hpp, 暂存 output/run/data/repair/), 进度阶段 Running; 修完即 Check
// 不可取消 (unrar / rar 逐个跑完). 每轮收尾 handoff.post 核查结论, Encode 页帧首收
// 线程模型 (gui/util/Service.hpp, 随对象): 进度面 = 自身 (done/total = 归档数)
#pragma once

#include "gui/util/Service.hpp"
#include "misc/file_check.hpp"

#include <string>
#include <vector>

namespace GUI::Database {

enum class FileCheckOp { Check,
                         Repair };

struct FileCheckRequest {
  FileCheckOp op = FileCheckOp::Check;
  std::string archive_dir;
  std::vector<FileCheck::Finding> findings; // Repair: 要修的核查结论
};

class FileCheckService : public Service<FileCheckService, FileCheckRequest>, public Progress {
  friend Service<FileCheckService, FileCheckRequest>;

public:
  static constexpr const char *kName = "FileCheckWorker";
  FileCheckService() { Start(); }
  ~FileCheckService() { Stop(); }

  bool repairing() const { return phase.load(std::memory_order_acquire) == Phase::Running; }

  Mailbox<FileCheck::FileCheckResult> handoff;

private:
  static constexpr Phase kStart = Phase::Scanning;
  Progress &board() { return *this; }
  bool run(FileCheckRequest &req);
  void check(const std::string &archive_dir);
  void repair(const std::vector<FileCheck::Finding> &findings);
};

} // namespace GUI::Database
