// FileCheckService - 归档格式 / 结构 / 完整性核查 (顺序探测, 单盘)
// 线程模型 (gui/util/Service.hpp, 随对象): 进度面 = 自身 (done/total = 归档数); 收尾 handoff.post 结论, Encode 页帧首收
#pragma once

#include "gui/util/Service.hpp"
#include "misc/file_check.hpp"

#include <string>

namespace GUI::Database {

class FileCheckService : public Service<FileCheckService, std::string>, public Progress {
  friend Service<FileCheckService, std::string>;

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
