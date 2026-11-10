#include "gui/task_database/services/FileCheckService.hpp"
#include "disk/Disk.hpp"
#include "misc/file_repair.hpp"

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

namespace GUI::Database {

bool FileCheckService::run(FileCheckRequest &req) {
  if (req.op == FileCheckOp::Repair)
    repair(req.findings);
  check(req.archive_dir);
  return true;
}

void FileCheckService::repair(const std::vector<FileCheck::Finding> &findings) {
  std::cout << "========================================\n"
            << "[File Repair] Repacking repairable archives\n"
            << "========================================" << std::endl;
  begin(Phase::Running, static_cast<size_t>(std::count_if(findings.begin(), findings.end(), [](const FileCheck::Finding &f) {
          return FileCheck::repairable(f.issue);
        })));
  auto progress = [this](size_t n_done, size_t n_total, const std::string &path) {
    publish(1);
    std::cout << "[File Repair]   (" << n_done << "/" << n_total << ") " << path << std::endl;
  };
  disk::remove(disk::RunRepair);
  FileRepair::repair(findings, disk::ensure(disk::RunRepair), progress);
  disk::remove(disk::RunRepair);
  std::cout << "[File Repair] ✓ Done\n"
            << std::endl;
}

void FileCheckService::check(const std::string &archive_dir) {
  std::cout << "========================================\n"
            << "[File Check] Archive path: " << archive_dir << "\n"
            << "========================================" << std::endl;
  begin(Phase::Scanning);
  auto progress = [this](size_t n_done, size_t n_total, const std::string &path) {
    if (n_done == 1)
      begin(Phase::Scanning, n_total);
    publish(1);
    std::cout << "[File Check]   (" << n_done << "/" << n_total << ") " << path << std::endl;
  };
  FileCheck::FileCheckResult r = FileCheck::check(archive_dir, progress);

  if (!r.dir_exists) {
    std::cout << "[File Check] ✗ Archive directory does not exist, using existing orders" << std::endl;
  } else if (!r.tools_ok) {
    std::cout << "[File Check] ✗ Missing commands, please install: unrar, 7z (p7zip-full), rar" << std::endl;
  } else {
    for (size_t i = 0; i < FileCheck::kIssueCount; ++i) {
      const auto issue = static_cast<FileCheck::Issue>(i);
      const FileCheck::IssueMeta &m = FileCheck::meta(issue);
      const size_t n = r.count(issue);
      if (n == 0)
        continue;
      std::cout << "[File Check] ✗ " << m.name << ": " << n << " (" << m.desc << ") — fix: " << m.fix << std::endl;
      for (const FileCheck::Finding &f : r.findings)
        if (f.issue == issue)
          std::cout << "[File Check]   - " << f.path << "  " << f.detail << std::endl;
    }
    std::cout << "[File Check] " << (r.passed() ? "✓ ALL CHECKS PASSED" : "✗ SOME CHECKS FAILED")
              << " — valid " << r.valid << ", findings " << r.findings.size()
              << " (" << r.repairable() << " repairable)" << std::endl;
  }
  std::cout << "========================================" << std::endl;

  handoff.post(std::move(r));
}

} // namespace GUI::Database
