#include "gui/task_database/services/FileCheckService.hpp"

#include <iostream>
#include <string>
#include <vector>

namespace GUI::Database {

bool FileCheckService::run(std::string &archive_base_dir) {
  std::cout << "========================================\n"
            << "[File Check] Starting Archive Validation\n"
            << "========================================\n"
            << "[File Check] Archive path: " << archive_base_dir << "\n"
            << std::endl;

  // Step 1: Check directory exists
  std::cout << "[File Check] Step 1: Checking archive directory..." << std::endl;

  // Each probe (unrar lb) does O(entries) scattered read+lseek pairs across
  // the whole archive to walk its header chain (measured via strace: ~6500
  // read+lseek pairs for a 3270-entry / 3.9GB archive). The archive store is
  // a single-actuator spinning disk, so probes run sequentially -- running
  // several concurrently thrashes the disk head (measured 359x slowdown).
  auto progress = [this](size_t n_done, size_t n_total, const std::string &path) {
    if (n_done == 1)
      begin(Phase::Scanning, n_total);
    publish(1);
    std::cout << "[File Check]   (" << n_done << "/" << n_total << ") " << path << std::endl;
  };

  FileCheck::FileCheckResult local_result =
      FileCheck::check_src_archives(archive_base_dir, progress);

  if (!local_result.archive_dir_exists) {
    std::cout << "[File Check] ✗ Archive directory does not exist\n"
              << "[File Check] Will use existing orders instead\n"
              << "========================================" << std::endl;
    handoff.post(std::move(local_result));
    return true;
  }

  std::cout << "[File Check] ✓ Archive directory exists\n"
            << std::endl;

  // Step 2: Check required commands
  std::cout << "[File Check] Step 2: Checking required commands (unrar, 7z, rar, gdb)..." << std::endl;
  if (!local_result.commands_available) {
    std::cout << "[File Check] ✗ Some required commands are missing\n"
              << "[File Check] Please install: unrar, 7z, rar, gdb\n"
              << "========================================" << std::endl;
    handoff.post(std::move(local_result));
    return true;
  }
  std::cout << "[File Check] ✓ All required commands available\n"
            << std::endl;

  // Step 3: Scan archives
  std::cout << "[File Check] Step 3: Scanning archive files...\n"
            << "[File Check] Total archives found: " << local_result.total_archives << "\n"
            << std::endl;

  // Step 4-7: Validate naming, format, structure, ZIP files
  auto print_errors = [](const std::string &step, const std::string &desc, size_t count,
                         const std::vector<std::string> &files, const std::string &fix = "") {
    std::cout << "[File Check] " << step << ": " << desc << "..." << std::endl;
    if (count > 0) {
      std::cout << "[File Check] ✗ Found " << count << " error(s)" << std::endl;
      if (!fix.empty())
        std::cout << "[File Check]   Fix: " << fix << std::endl;
      for (const auto &file : files) {
        std::cout << "[File Check]   - " << file << std::endl;
      }
    } else {
      std::cout << "[File Check] ✓ All correct" << std::endl;
    }
    std::cout << std::endl;
  };

  print_errors("Step 4", "Checking archive naming (YYYY/YYYYMM/YYYYMMDD.rar)",
               local_result.naming_errors, local_result.naming_error_files);

  print_errors("Step 5", "Checking archive format (RAR non-solid)",
               local_result.format_errors, local_result.format_error_files,
               "Run py/app/FileRepair/fix_to_rar.py or fix_solid_to_nonsolid.py");

  print_errors("Step 6", "Checking internal structure (YYYYMMDD/asset_code/*.csv)",
               local_result.structure_errors, local_result.structure_error_files,
               "Run py/app/FileRepair/fix_archive_structure.py");

  print_errors("Step 6b", "Checking archive integrity (truncated / corrupt headers)",
               local_result.integrity_errors, local_result.integrity_error_files,
               "Re-download or re-create the archive");

  print_errors("Step 7", "Checking for ZIP files (should be RAR)",
               local_result.zip_files, local_result.zip_error_files,
               "Run py/app/FileRepair/fix_to_rar.py");

  // Summary
  std::cout << "========================================" << std::endl;
  if (local_result.passed) {
    std::cout << "[File Check] ✓ ALL CHECKS PASSED\n"
              << "[File Check] Valid archives: " << local_result.valid_archives << std::endl;
  } else {
    std::cout << "[File Check] ✗ SOME CHECKS FAILED\n"
              << "[File Check] Valid: " << local_result.valid_archives
              << " / Total: " << local_result.total_archives << "\n"
              << "[File Check] Total errors: " << (local_result.naming_errors + local_result.format_errors + local_result.structure_errors + local_result.integrity_errors + local_result.zip_files)
              << std::endl;
  }
  std::cout << "========================================" << std::endl;

  // Publish result once, atomically, at the end.
  handoff.post(std::move(local_result));
  return true;
}

} // namespace GUI::Database
