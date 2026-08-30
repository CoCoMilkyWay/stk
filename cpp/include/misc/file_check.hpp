#pragma once

#include <functional>
#include <string>
#include <vector>

namespace FileCheck {

// Progress callback: (completed, total, current_archive_path)
using ProgressCallback = std::function<void(size_t, size_t, const std::string &)>;

// File check result structure
struct FileCheckResult {
  bool passed = false;
  bool archive_dir_exists = false;
  bool commands_available = false;

  size_t total_archives = 0;
  size_t valid_archives = 0;
  size_t naming_errors = 0;
  size_t format_errors = 0;
  size_t structure_errors = 0;
  size_t integrity_errors = 0;
  size_t zip_files = 0;

  std::vector<std::string> error_messages;

  // Detailed error lists with file paths
  std::vector<std::string> naming_error_files;
  std::vector<std::string> format_error_files;
  std::vector<std::string> structure_error_files;
  std::vector<std::string> integrity_error_files;
  std::vector<std::string> zip_error_files;

  // Check if file check was actually run (vs skipped due to missing dir)
  bool was_run() const { return archive_dir_exists; }

  // Check if there are any errors
  bool has_errors() const {
    return naming_errors > 0 || format_errors > 0 ||
           structure_errors > 0 || integrity_errors > 0 || zip_files > 0;
  }
};

// Run archive validation and return detailed results.
// progress is invoked per archive, sequentially (single-threaded probing --
// the archive store is a single-actuator spinning disk, where concurrent
// unrar lb calls thrash on seeks and can be 100s of times slower than
// sequential; measured 359x slowdown for 8 concurrent probes on this disk).
FileCheckResult check_src_archives(const std::string &archive_base_dir,
                                   ProgressCallback progress = nullptr);

// Legacy function for backward compatibility (prints to stdout)
bool check_src_archives_print(const std::string &archive_base_dir);

} // namespace FileCheck
