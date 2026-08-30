#include "misc/file_check.hpp"
#include "misc/cross_platform.hpp"

#include <cassert>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace FileCheck {

namespace {

// ============================================================================
// ARCHIVE VALIDATION DATA STRUCTURES
// ============================================================================

struct ArchiveErrors {
  std::vector<std::string> naming_errors;
  std::vector<std::string> format_errors;
  std::vector<std::string> structure_errors;
  std::vector<std::string> integrity_errors;
  std::vector<std::string> zip_files;
};

struct ArchiveCheckResult {
  bool is_valid;
  ArchiveErrors errors;
  std::vector<std::string> valid_archives;
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

bool is_valid_number_string(const std::string &str, size_t expected_len) {
  if (str.length() != expected_len)
    return false;
  for (char c : str) {
    if (!std::isdigit(c))
      return false;
  }
  return true;
}

struct ArchiveProbe {
  bool structure_ok = false;
  std::string structure_err;
  bool integrity_ok = false;
  std::string integrity_err;
};

ArchiveProbe probe_single_archive(const std::string &archive_path) {
  ArchiveProbe probe;

  // Run `unrar lb` capturing both stdout (file list) and stderr (diagnostics).
  // `unrar lb` reads only the archive index, not compressed data, so this is cheap.
  // Truncation / corrupt headers are reported on stderr with a non-zero exit code.
  std::string cmd = "unrar lb \"" + archive_path + "\" 2>&1";
  FILE *pipe = safe_popen(cmd.c_str(), "r");
  assert(pipe && "popen for unrar lb failed");

  std::string combined;
  char buffer[4096];
  while (fgets(buffer, sizeof(buffer), pipe)) {
    combined += buffer;
  }
  int exit_code = safe_pclose(pipe);

  // Integrity: non-zero exit or known corruption markers in the output.
  if (exit_code != 0 ||
      combined.find("Unexpected end of archive") != std::string::npos ||
      combined.find("Corrupt header") != std::string::npos) {
    probe.integrity_ok = false;
    probe.integrity_err = "corrupt or truncated (unrar exit " + std::to_string(exit_code) + ")";
  } else {
    probe.integrity_ok = true;
  }

  // Structure: first line matching YYYYMMDD/asset_code/*.csv.
  // Scan rather than take line 1, because stderr diagnostics may precede the
  // file list when stdout/stderr are merged.
  size_t pos = 0;
  while (pos < combined.size()) {
    size_t nl = combined.find('\n', pos);
    std::string line = (nl == std::string::npos)
                           ? combined.substr(pos)
                           : combined.substr(pos, nl - pos);
    if (!line.empty() && line.back() == '\r')
      line.pop_back();

    pos = (nl == std::string::npos) ? combined.size() : nl + 1;

    if (line.length() >= 9 && line[8] == '/' &&
        std::isdigit(line[0]) && std::isdigit(line[1]) &&
        std::isdigit(line[2]) && std::isdigit(line[3]) &&
        std::isdigit(line[4]) && std::isdigit(line[5]) &&
        std::isdigit(line[6]) && std::isdigit(line[7])) {
      probe.structure_ok = true;
      break;
    }
  }

  if (!probe.structure_ok) {
    probe.structure_err = "invalid structure (no YYYYMMDD/ entry)";
  }

  return probe;
}

// ============================================================================
// CHECK 1: REQUIRED COMMANDS
// ============================================================================

bool check_required_commands() {
  struct CommandInfo {
    std::string command;
    std::string install_hint;
  };

  std::vector<CommandInfo> required_commands = {
      {"unrar", "sudo apt install unrar"},
      {"7z", "sudo apt install p7zip-full"},
      {"rar", "sudo apt install rar"},
      {"gdb", "sudo apt install gdb"}};

  bool ok = true;
  for (const auto &cmd : required_commands) {
    int result = std::system(("which " + cmd.command + " > /dev/null 2>&1").c_str());
    if (result != 0) {
      // std::cout << "✗ Missing command: " << cmd.command << "\n";
      // std::cout << "  Install: " << cmd.install_hint << "\n";
      ok = false;
    }
  }

  // if (ok) {
  //   std::cout << "✓ Required commands      : all available (unrar, 7z, rar, gdb)\n";
  // }
  return ok;
}

// ============================================================================
// CHECK 2: ARCHIVE FORMAT (7Z AND SOLID RAR)
// ============================================================================

bool detect_7z_format(const std::string &archive_path) {
  std::ifstream file(archive_path, std::ios::binary);
  if (!file.is_open()) {
    return false;
  }

  char magic[6] = {0};
  file.read(magic, 6);

  return (magic[0] == '7' && magic[1] == 'z' &&
          magic[2] == static_cast<char>(0xbc) && magic[3] == static_cast<char>(0xaf) &&
          magic[4] == static_cast<char>(0x27) && magic[5] == static_cast<char>(0x1c));
}

bool detect_solid_rar(const std::string &archive_path) {
  std::ifstream file(archive_path, std::ios::binary);
  if (!file.is_open()) {
    return false;
  }

  // RAR 5.x signature: "Rar!\x1a\x07\x01\x00"
  // RAR 4.x signature: "Rar!\x1a\x07\x00"
  char header[8] = {0};
  file.read(header, 8);

  // Check RAR signature
  if (header[0] != 'R' || header[1] != 'a' || header[2] != 'r' || header[3] != '!') {
    return false;
  }

  // RAR 5.x format
  if (header[4] == 0x1a && header[5] == 0x07 && header[6] == 0x01 && header[7] == 0x00) {
    // Read archive header to check solid flag
    // In RAR 5.x, need to parse vint and flags
    // Skip for now - RAR 5.x is less common and checking is complex
    return false;
  }

  // RAR 4.x format
  if (header[4] == 0x1a && header[5] == 0x07 && header[6] == 0x00) {
    // Skip to main header block (after marker block)
    file.seekg(7, std::ios::beg);

    // Read block header: HEAD_CRC(2) + HEAD_TYPE(1) + HEAD_FLAGS(2) + HEAD_SIZE(2)
    char block[7];
    file.read(block, 7);

    unsigned char head_type = block[2];
    unsigned short head_flags = static_cast<unsigned char>(block[3]) | (static_cast<unsigned char>(block[4]) << 8);

    // Archive header has type 0x73
    if (head_type == 0x73) {
      // Solid flag is bit 3 (0x0008) in archive header flags
      return (head_flags & 0x0008) != 0;
    }
  }

  return false;
}

// ============================================================================
// CHECK 3: INCOMPATIBLE ARCHIVE STRUCTURE
// ============================================================================

// ============================================================================
// UNIFIED ARCHIVE VALIDATION
// ============================================================================

ArchiveCheckResult validate_archive_structure(const std::string &archive_base_dir,
                                              const ProgressCallback &progress) {
  ArchiveCheckResult result;
  result.is_valid = true;

  // Archives that pass naming + format checks and need an unrar lb probe.
  std::vector<std::string> to_probe;

  // Single pass through directory structure
  // Expected structure: archive_base/YYYY/YYYYMM/YYYYMMDD.rar
  for (const auto &year_entry : std::filesystem::directory_iterator(archive_base_dir)) {
    if (!year_entry.is_directory()) {
      result.errors.naming_errors.push_back("Non-directory in base: " + year_entry.path().filename().string());
      result.is_valid = false;
      continue;
    }

    std::string year_name = year_entry.path().filename().string();
    if (!is_valid_number_string(year_name, 4)) {
      result.errors.naming_errors.push_back("Invalid year directory: " + year_name + " (expected YYYY)");
      result.is_valid = false;
      continue;
    }

    for (const auto &month_entry : std::filesystem::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory()) {
        result.errors.naming_errors.push_back("Non-directory in " + year_name + ": " + month_entry.path().filename().string());
        result.is_valid = false;
        continue;
      }

      std::string month_name = month_entry.path().filename().string();
      if (!is_valid_number_string(month_name, 6)) {
        result.errors.naming_errors.push_back("Invalid month directory: " + year_name + "/" + month_name + " (expected YYYYMM)");
        result.is_valid = false;
        continue;
      }

      // Check month directory name matches year
      if (month_name.substr(0, 4) != year_name) {
        result.errors.naming_errors.push_back("Month directory mismatch: " + year_name + "/" + month_name);
        result.is_valid = false;
        continue;
      }

      for (const auto &file_entry : std::filesystem::directory_iterator(month_entry.path())) {
        if (!file_entry.is_regular_file()) {
          result.errors.naming_errors.push_back("Non-file in " + year_name + "/" + month_name + ": " + file_entry.path().filename().string());
          result.is_valid = false;
          continue;
        }

        std::string filename = file_entry.path().filename().string();
        std::string filepath = file_entry.path().string();

        // Check file extension
        if (filename.ends_with(".zip")) {
          result.errors.zip_files.push_back(year_name + "/" + month_name + "/" + filename);
          result.is_valid = false;
          continue;
        }

        if (!filename.ends_with(".rar")) {
          std::string ext = filename.substr(filename.find_last_of('.'));
          result.errors.naming_errors.push_back("Invalid file extension: " + year_name + "/" + month_name + "/" + filename + " (found " + ext + ", expected .rar)");
          result.is_valid = false;
          continue;
        }

        // Check filename format (YYYYMMDD.rar)
        std::string stem = filename.substr(0, filename.length() - 4);
        if (!is_valid_number_string(stem, 8)) {
          result.errors.naming_errors.push_back("Invalid filename: " + year_name + "/" + month_name + "/" + filename + " (expected YYYYMMDD.rar)");
          result.is_valid = false;
          continue;
        }

        // Check filename matches parent directories
        if (stem.substr(0, 4) != year_name || stem.substr(0, 6) != month_name) {
          result.errors.naming_errors.push_back("Filename path mismatch: " + year_name + "/" + month_name + "/" + filename);
          result.is_valid = false;
          continue;
        }

        // All naming checks passed, now check format. Probe (unrar lb) is
        // expensive, so only queue files that pass format checks for probing.
        bool format_ok = true;

        // Check format: 7z disguised as rar
        if (detect_7z_format(filepath)) {
          result.errors.format_errors.push_back(filepath + " (7z disguised as .rar)");
          format_ok = false;
        }

        // Check format: solid RAR
        if (detect_solid_rar(filepath)) {
          result.errors.format_errors.push_back(filepath + " (solid RAR)");
          format_ok = false;
        }

        if (!format_ok) {
          result.is_valid = false;
          continue; // skip unrar probe for non-RAR / solid archives
        }

        to_probe.push_back(filepath);
      }
    }
  }

  // Probe archives sequentially. The archive store is a single-actuator
  // spinning disk (measured: ST4000NM0053, 7200 RPM); concurrent unrar lb
  // calls each do O(entries) scattered seeks, and running several at once
  // thrashes the disk's single head -- measured 359x slowdown (303ms alone
  // vs 108s+ under 8-way concurrency) for the same archive. Sequential is
  // the only way to keep per-archive latency bounded on this storage.
  const size_t total = to_probe.size();
  for (size_t i = 0; i < total; ++i) {
    ArchiveProbe probe = probe_single_archive(to_probe[i]);
    if (!probe.integrity_ok) {
      result.errors.integrity_errors.push_back(to_probe[i] + " - " + probe.integrity_err);
      result.is_valid = false;
    }
    if (!probe.structure_ok) {
      result.errors.structure_errors.push_back(to_probe[i] + " - " + probe.structure_err);
      result.is_valid = false;
    }
    if (probe.integrity_ok && probe.structure_ok) {
      result.valid_archives.push_back(to_probe[i]);
    }

    if (progress)
      progress(i + 1, total, to_probe[i]);
  }

  return result;
}

} // anonymous namespace

// ============================================================================
// PUBLIC API
// ============================================================================

FileCheckResult check_src_archives(const std::string &archive_base_dir,
                                   ProgressCallback progress) {
  FileCheckResult result;

  // Check if path exists
  if (!std::filesystem::exists(archive_base_dir)) {
    result.archive_dir_exists = false;
    result.passed = true; // OK to proceed with built binaries
    return result;
  }

  result.archive_dir_exists = true;

  // Check 1: Required commands
  bool cmd_ok = check_required_commands();
  result.commands_available = cmd_ok;

  if (!cmd_ok) {
    result.passed = false;
    result.error_messages.push_back("Required commands not available");
    return result;
  }

  // Unified validation: naming, format, and structure checks in single pass
  ArchiveCheckResult arch_result = validate_archive_structure(archive_base_dir, progress);

  // Fill in statistics
  result.valid_archives = arch_result.valid_archives.size();
  result.naming_errors = arch_result.errors.naming_errors.size();
  result.format_errors = arch_result.errors.format_errors.size();
  result.structure_errors = arch_result.errors.structure_errors.size();
  result.integrity_errors = arch_result.errors.integrity_errors.size();
  result.zip_files = arch_result.errors.zip_files.size();
  result.total_archives = result.valid_archives + result.naming_errors +
                          result.format_errors + result.structure_errors +
                          result.integrity_errors + result.zip_files;

  // Copy all error file paths
  result.naming_error_files = arch_result.errors.naming_errors;
  result.format_error_files = arch_result.errors.format_errors;
  result.structure_error_files = arch_result.errors.structure_errors;
  result.integrity_error_files = arch_result.errors.integrity_errors;
  result.zip_error_files = arch_result.errors.zip_files;

  // Collect error messages (first few of each type)
  for (size_t i = 0; i < std::min(size_t(3), arch_result.errors.naming_errors.size()); ++i) {
    result.error_messages.push_back("Naming: " + arch_result.errors.naming_errors[i]);
  }
  for (size_t i = 0; i < std::min(size_t(3), arch_result.errors.format_errors.size()); ++i) {
    result.error_messages.push_back("Format: " + arch_result.errors.format_errors[i]);
  }
  for (size_t i = 0; i < std::min(size_t(3), arch_result.errors.structure_errors.size()); ++i) {
    result.error_messages.push_back("Structure: " + arch_result.errors.structure_errors[i]);
  }
  for (size_t i = 0; i < std::min(size_t(3), arch_result.errors.integrity_errors.size()); ++i) {
    result.error_messages.push_back("Integrity: " + arch_result.errors.integrity_errors[i]);
  }
  for (size_t i = 0; i < std::min(size_t(3), arch_result.errors.zip_files.size()); ++i) {
    result.error_messages.push_back("ZIP file: " + arch_result.errors.zip_files[i]);
  }

  result.passed = arch_result.is_valid;
  return result;
}

bool check_src_archives_print(const std::string &archive_base_dir) {

  // Check if path exists, return ok if not
  if (!std::filesystem::exists(archive_base_dir)) {
    // std::cout << "✗ Archive base directory does not exist, use built binaries ...: " << archive_base_dir << "\n\n";
    return true;
  }
  // std::cout << "✓ Archive base directory does exist, continue checking ...: " << archive_base_dir << "\n\n";

  // std::cout << "=== Archive Validation ===" << "\n\n";

  bool ok = true;

  // Check 1: Required commands
  bool cmd_ok = check_required_commands();
  ok = cmd_ok && ok;

  // Only continue if commands are available
  if (!cmd_ok) {
    return false;
  }

  // Unified validation: naming, format, and structure checks in single pass
  ArchiveCheckResult result = validate_archive_structure(archive_base_dir, nullptr);

  // Report naming errors
  if (!result.errors.naming_errors.empty()) {
    // std::cout << "✗ Archive naming         : " << result.errors.naming_errors.size() << " problem(s) found\n";
    // for (const auto &error : result.errors.naming_errors) {
    //   std::cout << "  " << error << "\n";
    // }
    // std::cout << "\n";
    // std::cout << "  Expected structure: YYYY/YYYYMM/YYYYMMDD.rar\n";
    // std::cout << "  Example: 2024/202411/20241119.rar\n";
    // std::cout << "\n";
    ok = false;
  } else {
    // std::cout << "✓ Archive naming         : all correct (YYYY/YYYYMM/YYYYMMDD.rar)\n";
  }

  // Report format errors
  if (!result.errors.format_errors.empty()) {
    // std::cout << "✗ Archive format         : " << result.errors.format_errors.size() << " problem(s) found\n";
    // for (const auto &error : result.errors.format_errors) {
    //   std::cout << "  " << error << "\n";
    // }
    // std::cout << "\n";
    // std::cout << "  Fix: Run py/app/FileRepair/fix_7z_to_rar.py or fix_solid_to_nonsolid.py\n";
    // std::cout << "\n";
    ok = false;
  } else {
    // std::cout << "✓ Archive format         : all correct (RAR non-solid)\n";
  }

  // Report structure errors
  if (!result.errors.structure_errors.empty()) {
    // std::cout << "✗ Internal hierarchy     : " << result.errors.structure_errors.size() << " problem(s) found\n";
    // for (const auto &error : result.errors.structure_errors) {
    //   std::cout << "  " << error << "\n";
    // }
    // std::cout << "\n";
    // std::cout << "  Expected: YYYYMMDD/asset_code/*.csv\n";
    // std::cout << "  Example: 20240925/000001.SZ/行情.csv\n";
    // std::cout << "\n";
    // std::cout << "  Fix: Run py/app/FileRepair/fix_archive_structure.py\n";
    // std::cout << "\n";
    ok = false;
  } else {
    // std::cout << "✓ Internal hierarchy     : all correct (YYYYMMDD/asset_code/*.csv)\n";
  }

  // Report integrity errors (truncated / corrupt headers)
  if (!result.errors.integrity_errors.empty()) {
    // std::cout << "✗ Archive integrity      : " << result.errors.integrity_errors.size() << " corrupt/truncated archive(s) found\n";
    // for (const auto &error : result.errors.integrity_errors) {
    //   std::cout << "  " << error << "\n";
    // }
    // std::cout << "\n";
    // std::cout << "  Fix: re-download or re-create the archive\n";
    // std::cout << "\n";
    ok = false;
  }

  // Report zip files separately
  if (!result.errors.zip_files.empty()) {
    // std::cout << "✗ Found .zip files       : " << result.errors.zip_files.size() << " file(s) need conversion\n";
    // for (const auto &zip_file : result.errors.zip_files) {
    //   std::cout << "  " << zip_file << "\n";
    // }
    // std::cout << "\n";
    // std::cout << "  Fix .zip files: Run py/app/FileRepair/fix_zip_to_rar.py\n";
    // std::cout << "\n";
    ok = false;
  }

  // Summary
  // std::cout << "\n";
  // if (ok) {
  //   std::cout << "========================================\n";
  //   std::cout << "✓ All checks passed (" << result.valid_archives.size() << " valid archives)\n";
  //   std::cout << "========================================\n";
  // } else {
  //   std::cout << "========================================\n";
  //   std::cout << "✗ Some checks failed. Please fix the issues above.\n";
  //   std::cout << "========================================\n";
  // }

  return ok;
}

} // namespace FileCheck
