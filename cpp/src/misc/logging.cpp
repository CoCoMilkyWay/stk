#include "misc/logging.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>

namespace Logger {

// Internal state
static std::ofstream decomp_log;
static std::ofstream parsing_log;
static std::ofstream analyze_log;
static std::mutex decomp_log_mutex;
static std::mutex parsing_log_mutex;
static std::mutex analyze_log_mutex;
static std::unordered_map<int, std::ofstream> worker_logs;
static std::unordered_map<int, std::mutex> worker_log_mutexes;
static std::shared_mutex worker_logs_map_mutex;
static bool initialized = false;
static std::string log_dir_path;

// Helper function to get current timestamp
static std::string get_timestamp() {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
            1000;

  std::stringstream ss;
  ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
  ss << '.' << std::setfill('0') << std::setw(3) << ms.count();
  return ss.str();
}

void init(const std::string &temp_base_path) {
  if (initialized) {
    return;
  }

  // Create log directory
  std::filesystem::path log_dir = std::filesystem::absolute(temp_base_path);
  std::filesystem::create_directories(log_dir);
  log_dir_path = log_dir.string();

  // Clear old logs
  for (const auto &entry : std::filesystem::directory_iterator(log_dir)) {
    if (entry.is_regular_file() && entry.path().extension() == ".log") {
      std::filesystem::remove(entry.path());
    }
  }

  // Initialize decompression log
  std::filesystem::path decomp_log_path = log_dir / "decompression.log";
  decomp_log.open(decomp_log_path);
  if (decomp_log.is_open()) {
    decomp_log << "[" << get_timestamp() << "] Decompression Log Started at: " << decomp_log_path << std::endl;
    std::cout << "Decompression log: \"" << decomp_log_path << "\"" << std::endl;
  } else {
    std::cerr << "Failed to create decompression log at: " << decomp_log_path << std::endl;
  }

  // Initialize parsing error log
  std::filesystem::path parsing_log_path = log_dir / "encoding.log";
  parsing_log.open(parsing_log_path);
  if (parsing_log.is_open()) {
    parsing_log << "[" << get_timestamp() << "] Parsing Error Log Started at: " << parsing_log_path << std::endl;
    std::cout << "Parsing error log: \"" << parsing_log_path << "\"" << std::endl;
  } else {
    std::cerr << "Failed to create parsing error log at: " << parsing_log_path << std::endl;
  }

  // Initialize analysis log
  std::filesystem::path analyze_log_path = log_dir / "analyzing.log";
  analyze_log.open(analyze_log_path);
  if (analyze_log.is_open()) {
    analyze_log << "[" << get_timestamp() << "] Analysis Log Started at: " << analyze_log_path << std::endl;
    std::cout << "Analysis log: \"" << analyze_log_path << "\"" << std::endl;
  } else {
    std::cerr << "Failed to create analysis log at: " << analyze_log_path << std::endl;
  }

  initialized = true;
}

void close() {
  if (!initialized) {
    return;
  }

  // Close decompression log
  if (decomp_log.is_open()) {
    decomp_log << "[" << get_timestamp() << "] Decompression Log Ended" << std::endl;
    decomp_log.close();
  }

  // Close parsing error log
  if (parsing_log.is_open()) {
    parsing_log << "[" << get_timestamp() << "] Parsing Error Log Ended" << std::endl;
    parsing_log.close();
  }

  // Close analysis log
  if (analyze_log.is_open()) {
    analyze_log << "[" << get_timestamp() << "] Analysis Log Ended" << std::endl;
    analyze_log.close();
  }

  // Close all worker logs
  std::unique_lock<std::shared_mutex> lock(worker_logs_map_mutex);
  for (auto &[worker_id, log_stream] : worker_logs) {
    if (log_stream.is_open()) {
      log_stream << "[" << get_timestamp() << "] Worker " << worker_id << " Log Ended" << std::endl;
      log_stream.close();
    }
  }
  worker_logs.clear();

  initialized = false;
}

void log_decomp(const std::string &message) {
  if (!initialized)
    return;

  std::lock_guard<std::mutex> lock(decomp_log_mutex);
  if (decomp_log.is_open()) {
    decomp_log << "[" << get_timestamp() << "] " << message << std::endl;
    decomp_log.flush();
  }
}

void log_encode(const std::string &message) {
  if (!initialized)
    return;

  std::lock_guard<std::mutex> lock(parsing_log_mutex);
  if (parsing_log.is_open()) {
    parsing_log << "[" << get_timestamp() << "] " << message << std::endl;
    parsing_log.flush();
  }
}

void log_analyze(const std::string &message) {
  if (!initialized)
    return;

  std::lock_guard<std::mutex> lock(analyze_log_mutex);
  if (analyze_log.is_open()) {
    analyze_log << "[" << get_timestamp() << "] " << message << std::endl;
    analyze_log.flush();
  }
}

void log_worker(int worker_id, const std::string &message) {
  if (!initialized)
    return;

  // Double-checked locking: fast path with shared lock
  {
    std::shared_lock<std::shared_mutex> shared_lock(worker_logs_map_mutex);
    if (worker_logs.find(worker_id) != worker_logs.end()) {
      // Worker log already exists, proceed to write
      shared_lock.unlock();
      std::lock_guard<std::mutex> lock(worker_log_mutexes[worker_id]);
      worker_logs[worker_id] << "[" << get_timestamp() << "] " << message << std::endl;
      worker_logs[worker_id].flush();
      return;
    }
  }

  // Slow path: worker log doesn't exist, create it with unique lock
  {
    std::unique_lock<std::shared_mutex> unique_lock(worker_logs_map_mutex);
    // Double-check after acquiring unique lock
    if (worker_logs.find(worker_id) == worker_logs.end()) {
      std::filesystem::path log_path = std::filesystem::path(log_dir_path) / ("worker_" + std::to_string(worker_id) + ".log");
      worker_logs[worker_id].open(log_path);
      worker_log_mutexes[worker_id]; // Create mutex entry
      if (worker_logs[worker_id].is_open()) {
        worker_logs[worker_id] << "[" << get_timestamp() << "] Worker " << worker_id << " Log Started at: " << log_path << std::endl;
      }
    }
  }

  // Write the log message
  std::lock_guard<std::mutex> lock(worker_log_mutexes[worker_id]);
  worker_logs[worker_id] << "[" << get_timestamp() << "] " << message << std::endl;
  worker_logs[worker_id].flush();
}

bool is_initialized() {
  return initialized;
}

} // namespace Logger
