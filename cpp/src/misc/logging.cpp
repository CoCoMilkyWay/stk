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
struct LogEntry {
  std::ofstream stream;
  std::mutex mutex;
};

static std::unordered_map<std::string, LogEntry> logs;
static std::shared_mutex logs_map_mutex;
static bool initialized = false;
static std::string log_dir_path;

// Helper function to get current timestamp
static std::string get_timestamp() {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
            1000;

  std::tm tm_buf;
  localtime_s(&tm_buf, &time_t);
  
  std::stringstream ss;
  ss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
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

  initialized = true;
}

void close() {
  if (!initialized) {
    return;
  }

  std::unique_lock<std::shared_mutex> lock(logs_map_mutex);
  for (auto &[log_name, entry] : logs) {
    std::lock_guard<std::mutex> stream_lock(entry.mutex);
    if (entry.stream.is_open()) {
      entry.stream << "[" << get_timestamp() << "] Log Ended" << std::endl;
      entry.stream.close();
    }
  }
  logs.clear();

  initialized = false;
}

void reg(const std::string &log_name) {
  if (!initialized)
    return;

  std::unique_lock<std::shared_mutex> lock(logs_map_mutex);
  if (logs.find(log_name) == logs.end()) {
    std::filesystem::path log_path = std::filesystem::path(log_dir_path) / (log_name + ".log");
    logs[log_name].stream.open(log_path);
    if (logs[log_name].stream.is_open()) {
      logs[log_name].stream << "[" << get_timestamp() << "] Log Started: " << log_name << " at " << log_path << std::endl;
      std::cout << "Log: \"" << log_name << "\" -> \"" << log_path << "\"" << std::endl;
    }
  }
}

void log(const std::string &log_name, const std::string &message) {
  if (!initialized) [[unlikely]] {
    std::cout << "Logging system " << log_name << " not initialized" << std::endl;
    std::exit(1);
  }

  // Fast path with shared lock
  {
    std::shared_lock<std::shared_mutex> shared_lock(logs_map_mutex);
    if (logs.find(log_name) != logs.end()) {
      shared_lock.unlock();
      std::lock_guard<std::mutex> stream_lock(logs[log_name].mutex);
      logs[log_name].stream << "[" << get_timestamp() << "] " << message << std::endl;
      logs[log_name].stream.flush();
      return;
    }
  }

  // Slow path: log doesn't exist, create it
  {
    std::unique_lock<std::shared_mutex> unique_lock(logs_map_mutex);
    if (logs.find(log_name) == logs.end()) {
      std::filesystem::path log_path = std::filesystem::path(log_dir_path) / (log_name + ".log");
      logs[log_name].stream.open(log_path);
      if (logs[log_name].stream.is_open()) {
        logs[log_name].stream << "[" << get_timestamp() << "] Log Started: " << log_name << " at " << log_path << std::endl;
      }
    }
  }

  // Write the log message
  std::lock_guard<std::mutex> stream_lock(logs[log_name].mutex);
  logs[log_name].stream << "[" << get_timestamp() << "] " << message << std::endl;
  logs[log_name].stream.flush();
}

bool is_initialized() {
  return initialized;
}

} // namespace Logger
