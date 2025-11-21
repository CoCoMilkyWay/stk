#include "shared/Config.hpp"
#include "package/nlohmann/json.hpp"
#include <fstream>

using json = nlohmann::json;
namespace fs = std::filesystem;

void Config::Initialize() {
  // Try to load existing file
  if (fs::exists(filepath)) {
    if (LoadFromFile()) {
      SyncStringBuffers();
      // Track file modification time
      last_file_time = fs::last_write_time(filepath);
      if (log_callback) {
        log_callback("Config loaded from: " + filepath);
      }
      return;
    }
  }
  
  // File doesn't exist, create with default values
  if (log_callback) {
    log_callback("Config file not found, creating with default values: " + filepath);
  }
  SaveToFile();
  SyncStringBuffers();
  if (fs::exists(filepath)) {
    last_file_time = fs::last_write_time(filepath);
  }
}

void Config::MarkDirty() {
  dirty = true;
  last_modified = std::chrono::steady_clock::now();
}

void Config::SyncStringBuffers() {
  snprintf(data_path_buf, sizeof(data_path_buf), "%s", data_path.c_str());
  snprintf(output_file_buf, sizeof(output_file_buf), "%s", output_file.c_str());
}

void Config::AutoSync() {
  // Check if file was modified externally
  if (fs::exists(filepath)) {
    auto current_file_time = fs::last_write_time(filepath);
    if (current_file_time != last_file_time) {
      last_file_time = current_file_time;
      if (log_callback) {
        log_callback("Config file changed externally, reloading...");
      }
      LoadFromFile();
      SyncStringBuffers();
      dirty = false; // Reset dirty flag since we just loaded
      return;
    }
  }
  
  // Debounced auto-save (200ms after last modification)
  if (dirty) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_modified);
    if (elapsed.count() >= 200) {
      SaveToFile();
      dirty = false;
      if (fs::exists(filepath)) {
        last_file_time = fs::last_write_time(filepath);
      }
    }
  }
}

bool Config::LoadFromFile() {
  std::ifstream file(filepath);
  if (!file.is_open()) {
    return false;
  }

  json j;
  file >> j;

  // Parse JSON to Config with default values as fallback
  port = j.value("port", port);
  buffer_size = j.value("buffer_size", buffer_size);
  sample_rate = j.value("sample_rate", sample_rate);
  threshold = j.value("threshold", threshold);
  enable_logging = j.value("enable_logging", enable_logging);
  auto_save = j.value("auto_save", auto_save);
  data_path = j.value("data_path", data_path);
  output_file = j.value("output_file", output_file);
  window_sizes = j.value("window_sizes", window_sizes);
  coefficients = j.value("coefficients", coefficients);

  return true;
}

bool Config::SaveToFile() {
  json j;
  
  // Convert Config to JSON
  j["port"] = port;
  j["buffer_size"] = buffer_size;
  j["sample_rate"] = sample_rate;
  j["threshold"] = threshold;
  j["enable_logging"] = enable_logging;
  j["auto_save"] = auto_save;
  j["data_path"] = data_path;
  j["output_file"] = output_file;
  j["window_sizes"] = window_sizes;
  j["coefficients"] = coefficients;

  std::ofstream file(filepath);
  if (!file.is_open()) {
    if (log_callback) {
      log_callback("Failed to save config file: " + filepath);
    }
    return false;
  }

  file << j.dump(2); // Pretty print with 2 spaces indent
  if (log_callback) {
    log_callback("Config auto-saved to: " + filepath);
  }
  return true;
}

