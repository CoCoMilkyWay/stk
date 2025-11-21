#pragma once
#include <string>
#include <vector>
#include <cstdio>
#include <chrono>
#include <filesystem>
#include <functional>

struct Config {
  // Integer types
  int port = 8080;
  int buffer_size = 1024;
  
  // Floating point types
  float sample_rate = 1000.0f;
  double threshold = 0.85;
  
  // Boolean types
  bool enable_logging = true;
  bool auto_save = false;
  
  // String types
  std::string data_path = "./data";
  std::string output_file = "output.txt";
  
  // Array types
  std::vector<int> window_sizes = {128, 256, 512};
  std::vector<float> coefficients = {0.1f, 0.5f, 0.9f};
  
  // Config file path (relative to build directory: ../../../config.json)
  std::string filepath = "../../../config.json";
  
  // String buffers for GUI
  char data_path_buf[256] = "";
  char output_file_buf[256] = "";
  
  // Auto-sync state
  bool dirty = false;
  std::chrono::steady_clock::time_point last_modified;
  std::filesystem::file_time_type last_file_time;
  
  // Log callback
  std::function<void(const std::string&)> log_callback;
  
  // Initialize config (load or create default)
  void Initialize();
  
  // Mark config as modified (will auto-save after debounce)
  void MarkDirty();
  
  // Sync string buffers from config
  void SyncStringBuffers();
  
  // Auto-sync: check file changes and debounced save
  void AutoSync();
  
private:
  // Load config from JSON file
  bool LoadFromFile();
  
  // Save config to JSON file
  bool SaveToFile();
};