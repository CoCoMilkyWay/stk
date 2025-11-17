/*
 * UNIFIED LOGGING SYSTEM
 *
 * Generic logging system with automatic initialization and lazy file creation.
 *
 * Features:
 * - Auto-initialization on first use (no manual init required)
 * - Thread-safe logging with per-file mutex protection
 * - Lazy log file creation (only creates files that are actually used)
 * - Generic naming: any log name creates a corresponding .log file
 * - Minimal performance impact with [[unlikely]] annotations
 *
 * Usage:
 *   Logger::log("decompression", "Worker started");
 *   Logger::log("encoding", "Failed to parse CSV: " + filepath);
 *   Logger::log("worker_" + std::to_string(worker_id), "Processing...");
 *   Logger::close();  // Optional: called automatically on program exit
 */

#pragma once

#include <string>

namespace Logger {

// Initialize logging system with temp directory base path
void init(const std::string &temp_base_path);

// Close all log files
void close();

// Register a new log file by name (thread-safe, creates file lazily)
void reg(const std::string &log_name);

// Generic logging function (thread-safe)
void log(const std::string &log_name, const std::string &message);

// Check if logging is initialized
bool is_initialized();

} // namespace Logger
