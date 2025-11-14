/*
 * UNIFIED LOGGING SYSTEM FOR L2 DATABASE
 *
 * Provides centralized logging functionality with minimal visibility in functional code.
 * All logs are placed under the temp directory for easy cleanup.
 *
 * Features:
 * - Decompression logging: Track worker progress and archive processing
 * - Parsing error logging: Capture CSV parsing errors and file issues
 * - Thread-safe logging with proper mutex protection
 * - Automatic log file placement in temp directory
 * - Minimal impact on functional code performance
 *
 * Usage:
 *   Logger::init(temp_base_path);
 *   Logger::log_decomp("Worker started");
 *   Logger::log_encode("Failed to parse CSV: " + filepath);
 *   Logger::close();
 */

#pragma once

#include <string>

namespace Logger {

// Initialize logging system with temp directory base path
void init(const std::string &temp_base_path);

// Close all log files
void close();

// Decompression logging functions
void log_decomp(const std::string &message);

// Parsing error logging functions
void log_encode(const std::string &message);

// Analysis logging functions
void log_analyze(const std::string &message);

// Worker-specific logging functions (each worker has its own log file)
void log_worker(int worker_id, const std::string &message);

// Check if logging is initialized
bool is_initialized();

} // namespace Logger
