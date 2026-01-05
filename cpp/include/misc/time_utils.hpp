#pragma once

#include <ctime>
#include <chrono>

/*
 * Cross-platform time utilities
 * Provides consistent time functionality across Windows, macOS, and Linux
 */

namespace TimeUtils {

/**
 * @brief Get current time as time_t
 * @return Current time in seconds since epoch
 */
inline std::time_t now() {
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

/**
 * @brief Convert time_t to struct tm (thread-safe, cross-platform)
 * @param time_value The time_t value to convert
 * @param result Output struct tm (must be pre-allocated)
 * @return Pointer to result on success, nullptr on failure
 */
inline std::tm* localtime_safe(const std::time_t* time_value, std::tm* result) {
#ifdef _WIN32
    if (localtime_s(result, time_value) == 0) {
        return result;
    }
    return nullptr;
#else
    return localtime_r(time_value, result);
#endif
}

/**
 * @brief Get local time as struct tm
 * @param time_value The time_t value to convert
 * @return struct tm with local time
 */
inline std::tm get_localtime(const std::time_t& time_value) {
    std::tm result{};
    localtime_safe(&time_value, &result);
    return result;
}

}  // namespace TimeUtils
