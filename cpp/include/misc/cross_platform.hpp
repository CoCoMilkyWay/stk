#pragma once

#include <cstdio>
#include <ctime>

// Cross-platform localtime (thread-safe)
inline std::tm safe_localtime(const std::time_t *timer) {
  std::tm result{};
#ifdef _WIN32
  localtime_s(&result, timer);
#else
  localtime_r(timer, &result);
#endif
  return result;
}

// Cross-platform popen/pclose
inline FILE *safe_popen(const char *command, const char *mode) {
#ifdef _WIN32
  return _popen(command, mode);
#else
  return popen(command, mode);
#endif
}

inline int safe_pclose(FILE *stream) {
#ifdef _WIN32
  return _pclose(stream);
#else
  return pclose(stream);
#endif
}
