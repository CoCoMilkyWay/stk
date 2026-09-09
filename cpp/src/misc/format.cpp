#include "misc/format.hpp"

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstring>

// 本 TU 依赖 NaN / Inf 判定, 必须 precise-math (CMakeLists PRECISE_MATH_FLAG 列表)
#ifdef __FAST_MATH__
#error "misc/format.cpp 依赖 NaN/Inf 判定, 需加入 CMakeLists 的 PRECISE_MATH_FLAG 列表"
#endif

namespace misc {

void fmt_compress_exp(char *buf) {
  char *e = std::strchr(buf, 'e');
  if (!e)
    return;
  char *w = e + 1;
  const char *r = e + 1;
  if (*r == '+' || *r == '-') {
    if (*r == '-')
      *w++ = '-';
    ++r;
  }
  while (*r == '0' && r[1] != '\0')
    ++r;
  while (*r)
    *w++ = *r++;
  *w = '\0';
}

void fmt_width(char *out, size_t cap, float v, int n, bool fixed_zero_ok) {
  assert(n >= 3 && cap > static_cast<size_t>(n));
  char buf[48];
  auto fits = [&] { return std::strlen(buf) <= static_cast<size_t>(n); };
  auto emit = [&](const char *s) { snprintf(out, cap, "%*s", n, s); };

  if (v != v)
    return emit("nan");
  if (std::isinf(v))
    return emit(v > 0.0f ? "inf" : (n >= 4 ? "-inf" : "-in"));
  if (v == 0.0f)
    return emit("0");

  // 1. 定点
  for (int d = n - 2; d >= 0; --d) {
    snprintf(buf, sizeof(buf), "%.*f", d, v);
    if (!fits())
      continue;
    if (fixed_zero_ok)
      return emit(buf);
    bool nonzero = false;
    for (const char *p = buf; *p; ++p)
      nonzero |= (*p >= '1' && *p <= '9');
    if (nonzero)
      return emit(buf);
    break; // 已全零, 更少小数位只会更零 → 换记法
  }

  // 2. SI 后缀
  const float av = std::fabs(v);
  int e3 = static_cast<int>(std::floor(std::log10(av) / 3.0f));
  float m = v / std::pow(10.0f, static_cast<float>(3 * e3));
  if (std::fabs(m) >= 999.5f) { // 舍入会进位成 "1000x" → 直接升一级后缀
    ++e3;
    m /= 1000.0f;
  }
  if (e3 != 0 && e3 >= -8 && e3 <= 8) {
    static constexpr char kSuffix[] = "yzafpnum kMGTPEZY"; // 下标 e3 + 8
    for (int d = n - 3; d >= 0; --d) {
      snprintf(buf, sizeof(buf), "%.*f%c", d, m, kSuffix[e3 + 8]);
      if (fits())
        return emit(buf);
    }
  }

  // 3. e 记法
  for (int d = n - 3; d >= 0; --d) {
    snprintf(buf, sizeof(buf), "%.*e", d, v);
    fmt_compress_exp(buf);
    if (fits())
      return emit(buf);
  }

  // 4. 仅指数
  snprintf(buf, sizeof(buf), "%se%d", v < 0.0f ? "-" : "", static_cast<int>(std::floor(std::log10(av))));
  if (fits())
    return emit(buf);

  // 5. 放不下
  std::memset(out, '#', static_cast<size_t>(n));
  out[n] = '\0';
}

} // namespace misc
