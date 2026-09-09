#pragma once

#include <cstddef>

// 定宽数值格式化: 任意 float 塞进恰好 n 个字符 (UI 表格 / 日志对齐), 精度换宽度.
// 业务无关, 可在任意子系统复用. 实现在 src/misc/format.cpp (依赖 NaN/Inf 判定 → precise-math TU).
namespace misc {

// 指数段就地压缩: "e+10" → "e10", "e-05" → "e-5"
void fmt_compress_exp(char *buf);

// 任意 float → 恰好 n 字符 (右对齐; n ≥ 3, cap > n), 任何正负/大小/小数都放得下, 精度换宽度.
// 逐级退让, 取首个放得下的:
//   1. 定点 (小数位从多到少; 默认须留 ≥ 1 位非零数字, 否则 "0.0" 会把小量抹成零 → 落到 SI;
//      fixed_zero_ok = true 时放得下就接受全零 —— 有界量如百分比 [0,100] 用这个, 0.04 → "0.0" 而非 "40m")
//        1.234 → "1.23"  -1.234 → "-1.2"  237 → " 237"  12.3 (n=3) → " 12"  0.05 (n=3) → "0.1"
//   2. SI 后缀 (千进位, 尾数 ∈ [1,1000) 进位自动升级, 1e-24..1e27)
//        5e10 → " 50G"  1e-5 → " 10u"  -5e10 → "-50G"  0.004 (n=3) → " 4m"  999.9 (n=3) → " 1k"
//   3. e 记法 (指数压缩):  3e38 → "3e38"
//   4. 仅指数 (尾数舍掉):  -3e38 (n=4) → "-e38"  1e-30 (n=4) → "e-30"
//   5. 放不下: 全 '#'  (n=3 时负大数 / 极端指数会落到这里)
//   nan → "nan", ±inf → "inf"/"-inf" (挤不下 "-in"), 0 → "0"
void fmt_width(char *out, size_t cap, float v, int n, bool fixed_zero_ok = false);

} // namespace misc
