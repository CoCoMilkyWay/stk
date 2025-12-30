#pragma once

#ifdef PROFILE_MODE
#include <Tracy.hpp>
  
// ===== Zone Marking =====
#define Trace                 ZoneScoped
#define TraceN(name)          ZoneScopedN(name)
#define TraceS(depth)         ZoneScopedS(depth)
#define TraceNS(name, depth)  ZoneScopedNS(name, depth)

// ===== Zone Attributes =====
#define TraceName(text, size)     ZoneName(text, size)
#define TraceNameS(text)          ZoneName(text, strlen(text))
#define TraceText(text, size)     ZoneText(text, size)
#define TraceTextS(text)          ZoneText(text, strlen(text))
#define TraceValue(value)         ZoneValue(value)
#define TraceColor(color)         ZoneColor(color)

// ===== Common Colors (0xRRGGBB) =====
constexpr uint32_t C_Red        = 0xFF0000;
constexpr uint32_t C_Green      = 0x00FF00;
constexpr uint32_t C_Blue       = 0x0000FF;
constexpr uint32_t C_Yellow     = 0xFFFF00;
constexpr uint32_t C_Magenta    = 0xFF00FF;
constexpr uint32_t C_Cyan       = 0x00FFFF;
constexpr uint32_t C_Orange     = 0xFFA500;
constexpr uint32_t C_Gray       = 0x808080;
constexpr uint32_t C_White      = 0xFFFFFF;
constexpr uint32_t C_LightBlue  = 0x87CEEB;
constexpr uint32_t C_Purple     = 0x9370DB;
constexpr uint32_t C_Pink       = 0xFFB6C1;
constexpr uint32_t C_Brown      = 0xA52A2A;
constexpr uint32_t C_Lime       = 0x32CD32;


// ===== Frame & Thread =====
#define TraceFrame                FrameMark
#define TraceFrameN(name)         FrameMarkNamed(name)
#define TraceThread(name)         tracy::SetThreadName(name)

// ===== Messages =====
#define TraceMessage(text, size)  TracyMessage(text, size)
#define TraceMessageS(text)       TracyMessageS(text, strlen(text))

// ===== Memory =====
#define TraceAlloc(ptr, size)     TracyAlloc(ptr, size)
#define TraceFree(ptr)            TracyFree(ptr)

// ========================================================================
// 一、Zone Marking(作用域标记 - 核心功能)
// ========================================================================

/**
 * Trace - 匿名作用域 zone
 *
 * 用法: 在函数/block 入口放一行 Trace;
 * 效果:
 *   - zone 名字 = 函数名(自动)
 *   - 生命周期 = 作用域(自动 RAII)
 *   - 层级 = 调用栈嵌套(自动)
 *
 * 示例:
 *   void my_function() {
 *     Trace;  // zone 名字显示为 "my_function"
 *     // ...
 *   }
 *
 * 适用: 80% 情况,函数入口,稳定低开销
 */

/**
 * TraceN(name) - 命名作用域 zone
 *
 * 区别: zone 名字由你指定(而不是函数名)
 *
 * 示例:
 *   for (...) {
 *     TraceN("InnerLoop");  // UI 显示 "InnerLoop"
 *     // ...
 *   }
 *
 * 适用: pipeline stage, loop block, 想用自定义名字
 * 注意: name 必须是静态字符串
 */

/**
 * TraceS(depth) - 带调用栈采集的 zone
 *
 * ⚠️ 重要: depth 不是"自动往下打 N 层"！
 *
 * 真实含义:
 *   - 从当前位置往上采集 depth 层调用栈(使用 dbghelp)
 *   - 每次进入 zone 时触发一次 StackWalk64
 *   - 开销: ~630ns/次(比普通 zone 慢 10 倍+)
 *
 * 示例:
 *   void hot_function() {
 *     TraceS(25);  // 采集 25 层调用栈,看谁调用了这个函数
 *     // ...
 *   }
 *
 * 适用:
 *   - 需要看"谁调用了这个函数"
 *   - 深度分析调用关系
 *   - ❌ 不适合高频调用(开销大)
 */

/**
 * TraceNS(name, depth) - 命名 + 调用栈采集
 *
 * = TraceN + TraceS 的组合
 *
 * 示例:
 *   TraceNS("SequentialWorker", 25);
 *   // zone 名字 = "SequentialWorker"
 *   // 调用栈深度 = 25 层
 *
 * 适用: 顶层入口函数(如 worker 线程),需要完整调用栈分析
 */

// ========================================================================
// 二、Zone Attributes(zone 属性 - 附加信息)
// ========================================================================
// ⚠️ 必须写在 zone 作用域内部

/**
 * TraceName / TraceNameS - 运行时改名
 *
 * 用法: 在 Trace; 之后调用,动态修改 zone 名字
 *
 * 示例:
 *   Trace;
 *   TraceNameS(asset.name.c_str());  // 名字 = 运行时变量
 *
 * 注意: 比 TraceN 慢(需要 runtime copy)
 */

/**
 * TraceText / TraceTextS - 附加说明文本
 *
 * 用法: 给 zone 添加额外信息(UI 右侧显示)
 *
 * 示例:
 *   TraceN("DateLoop");
 *   TraceTextS(date_str.c_str());  // 显示日期
 *
 * 适用: 参数、状态、分支说明
 */

/**
 * TraceValue(value) - 附加数值
 *
 * 用法: 给 zone 附一个 int64/double(Tracy 可画时间序列)
 *
 * 示例:
 *   TraceN("ProcessBatch");
 *   TraceValue(order_count);  // 显示订单数
 *
 * 适用: feature/indicator 统计
 */

/**
 * TraceColor(color) - 改显示颜色
 *
 * 用法: 纯 UI,不影响性能统计
 *
 * 示例:
 *   Trace;
 *   TraceColor(0xFF0000);  // 红色(RGB)
 */

// ========================================================================
// 三、Frame & Thread(时间轴结构)
// ========================================================================

/**
 * TraceFrame - 帧标记
 *
 * 用法: 标记一个逻辑"帧"(Tracy UI 按帧切分时间轴)
 *
 * 示例:
 *   for (int day = 0; day < days; ++day) {
 *     // ... 处理一天数据 ...
 *     TraceFrame;  // 标记一帧结束
 *   }
 *
 * 适用: tick, bar, batch, epoch(非图形项目也非常有用)
 */

/**
 * TraceFrameN(name) - 命名帧
 *
 * 用法: 多 frame 流(不同子系统独立帧)
 *
 * 示例:
 *   TraceFrameN("Strategy");
 *   TraceFrameN("IO");
 */

/**
 * TraceThread(name) - 线程命名
 *
 * 用法: 线程入口调用一次
 *
 * 示例:
 *   TraceThread("worker_0");
 *
 * 强烈建议: 否则 Windows 下全是 Thread 1234
 */

// ========================================================================
// 四、Messages(离散事件)
// ========================================================================

/**
 * TraceMessage / TraceMessageS - 即时消息
 *
 * 用法: 记录离散事件(不形成层级)
 *
 * 示例:
 *   TraceMessageS("Order rejected");
 *
 * 适用: 比 printf/log 更好(时间轴可见)
 */

// ========================================================================
// 五、Memory(分配追踪)
// ========================================================================

/**
 * TraceAlloc / TraceFree - 内存分配追踪
 *
 * 用法: 显式标记内存操作(Tracy 画内存曲线)
 *
 * 示例:
 *   void* ptr = malloc(size);
 *   TraceAlloc(ptr, size);
 *   // ...
 *   TraceFree(ptr);
 *   free(ptr);
 *
 * 注意: 如果已 hook allocator,可以不用
 */

#else

// ===== No-op when profiling disabled =====
#define Trace
#define TraceN(name)
#define TraceS(depth)
#define TraceNS(name, depth)

#define TraceName(text, size)
#define TraceNameS(text)
#define TraceText(text, size)
#define TraceTextS(text)
#define TraceValue(value)
#define TraceColor(color)

#define TraceFrame
#define TraceFrameN(name)
#define TraceThread(name)

#define TraceMessage(text, size)
#define TraceMessageS(text)

#define TraceAlloc(ptr, size)
#define TraceFree(ptr)

#endif

// ============================================================================
// 快速参考表
// ============================================================================
//
// 宏                     | 用途                  | 开销  | 自动递归?
// -----------------------|-----------------------|-------|------------
// Trace                  | 匿名 zone             | 极低  | ❌
// TraceN(name)           | 命名 zone             | 极低  | ❌
// TraceS(depth)          | zone + 调用栈采集     | 高    | ❌
// TraceNS(name, depth)   | 命名 + 调用栈         | 高    | ❌
// TraceName* / Text*     | 运行时附加信息        | 低    | —
// TraceFrame             | 时间轴分帧            | 极低  | —
// TraceThread            | 线程命名              | 一次  | —
// TraceMessage           | 离散事件              | 低    | —
//
// ⚠️ 重要提醒:
// 1. 所有宏都是"手动打桩",不会自动递归或展开
// 2. TraceS/TraceNS 的 depth 是"调用栈采集深度",不是"自动插桩深度"
// 3. 99% 情况只需要 Trace / TraceN,少用 TraceS/TraceNS
//
// ============================================================================
