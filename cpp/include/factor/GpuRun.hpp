#pragma once

// =============================================================================
// GPU 后端的对拍入口 (把 CUDA 编译单元与 op_check 的宿主代码隔开)
// =============================================================================
//   op_check 用纯 C++ 编译, Gpu.cuh 用 clang -x cuda 编译; 两边只通过这个声明相见.
//   拷入拷出由 .cu 侧包办: 一次性版本收**宿主指针** (op_check); 常驻 Session 版本一轮只上传一次
//   (GUI Operators 表: 算子之间除了 D2H 拷回一张输出, 不留别的 overhead).
//   未开 CUDA 时链接 op_check_gpu_stub.cpp, available() = false, op_check 退化成两方对拍.
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/Stat/Contract.hpp"

#include <cstdint>

namespace factor::gpu {

bool available();

// 0 号设备型号 (UI 显示用, 首次调用探测后缓存); 未编译 CUDA 或无设备 → nullptr
const char *device_name();

// ---- 常驻会话: 一轮对拍里输入只上传一次, 逐算子只跑 kernel + 拷回输出 ----
//   Session 持有: 上传过的输入平面 (归会话, close 时统一释放) / 一块输出平面 / 按需长大的工作区 / 计时事件.
//   n = T·A 开会话时定死 (整轮张量同形). 输出不清零 (契约: 后端写满每格).
//   pin / unpin: 把宿主输出缓冲注册成页锁定 (cudaHostRegister), D2H 走 DMA; 一轮一次, 不要逐算子做.
struct Session;
struct DevPlane; // 显存里一对 (值, 掩码) 平面
Session *session_open(size_t n);
void session_close(Session *s);
DevPlane *upload(Session *s, const float *v, const uint8_t *m); // n 取会话的
void pin(void *host, size_t bytes);
void unpin(void *host);

// name 必须在 OpTable 里; 不在则断言死 (三后端同名是硬约束). 元数不到的槽位传 nullptr.
// kernel_ms 非空 → 回填纯 kernel 耗时 (cudaEvent), 不含工作区分配 / D2H
void run_ts(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, float *ov, uint8_t *om,
            int T, int A, const Param &p, double *kernel_ms = nullptr);
void run_cs(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, float *ov, uint8_t *om,
            int T, int A, const Param &p, double *kernel_ms = nullptr);

// 一次性版本 (op_check 用): 宿主指针进出, 内部开临时会话 (上传 → 跑 → 关); kernel_ms 口径同上
void run_ts(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms = nullptr);
void run_cs(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms = nullptr);

// Stat 评估算子 (factor/Stat/Gpu.cuh; 契约 factor/Stat/Contract.hpp): 宿主指针进, rows[hd.n][T] 宿主出.
// lab = hd.n 组标签 (fp16 位 + 掩码), 内部拷进显存 → 每组 prep_label (rank_y) → eval → 拷回.
// prep_ms / eval_ms 非空 → 回填纯 kernel 耗时 (cudaEvent): 预处理是常驻期一次的成本, 评估是每因子的成本, 分开记
struct StatLabelHost {
  const uint16_t *lv, *sv;
  const uint8_t *m;
};
void run_stat(const float *xv, const uint8_t *xm, int T, int A, const factor::stat::Holds &hd, const StatLabelHost *lab,
              factor::stat::Row *rows, double *prep_ms = nullptr, double *eval_ms = nullptr);

} // namespace factor::gpu
