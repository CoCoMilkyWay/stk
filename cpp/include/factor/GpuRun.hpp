#pragma once

// =============================================================================
// GPU 后端的宿主接口 (POD 边界: 把 nvcc 编译单元与 clang 宿主代码隔开)
// =============================================================================
//   宿主侧纯 C++ 编译, GpuRun.cu 由 nvcc 编译; 两边只经本头声明相见 (引擎与 op_check 同一入口).
//   未开 CUDA 时链接 GpuRunStub.cpp, available() = false.
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Fold.hpp"

#include <cstdint>

namespace factor::gpu {

bool available();

// 0 号设备型号 (UI 显示用, 首次调用探测后缓存); 未编译 CUDA 或无设备 → nullptr
const char *device_name();

// 0 号设备显存 (空闲, 总量) 字节, 此刻 (cudaMemGetInfo); 搜索按它定块大小 / 批大小. 未编译 CUDA → 断言死
void mem_info(size_t *free_bytes, size_t *total_bytes);

// ---- 常驻会话: 形状 n = T·A 开会话时定死, 持有按需增长的算子工作区 / 计时事件 / (对拍用) 输出平面 ----
//   pin / unpin: 宿主缓冲注册成页锁定 (cudaHostRegister), D2H 走 DMA.
struct Session;
struct DevPlane; // 显存里一对 (值, 掩码) 平面
Session *session_open(size_t n);
void session_close(Session *s);
size_t session_bytes(const Session *s);
DevPlane *upload(Session *s, const float *v, const uint8_t *m); // 归会话释放 (对拍用)
DevPlane *upload_mask(Session *s, const uint8_t *m);            // 归会话释放 (只有掩码)
void pin(void *host, size_t bytes);
void unpin(void *host);

// op = OpTable 行序下标 (= expr::kOps 下标). in[0..n_in) = 输入表 (Contract Call::xs; N 元回归末元 = 目标标签).
// g = 截面池掩码 (只用 g->m; 契约【截面池 g】): CS 算子 (A 域 ALL / GROUP) 必填, TS 算子必须为空.
// seg = 段长 (Contract【层几何】, T 须为整段数).
// kernel_ms 非空 → 回填纯 kernel 耗时 (cudaEvent), 不含工作区分配 / D2H
void run(Session *s, int op, const DevPlane *const *in, int n_in, const DevPlane *g, float *ov, uint8_t *om, int T, int A, int seg,
         const Param &p, double *kernel_ms = nullptr);

// ---- 设备常驻平面 (因子引擎: 输入 / 中间量常驻显存, 见 factor/EvalGpu.hpp) ----
//   plane_new / mask_new / plane_del  会话形状的空平面 (值+掩码 / 仅掩码, 不清零), 归调用方释放
//   upload_to ⇄ download              宿主 ↔ 设备平面 (v / m 任一可空 = 只传另一侧)
//   run_dev / run_fused               算子 / 逐点融合链输出写进设备平面 out (不拷回)
DevPlane *plane_new(Session *s);
DevPlane *mask_new(Session *s);
void plane_del(Session *s, DevPlane *p);
void upload_to(Session *s, DevPlane *p, const float *v, const uint8_t *m);
void download(Session *s, const DevPlane *p, float *v, uint8_t *m);
void run_dev(Session *s, int op, const DevPlane *const *in, int n_in, const DevPlane *g, DevPlane *out, int T, int A, int seg,
             const Param &p, double *kernel_ms = nullptr);
void run_fused(Session *s, const Fuse &f, const DevPlane *const *in, int n_in, DevPlane *out, int T, int A, int seg,
               double *kernel_ms = nullptr);

// 一次性版本 (op_check 用): 宿主指针进出 (v[i] / m[i], i < n_in), 内部开临时会话 (上传 → 跑 → 关); gm 同 g; kernel_ms 口径同上
void run(int op, int n_in, const float *const *v, const uint8_t *const *m, const uint8_t *gm, float *ov, uint8_t *om, int T, int A,
         int seg, const Param &p, double *kernel_ms = nullptr);

// Stat 评估算子 (factor/Stat/Gpu.cuh; 契约 factor/Stat/Contract.hpp): 一个常驻会话 = 一块 (chunk) 的形状 (Span 的 T / A / g) + 候选上限 Cmax + 持有期 + 满仓 cap,
// 显存 open 时一次分配 (标签 / ry / rx[Cmax] / 一级 / 分片 / 出口). 流程: open → 每块 load (上传宿主标签 hd.n 组, ry 预处理; 各候选共用) →
// 每批 eval (C ≤ Cmax 张设备常驻候选平面 (DAG 根槽) + 池掩码 g (只用 g->m), 评估口径 m 逐批给) → close.
//   与 CPU cpu::stat::run 同参序 (x, g, m, s, …): s = 本块 Span (形状同会话; t0_abs / t_lo / t_back = 契约【分块流式】, 整日对齐).
//   出口 acc[C][hd.n] 宿主 + day[C][hd.n][D] 宿主 (D = 统计日数, 契约二级 Day); C = 1 走逐行并行, 另可拷回一级 rows[hd.n][T] /
//   已定向 r16 rx [T][A] (宿主侧账集 book_rows 与探针要); C > 1 走逐候选并行, 只出 acc / day. prep_ms / eval_ms 非空 → 回填纯 kernel 耗时 (cudaEvent).
// run_stat 一次性版 (op_check 用): 宿主指针进出, 内部 open + 上传 + load + eval + close.
int stat_max_batch(); // 一批候选上限 (= Stat/Contract.hpp kMaxBatch); 未编译 CUDA → 0
struct StatSession;
StatSession *stat_open(const factor::stat::Span &s, int Cmax, const Holds &hd, double cap);
void stat_load(StatSession *s, const factor::stat::Label *lab, double *prep_ms = nullptr);
void stat_eval(StatSession *ss, const DevPlane *const *x, int C, const DevPlane *g, const factor::stat::Mode &m, const factor::stat::Span &s, factor::stat::Accum *acc,
               factor::stat::Day *day, factor::stat::Row *rows = nullptr, uint16_t *rx = nullptr, double *eval_ms = nullptr);
void stat_close(StatSession *s);
size_t stat_bytes(const StatSession *s);
void run_stat(int C, const float *const *xv, const uint8_t *const *xm, const uint8_t *gm, const factor::stat::Mode &m, const factor::stat::Span &s, const Holds &hd,
              double cap, const factor::stat::Label *lab, factor::stat::Accum *acc, factor::stat::Day *day, factor::stat::Row *rows = nullptr, double *prep_ms = nullptr,
              double *eval_ms = nullptr);

} // namespace factor::gpu
