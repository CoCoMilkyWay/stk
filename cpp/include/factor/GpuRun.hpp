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
#include "factor/Search/Contract.hpp"
#include "factor/Stat/Contract.hpp"

#include <cstdint>

namespace factor::gpu {

bool available();

// 0 号设备型号 (UI 显示用, 首次调用探测后缓存); 未编译 CUDA 或无设备 → nullptr
const char *device_name();

// 0 号设备显存 (空闲, 总量) 字节, 此刻 (cudaMemGetInfo); 搜索按它定块大小 / 批大小. 未编译 CUDA → 断言死
void mem_info(size_t *free_bytes, size_t *total_bytes);
// Search 一批候选上限 (= Stat/Dev.cuh kMaxBatch, 候选指针走 kernel 参数区); 未编译 CUDA → 0
int search_max_batch();

// ---- 常驻会话: 一轮对拍里输入只上传一次, 逐算子只跑 kernel + 拷回输出 ----
//   Session 持有: 上传过的输入平面 (归会话, close 时统一释放) / 一块输出平面 / 按需长大的工作区 / 计时事件.
//   n = T·A 开会话时定死 (整轮张量同形). 输出不清零 (契约: 后端写满每格).
//   pin / unpin: 把宿主输出缓冲注册成页锁定 (cudaHostRegister), D2H 走 DMA; 一轮一次, 不要逐算子做.
struct Session;
struct DevPlane; // 显存里一对 (值, 掩码) 平面
Session *session_open(size_t n);
void session_close(Session *s);
DevPlane *upload(Session *s, const float *v, const uint8_t *m); // n 取会话的
DevPlane *upload_mask(Session *s, const uint8_t *m);            // 只有掩码的平面 (截面池 g; v = nullptr)
void pin(void *host, size_t bytes);
void unpin(void *host);

// name 必须在 OpTable 里; 不在则断言死 (三后端同名是硬约束). 元数不到的槽位传 nullptr.
// CS 算子多收池掩码 g (只用 g->m, 必填; 契约【截面池 g】).
// kernel_ms 非空 → 回填纯 kernel 耗时 (cudaEvent), 不含工作区分配 / D2H
void run_ts(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, float *ov, uint8_t *om,
            int T, int A, const Param &p, double *kernel_ms = nullptr);
void run_cs(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, const DevPlane *g,
            float *ov, uint8_t *om, int T, int A, const Param &p, double *kernel_ms = nullptr);

// ---- 设备常驻中间量 (因子 DAG: 算子之间不落宿主, 见 factor/EvalGpu.hpp) ----
//   plane_new / plane_del  会话形状的空平面 (不清零), 归调用方释放 (不在会话的 in 列表里)
//   download               拷回宿主 (v / m 任一可空 = 只拷另一侧; 如只要掩码算有效率)
//   run_*_dev              算子输出写进设备平面 out (不拷回); 其余同会话版
DevPlane *plane_new(Session *s);
void plane_del(Session *s, DevPlane *p);
void download(Session *s, const DevPlane *p, float *v, uint8_t *m);
void run_ts_dev(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, DevPlane *out, int T,
                int A, const Param &p, double *kernel_ms = nullptr);
void run_cs_dev(Session *s, const char *name, const DevPlane *x, const DevPlane *y, const DevPlane *z, const DevPlane *g,
                DevPlane *out, int T, int A, const Param &p, double *kernel_ms = nullptr);

// 一次性版本 (op_check 用): 宿主指针进出, 内部开临时会话 (上传 → 跑 → 关); kernel_ms 口径同上
void run_ts(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms = nullptr);
void run_cs(const char *name, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
            const float *zv, const uint8_t *zm, const uint8_t *gm, float *ov, uint8_t *om, int T, int A, const Param &p,
            double *kernel_ms = nullptr);

// Stat 评估算子 (factor/Stat/Gpu.cuh; 契约 factor/Stat/Contract.hpp): 宿主指针进, rows[hd.n][T] 宿主出.
// gm = 截面池掩码 [T][A] (只看 gm ∧ xm 的格). lab = hd.n 组**宿主**标签 (factor::stat::Label 的 v[kLegs] / m; ry 不看),
// 内部拷进显存 → 每组 prep_label (rank_y) → eval → 拷回.
// prep_ms / eval_ms 非空 → 回填纯 kernel 耗时 (cudaEvent): 预处理是常驻期一次的成本, 评估是每因子的成本, 分开记
void run_stat(const float *xv, const uint8_t *xm, const uint8_t *gm, factor::stat::Frame f, int T, int A, const factor::stat::Holds &hd,
              const factor::stat::Label *lab, factor::stat::Row *rows, double *prep_ms = nullptr, double *eval_ms = nullptr);

// Stat 常驻会话 (因子面板: 一个作用域多因子): 标签上传 + rank 预处理只做一次, 逐因子只跑 eval,
// x 是设备常驻平面 (DAG 根槽), g 是设备常驻池掩码平面 (只用 g->m), 口径 f 逐因子给 (会话与口径无关), rows[hd.n][T] 拷回;
// rx 非空再拷回 r16(x) 工作区 [T][A] (宿主侧换手成本 cost_tiers 要秩, 与 CPU 后端同一份; 秩是整数, 两后端逐位同).
// run_stat 一次性版 = open + 上传 x / g + eval + close.
struct StatSession;
StatSession *stat_open(int T, int A, const factor::stat::Holds &hd, const factor::stat::Label *lab, double *prep_ms = nullptr);
void stat_eval(StatSession *s, const DevPlane *x, const DevPlane *g, factor::stat::Frame f, factor::stat::Row *rows,
               double *eval_ms = nullptr, uint16_t *rx = nullptr);
void stat_close(StatSession *s);

// Search 批评估算子 (factor/Search/Gpu.cuh; 契约 factor/Search/Contract.hpp): 一个会话 = 一块 (chunk) 的形状 (T, A) + 候选上限 Cmax + 持有期,
// 显存 open 时一次分配 (标签 / ry / rx[Cmax] / 分片 / 结果). 流程: open → 每块 load (上传宿主标签 hd.n 组, ry 预处理, 候选共用) →
// 每批 eval (C ≤ Cmax 张设备常驻候选平面 (DAG 根槽) + 池掩码 g → out[C] 宿主 Result; t0_abs / t_lo 见契约【分块流式】) → close.
// prep_ms / eval_ms 非空 → 回填纯 kernel 耗时 (cudaEvent).
struct SearchSession;
SearchSession *search_open(int T, int A, int Cmax, const factor::stat::Holds &hd);
void search_load(SearchSession *s, const factor::stat::Label *lab, double *prep_ms = nullptr);
void search_eval(SearchSession *s, const DevPlane *const *x, int C, const DevPlane *g, factor::stat::Frame f, int t0_abs, int t_lo,
                 factor::search::Result *out, double *eval_ms = nullptr);
// 刚 eval 过的候选 c 的 r16(x) 抽样行: 行 t0 + i·stride (i < n_rows) 各 A 格 → out[n_rows][A] (调用方给候选间截面相关用)
void search_ranks(SearchSession *s, int c, int t0, int stride, int n_rows, uint16_t *out);
void search_close(SearchSession *s);
// 一次性版 (op_check 用): 宿主指针进出 (xv[c] / xm[c] 各一张 [T][A]), 内部 open + 上传 + load + eval + close
void run_search(int C, const float *const *xv, const uint8_t *const *xm, const uint8_t *gm, factor::stat::Frame f, int T, int A, int t0_abs,
                int t_lo, const factor::stat::Holds &hd, const factor::stat::Label *lab, factor::search::Result *out, double *prep_ms = nullptr,
                double *eval_ms = nullptr);

} // namespace factor::gpu
