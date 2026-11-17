#pragma once

// =============================================================================
// GPU 后端的宿主接口 (POD 边界: 把 nvcc 编译单元与 clang 宿主代码隔开)
// =============================================================================
//   宿主侧纯 C++ 编译, GpuRun.cu 由 nvcc 编译; 两边只经本头声明相见 (引擎与 op_check 同一入口).
//   未开 CUDA 时链接 GpuRunStub.cpp, available() = false.
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
// Search 一批候选上限 (= Stat/Contract.hpp kMaxBatch, 候选指针走 kernel 参数区); 未编译 CUDA → 0
int search_max_batch();

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

// Stat 评估算子 (factor/Stat/Gpu.cuh; 契约 factor/Stat/Contract.hpp): 宿主指针进, acc[L.hd.n] 宿主出 (二级由调用方 finish_all).
// gm = 截面池掩码 [T][A] (只看 gm ∧ x.m 的格). L = L.hd.n 组**宿主**标签 (Label 的 v[kLegs] / m / eo (各组同一张); ry 不看),
// 内部拷进显存 → 每组 prep_label (rank_y) → run → 拷回. rows 非空再拷回一级 [L.hd.n][T] (对拍用).
// prep_ms / eval_ms 非空 → 回填纯 kernel 耗时 (cudaEvent): 预处理是常驻期一次的成本, 评估是每因子的成本, 分开记.
// seg = 因子所在层的段长 (Span::g; Stat / Search 会话同)
void run_stat(factor::stat::Plane x, const uint8_t *gm, factor::stat::Frame f, int T, int A, int seg, const factor::stat::Labels &L,
              factor::stat::Accum *acc, factor::stat::Row *rows = nullptr, double *prep_ms = nullptr, double *eval_ms = nullptr);

// Stat 常驻会话 (因子表: 一个作用域多因子): 标签上传 + rank 预处理只做一次, 逐因子只跑 run,
// x 是设备常驻平面 (DAG 根槽), g 是设备常驻池掩码平面 (只用 g->m), 口径 f 逐因子给 (会话与口径无关), acc[L.hd.n] 拷回;
// rows 非空再拷回一级 [L.hd.n][T]; ec 非空再拷回方向 d 的端码 [T][A] (宿主侧换手成本 cost_rows 要, 与 CPU 后端 end_codes 同一份; 整数, 两后端逐位同);
// rx 非空再拷回 r16 [T][A] (宿主侧探针要: 可秩率 cover_rows / alpha 分布). t0_abs / t_lo / t_back 同 search_eval (契约【分块流式】: 一级只出行 [t_lo, T − t_back)).
// run_stat 一次性版 = open + 上传 x / g + eval (整块) + close.
struct StatSession;
StatSession *stat_open(int T, int A, int seg, const factor::stat::Labels &L, double *prep_ms = nullptr);
void stat_load(StatSession *s, const factor::stat::Labels &L, double *prep_ms = nullptr);
void stat_eval(StatSession *s, const DevPlane *x, const DevPlane *g, factor::stat::Frame f, factor::stat::Dir d, int t0_abs, int t_lo, int t_back,
               factor::stat::Accum *acc, factor::stat::Row *rows = nullptr, uint8_t *ec = nullptr, uint16_t *rx = nullptr, double *eval_ms = nullptr);
void stat_close(StatSession *s);
size_t stat_bytes(const StatSession *s);

// Search 批评估算子 (factor/Search/Gpu.cuh; 契约 factor/Search/Contract.hpp): 一个会话 = 一块 (chunk) 的形状 (T, A) + 候选上限 Cmax + 持有期,
// 显存 open 时一次分配 (标签 / ry / rx[Cmax] / 分片 / 结果). 流程: open → 每块 load (上传宿主标签 hd.n 组, ry 预处理, 候选共用) →
// 每批 eval (C ≤ Cmax 张设备常驻候选平面 (DAG 根槽) + 池掩码 g → out[C] 宿主 Result; t0_abs / t_lo / t_back 见契约【分块流式】) → close.
// cap = 满仓 (Labels::cap, 各块同一个). prep_ms / eval_ms 非空 → 回填纯 kernel 耗时 (cudaEvent).
struct SearchSession;
SearchSession *search_open(int T, int A, int seg, int Cmax, const factor::stat::Holds &hd, double cap);
void search_load(SearchSession *s, const factor::stat::Label *lab, double *prep_ms = nullptr);
void search_eval(SearchSession *s, const DevPlane *const *x, int C, const DevPlane *g, factor::stat::Frame f, int t0_abs, int t_lo, int t_back,
                 factor::search::Result *out, double *eval_ms = nullptr);
// 刚 eval 过的候选 c 的 r16(x) 抽样行: 行 t0 + i·stride (i < n_rows) 各 A 格 → out[n_rows][A] (调用方给候选间截面相关用)
void search_ranks(SearchSession *s, int c, int t0, int stride, int n_rows, uint16_t *out);
void search_close(SearchSession *s);
size_t search_bytes(const SearchSession *s);
// 一次性版 (op_check 用): 宿主指针进出 (xv[c] / xm[c] 各一张 [T][A]), 内部 open + 上传 + load + eval + close
void run_search(int C, const float *const *xv, const uint8_t *const *xm, const uint8_t *gm, factor::stat::Frame f, int T, int A, int seg,
                int t0_abs, int t_lo, int t_back, const factor::stat::Holds &hd, double cap, const factor::stat::Label *lab, factor::search::Result *out,
                double *prep_ms = nullptr, double *eval_ms = nullptr);

} // namespace factor::gpu
