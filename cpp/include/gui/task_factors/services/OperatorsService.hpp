// OperatorsService — Factors→Operators 表的单 worker: 一张合成张量喂给 OpTable 全部算子,
// 逐算子跑 cpu (挖掘向量) / stream (实盘流式) / gpu (CUDA, 编了才有), 对拍 + 计时, 算完一行发布一行.
//
// 线程模型 (对仗 task_features 的 StreamService, 但不读特征库, 故不套它):
//   - GUI 线程: Request(req) 覆盖挂起请求 + 取消在跑 + 唤醒 (worker 懒起); RequestCancel 只中断
//   - worker:   取最新请求 → 全行复位 Pending → 逐算子 Running → 算 → 持锁写回 Done, epoch++
//   - UI:       持 mutex 读 rows 快照; 进度走原子, 免锁
//
// 默认不自动跑: 进页先由 TabOperators 的 LoadOperatorTableJson 吃本地 operators.json (校验静态列
// 与整轮完整性, 不过就删文件), 命中的话经 AdoptSnapshot 直接摆成 Done, worker 根本不起; 只有手动
// Run (或本地无有效快照) 才真算, 一轮结束由 TaskFactors 落盘.
//
// 对拍口径与 op_check 完全一致 (同一份 factor/Check.hpp): PLAIN profile, 配方按算子,
// d/k 按每算子默认表 (kDParams / kKParams), 容差 tol_of(name, gpu, PLAIN). 差别只在
// op_check 全扫 profile × d, 这里每算子只跑一组默认参数.
// 计时: cpu / stream = 整段 wall time (steady_clock); gpu = 纯 kernel (cudaEvent, 不含
// cudaMalloc + H2D/D2H —— 搬运是 GpuRun 对拍接口的成本, 不是算子的), 首次调用前做一次热身.
// 计时之外不留 overhead: 输入张量一轮按 (槽位, Gen) 造一次 (并行 fill) 并上传显存一次, 输出缓冲
// 预触页后跨算子复用, GPU 走常驻 Session; 算子之间只剩一张输出的 D2H (宿主 pin 过, DMA) 与 compare.
#pragma once

#include "factor/Check.hpp"
#include "factor/Contract.hpp"
#include "factor/Stat/Contract.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace GUI::Factors {

struct RoundCtx; // 一轮的常驻件 (输入缓存 / 输出缓冲 / GPU 会话), 定义在 .cpp

// 页面参数: 张量形状 [times × A]. times = 时间轴长度 (期 = 分钟, 以后可选秒), 必须整段
// (段 = 交易日 = kSegLen 分钟, 段界对齐是 EXPAND 的前提, UI 负责取整).
// 默认 25500 (= 100 段) × 1000.
// d/k 每算子自带默认 (Check.hpp kDParams / kKParams); seed 由 Request() 取时间戳
// (不做复现, 同一轮内所有算子共享同一张量).
struct OperatorsRequest {
  int times = 100 * factor::kSegLen;
  int A = 1000;
  unsigned seed = 1; // Request() 时间戳填充, UI 不编辑
};

enum class RowStatus : uint8_t { Pending,
                                 Running,
                                 Done };

// 一行 = OpTable 一个算子: 静态列直接来自表 (三维分类: T 窗 × A 域 × 核类), 动态列由 worker 发布.
// 字段名 = JSON 键 = UI 表头 (全库统一叫法); 唯一例外: operator 是 C++ 保留字, 字段退而叫 op
// 首行是 Stat 评估算子 (factor/Stat, 不在 OpTable, 无流式后端): 同一张表同一套列, 没有的项空着
// (classified = false → T / A / Kernel 显示空, stream 列 n/a); 主要看 cpu / gpu 耗时 (= eval, 每因子一次).
struct OperatorRow {
  // 静态
  const char *e_name = nullptr; // 英文名 (OpTable 行名, 域_核_窗)
  const char *c_name = nullptr; // 中文名: 域 (时/截/组) + 窗 (累/滚/指) + 核, 与 e_name 逐段对应
  int arity = 0;
  factor::T T = factor::T::POINT;         // T 窗 (CS 组恒 POINT)
  factor::A A = factor::A::SELF;          // A 域
  factor::Kern kern = factor::Kern::MAP;  // 核类
  factor::KDom kdom = factor::KDom::NONE; // k 域 (OpTable k域 列)
  const char *params = "";                // 本算子读取的 Param 字段名, 如 "d,k" (= params_str(T, kdom), 由表推出)
  std::string operand;                    // 签名 LaTeX (Expr.hpp operand_tex 从 in / T / kdom 生成; 占位符 ⟨d⟩⟨k⟩⟨k2⟩ 由 UI 换成本轮实际值)
  factor::Dom in[3] = {};                 // 逐元自变量值域 (OpTable in 列)
  factor::Dom out = factor::Dom::REAL;    // 因变量值域 (OpTable out 列; Stat 行无意义, classified = false 时显示空)
  const char *op = nullptr;               // 算子定义 (LaTeX; JSON / UI 键叫 operator)
  const char *note = nullptr;             // 用途与选型 (一句话: 量什么; 怎么用 / 配什么)
  bool classified = true;                 // false = 不在 OpTable 三维分类里 (Stat), T / A / Kernel 列空着
  // 动态
  factor::Param param; // 本轮实际喂的参数 (d/k/k2 来自 Check.hpp 每算子默认表); 复位时就填, 不等跑到
  RowStatus status = RowStatus::Pending;
  factor::check::Diff stream;                    // stream vs cpu
  factor::check::Diff gpu;                       // gpu vs cpu (gpu_ms < 0 时无意义)
  double cpu_ms = 0, stream_ms = 0, gpu_ms = -1; // gpu_ms = 纯 kernel; < 0 = 无该后端
};

// Stat 行放不进表列的附带信息 (悬停 e_name 看; 也落 operators.json 的 stat 键):
// prep = 标签 rank 预处理 (常驻期一次, 不进耗时列), 两口径 (CS / TS, 下标 = Frame) 每持有期的 cpu 侧二级汇总.
// 造数与 op_check 同口径 (Stat/Check.hpp make: long = 0.3·z + 噪声, IC ≈ 0.3; TS 的 x = Φ(z)), 持有期 kHolds.
// 表列耗时 = 两口径 eval 之和; Diff = 两口径 × 两级合并.
struct StatExtra {
  double cpu_prep_ms = 0, gpu_prep_ms = -1;
  int n_hold = 0;
  factor::stat::HoldStat hold[2][factor::stat::kMaxHold]; // [Frame][hold]
};

enum class OperatorsStatus : uint8_t { Idle,
                                       Running,
                                       Done,
                                       Cancelled };

class OperatorsService {
public:
  OperatorsService(); // 由 OpTable 展开静态行 (顺序 = 表序: TS 逐点 / TS 有窗 / CS)
  ~OperatorsService() { Stop(); }
  OperatorsService(const OperatorsService &) = delete;
  OperatorsService &operator=(const OperatorsService &) = delete;

  // GUI 线程
  void Request(const OperatorsRequest &req);
  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }
  void Stop(); // join (幂等)
  // 直接吃一份落盘快照当"已跑完"(进页载入 operators.json 用, 不起 worker, 不算不落盘):
  // snap 由调用方从 rows 拷出后只填动态列 (静态列原样), 校验过了才进来; 仅允许 worker 未起时调.
  // extra = 快照里 Stat 行的附带信息 (旧文件没有 → 默认值)
  void AdoptSnapshot(const OperatorsRequest &req, std::vector<OperatorRow> &&snap, const StatExtra &extra, int failed);

  // 进度 (原子, 免锁)
  OperatorsStatus status() const { return status_.load(std::memory_order_acquire); }
  int done() const { return done_.load(std::memory_order_relaxed); }
  int total() const { return static_cast<int>(rows.size()); }
  int failed() const { return failed_.load(std::memory_order_relaxed); }
  uint64_t epoch() const { return epoch_.load(std::memory_order_relaxed); }
  // Stat 行 = 首行 (OpTable 之前); worker 每轮先跑它 (挖掘的第一道闸)
  size_t stat_index() const { return 0; }

  // UI 持锁读; current = 正在跑 / 上次跑完的参数快照
  std::mutex mutex;
  std::vector<OperatorRow> rows;
  StatExtra stat;
  OperatorsRequest current;
  bool from_json = false; // 表内容来自本地 operators.json 载入 (非本进程算的), UI 标注用

private:
  // 每行一个跑手: 由 OpTable 宏实例化的模板, 只算动态列 (静态列 worker 不碰); Stat 行另走 run_stat.
  // 输入 / 输出缓冲 / GPU 会话全从 RoundCtx 取 (一轮一份), 跑手内部不分配不造数: 计时外零 overhead
  using RunFn = void (*)(RoundCtx &, OperatorRow &);
  std::vector<RunFn> runners_;

  void worker_loop();

  std::thread thread_;
  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<OperatorsRequest> pending_;
  std::atomic<bool> cancel_{false};
  std::atomic<bool> stop_{false};
  std::atomic<OperatorsStatus> status_{OperatorsStatus::Idle};
  std::atomic<int> done_{0};
  std::atomic<int> failed_{0};
  std::atomic<uint64_t> epoch_{0};
  bool gpu_warmed_ = false; // 仅 worker 线程读写
};

} // namespace GUI::Factors
