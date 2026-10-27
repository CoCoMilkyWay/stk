// FactorsScope — 因子评估的作用域层 (四服务共用, 管线最底层): 一个因子的评估域 Domain (domain_of: 所用库 + 评估期 = 落盘日交集 + A 轴 +
//   子池成员, worker 按因子解析) / open_libs; 预算 Budget → 分块 Plan (plan_chunks: 整期一块 或 尽量多统计日 + 前带); Rc = 装载 / 评估的唯一结局.
//   之上: FactorsLoad (装载) → FactorsProbe (探针) → FactorsEngine (引擎 + 管线); 净值引擎 FactorsBook 吃装载层的 Y / B.
#pragma once

#include "features/Storage/DayBatchPlane.hpp"         // Day / LibReaders
#include "gui/task_factors/services/FactorsTable.hpp" // FeatureTable / FactorScope
#include "shared/Analysis.hpp"                        // ReadScope
#include "shared/Pool.hpp"                            // pool::Panel

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace GUI::Factors {

// ---- 评估域 (一个因子; 同文件各权重同叶 → 同一份) ----
// 设计决定: 每因子 (Run: 每文件) 现解析, 不做跨因子优化 (不跨因子缓存 / 合并评估域或森林), 勿改
struct Domain {
  PoolId pool = kAllPool;            // 因子池 (目录)
  PoolMask used = 0;                 // 所用库: 标签库 + 叶所在库 (+ 篮子原料所在库) (+ all: 池成员)
  analysis::ReadScope rs;            // main = 标签库, libs = used 各库清单, axis = A 轴 (标签库轴 / 子池并集), to = A → 各库轴
  std::vector<misc::Ymd> dates;      // 评估期 = used 各库落盘日交集 (升序, 非空)
  std::vector<Day> days;             // [dates] 当日 + 承接日 (标签库日期轴上的前一日, 0 = 轴首) 及其落盘位 (承接日按库判)
  std::optional<pool::Panel> member; // 子池成员 (Lob 池 × Bar 标签层; 第 D 日 = D−1 收盘行, 各层同一份, 池轴 = A 轴)
  std::string key() const;           // 缓存键: 池 | 所用库目录 + 池轴 | 评估期首末 + 天数
};
// worker: 作用域 sc 里已建的库全开 (清单没变的复用; 各因子只读自己用到的). abort = 库判废时拉起的取消旗
void open_libs(const FactorScope &sc, LibReaders &lr, std::atomic<bool> *abort);
// 作用域 sc + 叶 leaves (+ basket: 篮子原料 BCols) → 评估域. 所用库 = 读计划各列的库 (Col::ref) 之并 (+ all: 池成员);
// 所用库未建 / 无共同落盘日 → false + why (正常输入)
bool domain_of(const FeatureTable &ft, const FactorScope &sc, const std::vector<LeafId> &leaves, bool basket, Domain &dm, std::string &why);

// 宿主内存预算: 可用内存 × kHostFrac; 显存预算: 空闲显存 × kVramFrac (GPU 批核)
inline constexpr double kHostFrac = 0.6;
inline constexpr double kVramFrac = 0.85;
double host_budget();
// 整张量装载 (Run / Inspect) 放不下 → assert (who / 规模进消息): 该因子层太细或评估期太长, 拒绝评估
void assert_fits(const char *who, double bytes, int days, int A, int seg);

// ---- 计划: 预算 → 分块 (四服务同一份口径) ----
// 一块 = 评估域日 [d0, d1), 前带 [d0, s0) 只给回看 / lag, 统计日 [s0, d1); 各块统计段首尾相接覆盖全期 (Search 契约【分块流式】)
struct Chunk {
  size_t d0 = 0, s0 = 0, d1 = 0;
  int days() const { return static_cast<int>(d1 - d0); }
};
// 字节 / 格: load = 宿主块 (X + Y, 池篮子另 + B 装载峰值); work = 引擎 + 探针 (同时只一块在用); dev = 显存 (> 0 = 也按显存限块).
// fixed = 与格无关的宿主常驻 (字节)
struct Budget {
  double load = 0.0, work = 0.0, fixed = 0.0, dev = 0.0;
};
struct Plan {
  std::vector<Chunk> chunks;
  int D = 0;             // 每块统计日上限
  bool cache_all = true; // 全部块常驻 (各占一个块); 否则一块复用 (每趟逐块重读)
  double host = 0.0;     // 宿主预算 (字节, 已扣 fixed)
  double vram = 0.0;     // 显存预算 (字节; dev = 0 → 0)
  double cache = 0.0;    // 全部块常驻的装载量 (字节)
  int max_days() const;  // 最长一块的日数 (含前带)
};
// carry < 0 → 整期一块, 放不下 assert_fits (who / 规模进消息); 否则块 = 尽量多的统计日 (≥ 1) + 前带 carry 日, 块数最少
Plan plan_chunks(const Domain &dm, int seg, const Budget &b, int carry, const char *who);

// ---- 评估结局 (装载 / 评估 / 四服务同一个) ----
enum class Rc : uint8_t { Ok,
                          Empty,  // 评估域不成 (所用库未建 / 无共同落盘日), why
                          Broken, // 特征叶值域越界 (全部根), why
                          Cancel,
                          Stale }; // 库判废 (块已 reset; 调用方置 kStaleMsg)
inline constexpr const char *kStaleMsg = "特征库判废 (池轴/字段表与当前不符), 需重算特征";
// 装载的 bool 结局 → Rc: 成 → Ok; 败 → 库判废 Stale, 否则 Cancel
inline Rc rc_of(bool ok, const LibReaders &lr) { return ok ? Rc::Ok : lr.stale() ? Rc::Stale
                                                                                 : Rc::Cancel; }
// 中断结局 (Cancel / Stale): 本轮作废
inline bool aborted(Rc rc) { return rc == Rc::Cancel || rc == Rc::Stale; }

} // namespace GUI::Factors
