// FactorsScope — 因子评估的作用域层 (全部因子服务共用, 管线最底层): 一个因子的评估域 Domain (domain_of: 所用库 + 评估期 = 落盘日交集 + A 轴,
//   worker 按因子解析) / open_libs; 预算 Budget → 分块 Plan (plan_chunks: 整期一块 或 尽量多统计日 + 前后带); Rc = 装载 / 评估的唯一结局.
//   之上: FactorsLoad (装载) → FactorsProbe (探针) → FactorsEngine (引擎 + 管线); 净值引擎 FactorsBook 吃装载层的 Y / B.
#pragma once

#include "features/Storage/DayBatchPlane.hpp"       // Day / LibReaders
#include "gui/task_alpha/services/FactorsTable.hpp" // FeatureTable / FactorScope
#include "shared/Analysis.hpp"                      // ReadScope

#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Alpha {

// ---- 评估域 (一个因子; 同文件各权重同叶 → 同一份) ----
// 设计决定: 每因子 (Run: 每文件) 现解析, 不做跨因子优化 (不合并评估域或森林; 跨因子只在 Store 的平面层命中), 勿改
struct Domain {
  PoolId pool = kAllPool;       // 因子池 (目录)
  LibMask used = 0;             // 所用库: 标签库 + 叶所在库
  analysis::ReadScope rs;       // main = 标签库, libs = used 各库清单, axis = A 轴 (标签库轴), to = A → 各库轴
  std::vector<misc::Ymd> dates; // 评估期 = used 各库落盘日交集的最近 keep 日 (升序, 非空)
  std::vector<Day> days;        // [dates] 当日 + 承接日 (标签库日期轴上的前一日, 0 = 轴首) 及其落盘位 (承接日按库判)
  std::string key() const;      // 缓存键: 池 | 所用库目录 + 池轴 | 评估期首末 + 天数
  // 列 c 所在库在评估域里 (盘口段成本只在评估域含 Lob 库时可用, 见 y_spec)
  bool uses(const Col &c) const { return used & lib_bit(lib_id(pool, c.tier)); }
};
// 作用域 sc 里已建的库全开 (清单没变且未判废的复用; 各因子只读自己用到的). 判废由读端置 stale, 装载逐天查 (FactorsLoad read_days)
void open_libs(const FactorScope &sc, LibReaders &lr);
// 池 pool 上叶 leaves 的所用库 = 标签库 ∪ 各叶所在库 (Col::ref): 评估期只随它变 (同所用库的因子同评估域)
LibMask used_libs(const FeatureTable &ft, PoolId pool, const std::vector<LeafId> &leaves);
// 作用域 sc + 叶 leaves → 评估域, 评估期只留最近 keep 日 (≥ 1).
// 所用库 = used_libs: 只用 bar.* 叶的因子不碰 Lob 库, 评估期 = Bar 库全期;
// 所用库未建 / 无共同落盘日 → false + why (正常输入)
bool domain_of(const FeatureTable &ft, const FactorScope &sc, const std::vector<LeafId> &leaves, size_t keep, Domain &dm, std::string &why);
// domain_of 的前半 (pool / used / rs.libs / dates / days; 不建 A 轴, 不读库): 只问评估期时用
bool span_of(const FeatureTable &ft, const FactorScope &sc, const std::vector<LeafId> &leaves, size_t keep, Domain &dm, std::string &why);

// 宿主内存预算: 总内存 × kHostFrac − 已用 (整机占用封顶 kHostFrac; 平面缓存另算回, 见 Store::budget); 显存预算: 空闲显存 × kVramFrac
inline constexpr double kHostFrac = 0.8;
inline constexpr double kVramFrac = 0.85;
double host_budget();
double vram_budget();

// ---- 计划: 预算 → 分块 (全部服务同一份口径) ----
// 一块 = 评估域日 [d0, d1), 前带 [d0, s0) 只给回看 / lag / 交接链, 统计日 [s0, s1), 后带 [s1, d1) 只给续持;
// 各块统计段首尾相接覆盖全期 (Search 契约【分块流式】)
struct Chunk {
  size_t d0 = 0, s0 = 0, s1 = 0, d1 = 0;
  int days() const { return static_cast<int>(d1 - d0); }
};
// 字节 / 格: load = 宿主块 (X + Y; 池篮子作业 = B 装载峰值); work = 引擎 + 探针 (同时只一块在用); dev = 显存 (> 0 = 也按显存限块).
// fixed = 与格无关的宿主常驻 (字节)
struct Budget {
  double load = 0.0, work = 0.0, fixed = 0.0, dev = 0.0;
};
struct Plan {
  std::vector<Chunk> chunks;
  int D = 0;            // 每块统计日上限
  double host = 0.0;    // 宿主预算 (字节, 已扣 fixed)
  double vram = 0.0;    // 显存预算 (字节; dev = 0 → 0)
  double cache = 0.0;   // 全部块的装载量 (字节; 连同最长一块的 work 放得下 → 各趟全中缓存)
  int max_days() const; // 最长一块的日数 (含前带)
};
// 整期一块的宿主装载量 (字节): 全部日 × (load + work) + fixed; 显存量: 全部日 × dev
double whole_bytes(const Domain &dm, int seg, const Budget &b);
double whole_dev_bytes(const Domain &dm, int seg, const Budget &b);
// 前后带日数 (FactorsEngine bands_of); front < 0 → 整期一块 (评估期已按宿主 / 显存预算截过: FactorsEngine fit_recent)
struct Bands {
  int front = -1, back = 0;
};
// 整期一块 或 块 = 尽量多的统计日 (≥ 1) + 前带 front 日 + 后带 back 日, 块数最少 (最小的块都装不下 → assert).
// host = 宿主预算 (字节; FactorsLoad Store::budget: 平面缓存全可让)
Plan plan_chunks(const Domain &dm, int seg, const Budget &b, Bands bd, double host);

// ---- 评估结局 (装载 / 评估 / 全部服务同一个) ----
enum class Rc : uint8_t { Ok,
                          Empty,  // 评估域不成 (所用库未建 / 无共同落盘日), why
                          Broken, // 特征叶值域越界 (全部根), why
                          Cancel,
                          Stale }; // 库判废 (FactorWorker::settle 弃平面缓存 + 置 kStaleMsg)
inline constexpr const char *kStaleMsg = "特征库判废 (池轴/字段表与当前不符), 需重算特征";
// 装载的 bool 结局 → Rc: 成 → Ok; 败 → 库判废 Stale, 否则 Cancel
inline Rc rc_of(bool ok, const LibReaders &lr) { return ok ? Rc::Ok : lr.stale() ? Rc::Stale
                                                                                 : Rc::Cancel; }
// 中断结局 (Cancel / Stale): 本轮作废
inline bool aborted(Rc rc) { return rc == Rc::Cancel || rc == Rc::Stale; }

} // namespace GUI::Alpha