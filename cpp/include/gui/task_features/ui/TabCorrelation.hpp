// TabCorrelation — 特征两两相关矩阵热图 (口径 / 抽样 / 数值见 gui/task_features/services/Correlation.hpp)
//
// UI Layout:
//   1. 状态行: Compute | Cancel | Status (天进度) | 矩阵边长 / 空格数
//   2. 选项行: 色标 (带符号 ρ / |ρ|) | 阈值 (|ρ| < thr 按 0 画) | 排序 (特征表序 / 相关性聚类)
//              | Lag 图 (按 |ρ| 最大的 lag 着色, 0 中性, ±K 两端两色; CorrelationLagService 单独算)
//   3. 左: 下三角热图 (含对角; 上三角是镜像不画). hover 出对名 + ρ + 样本数, 并自动触发该对的
//         lead-lag 曲线 (换格子即换请求, 在跑的立刻止损)
//      右: 悬停对详情 + lead-lag 曲线 ρ(lag) (CorrelationPairService 单独跑, 不打断矩阵构建)
//
// 显示序与矩阵下标是两回事: 矩阵下标恒为 corr.spec.plan.cols 的升序位置, 显示序 (order) 只是一层
// 重排. 聚类序选中即自动算 (O(n³), n 几百卡一帧): 边长变了立刻重算, 构建中基于早期快照
// 先画, Done 后再按终版重算一次.
//
// Threading: UI 主线程, 渲染帧内持三个服务各自 target.mutex (矩阵 / 单对 / lag 矩阵)
#pragma once

#include "features/Storage/FeatureLevels.hpp" // ColRef

#include <cstdint>
#include <vector>

struct SharedData;

namespace GUI::Features {

class CorrelationService;
class CorrelationPairService;
class CorrelationLagService;
struct Preview;

struct CorrelationUIState {
  // ---- 显示选项 ----
  bool abs_mode = true;   // true = |ρ| 单极 (只看强度), false = 带符号双极 (看方向)
  float threshold = 0.0f; // |ρ| < threshold 的格子按 0 画 (压掉噪声, 留出块结构)
  int order_mode = 1;     // 0 = 特征表序 (与 Table 页对照), 1 = 相关性聚类 (块对角)
  bool lag_mode = false;  // true = 按 lag_best 着色 (CorrelationLag), false = 按 ρ 着色 (Correlation)

  // ---- 聚类重排缓存 (选聚类序时自动算) ----
  std::vector<int> cluster_order; // [n] 显示序 → 矩阵下标
  size_t cluster_n = 0;           // 算聚类时的矩阵边长 (与当前不符 = 已失效)
  uint64_t cluster_epoch = 0;     // 算聚类时的发布代 (构建中陈旧只提示, Done 后重算)

  // ---- 画图缓冲 (按显示序重排 + 阈值/绝对值处理后的 [n][n], 只填下三角) ----
  std::vector<float> disp;
  std::vector<int> disp_order; // 本帧实际使用的显示序 ([n], 恒非空)
  uint64_t disp_key = 0;       // 缓存键: epoch / 模式 / 阈值 / 序模式 / n / lag 图
  uint32_t n_blank = 0;        // 样本不足的格子数 (disp 里按 0 画, 悬停会说明)

  // ---- 悬停对 (矩阵下标; lead-lag 面板消费; 换对即提交 CorrelationPair 请求) ----
  int sel_i = -1, sel_j = -1;

  // ---- 请求变更检测 (列集 = 选中 ≥ 2 列 / 否则当前层过滤行集; stale = 强制重提): 矩阵与 lag 图各一份 ----
  std::vector<ColRef> req_cols;
  bool req_stale = true;
  std::vector<ColRef> lag_req_cols;
  bool lag_req_stale = true;
};

// Render tab (进 tab / 行集变更时自动提交计算请求)
void RenderTabCorrelation(CorrelationService *service, CorrelationPairService *pair_service, CorrelationLagService *lag_service,
                          SharedData &data, const Preview &pv, CorrelationUIState &ui);

// 切走 tab: 只中断在跑构建 (矩阵留在内存, 切回不重算)
void StopTabCorrelation(CorrelationService *service, CorrelationPairService *pair_service, CorrelationLagService *lag_service);

// 清掉请求快照 (池 / 区间 / 新库落盘 / 切回 tab): 下一帧按当前行集自发重新提交
void InvalidateCorrelationRequests(CorrelationUIState &ui);

} // namespace GUI::Features
