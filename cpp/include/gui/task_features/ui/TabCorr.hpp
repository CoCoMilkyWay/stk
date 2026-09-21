// TabCorr — 特征两两相关矩阵热图 (口径 / 抽样 / 数值见 shared/Correlation.hpp)
//
// UI Layout:
//   1. 状态行: Compute | Cancel | Status (天进度) | 矩阵边长 / 空格数
//   2. 选项行: 色标 (带符号 ρ / |ρ|) | 阈值 (|ρ| < thr 按 0 画) | 排序 (特征表序 / 相关性聚类)
//              | Lag 图 (按 |ρ| 最大的 lag 着色, 0 中性, ±K 两端两色; CorrLagService 单独算)
//   3. 左: 下三角热图 (含对角; 上三角是镜像不画). hover 出对名 + ρ + 样本数, 并自动触发该对的
//         lead-lag 曲线 (换格子即换请求, 在跑的立刻止损)
//      右: 悬停对详情 + lead-lag 曲线 ρ(lag) (CorrPairService 单独跑, 不打断矩阵构建)
//
// 显示序与矩阵下标是两回事: 矩阵下标恒为 corr.cols 的升序位置, 显示序 (order) 只是一层
// 重排. 聚类序选中即自动算 (O(n³), n 几百卡一帧): 边长变了立刻重算, 构建中基于早期快照
// 先画, Done 后再按终版重算一次.
//
// Threading: UI 主线程, 渲染帧内持 corr.mutex / corr_pair.mutex / corr_lag.mutex
#pragma once

#include <cstdint>
#include <vector>

struct SharedData;

namespace GUI::Features {

class CorrService;
class CorrPairService;
class CorrLagService;

struct CorrUIState {
  // ---- 显示选项 ----
  bool abs_mode = false;  // true = |ρ| 单极 (只看强度), false = 带符号双极 (看方向)
  float threshold = 0.0f; // |ρ| < threshold 的格子按 0 画 (压掉噪声, 留出块结构)
  int order_mode = 0;     // 0 = 特征表序 (与 Feature 表对照), 1 = 相关性聚类 (块对角)
  bool lag_mode = false;  // true = 按 lag_best 着色 (CorrLag), false = 按 ρ 着色 (Correlation)

  // ---- 聚类重排缓存 (选聚类序时自动算) ----
  std::vector<int> cluster_order; // [n] 显示序 → 矩阵下标
  size_t cluster_n = 0;           // 算聚类时的矩阵边长 (与当前不符 = 已失效)
  uint64_t cluster_epoch = 0;     // 算聚类时的发布代 (构建中陈旧只提示, Done 后重算)

  // ---- 画图缓冲 (按显示序重排 + 阈值/绝对值处理后的 [n][n], 只填下三角) ----
  std::vector<float> disp;
  std::vector<int> disp_order; // 本帧实际使用的显示序 ([n], 恒非空)
  uint64_t disp_key = 0;       // 缓存键: epoch / 模式 / 阈值 / 序模式 / n / lag 图
  uint32_t n_blank = 0;        // 样本不足的格子数 (disp 里按 0 画, 悬停会说明)

  // ---- 悬停对 (矩阵下标; lead-lag 面板消费; 换对即提交 CorrPair 请求) ----
  int sel_i = -1, sel_j = -1;

  // ---- 请求变更检测 (过滤行集 / 层): 矩阵与 lag 图各一份 ----
  std::vector<int> req_rows;
  int req_level = -1;
  std::vector<int> lag_req_rows;
  int lag_req_level = -1;
};

// Render tab (进 tab / 行集变更时自动提交计算请求)
void RenderTabCorr(CorrService *service, CorrPairService *pair_service,
                   CorrLagService *lag_service, SharedData &data, CorrUIState &ui);

// 切走 tab: 只中断在跑构建 (矩阵留在内存, 切回不重算)
void StopTabCorr(CorrService *service, CorrPairService *pair_service, CorrLagService *lag_service,
                 SharedData &data);

// 清掉请求快照 (universe / 区间 / 新库落盘 / 切回 tab): 下一帧按当前行集自发重新提交
void InvalidateCorrRequests(CorrUIState &ui);

} // namespace GUI::Features
