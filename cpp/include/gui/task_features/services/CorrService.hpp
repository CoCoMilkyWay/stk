// CorrService / CorrPairService / CorrLagService — 相关矩阵 / 单对 lead-lag / 全矩阵 lag 峭点
// 的单 worker 线程编排 (骨架见 StreamService.hpp; 口径与抽样见 shared/Correlation.hpp)
//
// 三个独立 service: 矩阵重 (F² × 样本数), 单对 lead-lag 轻 (只读两列, 悬停即算), lag 矩阵更重
// (只在勾选时算) —— 分开才互不打断.
//
// CorrRequest: 层 (跟随 UI 的 L0/L1) + 当前过滤后的行集 (去 META, 升序去重) + _meta 门控列.
// 触发: 进 Corr tab / 过滤行集变了 / 层变了 / universe / 日期区间变了 / Compute 落了新库.
#pragma once

#include "gui/task_features/services/StreamService.hpp"
#include "shared/Correlation.hpp"

#include <vector>

namespace GUI::Features {

struct CorrRequest {
  analysis::ReadScope scope;
  size_t level = 0;
  std::vector<uint32_t> cols;             // 矩阵列 (metadata 下标, 升序去重)
  std::vector<L2::ValidType> valid_types; // 与 cols 平行
  size_t meta_col = 0;
};

// GUI 线程: 由特征表过滤行集 (任意序) 解析出矩阵请求 (去 META, 升序去重, 门控列, 读取范围).
// 非 META 列不足 2 (L0 现状) 或区间为空 → false (不提交). Correlation / CorrLag 同一口径
bool MakeCorrRequest(SharedData &data, const std::vector<int> &rows, CorrRequest &req);

class CorrService : public StreamService<CorrService, CorrRequest> {
public:
  // GUI 线程: 参数快照 + 取消在跑. rows = 特征表当前过滤后的 metadata 下标 (任意序)
  void RequestCompute(SharedData &data, const std::vector<int> &rows);

  static constexpr const char *kWorkerName = "CorrWorker";
  static Correlation &target(SharedData &data);
  void reset(Correlation &corr, CorrRequest &req);
};

// 全矩阵 lead-lag 峭点 (与矩阵同一列集; 重 ≈ 矩阵 2~3 倍, UI 勾选才提交)
class CorrLagService : public StreamService<CorrLagService, CorrRequest> {
public:
  void RequestCompute(SharedData &data, const std::vector<int> &rows);

  static constexpr const char *kWorkerName = "CorrLagWorker";
  static CorrLag &target(SharedData &data);
  void reset(CorrLag &lag, CorrRequest &req);
};

struct CorrPairRequest {
  analysis::ReadScope scope;
  size_t level = 0;
  uint32_t col_a = 0, col_b = 0;
  L2::ValidType vt_a = L2::ValidType::ALL, vt_b = L2::ValidType::ALL;
  size_t meta_col = 0;
};

class CorrPairService : public StreamService<CorrPairService, CorrPairRequest> {
public:
  // GUI 线程: 点热图格子触发 (col_a / col_b = metadata 下标, 必须不同)
  void RequestCompute(SharedData &data, uint32_t col_a, uint32_t col_b);

  static constexpr const char *kWorkerName = "CorrPairWorker";
  static CorrPair &target(SharedData &data);
  void reset(CorrPair &pair, CorrPairRequest &req);
};

} // namespace GUI::Features
