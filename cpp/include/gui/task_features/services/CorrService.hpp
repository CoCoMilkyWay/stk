// CorrService / CorrPairService / CorrLagService — 相关矩阵 / 单对 lead-lag / 全矩阵 lag 峭点
// 的单 worker 线程编排 (骨架见 StreamService.hpp; 口径与抽样见 shared/Corr.hpp)
//
// 三个独立 service: 矩阵重 (F² × 样本数), 单对 lead-lag 轻 (只读两列, 悬停即算), lag 矩阵更重
// (只在勾选时算) —— 分开才互不打断. 三者同一个 Spec (CorrSpec), 同一个引擎.
//
// 矩阵请求: 列集 (UI 给的 ColRef: 跨库跨层选中 ≥ 2 列时 = 选中, 否则 = 当前过滤后的行集; 去 META, 升序去重)
//   网格 = 列集里最细的层 (粗层列 as-of 投影); 主库 = 列所在库里库轴最短者 (analysis::read_request), 单对随矩阵主库.
// 触发: 进 Corr tab / 列集变了 / 库 / 日期区间变了 / Compute 落了新库.
#pragma once

#include "gui/task_features/services/StreamService.hpp"
#include "shared/Corr.hpp"

#include <vector>

namespace GUI::Features {

using CorrRequest = analysis::Request<CorrSpec>;

// GUI 线程: 由列集 (任意序) 解析出矩阵请求 (去 META, 升序去重, 网格 = 最细层, 读取范围).
// 非 META 列不足 2 或区间为空 → false (不提交). Corr / CorrLag 同一口径
bool MakeCorrRequest(SharedData &data, const std::vector<ColRef> &cols, CorrRequest &req);

class CorrService : public StreamService<CorrService, Corr> {
public:
  // GUI 线程: 参数快照 + 取消在跑. cols = 矩阵列集 (任意序)
  void RequestCompute(SharedData &data, const std::vector<ColRef> &cols);

  static constexpr const char *kName = "CorrWorker";
  static Corr &target(SharedData &data);
};

// 全矩阵 lead-lag 峭点 (与矩阵同一列集; UI 勾选才提交)
class CorrLagService : public StreamService<CorrLagService, CorrLag> {
public:
  void RequestCompute(SharedData &data, const std::vector<ColRef> &cols);

  static constexpr const char *kName = "CorrLagWorker";
  static CorrLag &target(SharedData &data);
};

class CorrPairService : public StreamService<CorrPairService, CorrPair> {
public:
  // GUI 线程: 点热图格子触发 (a / b 必须不同; 主库 / 网格 = 矩阵的, 与矩阵同口径)
  void RequestCompute(SharedData &data, LibId main, size_t level, const ColRef &a, const ColRef &b);

  static constexpr const char *kName = "CorrPairWorker";
  static CorrPair &target(SharedData &data);
};

} // namespace GUI::Features
