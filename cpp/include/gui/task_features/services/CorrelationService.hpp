// CorrelationService / CorrelationPairService / CorrelationLagService — 相关矩阵 / 单对 lead-lag / 全矩阵 lag 峭点
// 的单 worker 线程编排 (骨架见 StreamService.hpp; 口径与抽样见 gui/task_features/services/Correlation.hpp)
//
// 三个独立 service: 矩阵重 (F² × 样本数), 单对 lead-lag 轻 (只读两列, 悬停即算), lag 矩阵更重
// (只在勾选时算) —— 分开才互不打断. 三者同一个 Spec (CorrelationSpec), 同一个引擎.
//
// 矩阵请求: 列集 (UI 给的 ColRef: 跨池跨层选中 ≥ 2 列时 = 选中, 否则 = 当前过滤后的行集; 去 META, 升序去重)
//   网格 = 列集里最细的层 (粗层列 as-of 投影); 主库 = 列所在库里池轴最短者 (reader::read_request), 单对随矩阵主库.
// 触发: 进 Correlation tab / 列集变了 / 池 / 日期区间变了.
#pragma once

#include "gui/task_features/services/Correlation.hpp"
#include "gui/task_features/services/StreamService.hpp"

#include <vector>

struct SharedData;

namespace GUI::Features {

using CorrelationRequest = reader::Request<CorrelationSpec>;

// GUI 线程: 由列集 (任意序) 解析出矩阵请求 (去 META, 升序去重, 网格 = 最细层, 读取范围).
// 非 META 列不足 2 或区间为空 → false (不提交). Correlation / CorrelationLag 同一口径
bool MakeCorrelationRequest(SharedData &data, const std::vector<ColRef> &cols, CorrelationRequest &req);

class CorrelationService : public StreamService<CorrelationService, Correlation> {
public:
  // GUI 线程: 参数快照 + 取消在跑. cols = 矩阵列集 (任意序)
  void RequestCompute(SharedData &data, const std::vector<ColRef> &cols);

  static constexpr const char *kName = "CorrelationWorker";
};

// 全矩阵 lead-lag 峭点 (与矩阵同一列集; UI 勾选才提交)
class CorrelationLagService : public StreamService<CorrelationLagService, CorrelationLag> {
public:
  void RequestCompute(SharedData &data, const std::vector<ColRef> &cols);

  static constexpr const char *kName = "CorrelationLagWorker";
};

class CorrelationPairService : public StreamService<CorrelationPairService, CorrelationPair> {
public:
  // GUI 线程: 点热图格子触发 (a / b 必须不同; 主库 / 网格 = 矩阵的, 与矩阵同口径)
  void RequestCompute(SharedData &data, LibId main, size_t level, const ColRef &a, const ColRef &b);

  static constexpr const char *kName = "CorrelationPairWorker";
};

} // namespace GUI::Features
