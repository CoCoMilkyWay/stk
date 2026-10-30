#pragma once

// =============================================================================
// Search: 参数搜索评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 建在 Stat/Contract.hpp 之上)
// =============================================================================
//   定位  同一因子表达式的一批参数候选 (C 张因子平面, 同池 g / 同口径 Frame / 同标签集), 每候选每持有期直接攒二级累加器 Accum,
//         不落一级 Row. 语义 = Stat: 候选 c 的 acc[h].finish(hold, seg) 与 Stat 对同一平面算出的 HoldStat 逐字段一致 (整数派生量逐位,
//         浮点和只差求和序; 对拍 Search/Check.hpp). Stat 服务交互 (一因子一次, 要 Row), Search 服务搜索 (一因子上万次, 只要分数);
//         两者同一后端共用同一组行核 (Stat/Host.hpp / Stat/Dev.cuh), 只在"行怎么排 / 攒到哪"上分叉.
//         算子对持有期数 H 不设限 (hd.n ≤ kMaxHold), 每 h 各自打分 (见【目标】: 不跨 h 比), 因子平面只算一遍, 多 h 只多行统计;
//         Search 服务只传上层给定的一个持有期 (搜哪个周期由上层定).
//
//   【分块流式】超大张量沿 T 切块: 一块 = 行 [0, T) 的平面 (因子 / 标签 / 池 同块同形), 带三个量 (Span):
//         t0_abs  块首行的绝对下标 (tail_masked / hold_nets 看绝对行)
//         t_lo    统计首行; 前带 [0, t_lo) 只给 因子回看 / rAC 的 lag 行 (t − h) / 交接链回走 (Stat 契约【仓位感知】: 至多 kChainDays 日,
//                 换手成本的前一批再早 h 行)
//         t_back  后带行数; 统计行 [t_lo, T − t_back), 后带只给续持 (行 t + h 的批) 用
//         块内 t − h < 0 → ok_rac = 0, 链越块视为断链. 调用方保证 前带 ≥ 因子回看 + hold_reach + kChainDays 日、后带 ≥ hold_reach
//         (评估域首 / 尾块除外: 整张量在那里同样断链) 则分块 ≡ 整张量; 跨块 Result::merge 续攒 (Accum 可合并).
//         一块内候选之间互不相干, 后端怎么并行由后端定.
//
//   【目标】只评**排序能力**, 一次搜索针对一个固定持有期 h (调用方选, 限 Config kSearchHolds: search_hold), 不跨 h 比 (不同周期的每笔收益 /
//         执行方式不可公平折算), 不看成本 (h ≥ 30m 时换手 ≈ 100%, 建仓冲击对参数几乎是常数, 减常数不改变 argmax; 成本 / 执行在选出因子之后独立处理):
//         score = ls_period · ric.mean · mono² (按方向, 见【方向】);  ls_period / rIC 任一 ≤ 0 或 mono < kMinMono 或 n < 3 → −∞
//         · ls_period  每期真实毛多空 (Stat 契约【持仓口径】: 不触发行记 0, 不足满仓按只数 / cap 建仓; 固定 h 下年化系数是常数, 不乘).
//                      触发率 / 资金利用率已在持仓口径里定价, 不另设活跃度项 / 样本数 / 显著性硬阈值: 稀疏触发的候选 ls_period → 0,
//                      10 行里挑出来的噪声赢不了; 超满仓均分, 不奖励超出.
//         · rIC        正则项: 要求全截面排序也成立, 不只尾部
//         · mono       分层先于收益: kGroups 组收益对组号的 Spearman. 硬门槛 ≥ kMinMono (Config), 只认各组逐级单调的; 过门槛后再以 mono²
//                      进分值, 同等收益下分层更清的胜出. 曲面拟合 g (mono 取正部), 不过门槛的点只是当不了峰 / 终选
//         有意不进目标的: 成本 / 换手 / 显著性 (ls.t, ric.ir) / 样本外 —— 搜索要快.
//         同一份 score_of 两个入口: Search 吃 Accum (score_signed: 定向后 finish), 因子表吃 stat 里该 h 的 HoldStat —— Search 只在内部用分数挑候选,
//         不往文件写结论.
//   【方向】Search 自动定向 (score_signed): 方向 d = rIC 的号 (≥ 0 正向 kFwd, 否则反向 kRev; Stat 契约【方向 Dir】), sign = ±1.
//         Accum 按 d 定向 (oriented: rIC / LS 变号, 组序翻转) 后 finish, 再按所评方向打分.
//         定向后 ls > 0 且 mono 过门槛即可用, 否则 −∞. sign 落进 weights, Run / Inspect / Portfolio 按 sign 的方向评同一个表达式
//         (同一份 oriented), 因子表的 score_of 与这里逐位同值. 两个入口同一份 score_of.
//         定向目标 g = ls_period · rIC · max(mono, 0)² (定向后): 可用时 = score; rIC 过 0 两侧都 → 0 —— 处处有限, Search 的曲面拟合吃它.
//
//   【噪声】每个候选自带标准误, 不分折、不加参数: 由 HoldStat 的矩按持有期重叠折算有效样本 (n / h, h = hold_rows):
//         SE(rIC) = ric.sd / √(n/h);  SE(ls_period) = 补 0 序列 (n_all 行, 同 ann_sharpe 还原) 的 sd / √(n_all/h);
//         SE(g) = mono₊² · √((rIC·SE_ls)² + (ls·SE_rIC)²) (delta 法; mono 当常数). 忽略 ls / rIC 正相关与 h 之外的
//         自相关 ⇒ 偏小, 后果只是多算 / 高原窄 (保守).
//
//   【多权重】一个模板在一个 h 下不止一个好参数 (曲面的多个峰). 终选 pick_diverse, 只认统计量, 无比例 / 个数 / 相关上限常数:
//         可选 = 与最高者之差 ≤ 两者 SE 的合成 (统计上分不开); 按分降序贪心, 候选 j 的新增价值 = μ_j · (1 − max_{已选 i} ρ_ij),
//         > SE_j 才入选 (首个即要求 μ > SE: 不显著的 h 不出权重). 近似重复 (ρ → 1, 含镜像参数) 新增价值 → 0 自动出局, 分略低但不相关的反而能进.
//         ρ = 截面相关 (定向后) = 同 t 两候选 r16 行的 Pearson (与 rAC 同一行核 lag_row, lag 换成另一候选), 沿抽样行取均值 (kCorrRows 行够估).
//         每 h 独立做, picks 替换该模板 weights 里同 h 的项.
// =============================================================================

#include "factor/Search/Config.hpp"
#include "factor/Stat/Contract.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <vector>

namespace factor::search {

using factor::stat::Accum;
using factor::stat::Holds;
using factor::stat::HoldStat;
using factor::stat::kGroups;
using factor::stat::kMaxHold;

// 终选 (见【多权重】): mu[M] = 分 (曲面均值), se[M] = 标准误, corr[M][M] 定向后截面相关 (对称, 对角不看).
// 返回入选下标 (mu 降序; 可空 = 无显著候选)
inline std::vector<int> pick_diverse(const std::vector<double> &mu, const std::vector<double> &se, const std::vector<std::vector<double>> &corr) {
  const int M = static_cast<int>(mu.size());
  assert(static_cast<int>(se.size()) == M && static_cast<int>(corr.size()) == M);
  std::vector<int> out;
  if (M == 0)
    return out;
  std::vector<int> ord(static_cast<size_t>(M));
  for (int i = 0; i < M; ++i)
    ord[static_cast<size_t>(i)] = i;
  std::stable_sort(ord.begin(), ord.end(), [&](int a, int b) { return mu[a] > mu[b]; });
  const int b = ord[0];
  for (const int j : ord) {
    if (mu[b] - mu[j] > std::hypot(se[j], se[b]))
      break; // 降序: 之后的都分得开
    double rho = 0.0;
    for (const int i : out)
      rho = std::max(rho, corr[i][j]);
    if (mu[j] * (1.0 - rho) > se[j])
      out.push_back(j);
  }
  return out;
}

// 可搜的持有期档 (见【目标】): Config kSearchHolds
inline constexpr bool search_hold(int hold) {
  for (const int h : kSearchHolds)
    if (h == hold)
      return true;
  return false;
}

// 最长持有期的行数 h (rAC 的 lag / 续持前看 / 前一批的链; 分块前后带都按它, 【分块流式】; seg = 因子所在层)
inline int hold_reach(const Holds &hd, int seg) {
  int m = 0;
  for (int i = 0; i < hd.n; ++i)
    m = std::max(m, factor::stat::hold_rows(hd.k[i], geo_of(seg)));
  return m;
}

// 一个候选的全部输出: 每持有期一份 Accum. 跨块 merge
struct Result {
  Accum acc[kMaxHold];
  void merge(const Result &o, int H) {
    assert(H >= 1 && H <= kMaxHold);
    for (int i = 0; i < H; ++i)
      acc[i].merge(o.acc[i]);
  }
};

// ---- 目标 (主机, double; 见【目标】【方向】【噪声】) ----
struct Score {
  bool ok = false;                        // n ≥ 3 (假 → 其余字段 0, score −∞)
  double ls = 0.0, ric = 0.0, mono = 0.0; // 按 sign 定向后的 每期真实毛多空 (ls_period) / rIC 均值 / 分层单调性 (可用时 ls / ric > 0, mono ≥ kMinMono)
  double score = -std::numeric_limits<double>::infinity();
  int sign = 1;             // 所评方向 (+1 正向 / −1 取反; score_signed 定, 因子表的 HoldStat 已按权重的方向评过, 记 +1)
  double g = 0.0, se = 0.0; // 定向目标 ls·ric·mono₊² / 其标准误
};

// 已按所评方向 finish 的二级的打分 (见【目标】【噪声】; 因子表入口: stat 里该 h 的 HoldStat). sign = 所评方向 (只记录)
inline Score score_of(const HoldStat &hs, int sign = 1) {
  Score s;
  if (hs.n < 3)
    return s;
  s.ok = true;
  s.sign = sign;
  s.ls = factor::stat::ls_period(hs);
  s.ric = hs.ric.mean;
  s.mono = hs.mono;
  const double h = factor::stat::hold_rows(hs.hold, geo_of(hs.seg));
  const double nn = hs.n, N = hs.n_all, mu = hs.ls.mean, sd = hs.ls.sd;
  const double s1 = nn * mu, s2 = (nn - 1.0) * sd * sd + nn * mu * mu, M = s1 / N;
  const double se_ls = std::sqrt(std::fmax(s2 - N * M * M, 0.0) / (N - 1.0) / (N / h));
  const double se_ric = hs.ric.sd / std::sqrt(nn / h);
  const double m2 = std::fmax(s.mono, 0.0) * std::fmax(s.mono, 0.0);
  s.g = m2 * s.ls * s.ric;
  s.se = m2 * std::hypot(s.ric * se_ls, s.ls * se_ric);
  if (s.ls > 0.0 && s.ric > 0.0 && s.mono >= kMinMono)
    s.score = s.g;
  return s;
}

// Result 入口 (正向; 算子表对拍): hi = hd 下标; seg = 因子所在层
inline Score score_of(const Result &r, const Holds &hd, int seg, int hi) {
  factor::stat::assert_holds(hd, seg);
  assert(hi >= 0 && hi < hd.n);
  return score_of(r.acc[hi].finish(hd.k[hi], seg));
}

// 自动定向 (见【方向】): 方向 = rIC 的号; 正向攒的 a 定向后 finish (hold = 持有期键, seg = 因子所在层)
inline Score score_signed(const Accum &a, int hold, int seg) {
  const factor::stat::Dir d = a.head.ric.s1 >= 0.0 ? factor::stat::kFwd : factor::stat::kRev;
  return score_of(factor::stat::oriented(a, d).finish(hold, seg), d == factor::stat::kFwd ? 1 : -1);
}

} // namespace factor::search
