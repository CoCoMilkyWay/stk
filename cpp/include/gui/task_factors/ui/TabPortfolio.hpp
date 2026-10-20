// Factors→Portfolio: 因子组合页 + Factors 页 1. 区右侧的两个组合小框.
//   小框 1: 选组合 (<portfolio_dir>/<name>.json) / New / Rename / Del;  小框 2: 当前组合的条目 "(池, 持有期): 名", Add = 加入 Factors 表里高光的
//   权重 (持有期 = Factors 页 Hold), Del = 删选中条目; 失效条目 (因子文件里找不到该规范串等) 红. 组合选择在内存, 内容即文件.
//   Portfolio 页 (对仗 OrderFlow 图 2 的横轴: x = 日下标 · 255 + 行末分钟, 刻度只到日 YY/MM/DD; 不画个股):
//     组合策略线 (有冲击 / 无冲击两条, 图例 = 组合名 | 冲击口径 | 区间年化, 与池线同格式列对齐) = 各因子独立回测 (PortfolioService FactorCurve, 各自的层) 的逐行增量等权聚合: 每个交易日在当日有数据的 n 个因子间等分,
//     因子增量 = (行收益 − 行成本) / h (h = hold_rows, 重叠持有期折算), 记在该行平仓时刻 (分钟型 = 行末分钟 + n, 收盘型 = 当日收盘, T+N = 第 N 个交易日开盘;
//       已实现口径, 与池指数对齐; 等分仍按建仓日); 净值 = 同一 x 的增量和逐点复利连乘.
//       只算 G20 多头 = G20 做多绝对收益 − 多腿成本 (无冲击 = 不扣, 细线; 默认只显有冲击). 成本 = 各因子自己的最小冲击段, 不可选:
//       Lob 层 = 最小金额段建仓冲击 + Config::fixed_impact; Bar 层无金额段 = 建仓 / 平仓都按 fixed_impact
//     池指数线 (日层, 每池一条等权; all 池两条 = 全A加权 (流通市值) / 全A等权; 全部在主图例): 只画因子日并集轴范围内, 当日收盘一点; 默认只显组合里因子所在的池;
//       图例带年换手 (全评估期日均单边 PoolCurve::turnover[该线权重] × kDaysPerYear, 倍; 指数线本身不扣成本)
//     单因子线 (右侧图例, 默认全关): 该因子单独的 G20 多头净值 (带它自己的冲击)
//     y = 净值 (线性下限钉 0; 可切 log, log 下正常缩放); 底部区间条选日段 [d0, d1) (两边竖把手 / 段内平移, 拖动中吸附到月初即生效, 把手旁标起止日), 各线在区间起点归一 (起点 = 1), 年化按区间复利折算;
//     主图例 / 右图例 / log / 区间 任一变 → 重新 fit; 每条可见线在视野内的末点右侧标小字名
//   页: NAV = 上面的资金曲线; VAL = 池估值 (同一布局, 区间 / log 共用): 右图例单选一个池或申万一级行业 (全A 内按上一行 ind_l1 切;
//     默认 = 组合首个因子所在池; 各项标区间末 PE / PB / DY), 主图 = 该池 / 行业流通市值加权指数 P (区间首点 = 1) + 滚动 PE / PB / DY Band (主图例开关,
//     默认只开 PE): 价格 band y_t = P_t · q_j(t) / m_t, m = 价格倍数 (PE / PB / 1/DY), q_j(t) = 过去 kBandYears 年窗内 m 的 kBandQs 分位 (ValCurve, Service 算);
//     band 间填色 (最便宜档以下绿 → 最贵档以上红, 低 alpha 渐变), 指数线在上; 不可算的点断开
#pragma once

#include "gui/task_factors/services/PortfolioService.hpp"

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Factors {

// 一条净值线 (x 升序, y = 净值, 起点 1)
struct PortfolioCurve {
  std::vector<double> x, y;
};

// Portfolio 页派生 (UI 线程, 结果 / 内容 / fixed_impact 变才重算): 全段, 起点 1
struct PortfolioDerived {
  std::vector<misc::Ymd> dates;   // 日并集轴 (横轴 day 下标)
  PortfolioCurve strat[kImpacts]; // 组合策略 (G20 多头): 起点 + 每个有增量的 x (各线同 x)
  float w_en = 0.f, w_cn = 0.f;   // 主图例前两列宽 (px; 池线与策略线共用, 列对齐)
  struct Pool {
    std::string label; // 图例: 池名 | 中文名 | 年换手 (补空格对齐; ###池名 定 ID, 换手到了不换 item)
    std::string tag;   // 线尾小字 (池名)
    bool own = false;  // 组合里有因子在这个池
    PortfolioCurve c;  // 收盘点
  };
  std::vector<Pool> pools; // [feature_pools 序]
  struct Factor {
    std::string key;   // PortfolioEntry::key (右图例显隐键)
    std::string label; // 右图例 (条目名 + 冲击口径)
    std::string tag;   // 线尾小字 (因子名 + 持有期)
    PortfolioCurve c;  // 该因子单独的 G20 多头 (有冲击), 每行一点
  };
  std::vector<Factor> factors;  // [items 序, 只含算成的]
  int n_ready = 0, n_error = 0; // 已算成 / 算失败的因子数
  struct Val {
    std::string name;        // 单选键: 池名 / "sw<ID>" (行业)
    std::string label;       // VAL 右图例: 池名 | 中文名 / sw<ID> | 行业名 (补空格对齐)
    std::string tag;         // 线尾小字 / 价格线图例: 池名 / 行业名
    PortfolioCurve p;        // 流通市值加权指数, 轴上每日收盘一点 (不归一; 视图在区间首点归一)
    ValCurve val[kPoolVals]; // [PoolVal] 同点 (p.x) 切出的 PoolCurve 估值 + 滚动分位
  };
  std::vector<Val> vals;  // [feature_pools 序, 只含算成的池] 后接 [SW2021 一级 1..31] (全A 内切, all 池算成才有)
  size_t n_val_pools = 0; // vals 前 n 个是池
};

// VAL 页选中池的一口 band (PoolVal): 滚动分位的价格线
struct PortfolioBand {
  bool ok = false;                       // 区间内有可画点 (否则其余空)
  PortfolioCurve line[kBandQ];           // y = P · q_j / m (m = 当日价格倍数; x = 价格线的 x; 不可画 = NaN), 升序 = 便宜 → 贵
  std::vector<std::pair<int, int>> segs; // line 全有限的连续段 [i0, i1) (填色按段画: PlotShaded 不跳 NaN)
  double now = 0.0, pct = 0.0;           // 区间末个可画点的值 (PE / PB / DY) / 估值分位 (%, 高 = 贵)
  double q[kBandQ] = {};                 // 同点各档的值 (与 now 同单位)
};

// 区间视图: 派生各线切到 [d0, d1) 并在起点归一 (派生 / 区间变才重算)
struct PortfolioView {
  PortfolioCurve strat[kImpacts];
  float ann[kImpacts] = {};                   // 区间复利年化 (%)
  std::vector<PortfolioCurve> pools, factors; // [der.pools] / [der.factors]
  std::vector<float> ann_f;                   // [der.factors]
  PortfolioCurve val;                         // VAL 选中池的价格线 (区间首点 = 1; 未选 → 空)
  PortfolioBand band[kPoolVals];              // [PoolVal] 选中池的
  std::vector<double> val_now[kPoolVals];     // [PoolVal][der.vals] 区间末个可算点的 PE / PB / DY (NaN = 无)
};

struct PortfolioUIState {
  // ---- 组合 (小框) ----
  std::string dir;                     // names 对应的目录 (config.portfolio_dir 变 → 重列)
  std::vector<std::string> names;      // 目录下的组合 (主干名)
  std::string name;                    // 当前组合; 空 = 无
  std::vector<PortfolioEntry> entries; // 当前组合内容 (= 文件)
  std::vector<PortfolioItem> items;    // 解析结果 (TaskFactors 在内容变 / 因子重扫后重解析; 与 entries 同序, 可能落后一帧)
  uint64_t content_gen = 0;            // 换组合 / 改内容 +1
  uint64_t items_gen = 0;              // items 每改一次 +1 (解析 / 建请求时标失效)
  int sel = -1;                        // 小框 2 选中条目
  char name_buf[64] = "";              // New / Rename 的名字
  std::string msg;                     // 上次操作结果
  bool confirm_delete = false;
  // ---- Portfolio 页 ----
  uint64_t der_gen = ~0ull, der_items = ~0ull;
  double der_impact = -1.0;
  PortfolioDerived der;
  int d0 = 0, d1 = 0;                            // 区间 [d0, d1) (der.dates 下标; 两边在月初 / 末尾); 日轴变 → 全段
  int drag = 0;                                  // 区间条拖动中: 0 无 / 1 左把手 / 2 右把手 / 3 段内平移
  double grab_day = 0.0;                         // 按下时鼠标所在日 (平移按与它的差算)
  int grab_k0 = 0, grab_k1 = 0;                  // 按下时两边的月初序号 (平移保持 k1 − k0)
  int view_d0 = -1, view_d1 = -1, view_val = -2; // view 算时的区间 / VAL 选中池 (der.vals 下标)
  PortfolioView view;
  std::string val_pool; // VAL 选中池 (空 = 默认: 组合首个因子所在池, 未算出前暂用首个池)
  std::string val_for;  // val_pool 对应的组合 (换组合 → 回默认)
  bool log_y = false;
  std::map<std::string, bool> factor_show; // 右图例: PortfolioEntry::key → 显示 (缺 = 关)
  std::string vis_key;                     // 池线默认显隐已按它设过 (组合 + 其因子所在池; 变了重设)
  std::string shown_key;                   // 上帧各线显隐 + log (变了 → 下帧重新 fit)
  bool refit = false;
};

// GUI 线程每帧 (两页都调): 目录变 → 重列, 当前组合不在了 → 选第一个 (载入内容, content_gen +1)
void SyncPortfolioDir(PortfolioUIState &ui, const std::string &dir);
// Factors 页 1. 区右侧两框. view_row = Factors 表高光行 (nullptr = 无); (pool, seg, hold) = Factors 页当前池 / 层 / Hold
void RenderPortfolioBoxes(PortfolioUIState &ui, const FactorRow *view_row, const std::string &pool, int seg, int hold);
// 返回值: 1 = Recompute (弃缓存重算), -1 = Cancel, 0 = 无
int RenderTabPortfolio(PortfolioService &svc, PortfolioUIState &ui, double fixed_impact);

} // namespace GUI::Factors
