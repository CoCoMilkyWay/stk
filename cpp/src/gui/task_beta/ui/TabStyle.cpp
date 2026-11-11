// Beta→Style: 左 = 参数网格条 + 状态 / 选池改动 + 累计中位夏普图 + 区间条, 右 = 风格表 (点开 = 跑该风格的网格, 其下列各组绩效, 点行选池) — 见头文件
#include "gui/task_beta/ui/TabStyle.hpp"

#include "disk/Disk.hpp"
#include "features/Storage/FeatureLib.hpp" // feature_pools (本进程池表) / kMaxPools
#include "gui/Tasks.hpp"                   // StatusColor
#include "gui/util/Chart.hpp"              // 色 / sheet_width / toggle_button / draw_end_tags / range_bar
#include "gui/util/Tip.hpp"
#include "imgui.h"
#include "implot.h"
#include "misc/date.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <initializer_list>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <span>
#include <string>
#include <vector>

namespace GUI::Beta {

namespace {

using pool::Spec;

constexpr ImVec4 kBenchCol(0.6f, 0.6f, 0.6f, 1.0f), kHotCol(1.0f, 1.0f, 1.0f, 1.0f), kNoneCol(0.5f, 0.5f, 0.5f, 1.0f);
constexpr ImVec4 kPairCol[2] = {{0.30f, 0.65f, 1.00f, 1.0f}, {1.00f, 0.55f, 0.15f, 1.0f}}; // 二元维的两色
constexpr float kIdleA = 0.4f;                                                             // 未选线的不透明度
constexpr float kFadeA = 0.12f;                                                            // 悬停网格取值时, 其余线的不透明度
constexpr float kCellA = 0.45f;                                                            // 名次底色的不透明度
constexpr float kNearPx = 8.0f;                                                            // 图上悬停: 离鼠标纵向这么近的线才算
constexpr float kTagScale = 0.85f;                                                         // 线尾小字字号
constexpr size_t kIdlePoints = 200000;                                                     // 未选线合计画点上限 (超了按等步长抽点; 有选池态 / 悬停的线画全点)
constexpr float kRangeH = 90.0f;                                                           // 区间条高 (同 Market 行情页)

constexpr const char *kStyleTip =
    "Style = 风格篮子的参数优选 (beta 收益): 选哪组参数做子池\n"
    "• 右表 = 风格 (一列排序键; 名后 = 落地 / 选中的池数), 点一行打开: 按左上参数网格逐组选池回测, 其下列各组 × 权重的绩效\n"
    "• 网格各维多选, 组 = 笛卡尔积: 端 (键低端 / 高端) · 只数 (全 = 切片内全选, 与风格无关) · 周期 (日 = 每日重选 / 周 = 按上周末重选, 权随价漂移) · 权重 (等权 / 总市值加权)\n"
    "  · 切片维 ST / 板块 / 融资 / 次新 / 停牌 / 行业 / 预警: 各按一列把在市 A 股分类, 全 = 不切; ST / 板块 / 融资 缺省全开, 其余缺省只勾全\n"
    "  · 切片 = 各切片维之交\n"
    "• 网格按钮: 按下色 = 勾, 灰字 = 不起作用的取值 (全; 只数只勾全时的端); 左键 = 翻转 (至少留一个), 右键 = 只留它 (已只留它 = 该维全开)\n"
    "• 名 = 风格端.只数.周期.ST.板块.融资.次新.停牌.行业.预警 (灰段 = 不起作用, 与网格按钮的灰字同一判据); 全程切片为空的组不列\n"
    "• 篮子 = 源行收盘按键选成员, 持有于其后交易日 (无前视); 收益 = 后复权收盘, 不看可成交; 全A = 行情页同一条 (当日活跃, 日频)\n"
    "• 滚动窗 = 以每日收尾的一年 (前段自建仓起缩短, 至少一季度): 年化 / 夏普 各取全部窗的平均与中位; 打分 = 中位夏普\n"
    "  平均 ≈ 全程, 一时的极端行情照样记满; 中位只看多数窗, 吃老本的参数压不上来\n"
    "• 点表行 / 图上的线 = 选中 ↔ 去掉 (选中的行底色亮; 名色 落地蓝 / 新选橙 / 去掉红; 有选池态的恒在表前); [确认…] 弹窗列出改动, 写 pools.json 即落地, 重启后计算 / 各页用新池表\n"
    "• 池表里的池名 = 简称 (高低端 + 风格 + 只数, 如 低β200; 全选池 = 切片名), 同简称按池表序加 A / B / C…; 悬停池名看全部参数\n"
    "• 主图 = 累计中位夏普: 每日 = 自首窗到该日各窗夏普的中位 (末日值 = 右表中位夏普), 任一日的截面 = 到该日为止的名次; 满一年的窗起才画\n"
    "• 底部区间条 (同行情页) 选回测区间: 右表 / 主图只算区间内 (起点 = 1), 只重算绩效\n"
    "• 线色 = 中位夏普名次 (绿好 → 红差); 悬停网格条某一维 = 按该维取值着色 (按钮字色即图例), 看哪个取值整体占优; 悬停到某个取值 = 只亮该取值的线\n"
    "• 细线 = 未选, 粗线 = 落地 / 新选 / 去掉, 灰 = 全A; 悬停图上 / 表行 = 白粗线 + 绩效";

bool has(const std::vector<Spec> &v, const Spec &p) { return std::ranges::find(v, p) != v.end(); }
// 选中 ↔ 去掉 (点表行 / 图上的线同一份)
void flip(StyleUIState &s, const Spec &p) {
  if (has(s.chosen, p))
    std::erase(s.chosen, p);
  else
    s.chosen.push_back(p);
  ++s.pick_gen;
}

// 池在选池表上的态
enum Mark : uint8_t { kMarkNone,  // 未选
                      kMarkGone,  // 落地, 已去掉
                      kMarkKept,  // 落地, 仍选
                      kMarkNew }; // 新选
Mark mark_of(const StyleUIState &s, const Spec &p) {
  const bool l = has(s.landed, p), c = has(s.chosen, p);
  return l && c ? kMarkKept : c ? kMarkNew
                          : l   ? kMarkGone
                                : kMarkNone;
}
ImVec4 mark_col(Mark m) {
  switch (m) {
  case kMarkKept:
    return kHead;
  case kMarkNew:
    return kStratCol;
  case kMarkGone:
    return StatusColor(TaskStatus::Kind::Error);
  default:
    return ImGui::GetStyleColorVec4(ImGuiCol_Text);
  }
}
constexpr const char *kMarkName[] = {"未选", "落地 → 去掉 (待确认)", "落地", "新选 (待确认)"};

// 风格 st 下的风格池数 / 全选池数
int count_of(const std::vector<Spec> &v, int st) {
  return static_cast<int>(std::ranges::count_if(v, [&](const Spec &p) { return p.keyed() && p.style() == st; }));
}
int count_all(const std::vector<Spec> &v) {
  return static_cast<int>(std::ranges::count_if(v, [](const Spec &p) { return !p.keyed(); }));
}

// 绩效列 [StyleCol]: 表头 / 悬停 / 格式 / 显示倍数 / 好的方向 (+1 = 高好)
enum StyleCol : int { kColAnn,
                      kColAnnMed,
                      kColSharpe,
                      kColSharpeMed,
                      kColMdd,
                      kColTurn,
                      kStyleCols };
struct ColDef {
  const char *name, *tip, *fmt;
  double scale;
  int better;
};
// 各列只算回测区间内; 滚动窗 = 以每日收尾的一年 (前段自起点缩短, 至少一季度); 年 = 基日 (窗起前一日) 到收尾日的日历跨度 / 365.25
constexpr ColDef kCols[kStyleCols] = {
    {"年化平均", "平均年化 = 各滚动窗年化收益的平均: 窗内 (收尾日净值 / 基日净值)^(1/年) − 1", "%+.1f%%", 100.0, 1},
    {"年化中位", "中位年化 = 各滚动窗年化收益的中位", "%+.1f%%", 100.0, 1},
    {"夏普平均", "平均夏普 = 各滚动窗夏普的平均: 窗内日收益 均值 / 标准差 × √每年交易日 (无风险 0)", "%.2f", 1.0, 1},
    {"夏普中位", "中位夏普 = 各滚动窗夏普的中位 (= 图上累计中位夏普的末日值)\n• 打分: 缺省按它降序; 线的缺省色 = 它的组内名次 (绿 = 好 → 红 = 差)", "%.2f", 1.0, 1},
    {"最大回撤", "最大回撤 = 区间内净值自前高 (含区间起点) 的最大跌幅", "%.1f%%", 100.0, -1},
    {"年换手", "年换手 = 区间内年化单边换手 (倍/年): 每次调仓 Σ (目标权 − 漂移后的权)⁺ 累计 / 年\n• 建仓 (在区间内) 计 1; 日频每日回到目标权, 周频两次调仓之间权随价漂移",
     "%.1f", 1.0, -1},
};
constexpr int kColBase = 1; // 表列 = 名 + 绩效列
double col_val(const StyleLine &l, int c) {
  const StyleStat &t = l.stat;
  const float v[kStyleCols] = {t.ann, t.ann_med, t.sharpe, t.sharpe_med, t.mdd, t.turn};
  return v[c];
}

// 网格维 = 池参数的维 pool::Dim (风格维由打开的风格定, 不上条) + 权重 (篮子的两条线, 不进池身份); 线在该维的取值 (−1 = 不起作用: 全选池的端)
constexpr int kDimWeight = pool::kDims;
const char *dim_name(int d) { return d == kDimWeight ? "权重" : pool::kDimDefs[d].name_cn; }
uint16_t dim_bits(const pool::Grid &g, int d) { return d == kDimWeight ? g.weight : g.bits[d]; }
void set_bits(pool::Grid &g, int d, uint16_t bits) {
  if (d == kDimWeight)
    g.weight = static_cast<uint8_t>(bits);
  else
    g.bits[d] = bits;
}
int dim_value(const StyleLine &l, int d) {
  if (d == kDimWeight)
    return l.weight;
  return d == pool::kDimEnd && !l.spec.keyed() ? -1 : l.spec.v[d];
}
// 维的取值 v 的色: 按网格勾选的取值排位分色 (两个 = 蓝 / 橙, 多个 = 色环等分); 不在网格 / 不起作用 = 灰
ImVec4 dim_col(uint16_t bits, int v) {
  if (v < 0 || !(bits >> v & 1))
    return kNoneCol;
  const int k = std::popcount(bits), pos = std::popcount(static_cast<unsigned>(bits & ((1u << v) - 1)));
  return k == 2 ? kPairCol[pos] : oklch(kRowL, kRowC, static_cast<float>(pos) / static_cast<float>(k));
}

// 横轴刻度: 日下标 → YY/MM/DD
int day_fmt(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<misc::Ymd> *>(ud);
  const long i = std::lround(v);
  if (i < 0 || i >= static_cast<long>(dates.size()))
    return std::snprintf(buf, static_cast<size_t>(size), " ");
  const misc::Ymd y = dates[static_cast<size_t>(i)];
  return std::snprintf(buf, static_cast<size_t>(size), "%02d/%02d/%02d", y / 10000 % 100, y / 100 % 100, y % 100);
}

// ---- 结果派生 (见 StyleUIState): 名 / 名次 / 画序随结果, 选池态随选中 ----
void derive(StyleUIState &s, const StyleResult &r) {
  const size_t n = r.lines.size();
  if (s.of_gen != r.gen) {
    s.of_gen = r.gen, s.of_pick = 0, s.of_sort = -2;
    s.names.resize(n);
    for (size_t i = 0; i < n; ++i)
      s.names[i] = pool::name_cn(r.lines[i].spec) + weight_suffix(r.lines[i].weight);
    s.ranks.assign(n * kStyleCols, std::numeric_limits<float>::quiet_NaN());
    std::vector<uint32_t> ok;
    for (int c = 0; c < kStyleCols; ++c) {
      ok.clear();
      for (size_t i = 0; i < n; ++i)
        if (std::isfinite(col_val(r.lines[i], c)))
          ok.push_back(static_cast<uint32_t>(i));
      std::ranges::sort(ok, [&](uint32_t a, uint32_t b) { return kCols[c].better * col_val(r.lines[a], c) > kCols[c].better * col_val(r.lines[b], c); });
      for (size_t k = 0; k < ok.size(); ++k)
        s.ranks[ok[k] * kStyleCols + c] = ok.size() > 1 ? static_cast<float>(k) / static_cast<float>(ok.size() - 1) : 0.5f;
    }
    s.paint.resize(n);
    std::iota(s.paint.begin(), s.paint.end(), 0u);
    const auto worse = [&](uint32_t i) { const float u = s.ranks[i * kStyleCols + kColSharpeMed]; return std::isnan(u) ? 2.0f : u; };
    std::ranges::stable_sort(s.paint, [&](uint32_t a, uint32_t b) { return worse(a) > worse(b); });
  }
  if (s.of_pick != s.pick_gen) {
    s.of_pick = s.pick_gen, s.of_sort = -2;
    s.marks.resize(n);
    for (size_t i = 0; i < n; ++i)
      s.marks[i] = mark_of(s, r.lines[i].spec);
  }
}
// 表序: 有选池态的在前, 各自按表头排序 (sort = 列 · 2 + 降序; −1 = 原序)
void sort_lines(StyleUIState &s, const StyleResult &r, int sort) {
  if (s.of_sort == sort)
    return;
  s.of_sort = sort;
  s.order.resize(r.lines.size());
  std::iota(s.order.begin(), s.order.end(), 0u);
  const int c = sort < 0 ? -1 : sort / 2 - kColBase;
  const bool desc = sort >= 0 && sort % 2;
  std::ranges::stable_sort(s.order, [&](uint32_t a, uint32_t b) {
    const bool ma = s.marks[a] != kMarkNone, mb = s.marks[b] != kMarkNone;
    if (ma != mb)
      return ma;
    if (sort < 0)
      return false;
    if (c < 0)
      return desc ? s.names[a] > s.names[b] : s.names[a] < s.names[b];
    const double x = col_val(r.lines[a], c), y = col_val(r.lines[b], c);
    if (!std::isfinite(x) || !std::isfinite(y))
      return std::isfinite(x) && !std::isfinite(y);
    return desc ? x > y : x < y;
  });
}

// 维 d 在网格 g 下不起作用的取值 (位; 与线名灰段同一判据 pool::on): 取值 全; 只数只勾 全 时端的全部取值
uint16_t idle_bits(const pool::Grid &g, int d) {
  if (d == kDimWeight)
    return 0;
  if (d == pool::kDimEnd && g.bits[pool::kDimN] == 1u << pool::kCountAll)
    return static_cast<uint16_t>((1u << pool::dim_size(d)) - 1);
  return pool::kDimDefs[d].all0 ? 1 : 0;
}

// 一维的多选条 (位 i = names[i], toggle_button 开 = 勾): 左键 = 翻转 (至少留一位), 右键 = 只留它 (已只留它 = 全开); tips 可空;
// 不起作用的取值字灰 (idle_bits); lit = 上帧悬停在这一维 (按钮字色 = 各取值的线色). 返回本帧悬停在这一维; val = 悬停的取值 (不在任一取值上则不动)
bool toggles(StyleUIState &s, int d, const char *const names[], const char *const tips[], int n, int &val) {
  uint16_t bits = dim_bits(s.grid, d);
  const uint16_t idle = idle_bits(s.grid, d), all = static_cast<uint16_t>((1u << n) - 1);
  const bool lit = s.dim == d;
  ImGui::PushID(d);
  ImGui::BeginGroup();
  ImGui::TextDisabled("%s", dim_name(d));
  for (int i = 0; i < n; ++i) {
    ImGui::SameLine(0, i ? 2.0f : -1.0f);
    const uint16_t b = static_cast<uint16_t>(1u << i);
    const bool grey = !lit && (idle & b);
    if (lit || grey)
      ImGui::PushStyleColor(ImGuiCol_Text, lit ? dim_col(bits, i) : ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
    ImGui::PushID(i);
    if (toggle_button(names[i], bits & b) && bits != b)
      bits ^= b;
    if (ImGui::IsItemClicked(ImGuiMouseButton_Right))
      bits = bits == b ? all : b;
    ImGui::PopID();
    if (lit || grey)
      ImGui::PopStyleColor();
    if (ImGui::IsItemHovered()) {
      val = i;
      if (tips)
        tip("%s", tips[i]);
    }
  }
  ImGui::EndGroup();
  ImGui::PopID();
  set_bits(s.grid, d, bits);
  return ImGui::IsItemHovered();
}

// 网格条: 端 / 只数 / 周期 / 权重 一行, ST / 板块 / 融资 一行, 次新 / 停牌 / 行业 / 预警 一行; 悬停的维 / 取值 → s.dim / s.val (本帧图起按它着色 / 提亮)
void grid_bar(StyleUIState &s) {
  int hov = -1, val = -1;
  const auto bar = [&](int d, const char *const names[], const char *const tips[], int n) {
    int v = -1;
    if (toggles(s, d, names, tips, n, v))
      hov = d, val = v;
  };
  // 维表的一维; 端名随打开的风格
  const auto dim = [&](int d) {
    const pool::DimDef &df = pool::kDimDefs[d];
    const char *names[8], *tips[8];
    assert(pool::dim_size(d) <= 8);
    for (int i = 0; i < pool::dim_size(d); ++i)
      names[i] = d == pool::kDimEnd && s.open >= 0 ? pool::kStyleDefs[s.open].end_cn[i] : df.values[i].name_cn, tips[i] = df.values[i].tip;
    bar(d, names, tips, pool::dim_size(d));
  };
  const auto line = [&](std::initializer_list<int> ds) {
    bool first = true;
    for (const int d : ds) {
      if (!first)
        ImGui::SameLine(0, 16);
      first = false;
      if (d == kDimWeight) {
        const char *weight[kWeights] = {"等权", "加权"}, *weight_tip[kWeights] = {"成分等权", "成分按源行收盘总市值加权"};
        bar(kDimWeight, weight, weight_tip, kWeights);
      } else
        dim(d);
    }
  };
  line({pool::kDimEnd, pool::kDimN, pool::kDimRebal, kDimWeight});
  line({pool::kDimSt, pool::kDimBoard, pool::kDimMargin});
  line({pool::kDimNew, pool::kDimSusp, pool::kDimInd, pool::kDimWarn});
  s.dim = hov, s.val = val;
}

// 状态: 进度 / 算不了的原因 / 结果跨度
void status(const StyleService &svc, const StyleUIState &s, const StyleResult *r, bool fresh) {
  const Phase ph = svc.phase.load(std::memory_order_acquire);
  const size_t done = svc.done.load(std::memory_order_relaxed), total = svc.total.load(std::memory_order_relaxed);
  if (s.open < 0)
    ImGui::TextDisabled("右表点开一个风格");
  else if (ph == Phase::Loading)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "读 all.bar %zu/%zu 日", done, total);
  else if (ph == Phase::Running)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "回测 %zu/%zu 组", done, total);
  else if (r && !r->lack.empty())
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s", r->lack.c_str());
  else if (r && fresh) {
    ImGui::TextDisabled("%s ~ %s  ·  %zu 线  ·  %zu 组空", misc::dash(r->dates.front()).c_str(), misc::dash(r->dates.back()).c_str(), r->lines.size(),
                        r->empty);
    if (ImGui::IsItemHovered())
      tip("组空 = 切片全程无资产 (如 退市整理 ∩ 科创板 ∩ 融资), 不列");
  } else
    ImGui::TextDisabled("计算中…");
  if (ph == Phase::Loading && ImGui::IsItemHovered())
    tip("首次读 all.bar 全部已落盘日的选池原料 (资产态 / 收益 / 市值) 常驻; 换风格只补读该风格的键并逐行排序, 改网格只算新出现的组");
}

// 改动: +新 / −去 [确认…] (弹窗列出改动, 写 pools.json) [撤销]
void changes(StyleUIState &s) {
  std::vector<Spec> add, del;
  for (const Spec &p : s.chosen)
    if (!has(s.landed, p))
      add.push_back(p);
  for (const Spec &p : s.landed)
    if (!has(s.chosen, p))
      del.push_back(p);
  constexpr const char *kModal = "确认选池##style_confirm";
  ImGui::SameLine(0, 16);
  if (add.empty() && del.empty()) {
    ImGui::TextDisabled("选池 %zu (= 落地)", s.landed.size());
    if (ImGui::IsItemHovered())
      tip("落地 = %s", disk::path(disk::PoolsFile).c_str());
    const std::span<const PoolDef> run = feature_pools();
    if (!std::ranges::equal(run.subspan(1), s.landed, {}, &PoolDef::spec)) {
      ImGui::SameLine();
      ImGui::TextColored(kStratCol, "重启生效");
      if (ImGui::IsItemHovered())
        tip("本进程的池表 (计算 / 各页) 启动时读一次, 落地的新池表重启后才用上");
    }
  } else {
    ImGui::TextColored(kStratCol, "+%zu", add.size());
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "−%zu", del.size());
    ImGui::SameLine();
    if (ImGui::SmallButton("确认…"))
      ImGui::OpenPopup(kModal);
    ImGui::SameLine();
    if (ImGui::SmallButton("撤销"))
      s.chosen = s.landed, ++s.pick_gen;
    if (ImGui::IsItemHovered())
      tip("选中回到落地");
  }
  if (!ImGui::BeginPopupModal(kModal, nullptr, ImGuiWindowFlags_AlwaysAutoResize))
    return;
  ImGui::Text("写入 %s: 池表 %zu → %zu (重启生效)", disk::path(disk::PoolsFile).c_str(), s.landed.size(), s.chosen.size());
  // 池名 = 新池表序的简称 (同简称加 A/B/C…); 去掉的按旧池表序
  const std::vector<std::string> now = pool::aliases(s.chosen), was = pool::aliases(s.landed);
  const auto alias = [](const std::vector<Spec> &t, const std::vector<std::string> &a, const Spec &p) { return a[static_cast<size_t>(std::ranges::find(t, p) - t.begin())]; };
  const float lh = ImGui::GetTextLineHeightWithSpacing();
  ImGui::BeginChild("##style_diff", ImVec2(0, std::min(static_cast<float>(add.size() + del.size()) * lh + lh, 24.0f * lh)), ImGuiChildFlags_Borders);
  for (const Spec &p : add) {
    ImGui::TextColored(kStratCol, "+ %s  %s", alias(s.chosen, now, p).c_str(), pool::code(p).c_str());
    if (ImGui::IsItemHovered())
      tip("%s", pool::detail_cn(p).c_str());
  }
  for (const Spec &p : del) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "− %s  %s", alias(s.landed, was, p).c_str(), pool::code(p).c_str());
    if (ImGui::IsItemHovered())
      tip("%s", pool::detail_cn(p).c_str());
  }
  ImGui::EndChild();
  const bool fits = s.chosen.size() + 1 <= kMaxPools;
  if (!fits)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "子池至多 %zu 个 (库号 = 池 × 档 是 8 位)", kMaxPools - 1);
  ImGui::BeginDisabled(!fits);
  if (ImGui::Button("写入", ImVec2(90, 0))) {
    pool::save_pools(s.chosen);
    s.landed = s.chosen, ++s.pick_gen;
    ImGui::CloseCurrentPopup();
  }
  ImGui::EndDisabled();
  ImGui::SameLine();
  if (ImGui::Button("取消", ImVec2(90, 0)))
    ImGui::CloseCurrentPopup();
  ImGui::EndPopup();
}

// 线名: 各段 "." 连, 不起作用的段灰, 其余 = col
void name_text(const StyleLine &l, const ImVec4 &col) {
  const ImVec4 grey = ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled);
  ImGui::BeginGroup();
  for (const pool::Part &p : pool::parts(l.spec)) {
    ImGui::TextColored(p.on ? col : grey, "%s", p.cn.c_str());
    ImGui::SameLine(0, 0);
    ImGui::TextColored(grey, ".");
    ImGui::SameLine(0, 0);
  }
  ImGui::TextColored(col, "%s", weight_suffix(l.weight) + 1);
  ImGui::EndGroup();
}

// 一条线的绩效提示
void line_tip(const StyleUIState &s, const StyleResult &r, size_t i) {
  const StyleLine &l = r.lines[i];
  std::string t = s.names[i] + "  " + pool::code(l.spec) + "  · " + kMarkName[s.marks[i]] + "\n简称: " + pool::short_cn(l.spec);
  for (int c = 0; c < kStyleCols; ++c) {
    char v[32];
    const double x = col_val(l, c);
    if (std::isfinite(x))
      std::snprintf(v, sizeof(v), kCols[c].fmt, kCols[c].scale * x);
    else
      std::snprintf(v, sizeof(v), "-");
    t += std::string("\n") + kCols[c].name + "  " + v;
  }
  tip("%s", t.c_str());
}

// 累计中位夏普图 (回测区间内, 各线自首个满一年的窗起): 全A 灰; 线色 = 中位夏普名次 (悬停网格维 = 该维取值色, 悬停到取值 = 只亮该取值的线),
// 未选细线 (合计点数超上限按步长抽点), 有选池态的粗线, 悬停白粗线; 点悬停的线 = 选中 ↔ 去掉; 返回图上悬停的线 (−1 = 无)
int plot(StyleUIState &s, const StyleResult &r, const ImVec2 &size) {
  const size_t E = r.d1, n = r.lines.size(); // E = 区间末日 + 1
  if (s.fit_gen != r.gen)
    ImPlot::SetNextAxesToFit();
  s.fit_gen = r.gen;
  if (!ImPlot::BeginPlot("##style_med", size, ImPlotFlags_NoTitle | ImPlotFlags_NoLegend | ImPlotFlags_NoMenus))
    return -1;
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_None, ImPlotAxisFlags_None);
  ImPlot::SetupAxisFormat(ImAxis_X1, day_fmt, const_cast<std::vector<misc::Ymd> *>(&r.dates));
  std::vector<EndTag> tags;
  char id[24];
  // 线 l 自首个满一年的窗 (full) 起到区间末日每 step 日一点; tag = 线尾小字 (nullptr = 不标)
  const auto draw = [&](const StyleLine &l, const ImVec4 &col, float w, size_t step, const std::string *tag) {
    if (l.full >= E)
      return;
    ImPlot::SetNextLineStyle(col, w);
    ImPlot::PlotLine(id, l.med.data() + l.full, static_cast<int>((E - 1 - l.full) / step + 1), static_cast<double>(step), static_cast<double>(l.full), 0, 0,
                     static_cast<int>(step * sizeof(float)));
    if (tag)
      tags.push_back({ImPlot::PlotToPixels(static_cast<double>(E - 1), l.med[E - 1]), ImGui::ColorConvertFloat4ToU32(col), *tag});
  };
  for (int w = 0; w < kWeights; ++w)
    if (r.grid.weight >> w & 1) {
      const std::string name = basket_name(0, static_cast<Weight>(w));
      std::snprintf(id, sizeof(id), "##bench%d", w);
      draw(r.bench[w], kBenchCol, 1.5f, 1, &name);
    }
  int near = -1;
  if (ImPlot::IsPlotHovered()) {
    const long d = std::lround(ImPlot::GetPlotMousePos().x);
    const float my = ImGui::GetMousePos().y;
    float best = kNearPx;
    if (d >= 0 && d < static_cast<long>(E))
      for (size_t i = 0; i < n; ++i) {
        const StyleLine &l = r.lines[i];
        if (static_cast<size_t>(d) < l.full)
          continue;
        if (const float dy = std::abs(ImPlot::PlotToPixels(static_cast<double>(d), l.med[static_cast<size_t>(d)]).y - my); dy < best)
          best = dy, near = static_cast<int>(i);
      }
  }
  const ImGuiIO &io = ImGui::GetIO(); // 松手且没拖过 = 点 (拖动归平移)
  if (near >= 0 && ImGui::IsMouseReleased(ImGuiMouseButton_Left) &&
      io.MouseDragMaxDistanceSqr[ImGuiMouseButton_Left] < io.MouseDragThreshold * io.MouseDragThreshold)
    flip(s, r.lines[static_cast<size_t>(near)].spec);
  const int hot = near >= 0 ? near : s.hot;
  const uint16_t bits = s.dim >= 0 ? dim_bits(r.grid, s.dim) : 0;
  const auto color = [&](size_t i, Mark m) {
    if (s.dim >= 0)
      return dim_col(bits, dim_value(r.lines[i], s.dim));
    if (m != kMarkNone)
      return mark_col(m);
    const float u = s.ranks[i * kStyleCols + kColSharpeMed];
    return std::isnan(u) ? kNoneCol : band_col(u, 1.0f);
  };
  size_t idle = 0;
  for (size_t i = 0; i < n; ++i)
    if (s.marks[i] == kMarkNone)
      idle += E - r.lines[i].full;
  const size_t step = std::max<size_t>(1, (idle + kIdlePoints - 1) / kIdlePoints);
  const int focus = s.dim >= 0 ? s.val : -1;
  const auto lit = [&](uint32_t i) { return focus >= 0 && dim_value(r.lines[i], s.dim) == focus; };
  // 层 = 悬停取值的线 · 2 + 有选池态 (自下而上): 悬停到取值时其余线淡出, 该取值的线不透明加粗
  for (const int layer : {0, 1, 2, 3})
    for (const uint32_t i : s.paint) {
      const Mark m = static_cast<Mark>(s.marks[i]);
      const bool marked = m != kMarkNone, up = lit(i);
      if ((up ? 2 : 0) + (marked ? 1 : 0) != layer || static_cast<int>(i) == hot)
        continue;
      ImVec4 c = color(i, m);
      if (focus >= 0 && !up)
        c.w = kFadeA;
      else if (!marked && !up)
        c.w = kIdleA;
      std::snprintf(id, sizeof(id), "##l%u", i);
      draw(r.lines[i], c, marked ? 2.5f : up ? 1.5f
                                             : 1.0f,
           marked ? 1 : step, marked ? &s.names[i] : nullptr);
    }
  if (hot >= 0 && hot < static_cast<int>(n)) {
    std::snprintf(id, sizeof(id), "##hot");
    draw(r.lines[static_cast<size_t>(hot)], kHotCol, 3.0f, 1, &s.names[static_cast<size_t>(hot)]);
  }
  draw_end_tags(tags, kTagScale, ImPlot::GetPlotPos().x + ImPlot::GetPlotSize().x);
  if (near >= 0)
    line_tip(s, r, static_cast<size_t>(near));
  ImPlot::EndPlot();
  return near;
}

// 区间条 (range_bar): 底线 = 全A 等权全程净值 (log); 选段 = 回测区间 → s.lo / s.hi (全段 = 0 不限)
void range(StyleUIState &s, const StyleResult &r, const ImVec2 &size) {
  const int D = static_cast<int>(r.dates.size());
  DayRange &rg = s.range;
  rg.d0 = static_cast<int>(std::ranges::lower_bound(r.dates, s.lo) - r.dates.begin());
  rg.d1 = s.hi ? static_cast<int>(std::ranges::upper_bound(r.dates, s.hi) - r.dates.begin()) : D;
  if (!ImPlot::BeginPlot("##style_range", size,
                         ImPlotFlags_NoTitle | ImPlotFlags_NoLegend | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoMouseText))
    return;
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_None, ImPlotAxisFlags_NoDecorations | ImPlotAxisFlags_AutoFit);
  ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, static_cast<double>(D), ImPlotCond_Always);
  ImPlot::SetupAxisFormat(ImAxis_X1, day_fmt, const_cast<std::vector<misc::Ymd> *>(&r.dates));
  ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
  ImPlot::SetNextLineStyle(kBenchCol, 1.0f);
  ImPlot::PlotLine("##style_range_nav", r.nav.data(), D);
  range_bar(rg, r.dates, 1.0);
  ImPlot::EndPlot();
  s.lo = rg.d0 == 0 ? 0 : r.dates[static_cast<size_t>(rg.d0)];
  s.hi = rg.d1 == D ? 0 : r.dates[static_cast<size_t>(rg.d1 - 1)];
}

// 右表: 风格行 (点 = 打开 / 收起) + 打开的风格之下 全A 两行 + 各线 (点行 = 选中 ↔ 去掉, 选中的行底色亮; 有选池态的在前; 底色 = 组内名次; 缺省中位夏普降序);
// 各列逐帧按可见行的内容定宽 (不拉满, 超宽横滚), 行高 = 字高; 只画可见行. 返回悬停的线 (−1 = 无)
int sheet(StyleUIState &s, const StyleResult *r, int plot_hot) {
  constexpr ImGuiTableFlags kFlags = ImGuiTableFlags_ScrollX | ImGuiTableFlags_ScrollY | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerV |
                                     ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate | ImGuiTableFlags_NoSavedSettings;
  ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(ImGui::GetStyle().CellPadding.x, 1.0f));
  const bool began = ImGui::BeginTable("##pf_style_tbl", kColBase + kStyleCols, kFlags);
  ImGui::PopStyleVar();
  if (!began)
    return -1;
  ImGui::TableSetupScrollFreeze(kColBase, 1);
  ImGui::TableSetupColumn("风格 / 参数");
  for (int c = 0; c < kStyleCols; ++c)
    ImGui::TableSetupColumn(kCols[c].name, c == kColSharpeMed ? ImGuiTableColumnFlags_DefaultSort | ImGuiTableColumnFlags_PreferSortDescending : 0);
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  for (int c = 0; c < kColBase + kStyleCols; ++c) {
    if (!ImGui::TableSetColumnIndex(c))
      continue;
    ImGui::TableHeader(c == 0 ? "风格 / 参数" : kCols[c - kColBase].name);
    if (ImGui::IsItemHovered())
      tip("%s", c == 0 ? "点风格行 = 打开 (跑参数网格) / 收起\n"
                         "点参数行 = 选为子池 ↔ 去掉 (图上点线同一份; 同一组参数的两种权重同一个池); 选中的行底色亮, 有选池态的线恒在前\n"
                         "名色: 蓝 = 落地, 橙 = 新选, 红 = 落地被去掉"
                       : kCols[c - kColBase].tip);
  }
  const bool open_ok = r && r->lack.empty();
  if (open_ok) {
    const ImGuiTableSortSpecs *ss = ImGui::TableGetSortSpecs();
    const int sort = ss && ss->SpecsCount > 0 ? ss->Specs[0].ColumnIndex * 2 + (ss->Specs[0].SortDirection == ImGuiSortDirection_Descending) : -1;
    sort_lines(s, *r, sort);
  }

  // 平铺的表行: 风格行 / 打开风格之下 (提示 | 全A 两行 + 各线) / 末行
  enum RowKind : uint8_t { kRowStyle,
                           kRowNote,
                           kRowBench,
                           kRowLine,
                           kRowFoot };
  struct Row {
    RowKind kind;
    uint32_t i;
  };
  std::vector<Row> rows;
  for (int st = 0; st < pool::kStyles; ++st) {
    rows.push_back({kRowStyle, static_cast<uint32_t>(st)});
    if (s.open != st)
      continue;
    if (!open_ok) {
      rows.push_back({kRowNote, 0});
      continue;
    }
    for (int w = 0; w < kWeights; ++w)
      if (r->grid.weight >> w & 1)
        rows.push_back({kRowBench, static_cast<uint32_t>(w)});
    for (const uint32_t i : s.order)
      rows.push_back({kRowLine, i});
  }
  rows.push_back({kRowFoot, 0});

  int hot = -1;
  const auto cells = [&](const StyleLine &l, const float *rk) {
    for (int c = 0; c < kStyleCols; ++c) {
      ImGui::TableSetColumnIndex(kColBase + c);
      const double x = col_val(l, c);
      if (rk && !std::isnan(rk[c]))
        ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::ColorConvertFloat4ToU32(band_col(rk[c], kCellA)));
      if (std::isfinite(x))
        ImGui::Text(kCols[c].fmt, kCols[c].scale * x);
      else
        ImGui::TextDisabled("-");
    }
  };
  const float rh = ImGui::GetTextLineHeight();
  constexpr ImGuiTreeNodeFlags kNode = ImGuiTreeNodeFlags_SpanAllColumns | ImGuiTreeNodeFlags_NoTreePushOnOpen;
  ImGuiListClipper clip;
  clip.Begin(static_cast<int>(rows.size()));
  while (clip.Step())
    for (int k = clip.DisplayStart; k < clip.DisplayEnd; ++k) {
      const Row &w = rows[static_cast<size_t>(k)];
      ImGui::TableNextRow(ImGuiTableRowFlags_None, rh);
      ImGui::PushID(k);
      ImGui::TableSetColumnIndex(0);
      switch (w.kind) {
      case kRowStyle: {
        const int st = static_cast<int>(w.i);
        const pool::StyleDef &sd = pool::kStyleDefs[st];
        char label[96];
        std::snprintf(label, sizeof(label), "%s  %s  (落地 %d · 选 %d)", sd.name_cn, sd.code, count_of(s.landed, st), count_of(s.chosen, st));
        ImGui::SetNextItemOpen(s.open == st, ImGuiCond_Always);
        ImGui::TreeNodeEx(label, kNode);
        if (ImGui::IsItemClicked())
          s.open = s.open == st ? -1 : st;
        if (ImGui::IsItemHovered())
          tip("%s: 键 = %s\n端: 低 = %s / 高 = %s", sd.name_cn, sd.key, sd.end_cn[0], sd.end_cn[1]);
        break;
      }
      case kRowNote:
        ImGui::Indent();
        ImGui::TextDisabled("%s", r ? r->lack.c_str() : "计算中…");
        ImGui::Unindent();
        break;
      case kRowBench:
        ImGui::Indent();
        ImGui::TextColored(kBenchCol, "%s", basket_name(0, static_cast<Weight>(w.i)).c_str());
        ImGui::Unindent();
        cells(r->bench[w.i], nullptr);
        break;
      case kRowLine: {
        const StyleLine &l = r->lines[w.i];
        if (static_cast<int>(w.i) == plot_hot)
          ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, ImGui::GetColorU32(ImGuiCol_HeaderHovered));
        // 整行可点 (先交, AllowOverlap 让名盖在上面); 名格悬停看绩效
        const ImVec2 at = ImGui::GetCursorPos();
        if (ImGui::Selectable("##pick", has(s.chosen, l.spec), ImGuiSelectableFlags_SpanAllColumns | ImGuiSelectableFlags_AllowOverlap, ImVec2(0, rh)))
          flip(s, l.spec);
        if (ImGui::IsItemHovered()) {
          hot = static_cast<int>(w.i);
          if (ImGui::TableGetHoveredColumn() == 0)
            line_tip(s, *r, w.i);
        }
        ImGui::SetCursorPos(at);
        ImGui::Indent();
        name_text(l, mark_col(static_cast<Mark>(s.marks[w.i])));
        ImGui::Unindent();
        cells(l, s.ranks.data() + static_cast<size_t>(w.i) * kStyleCols);
        break;
      }
      case kRowFoot:
        ImGui::TextDisabled("全选池  (落地 %d · 选 %d)", count_all(s.landed), count_all(s.chosen));
        if (ImGui::IsItemHovered())
          tip("全选池 = 只数 = 全, 不分风格: 切片内全选的池 (如 ST / *ST / 沪主板 / 融资); 任一风格的网格勾 只数 = 全 即列出");
        break;
      }
      ImGui::PopID();
    }
  ImGui::EndTable();
  return hot;
}

} // namespace

void SyncStylePools(StyleUIState &ui) {
  if (ui.loaded)
    return;
  ui.loaded = true;
  ui.landed = ui.chosen = pool::load_pools();
  ++ui.pick_gen;
}

void RenderTabStyle(StyleService &svc, StyleUIState &s) {
  const ImVec2 av = ImGui::GetContentRegionAvail();
  const float right = sheet_width(av.x), left = av.x - right - ImGui::GetStyle().ItemSpacing.x, h = av.y;
  if (s.open >= 0)
    s.grid.bits[pool::kDimStyle] = static_cast<uint16_t>(1u << s.open);
  if (s.open >= 0 && !(s.sent.gen && s.sent.grid == s.grid && s.sent.lo == s.lo && s.sent.hi == s.hi)) {
    s.sent = {s.sent.gen + 1, s.grid, s.lo, s.hi};
    svc.Request(s.sent);
  }
  std::shared_ptr<const StyleResult> r;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    r = svc.result;
  }
  const bool mine = s.open >= 0 && r && r->grid.bits[pool::kDimStyle] == s.grid.bits[pool::kDimStyle]; // 本风格的 (网格改了时新的还在算, 先显旧的)
  const StyleResult *show = mine && r->lack.empty() ? r.get() : nullptr;
  if (show) {
    derive(s, *show);
    if (s.hot >= static_cast<int>(show->lines.size()))
      s.hot = -1;
  }

  ImGui::BeginChild("##style_left", ImVec2(left, h));
  grid_bar(s);
  status(svc, s, mine ? r.get() : nullptr, mine && r->gen == s.sent.gen);
  changes(s);
  ImGui::SameLine(0, 16);
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered())
    tip("%s", kStyleTip);
  int plot_hot = -1;
  if (show) {
    derive(s, *show); // 改动 (撤销 / 写入) 可能刚变
    const ImVec2 pa = ImGui::GetContentRegionAvail();
    plot_hot = plot(s, *show, ImVec2(pa.x, pa.y - kRangeH - ImGui::GetStyle().ItemSpacing.y));
    range(s, *show, ImVec2(pa.x, kRangeH));
  }
  ImGui::EndChild();
  ImGui::SameLine();
  ImGui::BeginChild("##style_right", ImVec2(right, h), ImGuiChildFlags_Borders);
  const int row_hot = sheet(s, mine ? r.get() : nullptr, plot_hot);
  ImGui::EndChild();
  s.hot = row_hot;
}

} // namespace GUI::Beta