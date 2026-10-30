#pragma once

// =============================================================================
// Bridge — agent 与项目的唯一接口面
// =============================================================================
//   agent/ 其余代码不碰项目的因子 / 特征 / 服务头, 只经这里 (实现 Bridge.cpp 是唯一 include 它们的 TU). 两半:
//     catalog  作用域目录 (无状态): 可挖的 (池, 因子层, 持有期; 持有期限 Search 档) / 就绪判据 / 会话目录
//     Lab      作用域工作区 (bind 一个作用域): 上下文 ctx (该层可作输入的特征叶 / 算子表 / K 线叶 / 正式因子库在该持有期的统计) + 候选目录 results.
//              两件事:
//              eval  候选的一生: Cand ─stage→ 候选目录里的因子文件 (与正式因子同格式) ─eval→ Result ─adopt (过门槛)→ 正式因子目录;
//                    逐文件 Search (定方向 + 搜自由参数; 具体式只评本身) → Run, 结果写回文件; 评完一个只重扫它 (results 增量维护)
//              look  观察: 一组因子 (候选英文名 / 具体根式) + 一组特征式 → 评估期上整期一块的原料 Look (秩 / 行统计 / 两端持仓 / 特征原值 /
//                    门控 / 标签), 不写文件; 汇总成文在 agent/Views
//              可见: 只有统计与 Search 结局都在作用域此刻评估期上的因子才给 agent 看 (Result::visible), 不同窗口的数不可比
//              三道 (Search / Run / Look) 同构, 首次用到才建; 作用域与评估后端不变的连续会话共用其常驻块 (keep / Life::Worker), 变了即弃
//   worker 协议 (与 agent/Chat.hpp 对仗): 交一件事 (eval / look ↔ ask) → poll 逐个取走 Tick, 末个 = End → live 看在跑那步 → busy / cancel
//   全部 GUI 线程调用; 正式因子目录 / 字段表 / 资产轴由项目定, agent 只给候选目录
// =============================================================================

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

struct SharedData;

namespace agent {

struct Scope {
  std::string pool;
  int level = -1; // 因子层下标
  int hold = 0;   // 持有期键
  bool operator==(const Scope &) const = default;
};

struct Choice {
  int key = 0;
  std::string name;
};

// ---- 上下文 ----
struct Leaf {
  std::string code, name_cn, formula, desc; // code = 档.层.列 (模板里的写法)
};
struct Op {
  std::string name, name_cn, sig, out, tex, note; // sig = 元与参数的值域 (LaTeX); out = 输出值域名
};
// 定向后的一组统计 (Weight / Top 共有)
struct Stat {
  double mono = 0, ls = 0, ric = 0; // 分层单调性 / 每期毛多空 / rIC 均值
};
// 一个权重在作用域持有期下的统计 (has = Run 过且该持有期 n ≥ 3)
struct Weight {
  std::string expr;
  int sign = 1;     // 方向 (−1 = 取反后才是这组统计)
  int hold_src = 0; // 搜出它的持有期 (0 = 手填具体式)
  bool has = false;
  Stat st;
  double score = 0, se = 0, valid = 0; // Search 目标 (不过门槛 = −∞) / 目标的标准误 / 池内有效率 (0..1)
};
// Search 无显著时的代表点 (因子文件 search[]): 过 mono 门槛的峰里曲面估计最高的 (peak), 没有峰则 mono 最高的点 —— 告诉 agent 差在哪
struct Top {
  std::string expr;
  int sign = 1;                 // 方向 (−1 = 取反后才是这组统计)
  Stat st;                      // 定向后
  double g = 0, se = 0, mu = 0; // 定向目标 ls·rIC·mono² / 其标准误 / 曲面在此的估计 (peak 才有)
  bool peak = false;
  std::string miss; // 落选原因
};
// 一个候选的结局 (AGENT.md 结局表): 反馈 / 流水 / 界面同一份分类
enum class Verdict : uint8_t { Unstaged,  // 名字或模板不合法 / 重名, 没落文件
                               Broken,    // 落了文件, 回测算不出
                               Weights,   // 有带本持有期统计的权重
                               Null,      // Search 无显著 (或权重都无本持有期统计)
                               Pending }; // 未评估
inline constexpr const char *kVerdictNames[] = {"未落文件", "BROKEN", "有参数组", "无显著", "未评估"};
inline const char *name(Verdict v) { return kVerdictNames[static_cast<int>(v)]; }
// 一个因子文件
struct Result {
  std::string file, name, name_cn, tmpl, note;
  std::string error;                  // 没落文件 (Unstaged) / 回测算不出 (Broken) 的原因
  Verdict verdict = Verdict::Pending; // 扫描时定 (Unstaged 由 stage 定)
  std::optional<Top> top;             // 本持有期 Search 无显著时的代表点
  std::vector<Weight> ws;             // 按 score 降序
  int ops = 0;                        // 模板算子节点数
  bool visible = true;                // 统计 / Search 结局全在作用域此刻的评估期上 (同池同评估期) 且 ops ≤ kMaxOps; 否则 agent 看不见
  const Weight *best() const { return ws.empty() || !ws.front().has ? nullptr : &ws.front(); }
};
// 个股行情要用的叶 (全名; 字段表里没有的为空): 本层 K 线 + 日层昨收 / 涨跌停价
struct Kline {
  std::string close, high, low, volume, amount, pre_close, lim_up, lim_dn;
};
struct Context {
  std::string pool, pool_cn, level, hold, hold_desc;
  std::string window; // 评估期 (标签库与因子池库落盘日交集; 用到别的库的叶再交)
  int seg = 0;        // 本层每日行数 (d 的单位)
  int ts_root_d = 0;  // 时序口径根 TsRankRoll 须写死的 d
  int max_d = 0;
  double min_mono = 0; // Search 的 mono 硬门槛
  std::vector<Leaf> leaves;
  std::vector<Op> ops;
  Kline kline;
  std::vector<Result> library; // 正式因子库, 按 score 降序 (含不可见的, 由用方滤)
};

// ---- 观察 (Lab::look): 评估期上整期一块的原料, 平面 [T = 天 · seg][A] 行主序 ----
inline constexpr int kGroups = 20; // 截面分组数 (= Stat 契约, Bridge.cpp 核对)
// 一个要看的因子: 具体根式 (已校验) + 口径 + 方向 (−1 = 取反后高 = 做多端); label = 给 agent 看的名字 (候选英文名, 或根式本身)
struct LookFactor {
  std::string expr, label;
  bool ts = false;
  int sign = 1;
};
struct LookSpec {
  std::vector<LookFactor> factors;
  std::vector<std::string> feats; // 特征根 (Lab::feature_of 出的 CsRank 包装规范串)
  std::string code;               // 非空 = 只取该资产一列 (个股视图, 不在池的行也落)
};
// 一级行 (已定向): 组的超额持仓收益 / 真持有只数; ok = 本行有统计
struct LookRow {
  bool ok = false;
  float ric = 0, mkt = 0; // rIC / 本行基准 (超额 = 毛 − mkt)
  float grp[kGroups] = {};
  int cnt[kGroups] = {};
};
// 两端组真持有的一格: 行 t 开仓那批里资产 a 在做多端 (top) / 做空端组; ret = 持有毛收益 (超额 = ret − rows[t].mkt)
struct LookCell {
  int t = 0, a = 0;
  bool top = true;
  float ret = 0;
};
struct LookRoot {
  std::string error;           // 非空 = 没算成
  std::vector<float> rank;     // [T][A] 池内分位 ∈ [0, 1] (已定向: 高 = 做多端; 无效 NaN)
  std::vector<int8_t> grp;     // [T][A] 组号 0..kGroups−1 (已定向; −1 无效)
  std::vector<LookRow> rows;   // [T]
  std::vector<LookCell> cells; // 行序
};
struct LookFeat {
  std::string error;
  std::vector<float> v; // [T][A] 原值 (无效 NaN)
};
struct Look {
  std::string error;              // 非空 = 没算成 (评估域不成 / 资产不在池轴 / 全部根坏), 其余字段空
  std::vector<int> dates;         // [天] YYYYMMDD
  int seg = 0;                    // 每日行数
  std::vector<std::string> clock; // [seg] 行的收盘时刻 HH:MM
  std::vector<std::string> codes; // [A]
  std::vector<uint8_t> gate;      // [T][A] 当行在池
  std::vector<float> label;       // [T][A] 会话持有期的毛收益 (无效 NaN)
  std::vector<float> mkt;         // [T] 在池且标签有效格的均值 (整截面; 无 → NaN)
  std::vector<LookRoot> factors;  // 与 LookSpec::factors 对齐
  std::vector<LookFeat> feats;    // 与 LookSpec::feats 对齐
  int A() const { return static_cast<int>(codes.size()); }
  int T() const { return static_cast<int>(dates.size()) * seg; }
};
// 分层单调性: 组号 0..n−1 与 g 的 Spearman (Stat 契约 mono 同一份)
double Mono(const double *g, int n);

// 入库门槛: 一个权重 mono / rIC 均值都不低于下限才算达标 (多空不设门槛, 只比大小)
struct Gate {
  double mono = 0, ric = 0;
};

// ---- 候选的一生: 进候选目录 (stage) ↔ 进正式库 (adopt) ----
struct Cand {
  std::string name, name_cn, expr, note;
};
struct Staged {
  Cand cand;
  std::string file; // 已落的文件名; 空 = 没落
  Result res;       // 没落 = Unstaged (error = 原因); 落了 = 落时的扫描 (Pending), 评完由 Settle 换成定论
};
struct Submitted {
  std::string name, err; // 提交的英文名 / 未入库的原因 (空 = 入库)
};

namespace catalog {
std::vector<Choice> pools(); // key = 池下标
std::string pool_cn(const std::string &pool);
std::vector<Choice> levels(const std::string &pool); // 该池能用的因子层
std::vector<Choice> holds(const SharedData &data, int level);
std::string hold_desc(int hold, int level); // 开 / 清仓规则
bool gpu();
bool ready(const SharedData &data, const Scope &sc, bool gpu); // 资产轴就绪且标签库已建
std::string label(const Scope &sc);                            // 池 / 层 / 持有期
std::string dir(const Scope &sc);                              // 会话目录 kWorkDir/<池>/<层>/<持有期>
} // namespace catalog

class Lab {
public:
  // 一步 (poll 逐个取走): eval = file 进 Search / 进 Run / 评完 (res = 定论; 队列里直接跳过的只出这一步); look = 观察完 (look 有效);
  // 这件事做完 End (file 空)
  enum class Step : uint8_t { Search,
                              Run,
                              Look,
                              Settle,
                              End };
  struct Tick {
    Step step;
    std::string file;
    Result res;
    agent::Look look;
  };
  // 在评那步此刻的样子 (调用方持有, live 整份刷新)
  struct Live {
    double frac = 0.0;   // 该步进度
    std::string message; // 服务给的结论 / 没算成的原因
  };

  Lab();
  ~Lab();

  void bind(const SharedData &data, const Scope &sc); // 须不忙, 作用域须有该持有期; 重扫上下文与候选目录, 未取走的 Tick 作废; data 须比 Lab 活得久
  const Scope &scope() const;
  const Context &ctx() const;
  const std::vector<Result> &results() const; // 候选目录全部文件, 本持有期 score 降序

  void stage(Staged &s);                   // 名字 / 模板不合法, 算子超 kMaxOps 或同名已在 → Unstaged
  void adopt(Submitted &s, const Gate &g); // 候选目录里没有 / 不可见 / 不达门槛 / 正式目录已有同名 → err

  // 观察的参数 (只查字段表 / 候选目录 / 码表, 不碰数据): 不成 → false + err (原样给 agent)
  bool factor_of(const std::string &arg, LookFactor &out, std::string &err) const;  // 候选英文名 (取最佳权重) 或具体根式
  bool feature_of(const std::string &src, std::string &root, std::string &err) const; // 具体特征式 (任意子式) → CsRank 包装的规范串
  bool code_of(const std::string &arg, std::string &code, std::string &err) const;    // "000001" / "000001.SZ" → 码表里的码

  void eval(std::vector<std::string> files, bool gpu); // 须已 bind 且作用域 ready, 不忙
  void look(LookSpec s);                               // 同上; 参数须经上面三个校验
  void cancel();                                       // 清空队列, 取消在跑那步; 它收尾即 End
  bool busy() const;                                   // eval / look 起, 到 End 入队
  std::optional<Tick> poll();
  void live(Live &l) const;

private:
  struct Impl;
  std::unique_ptr<Impl> p_;
};

} // namespace agent
