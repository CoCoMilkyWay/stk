#pragma once

// =============================================================================
// Bridge — agent 与项目的唯一接口面
// =============================================================================
//   agent/ 其余代码不碰项目的因子 / 特征 / 服务头, 只经这里 (实现 Bridge.cpp 是唯一 include 它们的 TU). 两半:
//     catalog  作用域目录 (无状态): 可挖的 (池, 因子层, 持有期; 持有期限 Search 档) / 就绪判据 / 会话目录
//     Lab      作用域工作区 (bind 一个作用域): 上下文 ctx (该层可作输入的特征叶 / 算子表 / 正式因子库在该持有期的统计) + 候选目录 results.
//              候选的一生: Cand ─stage→ 候选目录里的因子文件 (与正式因子同格式) ─eval→ Result ─adopt (过门槛)→ 正式因子目录
//              eval: 逐文件 Search (定方向 + 搜自由参数; 具体式只评本身) → Run, 结果写回文件; 评完一个只重扫它 (results 增量维护)
//              可见: 只有统计与 Search 结局都在作用域此刻评估期上的因子才给 agent 看 (Result::visible), 不同窗口的数不可比
//              两道 (Search / Run) 同构, 首次 eval 才建; 作用域与评估后端不变的连续会话共用其常驻块 (SearchSpec / RunSpec keep), 变了即弃
//   worker 协议 (与 agent/Chat.hpp 对仗): 交一件事 (eval ↔ ask) → poll 逐个取走 Tick, 末个 = End → live 看在跑那步 → busy / cancel
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
struct Context {
  std::string pool, pool_cn, level, hold, hold_desc;
  std::string window; // 评估期 (标签库与因子池库落盘日交集; 用到别的库的叶再交)
  int seg = 0;        // 本层每日行数 (d 的单位)
  int ts_root_d = 0;  // 时序口径根 TsRankRoll 须写死的 d
  int max_d = 0;
  double min_mono = 0; // Search 的 mono 硬门槛
  std::vector<Leaf> leaves;
  std::vector<Op> ops;
  std::vector<Result> library; // 正式因子库, 按 score 降序 (含不可见的, 由用方滤)
};

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
  // 评估一步 (poll 逐个取走): file 进 Search / 进 Run / 评完 (res = 定论; 队列里直接跳过的只出这一步); 本批评完 End (file 空)
  enum class Step : uint8_t { Search,
                              Run,
                              Settle,
                              End };
  struct Tick {
    Step step;
    std::string file;
    Result res;
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

  void eval(std::vector<std::string> files, bool gpu); // 须已 bind 且作用域 ready, 不忙
  void cancel();                                       // 清空队列, 取消在评那步; 它收尾即 End
  bool busy() const;                                   // eval 起, 到 End 入队
  std::optional<Tick> poll();
  void live(Live &l) const;

private:
  struct Impl;
  std::unique_ptr<Impl> p_;
};

} // namespace agent
