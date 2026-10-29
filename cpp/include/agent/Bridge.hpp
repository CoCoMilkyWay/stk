#pragma once

// =============================================================================
// Bridge — agent 与项目的唯一接口面
// =============================================================================
//   agent/ 其余代码不碰项目的因子 / 特征 / 服务头, 只经这里 (实现 Bridge.cpp 是唯一 include 它们的 TU). 两半:
//     catalog  作用域目录 (无状态, 开会话前用): 可挖的 (池, 因子层, 持有期; 持有期限 Search 档) / 就绪判据 /
//              上下文 (该层可作输入的特征叶 / 算子表 / 正式因子库在该持有期的统计)
//     Lab      会话工作区 (bind 一个作用域 + 候选目录). 候选的一生:
//                Cand ─stage→ 候选目录里的因子文件 (与正式因子同格式) ─eval→ Result ─adopt (过门槛)→ 正式因子目录
//              eval 由 poll 推进 (每帧取到空): 逐文件 Search (有自由参数) → Run, 结果写回文件, 每步出一个 Tick; 评完一个只重扫它 (results 增量维护);
//              作用域不变的连续会话共用两个 worker 的常驻块 (SearchSpec / RunSpec keep), 换作用域即弃
//              与 agent/Chat.hpp 同一套 ask (eval) / poll / live / busy / cancel
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
  int hold_src = 0; // 搜出它的持有期 (0 = 手填具体式)
  bool has = false;
  Stat st;
  double score = 0, valid = 0; // Search 目标 / 池内有效率 (0..1)
};
// Search 无显著时定向目标 g 最高的候选 (不看门槛): 告诉 agent 差在哪
struct Top {
  std::string expr;
  int sign = 1;         // 方向 (−1 = 取反后才是这组统计)
  Stat st;              // 定向后
  double g = 0, se = 0; // 定向目标 ls·rIC / 其标准误
  std::string miss;     // 落选原因
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
  std::string file, name, name_cn, tmpl, note, error; // error 非空 = BROKEN
  Verdict verdict = Verdict::Pending;                 // 扫描时定
  std::optional<Top> top;                             // 本次运行里最近一趟 Search 无显著 (同作用域跨会话留着)
  std::vector<Weight> ws;                             // 按 score 降序
  const Weight *best() const { return ws.empty() || !ws.front().has ? nullptr : &ws.front(); }
};
struct Context {
  std::string pool, pool_cn, level, hold, hold_desc;
  int seg = 0;       // 本层每日行数 (d 的单位)
  int ts_root_d = 0; // 时序口径根 TsRankRoll 须写死的 d
  int max_d = 0;
  std::vector<Leaf> leaves;
  std::vector<Op> ops;
  std::vector<Result> library; // 正式因子库, 按 score 降序
};

// 入库门槛: 一个权重 mono / rIC 均值都不低于下限才算达标 (多空不设门槛, 只比大小)
struct Gate {
  double mono = 0, ric = 0;
};

// ---- 候选的一生: 进候选目录 (stage) ↔ 进正式库 (adopt), err 空 = 成 ----
struct Cand {
  std::string name, name_cn, expr, note;
};
struct Staged {
  Cand cand;
  std::string file, err; // 已落的文件名 / 未落的原因
  Result res;            // 没落 = Unstaged; 落了 = 落时的扫描 (Pending), 评完由 Tick 换成定论
};
struct Submitted {
  std::string name, err; // 提交的英文名 / 未入库的原因
};

// ---- 评估 ----
enum class Step : uint8_t { Idle,
                            Search,
                            Run };
inline constexpr const char *kStepNames[] = {"idle", "search", "run"};
inline const char *name(Step s) { return kStepNames[static_cast<int>(s)]; }
struct EvalState {
  Step step = Step::Idle;
  int done = 0, total = 0; // 队列: 已完成 / 总文件数
  std::string file;        // 在评的文件
  double frac = 0.0;       // 在评文件该步的进度
  std::string message;     // 服务给的结论 / 没算成的原因
};
// 一步: file 进 Search / Run; Idle = 评完, res = 定论 (队列里直接跳过的只出这一步)
struct Tick {
  Step step = Step::Idle;
  std::string file;
  Result res;
};

namespace catalog {
std::vector<Choice> pools(); // key = 池下标
std::string pool_cn(const std::string &pool);
std::vector<Choice> levels(const std::string &pool); // 该池能用的因子层
std::vector<Choice> holds(const SharedData &data, int level);
std::string hold_desc(int hold, int level); // 开 / 清仓规则
bool gpu();
bool ready(const SharedData &data, const Scope &sc, bool gpu); // 资产轴就绪且标签库已建
Context context(const SharedData &data, const Scope &sc);
} // namespace catalog

class Lab {
public:
  Lab();
  ~Lab();

  void bind(const SharedData &data, const Scope &sc, std::string dir, bool gpu); // 须 ready 且不忙; data 须比 Lab 活得久

  void stage(Staged &s);                   // 名字 / 模板不合法, 算子超 kMaxOps 或同名已在 → err
  void adopt(Submitted &s, const Gate &g); // 候选目录里没有 / 不达门槛 / 正式目录已有同名 → err

  void eval(std::vector<std::string> files); // 排进队列; 须不忙
  void cancel();                             // 取消在跑的并清空队列 (之后不再出 Tick)
  bool busy() const;
  std::optional<Tick> poll(); // 推进队列, 取走一步; 每帧取到空
  EvalState live() const;

  const std::vector<Result> &results() const; // 候选目录全部文件, 本持有期 score 降序

private:
  struct Impl;
  std::unique_ptr<Impl> p_;
};

} // namespace agent
