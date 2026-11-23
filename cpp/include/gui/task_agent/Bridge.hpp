#pragma once

// =============================================================================
// Bridge — 右翼 · agent 与因子引擎的桥及单线程异步工作台 Lab (与左翼 Chat 同构对仗)
// =============================================================================
//   agent 会话层与界面只认这里的值类型, 不碰 FactorRow / EvalStat / SearchResult / LimitOrderBook:
//     catalog   无状态作用域目录 (池 / 因子层 / 持有期 / GPU 可用 / 就绪判据 / 落盘键)
//     Lab       候选四拍 (stage / absorb / adopt / discard) + 单线程 Async 工作台:
//                 send(Tool, LabTask) → 共享不可变快照 ViewEnv (零跨线程拷贝) → worker 跑 LabTask → LabOut
//   异步五件与左翼 Chat 逐字同构: Tick / send / cancel / busy / poll / live
// =============================================================================

#include "disk/Disk.hpp"
#include "gui/task_agent/Enum.hpp"
#include "gui/task_agent/Tools.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

struct SharedData;

namespace GUI::Agent {

// 特征来源: bar = Bar 档 (因子池 Bar 库, 含基本面 / 估值的 Fund 族), 恒开放; lob = Lob 档 (因子池 Lob 库), 可选
enum class Src : uint8_t { Bar,
                           Lob };
struct SrcDef {
  const char *id;
};
inline constexpr SrcDef kSrcs[] = {{"bar"}, {"lob"}};
AGENT_ROWS(Src, kSrcs, Lob);

// 人开放给 agent 的一类 (特征叶 / 算子): all = 全开 (此后新加的也开); 否则只开 on 里的
struct Open {
  bool all = true;
  std::set<std::string> on;
  bool has(const std::string &k) const { return all || on.contains(k); }
  bool operator==(const Open &) const = default;
};

inline constexpr int kEvalYears = 1;
struct Scope {
  std::string pool;
  int level = -1;                     // 因子层下标
  int hold = 0;                       // 持有期键
  bool lob = true;                    // 开放 lob 叶
  int years = kEvalYears;             // 评估期只留最近几年 (≥ 1)
  std::map<std::string, Open> leaves; // 因子层名 → 该层开放的叶; 没有该层 = 全开
  Open ops;                           // 开放的算子
  bool has(Src s) const { return s == Src::Bar || lob; }
  const Open &leaves_of(const std::string &lv) const {
    static const Open kAll;
    const auto it = leaves.find(lv);
    return it == leaves.end() ? kAll : it->second;
  }
  bool operator==(const Scope &) const = default;
};

struct Choice {
  int key = 0;
  std::string name;
};

// ---- 特征叶与算子说明 ----
struct Leaf {
  std::string code, name_cn, formula, desc;
  std::string tf, tf_def;
  std::string family;
  Src src = Src::Bar;
  std::string kin, hay; // 同组键 (LeafKin + "|" + tf) / 小写检索串
};
std::pair<std::string, std::vector<std::string>> LeafWords(const std::string &code);
std::string LeafKin(const std::string &code);
std::string LeafCol(const std::string &code);                                          // 叶名去掉 档.层. 的列名
std::vector<std::vector<const Leaf *>> KinGroups(const std::vector<const Leaf *> &ls); // 按同组键分组 (组序 = 首见序)
std::string KinName(const std::vector<const Leaf *> &g);                               // 一组叶写成花括号式 (不成积的逐个列)

struct Op {
  std::string name, name_cn, sig, out, tex, note;
};

// ---- 回测统计与候选结局 (均已按所评方向) ----
struct Year {
  int year = 0;
  double ric = 0.0; // 年内 rIC 均值
};
struct Weight {
  std::string expr;
  int sign = 1;
  int hold_src = 0;
  bool has = false;              // 有该持有期的统计且有滚动窗
  double score = 0.0;            // 中位 rIC (滚动窗 = 评估期 / kRollParts; factor/Search/Contract.hpp【目标】)
  double gross = 0.0, net = 0.0; // 每期毛 / 净多空 (净 = 扣固定冲击的换手成本)
  double turn = 0.0;             // 年换手 (倍, 两端单边均)
  std::vector<double> grp;       // 20 组每期毛收益 (减组间均值)
  std::vector<Year> years;
};

enum class Verdict : uint8_t { Unstaged,
                               Broken,
                               Weights,
                               Null,
                               Pending };
struct VerdictDef {
  const char *cn;
};
inline constexpr VerdictDef kVerdicts[] = {{"未落文件"}, {"BROKEN"}, {"有权重"}, {"无权重"}, {"未评完"}};
AGENT_ROWS(Verdict, kVerdicts, Pending);

// Search 评过的一个参数点 (按写进的权重的符号读: 符号相反的点中位 rIC 为负)
struct Point {
  std::vector<double> v;   // 各自由参数的取值 (Sweep::slots 同序)
  double score = 0.0;      // 中位 rIC
  std::vector<float> line; // [日] 窗中位 rIC (Sweep::dates 同长; 不计入的窗 NaN)
};
// 一趟 Search 的全部点 (本进程内存里; 载入的会话没有)
struct Sweep {
  std::vector<std::string> slots;
  std::vector<int> dates; // 评估期交易日 YYYYMMDD
  std::vector<Point> pts; // 中位 rIC 降序
};
// 一个权重在本持有期的逐日序列 (Run 顺带, 本进程内存里; 载入的会话没有): 已按权重方向
struct Daily {
  std::string expr; // 哪个权重 (Weight::expr / sign)
  int sign = 1;
  std::vector<int> dates;                  // 评估期交易日 YYYYMMDD
  std::vector<float> cum;                  // [日] 累计中位 rIC (截至该日全部窗 rIC 的中位; 首窗前 NaN, 末日 = 中位 rIC)
  std::vector<std::vector<float>> lay, ls; // [线][日] 超额日收益: 分层 (20 组 + 两端尾部) / LS (整组 + 尾部); 线序同 Inspect
  std::vector<float> turn;                 // [LS 线] 年换手 (倍)
};

struct Result {
  std::string file, name, name_cn, tmpl, twin, note, error;
  Verdict verdict = Verdict::Pending;
  std::vector<Weight> ws;
  Sweep sweep;
  Daily daily; // 最佳权重的 (dates 空 = 没有)
  int ops = 0;
  bool visible = true;
  const Weight *best() const;
};

struct Context {
  std::string pool, pool_cn, level, hold, hold_key, hold_desc;
  std::string srcs;
  bool has_lob = false;
  int years = kEvalYears;
  std::string window;
  int first = 0, last = 0;
  int seg = 1, open = 0;
  std::vector<std::string> clock;
  int ts_root_d = 0, max_d = 0;
  int roll_parts = 0; // 滚动窗 = 评估期 / roll_parts
  std::vector<Leaf> leaves, all_leaves;
  std::vector<Op> ops, all_ops;
  std::vector<Result> library;
  std::vector<int> days;          // 评估期交易日 YYYYMMDD (工作台随机输入的取材)
  std::vector<std::string> codes; // 池轴上的代码 (同上)
  std::vector<uint8_t> traded;    // [days × codes] 当日有逐笔 (逐笔目录里没有的日子全记 1; 同上)
  std::string grid;               // 网格层在叶名里的层段 (档.<层>.列; 同上: 因子须含本层的叶)
};

struct Gate {
  double ric = 0.0; // 中位 rIC 下限
  bool operator==(const Gate &) const = default;
};
std::string GateMiss(const Gate &g, const Result &r);

struct Cand {
  std::string name, name_cn, expr, hyp, note;
};
struct Staged {
  Cand cand;
  std::string file;
  Result res;
  bool ok() const { return !file.empty(); }
};
struct Adopted {
  std::string name, error;
  bool ok() const { return error.empty(); }
};

// ---- 无状态作用域目录 ----
namespace catalog {
std::vector<Choice> pools();
struct PoolRow {
  int key = 0;
  std::string name, name_cn;
  Per<Src, bool> built{};
  size_t factors = 0;
};
std::vector<PoolRow> pool_rows(int level);
bool pool_movable(int at);
void move_pool(SharedData &data, int from, int to);
std::string pool_cn(const std::string &pool);
std::vector<Choice> levels();
std::vector<Choice> holds(const SharedData &data, int level);
std::string hold_desc(int hold, int level);
bool gpu();
bool ready(const SharedData &data, const Scope &sc, bool gpu);
std::string label(const Scope &sc);
disk::Key key(const Scope &sc, bool test);
} // namespace catalog

// ---- 右翼异步工作台 Lab (与左翼 Chat 完全对仗: Tick / send / cancel / busy / poll / live) ----
enum class LabStep : uint8_t { Search,
                               Run,
                               Look,
                               Docs,
                               End };
struct LabStepDef {
  const char *cn;
};
inline constexpr LabStepDef kLabSteps[] = {{"Search"}, {"Run"}, {"观察"}, {"查研报"}, {"完"}};
AGENT_ROWS(LabStep, kLabSteps, End);

struct Bay;
using LabTask = std::function<ToolOut(Bay &)>;

struct LabLive {
  double frac = 0.0;
};
struct LabOut {
  bool cancelled = false;
  ToolOut out;
};

class Lab {
public:
  struct Tick {
    LabStep step;
    LabOut out{};
  };

  Lab();
  ~Lab();
  Lab(const Lab &) = delete;
  Lab &operator=(const Lab &) = delete;

  void bind(const SharedData &data, const Scope &sc, bool test);
  const Scope &scope() const;
  bool test() const;
  const Context &ctx() const;
  const std::vector<Result> &results() const;
  const Result *tried(std::string_view name) const;
  const Result &result(const std::string &file) const; // 候选目录里按文件取 (不论可见)
  std::string search_log(const std::string &file) const;

  std::string vet(const Cand &c) const; // stage 落文件前的全部查验 (空 = 可落; 不写盘, 不查假设)
  Staged stage(Cand c, std::string_view hyp_title);
  const Result &absorb(const std::string &file, bool ok, const std::string &err, Sweep sweep, std::vector<Daily> days);
  Adopted adopt(std::string name, const Gate &g, bool dry); // dry = 只查门槛不拷进正式库 (工作台; 自检会话恒不拷)
  void discard(const std::string &file);

  void send(Tool tool, LabTask task);
  void cancel();
  bool busy() const;
  std::optional<Tick> poll();
  void live(LabLive &dst) const;

  struct Impl;
  const Impl &impl() const { return *impl_; }

private:
  std::unique_ptr<Impl> impl_;
};

} // namespace GUI::Agent
