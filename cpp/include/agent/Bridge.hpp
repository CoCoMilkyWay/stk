#pragma once

// =============================================================================
// Bridge — agent 与项目的唯一接口面
// =============================================================================
//   agent/ 其余代码不碰项目的因子 / 特征 / 服务头, 只经这里 (实现 Bridge.cpp 是唯一 include 它们的 TU):
//     ① 作用域   pools / levels / holds: 可挖的 (池, 因子层, 持有期); 持有期限 Search 档 (search_hold)
//     ② 上下文   context: 该层可作输入的特征叶 / 算子表 / 正式因子库 (含该持有期统计)
//     ③ 落地     stage: 候选 (名字 + 模板) 校验后写成 dir 里的因子文件 (与正式因子同格式)
//     ④ 评估     eval → update 每帧推进: 逐文件 Search (有自由参数) → Run, 结果写回文件; results 扫 dir 读回.
//                会话常驻: 作用域不变的连续评估共用 worker 的数据块 (SearchSpec / RunSpec keep), 换作用域即弃
//     ⑤ 采纳     adopt: dir 里的因子文件拷进正式因子目录
//   全部 GUI 线程调用; 正式因子目录 / 字段表 / 资产轴由项目定, agent 只给目录 dir
// =============================================================================

#include <cstdint>
#include <memory>
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

// ---- ② 上下文 ----
struct Leaf {
  std::string code, name_cn, formula, desc; // code = 档.层.列 (模板里的写法)
};
struct Op {
  std::string name, name_cn, sig, out, tex, note; // sig = 元与参数的值域 (LaTeX); out = 输出值域名
};
// 一个权重在作用域持有期下的统计 (has = Run 过且该持有期 n ≥ 3)
struct Weight {
  std::string expr;
  int hold_src = 0; // 搜出它的持有期 (0 = 手填具体式)
  bool has = false;
  double score = 0, ls = 0, ric = 0, ir = 0, t = 0, mono = 0, valid = 0; // ls = 每期毛多空; valid = 池内有效率
};
// 一个因子文件
struct Result {
  std::string file, name, name_cn, tmpl, note, error; // error 非空 = BROKEN
  bool searched = false;                              // dir/log 下有它的 Search 日志
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

// ---- ③ 落地 ----
struct Cand {
  std::string name, name_cn, expr, note;
};

// ---- ④ 评估进度 ----
struct EvalState {
  bool busy = false;
  int done = 0, total = 0; // 队列: 已完成 / 总文件数
  std::string file;        // 在评的文件
  std::string stage;       // search / run
  double frac = 0.0;       // 在评文件该阶段的进度
  std::string message;     // 服务给的结论 / 没算成的原因
};

class Bridge {
public:
  Bridge();
  ~Bridge();

  // ① 作用域
  static std::vector<Choice> pools(); // key = 池下标
  static std::string pool_cn(const std::string &pool);
  static std::vector<Choice> levels(const std::string &pool); // 该池能用的因子层
  std::vector<Choice> holds(const SharedData &data, int level);
  static std::string hold_desc(int hold, int level); // 开 / 清仓规则

  // ② 上下文
  Context context(const SharedData &data, const Scope &sc);

  // ③ 落地: 返回文件名; 名字 / 模板不合法或同名已在 → 空 + err
  std::string stage(const SharedData &data, const Scope &sc, const std::string &dir, const Cand &c, std::string &err);

  // ④ 评估: ready = 资产轴就绪且标签库已建; eval 把 files 排进队列 (忙或未 ready 时 assert)
  bool ready(const SharedData &data, const Scope &sc, bool gpu) const;
  void eval(const SharedData &data, const Scope &sc, const std::string &dir, const std::vector<std::string> &files, bool gpu);
  void update(const SharedData &data); // 每帧 (无论是否选中)
  void cancel();                       // 取消在跑的并清空队列
  EvalState state() const;
  uint64_t evals() const; // 评完的文件累计数 (UI 据此重扫 results)
  std::vector<Result> results(const SharedData &data, const Scope &sc, const std::string &dir);
  static bool gpu_available();

  // ⑤ 采纳: 正式目录已有同名 → false + err
  bool adopt(const SharedData &data, const Scope &sc, const std::string &dir, const std::string &file, std::string &err);

private:
  struct Impl;
  std::unique_ptr<Impl> p_;
};

} // namespace agent
