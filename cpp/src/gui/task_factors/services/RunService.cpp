// RunService — 见头文件. 本 TU include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点:
// CMake 里已列入 PRECISE_MATH 源 (-fno-fast-math), 与 OperatorsService 同待遇.
#include "gui/task_factors/services/RunService.hpp"

#include "gui/task_factors/services/FactorsEngine.hpp" // eval_job / Probes / FactorFlow (四服务同一条管线)
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <filesystem>
#include <map>
#include <tuple>

namespace GUI::Factors {

namespace {

// Run 的探针: EvalStat 通用五探针 (一个根一份, 同根的只评首个)
using RunProbes = EvalProbes<>;

} // namespace

bool MakeRunRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, bool evaluate, bool gpu, RunRequest &req) {
  req = RunRequest{};
  req.spec.evaluate = evaluate;
  return MakeFactorScope(data, std::move(ft), pool, gpu, req.scope) || !evaluate;
}

RunService::RunService() { Start(); }
RunService::~RunService() { Stop(); }

bool RunService::run(RunRequest &req) {
  std::vector<FactorRow> rs;
  scan(req, rs);
  int nb = 0;
  for (const FactorRow &r : rs)
    nb += !r.error.empty();
  {
    std::lock_guard<std::mutex> lock(mutex);
    rows = std::move(rs);
    current = req;
    message.clear();
  }
  broken_.store(nb, std::memory_order_relaxed);
  publish();

  if (!req.spec.evaluate)
    return true;
  const bool ok = eval(req, flow());
  flow().end(req.spec.keep ? Life::Worker : Life::Request);
  if (!ok) { // 中断: 没评到的行也收尾 (判废后不再有新请求刷新)
    std::lock_guard<std::mutex> lock(mutex);
    for (FactorRow &r : rows)
      r.status = RowStatus::Done;
  }
  return ok;
}

void RunService::mark(size_t i, const std::string &why) {
  FactorRow &r = rows[i];
  assert(r.status != RowStatus::Done && "每行至多收尾一次");
  r.error = why;
  r.status = RowStatus::Done;
  broken_.fetch_add(1, std::memory_order_relaxed);
}

// 扫目录: 每个 *.json 的每个权重一行 (文件级问题一行 widx −1); 解析 / 校验失败 → error (文件不动)
void RunService::scan(const RunRequest &req, std::vector<FactorRow> &out) {
  TraceN("FactorsScan");
  out.clear();
  const FeatureTable &ft = *req.scope.ft;
  for (const std::filesystem::path &p : list_factor_files(req.scope.factor_dir))
    ScanFactorFile(p, ft, out);
  std::map<std::tuple<int, int, std::string>, std::string> seen; // (hold, 方向, 具体规范串) → 首行 key
  for (FactorRow &w : out) {
    if (w.expr.empty())
      continue;
    if (const auto it = seen.find({w.hold_src, w.sign, w.expr}); it != seen.end())
      w.dup_of = it->second;
    else
      seen.emplace(std::tuple{w.hold_src, w.sign, w.expr}, w.key);
  }
  if (!req.spec.evaluate) // 只扫描: BROKEN 行也算"处理完"
    for (FactorRow &r : out)
      r.status = RowStatus::Done;
}

// 装载 + 评估 (worker 线程; 返回 false = 取消 / 判废): 逐因子文件 eval_file
bool RunService::eval(const RunRequest &req, FactorFlow &F) {
  TraceN("FactorsEvaluate");
  // 待评行 (有效权重; scan 已校验过), 按文件分组 (scan 出的行同文件相邻)
  std::vector<std::vector<size_t>> idx;
  {
    std::lock_guard<std::mutex> lock(mutex);
    for (size_t i = 0; i < rows.size(); ++i) {
      if (!rows[i].error.empty() || rows[i].expr.empty() || (!req.spec.only_file.empty() && rows[i].file != req.spec.only_file)) {
        rows[i].status = RowStatus::Done; // BROKEN / 未搜索 / 不在 only_file 的行本轮不算
        continue;
      }
      if (idx.empty() || rows[idx.back().front()].file != rows[i].file)
        idx.emplace_back();
      idx.back().push_back(i);
    }
  }
  if (idx.empty()) {
    std::lock_guard<std::mutex> lock(mutex);
    for (FactorRow &r : rows)
      r.status = RowStatus::Done;
    message = "无有效因子";
    return true;
  }
  return F.with_engine<RunProbes>(req.scope.gpu, [&](auto &eng) {
    for (const std::vector<size_t> &f : idx)
      if (!eval_file(req, F, eng, f))
        return false;
    return true;
  });
}

template <class E>
bool RunService::eval_file(const RunRequest &req, FactorFlow &F, E &eng, const std::vector<size_t> &idx) {
  TraceN("FactorsEvaluateFile");
  // 该文件的权重 (条目 e ↔ 行 idx[e]) → 根集 (同式同口径共根, 只评一次; 全部持有期) → 一张共享森林. rows 只本 worker 写, 读不用锁
  const Holds hd = keys_of<Holds>(req.scope.ft->y.labels);
  RootSet rs;
  for (const size_t i : idx)
    rs.add(*req.scope.ft, rows[i].expr, rows[i].frame, factor::stat::dir_of(rows[i].sign), hd);
  // Y = 全部持有期 + 全部金额段, 整期一块时同作用域跨文件复用 (分块 = 作业自有, 逐文件重读); X = 本文件的叶 (keep → 读过的全留)
  JobOut o;
  o.job.life = req.spec.keep ? Life::Worker : Life::Request;
  o.job.who = "Run";
  std::vector<RunProbes> ps(rs.size());
  std::vector<size_t> fin; // 本轮评完的条目 (森林走完 Ok 才一次回写)
  struct Hooks : HookBase {
    RunService &W;
    const FactorScope &sc;
    const std::vector<size_t> &idx;
    const std::vector<RunProbes> &ps;
    const JobOut &o;
    std::vector<size_t> &fin;
    int k_st = -1; // st = 根 k_st 的 stat (同根条目相继 post)
    EvalStat st = {};
    // 值域已查: 未收尾的行 Running (坏根的已由 fail 收尾)
    void plan(size_t, const Forest &, const std::vector<std::string> &, const std::vector<uint8_t> &) {
      std::lock_guard<std::mutex> lock(W.mutex);
      for (const size_t i : idx)
        if (W.rows[i].status != RowStatus::Done)
          W.rows[i].status = RowStatus::Running;
    }
    void fail(int e, const std::string &why) {
      {
        std::lock_guard<std::mutex> lock(W.mutex);
        W.mark(idx[static_cast<size_t>(e)], why);
      }
      W.publish();
    }
    // 条目 e 的根 k 评完 (同根共一份 stat, eval_ms 同)
    void post(int e, int k) {
      if (k != k_st) {
        st = EvalStat{};
        fill_eval(ps[static_cast<size_t>(k)], o.scope(sc), o.eval_ms(k), st);
        k_st = k;
      }
      {
        std::lock_guard<std::mutex> lock(W.mutex);
        FactorRow &r = W.rows[idx[static_cast<size_t>(e)]];
        static_cast<EvalStat &>(r) = st;
        r.has_stat = true;
        r.stat_from_file = false;
        r.status = RowStatus::Done;
      }
      fin.push_back(static_cast<size_t>(e));
      W.publish();
    }
  };
  const Rc rc = eval_job(eng, F, req.scope, rs, [&](int k) -> RunProbes & { return ps[static_cast<size_t>(k)]; }, o, Hooks{{}, *this, req.scope, idx, ps, o, fin});
  if (!settle(rc))
    return false;
  if (rc == Rc::Ok && !fin.empty()) { // 回写在锁外
    std::vector<const FactorRow *> out;
    for (const size_t e : fin)
      out.push_back(&rows[idx[e]]);
    WriteFactorStats(std::filesystem::path(req.scope.factor_dir) / rows[idx.front()].file, out);
  }
  return true;
}

} // namespace GUI::Factors
