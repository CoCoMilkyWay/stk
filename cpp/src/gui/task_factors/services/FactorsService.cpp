// FactorsService — 见头文件. 本 TU include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点:
// CMake 里已列入 PRECISE_MATH 源 (-fno-fast-math), 与 OperatorsService 同待遇.
#include "gui/task_factors/services/FactorsService.hpp"

#include "gui/task_factors/services/FactorsEngine.hpp" // eval_job / Probes / FactorFlow (四服务同一条管线)
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <filesystem>
#include <map>

namespace GUI::Factors {

namespace {

// Run 的探针: EvalStat 通用六探针 (一个根一份, 同根的只评首个)
using RunProbes = EvalProbes<>;

} // namespace

bool MakeFactorsRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, bool evaluate, bool gpu,
                        FactorsRequest &req) {
  req = FactorsRequest{};
  req.spec.evaluate = evaluate;
  return MakeFactorScope(data, std::move(ft), pool, gpu, req.scope) || !evaluate;
}

FactorsService::FactorsService() { Start(); }
FactorsService::~FactorsService() { Stop(); }

bool FactorsService::run(FactorsRequest &req) {
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
  if (req.spec.keep)
    flow().drop_engines(); // 块留给会话的下一个请求
  else
    flow().drop(); // 块 / 引擎只活一轮 (Y 跨文件复用)
  if (!ok) {       // 中断: 没评到的行也收尾 (判废后不再有新请求刷新)
    std::lock_guard<std::mutex> lock(mutex);
    for (FactorRow &r : rows)
      r.status = RowStatus::Done;
  }
  return ok;
}

void FactorsService::mark(size_t i, const std::string &why) {
  FactorRow &r = rows[i];
  if (r.status == RowStatus::Done)
    return;
  r.error = why;
  r.status = RowStatus::Done;
  broken_.fetch_add(1, std::memory_order_relaxed);
}

// 扫目录: 每个 *.json 的每个权重一行 (文件级问题一行 widx −1); 解析 / 校验失败 → error (文件不动)
void FactorsService::scan(const FactorsRequest &req, std::vector<FactorRow> &out) {
  TraceN("FactorsScan");
  out.clear();
  const FeatureTable &ft = *req.scope.ft;
  for (const std::filesystem::path &p : list_factor_files(req.scope.factor_dir))
    ScanFactorFile(p, ft, out);
  std::map<std::pair<int, std::string>, std::string> seen; // (hold, 具体规范串) → 首行 key
  for (FactorRow &w : out) {
    if (w.expr.empty())
      continue;
    if (const auto it = seen.find({w.hold_src, w.expr}); it != seen.end())
      w.dup_of = it->second;
    else
      seen.emplace(std::pair{w.hold_src, w.expr}, w.key);
  }
  if (!req.spec.evaluate) // 只扫描: BROKEN 行也算"处理完"
    for (FactorRow &r : out)
      r.status = RowStatus::Done;
}

// 装载 + 评估 (worker 线程; 返回 false = 取消 / 判废): 逐因子文件 eval_file
bool FactorsService::eval(const FactorsRequest &req, FactorFlow &F) {
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
bool FactorsService::eval_file(const FactorsRequest &req, FactorFlow &F, E &eng, const std::vector<size_t> &idx) {
  TraceN("FactorsEvaluateFile");
  // 该文件的权重 (条目 e ↔ 行 idx[e]) → 根集 (同式同口径共根, 只评一次) → 一张共享森林. rows 只本 worker 写, 读不用锁
  RootSet rs;
  for (const size_t i : idx)
    rs.add(*req.scope.ft, rows[i].expr, rows[i].frame);
  // Y = 全部持有期 + 全部金额段, 同作用域跨文件复用; X = 本文件的叶 (retain; keep → 读过的全留)
  JobOut o;
  o.job.spec.y = y_full(*req.scope.ft, req.scope.max_impact);
  o.job.retain = !req.spec.keep;
  o.job.who = "Run";
  std::vector<RunProbes> ps(rs.size());
  for (RunProbes &p : ps)
    p.hd = o.job.spec.y.hd;
  std::vector<size_t> fin; // 本轮评完的条目 (森林走完 Ok 才一次回写)
  struct Hooks : HookBase {
    FactorsService &W;
    const FactorsRequest &req;
    const std::vector<size_t> &idx;
    const RootSet &rs;
    const std::vector<RunProbes> &ps;
    JobOut &o;
    std::vector<size_t> &fin;
    // 值域已查: 坏根的条目 BROKEN, 好根的 Running
    void plan(size_t b0, const Forest &, const std::vector<std::string> &why, const std::vector<uint8_t> &) {
      assert(b0 == 0 && "eval_job 根全进一张森林");
      std::lock_guard<std::mutex> lock(W.mutex);
      for (size_t e = 0; e < idx.size(); ++e)
        if (const std::string &w = why[static_cast<size_t>(rs.of[e])]; !w.empty())
          W.mark(idx[e], w);
        else
          W.rows[idx[e]].status = RowStatus::Running;
    }
    // 发布根 k 的全部条目 (同根共一份 stat, eval_ms 同)
    void done(int k) {
      EvalStat st;
      fill_eval(ps[static_cast<size_t>(k)], stat_scope(req.scope, o.job.dm, o.job.blocks.of(0).y, E::kName), o.eval_ms(k), st);
      {
        std::lock_guard<std::mutex> lock(W.mutex);
        for (size_t e = 0; e < idx.size(); ++e) {
          if (rs.of[e] != k)
            continue;
          FactorRow &r = W.rows[idx[e]];
          static_cast<EvalStat &>(r) = st;
          r.has_stat = true;
          r.stat_from_file = false;
          r.status = RowStatus::Done;
          fin.push_back(e);
        }
      }
      W.publish();
    }
  };
  const Rc rc = eval_job(eng, F, req.scope, rs.roots(), [&](int k) -> RunProbes & { return ps[static_cast<size_t>(k)]; }, o, Hooks{{}, *this, req, idx, rs, ps, o, fin});
  const bool ok = settle(rc, o.job.why, [&](const std::string &why) { // 评估域不成 / 全部根坏 → 没收尾的行 BROKEN
    std::lock_guard<std::mutex> lock(mutex);
    for (const size_t i : idx)
      mark(i, why);
  });
  if (rc == Rc::Ok && !fin.empty()) { // 回写在锁外
    std::vector<const FactorRow *> out;
    for (const size_t e : fin)
      out.push_back(&rows[idx[e]]);
    WriteFactorStats(std::filesystem::path(req.scope.factor_dir) / rows[idx.front()].file, out);
  }
  publish();
  return ok;
}

} // namespace GUI::Factors
