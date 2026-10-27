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
  flow().drop(); // 作用域块 / 引擎只活一轮 (Y 跨文件复用)
  return ok;
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
  // 有效权重行 → Expr (scan 已校验过, 这里必成功), 按文件分组 (scan 出的行同文件相邻)
  std::vector<std::vector<size_t>> idx;
  std::vector<std::vector<factor::expr::Expr>> exprs;
  std::vector<std::vector<factor::stat::Frame>> frames; // [k] 口径 (同根的因子规范串相同 → 口径相同)
  {
    std::lock_guard<std::mutex> lock(mutex);
    for (size_t i = 0; i < rows.size(); ++i) {
      if (!rows[i].error.empty() || rows[i].expr.empty() || (!req.spec.only_file.empty() && rows[i].file != req.spec.only_file)) {
        rows[i].status = RowStatus::Done; // BROKEN / 未搜索 / 不在 only_file 的行本轮不算
        continue;
      }
      if (idx.empty() || rows[idx.back().front()].file != rows[i].file)
        idx.emplace_back(), exprs.emplace_back(), frames.emplace_back();
      idx.back().push_back(i);
      exprs.back().push_back(req.scope.ft->reparse(rows[i].expr));
      frames.back().push_back(rows[i].frame);
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
    for (size_t f = 0; f < idx.size(); ++f) {
      const Rc rc = eval_file(req, F, eng, idx[f], exprs[f], frames[f]);
      if (rc == Rc::Ok)
        continue;
      if (rc == Rc::Stale) {
        std::lock_guard<std::mutex> lock(mutex);
        for (FactorRow &r : rows)
          r.status = RowStatus::Done;
      }
      return abort_out(rc);
    }
    return true;
  });
}

template <class E>
Rc FactorsService::eval_file(const FactorsRequest &req, FactorFlow &F, E &eng, const std::vector<size_t> &idx,
                             const std::vector<factor::expr::Expr> &exprs, const std::vector<factor::stat::Frame> &frames) {
  TraceN("FactorsEvaluateFile");
  // 该文件的权重 → 一张共享森林 (roots[k] ↔ idx[k]); 同根的只评首个 (探针 ps[k0])
  std::vector<const factor::expr::Expr *> eptr;
  for (const factor::expr::Expr &e : exprs)
    eptr.push_back(&e);
  // Y = 全部持有期 + 全部金额段, 同作用域跨文件复用; X = 本文件的叶 (retain)
  const YSpec ys = y_full(*req.scope.ft, req.scope.max_impact);
  std::vector<RunProbes> ps(idx.size());
  JobOut o;
  std::vector<size_t> done;                                 // 本轮评完的 idx 下标 (森林走完 Ok 才一次回写)
  const auto mark = [&](size_t k, const std::string &why) { // 行 BROKEN (调用方持锁)
    rows[idx[k]].error = why;
    rows[idx[k]].status = RowStatus::Done;
    broken_.fetch_add(1, std::memory_order_relaxed);
  };
  const Rc rc = eval_job(
      eng, F, req.scope, eptr, frames.data(), ys, /*retain=*/true, "Run", [&](int k) -> RunProbes & { return ps[static_cast<size_t>(k)]; },
      cancel_, *this, o,
      [&](const std::vector<std::string> &why) { // 值域已查: 坏根 BROKEN, 好根 Running
        std::lock_guard<std::mutex> lock(mutex);
        for (size_t k = 0; k < idx.size(); ++k)
          if (!why[k].empty())
            mark(k, why[k]);
          else
            rows[idx[k]].status = RowStatus::Running;
      },
      [&](int k0) { // 发布该根的全部权重 (dup 共根: 同一子树, eval_ms 同)
        EvalStat st;
        fill_eval(ps[static_cast<size_t>(k0)], stat_scope(req.scope, o.job.es, o.job.blocks.of(0).y, E::kName), o.eval_ms(k0), st);
        {
          std::lock_guard<std::mutex> lock(mutex);
          for (size_t k = 0; k < idx.size(); ++k) {
            if (o.d.first_root(static_cast<int>(k)) != k0)
              continue;
            FactorRow &r = rows[idx[k]];
            static_cast<EvalStat &>(r) = st;
            r.has_stat = true;
            r.stat_from_file = false;
            r.status = RowStatus::Done;
            done.push_back(k);
          }
        }
        publish();
      });
  if (rc == Rc::Empty) { // 评估期 / A 轴不成 → 该文件各行 BROKEN
    std::lock_guard<std::mutex> lock(mutex);
    for (size_t k = 0; k < idx.size(); ++k)
      mark(k, o.job.why);
  }
  if (rc == Rc::Ok && !done.empty()) { // 回写在锁外 (rows 只本 worker 写, 读不用锁)
    std::vector<const FactorRow *> rs;
    for (size_t k : done)
      rs.push_back(&rows[idx[k]]);
    WriteFactorStats(std::filesystem::path(req.scope.factor_dir) / rows[idx.front()].file, rs);
  }
  publish();
  return rc == Rc::Empty || rc == Rc::Broken ? Rc::Ok : rc;
}

} // namespace GUI::Factors
