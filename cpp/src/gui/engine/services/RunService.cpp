// RunService — 见头文件. include 因子 CPU 后端头 (EvalCpu → TS/CS Cpu.hpp, Stat/Cpu.hpp), 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/services/RunService.hpp"

#include "gui/engine/Engine.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <map>
#include <tuple>
#include <type_traits>

namespace GUI::Engine {

namespace {

using RunProbes = EvalProbes<>;
using DayProbes = EvalProbes<NavP, TurnP>;

DayLine day_of(const DayProbes &p, const EvalStat &st, int hold) {
  const factor::stat::HoldStat *h = st.find_hold(hold);
  assert(h && "逐日序列的持有期须是本层标签组之一");
  const TurnP &tp = p.at<TurnP>();
  DayLine o{.cum = p.at<YearP>().cum[h - st.hold], .nav = p.at<NavP>().out};
  o.turn[0] = static_cast<float>(factor::stat::turn_annual(*h, st.turn_of(*h)));
  for (int j = 0; j < kTails; ++j)
    o.turn[1 + j] = static_cast<float>(factor::stat::turn_annual(*h, tp.acc[j][0]));
  return o;
}

template <class Ps>
Rc eval_file(const FactorScope &sc, FactorFlow &F, std::vector<FactorRow> &rows, std::span<const size_t> idx, const std::function<void()> &on_plan,
             const std::function<void(size_t)> &on_row, DayLines *day) {
  RootSet rs{.hd = keys_of<Holds>(sc.ft->y.labels)};
  for (const size_t i : idx)
    rs.add(*sc.ft, rows[i].expr, rows[i].frame, factor::stat::dir_of(rows[i].sign));
  ForestOut o;
  o.job.who = "Run";
  std::vector<Ps> ps(rs.size());
  if constexpr (std::is_same_v<Ps, DayProbes>)
    for (Ps &p : ps)
      p.template at<NavP>().only = p.template at<TurnP>().only = day->hold;
  std::vector<size_t> fin;
  int k_st = -1;
  EvalStat st{};

  const Rc rc = with_backend<Ps>(F, sc.gpu, [&](auto &eng) {
    return eval_forest(
        eng, F, sc, rs, std::span(ps), o, /*whole=*/false,
        [&](int e, int k, const std::string &why) {
          const size_t ri = idx[static_cast<size_t>(e)];
          FactorRow &r = rows[ri];
          assert(r.status != RowState::Done && "每行至多收尾一次");
          if (!why.empty()) {
            r.error = why;
            r.status = RowState::Done;
          } else {
            if (k != k_st) {
              st = EvalStat{};
              fill_eval(ps[static_cast<size_t>(k)], o.scope(sc), o.eval_ms(k), st);
              k_st = k;
            }
            static_cast<EvalStat &>(r) = st;
            r.has_stat = true;
            r.stat_from_file = false;
            r.status = RowState::Done;
            fin.push_back(ri);
            if constexpr (std::is_same_v<Ps, DayProbes>)
              day->rows[static_cast<size_t>(e)] = day_of(ps[static_cast<size_t>(k)], st, day->hold);
          }
          if (on_row)
            on_row(ri);
        },
        NoOp{},
        [&] {
          if (on_plan)
            on_plan();
        });
  });
  if (rc == Rc::Ok && !fin.empty()) {
    std::vector<const FactorRow *> out;
    out.reserve(fin.size());
    for (const size_t ri : fin)
      out.push_back(&rows[ri]);
    WriteFactorStats(sc.dir, out);
  }
  if constexpr (std::is_same_v<Ps, DayProbes>)
    if (rc == Rc::Ok)
      day->dates = o.job.dm.stat_dates();
  return rc;
}

} // namespace

bool MakeRunRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int years, bool evaluate, bool gpu, RunRequest &req) {
  req = RunRequest{};
  req.spec.evaluate = evaluate;
  return MakeFactorScope(data, std::move(ft), pool, years, gpu, req.scope) || !evaluate;
}

void ScanFactors(const FactorScope &sc, std::vector<FactorRow> &out) {
  TraceN("FactorsScan");
  out.clear();
  const FeatureTable &ft = *sc.ft;
  for (const std::string &file : sc.dir.files())
    ScanFactorFile(sc.dir, file, ft, out);
  std::map<std::tuple<int, int, std::string>, std::string> seen;
  for (FactorRow &w : out) {
    if (w.expr.empty())
      continue;
    if (const auto it = seen.find({w.hold_src, w.sign, w.expr}); it != seen.end())
      w.dup_of = it->second;
    else
      seen.emplace(std::tuple{w.hold_src, w.sign, w.expr}, w.key);
  }
}

Rc EvalFactorFile(const FactorScope &sc, FactorFlow &F, std::vector<FactorRow> &rows, std::span<const size_t> idx,
                  const std::function<void()> &on_plan, const std::function<void(size_t)> &on_row, DayLines *day) {
  TraceN("FactorsEvaluateFile");
  if (idx.empty())
    return Rc::Ok;
  if (!day)
    return eval_file<RunProbes>(sc, F, rows, idx, on_plan, on_row, nullptr);
  day->rows.assign(idx.size(), DayLine{});
  return eval_file<DayProbes>(sc, F, rows, idx, on_plan, on_row, day);
}

bool RunService::run(RunRequest &req) {
  std::vector<FactorRow> work;
  ScanFactors(req.scope, work);
  if (!req.spec.evaluate)
    for (FactorRow &r : work)
      r.status = RowState::Done;

  int nb = 0;
  for (const FactorRow &r : work)
    nb += !r.error.empty();
  {
    std::lock_guard<std::mutex> lock(mutex);
    rows = work;
    current = req;
    message.clear();
  }
  broken_.store(nb, std::memory_order_relaxed);
  publish();
  if (!req.spec.evaluate)
    return true;

  TraceN("FactorsEvaluate");
  std::vector<std::vector<size_t>> files;
  for (size_t i = 0; i < work.size(); ++i) {
    if (!work[i].error.empty() || work[i].expr.empty() || (!req.spec.only_file.empty() && work[i].file != req.spec.only_file)) {
      work[i].status = RowState::Done;
      continue;
    }
    if (files.empty() || work[files.back().front()].file != work[i].file)
      files.emplace_back();
    files.back().push_back(i);
  }
  {
    std::lock_guard<std::mutex> lock(mutex);
    for (size_t i = 0; i < work.size(); ++i)
      rows[i].status = work[i].status;
    if (files.empty()) {
      message = "无有效因子";
      return true;
    }
  }

  FactorFlow F = flow();
  for (const std::vector<size_t> &f : files) {
    Lease l(F);
    const Rc rc = l ? EvalFactorFile(
                          req.scope, F, work, f,
                          [&] {
                            std::lock_guard<std::mutex> lock(mutex);
                            for (const size_t i : f)
                              if (rows[i].status != RowState::Done)
                                rows[i].status = RowState::Running;
                          },
                          [&](size_t i) {
                            {
                              std::lock_guard<std::mutex> lock(mutex);
                              rows[i] = work[i];
                            }
                            if (!work[i].error.empty())
                              broken_.fetch_add(1, std::memory_order_relaxed);
                            publish();
                          })
                    : Rc::Cancel;
    if (!settle(rc)) {
      std::lock_guard<std::mutex> lock(mutex);
      for (FactorRow &r : rows)
        r.status = RowState::Done;
      return false;
    }
  }
  return true;
}

} // namespace GUI::Engine
