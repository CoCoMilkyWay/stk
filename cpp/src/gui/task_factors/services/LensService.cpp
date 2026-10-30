// LensService — 见头文件. include 因子 CPU 后端头 (Stat/Cpu.hpp / Host.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/LensService.hpp"

#include "factor/Stat/Host.hpp"                        // h2f
#include "gui/task_factors/services/FactorsEngine.hpp" // eval_job / Probes / FactorFlow
#include "misc/profiler.hpp"

#include <cassert>
#include <limits>
#include <mutex>
#include <utility>
#include <vector>

namespace GUI::Factors {

namespace {

namespace st = factor::stat;
constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();

// 落地列 (块钩子定, 各根探针共读): cols = 落地列 → 块 A 下标; col_of = 块 A 下标 → 落地列 (−1 = 不落)
struct Land {
  std::vector<int> cols, col_of;
};

// 两端组真持有的格 = label_row 的逐格扩展, 与 Row::grp 同一份判据 (本格所在组即端组, 沿交接链真持有, 收益按续持取名义值).
// k = 块内格下标 (cl.m − m0, m0 = 该持有期标签有效位的块首), row 钩子换成行 / 列
struct EndsExt {
  struct Got {
    size_t k;
    uint8_t e;
    float ret;
  };
  const uint8_t *m0 = nullptr;
  int d = st::kFwd;
  std::vector<Got> got;
  void cell(const factor::cpu::stat::RxCell &cl) {
    const int g = st::grp_of(*cl.code);
    for (int e = 0; e < st::kEnds; ++e) {
      if (g != st::end_group(e ^ d))
        continue;
      const auto in = [g](uint16_t v) { return v != st::kRankNone && st::grp_of(v) == g; };
      if (cl.held(in))
        got.push_back({static_cast<size_t>(cl.m - m0), static_cast<uint8_t>(e), cl.ret(cl.continues(in))});
    }
  }
};

// 一行的逐格扩展: 两端格 + 尾部分层
struct LensExt {
  EndsExt ends;
  TailExt tails;
  void cell(const factor::cpu::stat::RxCell &cl) { ends.cell(cl), tails.cell(cl); }
};

// 一个根的落地: as_root = 秩 / Row / 尾部 / 两端格; as_feat = 根的输入原值. 同一规范串两种条目都有时一个根两份都落
struct LensP : ProbeBase {
  static constexpr uint8_t need = kNeedRows | kNeedRx | kNeedAlpha;
  using Ext = LensExt;
  const Land *land = nullptr;
  bool as_root = false, as_feat = false;
  const uint8_t *m0 = nullptr;
  int d = st::kFwd, A = 0;
  double cap = 0.0;
  LensRoot r;
  LensFeat f;
  std::vector<std::vector<LensCell>> by_row; // [T] row 钩子收, root 按行序并进 r.cells
  static double bytes(const Shape &sh, int) {
    return (sizeof(uint16_t) + sizeof(float)) * static_cast<double>(sh.n()) + sizeof(st::Row) * static_cast<double>(sh.T);
  }
  Ext ext() const { return {{m0, d, {}}, {kTailThr.v, cap, d}}; }
  void open(const Ctx &c) {
    const st::Span s = c.s();
    assert(s.t0_abs == 0 && s.t_lo == 0 && s.t_back == 0 && s.T == c.c.rows_all() && "整期一块");
    assert(c.L.hd.n == 1 && "单持有期");
    m0 = c.L.l[0].m, d = c.d, A = s.A, cap = c.L.cap;
    if (as_root) {
      r.rows.assign(static_cast<size_t>(s.T), st::Row{});
      r.tails.assign(static_cast<size_t>(s.T), TailRow{});
      by_row.assign(static_cast<size_t>(s.T), {});
    }
  }
  void row(int, int t, const st::Row &w, const Ext &e) {
    if (!as_root)
      return;
    const size_t base = static_cast<size_t>(t) * A;
    r.rows[static_cast<size_t>(t)] = w;
    e.tails.finish(w, r.tails[static_cast<size_t>(t)]);
    for (const EndsExt::Got &g : e.ends.got) {
      assert(g.k >= base && g.k < base + static_cast<size_t>(A) && "格不在本行");
      if (const int a = land->col_of[g.k - base]; a >= 0)
        by_row[static_cast<size_t>(t)].push_back({t, a, g.e, g.ret});
    }
  }
  void root(const Ctx &c, const RootView &v) {
    const st::Span s = c.s();
    const size_t n = land->cols.size();
    if (as_root) {
      r.rx.resize(static_cast<size_t>(s.T) * n);
      c.ex.rows(s.T, [&](int t0, int t1, int) {
        for (int t = t0; t < t1; ++t)
          for (size_t j = 0; j < n; ++j) {
            const uint16_t x = v.rx[static_cast<size_t>(t) * A + static_cast<size_t>(land->cols[j])];
            r.rx[static_cast<size_t>(t) * n + j] = x == st::kRankNone || d == st::kFwd ? x : static_cast<uint16_t>(st::kRankMax - x);
          }
      });
      for (const std::vector<LensCell> &b : by_row)
        r.cells.insert(r.cells.end(), b.begin(), b.end());
      by_row = {};
    }
    if (as_feat) {
      f.v.resize(static_cast<size_t>(s.T) * n);
      c.ex.rows(s.T, [&](int t0, int t1, int) {
        for (int t = t0; t < t1; ++t)
          for (size_t j = 0; j < n; ++j) {
            const size_t k = static_cast<size_t>(t) * A + static_cast<size_t>(land->cols[j]);
            f.v[static_cast<size_t>(t) * n + j] = v.alpha.m[k] ? v.alpha.v[k] : kNaN;
          }
      });
    }
  }
};
using LensProbes = Probes<LensP>;

// 块钩子: 落地列 + 门控 / 标签 / 逐行池内标签均值; 条目终局: 坏根的原因
struct LensHooks : HookBase {
  static constexpr bool whole() { return true; }
  const LensSpec &sp;
  const JobOut &o;
  Land &land;
  LensResult &res;
  std::vector<std::string> &why; // [条目]
  void block(const Block &c, size_t, double) {
    const st::Span s = c.span();
    const size_t T = static_cast<size_t>(s.T), A = static_cast<size_t>(s.A);
    const PoolAxis &ax = o.job.dm.rs.axis;
    assert(ax.size() == A && "块 A 轴 = 评估域池轴");
    assert(c.y.hd.n == 1 && c.y.hd.k[0] == sp.hold && "Y 只装会话持有期");
    land.cols.clear();
    land.col_of.assign(A, -1);
    if (sp.asset != UINT32_MAX) {
      if (const size_t a = ax.sub_of(sp.asset); a < A)
        land.cols.push_back(static_cast<int>(a));
    } else {
      std::vector<uint8_t> any(A, 0);
      for (size_t k = 0; k < T * A; ++k)
        any[k % A] |= c.y.gate[k];
      for (size_t a = 0; a < A; ++a)
        if (any[a])
          land.cols.push_back(static_cast<int>(a));
    }
    const size_t n = land.cols.size();
    for (size_t j = 0; j < n; ++j)
      land.col_of[static_cast<size_t>(land.cols[j])] = static_cast<int>(j);
    const LegPlane &lb = c.y.labels[0];
    const uint16_t *lv = lb.v[st::kLong].data();
    res.gate.assign(T * n, 0);
    res.label.assign(T * n, kNaN);
    res.mkt.assign(T, kNaN);
    for (size_t t = 0; t < T; ++t) {
      double sum = 0.0;
      size_t cnt = 0;
      for (size_t a = 0, k = t * A; a < A; ++a, ++k)
        if (c.y.gate[k] && lb.m[k])
          sum += factor::cpu::stat::h2f(lv[k]), ++cnt;
      if (cnt > 0)
        res.mkt[t] = static_cast<float>(sum / static_cast<double>(cnt));
      for (size_t j = 0; j < n; ++j) {
        const size_t k = t * A + static_cast<size_t>(land.cols[j]);
        res.gate[t * n + j] = c.y.gate[k];
        if (lb.m[k])
          res.label[t * n + j] = factor::cpu::stat::h2f(lv[k]);
      }
    }
  }
  void fail(int e, const std::string &w) { why[static_cast<size_t>(e)] = w; }
};

} // namespace

LensService::LensService() { Start(); }
LensService::~LensService() { Stop(); }

bool LensService::run(LensRequest &req) {
  {
    std::lock_guard<std::mutex> lock(mutex);
    message.clear();
  }
  publish();
  LensResult res;
  const bool ok = eval(req, flow(), res);
  flow().end(Life::Worker);
  if (ok) {
    std::lock_guard<std::mutex> lock(mutex);
    result = std::move(res);
  }
  publish();
  return ok;
}

bool LensService::eval(const LensRequest &req, FactorFlow &F, LensResult &res) {
  TraceN("LensEvaluate");
  const FactorScope &sc = req.scope;
  const LensSpec &sp = req.spec;
  assert(!sp.roots.empty() || !sp.feats.empty());
  const Holds hd = keys_of<Holds>(std::vector<int>{sp.hold});
  RootSet rs;
  for (const FactorRef &f : sp.roots)
    rs.add(*sc.ft, f.expr, f.frame, f.dir, hd);
  for (const FactorRef &f : sp.feats)
    rs.add(*sc.ft, f.expr, f.frame, f.dir, hd);
  const size_t n_roots = sp.roots.size();
  Land land;
  std::vector<LensProbes> ps(rs.size());
  for (size_t e = 0; e < rs.of.size(); ++e) {
    LensP &p = ps[static_cast<size_t>(rs.of[e])].at<LensP>();
    (e < n_roots ? p.as_root : p.as_feat) = true;
    p.land = &land;
  }
  JobOut o;
  o.job.life = Life::Worker;
  o.job.who = "Lens";
  std::vector<std::string> why(rs.of.size());
  const Rc rc = F.with_engine<LensProbes>(false, [&](auto &eng) {
    return eval_job(eng, F, sc, rs, [&](int k) -> LensProbes & { return ps[static_cast<size_t>(k)]; }, o, LensHooks{{}, sp, o, land, res, why});
  });
  if (!settle(rc))
    return false;
  if (rc != Rc::Ok) {
    res = LensResult{};
    res.error = o.job.why;
    return true;
  }
  if (land.cols.empty()) {
    res = LensResult{};
    res.error = sp.asset != UINT32_MAX ? "该资产不在本池特征库的资产轴上" : "评估期内池里没有资产";
    return true;
  }
  res.dates = o.job.dm.dates;
  for (const int a : land.cols)
    res.ids.push_back(o.job.dm.rs.axis.global(static_cast<size_t>(a)));
  for (size_t e = 0; e < rs.of.size(); ++e) {
    const LensP &p = ps[static_cast<size_t>(rs.of[e])].at<LensP>();
    if (e < n_roots)
      res.roots.push_back(why[e].empty() ? p.r : LensRoot{why[e], {}, {}, {}, {}});
    else
      res.feats.push_back(why[e].empty() ? p.f : LensFeat{why[e], {}});
  }
  return true;
}

} // namespace GUI::Factors
