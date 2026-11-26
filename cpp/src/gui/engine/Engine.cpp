// Engine — 见头文件 (engine_space / open_job / plan_forest / eval_baskets). include 因子 CPU 后端头, 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/Engine.hpp"

#include "features/Storage/FeatureLib.hpp" // pool_id

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <memory>
#include <string>

namespace GUI::Engine {

EngineSpace &engine_space(Store &S) {
  if (!S.exec())
    S.set_exec(std::make_unique<EngineSpace>(S.ex));
  return *static_cast<EngineSpace *>(S.exec());
}

namespace {

std::string out_of_dom(misc::exec::ForkJoin &ex, const factor::check::Plane &p, factor::Dom dom) {
  const size_t n = p.v.size();
  std::vector<size_t> first(static_cast<size_t>(ex.threads()), n);
  ex.rows(static_cast<int>(n), [&](int i0, int i1, int tid) {
    for (size_t i = static_cast<size_t>(i0); i < static_cast<size_t>(i1); ++i)
      if (p.m[i] && !factor::expr::dom_holds(dom, p.v[i])) {
        first[static_cast<size_t>(tid)] = i;
        return;
      }
  });
  const size_t i = *std::min_element(first.begin(), first.end());
  if (i == n)
    return {};
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%g", static_cast<double>(p.v[i]));
  return buf;
}

// 整期一块装不下: 评估年数逐年减到装得下 (近期子期 = 整期的后缀), 记作用域年数上限 (UI 随之封顶) 并以 Empty 收场
template <class B>
bool fit_years(FactorFlow &F, const FactorScope &sc, Job &j, B &&bud) {
  const double host = F.S.budget();
  const double vram = bud().dev > 0.0 ? F.S.vram() : 0.0;
  const auto fits = [&] {
    const Budget b = bud();
    const double n = cells_of(j.dm, j.ft->seg);
    return b.host(n) <= host && (b.dev <= 0.0 || b.vram(n) <= vram);
  };
  if (fits())
    return true;
  assert(sc.years > 0 && "全期整期一块装不下内存 / 显存预算");
  const size_t days = j.dm.stat_days();
  FactorScope s = sc;
  for (s.years = sc.years - 1; s.years >= 1; --s.years) {
    std::string why;
    [[maybe_unused]] const bool ok = domain_of(*sc.ft, s, j.spec.x, SIZE_MAX, j.dm, why);
    assert(ok && "近期子期是整期的后缀, 评估域必成");
    if (fits())
      break;
  }
  assert(s.years >= 1 && "1 年的整期装载都放不下内存 / 显存 (因子层太细)");
  cap_years(sc.pool, sc.lv, s.years);
  j.why = "评估期近 " + std::to_string(sc.years) + " 年 (" + std::to_string(days) + " 天) 整段装不下内存 / 显存预算, 本作用域最多 " + std::to_string(s.years) + " 年";
  std::fprintf(stderr, "[factors] %s: %s, 评估年数封顶 %d 年\n", j.who, j.why.c_str(), s.years);
  return false;
}

} // namespace

Need basket_need(const FeatureTable &ft) {
  const std::array<LeafId, kPoolVals> val = pool_val_leaves(ft);
  const Holds hd = Holds::of(hold_bit(kHoldT1));
  return {{val.begin(), val.end()}, hd, Segs::None, false, bands_of(0, hd, ft.seg), [](const Job &j) {
            return Budget{.work = static_cast<double>(sizeof(PoolMask)), .fixed = BasketPass::fixed_bytes(j.dm)};
          }};
}

Rc open_job(FactorFlow &F, const FactorScope &sc, Need n, Job &j) {
  assert(sc.ft && sc.lv == sc.ft->lv && "作用域层与字段表层不符");
  assert(n.bud && "Need::bud 未给");
  j.ft = sc.ft.get();
  j.spec.x = std::move(n.x);
  if (!domain_of(*sc.ft, sc, j.spec.x, SIZE_MAX, j.dm, j.why))
    return Rc::Empty;
  open_libs(sc, F.S.lr);
  j.spec.y = y_spec(*j.ft, j.dm, n.hd, n.segs, sc.max_impact);
  j.spec.ry = n.ry;
  j.bands = n.bands;
  const auto bud = [&] {
    Budget b = n.bud(j);
    b.load += cell_bytes(j.spec, j.spec.x.size());
    return b;
  };
  if (j.bands.front < 0 && !fit_years(F, sc, j, bud))
    return Rc::Empty;
  j.b = bud();
  j.P = plan_chunks(j.dm, j.ft->seg, j.b, j.bands, F.S.budget(), j.b.dev > 0.0 ? F.S.vram() : 0.0);
  return Rc::Ok;
}

int plan_forest(misc::exec::ForkJoin &ex, const Block &c, const Forest &F, std::span<std::string> why, std::vector<uint8_t> &need) {
  const factor::Dag &d = F.d;
  assert(!d.roots.empty() && d.feats.size() == F.leaves.size());
  assert(why.size() == d.roots.size() && "why = 各根的已知坏因 (空 = 未坏)");
  const size_t N = d.nodes.size();
  std::vector<std::string> err(N);
  for (size_t i = 0; i < N; ++i) {
    const factor::DagNode &nd = d.nodes[i];
    if (nd.op < 0 || nd.fu.n > 0)
      continue;
    const factor::expr::OpInfo &o = factor::expr::kOps[nd.op];
    assert(nd.n_in == o.arity || factor::expr::nary(o));
    for (int s = 0; s < nd.n_in && err[i].empty(); ++s) {
      const factor::DagNode &g = d.nodes[static_cast<size_t>(nd.in[s])];
      const factor::Dom in = factor::expr::in_of(o, s);
      if (g.op >= 0 || !factor::dom_strict(in))
        continue;
      const size_t f = static_cast<size_t>(g.feat);
      const Leaf *lf = c.x.find(F.leaves[f]);
      assert(lf && "叶不在块上 (BlockSpec::x)");
      const auto [it, fresh] = lf->dom.try_emplace(in);
      if (fresh)
        it->second = out_of_dom(ex, *lf, in);
      if (!it->second.empty())
        err[i] = std::string(o.name) + " 第 " + std::to_string(s + 1) + " 元要求 " + factor::dom_name(in) + ", 特征 " + d.feats[f] + " 含越界值 " + it->second;
    }
  }
  need.assign(N, 0);
  int good = 0;
  for (size_t k = 0; k < d.roots.size(); ++k) {
    if (!why[k].empty())
      continue;
    const std::vector<int> sub = d.subtree(d.roots[k]);
    for (const int i : sub)
      if (!err[static_cast<size_t>(i)].empty()) {
        why[k] = err[static_cast<size_t>(i)];
        break;
      }
    if (!why[k].empty())
      continue;
    ++good;
    for (const int i : sub)
      need[static_cast<size_t>(i)] = 1;
  }
  return good;
}

Rc eval_baskets(FactorFlow &F, const FactorScope &sc, Job &j, Baskets &out) {
  assert(sc.ft && sc.ft->seg == 1 && pool_id(sc.pool) == kAllPool && "池篮子在 all 池日层评估域");
  assert(sc.ft->y.has(kHoldT1) && "篮子须有 T+1 标签 (日层: 池指数 C(行) → C(D+1))");
  if (const Rc rc = open_job(F, sc, basket_need(*sc.ft), j); rc != Rc::Ok)
    return rc;
  assert(j.dm.rs.has(kAllBar));
  std::shared_ptr<const pool::Picks> pk = pool::picks(j.dm.rs.libs[kAllBar], &F.cancel);
  if (!pk) {
    const Rc rc = rc_of(false, F.S.lr);
    if (rc == Rc::Stale)
      F.S.clear();
    return rc;
  }
  BasketPass pass(j.dm, std::move(pk), pool_val_leaves(*sc.ft));
  const size_t n = j.P.chunks.size();
  if (const Rc rc = for_blocks(F, j, [&](const Block &c, size_t ci, bool, double) {
        F.prog.begin(Phase::Running, n);
        F.prog.publish(ci);
        pass.chunk(F.S.ex, c);
        F.prog.publish(1);
        return Rc::Ok;
      });
      rc != Rc::Ok)
    return rc;
  out = pass.finish(F.S.ex);
  return Rc::Ok;
}

} // namespace GUI::Engine
