// OperatorsService — 见头文件. 本 TU include 三后端头 (Stream / Cpu; GPU 经 GpuRun.hpp),
// 依赖受控浮点: CMake 里已列入 PRECISE_MATH 源 (-fno-fast-math), 与 op_check 同待遇.
#include "gui/task_factors/services/OperatorsService.hpp"

#include "factor/TS/Cpu.hpp"    // IWYU pragma: keep
#include "factor/TS/Stream.hpp" // IWYU pragma: keep

#include "factor/CS/Cpu.hpp"    // IWYU pragma: keep
#include "factor/CS/Stream.hpp" // IWYU pragma: keep

#include "factor/Expr.hpp" // kOps / operand_tex (签名 LaTeX 由表列生成)
#include "factor/GpuRun.hpp"
#include "factor/OpTable.hpp"
#include "factor/Search/Check.hpp"
#include "factor/Stat/Check.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>

namespace GUI::Factors {

// 一轮的常驻件 (worker 栈上, 轮末析构释放):
//   输入  按 (槽位 x/y/z, Gen) 懒造一次 (并行 fill, 主 rng 派生) + 上传显存一次, 全表复用; 最多 8 张 ≈ 960MB 宿主
//   输出  cpu 参考 / stream / gpu 三块, 开轮 resize 预触页, 跨算子只 ensure 不清 (契约: 后端写满每格)
//   GPU   常驻 Session (输出平面 / 工作区在里面), gpu 输出宿主缓冲 pin 住走 DMA
// 算子之间除一张输出的 D2H 与 compare 外不留 overhead, cpu_ms / stream_ms 里也不再混首次触页
struct RoundCtx {
  static constexpr int kSlots = 3, kGens = 4; // Gen: NORM / POS / SMALL / GROUP
  const OperatorsRequest &rq;
  std::mt19937 rng;
  size_t n;
  factor::check::Plane host[kSlots][kGens];
  factor::gpu::DevPlane *dev[kSlots][kGens] = {};
  factor::gpu::Session *gpu = nullptr;
  factor::check::Plane ref, got, g;
  factor::check::Plane gate; // 截面池掩码 (CS 算子; fill_gate, 首次用到才造 + 上传)
  const factor::gpu::DevPlane *dgate = nullptr;

  RoundCtx(const OperatorsRequest &r, bool use_gpu) : rq(r), rng(r.seed), n(static_cast<size_t>(r.times) * r.A) {
    ref.resize(n), got.resize(n), g.resize(n);
    if (use_gpu) {
      gpu = factor::gpu::session_open(n);
      factor::gpu::pin(g.v.data(), n * sizeof(float));
      factor::gpu::pin(g.m.data(), n);
    }
  }
  ~RoundCtx() {
    if (gpu) {
      factor::gpu::unpin(g.v.data());
      factor::gpu::unpin(g.m.data());
      factor::gpu::session_close(gpu); // 上传过的输入平面随会话释放
    }
  }
  RoundCtx(const RoundCtx &) = delete;
  RoundCtx &operator=(const RoundCtx &) = delete;

  // 槽位 slot 的 Gen 平面: 首次用到才造 (+ 上传), 之后整轮复用. PLAIN profile (与 op_check 的 GUI 口径同)
  const factor::check::Plane &plane(int slot, factor::check::Gen gen) {
    assert(slot >= 0 && slot < kSlots);
    factor::check::Plane &p = host[slot][static_cast<int>(gen)];
    if (p.v.empty()) {
      factor::check::fill(p, gen, factor::check::Profile::PLAIN, rq.times, rq.A, rng);
      if (gpu)
        dev[slot][static_cast<int>(gen)] = factor::gpu::upload(gpu, p.v.data(), p.m.data());
    }
    return p;
  }
  const factor::gpu::DevPlane *dplane(int slot, factor::check::Gen gen) {
    plane(slot, gen);
    return dev[slot][static_cast<int>(gen)];
  }
  const factor::check::Plane &gate_plane() {
    if (gate.v.empty()) {
      factor::check::fill_gate(gate, factor::check::Profile::PLAIN, rq.times, rq.A, rng);
      if (gpu)
        dgate = factor::gpu::upload_mask(gpu, gate.m.data());
    }
    return gate;
  }
};

namespace {

using Clock = std::chrono::steady_clock;
double ms_since(Clock::time_point t0) {
  return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// 本轮实际参数: 参数列含 d 才吃默认窗长表 (否则与 op_check 同给 1); k/k2 按 Check.hpp kKParams
factor::Param param_of(const OperatorRow &row) {
  factor::Param p;
  p.d = std::strchr(row.params, 'd') != nullptr ? factor::check::default_d(row.e_name) : 1;
  factor::check::set_k(row.e_name, p);
  return p;
}

// 一个算子: 从 RoundCtx 取输入 (配方按算子, 元数不到的槽位空) → cpu / stream / gpu 各计时 → 对拍
template <class S, class C, int AR, factor::T W, bool IS_CS>
void run_op(RoundCtx &R, OperatorRow &row) {
  using namespace factor::check;
  const std::string nm = row.e_name;
  const Recipe rc = recipe_of(nm);
  const Gen gens[3] = {rc.x, rc.y, rc.z};
  const Plane *pl[3] = {};
  const factor::gpu::DevPlane *dp[3] = {};
  for (int s = 0; s < AR; ++s) {
    pl[s] = &R.plane(s, gens[s]);
    dp[s] = R.gpu ? R.dplane(s, gens[s]) : nullptr;
  }
  Data d;
  d.T = R.rq.times, d.A = R.rq.A;
  d.x = pl[0], d.y = pl[1], d.z = pl[2];
  if constexpr (IS_CS)
    d.g = &R.gate_plane();

  const factor::Param &p = row.param; // 复位阶段已按请求填好 (见 param_of)

  Clock::time_point t0 = Clock::now();
  if constexpr (IS_CS)
    run_cpu_cs<C>(d, p, AR, R.ref);
  else
    run_cpu<C>(d, p, AR, R.ref);
  row.cpu_ms = ms_since(t0);

  t0 = Clock::now();
  if constexpr (IS_CS)
    run_stream_cs<S>(d, p, AR, R.got);
  else
    run_stream_ts<S, AR, W>(d, p, R.got);
  row.stream_ms = ms_since(t0);
  row.cpu_chk = compare(R.ref, R.got, tol_of(nm, false, Profile::PLAIN));

  if (R.gpu) {
    double kms = -1; // 纯 kernel (cudaEvent); 输入已常驻, 这里只剩 kernel + 一张输出 D2H
    if constexpr (IS_CS)
      factor::gpu::run_cs(R.gpu, row.e_name, dp[0], dp[1], dp[2], R.dgate, R.g.v.data(), R.g.m.data(), d.T, d.A, p, &kms);
    else
      factor::gpu::run_ts(R.gpu, row.e_name, dp[0], dp[1], dp[2], R.g.v.data(), R.g.m.data(), d.T, d.A, p, &kms);
    row.gpu_ms = kms;
    row.gpu_chk = compare(R.ref, R.g, tol_of(nm, true, Profile::PLAIN));
  }
}

// ---- 评估算子跑手共用件 ----

// 多项对拍并进一个表格 Diff (计数相加, worst 取大, worst_at 取首个坏项)
void merge_diff(factor::check::Diff &into, const factor::check::Diff &d) {
  into.mask_bad += d.mask_bad;
  into.val_bad += d.val_bad;
  into.compared += d.compared;
  if (d.worst > into.worst)
    into.worst = d.worst;
  if (into.worst_at < 0)
    into.worst_at = d.worst_at;
}

// detail 文本按行追加 (printf 风格)
__attribute__((format(printf, 2, 3))) void detail_line(std::string &s, const char *fmt, ...) {
  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  const int n = std::vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  assert(n >= 0 && n < static_cast<int>(sizeof(buf)));
  if (!s.empty())
    s += '\n';
  s.append(buf, static_cast<size_t>(n));
}
void detail_prep(std::string &s, double cpu_ms, double gpu_ms) {
  if (gpu_ms >= 0)
    detail_line(s, "prep (标签 rank 预处理, 常驻期一次, 不进耗时列): cpu %.3f ms, gpu %.3f ms", cpu_ms, gpu_ms);
  else
    detail_line(s, "prep (标签 rank 预处理, 常驻期一次, 不进耗时列): cpu %.3f ms", cpu_ms);
}
constexpr factor::stat::Frame kFrames[2] = {factor::stat::Frame::CS, factor::stat::Frame::TS};

// Stat 评估算子: 两口径 (CS / TS) 各造数 (PLAIN, 与 op_check 同口径) → cpu (全核) / gpu 各计时 → gpu 对 cpu 一级 + 二级对拍.
// cpu 即 golden (cpu_chk 空). 表列耗时 = 两口径 eval 之和; detail = prep 耗时 (CS 那轮) + 两口径每持有期的 cpu 侧二级汇总
void run_stat(RoundCtx &R, OperatorRow &row) {
  using namespace factor::stat::check;
  const bool has_gpu = factor::gpu::available();
  row.gpu_ms = has_gpu ? 0 : -1;
  double prep_cpu = 0, prep_gpu = -1;
  std::string holds;
  for (const factor::stat::Frame fr : kFrames) {
    std::mt19937 rng(R.rq.seed);
    Data d;
    make(d, fr, Profile::PLAIN, R.rq.times, R.rq.A, rng);
    Result cpu;
    run_cpu(d, cpu, cpu_threads());
    row.cpu_ms += cpu.eval_ms;
    if (fr == factor::stat::Frame::CS)
      prep_cpu = cpu.prep_ms;
    detail_line(holds, "%s", factor::stat::frame_name(fr));
    for (int i = 0; i < d.hd.n; ++i) {
      const factor::stat::HoldStat &h = cpu.stat[static_cast<size_t>(i)];
      detail_line(holds,
                  "  h=%-5s n=%d/%d  rIC %+.4f std %.4f IR %+.3f t %+.2f pos %.2f skew %+.2f kurt %+.2f | LS %+.5f t %+.2f pos %.2f "
                  "SR %+.2f β %+.3f | mono %+.3f | rAC %+.3f",
                  factor::stat::hold_name(h.hold).c_str(), h.n, h.n_ac, h.ic_mean, h.ic_std, h.icir, h.ic_t, h.ic_pos, h.ic_skew, h.ic_kurt,
                  h.ls_mean, h.ls_t, h.ls_pos, h.sharpe, h.beta, h.mono, h.rank_ac);
    }
    if (!has_gpu)
      continue;
    Result gpu;
    gpu.rows.assign(cpu.rows.size(), factor::stat::Row{});
    std::vector<factor::stat::Label> lab(static_cast<size_t>(d.hd.n));
    for (int i = 0; i < d.hd.n; ++i)
      lab[static_cast<size_t>(i)] = {d.lab[static_cast<size_t>(i)].lv.data(), d.lab[static_cast<size_t>(i)].sv.data(),
                                     d.lab[static_cast<size_t>(i)].m.data()};
    factor::gpu::run_stat(d.x.v.data(), d.x.m.data(), d.g.m.data(), fr, d.T, d.A, d.hd, lab.data(), gpu.rows.data(), &gpu.prep_ms,
                          &gpu.eval_ms);
    summarize_all(gpu, d);
    const Tol tol = tol_of(Profile::PLAIN);
    row.gpu_ms += gpu.eval_ms;
    if (fr == factor::stat::Frame::CS)
      prep_gpu = gpu.prep_ms;
    merge_diff(row.gpu_chk, compare_rows(cpu.rows.data(), gpu.rows.data(), cpu.rows.size(), tol));
    merge_diff(row.gpu_chk, compare_stat(cpu.stat.data(), gpu.stat.data(), d.hd.n, tol));
  }
  detail_prep(row.detail, prep_cpu, prep_gpu);
  row.detail += '\n' + holds;
}

// Search 批评估算子 (与 op_check check_search 同口径): 两口径各造数 (Stat 造数 + kCands 张派生候选 + 成本); golden = Stat CPU
// 每候选 HoldStat + 朴素成本池化. cpu / gpu 各算 整块 + 两块 (带 carry): cpu_chk = cpu 整块 vs golden + 两块 merge vs 整块;
// gpu_chk = gpu 整块 vs cpu 整块 + 两块 merge vs 整块. 表列耗时 = 两口径整块 eval 之和 (kCands 张候选一批; 分块两趟只对拍不计时);
// detail = prep 耗时 (CS 那轮) + 两口径每候选的 cpu 侧得分 (Objective 默认: 净多空 = 毛 − 池化成本)
void run_search(RoundCtx &R, OperatorRow &row) {
  namespace sk = factor::search::check;
  const bool has_gpu = factor::gpu::available();
  const int threads = factor::stat::check::cpu_threads();
  row.gpu_ms = has_gpu ? 0 : -1;
  double prep_cpu = 0, prep_gpu = -1;
  std::string scores;
  for (const factor::stat::Frame fr : kFrames) {
    std::mt19937 rng(R.rq.seed);
    sk::Data d;
    sk::make(d, fr, sk::Profile::PLAIN, R.rq.times, R.rq.A, rng);
    const int T = d.base.T, A = d.base.A, H = d.base.hd.n;
    const factor::stat::Holds &hd = d.base.hd;
    const sk::Tol tol = factor::stat::check::tol_of(sk::Profile::PLAIN);
    const sk::Split sp = sk::split_of(T, hd);
    // 一个后端: 整块 vs golden (ref), 两块 merge vs 整块
    const auto judge = [&](factor::check::Diff &into, const sk::Ref *ref, const factor::search::Result *full,
                           const factor::search::Result *c1, const factor::search::Result *c2) {
      for (int c = 0; c < sk::kCands; ++c) {
        merge_diff(into, sk::compare(ref[c], full[c], hd, tol));
        factor::search::Result m = c1[c];
        m.merge(c2[c], hd.n);
        sk::Ref rf;
        sk::to_ref(full[c], hd, rf);
        merge_diff(into, sk::compare(rf, m, hd, tol));
      }
    };
    sk::Ref ref[sk::kCands];
    for (int c = 0; c < sk::kCands; ++c)
      sk::reference(d, c, threads, ref[c]);
    factor::search::Result full[sk::kCands], c1[sk::kCands], c2[sk::kCands];
    {
      double prep_ms = 0, eval_ms = 0;
      sk::run_cpu(d, threads, 0, T, 0, full, &prep_ms, &eval_ms);
      row.cpu_ms += eval_ms;
      if (fr == factor::stat::Frame::CS)
        prep_cpu = prep_ms;
      sk::run_cpu(d, threads, 0, sp.T1, 0, c1);
      sk::run_cpu(d, threads, sp.s, T - sp.s, sp.t_lo, c2);
      judge(row.cpu_chk, ref, full, c1, c2);
    }
    detail_line(scores, "%s", factor::stat::frame_name(fr));
    for (int c = 0; c < sk::kCands; ++c) {
      const factor::search::Score s = factor::search::score_of(full[c], hd, factor::search::Objective{});
      const factor::search::CostAcc &ca = full[c].cost[s.best >= 0 ? s.best : 0];
      detail_line(scores, "  c%d  best h=%-5s ls_net/yr %+.4f ic %+.4f score %+.3g | turn hi %.3f lo %.3f  buy %.5f (%lld) sell %.5f (%lld)", c,
                  s.best >= 0 ? factor::stat::hold_name(hd.h[s.best]).c_str() : "-", s.ls_net, s.ic, s.score, ca.turn_hi(), ca.turn_lo(),
                  ca.buy_mean(), ca.e_hi, ca.sell_mean(), ca.e_lo);
    }
    if (!has_gpu)
      continue;
    {
      std::vector<factor::stat::Label> lab(static_cast<size_t>(H));
      const float *xv[sk::kCands];
      const uint8_t *xm[sk::kCands];
      const auto run = [&](int row0, int rows, int t_lo, factor::search::Result *out, double *prep_ms, double *eval_ms) {
        const size_t off = static_cast<size_t>(row0) * A;
        for (int i = 0; i < H; ++i) {
          const factor::stat::check::LabelHost &L = d.base.lab[static_cast<size_t>(i)];
          lab[static_cast<size_t>(i)] = {L.lv.data() + off, L.sv.data() + off, L.m.data() + off};
        }
        for (int c = 0; c < sk::kCands; ++c)
          xv[c] = d.cand[c].v.data() + off, xm[c] = d.cand[c].m.data() + off;
        const factor::search::Cost cost{d.buy.data() + off, d.sell.data() + off, d.cm.data() + off};
        factor::gpu::run_search(sk::kCands, xv, xm, d.base.g.m.data() + off, fr, rows, A, row0, t_lo, hd, lab.data(), cost, out, prep_ms,
                                eval_ms);
      };
      sk::Ref cpu_ref[sk::kCands]; // gpu 的 golden = cpu 整块
      for (int c = 0; c < sk::kCands; ++c)
        sk::to_ref(full[c], hd, cpu_ref[c]);
      factor::search::Result gfull[sk::kCands], gc1[sk::kCands], gc2[sk::kCands];
      double prep_ms = 0, eval_ms = 0;
      run(0, T, 0, gfull, &prep_ms, &eval_ms);
      row.gpu_ms += eval_ms;
      if (fr == factor::stat::Frame::CS)
        prep_gpu = prep_ms;
      run(0, sp.T1, 0, gc1, nullptr, nullptr);
      run(sp.s, T - sp.s, sp.t_lo, gc2, nullptr, nullptr);
      judge(row.gpu_chk, cpu_ref, gfull, gc1, gc2);
    }
  }
  detail_prep(row.detail, prep_cpu, prep_gpu);
  row.detail += '\n' + scores;
}

} // namespace

OperatorsService::OperatorsService() {
  // 行序 = 全局 idx (UI 默认序 / operators.json 的 idx 都按它): 评估算子 Stat, Search 在前 (挖掘的第一道闸, 每轮先验; Search 的
  // golden 是 Stat, 跟在后), 之后 = OpTable 表序. 评估算子不在 OpTable: T / A / Kernel 空, 无 stream 后端.
  // operand / operator 与 OpTable 同规格 (单行, 不比表内最长的长, 否则整表撑宽): 持有期集合等细节只进 doc
  {
    OperatorRow s; // 二元 = 因子 x + 每持有期一组标签 y_h; 输出 = 每持有期一组实数汇总
    s.e_name = "Stat";
    s.c_name = "评估";
    s.arity = 2;
    s.params = "";
    s.operand = R"tex(x, y_h \in \mathbb{R})tex";
    s.out = factor::Dom::REAL;
    s.op = R"tex(\mathrm{rIC},\,\mathrm{IR},\,G_{20},\,\mathrm{LS},\,\mathrm{SR},\,\beta,\,\mathrm{mono},\,\mathrm{rAC})tex";
    s.note = "因子评估: 每 (h, t) 的 IC / 20 组均值 / 多空 / rank-AC, 再沿 t 汇总; 挖掘的第一道闸, 每轮先跑";
    s.doc = "因子评估算子 (factor/Stat): 因子 x + 每持有期 h 一组 fp16 标签 y_h → 每 (h, t) 的 IC / 20 组和 / 顶底多空 / rank-AC, 再沿 t 汇总 (组沿 t 池化).\n"
            "持有期集合 = Stat/Check.hpp kHolds (分钟档 / close / T+N). 标签统一超额 (减截面均值).\n"
            "两口径: CS 每 t 截面 rank x, 任一组空行无效; TS x 已是自身 5 日分位, 直接量化, 组可空.\n"
            "无流式后端, cpu 即 golden (CPU 列不对拍); GPU 列 Diff = 两口径 × (一级 rows + 二级 stat) gpu ↔ cpu 合并.\n"
            "表列耗时 = 两口径 eval 之和 (每因子一次: rank/量化 + 逐持有期一遍 A 轴), 不含 prep (标签 rank 预处理, 见下).\n"
            "造数: long = 0.3·z + 噪声 (IC ≈ 0.3; TS 的 x = Φ(z)). ic_t / ls_t 按 n/h 折算 (相邻 h 行标签重叠); Sharpe 以持有期为一期年化;\n"
            "rank-AC 的 lag = h; 段末 h+3 行 (持有到收盘) 不计标签统计.";
    s.classified = false;
    rows.push_back(s);
    runners_.push_back(&run_stat);
  }
  {
    OperatorRow s; // 三元 = 一批候选 x_c + 每持有期一组标签 y_h + 成本 κ (共用); 输出 = 每候选每持有期的实数累加
    s.e_name = "Search";
    s.c_name = "批评估";
    s.arity = 3;
    s.params = "";
    s.operand = R"tex(x_c, y_h \in \mathbb{R},\; \kappa \in \mathbb{R}_{\ge 0})tex";
    s.out = factor::Dom::REAL;
    s.op = R"tex(\Sigma_t\,\mathrm{row}_t(x_c, y_h),\; \bar\kappa_{\mathrm{top}},\, \bar\kappa_{\mathrm{bot}})tex";
    s.note = "挖掘批评估: 每候选每持有期沿 t 累加 (分块带 carry 可合并) + 顶底组成本池化; golden = Stat";
    s.doc = "挖掘批评估算子 (factor/Search): 一批候选平面 x_c (c < C) 共用标签 y_h 与成本 κ, 每候选每持有期一份沿 t 可合并的 Accum "
            "(= Stat 的 Row 累加) + 顶组买入 / 底组卖出成本两格池化 (κ̄_top / κ̄_bot). 持有期集合同 Stat.\n"
            "分块流式: 块带 carry (≥ max_lag) 接上一块, 统计从 t_lo 起, 各块 Result merge ≡ 整块一次算.\n"
            "golden = Stat CPU 对每张候选的 HoldStat + 朴素成本池化 (Stat 自身已 gpu ↔ cpu 对拍过):\n"
            "  CPU 列 Diff = cpu 整块 vs golden + 两块 merge vs 整块; GPU 列 Diff = gpu 整块 vs cpu 整块 + 两块 merge vs 整块; 两口径 × 候选合并.\n"
            "造数沿用 Stat (候选 = CS {x, −x, x²} / TS {x, 1−x, x²}; 成本 = POS 造数 × 1e-3 / 1.5e-3 经 fp16).\n"
            "表列耗时 = 两口径整块 eval 之和 (全部候选一批; 分块两趟只对拍不计时), 不含 prep.";
    s.classified = false;
    rows.push_back(s);
    runners_.push_back(&run_search);
  }
  // 静态列直接抄 OpTable 三维分类列, 跑手按 A 域 token 粘贴选后端命名空间
  // (SELF → ts, ALL/GROUP → cs; 缺任一后端同名 struct → 此处编译错)
#define RUN_SELF(Name, ar, t) (&run_op<factor::ts::Name, factor::cpu::ts::Name, ar, factor::T::t, false>)
#define RUN_ALL(Name, ar, t) (&run_op<factor::cs::Name, factor::cpu::cs::Name, ar, factor::T::t, true>)
#define RUN_GROUP RUN_ALL
#define ROW(Name, c_name, ar, t, a, kern, kdom, in, out, op, note)                                                       \
  rows.push_back({#Name, c_name, ar, factor::T::t, factor::A::a, factor::Kern::kern, factor::KDom::kdom,                 \
                  factor::params_str(factor::T::t, factor::KDom::kdom), std::string{}, in, factor::Dom::out, op, note}); \
  runners_.push_back(RUN_##a(Name, ar, t));
  OP_ALL(ROW)
#undef ROW
#undef RUN_GROUP
#undef RUN_ALL
#undef RUN_SELF
  assert(rows.size() == runners_.size());
  // OpTable 行的签名 LaTeX 由表列生成 (与 Expr 的 kOps 同序: rows[i] ↔ kOps[i - n_eval], n_eval = 前置评估算子行数)
  const size_t n_eval = static_cast<size_t>(std::count_if(rows.begin(), rows.end(), [](const OperatorRow &r) { return !r.classified; }));
  assert(rows.size() == static_cast<size_t>(factor::expr::kOpCount) + n_eval);
  for (size_t i = n_eval; i < rows.size(); ++i) {
    assert(rows[i].classified);
    assert(std::strcmp(rows[i].e_name, factor::expr::kOps[i - n_eval].name) == 0);
    rows[i].operand = factor::expr::operand_tex(factor::expr::kOps[i - n_eval]);
  }
  // 未跑之前也显示每算子默认参数, 表一打开 Operands 列就有实际值
  for (OperatorRow &r : rows)
    r.param = param_of(r);
}

void OperatorsService::Request(const OperatorsRequest &req) {
  assert(req.times >= factor::kSegLen && req.times % factor::kSegLen == 0 && req.A >= 2);
  OperatorsRequest r = req;
  // 时间戳做种子: 不做复现, 同一轮内所有算子共享同一张量 (run_op 都从 r.seed 造数)
  r.seed = static_cast<unsigned>(std::chrono::system_clock::now().time_since_epoch().count());
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_ = r;
    cancel_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  if (!thread_.joinable()) {
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread(&OperatorsService::worker_loop, this);
  }
}

void OperatorsService::AdoptSnapshot(const OperatorsRequest &req, std::vector<OperatorRow> &&snap, int failed) {
  assert(!thread_.joinable() && status_.load() == OperatorsStatus::Idle && "载入只在 worker 未起时做");
  assert(snap.size() == rows.size());
  {
    std::lock_guard<std::mutex> lock(mutex);
    rows = std::move(snap);
    current = req;
    from_json = true;
  }
  done_.store(total(), std::memory_order_relaxed);
  failed_.store(failed, std::memory_order_relaxed);
  status_.store(OperatorsStatus::Done, std::memory_order_release);
  epoch_.fetch_add(1, std::memory_order_relaxed);
}

void OperatorsService::Stop() {
  if (!thread_.joinable())
    return;
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    stop_.store(true, std::memory_order_relaxed);
    cancel_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  thread_.join();
}

void OperatorsService::worker_loop() {
  TraceThread("OperatorsWorker");
  while (true) {
    OperatorsRequest req;
    {
      std::unique_lock<std::mutex> lock(req_mutex_);
      req_cv_.wait(lock, [&] { return stop_.load() || pending_.has_value(); });
      if (stop_.load())
        return;
      req = *pending_;
      pending_.reset();
      cancel_.store(false, std::memory_order_relaxed); // 与消费同临界区, 免竞争
    }

    // GPU 探测 + 首次热身 (CUDA 上下文初始化几百 ms, 不能算进首个算子的 gpu_ms)
    if (factor::gpu::available() && !gpu_warmed_) {
      factor::check::Plane x, o;
      x.resize(factor::kSegLen), o.resize(factor::kSegLen);
      factor::Param p;
      factor::gpu::run_ts("TsAbs", x.v.data(), x.m.data(), nullptr, nullptr, nullptr, nullptr, o.v.data(),
                          o.m.data(), factor::kSegLen, 1, p);
      gpu_warmed_ = true;
    }

    // 全行复位 → Running
    {
      std::lock_guard<std::mutex> lock(mutex);
      current = req;
      from_json = false; // 这一轮是本进程算的
      for (OperatorRow &r : rows) {
        r.param = param_of(r);
        r.status = RowStatus::Pending;
        r.cpu_chk = {}, r.gpu_chk = {};
        r.cpu_ms = 0, r.stream_ms = -1, r.gpu_ms = -1;
        r.detail.clear();
      }
    }
    done_.store(0, std::memory_order_relaxed);
    failed_.store(0, std::memory_order_relaxed);
    status_.store(OperatorsStatus::Running, std::memory_order_release);
    epoch_.fetch_add(1, std::memory_order_relaxed);

    // 执行序 = 行序 (评估算子在前, 然后 OpTable 表序), 每行一个跑手, 不分种类.
    // 一轮的输入缓存 / 输出缓冲 / GPU 会话都在 R 里, 轮末 (含取消) 析构释放
    bool cancelled = false;
    RoundCtx R(req, factor::gpu::available());
    for (size_t i = 0; i < rows.size(); ++i) {
      if (cancel_.load(std::memory_order_relaxed)) {
        cancelled = true;
        break;
      }
      {
        std::lock_guard<std::mutex> lock(mutex);
        rows[i].status = RowStatus::Running;
      }
      epoch_.fetch_add(1, std::memory_order_relaxed);

      OperatorRow r = rows[i]; // 静态列 + Running + 已复位的动态列; 只有本线程写 rows, 无锁读安全
      runners_[i](R, r);
      r.status = RowStatus::Done;
      {
        std::lock_guard<std::mutex> lock(mutex);
        rows[i] = r;
      }
      done_.fetch_add(1, std::memory_order_relaxed);
      if (!r.cpu_chk.ok() || (r.gpu_ms >= 0 && !r.gpu_chk.ok()))
        failed_.fetch_add(1, std::memory_order_relaxed);
      epoch_.fetch_add(1, std::memory_order_relaxed);
    }
    status_.store(cancelled ? OperatorsStatus::Cancelled : OperatorsStatus::Done, std::memory_order_release);
  }
}

} // namespace GUI::Factors
