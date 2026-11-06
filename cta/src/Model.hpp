#pragma once

// 单资产时序模型 (只做多): 逐根看本根收盘, 给目标仓位 [0, 1] (按本根收盘价调到); 窗口一律以根计 (日线 / 分钟同一套)
//   入场 / 出场都只看收盘; 成交 / 仓位层 / 成本归 Engine
#include <algorithm>
#include <cassert>
#include <cmath>
#include <deque>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

struct Bar {
  float o, h, l, c;
};

class Model {
public:
  virtual ~Model() = default;
  virtual float step(const Bar &b) = 0;
};

// ---- 递推件 ----

// 指数均线, 跨度 n (α = 2 / (n + 1)), 首值即初值
struct Ema {
  double a, v = 0;
  bool on = false;
  explicit Ema(double n) : a(2 / (n + 1)) {}
  void push(double x) {
    v = on ? v + a * (x - v) : x;
    on = true;
  }
};

// 近 n 根 (含本根) 的极值: 单调队列
template <class Keep>
struct RollExt {
  long n, t = 0;
  std::deque<std::pair<long, float>> q;
  explicit RollExt(long n) : n(n) {}
  void push(float x) {
    while (!q.empty() && !Keep{}(q.back().second, x))
      q.pop_back();
    q.emplace_back(t, x);
    if (q.front().first <= t - n)
      q.pop_front();
    ++t;
  }
  bool full() const { return t >= n; }
  float top() const { return q.front().second; }
};
using RollMax = RollExt<std::greater<float>>;
using RollMin = RollExt<std::less<float>>;

// 回看: ago(k) = k 根前的值 (k ≤ n; 0 = 本根)
struct Lag {
  std::vector<float> b;
  long t = 0;
  explicit Lag(long n) : b(static_cast<size_t>(n) + 1) {}
  void push(float x) { b[static_cast<size_t>(t++ % static_cast<long>(b.size()))] = x; }
  bool has(long k) const { return t > k; }
  float ago(long k) const { return b[static_cast<size_t>((t - 1 - k) % static_cast<long>(b.size()))]; }
};

// 平均真实波幅: max(高, 前收) − min(低, 前收) 的 EMA
struct Atr {
  Ema e;
  float pc = 0;
  explicit Atr(double n) : e(n) {}
  void push(const Bar &b) {
    e.push(pc > 0 ? std::max(b.h, pc) - std::min(b.l, pc) : b.h - b.l);
    pc = b.c;
  }
  double v() const { return e.v; }
};

// 每根波动: 对数收益平方的 EMA 开方
struct Vol {
  Ema e;
  float pc = 0;
  explicit Vol(double n) : e(n) {}
  void push(float c) {
    if (pc > 0) {
      const double r = std::log(c / pc);
      e.push(r * r);
    }
    pc = c;
  }
  double v() const { return std::sqrt(e.v); }
};

inline constexpr double kAtrSpan = 20, kVolSpan = 60;

// 长期偏离 (慢变量): v = −ln(收 / EMA_L) / (σ_bar · √L); 正 = 低于长期均值 (便宜), 负 = 高于 (贵)
struct Dev {
  Ema m;
  Vol vol{kVolSpan};
  long L, t = 0;
  explicit Dev(long L) : m(static_cast<double>(L)), L(L) {}
  void push(float c) { m.push(c), vol.push(c), ++t; }
  bool ready() const { return t >= L; }
  double v(float c) const {
    const double s = vol.v() * std::sqrt(static_cast<double>(L));
    return s > 0 ? -std::log(c / m.v) / s : 0;
  }
};

// 考夫曼自适应均线: 效率比 ER = |c − c_n 根前| / 近 n 根 Σ|Δc|; 平滑系数 (ER · (2/3 − α_slow) + α_slow)², α_slow = 2 / (slow + 1)
//   震荡 (ER → 0) 时均线几乎不动, 趋势 (ER → 1) 时贴近价格; 满 n 根前 v = 收
struct KamaLine {
  long n;
  double slow, sum = 0, v = 0;
  Lag c, d;
  KamaLine(long n, double slow) : n(n), slow(2 / (slow + 1)), c(n), d(n) {}
  void push(float x) {
    if (c.has(0)) {
      const double dd = std::fabs(x - c.ago(0));
      d.push(static_cast<float>(dd));
      sum += dd;
      if (d.has(n))
        sum -= d.ago(n);
    }
    c.push(x);
    if (!ready()) {
      v = x;
      return;
    }
    const double er = sum > 0 ? std::fabs(x - c.ago(n)) / sum : 0, sc = er * (2 / 3.0 - slow) + slow;
    v += sc * sc * (x - v);
  }
  bool ready() const { return c.has(n); }
};

// ---- band 部件 (价格单位, 周期 n 根) ----
//   磨损 ≈ band 在震荡里的漂移: band 不动时仓位只是价格的函数, 区间内来回毛收益为零; 故中线要震荡不动、趋势紧跟, 宽度要慢变

inline constexpr const char *kCenters[] = {"kama", "mid"}, *kWidths[] = {"vol", "rng", "dev"};

struct Center {
  virtual ~Center() = default;
  virtual void push(const Bar &b) = 0;
  virtual double v() const = 0;
};

// ER 窗口与慢跨度都取 n: 震荡时等效跨度 ~ n² / 2
struct KamaCenter : Center {
  KamaLine k;
  explicit KamaCenter(long n) : k(n, static_cast<double>(n)) {}
  void push(const Bar &b) override { k.push(b.c); }
  double v() const override { return k.v; }
};

// 唐奇安中线 (近 n 根最高 + 最低) / 2: 区间内呈台阶
struct MidCenter : Center {
  RollMax hi;
  RollMin lo;
  explicit MidCenter(long n) : hi(n), lo(n) {}
  void push(const Bar &b) override { hi.push(b.h), lo.push(b.l); }
  double v() const override { return (hi.top() + lo.top()) / 2.0; }
};

struct Width {
  virtual ~Width() = default;
  virtual void push(const Bar &b, double center) = 0;
  virtual double v() const = 0;
};

// n 根收益的波动: σ_bar · √n · 收 (σ_bar 跨度 n)
struct VolWidth : Width {
  Vol s;
  double rn, c = 0;
  explicit VolWidth(long n) : s(static_cast<double>(n)), rn(std::sqrt(static_cast<double>(n))) {}
  void push(const Bar &b, double) override { s.push(b.c), c = b.c; }
  double v() const override { return s.v() * rn * c; }
};

// 唐奇安半宽 (近 n 根最高 − 最低) / 2
struct RngWidth : Width {
  RollMax hi;
  RollMin lo;
  explicit RngWidth(long n) : hi(n), lo(n) {}
  void push(const Bar &b, double) override { hi.push(b.h), lo.push(b.l); }
  double v() const override { return (hi.top() - lo.top()) / 2.0; }
};

// 偏离中线的均方根: √EMA_n((收 − 中线)²) (布林式)
struct DevWidth : Width {
  Ema e;
  explicit DevWidth(long n) : e(static_cast<double>(n)) {}
  void push(const Bar &b, double center) override {
    const double d = b.c - center;
    e.push(d * d);
  }
  double v() const override { return std::sqrt(e.v); }
};

inline std::unique_ptr<Center> center_of(int kind, long n) {
  switch (kind) {
  case 0:
    return std::make_unique<KamaCenter>(n);
  case 1:
    return std::make_unique<MidCenter>(n);
  }
  assert(false && "中线种类");
  return nullptr;
}

inline std::unique_ptr<Width> width_of(int kind, long n) {
  switch (kind) {
  case 0:
    return std::make_unique<VolWidth>(n);
  case 1:
    return std::make_unique<RngWidth>(n);
  case 2:
    return std::make_unique<DevWidth>(n);
  }
  assert(false && "宽度种类");
  return nullptr;
}

// ---- 模型 ----

// 买入持有 (基准)
class Hold : public Model {
public:
  float step(const Bar &) override { return 1; }
};

// 双均线: EMA(fast) > EMA(slow) 持有
class Ma : public Model {
  Ema f_, s_;
  long warm_, t_ = 0;

public:
  Ma(float fast, float slow) : f_(fast), s_(slow), warm_(static_cast<long>(slow)) {}
  float step(const Bar &b) override {
    f_.push(b.c), s_.push(b.c);
    return ++t_ >= warm_ && f_.v > s_.v ? 1.f : 0.f;
  }
};

// 时序动量: 收 > L 根前收 持有
class TsMom : public Model {
  Lag c_;
  long L_;

public:
  explicit TsMom(float L) : c_(static_cast<long>(L)), L_(static_cast<long>(L)) {}
  float step(const Bar &b) override {
    c_.push(b.c);
    return c_.has(L_) && b.c > c_.ago(L_) ? 1.f : 0.f;
  }
};

// 多周期动量集成: 回看 lo · 2^j (j = 0..4), 仓位 = 收 > 回看前收 的周期占比 (最长周期满了才出)
class Ens : public Model {
  std::vector<long> ls_;
  Lag c_;

public:
  explicit Ens(float lo) : c_(static_cast<long>(lo) << 4) {
    for (long j = 0; j < 5; ++j)
      ls_.push_back(static_cast<long>(lo) << j);
  }
  float step(const Bar &b) override {
    c_.push(b.c);
    if (!c_.has(ls_.back()))
      return 0;
    int up = 0;
    for (const long L : ls_)
      up += b.c > c_.ago(L);
    return static_cast<float>(up) / static_cast<float>(ls_.size());
  }
};

// 考夫曼自适应均线 (KamaLine): 收 > KAMA 持有
class Kama : public Model {
  KamaLine k_;

public:
  Kama(float n, float slow) : k_(static_cast<long>(n), slow) {}
  float step(const Bar &b) override {
    k_.push(b.c);
    return k_.ready() && b.c > k_.v ? 1.f : 0.f;
  }
};

// 唐奇安通道 (海龟): 收破前 n 根最高价入场; 收 ≤ 前 m 根最低价出场
class Donchian : public Model {
  RollMax hi_;
  RollMin lo_;
  bool in_ = false;

public:
  Donchian(float n, float m) : hi_(static_cast<long>(n)), lo_(static_cast<long>(m)) {}
  float step(const Bar &b) override {
    in_ = in_ && b.c > lo_.top();
    const bool brk = hi_.full() && b.c > hi_.top();
    hi_.push(b.h), lo_.push(b.l);
    in_ = in_ || brk;
    return in_ ? 1.f : 0.f;
  }
};

// 吊灯: 收破前 n 根最高价入场; 收 ≤ 出场线 (上根的 入场以来最高价 − k · ATR) 出场
class Chandelier : public Model {
  RollMax hi_;
  Atr atr_{kAtrSpan};
  float k_, top_ = 0, exit_ = 0;
  bool in_ = false;

public:
  Chandelier(float n, float k) : hi_(static_cast<long>(n)), k_(k) {}
  float step(const Bar &b) override {
    in_ = in_ && b.c > exit_;
    const bool brk = hi_.full() && b.c > hi_.top();
    hi_.push(b.h), atr_.push(b);
    top_ = in_ ? std::max(top_, b.h) : b.h;
    in_ = in_ || brk;
    exit_ = static_cast<float>(top_ - k_ * atr_.v());
    return in_ ? 1.f : 0.f;
  }
};

// 右侧抄底: 收自前 kPeak 根最高收的对数回撤 ≥ d · σ_bar · √kPeak → 待命; 待命中收破前 n 根最高价 → 入场;
//   收 ≤ 出场线 (上根的 入场以来最高价 − k · ATR) 出场, 之后回到看回撤 (仍够深 = 当根即待命)
class Rebound : public Model {
  static constexpr long kPeak = 250;
  RollMax peak_{kPeak}, hi_;
  Vol vol_{kVolSpan};
  Atr atr_{kAtrSpan};
  float d_, k_, top_ = 0, exit_ = 0;
  bool armed_ = false, in_ = false;

public:
  Rebound(float d, float n, float k) : hi_(static_cast<long>(n)), d_(d), k_(k) {}
  float step(const Bar &b) override {
    in_ = in_ && b.c > exit_;
    const bool brk = hi_.full() && b.c > hi_.top();
    hi_.push(b.h), peak_.push(b.c), vol_.push(b.c), atr_.push(b);
    if (in_) {
      top_ = std::max(top_, b.h);
    } else {
      const double s = vol_.v() * std::sqrt(static_cast<double>(kPeak));
      armed_ = armed_ || (peak_.t >= static_cast<long>(kVolSpan) && s > 0 && std::log(peak_.top() / b.c) >= d_ * s);
      if (armed_ && brk)
        in_ = true, armed_ = false, top_ = b.h;
    }
    exit_ = static_cast<float>(top_ - k_ * atr_.v());
    return in_ ? 1.f : 0.f;
  }
};

// 长周期均值回归: v > g 入场 (跌得够便宜), v < 0 出场 (回到均值)
class Revert : public Model {
  Dev dev_;
  float g_;
  bool in_ = false;

public:
  Revert(float L, float g) : dev_(static_cast<long>(L)), g_(g) {}
  float step(const Bar &b) override {
    dev_.push(b.c);
    if (!dev_.ready())
      return 0;
    const double v = dev_.v(b.c);
    in_ = in_ ? v >= 0 : v > g_;
    return in_ ? 1.f : 0.f;
  }
};

// 慢变量定区间 + 快变量择时: v > g 的区间内按短周期动量持有, 区间外空仓
//   g < 0 = 只避开偏贵 (摸顶), g > 0 = 只在偏便宜时做 (抄底)
//   动量 m = ln(收 / f 根前收) / (σ_bar · √f), 滞回: m > h 入场, m < −h 出场 (h = 0 即 收 > f 根前收)
class Gate : public Model {
  Dev dev_;
  Lag c_;
  long f_;
  float g_, h_;
  bool on_ = false;

public:
  Gate(float L, float g, float f, float h) : dev_(static_cast<long>(L)), c_(static_cast<long>(f)), f_(static_cast<long>(f)), g_(g), h_(h) {}
  float step(const Bar &b) override {
    dev_.push(b.c), c_.push(b.c);
    if (!dev_.ready() || !c_.has(f_))
      return 0;
    const double s = dev_.vol.v() * std::sqrt(static_cast<double>(f_)), m = s > 0 ? std::log(b.c / c_.ago(f_)) / s : 0;
    on_ = on_ ? m >= -h_ : m > h_;
    return on_ && dev_.v(b.c) > g_ ? 1.f : 0.f;
  }
};

// 带宽网格: z = (收 − 中线) / (k · 宽度), 仓位按 z 在 [−1, +1] 内线性分 N 档; z ≤ −1 空仓, z ≥ +1 满仓 (破网即到位)
//   网格只平滑仓位变动: 离当前档超过 0.5 + h 档才换档 (N = 1: h = 0 即穿中线进出, h = 0.5 即上沿入 / 下沿出)
class Band : public Model {
  std::unique_ptr<Center> c_;
  std::unique_ptr<Width> w_;
  long n_, t_ = 0;
  double k_, h_;
  int N_, L_ = 0;

public:
  Band(int center, int width, long n, double k, int N, double h) : c_(center_of(center, n)), w_(width_of(width, n)), n_(n), k_(k), h_(h), N_(N) {
    assert(n > 0 && k > 0 && N > 0 && h >= 0 && h <= 0.5 && "band 参数");
  }
  double mid() const { return c_->v(); }
  double half() const { return k_ * w_->v(); } // 中线到上 / 下沿
  float step(const Bar &b) override {
    c_->push(b);
    w_->push(b, c_->v());
    const double w = k_ * w_->v();
    if (++t_ < n_ || !(w > 0))
      return 0;
    const double q = std::clamp((b.c - c_->v()) / w + 1, 0.0, 2.0) / 2 * N_;
    if (q >= N_)
      L_ = N_;
    else if (q <= 0)
      L_ = 0;
    else if (std::fabs(q - L_) > 0.5 + h_)
      L_ = static_cast<int>(std::lround(q));
    return static_cast<float>(L_) / static_cast<float>(N_);
  }
};

// 吊灯外挂: 任一模型持仓期间, 收 ≤ 出场线 (上根的 入场以来最高价 − k · ATR) 即清仓; 之后须底层模型先给 0 才再入场
class Trail : public Model {
  std::unique_ptr<Model> m_;
  Atr atr_{kAtrSpan};
  float k_, top_ = 0, exit_ = 0;
  bool armed_ = true, in_ = false;

public:
  Trail(std::unique_ptr<Model> m, float k) : m_(std::move(m)), k_(k) {}
  float step(const Bar &b) override {
    const bool hit = in_ && b.c <= exit_;
    const float w = m_->step(b);
    atr_.push(b);
    armed_ = (armed_ && !hit) || w == 0;
    if (!armed_ || w == 0) {
      in_ = false;
      return 0;
    }
    top_ = in_ ? std::max(top_, b.h) : b.h;
    in_ = true;
    exit_ = static_cast<float>(top_ - k_ * atr_.v());
    return w;
  }
};
