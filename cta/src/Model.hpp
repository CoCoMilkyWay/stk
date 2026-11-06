#pragma once

// 单资产时序模型 (只做多): 逐根收盘后看本根, 给下一根的单 Order; 窗口一律以根计 (日线 / 分钟同一套)
//   模型 = 入场 / 持有判据 + 可选止损价; 成交 / 仓位层 / 成本归 Engine
#include <algorithm>
#include <cmath>
#include <deque>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

struct Bar {
  float o, h, l, c;
};

struct Order {
  float w = 0;    // 目标仓位 [0, 1]: 下一根开盘调到
  float stop = 0; // 止损价 (0 = 无): 触发即清仓, 判据 / 成交价见 Engine
};

class Model {
public:
  virtual ~Model() = default;
  // held = 本根收盘后的实际仓位 (本根被止损 = 0)
  virtual Order step(const Bar &b, float held) = 0;
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

// ---- 模型 ----

// 买入持有 (基准)
class Hold : public Model {
public:
  Order step(const Bar &, float) override { return {1, 0}; }
};

// 双均线: EMA(fast) > EMA(slow) 持有
class Ma : public Model {
  Ema f_, s_;
  long warm_, t_ = 0;

public:
  Ma(float fast, float slow) : f_(fast), s_(slow), warm_(static_cast<long>(slow)) {}
  Order step(const Bar &b, float) override {
    f_.push(b.c), s_.push(b.c);
    return {++t_ >= warm_ && f_.v > s_.v ? 1.f : 0.f, 0};
  }
};

// 时序动量: 收 > L 根前收 持有
class TsMom : public Model {
  Lag c_;
  long L_;

public:
  explicit TsMom(float L) : c_(static_cast<long>(L)), L_(static_cast<long>(L)) {}
  Order step(const Bar &b, float) override {
    c_.push(b.c);
    return {c_.has(L_) && b.c > c_.ago(L_) ? 1.f : 0.f, 0};
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
  Order step(const Bar &b, float) override {
    c_.push(b.c);
    if (!c_.has(ls_.back()))
      return {};
    int up = 0;
    for (const long L : ls_)
      up += b.c > c_.ago(L);
    return {static_cast<float>(up) / static_cast<float>(ls_.size()), 0};
  }
};

// 考夫曼自适应均线: 效率比 ER = |c − c_n 根前| / 近 n 根 Σ|Δc|; 平滑系数 (ER · (2/3 − α_slow) + α_slow)², α_slow = 2 / (slow + 1)
//   震荡 (ER → 0) 时均线几乎不动, 趋势 (ER → 1) 时贴近价格; 持有: 收 > KAMA
class Kama : public Model {
  long n_;
  double slow_, sum_ = 0, k_ = 0;
  Lag c_, d_;

public:
  Kama(float n, float slow) : n_(static_cast<long>(n)), slow_(2 / (slow + 1.0)), c_(n_), d_(n_) {}
  Order step(const Bar &b, float) override {
    if (c_.has(0)) {
      const double dd = std::fabs(b.c - c_.ago(0));
      d_.push(static_cast<float>(dd));
      sum_ += dd;
      if (d_.has(n_))
        sum_ -= d_.ago(n_);
    }
    c_.push(b.c);
    if (!c_.has(n_)) {
      k_ = b.c;
      return {};
    }
    const double er = sum_ > 0 ? std::fabs(b.c - c_.ago(n_)) / sum_ : 0, sc = er * (2 / 3.0 - slow_) + slow_;
    k_ += sc * sc * (b.c - k_);
    return {b.c > k_ ? 1.f : 0.f, 0};
  }
};

// 唐奇安通道 (海龟): 收破前 n 根最高价入场; 止损 = 近 m 根最低价
class Donchian : public Model {
  RollMax hi_;
  RollMin lo_;
  bool in_ = false;

public:
  Donchian(float n, float m) : hi_(static_cast<long>(n)), lo_(static_cast<long>(m)) {}
  Order step(const Bar &b, float held) override {
    in_ = in_ && held > 0;
    const bool brk = hi_.full() && b.c > hi_.top();
    hi_.push(b.h), lo_.push(b.l);
    in_ = in_ || brk;
    return in_ ? Order{1, lo_.top()} : Order{};
  }
};

// 吊灯止损: 收破前 n 根最高价入场; 止损 = 入场以来最高价 − k · ATR
class Chandelier : public Model {
  RollMax hi_;
  Atr atr_{kAtrSpan};
  float k_, top_ = 0;
  bool in_ = false;

public:
  Chandelier(float n, float k) : hi_(static_cast<long>(n)), k_(k) {}
  Order step(const Bar &b, float held) override {
    in_ = in_ && held > 0;
    const bool brk = hi_.full() && b.c > hi_.top();
    hi_.push(b.h), atr_.push(b);
    top_ = in_ ? std::max(top_, b.h) : b.h;
    in_ = in_ || brk;
    return in_ ? Order{1, static_cast<float>(top_ - k_ * atr_.v())} : Order{};
  }
};

// 右侧抄底: 收自前 kPeak 根最高收的对数回撤 ≥ d · σ_bar · √kPeak → 待命; 待命中收破前 n 根最高价 → 入场;
//   止损 = 入场以来最高价 − k · ATR; 出场后回到看回撤 (仍够深 = 当根即待命)
class Rebound : public Model {
  static constexpr long kPeak = 250;
  RollMax peak_{kPeak}, hi_;
  Vol vol_{kVolSpan};
  Atr atr_{kAtrSpan};
  float d_, k_, top_ = 0;
  bool armed_ = false, in_ = false;

public:
  Rebound(float d, float n, float k) : hi_(static_cast<long>(n)), d_(d), k_(k) {}
  Order step(const Bar &b, float held) override {
    in_ = in_ && held > 0;
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
    return in_ ? Order{1, static_cast<float>(top_ - k_ * atr_.v())} : Order{};
  }
};

// 长周期均值回归: v > g 入场 (跌得够便宜), v < 0 出场 (回到均值)
class Revert : public Model {
  Dev dev_;
  float g_;
  bool in_ = false;

public:
  Revert(float L, float g) : dev_(static_cast<long>(L)), g_(g) {}
  Order step(const Bar &b, float) override {
    dev_.push(b.c);
    if (!dev_.ready())
      return {};
    const double v = dev_.v(b.c);
    in_ = in_ ? v >= 0 : v > g_;
    return {in_ ? 1.f : 0.f, 0};
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
  Order step(const Bar &b, float) override {
    dev_.push(b.c), c_.push(b.c);
    if (!dev_.ready() || !c_.has(f_))
      return {};
    const double s = dev_.vol.v() * std::sqrt(static_cast<double>(f_)), m = s > 0 ? std::log(b.c / c_.ago(f_)) / s : 0;
    on_ = on_ ? m >= -h_ : m > h_;
    return {on_ && dev_.v(b.c) > g_ ? 1.f : 0.f, 0};
  }
};

// 吊灯止损外挂: 任一模型持仓期间叠加 入场以来最高价 − k · ATR 的止损 (取两者高者); 被止损后须底层模型先给 0 才再入场
class Trail : public Model {
  std::unique_ptr<Model> m_;
  Atr atr_{kAtrSpan};
  float k_, top_ = 0, last_ = 0;
  bool armed_ = true, in_ = false;

public:
  Trail(std::unique_ptr<Model> m, float k) : m_(std::move(m)), k_(k) {}
  Order step(const Bar &b, float held) override {
    if (last_ > 0 && held == 0)
      armed_ = false;
    Order o = m_->step(b, held);
    atr_.push(b);
    armed_ = armed_ || o.w == 0;
    if (!armed_ || o.w == 0) {
      in_ = false, last_ = 0;
      return {};
    }
    top_ = in_ ? std::max(top_, b.h) : b.h;
    in_ = true;
    o.stop = std::max(o.stop, static_cast<float>(top_ - k_ * atr_.v()));
    last_ = o.w;
    return o;
  }
};
