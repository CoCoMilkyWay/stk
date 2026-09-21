// =============================================================================
// op_stream: 流式算子对拍 CLI (与 py/factor/check 配对; 独立 target, 不进 app_main)
// =============================================================================
//   用法  op_stream <Name> <dir> [--d D] [--k K] [--k2 K2]
//   输入  <dir>/x.npy [y.npy z.npy] float32 [T, A];  CUM 轴另需 <dir>/days.npy int32 [T] (日 id, 变化即 reset)
//   输出  <dir>/out.npy float32 [T, A]
//   轴语义: ELEM 逐格; CUM 每资产一个 kernel, 沿 T 逐行 push, 日界 reset; ROLL 每资产一个 kernel, 沿 T 逐行 push (行 = 期);
//           CS 每行一个截面 apply. 分派表由 OpTable.hpp 展开: 表里有名字而 stream/ 无 struct → 此处编译错.
//   本 TU 依赖 NaN 语义, CMake 里全 target -fno-fast-math.
// =============================================================================

#include "factor/OpTable.hpp"
#include "factor/stream/Cs.hpp"
#include "factor/stream/Cum.hpp"
#include "factor/stream/Elem.hpp"
#include "factor/stream/Roll.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

namespace {

// ---- 最小 npy IO: 只认 little-endian f4/i4, C 序, 2 维 (days 为 1 维) ----
struct Npy {
  std::vector<size_t> shape;
  std::vector<char> data; // 原字节
  size_t T() const { return shape[0]; }
  size_t A() const { return shape.size() > 1 ? shape[1] : 1; }
  const float *f32() const { return reinterpret_cast<const float *>(data.data()); }
  const int32_t *i32() const { return reinterpret_cast<const int32_t *>(data.data()); }
};

Npy read_npy(const std::string &path, const char *descr) {
  std::ifstream f(path, std::ios::binary);
  if (!f) {
    std::fprintf(stderr, "op_stream: 打不开 %s\n", path.c_str());
    std::exit(2);
  }
  char magic[6];
  f.read(magic, 6);
  assert(std::memcmp(magic, "\x93NUMPY", 6) == 0);
  unsigned char ver[2];
  f.read(reinterpret_cast<char *>(ver), 2);
  uint32_t hlen = 0;
  if (ver[0] == 1) {
    uint16_t h16;
    f.read(reinterpret_cast<char *>(&h16), 2);
    hlen = h16;
  } else {
    f.read(reinterpret_cast<char *>(&hlen), 4);
  }
  std::string header(hlen, '\0');
  f.read(header.data(), hlen);
  assert(header.find(descr) != std::string::npos && "npy dtype 不符 (需 <f4 / <i4)");
  assert(header.find("'fortran_order': False") != std::string::npos);
  Npy r;
  const size_t sp = header.find("'shape': (");
  assert(sp != std::string::npos);
  size_t pos = sp + 10;
  while (header[pos] != ')') {
    while (header[pos] == ' ' || header[pos] == ',')
      ++pos;
    if (header[pos] == ')')
      break;
    r.shape.push_back(static_cast<size_t>(std::strtoull(header.c_str() + pos, nullptr, 10)));
    while (header[pos] >= '0' && header[pos] <= '9')
      ++pos;
  }
  assert(!r.shape.empty() && r.shape.size() <= 2);
  size_t count = 1;
  for (size_t s : r.shape)
    count *= s;
  r.data.resize(count * 4);
  f.read(r.data.data(), static_cast<std::streamsize>(r.data.size()));
  assert(f.gcount() == static_cast<std::streamsize>(r.data.size()));
  return r;
}

void write_npy(const std::string &path, const std::vector<float> &v, size_t T, size_t A) {
  std::string header = "{'descr': '<f4', 'fortran_order': False, 'shape': (" + std::to_string(T) + ", " + std::to_string(A) + "), }";
  const size_t pre = 6 + 2 + 2;
  size_t pad = 64 - (pre + header.size() + 1) % 64;
  header += std::string(pad, ' ') + "\n";
  const uint16_t hlen = static_cast<uint16_t>(header.size());
  std::ofstream f(path, std::ios::binary);
  f.write("\x93NUMPY\x01\x00", 8);
  f.write(reinterpret_cast<const char *>(&hlen), 2);
  f.write(header.data(), static_cast<std::streamsize>(header.size()));
  f.write(reinterpret_cast<const char *>(v.data()), static_cast<std::streamsize>(v.size() * 4));
}

// ---- 按轴执行 ----
struct In {
  Npy x, y, z, days;
  bool has_y = false, has_z = false, has_days = false;
  size_t T() const { return x.T(); }
  size_t A() const { return x.A(); }
};

using factor::Param;

template <class Op>
std::vector<float> run_elem1(const In &in, const Param &p) {
  std::vector<float> out(in.T() * in.A());
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = Op::apply(in.x.f32()[i], p);
  return out;
}
template <class Op>
std::vector<float> run_elem2(const In &in, const Param &p) {
  assert(in.has_y);
  std::vector<float> out(in.T() * in.A());
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = Op::apply(in.x.f32()[i], in.y.f32()[i], p);
  return out;
}
template <class Op>
std::vector<float> run_elem3(const In &in, const Param &p) {
  assert(in.has_y && in.has_z);
  std::vector<float> out(in.T() * in.A());
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = Op::apply(in.x.f32()[i], in.y.f32()[i], in.z.f32()[i], p);
  return out;
}

// 逐资产沿 T push; RESET_ON_DAY: 日 id 变化即 reset (CUM); ROLL 不看 days
template <class Op, bool RESET_ON_DAY, bool BINARY>
std::vector<float> run_seq(const In &in, const Param &p) {
  assert(!RESET_ON_DAY || in.has_days);
  assert(!BINARY || in.has_y);
  const size_t T = in.T(), A = in.A();
  std::vector<float> out(T * A);
  for (size_t a = 0; a < A; ++a) {
    Op op(p);
    int32_t day = INT32_MIN;
    for (size_t t = 0; t < T; ++t) {
      if constexpr (RESET_ON_DAY) {
        if (in.days.i32()[t] != day) {
          op.reset();
          day = in.days.i32()[t];
        }
      }
      const size_t i = t * A + a;
      if constexpr (BINARY)
        out[i] = op.push(in.x.f32()[i], in.y.f32()[i]);
      else
        out[i] = op.push(in.x.f32()[i]);
    }
  }
  return out;
}

template <class Op>
std::vector<float> run_cs1(const In &in, const Param &p) {
  const size_t T = in.T(), A = in.A();
  std::vector<float> out(T * A);
  for (size_t t = 0; t < T; ++t)
    Op::apply(in.x.f32() + t * A, out.data() + t * A, A, p);
  return out;
}
template <class Op>
std::vector<float> run_cs2(const In &in, const Param &p) {
  assert(in.has_y);
  const size_t T = in.T(), A = in.A();
  std::vector<float> out(T * A);
  for (size_t t = 0; t < T; ++t)
    Op::apply(in.x.f32() + t * A, in.y.f32() + t * A, out.data() + t * A, A, p);
  return out;
}
template <class Op>
std::vector<float> run_cs3(const In &in, const Param &p) {
  assert(in.has_y && in.has_z);
  const size_t T = in.T(), A = in.A();
  std::vector<float> out(T * A);
  for (size_t t = 0; t < T; ++t)
    Op::apply(in.x.f32() + t * A, in.y.f32() + t * A, in.z.f32() + t * A, out.data() + t * A, A, p);
  return out;
}

bool exists(const std::string &p) { return std::ifstream(p).good(); }

// 分派: OpTable 各组 → 对应 run_*; 未知名字返回 false
bool dispatch(const std::string &name, const In &in, const Param &p, std::vector<float> &out) {
#define D(Name, RUN)    \
  if (name == #Name) {  \
    out = (RUN)(in, p); \
    return true;        \
  }
#define D_ELEM1(Name, params, desc) D(Name, run_elem1<factor::Name>)
#define D_ELEM2(Name, params, desc) D(Name, run_elem2<factor::Name>)
#define D_ELEM3(Name, params, desc) D(Name, run_elem3<factor::Name>)
#define D_CUM1(Name, params, desc) D(Name, (run_seq<factor::Name, true, false>))
#define D_CUM2(Name, params, desc) D(Name, (run_seq<factor::Name, true, true>))
#define D_ROLL1(Name, params, desc) D(Name, (run_seq<factor::Name, false, false>))
#define D_ROLL2(Name, params, desc) D(Name, (run_seq<factor::Name, false, true>))
#define D_CS1(Name, params, desc) D(Name, run_cs1<factor::Name>)
#define D_CS2(Name, params, desc) D(Name, run_cs2<factor::Name>)
#define D_CS3(Name, params, desc) D(Name, run_cs3<factor::Name>)
  OP_ELEM1(D_ELEM1)
  OP_ELEM2(D_ELEM2)
  OP_ELEM3(D_ELEM3)
  OP_CUM1(D_CUM1)
  OP_CUM2(D_CUM2)
  OP_ROLL1(D_ROLL1)
  OP_ROLL2(D_ROLL2)
  OP_CS1(D_CS1)
  OP_CS2(D_CS2)
  OP_CS3(D_CS3)
#undef D
#undef D_ELEM1
#undef D_ELEM2
#undef D_ELEM3
#undef D_CUM1
#undef D_CUM2
#undef D_ROLL1
#undef D_ROLL2
#undef D_CS1
#undef D_CS2
#undef D_CS3
  return false;
}

} // namespace

int main(int argc, char **argv) {
  if (argc < 3) {
    std::fprintf(stderr, "用法: op_stream <Name> <dir> [--d D] [--k K] [--k2 K2]\n");
    return 2;
  }
  const std::string name = argv[1], dir = argv[2];
  Param p;
  for (int i = 3; i + 1 < argc; i += 2) {
    const std::string k = argv[i];
    if (k == "--d")
      p.d = std::atoi(argv[i + 1]);
    else if (k == "--k")
      p.k = std::strtof(argv[i + 1], nullptr);
    else if (k == "--k2")
      p.k2 = std::strtof(argv[i + 1], nullptr);
    else {
      std::fprintf(stderr, "op_stream: 未知参数 %s\n", k.c_str());
      return 2;
    }
  }

  In in;
  in.x = read_npy(dir + "/x.npy", "<f4");
  if ((in.has_y = exists(dir + "/y.npy")))
    in.y = read_npy(dir + "/y.npy", "<f4");
  if ((in.has_z = exists(dir + "/z.npy")))
    in.z = read_npy(dir + "/z.npy", "<f4");
  if ((in.has_days = exists(dir + "/days.npy")))
    in.days = read_npy(dir + "/days.npy", "<i4");
  assert(!in.has_y || (in.y.T() == in.T() && in.y.A() == in.A()));
  assert(!in.has_z || (in.z.T() == in.T() && in.z.A() == in.A()));
  assert(!in.has_days || in.days.T() == in.T());

  std::vector<float> out;
  if (!dispatch(name, in, p, out)) {
    std::fprintf(stderr, "op_stream: 未知算子 %s (不在 OpTable.hpp)\n", name.c_str());
    return 2;
  }
  write_npy(dir + "/out.npy", out, in.T(), in.A());
  return 0;
}
