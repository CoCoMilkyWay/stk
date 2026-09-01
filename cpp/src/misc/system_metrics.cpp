#include "misc/system_metrics.hpp"
#include "misc/cross_platform.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

// ---------------------------------------------------------------------------
// 平台头 / 架构判定
// ---------------------------------------------------------------------------
#if defined(_WIN32)
// windows.h 必须排在其余 Win32 头之前 (pdh / iphlpapi / dxgi 都依赖它的类型),
// 单独成块 —— clang-format 只在块内排序, 不会把它挪到后面去
#include <windows.h>

#include <dxgi1_4.h>
#include <intrin.h>
#include <iphlpapi.h>
#include <pdh.h>
#include <pdhmsg.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#include <net/if.h>
#include <net/route.h>
#include <sys/socket.h>
#include <sys/sysctl.h>
#include <unistd.h>
#else
#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <sys/utsname.h>
#include <unistd.h>
#endif

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#define SYSMON_X86 1
#else
#define SYSMON_X86 0
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#define SYSMON_ARM 1
#else
#define SYSMON_ARM 0
#endif

#if SYSMON_X86 && !defined(_WIN32)
#include <cpuid.h>
#endif

#if SYSMON_ARM && defined(__linux__)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif

namespace misc::sysmon {
namespace {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

constexpr double kMiB = 1024.0 * 1024.0;

// 采样间隔下限: 两次采样挨得太近, 累计计数器的差分会被量化噪声主导
constexpr auto kMinInterval = std::chrono::milliseconds(50);
// GPU 单独设下限: NVML 查询是毫秒级, 比读 /proc 贵两个数量级
constexpr auto kGpuInterval = std::chrono::milliseconds(500);
// 隔太久没采的源重新起头, 否则画出来的第一个点是"过去几分钟的平均", 具有误导性
constexpr auto kStaleAfter = std::chrono::milliseconds(2000);

float clamp_percent(double v) {
  return static_cast<float>(v < 0.0 ? 0.0 : (v > 100.0 ? 100.0 : v));
}

// ---------------------------------------------------------------------------
// 通用换算件
// ---------------------------------------------------------------------------

// 累计计数器 -> 速率。首次采样、计数器回绕、以及距上次超过 kStaleAfter 时
// 返回 0 并重新起头。
struct Differ {
  std::uint64_t prev = 0;
  TimePoint at{};
  bool primed = false;

  double per_second(std::uint64_t value, TimePoint now) {
    const double dt = std::chrono::duration<double>(now - at).count();
    const bool usable = primed && value >= prev && now - at <= kStaleAfter && dt > 0.0;
    const double rate = usable ? static_cast<double>(value - prev) / dt : 0.0;
    prev = value;
    at = now;
    primed = true;
    return rate;
  }
};

// CPU 时间片 -> 占用率。时间由 tick 本身归一, 不需要挂钟。
struct CpuTicks {
  std::uint64_t busy = 0;
  std::uint64_t total = 0;
  bool primed = false;

  float percent(std::uint64_t busy_now, std::uint64_t total_now) {
    const bool usable = primed && busy_now >= busy && total_now > total;
    const float out = usable ? clamp_percent(100.0 * static_cast<double>(busy_now - busy) /
                                             static_cast<double>(total_now - total))
                             : 0.0f;
    busy = busy_now;
    total = total_now;
    primed = true;
    return out;
  }
};

// 自适应量程: 缓慢衰减的历史峰值。给没有额定上限的指标 (磁盘吞吐) 当纵轴用。
struct Peak {
  float floor = 1.0f;
  float value = 1.0f;

  explicit Peak(float f) : floor(f), value(f) {}

  float update(float v) {
    value = std::max({floor, value * 0.995f, v});
    return value;
  }
};

// ---------------------------------------------------------------------------
// x86 CPUID (Windows / Linux / Intel Mac 共用)
// ---------------------------------------------------------------------------
#if SYSMON_X86

void cpuid_count(unsigned leaf, unsigned subleaf, unsigned regs[4]) {
#ifdef _WIN32
  __cpuidex(reinterpret_cast<int *>(regs), static_cast<int>(leaf), static_cast<int>(subleaf));
#else
  __cpuid_count(leaf, subleaf, regs[0], regs[1], regs[2], regs[3]);
#endif
}

// XCR0: 指令集光有硬件位还不够, 还要操作系统真的在上下文切换里保存对应的寄存器组,
// 否则用了就是 #UD。只有 OSXSAVE=1 时才能执行 xgetbv。
std::uint64_t xcr0() {
#ifdef _WIN32
  return _xgetbv(0);
#else
  unsigned eax = 0, edx = 0;
  __asm__ volatile("xgetbv" : "=a"(eax), "=d"(edx) : "c"(0));
  return (static_cast<std::uint64_t>(edx) << 32) | eax;
#endif
}

bool bit(unsigned reg, int n) { return (reg >> n) & 1u; }

#endif // SYSMON_X86

// ---------------------------------------------------------------------------
// 指令集探测结果 (两种架构各一份, 非当前架构的那份全 false)
// ---------------------------------------------------------------------------

struct X64Flags {
  bool sse = false, sse2 = false, sse3 = false, ssse3 = false, sse4_1 = false, sse4_2 = false;
  bool avx = false, avx2 = false, fma = false, f16c = false;
  bool avx512f = false, avx512cd = false, avx512vl = false, avx512bw = false, avx512dq = false;
  bool avx512_fp16 = false, avx512_bf16 = false, avx512_vnni = false, avx_vnni = false, amx_tile = false;
  bool aes = false, sha = false, gfni = false, avx512_ifma = false;
  bool prefetchw = false, clflushopt = false, clwb = false, movdir64b = false, rtm = false;
  bool rdrand = false, rdseed = false;
};

struct ArmFlags {
  bool neon = false, fp16 = false, dotprod = false, fcma = false;
  bool sve = false, sve2 = false, sme = false, sme2 = false;
  bool fp64 = false, lse = false;
  bool bf16 = false, i8mm = false, amx = false, neural_engine = false;
  bool aes = false, sha1 = false, sha2 = false, sha3 = false, sha512 = false, pmull = false, crc32 = false;
  bool prefetch = false, dc_zva = false;
  bool rndr = false, pac = false, mte = false;
};

void detect_x64_flags(X64Flags &f) {
#if SYSMON_X86
  unsigned r[4] = {};
  cpuid_count(0, 0, r);
  const unsigned max_leaf = r[0];
  cpuid_count(0x80000000u, 0, r);
  const unsigned max_ext = r[0];

  if (max_leaf < 1)
    return;

  cpuid_count(1, 0, r);
  const unsigned ecx1 = r[2], edx1 = r[3];
  f.sse = bit(edx1, 25);
  f.sse2 = bit(edx1, 26);
  f.sse3 = bit(ecx1, 0);
  f.ssse3 = bit(ecx1, 9);
  f.sse4_1 = bit(ecx1, 19);
  f.sse4_2 = bit(ecx1, 20);
  f.aes = bit(ecx1, 25);
  f.rdrand = bit(ecx1, 30);

  const bool osxsave = bit(ecx1, 27);
  const std::uint64_t xcr = osxsave ? xcr0() : 0;
  const bool ymm_ok = (xcr & 0x6) == 0x6;          // XMM + YMM
  const bool zmm_ok = ymm_ok && (xcr & 0xE0) == 0xE0; // + opmask/ZMM_hi/hi16_ZMM
  const bool tile_ok = (xcr & 0x60000) == 0x60000; // XTILECFG + XTILEDATA

  f.avx = ymm_ok && bit(ecx1, 28);
  f.fma = ymm_ok && bit(ecx1, 12);
  f.f16c = ymm_ok && bit(ecx1, 29);

  if (max_leaf >= 7) {
    cpuid_count(7, 0, r);
    const unsigned ebx7 = r[1], ecx7 = r[2], edx7 = r[3];
    f.avx2 = ymm_ok && bit(ebx7, 5);
    f.rtm = bit(ebx7, 11);
    f.avx512f = zmm_ok && bit(ebx7, 16);
    f.avx512dq = zmm_ok && bit(ebx7, 17);
    f.rdseed = bit(ebx7, 18);
    f.avx512_ifma = zmm_ok && bit(ebx7, 21);
    f.clflushopt = bit(ebx7, 23);
    f.clwb = bit(ebx7, 24);
    f.avx512cd = zmm_ok && bit(ebx7, 28);
    f.sha = bit(ebx7, 29);
    f.avx512bw = zmm_ok && bit(ebx7, 30);
    f.avx512vl = zmm_ok && bit(ebx7, 31);

    f.gfni = bit(ecx7, 8);
    f.avx512_vnni = zmm_ok && bit(ecx7, 11);
    f.movdir64b = bit(ecx7, 28);

    f.avx512_fp16 = zmm_ok && bit(edx7, 23);
    f.amx_tile = tile_ok && bit(edx7, 24);

    cpuid_count(7, 1, r);
    f.avx_vnni = ymm_ok && bit(r[0], 4);
    f.avx512_bf16 = zmm_ok && bit(r[0], 5);
  }

  if (max_ext >= 0x80000001u) {
    cpuid_count(0x80000001u, 0, r);
    f.prefetchw = bit(r[2], 8); // 3DNowPrefetch
  }
#else
  (void)f;
#endif
}

void detect_arm_flags(ArmFlags &f, const std::string &cpu_model) {
#if SYSMON_ARM
  // ARMv8 基线: 这些是架构强制的, 不需要探测
  f.neon = true;
  f.fp64 = true;
  f.prefetch = true;
  f.dc_zva = true;

#if defined(__linux__)
  // 位值一律取内核头里的宏, 不硬编码; 老内核缺哪个宏就当作不支持。
  const unsigned long hwcap = ::getauxval(AT_HWCAP);
  const unsigned long hwcap2 = ::getauxval(AT_HWCAP2);
  const auto has = [](unsigned long caps, unsigned long mask) { return (caps & mask) != 0; };
#ifdef HWCAP_ASIMD
  f.neon = has(hwcap, HWCAP_ASIMD);
#endif
#ifdef HWCAP_ASIMDHP
  f.fp16 = has(hwcap, HWCAP_ASIMDHP);
#endif
#ifdef HWCAP_ASIMDDP
  f.dotprod = has(hwcap, HWCAP_ASIMDDP);
#endif
#ifdef HWCAP_FCMA
  f.fcma = has(hwcap, HWCAP_FCMA);
#endif
#ifdef HWCAP_SVE
  f.sve = has(hwcap, HWCAP_SVE);
#endif
#ifdef HWCAP_ATOMICS
  f.lse = has(hwcap, HWCAP_ATOMICS);
#endif
#ifdef HWCAP_AES
  f.aes = has(hwcap, HWCAP_AES);
#endif
#ifdef HWCAP_PMULL
  f.pmull = has(hwcap, HWCAP_PMULL);
#endif
#ifdef HWCAP_SHA1
  f.sha1 = has(hwcap, HWCAP_SHA1);
#endif
#ifdef HWCAP_SHA2
  f.sha2 = has(hwcap, HWCAP_SHA2);
#endif
#ifdef HWCAP_SHA3
  f.sha3 = has(hwcap, HWCAP_SHA3);
#endif
#ifdef HWCAP_SHA512
  f.sha512 = has(hwcap, HWCAP_SHA512);
#endif
#ifdef HWCAP_CRC32
  f.crc32 = has(hwcap, HWCAP_CRC32);
#endif
#ifdef HWCAP_PACA
  f.pac = has(hwcap, HWCAP_PACA);
#endif
#ifdef HWCAP2_SVE2
  f.sve2 = has(hwcap2, HWCAP2_SVE2);
#endif
#ifdef HWCAP2_SME
  f.sme = has(hwcap2, HWCAP2_SME);
#endif
#ifdef HWCAP2_SME2
  f.sme2 = has(hwcap2, HWCAP2_SME2);
#endif
#ifdef HWCAP2_BF16
  f.bf16 = has(hwcap2, HWCAP2_BF16);
#endif
#ifdef HWCAP2_I8MM
  f.i8mm = has(hwcap2, HWCAP2_I8MM);
#endif
#ifdef HWCAP2_MTE
  f.mte = has(hwcap2, HWCAP2_MTE);
#endif
#ifdef HWCAP2_RNG
  f.rndr = has(hwcap2, HWCAP2_RNG);
#endif
  (void)hwcap;
  (void)hwcap2;

#elif defined(__APPLE__)
  // Apple 用 sysctl 暴露 FEAT_* 能力位, 键不存在即为不支持。
  const auto opt = [](const char *key) {
    int value = 0;
    std::size_t len = sizeof(value);
    return ::sysctlbyname(key, &value, &len, nullptr, 0) == 0 && value != 0;
  };
  f.neon = opt("hw.optional.AdvSIMD");
  f.fp16 = opt("hw.optional.arm.FEAT_FP16");
  f.dotprod = opt("hw.optional.arm.FEAT_DotProd");
  f.fcma = opt("hw.optional.arm.FEAT_FCMA");
  f.lse = opt("hw.optional.arm.FEAT_LSE");
  f.sme = opt("hw.optional.arm.FEAT_SME");
  f.sme2 = opt("hw.optional.arm.FEAT_SME2");
  f.bf16 = opt("hw.optional.arm.FEAT_BF16");
  f.i8mm = opt("hw.optional.arm.FEAT_I8MM");
  f.aes = opt("hw.optional.arm.FEAT_AES");
  f.pmull = opt("hw.optional.arm.FEAT_PMULL");
  f.sha1 = opt("hw.optional.arm.FEAT_SHA1");
  f.sha2 = opt("hw.optional.arm.FEAT_SHA256");
  f.sha3 = opt("hw.optional.arm.FEAT_SHA3");
  f.sha512 = opt("hw.optional.arm.FEAT_SHA512");
  f.crc32 = opt("hw.optional.armv8_crc32");
  f.pac = opt("hw.optional.arm.FEAT_PAuth");
  // AMX / 神经引擎没有公开的能力位, 只能按机型推断 —— 标成"厂商私有"提醒这点
  f.amx = cpu_model.find("Apple") != std::string::npos;
  f.neural_engine = f.amx;

#elif defined(_WIN32)
#ifdef PF_ARM_NEON_INSTRUCTIONS_AVAILABLE
  f.neon = ::IsProcessorFeaturePresent(PF_ARM_NEON_INSTRUCTIONS_AVAILABLE) != 0;
#endif
#ifdef PF_ARM_V8_CRYPTO_INSTRUCTIONS_AVAILABLE
  f.aes = ::IsProcessorFeaturePresent(PF_ARM_V8_CRYPTO_INSTRUCTIONS_AVAILABLE) != 0;
  f.sha1 = f.aes;
  f.sha2 = f.aes;
  f.pmull = f.aes;
#endif
#ifdef PF_ARM_V8_CRC32_INSTRUCTIONS_AVAILABLE
  f.crc32 = ::IsProcessorFeaturePresent(PF_ARM_V8_CRC32_INSTRUCTIONS_AVAILABLE) != 0;
#endif
#ifdef PF_ARM_V81_ATOMIC_INSTRUCTIONS_AVAILABLE
  f.lse = ::IsProcessorFeaturePresent(PF_ARM_V81_ATOMIC_INSTRUCTIONS_AVAILABLE) != 0;
#endif
#ifdef PF_ARM_V82_DP_INSTRUCTIONS_AVAILABLE
  f.dotprod = ::IsProcessorFeaturePresent(PF_ARM_V82_DP_INSTRUCTIONS_AVAILABLE) != 0;
#endif
#endif

#else
  (void)f;
  (void)cpu_model;
#endif // SYSMON_ARM
}

std::vector<IsaRow> build_isa_rows(const X64Flags &x, const ArmFlags &a) {
  return {
      {"Legacy:", "128-bit SIMD",
       {{"SSE", x.sse}, {"SSE2", x.sse2}, {"SSE3", x.sse3}, {"SSSE3", x.ssse3}, {"SSE4.1", x.sse4_1}, {"SSE4.2", x.sse4_2}},
       {{"NEON", a.neon}, {"FP16", a.fp16}, {"DotProd", a.dotprod}, {"FCMA", a.fcma}}},

      {"SIMD:", "256+bit wide vector",
       {{"AVX", x.avx}, {"AVX2", x.avx2}, {"FMA", x.fma}, {"F16C", x.f16c}},
       {{"SVE", a.sve}, {"SVE2", a.sve2}, {"SME", a.sme}, {"SME2", a.sme2}}},

      {"AVX512:", "512-bit SIMD modules",
       {{"F", x.avx512f}, {"CD", x.avx512cd}, {"VL", x.avx512vl}, {"BW", x.avx512bw}, {"DQ", x.avx512dq}},
       {{"FP64", a.fp64}, {"LSE", a.lse}}},

      {"AI/ML:", "FP16/BF16/INT8 accel",
       {{"FP16", x.avx512_fp16}, {"BF16", x.avx512_bf16}, {"VNNI", x.avx512_vnni}, {"AVX-VNNI", x.avx_vnni}, {"AMX", x.amx_tile}},
       {{"BF16", a.bf16}, {"I8MM", a.i8mm}, {"AMX", a.amx, true}, {"NeuralEngine", a.neural_engine, true}}},

      {"Crypto:", "AES/SHA hardware accel",
       {{"AES", x.aes}, {"SHA", x.sha}, {"GFNI", x.gfni}, {"IFMA", x.avx512_ifma}},
       {{"AES", a.aes}, {"SHA1", a.sha1}, {"SHA2", a.sha2}, {"SHA3", a.sha3}, {"SHA512", a.sha512}, {"PMULL", a.pmull}, {"CRC32", a.crc32}}},

      {"Memory:", "cache/prefetch ops",
       {{"PREFETCHW", x.prefetchw}, {"CLFLUSHOPT", x.clflushopt}, {"CLWB", x.clwb}, {"MOVDIR64B", x.movdir64b}, {"RTM", x.rtm}},
       {{"PREFETCH", a.prefetch}, {"DC_ZVA", a.dc_zva}}},

      {"System:", "RNG/security features",
       {{"RDRAND", x.rdrand}, {"RDSEED", x.rdseed}},
       {{"RNDR", a.rndr}, {"PAC", a.pac}, {"MTE", a.mte}}},
  };
}

// x86 的厂商串与型号串来自 CPUID, 三个平台完全一样
void detect_x86_identity(StaticInfo &info) {
#if SYSMON_X86
  unsigned r[4] = {};
  cpuid_count(0, 0, r);
  char vendor[13] = {};
  std::memcpy(vendor + 0, &r[1], 4);
  std::memcpy(vendor + 4, &r[3], 4);
  std::memcpy(vendor + 8, &r[2], 4);
  info.cpu_vendor = vendor;

  cpuid_count(0x80000000u, 0, r);
  if (r[0] >= 0x80000004u) {
    char brand[49] = {};
    for (unsigned i = 0; i < 3; ++i) {
      cpuid_count(0x80000002u + i, 0, r);
      std::memcpy(brand + i * 16, r, sizeof(r));
    }
    std::string model = brand;
    const std::size_t b = model.find_first_not_of(" \t");
    const std::size_t e = model.find_last_not_of(" \t");
    info.cpu_model = (b == std::string::npos) ? "" : model.substr(b, e - b + 1);
  }
#else
  (void)info;
#endif
}

// ---------------------------------------------------------------------------
// NVML: NVIDIA 的官方查询接口, 运行时加载, 编译期零依赖。
// Windows / Linux 只差一个加载器, 其余完全共用。
// ---------------------------------------------------------------------------
#if !defined(__APPLE__)

struct NvmlMemory {
  unsigned long long total, free, used;
};
struct NvmlUtilization {
  unsigned int gpu, memory;
};

class Nvml {
public:
  bool open() {
#if defined(_WIN32)
    lib_ = ::LoadLibraryA("nvml.dll");
#else
    lib_ = ::dlopen("libnvidia-ml.so.1", RTLD_LAZY | RTLD_LOCAL);
#endif
    if (!lib_)
      return false;

    init_ = symbol<int (*)()>("nvmlInit_v2");
    shutdown_ = symbol<int (*)()>("nvmlShutdown");
    get_handle_ = symbol<int (*)(unsigned, void **)>("nvmlDeviceGetHandleByIndex_v2");
    get_name_ = symbol<int (*)(void *, char *, unsigned)>("nvmlDeviceGetName");
    get_memory_ = symbol<int (*)(void *, NvmlMemory *)>("nvmlDeviceGetMemoryInfo");
    get_util_ = symbol<int (*)(void *, NvmlUtilization *)>("nvmlDeviceGetUtilizationRates");
    if (!init_ || !get_handle_ || !get_name_ || !get_memory_ || !get_util_ || init_() != 0) {
      close();
      return false;
    }
    if (get_handle_(0, &device_) != 0) { // 只看首块卡: 计算任务都在它上面
      close();
      return false;
    }
    return true;
  }

  void close() {
    if (lib_ && shutdown_)
      shutdown_();
#if defined(_WIN32)
    if (lib_)
      ::FreeLibrary(static_cast<HMODULE>(lib_));
#else
    if (lib_)
      ::dlclose(lib_);
#endif
    lib_ = nullptr;
    device_ = nullptr;
  }

  bool ready() const { return device_ != nullptr; }

  std::string name() const {
    char buf[96] = {};
    return get_name_(device_, buf, sizeof(buf)) == 0 ? buf : "NVIDIA GPU";
  }

  bool memory(NvmlMemory &out) const { return get_memory_(device_, &out) == 0; }
  bool utilization(NvmlUtilization &out) const { return get_util_(device_, &out) == 0; }

private:
  template <typename Fn>
  Fn symbol(const char *name) {
#if defined(_WIN32)
    return reinterpret_cast<Fn>(::GetProcAddress(static_cast<HMODULE>(lib_), name));
#else
    return reinterpret_cast<Fn>(::dlsym(lib_, name));
#endif
  }

  void *lib_ = nullptr;
  void *device_ = nullptr;
  int (*init_)() = nullptr;
  int (*shutdown_)() = nullptr;
  int (*get_handle_)(unsigned, void **) = nullptr;
  int (*get_name_)(void *, char *, unsigned) = nullptr;
  int (*get_memory_)(void *, NvmlMemory *) = nullptr;
  int (*get_util_)(void *, NvmlUtilization *) = nullptr;
};

#endif // !__APPLE__

} // namespace

// ===========================================================================
// Linux 后端
// ===========================================================================
#if !defined(_WIN32) && !defined(__APPLE__)

namespace {

// procfs 是 seq_file: 每次都要回到偏移 0 才会重新生成内容。
// 句柄常驻省掉每次采样的 open/close, 缓冲区复用省掉堆分配。
class ProcFile {
public:
  explicit ProcFile(const char *path) : fd_(::open(path, O_RDONLY | O_CLOEXEC)), buf_(8192) {
    assert(fd_ >= 0 && "打不开 /proc 文件");
  }
  ~ProcFile() {
    if (fd_ >= 0)
      ::close(fd_);
  }
  ProcFile(const ProcFile &) = delete;
  ProcFile &operator=(const ProcFile &) = delete;

  const char *read() {
    for (;;) {
      const off_t pos = ::lseek(fd_, 0, SEEK_SET);
      assert(pos == 0 && "/proc 定位失败");
      (void)pos;
      const ssize_t n = ::read(fd_, buf_.data(), buf_.size() - 1);
      assert(n >= 0 && "/proc 读失败");
      if (static_cast<std::size_t>(n) + 1 < buf_.size()) {
        buf_[static_cast<std::size_t>(n)] = '\0';
        return buf_.data();
      }
      buf_.resize(buf_.size() * 2); // 内容被截断 (核数/设备数太多), 扩一倍重来
    }
  }

private:
  int fd_ = -1;
  std::vector<char> buf_;
};

// 手写十进制解析: 每次采样要过 ~10KB /proc 文本, sscanf 在这里是纯浪费。
// 停在行尾: '\n' 既不是空格也不是数字, 循环自然结束, 返回 0。
std::uint64_t parse_u64(const char *&p) {
  while (*p == ' ')
    ++p;
  std::uint64_t v = 0;
  while (*p >= '0' && *p <= '9')
    v = v * 10 + static_cast<std::uint64_t>(*p++ - '0');
  return v;
}

const char *next_line(const char *p) {
  while (*p && *p != '\n')
    ++p;
  return *p ? p + 1 : p;
}

long read_sysfs_long(const std::string &path, long fallback) {
  char buf[64];
  const std::size_t n = read_file_head(path.c_str(), buf, sizeof(buf) - 1);
  if (n == 0)
    return fallback;
  buf[n] = '\0';
  return std::strtol(buf, nullptr, 10);
}

std::string read_sysfs_text(const std::string &path) {
  char buf[256];
  const std::size_t n = read_file_head(path.c_str(), buf, sizeof(buf) - 1);
  if (n == 0)
    return {};
  buf[n] = '\0';
  std::string s(buf);
  while (!s.empty() && (s.back() == '\n' || s.back() == ' '))
    s.pop_back();
  return s;
}

// 列目录, 按名字过滤后返回。用于枚举块设备/网卡/缓存层级。
std::vector<std::string> list_dir(const char *path) {
  std::vector<std::string> names;
  DIR *dir = ::opendir(path);
  if (!dir)
    return names;
  while (const dirent *e = ::readdir(dir)) {
    if (e->d_name[0] != '.')
      names.emplace_back(e->d_name);
  }
  ::closedir(dir);
  std::sort(names.begin(), names.end());
  return names;
}

bool path_exists(const std::string &path) { return ::access(path.c_str(), F_OK) == 0; }

} // namespace

struct Monitor::Backend {
  ProcFile stat{"/proc/stat"};
  ProcFile diskstats{"/proc/diskstats"};
  ProcFile netdev{"/proc/net/dev"};

  std::vector<CpuTicks> cpu; // [0] 是全核聚合, [1..] 对应每个逻辑核

  std::vector<std::string> nics;
  Differ net_rx, net_tx;
  Peak net_peak{1.0f};
  long link_speed_mbps = 0;

  std::vector<std::string> disks;
  Differ disk_read, disk_write;
  std::vector<Differ> disk_busy; // 每盘一个 io_ticks 差分器
  Peak disk_peak{10.0f};

  Nvml nvml;
  std::string gpu_busy_path; // amdgpu/i915 的 sysfs 占用率
  std::string vram_total_path;
  std::string vram_used_path;

  ~Backend() { nvml.close(); }

  // -------------------------------------------------------------------------
  // 静态探测
  // -------------------------------------------------------------------------
  void detect(StaticInfo &info) {
    utsname uts{};
    const bool ok = ::uname(&uts) == 0;
    assert(ok && "uname 失败");
    (void)ok;
    info.os_name = "Linux";
    info.kernel_version = uts.release;
    info.hostname = uts.nodename;
    info.arch_name = uts.machine;
    info.arch = SYSMON_X86 ? CpuArch::X86_64 : (SYSMON_ARM ? CpuArch::AArch64 : CpuArch::Unknown);

    info.logical_cores = static_cast<int>(::sysconf(_SC_NPROCESSORS_ONLN));
    assert(info.logical_cores > 0);
    info.physical_cores = count_physical_cores(info.logical_cores);
    detect_caches(info);

    if (info.arch == CpuArch::X86_64) {
      detect_x86_identity(info);
    } else {
      // ARM 上 /proc/cpuinfo 没有型号串, 设备树的 model 是最接近"机型"的那个字段
      info.cpu_vendor = "ARM";
      info.cpu_model = read_sysfs_text("/sys/firmware/devicetree/base/model");
    }

    X64Flags x64;
    ArmFlags arm;
    detect_x64_flags(x64);
    detect_arm_flags(arm, info.cpu_model);
    info.isa = build_isa_rows(x64, arm);

    info.ram_total_bytes = memory_usage().total_bytes;

    detect_nics(info);
    detect_disks(info);

    cpu.resize(static_cast<std::size_t>(info.logical_cores) + 1);
  }

  // 逻辑核会成对共享同一个物理核, 唯一的 (package, core) 组合数才是物理核数
  static int count_physical_cores(int logical) {
    std::vector<std::pair<long, long>> ids;
    ids.reserve(static_cast<std::size_t>(logical));
    for (int i = 0; i < logical; ++i) {
      const std::string base = "/sys/devices/system/cpu/cpu" + std::to_string(i) + "/topology/";
      const long pkg = read_sysfs_long(base + "physical_package_id", -1);
      const long core = read_sysfs_long(base + "core_id", -1);
      if (pkg >= 0 && core >= 0)
        ids.emplace_back(pkg, core);
    }
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    return ids.empty() ? logical : static_cast<int>(ids.size());
  }

  static void detect_caches(StaticInfo &info) {
    const char *root = "/sys/devices/system/cpu/cpu0/cache";
    for (const std::string &entry : list_dir(root)) {
      if (entry.compare(0, 5, "index") != 0)
        continue;
      const std::string dir = std::string(root) + "/" + entry + "/";
      const long level = read_sysfs_long(dir + "level", 0);
      const std::string type = read_sysfs_text(dir + "type");
      const std::string size = read_sysfs_text(dir + "size");
      if (size.empty())
        continue;
      long kb = std::strtol(size.c_str(), nullptr, 10);
      if (size.back() == 'M')
        kb *= 1024;
      if (level == 1 && type == "Data")
        info.cache_l1d_kb = kb;
      else if (level == 2)
        info.cache_l2_kb = std::max(info.cache_l2_kb, kb);
      else if (level == 3)
        info.cache_l3_kb = std::max(info.cache_l3_kb, kb);
    }
  }

  // 只统计物理网卡: 虚拟设备 (lo / docker0 / veth / br-*) 在 sysfs 下没有 device 链接
  void detect_nics(StaticInfo &info) {
    long link_mbps = 0;
    for (const std::string &name : list_dir("/sys/class/net")) {
      const std::string base = "/sys/class/net/" + name;
      if (name == "lo" || !path_exists(base + "/device"))
        continue;
      nics.push_back(name);
      const long speed = read_sysfs_long(base + "/speed", -1);
      if (speed > 0)
        link_mbps += speed; // 多网卡同时在用时, 容量是各自额定速率之和
    }
    link_speed_mbps = link_mbps;
    info.net_available = !nics.empty();
  }

  // 同理, loop / ram / dm 这些伪设备在 /sys/block 下没有 device 链接
  void detect_disks(StaticInfo &info) {
    for (const std::string &name : list_dir("/sys/block")) {
      if (path_exists("/sys/block/" + name + "/device"))
        disks.push_back(name);
    }
    disk_busy.resize(disks.size());
    info.disk_available = !disks.empty();
  }

  // -------------------------------------------------------------------------
  // CPU + 内存
  // -------------------------------------------------------------------------
  void sample_cpu_mem(const StaticInfo &info, Sample &out) {
    const char *p = stat.read();
    // /proc/stat 开头是 "cpu " 全核聚合行, 之后 "cpu0".."cpuN" 依次是每个逻辑核
    while (p[0] == 'c' && p[1] == 'p' && p[2] == 'u') {
      p += 3;
      std::size_t slot = 0;
      if (*p == ' ') {
        slot = 0;
      } else {
        int idx = 0;
        while (*p >= '0' && *p <= '9')
          idx = idx * 10 + (*p++ - '0');
        slot = static_cast<std::size_t>(idx) + 1;
      }

      // user nice system idle iowait irq softirq steal (guest 已计入 user/nice)
      std::uint64_t t[8] = {};
      for (std::uint64_t &v : t)
        v = parse_u64(p);
      const std::uint64_t busy = t[0] + t[1] + t[2] + t[5] + t[6] + t[7];
      const std::uint64_t total = busy + t[3] + t[4];

      if (slot < cpu.size()) {
        const float percent = cpu[slot].percent(busy, total);
        if (slot == 0)
          out.cpu_total_percent = percent;
        else
          out.cpu_core_percent[slot - 1] = percent;
      }
      p = next_line(p);
    }

    const MemoryUsage mem = memory_usage();
    out.mem_used_bytes = mem.used_bytes;
    out.mem_used_percent = mem.used_percent();
    (void)info;
  }

  // -------------------------------------------------------------------------
  // 网络 + 磁盘
  // -------------------------------------------------------------------------
  void sample_io(const StaticInfo &info, Sample &out, TimePoint now) {
    if (info.net_available)
      sample_net(out, now);
    if (info.disk_available)
      sample_disk(out, now);
  }

  void sample_net(Sample &out, TimePoint now) {
    const char *p = next_line(next_line(netdev.read())); // 两行表头
    std::uint64_t rx = 0, tx = 0;
    while (*p) {
      while (*p == ' ')
        ++p;
      const char *name = p;
      while (*p && *p != ':' && *p != '\n')
        ++p;
      if (*p == ':') {
        const std::size_t len = static_cast<std::size_t>(p - name);
        ++p;
        // rx: bytes packets errs drop fifo frame compressed multicast, 然后 tx bytes
        std::uint64_t f[9] = {};
        for (std::uint64_t &v : f)
          v = parse_u64(p);
        if (selected(nics, name, len)) {
          rx += f[0];
          tx += f[8];
        }
      }
      p = next_line(p);
    }

    out.net_rx_mbps = static_cast<float>(net_rx.per_second(rx, now) * 8.0 / 1e6);
    out.net_tx_mbps = static_cast<float>(net_tx.per_second(tx, now) * 8.0 / 1e6);
    out.net_scale_mbps = link_speed_mbps > 0
                             ? static_cast<float>(link_speed_mbps)
                             : net_peak.update(std::max(out.net_rx_mbps, out.net_tx_mbps));
    out.net_rx_percent = clamp_percent(100.0 * out.net_rx_mbps / out.net_scale_mbps);
    out.net_tx_percent = clamp_percent(100.0 * out.net_tx_mbps / out.net_scale_mbps);
  }

  void sample_disk(Sample &out, TimePoint now) {
    const char *p = diskstats.read();
    std::uint64_t sectors_read = 0, sectors_written = 0;
    float busy = 0.0f;
    while (*p) {
      parse_u64(p); // major
      parse_u64(p); // minor
      while (*p == ' ')
        ++p;
      const char *name = p;
      while (*p && *p != ' ' && *p != '\n')
        ++p;
      const int slot = index_of(disks, name, static_cast<std::size_t>(p - name));
      if (slot >= 0) {
        // reads merged sectors ms | writes merged sectors ms | inflight io_ticks weighted
        std::uint64_t f[11] = {};
        for (std::uint64_t &v : f)
          v = parse_u64(p);
        sectors_read += f[2];
        sectors_written += f[6];
        // io_ticks 单位是毫秒: 每秒累计的毫秒数 / 10 就是繁忙百分比
        const double ms_per_s = disk_busy[static_cast<std::size_t>(slot)].per_second(f[9], now);
        busy = std::max(busy, clamp_percent(ms_per_s / 10.0));
      }
      p = next_line(p);
    }

    out.disk_read_mbps = static_cast<float>(disk_read.per_second(sectors_read, now) * 512.0 / kMiB);
    out.disk_write_mbps = static_cast<float>(disk_write.per_second(sectors_written, now) * 512.0 / kMiB);
    out.disk_busy_percent = busy;
    out.disk_scale_mbps = disk_peak.update(std::max(out.disk_read_mbps, out.disk_write_mbps));
    out.disk_read_percent = clamp_percent(100.0 * out.disk_read_mbps / out.disk_scale_mbps);
    out.disk_write_percent = clamp_percent(100.0 * out.disk_write_mbps / out.disk_scale_mbps);
  }

  static int index_of(const std::vector<std::string> &pool, const char *name, std::size_t len) {
    for (std::size_t i = 0; i < pool.size(); ++i) {
      if (pool[i].size() == len && std::memcmp(pool[i].data(), name, len) == 0)
        return static_cast<int>(i);
    }
    return -1;
  }
  static bool selected(const std::vector<std::string> &pool, const char *name, std::size_t len) {
    return index_of(pool, name, len) >= 0;
  }

  // -------------------------------------------------------------------------
  // GPU: NVML 优先, 退到 amdgpu/i915 的 sysfs
  // -------------------------------------------------------------------------
  void probe_gpu(StaticInfo &info) {
    info.gpu_probed = true;

    if (nvml.open()) {
      info.gpu_vendor = GpuVendor::NVIDIA;
      info.gpu_name = nvml.name();
      info.gpu_usage_available = true;
      NvmlMemory mem{};
      if (nvml.memory(mem)) {
        info.vram_total_bytes = mem.total;
        info.vram_available = true;
      }
      return;
    }

    for (const std::string &card : list_dir("/sys/class/drm")) {
      if (card.compare(0, 4, "card") != 0 || card.find('-') != std::string::npos)
        continue;
      const std::string dev = "/sys/class/drm/" + card + "/device/";
      const long vendor_id = std::strtol(read_sysfs_text(dev + "vendor").c_str(), nullptr, 16);
      if (vendor_id == 0x10de)
        info.gpu_vendor = GpuVendor::NVIDIA;
      else if (vendor_id == 0x1002)
        info.gpu_vendor = GpuVendor::AMD;
      else if (vendor_id == 0x8086)
        info.gpu_vendor = GpuVendor::Intel;
      else
        continue;

      info.gpu_name = vendor_label(info.gpu_vendor) + std::string(" GPU (") + card + ")";
      if (path_exists(dev + "gpu_busy_percent")) {
        gpu_busy_path = dev + "gpu_busy_percent";
        info.gpu_usage_available = true;
      }
      if (path_exists(dev + "mem_info_vram_total")) {
        vram_total_path = dev + "mem_info_vram_total";
        vram_used_path = dev + "mem_info_vram_used";
        info.vram_total_bytes = static_cast<std::size_t>(read_sysfs_long(vram_total_path, 0));
        info.vram_available = info.vram_total_bytes > 0;
      }
      return;
    }
  }

  static std::string vendor_label(GpuVendor v) {
    switch (v) {
    case GpuVendor::NVIDIA:
      return "NVIDIA";
    case GpuVendor::AMD:
      return "AMD";
    case GpuVendor::Intel:
      return "Intel";
    case GpuVendor::Apple:
      return "Apple";
    default:
      return "";
    }
  }

  void sample_gpu(const StaticInfo &info, Sample &out) {
    if (nvml.ready()) {
      NvmlUtilization util{};
      if (nvml.utilization(util))
        out.gpu_percent = clamp_percent(util.gpu);
      NvmlMemory mem{};
      if (info.vram_available && nvml.memory(mem)) {
        out.vram_used_bytes = mem.used;
        out.vram_percent = clamp_percent(100.0 * static_cast<double>(mem.used) /
                                         static_cast<double>(mem.total));
      }
      return;
    }
    if (!gpu_busy_path.empty())
      out.gpu_percent = clamp_percent(static_cast<double>(read_sysfs_long(gpu_busy_path, 0)));
    if (info.vram_available) {
      out.vram_used_bytes = static_cast<std::size_t>(read_sysfs_long(vram_used_path, 0));
      out.vram_percent = clamp_percent(100.0 * static_cast<double>(out.vram_used_bytes) /
                                       static_cast<double>(info.vram_total_bytes));
    }
  }
};

#endif // Linux

// ===========================================================================
// Windows 后端
// ===========================================================================
#if defined(_WIN32)

namespace {

// PDH 计数器名必须走 English 版: 本地化系统上英文名注册表键才是稳定的那一份
PDH_HCOUNTER add_counter(PDH_HQUERY query, const wchar_t *path) {
  PDH_HCOUNTER counter = nullptr;
  const PDH_STATUS status = PdhAddEnglishCounterW(query, path, 0, &counter);
  return status == ERROR_SUCCESS ? counter : nullptr;
}

double counter_value(PDH_HCOUNTER counter) {
  if (!counter)
    return 0.0;
  PDH_FMT_COUNTERVALUE value{};
  return PdhGetFormattedCounterValue(counter, PDH_FMT_DOUBLE, nullptr, &value) == ERROR_SUCCESS
             ? value.doubleValue
             : 0.0;
}

} // namespace

struct Monitor::Backend {
  PDH_HQUERY query = nullptr;
  PDH_HCOUNTER cpu_counter = nullptr; // \Processor Information(*)\% Processor Time
  PDH_HCOUNTER disk_read = nullptr;
  PDH_HCOUNTER disk_write = nullptr;
  PDH_HCOUNTER disk_busy = nullptr;

  std::vector<char> pdh_buffer;  // 复用, 免得每次采样都分配
  std::vector<int> cpu_slot_map; // PDH 实例顺序 -> 逻辑核序号

  Differ net_rx, net_tx;
  Peak net_peak{1.0f};
  long link_speed_mbps = 0;

  Peak disk_peak{10.0f};

  Nvml nvml;
  IDXGIAdapter3 *adapter = nullptr;

  ~Backend() {
    nvml.close();
    if (adapter)
      adapter->Release();
    if (query)
      PdhCloseQuery(query);
  }

  // -------------------------------------------------------------------------
  // 静态探测
  // -------------------------------------------------------------------------
  void detect(StaticInfo &info) {
    detect_os(info);
    detect_cpu(info);

    X64Flags x64;
    ArmFlags arm;
    detect_x64_flags(x64);
    detect_arm_flags(arm, info.cpu_model);
    info.isa = build_isa_rows(x64, arm);

    info.ram_total_bytes = memory_usage().total_bytes;
    info.net_available = true;
    info.disk_available = true;

    open_pdh(info);
  }

  static void detect_os(StaticInfo &info) {
    info.os_name = "Windows";

    // GetVersionEx 会被兼容性垫片骗, RtlGetVersion 才是真实版本
    OSVERSIONINFOEXW osvi{};
    osvi.dwOSVersionInfoSize = sizeof(osvi);
    using RtlGetVersionFn = LONG(WINAPI *)(OSVERSIONINFOEXW *);
    if (HMODULE ntdll = GetModuleHandleW(L"ntdll.dll")) {
      if (auto rtl_get_version = reinterpret_cast<RtlGetVersionFn>(GetProcAddress(ntdll, "RtlGetVersion"))) {
        rtl_get_version(&osvi);
        char buf[64];
        snprintf(buf, sizeof(buf), "%lu.%lu.%lu", osvi.dwMajorVersion, osvi.dwMinorVersion, osvi.dwBuildNumber);
        info.kernel_version = buf;
      }
    }

    char hostname[MAX_COMPUTERNAME_LENGTH + 1] = {};
    DWORD size = sizeof(hostname);
    if (GetComputerNameA(hostname, &size))
      info.hostname = hostname;
  }

  static void detect_cpu(StaticInfo &info) {
    SYSTEM_INFO sys{};
    GetNativeSystemInfo(&sys);
    switch (sys.wProcessorArchitecture) {
    case PROCESSOR_ARCHITECTURE_AMD64:
      info.arch = CpuArch::X86_64;
      info.arch_name = "x86_64";
      break;
    case PROCESSOR_ARCHITECTURE_ARM64:
      info.arch = CpuArch::AArch64;
      info.arch_name = "aarch64";
      break;
    default:
      info.arch = CpuArch::Unknown;
      info.arch_name = "unknown";
      break;
    }

    if (info.arch == CpuArch::X86_64)
      detect_x86_identity(info);

    // 一次拿到物理核 / 逻辑核 / 各级缓存; 也是唯一能跨处理器组正确计数的接口
    DWORD bytes = 0;
    GetLogicalProcessorInformationEx(RelationAll, nullptr, &bytes);
    std::vector<char> buffer(bytes);
    if (GetLogicalProcessorInformationEx(RelationAll, reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX *>(buffer.data()), &bytes)) {
      for (DWORD offset = 0; offset < bytes;) {
        auto *entry = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX *>(buffer.data() + offset);
        if (entry->Relationship == RelationProcessorCore) {
          ++info.physical_cores;
          for (WORD g = 0; g < entry->Processor.GroupCount; ++g)
            info.logical_cores += static_cast<int>(__popcnt64(entry->Processor.GroupMask[g].Mask));
        } else if (entry->Relationship == RelationCache) {
          const long kb = static_cast<long>(entry->Cache.CacheSize / 1024);
          if (entry->Cache.Level == 1 && entry->Cache.Type == CacheData)
            info.cache_l1d_kb = kb;
          else if (entry->Cache.Level == 2)
            info.cache_l2_kb = std::max(info.cache_l2_kb, kb);
          else if (entry->Cache.Level == 3)
            info.cache_l3_kb = std::max(info.cache_l3_kb, kb);
        }
        offset += entry->Size;
      }
    }
    assert(info.logical_cores > 0);
  }

  void open_pdh(const StaticInfo &info) {
    const PDH_STATUS status = PdhOpenQueryW(nullptr, 0, &query);
    assert(status == ERROR_SUCCESS && "PdhOpenQuery 失败");
    (void)status;

    // Processor Information 而不是 Processor: 后者在 >64 逻辑核 (多处理器组) 上只看得到第一组
    cpu_counter = add_counter(query, L"\\Processor Information(*)\\% Processor Time");
    disk_read = add_counter(query, L"\\PhysicalDisk(_Total)\\Disk Read Bytes/sec");
    disk_write = add_counter(query, L"\\PhysicalDisk(_Total)\\Disk Write Bytes/sec");
    disk_busy = add_counter(query, L"\\PhysicalDisk(_Total)\\% Disk Time");
    PdhCollectQueryData(query); // 首次采集只为建立基线
    (void)info;
  }

  // -------------------------------------------------------------------------
  // CPU + 内存
  // -------------------------------------------------------------------------
  void sample_cpu_mem(const StaticInfo &info, Sample &out) {
    PdhCollectQueryData(query);

    DWORD size = 0, count = 0;
    if (cpu_counter &&
        PdhGetFormattedCounterArrayW(cpu_counter, PDH_FMT_DOUBLE, &size, &count, nullptr) == PDH_MORE_DATA) {
      pdh_buffer.resize(size);
      auto *items = reinterpret_cast<PDH_FMT_COUNTERVALUE_ITEM_W *>(pdh_buffer.data());
      if (PdhGetFormattedCounterArrayW(cpu_counter, PDH_FMT_DOUBLE, &size, &count, items) == ERROR_SUCCESS) {
        if (cpu_slot_map.size() != count)
          build_cpu_slot_map(items, count, info.logical_cores);
        for (DWORD i = 0; i < count; ++i) {
          const int slot = cpu_slot_map[i];
          if (slot == kTotalSlot)
            out.cpu_total_percent = clamp_percent(items[i].FmtValue.doubleValue);
          else if (slot >= 0 && slot < info.logical_cores)
            out.cpu_core_percent[static_cast<std::size_t>(slot)] = clamp_percent(items[i].FmtValue.doubleValue);
        }
      }
    }

    const MemoryUsage mem = memory_usage();
    out.mem_used_bytes = mem.used_bytes;
    out.mem_used_percent = mem.used_percent();
  }

  static constexpr int kTotalSlot = -2;

  // 实例名形如 "0,5" (处理器组, 组内序号), 另有 "_Total" 与每组的 "0,_Total"。
  // 按 (组, 序号) 排序后依次编号, 得到跨组连续的逻辑核序号。
  void build_cpu_slot_map(const PDH_FMT_COUNTERVALUE_ITEM_W *items, DWORD count, int logical_cores) {
    cpu_slot_map.assign(count, -1);
    std::vector<std::pair<std::pair<int, int>, DWORD>> cores;
    for (DWORD i = 0; i < count; ++i) {
      int group = 0, core = 0;
      if (swscanf(items[i].szName, L"%d,%d", &group, &core) == 2)
        cores.push_back({{group, core}, i});
      else if (wcscmp(items[i].szName, L"_Total") == 0)
        cpu_slot_map[i] = kTotalSlot;
    }
    std::sort(cores.begin(), cores.end());
    for (std::size_t n = 0; n < cores.size() && n < static_cast<std::size_t>(logical_cores); ++n)
      cpu_slot_map[cores[n].second] = static_cast<int>(n);
  }

  // -------------------------------------------------------------------------
  // 网络 + 磁盘
  // -------------------------------------------------------------------------
  void sample_io(const StaticInfo &info, Sample &out, TimePoint now) {
    sample_net(out, now);
    sample_disk(out);
    (void)info;
  }

  void sample_net(Sample &out, TimePoint now) {
    ULONG64 rx = 0, tx = 0;
    long link_mbps = 0;
    MIB_IF_TABLE2 *table = nullptr;
    if (GetIfTable2(&table) == NO_ERROR && table) {
      for (ULONG i = 0; i < table->NumEntries; ++i) {
        const MIB_IF_ROW2 &row = table->Table[i];
        if (row.Type != IF_TYPE_ETHERNET_CSMACD && row.Type != IF_TYPE_IEEE80211)
          continue;
        rx += row.InOctets;
        tx += row.OutOctets;
        if (row.OperStatus == IfOperStatusUp && row.ReceiveLinkSpeed > 0)
          link_mbps += static_cast<long>(row.ReceiveLinkSpeed / 1000000ull);
      }
      FreeMibTable(table);
    }
    link_speed_mbps = link_mbps;

    out.net_rx_mbps = static_cast<float>(net_rx.per_second(rx, now) * 8.0 / 1e6);
    out.net_tx_mbps = static_cast<float>(net_tx.per_second(tx, now) * 8.0 / 1e6);
    out.net_scale_mbps = link_speed_mbps > 0
                             ? static_cast<float>(link_speed_mbps)
                             : net_peak.update(std::max(out.net_rx_mbps, out.net_tx_mbps));
    out.net_rx_percent = clamp_percent(100.0 * out.net_rx_mbps / out.net_scale_mbps);
    out.net_tx_percent = clamp_percent(100.0 * out.net_tx_mbps / out.net_scale_mbps);
  }

  void sample_disk(Sample &out) {
    // PDH 的这三个计数器本身就是速率, 不需要再差分
    out.disk_read_mbps = static_cast<float>(counter_value(disk_read) / kMiB);
    out.disk_write_mbps = static_cast<float>(counter_value(disk_write) / kMiB);
    out.disk_busy_percent = clamp_percent(counter_value(disk_busy));
    out.disk_scale_mbps = disk_peak.update(std::max(out.disk_read_mbps, out.disk_write_mbps));
    out.disk_read_percent = clamp_percent(100.0 * out.disk_read_mbps / out.disk_scale_mbps);
    out.disk_write_percent = clamp_percent(100.0 * out.disk_write_mbps / out.disk_scale_mbps);
  }

  // -------------------------------------------------------------------------
  // GPU: 型号与显存走 DXGI (全厂商可用), 利用率只有 NVML 能给准数
  // -------------------------------------------------------------------------
  void probe_gpu(StaticInfo &info) {
    info.gpu_probed = true;

    IDXGIFactory1 *factory = nullptr;
    if (SUCCEEDED(CreateDXGIFactory1(__uuidof(IDXGIFactory1), reinterpret_cast<void **>(&factory))) && factory) {
      IDXGIAdapter1 *first = nullptr;
      if (factory->EnumAdapters1(0, &first) != DXGI_ERROR_NOT_FOUND) {
        DXGI_ADAPTER_DESC1 desc{};
        first->GetDesc1(&desc);
        char name[128] = {};
        WideCharToMultiByte(CP_UTF8, 0, desc.Description, -1, name, sizeof(name), nullptr, nullptr);
        info.gpu_name = name;
        switch (desc.VendorId) {
        case 0x10DE:
          info.gpu_vendor = GpuVendor::NVIDIA;
          break;
        case 0x1002:
          info.gpu_vendor = GpuVendor::AMD;
          break;
        case 0x8086:
          info.gpu_vendor = GpuVendor::Intel;
          break;
        default:
          info.gpu_vendor = GpuVendor::None;
          break;
        }
        info.vram_total_bytes = desc.DedicatedVideoMemory;
        info.vram_available = info.vram_total_bytes > 0;
        first->QueryInterface(__uuidof(IDXGIAdapter3), reinterpret_cast<void **>(&adapter));
        first->Release();
      }
      factory->Release();
    }

    if (nvml.open()) {
      info.gpu_vendor = GpuVendor::NVIDIA;
      info.gpu_name = nvml.name();
      info.gpu_usage_available = true;
      NvmlMemory mem{};
      if (nvml.memory(mem)) {
        info.vram_total_bytes = mem.total;
        info.vram_available = true;
      }
    }
  }

  void sample_gpu(const StaticInfo &info, Sample &out) {
    if (nvml.ready()) {
      NvmlUtilization util{};
      if (nvml.utilization(util))
        out.gpu_percent = clamp_percent(util.gpu);
      NvmlMemory mem{};
      if (info.vram_available && nvml.memory(mem)) {
        out.vram_used_bytes = mem.used;
        out.vram_percent = clamp_percent(100.0 * static_cast<double>(mem.used) / static_cast<double>(mem.total));
        return;
      }
    }
    if (adapter && info.vram_available) {
      DXGI_QUERY_VIDEO_MEMORY_INFO vram{};
      if (SUCCEEDED(adapter->QueryVideoMemoryInfo(0, DXGI_MEMORY_SEGMENT_GROUP_LOCAL, &vram))) {
        out.vram_used_bytes = vram.CurrentUsage;
        out.vram_percent = clamp_percent(100.0 * static_cast<double>(vram.CurrentUsage) /
                                         static_cast<double>(info.vram_total_bytes));
      }
    }
  }
};

#endif // _WIN32

// ===========================================================================
// macOS 后端
// ===========================================================================
#if defined(__APPLE__)

namespace {

std::string sysctl_string(const char *key) {
  std::size_t len = 0;
  if (::sysctlbyname(key, nullptr, &len, nullptr, 0) != 0 || len == 0)
    return {};
  std::string out(len, '\0');
  if (::sysctlbyname(key, out.data(), &len, nullptr, 0) != 0)
    return {};
  out.resize(std::strlen(out.c_str()));
  return out;
}

std::uint64_t sysctl_u64(const char *key, std::uint64_t fallback) {
  std::uint64_t value = 0;
  std::size_t len = sizeof(value);
  if (::sysctlbyname(key, &value, &len, nullptr, 0) == 0)
    return len == sizeof(std::uint32_t) ? static_cast<std::uint32_t>(value) : value;
  return fallback;
}

} // namespace

struct Monitor::Backend {
  std::vector<CpuTicks> cpu; // [0] 是全核聚合, [1..] 对应每个逻辑核
  Differ net_rx, net_tx;
  Peak net_peak{1.0f};
  long link_speed_mbps = 0;
  std::vector<char> iflist;

  // -------------------------------------------------------------------------
  // 静态探测
  // -------------------------------------------------------------------------
  void detect(StaticInfo &info) {
    info.os_name = "macOS";
    info.kernel_version = sysctl_string("kern.osrelease");
    char hostname[256] = {};
    if (::gethostname(hostname, sizeof(hostname)) == 0)
      info.hostname = hostname;

    info.arch = SYSMON_ARM ? CpuArch::AArch64 : CpuArch::X86_64;
    info.arch_name = SYSMON_ARM ? "aarch64" : "x86_64";
    info.cpu_model = sysctl_string("machdep.cpu.brand_string");
    if (info.cpu_model.empty())
      info.cpu_model = sysctl_string("hw.model");
    if (info.arch == CpuArch::X86_64)
      detect_x86_identity(info);
    else
      info.cpu_vendor = "Apple";

    info.logical_cores = static_cast<int>(sysctl_u64("hw.logicalcpu_max", 1));
    info.physical_cores = static_cast<int>(sysctl_u64("hw.physicalcpu_max", info.logical_cores));
    info.cache_l1d_kb = static_cast<long>(sysctl_u64("hw.l1dcachesize", 0) / 1024);
    info.cache_l2_kb = static_cast<long>(sysctl_u64("hw.l2cachesize", 0) / 1024);
    info.cache_l3_kb = static_cast<long>(sysctl_u64("hw.l3cachesize", 0) / 1024);

    X64Flags x64;
    ArmFlags arm;
    detect_x64_flags(x64);
    detect_arm_flags(arm, info.cpu_model);
    info.isa = build_isa_rows(x64, arm);

    info.ram_total_bytes = memory_usage().total_bytes;
    info.net_available = true;
    // 磁盘 IO 要走 IOKit 的 IOBlockStorageDriver 统计, 未实现 —— UI 显示 N/A
    info.disk_available = false;

    cpu.resize(static_cast<std::size_t>(info.logical_cores) + 1);
  }

  // -------------------------------------------------------------------------
  // CPU + 内存
  // -------------------------------------------------------------------------
  void sample_cpu_mem(const StaticInfo &info, Sample &out) {
    natural_t cpu_count = 0;
    processor_cpu_load_info_t load = nullptr;
    mach_msg_type_number_t load_count = 0;
    if (host_processor_info(mach_host_self(), PROCESSOR_CPU_LOAD_INFO, &cpu_count,
                            reinterpret_cast<processor_info_array_t *>(&load), &load_count) == KERN_SUCCESS) {
      std::uint64_t busy_all = 0, total_all = 0;
      for (natural_t i = 0; i < cpu_count && i + 1 < cpu.size(); ++i) {
        const auto &ticks = load[i].cpu_ticks;
        const std::uint64_t busy = ticks[CPU_STATE_USER] + ticks[CPU_STATE_SYSTEM] + ticks[CPU_STATE_NICE];
        const std::uint64_t total = busy + ticks[CPU_STATE_IDLE];
        out.cpu_core_percent[i] = cpu[i + 1].percent(busy, total);
        busy_all += busy;
        total_all += total;
      }
      out.cpu_total_percent = cpu[0].percent(busy_all, total_all);
      vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(load),
                    load_count * sizeof(integer_t));
    }

    const MemoryUsage mem = memory_usage();
    out.mem_used_bytes = mem.used_bytes;
    out.mem_used_percent = mem.used_percent();
    (void)info;
  }

  // -------------------------------------------------------------------------
  // 网络 (磁盘/GPU 未实现, 由 StaticInfo 的可用位关掉)
  // -------------------------------------------------------------------------
  void sample_io(const StaticInfo &info, Sample &out, TimePoint now) {
    int mib[6] = {CTL_NET, PF_ROUTE, 0, 0, NET_RT_IFLIST2, 0};
    std::size_t len = 0;
    if (::sysctl(mib, 6, nullptr, &len, nullptr, 0) != 0)
      return;
    iflist.resize(len);
    if (::sysctl(mib, 6, iflist.data(), &len, nullptr, 0) != 0)
      return;

    std::uint64_t rx = 0, tx = 0;
    long link_mbps = 0;
    for (char *p = iflist.data(); p < iflist.data() + len;) {
      auto *msg = reinterpret_cast<if_msghdr *>(p);
      if (msg->ifm_type == RTM_IFINFO2) {
        auto *row = reinterpret_cast<if_msghdr2 *>(p);
        if ((row->ifm_flags & IFF_LOOPBACK) == 0) {
          rx += row->ifm_data.ifi_ibytes;
          tx += row->ifm_data.ifi_obytes;
          link_mbps = std::max(link_mbps, static_cast<long>(row->ifm_data.ifi_baudrate / 1000000ull));
        }
      }
      p += msg->ifm_msglen;
    }
    link_speed_mbps = link_mbps;

    out.net_rx_mbps = static_cast<float>(net_rx.per_second(rx, now) * 8.0 / 1e6);
    out.net_tx_mbps = static_cast<float>(net_tx.per_second(tx, now) * 8.0 / 1e6);
    out.net_scale_mbps = link_speed_mbps > 0
                             ? static_cast<float>(link_speed_mbps)
                             : net_peak.update(std::max(out.net_rx_mbps, out.net_tx_mbps));
    out.net_rx_percent = clamp_percent(100.0 * out.net_rx_mbps / out.net_scale_mbps);
    out.net_tx_percent = clamp_percent(100.0 * out.net_tx_mbps / out.net_scale_mbps);
    (void)info;
  }

  // Metal 不公开利用率, IOKit 那套是私有键值 —— 只报型号, 使用率标 N/A
  void probe_gpu(StaticInfo &info) {
    info.gpu_probed = true;
    info.gpu_vendor = GpuVendor::Apple;
    info.gpu_name = info.cpu_model.empty() ? "Apple GPU" : info.cpu_model + " GPU";
    info.vram_total_bytes = sysctl_u64("hw.gpumem_total", 0);
    info.vram_available = info.vram_total_bytes > 0;
  }

  void sample_gpu(const StaticInfo &info, Sample &out) {
    (void)info;
    (void)out;
  }
};

#endif // __APPLE__

// ===========================================================================
// Monitor
// ===========================================================================

Monitor &Monitor::instance() {
  static Monitor monitor;
  return monitor;
}

Monitor::Monitor() : backend_(std::make_unique<Backend>()) {
  backend_->detect(info_);
  sample_.cpu_core_percent.assign(static_cast<std::size_t>(info_.logical_cores), 0.0f);
}

Monitor::~Monitor() = default;

void Monitor::poll(Scope scope, std::chrono::milliseconds interval) {
  const TimePoint now = Clock::now();
  const auto due = [&](TimePoint &at, std::chrono::milliseconds floor) {
    if (now - at < std::max(interval, floor))
      return false;
    at = now;
    return true;
  };

  if (due(cpu_at_, kMinInterval))
    backend_->sample_cpu_mem(info_, sample_);

  if (scope != Scope::Full)
    return;

  if (due(io_at_, kMinInterval))
    backend_->sample_io(info_, sample_, now);

  if (due(gpu_at_, kGpuInterval)) {
    if (!info_.gpu_probed)
      backend_->probe_gpu(info_);
    if (info_.gpu_usage_available || info_.vram_available)
      backend_->sample_gpu(info_, sample_);
  }
}

} // namespace misc::sysmon
