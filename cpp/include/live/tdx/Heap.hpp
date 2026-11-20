// 通达信适配: 客户端进程堆 (只读) — 定位 TdxW 进程、确保有跨进程读权限 (本机存的密码自动 sudo 放开 ptrace)、按址试读与堆段逐段读;
//   只给 shim probe (探内存) 与其进程映像 (Image) 用; 实盘路径不读进程堆 (虚表与字典经 shim 进程内找). find_pid / gbk 各处共用. 客户端里的文本一律 GBK
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace live::tdx::mem {

using Addr = uint64_t;

// pid + 常开的 /proc/<pid>/mem (各线程共用, pread 不带状态); 堆段 = 可读写、私有、非文件映射, 排除超大段
class Heap {
public:
  explicit Heap(int pid);
  ~Heap();
  Heap(const Heap &) = delete;
  Heap &operator=(const Heap &) = delete;

  int pid() const { return pid_; }
  std::optional<uint32_t> u16(Addr a) const; // 读不到 (含 64K 以下的址) → 无
  std::optional<uint32_t> u32(Addr a) const;
  std::string bytes(Addr a, size_t n) const; // 逐页读, 读不到的页填 0
  // 逐段 (段序; near 给定时只读含其中任一地址的段): fn(段首址, 读到的字节)
  void each(const std::function<void(Addr lo, std::string_view data)> &fn, const std::vector<Addr> *near = nullptr) const;

private:
  int pid_, fd_;
};
using HeapP = std::shared_ptr<const Heap>;

int find_pid(); // 0 = 没有
HeapP attach(); // 定位进程、确保有读权限、开堆; 找不到 → Fail

std::string gbk(std::string_view raw); // GBK → UTF-8, 非法字节换 U+FFFD (0x80 即 €)

} // namespace live::tdx::mem
