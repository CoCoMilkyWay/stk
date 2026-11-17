// 通达信适配: 客户端进程的映像 (建在进程堆的只读访问上): PE 模块 / MSVC RTTI 类名 / 按类名找虚表
#pragma once

#include "live/Broker.hpp"
#include "live/tdx/Heap.hpp"

#include <cstring>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace live::tdx {

// Wine 只把 PE 映像的头页按文件映射 (/proc/<pid>/maps 里带路径、文件偏移 0), 各节多是匿名段:
// 模块 = 头页的址起、映像头 SizeOfImage 那么长
struct Image {
  struct Mod {
    mem::Addr base, size;
    std::string name;
  };
  mem::HeapP heap;
  std::vector<Mod> mods;

  explicit Image(mem::HeapP h) : heap(std::move(h)) {
    std::ifstream in("/proc/" + std::to_string(heap->pid()) + "/maps");
    for (std::string line; std::getline(in, line);) {
      std::istringstream f(line);
      std::string range, perms, off, dev, inode, path;
      f >> range >> perms >> off >> dev >> inode >> path;
      if (path.empty() || std::stoull(off, nullptr, 16) != 0)
        continue;
      const mem::Addr base = std::stoull(range.substr(0, range.find('-')), nullptr, 16);
      const std::optional<uint32_t> mz = heap->u16(base), nt = heap->u32(base + 0x3c), sig = nt ? heap->u32(base + *nt) : std::nullopt,
                                    size = sig == 0x4550 ? heap->u32(base + *nt + 0x50) : std::nullopt;
      if (mz == 0x5a4d && size)
        mods.push_back({base, *size, path.substr(path.rfind('/') + 1)});
    }
  }

  const Mod *mod(mem::Addr a) const {
    for (const Mod &m : mods)
      if (m.base <= a && a < m.base + m.size)
        return &m;
    return nullptr;
  }
  std::string at(mem::Addr a) const {
    const Mod *m = mod(a);
    return m ? strf("%s+0x%llx", m->name.c_str(), static_cast<unsigned long long>(a - m->base)) : strf("0x%08llx", static_cast<unsigned long long>(a));
  }
  // MSVC RTTI: vtable[-1] → CompleteObjectLocator {sig, off, cd, pTypeDescriptor, …}, TypeDescriptor {pVFTable, spare, ".?AV名@@"}
  std::optional<std::string> rtti(mem::Addr vt) const {
    const std::optional<uint32_t> col = heap->u32(vt - 4), td = col ? heap->u32(*col + 12) : std::nullopt;
    if (!td || !mod(*col) || !mod(*td))
      return std::nullopt;
    const std::string raw = heap->bytes(*td + 8, 128);
    const std::string s = raw.substr(0, raw.find('\0'));
    if (!s.starts_with(".?A") || s.size() < 4)
      return std::nullopt;
    std::string name = s.substr(4);
    if (name.ends_with("@@"))
      name.resize(name.size() - 2);
    return name;
  }
  // 类 cls (RTTI 名, 嵌套类写 内@外, 如 CTcJob@CTcComm) 的各张主虚表 (COL 的 off = 0): TypeDescriptor → 引它的 COL → 引 COL 的那格之后即虚表
  std::vector<mem::Addr> vtables(const std::string &cls) const {
    std::vector<mem::Addr> out;
    for (const Mod &m : mods) {
      const std::string d = heap->bytes(m.base, m.size);
      const auto word = [&](size_t i) {
        uint32_t v;
        std::memcpy(&v, d.data() + i, 4);
        return v;
      };
      const auto refs = [&](uint32_t v, auto &&fn) {
        for (size_t i = 0; i + 4 <= d.size(); i += 4)
          if (word(i) == v)
            fn(i);
      };
      for (const char *kind : {".?AV", ".?AU"})
        for (size_t p = d.find(kind + cls + "@@" + std::string(1, '\0')); p != std::string::npos && p >= 8;
             p = d.find(kind + cls + "@@" + std::string(1, '\0'), p + 1))
          refs(static_cast<uint32_t>(m.base + p - 8), [&](size_t i) {
            if (i < 12 || word(i - 12) != 0 || word(i - 8) != 0)
              return;
            refs(static_cast<uint32_t>(m.base + i - 12), [&](size_t j) { out.push_back(m.base + j + 4); });
          });
    }
    return out;
  }
};

} // namespace live::tdx
