#pragma once

#include "misc/cross_platform.hpp"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

// ============================================================================
// 续算点 — <库目录>/snapshot.bin, 一库一份: 库末日收盘后全部资产 TS 核的跨天态 (CoreTs::snap)
// ============================================================================
//
//   [SnapHeader][偏移 uint64 × (A + 1)][记录 × A]
//
//   记录 a = 资产 a 的核经 misc::snap::Save 写出, 偏移相对记录区首 (逐条记: 读端不必知道核布局, 长短由 Load 读尽校验).
//   头 = 可续算的全部前提: 档 / 末日 (= 库清单末日) / A + 池轴指纹 (= 库清单轴) / state (状态指纹, 见 snap_state),
//     任一不符 = 本库不可续算, 只能全量 (不判废、不删: 全量清库时随库一起没).
//   写: 管线一轮跑完 (未取消) 才落, .tmp → rename 原子替换; 读: 续算启动时整读, 各资产 record(a) → misc::snap::Load.
//   kSnapSemver: 节点 snap 清单 / 跨天口径变而成员布局没变时 +1, 旧快照即不可续算.
// ============================================================================

inline constexpr uint64_t kSnapSemver = 1;

struct SnapHeader {
  static constexpr uint64_t kMagic = 0x31504e5354414546ULL; // 'F','E','A','T','S','N','P','1' 小端

  uint64_t magic = 0;
  uint64_t tier = 0;
  uint64_t date = 0;
  uint64_t A = 0;
  uint64_t axis_hash = 0;
  uint64_t state = 0;
  bool operator==(const SnapHeader &) const = default;
};
static_assert(sizeof(SnapHeader) == 48, "续算点头必须定宽无填充");

class FeatureSnap {
public:
  static std::string path(const std::string &dir) { return dir + "/snapshot.bin"; }

  // 头; 无文件 / 读不全 = 全零头 (与任何期望头都不等)
  static SnapHeader head(const std::string &dir) {
    SnapHeader h;
    return read_file_head(path(dir).c_str(), &h, sizeof(h)) == sizeof(h) ? h : SnapHeader{};
  }

  static void write(const std::string &dir, const SnapHeader &h, std::span<const std::vector<std::byte>> records) {
    assert(h.magic == SnapHeader::kMagic && records.size() == h.A && "续算点: 记录数须 = A");
    std::vector<uint64_t> off(records.size() + 1, 0);
    for (size_t a = 0; a < records.size(); ++a) {
      assert(!records[a].empty() && "续算点: 资产缺记录 (末日没走到?)");
      off[a + 1] = off[a] + records[a].size();
    }
    const std::string final_path = path(dir), tmp_path = final_path + ".tmp";
    const int fd = file_open_trunc(tmp_path.c_str());
    assert(fd >= 0 && "续算点 .tmp 建不出来");
    uint64_t at = 0;
    const auto put = [&](const void *buf, size_t n) {
      const size_t written = file_pwrite(fd, buf, n, at);
      assert(written == n && "续算点写入失败 (磁盘满?)");
      (void)written;
      at += n;
    };
    put(&h, sizeof(h));
    put(off.data(), off.size() * sizeof(uint64_t));
    for (const std::vector<std::byte> &r : records)
      put(r.data(), r.size());
    file_close(fd);
    std::filesystem::rename(tmp_path, final_path);
  }

  // 整读; 头须 == want (调用方已以 head 判过可续算)
  FeatureSnap(const std::string &dir, const SnapHeader &want) {
    const int fd = file_open_read(path(dir).c_str());
    assert(fd >= 0 && "续算点不在");
    SnapHeader h;
    const size_t got = file_pread(fd, &h, sizeof(h), 0);
    assert(got == sizeof(h) && h == want && "续算点头与本轮不符");
    (void)got;
    off_.resize(h.A + 1);
    const size_t off_got = file_pread(fd, off_.data(), off_.size() * sizeof(uint64_t), sizeof(h));
    assert(off_got == off_.size() * sizeof(uint64_t) && "续算点偏移表读不全");
    (void)off_got;
    blob_.resize(off_.back());
    const size_t blob_got = file_pread(fd, blob_.data(), blob_.size(), sizeof(h) + off_.size() * sizeof(uint64_t));
    assert(blob_got == blob_.size() && "续算点记录区读不全");
    (void)blob_got;
    file_close(fd);
  }

  std::span<const std::byte> record(size_t a) const {
    assert(a + 1 < off_.size());
    return {blob_.data() + off_[a], off_[a + 1] - off_[a]};
  }

private:
  std::vector<uint64_t> off_;
  std::vector<std::byte> blob_;
};
