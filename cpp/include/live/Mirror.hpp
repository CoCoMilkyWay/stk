// 实盘: 客户端窗口复现的券商无关接口 — 窗口树 + GDI 绘制层 + 控件子项 + 鼠键透传
//   由支持画面复现的 Driver 提供 (Driver::mirror); GUI 的 Client 页只认这里的类型, 不认具体客户端
#pragma once

#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace live {

// 窗口树节点 (先序, depth 定父子, 兄弟序即 z 序, rect 为屏幕坐标 [l,t,r,b])
struct WinNode {
  int depth = 0;
  std::string hwnd, parent;
  long id = 0;
  std::string cls, title;
  int rect[4] = {};
  bool vis = false, en = false;
};

// 图层一项 (客户区坐标, 后出的在上; gdi 里 v 为 0x00BBGGRR, detail 里 v 为状态位)
struct DrawItem {
  int l = 0, t = 0, r = 0, b = 0;
  unsigned v = 0;
  std::optional<std::string> text;
};

struct DrawLayer {
  std::string hwnd;
  int off[2] = {}; // 客户区左上相对窗口左上
  std::vector<DrawItem> items;
};

// 一帧完整的窗口复现数据 (不可变发布给 GUI)
struct MirrorFrame {
  int64_t gen = 0;
  std::vector<WinNode> nodes;
  std::unordered_map<std::string, DrawLayer> gdi;    // hwnd → GDI 绘制层
  std::unordered_map<std::string, DrawLayer> detail; // hwnd → 标准控件子项 (列表/表头/树/菜单)
};

// 客户端窗口复现驱动 (同步原语, 失败 → Fail)
class Mirror {
public:
  virtual ~Mirror() = default;

  virtual void ensure() = 0;                                            // 确保客户端已接入且开启绘制层
  virtual std::vector<WinNode> tree() = 0;                              // 全量窗口树
  virtual std::vector<DrawLayer> gdi_dump() = 0;                        // 全量 GDI 绘制层
  virtual DrawLayer detail(const std::string &hwnd) = 0;                // 单控件子项
  virtual int64_t tree_gen() = 0;                                       // 窗口树代数
  virtual void watch_gdi(const std::function<bool(DrawLayer)> &cb) = 0; // 阻塞收 GDI 增量推送, cb 返回 false 即停

  // 鼠键透传 (屏幕坐标 / Win32 虚拟键 / Unicode 码点)
  virtual void mouse(const char *kind, int x, int y, unsigned mk = 0, int delta = 0) = 0;
  virtual void key(unsigned vk) = 0;
  virtual void ch(unsigned cp) = 0;
};

} // namespace live
