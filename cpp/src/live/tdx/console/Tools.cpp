// 工具: 对选中控件的操作、原始消息 post / send、Spy 抓消息 (行内可回放 / 定位)
#include "live/tdx/Shim.hpp"
#include "live/tdx/console/Model.hpp"

#include <algorithm>

namespace live::tdx::console {

namespace {

using Lock = std::lock_guard<std::mutex>;

void set_out(Core &c, std::string Core::*out, std::string s) {
  Lock lk(c.mu);
  c.*out = std::move(s);
}

// 绘制元素没有自己的窗口: 点击 = 往所画窗口投其中心点, 文本 / 矩形取自绘制层, 闪烁即布局图上的选中框
std::string elem_act(Core &c, const Pick &e, const std::string &kind) {
  if (kind == "click") {
    input(c, strf("clickat %s %d %d", e.host.c_str(), (e.rect[0] + e.rect[2]) >> 1, (e.rect[1] + e.rect[3]) >> 1));
    return "已向 " + e.host + " 投中心点击";
  }
  if (kind == "gettext")
    return json{{"text", e.title}}.dump(1);
  if (kind == "rect")
    return json{{"rect", e.rect}}.dump(1);
  if (kind == "highlight")
    return "";
  return kind + " 对绘制元素无效（它没有自己的窗口）";
}

void raw_msg(Core &c, const View &v, const char *kind) {
  const std::string line = std::string(kind) + " " + v.m_target + " " + v.m_msg + " " + v.m_wp + " " + v.m_lp;
  act(c, [&c, line] { set_out(c, &Core::msg_out, shown([&] { return shim::call(line); })); });
}

void fill_msg(View &v, const SpyRec &e) {
  v.m_target = e.hwnd;
  v.m_msg = e.msg;
  v.m_wp = e.wparam;
  v.m_lp = e.lparam;
}

void sel_desc(const View &v) {
  if (!v.sel)
    return ImGui::TextDisabled("（点树节点选中）");
  const Pick &n = *v.sel;
  const std::string rect = strf("[%d,%d,%d,%d]", n.rect[0], n.rect[1], n.rect[2], n.rect[3]);
  if (!n.host.empty())
    return ImGui::TextWrapped("绘制元素 \"%s\" · 画在 %s · rect=%s", n.title.c_str(), n.host.c_str(), rect.c_str());
  const std::string t = ui::squash(n.title);
  ImGui::TextWrapped("%s · id=%ld · class=%s", n.hwnd.c_str(), n.id, n.cls.c_str());
  ImGui::TextWrapped("title=%s · rect=%s · vis=%d en=%d", t.empty() ? "(空)" : t.c_str(), rect.c_str(), n.vis ? 1 : 0, n.en ? 1 : 0);
}

} // namespace

void act_sel(Core &c, View &v, const std::string &kind, const std::string &arg) {
  if (!v.sel)
    return set_out(c, &Core::sel_out, "先选一个控件");
  if (!v.sel->host.empty())
    return set_out(c, &Core::sel_out, elem_act(c, *v.sel, kind));
  const std::string line = kind + " " + v.sel->hwnd + (arg.empty() ? "" : " " + arg);
  act(c, [&c, line] { set_out(c, &Core::sel_out, shown([&] { return shim::call(line); })); });
}

void draw_tools(Core &c, View &v) {
  ImGui::SeparatorText("选中控件");
  sel_desc(v);
  if (ImGui::Button("读文本"))
    act_sel(c, v, "gettext");
  ImGui::SameLine();
  if (ImGui::Button("点击(BM_CLICK)"))
    act_sel(c, v, "click");
  ImGui::SameLine();
  if (ImGui::Button("闪烁"))
    act_sel(c, v, "highlight");
  ImGui::SameLine();
  if (ImGui::Button("取矩形"))
    act_sel(c, v, "rect");
  ImGui::SetNextItemWidth(-ImGui::GetFontSize() * 5);
  input_text("##setval", v.setval, "要写入的文本（UTF-8）");
  ImGui::SameLine();
  if (ImGui::Button("设文本"))
    act_sel(c, v, "settext", v.setval);
  out_box("##selout", v.sel_out, 5);

  ImGui::SeparatorText("原始消息 (post / send)");
  const float em = ImGui::GetFontSize();
  const auto field = [&](const char *lbl, const char *id, std::string &s, float w, const char *hint = nullptr) {
    ImGui::TextDisabled("%s", lbl);
    ImGui::SameLine();
    ImGui::SetNextItemWidth(w * em);
    input_text(id, s, hint);
    ImGui::SameLine();
  };
  field("target", "##mtarget", v.m_target, 12, "0x…. 或 class=…");
  field("msg", "##mmsg", v.m_msg, 6);
  field("wparam", "##mwp", v.m_wp, 7);
  field("lparam", "##mlp", v.m_lp, 7);
  if (ImGui::Button("post"))
    raw_msg(c, v, "post");
  ImGui::SameLine();
  if (ImGui::Button("send"))
    raw_msg(c, v, "send");
  out_box("##msgout", v.msg_out, 4);

  ImGui::SeparatorText("Spy 录制（抓手点产生的消息）");
  ImGui::TextDisabled("filter");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(10 * em);
  input_text("##spyfilter", v.spy_filter, "msg=0x0111 或 all");
  for (const char *a : {"on", "off", "clear", "dump"}) {
    ImGui::SameLine();
    const std::string_view s = a;
    if (ImGui::Button(s == "on" ? "开" : s == "off" ? "关"
                                     : s == "clear" ? "清"
                                                    : "抓一次"))
      act(c, [&c, action = std::string(a), filter = v.spy_filter] { spy(c, action, filter); });
  }
  ImGui::SameLine();
  ImGui::Checkbox("自动(1s)", &v.spy_auto);
  ImGui::SameLine();
  ImGui::TextDisabled("%s", v.spy_st.c_str());

  const float h = std::min(260.f, ImGui::GetTextLineHeightWithSpacing() * static_cast<float>(v.spy.size() + 2) + ImGui::GetStyle().WindowPadding.y * 2);
  if (!ImGui::BeginTable("##spy", 6, ImGuiTableFlags_RowBg | ImGuiTableFlags_BordersInnerH | ImGuiTableFlags_ScrollY | ImGuiTableFlags_SizingFixedFit,
                         ImVec2(0, h)))
    return;
  ImGui::TableSetupScrollFreeze(0, 1);
  for (const char *col : {"msg", "hwnd", "class", "wparam", "lparam", ""})
    ImGui::TableSetupColumn(col);
  ImGui::TableHeadersRow();
  for (size_t i = 0; i < v.spy.size(); ++i) {
    const SpyRec e = v.spy[i];
    ImGui::PushID(static_cast<int>(i));
    ImGui::TableNextRow();
    for (const std::string *s : {&e.msg, &e.hwnd, &e.cls, &e.wparam, &e.lparam}) {
      ImGui::TableNextColumn();
      ImGui::TextUnformatted(s->c_str());
    }
    ImGui::TableNextColumn();
    if (ImGui::SmallButton("回放")) { // 把这条消息 post 回其 hwnd
      fill_msg(v, e);
      raw_msg(c, v, "post");
    }
    ImGui::SameLine();
    if (ImGui::SmallButton("定位")) { // 在当前树里选中该 hwnd, 消息填进原始消息栏
      if (const ui::Node *n = v.tree->find(e.hwnd)) {
        select(v, Pick{*n, ""});
        fill_msg(v, e);
      } else
        set_out(c, &Core::sel_out, "当前树里没有 " + e.hwnd + "，先刷新窗口树");
    }
    ImGui::PopID();
  }
  ImGui::EndTable();
}

} // namespace live::tdx::console
