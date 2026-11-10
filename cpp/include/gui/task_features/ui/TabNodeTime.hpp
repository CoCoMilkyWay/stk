// TabNodeTime — 节点用时 (NodeTime 页): 当前库 output/run/features/<池>/<档>/node_time.json 按节点文件归并成堆叠条
//   作用域条与 Table 页同一份 (Common.hpp RenderScope)
#pragma once

struct SharedData;

namespace GUI::Features {

void RenderTabNodeTime(SharedData &data);

} // namespace GUI::Features
