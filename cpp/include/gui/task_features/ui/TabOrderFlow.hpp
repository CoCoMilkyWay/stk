// TabOrderFlow - OrderFlow Visualization Tab
// Lifecycle:
//   - RenderTabOrderFlow: spawns loader coroutine on first call
//   - StopTabOrderFlow: cancels loader coroutine when tab closes
#pragma once

struct SharedData;

namespace GUI::Features {

class DataLoader;

// Render OrderFlow tab - spawns loader on first call
void RenderTabOrderFlow(DataLoader *loader, SharedData &data);

// Stop loader coroutine - call when tab is closed
void StopTabOrderFlow(DataLoader *loader, SharedData &data);

} // namespace GUI::Features
