#pragma once

class TaskTerminal;
class CoroManager;
class CoroNetwork;

// GUI State - Container for GUI-specific state
// Acts as a thin wrapper, actual implementations are in gui/ module
struct GuiState {
  TaskTerminal *terminal = nullptr;
  CoroNetwork *network = nullptr;
  CoroManager *coro_mgr = nullptr;

  void Update(float dt);
  CoroManager &Coro();
};
