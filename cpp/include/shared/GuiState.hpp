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

  // High Performance Mode: GUI thread sleeps, all CPU for compute tasks
  // Use cases: encoding, feature calculation, heavy batch processing
  bool high_performance_mode = false;

  void Update(float dt);
  CoroManager &Coro();
  
  // Enable high performance mode (GUI sleeps)
  void EnableHighPerformanceMode() { high_performance_mode = true; }
  
  // Disable high performance mode (GUI resumes)
  void DisableHighPerformanceMode() { high_performance_mode = false; }
};
