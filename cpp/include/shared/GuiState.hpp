#pragma once
#include "gui/task_terminal/TaskTerminal.hpp"
#include "gui/coro/CoroManager.hpp"

// GUI State - Container for GUI-specific state
struct GuiState {
  TaskTerminal terminal;
  CoroManager coro_mgr;

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
