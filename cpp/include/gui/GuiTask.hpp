#pragma once
#include <string>

struct SharedData;
struct GuiState;

// Status colors
struct StatusColor {
  float r, g, b, a;
  
  static StatusColor Green()  { return {0.0f, 1.0f, 0.0f, 1.0f}; }
  static StatusColor Purple() { return {0.8f, 0.4f, 1.0f, 1.0f}; }
  static StatusColor Brown()  { return {0.8f, 0.5f, 0.2f, 1.0f}; }
  static StatusColor None()   { return {0.5f, 0.5f, 0.5f, 0.0f}; } // Transparent for no status
};

// Base interface for GUI tasks (left panel items)
struct IGuiTask {
  virtual ~IGuiTask() = default;
  
  // Get task name for display in list
  virtual const char* GetName() const = 0;
  
  // Get task status text (empty string for no status)
  virtual const char* GetStatus() const = 0;
  
  // Get status color
  virtual StatusColor GetStatusColor() const = 0;
  
  // Draw task content in right panel
  virtual void DrawPanel(SharedData &data, GuiState &gui_state) = 0;
  
  // Called when task is selected/expanded
  virtual void OnExpand() {}
  
  // Called when task is collapsed
  virtual void OnCollapse() {}
};

