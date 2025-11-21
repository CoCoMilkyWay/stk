#include "gui/GuiTask.hpp"
#include "gui/GuiState.hpp"
#include "shared/SharedData.hpp"
#include "imgui.h"
#include <chrono>

// Settings task - config management with auto-sync
class SettingsTask : public IGuiTask {
private:
  bool is_expanded = false;
  bool initial_sync_done = false;
  bool is_writing = false;
  
public:
  const char* GetName() const override {
    return "Settings";
  }
  
  const char* GetStatus() const override {
    if (!initial_sync_done) {
      return "initializing";
    }
    if (is_writing) {
      return "writing";
    }
    if (is_expanded) {
      return "syncing";
    }
    return "synced";
  }
  
  StatusColor GetStatusColor() const override {
    if (!initial_sync_done) {
      return StatusColor::Purple();
    }
    if (is_writing) {
      return StatusColor::Brown();
    }
    if (is_expanded) {
      return StatusColor::Purple();
    }
    return StatusColor::Green();
  }
  
  void OnExpand() override {
    is_expanded = true;
  }
  
  void OnCollapse() override {
    is_expanded = false;
  }
  
  void DrawPanel(SharedData &data, GuiState & /*gui_state*/) override {
    // Initial sync on first draw
    if (!initial_sync_done) {
      // Set log callback
      data.config.log_callback = [&data](const std::string &msg) {
        data.Log(msg);
      };
      data.config.Initialize();
      initial_sync_done = true;
    }
    
    // Auto-sync when expanded
    if (is_expanded) {
      // Check if we're about to write (dirty flag is set and within write window)
      if (data.config.dirty) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - data.config.last_modified);
        is_writing = (elapsed.count() >= 150 && elapsed.count() < 250);
      } else {
        is_writing = false;
      }
      
      data.config.AutoSync();
    }
    
    Config &cfg = data.config;
    bool changed = false;
    
    // Integer settings
    ImGui::SeparatorText("Integer Settings");
    changed |= ImGui::InputInt("Port", &cfg.port);
    changed |= ImGui::InputInt("Buffer Size", &cfg.buffer_size);
    
    // Floating point settings
    ImGui::SeparatorText("Floating Point Settings");
    changed |= ImGui::InputFloat("Sample Rate", &cfg.sample_rate);
    changed |= ImGui::InputDouble("Threshold", &cfg.threshold);
    
    // Boolean settings
    ImGui::SeparatorText("Boolean Settings");
    changed |= ImGui::Checkbox("Enable Logging", &cfg.enable_logging);
    changed |= ImGui::Checkbox("Auto Save", &cfg.auto_save);
    
    // String settings
    ImGui::SeparatorText("String Settings");
    if (ImGui::InputText("Data Path", cfg.data_path_buf, sizeof(cfg.data_path_buf))) {
      cfg.data_path = cfg.data_path_buf;
      changed = true;
    }
    if (ImGui::InputText("Output File", cfg.output_file_buf, sizeof(cfg.output_file_buf))) {
      cfg.output_file = cfg.output_file_buf;
      changed = true;
    }
    
    // Array settings
    ImGui::SeparatorText("Array Settings");
    ImGui::Text("Window Sizes:");
    for (size_t i = 0; i < cfg.window_sizes.size(); i++) {
      ImGui::PushID(i);
      changed |= ImGui::InputInt("", &cfg.window_sizes[i]);
      ImGui::PopID();
    }
    
    ImGui::Text("Coefficients:");
    for (size_t i = 0; i < cfg.coefficients.size(); i++) {
      ImGui::PushID(100 + i);
      changed |= ImGui::InputFloat("", &cfg.coefficients[i]);
      ImGui::PopID();
    }
    
    // Mark dirty if changed
    if (changed) {
      cfg.MarkDirty();
    }
    
    // Status info
    ImGui::Separator();
    ImGui::TextDisabled("File: %s", cfg.filepath.c_str());
    ImGui::SameLine();
    if (cfg.dirty) {
      ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "[Pending save...]");
    } else {
      ImGui::TextColored(ImVec4(0.0f, 1.0f, 0.0f, 1.0f), "[Synced]");
    }
  }
};

// Factory function
IGuiTask* CreateSettingsTask() {
  return new SettingsTask();
}

