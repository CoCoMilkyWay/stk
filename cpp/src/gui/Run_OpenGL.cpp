// OpenGL Rendering Pipeline for GUI
// This file contains only OpenGL-specific rendering code
// Business logic is in Gui.cpp and shared between OpenGL and Vulkan

#include "gui/Gui.h"
#include "imgui.h"
#include "imgui_impl_glfw.h"
#include "imgui_impl_opengl3.h"
#include "implot.h"
#include <GLFW/glfw3.h>
#include <fstream>
#include <vector>
#include "shared/SharedData.hpp"
#include "gui/GuiTask.hpp"
#include "gui/GuiState.hpp"

// ============================================================================
// Configuration: Frame rate limit
// ============================================================================
constexpr double TARGET_FPS = 10.0;  // Adjust this to control CPU usage
constexpr double FRAME_TIME = 1.0 / TARGET_FPS;

// Forward declarations for task creation
IGuiTask* CreateSettingsTask();
IGuiTask* CreateSystemInfoTask();

// Forward declarations for icon bar
void InitIconBar();
void CleanupIconBar();

namespace GUI {

// Global pointer to GUI state for error callback
static GuiState* g_gui_state = nullptr;

// GLFW error callback
void glfw_error_callback(int error, const char *description) {
  char buffer[512];
  snprintf(buffer, sizeof(buffer), "GLFW Error %d: %s", error, description);
  if (g_gui_state) {
    g_gui_state->AddTerminalLog(buffer);
  }
}

void RunGUI() {
  // Initialize shared data
  SharedData sharedData;
  
  // Initialize GUI state
  GuiState guiState;
  
  // Link GUI state to shared data for logging
  sharedData.gui_state = &guiState;
  g_gui_state = &guiState;
  
  // Create GUI tasks
  std::vector<IGuiTask*> tasks;
  tasks.push_back(CreateSettingsTask());
  tasks.push_back(CreateSystemInfoTask());
  
  // Track selected task
  int selected_task = 0;
  tasks[selected_task]->OnExpand();
  
  // Print startup banner
  guiState.AddTerminalLog("=== Launching GUI ===", TerminalColor::Green());
  guiState.AddTerminalLog("平台窗口库           : Linux(Wayland/X11), macOS(Cocoa), Windows(Win32)", TerminalColor::Green());
  guiState.AddTerminalLog("跨平台窗口管理库      : GLFW (Graphics Library Framework)", TerminalColor::Green());
  guiState.AddTerminalLog("GPU 渲染库          : Vulkan", TerminalColor::Green());
  guiState.AddTerminalLog("UI库(即时模式)       : ImGui", TerminalColor::Green());
  guiState.AddTerminalLog("绘图库              : ImPlot", TerminalColor::Green());
  char init_msg[256];
  snprintf(init_msg, sizeof(init_msg), "GUI initialized (OpenGL backend, %.0f FPS)", TARGET_FPS);
  guiState.AddTerminalLog(init_msg, TerminalColor::Blue());
  
  // Initialize icon bar
  InitIconBar();

  // Setup error callback
  glfwSetErrorCallback(glfw_error_callback);

  // Initialize GLFW
  if (!glfwInit()) {
    guiState.AddTerminalLog("Failed to initialize GLFW");
    return;
  }

  // Setup OpenGL version
#if defined(__APPLE__)
  // GL 3.2 + GLSL 150 (MacOS)
  const char *glsl_version = "#version 150";
  glfwWindowHint(GLFW_CONTEXT_VERSION_MAJOR, 3);
  glfwWindowHint(GLFW_CONTEXT_VERSION_MINOR, 2);
  glfwWindowHint(GLFW_OPENGL_PROFILE, GLFW_OPENGL_CORE_PROFILE);
  glfwWindowHint(GLFW_OPENGL_FORWARD_COMPAT, GL_TRUE);
#else
  // GL 3.0 + GLSL 130 (Windows and Linux)
  const char *glsl_version = "#version 130";
  glfwWindowHint(GLFW_CONTEXT_VERSION_MAJOR, 3);
  glfwWindowHint(GLFW_CONTEXT_VERSION_MINOR, 0);
#endif

  // Create window
  GLFWwindow *window = glfwCreateWindow(1280, 720, "L2 Data Processor (OpenGL)", nullptr, nullptr);
  if (!window) {
    guiState.AddTerminalLog("Failed to create GLFW window");
    glfwTerminate();
    return;
  }
  glfwMakeContextCurrent(window);
  glfwSwapInterval(1); // Enable vsync

  // Setup ImGui context
  IMGUI_CHECKVERSION();
  ImGui::CreateContext();
  ImPlot::CreateContext();

  // Setup style
  ImGui::StyleColorsDark();
  
  // Setup Chinese font
  ImGuiIO& io = ImGui::GetIO();
  io.Fonts->Clear();
  
  ImFontConfig config;
  config.MergeMode = false;
  config.PixelSnapH = true;
  
  // Load Chinese font from bundled fonts
  const char* font_path = "fonts/NotoSansMonoCJKsc-Regular.otf";
  if (std::ifstream(font_path).good()) {
    io.Fonts->AddFontFromFileTTF(font_path, 16.0f, &config, io.Fonts->GetGlyphRangesChineseFull());
  } else {
    // Fallback to default font
    io.Fonts->AddFontDefault();
    guiState.AddTerminalLog("[Warning] Chinese font not found, using default font");
  }

  // Setup backend
  ImGui_ImplGlfw_InitForOpenGL(window, true);
  ImGui_ImplOpenGL3_Init(glsl_version);

  // Main loop
  while (!glfwWindowShouldClose(window)) {
    glfwWaitEventsTimeout(FRAME_TIME);
    
    // Update GUI state
    guiState.Update(static_cast<float>(FRAME_TIME));

    // Start frame
    ImGui_ImplOpenGL3_NewFrame();
    ImGui_ImplGlfw_NewFrame();
    ImGui::NewFrame();

    // Draw GUI layout (shared business logic)
    DrawGUILayout(sharedData, guiState, tasks, selected_task);

    // Render
    ImGui::Render();
    int display_w, display_h;
    glfwGetFramebufferSize(window, &display_w, &display_h);
    glViewport(0, 0, display_w, display_h);
    glClearColor(0.1f, 0.1f, 0.1f, 1.0f);
    glClear(GL_COLOR_BUFFER_BIT);
    ImGui_ImplOpenGL3_RenderDrawData(ImGui::GetDrawData());

    // Swap buffers
    glfwSwapBuffers(window);
  }

  // Cleanup tasks
  for (auto *task : tasks) {
    delete task;
  }
  tasks.clear();
  
  // Cleanup
  CleanupIconBar();
  g_gui_state = nullptr;
  ImGui_ImplOpenGL3_Shutdown();
  ImGui_ImplGlfw_Shutdown();
  ImPlot::DestroyContext();
  ImGui::DestroyContext();
  glfwDestroyWindow(window);
  glfwTerminate();
}

} // namespace GUI

