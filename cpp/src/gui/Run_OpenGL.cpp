// OpenGL Rendering Pipeline for GUI
// This file contains only OpenGL-specific rendering code
// Business logic is in Gui.cpp and shared between OpenGL and Vulkan

#include "gui/Config.hpp"
#include "gui/Gui.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "imgui.h"
#include "imgui_impl_glfw.h"
#include "imgui_impl_opengl3.h"
#include "implot.h"
#include "shared/GuiState.hpp"
#include "shared/SharedData.hpp"
#include <GLFW/glfw3.h>
#include <chrono>
#include <fstream>
#include <thread>

namespace GUI {

// Global pointer to GUI state for error callback
static GuiState *g_gui_state = nullptr;

// GLFW error callback
void glfw_error_callback(int error, const char *description) {
  char buffer[512];
  snprintf(buffer, sizeof(buffer), "GLFW Error %d: %s", error, description);
  if (g_gui_state) {
    g_gui_state->terminal->AddLine(buffer);
  }
}

void RunGUI() {
  // Initialize shared data
  SharedData sharedData;

  // Initialize GUI state
  GuiState guiState;
  TaskTerminal terminal;
  guiState.terminal = &terminal;

  // Link GUI state to shared data for logging
  sharedData.gui_state = &guiState;
  g_gui_state = &guiState;

  // Create GUI tasks
  auto tasks = GUI::CreateAllTasks();

  // Track selected task
  int selected_task = 0;
  if (!tasks.empty()) {
    tasks[selected_task].OnExpand();
  }

  // Print startup banner
  guiState.terminal->AddLine("=== Launching GUI ===", Color::Green());
  guiState.terminal->AddLine("平台窗口库 : Linux(Wayland/X11), macOS(Cocoa), Windows(Win32)", Color::Green());
  guiState.terminal->AddLine("跨平台窗口管理库 : GLFW (Graphics Library Framework)", Color::Green());
  guiState.terminal->AddLine("GPU 渲染库 : OpenGL", Color::Green());
  guiState.terminal->AddLine("UI库(即时模式) : ImGui", Color::Green());
  guiState.terminal->AddLine("绘图库 : ImPlot", Color::Green());
  char init_msg[256];
  snprintf(init_msg, sizeof(init_msg), "GUI initialized (OpenGL backend, %.0f FPS)", TARGET_FPS);
  guiState.terminal->AddLine(init_msg, Color::Blue());

  // Initialize icon bar with network monitoring
  TaskIconBar::InitIconBar(guiState);

  // Setup error callback
  glfwSetErrorCallback(glfw_error_callback);

  // Initialize GLFW
  if (!glfwInit()) {
    guiState.terminal->AddLine("Failed to initialize GLFW");
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
  GLFWwindow *window = glfwCreateWindow(1920, 1080, "L2 Data Processor (OpenGL)", nullptr, nullptr);
  if (!window) {
    guiState.terminal->AddLine("Failed to create GLFW window");
    glfwTerminate();
    return;
  }
  glfwMakeContextCurrent(window);
  glfwSwapInterval(VSYNC_ENABLE ? 1 : 0);

  // Setup ImGui context
  IMGUI_CHECKVERSION();
  ImGui::CreateContext();
  ImPlot::CreateContext();

  // Setup style
  ImGui::StyleColorsDark();

  // Setup Chinese font
  ImGuiIO &io = ImGui::GetIO();
  io.Fonts->Clear();

  ImFontConfig config;
  config.MergeMode = false;
  config.PixelSnapH = true;

  // Load Chinese font from bundled fonts
  const char *font_path = "fonts/MapleMonoNormal-NF-CN-Regular.ttf";
  if (std::ifstream(font_path).good()) {
    io.Fonts->AddFontFromFileTTF(font_path, 16.0f, &config, io.Fonts->GetGlyphRangesChineseFull());
  } else {
    // Fallback to default font
    io.Fonts->AddFontDefault();
    guiState.terminal->AddLine("[Warning] Chinese font not found, using default font");
  }

  // Setup backend
  ImGui_ImplGlfw_InitForOpenGL(window, true);
  ImGui_ImplOpenGL3_Init(glsl_version);

  // Main loop
  while (!glfwWindowShouldClose(window)) {
    double frame_start = 0.0;

    if constexpr (HIGH_FPS_ON_EVENTS) {
      glfwWaitEventsTimeout(FRAME_TIME);
    } else {
      frame_start = glfwGetTime();
      glfwPollEvents();
    }

    // Update GUI state
    guiState.Update(static_cast<float>(FRAME_TIME));

    // Start frame
    ImGui_ImplOpenGL3_NewFrame();
    ImGui_ImplGlfw_NewFrame();
    ImGui::NewFrame();

    // Draw GUI layout (shared business logic)
    GUI::DrawGUILayout(sharedData, guiState, tasks, selected_task);

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

    // Enforce fixed frame rate (only when HIGH_FPS_ON_EVENTS is disabled)
    if constexpr (!HIGH_FPS_ON_EVENTS) {
      double frame_end = glfwGetTime();
      double elapsed = frame_end - frame_start;
      if (elapsed < FRAME_TIME) {
        auto sleep_duration = std::chrono::duration<double>(FRAME_TIME - elapsed);
        std::this_thread::sleep_for(sleep_duration);
      }
    }
  }

  TaskIconBar::CleanupIconBar();

  // Cleanup
  TaskIconBar::CleanupIconBar();
  g_gui_state = nullptr;
  ImGui_ImplOpenGL3_Shutdown();
  ImGui_ImplGlfw_Shutdown();
  ImPlot::DestroyContext();
  ImGui::DestroyContext();
  glfwDestroyWindow(window);
  glfwTerminate();
}

} // namespace GUI
