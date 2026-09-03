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
#include "shared/SharedData.hpp"
#include <GLFW/glfw3.h>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <thread>

namespace GUI {

// Global pointer for error callback logging
static TaskTerminal *g_terminal = nullptr;

// GLFW error callback
void glfw_error_callback(int error, const char *description) {
  char buffer[512];
  snprintf(buffer, sizeof(buffer), "GLFW Error %d: %s", error, description);
  if (g_terminal) {
    g_terminal->AddLine(buffer);
  }
}

int RunGUI() {
  // Initialize shared data (contains everything)
  SharedData data;

  // Setup global state for logging
  g_terminal = &data.terminal;

  // Setup config reinit callback
  data.config.reinit_callback = [&data]() {
    data.request_reinit = true;
  };

  // Create GUI tasks (Init 在其中按顺序立即触发, 后台检查无需等待手动打开页面)
  auto tasks = GUI::CreateAllTasks(data);

  // Track selected task
  int selected_task = 0;
  if (!tasks.empty()) {
    tasks[selected_task].OnExpand();
  }

  // Print startup banner
  data.terminal.AddLine("=== Launching GUI ===", Color::Green());
  data.terminal.AddLine("平台窗口库 : Linux(Wayland/X11), macOS(Cocoa), Windows(Win32)", Color::Green());
  data.terminal.AddLine("跨平台窗口管理库 : GLFW (Graphics Library Framework)", Color::Green());
  data.terminal.AddLine("GPU 渲染库 : OpenGL", Color::Green());
  data.terminal.AddLine("UI库(即时模式) : ImGui", Color::Green());
  data.terminal.AddLine("绘图库 : ImPlot", Color::Green());
  char init_msg[256];
  snprintf(init_msg, sizeof(init_msg), "GUI initialized (OpenGL backend, %.0f FPS)", TARGET_FPS);
  data.terminal.AddLine(init_msg, Color::Blue());

  // Initialize icon bar with network monitoring
  TaskIconBar::InitIconBar(data.coromgr);

  // Setup error callback
  glfwSetErrorCallback(glfw_error_callback);

  // Initialize GLFW
  if (!glfwInit()) {
    const char *err_desc = nullptr;
    int err_code = glfwGetError(&err_desc);
    if (err_desc) {
      fprintf(stderr, "ERROR: Failed to initialize GLFW (code %d): %s\n", err_code, err_desc);
    } else {
      fprintf(stderr, "ERROR: Failed to initialize GLFW (code %d)\n", err_code);
    }
    fprintf(stderr, "Hint: Check DISPLAY environment variable and X Server status\n");
    data.terminal.AddLine("Failed to initialize GLFW");
    return 1;
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
  GLFWwindow *window = glfwCreateWindow(WINDOW_WIDTH, WINDOW_HEIGHT, "L2 Data Processor (OpenGL)", nullptr, nullptr);
  if (!window) {
    const char *err_desc = nullptr;
    int err_code = glfwGetError(&err_desc);
    if (err_desc) {
      fprintf(stderr, "ERROR: Failed to create GLFW window (code %d): %s\n", err_code, err_desc);
    } else {
      fprintf(stderr, "ERROR: Failed to create GLFW window (code %d)\n", err_code);
    }
    fprintf(stderr, "Hint: Check OpenGL drivers and X Server compatibility\n");
    data.terminal.AddLine("Failed to create GLFW window");
    glfwTerminate();
    return 1;
  }
  glfwMakeContextCurrent(window);
  glfwSwapInterval(VSYNC_ENABLE ? 1 : 0);

  // Setup ImGui context
  IMGUI_CHECKVERSION();
  ImGui::CreateContext();
  ImPlot::CreateContext();

  // Setup style
  ImGui::StyleColorsDark();

  // Get actual framebuffer size and window size
  int fb_width, fb_height;
  int win_width, win_height;
  glfwGetFramebufferSize(window, &fb_width, &fb_height);
  glfwGetWindowSize(window, &win_width, &win_height);

  // Calculate actual DPI scale from framebuffer vs window size
  float dpi_scale = (float)fb_width / (float)win_width;

  // Use physical pixels approach: DisplaySize = framebuffer, FramebufferScale = 1.0
  ImGuiIO &io = ImGui::GetIO();
  io.DisplaySize = ImVec2((float)fb_width, (float)fb_height);
  io.DisplayFramebufferScale = ImVec2(1.0f, 1.0f);
  io.Fonts->Clear();

  // Font configuration with RasterizerDensity for crisp rendering
  const char *font_path = "fonts/MapleMonoNormal-NF-CN-Regular.ttf";
  float base_font_size = 16.0f;
  float font_size = base_font_size * dpi_scale;

  assert(std::ifstream(font_path).good() && "Font file not found!");

  // Log font configuration
  char config_msg[256];
  snprintf(config_msg, sizeof(config_msg), "Font: %.1fpx (base: %.1f, DPI: %.2f, physical pixels)",
           font_size, base_font_size, dpi_scale);
  data.terminal.AddLine(config_msg, Color::Blue());

  // Load font with RasterizerDensity
  ImFontConfig config;
  config.MergeMode = false;
  config.PixelSnapH = false; // Better for CJK
  config.OversampleH = 1;
  config.OversampleV = 1;
  config.RasterizerDensity = dpi_scale; // Key: high-res font rendering

  io.Fonts->AddFontFromFileTTF(font_path, font_size, &config, io.Fonts->GetGlyphRangesChineseSimplifiedCommon());

  // Setup backend
  ImGui_ImplGlfw_InitForOpenGL(window, true);
  ImGui_ImplOpenGL3_Init(glsl_version);

  // Main loop
  while (!glfwWindowShouldClose(window)) {
    double frame_start = 0.0;

    // Check for reinit request (triggered by config save)
    if (data.request_reinit) {
      data.terminal.AddLine("=== Reinitializing GUI (config changed) ===", Color::Yellow());

      // Cleanup and recreate all tasks and state
      GUI::ReinitAllTasks(tasks, selected_task, data);

      data.terminal.AddLine("GUI reinitialized successfully", Color::Green());
    }

    // High Performance Mode: GUI sleeps 1 second, all CPU for compute tasks
    if (data.high_performance_mode) {
      std::this_thread::sleep_for(std::chrono::seconds(1)); // 1 FPS
      glfwPollEvents();
      data.coromgr.Poll();
      continue; // Skip rendering entirely
    }

    // Normal Mode: Full GUI rendering
    if constexpr (HIGH_FPS_ON_EVENTS) {
      glfwWaitEventsTimeout(FRAME_TIME);
    } else {
      frame_start = glfwGetTime();
      glfwPollEvents();
    }

    // Poll coroutines
    data.coromgr.Poll();

    // Start frame
    ImGui_ImplOpenGL3_NewFrame();
    ImGui_ImplGlfw_NewFrame();
    ImGui::NewFrame();

    // Draw GUI layout (shared business logic)
    GUI::DrawGUILayout(data, tasks, selected_task);

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

  // Cleanup
  TaskIconBar::CleanupIconBar();
  g_terminal = nullptr;
  ImGui_ImplOpenGL3_Shutdown();
  ImGui_ImplGlfw_Shutdown();
  ImPlot::DestroyContext();
  ImGui::DestroyContext();
  glfwDestroyWindow(window);
  glfwTerminate();

  return 0;
}

} // namespace GUI
