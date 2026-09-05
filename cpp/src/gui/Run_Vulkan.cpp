// Vulkan Rendering Pipeline for GUI
// This file contains only Vulkan-specific rendering code
// Business logic is in Gui.cpp and shared between OpenGL and Vulkan

#include "gui/Config.hpp"
#include "gui/Gui.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_icon_bar/TaskIconBar.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "imgui.h"
#include "imgui_impl_glfw.h"
#include "imgui_impl_vulkan.h"
#include "implot.h"
#include "shared/SharedData.hpp"

#define GLFW_INCLUDE_NONE
#define GLFW_INCLUDE_VULKAN
#include <GLFW/glfw3.h>
#include <cstdio>
#include <thread>
#include <vulkan/vulkan.h>

namespace GUI {

// Global pointer for error callback logging
static TaskTerminal *g_terminal = nullptr;

// Vulkan globals
static VkAllocationCallbacks *g_Allocator = nullptr;
static VkInstance g_Instance = VK_NULL_HANDLE;
static VkPhysicalDevice g_PhysicalDevice = VK_NULL_HANDLE;
static VkDevice g_Device = VK_NULL_HANDLE;
static uint32_t g_QueueFamily = (uint32_t)-1;
static VkQueue g_Queue = VK_NULL_HANDLE;
static VkDescriptorPool g_DescriptorPool = VK_NULL_HANDLE;
static ImGui_ImplVulkanH_Window g_MainWindowData;
static int g_MinImageCount = 2;
static bool g_SwapChainRebuild = false;

// GLFW error callback
void glfw_error_callback_vulkan(int error, const char *description) {
  char buffer[512];
  snprintf(buffer, sizeof(buffer), "GLFW Error %d: %s", error, description);
  if (g_terminal) {
    g_terminal->AddLine(buffer);
  }
}

// Vulkan error checking
static void check_vk_result(VkResult err) {
  if (err == 0)
    return;
  char buffer[256];
  snprintf(buffer, sizeof(buffer), "[Vulkan] Error: VkResult = %d", err);
  if (g_terminal) {
    g_terminal->AddLine(buffer);
  }
  if (err < 0) {
    abort();
  }
}

// Check if extension is available
static bool IsExtensionAvailable(const std::vector<VkExtensionProperties> &properties, const char *extension) {
  for (const auto &p : properties) {
    if (strcmp(p.extensionName, extension) == 0)
      return true;
  }
  return false;
}

// Select physical device
static VkPhysicalDevice SetupVulkan_SelectPhysicalDevice() {
  uint32_t gpu_count;
  VkResult err = vkEnumeratePhysicalDevices(g_Instance, &gpu_count, nullptr);
  check_vk_result(err);

  std::vector<VkPhysicalDevice> gpus(gpu_count);
  err = vkEnumeratePhysicalDevices(g_Instance, &gpu_count, gpus.data());
  check_vk_result(err);

  // Prefer discrete GPU
  for (const auto &device : gpus) {
    VkPhysicalDeviceProperties properties;
    vkGetPhysicalDeviceProperties(device, &properties);
    if (properties.deviceType == VK_PHYSICAL_DEVICE_TYPE_DISCRETE_GPU)
      return device;
  }

  // Use integrated GPU
  for (const auto &device : gpus) {
    VkPhysicalDeviceProperties properties;
    vkGetPhysicalDeviceProperties(device, &properties);
    if (properties.deviceType == VK_PHYSICAL_DEVICE_TYPE_INTEGRATED_GPU)
      return device;
  }

  // Use first GPU
  if (gpu_count > 0)
    return gpus[0];

  return VK_NULL_HANDLE;
}

// Setup Vulkan
static void SetupVulkan(const char **extensions, uint32_t extensions_count) {
  VkResult err;

  // Create Vulkan Instance
  {
    VkInstanceCreateInfo create_info = {};
    create_info.sType = VK_STRUCTURE_TYPE_INSTANCE_CREATE_INFO;

    // Enumerate available extensions
    uint32_t properties_count;
    std::vector<VkExtensionProperties> properties;
    vkEnumerateInstanceExtensionProperties(nullptr, &properties_count, nullptr);
    properties.resize(properties_count);
    err = vkEnumerateInstanceExtensionProperties(nullptr, &properties_count, properties.data());
    check_vk_result(err);

    // Build extension list
    std::vector<const char *> instance_extensions(extensions, extensions + extensions_count);

    if (IsExtensionAvailable(properties, VK_KHR_GET_PHYSICAL_DEVICE_PROPERTIES_2_EXTENSION_NAME))
      instance_extensions.push_back(VK_KHR_GET_PHYSICAL_DEVICE_PROPERTIES_2_EXTENSION_NAME);

#ifdef VK_KHR_PORTABILITY_ENUMERATION_EXTENSION_NAME
    if (IsExtensionAvailable(properties, VK_KHR_PORTABILITY_ENUMERATION_EXTENSION_NAME)) {
      instance_extensions.push_back(VK_KHR_PORTABILITY_ENUMERATION_EXTENSION_NAME);
      create_info.flags |= VK_INSTANCE_CREATE_ENUMERATE_PORTABILITY_BIT_KHR;
    }
#endif

    // Create Vulkan Instance
    create_info.enabledExtensionCount = (uint32_t)instance_extensions.size();
    create_info.ppEnabledExtensionNames = instance_extensions.data();
    err = vkCreateInstance(&create_info, g_Allocator, &g_Instance);
    check_vk_result(err);
  }

  // Select Physical Device
  g_PhysicalDevice = SetupVulkan_SelectPhysicalDevice();

  // Select graphics queue family
  {
    uint32_t count;
    vkGetPhysicalDeviceQueueFamilyProperties(g_PhysicalDevice, &count, nullptr);
    std::vector<VkQueueFamilyProperties> queues(count);
    vkGetPhysicalDeviceQueueFamilyProperties(g_PhysicalDevice, &count, queues.data());
    for (uint32_t i = 0; i < count; i++) {
      if (queues[i].queueFlags & VK_QUEUE_GRAPHICS_BIT) {
        g_QueueFamily = i;
        break;
      }
    }
  }

  // Create Logical Device
  {
    std::vector<const char *> device_extensions;
    device_extensions.push_back("VK_KHR_swapchain");

    // Enumerate physical device extension
    uint32_t properties_count;
    std::vector<VkExtensionProperties> properties;
    vkEnumerateDeviceExtensionProperties(g_PhysicalDevice, nullptr, &properties_count, nullptr);
    properties.resize(properties_count);
    vkEnumerateDeviceExtensionProperties(g_PhysicalDevice, nullptr, &properties_count, properties.data());

#ifdef VK_KHR_PORTABILITY_SUBSET_EXTENSION_NAME
    if (IsExtensionAvailable(properties, VK_KHR_PORTABILITY_SUBSET_EXTENSION_NAME))
      device_extensions.push_back(VK_KHR_PORTABILITY_SUBSET_EXTENSION_NAME);
#endif

    const float queue_priority[] = {1.0f};
    VkDeviceQueueCreateInfo queue_info = {};
    queue_info.sType = VK_STRUCTURE_TYPE_DEVICE_QUEUE_CREATE_INFO;
    queue_info.queueFamilyIndex = g_QueueFamily;
    queue_info.queueCount = 1;
    queue_info.pQueuePriorities = queue_priority;

    VkDeviceCreateInfo create_info = {};
    create_info.sType = VK_STRUCTURE_TYPE_DEVICE_CREATE_INFO;
    create_info.queueCreateInfoCount = 1;
    create_info.pQueueCreateInfos = &queue_info;
    create_info.enabledExtensionCount = (uint32_t)device_extensions.size();
    create_info.ppEnabledExtensionNames = device_extensions.data();

    err = vkCreateDevice(g_PhysicalDevice, &create_info, g_Allocator, &g_Device);
    check_vk_result(err);
    vkGetDeviceQueue(g_Device, g_QueueFamily, 0, &g_Queue);
  }

  // Create Descriptor Pool
  {
    VkDescriptorPoolSize pool_sizes[] = {
        {VK_DESCRIPTOR_TYPE_COMBINED_IMAGE_SAMPLER, 1000},
    };
    VkDescriptorPoolCreateInfo pool_info = {};
    pool_info.sType = VK_STRUCTURE_TYPE_DESCRIPTOR_POOL_CREATE_INFO;
    pool_info.flags = VK_DESCRIPTOR_POOL_CREATE_FREE_DESCRIPTOR_SET_BIT;
    pool_info.maxSets = 1000;
    pool_info.poolSizeCount = (uint32_t)IM_ARRAYSIZE(pool_sizes);
    pool_info.pPoolSizes = pool_sizes;
    err = vkCreateDescriptorPool(g_Device, &pool_info, g_Allocator, &g_DescriptorPool);
    check_vk_result(err);
  }
}

// Setup Vulkan Window using helper functions
static void SetupVulkanWindow(ImGui_ImplVulkanH_Window *wd, VkSurfaceKHR surface, int width, int height) {
  wd->Surface = surface;

  // Check for WSI support
  VkBool32 res;
  vkGetPhysicalDeviceSurfaceSupportKHR(g_PhysicalDevice, g_QueueFamily, wd->Surface, &res);
  if (res != VK_TRUE) {
    if (g_terminal) {
      g_terminal->AddLine("Error: No WSI support on physical device");
    }
    exit(-1);
  }

  // Select Surface Format
  const VkFormat requestSurfaceImageFormat[] = {
      VK_FORMAT_B8G8R8A8_UNORM, VK_FORMAT_R8G8B8A8_UNORM,
      VK_FORMAT_B8G8R8_UNORM, VK_FORMAT_R8G8B8_UNORM};
  const VkColorSpaceKHR requestSurfaceColorSpace = VK_COLORSPACE_SRGB_NONLINEAR_KHR;
  wd->SurfaceFormat = ImGui_ImplVulkanH_SelectSurfaceFormat(
      g_PhysicalDevice, wd->Surface, requestSurfaceImageFormat,
      (size_t)IM_ARRAYSIZE(requestSurfaceImageFormat), requestSurfaceColorSpace);

  // Select Present Mode
  VkPresentModeKHR present_modes[] = {
      VSYNC_ENABLE ? VK_PRESENT_MODE_FIFO_KHR : VK_PRESENT_MODE_IMMEDIATE_KHR};
  wd->PresentMode = ImGui_ImplVulkanH_SelectPresentMode(
      g_PhysicalDevice, wd->Surface, &present_modes[0], IM_ARRAYSIZE(present_modes));

  // Create SwapChain, RenderPass, Framebuffer
  ImGui_ImplVulkanH_CreateOrResizeWindow(
      g_Instance, g_PhysicalDevice, g_Device, wd, g_QueueFamily, g_Allocator,
      width, height, g_MinImageCount, VK_IMAGE_USAGE_COLOR_ATTACHMENT_BIT);
}

// Cleanup Vulkan
static void CleanupVulkan() {
  vkDestroyDescriptorPool(g_Device, g_DescriptorPool, g_Allocator);
  vkDestroyDevice(g_Device, g_Allocator);
  vkDestroyInstance(g_Instance, g_Allocator);
}

static void CleanupVulkanWindow() {
  ImGui_ImplVulkanH_DestroyWindow(g_Instance, g_Device, &g_MainWindowData, g_Allocator);
}

// Frame rendering
static void FrameRender(ImGui_ImplVulkanH_Window *wd, ImDrawData *draw_data) {
  VkResult err;

  VkSemaphore image_acquired_semaphore = wd->FrameSemaphores[wd->SemaphoreIndex].ImageAcquiredSemaphore;
  VkSemaphore render_complete_semaphore = wd->FrameSemaphores[wd->SemaphoreIndex].RenderCompleteSemaphore;
  err = vkAcquireNextImageKHR(g_Device, wd->Swapchain, UINT64_MAX, image_acquired_semaphore, VK_NULL_HANDLE, &wd->FrameIndex);
  if (err == VK_ERROR_OUT_OF_DATE_KHR || err == VK_SUBOPTIMAL_KHR) {
    g_SwapChainRebuild = true;
    return;
  }
  check_vk_result(err);

  ImGui_ImplVulkanH_Frame *fd = &wd->Frames[wd->FrameIndex];
  {
    err = vkWaitForFences(g_Device, 1, &fd->Fence, VK_TRUE, UINT64_MAX);
    check_vk_result(err);

    err = vkResetFences(g_Device, 1, &fd->Fence);
    check_vk_result(err);
  }
  {
    err = vkResetCommandPool(g_Device, fd->CommandPool, 0);
    check_vk_result(err);
    VkCommandBufferBeginInfo info = {};
    info.sType = VK_STRUCTURE_TYPE_COMMAND_BUFFER_BEGIN_INFO;
    info.flags |= VK_COMMAND_BUFFER_USAGE_ONE_TIME_SUBMIT_BIT;
    err = vkBeginCommandBuffer(fd->CommandBuffer, &info);
    check_vk_result(err);
  }
  {
    VkRenderPassBeginInfo info = {};
    info.sType = VK_STRUCTURE_TYPE_RENDER_PASS_BEGIN_INFO;
    info.renderPass = wd->RenderPass;
    info.framebuffer = fd->Framebuffer;
    info.renderArea.extent.width = wd->Width;
    info.renderArea.extent.height = wd->Height;
    info.clearValueCount = 1;
    info.pClearValues = &wd->ClearValue;
    vkCmdBeginRenderPass(fd->CommandBuffer, &info, VK_SUBPASS_CONTENTS_INLINE);
  }

  // Record dear imgui primitives into command buffer
  ImGui_ImplVulkan_RenderDrawData(draw_data, fd->CommandBuffer);

  // Submit command buffer
  vkCmdEndRenderPass(fd->CommandBuffer);
  {
    VkPipelineStageFlags wait_stage = VK_PIPELINE_STAGE_COLOR_ATTACHMENT_OUTPUT_BIT;
    VkSubmitInfo info = {};
    info.sType = VK_STRUCTURE_TYPE_SUBMIT_INFO;
    info.waitSemaphoreCount = 1;
    info.pWaitSemaphores = &image_acquired_semaphore;
    info.pWaitDstStageMask = &wait_stage;
    info.commandBufferCount = 1;
    info.pCommandBuffers = &fd->CommandBuffer;
    info.signalSemaphoreCount = 1;
    info.pSignalSemaphores = &render_complete_semaphore;

    err = vkEndCommandBuffer(fd->CommandBuffer);
    check_vk_result(err);
    err = vkQueueSubmit(g_Queue, 1, &info, fd->Fence);
    check_vk_result(err);
  }
}

static void FramePresent(ImGui_ImplVulkanH_Window *wd) {
  if (g_SwapChainRebuild)
    return;
  VkSemaphore render_complete_semaphore = wd->FrameSemaphores[wd->SemaphoreIndex].RenderCompleteSemaphore;
  VkPresentInfoKHR info = {};
  info.sType = VK_STRUCTURE_TYPE_PRESENT_INFO_KHR;
  info.waitSemaphoreCount = 1;
  info.pWaitSemaphores = &render_complete_semaphore;
  info.swapchainCount = 1;
  info.pSwapchains = &wd->Swapchain;
  info.pImageIndices = &wd->FrameIndex;
  VkResult err = vkQueuePresentKHR(g_Queue, &info);
  if (err == VK_ERROR_OUT_OF_DATE_KHR || err == VK_SUBOPTIMAL_KHR) {
    g_SwapChainRebuild = true;
    return;
  }
  check_vk_result(err);
  wd->SemaphoreIndex = (wd->SemaphoreIndex + 1) % wd->SemaphoreCount;
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
  data.terminal.AddLine("GPU 渲染库 : Vulkan", Color::Green());
  data.terminal.AddLine("UI库(即时模式) : ImGui", Color::Green());
  data.terminal.AddLine("绘图库 : ImPlot", Color::Green());
  char init_msg[256];
  snprintf(init_msg, sizeof(init_msg), "GUI initialized (Vulkan backend, %.0f FPS)", TARGET_FPS);
  data.terminal.AddLine(init_msg, Color::Blue());

  // Initialize icon bar with network monitoring
  TaskIconBar::InitIconBar(data.coromgr);

  // Setup error callback
  glfwSetErrorCallback(glfw_error_callback_vulkan);

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

  // Create window with Vulkan context
  glfwWindowHint(GLFW_CLIENT_API, GLFW_NO_API);
  GLFWwindow *window = glfwCreateWindow(WINDOW_WIDTH, WINDOW_HEIGHT, "L2 Data Processor (Vulkan)", nullptr, nullptr);
  if (!window) {
    const char *err_desc = nullptr;
    int err_code = glfwGetError(&err_desc);
    if (err_desc) {
      fprintf(stderr, "ERROR: Failed to create GLFW window (code %d): %s\n", err_code, err_desc);
    } else {
      fprintf(stderr, "ERROR: Failed to create GLFW window (code %d)\n", err_code);
    }
    fprintf(stderr, "Hint: Check X Server compatibility\n");
    data.terminal.AddLine("Failed to create GLFW window");
    glfwTerminate();
    return 1;
  }

  if (!glfwVulkanSupported()) {
    fprintf(stderr, "ERROR: Vulkan not supported on this system\n");
    fprintf(stderr, "Hint: Check Vulkan drivers or use OpenGL backend instead\n");
    data.terminal.AddLine("GLFW: Vulkan Not Supported");
    glfwDestroyWindow(window);
    glfwTerminate();
    return 1;
  }

  // Setup Vulkan
  uint32_t extensions_count = 0;
  const char **extensions = glfwGetRequiredInstanceExtensions(&extensions_count);
  SetupVulkan(extensions, extensions_count);

  // Create Window Surface
  VkSurfaceKHR surface;
  VkResult err = glfwCreateWindowSurface(g_Instance, window, g_Allocator, &surface);
  check_vk_result(err);

  // Create Framebuffers
  int w, h;
  glfwGetFramebufferSize(window, &w, &h);
  ImGui_ImplVulkanH_Window *wd = &g_MainWindowData;
  SetupVulkanWindow(wd, surface, w, h);

  // Setup Dear ImGui context
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

  // Setup Platform/Renderer backends
  ImGui_ImplGlfw_InitForVulkan(window, true);
  ImGui_ImplVulkan_InitInfo init_info = {};
  init_info.Instance = g_Instance;
  init_info.PhysicalDevice = g_PhysicalDevice;
  init_info.Device = g_Device;
  init_info.QueueFamily = g_QueueFamily;
  init_info.Queue = g_Queue;
  init_info.DescriptorPool = g_DescriptorPool;
  init_info.MinImageCount = g_MinImageCount;
  init_info.ImageCount = wd->ImageCount;
  init_info.Allocator = g_Allocator;
  init_info.CheckVkResultFn = check_vk_result;
  init_info.PipelineInfoMain.RenderPass = wd->RenderPass;
  init_info.PipelineInfoMain.Subpass = 0;
  init_info.PipelineInfoMain.MSAASamples = VK_SAMPLE_COUNT_1_BIT;
  ImGui_ImplVulkan_Init(&init_info);

  data.terminal.AddLine("Vulkan initialized successfully");

  // Main loop
  while (!glfwWindowShouldClose(window)) {
    double frame_start = 0.0;
    const double frame_time = data.high_performance_mode ? COMPUTE_FRAME_TIME : FRAME_TIME;

    // Check for reinit request (triggered by config save)
    if (data.request_reinit) {
      data.terminal.AddLine("=== Reinitializing GUI (config changed) ===", Color::Yellow());

      // Cleanup and recreate all tasks and state
      GUI::ReinitAllTasks(tasks, selected_task, data);

      data.terminal.AddLine("GUI reinitialized successfully", Color::Green());
    }

    if constexpr (HIGH_FPS_ON_EVENTS) {
      glfwWaitEventsTimeout(frame_time);
    } else {
      frame_start = glfwGetTime();
      glfwPollEvents();
    }

    // Poll coroutines
    data.coromgr.Poll();

    // Resize swap chain?
    int fb_width, fb_height;
    glfwGetFramebufferSize(window, &fb_width, &fb_height);
    if (fb_width > 0 && fb_height > 0 &&
        (g_SwapChainRebuild || g_MainWindowData.Width != fb_width ||
         g_MainWindowData.Height != fb_height)) {
      ImGui_ImplVulkan_SetMinImageCount(g_MinImageCount);
      ImGui_ImplVulkanH_CreateOrResizeWindow(
          g_Instance, g_PhysicalDevice, g_Device, wd, g_QueueFamily,
          g_Allocator, fb_width, fb_height, g_MinImageCount,
          VK_IMAGE_USAGE_COLOR_ATTACHMENT_BIT);
      g_MainWindowData.FrameIndex = 0;
      g_SwapChainRebuild = false;
    }
    if (glfwGetWindowAttrib(window, GLFW_ICONIFIED) != 0) {
      ImGui_ImplGlfw_Sleep(10);
      continue;
    }

    // Start frame
    ImGui_ImplVulkan_NewFrame();
    ImGui_ImplGlfw_NewFrame();
    ImGui::NewFrame();

    // Draw GUI layout (shared business logic)
    GUI::DrawGUILayout(data, tasks, selected_task);

    // Render
    ImGui::Render();
    ImDrawData *draw_data = ImGui::GetDrawData();
    const bool is_minimized = (draw_data->DisplaySize.x <= 0.0f || draw_data->DisplaySize.y <= 0.0f);
    if (!is_minimized) {
      wd->ClearValue.color.float32[0] = 0.1f;
      wd->ClearValue.color.float32[1] = 0.1f;
      wd->ClearValue.color.float32[2] = 0.1f;
      wd->ClearValue.color.float32[3] = 1.0f;
      FrameRender(wd, draw_data);
      FramePresent(wd);
    }

    // Enforce fixed frame rate (only when HIGH_FPS_ON_EVENTS is disabled)
    if constexpr (!HIGH_FPS_ON_EVENTS) {
      double frame_end = glfwGetTime();
      double elapsed = frame_end - frame_start;
      if (elapsed < frame_time) {
        auto sleep_duration = std::chrono::duration<double>(frame_time - elapsed);
        std::this_thread::sleep_for(sleep_duration);
      }
    }
  }

  // Wait for device to finish
  err = vkDeviceWaitIdle(g_Device);
  check_vk_result(err);

  // Cleanup
  TaskIconBar::CleanupIconBar();
  g_terminal = nullptr;
  ImGui_ImplVulkan_Shutdown();
  ImGui_ImplGlfw_Shutdown();
  ImPlot::DestroyContext();
  ImGui::DestroyContext();
  CleanupVulkanWindow();
  CleanupVulkan();
  glfwDestroyWindow(window);
  glfwTerminate();

  return 0;
}

} // namespace GUI
