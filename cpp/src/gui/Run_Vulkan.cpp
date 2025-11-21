// Vulkan Rendering Pipeline for GUI
// This file contains only Vulkan-specific rendering code
// Business logic is in Gui.cpp and shared between OpenGL and Vulkan

#include "gui/Gui.h"
#include "imgui.h"
#include "imgui_impl_glfw.h"
#include "imgui_impl_vulkan.h"
#include "implot.h"
#define GLFW_INCLUDE_NONE
#define GLFW_INCLUDE_VULKAN
#include "gui/GuiState.hpp"
#include "gui/GuiTask.hpp"
#include "shared/SharedData.hpp"
#include <GLFW/glfw3.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <vector>
#include <vulkan/vulkan.h>

// ============================================================================
// Configuration: Frame rate limit
// ============================================================================
constexpr double TARGET_FPS = 10.0; // Adjust this to control CPU usage
constexpr double FRAME_TIME = 1.0 / TARGET_FPS;

// Forward declarations for task creation
IGuiTask *CreateSettingsTask();
IGuiTask *CreateSystemInfoTask();

// Forward declarations for icon bar
void InitIconBar();
void CleanupIconBar();

namespace GUI {

// Global pointer to GUI state for error callback
static GuiState *g_gui_state = nullptr;

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
void glfw_error_callback(int error, const char *description) {
  char buffer[512];
  snprintf(buffer, sizeof(buffer), "GLFW Error %d: %s", error, description);
  if (g_gui_state) {
    g_gui_state->AddTerminalLog(buffer);
  }
}

// Vulkan error checking
static void check_vk_result(VkResult err) {
  if (err == 0)
    return;
  char buffer[256];
  snprintf(buffer, sizeof(buffer), "[Vulkan] Error: VkResult = %d", err);
  if (g_gui_state) {
    g_gui_state->AddTerminalLog(buffer);
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
    if (g_gui_state) {
      g_gui_state->AddTerminalLog("Error: No WSI support on physical device");
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

  // Select Present Mode (FIFO for VSync)
  VkPresentModeKHR present_modes[] = {VK_PRESENT_MODE_FIFO_KHR};
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

void RunGUI() {
  // Initialize shared data
  SharedData sharedData;

  // Initialize GUI state
  GuiState guiState;

  // Link GUI state to shared data for logging
  sharedData.gui_state = &guiState;
  g_gui_state = &guiState;

  // Create GUI tasks
  std::vector<IGuiTask *> tasks;
  tasks.push_back(CreateSettingsTask());
  tasks.push_back(CreateSystemInfoTask());

  // Track selected task
  int selected_task = 0;
  tasks[selected_task]->OnExpand();

  // Print startup banner
  guiState.AddTerminalLog("=== Launching GUI ===", TerminalColor::Green());
  guiState.AddTerminalLog("平台窗口库 : Linux(Wayland/X11), macOS(Cocoa), Windows(Win32)", TerminalColor::Green());
  guiState.AddTerminalLog("跨平台窗口管理库 : GLFW (Graphics Library Framework)", TerminalColor::Green());
  guiState.AddTerminalLog("GPU 渲染库 : Vulkan", TerminalColor::Green());
  guiState.AddTerminalLog("UI库(即时模式) : ImGui", TerminalColor::Green());
  guiState.AddTerminalLog("绘图库 : ImPlot", TerminalColor::Green());
  char init_msg[256];
  snprintf(init_msg, sizeof(init_msg), "GUI initialized (Vulkan backend, %.0f FPS)", TARGET_FPS);
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

  // Create window with Vulkan context
  glfwWindowHint(GLFW_CLIENT_API, GLFW_NO_API);
  GLFWwindow *window = glfwCreateWindow(1280, 720, "L2 Data Processor (Vulkan)", nullptr, nullptr);
  if (!window) {
    guiState.AddTerminalLog("Failed to create GLFW window");
    glfwTerminate();
    return;
  }

  if (!glfwVulkanSupported()) {
    guiState.AddTerminalLog("GLFW: Vulkan Not Supported");
    glfwDestroyWindow(window);
    glfwTerminate();
    return;
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

  // Setup Chinese font
  ImGuiIO &io = ImGui::GetIO();
  io.Fonts->Clear();

  ImFontConfig config;
  config.MergeMode = false;
  config.PixelSnapH = true;

  // Load Chinese font from bundled fonts
  const char *font_path = "fonts/NotoSansMonoCJKsc-Regular.otf";
  if (std::ifstream(font_path).good()) {
    io.Fonts->AddFontFromFileTTF(font_path, 16.0f, &config, io.Fonts->GetGlyphRangesChineseFull());
  } else {
    // Fallback to default font
    io.Fonts->AddFontDefault();
    guiState.AddTerminalLog("[Warning] Chinese font not found, using default font");
  }

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

  guiState.AddTerminalLog("Vulkan initialized successfully");

  // Main loop
  while (!glfwWindowShouldClose(window)) {
    glfwWaitEventsTimeout(FRAME_TIME);

    // Update GUI state
    guiState.Update(static_cast<float>(FRAME_TIME));

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
    DrawGUILayout(sharedData, guiState, tasks, selected_task);

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
  }

  // Wait for device to finish
  err = vkDeviceWaitIdle(g_Device);
  check_vk_result(err);

  // Cleanup tasks
  for (auto *task : tasks) {
    delete task;
  }
  tasks.clear();

  // Cleanup
  CleanupIconBar();
  g_gui_state = nullptr;
  ImGui_ImplVulkan_Shutdown();
  ImGui_ImplGlfw_Shutdown();
  ImPlot::DestroyContext();
  ImGui::DestroyContext();

  CleanupVulkanWindow();
  CleanupVulkan();

  glfwDestroyWindow(window);
  glfwTerminate();
}

} // namespace GUI
