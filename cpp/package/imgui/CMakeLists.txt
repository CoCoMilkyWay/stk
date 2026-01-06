cmake_minimum_required(VERSION 3.16)
project(imgui LANGUAGES CXX)

# Collect source files
set(IMGUI_SOURCE
    imgui.cpp
    imgui_demo.cpp
    imgui_draw.cpp
    imgui_tables.cpp
    imgui_widgets.cpp
    backends/imgui_impl_glfw.cpp
)

# Add backend-specific implementation
if(USE_VULKAN_BACKEND)
    list(APPEND IMGUI_SOURCE backends/imgui_impl_vulkan.cpp)
else()
    list(APPEND IMGUI_SOURCE backends/imgui_impl_opengl3.cpp)
endif()

add_library(imgui STATIC ${IMGUI_SOURCE})

target_include_directories(imgui SYSTEM PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/backends
)

# Link graphics libraries
if(USE_VULKAN_BACKEND)
    target_link_libraries(imgui PUBLIC glfw Vulkan::Vulkan)
else()
    target_link_libraries(imgui PUBLIC glfw OpenGL::GL)
endif()

