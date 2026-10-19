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
    backends/imgui_impl_${GUI_BACKEND_IMPL}.cpp
)

add_library(imgui STATIC ${IMGUI_SOURCE})

target_include_directories(imgui SYSTEM PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/backends
)

# Link graphics libraries (GUI_BACKEND_IMPL / GUI_BACKEND_LINK 由主 CMakeLists 的 GUI_BACKEND 轴给出)
target_link_libraries(imgui PUBLIC glfw ${GUI_BACKEND_LINK})

