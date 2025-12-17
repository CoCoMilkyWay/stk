@echo off
REM C++ Build Script (Windows)
REM Compiles the project and copies compile_commands.json for clangd

setlocal enabledelayedexpansion

echo ========================================
echo   C++ Build Script (Windows)
echo ========================================

REM Build configuration
set BUILD_TYPE=Release
set CMAKE_ARGS=-S . -B build
set CMAKE_ARGS=%CMAKE_ARGS% -DCMAKE_BUILD_TYPE=%BUILD_TYPE%
set CMAKE_ARGS=%CMAKE_ARGS% -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

REM ThreadSanitizer mode
if "%TSAN_MODE%"=="ON" (
    echo ThreadSanitizer mode: ENABLED
    set CMAKE_ARGS=%CMAKE_ARGS% -DTSAN_MODE=ON
) else (
    set CMAKE_ARGS=%CMAKE_ARGS% -DTSAN_MODE=OFF
)

REM Profile mode
if "%PROFILE_MODE%"=="ON" (
    echo Profile mode: ENABLED
    set CMAKE_ARGS=%CMAKE_ARGS% -DPROFILE_MODE=ON
) else (
    set CMAKE_ARGS=%CMAKE_ARGS% -DPROFILE_MODE=OFF
)

REM Debug mode
if "%DEBUG_MODE%"=="ON" (
    echo Debug mode: ENABLED
    set CMAKE_ARGS=%CMAKE_ARGS% -DDEBUG_MODE=ON
) else (
    set CMAKE_ARGS=%CMAKE_ARGS% -DDEBUG_MODE=OFF
)

REM Assert mode
if "%ASSERT_MODE%"=="ON" (
    echo Assert mode: ENABLED
    set CMAKE_ARGS=%CMAKE_ARGS% -DASSERT_MODE=ON
) else (
    set CMAKE_ARGS=%CMAKE_ARGS% -DASSERT_MODE=OFF
)

REM Configure
echo.
echo Configuring with CMake...
cmake %CMAKE_ARGS%
if errorlevel 1 (
    echo CMake configuration failed!
    exit /b 1
)

REM Build
echo.
echo Building...
cmake --build build --config %BUILD_TYPE% --parallel
if errorlevel 1 (
    echo Build failed!
    exit /b 1
)

REM Copy compile_commands.json for clangd
if exist "build\compile_commands.json" (
    copy /Y "build\compile_commands.json" "..\..\compile_commands.json" >nul
    echo.
    echo ✓ compile_commands.json copied for clangd
)

echo.
echo ========================================
echo   Build completed successfully!
echo ========================================

