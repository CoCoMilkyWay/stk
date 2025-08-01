@echo off
echo ========================================
echo    BinaryParser Build Script
echo ========================================

REM Set up Visual Studio environment using the official script
echo Setting up Visual Studio environment...
call "D:\WS\VC\Auxiliary\Build\vcvarsall.bat" x64

REM Set compiler environment variables
set CC=cl
set CXX=cl

::REM Clear old build directory to avoid generator conflicts
::if exist "build" (
::    echo Clearing old build directory...
::    rmdir /s /q build
::)

REM Configure with CMake using Ninja Multi-Config generator
echo Configuring project with CMake using Ninja Multi-Config generator...
cmake -S . -B build -G "Ninja Multi-Config" -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

REM Check if configuration was successful
if %ERRORLEVEL% neq 0 (
    echo CMake configuration failed!
    pause
    exit /b 1
)

REM Build the project
echo Building project...
cmake --build build --config Release

REM Check if build was successful
if %ERRORLEVEL% neq 0 (
    echo Build failed!
    pause
    exit /b 1
)

echo Build completed successfully!

REM Copy compile_commands.json to root directory for IDE access
if exist "build\compile_commands.json" (
    echo Copying compile_commands.json to root directory...
    copy "build\compile_commands.json" "compile_commands.json"
    echo compile_commands.json copied to root directory
) else (
    echo Warning: compile_commands.json not found
)

REM Move executable to root directory
if exist "build\bin\Release\binary_parser.exe" (
    echo Moving executable to root directory...
    move "build\bin\Release\binary_parser.exe" "binary_parser.exe"
    echo Executable moved to: binary_parser.exe
) else (
    echo Warning: Executable not found at expected location
)

REM Run the executable automatically
if exist "binary_parser.exe" (
    echo Running binary_parser.exe...
    binary_parser.exe
) else (
    echo Error: binary_parser.exe not found in root directory
)

echo.
echo ========================================
echo    Build process finished successfully!
echo    compile_commands.json is available for IDE
echo ======================================== 