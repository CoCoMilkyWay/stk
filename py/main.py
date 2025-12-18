"""
Build script for 'main' C++ project (Windows).
Directly invokes CMake to configure and build the project.
"""

import os
import shutil
import subprocess
import sys


def build_main_project():
    """Build main project using CMake."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cpp_project_dir = os.path.abspath(
        os.path.join(script_dir, "../cpp/projects/main"))
    build_dir = os.path.join(cpp_project_dir, "build")

    print("=" * 40)
    print("  C++ Build Script (Windows)")
    print("=" * 40)

    # # Clean build directory
    # if os.path.exists(build_dir):
    #     print("\nCleaning build directory...")
    #     shutil.rmtree(build_dir)
    #     print("✓ Build directory cleaned")

    env = os.environ.copy()

    # Build configuration
    build_type = "Release"
    cmake_args = [
        "cmake",
        "-G", "Ninja",
        "-S", ".",
        "-B", "build",
        "-DCMAKE_CXX_COMPILER=clang-cl",
        "-DCMAKE_C_COMPILER=clang-cl",
        f"-DCMAKE_BUILD_TYPE={build_type}",
        "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON"
    ]

    # ThreadSanitizer mode
    if env.get('TSAN_MODE') == 'ON':
        print("ThreadSanitizer mode: ENABLED")
        cmake_args.append("-DTSAN_MODE=ON")
    else:
        cmake_args.append("-DTSAN_MODE=OFF")

    # Profile mode
    if env.get('PROFILE_MODE') == 'ON':
        print("Profile mode: ENABLED")
        cmake_args.append("-DPROFILE_MODE=ON")
    else:
        cmake_args.append("-DPROFILE_MODE=OFF")

    # Debug mode
    if env.get('DEBUG_MODE') == 'ON':
        print("Debug mode: ENABLED")
        cmake_args.append("-DDEBUG_MODE=ON")
    else:
        cmake_args.append("-DDEBUG_MODE=OFF")

    # Assert mode
    if env.get('ASSERT_MODE') == 'ON':
        print("Assert mode: ENABLED")
        cmake_args.append("-DASSERT_MODE=ON")
    else:
        cmake_args.append("-DASSERT_MODE=OFF")

    # Configure
    print("\nConfiguring with CMake...")
    result = subprocess.run(cmake_args, cwd=cpp_project_dir, env=env)
    if result.returncode != 0:
        print(f"\nError: CMake configure failed with code {result.returncode}")
        sys.exit(result.returncode)

    # Build
    print("\nBuilding...")
    result = subprocess.run(
        ["cmake", "--build", "build", "--config", build_type, "--parallel"],
        cwd=cpp_project_dir,
        env=env
    )
    if result.returncode != 0:
        print(f"\nError: CMake build failed with code {result.returncode}")
        sys.exit(result.returncode)

    # Copy compile_commands.json for clangd
    compile_commands = os.path.join(build_dir, "compile_commands.json")
    if os.path.exists(compile_commands):
        dest = os.path.join(cpp_project_dir, "../../compile_commands.json")
        shutil.copy(compile_commands, dest)
        print("\n✓ compile_commands.json copied for clangd")

    print("\n" + "=" * 40)
    print("  Build completed successfully!")
    print("=" * 40)


if __name__ == '__main__':
    build_main_project()
