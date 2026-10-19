"""
Build script for 'main' C++ project (Linux).
Directly invokes CMake to configure and build the project.
"""

import os
import platform
import shutil
import subprocess
import sys


def build_main_project():
    """Build main project using CMake."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cpp_project_dir = os.path.abspath(os.path.join(script_dir, "../cpp/projects/main"))
    build_dir = os.path.join(cpp_project_dir, "build")

    system = platform.system()
    print("=" * 40)
    print(f"  C++ Build Script ({system})")
    print("=" * 40)

    # # Clean build directory
    # if os.path.exists(build_dir):
    #     print("\nCleaning build directory...")
    #     shutil.rmtree(build_dir)
    #     print("✓ Build directory cleaned")

    env = os.environ.copy()

    cmake_args = [
        "cmake",
        "-S",
        ".",
        "-B",
        "build",
    ]

    # Linux: clang + Ninja
    cmake_args.extend(
        [
            "-G",
            "Ninja",
            "-DCMAKE_CXX_COMPILER=clang++",
            "-DCMAKE_C_COMPILER=clang",
        ]
    )

    # 选择轴 (CMakeLists axis): run.py 的 <MODE>_MODE / FACTOR_CUDA 开关 → BUILD_MODE / GPU_BACKEND 取值
    modes = [m for m in ("TSAN", "DEBUG", "PROFILE", "ASSERT") if env.get(f"{m}_MODE") == "ON"]
    assert len(modes) <= 1, f"模式开关互斥: {modes}"
    build_mode = modes[0] if modes else "PRODUCTION"
    gpu_backend = "CUDA" if env.get("FACTOR_CUDA") == "ON" else "Stub"
    print(f"BUILD_MODE: {build_mode}, GPU_BACKEND: {gpu_backend}")
    cmake_args.append(f"-DBUILD_MODE={build_mode}")
    cmake_args.append(f"-DGPU_BACKEND={gpu_backend}")

    # Configure
    print("\nConfiguring with CMake...")
    result = subprocess.run(cmake_args, cwd=cpp_project_dir, env=env)
    if result.returncode != 0:
        print(f"\nError: CMake configure failed with code {result.returncode}")
        sys.exit(result.returncode)

    # Build
    print("\nBuilding...")
    result = subprocess.run(
        ["cmake", "--build", "build", "--parallel"],
        cwd=cpp_project_dir,
        env=env,
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


if __name__ == "__main__":
    build_main_project()
