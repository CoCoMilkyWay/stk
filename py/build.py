"""CMake 配置 + 构建 (clang + Ninja); compile_commands.json 拷到 cpp/ 供 clangd"""

import shutil
import subprocess
from pathlib import Path

CPP_DIR = Path(__file__).resolve().parents[1] / "cpp"

# run.py 的 MODE → CMakeLists 选择轴 BUILD_MODE
BUILD_MODE = {
    "tsan": "TSAN",
    "debug": "DEBUG",
    "profile": "PROFILE",
    "assert": "ASSERT",
    "production": "PRODUCTION",
}


def build(project_dir: Path, mode: str, cuda: bool) -> None:
    build_mode = BUILD_MODE[mode]
    gpu_backend = "CUDA" if cuda else "Stub"
    print(f"BUILD_MODE: {build_mode}, GPU_BACKEND: {gpu_backend}")

    configure = [
        "cmake",
        "-S",
        ".",
        "-B",
        "build",
        "-G",
        "Ninja",
        "-DCMAKE_CXX_COMPILER=clang++",
        "-DCMAKE_C_COMPILER=clang",
        f"-DBUILD_MODE={build_mode}",
        f"-DGPU_BACKEND={gpu_backend}",
    ]
    make = ["cmake", "--build", "build", "--parallel"]
    subprocess.run(configure, cwd=project_dir, check=True)
    subprocess.run(make, cwd=project_dir, check=True)

    shutil.copy(
        project_dir / "build" / "compile_commands.json",
        CPP_DIR / "compile_commands.json",
    )
