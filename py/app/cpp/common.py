"""cpp/ 源文件枚举与工具定位: format 与 lint 共用"""

import os
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CPP_DIR = PROJECT_ROOT / "cpp"

FORMAT_EXTENSIONS = {".hpp", ".cpp", ".h", ".cc", ".hh", ".ipp", ".cxx", ".cu", ".cuh"}
LINT_EXTENSIONS = FORMAT_EXTENSIONS - {".cu", ".cuh"}

# 构建产物与第三方代码 (package/: arrow, glfw, imgui, implot, microtex, nlohmann, tinyxml2,
# tracy, utfcpp, yyjson, zlib, zstd; boost/: cpp/include/boost)
SKIP_DIRS = {
    "build",
    ".git",
    "CMakeFiles",
    "cmake-build-debug",
    "cmake-build-release",
    "package",
    "boost",
}


def which(tool: str) -> str:
    found = shutil.which(tool)
    assert found, f"{tool} not found on PATH"
    return found


def iter_sources(root: Path, extensions: set[str]):
    assert root.is_dir(), f"directory not found: {root}"
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if Path(name).suffix in extensions:
                yield Path(dirpath) / name
