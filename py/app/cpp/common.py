"""cpp/ 源文件枚举、工具定位与结果缓存: format 与 lint 共用"""

import hashlib
import json
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


def file_hash(path: Path) -> str:
    return hashlib.blake2b(path.read_bytes(), digest_size=16).hexdigest()


def load_cache(cache_file: Path, key: str) -> dict:
    if not cache_file.exists():
        return {}
    data = json.loads(cache_file.read_text())
    if data["key"] != key:
        return {}
    return data["files"]


def save_cache(cache_file: Path, key: str, files: dict):
    # 先写临时文件再 rename: 写到一半被打断也不会留下半截 JSON
    cache_file.parent.mkdir(exist_ok=True)
    tmp = cache_file.with_suffix(".tmp")
    tmp.write_text(json.dumps({"key": key, "files": files}, indent=0))
    os.replace(tmp, cache_file)


def iter_sources(root: Path, extensions: set[str]):
    assert root.is_dir(), f"directory not found: {root}"
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if Path(name).suffix in extensions:
                yield Path(dirpath) / name
