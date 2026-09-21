"""Profile Mode - CPU profiling with Tracy"""

import os
import subprocess
import sys
import time
from typing import Optional


def _repo_root() -> str:
    # py/ is directly under repo root
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _find_tracy_profiler_exe() -> Optional[str]:
    # Optional override (absolute or relative path)
    override = os.environ.get("TRACY_PROFILER_EXE") or os.environ.get(
        "TRACY_PROFILER_PATH"
    )
    if override:
        override_path = os.path.abspath(override)
        if os.path.isfile(override_path):
            return override_path

    # Repo vendored Tracy UI
    candidate = os.path.join(_repo_root(), "cpp", "package", "tracy", "tracy-profiler")
    if os.path.isfile(candidate):
        return candidate

    return None


# soname -> apt package (Ubuntu/Debian). tracy-profiler 预编译版的非核心依赖.
_TRACY_DEP_TO_APT = {
    "libglfw.so.3": "libglfw3",
    "libfreetype.so.6": "libfreetype6",
    "libssl.so.3": "libssl3",
    "libcrypto.so.3": "libssl3",
    "libdbus-1.so.3": "libdbus-1-3",
    "libpng16.so.16": "libpng16-16",
    "libbrotlidec.so.1": "libbrotli1",
    "libbrotlicommon.so.1": "libbrotli1",
    "libbz2.so.1.0": "libbz2-1.0",
}


def _missing_tracy_deps(tracy_exe: str) -> list[str]:
    """ldd 检查 tracy-profiler 缺失的共享库 (返回 soname 列表)."""
    res = subprocess.run(["ldd", tracy_exe], capture_output=True, text=True)
    missing = []
    for line in res.stdout.splitlines():
        if "not found" in line:
            soname = line.strip().split()[0]
            if soname not in missing:
                missing.append(soname)
    return missing


def _check_tracy_deps_or_exit(tracy_exe: str) -> None:
    """启动前检查 tracy-profiler 依赖; 缺失则打印安装指令并 sys.exit(1)."""
    missing = _missing_tracy_deps(tracy_exe)
    if not missing:
        return
    pkgs = sorted({_TRACY_DEP_TO_APT[s] for s in missing if s in _TRACY_DEP_TO_APT})
    unmapped = [s for s in missing if s not in _TRACY_DEP_TO_APT]
    print(f"[Tracy] tracy-profiler missing shared libraries: {', '.join(missing)}")
    if pkgs:
        print(f"  Install with:\n  sudo apt install -y {' '.join(pkgs)}")
    if unmapped:
        print(f"  (no apt mapping for: {', '.join(unmapped)} — install manually)")
    sys.exit(1)


def _launch_tracy_ui(tracy_exe: str) -> Optional[subprocess.Popen]:
    # start_new_session: detach from current process group so Ctrl+C
    # only affects the build/run pipeline, not the Tracy UI.
    try:
        # Launch with auto-connect to localhost
        proc = subprocess.Popen(
            [tracy_exe, "-a", "localhost"],
            cwd=os.path.dirname(tracy_exe),
            start_new_session=True,
        )
        print(f"[Tracy] UI launched with auto-connect: {tracy_exe}")
        return proc
    except Exception as e:
        print(f"[Tracy] Failed to launch tracy-profiler: {e}")
        return None


def run(binary_path, working_dir):
    """Run with Tracy profiler."""
    print(f"\n{'='*80}")
    print("Tracy Profiler Mode")
    print(f"{'='*80}\n")

    # Locate tracy-profiler, gate on deps, then launch UI with auto-connect
    tracy_exe = _find_tracy_profiler_exe()
    if not tracy_exe:
        print("[Tracy] tracy-profiler not found (skipping UI auto-launch).")
    else:
        _check_tracy_deps_or_exit(tracy_exe)
        _launch_tracy_ui(tracy_exe)
    time.sleep(1.0)

    print("Starting application with tracy profiling...")
    print("Tracy UI will auto-connect to localhost")
    print("Press Ctrl+C to stop\n")

    # Run binary
    start_time = time.time()
    proc = subprocess.Popen([binary_path], cwd=working_dir)
    try:
        return_code = proc.wait()
    except KeyboardInterrupt:
        print("\n[Interrupted] Stopping application...")
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        return_code = proc.returncode if proc.returncode is not None else 1
    elapsed_time = time.time() - start_time

    print(f"\n{'='*80}")
    if return_code == 0:
        print(f"[OK] Profile Complete! ({elapsed_time:.2f}s)")
    else:
        print(f"[X] Profile exited with code {return_code} ({elapsed_time:.2f}s)")
    print(f"{'='*80}")
