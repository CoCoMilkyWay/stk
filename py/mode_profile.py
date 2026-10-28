"""Profile Mode - 特征计算 perf 采样 (落 output/profile/, 布局见 README)"""

import os
import shutil
import subprocess
import sys
import time
from typing import Optional

# Tracy UI 自动拉起: 与 CMake PROFILE 行的 TRACY_ENABLE 同开同关 (默认 perf 采样, Tracy 不激活)
TRACY_UI = False


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
    # 系统 GLFW 3.4 在 Wayland 会话下优先选 Wayland, 而 tracy-profiler 每帧调 X11 专属接口
    # (刷屏 "X11: Platform not initialized"). GLFW 按 XDG_SESSION_TYPE (+ DISPLAY) 选平台,
    # 缺 WAYLAND_DISPLAY 时 libwayland 仍默认连 wayland-0, 故须改会话类型才能与 app_main 一样走 XWayland.
    env = os.environ.copy()
    env["XDG_SESSION_TYPE"] = "x11"
    try:
        # Launch with auto-connect to localhost
        proc = subprocess.Popen(
            [tracy_exe, "-a", "localhost"],
            cwd=os.path.dirname(tracy_exe),
            env=env,
            start_new_session=True,
        )
        print(f"[Tracy] UI launched with auto-connect: {tracy_exe}")
        return proc
    except Exception as e:
        print(f"[Tracy] Failed to launch tracy-profiler: {e}")
        return None


def _check_perf_or_exit() -> None:
    """启动前检查 perf 可用 + perf_event_paranoid ≤ 1 (app 内开采时同样断言, 这里提前失败)."""
    if not shutil.which("perf"):
        print("[perf] perf not found. Install with:\n  sudo apt install -y linux-tools-$(uname -r)")
        sys.exit(1)
    with open("/proc/sys/kernel/perf_event_paranoid") as f:
        paranoid = int(f.read())
    if paranoid > 1:
        print(f"[perf] kernel.perf_event_paranoid = {paranoid} (须 ≤ 1):\n  sudo sysctl kernel.perf_event_paranoid=1")
        sys.exit(1)


def run(binary_path, working_dir):
    """Run with feature-compute perf sampling."""
    print(f"\n{'='*80}")
    print("Profile Mode - perf sampling feature compute → output/profile/")
    print(f"{'='*80}\n")

    _check_perf_or_exit()
    if TRACY_UI:
        # Locate tracy-profiler, gate on deps, then launch UI with auto-connect
        tracy_exe = _find_tracy_profiler_exe()
        if not tracy_exe:
            print("[Tracy] tracy-profiler not found (skipping UI auto-launch).")
        else:
            _check_tracy_deps_or_exit(tracy_exe)
            _launch_tracy_ui(tracy_exe)
        time.sleep(1.0)

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
