"""Build & Run Pipeline (Linux)

Usage:
    Set MODE, then: python run.py
"""

import os
import subprocess
import sys
import time

# Import mode handlers
from py import mode_tsan, mode_debug, mode_profile, mode_assert, mode_production

# ============================================================================
# Configuration
# ============================================================================
APP_NAME = "main"

# Build & Run mode:
#   MODE          优化  调试符号  断言  运行方式                     产出
#   "tsan"        -O1   -g        开    ThreadSanitizer, 首错即停    output/log/tsan_report.log
#   "debug"       -O0   -g        开    gdb 托管, 崩溃抓全线程栈     output/debug/crash_stacks.txt
#   "profile"     -O2   -g        开    perf 采样特征计算            output/profile/
#   "assert"      -O2   -         开    直接运行                     -
#   "production"  -O2   -         关    直接运行 (NDEBUG)            -
MODE = "assert"

# 因子 GPU 后端 (factor/*/Gpu.cuh, nvcc): 与上面的模式正交. ON 需要完整 CUDA Toolkit (nvcc + CCCL),
# op_check 与 app_main (Factors→Operators 页 GPU 列) 一并链 cudart; OFF 链 stub, GPU 列显示 n/a.
ENABLE_CUDA = True


def _sync_remote():
    """Fast-forward to upstream if behind, then re-exec so run.py itself is fresh."""
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    subprocess.run(["git", "fetch", "--quiet"], env=env, timeout=15, check=True)
    behind = subprocess.run(
        ["git", "rev-list", "--count", "HEAD..@{u}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if behind == "0":
        return
    print(f"Pulling {behind} new commit(s)...")
    subprocess.run(["git", "pull", "--ff-only", "--autostash"], env=env, check=True)
    os.execv(sys.executable, [sys.executable] + sys.argv)


def _cleanup_processes():
    """Kill old processes."""
    subprocess.run(["pkill", "-f", f"app_{APP_NAME}"], capture_output=True, check=False)
    time.sleep(0.3)


def _build(app_name, mode, enable_cuda):
    """Build project via py/{app_name}.py."""
    py_script = f"py/{app_name}.py"

    if not os.path.exists(py_script):
        print(f"ERROR: Build script not found: {py_script}")
        sys.exit(1)

    env = os.environ.copy()
    env["TSAN_MODE"] = "ON" if mode == "tsan" else "OFF"
    env["DEBUG_MODE"] = "ON" if mode == "debug" else "OFF"
    env["PROFILE_MODE"] = "ON" if mode == "profile" else "OFF"
    env["ASSERT_MODE"] = "ON" if mode == "assert" else "OFF"
    env["FACTOR_CUDA"] = "ON" if enable_cuda else "OFF"

    result = subprocess.run(["python", py_script], env=env)
    if result.returncode != 0:
        sys.exit(1)


def _run(binary_path, working_dir, mode):
    """Run binary with selected mode."""
    if not os.path.exists(binary_path):
        print(f"ERROR: Binary not found: {binary_path}")
        sys.exit(1)

    match mode:
        case "tsan":
            mode_tsan.run(binary_path, working_dir)
        case "debug":
            mode_debug.run(binary_path, working_dir)
        case "profile":
            mode_profile.run(binary_path, working_dir)
        case "assert":
            mode_assert.run(binary_path, working_dir)
        case "production":
            mode_production.run(binary_path, working_dir)


def main():
    assert MODE in ("tsan", "debug", "profile", "assert", "production"), MODE
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("Syncing remote...")
    _sync_remote()

    print("Cleanup...")
    _cleanup_processes()

    print("Formatting...")
    result = subprocess.run(["python", "py/app/format_cpp.py"])
    if result.returncode != 0:
        sys.exit(1)

    print("Building...")
    _build(APP_NAME, MODE, ENABLE_CUDA)

    print("Running...")
    build_dir = os.path.abspath(f"cpp/projects/{APP_NAME}/build")
    binary_path = os.path.join(build_dir, f"bin/app_{APP_NAME}")
    _run(binary_path, build_dir, MODE)


if __name__ == "__main__":
    main()
