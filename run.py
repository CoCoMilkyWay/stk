"""Build & Run Pipeline (Windows/macOS/Linux)

Usage:
    Set ONE mode flag to True, then: python run.py

Modes:
    - TSAN:       Race condition detection (5-15x slower)
    - DEBUG:      Interactive debugging (very slow)
    - PROFILE:    CPU profiling with Tracy
    - ASSERT:     Optimized build with assertions
    - PRODUCTION: Maximum performance (-O3, fastest)
"""
import os
import platform
import subprocess
import sys
import time

# Import mode handlers
from py import mode_tsan, mode_debug, mode_profile, mode_assert, mode_production

# ============================================================================
# Configuration
# ============================================================================
APP_NAME = "main"

# Build & Run modes (set ONLY ONE to True)
ENABLE_TSAN = False
ENABLE_DEBUG = False
ENABLE_PROFILE = False
ENABLE_ASSERT = True
ENABLE_PRODUCTION = False  # Auto-enabled if all others are False


def _cleanup_processes():
    """Kill old processes."""
    if platform.system() == "Windows":
        process_names = [f"app_{APP_NAME}.exe"]
        for name in process_names:
            subprocess.run(["taskkill", "/F", "/IM", name],
                           capture_output=True, check=False)
    else:  # macOS/Linux
        subprocess.run(["pkill", "-f", f"app_{APP_NAME}"],
                       capture_output=True, check=False)
    time.sleep(0.3)


def _build(app_name, enable_tsan, enable_debug, enable_profile, enable_assert):
    """Build project via py/{app_name}.py."""
    py_script = f"py/{app_name}.py"

    if not os.path.exists(py_script):
        print(f"ERROR: Build script not found: {py_script}")
        sys.exit(1)

    env = os.environ.copy()
    env['TSAN_MODE'] = 'ON' if enable_tsan else 'OFF'
    env['DEBUG_MODE'] = 'ON' if enable_debug else 'OFF'
    env['PROFILE_MODE'] = 'ON' if enable_profile else 'OFF'
    env['ASSERT_MODE'] = 'ON' if enable_assert else 'OFF'

    result = subprocess.run(["python", py_script], env=env)
    if result.returncode != 0:
        sys.exit(1)


def _run(binary_path, working_dir, enable_tsan, enable_debug, enable_profile, enable_assert):
    """Run binary with selected mode."""
    if not os.path.exists(binary_path):
        print(f"ERROR: Binary not found: {binary_path}")
        sys.exit(1)

    # Dispatch to mode handler (all modes are symmetric)
    if enable_tsan:
        mode_tsan.run(binary_path, working_dir)
    elif enable_debug:
        mode_debug.run(binary_path, working_dir)
    elif enable_profile:
        mode_profile.run(binary_path, working_dir)
    elif enable_assert:
        mode_assert.run(binary_path, working_dir)
    else:
        mode_production.run(binary_path, working_dir)


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("Cleanup...")
    _cleanup_processes()

    print("Building...")
    _build(APP_NAME, ENABLE_TSAN, ENABLE_DEBUG, ENABLE_PROFILE, ENABLE_ASSERT)

    print("Running...")
    build_dir = os.path.abspath(f"cpp/projects/{APP_NAME}/build")
    exe_ext = ".exe" if platform.system() == "Windows" else ""
    binary_path = os.path.join(build_dir, f"bin/app_{APP_NAME}{exe_ext}")
    _run(binary_path, build_dir, ENABLE_TSAN,
         ENABLE_DEBUG, ENABLE_PROFILE, ENABLE_ASSERT)


if __name__ == "__main__":
    main()
