"""Build & Run Pipeline
Calling chain: run.py -> py/{APP_NAME}.py -> cpp/projects/{APP_NAME}/build.sh

Usage:
    Set ONE mode flag to True, then: python run.py

Modes:
    - TSAN:    Race condition detection (5-15x slower)
    - DEBUG:   Interactive GDB debugging (very slow)
    - PROFILE: CPU profiling with gperftools (slow)
    - Default: Production build (-O3, fastest)
"""
import os
import subprocess
import sys
import time

# Import mode handlers
from py import mode_profile, mode_tsan, mode_debug

# ============================================================================
# Configuration
# ============================================================================
APP_NAME = "main"

# Build & Run modes (set ONE to True)
ENABLE_TSAN = False
ENABLE_DEBUG = False
ENABLE_PROFILE = True


def _cleanup_processes():
    """Kill old processes."""
    patterns = ["pprof.*8080", f"app_{APP_NAME}"]
    for pattern in patterns:
        subprocess.run(["pkill", "-9", "-f", pattern], 
                      capture_output=True, check=False)
    time.sleep(0.3)


def _build(app_name, enable_tsan, enable_debug, enable_profile):
    """Build project via py/{app_name}.py -> build.sh."""
    py_script = f"./py/{app_name}.py"
    
    if not os.path.exists(py_script):
        print(f"ERROR: Build script not found: {py_script}")
        sys.exit(1)
    
    env = os.environ.copy()
    env['TSAN_MODE'] = 'ON' if enable_tsan else 'OFF'
    env['DEBUG_MODE'] = 'ON' if enable_debug else 'OFF'
    env['PROFILE_MODE'] = 'ON' if enable_profile else 'OFF'
    
    result = subprocess.run(["python3", py_script], env=env)
    if result.returncode != 0:
        sys.exit(1)


def _run(binary_path, working_dir, enable_tsan, enable_debug, enable_profile):
    """Run binary with selected mode."""
    if not os.path.exists(binary_path):
        print(f"ERROR: Binary not found: {binary_path}")
        sys.exit(1)
    
    # Dispatch to mode handler
    if enable_tsan:
        mode_tsan.run(binary_path, working_dir)
    elif enable_debug:
        mode_debug.run(binary_path, working_dir)
    elif enable_profile:
        mode_profile.run(binary_path, working_dir)
    else:
        # Production mode
        start_time = time.time()
        subprocess.run([binary_path], cwd=working_dir, check=True)
        elapsed_time = time.time() - start_time
        print(f"\n✓ Complete! ({elapsed_time:.2f}s)")


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    print("Cleanup...")
    _cleanup_processes()
    
    print("Building...")
    _build(APP_NAME, ENABLE_TSAN, ENABLE_DEBUG, ENABLE_PROFILE)
    
    print("Running...")
    build_dir = os.path.abspath(f"cpp/projects/{APP_NAME}/build")
    binary_path = os.path.join(build_dir, f"bin/app_{APP_NAME}")
    _run(binary_path, build_dir, ENABLE_TSAN, ENABLE_DEBUG, ENABLE_PROFILE)


if __name__ == "__main__":
    main()
