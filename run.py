import os
import signal
import subprocess
import sys
import time

# ============================================================================
# Build & Run Pipeline
# ============================================================================
# Calling chain: run.py -> py/{APP_NAME}.py -> cpp/projects/{APP_NAME}/build.sh
#
# - run.py: orchestrates build -> run -> profiling
# - py/{APP_NAME}.py: project-specific preparation + triggers build.sh
# - build.sh: compiles C++ + copies compile_commands.json for clangd
# ============================================================================

# ============================================================================
# Configuration
# ============================================================================
APP_NAME = "main"

# Build & Run modes (set ONE to True, others to False)
ENABLE_PROFILE = False
ENABLE_DEBUG   = False
# Default: production mode with -O3 optimizations

# Profiler settings
CPUPROFILE_FREQUENCY = 1000000                 # Sampling rate (Hz)
PROFILER_LIB = '/usr/lib/x86_64-linux-gnu/libprofiler.so.0'
TARGET_NAMESPACE = "Analysis"                  # Focus namespace
PPROF_PORT = 8080                              # Web GUI port
PPROF_IGNORE = "std::|__gnu_cxx::"             # Filter standard library


def _cleanup_background_processes():
    """Kill background processes that may cause memory bloat."""
    processes_to_kill = [
        f"pprof.*{PPROF_PORT}",      # Old pprof web servers
        f"app_{APP_NAME}",            # Old app instances
    ]

    for pattern in processes_to_kill:
        subprocess.run(["pkill", "-9", "-f", pattern],
                       capture_output=True, check=False)

    time.sleep(0.3)


def _cleanup_old_profiler():
    """Kill old pprof web server."""
    subprocess.run(["pkill", "-f", f"pprof.*{PPROF_PORT}"],
                   check=False, capture_output=True)
    time.sleep(0.2)


def _find_pprof_command():
    """Find pprof executable."""
    for cmd in ["pprof", "google-pprof", os.path.expanduser("~/go/bin/pprof")]:
        result = subprocess.run(
            ["which", cmd], capture_output=True, check=False)
        if result.returncode == 0:
            return cmd
    return None


def _show_profile_report(binary_path, profile_file):
    """Generate and display profiling report."""
    pprof_cmd = _find_pprof_command()
    if not pprof_cmd:
        print("pprof not found. Profile data saved to:", profile_file)
        return

    print(f"\n{'='*80}")
    print(f"SAMPLING PROFILE - Top Functions ({CPUPROFILE_FREQUENCY} Hz)")
    print(f"{'='*80}\n")

    pprof_top = subprocess.run(
        [pprof_cmd, "--top", "--cum", "--nodecount=20",
         f"--focus={TARGET_NAMESPACE}", f"--hide={PPROF_IGNORE}",
         binary_path, profile_file],
        capture_output=True, text=True, check=False
    )

    if pprof_top.returncode == 0:
        print(pprof_top.stdout)

    # Launch web server
    subprocess.Popen(
        [pprof_cmd, f"-http=:{PPROF_PORT}", binary_path, profile_file],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    time.sleep(1.5)
    subprocess.run(["cmd.exe", "/c", "start", f"http://localhost:{PPROF_PORT}"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)

    print(f"\n✓ Web GUI: http://localhost:{PPROF_PORT}")
    print(f"  To stop: pkill -f 'pprof.*{PPROF_PORT}'\n")


def run_with_profiling(binary_path, working_dir):
    """Run C++ binary with gperftools profiler."""
    profile_file = os.path.join(working_dir, "profile.out")

    _cleanup_old_profiler()
    if os.path.exists(profile_file):
        os.remove(profile_file)

    print(f"Running with gperftools profiler ({CPUPROFILE_FREQUENCY} Hz)...")

    env = os.environ.copy()
    env['CPUPROFILE'] = profile_file
    env['CPUPROFILE_FREQUENCY'] = str(CPUPROFILE_FREQUENCY)
    if os.path.exists(PROFILER_LIB):
        env['LD_PRELOAD'] = PROFILER_LIB

    start_time = time.time()
    subprocess.run([binary_path], cwd=working_dir, env=env, check=True)
    elapsed_time = time.time() - start_time

    print(f"\nProfiling complete (Time: {elapsed_time:.2f}s)")

    if os.path.exists(profile_file):
        _show_profile_report(binary_path, profile_file)
    else:
        print(
            "\nProfile not generated. Install: sudo apt-get install libgoogle-perftools-dev")


def build_project(app_name, enable_profile, enable_debug):
    """Trigger build via py/{app_name}.py -> build.sh."""
    py_script = f"./py/{app_name}.py"

    if not os.path.exists(py_script):
        print(f"Error: Build script not found: {py_script}")
        sys.exit(1)

    env = os.environ.copy()
    env['PROFILE_MODE'] = 'ON' if enable_profile else 'OFF'
    env['DEBUG_MODE']   = 'ON' if enable_debug   else 'OFF'

    result = subprocess.run(["python3", py_script], env=env, check=False)

    if result.returncode != 0:
        print(f"\nBuild failed with exit code {result.returncode}")
        sys.exit(1)


def run_with_gdb_debug(binary_path, working_dir):
    """Run binary under GDB with automatic thread stack trace capture."""
    log_dir = os.path.abspath("output/log")
    os.makedirs(log_dir, exist_ok=True)

    trace_file = os.path.join(log_dir, "gdb_trace.txt")
    gdb_script = os.path.join(working_dir, "auto_debug.gdb")

    # Generate GDB script for automatic trace capture
    gdb_commands = f"""# Auto-generated GDB script
set pagination off
set logging file {trace_file}
set logging overwrite on
set logging on
set confirm off

# Catch signals and crashes
catch signal SIGSEGV SIGABRT SIGFPE SIGILL SIGBUS
commands
  echo \\n=== CRASH DETECTED ===\\n
  info threads
  echo \\n=== STACK TRACES ===\\n
  thread apply all bt
  quit
end

# Run program
run

# Normal exit
echo \\n=== NORMAL EXIT ===\\n
info threads
thread apply all bt

set logging off
quit
"""

    with open(gdb_script, 'w') as f:
        f.write(gdb_commands)

    print(f"Running under GDB with automatic stack trace capture...")
    print(f"Trace will be saved to: {trace_file}")
    print(f"Press Ctrl+C to capture current thread state.\n")

    start_time = time.time()

    # Run under GDB
    proc = subprocess.Popen(
        ["gdb", "-x", gdb_script, binary_path],
        cwd=working_dir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    try:
        # Stream output in real-time
        if proc.stdout:
            for line in proc.stdout:
                print(line, end='')

        proc.wait()
        elapsed = time.time() - start_time

        if proc.returncode == 0:
            print(f"\n✓ Program completed successfully")
            print(f"Execution time: {elapsed:.2f}s ({elapsed/60:.2f}min)")
        else:
            print(f"\n✗ Program exited with code {proc.returncode}")

    except KeyboardInterrupt:
        print("\n\n=== Ctrl+C detected - Capturing thread state ===")

        # Send interrupt to GDB (which forwards to program)
        proc.send_signal(signal.SIGINT)
        time.sleep(0.5)

        # Send GDB commands to capture state
        capture_commands = """
echo \\n=== MANUAL INTERRUPT (Ctrl+C) ===\\n
info threads
echo \\n=== STACK TRACES ===\\n
thread apply all bt
quit
"""
        if proc.stdin:
            proc.stdin.write(capture_commands)
            proc.stdin.flush()

        # Wait for GDB to finish
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

        elapsed = time.time() - start_time
        print(f"\n✓ Thread trace captured to: {trace_file}")
        print(f"Execution time: {elapsed:.2f}s ({elapsed/60:.2f}min)")
        print(f"Analyze with: less {trace_file}")
        sys.exit(1)


def run_binary(binary_path, working_dir, enable_profile, enable_debug):
    """Run binary with optional profiling or GDB debugging."""
    if not os.path.exists(binary_path):
        print(f"Error: Binary not found: {binary_path}")
        sys.exit(1)

    if enable_debug:
        run_with_gdb_debug(binary_path, working_dir)
    elif enable_profile:
        run_with_profiling(binary_path, working_dir)
    else:
        start_time = time.time()
        subprocess.run([binary_path], cwd=working_dir, check=True)
        elapsed = time.time() - start_time
        print(f"\nExecution time: {elapsed:.2f}s ({elapsed/60:.2f}min)")


def main():
    # Change to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Cleanup background processes first
    print("Cleaning up background processes...")
    _cleanup_background_processes()

    # Build project
    build_project(APP_NAME, ENABLE_PROFILE, ENABLE_DEBUG)

    # Run binary from build directory (binary expects to run from build/ for relative paths)
    build_dir = os.path.abspath(f"cpp/projects/{APP_NAME}/build")
    binary_path = os.path.join(build_dir, f"bin/app_{APP_NAME}")
    run_binary(binary_path, build_dir, ENABLE_PROFILE, ENABLE_DEBUG)


if __name__ == "__main__":
    main()
