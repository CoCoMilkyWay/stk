"""Debug mode: Interactive debugging with LLDB (Windows)"""
import os
import signal
import subprocess
import sys
import time


def run(binary_path, working_dir):
    """Run with LLDB debugger."""
    log_dir = os.path.abspath("output/log")
    os.makedirs(log_dir, exist_ok=True)
    trace_file = os.path.join(log_dir, "lldb_trace.txt")
    lldb_script = os.path.join(working_dir, "auto_debug.lldb")
    
    # Generate LLDB script
    lldb_commands = f"""# Auto-generated LLDB script
settings set stop-line-count-after 3
settings set stop-line-count-before 3
log enable lldb all -f {trace_file}

# Catch signals and crashes
breakpoint set -n main
breakpoint command add
bt all
continue
DONE

# Run program
run

# Capture final state
thread backtrace all
quit
"""
    
    with open(lldb_script, 'w') as f:
        f.write(lldb_commands)
    
    # Run under LLDB
    print("Running under LLDB (automatic stack trace capture)...")
    print(f"Trace: {trace_file}")
    print(f"Press Ctrl+C to capture thread state\n")
    start_time = time.time()
    
    proc = subprocess.Popen(
        ["lldb", "-s", lldb_script, binary_path],
        cwd=working_dir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    try:
        if proc.stdout:
            for line in proc.stdout:
                print(line, end='')
        
        proc.wait()
        elapsed_time = time.time() - start_time
        
        print(f"\n{'='*80}")
        if proc.returncode == 0:
            print(f"✓ Debug Complete! ({elapsed_time:.2f}s)")
        else:
            print(f"✗ Exited with code {proc.returncode} ({elapsed_time:.2f}s)")
        print(f"{'='*80}\n")
        
    except KeyboardInterrupt:
        print("\n\n=== Ctrl+C: Capturing thread state ===")
        
        proc.send_signal(signal.SIGINT)
        time.sleep(0.5)
        
        if proc.stdin:
            proc.stdin.write("bt all\nquit\n")
            proc.stdin.flush()
        
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        
        elapsed_time = time.time() - start_time
        print(f"\n✓ Trace captured: {trace_file} ({elapsed_time:.2f}s)")
        sys.exit(1)

