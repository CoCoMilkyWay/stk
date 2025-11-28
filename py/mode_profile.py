"""Profile mode: CPU profiling with gperftools"""
import os
import subprocess
import sys
import time


def run(binary_path, working_dir):
    """Run with gperftools profiler and start web UI."""
    profile_out = os.path.join(working_dir, "lob.prof")
    
    # Clean up
    if os.path.exists(profile_out):
        os.remove(profile_out)
    
    # Run binary
    start_time = time.time()
    try:
        subprocess.run([binary_path], cwd=working_dir, check=True)
    except KeyboardInterrupt:
        print("\n[Interrupted]")
    elapsed_time = time.time() - start_time
    
    # Check result
    if not os.path.exists(profile_out):
        print(f"\nERROR: No profile data: {profile_out}")
        sys.exit(1)
    
    # Start web UI
    print(f"\n{'='*80}")
    print(f"✓ Profile Complete! ({elapsed_time:.2f}s)")
    print(f"{'='*80}")
    print(f"\n🌐 Starting web UI on http://localhost:8080")
    print(f"   (Ctrl+C to stop)\n")
    
    subprocess.run(["pprof", "-http", "localhost:8080", binary_path, profile_out])

