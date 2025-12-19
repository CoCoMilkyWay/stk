"""Production Mode - Fastest optimized build"""
import subprocess
import time


def run(binary_path, working_dir):
    """Run in production mode."""
    print(f"\n{'='*80}")
    print("Production Mode - Maximum Performance")
    print(f"{'='*80}\n")

    start_time = time.time()
    result = subprocess.run([binary_path], cwd=working_dir)
    elapsed_time = time.time() - start_time

    print(f"\n{'='*80}")
    if result.returncode != 0:
        print(f"[X] Production Failed! (exit code: {result.returncode}, {elapsed_time:.2f}s)")
        print(f"{'='*80}")
        raise subprocess.CalledProcessError(result.returncode, binary_path)
    else:
        print(f"[OK] Production Complete! ({elapsed_time:.2f}s)")
        print(f"{'='*80}")

