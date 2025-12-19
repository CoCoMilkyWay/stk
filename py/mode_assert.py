"""Assert Mode - Optimized build with assertions enabled"""
import subprocess
import time


def run(binary_path, working_dir):
    """Run with assertions enabled."""
    print(f"\n{'='*80}")
    print("Assert Mode - Optimized + Assertions")
    print(f"{'='*80}\n")

    start_time = time.time()
    result = subprocess.run([binary_path], cwd=working_dir)
    elapsed_time = time.time() - start_time

    print(f"\n{'='*80}")
    if result.returncode != 0:
        print(f"[X] Assert Mode Failed! (exit code: {result.returncode}, {elapsed_time:.2f}s)")
        print("(Assert messages should appear above)")
        print(f"{'='*80}")
        raise subprocess.CalledProcessError(result.returncode, binary_path)
    else:
        print(f"[OK] Assert Mode Complete! ({elapsed_time:.2f}s)")
        print(f"{'='*80}")

