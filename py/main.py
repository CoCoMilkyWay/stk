#!/usr/bin/env python3
"""
Build script for 'main' C++ project.
Triggers build.sh after any project-specific preparation.
"""

import os
import subprocess
import sys

def build_main_project():
    """Trigger build.sh for main project."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cpp_project_dir = os.path.join(script_dir, "../cpp/projects/main")
    
    print("Building C++ project: main")
    
    # Pass through TSAN_MODE, PROFILE_MODE, and DEBUG_MODE to build.sh
    env = os.environ.copy()
    
    result = subprocess.run(
        ["./build.sh"],
        cwd=cpp_project_dir,
        env=env,
        check=False
    )
    
    if result.returncode != 0:
        print(f"\nError: build.sh exited with code {result.returncode}")
        sys.exit(result.returncode)
    
    print("Build successful!")


if __name__ == '__main__':
    build_main_project()
