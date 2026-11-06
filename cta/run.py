#!/usr/bin/env python3
# run.py — CTA 测试台启动器: 构建 cta_bench (CMake + Ninja, clang) → 跑 (参数原样转交, 见 src/main.cpp 文件头) → plot.py 弹窗
#   python3 cta/run.py [--sets ind] [--models band,tsmom] [--roll 500] [--cost 0.0015] …

import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BUILD = os.path.join(HERE, "build")


def main():
    if not os.path.exists(os.path.join(BUILD, "build.ninja")):
        subprocess.run(
            [
                "cmake",
                "-S",
                HERE,
                "-B",
                BUILD,
                "-G",
                "Ninja",
                "-DCMAKE_CXX_COMPILER=clang++",
            ],
            check=True,
        )
    subprocess.run(["cmake", "--build", BUILD], check=True)
    subprocess.run([os.path.join(BUILD, "cta_bench"), *sys.argv[1:]], check=True)
    freq = sys.argv[sys.argv.index("--freq") + 1] if "--freq" in sys.argv else "1m"
    subprocess.run([sys.executable, os.path.join(HERE, "plot.py"), freq], check=True)


if __name__ == "__main__":
    main()
