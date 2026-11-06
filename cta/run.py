#!/usr/bin/env python3
# run.py — CTA 启动器: 构建 (CMake + Ninja, clang) → 按 APP 跑
#   APP = "trace": cta_trace 交互轨迹 (浏览器选资产 / band 模型 + 优化目标, 后端滚动调参现算); python3 cta/run.py [端口]
#   APP = "bench": cta_bench 测试台 (参数原样转交, 见 src/main.cpp 文件头) → plot.py 弹窗; python3 cta/run.py [--sets ind] [--roll 500] …

import os
import subprocess
import sys

APP = "trace"  # trace | bench

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
    if APP == "trace":
        subprocess.run([os.path.join(BUILD, "cta_trace"), *sys.argv[1:]], check=True)
        return
    assert APP == "bench", APP
    subprocess.run([os.path.join(BUILD, "cta_bench"), *sys.argv[1:]], check=True)
    freq = sys.argv[sys.argv.index("--freq") + 1] if "--freq" in sys.argv else "1m"
    subprocess.run([sys.executable, os.path.join(HERE, "plot.py"), freq], check=True)


if __name__ == "__main__":
    main()
