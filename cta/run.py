#!/usr/bin/env python3
# run.py — CTA 测试台入口: 张量 (缺或旧于篮子账本即重建) → 构建 cta_bench (CMake + Ninja, clang) → 跑
#   python3 cta/run.py [cta_bench 参数 …]   例: --sets ind --models ma,ens --cost 0.0015 --stop intra
#   参数见 src/main.cpp 文件头; --dir 由这里给 (日线张量目录)

import os
import subprocess
import sys

import tensor

HERE = os.path.dirname(os.path.abspath(__file__))
BUILD = os.path.join(HERE, "build")


def main():
    if tensor.stale():
        tensor.build()
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
    subprocess.run(
        [os.path.join(BUILD, "cta_bench"), "--dir", tensor.OUT, *sys.argv[1:]],
        check=True,
    )


if __name__ == "__main__":
    main()
