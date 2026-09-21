#!/usr/bin/env python3
"""
流式 (op_stream, C++) vs 向量式 (ops/, torch) 对拍. 合入门槛: 全部 PASS 才退出 0.

    python3 py/factor/check/run.py                 # 全表
    python3 py/factor/check/run.py --op TsMean     # 单算子 (可多次)
    python3 py/factor/check/run.py --device cuda   # 向量式跑 GPU
    python3 py/factor/check/run.py --keep          # 保留中间 npy (check/data/<op>/<case>/)

流程: OpTable → 每算子若干 Case (cases.py) → npy 落盘 → op_stream 读写 out.npy → torch 算 → 比对 (tol.py)
判定: NaN 掩码逐位一致 (mismatch = 0) 且 有效位 |Δ| ≤ atol + rtol·max(|a|,|b|)
结果列: op / case / nan_mismatch / n_valid / max_abs / max_rel / 状态 (PASS / FAIL / MISSING_VEC / STREAM_ERR)
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "py"))

import torch  # noqa: E402

from factor import optable  # noqa: E402
from factor import ops  # noqa: E402
from factor.check.cases import cases_for  # noqa: E402
from factor.check.tol import tol_of  # noqa: E402

OP_STREAM = ROOT / "cpp/projects/main/build/bin/op_stream"
DATA = Path(__file__).resolve().parent / "data"


def run_stream(op, case, d):
    d.mkdir(parents=True, exist_ok=True)
    np.save(d / "x.npy", case.x)
    for name in ("y", "z"):
        arr = getattr(case, name)
        f = d / f"{name}.npy"
        if arr is not None:
            np.save(f, arr)
        elif f.exists():
            f.unlink()
    if case.days is not None:
        np.save(d / "days.npy", case.days)
    args = [str(OP_STREAM), op.name, str(d)]
    for k in op.params:
        args += [f"--{k}", str(getattr(case.param, k))]
    r = subprocess.run(args, capture_output=True, text=True)
    if r.returncode != 0:
        return None, r.stderr.strip()
    return np.load(d / "out.npy"), ""


def run_vec(op, case, device):
    fn = ops.get(op.module, op.name)
    t = lambda a: torch.from_numpy(a).to(device)
    ins = [t(case.x)]
    if op.arity >= 2:
        ins.append(t(case.y))
    if op.arity >= 3:
        ins.append(t(case.z))
    if op.axis == "CUM":
        ins.append(t(case.days))
    with torch.no_grad():
        out = fn(*ins, case.param)
    assert out.dtype == torch.float32 and tuple(out.shape) == case.x.shape, f"{op.name}: 输出 {out.dtype} {tuple(out.shape)}"
    return out.cpu().numpy()


def compare(a, b, tol):
    fa, fb = np.isfinite(a), np.isfinite(b)
    mismatch = int((fa != fb).sum())
    both = fa & fb
    if both.any():
        da = np.abs(a[both].astype(np.float64) - b[both].astype(np.float64))
        scale = np.maximum(np.abs(a[both]), np.abs(b[both])).astype(np.float64)
        max_abs = float(da.max())
        max_rel = float((da / np.maximum(scale, 1e-300)).max())
        ok = bool((da <= tol.atol + tol.rtol * scale).all())
    else:
        max_abs = max_rel = 0.0
        ok = True
    return mismatch, int(both.sum()), max_abs, max_rel, ok and mismatch == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--op", action="append", help="只跑这些算子")
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--keep", action="store_true", help="保留中间 npy")
    ap.add_argument("--seed", type=int, default=0)
    a = ap.parse_args()

    assert OP_STREAM.exists(), f"先构建 op_stream: {OP_STREAM}"
    all_ops = optable.load()
    sel = [o for o in all_ops if not a.op or o.name in a.op]
    assert sel, f"--op 无匹配: {a.op}"

    rows, n_fail = [], 0
    for op in sel:
        tol = tol_of(op.name)
        for case in cases_for(op, a.seed):
            d = DATA / op.name / case.tag
            stream, err = run_stream(op, case, d)
            if stream is None:
                rows.append((op.name, case.tag, "-", "-", "-", "-", f"STREAM_ERR {err}"))
                n_fail += 1
                continue
            try:
                vec = run_vec(op, case, a.device)
            except AttributeError:
                rows.append((op.name, case.tag, "-", "-", "-", "-", "MISSING_VEC"))
                n_fail += 1
                continue
            mm, nv, mabs, mrel, ok = compare(stream, vec, tol)
            rows.append((op.name, case.tag, mm, nv, f"{mabs:.2e}", f"{mrel:.2e}", "PASS" if ok else "FAIL"))
            n_fail += not ok
            if not a.keep and ok:
                shutil.rmtree(d, ignore_errors=True)

    w = max(len(r[0]) for r in rows), max(len(r[1]) for r in rows)
    print(f"{'op':{w[0]}} {'case':{w[1]}} {'nan_mm':>6} {'n_valid':>7} {'max_abs':>9} {'max_rel':>9}  status")
    for r in rows:
        print(f"{r[0]:{w[0]}} {r[1]:{w[1]}} {str(r[2]):>6} {str(r[3]):>7} {str(r[4]):>9} {str(r[5]):>9}  {r[6]}")
    print(f"\n{len(rows)} cases, {len(sel)} ops, {n_fail} failed")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
