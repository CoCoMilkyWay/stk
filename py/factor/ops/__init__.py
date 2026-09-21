"""
向量式算子 (torch, GPU 用于因子挖掘). 与 cpp/include/factor/stream/*.hpp 同名同义, 真相表 factor/OpTable.hpp.

张量约定: 输入 float32 [T, A] (T = 期 / 分钟, A = 资产); 内部 float64 累加, 输出 float32 [T, A].
签名 (对仗流式 Op::apply / push):
    ELEM  f(x[, y[, z]], p)          逐格
    CUM   f(x[, y], days, p)         days: int [T] 日 id, 变化即 reset
    ROLL  f(x[, y], p)               行 = 期, 窗 p.d
    CS    f(x[, y[, z]], p)          行 = 截面
p = Param(d, k, k2). 语义契约见 Kernel.hpp 头注 (NaN 跳过 / min_n / 窗未满 NaN / 并列均秩).
"""

from dataclasses import dataclass
from importlib import import_module


@dataclass(frozen=True)
class Param:
    d: int = 0
    k: float = 0.0
    k2: float = 0.0


def get(module, name):
    """按 OpTable 的 (模块, 名) 取向量式函数; 缺 → AttributeError (check 报 missing)."""
    return getattr(import_module(f"{__name__}.{module}"), name)
