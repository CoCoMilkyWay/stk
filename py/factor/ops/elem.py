"""ELEM 逐值算子 (对仗 factor/stream/Elem.hpp). float32 逐格, NaN 自然传播, 仅无定义处显式 NaN."""

import torch

from ._util import NAN, finite, where_nan


# ---- 一元 ----
def Abs(x, p):
    return x.abs()


def Sign(x, p):
    return where_nan(finite(x), torch.sign(x))


def Log(x, p):
    return torch.log1p(x.abs()).copysign(x)


def Asinh(x, p):
    return torch.asinh(x)


def Tanh(x, p):
    return torch.tanh(x)


def Sqrt(x, p):
    return x.abs().sqrt().copysign(x)


def Relu(x, p):
    return where_nan(finite(x), x.clamp(min=0))


def Recip(x, p):
    return where_nan(x != 0, 1.0 / x)


def SignedPow(x, p):
    return x.abs().pow(p.k).copysign(x)


def Clip(x, p):
    return where_nan(finite(x), x.clamp(-p.k, p.k))


# ---- 二元 ----
def Add(x, y, p):
    return x + y


def Sub(x, y, p):
    return x - y


def Mul(x, y, p):
    return x * y


def Div(x, y, p):
    return where_nan(y != 0, x / y)


def Max(x, y, p):
    return where_nan(finite(x) & finite(y), torch.maximum(x, y))


def Min(x, y, p):
    return where_nan(finite(x) & finite(y), torch.minimum(x, y))


def Imb(x, y, p):
    s = x + y
    return where_nan(s != 0, (x - y) / s)


def Share(x, y, p):
    s = x + y
    return where_nan(s != 0, x / s)


def LogRatio(x, y, p):
    return where_nan((x > 0) & (y > 0), torch.log(x / y))


# ---- 三元 ----
def Where(x, y, z, p):
    return where_nan(finite(x), torch.where(x > 0, y, z))
