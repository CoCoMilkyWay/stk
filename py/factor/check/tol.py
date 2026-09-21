"""容差策略: 有效位 |Δ| ≤ atol + rtol·max(|a|, |b|); NaN 掩码逐位必须一致 (无容差)."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Tol:
    rtol: float
    atol: float


# 双侧输出皆 float32, 内部累加皆 double: 差异来源只有 (a) 公式合成路径 (Welford vs 幂和 / 两遍) (b) float32 末位.
_DEFAULT = Tol(rtol=1e-5, atol=1e-6)

_BY_OP = {
    # 三/四阶矩由幂和合成有抵消, 放宽
    "CumSkew": Tol(1e-4, 1e-5),
    "CumKurt": Tol(1e-4, 1e-5),
    "TsSkew": Tol(1e-4, 1e-5),
    "TsKurt": Tol(1e-4, 1e-5),
    "CumEntropy": Tol(1e-4, 1e-6),
    # cs:: 复刻: C++ 侧 float 算术 (中位 / 逆正态多项式), torch 侧 float64
    "CsNormRank": Tol(1e-4, 1e-4),
    "CsWinsorRank": Tol(1e-4, 1e-5),
    "CsWinsorZ": Tol(1e-4, 1e-5),
    "CsZ": Tol(1e-4, 1e-5),
    "CsDemean": Tol(1e-4, 1e-5),
    # EMA: 流式 float32 递推 vs 向量 float64 递推
    "TsEma": Tol(1e-4, 1e-5),
}


def tol_of(name):
    return _BY_OP.get(name, _DEFAULT)
