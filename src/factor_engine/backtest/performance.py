"""Performance metric utilities."""

from __future__ import annotations

from dataclasses import dataclass
import numpy as np


@dataclass
class PerformanceSummary:
    cagr: float
    volatility: float
    max_drawdown: float
    periods: int


def compute_summary(equity_curve: list[float], periods_per_year: int = 12) -> PerformanceSummary:
    if not equity_curve:
        return PerformanceSummary(0.0, 0.0, 0.0, 0)
    returns = np.diff(equity_curve) / equity_curve[:-1]
    compounded = equity_curve[-1] / equity_curve[0]
    years = max(len(equity_curve) - 1, 1) / periods_per_year
    cagr = compounded ** (1 / years) - 1 if compounded > 0 else 0.0
    volatility = returns.std() * np.sqrt(periods_per_year) if returns.size > 0 else 0.0
    running_max = np.maximum.accumulate(equity_curve)
    drawdowns = equity_curve / running_max - 1
    max_drawdown = float(drawdowns.min())
    return PerformanceSummary(float(cagr), float(volatility), max_drawdown, len(equity_curve))
