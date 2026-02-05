"""Execution model for t/t+1 logic with transaction costs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd

from ..config import ConfigBundle


@dataclass
class Trade:
    ticker: str
    weight: float
    execution_date: pd.Timestamp
    price: float
    cost_bps: float


class ExecutionModel:
    def __init__(self, config: ConfigBundle):
        self.config = config

    def apply_costs(self, gross_return: float) -> float:
        tc = self.config.strategy.execution_timing.get("transaction_cost_bps", 0)
        slippage = self.config.strategy.execution_timing.get("slippage_bps", 0)
        net = gross_return - (tc + slippage) / 10000
        return net
