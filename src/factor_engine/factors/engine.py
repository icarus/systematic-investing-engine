"""Factor computation engine (momentum, risk, liquidity)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from ..config import ConfigBundle
from ..db.models import FactorValue, LiquidityMetric, PriceAdjusted, Symbol
from ..db.session import session_scope
from ..runs.context import RunContext
from ..universe import get_active_symbols


@dataclass
class FactorDefinition:
    name: str
    lookback_days: int
    method: str  # e.g., momentum, realized_vol


class FactorEngine:
    def __init__(self, config: ConfigBundle, run: RunContext):
        self.config = config
        self.run = run
        self.definitions = [
            FactorDefinition(name="momentum_12_1", lookback_days=252, method="momentum"),
            FactorDefinition(name="momentum_6_1", lookback_days=126, method="momentum"),
            FactorDefinition(name="realized_vol", lookback_days=126, method="volatility"),
            FactorDefinition(name="max_drawdown", lookback_days=252, method="max_drawdown"),
        ]

    def compute(self, as_of: date) -> None:
        with session_scope() as session:
            symbols = get_active_symbols(session, self.run.as_of_date)
            for symbol in symbols:
                df = self._load_prices(session, symbol.id, as_of)
                if df.empty:
                    continue
                for definition in self.definitions:
                    value = self._compute_factor(df, definition)
                    if value is None:
                        continue
                    session.add(
                        FactorValue(
                            symbol_id=symbol.id,
                            run_id=self.run.run_id,
                            factor_name=definition.name,
                            value_date=as_of,
                            value=value,
                        )
                    )
                liquidity = self._compute_liquidity(df)
                if liquidity is not None:
                    session.add(
                        LiquidityMetric(
                            symbol_id=symbol.id,
                            run_id=self.run.run_id,
                            lookback_days=90,
                            median_traded_value_clp=liquidity,
                            metric_date=as_of,
                        )
                    )

    def _load_prices(self, session, symbol_id: int, as_of: date) -> pd.DataFrame:
        query = (
            session.query(PriceAdjusted)
            .filter(PriceAdjusted.symbol_id == symbol_id)
            .filter(PriceAdjusted.price_date <= as_of)
            .order_by(PriceAdjusted.price_date.desc())
            .limit(300)
        )
        rows = query.all()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(
            {
                "date": [row.price_date for row in rows],
                "adj_close": [row.adj_close for row in rows],
                "volume": [row.volume for row in rows],
            }
        )
        return df.sort_values("date").reset_index(drop=True)

    def _compute_factor(self, df: pd.DataFrame, definition: FactorDefinition) -> float | None:
        if definition.method == "momentum":
            return self._momentum(df, definition.lookback_days)
        if definition.method == "volatility":
            return self._realized_vol(df, definition.lookback_days)
        if definition.method == "max_drawdown":
            return self._max_drawdown(df)
        return None

    def _momentum(self, df: pd.DataFrame, lookback: int) -> float | None:
        if len(df) < lookback:
            return None
        start_price = df.iloc[-lookback]["adj_close"]
        end_price = df.iloc[-1]["adj_close"]
        return (end_price / start_price) - 1

    def _realized_vol(self, df: pd.DataFrame, lookback: int) -> float | None:
        if len(df) < lookback:
            return None
        returns = df["adj_close"].pct_change().dropna().tail(lookback)
        return returns.std() * (252 ** 0.5)

    def _max_drawdown(self, df: pd.DataFrame) -> float | None:
        running_max = df["adj_close"].cummax()
        drawdown = df["adj_close"] / running_max - 1
        return drawdown.min()

    def _compute_liquidity(self, df: pd.DataFrame) -> float | None:
        if "volume" not in df:
            return None
        window = df.tail(90)
        if window.empty:
            return None
        traded_value = window["adj_close"] * window["volume"]
        return float(traded_value.median())
