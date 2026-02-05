"""Generate composite factor scores and liquidity-aware signals."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import desc

from ..config import ConfigBundle
from ..db.models import FactorValue, LiquidityMetric, SignalRecord, Symbol
from ..db.session import session_scope
from ..runs.context import RunContext
from ..universe import get_active_symbols


@dataclass
class Signal:
    ticker: str
    score: float
    liquidity: float


class SignalGenerator:
    def __init__(self, config: ConfigBundle, run: RunContext):
        self.config = config
        self.run = run

    def build_signals(self, as_of: date) -> list[Signal]:
        with session_scope() as session:
            # Check for existing signals
            existing = (
                session.query(SignalRecord, Symbol.ticker)
                .join(Symbol, Symbol.id == SignalRecord.symbol_id)
                .filter(SignalRecord.run_id == self.run.run_id, SignalRecord.as_of_date == as_of)
                .all()
            )
            if existing:
                signals = [
                    Signal(ticker=ticker, score=rec.score, liquidity=rec.liquidity)
                    for rec, ticker in existing
                ]
                signals.sort(key=lambda s: s.score, reverse=True)
                return signals

            symbols = get_active_symbols(session, self.run.as_of_date)
            signals: list[Signal] = []
            seen_symbols = set()
            for symbol in symbols:
                if symbol.id in seen_symbols:
                    continue
                seen_symbols.add(symbol.id)
                liquidity = self._latest_liquidity(session, symbol.id)
                if not self._passes_liquidity(liquidity):
                    continue
                factor_values: dict[str, float] = {}
                missing = False
                for factor_name in self.config.strategy.factor_weights.keys():
                    value = self._get_factor(session, symbol.id, factor_name)
                    if value is None:
                        missing = True
                        break
                    factor_values[factor_name] = value
                if missing:
                    continue
                score = self._compose_score(factor_values)
                signals.append(Signal(ticker=symbol.ticker, score=score, liquidity=liquidity or 0.0))
            self._persist_signals(session, signals)
        signals.sort(key=lambda s: s.score, reverse=True)
        return signals

    def _persist_signals(self, session, signals: list[Signal]) -> None:
        for signal in signals:
            symbol = session.query(Symbol).filter_by(ticker=signal.ticker).one()
            session.add(
                SignalRecord(
                    run_id=self.run.run_id,
                    symbol_id=symbol.id,
                    as_of_date=self.run.as_of_date,
                    score=signal.score,
                    liquidity=signal.liquidity,
                )
            )

    def _compose_score(self, factors: dict[str, float]) -> float:
        score = 0.0
        for name, weight in self.config.strategy.factor_weights.items():
            score += weight * factors.get(name, 0.0)
        return score

    def _passes_liquidity(self, liquidity: float | None) -> bool:
        if liquidity is None:
            return False
        threshold = self.config.strategy.liquidity_filters["median_traded_value_clp"]
        return liquidity >= threshold

    def _latest_liquidity(self, session, symbol_id: int) -> float | None:
        metric = (
            session.query(LiquidityMetric)
            .filter_by(symbol_id=symbol_id, run_id=self.run.run_id)
            .order_by(desc(LiquidityMetric.id))
            .first()
        )
        return metric.median_traded_value_clp if metric else None

    def _get_factor(self, session, symbol_id: int, name: str) -> float | None:
        factor = (
            session.query(FactorValue)
            .filter_by(symbol_id=symbol_id, run_id=self.run.run_id, factor_name=name)
            .first()
        )
        return factor.value if factor else None
