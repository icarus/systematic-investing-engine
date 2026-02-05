"""Construct long-only liquidity-aware portfolios."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from ..config import ConfigBundle
from ..db.models import PortfolioPositionRecord, Symbol
from ..db.session import session_scope
from ..runs.context import RunContext
from ..signals.generator import Signal


@dataclass
class PortfolioPosition:
    ticker: str
    weight: float
    liquidity_share: float


class PortfolioBuilder:
    def __init__(self, config: ConfigBundle, run: RunContext):
        self.config = config
        self.run = run

    def build(self, signals: list[Signal], top_n: int = 15) -> list[PortfolioPosition]:
        selected = signals[:top_n]
        if not selected:
            return []
        total_score = sum(sig.score for sig in selected)
        positions: list[PortfolioPosition] = []
        for signal in selected:
            if total_score == 0:
                weight = 1 / len(selected)
            else:
                weight = signal.score / total_score
            liquidity_cap = self._liquidity_cap(signal)
            final_weight = min(weight, liquidity_cap)
            positions.append(
                PortfolioPosition(
                    ticker=signal.ticker,
                    weight=final_weight,
                    liquidity_share=liquidity_cap,
                )
            )
        total = sum(p.weight for p in positions)
        if total > 0:
            for pos in positions:
                pos.weight = pos.weight / total
        self._persist_positions(positions)
        return positions

    def _liquidity_cap(self, signal: Signal) -> float:
        threshold_pct = self.config.strategy.liquidity_filters["max_weight_pct_of_adv"] / 100
        liquidity = signal.liquidity
        if liquidity <= 0:
            return 0.0
        return threshold_pct

    def _persist_positions(self, positions: list[PortfolioPosition]) -> None:
        if not positions:
            return
        with session_scope() as session:
            for pos in positions:
                symbol = session.query(Symbol).filter_by(ticker=pos.ticker).one()
                session.add(
                    PortfolioPositionRecord(
                        run_id=self.run.run_id,
                        symbol_id=symbol.id,
                        rebalance_date=self.run.as_of_date,
                        weight=pos.weight,
                        liquidity_cap=pos.liquidity_share,
                    )
                )
