"""Walk-forward backtest honoring execution timing."""

from __future__ import annotations

from datetime import date
from typing import List

from sqlalchemy import asc, desc

from ..config import ConfigBundle
from ..db.models import BacktestEquityPoint, BacktestResult, PriceAdjusted, Symbol
from ..db.session import session_scope
from ..portfolio.builder import PortfolioBuilder, PortfolioPosition
from ..runs.context import RunContext
from ..signals.generator import SignalGenerator
from .schedule import month_end_dates, next_trading_day
from .performance import compute_summary
from .execution import ExecutionModel


class BacktestRunner:
    def __init__(self, config: ConfigBundle, run: RunContext):
        self.config = config
        self.run = run
        self.signal_generator = SignalGenerator(config, run)
        self.portfolio_builder = PortfolioBuilder(config, run)
        self.execution = ExecutionModel(config)

    def run_backtest(self, start_date: date, end_date: date) -> dict:
        schedule = month_end_dates(start_date, end_date)
        if len(schedule) < 2:
            raise ValueError("Backtest requires at least two month-end dates")
        equity_points: list[tuple[date, float]] = [(start_date, 1.0)]
        capital = 1.0
        with session_scope() as session:
            for idx in range(len(schedule) - 1):
                rebalance_date = schedule[idx]
                next_rebalance = schedule[idx + 1]
                self.run.as_of_date = rebalance_date
                signals = self.signal_generator.build_signals(rebalance_date)
                portfolio = self.portfolio_builder.build(signals)
                if not portfolio:
                    equity_points.append((next_rebalance, capital))
                    continue
                trade_date = next_trading_day(rebalance_date)
                period_return = self._portfolio_return(session, portfolio, trade_date, next_rebalance)
                capital *= 1 + period_return
                equity_points.append((next_rebalance, capital))
        equity_values = [value for _, value in equity_points]
        summary = compute_summary(equity_values)
        self._persist_results(start_date, end_date, equity_points, summary)
        return {
            "run_id": self.run.run_id,
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "equity_curve": equity_points,
            "cagr": summary.cagr,
            "volatility": summary.volatility,
            "max_drawdown": summary.max_drawdown,
        }

    def _portfolio_return(
        self,
        session,
        positions: List[PortfolioPosition],
        trade_date: date,
        exit_date: date,
    ) -> float:
        tickers = [pos.ticker for pos in positions]
        symbols = {sym.ticker: sym for sym in session.query(Symbol).filter(Symbol.ticker.in_(tickers))}
        total = 0.0
        effective_weight = 0.0
        for position in positions:
            symbol = symbols.get(position.ticker)
            if not symbol:
                continue
            entry_price = self._price_on_or_after(session, symbol.id, trade_date)
            exit_price = self._price_on_or_before(session, symbol.id, exit_date)
            if entry_price is None or exit_price is None:
                continue
            gross = (exit_price / entry_price) - 1
            total += position.weight * gross
            effective_weight += position.weight
        if effective_weight == 0:
            return 0.0
        avg_return = total
        return self.execution.apply_costs(avg_return)

    def _price_on_or_after(self, session, symbol_id: int, target: date) -> float | None:
        row = (
            session.query(PriceAdjusted)
            .filter(PriceAdjusted.symbol_id == symbol_id)
            .filter(PriceAdjusted.price_date >= target)
            .order_by(asc(PriceAdjusted.price_date))
            .first()
        )
        return row.adj_close if row else None

    def _price_on_or_before(self, session, symbol_id: int, target: date) -> float | None:
        row = (
            session.query(PriceAdjusted)
            .filter(PriceAdjusted.symbol_id == symbol_id)
            .filter(PriceAdjusted.price_date <= target)
            .order_by(desc(PriceAdjusted.price_date))
            .first()
        )
        return row.adj_close if row else None

    def _persist_results(self, start_date, end_date, equity_points, summary) -> None:
        with session_scope() as session:
            session.merge(
                BacktestResult(
                    run_id=self.run.run_id,
                    start_date=start_date,
                    end_date=end_date,
                    final_capital=equity_points[-1][1],
                    cagr=summary.cagr,
                    volatility=summary.volatility,
                    max_drawdown=summary.max_drawdown,
                    periods=summary.periods,
                )
            )
            for point_date, capital in equity_points:
                session.merge(
                    BacktestEquityPoint(
                        run_id=self.run.run_id,
                        point_date=point_date,
                        capital=capital,
                    )
                )
