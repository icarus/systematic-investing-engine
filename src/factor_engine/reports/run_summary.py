"""Helpers to build summaries of runs for reporting/Notion."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import desc

from ..db.models import BacktestResult, PortfolioPositionRecord, Run, Symbol
from ..db.session import session_scope


def build_run_summary(run_id: str) -> dict:
    with session_scope() as session:
        run = session.get(Run, run_id)
        if not run:
            raise ValueError(f"Run {run_id} not found")
        positions = (
            session.query(PortfolioPositionRecord, Symbol)
            .join(Symbol, PortfolioPositionRecord.symbol_id == Symbol.id)
            .filter(PortfolioPositionRecord.run_id == run_id)
            .order_by(desc(PortfolioPositionRecord.weight))
            .all()
        )
        positions_payload = [
            {
                "ticker": symbol.ticker,
                "weight": float(record.weight),
                "liquidity_cap": float(record.liquidity_cap),
            }
            for record, symbol in positions
        ]
        backtest = session.query(BacktestResult).filter_by(run_id=run_id).one_or_none()
        metrics = (
            {
                "start": backtest.start_date.isoformat(),
                "end": backtest.end_date.isoformat(),
                "final_capital": float(backtest.final_capital),
                "cagr": backtest.cagr,
                "volatility": backtest.volatility,
                "max_drawdown": backtest.max_drawdown,
            }
            if backtest
            else None
        )
        return {
            "run_id": run.run_id,
            "as_of_date": run.as_of_date.isoformat(),
            "rebalance_date": run.rebalance_date.isoformat() if run.rebalance_date else None,
            "stage": run.stage,
            "survivorship_flag": run.survivorship_flag,
            "created_at": run.created_at.isoformat() if isinstance(run.created_at, datetime) else str(run.created_at),
            "positions": positions_payload,
            "metrics": metrics,
            "params": run.params_json,
        }
