from datetime import date

from factor_engine.db.models import PortfolioPositionRecord, Symbol
from factor_engine.db.session import session_scope
from factor_engine.reports import build_run_summary
from factor_engine.runs import create_run


def test_build_run_summary_with_positions(temp_db):
    run = create_run(date(2024, 12, 31))
    with session_scope() as session:
        symbol = Symbol(ticker="TEST.SN", name="Test")
        session.add(symbol)
        session.flush()
        session.add(
            PortfolioPositionRecord(
                run_id=run.run_id,
                symbol_id=symbol.id,
                rebalance_date=run.as_of_date,
                weight=0.5,
                liquidity_cap=0.05,
            )
        )
    summary = build_run_summary(run.run_id)
    assert summary["run_id"] == run.run_id
    assert summary["positions"][0]["ticker"] == "TEST.SN"
