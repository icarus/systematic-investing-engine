from datetime import date, timedelta

import pandas as pd
import pytest

from factor_engine.config.loader import (
    ConfigBundle,
    NotionConfig,
    NotionDatabaseConfig,
    NotionOverridesConfig,
    ProviderConfig,
    StrategyConfig,
    UniverseConfig,
    UniverseEntry,
)
from factor_engine.ingest.service import IngestionService
from factor_engine.providers.base import PriceFrame
from factor_engine.factors import FactorEngine
from factor_engine.signals import SignalGenerator
from factor_engine.portfolio import PortfolioBuilder
from factor_engine.backtest import BacktestRunner
from factor_engine.runs import create_run
from factor_engine.db.session import session_scope
from factor_engine.db.models import Symbol, UniverseMembership


class StubProvider:
    name = "stub"

    def __init__(self, run):
        self.run = run

    def fetch_prices(self, requests):
        frames = {}
        for req in requests:
            idx = pd.date_range(req.start, req.end, freq="D")
            base = pd.Series(range(len(idx)), index=idx)
            data = pd.DataFrame({
                "adj_close": 100 + base.cumsum() * 0.01,
                "volume": 1_000_000,
            }, index=idx)
            frames[req.ticker] = PriceFrame(ticker=req.ticker, data=data)
        return frames

    def fetch_metadata(self, tickers):
        return {}


def sample_config() -> ConfigBundle:
    provider = ProviderConfig(id="stub", module="stub.module")
    strategy = StrategyConfig()
    universe = UniverseConfig(
        name="Test",
        description="Test universe",
        constituents=[
            UniverseEntry(ticker="TEST.SN", name="Test Co", currency="CLP", sector="Test"),
        ],
    )
    notion = NotionConfig(
        databases=NotionDatabaseConfig(
            universe="u",
            runs="r",
            signals="s",
            portfolio_state="p",
            trades_log="t",
            backtests="b",
            overrides="o",
            progress_tracker="pt",
            research_journal="rj",
        ),
        overrides=NotionOverridesConfig(allowed_fields=[]),
    )
    return ConfigBundle(provider=provider, strategy=strategy, universe=universe, notion=notion)


def test_pipeline_end_to_end(temp_db):
    config = sample_config()
    as_of = date(2024, 12, 31)
    run = create_run(as_of)
    provider = StubProvider(run)
    service = IngestionService(provider, config, run)
    start = as_of - timedelta(days=400)
    service.ingest(start=start, end=as_of)
    with session_scope() as session:
        symbol = session.query(Symbol).filter_by(ticker="TEST.SN").one()
        membership = session.query(UniverseMembership).filter_by(symbol_id=symbol.id).one()
        membership.start_date = start
    FactorEngine(config, run).compute(as_of)
    signals = SignalGenerator(config, run).build_signals(as_of)
    assert signals, "Signals should be generated"
    builder = PortfolioBuilder(config, run)
    positions = builder.build(signals, top_n=1)
    assert positions and pytest.approx(1.0) == sum(p.weight for p in positions)
    runner = BacktestRunner(config, run)
    report = runner.run_backtest(date(2024, 10, 31), as_of)
    assert report["equity_curve"][-1][1] > 0
