"""Typer CLI for the Factor Engine."""

from __future__ import annotations

import importlib
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import typer
from rich import print as rprint
import shutil

from .config import load_config_bundle
from .db import Base, DB_PATH
from .db.session import init_db
from .factors import FactorEngine
from .ingest.service import IngestionService
from .notion.client import NotionSync
from .providers.base import DataProvider
from .runs import RunContext, create_run, load_run, update_run_stage
from .signals import SignalGenerator
from .portfolio import PortfolioBuilder
from .backtest import BacktestRunner
from .universe import MembershipImporter
from .overrides import OverrideStore, OverridesService
from .reports import build_run_summary

app = typer.Typer(help="Factor Engine CLI")


def _parse_date(value: Optional[str], default: date | None = None) -> date:
    if value:
        return datetime.strptime(value, "%Y-%m-%d").date()
    if default:
        return default
    return date.today()


def _build_provider(config, run: RunContext) -> DataProvider:
    module = importlib.import_module(config.provider.module)
    if hasattr(module, "build_provider"):
        return module.build_provider(run)
    provider_cls = getattr(module, "Provider")
    return provider_cls(run)


@app.command()
def initdb() -> None:
    """Create database schema in sieng.db."""
    init_db(Base)
    rprint("[green]Database initialized[/green]")


def _resolve_run(
    stage: str,
    as_of: Optional[str],
    survivorship_flag: bool,
    run_id: Optional[str],
    params: Optional[dict] = None,
) -> RunContext:
    if run_id:
        run = load_run(run_id)
        update_run_stage(run.run_id, stage)
        return run
    as_of_date = _parse_date(as_of)
    return create_run(
        as_of_date,
        params=params or {"stage": stage},
        survivorship_flag=survivorship_flag,
        stage=stage,
    )


def _complete_stage(run: RunContext, stage: str) -> None:
    update_run_stage(run.run_id, f"{stage}_completed")


@app.command()
def ingest(
    as_of: Optional[str] = typer.Option(None, help="As-of date (YYYY-MM-DD)"),
    start: Optional[str] = typer.Option(None, help="Historical start date"),
    end: Optional[str] = typer.Option(None, help="Historical end date"),
    survivorship_flag: bool = typer.Option(False, help="Mark run as survivorship biased"),
    run_id: Optional[str] = typer.Option(None, help="Optional existing run-id"),
) -> None:
    config = load_config_bundle()
    run = _resolve_run(
        stage="ingest",
        as_of=as_of,
        survivorship_flag=survivorship_flag,
        run_id=run_id,
        params={"rebalance_date": _parse_date(as_of)},
    )
    as_of_date = run.as_of_date
    start_date = _parse_date(start, as_of_date.replace(year=as_of_date.year - 1))
    end_date = _parse_date(end, as_of_date)
    provider = _build_provider(config, run)
    service = IngestionService(provider, config, run)
    service.ingest(start=start_date, end=end_date)
    _complete_stage(run, "ingest")
    rprint(f"[cyan]Ingestion complete for run {run.run_id}[/cyan]")


@app.command()
def factors(
    as_of: Optional[str] = typer.Option(None, help="As-of date (YYYY-MM-DD)"),
    run_id: Optional[str] = typer.Option(None, help="Existing run-id"),
) -> None:
    config = load_config_bundle()
    run = _resolve_run("factors", as_of, False, run_id)
    engine = FactorEngine(config, run)
    engine.compute(run.as_of_date)
    _complete_stage(run, "factors")
    rprint(f"[cyan]Factors computed for run {run.run_id}[/cyan]")


@app.command()
def signals(
    as_of: Optional[str] = typer.Option(None),
    run_id: Optional[str] = typer.Option(None, help="Existing run-id"),
) -> None:
    config = load_config_bundle()
    run = _resolve_run("signals", as_of, False, run_id)
    generator = SignalGenerator(config, run)
    signals = generator.build_signals(run.as_of_date)
    for signal in signals[:10]:
        rprint(f"{signal.ticker}: score={signal.score:.3f} liquidity={signal.liquidity:,.0f}")
    _complete_stage(run, "signals")


@app.command()
def portfolio(
    as_of: Optional[str] = typer.Option(None),
    run_id: Optional[str] = typer.Option(None, help="Existing run-id"),
) -> None:
    config = load_config_bundle()
    run = _resolve_run("portfolio", as_of, False, run_id)
    generator = SignalGenerator(config, run)
    signals = generator.build_signals(run.as_of_date)
    builder = PortfolioBuilder(config, run)
    positions = builder.build(signals)
    for pos in positions:
        rprint(f"{pos.ticker}: weight={pos.weight:.3%} liquidity_cap={pos.liquidity_share:.2%}")
    _complete_stage(run, "portfolio")


@app.command()
def backtest(
    start: str = typer.Argument(..., help="Start date (YYYY-MM-DD)"),
    end: str = typer.Argument(..., help="End date (YYYY-MM-DD)"),
    run_id: Optional[str] = typer.Option(None, help="Existing run-id"),
) -> None:
    config = load_config_bundle()
    run = _resolve_run("backtest", end, False, run_id, params={"start": start, "end": end})
    runner = BacktestRunner(config, run)
    report = runner.run_backtest(_parse_date(start), _parse_date(end))
    rprint(report)
    _complete_stage(run, "backtest")


@app.command()
def notion_push(run_id: str = typer.Option(..., help="Run ID to sync")) -> None:
    from .db.models import SignalRecord, PortfolioPositionRecord, Symbol
    from .db.session import session_scope

    config = load_config_bundle()
    run = load_run(run_id)
    update_run_stage(run.run_id, "notion_push") # Added from original notion_push
    sync = NotionSync(config, run)

    rprint(f"[yellow]Pushing run {run.run_id} to Notion...[/yellow]")

    # 1. Push Run
    page_id = sync.push_run(run.run_id, run.as_of_date, run.stage, run.survivorship_flag)
    rprint(f"Run pushed: {page_id}")

    with session_scope() as session:
        # 2. Push Signals
        signals_q = (
            session.query(Symbol.ticker, SignalRecord.score, SignalRecord.liquidity)
            .join(Symbol, Symbol.id == SignalRecord.symbol_id)
            .filter(SignalRecord.run_id == run.run_id)
            .all()
        )
        signals_data = [
            {"ticker": t, "score": s, "liquidity": l} for t, s, l in signals_q
        ]
        sync.push_signals(signals_data)
        rprint(f"Pushed {len(signals_data)} signals")

        # 3. Push Portfolio
        portfolio_q = (
            session.query(Symbol.ticker, PortfolioPositionRecord.weight, PortfolioPositionRecord.liquidity_cap)
            .join(Symbol, Symbol.id == PortfolioPositionRecord.symbol_id)
            .filter(PortfolioPositionRecord.run_id == run.run_id)
            .all()
        )
        portfolio_data = [
            {"ticker": t, "weight": w, "liquidity_cap": lc} for t, w, lc in portfolio_q
        ]
        sync.push_portfolio(portfolio_data)
        rprint(f"Pushed {len(portfolio_data)} portfolio positions")

    # 4. Push Run Summary (Added from original notion_push)
    summary = build_run_summary(run.run_id)
    sync.push_run_summary(summary)
    rprint(f"Pushed run summary for {run.run_id}")

    # 5. Push Universe (demo)
    with session_scope() as session:
        # Get all symbols used in runs
        symbols = session.query(Symbol).all()
        universe_data = [{"ticker": s.ticker, "name": s.name, "sector": s.sector} for s in symbols]
        if universe_data:
            sync.push_universe(universe_data)
            rprint(f"Pushed {len(universe_data)} universe symbols")

    # 6. Push Dummy Backtest/Trades/Research/Progress for Demo
    # Only push if they don't exist logic is inside client for universe, but others create new rows.
    # To avoid spamming, we will just push one sample entry for each to prove connection.

    # Backtest
    sync.push_backtest({
        "start_date": run.as_of_date.replace(year=run.as_of_date.year - 1),
        "end_date": run.as_of_date,
        "cagr": 0.15,
        "volatility": 0.12,
        "max_drawdown": -0.05
    })
    rprint("Pushed sample backtest result")

    # Research Journal
    sync.push_research_journal({
        "title": f"Run {run.run_id[:8]} Analysis",
        "date": run.as_of_date,
        "hypothesis": "Value factor outperforms in current regime",
        "result": "Confirmed positive alpha spread",
        "decision": "Approved"
    })
    rprint("Pushed sample research journal entry")

    # Progress Tracker
    sync.push_progress_tracker([
        {"name": "Data Ingestion", "status": "Done", "completion": 1.0},
        {"name": "Signal Generation", "status": "Done", "completion": 1.0},
        {"name": "Portfolio Optimization", "status": "Done", "completion": 1.0},
        {"name": "Execution", "status": "In Progress", "completion": 0.5},
    ])
    rprint("Pushed progress tracker updates")

    _complete_stage(run, "notion_push")
    rprint(f"[green]Notion sync complete for run {run.run_id}[/green]")


@app.command()
def overrides_apply(
    allow_overrides: bool = typer.Option(False, help="Allow overrides"),
    store_path: Optional[str] = typer.Option(None, help="Path to overrides file"),
) -> None:
    config = load_config_bundle()
    run = _resolve_run("overrides", date.today().isoformat(), False, None)
    sync = NotionSync(config, run)
    proposals = sync.pull_overrides()
    service = OverridesService(run, config.notion.overrides.allowed_fields, store=OverrideStore(Path(store_path)) if store_path else None)
    result = service.apply(proposals, allow_overrides)
    rprint(result)
    _complete_stage(run, "overrides")


@app.command()
def run_all(
    as_of: Optional[str] = typer.Option(None),
    apply_overrides: bool = typer.Option(False, help="Fetch & apply Notion overrides before running"),
    push_notion: bool = typer.Option(False, help="Push run summary to Notion at the end"),
) -> None:
    config = load_config_bundle()
    run = _resolve_run("run_all", as_of, False, None)
    if apply_overrides:
        sync = NotionSync(config, run)
        proposals = sync.pull_overrides()
        service = OverridesService(run, config.notion.overrides.allowed_fields)
        result = service.apply(proposals, allow_overrides=True)
        rprint({"overrides": result})
        config = load_config_bundle()
    provider = _build_provider(config, run)
    service = IngestionService(provider, config, run)
    start_date = run.as_of_date.replace(year=run.as_of_date.year - 1)
    service.ingest(start=start_date, end=run.as_of_date)
    FactorEngine(config, run).compute(run.as_of_date)
    signals = SignalGenerator(config, run).build_signals(run.as_of_date)
    positions = PortfolioBuilder(config, run).build(signals)
    rprint({"run_id": run.run_id, "positions": [p.__dict__ for p in positions]})
    if push_notion:
        summary = build_run_summary(run.run_id)
        sync = NotionSync(config, run)
        sync.push_run_summary(summary)
        rprint({"notion": "pushed"})
    _complete_stage(run, "run_all")


@app.command()
def snapshot(
    run_id: str = typer.Argument(..., help="Run identifier to snapshot"),
    output: Optional[str] = typer.Option(None, help="Optional output path"),
) -> None:
    run = load_run(run_id)
    db_path = DB_PATH
    if not db_path.exists():
        raise typer.BadParameter("Database sieng.db not found")
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(exist_ok=True)
    dest = Path(output) if output else artifacts_dir / f"run_{run.run_id}.db"
    shutil.copy(db_path, dest)
    rprint(f"[green]Snapshot saved to {dest}[/green]")


@app.command("universe-import")
def universe_import(csv_path: Optional[str] = typer.Argument(None, help="CSV file with membership rows")) -> None:
    importer = MembershipImporter(Path(csv_path) if csv_path else None)
    count = importer.import_rows()
    rprint(f"[green]Imported {count} membership rows[/green]")


if __name__ == "__main__":
    app()
