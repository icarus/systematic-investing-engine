"""Ingestion service orchestrating provider calls and persistence."""

from __future__ import annotations

from datetime import date
from typing import Iterable

import pandas as pd

from ..config import ConfigBundle
from ..db.models import PriceAdjusted, Symbol, UniverseMembership
from ..db.session import session_scope
from ..providers.base import DataProvider, PriceRequest, PriceFrame
from ..runs.context import RunContext, mark_survivorship
from ..universe import get_active_symbols


class IngestionService:
    """Handle ingesting metadata and prices into SQLite."""

    def __init__(self, provider: DataProvider, config: ConfigBundle, run: RunContext):
        self.provider = provider
        self.config = config
        self.run = run

    def ingest(self, start: date, end: date) -> None:
        tickers = self._active_tickers()
        self._ensure_symbols()
        requests = [PriceRequest(ticker=t, start=start, end=end) for t in tickers]
        frames = self.provider.fetch_prices(requests)
        self._persist_prices(frames)

    def _ensure_symbols(self) -> None:
        with session_scope() as session:
            for entry in self.config.universe.constituents:
                symbol = session.query(Symbol).filter_by(ticker=entry.ticker).one_or_none()
                if not symbol:
                    symbol = Symbol(ticker=entry.ticker, name=entry.name, currency=entry.currency, sector=entry.sector)
                    session.add(symbol)
                    session.flush()
                open_membership = (
                    session.query(UniverseMembership)
                    .filter_by(symbol_id=symbol.id, end_date=None)
                    .one_or_none()
                )
                if not open_membership:
                    session.add(
                        UniverseMembership(
                            symbol_id=symbol.id,
                            start_date=self.run.as_of_date,
                            end_date=None,
                            source="universe_config",
                        )
                    )

    def _persist_prices(self, frames: dict[str, PriceFrame]) -> None:
        with session_scope() as session:
            for ticker, frame in frames.items():
                symbol = session.query(Symbol).filter_by(ticker=ticker).one()
                df: pd.DataFrame = frame.data
                for idx, row in df.iterrows():
                    adj_close = float(row.get("adj_close") or row.get("close"))
                    price = PriceAdjusted(
                        symbol_id=symbol.id,
                        run_id=self.run.run_id,
                        price_date=idx.to_pydatetime().date(),
                        adj_close=adj_close,
                        volume=float(row.get("volume", 0.0)),
                        currency=symbol.currency,
                    )
                    session.add(price)

    def _active_tickers(self) -> list[str]:
        with session_scope() as session:
            active_symbols = get_active_symbols(session, self.run.as_of_date)
            tickers = [symbol.ticker for symbol in active_symbols]

        if not tickers:
            mark_survivorship(self.run.run_id, True)
            return [entry.ticker for entry in self.config.universe.constituents]
        return tickers
