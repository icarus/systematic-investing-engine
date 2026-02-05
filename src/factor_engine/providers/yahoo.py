"""Yahoo Finance provider implementation."""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Iterable

import pandas as pd
import yfinance as yf

from ..db.models import ProviderLog
from ..db.session import session_scope
from ..runs.context import RunContext
from .base import DataProvider, PriceFrame, PriceRequest


class YahooProvider(DataProvider):
    name = "yahoo"

    def __init__(self, run: RunContext):
        self.run = run

    def fetch_prices(self, requests: Iterable[PriceRequest]) -> dict[str, PriceFrame]:
        results: dict[str, PriceFrame] = {}
        for req in requests:
            ticker = req.ticker
            data = yf.download(ticker, start=req.start, end=req.end, interval=req.interval, progress=False)
            if data.empty:
                continue
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            data = data.rename(columns=lambda c: c.lower().replace(" ", "_"))
            results[ticker] = PriceFrame(ticker=ticker, data=data)
            self._log_provider_call(
                endpoint="prices",
                params=f"{ticker}-{req.start}-{req.end}-{req.interval}",
                payload=data,
            )
        return results

    def fetch_metadata(self, tickers: Iterable[str]) -> dict[str, dict]:
        info: dict[str, dict] = {}
        for ticker in tickers:
            ticker_obj = yf.Ticker(ticker)
            meta = ticker_obj.info or {}
            info[ticker] = meta
            self._log_provider_call(
                endpoint="metadata",
                params=ticker,
                payload=meta,
            )
        return info

    def _log_provider_call(self, endpoint: str, params: str, payload) -> None:
        params_hash = _hash_text(params)
        response_hash = _hash_text(str(payload)[:10_000])
        with session_scope() as session:
            session.add(
                ProviderLog(
                    run_id=self.run.run_id,
                    provider=self.name,
                    endpoint=endpoint,
                    params_hash=params_hash,
                    response_hash=response_hash,
                    checksum=response_hash,
                )
            )


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_provider(run: RunContext) -> DataProvider:
    return YahooProvider(run)
