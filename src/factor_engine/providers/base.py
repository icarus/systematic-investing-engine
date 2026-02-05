"""Provider abstraction to isolate market data sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Protocol


@dataclass(frozen=True)
class PriceRequest:
    ticker: str
    start: date
    end: date
    interval: str = "1d"


class DataProvider(Protocol):
    name: str

    def fetch_prices(self, requests: Iterable[PriceRequest]) -> dict[str, "PriceFrame"]:
        """Fetch price history keyed by ticker."""

    def fetch_metadata(self, tickers: Iterable[str]) -> dict[str, dict]:
        """Fetch metadata for tickers (sector, currency, etc.)."""


@dataclass
class PriceFrame:
    ticker: str
    data: "pandas.DataFrame"


__all__ = ["DataProvider", "PriceRequest", "PriceFrame"]
