"""Monthly rebalance schedule helpers."""

from __future__ import annotations

from datetime import date, timedelta
from typing import List

import pandas as pd


def month_end_dates(start: date, end: date) -> List[date]:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    offsets = pd.date_range(start_ts, end_ts, freq="M")
    if not offsets or offsets[-1].date() != end:
        offsets = offsets.append(pd.Index([pd.Timestamp(end)])).drop_duplicates()
    return [ts.date() for ts in offsets]


def next_trading_day(day: date) -> date:
    next_day = day + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day
