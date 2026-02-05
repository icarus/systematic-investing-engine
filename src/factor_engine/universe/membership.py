"""Utilities to manage historical universe membership."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from ..db.models import Symbol, UniverseMembership
from ..db.session import session_scope


@dataclass
class MembershipRow:
    ticker: str
    start_date: datetime
    end_date: Optional[datetime]
    source: str


class MembershipImporter:
    def __init__(self, csv_path: Path | None = None):
        self.csv_path = csv_path or Path("data/universe_membership.csv")

    def load_rows(self) -> Iterable[MembershipRow]:
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Membership file {self.csv_path} not found")
        with self.csv_path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                yield MembershipRow(
                    ticker=row["ticker"].strip(),
                    start_date=datetime.strptime(row["start_date"], "%Y-%m-%d"),
                    end_date=datetime.strptime(row["end_date"], "%Y-%m-%d") if row.get("end_date") else None,
                    source=row.get("source", "csv"),
                )

    def import_rows(self) -> int:
        rows = list(self.load_rows())
        count = 0
        with session_scope() as session:
            for row in rows:
                symbol = session.query(Symbol).filter_by(ticker=row.ticker).one_or_none()
                if not symbol:
                    symbol = Symbol(ticker=row.ticker, name=row.ticker)
                    session.add(symbol)
                    session.flush()
                membership = (
                    session.query(UniverseMembership)
                    .filter_by(symbol_id=symbol.id, start_date=row.start_date.date())
                    .one_or_none()
                )
                if membership:
                    membership.end_date = row.end_date.date() if row.end_date else None
                    membership.source = row.source
                else:
                    session.add(
                        UniverseMembership(
                            symbol_id=symbol.id,
                            start_date=row.start_date.date(),
                            end_date=row.end_date.date() if row.end_date else None,
                            source=row.source,
                        )
                    )
                count += 1
        return count
