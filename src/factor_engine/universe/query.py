"""Helper functions to query universe membership."""

from __future__ import annotations

from datetime import date

from sqlalchemy import or_
from sqlalchemy.orm import Session

from ..db.models import Symbol, UniverseMembership


def get_active_symbols(session: Session, as_of: date) -> list[Symbol]:
    return (
        session.query(Symbol)
            .join(UniverseMembership, UniverseMembership.symbol_id == Symbol.id)
            .filter(UniverseMembership.start_date <= as_of)
            .filter(or_(UniverseMembership.end_date == None, UniverseMembership.end_date >= as_of))
            .distinct()
            .all()
    )


def get_active_symbol_ids(session: Session, as_of: date) -> list[int]:
    symbols = get_active_symbols(session, as_of)
    return [symbol.id for symbol in symbols]
