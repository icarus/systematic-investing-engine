"""Run context creation and utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict

from ..db.models import Run
from ..db.session import session_scope


@dataclass
class RunContext:
    run_id: str
    as_of_date: date
    rebalance_date: date | None
    created_at: datetime
    survivorship_flag: bool
    params_json: Dict[str, Any] | None
    stage: str


def create_run(
    as_of_date: date,
    params: Dict[str, Any] | None = None,
    survivorship_flag: bool = False,
    stage: str = "initialized",
) -> RunContext:
    with session_scope() as session:
        run = Run(
            as_of_date=as_of_date,
            rebalance_date=params.get("rebalance_date") if params else None,
            params_json=_serialize_params(params),
            survivorship_flag=survivorship_flag,
            stage=stage,
        )
        session.add(run)
        session.flush()
        return _to_context(run)


def load_run(run_id: str) -> RunContext:
    with session_scope() as session:
        run = session.get(Run, run_id)
        if not run:
            raise ValueError(f"Run {run_id} not found")
        return _to_context(run)


def update_run_stage(run_id: str, stage: str) -> None:
    with session_scope() as session:
        run = session.get(Run, run_id)
        if not run:
            raise ValueError(f"Run {run_id} not found")
        run.stage = stage


def mark_survivorship(run_id: str, flag: bool = True) -> None:
    with session_scope() as session:
        run = session.get(Run, run_id)
        if not run:
            raise ValueError(f"Run {run_id} not found")
        run.survivorship_flag = flag


def _to_context(run: Run) -> RunContext:
    return RunContext(
        run_id=run.run_id,
        as_of_date=run.as_of_date,
        rebalance_date=run.rebalance_date,
        created_at=run.created_at,
        survivorship_flag=run.survivorship_flag,
        params_json=run.params_json,
        stage=run.stage,
    )


def _serialize_params(params: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if params is None:
        return None
    new_params = {}
    for k, v in params.items():
        if isinstance(v, (date, datetime)):
            new_params[k] = v.isoformat()
        elif isinstance(v, dict):
            new_params[k] = _serialize_params(v)
        else:
            new_params[k] = v
    return new_params
