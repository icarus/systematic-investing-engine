"""Apply overrides sourced from Notion with audit logging."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from ..db.models import OverrideAudit
from ..db.session import session_scope
from ..runs.context import RunContext
from .store import OverrideStore


@dataclass
class OverrideProposal:
    field: str
    value: Any
    author: str | None
    notion_id: str | None
    enabled: bool


class OverridesService:
    def __init__(self, run: RunContext, allowed_fields: Iterable[str], store: OverrideStore | None = None):
        self.run = run
        self.allowed_fields = set(allowed_fields)
        self.store = store or OverrideStore()

    def apply(self, proposals: list[OverrideProposal], allow_overrides: bool) -> dict:
        applied = 0
        skipped = []
        with session_scope() as session:
            for proposal in proposals:
                is_allowed = proposal.field in self.allowed_fields
                should_apply = allow_overrides and proposal.enabled and is_allowed
                normalized_value = _coerce_value(proposal.value)
                session.add(
                    OverrideAudit(
                        run_id=self.run.run_id,
                        source="notion",
                        field=proposal.field,
                        old_value=None,
                        new_value=str(proposal.value),
                        author=proposal.author,
                        enabled=should_apply,
                    )
                )
                if should_apply:
                    self.store.update_field(proposal.field, normalized_value)
                    applied += 1
                else:
                    skipped.append(proposal.field)
        return {
            "applied": applied,
            "skipped": skipped,
            "total": len(proposals),
            "allow_flag": allow_overrides,
        }


def _coerce_value(value: Any) -> Any:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value
    return value
