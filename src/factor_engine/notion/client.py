"""Guarded Notion synchronization using overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from ..config import ConfigBundle
from ..overrides import OverrideProposal
from ..runs.context import RunContext

try:
    from notion_client import Client
except ImportError:  # pragma: no cover - optional dependency
    Client = None

load_dotenv()


@dataclass
class NotionSync:
    config: ConfigBundle
    run: RunContext

    def _client(self):
        if Client is None:
            raise RuntimeError("notion_client not installed")
        token = os.getenv("NOTION_TOKEN")
        if not token:
            raise RuntimeError("NOTION_TOKEN missing")
        return Client(auth=token)

    def _db_id(self, name: str) -> str:
        return getattr(self.config.notion.databases, name)

    def push_run(self, run_id: str, as_of: date, stage: str, survivorship: bool) -> str:
        if Client is None:
            return ""
        client = self._client()
        db_id = self._db_id("runs")

        properties = {
            "Run id": {"title": [{"text": {"content": run_id}}]},
            "As of date": {"date": {"start": as_of.isoformat()}},
            "Run status": {"select": {"name": stage}},
            "Survivorship quality": {"select": {"name": "Biased" if survivorship else "Free"}},
        }
        res = client.pages.create(parent={"database_id": db_id}, properties=properties)
        return res["id"]

    def push_signals(self, signals: List[Dict[str, Any]]) -> None:
        if Client is None:
            return
        client = self._client()
        db_id = self._db_id("signals")

        for sig in signals:
            unique_id = f"{self.run.run_id[:8]}-{sig['ticker']}"
            properties = {
                "Id": {"title": [{"text": {"content": unique_id}}]},
                "Ticker ": {"rich_text": [{"text": {"content": sig["ticker"]}}]},
                "Composite score": {"number": sig["score"]},
                "Signal date": {"date": {"start": self.run.as_of_date.isoformat()}},
            }
            client.pages.create(parent={"database_id": db_id}, properties=properties)

    def push_portfolio(self, positions: List[Dict[str, Any]]) -> None:
        if Client is None:
            return
        client = self._client()
        db_id = self._db_id("portfolio_state")

        for pos in positions:
            properties = {
                "Ticker": {"title": [{"text": {"content": pos["ticker"]}}]},
                "Target weight": {"number": pos["weight"]},
                "Entry date": {"date": {"start": self.run.as_of_date.isoformat()}},
            }
            client.pages.create(parent={"database_id": db_id}, properties=properties)

    def push_run_summary(self, summary: Dict[str, Any]) -> None:  # pragma: no cover
        if Client is None:
            return
        client = self._client()
        parent = {"database_id": self._db_id("runs")}
        title = f"Run {summary['run_id']}"
        children = _build_summary_blocks(summary)
        properties = {
            "Run id": {
                "title": [{"text": {"content": title}}],
            },
        }
        client.pages.create(parent=parent, properties=properties, children=children)

    def pull_overrides(self) -> List[OverrideProposal]:
        if Client is None:
            return []
        overrides_cfg = self.config.notion.overrides
        allowed = set(overrides_cfg.allowed_fields)
        client = self._client()
        overrides_db = self._db_id("overrides")
        response = client.databases.query(database_id=overrides_db)
        proposals: List[OverrideProposal] = []
        for entry in response.get("results", []):
            proposal = self._to_proposal(entry, overrides_cfg)
            if not proposal:
                continue
            if allowed and proposal.field not in allowed:
                continue
            proposals.append(proposal)
        return proposals

    def _to_proposal(self, entry: Dict[str, Any], overrides_cfg) -> Optional[OverrideProposal]:
        props = entry.get("properties", {})
        field_name = self._extract_value(props.get(overrides_cfg.field_property))
        if not field_name:
            return None
        enabled = bool(self._extract_value(props.get(overrides_cfg.enabled_property)))
        value = self._extract_value(props.get(overrides_cfg.value_property))
        author = self._extract_value(props.get(overrides_cfg.author_property))
        return OverrideProposal(
            field=str(field_name).strip(),
            value=value,
            author=author,
            notion_id=entry.get("id"),
            enabled=enabled,
        )

    def _extract_value(self, prop: Optional[Dict[str, Any]]) -> Any:
        if not prop:
            return None
        prop_type = prop.get("type")
        if prop_type == "title":
            return "".join(chunk.get("plain_text", "") for chunk in prop.get("title", []))
        if prop_type == "rich_text":
            return "".join(chunk.get("plain_text", "") for chunk in prop.get("rich_text", []))
        if prop_type == "checkbox":
            return prop.get("checkbox")
        if prop_type == "number":
            return prop.get("number")
        if prop_type == "select":
            option = prop.get("select")
            return option.get("name") if option else None
        if prop_type == "multi_select":
            return ", ".join(opt.get("name", "") for opt in prop.get("multi_select", []))
        if prop_type == "people":
            people = prop.get("people", [])
            names = [p.get("name") or p.get("person", {}).get("email") for p in people]
            return ", ".join(filter(None, names))
        if prop_type == "url":
            return prop.get("url")
        if prop_type == "email":
            return prop.get("email")
        if prop_type == "date":
            date_val = prop.get("date") or {}
            return date_val.get("start")
        return prop.get(prop_type)


def _build_summary_blocks(summary: Dict[str, Any]) -> list[Dict[str, Any]]:
    blocks: list[Dict[str, Any]] = []
    meta_lines = [
        f"As-of: {summary['as_of_date']}",
        f"Stage: {summary['stage']}",
        f"Survivorship: {'Yes' if summary['survivorship_flag'] else 'No'}",
    ]
    if summary.get("metrics"):
        metrics = summary["metrics"]
        meta_lines.append(
            f"CAGR: {metrics.get('cagr') or 0:.2%} | MaxDD: {metrics.get('max_drawdown') or 0:.2%}"
        )
    blocks.append(
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": " | ".join(meta_lines)}}],
            },
        }
    )
    positions = summary.get("positions") or []
    if positions:
        blocks.append(
            {
                "object": "block",
                "type": "heading_3",
                "heading_3": {"rich_text": [{"type": "text", "text": {"content": "Positions"}}]},
            }
        )
        for pos in positions[:15]:
            content = f"{pos['ticker']}: {pos['weight']:.2%} (cap {pos['liquidity_cap']:.2%})"
            blocks.append(
                {
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": content}}]},
                }
            )
    return blocks
