"""Persistent store for applied overrides."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

DEFAULT_STORE_PATH = Path("configs/overrides_applied.yml")


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


class OverrideStore:
    def __init__(self, path: Path | None = None):
        self.path = Path(path or DEFAULT_STORE_PATH)

    def read(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
        return data or {}

    def write(self, data: Dict[str, Any]) -> None:
        _ensure_parent(self.path)
        with self.path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(data, handle, sort_keys=True)

    def update_field(self, field_path: str, value: Any) -> Dict[str, Any]:
        data = self.read()
        node = data
        parts = field_path.split(".")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
        self.write(data)
        return data
