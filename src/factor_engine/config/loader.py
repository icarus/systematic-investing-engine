"""Load YAML configuration bundles for the engine."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseModel, Field

CONFIG_DIR = Path("configs")
OVERRIDES_PATH = CONFIG_DIR / "overrides_applied.yml"


class ProviderConfig(BaseModel):
    id: str = Field(..., description="Provider identifier, e.g., yahoo")
    module: str = Field(..., description="Python path to provider implementation")
    retries: int = 3
    timeout: int = 30
    base_url: Optional[str] = None


class StrategyConfig(BaseModel):
    name: str = "default"
    rebalance_cadence: str = "monthly"
    factor_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            "momentum_12_1": 0.4,
            "momentum_6_1": 0.4,
            "realized_vol": -0.2,
        }
    )
    execution_timing: Dict[str, Any] = Field(
        default_factory=lambda: {
            "signal_time": "month_end_close",
            "execution_time": "next_open",
            "transaction_cost_bps": 20,
            "slippage_bps": 5,
        }
    )
    liquidity_filters: Dict[str, Any] = Field(
        default_factory=lambda: {
            "median_traded_value_clp": 20_000_000,
            "lookback_days": 90,
            "max_weight_pct_of_adv": 5,
        }
    )
    transaction_costs: Dict[str, Any] = Field(default_factory=dict)


class UniverseEntry(BaseModel):
    ticker: str
    name: str
    currency: str = "CLP"
    sector: Optional[str] = None
    notes: Optional[str] = None


class UniverseConfig(BaseModel):
    name: str
    description: str
    constituents: list[UniverseEntry]


class NotionDatabaseConfig(BaseModel):
    universe: str
    runs: str
    signals: str
    portfolio_state: str
    trades_log: str
    backtests: str
    overrides: str
    progress_tracker: str
    research_journal: str


class NotionOverridesConfig(BaseModel):
    allowed_fields: list[str] = Field(default_factory=list)
    field_property: str = "Field"
    value_property: str = "Value"
    enabled_property: str = "Override Enabled"
    author_property: str = "Author"


class NotionConfig(BaseModel):
    databases: NotionDatabaseConfig
    overrides: NotionOverridesConfig = Field(default_factory=NotionOverridesConfig)


class ConfigBundle(BaseModel):
    provider: ProviderConfig
    strategy: StrategyConfig
    universe: UniverseConfig
    notion: NotionConfig


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _merge_dict(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _merge_dict(base.get(key, {}), value)
        else:
            base[key] = value
    return base


def _load_applied_overrides(path: Path | None = None) -> Dict[str, Any]:
    file_path = path or OVERRIDES_PATH
    if not file_path.exists():
        return {}
    data = load_yaml(file_path)
    return data or {}


def load_config_bundle(
    provider_path: Path | None = None,
    strategy_path: Path | None = None,
    universe_path: Path | None = None,
    notion_path: Path | None = None,
    overrides_path: Path | None = None,
) -> ConfigBundle:
    provider_cfg = load_yaml(provider_path or CONFIG_DIR / "providers.yml")
    strategy_cfg = load_yaml(strategy_path or CONFIG_DIR / "strategy" / "default.yml")
    universe_cfg = load_yaml(universe_path or CONFIG_DIR / "universe" / "ipsa.yml")
    notion_cfg = load_yaml(notion_path or CONFIG_DIR / "notion.yml")
    overrides = _load_applied_overrides(overrides_path)
    if overrides.get("strategy"):
        strategy_cfg["strategy"] = _merge_dict(strategy_cfg["strategy"], overrides["strategy"])
    if overrides.get("universe"):
        universe_cfg["universe"] = _merge_dict(universe_cfg["universe"], overrides["universe"])
    return ConfigBundle(
        provider=ProviderConfig(**provider_cfg["providers"][0]),
        strategy=StrategyConfig(**strategy_cfg["strategy"]),
        universe=UniverseConfig(**universe_cfg["universe"]),
        notion=NotionConfig(**notion_cfg["notion"]),
    )
