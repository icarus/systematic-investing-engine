from datetime import date

import pytest

from factor_engine.config.loader import load_config_bundle
from factor_engine.overrides import OverrideProposal, OverridesService, OverrideStore
from factor_engine.runs import create_run


def test_overrides_apply_and_merge(temp_db, tmp_path):
    run = create_run(date(2024, 12, 31))
    store_path = tmp_path / "overrides.yml"
    service = OverridesService(
        run,
        ["strategy.factor_weights.momentum_12_1"],
        store=OverrideStore(store_path),
    )
    proposals = [
        OverrideProposal(
            field="strategy.factor_weights.momentum_12_1",
            value="0.55",
            author="quant",
            notion_id=None,
            enabled=True,
        )
    ]
    result = service.apply(proposals, allow_overrides=True)
    assert result["applied"] == 1
    config = load_config_bundle(overrides_path=store_path)
    assert pytest.approx(0.55) == config.strategy.factor_weights["momentum_12_1"]
