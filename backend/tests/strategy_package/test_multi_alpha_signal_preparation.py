from __future__ import annotations

import pytest

from backend.services.strategy_package.multi_alpha_live import MultiAlphaLivePredictionProvider
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.tests.strategy_package.test_multi_alpha_live_selection import (
    FakeProvider,
    FakeResolver,
    TRADE_DATE,
    _live_weight_history,
    _make_parent,
    _runtime_config,
    _score_rows,
)


def _provider(package_repo):  # noqa: ANN001, ANN202
    inference = FakeProvider(
        {
            "a1_plus3_LSTM_h20": _score_rows(reverse=False),
            "new_FUNDGROWTH_h20": _score_rows(reverse=True),
        }
    )
    provider = MultiAlphaLivePredictionProvider(
        package_repository=package_repo,
        artifact_repository=None,
        runtime_asset_resolver=FakeResolver(),
        live_inference_provider=inference,
    )
    return provider, inference


def test_historical_multi_alpha_preparation_uses_frozen_weights_and_forwards_read_only_mode() -> None:
    package_repo, parent = _make_parent(live_weight_policy=False)
    provider, inference = _provider(package_repo)

    artifacts = provider.prepare_artifacts(
        package_id=parent.package_id,
        trade_dates=[TRADE_DATE],
        data_source="DB_HISTORICAL",
        runtime_config=_runtime_config(),
        include_reference_price=False,
        cutoff_date=TRADE_DATE,
        inference_backend="wsl",
        historical_read_only=True,
    )

    assert len(artifacts) == 1
    assert artifacts[0].metadata["weight_policy"]["mode"] == "frozen_backtest_terminal_weights"
    assert all(call["historical_read_only"] is True for call in inference.calls)
    assert {
        item["admissibility"] for item in artifacts[0].metadata["asset_closure"]
    } == {"FROZEN_ARTIFACT"}
    assert {
        leg_id: item["input_context"]["window_start_date"]
        for leg_id, item in artifacts[0].metadata["component_artifacts"].items()
    } == {
        "a1_plus3_LSTM_h20": "2024-03-01",
        "new_FUNDGROWTH_h20": "2024-03-01",
    }


def test_historical_multi_alpha_rejects_runtime_weight_rows_and_rolling_contract() -> None:
    package_repo, parent = _make_parent(live_weight_policy=True)
    provider, inference = _provider(package_repo)

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        provider.prepare_artifacts(
            package_id=parent.package_id,
            trade_dates=[TRADE_DATE],
            data_source="DB_HISTORICAL",
            runtime_config=_runtime_config(
                extra_artifact={"multi_alpha_weight_history": _live_weight_history()}
            ),
            include_reference_price=False,
            cutoff_date=TRADE_DATE,
            inference_backend="wsl",
            historical_read_only=True,
        )

    assert exc_info.value.context["reason_code"] == "ADVISORY_HR_PACKAGE_WEIGHT_CONTRACT_UNSUPPORTED"
    assert inference.calls == []
