from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing import action_value_diagnostics as diagnostics


def _oof(*, optional: bool = False) -> pd.DataFrame:
    predictions = [-2.0, 3.0, 1.0, -1.0, 0.5, -0.5]
    if optional:
        predictions = [2.0, -1.0, 4.0, -1.0, -0.5, 3.0]
    return pd.DataFrame(
        {
            "symbol": ["000001.SZ"] * 6,
            "decision_as_of": ["2026-01-02"] * 3 + ["2026-01-05"] * 3,
            "objective": ["ENTRY_ACTION_VALUE_V2"] * 3 + ["EXIT_ACTION_VALUE_V2"] * 3,
            "planned_delta_qty": [100, 200, 300, -100, -200, -300],
            "net_action_value_bps": [5.0, -4.0, 2.0, -3.0, 1.0, 6.0],
            "predicted_action_value_bps": predictions,
            "model_sha256": ["a" * 64] * 6,
        }
    )


def _continuous(*, optional: bool = False) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sleeve_id": ["A", "A", "B"],
            "valuation_date": ["2026-01-02", "2026-01-05", "2026-01-02"],
            "baseline": ["BUY_AND_HOLD", "BUY_AND_HOLD", "FROZEN_L1_V1"],
            "action": ["HOLD", "ADD" if optional else "HOLD", "EXIT"],
            "decision_input_status": ["AVAILABLE", "AVAILABLE", "AVAILABLE"],
            "fill_status": ["NO_ACTION", "FILLED" if optional else "NO_ACTION", "FILLED"],
            "planned_delta_qty": [0, 100 if optional else 0, -100],
            "incremental_net_value_cny": [0.0, 3.0 if optional else 0.0, 4.0],
        }
    )


def _paired() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "valuation_date": ["2026-01-02", "2026-01-05"],
            "incremental_net_value_cny": [10.0, -4.0],
            "sleeve_count": [2, 2],
            "incremental_net_value_bps": [1.5, -0.5],
        }
    )


def test_oof_diagnostics_and_existing_selection_semantics() -> None:
    summary = diagnostics.summarize_oof(_oof())
    comparison = diagnostics.compare_selected_actions(_oof(), _oof(optional=True))

    assert summary["overall"]["row_count"] == 6
    assert set(summary["by_objective"]) == {
        "ENTRY_ACTION_VALUE_V2",
        "EXIT_ACTION_VALUE_V2",
    }
    assert summary["overall"]["predicted_positive_count"] == 3
    assert comparison["decision_group_count"] == 2
    assert comparison["selection_changed_count"] == 2
    assert comparison["both_selected"] == 2
    assert comparison["optional_minus_core_selected_realized_mean_bps"] == pytest.approx(5.5)


def test_zero_variance_and_no_positive_predictions_return_null_not_nan() -> None:
    frame = _oof()
    frame["predicted_action_value_bps"] = 0.0

    result = diagnostics.summarize_oof(frame)["overall"]

    assert result["spearman_prediction_label"] is None
    assert result["calibration_intercept_bps"] is None
    assert result["calibration_slope"] is None
    assert result["predicted_positive_count"] == 0
    assert result["predicted_positive_realized_mean_bps"] is None
    assert result["predicted_positive_realized_positive_rate"] is None


def test_oof_key_mismatch_and_duplicate_fail_closed() -> None:
    duplicate = pd.concat([_oof(), _oof().iloc[:1]], ignore_index=True)
    with pytest.raises(ActionValueError, match="DIAGNOSTIC_OOF_KEY_DUPLICATE"):
        diagnostics.summarize_oof(duplicate)

    optional = _oof(optional=True).iloc[:-1]
    with pytest.raises(ActionValueError, match="DIAGNOSTIC_OOF_KEYS_MISMATCH"):
        diagnostics.compare_selected_actions(_oof(), optional)

    mismatched_label = _oof(optional=True)
    mismatched_label.loc[0, "net_action_value_bps"] += 1
    with pytest.raises(ActionValueError, match="DIAGNOSTIC_OOF_LABELS_MISMATCH"):
        diagnostics.compare_selected_actions(_oof(), mismatched_label)


def test_continuous_diagnostics_use_complete_key_identity() -> None:
    core = _continuous()
    optional = _continuous(optional=True)

    summary = diagnostics.summarize_continuous(optional)
    comparison = diagnostics.compare_continuous(core, optional)

    assert summary["nonzero_plan_count"] == 2
    assert summary["fill_rate_among_nonzero_plans"] == 1.0
    assert comparison["action_changed_count"] == 1
    assert comparison["planned_delta_changed_count"] == 1
    assert comparison["incremental_net_value_changed_count"] == 1
    with pytest.raises(ActionValueError, match="DIAGNOSTIC_CONTINUOUS_KEYS_MISMATCH"):
        diagnostics.compare_continuous(core, optional.iloc[:-1])


def test_paired_daily_must_match_source_receipt_point_estimate() -> None:
    receipt = {"incremental_comparison": {"daily_mean_incremental_bps": 0.5}}
    summary = diagnostics.summarize_paired_daily(_paired(), receipt)
    assert summary["daily_mean_incremental_bps"] == 0.5
    assert summary["period_cumulative_incremental_bps"] == 1.0
    assert summary["positive_day_rate"] == 0.5

    receipt["incremental_comparison"]["daily_mean_incremental_bps"] = 0.6
    with pytest.raises(ActionValueError, match="DIAGNOSTIC_PAIRED_DAILY_RECEIPT_MISMATCH"):
        diagnostics.summarize_paired_daily(_paired(), receipt)


def _write_increment_fixture(root: Path, block: str) -> tuple[Path, dict[str, object]]:
    bundle = root / block
    bundle.mkdir(parents=True)
    optional_prefix = block.lower()
    _oof().to_parquet(bundle / "core_oof_action_predictions.parquet", index=False)
    _oof(optional=True).to_parquet(
        bundle / f"{optional_prefix}_oof_action_predictions.parquet", index=False
    )
    _continuous().to_parquet(bundle / "core_continuous_sleeve_days.parquet", index=False)
    _continuous(optional=True).to_parquet(
        bundle / f"{optional_prefix}_continuous_sleeve_days.parquet", index=False
    )
    _paired().to_parquet(bundle / "paired_daily_increment.parquet", index=False)
    manifest = {
        "manifest_sha256": (block.encode("utf-8").hex() + "0" * 64)[:64],
        "files": {
            "core_oof_action_predictions.parquet": {},
            f"{optional_prefix}_oof_action_predictions.parquet": {},
            "core_continuous_sleeve_days.parquet": {},
            f"{optional_prefix}_continuous_sleeve_days.parquet": {},
            "paired_daily_increment.parquet": {},
        },
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    inspected: dict[str, object] = {
        "manifest": manifest,
        "request": {"information_block": block, "request_sha256": "b" * 64},
        "receipt": {"incremental_comparison": {"daily_mean_incremental_bps": 0.5}},
    }
    return bundle, inspected


def test_prepare_run_inspect_and_exact_retry_are_diagnostic_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixtures = dict(
        _write_increment_fixture(tmp_path / "inputs", block)
        for block in diagnostics.EXPECTED_INFORMATION_BLOCKS
    )

    def fake_inspect(path: Path) -> dict[str, object]:
        return fixtures[path.resolve()]

    monkeypatch.setattr(diagnostics, "inspect_increment_bundle", fake_inspect)
    monkeypatch.setattr(diagnostics, "_clean_repository_commit", lambda _: "a" * 40)
    timing_root = tmp_path / "timing"
    request_path = diagnostics.prepare_diagnostic_request(
        timing_root=timing_root,
        repository_root=tmp_path,
        input_bundles=list(reversed(fixtures)),
    )
    request = json.loads(request_path.read_text(encoding="utf-8"))
    assert [item["information_block"] for item in request["input_bundles"]] == list(
        diagnostics.EXPECTED_INFORMATION_BLOCKS
    )
    assert request["trial_count"] == 0
    assert request["global_registry_write"] is False

    first = diagnostics.run_diagnostic_request(request_path)
    bundle = Path(first["bundle"])
    manifest_before = (bundle / "manifest.json").read_bytes()
    second = diagnostics.run_diagnostic_request(request_path)

    assert first["status"] == "MATERIALIZED"
    assert second["status"] == "ALREADY_MATERIALIZED"
    assert (bundle / "manifest.json").read_bytes() == manifest_before
    assert first["receipt"]["trial_count"] == 0
    assert first["receipt"]["selected_trial_count"] == 0
    assert first["receipt"]["next_hypothesis"] is None
    assert first["receipt"]["serving_status"] == "DIAGNOSTIC_ONLY_NO_RUNTIME_MODEL"
    assert first["receipt"]["input_bundle_count"] == 4


def test_bound_manifest_and_repository_commit_mismatch_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixtures = dict(
        _write_increment_fixture(tmp_path / "inputs", block)
        for block in diagnostics.EXPECTED_INFORMATION_BLOCKS
    )
    monkeypatch.setattr(diagnostics, "inspect_increment_bundle", lambda path: fixtures[path.resolve()])
    monkeypatch.setattr(diagnostics, "_clean_repository_commit", lambda _: "a" * 40)
    request_path = diagnostics.prepare_diagnostic_request(
        timing_root=tmp_path / "timing",
        repository_root=tmp_path,
        input_bundles=list(fixtures),
    )

    monkeypatch.setattr(diagnostics, "_clean_repository_commit", lambda _: "c" * 40)
    with pytest.raises(ActionValueError, match="DIAGNOSTIC_CODE_IDENTITY_MISMATCH"):
        diagnostics.run_diagnostic_request(request_path)

    monkeypatch.setattr(diagnostics, "_clean_repository_commit", lambda _: "a" * 40)
    first_manifest = next(iter(fixtures)) / "manifest.json"
    first_manifest.write_text("changed", encoding="utf-8")
    with pytest.raises(ActionValueError, match="DIAGNOSTIC_INPUT_MANIFEST_FILE_MISMATCH"):
        diagnostics.run_diagnostic_request(request_path)


def test_non_finite_source_values_fail_closed() -> None:
    frame = _oof()
    frame.loc[0, "predicted_action_value_bps"] = np.inf
    with pytest.raises(ActionValueError, match="DIAGNOSTIC_OOF_NON_FINITE"):
        diagnostics.summarize_oof(frame)
