from __future__ import annotations

import importlib.util
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = ROOT / "scripts/qe_alpha_candidates/sector_rotation/p0_d3_benchmark_brinson.py"
SPEC = importlib.util.spec_from_file_location("p0_d3_benchmark_brinson", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _sha(value: str) -> str:
    return MODULE.canonical_sha256({"value": value})


def _panel(days: int = 8) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day_index, current_date in enumerate(pd.bdate_range("2025-01-02", periods=days)):
        benchmark_returns = (
            0.002 + day_index * 0.0003,
            -0.001 + day_index * 0.0001,
            0.0005 - day_index * 0.00015,
        )
        portfolio_returns = (
            benchmark_returns[0] + 0.001 + day_index * 0.00005,
            benchmark_returns[1] - 0.0004,
            benchmark_returns[2] + 0.0002,
        )
        for sector, portfolio_weight, benchmark_weight, portfolio_return, benchmark_return in zip(
            (101, 202, 303),
            (0.5, 0.3, 0.2),
            (0.3, 0.4, 0.3),
            portfolio_returns,
            benchmark_returns,
            strict=True,
        ):
            rows.append(
                {
                    "datetime": current_date,
                    "l2_code_id": sector,
                    "portfolio_weight": portfolio_weight,
                    "benchmark_weight": benchmark_weight,
                    "portfolio_sector_return": portfolio_return,
                    "benchmark_sector_return": benchmark_return,
                }
            )
    return pd.DataFrame(rows)


def _manifest(panel_path: Path, **config_overrides: object) -> dict[str, object]:
    config: dict[str, object] = {
        "annualization_days": 252,
        "weight_tolerance": 1e-9,
        "reconciliation_tolerance": 1e-12,
        "bootstrap_block_days": 3,
        "bootstrap_samples": 200,
        "bootstrap_seed": 123,
        "max_rows": 10_000,
        "max_file_bytes": 10_000_000,
    }
    config.update(config_overrides)
    payload: dict[str, object] = {
        "schema_version": MODULE.INPUT_SCHEMA,
        "panel_sha256": MODULE.file_sha256(panel_path),
        "identities": {
            key: {"identity": f"{key}-v1", "sha256": _sha(key)}
            for key in MODULE.IDENTITY_KEYS
        },
        "config": config,
    }
    payload["manifest_sha256"] = MODULE.canonical_sha256(payload)
    return payload


def _resign(manifest: dict[str, object]) -> None:
    manifest.pop("manifest_sha256", None)
    manifest["manifest_sha256"] = MODULE.canonical_sha256(manifest)


def _write_inputs(
    tmp_path: Path,
    panel: pd.DataFrame | None = None,
    **config_overrides: object,
) -> tuple[Path, Path, dict[str, object]]:
    panel_path = tmp_path / "panel.parquet"
    (panel if panel is not None else _panel()).to_parquet(panel_path, index=False)
    manifest = _manifest(panel_path, **config_overrides)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_path, panel_path, manifest


def _validated(tmp_path: Path, panel: pd.DataFrame | None = None):
    _manifest_path, panel_path, manifest = _write_inputs(tmp_path, panel)
    return MODULE.validate_input_manifest(manifest, panel_path=panel_path)


def _assert_reason(tmp_path: Path, panel: pd.DataFrame, reason: str) -> None:
    validated = _validated(tmp_path, panel)
    with pytest.raises(MODULE.D3InputError) as exc_info:
        MODULE.evaluate_panel(panel, validated_input=validated)
    assert exc_info.value.reason_code == reason


def test_reconciles_absolute_active_and_brinson_metrics(tmp_path: Path) -> None:
    receipt = MODULE.evaluate_panel(_panel(), validated_input=_validated(tmp_path))

    assert receipt["outcome"] == MODULE.OUTCOME_RECONCILED
    assert receipt["row_count"] == 24
    assert receipt["date_count"] == 8
    assert len(receipt["daily_sample_counts"]) == 8
    assert all(value == {"rows": 3, "sectors": 3} for value in receipt["daily_sample_counts"].values())
    active_sum = receipt["absolute_and_active"]["active_arithmetic_sum"]
    effect_sum = sum(item["sum"] for item in receipt["brinson"]["effects"].values())
    assert effect_sum == pytest.approx(active_sum, abs=1e-12)
    assert receipt["brinson"]["max_abs_daily_reconciliation_residual"] <= 1e-12
    assert receipt["absolute_and_active"]["tracking_error_annualized"] > 0
    assert receipt["absolute_and_active"]["information_ratio_annualized"] > 0
    panel = _panel()
    daily = panel.groupby("datetime", sort=True).apply(
        lambda group: pd.Series(
            {
                "portfolio": (group["portfolio_weight"] * group["portfolio_sector_return"]).sum(),
                "benchmark": (group["benchmark_weight"] * group["benchmark_sector_return"]).sum(),
            }
        ),
        include_groups=False,
    )
    expected_beta = np.cov(daily["portfolio"], daily["benchmark"], ddof=1)[0, 1] / np.var(
        daily["benchmark"], ddof=1
    )
    assert math.isfinite(receipt["absolute_and_active"]["beta"])
    assert receipt["absolute_and_active"]["beta"] == pytest.approx(expected_beta)
    unsigned = dict(receipt)
    supplied = unsigned.pop("receipt_sha256")
    assert supplied == MODULE.canonical_sha256(unsigned)


def test_cumulative_returns_use_compounding_not_arithmetic_sum(tmp_path: Path) -> None:
    receipt = MODULE.evaluate_panel(_panel(), validated_input=_validated(tmp_path))
    absolute = receipt["absolute_and_active"]
    assert absolute["portfolio_cumulative_return"] != pytest.approx(absolute["active_arithmetic_sum"])
    assert absolute["active_cumulative_return_difference"] == pytest.approx(
        absolute["portfolio_cumulative_return"] - absolute["benchmark_cumulative_return"]
    )


def test_receipt_and_bootstrap_are_deterministic(tmp_path: Path) -> None:
    validated = _validated(tmp_path)
    first = MODULE.evaluate_panel(_panel(), validated_input=validated)
    second = MODULE.evaluate_panel(_panel(), validated_input=validated)
    assert MODULE._canonical_json_bytes(first) == MODULE._canonical_json_bytes(second)


def test_cli_writes_reconciled_receipt_atomically(tmp_path: Path) -> None:
    manifest_path, panel_path, _manifest_payload = _write_inputs(tmp_path)
    output_path = tmp_path / "receipt.json"

    exit_code = MODULE.main(
        ["--input-manifest", str(manifest_path), "--panel", str(panel_path), "--output", str(output_path)]
    )

    receipt = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert receipt["outcome"] == MODULE.OUTCOME_RECONCILED
    assert not list(tmp_path.glob(".*.tmp"))


def test_manifest_hash_drift_is_not_computable(tmp_path: Path) -> None:
    manifest_path, panel_path, manifest = _write_inputs(tmp_path)
    manifest["config"]["bootstrap_seed"] = 999  # type: ignore[index]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    output_path = tmp_path / "receipt.json"
    assert MODULE.main(
        ["--input-manifest", str(manifest_path), "--panel", str(panel_path), "--output", str(output_path)]
    ) == 2
    assert json.loads(output_path.read_text(encoding="utf-8"))["reason_codes"] == [
        "qe_p0_d3_manifest_sha_mismatch"
    ]


def test_panel_hash_drift_is_not_computable(tmp_path: Path) -> None:
    manifest_path, panel_path, _manifest = _write_inputs(tmp_path)
    changed = _panel()
    changed.loc[0, "portfolio_weight"] = 0.49
    changed.to_parquet(panel_path, index=False)
    output_path = tmp_path / "receipt.json"
    assert MODULE.main(
        ["--input-manifest", str(manifest_path), "--panel", str(panel_path), "--output", str(output_path)]
    ) == 2
    assert json.loads(output_path.read_text(encoding="utf-8"))["reason_codes"] == [
        "qe_p0_d3_panel_sha_mismatch"
    ]


@pytest.mark.parametrize("column", ["l2_code_id", "portfolio_weight", "benchmark_sector_return"])
def test_missing_parquet_column_is_rejected(tmp_path: Path, column: str) -> None:
    panel_path = tmp_path / "panel.parquet"
    _panel().drop(columns=[column]).to_parquet(panel_path, index=False)
    with pytest.raises(MODULE.D3InputError) as exc_info:
        MODULE.validate_input_manifest(_manifest(panel_path), panel_path=panel_path)
    assert exc_info.value.reason_code == "qe_p0_d3_panel_columns_invalid"


def test_extra_parquet_column_is_not_silently_ignored(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.parquet"
    panel = _panel().assign(future_secret=1.0)
    panel.to_parquet(panel_path, index=False)
    with pytest.raises(MODULE.D3InputError) as exc_info:
        MODULE.validate_input_manifest(_manifest(panel_path), panel_path=panel_path)
    assert exc_info.value.reason_code == "qe_p0_d3_panel_columns_invalid"


def test_duplicate_key_is_rejected(tmp_path: Path) -> None:
    panel = pd.concat([_panel(), _panel().iloc[[0]]], ignore_index=True)
    _assert_reason(tmp_path, panel, "qe_p0_d3_panel_duplicate_key")


@pytest.mark.parametrize("column,value", [("portfolio_weight", -0.1), ("benchmark_weight", 1.1)])
def test_weight_outside_unit_interval_is_rejected(tmp_path: Path, column: str, value: float) -> None:
    panel = _panel()
    panel.loc[0, column] = value
    _assert_reason(tmp_path, panel, "qe_p0_d3_weight_invalid")


def test_weight_sum_mismatch_is_rejected(tmp_path: Path) -> None:
    panel = _panel()
    panel.loc[0, "portfolio_weight"] = 0.49
    _assert_reason(tmp_path, panel, "qe_p0_d3_weight_sum_mismatch")


@pytest.mark.parametrize("column", ["portfolio_sector_return", "benchmark_sector_return"])
def test_return_at_or_below_minus_one_is_rejected(tmp_path: Path, column: str) -> None:
    panel = _panel()
    panel.loc[0, column] = -1.0
    _assert_reason(tmp_path, panel, "qe_p0_d3_return_invalid")


def test_invalid_taxonomy_is_rejected(tmp_path: Path) -> None:
    panel = _panel()
    panel.loc[0, "l2_code_id"] = -1
    _assert_reason(tmp_path, panel, "qe_p0_d3_taxonomy_invalid")


def test_insufficient_sector_coverage_is_rejected(tmp_path: Path) -> None:
    panel = _panel().loc[lambda frame: frame["l2_code_id"] == 101].copy()
    panel["portfolio_weight"] = 1.0
    panel["benchmark_weight"] = 1.0
    _assert_reason(tmp_path, panel, "qe_p0_d3_sector_coverage_insufficient")


def test_insufficient_date_coverage_is_rejected(tmp_path: Path) -> None:
    panel = _panel(days=2)
    _assert_reason(tmp_path, panel, "qe_p0_d3_date_coverage_insufficient")


def test_benchmark_variance_zero_is_rejected(tmp_path: Path) -> None:
    panel = _panel()
    panel["benchmark_sector_return"] = 0.001
    _assert_reason(tmp_path, panel, "qe_p0_d3_benchmark_variance_zero")


def test_tracking_error_zero_is_rejected(tmp_path: Path) -> None:
    panel = _panel()
    panel["portfolio_weight"] = panel["benchmark_weight"]
    panel["portfolio_sector_return"] = panel["benchmark_sector_return"]
    _assert_reason(tmp_path, panel, "qe_p0_d3_tracking_error_zero")


@pytest.mark.parametrize(
    "key,value",
    [
        ("max_rows", MODULE.HARD_MAX_ROWS + 1),
        ("max_file_bytes", MODULE.HARD_MAX_FILE_BYTES + 1),
        ("bootstrap_samples", MODULE.HARD_MAX_BOOTSTRAP_SAMPLES + 1),
    ],
)
def test_resource_hard_caps_cannot_be_raised(tmp_path: Path, key: str, value: int) -> None:
    _manifest_path, panel_path, manifest = _write_inputs(tmp_path)
    manifest["config"][key] = value  # type: ignore[index]
    _resign(manifest)
    with pytest.raises(MODULE.D3InputError) as exc_info:
        MODULE.validate_input_manifest(manifest, panel_path=panel_path)
    assert exc_info.value.reason_code == "qe_p0_d3_config_invalid"


def test_uppercase_sha_is_rejected(tmp_path: Path) -> None:
    _manifest_path, panel_path, manifest = _write_inputs(tmp_path)
    manifest["identities"]["benchmark"]["sha256"] = (  # type: ignore[index]
        str(manifest["identities"]["benchmark"]["sha256"]).upper()  # type: ignore[index]
    )
    _resign(manifest)
    with pytest.raises(MODULE.D3InputError) as exc_info:
        MODULE.validate_input_manifest(manifest, panel_path=panel_path)
    assert exc_info.value.reason_code == "qe_p0_d3_identity_sha_invalid"


def test_output_cannot_replace_an_input(tmp_path: Path) -> None:
    manifest_path, panel_path, _manifest = _write_inputs(tmp_path)
    assert MODULE.main(
        ["--input-manifest", str(manifest_path), "--panel", str(panel_path), "--output", str(panel_path)]
    ) == 2
    assert MODULE.file_sha256(panel_path) == _manifest["panel_sha256"]


def test_source_has_no_database_network_or_process_control_tokens() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8").lower()
    forbidden = (
        "psycopg",
        "sqlalchemy",
        "get_conn",
        "requests.",
        "httpx",
        "subprocess",
        "os.system",
        "market.",
        "database_url",
    )
    assert all(token not in source for token in forbidden)


def test_tool_is_exactly_classified_as_non_runtime() -> None:
    from scripts.aistock_issue_workflow import _classify_runtime_impact

    result = _classify_runtime_impact(
        ["scripts/qe_alpha_candidates/sector_rotation/p0_d3_benchmark_brinson.py"],
        root=ROOT,
    )

    assert result["runtime_impact"] == "none"
    assert result["observed_impacts"] == ["none"]
    assert result["target_ids"] == []
