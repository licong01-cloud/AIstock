from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = ROOT / "scripts/qe_alpha_candidates/sector_rotation/p0_d2_sector_oracle.py"
SPEC = importlib.util.spec_from_file_location("p0_d2_sector_oracle", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _sha(value: str) -> str:
    return MODULE.canonical_sha256({"value": value})


def _panel(days: int = 10) -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=days)
    sectors = (101, 202, 303, 404)
    rows: list[dict[str, object]] = []
    for day_index, current_date in enumerate(dates):
        for sector_index, sector in enumerate(sectors):
            # Reality prefers 101/202; oracle future return prefers 202/303.
            sector_score = {101: 4.0, 202: 3.0, 303: 2.0, 404: 1.0}[sector]
            sector_label = {101: 1.0, 202: 4.0, 303: 3.0, 404: 0.5}[sector]
            for stock_index in range(4):
                code = sector_index * 10 + stock_index + 1
                rows.append(
                    {
                        "datetime": current_date,
                        "instrument": f"{code:06d}.SZ",
                        "score": sector_score + stock_index * 0.01 + day_index * 0.0001,
                        # Reverse the within-sector order relative to score.
                        "label": sector_label + (3 - stock_index) * 0.1 + day_index * 0.0002,
                        "l2_code_id": sector,
                        "tradable": True,
                    }
                )
    return pd.DataFrame(rows)


def _manifest(panel_path: Path, **config_overrides: object) -> dict[str, object]:
    config: dict[str, object] = {
        "top_m": 2,
        "top_k": 4,
        "tail_fraction": 0.25,
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


def _write_inputs(tmp_path: Path, panel: pd.DataFrame | None = None) -> tuple[Path, Path, dict[str, object]]:
    panel_path = tmp_path / "panel.parquet"
    (panel if panel is not None else _panel()).to_parquet(panel_path, index=False)
    manifest = _manifest(panel_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return manifest_path, panel_path, manifest


def _validated(tmp_path: Path, panel: pd.DataFrame | None = None):
    _manifest_path, panel_path, manifest = _write_inputs(tmp_path, panel)
    return MODULE.validate_input_manifest(manifest, panel_path=panel_path), panel_path


def test_four_cells_and_two_gating_modes_are_computed(tmp_path: Path) -> None:
    validated, _panel_path = _validated(tmp_path)

    receipt = MODULE.evaluate_panel(_panel(), validated_input=validated)

    assert receipt["outcome"] == MODULE.OUTCOME_COMPUTABLE
    assert receipt["cell_count"] == 8
    assert receipt["date_count"] == 10
    assert receipt["row_count"] == 160
    assert len(receipt["daily_sample_counts"]) == 10
    assert all(
        value == {"rows": 16, "sectors": 4}
        for value in receipt["daily_sample_counts"].values()
    )
    assert {(row["cell_id"], row["gating"]) for row in receipt["cells"]} == {
        (cell_id, gating) for cell_id, _sector, _stock in MODULE.CELL_SPECS for gating in MODULE.GATING_MODES
    }
    assert all(row["portfolio_status"] == MODULE.PORTFOLIO_STATUS for row in receipt["cells"])
    assert all(
        row["deployability"] == ("REALITY_BASELINE" if row["cell_id"] == "D2-RR" else MODULE.ORACLE_MARKER)
        for row in receipt["cells"]
    )
    unsigned = dict(receipt)
    supplied = unsigned.pop("receipt_sha256")
    assert supplied == MODULE.canonical_sha256(unsigned)


def test_oracle_sector_improves_sector_recall_over_reality(tmp_path: Path) -> None:
    validated, _panel_path = _validated(tmp_path)
    receipt = MODULE.evaluate_panel(_panel(), validated_input=validated)

    hard = {row["cell_id"]: row for row in receipt["cells"] if row["gating"] == "hard"}
    assert hard["D2-OR"]["metrics"]["sector_recall_at_m"] == 1.0
    assert hard["D2-RR"]["metrics"]["sector_recall_at_m"] < 1.0


def test_oracle_stock_improves_within_sector_rankic(tmp_path: Path) -> None:
    validated, _panel_path = _validated(tmp_path)
    receipt = MODULE.evaluate_panel(_panel(), validated_input=validated)

    hard = {row["cell_id"]: row for row in receipt["cells"] if row["gating"] == "hard"}
    assert hard["D2-RO"]["metrics"]["within_sector_rankic"] == pytest.approx(1.0)
    assert hard["D2-RR"]["metrics"]["within_sector_rankic"] == pytest.approx(-1.0)


def test_same_inputs_produce_byte_identical_receipt(tmp_path: Path) -> None:
    validated, _panel_path = _validated(tmp_path)

    first = MODULE.evaluate_panel(_panel(), validated_input=validated)
    second = MODULE.evaluate_panel(_panel(), validated_input=validated)

    assert MODULE._canonical_json_bytes(first) == MODULE._canonical_json_bytes(second)


def test_cli_writes_computable_receipt(tmp_path: Path) -> None:
    manifest_path, panel_path, _manifest_payload = _write_inputs(tmp_path)
    output_path = tmp_path / "receipt.json"

    exit_code = MODULE.main(
        [
            "--input-manifest",
            str(manifest_path),
            "--panel",
            str(panel_path),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert json.loads(output_path.read_text(encoding="utf-8"))["cell_count"] == 8
    assert not list(tmp_path.glob(".*.tmp"))


def test_manifest_hash_drift_is_not_computable(tmp_path: Path) -> None:
    manifest_path, panel_path, manifest = _write_inputs(tmp_path)
    manifest["config"]["top_m"] = 3  # type: ignore[index]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    output_path = tmp_path / "receipt.json"

    exit_code = MODULE.main(
        ["--input-manifest", str(manifest_path), "--panel", str(panel_path), "--output", str(output_path)]
    )

    receipt = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 2
    assert receipt["reason_codes"] == ["qe_p0_d2_manifest_sha_mismatch"]


def test_panel_hash_drift_is_not_computable(tmp_path: Path) -> None:
    manifest_path, panel_path, _manifest_payload = _write_inputs(tmp_path)
    changed = _panel()
    changed.loc[0, "score"] += 1.0
    changed.to_parquet(panel_path, index=False)
    output_path = tmp_path / "receipt.json"

    exit_code = MODULE.main(
        ["--input-manifest", str(manifest_path), "--panel", str(panel_path), "--output", str(output_path)]
    )

    assert exit_code == 2
    assert json.loads(output_path.read_text(encoding="utf-8"))["reason_codes"] == [
        "qe_p0_d2_panel_sha_mismatch"
    ]


@pytest.mark.parametrize("column", ["score", "l2_code_id", "tradable"])
def test_missing_required_column_is_not_computable(tmp_path: Path, column: str) -> None:
    panel_path = tmp_path / "panel.parquet"
    _panel().drop(columns=[column]).to_parquet(panel_path, index=False)
    manifest = _manifest(panel_path)

    with pytest.raises(MODULE.D2InputError) as exc_info:
        MODULE.validate_input_manifest(manifest, panel_path=panel_path)

    assert exc_info.value.reason_code == "qe_p0_d2_panel_columns_invalid"


def test_extra_parquet_column_is_not_silently_ignored(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.parquet"
    panel = _panel()
    panel["future_secret"] = 1.0
    panel.to_parquet(panel_path, index=False)
    manifest = _manifest(panel_path)

    with pytest.raises(MODULE.D2InputError) as exc_info:
        MODULE.validate_input_manifest(manifest, panel_path=panel_path)

    assert exc_info.value.reason_code == "qe_p0_d2_panel_columns_invalid"


def test_duplicate_panel_key_is_not_computable(tmp_path: Path) -> None:
    panel = pd.concat([_panel(), _panel().iloc[[0]]], ignore_index=True)
    validated, _panel_path = _validated(tmp_path, panel)

    with pytest.raises(MODULE.D2InputError) as exc_info:
        MODULE.evaluate_panel(panel, validated_input=validated)

    assert exc_info.value.reason_code == "qe_p0_d2_panel_duplicate_key"


def test_unmapped_sector_is_not_computable(tmp_path: Path) -> None:
    panel = _panel()
    panel.loc[0, "l2_code_id"] = -1
    validated, _panel_path = _validated(tmp_path, panel)

    with pytest.raises(MODULE.D2InputError) as exc_info:
        MODULE.evaluate_panel(panel, validated_input=validated)

    assert exc_info.value.reason_code == "qe_p0_d2_taxonomy_invalid"


def test_non_tradable_row_is_not_silently_filtered(tmp_path: Path) -> None:
    panel = _panel()
    panel.loc[0, "tradable"] = False
    validated, _panel_path = _validated(tmp_path, panel)

    with pytest.raises(MODULE.D2InputError) as exc_info:
        MODULE.evaluate_panel(panel, validated_input=validated)

    assert exc_info.value.reason_code == "qe_p0_d2_tradability_invalid"


def test_insufficient_sector_coverage_is_not_computable(tmp_path: Path) -> None:
    panel = _panel().loc[lambda frame: frame["l2_code_id"].isin([101])].copy()
    validated, _panel_path = _validated(tmp_path, panel)

    with pytest.raises(MODULE.D2InputError) as exc_info:
        MODULE.evaluate_panel(panel, validated_input=validated)

    assert exc_info.value.reason_code == "qe_p0_d2_sector_coverage_insufficient"


def test_insufficient_daily_stock_coverage_is_not_computable(tmp_path: Path) -> None:
    panel = _panel().groupby("datetime", sort=True).head(3).copy()
    validated, _panel_path = _validated(tmp_path, panel)

    with pytest.raises(MODULE.D2InputError) as exc_info:
        MODULE.evaluate_panel(panel, validated_input=validated)

    assert exc_info.value.reason_code == "qe_p0_d2_daily_coverage_insufficient"


def test_single_stock_sector_is_not_computable(tmp_path: Path) -> None:
    panel = _panel()
    first_date = panel["datetime"].min()
    remove = (panel["datetime"] == first_date) & (panel["l2_code_id"] == 101)
    panel = panel.loc[~remove | panel.index.to_series().eq(panel.loc[remove].index[0])].copy()
    validated, _panel_path = _validated(tmp_path, panel)

    with pytest.raises(MODULE.D2InputError) as exc_info:
        MODULE.evaluate_panel(panel, validated_input=validated)

    assert exc_info.value.reason_code == "qe_p0_d2_within_sector_coverage_insufficient"


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("max_rows", MODULE.HARD_MAX_ROWS + 1),
        ("max_file_bytes", MODULE.HARD_MAX_FILE_BYTES + 1),
        ("bootstrap_samples", MODULE.HARD_MAX_BOOTSTRAP_SAMPLES + 1),
        ("top_k", 1_001),
    ],
)
def test_config_cannot_raise_hard_resource_limits(tmp_path: Path, key: str, value: int) -> None:
    panel_path = tmp_path / "panel.parquet"
    _panel().to_parquet(panel_path, index=False)
    manifest = _manifest(panel_path, **{key: value})

    with pytest.raises(MODULE.D2InputError) as exc_info:
        MODULE.validate_input_manifest(manifest, panel_path=panel_path)

    assert exc_info.value.reason_code == "qe_p0_d2_config_invalid"


def test_uppercase_identity_sha_is_rejected(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.parquet"
    _panel().to_parquet(panel_path, index=False)
    manifest = _manifest(panel_path)
    manifest["identities"]["dataset"]["sha256"] = "A" * 64  # type: ignore[index]
    _resign(manifest)

    with pytest.raises(MODULE.D2InputError) as exc_info:
        MODULE.validate_input_manifest(manifest, panel_path=panel_path)

    assert exc_info.value.reason_code == "qe_p0_d2_identity_sha_invalid"


def test_output_cannot_replace_panel(tmp_path: Path) -> None:
    manifest_path, panel_path, _manifest_payload = _write_inputs(tmp_path)
    original_sha = MODULE.file_sha256(panel_path)

    exit_code = MODULE.main(
        ["--input-manifest", str(manifest_path), "--panel", str(panel_path), "--output", str(panel_path)]
    )

    assert exit_code == 2
    assert MODULE.file_sha256(panel_path) == original_sha


def test_tool_has_no_database_network_or_process_control_dependency() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    forbidden = (
        "psycopg",
        "sqlalchemy",
        "get_conn(",
        "requests.",
        "httpx.",
        "subprocess",
        "market.",
        "DATABASE_URL",
        "TDX_DB_",
    )
    assert all(token not in source for token in forbidden)


def test_tool_is_exactly_classified_as_non_runtime() -> None:
    from scripts.aistock_issue_workflow import _classify_runtime_impact

    result = _classify_runtime_impact(
        ["scripts/qe_alpha_candidates/sector_rotation/p0_d2_sector_oracle.py"],
        root=ROOT,
    )

    assert result["runtime_impact"] == "none"
    assert result["observed_impacts"] == ["none"]
    assert result["target_ids"] == []
