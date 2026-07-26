from __future__ import annotations

import hashlib
import json

import pandas as pd
import pytest

from backend.services.quantevolver.sector_risk_overlay import canonical_json_sha256
from scripts.qe_sector_risk_overlay import QESectorRiskOverlayPolicy


COMPONENTS = {
    "rs_turn_risk": 0.5,
    "breadth_deterioration": 0.5,
    "flow_divergence_risk": 0.5,
    "leadership_concentration": 0.5,
    "vol_crowding_risk": 0.5,
}


def _write_artifact(tmp_path, rows):
    data_path = tmp_path / "runtime.parquet"
    pd.DataFrame(rows).to_parquet(data_path, index=False)
    digest = hashlib.sha256(data_path.read_bytes()).hexdigest()
    manifest = {
        "schema_version": "qe_sector_risk_overlay_manifest_v1",
        "runtime_schema_version": "qe_sector_risk_overlay_runtime_v1",
        "dataset_identity": "fixture-v1",
        "output_start": "2026-01-05",
        "output_end": "2026-01-09",
        "artifacts": {"runtime": {"sha256": digest}},
    }
    manifest["manifest_payload_sha256"] = canonical_json_sha256(manifest)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path, data_path


def _row(date, instrument, sector, state):
    return {
        "signal_date": pd.Timestamp(date) - pd.offsets.BDay(1),
        "effective_trade_date": pd.Timestamp(date),
        "instrument": instrument,
        "l2_code_id": sector,
        "risk_score": 0.95 if state == "CRITICAL" else 0.40,
        "risk_state": state,
        **COMPONENTS,
    }


def test_runtime_uses_confirmed_low_risk_days_for_reentry(tmp_path) -> None:
    rows = [
        _row("2026-01-05", "000001.SZ", 1, "CRITICAL"),
        _row("2026-01-06", "000001.SZ", 1, "NORMAL"),
        _row("2026-01-07", "000001.SZ", 1, "CAUTION"),
        _row("2026-01-08", "000001.SZ", 1, "NORMAL"),
    ]
    manifest_path, data_path = _write_artifact(tmp_path, rows)
    policy = QESectorRiskOverlayPolicy(
        enabled=True,
        mode="exit_reentry",
        manifest_file=manifest_path,
        data_file=data_path,
        reentry_confirm_days=3,
    )

    assert not policy.entry_allowed("000001.SZ", "2026-01-07")
    assert policy.entry_allowed("000001.SZ", "2026-01-08")
    assert policy.multiplier("000001.SZ", "2026-01-05") == 0.0


def test_runtime_rejects_conflicting_sector_states_before_deduplication(tmp_path) -> None:
    rows = [
        _row("2026-01-05", "000001.SZ", 1, "NORMAL"),
        _row("2026-01-05", "000002.SZ", 1, "HIGH"),
    ]
    manifest_path, data_path = _write_artifact(tmp_path, rows)
    with pytest.raises(RuntimeError, match="conflicting sector states"):
        QESectorRiskOverlayPolicy(
            enabled=True,
            mode="bounded_de_risk",
            manifest_file=manifest_path,
            data_file=data_path,
        )


def test_runtime_strict_mode_rejects_missing_stock_date(tmp_path) -> None:
    manifest_path, data_path = _write_artifact(
        tmp_path,
        [_row("2026-01-05", "000001.SZ", 1, "NORMAL")],
    )
    policy = QESectorRiskOverlayPolicy(
        enabled=True,
        mode="entry_gate",
        manifest_file=manifest_path,
        data_file=data_path,
        strict=True,
    )
    with pytest.raises(RuntimeError, match="has no row"):
        policy.state("000002.SZ", "2026-01-05")


@pytest.mark.parametrize(
    ("mode", "state", "expected_multiplier", "entry_allowed"),
    [
        ("none", "CRITICAL", 1.0, True),
        ("entry_gate", "HIGH", 1.0, False),
        ("bounded_de_risk", "HIGH", 0.5, False),
        ("exit_reentry", "CRITICAL", 0.0, False),
    ],
)
def test_runtime_four_arm_policy_semantics(
    tmp_path, mode, state, expected_multiplier, entry_allowed
) -> None:
    manifest_path, data_path = _write_artifact(
        tmp_path,
        [_row("2026-01-05", "000001.SZ", 1, state)],
    )
    policy = QESectorRiskOverlayPolicy(
        enabled=True,
        mode=mode,
        manifest_file=manifest_path,
        data_file=data_path,
    )

    assert policy.multiplier("000001.SZ", "2026-01-05") == expected_multiplier
    assert policy.entry_allowed("000001.SZ", "2026-01-05") is entry_allowed


def test_runtime_rejects_invalid_policy_configuration() -> None:
    disabled = QESectorRiskOverlayPolicy(enabled=False, mode="none")
    assert disabled.row("000001.SZ", "2026-01-05") is None
    assert disabled.multiplier("000001.SZ", "2026-01-05") == 1.0
    with pytest.raises(RuntimeError, match="unsupported"):
        QESectorRiskOverlayPolicy(mode="mystery")
    with pytest.raises(RuntimeError, match="confirm_days"):
        QESectorRiskOverlayPolicy(reentry_confirm_days=0)
    with pytest.raises(RuntimeError, match="must be a mapping"):
        QESectorRiskOverlayPolicy(state_multipliers=[])
    with pytest.raises(RuntimeError, match="unknown states"):
        QESectorRiskOverlayPolicy(state_multipliers={"MYSTERY": 0.5})
    with pytest.raises(RuntimeError, match=r"must be in \[0, 1\]"):
        QESectorRiskOverlayPolicy(state_multipliers={"HIGH": 1.5})
    with pytest.raises(RuntimeError, match="requires manifest_file"):
        QESectorRiskOverlayPolicy(enabled=True, mode="entry_gate")


def test_runtime_non_strict_missing_row_is_explicit_unmapped(tmp_path) -> None:
    manifest_path, data_path = _write_artifact(
        tmp_path,
        [_row("2026-01-05", "000001.SZ", 1, "NORMAL")],
    )
    policy = QESectorRiskOverlayPolicy(
        enabled=True,
        mode="entry_gate",
        manifest_file=manifest_path,
        data_file=data_path,
        strict=False,
    )
    assert policy.state("000002.SZ", "2026-01-05") == "UNMAPPED"
    assert policy.entry_allowed("000002.SZ", "2026-01-05") is True


def test_runtime_rejects_manifest_schema_hash_and_data_schema_drift(tmp_path) -> None:
    manifest_path, data_path = _write_artifact(
        tmp_path,
        [_row("2026-01-05", "000001.SZ", 1, "NORMAL")],
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = "wrong"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RuntimeError, match="manifest schema"):
        QESectorRiskOverlayPolicy(
            enabled=True, mode="entry_gate", manifest_file=manifest_path, data_file=data_path
        )

    manifest["schema_version"] = "qe_sector_risk_overlay_manifest_v1"
    manifest["artifacts"]["runtime"]["sha256"] = "bad"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RuntimeError, match="hash mismatch"):
        QESectorRiskOverlayPolicy(
            enabled=True, mode="entry_gate", manifest_file=manifest_path, data_file=data_path
        )

    pd.DataFrame({"instrument": ["000001.SZ"]}).to_parquet(data_path, index=False)
    manifest["artifacts"]["runtime"]["sha256"] = hashlib.sha256(data_path.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RuntimeError, match="missing columns"):
        QESectorRiskOverlayPolicy(
            enabled=True, mode="entry_gate", manifest_file=manifest_path, data_file=data_path
        )
