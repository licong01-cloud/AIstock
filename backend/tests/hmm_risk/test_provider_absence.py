from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from backend.services.hmm_risk.provider_absence import load_provider_absence_manifest
from backend.services.hmm_risk.state_model_set import StateModelSetError, canonical_sha256


def _path() -> Path:
    return Path(__file__).resolve().parents[2] / "services" / "hmm_risk" / "manifests" / "provider_absence_v1.json"


def test_provider_absence_manifest_resolves_only_exact_audited_key() -> None:
    path = _path()
    payload = json.loads(path.read_text(encoding="utf-8"))
    manifest = load_provider_absence_manifest(path, expected_sha256=canonical_sha256(payload))

    evidence = manifest.resolve(
        canonical_ts_code="603595.SH",
        source_dataset="market.moneyflow_ts",
        source_ts_code="603595.SH",
        trade_date=date(2024, 1, 8),
    )

    assert len(manifest.rows) == 502
    assert evidence.provider_audit_receipt_sha256 == canonical_sha256(manifest.provider_audit_receipt)
    with pytest.raises(StateModelSetError, match="provider_absence_unverified"):
        manifest.resolve(
            canonical_ts_code="603595.SH",
            source_dataset="market.moneyflow_ts",
            source_ts_code="603595.SH",
            trade_date=date(2024, 1, 9),
        )


def test_provider_absence_manifest_rejects_tampered_row(tmp_path) -> None:
    payload = json.loads(_path().read_text(encoding="utf-8"))
    payload["rows"][0]["trade_date"] = "2022-01-05"
    path = tmp_path / "provider-absence.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(StateModelSetError, match="row hash mismatch"):
        load_provider_absence_manifest(path, expected_sha256=canonical_sha256(payload))
