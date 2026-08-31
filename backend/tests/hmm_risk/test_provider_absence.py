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

    assert manifest.manifest_version == "hmm_risk_provider_absence_20260824_v2"
    assert len(manifest.rows) == 563
    assert evidence.provider_audit_receipt_sha256 == canonical_sha256(manifest.provider_audit_receipt)
    assert manifest.provider_audit_receipt["absent_key_count"] == 563
    assert manifest.provider_audit_receipt["audit_lineage"] == {
        "current_local_missing_candidate_count": 563,
        "db_writes": False,
        "prior_absent_key_count": 502,
        "prior_audit_receipt_sha256": "a96c19313e110e7ea3ce67f33d0027eaef3ef494898f5d8db7362c9e88670fec",
        "supplemental_absent_key_count": 61,
        "supplemental_absent_key_sha256": "bf638bcb87d51aadc561412a8908aeca74c94e1356cced2eec2114ba449b2ac6",
        "supplemental_candidate_count": 61,
        "supplemental_query_authority": "trade_date_full_market",
        "supplemental_query_date_count": 40,
    }

    supplemental_evidence = manifest.resolve(
        canonical_ts_code="002366.SZ",
        source_dataset="market.moneyflow_ts",
        source_ts_code="002366.SZ",
        trade_date=date(2021, 9, 23),
    )
    assert supplemental_evidence.provider_audit_receipt_sha256 == canonical_sha256(manifest.provider_audit_receipt)
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
