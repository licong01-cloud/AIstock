from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from backend.services.hmm_risk.security_identity import load_security_source_identity_manifest
from backend.services.hmm_risk.state_model_set import StateModelSetError, canonical_sha256


def _manifest_path() -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "backend"
        / "services"
        / "hmm_risk"
        / "manifests"
        / "security_source_identity_v1.json"
    )


def test_manifest_resolves_effective_alias_and_dataset_specific_default() -> None:
    path = _manifest_path()
    payload = json.loads(path.read_text(encoding="utf-8"))
    manifest = load_security_source_identity_manifest(path, expected_sha256=canonical_sha256(payload))

    historical = manifest.resolve("302132.SZ", date(2024, 6, 28), "market.moneyflow_ts")
    assert historical.source_ts_code == "300114.SZ"
    assert historical.security_identity_id == "szse_300114_302132"
    assert historical.resolution_kind == "explicit_effective_alias"

    after_change = manifest.resolve("302132.SZ", date(2025, 2, 17), "market.moneyflow_ts")
    assert after_change.source_ts_code == "302132.SZ"
    assert after_change.resolution_kind == "canonical_same_code"

    daily_basic = manifest.resolve("302132.SZ", date(2024, 6, 28), "market.daily_basic")
    assert daily_basic.source_ts_code == "302132.SZ"
    assert daily_basic.resolution_kind == "canonical_same_code"


def test_manifest_rejects_hash_mismatch(tmp_path: Path) -> None:
    path = _manifest_path()
    copied = tmp_path / "identity.json"
    copied.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")

    with pytest.raises(StateModelSetError, match="manifest hash mismatch"):
        load_security_source_identity_manifest(copied, expected_sha256="0" * 64)


def test_manifest_rejects_overlapping_effective_aliases(tmp_path: Path) -> None:
    path = _manifest_path()
    payload = json.loads(path.read_text(encoding="utf-8"))
    duplicate = dict(payload["rows"][0])
    duplicate["effective_start"] = "2024-01-01"
    duplicate["effective_end"] = "2025-02-16"
    body = {key: duplicate[key] for key in sorted(set(duplicate) - {"row_hash"})}
    duplicate["row_hash"] = canonical_sha256(body)
    payload["rows"].append(duplicate)
    payload["rows"].sort(
        key=lambda row: (
            row["source_dataset"],
            row["canonical_ts_code"],
            row["effective_start"],
            row["effective_end"],
        )
    )
    target = tmp_path / "overlap.json"
    target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(StateModelSetError, match="source_identity_ambiguous"):
        load_security_source_identity_manifest(target, expected_sha256=canonical_sha256(payload))


def test_manifest_rejects_tampered_row_hash(tmp_path: Path) -> None:
    path = _manifest_path()
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["rows"][0]["source_ts_code"] = "300115.SZ"
    target = tmp_path / "tampered.json"
    target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(StateModelSetError, match="row hash mismatch"):
        load_security_source_identity_manifest(target, expected_sha256=canonical_sha256(payload))


def test_manifest_rejects_explicit_alias_that_does_not_change_source_code(tmp_path: Path) -> None:
    payload = json.loads(_manifest_path().read_text(encoding="utf-8"))
    payload["rows"][0]["source_ts_code"] = payload["rows"][0]["canonical_ts_code"]
    body = {key: payload["rows"][0][key] for key in sorted(set(payload["rows"][0]) - {"row_hash"})}
    payload["rows"][0]["row_hash"] = canonical_sha256(body)
    target = tmp_path / "same-code-alias.json"
    target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(StateModelSetError, match="explicit alias must change"):
        load_security_source_identity_manifest(target, expected_sha256=canonical_sha256(payload))
