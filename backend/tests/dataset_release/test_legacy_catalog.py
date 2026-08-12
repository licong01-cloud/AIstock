from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from backend.data_service.moneyflow_contract import MONEYFLOW_UNIT_CONTRACT_VERSION
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.contracts import (
    Component,
    Scope,
    UNKNOWN_PRODUCER_PROVENANCE,
)
from backend.services.dataset_release.control_service import (
    DatasetReleaseControlService,
    DatasetReleaseProfileBinding,
)
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.index_contract import index_contract_payload
from backend.services.dataset_release.legacy_catalog import (
    LEGACY_CATALOG_EVIDENCE_SCHEMA,
    LegacyCandidateCataloger,
    LegacyCatalogError,
    LegacyCatalogRequest,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.publisher import artifact_tree_digest
from scripts import update_backtest_dataset_monthly as cli


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _fixture(tmp_path: Path, dataset_profile):
    candidate_root = tmp_path / "candidates"
    candidate = candidate_root / "qe-candidate-20260731"
    metadata = candidate / "metadata"
    metadata.mkdir(parents=True)
    component_refs: dict[str, dict[str, str]] = {}
    for component in Component:
        schema = f"fixture_{component.value}_manifest_v1"
        path = metadata / f"{component.value}.json"
        _json(
            path,
            {
                "schema_version": schema,
                "component": component.value,
                "status": "fixture-only",
            },
        )
        component_refs[component.value] = {
            "relative_path": path.relative_to(candidate).as_posix(),
            "sha256": _sha(path),
            "schema_version": schema,
        }

    pit = freeze_pit_snapshot(
        [
            {
                "ts_code": "000001.SZ",
                "eligible_start": "2019-08-01",
                "eligible_end": "2026-07-31",
                "entry_reason": "ipo365",
                "exit_reason": None,
            },
            {
                "ts_code": "600000.SH",
                "eligible_start": "2018-08-01",
                "eligible_end": "2026-07-31",
                "entry_reason": "listed",
                "exit_reason": None,
            },
        ],
        universe_key=dataset_profile.universe_key,
        rule_version=dataset_profile.universe_rule_version,
        scope_start=dataset_profile.start_date,
        cutoff=date(2026, 7, 31),
        state_identity="fixture-state",
        source_fingerprint_sha256="1" * 64,
        parameter_hash="2" * 64,
    )
    pit_path = metadata / "frozen_pit.json"
    _json(pit_path, pit.as_dict())
    (candidate / "payload.bin").write_bytes(b"immutable candidate bytes")
    artifact_root = artifact_tree_digest(candidate)
    evidence = tmp_path / "legacy-evidence.json"
    _json(
        evidence,
        {
            "schema_version": LEGACY_CATALOG_EVIDENCE_SCHEMA,
            "profile": dataset_profile.profile,
            "semantic_profile_digest": dataset_profile.semantic_profile_digest,
            "scope": "full",
            "cutoff": "2026-07-31",
            "artifact_root": artifact_root,
            "artifact_schema_version": "fixture_qe_candidate_v1",
            "pit_manifest": {
                "relative_path": pit_path.relative_to(candidate).as_posix(),
                "sha256": _sha(pit_path),
                "schema_version": "dataset_release_frozen_pit_v1",
            },
            "pit_snapshot_digest": pit.spans_sha256,
            "moneyflow_contract": MONEYFLOW_UNIT_CONTRACT_VERSION,
            "static_contract": {
                "schema_version": dataset_profile.static_schema_version,
                "ordered_columns_digest": dataset_profile.static_schema_digest,
                "column_count": dataset_profile.static_column_count,
                "l2_code_id_dtype": dataset_profile.l2_code_id_dtype,
                "l2_code_id_missing": dataset_profile.l2_code_id_missing,
            },
            "index_contract": index_contract_payload(),
            "component_manifests": component_refs,
            "producer_provenance": {
                "state": "UNKNOWN",
                "digest_or_sentinel": UNKNOWN_PRODUCER_PROVENANCE,
            },
        },
    )
    store = ControlStore.initialize(tmp_path / "control")
    service = DatasetReleaseControlService(
        (
            DatasetReleaseProfileBinding(
                profile_id=dataset_profile.profile,
                semantic_profile_digest=dataset_profile.semantic_profile_digest,
                cutoff_policy=dataset_profile.cutoff_policy,
                store=store,
                cas=CASStore(store.root),
                cutoff_resolver=lambda _: date(2026, 7, 31),
                candidate_root_id=dataset_profile.candidate_root_id,
            ),
        )
    )
    cataloger = LegacyCandidateCataloger(
        service=service,
        profile=dataset_profile,
        candidate_root=candidate_root,
    )
    return candidate, evidence, store, service, cataloger


def _candidate_snapshot(candidate: Path) -> dict[str, tuple[int, int, str]]:
    return {
        path.relative_to(candidate).as_posix(): (
            path.stat().st_size,
            path.stat().st_mtime_ns,
            _sha(path),
        )
        for path in sorted(candidate.rglob("*"))
        if path.is_file()
    }


def test_catalog_existing_is_read_only_idempotent_and_artifact_only(tmp_path: Path, dataset_profile) -> None:
    candidate, evidence, store, _service, cataloger = _fixture(tmp_path, dataset_profile)
    before = _candidate_snapshot(candidate)
    request = LegacyCatalogRequest(
        candidate_path=candidate,
        evidence_manifest=evidence,
        scope=Scope.FULL,
        cutoff=date(2026, 7, 31),
    )

    first = cataloger.catalog(request)
    replay = cataloger.catalog(request)

    assert replay["registration_id"] == first["registration_id"]
    assert replay["candidate_identity"] == first["candidate_identity"]
    assert replay["artifact_root"] == artifact_tree_digest(candidate)
    assert replay["producer_provenance_state"] == "UNKNOWN"
    assert replay["pit_provenance_state"] == "KNOWN"
    assert replay["candidate_write"] == "forbidden"
    assert _candidate_snapshot(candidate) == before
    assert len(store._many("SELECT * FROM candidate_registrations", ())) == 1
    receipt = CASStore(store.root).get_json_bounded(replay["legacy_catalog_receipt_ref"], max_bytes=2 * 1024 * 1024)
    assert receipt["artifact_scan_count"] == 2
    assert receipt["source_equivalence"] == "not_claimed_catalog_only"
    assert all(value == 0 for value in receipt["safety"].values())


def test_catalog_existing_cli_only_writes_control_catalog(tmp_path: Path, dataset_profile, capsys) -> None:
    candidate, evidence, store, service, cataloger = _fixture(tmp_path, dataset_profile)
    before = _candidate_snapshot(candidate)
    assert (
        cli.main(
            [
                "catalog-existing",
                "--candidate-path",
                str(candidate),
                "--evidence-manifest",
                str(evidence),
                "--cutoff",
                "2026-07-31",
            ],
            service=service,
            cataloger=cataloger,
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["action"] == "catalog-existing"
    assert output["candidate_write"] == "forbidden"
    assert output["execution_started_by_cli"] is False
    assert output["production_activation"] == "not_requested"
    assert output["source_equivalence"] == "not_claimed_catalog_only"
    assert (
        store.latest_candidate_registration(profile=dataset_profile.profile, scope="full")["registration_id"]
        == output["registration_id"]
    )
    assert _candidate_snapshot(candidate) == before


def test_catalog_existing_rejects_drift_escape_and_unstable_scan(
    tmp_path: Path, dataset_profile, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, evidence, store, _service, cataloger = _fixture(tmp_path, dataset_profile)
    request = LegacyCatalogRequest(
        candidate_path=candidate,
        evidence_manifest=evidence,
        scope=Scope.FULL,
        cutoff=date(2026, 7, 31),
    )
    (candidate / "payload.bin").write_bytes(b"drifted")
    with pytest.raises(LegacyCatalogError, match="artifact root"):
        cataloger.catalog(request)
    assert store.latest_candidate_registration(profile=dataset_profile.profile, scope="full") is None

    outside = tmp_path / "outside-candidate"
    outside.mkdir()
    with pytest.raises(LegacyCatalogError, match="escapes"):
        cataloger.catalog(
            LegacyCatalogRequest(
                candidate_path=outside,
                evidence_manifest=evidence,
                scope=Scope.FULL,
                cutoff=date(2026, 7, 31),
            )
        )

    roots = iter(("a" * 64, "b" * 64))
    monkeypatch.setattr(
        "backend.services.dataset_release.legacy_catalog.artifact_tree_digest",
        lambda _path: next(roots),
    )
    with pytest.raises(LegacyCatalogError, match="unstable"):
        cataloger.catalog(request)
