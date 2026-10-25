from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from backend.services.dataset_release import monthly_file_identity as identity

from backend.data_service.security_source_identity import (
    DEFAULT_MANIFEST_PATH,
    load_security_source_identity_manifest,
)
from backend.services.dataset_release.canonical import (
    canonical_json_bytes,
    digest_named_fields,
)
from backend.services.dataset_release.monthly_local_validation import (
    CONSUMER_CONTRACT_SCHEMA,
    MonthlyCandidateLocalValidationExecutor,
    MonthlyLocalValidationError,
)
from backend.services.dataset_release.monthly_official_adapters import (
    OfficialLocalValidateAdapter,
)
from backend.services.dataset_release.monthly_unified import REQUIRED_CONSUMERS
from backend.services.dataset_release.monthly_worker import ProducerContext


def _json(path: Path, value: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return path


def test_monthly_stages_and_hardlinks_reuse_only_verified_file_bytes(tmp_path, monkeypatch):
    path = tmp_path / "large.h5"
    path.write_bytes(b"frozen output")
    alias = tmp_path / "consumer.h5"
    os.link(path, alias)
    calls = []
    original = Path.open

    def opened(self, mode="r", *args, **kwargs):
        if self in (path, alias) and mode == "rb":
            calls.append(self)
        return original(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", opened)
    expected = hashlib.sha256(b"frozen output").hexdigest()
    for module, name in (
        ("monthly_local_validation", "_sha256"),
        ("monthly_official_adapters", "_sha256"),
        ("monthly_worker", "_file_sha256"),
        ("monthly_immutable_deploy", "_sha256"),
    ):
        imported = __import__("backend.services.dataset_release." + module, fromlist=[name])
        assert getattr(imported, name)(path) == expected
        assert getattr(imported, name)(alias) == expected
    assert len(calls) == 1


def test_writer_pin_reuse_requires_unchanged_signature(tmp_path, monkeypatch):
    path = tmp_path / "written.bin"
    path.write_bytes(b"source output")
    digest = hashlib.sha256(b"source output").hexdigest()
    stat = path.stat()
    signature = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
    identity.remember_verified_file(path, digest, expected_signature=signature)
    original = Path.open

    def opened(self, mode="r", *args, **kwargs):
        if self == path and mode == "rb":
            pytest.fail("unchanged writer output must not be read again")
        return original(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", opened)
    assert identity.file_sha256(path) == digest
    path.write_bytes(b"changed output")
    with pytest.raises(ValueError, match="signature"):
        identity.remember_verified_file(path, digest, expected_signature=signature)


def test_changed_file_is_rehashed_not_trusted_by_path_or_size(tmp_path):
    path = tmp_path / "data.bin"
    path.write_bytes(b"one")
    old = identity.file_sha256(path)
    path.write_bytes(b"two")
    stamp = path.stat()
    os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns + 10_000_000))
    assert identity.file_sha256(path) == hashlib.sha256(b"two").hexdigest() != old


def test_file_changing_while_hashing_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / "moving.bin"
    path.write_bytes(b"before")
    original = identity._stream_sha256

    def changing(target):
        result = original(target)
        target.write_bytes(b"after")
        return result

    monkeypatch.setattr(identity, "_stream_sha256", changing)
    with pytest.raises(ValueError, match="changed"):
        identity.file_sha256(path)


def test_unknown_pin_and_nonregular_file_are_rejected(tmp_path):
    with pytest.raises(ValueError):
        identity.file_sha256(tmp_path)
    path = tmp_path / "data.bin"
    path.write_bytes(b"data")
    with pytest.raises(ValueError):
        identity.remember_verified_file(path, "not-a-digest", expected_signature=(0, 0, 0, 0))


def test_writer_cannot_replace_an_already_computed_digest(tmp_path):
    path = tmp_path / "output.bin"
    path.write_bytes(b"real bytes")
    identity.file_sha256(path)
    value = path.stat()
    signature = (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
    with pytest.raises(ValueError, match="conflicting"):
        identity.remember_verified_file(path, "f" * 64, expected_signature=signature)


def test_bounded_eviction_rehashes_real_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(identity, "_MAX_ENTRIES", 1)
    with identity._LOCK:
        identity._CACHE.clear()
    left, right = tmp_path / "left.bin", tmp_path / "right.bin"
    left.write_bytes(b"left")
    right.write_bytes(b"right")
    expected = identity.file_sha256(left)
    identity.file_sha256(right)
    calls = []
    original = identity._stream_sha256

    def streamed(path):
        calls.append(path)
        return original(path)

    monkeypatch.setattr(identity, "_stream_sha256", streamed)
    assert identity.file_sha256(left) == expected
    assert calls == [left]


def _file(path: Path, value: bytes = b"value") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(
    tmp_path: Path,
    *,
    gaps: bool = False,
    physical_gaps: bool = False,
    layout_authority_drift: bool = False,
    sealed_provenance: bool = False,
) -> tuple[ProducerContext, str, Path]:
    root = tmp_path / "candidate"
    files = [
        _file(root / "components/daily_bin_candidate/calendars/day.txt"),
        _file(root / "components/daily_bin_candidate/instruments/all.txt"),
        _file(root / "components/daily_bin_candidate/instruments/benchmark.txt"),
        _file(root / "components/minute_bin_candidate/calendars/1min.txt"),
        _file(root / "components/factor_h5_static_candidate_v2/sector_data.h5"),
        _file(root / "components/factor_h5_static_candidate_v2/moneyflow.h5"),
        _file(root / "components/index_context/index_daily.h5"),
        _file(root / "components/suspend_d_daily_candidate_v2/suspend_d.parquet"),
        _file(root / "components/sector_context_candidate_v1/sector_code_map.json"),
    ]
    if sealed_provenance:
        files.append(_json(root / "provenance/base_dataset_manifest.json", {
            "schema_version": "aistock_shared_sector_base_dataset_manifest_v1",
            "cutoff_trade_date": "2026-09-30",
        }))
    identity_path = root / "components/factor_h5_static_candidate_v2/security_source_identity.json"
    identity_path.write_bytes(DEFAULT_MANIFEST_PATH.read_bytes())
    identity = load_security_source_identity_manifest(identity_path)
    alias_path = _json(
        root / "components/factor_h5_static_candidate_v2/moneyflow_alias_coverage_v1.json",
        {
            "schema_version": "qe_moneyflow_alias_coverage_receipt_v1",
            "status": "PASS",
            "identity_authority": identity.evidence(),
            "moneyflow_sha256": _sha(
                root / "components/factor_h5_static_candidate_v2/moneyflow.h5"
            ),
            "expected": 0,
            "resolved": 0,
            "provider_absence": 0,
            "unknown": 0,
            "nonfinite": 0,
            "mismatched": 0,
            "database_read": False,
            "database_write": False,
        },
    )
    files.extend((identity_path, alias_path))
    pool_names = (
        "stock_universe",
        "csi300",
        "csi500",
        "csi1000",
        "star50",
        "star100",
    )
    for name in pool_names:
        filename = "stock_universe.txt" if name == "stock_universe" else f"index_pool__{name}.txt"
        files.append(_file(root / "stock_pools" / filename))
    coverage = _json(
        root / "reports/qe_index_pool_coverage_receipt.json",
        {
            "schema_version": "qe_index_pool_coverage_receipt_v1",
            "release_id": "qe_hmm_full_v2_20260930",
            "cutoff": "2026-09-30",
            "pools": {
                name: {
                    "available_start": "2018-08-01",
                    "available_end": "2026-09-30",
                    "gaps": ([{"date": "2026-09-29"}] if gaps and name == "csi300" else []),
                }
                for name in pool_names
            },
        },
    )
    validation_ref = {
        "sha256": "d" * 64,
        "size": 12,
        "relative_path": f"cas/sha256/dd/{'d' * 64}",
    }
    component_ref = {
        "sha256": "e" * 64,
        "size": 34,
        "relative_path": f"cas/sha256/ee/{'e' * 64}",
    }
    build = _json(
        root / "reports/monthly_candidate_build_evidence.json",
        {
            "schema_version": "aistock_monthly_candidate_build_evidence_v1",
            "operation_id": "dmr_" + "1" * 32,
            "attempt": 1,
            "release_id": "qe_hmm_full_v2_20260930",
            "target_cutoff": "2026-09-30",
            "source_bundle_sha256": "a" * 64,
            "validation_ref": validation_ref,
            "component_artifact_manifest_ref": component_ref,
            "database_read_performed": False,
            "database_write_performed": False,
            "runtime_action_performed": False,
        },
    )
    physical_coverage = _json(
        root / "reports/index_pool_coverage.json",
        {
            "schema_version": "aistock_monthly_six_pool_coverage_v1",
            "cutoff_trade_date": "2026-09-30",
            "physical_validation_ref": validation_ref,
            "component_artifact_manifest_ref": component_ref,
            "pools": {
                name: {
                    "symbol_count": 1,
                    "span_count": 1,
                    "day_gap_count": 0,
                    "minute_gap_count": (
                        1 if physical_gaps and name == "csi500" else 0
                    ),
                    "subset_of_frozen_pit": True,
                    "sidecar_sha256": _sha(
                        root
                        / "stock_pools"
                        / (
                            "stock_universe.txt"
                            if name == "stock_universe"
                            else f"index_pool__{name}.txt"
                        )
                    ),
                }
                for name in pool_names
            },
            "unexplained_gap_count": 1 if physical_gaps else 0,
        },
    )
    component_prefixes = {
        "day": "components/daily_bin_candidate/",
        "minute": "components/minute_bin_candidate/",
        "factor": "components/factor_h5_static_candidate_v2/",
        "index": "components/index_context/",
    }
    summaries = {}
    for name, prefix in component_prefixes.items():
        rows = {
            path.relative_to(root).as_posix(): {
                "sha256": _sha(path),
                "size": path.stat().st_size,
            }
            for path in sorted(files)
            if path.relative_to(root).as_posix().startswith(prefix)
        }
        summaries[name] = {
            "file_count": len(rows),
            "logical_bytes": sum(int(item["size"]) for item in rows.values()),
            "content_digest": digest_named_fields(
                "aistock_monthly_consumer_component_v1", rows
            ),
        }
    layout_authority = {
        "validation_ref": validation_ref,
        "component_artifact_manifest_ref": component_ref,
    }
    if layout_authority_drift:
        layout_authority = {
            **layout_authority,
            "validation_ref": {**validation_ref, "sha256": "f" * 64},
        }
    layout_body = {
        "schema_version": "aistock_monthly_consumer_layout_v1",
        "release_id": "qe_hmm_full_v2_20260930",
        "cutoff": "2026-09-30",
        "publication_mode": "same_filesystem_hardlink_v1",
        "components": summaries,
        "coverage_receipt": {
            "path": "reports/qe_index_pool_coverage_receipt.json",
            "sha256": _sha(coverage),
            "size": coverage.stat().st_size,
        },
        "validation_authority": layout_authority,
        "source_freeze": True,
        "database_read": False,
        "database_write": False,
        "runtime_action": False,
    }
    layout = _json(
        root / "reports/monthly_consumer_layout_receipt.json",
        {
            **layout_body,
            "consumer_layout_digest": digest_named_fields(
                "aistock_monthly_consumer_layout_v1", layout_body
            ),
        },
    )
    files.extend((coverage, physical_coverage, build, layout))
    components = {
        path.relative_to(root).as_posix().replace("/", "__"): {
            "path": path.relative_to(root).as_posix(),
            "sha256": _sha(path),
            "size": path.stat().st_size,
        }
        for path in files
    }
    sidecars = {
        name: components[
            (
                "stock_pools/stock_universe.txt"
                if name == "stock_universe"
                else f"stock_pools/index_pool__{name}.txt"
            ).replace("/", "__")
        ]
        for name in pool_names
    }
    st_pit = {
        "schema_version": "qe_st_pit_manifest_v1",
        "snapshot_id": "pit-20260930",
        "cutoff_trade_date": "2026-09-30",
        "universe_key": "aistock_equity_pit_canonical_v2",
        "rule_version": "shsz_a_252td_st_delist_asof_v2",
        "selection_universe": sidecars["stock_universe"],
        "index_membership_sidecars": sidecars,
    }
    deployment_content = hashlib.sha256(canonical_json_bytes(components)).hexdigest()
    manifest: dict[str, Any] = {
        "schema_version": "qe_dataset_manifest_v1",
        "release_id": "qe_hmm_full_v2_20260930",
        "revision": "20260930-monthly-v2",
        "cutoff_trade_date": "2026-09-30",
        "deployment_content_sha256": deployment_content,
        "deployment_snapshot_id": f"qe_hmm_full_v2_20260930_{deployment_content[:16]}",
        "qlib_calendar_sha256": components[
            "components__daily_bin_candidate__calendars__day.txt"
        ]["sha256"],
        "qlib_instruments_sha256": components[
            "components__daily_bin_candidate__instruments__all.txt"
        ]["sha256"],
        "st_pit_snapshot_id": "pit-20260930",
        "st_pit_manifest_sha256": hashlib.sha256(
            canonical_json_bytes(st_pit)
        ).hexdigest(),
        "st_pit_manifest": st_pit,
        "source_contract": {
            "no_fabrication": True,
            "database_fallback": False,
        },
        "components": components,
    }
    manifest_sha = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    manifest["dataset_manifest_sha256"] = manifest_sha
    _json(root / "qe_dataset_manifest.json", manifest)

    asset = _json(
        root / "derived/coefficients.json",
        {
            "schema_version": "hmm_coefficients_v1",
            "dataset_manifest_sha256": manifest_sha,
        },
    )
    registry = _json(
        root / "derived/derived_asset_registry.json",
        {
            "schema_version": "aistock_dataset_derived_asset_registry_v1",
            "source_dataset_manifest_sha256": manifest_sha,
            "assets": [
                {
                    "asset_id": "hmm_coefficients",
                    "path": "coefficients.json",
                    "sha256": _sha(asset),
                    "size": asset.stat().st_size,
                    "schema_version": "hmm_coefficients_v1",
                }
            ],
        },
    )
    context = ProducerContext(
        stage="LOCAL_VALIDATE",
        operation_id="dmr_" + "1" * 32,
        attempt=1,
        request={},
        plan={
            "candidate_root": str(root.resolve()),
            "release_id": "qe_hmm_full_v2_20260930",
            "revision": "20260930-monthly-v2",
            "target_cutoff": "2026-09-30",
            "profile_candidate": str((tmp_path / "profile.json").resolve()),
            "predecessor": {"dataset_manifest_sha256": "b" * 64},
            "predecessor_profile_ref": {
                "id": "active.json",
                "sha256": "c" * 64,
                "size": 1,
            },
        },
        prior_receipts={
            "BUILD": {
                "scope": {
                    "dataset_manifest_sha256": manifest_sha,
                    "dataset_manifest_ref": {
                        "id": "candidate/qe_dataset_manifest.json",
                        "sha256": _sha(root / "qe_dataset_manifest.json"),
                        "size": (root / "qe_dataset_manifest.json").stat().st_size,
                    },
                }
            },
            "DERIVE": {
                "scope": {
                    "derived_asset_registry_ref": {
                        "id": "candidate/derived/derived_asset_registry.json",
                        "sha256": _sha(registry),
                        "size": registry.stat().st_size,
                    },
                    "derived_assets": [
                        {
                            "id": "candidate/derived/coefficients.json",
                            "sha256": _sha(asset),
                            "size": asset.stat().st_size,
                        }
                    ],
                }
            }
        },
    )
    return context, manifest_sha, root


class _ProfileBuilder:
    def execute(
        self,
        context: ProducerContext,
        *,
        release_closure_path: Path,
        derived_asset_registry_path: Path,
    ) -> Path:
        return _json(
            Path(str(context.plan["profile_candidate"])),
            {
                "schema_version": "aistock_active_dataset_profile_v4",
                "dataset_manifest_sha256": context.prior_receipts["BUILD"]["scope"][
                    "dataset_manifest_sha256"
                ],
                "release_closure_sha256": _sha(release_closure_path),
                "derived_asset_registry_sha256": _sha(derived_asset_registry_path),
            },
        )


def test_local_validator_emits_manifest_bound_consumer_evidence(tmp_path: Path) -> None:
    context, manifest_sha, root = _fixture(tmp_path)

    result = MonthlyCandidateLocalValidationExecutor().execute(
        context,
        dataset_manifest_sha256=manifest_sha,
    )

    assert set(result.pool_gap_counts) == {
        "stock_universe",
        "csi300",
        "csi500",
        "csi1000",
        "star50",
        "star100",
    }
    assert set(result.pool_gap_counts.values()) == {0}
    assert len(result.consumer_contracts) == len(REQUIRED_CONSUMERS)
    contracts = [json.loads(path.read_bytes()) for path in result.consumer_contracts]
    assert {value["consumer_id"] for value in contracts} == set(REQUIRED_CONSUMERS)
    assert {value["schema_version"] for value in contracts} == {CONSUMER_CONTRACT_SCHEMA}
    assert {value["dataset_manifest_sha256"] for value in contracts} == {manifest_sha}
    assert all(path.is_relative_to(root / "provenance") for path in result.consumer_contracts)
    assert result.dataset_identity_complete is True
    assert result.workload.bytes_transferred == 0


def test_local_validator_accepts_build_manifest_pinned_provenance(tmp_path: Path) -> None:
    context, manifest_sha, root = _fixture(tmp_path, sealed_provenance=True)
    before = (root / "provenance/base_dataset_manifest.json").read_bytes()
    executor = MonthlyCandidateLocalValidationExecutor()
    executor.execute(context, dataset_manifest_sha256=manifest_sha)
    executor.execute(context, dataset_manifest_sha256=manifest_sha)
    assert (root / "provenance/base_dataset_manifest.json").read_bytes() == before


def test_local_validator_still_rejects_unknown_provenance(tmp_path: Path) -> None:
    context, manifest_sha, root = _fixture(tmp_path, sealed_provenance=True)
    _json(root / "provenance/foreign.json", {"status": "PASS"})
    with pytest.raises(MonthlyLocalValidationError, match="unknown entries"):
        MonthlyCandidateLocalValidationExecutor().execute(context, dataset_manifest_sha256=manifest_sha)


def test_local_validator_is_identical_resume_safe(tmp_path: Path) -> None:
    context, manifest_sha, _root = _fixture(tmp_path)
    executor = MonthlyCandidateLocalValidationExecutor()
    first = executor.execute(context, dataset_manifest_sha256=manifest_sha)

    second = executor.execute(context, dataset_manifest_sha256=manifest_sha)

    assert [path.read_bytes() for path in first.consumer_contracts] == [
        path.read_bytes() for path in second.consumer_contracts
    ]


def test_local_validator_closes_official_adapter_without_fixture_evidence(
    tmp_path: Path,
) -> None:
    context, manifest_sha, root = _fixture(tmp_path)
    adapter = OfficialLocalValidateAdapter(
        artifact_root=tmp_path,
        executor=MonthlyCandidateLocalValidationExecutor(),
        profile_builder=_ProfileBuilder(),
    )

    result = adapter.execute(context)

    assert result.scope["dataset_manifest_sha256"] == manifest_sha
    assert result.scope["dataset_identity_complete"] is True
    assert (root / "release_closure_receipt.json").is_file()
    assert Path(str(context.plan["profile_candidate"])).is_file()

    resumed = adapter.execute(context)

    assert resumed.scope["release_closure_file_sha256"] == result.scope[
        "release_closure_file_sha256"
    ]


def test_local_validator_rejects_manifest_component_drift(tmp_path: Path) -> None:
    context, manifest_sha, root = _fixture(tmp_path)
    (root / "components/index_context/index_daily.h5").write_bytes(b"drift")

    with pytest.raises(MonthlyLocalValidationError, match="component bytes differ"):
        MonthlyCandidateLocalValidationExecutor().execute(
            context,
            dataset_manifest_sha256=manifest_sha,
        )


def test_local_validator_rejects_six_pool_gap(tmp_path: Path) -> None:
    context, manifest_sha, _root = _fixture(tmp_path, gaps=True)

    with pytest.raises(MonthlyLocalValidationError, match="coverage contains gaps"):
        MonthlyCandidateLocalValidationExecutor().execute(
            context,
            dataset_manifest_sha256=manifest_sha,
        )


def test_local_validator_rejects_unpinned_core_file(tmp_path: Path) -> None:
    context, manifest_sha, root = _fixture(tmp_path)
    _file(root / "components/index_context/unpinned.bin")

    with pytest.raises(MonthlyLocalValidationError, match="not manifest-pinned"):
        MonthlyCandidateLocalValidationExecutor().execute(
            context,
            dataset_manifest_sha256=manifest_sha,
        )


def test_local_validator_rejects_physical_six_pool_gap(tmp_path: Path) -> None:
    context, manifest_sha, _root = _fixture(tmp_path, physical_gaps=True)

    with pytest.raises(MonthlyLocalValidationError, match="physical six-pool"):
        MonthlyCandidateLocalValidationExecutor().execute(
            context,
            dataset_manifest_sha256=manifest_sha,
        )


def test_local_validator_rejects_layout_authority_drift(tmp_path: Path) -> None:
    context, manifest_sha, _root = _fixture(tmp_path, layout_authority_drift=True)

    with pytest.raises(MonthlyLocalValidationError, match="layout readiness"):
        MonthlyCandidateLocalValidationExecutor().execute(
            context,
            dataset_manifest_sha256=manifest_sha,
        )


def test_local_validator_rejects_unregistered_derived_file(tmp_path: Path) -> None:
    context, manifest_sha, root = _fixture(tmp_path)
    _file(root / "derived/unregistered.json")

    with pytest.raises(MonthlyLocalValidationError, match="unregistered files"):
        MonthlyCandidateLocalValidationExecutor().execute(
            context,
            dataset_manifest_sha256=manifest_sha,
        )
