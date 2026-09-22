from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

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
) -> tuple[ProducerContext, str, Path]:
    root = tmp_path / "candidate"
    files = [
        _file(root / "components/daily_bin_candidate/calendars/day.txt"),
        _file(root / "components/daily_bin_candidate/instruments/all.txt"),
        _file(root / "components/daily_bin_candidate/instruments/benchmark.txt"),
        _file(root / "components/minute_bin_candidate/calendars/1min.txt"),
        _file(root / "components/factor_h5_static_candidate_v2/sector_data.h5"),
        _file(root / "components/index_context/index_daily.h5"),
        _file(root / "components/suspend_d_daily_candidate_v2/suspend_d.parquet"),
        _file(root / "components/sector_context_candidate_v1/sector_code_map.json"),
    ]
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
