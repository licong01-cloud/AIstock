from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.contracts import (
    canonical_json_bytes,
    canonical_sha256,
)
from backend.services.position_timing.pattern_adj_factor_restatement import (
    audit_candidate_adj_factor_restatement,
    open_adj_factor_restatement_authority,
)
from backend.services.position_timing.pattern_research import (
    SOURCE_REVISION_REASON,
    SOURCE_REVISION_SELECTION_AUTHORITY,
    _source_revision_request_contract_invalid,
)


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _authority_candidate(tmp_path: Path) -> tuple[Path, str, str]:
    root = tmp_path / "candidate"
    authority_root = root / "components" / "position_timing_source_authority_v1"
    authority_root.mkdir(parents=True)
    raw_series = [
        {
            "ts_code": "300506.SZ",
            "start": "2026-07-03",
            "end": "2026-08-31",
            "row_count": 3,
            "rows": [
                {"ts_code": "300506.SZ", "trade_date": "2026-07-03", "adj_factor": 2.0},
                {"ts_code": "300506.SZ", "trade_date": "2026-07-06", "adj_factor": 2.0},
                {"ts_code": "300506.SZ", "trade_date": "2026-08-31", "adj_factor": 4.0},
            ],
        },
        {
            "ts_code": "688109.SH",
            "start": "2026-07-08",
            "end": "2026-08-31",
            "row_count": 3,
            "rows": [
                {"ts_code": "688109.SH", "trade_date": "2026-07-08", "adj_factor": 1.0},
                {"ts_code": "688109.SH", "trade_date": "2026-07-09", "adj_factor": 1.0},
                {"ts_code": "688109.SH", "trade_date": "2026-08-31", "adj_factor": 2.0},
            ],
        },
    ]
    for item in raw_series:
        item["ordered_rows_sha256"] = canonical_sha256(item["rows"])
    authority = {
        "schema_version": "dataset_release_adj_factor_restatement_authority_v1",
        "cutoff_trade_date": "2026-08-31",
        "diagnosis_sha256": "d" * 64,
        "provider": "tushare",
        "request_id": "PT-NEXT-018",
        "safety": {
            "candidate_writes": 0,
            "database_writes": 0,
            "production_deletes": 0,
            "production_pointer_changes": 0,
            "production_writes": 0,
            "provider_database_writes": 0,
            "service_process_controls": 0,
        },
        "series": raw_series,
    }
    authority["canonical_sha256"] = canonical_sha256(authority)
    authority_path = authority_root / "adj_factor_restatement_authority.json"
    authority_path.write_bytes(canonical_json_bytes(authority))
    manifest = {
        "schema_version": "qe_dataset_manifest_v1",
        "availability_status": "CANDIDATE_READY",
        "revision": "test-r5",
        "cutoff_trade_date": "2026-08-31",
        "components": {
            "adj_factor_restatement_authority": {
                "path": "components/position_timing_source_authority_v1/adj_factor_restatement_authority.json",
                "sha256": _sha(authority_path.read_bytes()),
                "size": authority_path.stat().st_size,
            }
        },
    }
    manifest["dataset_manifest_sha256"] = canonical_sha256(manifest)
    manifest_path = root / "qe_dataset_manifest.json"
    manifest_path.write_bytes(canonical_json_bytes(manifest))
    return root, _sha(manifest_path.read_bytes()), authority["canonical_sha256"]


class _Candidate:
    def __init__(self, root: Path):
        self.root = root
        self.calendar = pd.DatetimeIndex(
            pd.to_datetime(
                [
                    "2026-07-03",
                    "2026-07-06",
                    "2026-07-08",
                    "2026-07-09",
                    "2026-08-31",
                ]
            )
        )
        self._factors = {
            "300506.SZ": [0.5, 0.5, float("nan"), float("nan"), 1.0],
            "688109.SH": [float("nan"), float("nan"), 0.5, 0.5, 1.0],
        }

    def bars(self, symbol: str) -> pd.DataFrame:
        return pd.DataFrame({"factor": self._factors[symbol]}, index=self.calendar)


def test_reader_and_candidate_factor_audit_are_hash_bound(tmp_path: Path):
    root, manifest_sha, authority_sha = _authority_candidate(tmp_path)
    authority = open_adj_factor_restatement_authority(
        candidate_root=root,
        expected_candidate_manifest_sha256=manifest_sha,
        expected_authority_canonical_sha256=authority_sha,
    )
    audit = audit_candidate_adj_factor_restatement(_Candidate(root), authority)

    assert authority.candidate_revision == "test-r5"
    assert [series.symbol for series in authority.series] == [
        "300506.SZ",
        "688109.SH",
    ]
    assert audit["coverage_complete"] is True
    assert audit["candidate_factor_dates_without_authority_count"] == 0
    assert audit["normalized_factor_mismatch_count"] == 0
    assert audit["required_stable_seam_count"] == 2
    assert audit["factor_account_participation_inference"] is False
    assert audit["outcomes_read"] is False
    assert audit["audit_sha256"] == canonical_sha256(
        {key: value for key, value in audit.items() if key != "audit_sha256"}
    )


def test_reader_rejects_tampered_series_identity(tmp_path: Path):
    root, manifest_sha, authority_sha = _authority_candidate(tmp_path)
    authority_path = (
        root
        / "components"
        / "position_timing_source_authority_v1"
        / "adj_factor_restatement_authority.json"
    )
    payload = json.loads(authority_path.read_text(encoding="utf-8"))
    payload["series"][0]["rows"][0]["adj_factor"] = 9.0
    authority_path.write_bytes(canonical_json_bytes(payload))

    with pytest.raises(ActionValueError, match="MANIFEST_IDENTITY_MISMATCH"):
        open_adj_factor_restatement_authority(
            candidate_root=root,
            expected_candidate_manifest_sha256=manifest_sha,
            expected_authority_canonical_sha256=authority_sha,
        )


def test_reader_rejects_incomplete_authority_series_set(tmp_path: Path):
    root, _, _ = _authority_candidate(tmp_path)
    authority_path = (
        root
        / "components"
        / "position_timing_source_authority_v1"
        / "adj_factor_restatement_authority.json"
    )
    payload = json.loads(authority_path.read_text(encoding="utf-8"))
    payload["series"] = payload["series"][:1]
    payload["canonical_sha256"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "canonical_sha256"}
    )
    authority_path.write_bytes(canonical_json_bytes(payload))
    manifest_path = root / "qe_dataset_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    component = manifest["components"]["adj_factor_restatement_authority"]
    component["sha256"] = _sha(authority_path.read_bytes())
    component["size"] = authority_path.stat().st_size
    manifest["dataset_manifest_sha256"] = canonical_sha256(
        {
            key: value
            for key, value in manifest.items()
            if key != "dataset_manifest_sha256"
        }
    )
    manifest_path.write_bytes(canonical_json_bytes(manifest))

    with pytest.raises(ActionValueError, match="SERIES_SET_INVALID"):
        open_adj_factor_restatement_authority(
            candidate_root=root,
            expected_candidate_manifest_sha256=_sha(manifest_path.read_bytes()),
            expected_authority_canonical_sha256=payload["canonical_sha256"],
        )


def test_audit_rejects_factor_mismatch(tmp_path: Path):
    root, manifest_sha, authority_sha = _authority_candidate(tmp_path)
    authority = open_adj_factor_restatement_authority(
        candidate_root=root,
        expected_candidate_manifest_sha256=manifest_sha,
        expected_authority_canonical_sha256=authority_sha,
    )
    candidate = _Candidate(root)
    candidate._factors["300506.SZ"][-1] = 0.9

    with pytest.raises(ActionValueError, match="COVERAGE_INCOMPLETE"):
        audit_candidate_adj_factor_restatement(candidate, authority)


def test_audit_rejects_candidate_factor_date_without_authority(tmp_path: Path):
    root, manifest_sha, authority_sha = _authority_candidate(tmp_path)
    authority = open_adj_factor_restatement_authority(
        candidate_root=root,
        expected_candidate_manifest_sha256=manifest_sha,
        expected_authority_canonical_sha256=authority_sha,
    )
    candidate = _Candidate(root)
    candidate._factors["300506.SZ"][2] = 0.5

    with pytest.raises(ActionValueError, match="COVERAGE_INCOMPLETE"):
        audit_candidate_adj_factor_restatement(candidate, authority)


def _source_revision_request_contract() -> dict:
    declared_series = [
        {
            "symbol": "300506.SZ",
            "start": "2016-03-24",
            "end": "2026-08-31",
            "row_count": 2537,
            "ordered_rows_sha256": "1" * 64,
        },
        {
            "symbol": "688109.SH",
            "start": "2021-03-30",
            "end": "2026-08-31",
            "row_count": 1316,
            "ordered_rows_sha256": "2" * 64,
        },
    ]
    audit = {
        "schema_version": "position_timing_adj_factor_restatement_application_audit_v1",
        "candidate_manifest_sha256": "a" * 64,
        "candidate_dataset_manifest_sha256": "b" * 64,
        "candidate_revision": "test-r5",
        "authority_file_sha256": "c" * 64,
        "authority_canonical_sha256": "d" * 64,
        "diagnosis_sha256": "e" * 64,
        "normalized_factor_abs_tolerance": "0.000001",
        "series": [
            {
                **item,
                "authority_row_count": item["row_count"],
                "candidate_finite_factor_count": 10,
                "matched_candidate_factor_count": 10,
                "candidate_factor_dates_without_authority": [],
                "authority_dates_without_candidate_factor_count": 0,
                "max_normalized_factor_abs_error": "0",
                "stable_seam": {},
            }
            for item in declared_series
        ],
        "series_count": 2,
        "candidate_factor_dates_without_authority_count": 0,
        "normalized_factor_mismatch_count": 0,
        "required_stable_seam_count": 2,
        "coverage_complete": True,
        "factor_account_participation_inference": False,
        "outcomes_read": False,
        "database_read": False,
        "database_write": False,
        "candidate_write": False,
        "runtime_action_performed": False,
    }
    for item in audit["series"]:
        item.pop("row_count")
    audit["audit_sha256"] = canonical_sha256(audit)
    request = {
        "candidate_selection_authority": SOURCE_REVISION_SELECTION_AUTHORITY,
        "training_symbols": ["000001.SZ"],
        "evaluation_symbols": ["000002.SZ"],
        "snapshot_symbols": ["000001.SZ", "000002.SZ"],
        "population_spec": {"start": "2018-08-01", "end": "2026-08-31"},
        "prior_request_identity": {"aggregate_sha256": "f" * 64},
        "candidate_manifest_sha256": "a" * 64,
        "candidate_dataset_manifest_sha256": "b" * 64,
        "candidate_revision": "test-r5",
        "superseded_request": None,
        "superseded_request_sha256": None,
        "superseded_request_lineage_sha256s": None,
        "supersession_reason": None,
        "adj_factor_restatement_authority": {
            "path": "candidate/adj_factor_restatement_authority.json",
            "sha256": "c" * 64,
        },
        "adj_factor_restatement_authority_canonical_sha256": "d" * 64,
        "adj_factor_restatement_diagnosis_sha256": "e" * 64,
        "adj_factor_restatement_series": declared_series,
        "adj_factor_restatement_audit": audit,
        "adj_factor_restatement_audit_sha256": audit["audit_sha256"],
    }
    binding = {
        "schema_version": "position_timing_pattern_source_revision_binding_v1",
        "reason": SOURCE_REVISION_REASON,
        "source_request": {"path": "source/request.json", "sha256": "3" * 64},
        "source_request_sha256": "4" * 64,
        "source_bundle_manifest": {
            "path": "source/bundle/manifest.json",
            "sha256": "5" * 64,
        },
        "source_bundle_manifest_sha256": "6" * 64,
        "source_candidate_manifest_sha256": "7" * 64,
        "source_candidate_dataset_manifest_sha256": "8" * 64,
        "source_candidate_revision": "test-r4",
        "frozen_population_sha256": canonical_sha256(
            {
                "training_symbols": request["training_symbols"],
                "evaluation_symbols": request["evaluation_symbols"],
                "snapshot_symbols": request["snapshot_symbols"],
                "population_spec": request["population_spec"],
                "prior_request_identity": request["prior_request_identity"],
            }
        ),
        "source_result_used_only_for_technical_lineage_validation": True,
        "new_hypothesis_or_population_selection": False,
    }
    binding["binding_sha256"] = canonical_sha256(binding)
    request["source_revision_binding"] = binding
    request["source_revision_binding_sha256"] = binding["binding_sha256"]
    return request


def test_source_revision_contract_binds_population_and_restatement_audit():
    request = _source_revision_request_contract()

    assert _source_revision_request_contract_invalid(request) is False
    request["evaluation_symbols"] = ["000003.SZ"]
    assert _source_revision_request_contract_invalid(request) is True


def test_source_revision_contract_rejects_missing_authority():
    request = _source_revision_request_contract()
    request.pop("adj_factor_restatement_authority")

    assert _source_revision_request_contract_invalid(request) is True
