from __future__ import annotations

from datetime import date

import pytest

from backend.services.dataset_release import candidate_validator
from backend.services.dataset_release.candidate_validator import CandidateValidationError


def _candidate_root(tmp_path):
    root = tmp_path / "candidate"
    root.mkdir()
    for relative in candidate_validator._COMPONENT_CANDIDATE_ROOT.values():
        (root / relative).mkdir()
    return root


def test_candidate_root_namespace_accepts_only_four_components_and_bound_metadata(tmp_path) -> None:
    root = _candidate_root(tmp_path)
    metadata = root / "metadata"
    metadata.mkdir()
    (metadata / "index_context_manifest.json").write_text("{}", encoding="utf-8")

    candidate_validator._validate_candidate_root_namespace(root)

    (root / "unbound.json").write_text("{}", encoding="utf-8")
    with pytest.raises(CandidateValidationError, match="root namespace differs"):
        candidate_validator._validate_candidate_root_namespace(root)


def test_instrument_rows_preserve_disjoint_pit_spans_and_reject_overlap(tmp_path) -> None:
    path = tmp_path / "all.txt"
    path.write_text(
        "000001.SZ\t2018-08-01\t2020-01-01\n000001.SZ\t2021-01-01\t2026-08-31\n",
        encoding="utf-8",
    )
    assert candidate_validator._parse_instrument_rows(path, allow_multiple=True) == {
        "000001.SZ": [
            (date(2018, 8, 1), date(2020, 1, 1)),
            (date(2021, 1, 1), date(2026, 8, 31)),
        ]
    }

    path.write_text(
        "000001.SZ\t2018-08-01\t2020-01-01\n000001.SZ\t2019-01-01\t2026-08-31\n",
        encoding="utf-8",
    )
    with pytest.raises(CandidateValidationError, match="spans overlap"):
        candidate_validator._parse_instrument_rows(path, allow_multiple=True)


def test_minute_overlay_contract_is_complete_nonmutating_and_serial() -> None:
    evidence = candidate_validator._validate_minute_overlay(
        {
            "source_policy": "tdx_then_tushare_missing_keys_conflict_fail_v1",
            "database_rows": 240,
            "overlay_rows": 240,
            "synthesized_suspend_rows": 240,
            "provider_concurrency": 1,
            "missing_keys": 0,
            "duplicate_keys": 0,
            "overlap_mismatch_cells": 0,
            "database_writes": 0,
            "production_writes": 0,
        }
    )
    assert evidence["source_rows"] == 720
    assert evidence["provider_concurrency"] == 1

    with pytest.raises(CandidateValidationError, match="concurrency"):
        candidate_validator._validate_minute_overlay({**evidence, "provider_concurrency": 2})
