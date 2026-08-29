from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date
from pathlib import Path, PureWindowsPath
from types import SimpleNamespace

import pytest

import backend.services.dataset_release.sector_data_candidate_source as candidate_source_module
from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields
from backend.services.dataset_release.pit import FrozenPitSnapshot, FrozenPitSpan
from backend.services.dataset_release.sector_data_candidate_source import (
    SectorCandidateSource,
    SectorCandidateSourceError,
    sector_candidate_artifact_root,
)
from backend.services.sector_data_builder import (
    MONEYFLOW_FIELDS,
    SECTOR_DATA_ASSIGNMENT_SCHEMA,
    SECTOR_DATA_FACT_SCHEMA,
    SECTOR_DATA_OPPORTUNITY_SCHEMA,
    SW_DAILY_FIELDS,
    SectorDataCandidateDay,
    read_sector_data_candidate,
    write_sector_data_candidate,
)


TRADE_DATE = date(2026, 7, 31)
CLASSIFICATION_RECEIPT = "1" * 64
INDEX_RECEIPT = "2" * 64
DENOMINATOR_DIGEST = "3" * 64
BUNDLE_HASH = "4" * 64


def _row(schema: str, payload: dict) -> dict:
    return {**payload, "row_hash": digest_named_fields(schema, payload)}


def _resolved_projection(*, receipt: str, l2_code: str, identity_hash: str) -> dict:
    return {
        "status": "resolved",
        "identity_codes": {"l1_code": "220000", "l2_code": l2_code, "l3_code": "220315"},
        "identity_hash": identity_hash,
        "authority_identity": {"classification_l2_code": l2_code},
        "valid_from": "2021-07-30",
        "valid_to_exclusive": None,
        "known_from": "2021-08-02",
        "taxonomy_contract_id": "sw_2021",
        "taxonomy_version": "2021",
        "candidate_row_hashes": ["5" * 64],
        "authority_receipt_hash": receipt,
        "non_as_known_taxonomy": False,
        "resolution_hash": "6" * 64,
    }


def _unavailable_projection(*, receipt: str) -> dict:
    return {
        "status": "unavailable",
        "reason": "membership_boundary_unavailable",
        "authority_receipt_hash": receipt,
        "conflict_candidates": [],
    }


def _opportunity_digest(symbols: tuple[str, ...]) -> str:
    digest = hashlib.sha256()
    for symbol in symbols:
        digest.update(
            canonical_json_bytes(
                {
                    "schema_version": SECTOR_DATA_OPPORTUNITY_SCHEMA,
                    "trade_date": TRADE_DATE.isoformat(),
                    "canonical_symbol": symbol,
                }
            )
            + b"\n"
        )
    return digest.hexdigest()


def _write_candidate(profile, *, symbols: tuple[str, ...] = ("000001.SZ", "000002.SZ")) -> Path:
    root = sector_candidate_artifact_root(profile, cutoff=TRADE_DATE)
    root.parent.mkdir(parents=True, exist_ok=True)
    sw_daily = {field: str(index + 1) for index, field in enumerate(SW_DAILY_FIELDS)}
    moneyflow = {field: str(index + 11) for index, field in enumerate(MONEYFLOW_FIELDS)}
    fact = _row(
        SECTOR_DATA_FACT_SCHEMA,
        {
            "schema_version": SECTOR_DATA_FACT_SCHEMA,
            "trade_date": TRADE_DATE.isoformat(),
            "classification_l2_code": "220300",
            "classification_l2_identity_hash": "7" * 64,
            "index_l2_code": "801030.SI",
            "index_l2_identity_hash": "8" * 64,
            "classification_authority_receipt_hash": CLASSIFICATION_RECEIPT,
            "index_membership_authority_receipt_hash": INDEX_RECEIPT,
            "sw_daily": sw_daily,
            "moneyflow_aggregate": moneyflow,
            "contributor_coverage": {"expected": 1, "resolved": 1, "ratio": "1"},
        },
    )
    resolved = _row(
        SECTOR_DATA_ASSIGNMENT_SCHEMA,
        {
            "schema_version": SECTOR_DATA_ASSIGNMENT_SCHEMA,
            "canonical_symbol": symbols[0],
            "trade_date": TRADE_DATE.isoformat(),
            "status": "resolved",
            "alignment_state": "aligned",
            "classification": _resolved_projection(
                receipt=CLASSIFICATION_RECEIPT,
                l2_code="220300",
                identity_hash="9" * 64,
            ),
            "index_membership": _resolved_projection(
                receipt=INDEX_RECEIPT,
                l2_code="220300",
                identity_hash="a" * 64,
            ),
            "sector_fact_row_hash": fact["row_hash"],
            "unavailable_reasons": [],
        },
    )
    assignments = [resolved]
    if len(symbols) == 2:
        assignments.append(
            _row(
                SECTOR_DATA_ASSIGNMENT_SCHEMA,
                {
                    "schema_version": SECTOR_DATA_ASSIGNMENT_SCHEMA,
                    "canonical_symbol": symbols[1],
                    "trade_date": TRADE_DATE.isoformat(),
                    "status": "unavailable",
                    "alignment_state": "unavailable",
                    "classification": _unavailable_projection(receipt=CLASSIFICATION_RECEIPT),
                    "index_membership": _unavailable_projection(receipt=INDEX_RECEIPT),
                    "sector_fact_row_hash": None,
                    "unavailable_reasons": [
                        "classification:membership_boundary_unavailable",
                        "index_membership:membership_boundary_unavailable",
                    ],
                },
            )
        )
    receipt = SimpleNamespace(
        denominator_digest=DENOMINATOR_DIGEST,
        frozen_denominator=len(symbols),
        receipt_hash=CLASSIFICATION_RECEIPT,
    )
    index_receipt = SimpleNamespace(
        denominator_digest=DENOMINATOR_DIGEST,
        frozen_denominator=len(symbols),
        receipt_hash=INDEX_RECEIPT,
    )
    write_sector_data_candidate(
        artifact_root=root,
        forbidden_roots=(),
        authority_bundle=SimpleNamespace(
            classification_receipt=receipt,
            index_membership_receipt=index_receipt,
            manifest={"bundle_hash": BUNDLE_HASH},
        ),
        days=(
            SectorDataCandidateDay(
                trade_date=TRADE_DATE,
                assignments=tuple(assignments),
                sector_facts=(fact,),
            ),
        ),
        expected_opportunities=len(symbols),
        expected_opportunity_digest=_opportunity_digest(symbols),
        candidate_scope="full",
        producer_commit="b" * 40,
        producer_tree="c" * 40,
    )
    return root


def _profile(dataset_profile, tmp_path: Path):
    allowed = tmp_path / "AIstock_candidates"
    allowed.mkdir()
    return replace(
        dataset_profile,
        profile="qe_hmm_full_v2",
        start_date=TRADE_DATE,
        minute_start_date=TRADE_DATE,
        candidate_root=PureWindowsPath(str(allowed)),
        candidate_root_id="fixture-sector-candidate-root",
    )


def _pit(symbols: tuple[str, ...]) -> FrozenPitSnapshot:
    return FrozenPitSnapshot(
        universe_key="aistock_equity_pit_canonical_v2",
        rule_version="shsz_a_252td_st_delist_asof_v2",
        scope_start=TRADE_DATE,
        cutoff=TRADE_DATE,
        state_identity="d" * 64,
        source_fingerprint_sha256="e" * 64,
        parameter_hash="f" * 64,
        spans_sha256="0" * 64,
        spans=tuple(
            FrozenPitSpan(symbol, TRADE_DATE, TRADE_DATE, "listed", "scope_end")
            for symbol in symbols
        ),
    )


def test_p3a_candidate_source_proves_denominator_and_streams_only_resolved_rows(
    tmp_path: Path,
    dataset_profile,
) -> None:
    profile = _profile(dataset_profile, tmp_path)
    root = _write_candidate(profile)
    source = SectorCandidateSource.load(
        profile,
        cutoff=TRADE_DATE,
        pit_snapshot=_pit(("000001.SZ", "000002.SZ")),
        trading_dates=(TRADE_DATE,),
    )

    rows = list(
        source.iter_rows(
            start=TRADE_DATE,
            end=TRADE_DATE,
            l2_code_map={"801030.SI": 17},
        )
    )

    assert source.artifact_root == root.resolve()
    assert source.query_version.endswith(source.candidate_hash)
    assert len(rows) == 1
    assert rows[0]["ts_code"] == "000001.SZ"
    assert rows[0]["l2_code_id"] == 17
    assert rows[0]["sw2_close"] == "4"
    assert rows[0]["sw2_mf_net_amt"] == str(tuple(MONEYFLOW_FIELDS).index("net_mf_amount") + 11)
    receipt = source.receipt(code_map_digest="1" * 64, classify_partitions=[])
    assert receipt["candidate_hash"] == source.candidate_hash
    assert receipt["status_counts"] == {"resolved": 1, "unavailable": 1}
    assert receipt["unavailable_by_reason"]
    assert receipt["safety"]["candidate_writes"] == 0
    with pytest.raises(SectorCandidateSourceError, match="index L2 code is absent"):
        list(
            source.iter_rows(
                start=TRADE_DATE,
                end=TRADE_DATE,
                l2_code_map={"220300": 17},
            )
        )


def test_p3a_candidate_source_fails_closed_on_frozen_denominator_drift(
    tmp_path: Path,
    dataset_profile,
) -> None:
    profile = _profile(dataset_profile, tmp_path)
    _write_candidate(profile)

    with pytest.raises(SectorCandidateSourceError, match="denominator/opportunity identity differs"):
        SectorCandidateSource.load(
            profile,
            cutoff=TRADE_DATE,
            pit_snapshot=_pit(("000001.SZ",)),
            trading_dates=(TRADE_DATE,),
        )


def test_p3a_candidate_source_requires_the_deterministic_allowlisted_path(
    tmp_path: Path,
    dataset_profile,
) -> None:
    profile = _profile(dataset_profile, tmp_path)
    root = _write_candidate(profile)
    moved = root.parent / "arbitrary-copy"
    root.rename(moved)

    with pytest.raises(SectorCandidateSourceError, match="deterministic path is unavailable"):
        SectorCandidateSource.load(
            profile,
            cutoff=TRADE_DATE,
            pit_snapshot=_pit(("000001.SZ", "000002.SZ")),
            trading_dates=(TRADE_DATE,),
        )


def test_p3a_candidate_source_recomputes_assignment_report_aggregates(
    tmp_path: Path,
    dataset_profile,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(dataset_profile, tmp_path)
    root = _write_candidate(profile)
    readback = read_sector_data_candidate(artifact_root=root, forbidden_roots=())
    report = dict(readback.report)
    report["status_counts"] = {"resolved": 2}
    inconsistent = replace(readback, report=report)
    monkeypatch.setattr(
        candidate_source_module,
        "read_sector_data_candidate",
        lambda **_kwargs: inconsistent,
    )

    with pytest.raises(SectorCandidateSourceError, match="assignment/report aggregate readback"):
        SectorCandidateSource.load(
            profile,
            cutoff=TRADE_DATE,
            pit_snapshot=_pit(("000001.SZ", "000002.SZ")),
            trading_dates=(TRADE_DATE,),
        )
