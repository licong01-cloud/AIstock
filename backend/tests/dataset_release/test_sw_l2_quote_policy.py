from __future__ import annotations

from datetime import date

import pytest

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.shared_sector_context import (
    build_release_sw_l2_code_map_payload,
    validate_release_sw_l2_code_map,
)
from backend.services.dataset_release import sw_l2_quote_policy as policy


@pytest.fixture
def inputs(monkeypatch):
    codes = ["801011.SI", "801012.SI"] + [f"801{index:03d}.SI" for index in range(1000)
                                           if index not in (11, 12)][:129]
    monkeypatch.setattr(policy, "SW2021_L2_TAXONOMY_DIGEST", policy.taxonomy_digest(codes))
    mapping = _code_map(codes)
    return dict(code_map=mapping, required_start=date(2024, 7, 1), cutoff=date(2026, 9, 30),
                classification_rows=[{"index_code": code, "level": "L2", "src": "SW2021", "is_pub": str(int(index != 0))}
                                     for index, code in enumerate(codes)])


def _observation(day="2024-07-01", **fields):
    return {"trade_date": day, "l2_code_id": 0, "sw2_pct_change": 3.48,
            "sw2_vol": 8175.6, "sw2_amount": 16559.9817, **fields}


def _spans(payload):
    return {row["canonical_l2_code"]: row["availability_spans"] for row in payload["entries"]}


def test_nonpublished_catalog_preserves_dated_quote_without_extrapolation(inputs):
    payload = policy.build_quote_availability_payload(**inputs, quote_observations=[_observation()])
    spans = _spans(payload)
    assert spans["801011.SI"] == [{"start_date": "2024-07-01", "end_date": "2024-07-01"}]
    assert spans["801012.SI"] == [{"start_date": "2024-07-01", "end_date": "2026-09-30"}]
    assert set(payload) == {"schema_version", "mapping_authority", "entries", "quote_availability_digest"}


def test_nonpublication_requires_catalog_not_inferred_from_quote_na(inputs):
    rows = [_observation("2026-09-30", sw2_pct_change=None, sw2_vol=None, sw2_amount=None)]
    assert _spans(policy.build_quote_availability_payload(**inputs, quote_observations=rows))["801011.SI"] == []
    inputs["classification_rows"][0]["is_pub"] = "1"
    assert _spans(policy.build_quote_availability_payload(**inputs, quote_observations=rows))["801011.SI"] == [
        {"start_date": "2024-07-01", "end_date": "2026-09-30"}]


@pytest.mark.parametrize("fields", [{"sw2_vol": None}, {"sw2_vol": float("inf")},
                                   {"sw2_amount": float("-inf")}, {"sw2_amount": "bad"}])
def test_partial_or_invalid_nonpublished_quotes_remain_errors(inputs, fields):
    with pytest.raises(policy.SWL2QuotePolicyError, match="quote observation"):
        policy.build_quote_availability_payload(**inputs, quote_observations=[_observation(**fields)])


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unknown_flag", "wrong_version", "wrong_level"])
def test_publication_scope_requires_unambiguous_versioned_catalog(inputs, mutation):
    rows = inputs["classification_rows"]
    if mutation == "missing":
        rows.pop()
    elif mutation == "duplicate":
        rows.append(dict(rows[0]))
    else:
        rows[0][{"unknown_flag": "is_pub", "wrong_version": "src", "wrong_level": "level"}[mutation]] = "invalid"
    with pytest.raises(policy.SWL2QuotePolicyError, match="publication catalog"):
        policy.build_quote_availability_payload(**inputs, quote_observations=[])


def test_quotes_are_point_evidence_not_last_seen_or_membership_stop_date(inputs):
    rows = [_observation("2026-04-28"), _observation("2026-04-30"), _observation("2026-04-28")]
    payload = policy.build_quote_availability_payload(**inputs, quote_observations=iter(rows))
    assert _spans(payload)["801011.SI"] == [
        {"start_date": "2026-04-28", "end_date": "2026-04-28"},
        {"start_date": "2026-04-30", "end_date": "2026-04-30"}]
    assert payload == policy.build_quote_availability_payload(**inputs, quote_observations=reversed(rows))


def _code_map(codes: list[str]):  # type: ignore[no-untyped-def]
    return validate_release_sw_l2_code_map(
        build_release_sw_l2_code_map_payload(
            code_to_id={code: index for index, code in enumerate(codes)},
            member_backed_codes=sorted(codes),
            authority_id="fixture",
            authority_sha256="a" * 64,
        )
    )


def test_quote_policy_fails_closed_on_taxonomy_drift(inputs, monkeypatch) -> None:
    monkeypatch.setattr(
        policy,
        "SW2021_L2_TAXONOMY_DIGEST",
        digest_named_fields("different", {"ordered_codes": []}),
    )

    with pytest.raises(policy.SWL2QuotePolicyError, match="taxonomy differs"):
        policy.build_quote_availability_payload(
            **inputs, quote_observations=[],
        )


def test_window_and_evidence_identity(inputs):
    evidence = {}
    policy.build_quote_availability_payload(**inputs, quote_observations=[_observation()], evidence=evidence)
    assert evidence["nonpublished_catalog_codes"] == ["801011.SI"]
    assert evidence["dated_observation_count"] == 1
    assert evidence["historical_stop_date_inferred"] is False
    with pytest.raises(policy.SWL2QuotePolicyError, match="inverted"):
        policy.build_quote_availability_payload(**{**inputs, "required_start": date(2027, 1, 1)}, quote_observations=[])
