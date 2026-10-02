from __future__ import annotations

from datetime import date

import pytest

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.shared_sector_context import (
    build_release_sw_l2_code_map_payload,
    validate_release_sw_l2_code_map,
)
from backend.services.dataset_release import sw_l2_quote_policy as policy


def _code_map(codes: list[str]):  # type: ignore[no-untyped-def]
    return validate_release_sw_l2_code_map(
        build_release_sw_l2_code_map_payload(
            code_to_id={code: index for index, code in enumerate(codes)},
            member_backed_codes=codes,
            authority_id="fixture",
            authority_sha256="a" * 64,
        )
    )


def test_quote_policy_clips_stopped_and_never_published_codes(monkeypatch) -> None:
    codes = [f"801{index:03d}.SI" for index in range(131)]
    monkeypatch.setattr(policy, "_NEVER_QUOTED", frozenset({codes[0]}))
    monkeypatch.setattr(policy, "_LAST_PUBLISHED", {codes[1]: date(2026, 4, 28)})
    monkeypatch.setattr(
        policy,
        "SW2021_L2_TAXONOMY_DIGEST",
        policy.taxonomy_digest(codes),
    )

    payload = policy.build_quote_availability_payload(
        code_map=_code_map(codes),
        required_start=date(2024, 7, 1),
        cutoff=date(2026, 9, 30),
    )

    by_code = {
        entry["canonical_l2_code"]: entry["availability_spans"]
        for entry in payload["entries"]
    }
    assert by_code[codes[0]] == []
    assert by_code[codes[1]][0]["end_date"] == "2026-04-28"
    assert by_code[codes[2]][0] == {
        "start_date": "2024-07-01",
        "end_date": "2026-09-30",
    }


def test_quote_policy_fails_closed_on_taxonomy_drift(monkeypatch) -> None:
    codes = [f"801{index:03d}.SI" for index in range(131)]
    monkeypatch.setattr(policy, "_NEVER_QUOTED", frozenset())
    monkeypatch.setattr(policy, "_LAST_PUBLISHED", {})
    monkeypatch.setattr(
        policy,
        "SW2021_L2_TAXONOMY_DIGEST",
        digest_named_fields("different", {"ordered_codes": codes}),
    )

    with pytest.raises(policy.SWL2QuotePolicyError, match="taxonomy differs"):
        policy.build_quote_availability_payload(
            code_map=_code_map(codes),
            required_start=date(2024, 7, 1),
            cutoff=date(2026, 9, 30),
        )
