"""Release-neutral publication policy for canonical SW2021 L2 quotes.

This is not an HMM policy.  It records the publication boundary of the shared
SW L2 quote source.  A monthly release clips the formal spans to its requested
consumer window and cutoff.  Taxonomy drift is rejected instead of silently
assigning the default publication rule to a new or renamed industry.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from .canonical import digest_named_fields
from .shared_sector_context import (
    SECTOR_QUOTE_AVAILABILITY_SCHEMA,
    ReleaseSWL2CodeMap,
    validate_sector_quote_availability,
)


QUOTE_PUBLICATION_POLICY_SCHEMA = "aistock_sw_l2_quote_publication_policy_v1"
SW2021_L2_TAXONOMY_DIGEST = (
    "ce01d8dac21b52ad9852d11f21225bb063964962b0cdaef580d75e7b396abb74"
)

# 801011.SI is a taxonomy/catalog identity without a published quote series in
# the approved shared authority.  The six dated rows stopped publication after
# 2026-04-28; their stock membership and money-flow semantics remain valid.
_NEVER_QUOTED = frozenset({"801011.SI"})
_LAST_PUBLISHED: Mapping[str, date] = {
    "801019.SI": date(2026, 4, 28),
    "801117.SI": date(2026, 4, 28),
    "801207.SI": date(2026, 4, 28),
    "801216.SI": date(2026, 4, 28),
    "801961.SI": date(2026, 4, 28),
    "801983.SI": date(2026, 4, 28),
}


class SWL2QuotePolicyError(RuntimeError):
    """The frozen taxonomy cannot be bound to the publication policy."""


def taxonomy_digest(ordered_codes: Sequence[str]) -> str:
    normalized = sorted(str(value).strip().upper() for value in ordered_codes)
    if not normalized or len(normalized) != len(set(normalized)):
        raise SWL2QuotePolicyError("SW L2 taxonomy is empty or duplicated")
    return digest_named_fields(
        "aistock_sw2021_l2_taxonomy_catalog_v1",
        {"ordered_codes": normalized},
    )


def quote_publication_policy_identity() -> str:
    return digest_named_fields(
        QUOTE_PUBLICATION_POLICY_SCHEMA,
        {
            "taxonomy_digest": SW2021_L2_TAXONOMY_DIGEST,
            "never_quoted": sorted(_NEVER_QUOTED),
            "last_published": {
                code: value.isoformat() for code, value in sorted(_LAST_PUBLISHED.items())
            },
            "default_rule": "available_through_release_cutoff_v1",
        },
    )


def build_quote_availability_payload(
    *,
    code_map: ReleaseSWL2CodeMap,
    required_start: date,
    cutoff: date,
) -> dict[str, Any]:
    """Clip the approved publication authority to one release window."""

    if cutoff < required_start:
        raise SWL2QuotePolicyError("quote availability window is inverted")
    ordered_codes = sorted(code_map.code_to_id)
    observed_digest = taxonomy_digest(ordered_codes)
    if observed_digest != SW2021_L2_TAXONOMY_DIGEST:
        raise SWL2QuotePolicyError(
            "SW L2 taxonomy differs from the approved quote policy: "
            f"expected={SW2021_L2_TAXONOMY_DIGEST} actual={observed_digest}"
        )
    exceptions = _NEVER_QUOTED | set(_LAST_PUBLISHED)
    if not exceptions.issubset(ordered_codes):
        raise SWL2QuotePolicyError("quote policy exceptions are absent from the taxonomy")

    entries: list[dict[str, Any]] = []
    for code in ordered_codes:
        spans: list[dict[str, str]] = []
        if code not in _NEVER_QUOTED:
            end = min(cutoff, _LAST_PUBLISHED.get(code, cutoff))
            if required_start <= end:
                spans.append(
                    {
                        "start_date": required_start.isoformat(),
                        "end_date": end.isoformat(),
                    }
                )
        entries.append(
            {"canonical_l2_code": code, "availability_spans": spans}
        )
    authority = dict(code_map.mapping_authority)
    payload = {
        "schema_version": SECTOR_QUOTE_AVAILABILITY_SCHEMA,
        "mapping_authority": authority,
        "entries": entries,
        "quote_availability_digest": digest_named_fields(
            SECTOR_QUOTE_AVAILABILITY_SCHEMA,
            {"mapping_authority": authority, "entries": entries},
        ),
    }
    validate_sector_quote_availability(
        payload,
        code_map=code_map,
        required_end=cutoff,
    )
    return payload


__all__ = (
    "QUOTE_PUBLICATION_POLICY_SCHEMA",
    "SW2021_L2_TAXONOMY_DIGEST",
    "SWL2QuotePolicyError",
    "build_quote_availability_payload",
    "quote_publication_policy_identity",
    "taxonomy_digest",
)
