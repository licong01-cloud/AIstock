"""Release-neutral publication policy for canonical SW2021 L2 quotes.

This is not an HMM policy.  It records the publication boundary of the shared
SW L2 quote source.  A monthly release clips the formal spans to its requested
consumer window and cutoff.  Taxonomy drift is rejected instead of silently
assigning the default publication rule to a new or renamed industry.
"""

from __future__ import annotations

from datetime import date, timedelta
import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .canonical import digest_named_fields
from .shared_sector_context import (
    SECTOR_QUOTE_AVAILABILITY_SCHEMA,
    ReleaseSWL2CodeMap,
    validate_sector_quote_availability,
)


QUOTE_PUBLICATION_POLICY_SCHEMA = "aistock_sw_l2_quote_publication_policy_v2"
SW2021_L2_TAXONOMY_DIGEST = (
    "ce01d8dac21b52ad9852d11f21225bb063964962b0cdaef580d75e7b396abb74"
)

# The provider's version-qualified publication catalog, not stock membership,
# determines which quotes are required. The SW2021 table explicitly lists
# 801011 as unpublished. Authentic historical sw_daily observations still
# prove availability ON THOSE DATES; neither fact supplies a stop/start date.
PUBLICATION_RULE_URL = "https://tushare.pro/document/2?doc_id=181"
_QUOTE_FIELDS = ("sw2_pct_change", "sw2_vol", "sw2_amount")


class SWL2QuotePolicyError(RuntimeError):
    """The frozen taxonomy cannot be bound to the publication policy."""


def publication_quote_observations(path: Path, *, chunk_rows: int):
    """Read the existing small quote projection, not historical stock prices.

    Private preparation may pass its already materialized sector facts. Final
    BUILD uses sector_quote_facts, which it already has to produce exactly once.
    This is a projection of positive evidence, not a historical quality audit.
    """
    import pandas as pd

    before = path.stat()
    with pd.HDFStore(path, mode="r") as store:
        for frame in store.select("data", columns=["l2_code_id", *_QUOTE_FIELDS], chunksize=chunk_rows):
            for row in frame.reset_index().itertuples(index=False):
                yield {"trade_date": row.datetime, "l2_code_id": row.l2_code_id,
                       **{key: getattr(row, key) for key in _QUOTE_FIELDS}}
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns
    ):
        raise SWL2QuotePolicyError("frozen quote observation file changed during read")


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
            "publication_catalog": "frozen_index_classify_SW2021_is_pub",
            "publication_rule_url": PUBLICATION_RULE_URL,
            "nonpublished_rule": "retain_dated_observations_without_extrapolation_v1",
        },
    )


def build_quote_availability_payload(
    *,
    code_map: ReleaseSWL2CodeMap,
    required_start: date,
    cutoff: date,
    classification_rows: Iterable[Mapping[str, Any]],
    quote_observations: Iterable[Mapping[str, Any]],
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Bind publication scope and dated facts without inventing a stop date.

    is_pub=0 is a provider/version contract, NOT a backdated event or an
    inference from membership/NaN. Dated complete quote observations remain
    usable even for a nonpublished catalog entry, but cannot license its next
    day/month. A partial observation never becomes an absence exemption.
    """

    if cutoff < required_start:
        raise SWL2QuotePolicyError("quote availability window is inverted")
    ordered_codes = sorted(code_map.code_to_id)
    observed_digest = taxonomy_digest(ordered_codes)
    if observed_digest != SW2021_L2_TAXONOMY_DIGEST:
        raise SWL2QuotePolicyError(
            "SW L2 taxonomy differs from the approved quote policy: "
            f"expected={SW2021_L2_TAXONOMY_DIGEST} actual={observed_digest}"
        )
    catalog: dict[str, str] = {}
    for row in classification_rows:
        code = str(row.get("index_code", ""))
        if (code in catalog or row.get("src") != "SW2021" or row.get("level") != "L2"
            or type(row.get("is_pub")) not in (str, int) or str(row["is_pub"]) not in {"0", "1"}):
            raise SWL2QuotePolicyError(f"SW L2 publication catalog is invalid: {code}")
        catalog[code] = str(row["is_pub"])
    if not set(ordered_codes) <= catalog.keys():
        raise SWL2QuotePolicyError("SW L2 publication catalog omits a canonical code")
    observed: dict[str, set[date]] = {code: set() for code in ordered_codes if catalog[code] == "0"}
    for row in quote_observations:
        code = code_map.id_to_code.get(row.get("l2_code_id"))
        if code not in observed:
            continue
        try:
            day = date.fromisoformat(str(row["trade_date"])[:10])
            if not required_start <= day <= cutoff:
                continue
            values = [row[key] for key in _QUOTE_FIELDS]
            if all(value is None or math.isnan(float(value)) for value in values):
                continue
            if (any(value is None or isinstance(value, bool) or not math.isfinite(float(value)) for value in values)
                or float(values[1]) < 0 or float(values[2]) < 0):
                raise ValueError("incomplete or non-finite fields")
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise SWL2QuotePolicyError(f"SW L2 quote observation invalid: {code} {row.get('trade_date')}") from exc
        observed[code].add(day)

    entries: list[dict[str, Any]] = []
    for code in ordered_codes:
        spans: list[dict[str, str]] = []
        if catalog[code] == "1":
            spans.append({"start_date": required_start.isoformat(), "end_date": cutoff.isoformat()})
        else:
            for day in sorted(observed[code]):
                if spans and date.fromisoformat(spans[-1]["end_date"]) + timedelta(days=1) == day:
                    spans[-1]["end_date"] = day.isoformat()
                else:
                    spans.append({"start_date": day.isoformat(), "end_date": day.isoformat()})
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
    if evidence is not None:
        evidence.update({
            "publication_rule_url": PUBLICATION_RULE_URL,
            "publication_policy_identity": quote_publication_policy_identity(),
            "classification_catalog_digest": digest_named_fields(
                "aistock_sw_l2_frozen_publication_catalog_v1", {"catalog": catalog}),
            "nonpublished_catalog_codes": sorted(observed),
            "dated_observation_count": sum(map(len, observed.values())),
            "scope_semantics": "versioned_catalog_obligation_plus_dated_provider_facts_v1",
            "historical_stop_date_inferred": False,
            "membership_used_to_infer_publication": False,
        })
    return payload


__all__ = (
    "QUOTE_PUBLICATION_POLICY_SCHEMA",
    "SW2021_L2_TAXONOMY_DIGEST",
    "SWL2QuotePolicyError",
    "build_quote_availability_payload",
    "quote_publication_policy_identity",
    "publication_quote_observations",
    "taxonomy_digest",
)
