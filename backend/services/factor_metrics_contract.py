"""Shared nullable extension fields for factor-metric persistence contracts."""

from __future__ import annotations

from typing import Any, Mapping


H20_METRIC_FIELDS: tuple[str, ...] = (
    "h20_return_horizon",
    "h20_ic_mean",
    "h20_ic_std",
    "h20_rank_ic_mean",
    "h20_rank_ic_std",
    "h20_icir",
    "h20_rank_icir",
    "h20_icir_hac",
    "h20_rank_icir_hac",
    "h20_ic_positive_ratio",
    "h20_n_obs",
    "h20_hac_lag",
)
H20_CONTRACT_PRESENT_PARAM = "_h20_contract_present"


def with_h20_metric_defaults(values: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy whose optional h20 named parameters always exist.

    RD-Agent payloads created before the h20 contract contain none of these
    keys. Psycopg named placeholders raise ``KeyError`` for missing keys, so
    every writer must normalize the payload before executing an h20-aware SQL
    statement. A separate presence flag lets an old payload preserve an existing
    h20 companion value, while a new payload may explicitly overwrite it with
    ``None`` when the horizon has insufficient observations.
    """

    normalized = dict(values)
    normalized[H20_CONTRACT_PRESENT_PARAM] = any(
        field in values for field in H20_METRIC_FIELDS
    )
    for field in H20_METRIC_FIELDS:
        normalized.setdefault(field, None)
    return normalized
