"""One fail-closed daily overlay contract for preflight and score consumption.

An absent coefficient is never a neutral coefficient.  Explicit non-application
must be part of a hash-pinned, registered producer artifact.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Mapping, Sequence

from backend.services.dataset_release.shared_sector_context import (
    RELEASE_SW_L2_CODE_MAP_SCHEMA,
    SECTOR_QUOTE_AVAILABILITY_SCHEMA,
)
from backend.services.trading_core.errors import HMMRuntimeUnavailableError


NO_L2_OVERLAY_POLICY = "explicit_no_l2_overlay_v1"


@dataclass(frozen=True)
class HMMOverlayDay:
    coefficients: dict[str, float]
    membership: dict[str, str]
    no_overlay_sectors: frozenset[str]
    metadata: dict[str, Any]


def validate_overlay_day(
    *,
    payload: Mapping[str, Any],
    membership: Any,
    trade_date: date,
    effective_trade_date: date | None,
    snapshot_id: str,
    config_id: str | None,
    signal_preset: str,
    coefficient_filename: str,
    coefficient_sha256: str,
    model_sha256: str,
    registered_windows: Callable[[], Sequence[Mapping[str, Any]]],
    context: dict[str, Any],
) -> HMMOverlayDay:
    def fail(message: str, **details: Any) -> None:
        raise HMMRuntimeUnavailableError(message, context={**context, **details})

    def digest(value: Any, field: str) -> str:
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            fail("HMM no-overlay declaration has an invalid hash", field=field)
        return value

    def iso_day(value: Any, field: str) -> str:
        try:
            if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                raise ValueError
        except ValueError:
            fail("HMM no-overlay declaration has an invalid date", field=field)
        return value

    day_key = trade_date.isoformat()
    daily = payload.get("daily_coefficients")
    if not isinstance(daily, dict) or not isinstance(membership, dict):
        fail("HMM coefficient artifact is missing required keys")
    day = daily.get(day_key)
    if not isinstance(day, dict):
        fail("HMM coefficient artifact has no coefficients for trade_date")
    if not membership:
        fail("HMM coefficient artifact is missing stock sector mapping")
    empty = [str(symbol) for symbol, code in membership.items() if not str(code or "").strip()]
    if empty:
        fail(
            "HMM coefficient artifact contains empty stock sector mapping values",
            symbol_samples=empty[:20],
            missing_count=len(empty),
        )
    coefficients: dict[str, float] = {}
    for code, value in day.items():
        if not isinstance(code, str) or not code.strip() or code != code.strip():
            fail("HMM coefficient artifact contains an empty sector code", sector_code=code)
        try:
            number = float(value)
            if isinstance(value, bool) or not math.isfinite(number) or number <= 0:
                raise ValueError
        except (TypeError, ValueError, OverflowError):
            fail("HMM sector coefficient must be positive finite", sector_code=code, coefficient=value)
        coefficients[code] = number
    normalized_map = {symbol: str(code).strip() for symbol, code in membership.items()}
    missing = set(normalized_map.values()) - coefficients.keys()
    declared = "l2_overlay_unavailable_policy" in payload or "quote_unavailable_sector_codes_by_date" in payload
    if not declared:
        if not coefficients:
            fail("HMM coefficient artifact has empty coefficients for trade_date")
        if missing:
            fail(
                "HMM coefficient artifact has stock sector mapping without coefficients",
                missing_sector_count=len(missing),
                missing_sector_samples=sorted(missing)[:20],
            )
        return HMMOverlayDay(coefficients, normalized_map, frozenset(), {})

    if payload.get("l2_overlay_unavailable_policy") != NO_L2_OVERLAY_POLICY:
        fail("HMM no-overlay policy is missing or unsupported")
    if payload.get("source_mode") not in ("database_daily", "qe_frozen_file"):
        fail("HMM no-overlay artifact has an unsupported source identity")
    if (
        payload.get("sector_code_map_schema") != RELEASE_SW_L2_CODE_MAP_SCHEMA
        or payload.get("quote_availability_schema") != SECTOR_QUOTE_AVAILABILITY_SCHEMA
    ):
        fail("HMM no-overlay declaration has an unsupported authority schema")
    authority = payload.get("quote_availability_authority")
    if (
        not isinstance(authority, dict)
        or set(authority) != {"authority_id", "authority_sha256"}
        or not isinstance(authority.get("authority_id"), str)
        or not authority["authority_id"].strip()
        or authority != payload.get("sector_code_map_authority")
    ):
        fail("HMM no-overlay quote and membership authorities differ")
    digest(authority["authority_sha256"], "authority_sha256")
    for field in ("quote_availability_digest", "sector_code_map_digest", "model_sha256"):
        digest(payload.get(field), field)
    if (
        payload.get("snapshot_id") not in (None, snapshot_id)
        or payload.get("config_id") not in (None, config_id)
        or payload.get("model_sha256") != model_sha256
        or payload.get("preset_key") != signal_preset
    ):
        fail("HMM no-overlay artifact differs from the runtime model identity")

    by_date = payload.get("quote_unavailable_sector_codes_by_date")
    unavailable = by_date.get(day_key) if isinstance(by_date, dict) else None
    if (
        not isinstance(unavailable, list)
        or any(not isinstance(code, str) or not code.strip() for code in unavailable)
        or len(unavailable) != len(set(unavailable))
        or set(unavailable) != missing
    ):
        fail("HMM no-overlay declaration does not explain the exact coefficient gap")

    as_of = day_key
    effective = effective_trade_date.isoformat() if effective_trade_date else day_key
    if payload.get("generation_mode") == "daily_asof_prediction_v1":
        as_of = iso_day(payload.get("as_of_trade_date"), "as_of_trade_date")
        artifact_effective = iso_day(payload.get("effective_trade_date"), "effective_trade_date")
        if as_of >= artifact_effective or artifact_effective != day_key or artifact_effective != effective:
            fail("HMM no-overlay artifact differs from the requested day identity")
    watermarks = payload.get("input_data_max_dates_by_date")
    dates = watermarks.get(day_key) if isinstance(watermarks, dict) else None
    if not isinstance(dates, dict) or not dates:
        fail("HMM no-overlay artifact has no dated input watermark")
    for field, value in dates.items():
        maximum = effective if field == "sw_index_member_effective_as_of" else as_of
        if iso_day(value, str(field)) > maximum:
            fail("HMM no-overlay artifact contains a future input date", field=field)

    matches = [row for row in registered_windows() if row.get("coefficient_filename") == coefficient_filename]
    if len(matches) != 1:
        fail("HMM no-overlay artifact requires one exact registered coefficient window")
    registration = matches[0]
    expected = {"coefficient_sha256": coefficient_sha256, "preset": signal_preset}
    if config_id:
        expected["config_id"] = config_id
    if payload.get("source_mode") == "database_daily":
        if payload.get("snapshot_id") != snapshot_id or payload.get("config_id") != config_id:
            fail("HMM no-overlay database artifact has an incomplete model identity")
        expected.update(
            {
                "snapshot_id": snapshot_id,
                "source_mode": "database_daily",
                "as_of_trade_date": as_of,
                "model_sha256": model_sha256,
                "input_identity": payload.get("input_identity"),
            }
        )
        identity = payload.get("input_identity")
        if (
            not isinstance(identity, dict)
            or identity.get("schema_version") != "hmm_database_daily_input_v1"
            or identity.get("mapping_authority") != authority
            or identity.get("as_of_trade_date") != as_of
            or identity.get("effective_trade_date") != effective
        ):
            fail("HMM no-overlay database input identity differs from the authority/day")
    start = iso_day(registration.get("test_start"), "registered_test_start")
    end = iso_day(registration.get("backtest_end"), "registered_backtest_end")
    if any(registration.get(key) != value for key, value in expected.items()) or not start <= day_key <= end:
        fail("HMM no-overlay registered identity/hash differs from the artifact")
    metadata = {
        "l2_overlay_unavailable_policy": NO_L2_OVERLAY_POLICY,
        "no_overlay_sector_codes": sorted(unavailable),
        "no_overlay_sector_count": len(unavailable),
        "quote_availability_authority": dict(authority),
        "quote_availability_digest": payload["quote_availability_digest"],
        "as_of_trade_date": as_of,
        "effective_trade_date": effective,
    }
    return HMMOverlayDay(coefficients, normalized_map, frozenset(unavailable), metadata)
