"""Current-day PT-NEXT-004 model advice materialization and readback.

This is deliberately an artifact-backed extension of the existing position-
timing service. It does not train, submit orders, emit alerts, or modify v1
cards. Unsupported research is still useful as explicitly experimental,
per-stock shadow advice.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from .action_value import (
    ActionValueError,
    FEATURE_SPEC_SHA256,
    POLICY_SHA256,
    PositionState,
    money,
)
from .action_value_advice import decide_stock_day, public_advice
from .action_value_data import runtime_core_frame
from .action_value_model import read_local_model
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256


MODEL_ADVICE_SCHEMA = "position_timing_model_advice_v2"
CURRENT_RESEARCH_SCHEMA = "position_timing_action_value_current_research_v2"
DIRECTION_REFERENCE_NOTIONAL_CNY = Decimal("100000")


def materialize_model_advice(
    *,
    timing_root: Path,
    now: datetime,
    decision_date: date,
    decision_as_of: datetime,
    target_date: date,
    calendar: Sequence[date],
    members: Sequence[Mapping[str, Any]],
    snapshot_loader: Callable[[list[str], date, date, datetime], dict[str, Any]] | None,
) -> dict[str, Any]:
    """Materialize one immutable shadow advice set after the 20:00 cutoff."""

    if now.tzinfo is None or decision_as_of.tzinfo is None:
        raise ActionValueError("MODEL_ADVICE_CLOCK_TIMEZONE_REQUIRED")
    if now < decision_as_of:
        return _status("DECISION_CUTOFF_NOT_REACHED", decision_date, target_date)
    existing = _read_date_advice(timing_root, decision_date)
    if existing is not None:
        return {"status": "ALREADY_MATERIALIZED", "created": False, "advice_set": existing}
    research = load_current_research_state(timing_root)
    if research is None:
        return _status("MODEL_RESEARCH_NOT_AVAILABLE", decision_date, target_date)
    if snapshot_loader is None:
        return _status("MODEL_CORE_SOURCE_UNAVAILABLE", decision_date, target_date)
    ordered_calendar = tuple(calendar)
    if (
        not ordered_calendar
        or tuple(sorted(set(ordered_calendar))) != ordered_calendar
        or ordered_calendar[-1] != decision_date
        or len(ordered_calendar) < 31
    ):
        raise ActionValueError("MODEL_ADVICE_CALENDAR_INVALID")
    normalized_members = tuple(_normalize_member(item) for item in members)
    symbols = [item["canonical_symbol"] for item in normalized_members]
    if not symbols:
        return _status("NO_ANALYSIS_SCOPE", decision_date, target_date)

    model = read_local_model(
        timing_root=timing_root,
        model_sha256=str(research["model_sha256"]),
        decision_as_of=decision_as_of,
    )
    snapshot = snapshot_loader(symbols, ordered_calendar[0], decision_date, now)
    if not isinstance(snapshot, dict):
        raise ActionValueError("MODEL_CORE_SOURCE_UNAVAILABLE")
    stock_frames: dict[str, pd.DataFrame] = {}
    stock_errors: dict[str, ActionValueError] = {}
    for symbol in symbols:
        try:
            stock_frames[symbol] = runtime_core_frame(
                snapshot, symbol, calendar=ordered_calendar, cutoff=decision_as_of
            )
        except ActionValueError as exc:
            stock_errors[symbol] = exc
    benchmark_frame = _runtime_benchmark_frame(snapshot, calendar=ordered_calendar, cutoff=decision_as_of)
    benchmark = benchmark_frame["close"]
    source_identity = _validated_snapshot_identity(snapshot)
    research_population = set(research.get("_research_population_symbols") or ())
    items: list[dict[str, Any]] = []
    for member in normalized_members:
        symbol = member["canonical_symbol"]
        try:
            if symbol in stock_errors:
                raise stock_errors[symbol]
            if member["delist_risk"] is None:
                raise ActionValueError(
                    member.get("delist_reason_code")
                    or "MODEL_DELIST_CONTEXT_UNAVAILABLE"
                )
            state, max_exposure, direction_only = _state_from_member(member, stock_frames[symbol])
            decision = decide_stock_day(
                symbol=symbol,
                state=state,
                bars=stock_frames[symbol],
                benchmark=benchmark,
                decision_as_of=decision_as_of,
                model=model,
                max_exposure=max_exposure,
                delist_risk=member["delist_risk"],
            )
            item = {
                **public_advice(decision, direction_only=direction_only),
                "canonical_symbol": symbol,
                "display_name": member.get("display_name"),
                "primary_source_role": member["primary_source_role"],
                "status": "AVAILABLE",
                "evidence_tier": "EXPERIMENTAL_MODEL_ADVICE",
                "target_trade_date": target_date.isoformat(),
                "feature_spec_sha256": FEATURE_SPEC_SHA256,
                "source_identity_sha256": source_identity["identity_sha256"],
                "research_population_status": (
                    "IN_FROZEN_RESEARCH_SAMPLE"
                    if symbol in research_population
                    else "OUT_OF_RESEARCH_SAMPLE_EXTRAPOLATION"
                ),
            }
        except ActionValueError as exc:
            item = {
                "canonical_symbol": symbol,
                "display_name": member.get("display_name"),
                "primary_source_role": member["primary_source_role"],
                "status": "UNAVAILABLE",
                "action": "UNAVAILABLE",
                "evidence_tier": "EXPERIMENTAL_MODEL_ADVICE",
                "reason_codes": [exc.code],
                "executable_alert": False,
                "planned_delta_qty": None,
            }
        items.append(item)

    payload = {
        "schema_version": MODEL_ADVICE_SCHEMA,
        "decision_trade_date": decision_date.isoformat(),
        "decision_as_of": decision_as_of.isoformat(),
        "target_trade_date": target_date.isoformat(),
        "created_at": now.isoformat(),
        "advice_tier": "EXPERIMENTAL_MODEL_ADVICE",
        "effect_evidence": research["effect_evidence"],
        "model_sha256": model.metadata["model_sha256"],
        "model_training_cutoff": model.metadata["training_cutoff"],
        "model_available_at": model.metadata["available_at"],
        "feature_spec_sha256": FEATURE_SPEC_SHA256,
        "policy_sha256": POLICY_SHA256,
        "source_identity": source_identity,
        "analysis_scope_sha256": canonical_sha256(normalized_members),
        "items": items,
        "formal_card_changed": False,
        "alert_emitted": False,
        "order_created": False,
    }
    payload["advice_sha256"] = canonical_sha256(payload)
    selected, created = _publish_date_advice(timing_root, payload)
    return {"status": "MATERIALIZED" if created else "ALREADY_MATERIALIZED", "created": created, "advice_set": selected}


def current_model_advice(*, timing_root: Path, now: datetime) -> dict[str, Any]:
    """Read only. A GET never trains, snapshots data, or writes a pointer."""

    pointer = timing_root.resolve() / "model_advice_v2" / "current.json"
    if not pointer.exists():
        return {"schema_version": MODEL_ADVICE_SCHEMA, "status": "NO_MODEL_ADVICE", "advice_set": None}
    state = _read_hash_bound(pointer, "state_sha256", "MODEL_ADVICE_CURRENT_STATE_INVALID")
    advice_path = Path(state["advice_path"]).resolve()
    if not advice_path.is_relative_to(timing_root.resolve()):
        raise ActionValueError("MODEL_ADVICE_PATH_OUTSIDE_OWNER")
    advice = _read_hash_bound(advice_path, "advice_sha256", "MODEL_ADVICE_ARTIFACT_INVALID")
    if (
        state.get("advice_sha256") != advice.get("advice_sha256")
        or state.get("decision_trade_date") != advice.get("decision_trade_date")
    ):
        raise ActionValueError("MODEL_ADVICE_CURRENT_STATE_MISMATCH")
    target = date.fromisoformat(advice["target_trade_date"])
    if now.date() < target:
        status = "UPCOMING"
    elif now.date() == target:
        status = "VALID_TODAY"
    else:
        status = "EXPIRED"
    return {"schema_version": MODEL_ADVICE_SCHEMA, "status": status, "advice_set": advice}


def load_current_research_state(timing_root: Path) -> dict[str, Any] | None:
    path = timing_root.resolve() / "research" / "action_value_v2" / "current.json"
    if not path.exists():
        return None
    payload = _read_hash_bound(path, "state_sha256", "ACTION_VALUE_CURRENT_STATE_INVALID")
    if payload.get("schema_version") != CURRENT_RESEARCH_SCHEMA:
        raise ActionValueError("ACTION_VALUE_CURRENT_STATE_SCHEMA_MISMATCH")
    bundle = Path(payload.get("bundle_path") or "").resolve()
    if not bundle.is_relative_to(timing_root.resolve()):
        raise ActionValueError("ACTION_VALUE_CURRENT_STATE_PATH_OUTSIDE_OWNER")
    receipt = _read_hash_bound(
        bundle / "receipt.json",
        "receipt_sha256",
        "ACTION_VALUE_CURRENT_RECEIPT_INVALID",
    )
    if (
        payload.get("receipt_sha256") != receipt.get("receipt_sha256")
        or payload.get("model_sha256") != receipt.get("final_model_sha256")
        or payload.get("effect_evidence") != receipt.get("effect_evidence")
    ):
        raise ActionValueError("ACTION_VALUE_CURRENT_RECEIPT_MISMATCH")
    payload = dict(payload)
    payload["_research_population_symbols"] = tuple(
        str(value) for value in (receipt.get("population") or {}).get("symbols", ())
    )
    return payload


def _normalize_member(raw: Mapping[str, Any]) -> dict[str, Any]:
    symbol = str(raw.get("canonical_symbol") or "").upper()
    if not symbol or not isinstance(raw.get("primary_source_role"), str):
        raise ActionValueError("MODEL_ADVICE_MEMBER_INVALID")
    holding = dict(raw.get("holding") or {})
    intent = dict(raw.get("intent") or {})
    return {
        "canonical_symbol": symbol,
        "display_name": raw.get("display_name"),
        "primary_source_role": raw["primary_source_role"],
        "holding": holding,
        "intent": intent,
        "delist_risk": (
            raw.get("delist_risk") if isinstance(raw.get("delist_risk"), bool) else None
        ),
        "delist_reason_code": raw.get("delist_reason_code"),
        "delist_identity": dict(raw.get("delist_identity") or {}),
    }


def _state_from_member(member: Mapping[str, Any], bars: pd.DataFrame) -> tuple[PositionState, Decimal, bool]:
    price = money(bars.iloc[-1]["close"])
    holding = member["holding"]
    intent = member["intent"]
    quantity = _non_negative_quantity(holding.get("quantity"))
    full_notional = _positive_optional(intent.get("planned_full_notional_cny"))
    target = _bounded_exposure(intent.get("desired_target_exposure"))
    entry_cost = _positive_optional(holding.get("cost_price"))
    if quantity:
        market_value = price * quantity
        capital = max(full_notional or Decimal(0), market_value)
        cash = max(Decimal(0), capital - market_value)
    else:
        capital = full_notional or DIRECTION_REFERENCE_NOTIONAL_CNY
        cash = capital
    state = PositionState(
        quantity=quantity,
        sellable=quantity,
        cash=cash,
        capital=capital,
        entry_cost=entry_cost,
        holding_age=None,
    )
    return state, target or Decimal(1), full_notional is None


def _runtime_benchmark_frame(snapshot: Mapping[str, Any], *, calendar: Sequence[date], cutoff: datetime) -> pd.DataFrame:
    rows = snapshot.get("benchmark_rows")
    if not isinstance(rows, dict):
        raise ActionValueError("RUNTIME_BENCHMARK_SOURCE_UNAVAILABLE")
    records = []
    for day in calendar:
        row = dict(rows.get(day.isoformat()) or {})
        available = row.get("feature_available_at")
        if row and available is None:
            raise ActionValueError("RUNTIME_BENCHMARK_FEATURE_TIME_UNAVAILABLE")
        if available is not None:
            parsed = datetime.fromisoformat(str(available))
            if parsed.tzinfo is None or parsed > cutoff:
                raise ActionValueError("RUNTIME_BENCHMARK_PIT_UNAVAILABLE")
        records.append(row)
    frame = pd.DataFrame(records, index=pd.DatetimeIndex(calendar))
    if "close" not in frame:
        raise ActionValueError("RUNTIME_BENCHMARK_SCHEMA_MISSING")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    if frame["close"].tail(21).isna().any() or (frame["close"].tail(21) <= 0).any():
        raise ActionValueError("RUNTIME_BENCHMARK_CORE_UNAVAILABLE")
    return frame


def _validated_snapshot_identity(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    identity = snapshot.get("identity")
    if not isinstance(identity, dict):
        raise ActionValueError("RUNTIME_CORE_IDENTITY_MISMATCH")
    expected = canonical_sha256({"rows": snapshot.get("rows"), "benchmark_rows": snapshot.get("benchmark_rows")})
    if identity.get("rows_sha256") != expected:
        raise ActionValueError("RUNTIME_CORE_IDENTITY_MISMATCH")
    if identity.get("identity_sha256") != canonical_sha256(
        {key: value for key, value in identity.items() if key != "identity_sha256"}
    ):
        raise ActionValueError("RUNTIME_CORE_IDENTITY_MISMATCH")
    return dict(identity)


def _read_date_advice(root: Path, decision_date: date) -> dict[str, Any] | None:
    folder = root.resolve() / "model_advice_v2" / decision_date.isoformat()
    if not folder.exists():
        return None
    paths = tuple(folder.glob("advice-*.json"))
    if len(paths) != 1:
        raise ActionValueError("MODEL_ADVICE_DATE_IDENTITY_CONFLICT")
    return _read_hash_bound(paths[0], "advice_sha256", "MODEL_ADVICE_ARTIFACT_INVALID")


def _publish_date_advice(root: Path, payload: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    root = root.resolve()
    decision_date = date.fromisoformat(str(payload["decision_trade_date"]))
    lock = root / "locks" / f"model-advice-v2-{decision_date.isoformat()}.lock"
    with _exclusive_file_lock(lock):
        existing = _read_date_advice(root, decision_date)
        if existing is not None:
            # The first immutable daily artifact is authoritative. Concurrent
            # callers can differ in request-time metadata such as created_at;
            # they must converge exactly like the pre-lock fast path does.
            selected, created = existing, False
        else:
            path = root / "model_advice_v2" / decision_date.isoformat() / f"advice-{payload['advice_sha256']}.json"
            PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(payload) + b"\n")
            selected, created = dict(payload), True
    advice_path = root / "model_advice_v2" / decision_date.isoformat() / f"advice-{selected['advice_sha256']}.json"
    state = {
        "schema_version": "position_timing_model_advice_current_v2",
        "decision_trade_date": decision_date.isoformat(),
        "advice_sha256": selected["advice_sha256"],
        "advice_path": advice_path.as_posix(),
    }
    state["state_sha256"] = canonical_sha256(state)
    PositionTimingArtifactStore._atomic_replace(root / "model_advice_v2" / "current.json", canonical_json_bytes(state) + b"\n")
    return selected, created


def _read_hash_bound(path: Path, hash_field: str, reason: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError) as exc:
        raise ActionValueError(reason, cause=type(exc).__name__) from exc
    expected = canonical_sha256({key: value for key, value in payload.items() if key != hash_field})
    if payload.get(hash_field) != expected:
        raise ActionValueError(reason)
    return payload


def _positive_optional(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    parsed = money(value)
    return parsed if parsed > 0 else None


def _bounded_exposure(value: Any) -> Decimal | None:
    parsed = _positive_optional(value)
    if parsed is None:
        return None
    if parsed > 1:
        raise ActionValueError("ACTION_BUDGET_INVALID")
    return parsed


def _non_negative_quantity(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        raise ActionValueError("POSITION_QUANTITY_INVALID")
    parsed = int(value)
    if parsed < 0 or Decimal(str(value)) != parsed:
        raise ActionValueError("POSITION_QUANTITY_INVALID")
    return parsed


def _status(status: str, decision_date: date, target_date: date) -> dict[str, Any]:
    return {
        "schema_version": MODEL_ADVICE_SCHEMA,
        "status": status,
        "created": False,
        "decision_trade_date": decision_date.isoformat(),
        "target_trade_date": target_date.isoformat(),
        "advice_set": None,
    }


__all__ = [
    "current_model_advice",
    "load_current_research_state",
    "materialize_model_advice",
]
