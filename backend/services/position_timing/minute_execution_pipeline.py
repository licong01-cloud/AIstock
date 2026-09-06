"""Offline L4b-1 minute execution-window counterfactual.

The pipeline consumes only immutable prospective position-timing cards and a
frozen Qlib minute candidate.  It never generates a direction, order, runtime
policy, card, or event, and it has no router, worker, scheduler, DB, TDX, QE,
Paper, or MiniQMT adapter.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import subprocess
import tempfile
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Mapping, NoReturn, Sequence

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import (
    ExecutionWindow,
    PositionTimingCardSetV1,
    PositionTimingCardV1,
    TimingAction,
    TriggerOperator,
    TriggerSide,
    canonical_json_bytes,
    canonical_sha256,
)
from .policy import COST_POLICY_SHA256, PERSONAL_MANUAL_COMPONENT_COST_V1, component_cost_for_parent_notionals


REQUEST_SCHEMA = "position_timing_l4b1_request_v1"
BUNDLE_SCHEMA = "position_timing_l4b1_bundle_v1"
RECEIPT_SCHEMA = "position_timing_l4b1_receipt_v1"
REGISTRY_SCHEMA = "position_timing_l4b1_registry_record_v1"
BENCHMARK_ID = "AT_OPEN_RAW_V1"
CHALLENGER_ID = "OPENING_30M_VWAP_RAW_V1"
LEGACY_DIRECTION_ORDER = ("BUY", "SELL")
ACTIVE_DIRECTION_ORDER = ("SELL",)
PRIMARY_HORIZON = 20
WINDOW_TIMES = tuple(f"09:{minute:02d}:00" for minute in range(30, 60))
BOOTSTRAP_REPETITIONS = 5_000
BOOTSTRAP_SEED = 20_260_906
BOOTSTRAP_BLOCK_DAYS = 5
MIN_PAIRED_CARDS = 30
MIN_PAIRED_TRADE_DATES = 20
FAMILYWISE_HYPOTHESIS_COUNT = 1
ALPHA = 0.05
LIMIT_TOLERANCE = 1e-4
REQUIRED_MINUTE_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "factor",
    "up_limit_price",
    "down_limit_price",
    "limit_up",
    "limit_down",
)
ACTION_SIDE = {
    TimingAction.OPEN: TriggerSide.BUY,
    TimingAction.ADD: TriggerSide.BUY,
    TimingAction.REDUCE: TriggerSide.SELL,
    TimingAction.EXIT: TriggerSide.SELL,
}
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_COMMIT_PATTERN = r"^[0-9a-f]{40}$"


class PositionTimingL4b1Error(RuntimeError):
    """Typed fail-closed error for the offline audit."""

    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": "failed",
            "reason_code": self.reason_code,
            "message": str(self),
            "context": self.context,
        }


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class FileReferenceV1(FrozenModel):
    role: str = Field(min_length=1)
    artifact_uri: str = Field(min_length=1)
    sha256: str = Field(pattern=_SHA256_PATTERN)
    size_bytes: int = Field(ge=0)


class ProspectiveCardItemV1(FrozenModel):
    card_id: str = Field(min_length=1)
    card_set_id: str = Field(min_length=1)
    card_set_ref_role: str = Field(min_length=1)
    card_artifact_sha256: str = Field(pattern=_SHA256_PATTERN)
    card_issued_event_sha256: str = Field(pattern=_SHA256_PATTERN)
    canonical_symbol: str = Field(pattern=r"^\d{6}\.(SH|SZ)$")
    target_trade_date: date
    action: str
    execution_window: str
    side: Literal["BUY", "SELL", "NONE"]
    quantity: int = Field(ge=0)
    cost_policy_sha256: str = Field(pattern=_SHA256_PATTERN)
    population_status: Literal[
        "ELIGIBLE",
        "NON_ACTION",
        "NOT_AT_OPEN",
        "ZERO_QUANTITY",
        "DIRECTION_OUT_OF_SCOPE",
        "ACTION_OUT_OF_SCOPE",
        "ACTION_CONTRACT_OUT_OF_SCOPE",
        "COST_POLICY_INCOMPATIBLE",
        "MINUTE_COVERAGE_BEFORE_START",
        "MINUTE_COVERAGE_PENDING",
        "MINUTE_UNIVERSE_EXCLUDED",
    ]


class FrozenL4b1RequestV1(FrozenModel):
    schema_version: Literal["position_timing_l4b1_request_v1"] = REQUEST_SCHEMA
    request_id: str = Field(pattern=r"^ptl4b1req_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=_SHA256_PATTERN)
    created_at: datetime
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=_COMMIT_PATTERN)
    timing_artifact_root: str = Field(min_length=1)
    minute_provider_root: str = Field(min_length=1)
    output_root: str = Field(min_length=1)
    registry_path: str = Field(min_length=1)
    minute_snapshot_id: str = Field(min_length=1)
    minute_coverage_start: date
    minute_coverage_end: date
    source_refs: dict[str, FileReferenceV1]
    population_items: tuple[ProspectiveCardItemV1, ...]
    population_counts: dict[str, int]
    population_sha256: str = Field(pattern=_SHA256_PATTERN)
    minute_source_identity_sha256: str = Field(pattern=_SHA256_PATTERN)
    cost_policy: dict[str, Any]
    cost_policy_sha256: str = Field(pattern=_SHA256_PATTERN)
    benchmark_id: Literal["AT_OPEN_RAW_V1"] = BENCHMARK_ID
    challenger_id: Literal["OPENING_30M_VWAP_RAW_V1"] = CHALLENGER_ID
    primary_horizon_trading_days: Literal[20] = PRIMARY_HORIZON
    parent_order_count: Literal[1] = 1
    familywise_hypothesis_count: Literal[1, 2] = FAMILYWISE_HYPOTHESIS_COUNT
    bootstrap_repetitions: Literal[5000] = BOOTSTRAP_REPETITIONS
    bootstrap_seed: Literal[20260906] = BOOTSTRAP_SEED
    bootstrap_block_observed_trade_dates: Literal[5] = BOOTSTRAP_BLOCK_DAYS
    alpha: Literal[0.05] = ALPHA
    minimum_paired_cards_per_side: Literal[30] = MIN_PAIRED_CARDS
    minimum_paired_trade_dates_per_side: Literal[20] = MIN_PAIRED_TRADE_DATES
    market_impact_assumption: Literal["NO_MARKET_IMPACT_ASSUMED"] = "NO_MARKET_IMPACT_ASSUMED"

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenL4b1RequestV1":
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("L4b-1 request created_at must be timezone-aware")
        if self.minute_coverage_end < self.minute_coverage_start:
            raise ValueError("minute coverage is inverted")
        timing_root = Path(self.timing_artifact_root).resolve()
        if Path(self.output_root).resolve() != (timing_root / "research").resolve():
            raise ValueError("L4b-1 output root is outside timing-owned research")
        if Path(self.registry_path).resolve() != (
            timing_root / "research_registry" / "timing_execution_window_registry_v1.jsonl"
        ).resolve():
            raise ValueError("L4b-1 registry is outside the timing-owned registry")
        if self.cost_policy_sha256 != canonical_sha256(self.cost_policy):
            raise ValueError("L4b-1 cost policy identity mismatch")
        expected_population = canonical_sha256(self.population_items)
        if self.population_sha256 != expected_population:
            raise ValueError("L4b-1 population identity mismatch")
        expected_counts = dict(sorted(Counter(item.population_status for item in self.population_items).items()))
        if self.population_counts != expected_counts:
            raise ValueError("L4b-1 population counts mismatch")
        control_roles = {"minute_meta", "minute_calendar", "minute_instruments"}
        if not control_roles.issubset(self.source_refs):
            raise ValueError("L4b-1 minute control refs are incomplete")
        feature_roles = {
            _feature_role(item.canonical_symbol, field)
            for item in self.population_items
            if item.population_status == "ELIGIBLE"
            for field in REQUIRED_MINUTE_FIELDS
        }
        if not feature_roles.issubset(self.source_refs):
            raise ValueError("L4b-1 minute feature refs are incomplete")
        supported_directions = set(_directions_for_family(self.familywise_hypothesis_count))
        if any(
            item.population_status == "ELIGIBLE" and item.side not in supported_directions
            for item in self.population_items
        ):
            raise ValueError("L4b-1 eligible population contains an unregistered direction")
        supported_actions = set(_actions_for_family(self.familywise_hypothesis_count))
        if any(
            item.population_status == "ELIGIBLE" and item.action not in supported_actions
            for item in self.population_items
        ):
            raise ValueError("L4b-1 eligible population contains an unregistered action")
        card_set_roles = {item.card_set_ref_role for item in self.population_items}
        if not card_set_roles.issubset(self.source_refs):
            raise ValueError("L4b-1 card-set refs are incomplete")
        minute_refs = {
            key: value.model_dump(mode="json")
            for key, value in self.source_refs.items()
            if key in control_roles or key.startswith("minute_feature:")
        }
        if self.minute_source_identity_sha256 != canonical_sha256(minute_refs):
            raise ValueError("L4b-1 minute source identity mismatch")
        expected = canonical_sha256(self.functional_payload())
        if self.request_sha256 != expected or self.request_id != f"ptl4b1req_{expected[:24]}":
            raise ValueError("L4b-1 request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256", "created_at"})


CardResultStatus = Literal[
    "PAIRED",
    "DATA_ERROR_MINUTE_CALENDAR",
    "DATA_ERROR_REQUIRED_FIELD",
    "DATA_ERROR_FACTOR_INVALID",
    "DATA_ERROR_PRICE_BASIS",
    "BENCHMARK_NO_FILL_LIMIT_UP",
    "BENCHMARK_NO_FILL_LIMIT_DOWN",
    "BENCHMARK_NO_FILL_VOLUME_BELOW_QUANTITY",
    "CHALLENGER_NO_FILL_LIMIT_UP",
    "CHALLENGER_NO_FILL_LIMIT_DOWN",
    "CHALLENGER_NO_FILL_NO_VOLUME",
    "CHALLENGER_NO_FILL_VOLUME_BELOW_QUANTITY",
]


class MinuteExecutionCardResultV1(FrozenModel):
    card_id: str
    canonical_symbol: str
    target_trade_date: date
    side: Literal["BUY", "SELL"]
    quantity: int = Field(gt=0)
    status: CardResultStatus
    reason_code: str
    benchmark_price_raw: float | None = Field(default=None, gt=0)
    challenger_price_raw: float | None = Field(default=None, gt=0)
    benchmark_cost_cny: float | None = Field(default=None, ge=0)
    challenger_cost_cny: float | None = Field(default=None, ge=0)
    benchmark_gross_notional_cny: float | None = Field(default=None, gt=0)
    challenger_gross_notional_cny: float | None = Field(default=None, gt=0)
    challenger_raw_volume_shares: float | None = Field(default=None, ge=0)
    net_improvement_cny: float | None = None
    net_improvement_bps: float | None = None

    @model_validator(mode="after")
    def validate_paired_result(self) -> "MinuteExecutionCardResultV1":
        paired_fields = (
            self.benchmark_price_raw,
            self.challenger_price_raw,
            self.benchmark_cost_cny,
            self.challenger_cost_cny,
            self.benchmark_gross_notional_cny,
            self.challenger_gross_notional_cny,
            self.challenger_raw_volume_shares,
            self.net_improvement_cny,
            self.net_improvement_bps,
        )
        if self.status == "PAIRED" and any(value is None for value in paired_fields):
            raise ValueError("paired L4b-1 result requires complete economics")
        return self


class IntervalV1(FrozenModel):
    lower_bps: float
    upper_bps: float
    alpha: float = Field(gt=0, lt=1)


class DirectionSummaryV1(FrozenModel):
    side: Literal["BUY", "SELL"]
    eligible_card_count: int = Field(ge=0)
    paired_card_count: int = Field(ge=0)
    paired_trade_date_count: int = Field(ge=0)
    market_no_fill_count: int = Field(ge=0)
    data_error_count: int = Field(ge=0)
    point_estimate_bps: float | None = None
    nominal_interval: IntervalV1 | None = None
    adjusted_interval: IntervalV1 | None = None
    effect_evidence: Literal["SUPPORTED", "NEGATIVE", "INCONCLUSIVE", "INSUFFICIENT_DATA"]
    power_status: Literal["ADEQUATE", "UNDERPOWERED"]
    evidence_reason_codes: tuple[str, ...]


class L4b1ReceiptV1(FrozenModel):
    schema_version: Literal["position_timing_l4b1_receipt_v1"] = RECEIPT_SCHEMA
    receipt_id: str = Field(pattern=r"^ptl4b1rcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=_SHA256_PATTERN)
    request_sha256: str = Field(pattern=_SHA256_PATTERN)
    repository_commit: str = Field(pattern=_COMMIT_PATTERN)
    minute_source_identity_sha256: str = Field(pattern=_SHA256_PATTERN)
    population_sha256: str = Field(pattern=_SHA256_PATTERN)
    card_results_sha256: str = Field(pattern=_SHA256_PATTERN)
    created_at: datetime
    status: Literal[
        "INSUFFICIENT_PROSPECTIVE_ACTION_CARDS",
        "NO_PAIRED_EXECUTIONS",
        "INSUFFICIENT_DATA",
        "SUPPORTED",
        "NEGATIVE",
        "INCONCLUSIVE",
    ]
    direction_summaries: tuple[DirectionSummaryV1, ...]
    selected_sides: tuple[Literal["BUY", "SELL"], ...]
    coverage_counts: dict[str, int]
    familywise_hypothesis_count: Literal[1, 2] = FAMILYWISE_HYPOTHESIS_COUNT
    runtime_policy_written: Literal[False] = False
    card_or_event_written: Literal[False] = False
    order_written: Literal[False] = False
    global_registry_written: Literal[False] = False
    current_route_written: Literal[False] = False
    l1_l1a_gate_applied: Literal[False] = False
    market_impact_assumption: Literal["NO_MARKET_IMPACT_ASSUMED"] = "NO_MARKET_IMPACT_ASSUMED"

    @model_validator(mode="after")
    def validate_identity(self) -> "L4b1ReceiptV1":
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("L4b-1 receipt created_at must be timezone-aware")
        if tuple(item.side for item in self.direction_summaries) != _directions_for_family(
            self.familywise_hypothesis_count
        ):
            raise ValueError("L4b-1 direction order drift")
        supported = tuple(item.side for item in self.direction_summaries if item.effect_evidence == "SUPPORTED")
        if self.selected_sides != supported:
            raise ValueError("L4b-1 selected sides mismatch")
        expected = canonical_sha256(self.functional_payload())
        if self.receipt_sha256 != expected or self.receipt_id != f"ptl4b1rcpt_{expected[:24]}":
            raise ValueError("L4b-1 receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})


class L4b1RegistryRecordV1(FrozenModel):
    schema_version: Literal["position_timing_l4b1_registry_record_v1"] = REGISTRY_SCHEMA
    record_id: str = Field(pattern=r"^ptl4b1rec_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=_SHA256_PATTERN)
    bundle_id: str = Field(pattern=_SHA256_PATTERN)
    receipt_sha256: str = Field(pattern=_SHA256_PATTERN)
    repository_commit: str = Field(pattern=_COMMIT_PATTERN)
    status: str
    selected_sides: tuple[Literal["BUY", "SELL"], ...]
    created_at: datetime


def prepare_l4b1_request(
    *,
    timing_artifact_root: str | Path,
    minute_provider_root: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    registry_path: str | Path,
    output_path: str | Path,
) -> FrozenL4b1RequestV1:
    """Freeze one prospective-card request without writing product artifacts."""

    repo = Path(repository_root).resolve()
    dirty = _repository_dirty_paths(repo)
    if dirty:
        _raise("L4b-1 request requires a clean repository", "POSITION_TIMING_L4B1_REPOSITORY_DIRTY", dirty=dirty)
    commit = _repository_commit(repo)
    timing_root = Path(timing_artifact_root).resolve()
    minute_root = Path(minute_provider_root).resolve()
    resolved_output_root = Path(output_root).resolve()
    resolved_registry_path = Path(registry_path).resolve()
    resolved_output_path = Path(output_path).resolve()
    _validate_owned_output_paths(
        timing_root=timing_root,
        output_root=resolved_output_root,
        registry_path=resolved_registry_path,
        request_path=resolved_output_path,
    )
    meta_path = minute_root / "meta_export.json"
    calendar_path = minute_root / "calendars" / "1min.txt"
    instruments_path = minute_root / "instruments" / "all.txt"
    meta = _read_json(meta_path, "POSITION_TIMING_L4B1_MINUTE_SOURCE_INVALID")
    fields = tuple(meta.get("required_minute_fields", ()))
    if any(field not in fields for field in REQUIRED_MINUTE_FIELDS):
        _raise(
            "minute candidate does not advertise every L4b-1 field",
            "POSITION_TIMING_L4B1_MINUTE_SOURCE_INVALID",
            advertised_fields=list(fields),
        )
    try:
        coverage_start = date.fromisoformat(str(meta["start"]))
        coverage_end = date.fromisoformat(str(meta["end"]))
    except (KeyError, ValueError) as exc:
        _raise("minute candidate coverage is invalid", "POSITION_TIMING_L4B1_MINUTE_SOURCE_INVALID", error=str(exc))

    source_refs: dict[str, FileReferenceV1] = {
        "minute_meta": _file_ref(meta_path, "position_timing_l4b1_minute_meta"),
        "minute_calendar": _file_ref(calendar_path, "position_timing_l4b1_minute_calendar"),
        "minute_instruments": _file_ref(instruments_path, "position_timing_l4b1_minute_instruments"),
    }
    instrument_spans = _load_instrument_spans(instruments_path)
    store = PositionTimingArtifactStore(timing_root)
    card_sets = store.list_card_sets()
    events = {item["card_id"]: item for item in store.list_events(event_type="CARD_ISSUED")}
    items: list[ProspectiveCardItemV1] = []
    eligible_symbols: set[str] = set()
    for card_set in card_sets:
        card_set_path = _card_set_path(timing_root, card_set)
        ref_role = f"card_set:{card_set.card_set_id}"
        source_refs[ref_role] = _file_ref(card_set_path, f"position_timing_l4b1_{ref_role}")
        for card in card_set.cards:
            event = events.get(card.card_id)
            if event is None or event.get("card_set_id") != card.card_set_id:
                _raise(
                    "immutable card is missing its CARD_ISSUED event",
                    "POSITION_TIMING_L4B1_CARD_EVENT_IDENTITY_MISMATCH",
                    card_id=card.card_id,
                )
            item = _population_item(
                card=card,
                event=event,
                card_set_ref_role=ref_role,
                coverage_start=coverage_start,
                coverage_end=coverage_end,
                instrument_spans=instrument_spans,
                included_directions=ACTIVE_DIRECTION_ORDER,
                included_actions=(TimingAction.EXIT.value,),
                require_risk_exit_contract=True,
            )
            items.append(item)
            if item.population_status == "ELIGIBLE":
                eligible_symbols.add(item.canonical_symbol)
    items.sort(key=lambda item: (item.target_trade_date, item.card_id))

    for symbol in sorted(eligible_symbols):
        for field in REQUIRED_MINUTE_FIELDS:
            role = _feature_role(symbol, field)
            path = minute_root / "features" / symbol.lower() / f"{field}.1min.bin"
            source_refs[role] = _file_ref(path, f"position_timing_l4b1_{role}")
    minute_ref_payload = {
        key: value.model_dump(mode="json")
        for key, value in source_refs.items()
        if key in {"minute_meta", "minute_calendar", "minute_instruments"} or key.startswith("minute_feature:")
    }
    population = tuple(items)
    functional = {
        "schema_version": REQUEST_SCHEMA,
        "repository_root": repo.as_posix(),
        "repository_commit": commit,
        "timing_artifact_root": timing_root.as_posix(),
        "minute_provider_root": minute_root.as_posix(),
        "output_root": resolved_output_root.as_posix(),
        "registry_path": resolved_registry_path.as_posix(),
        "minute_snapshot_id": str(meta.get("snapshot_id") or minute_root.name),
        "minute_coverage_start": coverage_start,
        "minute_coverage_end": coverage_end,
        "source_refs": source_refs,
        "population_items": population,
        "population_counts": dict(sorted(Counter(item.population_status for item in population).items())),
        "population_sha256": canonical_sha256(population),
        "minute_source_identity_sha256": canonical_sha256(minute_ref_payload),
        "cost_policy": PERSONAL_MANUAL_COMPONENT_COST_V1,
        "cost_policy_sha256": COST_POLICY_SHA256,
        "benchmark_id": BENCHMARK_ID,
        "challenger_id": CHALLENGER_ID,
        "primary_horizon_trading_days": PRIMARY_HORIZON,
        "parent_order_count": 1,
        "familywise_hypothesis_count": FAMILYWISE_HYPOTHESIS_COUNT,
        "bootstrap_repetitions": BOOTSTRAP_REPETITIONS,
        "bootstrap_seed": BOOTSTRAP_SEED,
        "bootstrap_block_observed_trade_dates": BOOTSTRAP_BLOCK_DAYS,
        "alpha": ALPHA,
        "minimum_paired_cards_per_side": MIN_PAIRED_CARDS,
        "minimum_paired_trade_dates_per_side": MIN_PAIRED_TRADE_DATES,
        "market_impact_assumption": "NO_MARKET_IMPACT_ASSUMED",
    }
    request_sha = canonical_sha256(functional)
    request = FrozenL4b1RequestV1(
        request_id=f"ptl4b1req_{request_sha[:24]}",
        request_sha256=request_sha,
        created_at=datetime.now(timezone.utc),
        **functional,
    )
    _write_immutable_request(resolved_output_path, request)
    return request


def run_l4b1_audit(request_path: str | Path) -> dict[str, Any]:
    request = _read_request(Path(request_path).resolve())
    existing = _find_existing_bundle(request)
    if existing is not None:
        loaded = _inspect_bundle(existing)
        appended = _append_registry(request=request, bundle_path=existing, receipt=loaded["receipt"])
        return _run_response(request, existing, loaded["receipt"], appended=appended, exact_retry=True)
    _verify_run_environment(request)
    calendar = _load_calendar(Path(request.source_refs["minute_calendar"].artifact_uri))
    results = tuple(
        _evaluate_card(item, request=request, calendar=calendar)
        for item in request.population_items
        if item.population_status == "ELIGIBLE"
    )
    summaries = tuple(
        _summarize_direction(side, request=request, results=results)
        for side in _directions_for_family(request.familywise_hypothesis_count)
    )
    receipt = _build_receipt(request=request, results=results, summaries=summaries)
    bundle = _publish_bundle(request=request, results=results, receipt=receipt)
    appended = _append_registry(request=request, bundle_path=bundle, receipt=receipt)
    return _run_response(request, bundle, receipt, appended=appended, exact_retry=False)


def inspect_l4b1_bundle(bundle_path: str | Path) -> dict[str, Any]:
    path = Path(bundle_path).resolve()
    loaded = _inspect_bundle(path)
    receipt: L4b1ReceiptV1 = loaded["receipt"]
    return {
        "status": "BUNDLE_VALID",
        "bundle_id": path.name,
        "request_sha256": loaded["request"].request_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "effect_status": receipt.status,
        "selected_sides": list(receipt.selected_sides),
        "runtime_policy_written": False,
        "order_written": False,
    }


def _population_item(
    *,
    card: PositionTimingCardV1,
    event: Mapping[str, Any],
    card_set_ref_role: str,
    coverage_start: date,
    coverage_end: date,
    instrument_spans: Mapping[str, tuple[tuple[date, date], ...]],
    included_directions: Sequence[str],
    included_actions: Sequence[str],
    require_risk_exit_contract: bool,
) -> ProspectiveCardItemV1:
    card_hash = canonical_sha256(card)
    if event.get("card_artifact_sha256") != card_hash:
        _raise(
            "CARD_ISSUED does not bind the immutable card",
            "POSITION_TIMING_L4B1_CARD_EVENT_IDENTITY_MISMATCH",
            card_id=card.card_id,
        )
    side_enum = ACTION_SIDE.get(card.action, TriggerSide.NONE)
    quantity = abs(card.requested_delta_qty)
    if side_enum is TriggerSide.NONE:
        status = "NON_ACTION"
    elif quantity == 0:
        status = "ZERO_QUANTITY"
    elif card.execution_window is not ExecutionWindow.AT_OPEN:
        status = "NOT_AT_OPEN"
    elif side_enum.value not in included_directions:
        status = "DIRECTION_OUT_OF_SCOPE"
    elif card.action.value not in included_actions:
        status = "ACTION_OUT_OF_SCOPE"
    elif require_risk_exit_contract and not _matches_risk_exit_contract(card):
        status = "ACTION_CONTRACT_OUT_OF_SCOPE"
    elif card.cost_policy_sha256 != COST_POLICY_SHA256:
        status = "COST_POLICY_INCOMPATIBLE"
    elif card.target_trade_date < coverage_start:
        status = "MINUTE_COVERAGE_BEFORE_START"
    elif card.target_trade_date > coverage_end:
        status = "MINUTE_COVERAGE_PENDING"
    elif not _date_in_instrument_spans(card.canonical_symbol, card.target_trade_date, instrument_spans):
        status = "MINUTE_UNIVERSE_EXCLUDED"
    else:
        status = "ELIGIBLE"
    return ProspectiveCardItemV1(
        card_id=card.card_id,
        card_set_id=card.card_set_id,
        card_set_ref_role=card_set_ref_role,
        card_artifact_sha256=card_hash,
        card_issued_event_sha256=canonical_sha256(dict(event)),
        canonical_symbol=card.canonical_symbol,
        target_trade_date=card.target_trade_date,
        action=card.action.value,
        execution_window=card.execution_window.value,
        side=side_enum.value,
        quantity=quantity,
        cost_policy_sha256=card.cost_policy_sha256,
        population_status=status,
    )


def _evaluate_card(
    item: ProspectiveCardItemV1,
    *,
    request: FrozenL4b1RequestV1,
    calendar: tuple[str, ...],
) -> MinuteExecutionCardResultV1:
    side = TriggerSide(item.side)
    indexes = _window_indexes(calendar, item.target_trade_date)
    if len(indexes) != len(WINDOW_TIMES):
        return _result(item, "DATA_ERROR_MINUTE_CALENDAR", "MINUTE_WINDOW_CALENDAR_INCOMPLETE")
    arrays: dict[str, np.ndarray] = {}
    for field in REQUIRED_MINUTE_FIELDS:
        role = _feature_role(item.canonical_symbol, field)
        try:
            arrays[field] = _read_bin_values(Path(request.source_refs[role].artifact_uri), indexes)
        except (OSError, ValueError):
            return _result(item, "DATA_ERROR_REQUIRED_FIELD", f"MINUTE_FIELD_INVALID:{field}")
    factor = arrays["factor"].astype(float)
    if not np.isfinite(factor).all() or np.any(factor <= 0):
        return _result(item, "DATA_ERROR_FACTOR_INVALID", "MINUTE_FACTOR_INVALID")
    for field in ("open", "high", "low", "close", "volume", "amount", "up_limit_price", "down_limit_price"):
        if not np.isfinite(arrays[field]).all():
            return _result(item, "DATA_ERROR_REQUIRED_FIELD", f"MINUTE_FIELD_NONFINITE:{field}")
    flags = np.column_stack((arrays["limit_up"], arrays["limit_down"])).astype(float)
    if not np.isfinite(flags).all() or not np.isin(flags, (0.0, 1.0)).all():
        return _result(item, "DATA_ERROR_REQUIRED_FIELD", "MINUTE_LIMIT_FLAG_INVALID")
    raw_open = arrays["open"].astype(float) / factor
    raw_high = arrays["high"].astype(float) / factor
    raw_low = arrays["low"].astype(float) / factor
    raw_close = arrays["close"].astype(float) / factor
    if (
        np.any(raw_open <= 0)
        or np.any(raw_high <= 0)
        or np.any(raw_low <= 0)
        or np.any(raw_close <= 0)
        or np.any(raw_high + LIMIT_TOLERANCE < raw_low)
    ):
        return _result(item, "DATA_ERROR_PRICE_BASIS", "MINUTE_RAW_PRICE_INVALID")
    up_limit = arrays["up_limit_price"].astype(float)
    down_limit = arrays["down_limit_price"].astype(float)
    if np.any(up_limit <= 0) or np.any(down_limit <= 0) or np.any(up_limit <= down_limit):
        return _result(item, "DATA_ERROR_PRICE_BASIS", "MINUTE_RAW_LIMIT_INVALID")

    benchmark_price = float(raw_open[0])
    if side is TriggerSide.BUY and benchmark_price >= up_limit[0] - LIMIT_TOLERANCE:
        return _result(item, "BENCHMARK_NO_FILL_LIMIT_UP", "LIMIT_UP_BUY_BLOCKED_AT_OPEN")
    if side is TriggerSide.SELL and benchmark_price <= down_limit[0] + LIMIT_TOLERANCE:
        return _result(item, "BENCHMARK_NO_FILL_LIMIT_DOWN", "LIMIT_DOWN_SELL_BLOCKED_AT_OPEN")

    direction_blocked = arrays["limit_up"] >= 0.5 if side is TriggerSide.BUY else arrays["limit_down"] >= 0.5
    raw_volume = arrays["volume"].astype(float) * factor
    amount = arrays["amount"].astype(float)
    if raw_volume[0] + 1e-9 < item.quantity:
        return _result(
            item,
            "BENCHMARK_NO_FILL_VOLUME_BELOW_QUANTITY",
            "OPENING_MINUTE_RAW_VOLUME_BELOW_CARD_QUANTITY",
            challenger_raw_volume_shares=float(raw_volume[0]),
        )
    valid_turnover = (~direction_blocked) & (raw_volume > 0) & (amount > 0)
    if not valid_turnover.any():
        if direction_blocked.all():
            status = "CHALLENGER_NO_FILL_LIMIT_UP" if side is TriggerSide.BUY else "CHALLENGER_NO_FILL_LIMIT_DOWN"
            reason = "LIMIT_UP_BUY_BLOCKED_FOR_WINDOW" if side is TriggerSide.BUY else "LIMIT_DOWN_SELL_BLOCKED_FOR_WINDOW"
            return _result(item, status, reason)
        return _result(item, "CHALLENGER_NO_FILL_NO_VOLUME", "NO_DIRECTIONALLY_TRADABLE_WINDOW_VOLUME")
    available_volume = float(raw_volume[valid_turnover].sum())
    if available_volume + 1e-9 < item.quantity:
        return _result(
            item,
            "CHALLENGER_NO_FILL_VOLUME_BELOW_QUANTITY",
            "WINDOW_RAW_VOLUME_BELOW_CARD_QUANTITY",
            challenger_raw_volume_shares=available_volume,
        )
    challenger_price = float(amount[valid_turnover].sum() / raw_volume[valid_turnover].sum())
    raw_min = float(raw_low[valid_turnover].min())
    raw_max = float(raw_high[valid_turnover].max())
    if not math.isfinite(challenger_price) or challenger_price <= 0 or not (
        raw_min - LIMIT_TOLERANCE <= challenger_price <= raw_max + LIMIT_TOLERANCE
    ):
        return _result(item, "DATA_ERROR_PRICE_BASIS", "MINUTE_RAW_VWAP_OUTSIDE_WINDOW_RANGE")

    benchmark_price_decimal = Decimal(str(benchmark_price))
    challenger_price_decimal = Decimal(str(challenger_price))
    quantity = Decimal(item.quantity)
    benchmark_gross = benchmark_price_decimal * quantity
    challenger_gross = challenger_price_decimal * quantity
    benchmark_cost = component_cost_for_parent_notionals(side=side, notionals=(benchmark_gross,))["total"]
    challenger_cost = component_cost_for_parent_notionals(side=side, notionals=(challenger_gross,))["total"]
    if side is TriggerSide.BUY:
        improvement = (benchmark_gross + benchmark_cost) - (challenger_gross + challenger_cost)
    else:
        improvement = (challenger_gross - challenger_cost) - (benchmark_gross - benchmark_cost)
    improvement_bps = improvement / benchmark_gross * Decimal("10000")
    return MinuteExecutionCardResultV1(
        card_id=item.card_id,
        canonical_symbol=item.canonical_symbol,
        target_trade_date=item.target_trade_date,
        side=item.side,
        quantity=item.quantity,
        status="PAIRED",
        reason_code="PAIRED_EXECUTION_COUNTERFACTUAL_AVAILABLE",
        benchmark_price_raw=benchmark_price,
        challenger_price_raw=challenger_price,
        benchmark_cost_cny=float(benchmark_cost),
        challenger_cost_cny=float(challenger_cost),
        benchmark_gross_notional_cny=float(benchmark_gross),
        challenger_gross_notional_cny=float(challenger_gross),
        challenger_raw_volume_shares=available_volume,
        net_improvement_cny=float(improvement),
        net_improvement_bps=float(improvement_bps),
    )


def _summarize_direction(
    side: Literal["BUY", "SELL"],
    *,
    request: FrozenL4b1RequestV1,
    results: Sequence[MinuteExecutionCardResultV1],
) -> DirectionSummaryV1:
    side_rows = [row for row in results if row.side == side]
    paired = [row for row in side_rows if row.status == "PAIRED"]
    data_errors = [row for row in side_rows if row.status.startswith("DATA_ERROR_")]
    market_no_fill = [row for row in side_rows if "NO_FILL" in row.status]
    by_date: dict[date, list[float]] = defaultdict(list)
    for row in paired:
        assert row.net_improvement_bps is not None
        by_date[row.target_trade_date].append(row.net_improvement_bps)
    daily = np.asarray([np.mean(by_date[key]) for key in sorted(by_date)], dtype=float)
    point = float(daily.mean()) if len(daily) else None
    reasons: list[str] = []
    if len(paired) < request.minimum_paired_cards_per_side:
        reasons.append("PAIRED_CARD_COUNT_BELOW_FROZEN_MINIMUM")
    if len(daily) < request.minimum_paired_trade_dates_per_side:
        reasons.append("PAIRED_TRADE_DATE_COUNT_BELOW_FROZEN_MINIMUM")
    if reasons:
        if data_errors:
            reasons.append("UNEXPLAINED_MINUTE_DATA_ERROR_PRESENT")
        return DirectionSummaryV1(
            side=side,
            eligible_card_count=len(side_rows),
            paired_card_count=len(paired),
            paired_trade_date_count=len(daily),
            market_no_fill_count=len(market_no_fill),
            data_error_count=len(data_errors),
            point_estimate_bps=point,
            effect_evidence="INSUFFICIENT_DATA",
            power_status="UNDERPOWERED",
            evidence_reason_codes=tuple(reasons),
        )
    nominal = _moving_block_interval(
        daily,
        alpha=request.alpha,
        block_length=request.bootstrap_block_observed_trade_dates,
        repetitions=request.bootstrap_repetitions,
        seed=request.bootstrap_seed + LEGACY_DIRECTION_ORDER.index(side),
    )
    adjusted_alpha = request.alpha / request.familywise_hypothesis_count
    adjusted = _moving_block_interval(
        daily,
        alpha=adjusted_alpha,
        block_length=request.bootstrap_block_observed_trade_dates,
        repetitions=request.bootstrap_repetitions,
        seed=request.bootstrap_seed + LEGACY_DIRECTION_ORDER.index(side),
    )
    if data_errors:
        evidence = "INCONCLUSIVE"
        reasons.append("UNEXPLAINED_MINUTE_DATA_ERROR_PRESENT")
    elif adjusted.lower_bps > 0:
        evidence = "SUPPORTED"
        reasons.append("ADJUSTED_LOWER_BOUND_ABOVE_ZERO")
    elif adjusted.upper_bps < 0:
        evidence = "NEGATIVE"
        reasons.append("ADJUSTED_UPPER_BOUND_BELOW_ZERO")
    else:
        evidence = "INCONCLUSIVE"
        reasons.append("ADJUSTED_INTERVAL_CROSSES_ZERO")
    return DirectionSummaryV1(
        side=side,
        eligible_card_count=len(side_rows),
        paired_card_count=len(paired),
        paired_trade_date_count=len(daily),
        market_no_fill_count=len(market_no_fill),
        data_error_count=len(data_errors),
        point_estimate_bps=point,
        nominal_interval=nominal,
        adjusted_interval=adjusted,
        effect_evidence=evidence,
        power_status="ADEQUATE",
        evidence_reason_codes=tuple(reasons),
    )


def _moving_block_interval(
    values: np.ndarray, *, alpha: float, block_length: int, repetitions: int, seed: int
) -> IntervalV1:
    if len(values) < 2 or not np.isfinite(values).all():
        raise ValueError("moving-block interval requires at least two finite daily values")
    block = min(block_length, len(values))
    blocks_needed = math.ceil(len(values) / block)
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, len(values), size=(repetitions, blocks_needed))
    offsets = np.arange(block)
    indexes = (starts[..., None] + offsets) % len(values)
    samples = values[indexes.reshape(repetitions, -1)[:, : len(values)]]
    means = samples.mean(axis=1)
    lower, upper = np.quantile(means, (alpha / 2, 1 - alpha / 2))
    return IntervalV1(lower_bps=float(lower), upper_bps=float(upper), alpha=alpha)


def _build_receipt(
    *,
    request: FrozenL4b1RequestV1,
    results: tuple[MinuteExecutionCardResultV1, ...],
    summaries: tuple[DirectionSummaryV1, ...],
) -> L4b1ReceiptV1:
    eligible_count = request.population_counts.get("ELIGIBLE", 0)
    paired_count = sum(item.paired_card_count for item in summaries)
    selected = tuple(item.side for item in summaries if item.effect_evidence == "SUPPORTED")
    if eligible_count == 0:
        status = "INSUFFICIENT_PROSPECTIVE_ACTION_CARDS"
    elif paired_count == 0:
        status = "NO_PAIRED_EXECUTIONS"
    elif all(item.effect_evidence == "INSUFFICIENT_DATA" for item in summaries):
        status = "INSUFFICIENT_DATA"
    elif selected:
        status = "SUPPORTED"
    elif all(item.effect_evidence == "NEGATIVE" for item in summaries):
        status = "NEGATIVE"
    else:
        status = "INCONCLUSIVE"
    counts = Counter(row.status for row in results)
    counts.update({f"POPULATION_{key}": value for key, value in request.population_counts.items()})
    functional = {
        "schema_version": RECEIPT_SCHEMA,
        "request_sha256": request.request_sha256,
        "repository_commit": request.repository_commit,
        "minute_source_identity_sha256": request.minute_source_identity_sha256,
        "population_sha256": request.population_sha256,
        "card_results_sha256": canonical_sha256(results),
        "status": status,
        "direction_summaries": summaries,
        "selected_sides": selected,
        "coverage_counts": dict(sorted(counts.items())),
        "familywise_hypothesis_count": request.familywise_hypothesis_count,
        "runtime_policy_written": False,
        "card_or_event_written": False,
        "order_written": False,
        "global_registry_written": False,
        "current_route_written": False,
        "l1_l1a_gate_applied": False,
        "market_impact_assumption": "NO_MARKET_IMPACT_ASSUMED",
    }
    receipt_sha = canonical_sha256(functional)
    return L4b1ReceiptV1(
        receipt_id=f"ptl4b1rcpt_{receipt_sha[:24]}",
        receipt_sha256=receipt_sha,
        created_at=datetime.now(timezone.utc),
        **functional,
    )


def _publish_bundle(
    *,
    request: FrozenL4b1RequestV1,
    results: tuple[MinuteExecutionCardResultV1, ...],
    receipt: L4b1ReceiptV1,
) -> Path:
    root = Path(request.output_root) / "l4b1_execution_window_bundles"
    root.mkdir(parents=True, exist_ok=True)
    results_sha = canonical_sha256(results)
    bundle_id = canonical_sha256(
        {"request_sha256": request.request_sha256, "receipt_sha256": receipt.receipt_sha256, "results_sha256": results_sha}
    )
    destination = root / bundle_id
    if destination.exists():
        _inspect_bundle(destination)
        return destination
    temporary = Path(tempfile.mkdtemp(prefix=f".{request.request_id}.", dir=root))
    try:
        _write_json(temporary / "request.json", request.model_dump(mode="json"))
        _write_json(temporary / "card_results.json", [item.model_dump(mode="json") for item in results])
        _write_json(temporary / "receipt.json", receipt.model_dump(mode="json"))
        manifest = {
            "schema_version": BUNDLE_SCHEMA,
            "bundle_id": bundle_id,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
            "card_results_sha256": results_sha,
            "repository_commit": request.repository_commit,
            "members": {
                name: _sha256_file(temporary / name)
                for name in ("request.json", "card_results.json", "receipt.json")
            },
        }
        _write_json(temporary / "manifest.json", manifest)
        try:
            os.replace(temporary, destination)
        except OSError:
            if not destination.exists():
                raise
            _inspect_bundle(destination)
        return destination
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def _inspect_bundle(path: Path) -> dict[str, Any]:
    expected = {"request.json", "card_results.json", "receipt.json", "manifest.json"}
    if not path.is_dir() or {item.name for item in path.iterdir()} != expected:
        _raise("L4b-1 bundle members are invalid", "POSITION_TIMING_L4B1_BUNDLE_INVALID", path=path.as_posix())
    request = _read_request(path / "request.json")
    try:
        raw_results = _read_json(path / "card_results.json", "POSITION_TIMING_L4B1_BUNDLE_INVALID")
        results = tuple(MinuteExecutionCardResultV1.model_validate(item) for item in raw_results)
        receipt = L4b1ReceiptV1.model_validate(
            _read_json(path / "receipt.json", "POSITION_TIMING_L4B1_BUNDLE_INVALID")
        )
        manifest = _read_json(path / "manifest.json", "POSITION_TIMING_L4B1_BUNDLE_INVALID")
    except (TypeError, ValidationError) as exc:
        _raise("L4b-1 bundle contract is invalid", "POSITION_TIMING_L4B1_BUNDLE_INVALID", error=str(exc))
    bundle_id = canonical_sha256(
        {
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
            "results_sha256": canonical_sha256(results),
        }
    )
    members = {name: _sha256_file(path / name) for name in ("request.json", "card_results.json", "receipt.json")}
    if (
        path.name != bundle_id
        or manifest.get("schema_version") != BUNDLE_SCHEMA
        or manifest.get("bundle_id") != bundle_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("receipt_sha256") != receipt.receipt_sha256
        or manifest.get("card_results_sha256") != canonical_sha256(results)
        or manifest.get("members") != members
        or receipt.request_sha256 != request.request_sha256
        or receipt.card_results_sha256 != canonical_sha256(results)
    ):
        _raise("L4b-1 bundle identity mismatch", "POSITION_TIMING_L4B1_BUNDLE_INVALID", path=path.as_posix())
    return {"request": request, "results": results, "receipt": receipt, "manifest": manifest}


def _append_registry(*, request: FrozenL4b1RequestV1, bundle_path: Path, receipt: L4b1ReceiptV1) -> bool:
    path = Path(request.registry_path)
    record = L4b1RegistryRecordV1(
        record_id=f"ptl4b1rec_{request.request_sha256[:24]}",
        request_sha256=request.request_sha256,
        bundle_id=bundle_path.name,
        receipt_sha256=receipt.receipt_sha256,
        repository_commit=request.repository_commit,
        status=receipt.status,
        selected_sides=receipt.selected_sides,
        created_at=receipt.created_at,
    )
    lock = path.with_suffix(path.suffix + ".lock")
    with _exclusive_file_lock(lock):
        existing: dict[str, L4b1RegistryRecordV1] = {}
        if path.exists():
            for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
                if not line.strip():
                    continue
                try:
                    item = L4b1RegistryRecordV1.model_validate_json(line)
                except ValidationError as exc:
                    _raise(
                        "L4b-1 registry contains an invalid record",
                        "POSITION_TIMING_L4B1_REGISTRY_INVALID",
                        line_number=line_number,
                        error=str(exc),
                    )
                if item.request_sha256 in existing:
                    _raise(
                        "L4b-1 registry contains duplicate request identity",
                        "POSITION_TIMING_L4B1_REGISTRY_INVALID",
                        request_sha256=item.request_sha256,
                    )
                existing[item.request_sha256] = item
        prior = existing.get(request.request_sha256)
        if prior is not None:
            if canonical_sha256(prior) != canonical_sha256(record):
                _raise(
                    "L4b-1 registry request identity conflicts",
                    "POSITION_TIMING_L4B1_REGISTRY_CONFLICT",
                    request_sha256=request.request_sha256,
                )
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as handle:
            handle.write(canonical_json_bytes(record) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        return True


def _verify_run_environment(request: FrozenL4b1RequestV1) -> None:
    if _repository_commit(Path(request.repository_root)) != request.repository_commit:
        _raise("L4b-1 repository commit drift", "POSITION_TIMING_L4B1_REPOSITORY_DRIFT")
    if _repository_dirty_paths(Path(request.repository_root)):
        _raise("L4b-1 run requires a clean repository", "POSITION_TIMING_L4B1_REPOSITORY_DIRTY")
    for key, reference in request.source_refs.items():
        path = Path(reference.artifact_uri)
        if not path.is_file() or path.stat().st_size != reference.size_bytes or _sha256_file(path) != reference.sha256:
            _raise(
                "L4b-1 frozen source drift",
                "POSITION_TIMING_L4B1_SOURCE_DRIFT",
                source_role=key,
                path=path.as_posix(),
            )
    store = PositionTimingArtifactStore(request.timing_artifact_root)
    events = {item["card_id"]: item for item in store.list_events(event_type="CARD_ISSUED")}
    instrument_spans = _load_instrument_spans(Path(request.source_refs["minute_instruments"].artifact_uri))
    card_sets: dict[str, PositionTimingCardSetV1] = {}
    for item in request.population_items:
        reference = request.source_refs[item.card_set_ref_role]
        card_set = card_sets.get(item.card_set_id)
        if card_set is None:
            card_set = PositionTimingCardSetV1.model_validate_json(Path(reference.artifact_uri).read_text(encoding="utf-8"))
            card_sets[item.card_set_id] = card_set
        card = next((value for value in card_set.cards if value.card_id == item.card_id), None)
        event = events.get(item.card_id)
        if card is None or event is None:
            _raise("L4b-1 card/event disappeared", "POSITION_TIMING_L4B1_CARD_EVENT_IDENTITY_MISMATCH")
        replayed = _population_item(
            card=card,
            event=event,
            card_set_ref_role=item.card_set_ref_role,
            coverage_start=request.minute_coverage_start,
            coverage_end=request.minute_coverage_end,
            instrument_spans=instrument_spans,
            included_directions=_directions_for_family(request.familywise_hypothesis_count),
            included_actions=_actions_for_family(request.familywise_hypothesis_count),
            require_risk_exit_contract=request.familywise_hypothesis_count == 1,
        )
        if replayed != item:
            _raise(
                "L4b-1 prospective population drift",
                "POSITION_TIMING_L4B1_CARD_EVENT_IDENTITY_MISMATCH",
                card_id=item.card_id,
            )


def _find_existing_bundle(request: FrozenL4b1RequestV1) -> Path | None:
    root = Path(request.output_root) / "l4b1_execution_window_bundles"
    if not root.exists():
        return None
    matches: list[Path] = []
    for path in root.iterdir():
        if not path.is_dir() or not (path / "manifest.json").is_file():
            continue
        manifest = _read_json(path / "manifest.json", "POSITION_TIMING_L4B1_BUNDLE_INVALID")
        if manifest.get("request_sha256") == request.request_sha256:
            matches.append(path)
    if len(matches) > 1:
        _raise("request maps to multiple L4b-1 bundles", "POSITION_TIMING_L4B1_BUNDLE_INVALID")
    return matches[0] if matches else None


def _run_response(
    request: FrozenL4b1RequestV1,
    bundle: Path,
    receipt: L4b1ReceiptV1,
    *,
    appended: bool,
    exact_retry: bool,
) -> dict[str, Any]:
    return {
        "status": receipt.status,
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "bundle_id": bundle.name,
        "bundle_path": bundle.as_posix(),
        "receipt_sha256": receipt.receipt_sha256,
        "selected_sides": list(receipt.selected_sides),
        "registry_appended": appended,
        "exact_retry": exact_retry,
        "runtime_policy_written": False,
        "card_or_event_written": False,
        "order_written": False,
    }


def _result(
    item: ProspectiveCardItemV1,
    status: CardResultStatus,
    reason_code: str,
    *,
    challenger_raw_volume_shares: float | None = None,
) -> MinuteExecutionCardResultV1:
    return MinuteExecutionCardResultV1(
        card_id=item.card_id,
        canonical_symbol=item.canonical_symbol,
        target_trade_date=item.target_trade_date,
        side=item.side,
        quantity=item.quantity,
        status=status,
        reason_code=reason_code,
        challenger_raw_volume_shares=challenger_raw_volume_shares,
    )


def _window_indexes(calendar: Sequence[str], target: date) -> tuple[int, ...]:
    prefix = target.isoformat() + " "
    expected = {prefix + value for value in WINDOW_TIMES}
    matches = tuple(index for index, value in enumerate(calendar) if value in expected)
    return matches if tuple(calendar[index] for index in matches) == tuple(prefix + value for value in WINDOW_TIMES) else ()


def _directions_for_family(familywise_hypothesis_count: int) -> tuple[Literal["BUY", "SELL"], ...]:
    if familywise_hypothesis_count == 1:
        return ACTIVE_DIRECTION_ORDER
    if familywise_hypothesis_count == 2:
        return LEGACY_DIRECTION_ORDER
    raise ValueError("unsupported L4b-1 hypothesis family size")


def _actions_for_family(familywise_hypothesis_count: int) -> tuple[str, ...]:
    if familywise_hypothesis_count == 1:
        return (TimingAction.EXIT.value,)
    if familywise_hypothesis_count == 2:
        return tuple(action.value for action in ACTION_SIDE)
    raise ValueError("unsupported L4b-1 hypothesis family size")


def _matches_risk_exit_contract(card: PositionTimingCardV1) -> bool:
    if len(card.triggers) != 1:
        return False
    trigger = card.triggers[0]
    return (
        card.action is TimingAction.EXIT
        and card.execution_window is ExecutionWindow.AT_OPEN
        and card.requested_delta_qty < 0
        and trigger.branch == "RISK_EXIT_AT_OPEN"
        and trigger.side is TriggerSide.SELL
        and trigger.operator is TriggerOperator.ALWAYS
        and trigger.planned_delta_qty == card.requested_delta_qty
        and trigger.conditions.get("sell_reason") == "risk_exit"
    )


def _read_bin_values(path: Path, indexes: Sequence[int]) -> np.ndarray:
    values = np.fromfile(path, dtype="<f4")
    if len(values) < 2 or not np.isfinite(values[0]) or values[0] < 0 or float(values[0]).is_integer() is False:
        raise ValueError(f"invalid Qlib bin header: {path}")
    start = int(values[0])
    result = np.full(len(indexes), np.nan, dtype=np.float64)
    for output_index, calendar_index in enumerate(indexes):
        position = calendar_index - start + 1
        if 1 <= position < len(values):
            result[output_index] = float(values[position])
    return result


def _load_calendar(path: Path) -> tuple[str, ...]:
    values = tuple(line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    if not values or values != tuple(sorted(values)) or len(values) != len(set(values)):
        _raise("minute calendar is invalid", "POSITION_TIMING_L4B1_MINUTE_SOURCE_INVALID")
    return values


def _load_instrument_spans(path: Path) -> dict[str, tuple[tuple[date, date], ...]]:
    spans: dict[str, list[tuple[date, date]]] = defaultdict(list)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        for line_number, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) != 3:
                raise ValueError(f"line {line_number} must have three tab-separated fields")
            symbol = parts[0].strip().upper()
            if len(symbol) != 9 or symbol[6:] not in {".SH", ".SZ"} or not symbol[:6].isdigit():
                raise ValueError(f"line {line_number} has invalid canonical symbol")
            start = datetime.fromisoformat(parts[1].strip()).date()
            end = datetime.fromisoformat(parts[2].strip()).date()
            if end < start:
                raise ValueError(f"line {line_number} has inverted span")
            spans[symbol].append((start, end))
    except (OSError, ValueError) as exc:
        _raise(
            "minute instrument spans are invalid",
            "POSITION_TIMING_L4B1_MINUTE_SOURCE_INVALID",
            path=path.as_posix(),
            error=str(exc),
        )
    if not spans:
        _raise(
            "minute instrument spans are empty",
            "POSITION_TIMING_L4B1_MINUTE_SOURCE_INVALID",
            path=path.as_posix(),
        )
    return {symbol: tuple(sorted(values)) for symbol, values in spans.items()}


def _date_in_instrument_spans(
    symbol: str,
    target: date,
    spans: Mapping[str, tuple[tuple[date, date], ...]],
) -> bool:
    return any(start <= target <= end for start, end in spans.get(symbol, ()))


def _card_set_path(root: Path, card_set: PositionTimingCardSetV1) -> Path:
    folder = root / "cards" / card_set.decision_trade_date.isoformat() / card_set.card_set_id
    expected = folder / f"card_set-{canonical_sha256(card_set)}.json"
    if not expected.is_file():
        _raise(
            "validated card set does not have its content-addressed file",
            "POSITION_TIMING_L4B1_CARD_EVENT_IDENTITY_MISMATCH",
            card_set_id=card_set.card_set_id,
        )
    return expected


def _feature_role(symbol: str, field: str) -> str:
    return f"minute_feature:{symbol}:{field}"


def _file_ref(path: Path, role: str) -> FileReferenceV1:
    if not path.is_file():
        _raise("required L4b-1 source file is missing", "POSITION_TIMING_L4B1_SOURCE_MISSING", path=path.as_posix())
    return FileReferenceV1(role=role, artifact_uri=path.resolve().as_posix(), sha256=_sha256_file(path), size_bytes=path.stat().st_size)


def _validate_owned_output_paths(
    *,
    timing_root: Path,
    output_root: Path,
    registry_path: Path,
    request_path: Path,
) -> None:
    expected_output_root = (timing_root / "research").resolve()
    expected_registry = (timing_root / "research_registry" / "timing_execution_window_registry_v1.jsonl").resolve()
    expected_request_root = (expected_output_root / "l4b1_requests").resolve()
    if output_root != expected_output_root:
        _raise(
            "L4b-1 output root must be timing-owned research",
            "POSITION_TIMING_L4B1_OUTPUT_BOUNDARY_INVALID",
            expected=expected_output_root.as_posix(),
            observed=output_root.as_posix(),
        )
    if registry_path != expected_registry:
        _raise(
            "L4b-1 registry path must be the timing-owned execution-window registry",
            "POSITION_TIMING_L4B1_OUTPUT_BOUNDARY_INVALID",
            expected=expected_registry.as_posix(),
            observed=registry_path.as_posix(),
        )
    if request_path.parent.resolve() != expected_request_root:
        _raise(
            "L4b-1 request must be written under timing-owned l4b1_requests",
            "POSITION_TIMING_L4B1_OUTPUT_BOUNDARY_INVALID",
            expected_parent=expected_request_root.as_posix(),
            observed_parent=request_path.parent.resolve().as_posix(),
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, reason_code: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise("JSON source cannot be read", reason_code, path=path.as_posix(), error=str(exc))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = canonical_json_bytes(payload) + b"\n"
    with path.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _write_immutable_request(path: Path, request: FrozenL4b1RequestV1) -> None:
    data = canonical_json_bytes(request) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = _read_request(path)
        if existing.request_sha256 != request.request_sha256:
            _raise("request path contains different content", "POSITION_TIMING_L4B1_REQUEST_CONFLICT")
        return
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temp_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_request(path: Path) -> FrozenL4b1RequestV1:
    try:
        return FrozenL4b1RequestV1.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValidationError) as exc:
        _raise("L4b-1 request is invalid", "POSITION_TIMING_L4B1_REQUEST_INVALID", path=path.as_posix(), error=str(exc))


def _repository_commit(root: Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    commit = result.stdout.strip().lower()
    if result.returncode != 0 or len(commit) != 40 or any(char not in "0123456789abcdef" for char in commit):
        _raise("repository commit cannot be resolved", "POSITION_TIMING_L4B1_REPOSITORY_INVALID")
    return commit


def _repository_dirty_paths(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain=v1", "--untracked-files=all"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        _raise("repository status cannot be resolved", "POSITION_TIMING_L4B1_REPOSITORY_INVALID")
    return [line[3:] for line in result.stdout.splitlines() if line.strip()]


def _raise(message: str, reason_code: str, **context: Any) -> NoReturn:
    raise PositionTimingL4b1Error(message, reason_code=reason_code, context=context)


class _ArgumentError(ValueError):
    pass


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _ArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(description="Offline position-timing L4b-1 minute execution-window audit")
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--timing-artifact-root", required=True)
    prepare.add_argument("--minute-provider-root", required=True)
    prepare.add_argument("--repository-root", required=True)
    prepare.add_argument("--output-root", required=True)
    prepare.add_argument("--registry-path", required=True)
    prepare.add_argument("--output", required=True)
    run = commands.add_parser("run")
    run.add_argument("--request", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        if args.command == "prepare":
            request = prepare_l4b1_request(
                timing_artifact_root=args.timing_artifact_root,
                minute_provider_root=args.minute_provider_root,
                repository_root=args.repository_root,
                output_root=args.output_root,
                registry_path=args.registry_path,
                output_path=args.output,
            )
            result = {
                "status": "FROZEN_REQUEST",
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "repository_commit": request.repository_commit,
                "population_counts": request.population_counts,
                "output_path": Path(args.output).resolve().as_posix(),
            }
        elif args.command == "run":
            result = run_l4b1_audit(args.request)
        else:
            result = inspect_l4b1_bundle(args.bundle)
        exit_code = 0
    except PositionTimingL4b1Error as exc:
        result = exc.as_dict()
        exit_code = 1
    except (_ArgumentError, ValidationError, ValueError, KeyError) as exc:
        result = {
            "status": "failed",
            "reason_code": "POSITION_TIMING_L4B1_REQUEST_INVALID",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    except Exception as exc:  # pragma: no cover - last-resort typed CLI boundary
        result = {
            "status": "failed",
            "reason_code": "POSITION_TIMING_L4B1_UNEXPECTED_FAILURE",
            "message": str(exc),
            "context": {"error_type": type(exc).__name__},
        }
        exit_code = 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "FrozenL4b1RequestV1",
    "L4b1ReceiptV1",
    "MinuteExecutionCardResultV1",
    "PositionTimingL4b1Error",
    "inspect_l4b1_bundle",
    "main",
    "prepare_l4b1_request",
    "run_l4b1_audit",
]
