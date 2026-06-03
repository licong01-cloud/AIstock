"""Advisory watchlist lifecycle and daily review service.

The service is deliberately advisory-only: it writes watchlist lifecycle state
and append-only review facts, never OMS/broker/Paper ledger records.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from math import isfinite
from typing import Any, Iterable, Mapping, Protocol

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.selection_center.price_guidance import GUIDANCE_STATUS_RULE_DEFAULT, PRICE_BASIS_RAW
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, RuntimeConfigInvalidError
from backend.services.trading_core.exit_guard import ExitGuardContext, ExitGuardPolicy, evaluate as evaluate_exit_guard
from backend.services.trading_core.price_guard import (
    HOLD,
    STOP_LOSS_DEFERRED_T1,
    WAITING_FOR_PRICE_GUARD_INPUT,
)


LIFECYCLE_CANDIDATE = "CANDIDATE"
LIFECYCLE_ENTERED = "ENTERED"
LIFECYCLE_HOLDING = "HOLDING"
LIFECYCLE_EXITED = "EXITED"
LEGAL_TRANSITIONS = {
    LIFECYCLE_CANDIDATE: {LIFECYCLE_ENTERED},
    LIFECYCLE_ENTERED: {LIFECYCLE_HOLDING, LIFECYCLE_EXITED},
    LIFECYCLE_HOLDING: {LIFECYCLE_HOLDING, LIFECYCLE_EXITED},
    LIFECYCLE_EXITED: set(),
}
REVIEW_LAYER = "advisory"


@dataclass(frozen=True)
class AdvisoryWatchlistItem:
    watchlist_item_id: int
    code: str
    lifecycle_status: str = LIFECYCLE_ENTERED
    advisory_enabled: bool = True
    planned_entry_price: float | None = None
    actual_entry_price: float | None = None
    actual_entry_date: date | None = None
    exited_at: datetime | None = None
    exit_reason: str | None = None
    entry_band_json: dict[str, Any] | None = None
    base_factor: float | None = None
    latest_factor: float | None = None
    high_since_entry: float | None = None
    days_since_entry: int | None = None


@dataclass(frozen=True)
class DailySelectionEvidenceSnapshot:
    evidence_id: str | None
    code: str
    trade_date: date
    score: float | None = None
    rank: int | None = None
    latest_rank_pct: float | None = None
    alpha_decay_confirm_days: int = 0
    feature_availability_ts: datetime | None = None
    package_raw_scores: dict[str, float] = field(default_factory=dict)
    package_raw_ranks: dict[str, int] = field(default_factory=dict)
    package_rank_scores: dict[str, float] = field(default_factory=dict)
    package_presence: dict[str, str] = field(default_factory=dict)
    package_weights: dict[str, float] = field(default_factory=dict)
    normalized_package_weights: dict[str, float] = field(default_factory=dict)
    support_count: int | None = None
    rank_dispersion: int | None = None
    fusion_score: float | None = None
    fusion_rank: int | None = None
    fusion_policy_sha256: str | None = None


@dataclass(frozen=True)
class AdvisoryMarketSnapshot:
    code: str
    trade_date: date
    current_price: float | None
    suspend_status: str | None = None
    high_since_entry: float | None = None
    latest_factor: float | None = None


@dataclass(frozen=True)
class AdvisoryReviewRecord:
    watchlist_item_id: int
    code: str
    trade_date: date
    evidence_id: str | None
    score: float | None
    rank: int | None
    current_price: float | None
    entry_band_json: dict[str, Any] | None
    stop_price: float | None
    take_price: float | None
    action: str
    reason_code: str
    policy_sha256: str
    guidance_status: str
    price_basis: str
    feature_availability_ts: datetime
    t1_note: str | None = None
    layer: str = REVIEW_LAYER
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class PackageSelectionEvidence:
    package_id: str
    evidence_id: str | None
    code: str
    trade_date: date
    score: float | None = None
    rank: int | None = None
    candidate_count: int | None = None
    presence: str = "selected_topK"
    feature_availability_ts: datetime | None = None


@dataclass(frozen=True)
class FusionPolicy:
    method: str = "weighted_rank_fusion"
    package_weights: dict[str, float] = field(default_factory=dict)
    candidate_top_k: int | None = None
    missing_rank_policy: str = "not_selected_zero_score"

    def normalized_weights(self, package_ids: Iterable[str]) -> dict[str, float]:
        ordered = list(package_ids)
        raw = self.package_weights or {package_id: 1.0 for package_id in ordered}
        missing = set(ordered) - set(raw)
        extra = set(raw) - set(ordered)
        if missing or extra:
            raise RuntimeConfigInvalidError(
                "fusion policy package_weights must match package evidence packages",
                context={"missing": sorted(missing), "extra": sorted(extra)},
            )
        weights: dict[str, float] = {}
        for package_id in ordered:
            value = _optional_float(raw.get(package_id))
            if value is None:
                raise RuntimeConfigInvalidError(
                    "fusion policy package weights must be positive finite numbers",
                    context={"package_id": package_id, "weight": raw.get(package_id)},
                )
            weights[package_id] = value
        total = sum(weights.values())
        return {package_id: weight / total for package_id, weight in weights.items()}

    def sha256(self, package_ids: Iterable[str]) -> str:
        ordered = list(package_ids)
        payload = {
            "method": self.method,
            "package_weights": {package_id: self.package_weights.get(package_id, 1.0) for package_id in ordered},
            "normalized_package_weights": self.normalized_weights(ordered),
            "candidate_top_k": self.candidate_top_k,
            "missing_rank_policy": self.missing_rank_policy,
        }
        return _canonical_sha256(payload)


class AdvisoryReviewRepository(Protocol):
    def insert_review_once(self, record: AdvisoryReviewRecord) -> AdvisoryReviewRecord: ...

    def update_item_lifecycle(self, item_id: int, status: str, *, exit_reason: str | None = None) -> None: ...


class InMemoryAdvisoryReviewRepository:
    def __init__(self) -> None:
        self.reviews: dict[tuple[int, date], AdvisoryReviewRecord] = {}
        self.lifecycle_updates: list[dict[str, Any]] = []

    def insert_review_once(self, record: AdvisoryReviewRecord) -> AdvisoryReviewRecord:
        key = (record.watchlist_item_id, record.trade_date)
        existing = self.reviews.get(key)
        if existing is not None:
            return existing
        self.reviews[key] = record
        return record

    def update_item_lifecycle(self, item_id: int, status: str, *, exit_reason: str | None = None) -> None:
        self.lifecycle_updates.append({"item_id": item_id, "status": status, "exit_reason": exit_reason})


class AdvisoryReviewPGRepository:
    def __init__(self, conn_factory: Any | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def insert_review_once(self, record: AdvisoryReviewRecord) -> AdvisoryReviewRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_daily_review (
                        watchlist_item_id, code, trade_date, evidence_id, score, rank,
                        current_price, entry_band_json, stop_price, take_price,
                        action, reason_code, policy_sha256, guidance_status, price_basis,
                        feature_availability_ts, t1_note, layer
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (watchlist_item_id, trade_date) DO NOTHING
                    RETURNING *
                    """,
                    (
                        record.watchlist_item_id,
                        record.code,
                        record.trade_date,
                        record.evidence_id,
                        record.score,
                        record.rank,
                        record.current_price,
                        psycopg2.extras.Json(record.entry_band_json) if record.entry_band_json else None,
                        record.stop_price,
                        record.take_price,
                        record.action,
                        record.reason_code,
                        record.policy_sha256,
                        record.guidance_status,
                        record.price_basis,
                        record.feature_availability_ts,
                        record.t1_note,
                        record.layer,
                    ),
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        """
                        SELECT *
                        FROM app.advisory_daily_review
                        WHERE watchlist_item_id = %s AND trade_date = %s
                        """,
                        (record.watchlist_item_id, record.trade_date),
                    )
                    row = cur.fetchone()
        if row is None:
            raise DataUnavailableError(
                "advisory daily review insert/readback failed",
                context={"watchlist_item_id": record.watchlist_item_id, "trade_date": record.trade_date.isoformat()},
            )
        return record

    def update_item_lifecycle(self, item_id: int, status: str, *, exit_reason: str | None = None) -> None:
        if status == LIFECYCLE_EXITED:
            sql = """
                UPDATE app.watchlist_items
                SET lifecycle_status = %s, exited_at = now(), exit_reason = %s, updated_at = now()
                WHERE id = %s
            """
            params = (status, exit_reason, item_id)
        else:
            sql = """
                UPDATE app.watchlist_items
                SET lifecycle_status = %s, updated_at = now()
                WHERE id = %s
            """
            params = (status, item_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)


class AdvisoryLifecycleService:
    def __init__(self, repository: AdvisoryReviewRepository | None = None) -> None:
        self.repository = repository or AdvisoryReviewPGRepository()

    def transition_status(
        self,
        current_status: str,
        target_status: str,
        *,
        watchlist_item_id: int | None = None,
        exit_reason: str | None = None,
    ) -> str:
        current = _normalize_status(current_status)
        target = _normalize_status(target_status)
        if target not in LEGAL_TRANSITIONS[current]:
            raise InvalidStateTransitionError(
                "illegal advisory lifecycle transition",
                context={"current_status": current, "target_status": target, "watchlist_item_id": watchlist_item_id},
            )
        if watchlist_item_id is not None:
            self.repository.update_item_lifecycle(watchlist_item_id, target, exit_reason=exit_reason)
        return target

    def run_daily_review(
        self,
        *,
        items: Iterable[AdvisoryWatchlistItem],
        evidence_by_code: Mapping[str, DailySelectionEvidenceSnapshot],
        market_by_code: Mapping[str, AdvisoryMarketSnapshot],
        trade_date: date,
        policy: ExitGuardPolicy,
    ) -> list[AdvisoryReviewRecord]:
        records: list[AdvisoryReviewRecord] = []
        for item in items:
            if not item.advisory_enabled or item.lifecycle_status not in {LIFECYCLE_ENTERED, LIFECYCLE_HOLDING}:
                continue
            evidence = evidence_by_code.get(item.code)
            market = market_by_code.get(item.code)
            if market is None:
                raise DataUnavailableError(
                    "advisory daily review requires market snapshot for every active item",
                    context={"code": item.code, "trade_date": trade_date.isoformat()},
                )
            adjusted_entry_cost = adjust_price_for_factor(
                _entry_cost(item),
                base_factor=item.base_factor,
                current_factor=market.latest_factor if market.latest_factor is not None else item.latest_factor,
            )
            t1_eligible = item.actual_entry_date is None or item.actual_entry_date < trade_date
            feature_ts = (
                evidence.feature_availability_ts
                if evidence and evidence.feature_availability_ts
                else datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC)
            )
            if _is_suspended(market.suspend_status):
                decision_reason = WAITING_FOR_PRICE_GUARD_INPUT
                record = AdvisoryReviewRecord(
                    watchlist_item_id=item.watchlist_item_id,
                    code=item.code,
                    trade_date=trade_date,
                    evidence_id=evidence.evidence_id if evidence else None,
                    score=evidence.score if evidence else None,
                    rank=evidence.rank if evidence else None,
                    current_price=market.current_price,
                    entry_band_json=item.entry_band_json,
                    stop_price=None,
                    take_price=None,
                    action="WAITING",
                    reason_code=decision_reason,
                    policy_sha256=_policy_sha(policy),
                    guidance_status=policy.guidance_status or GUIDANCE_STATUS_RULE_DEFAULT,
                    price_basis=policy.price_basis or PRICE_BASIS_RAW,
                    feature_availability_ts=feature_ts,
                    t1_note="suspended_carry",
                )
                records.append(self.repository.insert_review_once(record))
                continue

            ctx = ExitGuardContext(
                actual_entry_cost=adjusted_entry_cost,
                current_price=market.current_price,
                high_since_entry=market.high_since_entry or item.high_since_entry,
                latest_rank=evidence.rank if evidence else None,
                latest_rank_pct=evidence.latest_rank_pct if evidence else None,
                days_since_entry=item.days_since_entry,
                t1_eligible=t1_eligible,
                alpha_decay_confirm_days=evidence.alpha_decay_confirm_days if evidence else 0,
                lifecycle_status=item.lifecycle_status,
                suspend_status=market.suspend_status,
                score=evidence.score if evidence else None,
                rank=evidence.rank if evidence else None,
                evidence_id=evidence.evidence_id if evidence else None,
                price_basis=policy.price_basis or PRICE_BASIS_RAW,
                feature_availability_ts=feature_ts.isoformat(),
            )
            decision = evaluate_exit_guard(ctx, policy)
            record = AdvisoryReviewRecord(
                watchlist_item_id=item.watchlist_item_id,
                code=item.code,
                trade_date=trade_date,
                evidence_id=evidence.evidence_id if evidence else None,
                score=evidence.score if evidence else None,
                rank=evidence.rank if evidence else None,
                current_price=market.current_price,
                entry_band_json=item.entry_band_json,
                stop_price=decision.stop_price,
                take_price=decision.take_price,
                action=decision.action,
                reason_code=decision.reason_code,
                policy_sha256=_policy_sha(policy),
                guidance_status=decision.guidance_status or GUIDANCE_STATUS_RULE_DEFAULT,
                price_basis=policy.price_basis or PRICE_BASIS_RAW,
                feature_availability_ts=feature_ts,
                t1_note=decision.t1_note,
            )
            stored = self.repository.insert_review_once(record)
            records.append(stored)
            if decision.should_exit and decision.reason_code != STOP_LOSS_DEFERRED_T1:
                self.transition_status(
                    item.lifecycle_status,
                    LIFECYCLE_EXITED,
                    watchlist_item_id=item.watchlist_item_id,
                    exit_reason=decision.reason_code,
                )
            elif item.lifecycle_status == LIFECYCLE_ENTERED and decision.reason_code in {HOLD, STOP_LOSS_DEFERRED_T1}:
                if decision.reason_code != STOP_LOSS_DEFERRED_T1:
                    self.transition_status(
                        item.lifecycle_status,
                        LIFECYCLE_HOLDING,
                        watchlist_item_id=item.watchlist_item_id,
                    )
        return records

    def run_multi_package_daily_review(
        self,
        *,
        items: Iterable[AdvisoryWatchlistItem],
        package_evidence_by_code: Mapping[str, Mapping[str, PackageSelectionEvidence]],
        market_by_code: Mapping[str, AdvisoryMarketSnapshot],
        trade_date: date,
        policy: ExitGuardPolicy,
        fusion_policy: FusionPolicy,
    ) -> list[AdvisoryReviewRecord]:
        item_list = list(items)
        active_codes = {
            item.code
            for item in item_list
            if item.advisory_enabled and item.lifecycle_status in {LIFECYCLE_ENTERED, LIFECYCLE_HOLDING}
        }
        missing_codes = sorted(active_codes - set(package_evidence_by_code))
        if missing_codes:
            raise DataUnavailableError(
                "multi-package advisory review requires fusion evidence coverage for every active item",
                context={"missing_codes": missing_codes, "trade_date": trade_date.isoformat()},
            )
        evidence_by_code = build_fusion_evidence_by_code(
            package_evidence_by_code=package_evidence_by_code,
            trade_date=trade_date,
            fusion_policy=fusion_policy,
        )
        return self.run_daily_review(
            items=item_list,
            evidence_by_code=evidence_by_code,
            market_by_code=market_by_code,
            trade_date=trade_date,
            policy=policy,
        )


def adjust_price_for_factor(price: float, *, base_factor: float | None, current_factor: float | None) -> float:
    if not _positive(base_factor) or not _positive(current_factor):
        return price
    return price * float(current_factor) / float(base_factor)


def adjusted_stop_take(
    *,
    stop_price: float | None,
    take_price: float | None,
    base_factor: float | None,
    current_factor: float | None,
) -> dict[str, float | None]:
    return {
        "stop_price": adjust_price_for_factor(stop_price, base_factor=base_factor, current_factor=current_factor) if stop_price is not None else None,
        "take_price": adjust_price_for_factor(take_price, base_factor=base_factor, current_factor=current_factor) if take_price is not None else None,
    }


def build_fusion_evidence_by_code(
    *,
    package_evidence_by_code: Mapping[str, Mapping[str, PackageSelectionEvidence]],
    trade_date: date,
    fusion_policy: FusionPolicy,
) -> dict[str, DailySelectionEvidenceSnapshot]:
    package_ids = _package_ids_from_evidence(package_evidence_by_code)
    if not package_ids:
        raise DataUnavailableError("multi-package advisory review requires package evidence")
    if fusion_policy.method != "weighted_rank_fusion":
        raise RuntimeConfigInvalidError(
            "unsupported advisory fusion policy method",
            context={"method": fusion_policy.method},
        )
    normalized_weights = fusion_policy.normalized_weights(package_ids)
    fusion_policy_sha256 = fusion_policy.sha256(package_ids)
    raw_weights = {package_id: fusion_policy.package_weights.get(package_id, 1.0) for package_id in package_ids}

    snapshots: list[DailySelectionEvidenceSnapshot] = []
    for code, by_package in package_evidence_by_code.items():
        package_raw_scores: dict[str, float] = {}
        package_raw_ranks: dict[str, int] = {}
        package_rank_scores: dict[str, float] = {package_id: 0.0 for package_id in package_ids}
        package_presence: dict[str, str] = {}
        feature_ts_values: list[datetime] = []

        for package_id in package_ids:
            evidence = by_package.get(package_id)
            if evidence is None:
                if fusion_policy.missing_rank_policy == "data_missing_fail_fast":
                    raise DataUnavailableError(
                        "multi-package advisory review missing package evidence",
                        context={"code": code, "package_id": package_id, "trade_date": trade_date.isoformat()},
                    )
                package_presence[package_id] = "not_selected_in_full_evidence"
                continue
            if evidence.trade_date != trade_date:
                raise RuntimeConfigInvalidError(
                    "multi-package advisory evidence trade_date mismatch",
                    context={
                        "code": code,
                        "package_id": package_id,
                        "evidence_trade_date": evidence.trade_date.isoformat(),
                        "trade_date": trade_date.isoformat(),
                    },
                )
            presence = str(evidence.presence or "selected_topK")
            package_presence[package_id] = presence
            if presence == "data_missing":
                raise DataUnavailableError(
                    "multi-package advisory review package evidence is data_missing",
                    context={"code": code, "package_id": package_id, "trade_date": trade_date.isoformat()},
                )
            if evidence.feature_availability_ts:
                feature_ts_values.append(evidence.feature_availability_ts)
            score = _optional_any_float(evidence.score)
            rank = _optional_int(evidence.rank)
            if score is not None:
                package_raw_scores[package_id] = score
            if rank is None:
                if presence == "selected_topK":
                    raise DataUnavailableError(
                        "selected package evidence requires rank",
                        context={"code": code, "package_id": package_id, "trade_date": trade_date.isoformat()},
                    )
                continue
            package_raw_ranks[package_id] = rank
            denominator = max(int(evidence.candidate_count or 1) - 1, 1)
            package_rank_scores[package_id] = max(0.0, 1.0 - ((rank - 1) / denominator))

        support_count = sum(1 for value in package_presence.values() if value == "selected_topK")
        if support_count <= 0:
            continue
        ranks = list(package_raw_ranks.values())
        rank_dispersion = max(ranks) - min(ranks) if len(ranks) > 1 else 0
        fusion_score = sum(normalized_weights[package_id] * package_rank_scores.get(package_id, 0.0) for package_id in package_ids)
        snapshots.append(
            DailySelectionEvidenceSnapshot(
                # No persisted fusion evidence row exists yet; keep the FK-safe review evidence_id empty.
                evidence_id=None,
                code=code,
                trade_date=trade_date,
                score=fusion_score,
                rank=0,
                package_raw_scores=package_raw_scores,
                package_raw_ranks=package_raw_ranks,
                package_rank_scores=package_rank_scores,
                package_presence=package_presence,
                package_weights=raw_weights,
                normalized_package_weights=normalized_weights,
                support_count=support_count,
                rank_dispersion=rank_dispersion,
                fusion_score=fusion_score,
                fusion_policy_sha256=fusion_policy_sha256,
                feature_availability_ts=max(feature_ts_values) if feature_ts_values else None,
            )
        )

    snapshots.sort(
        key=lambda item: (
            -float(item.fusion_score or 0.0),
            -int(item.support_count or 0),
            min(item.package_raw_ranks.values()) if item.package_raw_ranks else 999999,
            item.code,
        )
    )
    universe_count = max(int(fusion_policy.candidate_top_k or 0), len(snapshots), 1)
    return {
        item.code: _with_fusion_rank(item, rank, universe_count=universe_count)
        for rank, item in enumerate(snapshots, start=1)
    }


def item_from_watchlist_row(row: Mapping[str, Any]) -> AdvisoryWatchlistItem:
    return AdvisoryWatchlistItem(
        watchlist_item_id=int(row["id"]),
        code=str(row["code"]),
        lifecycle_status=str(row.get("lifecycle_status") or LIFECYCLE_CANDIDATE),
        advisory_enabled=bool(row.get("advisory_enabled", False)),
        planned_entry_price=_optional_float(row.get("planned_entry_price")),
        actual_entry_price=_optional_float(row.get("actual_entry_price") or row.get("entry_price")),
        actual_entry_date=row.get("actual_entry_date") or row.get("entry_as_of"),
        exited_at=row.get("exited_at"),
        exit_reason=row.get("exit_reason"),
    )


def _entry_cost(item: AdvisoryWatchlistItem) -> float:
    value = _optional_float(item.actual_entry_price) or _optional_float(item.planned_entry_price)
    if value is None:
        raise DataUnavailableError(
            "advisory item requires planned_entry_price or actual_entry_price",
            context={"watchlist_item_id": item.watchlist_item_id, "code": item.code},
        )
    return value


def _policy_sha(policy: ExitGuardPolicy) -> str:
    if policy.policy_sha256:
        return policy.policy_sha256
    raise RuntimeConfigInvalidError("advisory daily review requires exit_guard policy_sha256")


def _normalize_status(status: str) -> str:
    normalized = str(status or "").strip().upper()
    if normalized not in LEGAL_TRANSITIONS:
        raise InvalidStateTransitionError("unknown advisory lifecycle status", context={"status": status})
    return normalized


def _is_suspended(status: str | None) -> bool:
    text = str(status or "").strip().upper()
    return text in {"S", "SUSPENDED", "HALT", "停牌"}


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) and parsed > 0 else None


def _optional_any_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) else None


def _optional_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _positive(value: Any) -> bool:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return False
    return isfinite(parsed) and parsed > 0


def _package_ids_from_evidence(package_evidence_by_code: Mapping[str, Mapping[str, PackageSelectionEvidence]]) -> list[str]:
    package_ids: set[str] = set()
    for by_package in package_evidence_by_code.values():
        package_ids.update(by_package)
    return sorted(package_ids)


def _with_fusion_rank(item: DailySelectionEvidenceSnapshot, rank: int, *, universe_count: int) -> DailySelectionEvidenceSnapshot:
    return DailySelectionEvidenceSnapshot(
        evidence_id=item.evidence_id,
        code=item.code,
        trade_date=item.trade_date,
        score=item.score,
        rank=rank,
        latest_rank_pct=rank / universe_count,
        alpha_decay_confirm_days=item.alpha_decay_confirm_days,
        feature_availability_ts=item.feature_availability_ts,
        package_raw_scores=item.package_raw_scores,
        package_raw_ranks=item.package_raw_ranks,
        package_rank_scores=item.package_rank_scores,
        package_presence=item.package_presence,
        package_weights=item.package_weights,
        normalized_package_weights=item.normalized_package_weights,
        support_count=item.support_count,
        rank_dispersion=item.rank_dispersion,
        fusion_score=item.fusion_score,
        fusion_rank=rank,
        fusion_policy_sha256=item.fusion_policy_sha256,
    )


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
