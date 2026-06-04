"""Full Advisory Program lifecycle service.

The Advisory Program layer is deliberately non-trading: it manages long-running
recommendation programs, daily reviews, episode return snapshots, replay, and
leaderboard metrics without creating orders or writing execution-side state.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from math import isfinite
from statistics import mean, median
from typing import Any, Iterable, Mapping, Protocol
from uuid import uuid4

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_quality import generate_quality_report
from backend.services.selection_center.models import SelectionMode, SelectionRun, SelectionRunStatus
from backend.services.selection_center.service import SelectionCenterService
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    RuntimeConfigInvalidError,
    UnsupportedFeatureError,
)


PROGRAM_STATUS_DRAFT = "DRAFT"
PROGRAM_STATUS_ENABLED = "ENABLED"
PROGRAM_STATUS_PAUSED = "PAUSED"
PROGRAM_STATUS_REVIEWING = "REVIEWING"
PROGRAM_STATUS_WAITING_DATA = "WAITING_DATA"
PROGRAM_STATUS_REVIEW_FAILED = "REVIEW_FAILED"
PROGRAM_STATUS_ARCHIVED = "ARCHIVED"
ACTIVE_PROGRAM_STATUSES = {PROGRAM_STATUS_ENABLED, PROGRAM_STATUS_REVIEWING, PROGRAM_STATUS_WAITING_DATA}

PACKAGE_MODE_SINGLE = "single_package"
PACKAGE_MODE_FUSION = "fusion_pool"
PACKAGE_MODE_SLEEVE_FUTURE = "sleeve_mode_future"

PRICE_BASIS_NEXT_OPEN = "next_open_executable"
PRICE_BASIS_SIGNAL_CLOSE = "signal_close"
PRICE_BASIS_NEXT_CLOSE = "next_close"
SUPPORTED_PRICE_BASIS = {PRICE_BASIS_NEXT_OPEN, PRICE_BASIS_SIGNAL_CLOSE, PRICE_BASIS_NEXT_CLOSE}

REVIEW_STATUS_SUCCEEDED = "SUCCEEDED"
REVIEW_STATUS_WAITING_DATA = "WAITING_DATA"
REVIEW_STATUS_REVIEW_FAILED = "REVIEW_FAILED"
REVIEW_STATUS_STALE = "STALE"

EPISODE_STATUS_ACTIVE = "ACTIVE"
EPISODE_STATUS_EXITED = "EXITED"

ACTION_ENTER = "ENTER"
ACTION_HOLD = "HOLD"
ACTION_EXIT = "EXIT"
ACTION_WAITING = "WAITING"

EXIT_NONE = "NONE"
EXIT_STOP_LOSS = "STOP_LOSS"
EXIT_STOP_LOSS_DEFERRED_T1 = "STOP_LOSS_DEFERRED_T1"
EXIT_TAKE_PROFIT = "TAKE_PROFIT"
EXIT_TRAILING_TAKE_PROFIT = "TRAILING_TAKE_PROFIT"
EXIT_ALPHA_RANK_DROP = "ALPHA_RANK_DROP_EXIT"
EXIT_TIME_STOP = "TIME_STOP"
EXIT_REPLACEMENT_BUDGET = "REPLACEMENT_BUDGET_LIMIT"
EXIT_REPLAY_END = "REPLAY_END_MARK"

WIN_INCLUDED = "INCLUDED"
WIN_EXCLUDED = "EXCLUDED"
WIN_OPEN_MARK = "OPEN_MARK_TO_MARKET"

DEFAULT_REVIEW_POLICY: dict[str, Any] = {
    "rank_enter_threshold": 20,
    "rank_exit_threshold": 40,
    "rank_exit_confirm_days": 2,
    "daily_replacement_budget": 5,
    "stop_loss_bps": 800,
    "take_profit_bps": 1800,
    "trailing_stop_bps": 700,
    "time_stop_days": 20,
    "take_profit_mode": "trailing",
}


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class AdvisoryProgram:
    program_id: str
    program_name: str
    status: str
    target_count: int
    package_mode: str
    package_ids: list[str]
    package_weights: dict[str, float]
    fusion_method: str | None
    package_set_hash: str
    fusion_policy_sha256: str | None
    review_policy: dict[str, Any]
    review_policy_sha256: str
    entry_price_basis: str
    exit_price_basis: str
    review_schedule: dict[str, Any]
    version: int = 1
    created_by: str | None = None
    enabled_since: datetime | None = None
    last_review_status: str | None = None
    latest_review_trade_date: date | None = None
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class AdvisoryCandidate:
    symbol: str
    rank: int
    score: float | None = None
    reference_price: float | None = None
    signal_close: float | None = None
    next_open_executable: float | None = None
    next_close: float | None = None
    component_scores: dict[str, Any] = field(default_factory=dict)
    stock_name: str | None = None
    source_run_id: str | None = None


@dataclass(frozen=True)
class AdvisoryMarketMark:
    symbol: str
    trade_date: date
    mark_price: float | None = None
    signal_close: float | None = None
    next_open_executable: float | None = None
    next_close: float | None = None
    suspended: bool = False
    price_quality_status: str = "OK"


@dataclass(frozen=True)
class AdvisoryEpisode:
    episode_id: str
    program_id: str
    program_version: int
    symbol: str
    status: str
    signal_date: date
    effective_entry_date: date
    entry_price: float
    entry_price_basis: str
    entry_rank: int
    entry_score: float | None = None
    current_rank: int | None = None
    current_score: float | None = None
    exit_signal_date: date | None = None
    effective_exit_date: date | None = None
    exit_price: float | None = None
    exit_price_basis: str | None = None
    exit_reason: str | None = None
    holding_trading_days: int = 0
    return_bps: float | None = None
    is_win: bool | None = None
    win_rate_inclusion_status: str = WIN_INCLUDED
    max_runup_bps: float | None = None
    max_drawdown_bps: float | None = None
    still_active_mark_price: float | None = None
    price_quality_status: str = "OK"
    weak_rank_confirm_days: int = 0
    source_run_id: str | None = None
    evidence_json: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class AdvisoryReviewDecision:
    program_id: str
    program_version: int
    trade_date: date
    symbol: str
    action: str
    reason_code: str
    review_status: str
    episode_id: str | None = None
    rank: int | None = None
    score: float | None = None
    entry_price: float | None = None
    exit_price: float | None = None
    return_bps: float | None = None
    evidence_json: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class AdvisoryReviewResult:
    program: AdvisoryProgram
    trade_date: date
    review_status: str
    decisions: list[AdvisoryReviewDecision]
    active_pool: list[AdvisoryEpisode]
    metrics: dict[str, Any]
    preview: bool = False


class AdvisoryProgramRepository(Protocol):
    def create_program(self, program: AdvisoryProgram) -> AdvisoryProgram: ...
    def update_program(self, program: AdvisoryProgram) -> AdvisoryProgram: ...
    def get_program(self, program_id: str) -> AdvisoryProgram: ...
    def list_programs(self, *, include_archived: bool = False) -> list[AdvisoryProgram]: ...
    def active_episodes(self, program_id: str) -> list[AdvisoryEpisode]: ...
    def all_latest_episodes(self, program_id: str) -> list[AdvisoryEpisode]: ...
    def insert_episode_snapshot(self, episode: AdvisoryEpisode) -> AdvisoryEpisode: ...
    def insert_review_decision_once(self, decision: AdvisoryReviewDecision) -> AdvisoryReviewDecision: ...
    def list_review_decisions(self, program_id: str, *, limit: int = 100) -> list[AdvisoryReviewDecision]: ...
    def insert_metric_snapshot(self, program_id: str, metrics: dict[str, Any]) -> dict[str, Any]: ...
    def latest_metric_snapshot(self, program_id: str) -> dict[str, Any] | None: ...
    def create_replay_run(self, payload: dict[str, Any]) -> dict[str, Any]: ...


class AdvisoryTradingCalendarProvider(Protocol):
    def list_trading_days(self, start_date: date, end_date: date) -> list[date]: ...


class InMemoryAdvisoryProgramRepository:
    def __init__(self) -> None:
        self.programs: dict[str, AdvisoryProgram] = {}
        self.episode_snapshots: list[AdvisoryEpisode] = []
        self.review_decisions: dict[tuple[str, str, date], AdvisoryReviewDecision] = {}
        self.metric_snapshots: list[dict[str, Any]] = []
        self.replay_runs: list[dict[str, Any]] = []

    def create_program(self, program: AdvisoryProgram) -> AdvisoryProgram:
        if program.program_id in self.programs:
            raise RuntimeConfigInvalidError("advisory program already exists", context={"program_id": program.program_id})
        self.programs[program.program_id] = program
        return program

    def update_program(self, program: AdvisoryProgram) -> AdvisoryProgram:
        if program.program_id not in self.programs:
            raise DataUnavailableError("advisory program does not exist", context={"program_id": program.program_id})
        self.programs[program.program_id] = program
        return program

    def get_program(self, program_id: str) -> AdvisoryProgram:
        program = self.programs.get(program_id)
        if program is None:
            raise DataUnavailableError("advisory program does not exist", context={"program_id": program_id})
        return program

    def list_programs(self, *, include_archived: bool = False) -> list[AdvisoryProgram]:
        rows = list(self.programs.values())
        if not include_archived:
            rows = [row for row in rows if row.status != PROGRAM_STATUS_ARCHIVED]
        return sorted(rows, key=lambda row: (row.created_at, row.program_id))

    def active_episodes(self, program_id: str) -> list[AdvisoryEpisode]:
        return [row for row in self.all_latest_episodes(program_id) if row.status == EPISODE_STATUS_ACTIVE]

    def all_latest_episodes(self, program_id: str) -> list[AdvisoryEpisode]:
        latest: dict[str, AdvisoryEpisode] = {}
        for row in self.episode_snapshots:
            if row.program_id != program_id:
                continue
            current = latest.get(row.episode_id)
            if current is None or row.updated_at >= current.updated_at:
                latest[row.episode_id] = row
        return sorted(latest.values(), key=lambda row: (row.signal_date, row.symbol, row.episode_id))

    def insert_episode_snapshot(self, episode: AdvisoryEpisode) -> AdvisoryEpisode:
        self.episode_snapshots.append(episode)
        return episode

    def insert_review_decision_once(self, decision: AdvisoryReviewDecision) -> AdvisoryReviewDecision:
        key = (decision.program_id, decision.symbol, decision.trade_date)
        existing = self.review_decisions.get(key)
        if existing is not None:
            return existing
        self.review_decisions[key] = decision
        return decision

    def list_review_decisions(self, program_id: str, *, limit: int = 100) -> list[AdvisoryReviewDecision]:
        rows = [row for row in self.review_decisions.values() if row.program_id == program_id]
        return sorted(rows, key=lambda row: (row.trade_date, row.symbol), reverse=True)[:limit]

    def insert_metric_snapshot(self, program_id: str, metrics: dict[str, Any]) -> dict[str, Any]:
        snapshot = {"program_id": program_id, "snapshot_id": f"advm_{uuid4().hex}", "created_at": _utcnow(), **deepcopy(metrics)}
        self.metric_snapshots.append(snapshot)
        return snapshot

    def latest_metric_snapshot(self, program_id: str) -> dict[str, Any] | None:
        rows = [row for row in self.metric_snapshots if row["program_id"] == program_id]
        return sorted(rows, key=lambda row: row["created_at"])[-1] if rows else None

    def create_replay_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        row = {"replay_run_id": f"adv_replay_{uuid4().hex}", "created_at": _utcnow(), **deepcopy(payload)}
        self.replay_runs.append(row)
        return row


class AdvisoryProgramPGRepository:
    def __init__(self, conn_factory: Any | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def create_program(self, program: AdvisoryProgram) -> AdvisoryProgram:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_program (
                        program_id, program_name, status, target_count, package_mode,
                        package_ids, package_weights, fusion_method, package_set_hash,
                        fusion_policy_sha256, review_policy, review_policy_sha256,
                        entry_price_basis, exit_price_basis, review_schedule, version,
                        created_by, enabled_since, last_review_status,
                        latest_review_trade_date, program_payload_json, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    _program_sql_params(program),
                )
                self._replace_program_packages(cur, program)
        return program

    def update_program(self, program: AdvisoryProgram) -> AdvisoryProgram:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE app.advisory_program
                    SET program_name = %s, status = %s, target_count = %s, package_mode = %s,
                        package_ids = %s, package_weights = %s, fusion_method = %s,
                        package_set_hash = %s, fusion_policy_sha256 = %s, review_policy = %s,
                        review_policy_sha256 = %s, entry_price_basis = %s, exit_price_basis = %s,
                        review_schedule = %s, version = %s, created_by = %s, enabled_since = %s,
                        last_review_status = %s, latest_review_trade_date = %s,
                        program_payload_json = %s, updated_at = %s
                    WHERE program_id = %s
                    """,
                    (
                        program.program_name,
                        program.status,
                        program.target_count,
                        program.package_mode,
                        psycopg2.extras.Json(program.package_ids),
                        psycopg2.extras.Json(program.package_weights),
                        program.fusion_method,
                        program.package_set_hash,
                        program.fusion_policy_sha256,
                        psycopg2.extras.Json(program.review_policy),
                        program.review_policy_sha256,
                        program.entry_price_basis,
                        program.exit_price_basis,
                        psycopg2.extras.Json(program.review_schedule),
                        program.version,
                        program.created_by,
                        program.enabled_since,
                        program.last_review_status,
                        program.latest_review_trade_date,
                        psycopg2.extras.Json(program_to_dict(program)),
                        program.updated_at,
                        program.program_id,
                    ),
                )
                if cur.rowcount == 0:
                    raise DataUnavailableError("advisory program does not exist", context={"program_id": program.program_id})
                self._replace_program_packages(cur, program)
        return program

    def get_program(self, program_id: str) -> AdvisoryProgram:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM app.advisory_program WHERE program_id = %s", (program_id,))
                row = cur.fetchone()
        if row is None:
            raise DataUnavailableError("advisory program does not exist", context={"program_id": program_id})
        return _program_from_row(row)

    def list_programs(self, *, include_archived: bool = False) -> list[AdvisoryProgram]:
        where = "" if include_archived else "WHERE status <> 'ARCHIVED'"
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(f"SELECT * FROM app.advisory_program {where} ORDER BY created_at ASC, program_id ASC")
                rows = cur.fetchall()
        return [_program_from_row(row) for row in rows]

    def active_episodes(self, program_id: str) -> list[AdvisoryEpisode]:
        return [row for row in self.all_latest_episodes(program_id) if row.status == EPISODE_STATUS_ACTIVE]

    def all_latest_episodes(self, program_id: str) -> list[AdvisoryEpisode]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (episode_id) *
                    FROM app.advisory_episode_return
                    WHERE program_id = %s
                    ORDER BY episode_id, updated_at DESC, created_at DESC
                    """,
                    (program_id,),
                )
                rows = cur.fetchall()
        return [_episode_from_row(row) for row in rows]

    def insert_episode_snapshot(self, episode: AdvisoryEpisode) -> AdvisoryEpisode:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_episode_return (
                        episode_id, program_id, program_version, symbol, episode_status,
                        signal_date, effective_entry_date, entry_price, entry_price_basis,
                        entry_rank, entry_score, current_rank, current_score, exit_signal_date,
                        effective_exit_date, exit_price, exit_price_basis, exit_reason,
                        holding_trading_days, return_bps, is_win, win_rate_inclusion_status,
                        max_runup_bps, max_drawdown_bps, still_active_mark_price,
                        price_quality_status, weak_rank_confirm_days, source_run_id,
                        evidence_json, episode_payload_json, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    _episode_sql_params(episode),
                )
        return episode

    def insert_review_decision_once(self, decision: AdvisoryReviewDecision) -> AdvisoryReviewDecision:
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_daily_review (
                        program_id, program_version, episode_id, watchlist_item_id, code,
                        trade_date, evidence_id, score, rank, current_price, entry_band_json,
                        stop_price, take_price, action, reason_code, policy_sha256,
                        guidance_status, price_basis, feature_availability_ts, t1_note, layer,
                        review_status, fusion_evidence_json, decision_input_json
                    ) VALUES (
                        %s, %s, %s, NULL, %s, %s, NULL, %s, %s, %s, NULL, NULL, NULL,
                        %s, %s, %s, %s, %s, %s, NULL, 'advisory_program', %s, %s, %s
                    )
                    ON CONFLICT (program_id, code, trade_date) WHERE program_id IS NOT NULL DO NOTHING
                    """,
                    (
                        decision.program_id,
                        decision.program_version,
                        decision.episode_id,
                        decision.symbol,
                        decision.trade_date,
                        decision.score,
                        decision.rank,
                        decision.exit_price if decision.action == ACTION_EXIT else decision.entry_price,
                        decision.action,
                        decision.reason_code,
                        decision.evidence_json.get("review_policy_sha256") or "unknown",
                        decision.evidence_json.get("guidance_status") or "rule_default",
                        decision.evidence_json.get("price_basis") or PRICE_BASIS_NEXT_OPEN,
                        decision.created_at,
                        decision.review_status,
                        psycopg2.extras.Json(decision.evidence_json),
                        psycopg2.extras.Json(decision_to_dict(decision)),
                    ),
                )
        return decision

    def list_review_decisions(self, program_id: str, *, limit: int = 100) -> list[AdvisoryReviewDecision]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM app.advisory_daily_review
                    WHERE program_id = %s
                    ORDER BY trade_date DESC, code ASC
                    LIMIT %s
                    """,
                    (program_id, limit),
                )
                rows = cur.fetchall()
        return [_decision_from_row(row) for row in rows]

    def insert_metric_snapshot(self, program_id: str, metrics: dict[str, Any]) -> dict[str, Any]:
        payload = deepcopy(metrics)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_program_metric_snapshot (
                        program_id, snapshot_date, enabled_since, entered_episode_count,
                        active_count, take_profit_count, stop_loss_count, win_rate,
                        avg_return_bps, median_return_bps, max_drawdown_bps,
                        avg_holding_days, last_review_status, metrics_json
                    ) VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        program_id,
                        payload.get("enabled_since"),
                        payload.get("entered_episode_count"),
                        payload.get("active_count"),
                        payload.get("take_profit_count"),
                        payload.get("stop_loss_count"),
                        payload.get("win_rate"),
                        payload.get("avg_return_bps"),
                        payload.get("median_return_bps"),
                        payload.get("max_drawdown_bps"),
                        payload.get("avg_holding_days"),
                        payload.get("last_review_status"),
                        psycopg2.extras.Json(payload),
                    ),
                )
                row = cur.fetchone()
        return dict(row or payload)

    def latest_metric_snapshot(self, program_id: str) -> dict[str, Any] | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM app.advisory_program_metric_snapshot
                    WHERE program_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (program_id,),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def create_replay_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        replay_run_id = f"adv_replay_{uuid4().hex}"
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO app.advisory_replay_run (
                        replay_run_id, program_id, program_version, start_date, end_date,
                        entry_price_basis, exit_price_basis, status, replay_config_json, summary_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        replay_run_id,
                        payload.get("program_id"),
                        payload.get("program_version"),
                        payload.get("start_date"),
                        payload.get("end_date"),
                        payload.get("entry_price_basis"),
                        payload.get("exit_price_basis"),
                        payload.get("status"),
                        psycopg2.extras.Json(payload.get("replay_config_json") or {}),
                        psycopg2.extras.Json(payload.get("summary_json") or {}),
                    ),
                )
                row = cur.fetchone()
        return dict(row or {"replay_run_id": replay_run_id, **payload})

    @staticmethod
    def _replace_program_packages(cur: Any, program: AdvisoryProgram) -> None:
        cur.execute(
            "DELETE FROM app.advisory_program_package WHERE program_id = %s AND program_version = %s",
            (program.program_id, program.version),
        )
        for index, package_id in enumerate(program.package_ids, start=1):
            cur.execute(
                """
                INSERT INTO app.advisory_program_package (
                    program_id, program_version, package_id, weight, package_role, package_order
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    program.program_id,
                    program.version,
                    package_id,
                    program.package_weights[package_id],
                    "primary" if program.package_mode == PACKAGE_MODE_SINGLE else "fusion_member",
                    index,
                ),
            )


class AdvisoryProgramService:
    def __init__(
        self,
        *,
        repository: AdvisoryProgramRepository | None = None,
        selection_service: SelectionCenterService | Any | None = None,
        calendar_provider: AdvisoryTradingCalendarProvider | Any | None = None,
    ) -> None:
        self.repository = repository or AdvisoryProgramPGRepository()
        self.selection_service = selection_service or SelectionCenterService()
        self.calendar_provider = calendar_provider or TradingCalendarStatusService()

    def create_program(
        self,
        *,
        program_name: str,
        package_mode: str,
        package_ids: list[str],
        target_count: int = 20,
        package_weights: Mapping[str, Any] | None = None,
        review_policy: Mapping[str, Any] | None = None,
        entry_price_basis: str = PRICE_BASIS_NEXT_OPEN,
        exit_price_basis: str = PRICE_BASIS_NEXT_OPEN,
        review_schedule: Mapping[str, Any] | None = None,
        created_by: str | None = None,
        status: str = PROGRAM_STATUS_DRAFT,
    ) -> AdvisoryProgram:
        config = self._validated_config(
            program_name=program_name,
            package_mode=package_mode,
            package_ids=package_ids,
            target_count=target_count,
            package_weights=package_weights,
            review_policy=review_policy,
            entry_price_basis=entry_price_basis,
            exit_price_basis=exit_price_basis,
            review_schedule=review_schedule,
        )
        clean_status = self._normalize_status(status)
        if clean_status == PROGRAM_STATUS_REVIEWING:
            raise RuntimeConfigInvalidError("new advisory program cannot start in REVIEWING status")
        now = _utcnow()
        program = AdvisoryProgram(
            program_id=f"advp_{uuid4().hex}",
            status=clean_status,
            created_by=created_by,
            enabled_since=now if clean_status == PROGRAM_STATUS_ENABLED else None,
            created_at=now,
            updated_at=now,
            **config,
        )
        return self.repository.create_program(program)

    def update_program(self, program_id: str, updates: Mapping[str, Any]) -> AdvisoryProgram:
        program = self.repository.get_program(program_id)
        if program.status == PROGRAM_STATUS_ARCHIVED:
            raise InvalidStateTransitionError("archived advisory program cannot be updated", context={"program_id": program_id})
        config_fields = {
            "program_name",
            "package_mode",
            "package_ids",
            "target_count",
            "package_weights",
            "review_policy",
            "entry_price_basis",
            "exit_price_basis",
            "review_schedule",
        }
        unknown = set(updates) - config_fields - {"status"}
        if unknown:
            raise RuntimeConfigInvalidError("unsupported advisory program update fields", context={"fields": sorted(unknown)})
        changed_config = any(field_name in updates for field_name in config_fields)
        if changed_config:
            config = self._validated_config(
                program_name=str(updates.get("program_name", program.program_name)),
                package_mode=str(updates.get("package_mode", program.package_mode)),
                package_ids=list(updates.get("package_ids", program.package_ids)),
                target_count=int(updates.get("target_count", program.target_count)),
                package_weights=updates.get("package_weights", program.package_weights),
                review_policy=updates.get("review_policy", program.review_policy),
                entry_price_basis=str(updates.get("entry_price_basis", program.entry_price_basis)),
                exit_price_basis=str(updates.get("exit_price_basis", program.exit_price_basis)),
                review_schedule=updates.get("review_schedule", program.review_schedule),
            )
            program = replace(program, version=program.version + 1, updated_at=_utcnow(), **config)
        if "status" in updates:
            program = self._with_status(program, str(updates["status"]))
        return self.repository.update_program(program if changed_config or "status" in updates else replace(program, updated_at=_utcnow()))

    def change_status(self, program_id: str, status: str) -> AdvisoryProgram:
        program = self.repository.get_program(program_id)
        return self.repository.update_program(self._with_status(program, status))

    def clone_program(self, program_id: str, *, program_name: str | None = None, created_by: str | None = None) -> AdvisoryProgram:
        source = self.repository.get_program(program_id)
        clone = replace(
            source,
            program_id=f"advp_{uuid4().hex}",
            program_name=program_name or f"{source.program_name} Copy",
            status=PROGRAM_STATUS_DRAFT,
            version=1,
            created_by=created_by or source.created_by,
            enabled_since=None,
            last_review_status=None,
            latest_review_trade_date=None,
            created_at=_utcnow(),
            updated_at=_utcnow(),
        )
        return self.repository.create_program(clone)

    def list_programs(self, *, include_archived: bool = False) -> list[AdvisoryProgram]:
        return self.repository.list_programs(include_archived=include_archived)

    def get_program(self, program_id: str) -> AdvisoryProgram:
        return self.repository.get_program(program_id)

    def leaderboard(self, *, sort_by: str = "win_rate", include_archived: bool = False) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for program in self.repository.list_programs(include_archived=include_archived):
            if program.status not in ACTIVE_PROGRAM_STATUSES and not include_archived:
                continue
            rows.append({**program_to_dict(program), **self.program_metrics(program.program_id)})
        return sorted(rows, key=self._leaderboard_key(sort_by))

    def active_pool(self, program_id: str) -> list[dict[str, Any]]:
        self.repository.get_program(program_id)
        return [episode_to_dict(row) for row in self.repository.active_episodes(program_id)]

    def review_history(self, program_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        self.repository.get_program(program_id)
        return [decision_to_dict(row) for row in self.repository.list_review_decisions(program_id, limit=limit)]

    def return_history(self, program_id: str) -> list[dict[str, Any]]:
        self.repository.get_program(program_id)
        return [episode_to_dict(row) for row in self.repository.all_latest_episodes(program_id)]

    def program_metrics(self, program_id: str) -> dict[str, Any]:
        program = self.repository.get_program(program_id)
        return compute_program_metrics(program, self.repository.all_latest_episodes(program_id))

    def run_review_from_selection(
        self,
        program_id: str,
        *,
        trade_date: date,
        selection_run_id: str | None = None,
        data_source: str = "DB_HISTORICAL",
        runtime_config: dict[str, Any] | None = None,
        candidates: list[Mapping[str, Any]] | None = None,
        market_by_symbol: Mapping[str, Mapping[str, Any]] | None = None,
        preview: bool = False,
    ) -> AdvisoryReviewResult:
        program = self.repository.get_program(program_id)
        if candidates is None:
            run = self._selection_run_for_review(
                program=program,
                trade_date=trade_date,
                selection_run_id=selection_run_id,
                data_source=data_source,
                runtime_config=runtime_config or {},
            )
            normalized_candidates = candidates_from_selection_run(run)
        else:
            normalized_candidates = [_candidate_from_mapping(row) for row in candidates]
        market = {
            symbol: _market_from_mapping(symbol, payload, trade_date=trade_date)
            for symbol, payload in (market_by_symbol or {}).items()
        }
        return self.run_review(
            program_id,
            trade_date=trade_date,
            candidates=normalized_candidates,
            market_by_symbol=market,
            preview=preview,
        )

    def run_review(
        self,
        program_id: str,
        *,
        trade_date: date,
        candidates: list[AdvisoryCandidate | Mapping[str, Any]],
        market_by_symbol: Mapping[str, AdvisoryMarketMark] | None = None,
        preview: bool = False,
        initial_active_episodes: list[AdvisoryEpisode] | None = None,
    ) -> AdvisoryReviewResult:
        program = self.repository.get_program(program_id)
        if not preview and program.status not in {PROGRAM_STATUS_ENABLED, PROGRAM_STATUS_WAITING_DATA, PROGRAM_STATUS_REVIEW_FAILED}:
            raise InvalidStateTransitionError(
                "advisory program must be enabled before daily review",
                context={"program_id": program_id, "status": program.status},
            )
        normalized_candidates = [
            row if isinstance(row, AdvisoryCandidate) else _candidate_from_mapping(row)
            for row in candidates
        ]
        normalized_market = {
            symbol: row if isinstance(row, AdvisoryMarketMark) else _market_from_mapping(symbol, row, trade_date=trade_date)
            for symbol, row in dict(market_by_symbol or {}).items()
        }
        result = self._evaluate_review(
            program=program,
            trade_date=trade_date,
            candidates=normalized_candidates,
            market_by_symbol=normalized_market,
            active_episodes=initial_active_episodes if initial_active_episodes is not None else self.repository.active_episodes(program_id),
            preview=preview,
        )
        if preview:
            return result
        for episode in result.active_pool:
            self.repository.insert_episode_snapshot(episode)
        for decision in result.decisions:
            self.repository.insert_review_decision_once(decision)
        metric_snapshot = self.repository.insert_metric_snapshot(program.program_id, result.metrics)
        updated = replace(
            program,
            status=PROGRAM_STATUS_ENABLED if result.review_status == REVIEW_STATUS_SUCCEEDED else PROGRAM_STATUS_WAITING_DATA,
            last_review_status=result.review_status,
            latest_review_trade_date=trade_date,
            updated_at=_utcnow(),
        )
        updated = self.repository.update_program(updated)
        return replace(result, program=updated, metrics={**result.metrics, "snapshot_id": metric_snapshot.get("snapshot_id")})

    def run_replay(
        self,
        program_id: str,
        *,
        start_date: date,
        end_date: date,
        candidates_by_date: Mapping[date | str, list[Mapping[str, Any]]],
        market_by_date: Mapping[date | str, Mapping[str, Mapping[str, Any]]],
        data_source: str = "DB_HISTORICAL",
        runtime_config: Mapping[str, Any] | None = None,
        entry_price_basis: str | None = None,
        exit_price_basis: str | None = None,
    ) -> dict[str, Any]:
        if start_date > end_date:
            raise RuntimeConfigInvalidError("advisory replay start_date must be <= end_date")
        source_program = self.repository.get_program(program_id)
        program = replace(
            source_program,
            entry_price_basis=self._normalize_price_basis(entry_price_basis or source_program.entry_price_basis),
            exit_price_basis=self._normalize_price_basis(exit_price_basis or source_program.exit_price_basis),
        )
        active: list[AdvisoryEpisode] = []
        daily: list[AdvisoryReviewResult] = []
        for current in self.calendar_provider.list_trading_days(start_date, end_date):
            raw_candidates = candidates_by_date.get(current) or candidates_by_date.get(current.isoformat())
            raw_market = market_by_date.get(current) or market_by_date.get(current.isoformat())
            if raw_candidates is None:
                run = self._selection_run_for_review(
                    program=program,
                    trade_date=current,
                    selection_run_id=None,
                    data_source=data_source,
                    runtime_config=dict(runtime_config or {}),
                )
                candidates = candidates_from_selection_run(run)
            else:
                candidates = [_candidate_from_mapping(row) for row in raw_candidates]
            result = self._evaluate_review(
                program=program,
                trade_date=current,
                candidates=candidates,
                market_by_symbol={
                    symbol: _market_from_mapping(symbol, payload, trade_date=current)
                    for symbol, payload in (raw_market or {}).items()
                },
                active_episodes=active,
                preview=True,
            )
            active = [row for row in result.active_pool if row.status == EPISODE_STATUS_ACTIVE]
            daily.append(result)
        latest = _latest_by_episode_id([row for result in daily for row in result.active_pool])
        summary = compute_program_metrics(program, latest)
        replay = self.repository.create_replay_run(
            {
                "program_id": program.program_id,
                "program_version": program.version,
                "start_date": start_date,
                "end_date": end_date,
                "entry_price_basis": program.entry_price_basis,
                "exit_price_basis": program.exit_price_basis,
                "status": REVIEW_STATUS_SUCCEEDED if daily else REVIEW_STATUS_WAITING_DATA,
                "replay_config_json": {
                    "pit_required": True,
                    "entry_price_basis": program.entry_price_basis,
                    "exit_price_basis": program.exit_price_basis,
                },
                "summary_json": summary,
            }
        )
        return {
            "replay_run": _json_ready(replay),
            "daily_reviews": [review_result_to_dict(row) for row in daily],
            "episodes": [episode_to_dict(row) for row in latest],
            "summary": summary,
        }

    @staticmethod
    def quality_report(records: Iterable[Mapping[str, Any]], *, min_bucket_size: int = 30) -> dict[str, Any]:
        return generate_quality_report(records, min_bucket_size=min_bucket_size)

    def _evaluate_review(
        self,
        *,
        program: AdvisoryProgram,
        trade_date: date,
        candidates: list[AdvisoryCandidate],
        market_by_symbol: Mapping[str, AdvisoryMarketMark],
        active_episodes: list[AdvisoryEpisode],
        preview: bool,
    ) -> AdvisoryReviewResult:
        evidence_by_symbol = {row.symbol: row for row in candidates}
        decisions: list[AdvisoryReviewDecision] = []
        snapshots: list[AdvisoryEpisode] = []
        review_status = REVIEW_STATUS_SUCCEEDED
        replacement_budget = int(program.review_policy["daily_replacement_budget"])
        rank_drop_candidates: list[AdvisoryEpisode] = []

        for episode in active_episodes:
            evidence = evidence_by_symbol.get(episode.symbol)
            if evidence is None:
                review_status = REVIEW_STATUS_WAITING_DATA
                kept = replace(episode, price_quality_status="WAITING_EVIDENCE", updated_at=_utcnow())
                snapshots.append(kept)
                decisions.append(self._decision(program, trade_date, kept.symbol, ACTION_WAITING, "WAITING_EVIDENCE", REVIEW_STATUS_WAITING_DATA, kept, None))
                continue
            price = _price_for_basis(market_by_symbol.get(episode.symbol), evidence, program.exit_price_basis)
            if price is None:
                review_status = REVIEW_STATUS_WAITING_DATA
                kept = replace(episode, current_rank=evidence.rank, current_score=evidence.score, price_quality_status="WAITING_PRICE", updated_at=_utcnow())
                snapshots.append(kept)
                decisions.append(self._decision(program, trade_date, kept.symbol, ACTION_WAITING, "WAITING_PRICE", REVIEW_STATUS_WAITING_DATA, kept, evidence))
                continue
            marked = _episode_with_mark(episode, evidence=evidence, price=price, program=program)
            exit_reason = _exit_reason(marked, evidence=evidence, program=program)
            if exit_reason == EXIT_ALPHA_RANK_DROP and marked.weak_rank_confirm_days < int(program.review_policy["rank_exit_confirm_days"]):
                exit_reason = None
            if exit_reason == EXIT_STOP_LOSS and trade_date < marked.effective_entry_date:
                deferred = replace(
                    marked,
                    return_bps=None,
                    is_win=None,
                    win_rate_inclusion_status=WIN_EXCLUDED,
                    price_quality_status=EXIT_STOP_LOSS_DEFERRED_T1,
                    updated_at=_utcnow(),
                )
                snapshots.append(deferred)
                decisions.append(self._decision(program, trade_date, deferred.symbol, ACTION_WAITING, EXIT_STOP_LOSS_DEFERRED_T1, REVIEW_STATUS_SUCCEEDED, deferred, evidence))
                continue
            if exit_reason == EXIT_ALPHA_RANK_DROP:
                rank_drop_candidates.append(marked)
                continue
            if exit_reason:
                exited = _episode_exited(marked, trade_date=trade_date, exit_price=price, exit_basis=program.exit_price_basis, exit_reason=exit_reason)
                snapshots.append(exited)
                decisions.append(self._decision(program, trade_date, exited.symbol, ACTION_EXIT, exit_reason, REVIEW_STATUS_SUCCEEDED, exited, evidence))
                continue
            snapshots.append(marked)
            decisions.append(self._decision(program, trade_date, marked.symbol, ACTION_HOLD, EXIT_NONE, REVIEW_STATUS_SUCCEEDED, marked, evidence))

        rank_drop_candidates.sort(key=lambda row: (row.current_rank or 0, row.symbol), reverse=True)
        for index, episode in enumerate(rank_drop_candidates):
            evidence = evidence_by_symbol[episode.symbol]
            price = _price_for_basis(market_by_symbol.get(episode.symbol), evidence, program.exit_price_basis) or episode.still_active_mark_price or episode.entry_price
            if index < replacement_budget:
                exited = _episode_exited(episode, trade_date=trade_date, exit_price=price, exit_basis=program.exit_price_basis, exit_reason=EXIT_ALPHA_RANK_DROP)
                snapshots.append(exited)
                decisions.append(self._decision(program, trade_date, exited.symbol, ACTION_EXIT, EXIT_ALPHA_RANK_DROP, REVIEW_STATUS_SUCCEEDED, exited, evidence))
            else:
                kept = replace(episode, evidence_json={**episode.evidence_json, "replacement_budget_state": EXIT_REPLACEMENT_BUDGET})
                snapshots.append(kept)
                decisions.append(self._decision(program, trade_date, kept.symbol, ACTION_HOLD, EXIT_REPLACEMENT_BUDGET, REVIEW_STATUS_SUCCEEDED, kept, evidence))

        active_symbols = {row.symbol for row in snapshots if row.status == EPISODE_STATUS_ACTIVE}
        slots = max(program.target_count - len(active_symbols), 0)
        entry_limit = slots if not active_episodes else min(slots, replacement_budget)
        entered = 0
        for candidate in sorted(candidates, key=lambda row: (row.rank, row.symbol)):
            if entered >= entry_limit:
                break
            if candidate.rank > int(program.review_policy["rank_enter_threshold"]) or candidate.symbol in active_symbols:
                continue
            entry_price = _price_for_basis(market_by_symbol.get(candidate.symbol), candidate, program.entry_price_basis)
            if entry_price is None:
                review_status = REVIEW_STATUS_WAITING_DATA
                decisions.append(self._decision(program, trade_date, candidate.symbol, ACTION_WAITING, "MISSING_ENTRY_PRICE", REVIEW_STATUS_WAITING_DATA, None, candidate))
                continue
            episode = AdvisoryEpisode(
                episode_id=f"advep_{uuid4().hex}",
                program_id=program.program_id,
                program_version=program.version,
                symbol=candidate.symbol,
                status=EPISODE_STATUS_ACTIVE,
                signal_date=trade_date,
                effective_entry_date=_effective_date(trade_date, program.entry_price_basis),
                entry_price=entry_price,
                entry_price_basis=program.entry_price_basis,
                entry_rank=candidate.rank,
                entry_score=candidate.score,
                current_rank=candidate.rank,
                current_score=candidate.score,
                still_active_mark_price=entry_price,
                max_runup_bps=0.0,
                max_drawdown_bps=0.0,
                source_run_id=candidate.source_run_id,
                evidence_json=_candidate_evidence(candidate, program),
            )
            snapshots.append(episode)
            active_symbols.add(candidate.symbol)
            entered += 1
            decisions.append(self._decision(program, trade_date, candidate.symbol, ACTION_ENTER, ACTION_ENTER, REVIEW_STATUS_SUCCEEDED, episode, candidate, entry_price=entry_price))

        latest = _latest_by_episode_id(snapshots)
        status_program = replace(program, last_review_status=review_status, latest_review_trade_date=trade_date)
        metrics = compute_program_metrics(status_program, latest)
        return AdvisoryReviewResult(status_program, trade_date, review_status, decisions, latest, metrics, preview=preview)

    @staticmethod
    def _decision(
        program: AdvisoryProgram,
        trade_date: date,
        symbol: str,
        action: str,
        reason_code: str,
        review_status: str,
        episode: AdvisoryEpisode | None,
        evidence: AdvisoryCandidate | None,
        *,
        entry_price: float | None = None,
    ) -> AdvisoryReviewDecision:
        return AdvisoryReviewDecision(
            program_id=program.program_id,
            program_version=program.version,
            trade_date=trade_date,
            symbol=symbol,
            action=action,
            reason_code=reason_code,
            review_status=review_status,
            episode_id=episode.episode_id if episode else None,
            rank=evidence.rank if evidence else episode.current_rank if episode else None,
            score=evidence.score if evidence else episode.current_score if episode else None,
            entry_price=entry_price or (episode.entry_price if episode else None),
            exit_price=episode.exit_price if episode else None,
            return_bps=episode.return_bps if episode else None,
            evidence_json=_candidate_evidence(evidence, program) if evidence else {"review_policy_sha256": program.review_policy_sha256},
        )

    def _selection_run_for_review(
        self,
        *,
        program: AdvisoryProgram,
        trade_date: date,
        selection_run_id: str | None,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> SelectionRun:
        if selection_run_id:
            run = self.selection_service.get_run(selection_run_id)
        else:
            mode = SelectionMode.SINGLE_PACKAGE if program.package_mode == PACKAGE_MODE_SINGLE else SelectionMode.WEIGHTED_FUSION
            config = dict(runtime_config)
            if program.package_mode == PACKAGE_MODE_FUSION:
                config["package_weights"] = program.package_weights
            run = self.selection_service.run_packages(
                package_ids=program.package_ids,
                mode=mode,
                trade_date=trade_date,
                data_source=data_source,
                runtime_config=config,
            )
        if run.status != SelectionRunStatus.SUCCEEDED:
            raise InvalidStateTransitionError("selection run must be succeeded for advisory review", context={"run_id": run.run_id, "status": run.status.value})
        if sorted(run.package_ids) != sorted(program.package_ids):
            raise RuntimeConfigInvalidError("selection run packages must match advisory program packages", context={"run_id": run.run_id})
        if run.trade_date != trade_date:
            raise RuntimeConfigInvalidError("selection run trade_date must match advisory review trade_date", context={"run_id": run.run_id})
        return run

    def _validated_config(
        self,
        *,
        program_name: str,
        package_mode: str,
        package_ids: list[str],
        target_count: int,
        package_weights: Mapping[str, Any] | None,
        review_policy: Mapping[str, Any] | None,
        entry_price_basis: str,
        exit_price_basis: str,
        review_schedule: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        name = str(program_name or "").strip()
        if not name:
            raise RuntimeConfigInvalidError("advisory program_name is required")
        mode = str(package_mode or "").strip()
        if mode == PACKAGE_MODE_SLEEVE_FUTURE:
            raise UnsupportedFeatureError("sleeve_mode_future is design-reserved and is not enabled")
        if mode not in {PACKAGE_MODE_SINGLE, PACKAGE_MODE_FUSION}:
            raise RuntimeConfigInvalidError("unsupported advisory package_mode", context={"package_mode": package_mode})
        clean_package_ids = [str(item).strip() for item in package_ids if str(item).strip()]
        if len(clean_package_ids) != len(set(clean_package_ids)):
            raise RuntimeConfigInvalidError("advisory package_ids must be unique")
        if mode == PACKAGE_MODE_SINGLE and len(clean_package_ids) != 1:
            raise RuntimeConfigInvalidError("single_package advisory program requires exactly one StrategyPackage")
        if mode == PACKAGE_MODE_FUSION and len(clean_package_ids) < 2:
            raise RuntimeConfigInvalidError("fusion_pool advisory program requires at least two StrategyPackages")
        if target_count <= 0 or target_count > 100:
            raise RuntimeConfigInvalidError("advisory target_count must be between 1 and 100", context={"target_count": target_count})
        weights = self._normalize_weights(clean_package_ids, package_weights)
        policy = self._normalize_review_policy(review_policy, target_count=target_count)
        entry_basis = self._normalize_price_basis(entry_price_basis)
        exit_basis = self._normalize_price_basis(exit_price_basis)
        fusion_method = "weighted_rank_fusion" if mode == PACKAGE_MODE_FUSION else None
        fusion_sha = _canonical_sha256({"method": fusion_method, "package_ids": clean_package_ids, "package_weights": weights}) if fusion_method else None
        return {
            "program_name": name,
            "package_mode": mode,
            "package_ids": clean_package_ids,
            "target_count": target_count,
            "package_weights": weights,
            "fusion_method": fusion_method,
            "package_set_hash": _canonical_sha256({"package_mode": mode, "package_ids": clean_package_ids}),
            "fusion_policy_sha256": fusion_sha,
            "review_policy": policy,
            "review_policy_sha256": _canonical_sha256(policy),
            "entry_price_basis": entry_basis,
            "exit_price_basis": exit_basis,
            "review_schedule": dict(review_schedule or {"frequency": "daily_after_close"}),
        }

    @staticmethod
    def _normalize_weights(package_ids: list[str], raw: Mapping[str, Any] | None) -> dict[str, float]:
        values = dict(raw or {package_id: 1.0 for package_id in package_ids})
        if set(values) != set(package_ids):
            raise RuntimeConfigInvalidError("advisory package_weights must match package_ids exactly", context={"package_ids": package_ids, "weight_keys": sorted(values)})
        out: dict[str, float] = {}
        for package_id in package_ids:
            value = _optional_float(values[package_id])
            if value is None or value <= 0:
                raise RuntimeConfigInvalidError("advisory package weight must be positive", context={"package_id": package_id})
            out[package_id] = value
        return out

    @staticmethod
    def _normalize_review_policy(raw: Mapping[str, Any] | None, *, target_count: int) -> dict[str, Any]:
        policy = {
            **DEFAULT_REVIEW_POLICY,
            "rank_enter_threshold": target_count,
            "rank_exit_threshold": target_count * 2,
            **dict(raw or {}),
        }
        for key in (
            "rank_enter_threshold",
            "rank_exit_threshold",
            "rank_exit_confirm_days",
            "daily_replacement_budget",
            "stop_loss_bps",
            "take_profit_bps",
            "trailing_stop_bps",
            "time_stop_days",
        ):
            policy[key] = int(policy[key])
            if policy[key] < 0:
                raise RuntimeConfigInvalidError("advisory review policy values must be non-negative", context={"field": key})
        if policy["rank_enter_threshold"] <= 0 or policy["rank_exit_threshold"] < policy["rank_enter_threshold"]:
            raise RuntimeConfigInvalidError("rank_exit_threshold must be >= rank_enter_threshold > 0")
        if policy["daily_replacement_budget"] <= 0:
            raise RuntimeConfigInvalidError("daily_replacement_budget must be positive")
        return policy

    @staticmethod
    def _normalize_price_basis(value: str) -> str:
        text = str(value or "").strip()
        if text not in SUPPORTED_PRICE_BASIS:
            raise RuntimeConfigInvalidError("unsupported advisory price basis", context={"price_basis": value, "supported": sorted(SUPPORTED_PRICE_BASIS)})
        return text

    @staticmethod
    def _normalize_status(value: str) -> str:
        text = str(value or "").strip().upper()
        allowed = {
            PROGRAM_STATUS_DRAFT,
            PROGRAM_STATUS_ENABLED,
            PROGRAM_STATUS_PAUSED,
            PROGRAM_STATUS_REVIEWING,
            PROGRAM_STATUS_WAITING_DATA,
            PROGRAM_STATUS_REVIEW_FAILED,
            PROGRAM_STATUS_ARCHIVED,
        }
        if text not in allowed:
            raise RuntimeConfigInvalidError("unsupported advisory program status", context={"status": value})
        return text

    def _with_status(self, program: AdvisoryProgram, status: str) -> AdvisoryProgram:
        target = self._normalize_status(status)
        if program.status == PROGRAM_STATUS_ARCHIVED and target != PROGRAM_STATUS_ARCHIVED:
            raise InvalidStateTransitionError("archived advisory program cannot be reactivated", context={"program_id": program.program_id})
        if target == PROGRAM_STATUS_REVIEWING:
            raise RuntimeConfigInvalidError("use run_review for REVIEWING state")
        enabled_since = program.enabled_since
        if target == PROGRAM_STATUS_ENABLED and enabled_since is None:
            enabled_since = _utcnow()
        return replace(program, status=target, enabled_since=enabled_since, updated_at=_utcnow())

    @staticmethod
    def _leaderboard_key(sort_by: str) -> Any:
        desc = {
            "win_rate": "win_rate",
            "all_episode_win_rate": "win_rate",
            "avg_return_bps": "avg_return_bps",
            "median_return_bps": "median_return_bps",
            "entered_episode_count": "entered_episode_count",
        }
        asc = {"enabled_since": "enabled_since", "max_drawdown_bps": "max_drawdown_bps"}
        if sort_by in asc:
            field_name = asc[sort_by]
            return lambda row: (_sort_none_last(row.get(field_name)), row["program_id"])
        field_name = desc.get(sort_by, "win_rate")
        return lambda row: (-_sort_number(row.get(field_name)), row["program_id"])


def candidates_from_selection_run(run: SelectionRun) -> list[AdvisoryCandidate]:
    return [
        AdvisoryCandidate(
            symbol=item.symbol,
            rank=item.rank,
            score=item.score,
            reference_price=item.reference_price,
            signal_close=item.previous_close,
            next_open_executable=item.selection_entry_price,
            next_close=item.current_price,
            component_scores=dict(item.component_scores or {}),
            stock_name=item.stock_name,
            source_run_id=run.run_id,
        )
        for item in run.aggregate_results
    ]


def compute_program_metrics(program: AdvisoryProgram, episodes: Iterable[AdvisoryEpisode]) -> dict[str, Any]:
    rows = list(episodes)
    evaluable = [row for row in rows if row.return_bps is not None and row.win_rate_inclusion_status in {WIN_INCLUDED, WIN_OPEN_MARK}]
    returns = [float(row.return_bps) for row in evaluable if row.return_bps is not None]
    wins = [row for row in evaluable if row.return_bps is not None and row.return_bps > 0]
    drawdowns = [float(row.max_drawdown_bps) for row in rows if row.max_drawdown_bps is not None]
    holding_days = [row.holding_trading_days for row in rows]
    return {
        "enabled_since": program.enabled_since.isoformat() if program.enabled_since else None,
        "entered_episode_count": len(rows),
        "active_count": sum(1 for row in rows if row.status == EPISODE_STATUS_ACTIVE),
        "take_profit_count": sum(1 for row in rows if row.exit_reason in {EXIT_TAKE_PROFIT, EXIT_TRAILING_TAKE_PROFIT}),
        "stop_loss_count": sum(1 for row in rows if row.exit_reason == EXIT_STOP_LOSS),
        "win_rate": (len(wins) / len(evaluable)) if evaluable else None,
        "avg_return_bps": mean(returns) if returns else None,
        "median_return_bps": median(returns) if returns else None,
        "max_drawdown_bps": min(drawdowns) if drawdowns else None,
        "avg_holding_days": mean(holding_days) if holding_days else None,
        "last_review_status": program.last_review_status,
    }


def program_to_dict(program: AdvisoryProgram) -> dict[str, Any]:
    return _json_ready(asdict(program))


def episode_to_dict(episode: AdvisoryEpisode) -> dict[str, Any]:
    return _json_ready(asdict(episode))


def decision_to_dict(decision: AdvisoryReviewDecision) -> dict[str, Any]:
    return _json_ready(asdict(decision))


def review_result_to_dict(result: AdvisoryReviewResult) -> dict[str, Any]:
    return {
        "program": program_to_dict(result.program),
        "trade_date": result.trade_date.isoformat(),
        "review_status": result.review_status,
        "decisions": [decision_to_dict(row) for row in result.decisions],
        "active_pool": [episode_to_dict(row) for row in result.active_pool],
        "metrics": _json_ready(result.metrics),
        "preview": result.preview,
    }


def _candidate_from_mapping(row: Mapping[str, Any]) -> AdvisoryCandidate:
    symbol = str(row.get("symbol") or row.get("code") or "").strip().upper()
    if not symbol:
        raise RuntimeConfigInvalidError("advisory candidate symbol is required")
    rank = _optional_int(row.get("rank"))
    if rank is None or rank <= 0:
        raise RuntimeConfigInvalidError("advisory candidate rank must be positive", context={"symbol": symbol, "rank": row.get("rank")})
    return AdvisoryCandidate(
        symbol=symbol,
        rank=rank,
        score=_optional_float(row.get("score")),
        reference_price=_optional_float(row.get("reference_price")),
        signal_close=_optional_float(row.get("signal_close")),
        next_open_executable=_optional_float(row.get("next_open_executable")),
        next_close=_optional_float(row.get("next_close")),
        component_scores=dict(row.get("component_scores") or row.get("fusion_evidence") or {}),
        stock_name=row.get("stock_name"),
        source_run_id=row.get("source_run_id"),
    )


def _market_from_mapping(symbol: str, row: Mapping[str, Any], *, trade_date: date) -> AdvisoryMarketMark:
    return AdvisoryMarketMark(
        symbol=str(row.get("symbol") or symbol).strip().upper(),
        trade_date=date.fromisoformat(str(row["trade_date"])) if row.get("trade_date") else trade_date,
        mark_price=_optional_float(row.get("mark_price") or row.get("current_price")),
        signal_close=_optional_float(row.get("signal_close")),
        next_open_executable=_optional_float(row.get("next_open_executable")),
        next_close=_optional_float(row.get("next_close")),
        suspended=bool(row.get("suspended", False)),
        price_quality_status=str(row.get("price_quality_status") or "OK"),
    )


def _candidate_evidence(candidate: AdvisoryCandidate | None, program: AdvisoryProgram) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "program_id": program.program_id,
        "program_version": program.version,
        "review_policy_sha256": program.review_policy_sha256,
        "price_basis": program.entry_price_basis,
        "package_mode": program.package_mode,
        "package_ids": program.package_ids,
        "fusion_policy_sha256": program.fusion_policy_sha256,
    }
    if candidate is not None:
        payload.update(
            {
                "symbol": candidate.symbol,
                "rank": candidate.rank,
                "score": candidate.score,
                "source_run_id": candidate.source_run_id,
                "component_scores": deepcopy(candidate.component_scores),
            }
        )
    return payload


def _episode_with_mark(episode: AdvisoryEpisode, *, evidence: AdvisoryCandidate, price: float, program: AdvisoryProgram) -> AdvisoryEpisode:
    return_bps = (float(price) / episode.entry_price - 1.0) * 10000.0
    weak_days = episode.weak_rank_confirm_days + 1 if evidence.rank > int(program.review_policy["rank_exit_threshold"]) else 0
    return replace(
        episode,
        current_rank=evidence.rank,
        current_score=evidence.score,
        holding_trading_days=episode.holding_trading_days + 1,
        return_bps=return_bps,
        is_win=return_bps > 0,
        max_runup_bps=max(_coalesce_float(episode.max_runup_bps, return_bps), return_bps),
        max_drawdown_bps=min(_coalesce_float(episode.max_drawdown_bps, return_bps), return_bps),
        still_active_mark_price=price,
        weak_rank_confirm_days=weak_days,
        evidence_json=_candidate_evidence(evidence, program),
        updated_at=_utcnow(),
    )


def _exit_reason(episode: AdvisoryEpisode, *, evidence: AdvisoryCandidate, program: AdvisoryProgram) -> str | None:
    policy = program.review_policy
    return_bps = episode.return_bps
    if return_bps is None:
        return None
    if int(policy["stop_loss_bps"]) > 0 and return_bps <= -int(policy["stop_loss_bps"]):
        return EXIT_STOP_LOSS
    if int(policy["time_stop_days"]) > 0 and episode.holding_trading_days >= int(policy["time_stop_days"]):
        return EXIT_TIME_STOP
    if evidence.rank > int(policy["rank_exit_threshold"]):
        return EXIT_ALPHA_RANK_DROP
    take_bps = int(policy["take_profit_bps"])
    trailing_bps = int(policy["trailing_stop_bps"])
    if take_bps > 0:
        if str(policy.get("take_profit_mode") or "trailing") == "fixed" and return_bps >= take_bps:
            return EXIT_TAKE_PROFIT
        if (episode.max_runup_bps or 0) >= take_bps and trailing_bps > 0 and return_bps <= float(episode.max_runup_bps or 0) - trailing_bps:
            return EXIT_TRAILING_TAKE_PROFIT
    return None


def _episode_exited(episode: AdvisoryEpisode, *, trade_date: date, exit_price: float, exit_basis: str, exit_reason: str) -> AdvisoryEpisode:
    return_bps = (float(exit_price) / episode.entry_price - 1.0) * 10000.0
    return replace(
        episode,
        status=EPISODE_STATUS_EXITED,
        exit_signal_date=trade_date,
        effective_exit_date=_effective_date(trade_date, exit_basis),
        exit_price=exit_price,
        exit_price_basis=exit_basis,
        exit_reason=exit_reason,
        return_bps=return_bps,
        is_win=return_bps > 0,
        still_active_mark_price=None,
        updated_at=_utcnow(),
    )


def _price_for_basis(mark: AdvisoryMarketMark | None, candidate: AdvisoryCandidate | None, basis: str) -> float | None:
    if basis == PRICE_BASIS_NEXT_OPEN:
        values = [getattr(mark, "next_open_executable", None), getattr(candidate, "next_open_executable", None)]
    elif basis == PRICE_BASIS_SIGNAL_CLOSE:
        values = [getattr(mark, "signal_close", None), getattr(candidate, "signal_close", None), getattr(candidate, "reference_price", None), getattr(mark, "mark_price", None)]
    else:
        values = [getattr(mark, "next_close", None), getattr(candidate, "next_close", None), getattr(mark, "mark_price", None), getattr(candidate, "reference_price", None)]
    if mark is not None and mark.suspended:
        return None
    for value in values:
        parsed = _optional_float(value)
        if parsed is not None and parsed > 0:
            return parsed
    return None


def _effective_date(signal_date: date, basis: str) -> date:
    return signal_date if basis == PRICE_BASIS_SIGNAL_CLOSE else signal_date + timedelta(days=1)


def _latest_by_episode_id(rows: Iterable[AdvisoryEpisode]) -> list[AdvisoryEpisode]:
    latest: dict[str, AdvisoryEpisode] = {}
    for row in rows:
        current = latest.get(row.episode_id)
        if current is None or row.updated_at >= current.updated_at:
            latest[row.episode_id] = row
    return sorted(latest.values(), key=lambda row: (row.status != EPISODE_STATUS_ACTIVE, row.entry_rank, row.symbol, row.episode_id))


def _program_sql_params(program: AdvisoryProgram) -> tuple[Any, ...]:
    return (
        program.program_id,
        program.program_name,
        program.status,
        program.target_count,
        program.package_mode,
        psycopg2.extras.Json(program.package_ids),
        psycopg2.extras.Json(program.package_weights),
        program.fusion_method,
        program.package_set_hash,
        program.fusion_policy_sha256,
        psycopg2.extras.Json(program.review_policy),
        program.review_policy_sha256,
        program.entry_price_basis,
        program.exit_price_basis,
        psycopg2.extras.Json(program.review_schedule),
        program.version,
        program.created_by,
        program.enabled_since,
        program.last_review_status,
        program.latest_review_trade_date,
        psycopg2.extras.Json(program_to_dict(program)),
        program.created_at,
        program.updated_at,
    )


def _episode_sql_params(episode: AdvisoryEpisode) -> tuple[Any, ...]:
    return (
        episode.episode_id,
        episode.program_id,
        episode.program_version,
        episode.symbol,
        episode.status,
        episode.signal_date,
        episode.effective_entry_date,
        episode.entry_price,
        episode.entry_price_basis,
        episode.entry_rank,
        episode.entry_score,
        episode.current_rank,
        episode.current_score,
        episode.exit_signal_date,
        episode.effective_exit_date,
        episode.exit_price,
        episode.exit_price_basis,
        episode.exit_reason,
        episode.holding_trading_days,
        episode.return_bps,
        episode.is_win,
        episode.win_rate_inclusion_status,
        episode.max_runup_bps,
        episode.max_drawdown_bps,
        episode.still_active_mark_price,
        episode.price_quality_status,
        episode.weak_rank_confirm_days,
        episode.source_run_id,
        psycopg2.extras.Json(episode.evidence_json),
        psycopg2.extras.Json(episode_to_dict(episode)),
        episode.created_at,
        episode.updated_at,
    )


def _program_from_row(row: Mapping[str, Any]) -> AdvisoryProgram:
    payload = dict(row.get("program_payload_json") or {})
    if payload:
        return AdvisoryProgram(
            program_id=str(payload["program_id"]),
            program_name=str(payload["program_name"]),
            status=str(payload["status"]),
            target_count=int(payload["target_count"]),
            package_mode=str(payload["package_mode"]),
            package_ids=list(payload["package_ids"]),
            package_weights={str(k): float(v) for k, v in dict(payload["package_weights"]).items()},
            fusion_method=payload.get("fusion_method"),
            package_set_hash=str(payload["package_set_hash"]),
            fusion_policy_sha256=payload.get("fusion_policy_sha256"),
            review_policy=dict(payload["review_policy"]),
            review_policy_sha256=str(payload["review_policy_sha256"]),
            entry_price_basis=str(payload["entry_price_basis"]),
            exit_price_basis=str(payload["exit_price_basis"]),
            review_schedule=dict(payload["review_schedule"]),
            version=int(payload.get("version") or 1),
            created_by=payload.get("created_by"),
            enabled_since=_parse_datetime(payload.get("enabled_since")),
            last_review_status=payload.get("last_review_status"),
            latest_review_trade_date=_parse_date(payload.get("latest_review_trade_date")),
            created_at=_parse_datetime(payload.get("created_at")) or _utcnow(),
            updated_at=_parse_datetime(payload.get("updated_at")) or _utcnow(),
        )
    return AdvisoryProgram(
        program_id=str(row["program_id"]),
        program_name=str(row["program_name"]),
        status=str(row["status"]),
        target_count=int(row["target_count"]),
        package_mode=str(row["package_mode"]),
        package_ids=list(row.get("package_ids") or []),
        package_weights={str(k): float(v) for k, v in dict(row.get("package_weights") or {}).items()},
        fusion_method=row.get("fusion_method"),
        package_set_hash=str(row["package_set_hash"]),
        fusion_policy_sha256=row.get("fusion_policy_sha256"),
        review_policy=dict(row.get("review_policy") or {}),
        review_policy_sha256=str(row["review_policy_sha256"]),
        entry_price_basis=str(row["entry_price_basis"]),
        exit_price_basis=str(row["exit_price_basis"]),
        review_schedule=dict(row.get("review_schedule") or {}),
        version=int(row.get("version") or 1),
        created_by=row.get("created_by"),
        enabled_since=row.get("enabled_since"),
        last_review_status=row.get("last_review_status"),
        latest_review_trade_date=row.get("latest_review_trade_date"),
        created_at=row.get("created_at") or _utcnow(),
        updated_at=row.get("updated_at") or _utcnow(),
    )


def _episode_from_row(row: Mapping[str, Any]) -> AdvisoryEpisode:
    payload = dict(row.get("episode_payload_json") or {})
    if payload:
        return _episode_from_payload(payload)
    return AdvisoryEpisode(
        episode_id=str(row["episode_id"]),
        program_id=str(row["program_id"]),
        program_version=int(row.get("program_version") or 1),
        symbol=str(row["symbol"]),
        status=str(row.get("episode_status") or EPISODE_STATUS_ACTIVE),
        signal_date=row["signal_date"],
        effective_entry_date=row["effective_entry_date"],
        entry_price=float(row["entry_price"]),
        entry_price_basis=str(row["entry_price_basis"]),
        entry_rank=int(row["entry_rank"]),
        entry_score=_optional_float(row.get("entry_score")),
        current_rank=_optional_int(row.get("current_rank")),
        current_score=_optional_float(row.get("current_score")),
        exit_signal_date=row.get("exit_signal_date"),
        effective_exit_date=row.get("effective_exit_date"),
        exit_price=_optional_float(row.get("exit_price")),
        exit_price_basis=row.get("exit_price_basis"),
        exit_reason=row.get("exit_reason"),
        holding_trading_days=int(row.get("holding_trading_days") or 0),
        return_bps=_optional_float(row.get("return_bps")),
        is_win=row.get("is_win"),
        win_rate_inclusion_status=str(row.get("win_rate_inclusion_status") or WIN_INCLUDED),
        max_runup_bps=_optional_float(row.get("max_runup_bps")),
        max_drawdown_bps=_optional_float(row.get("max_drawdown_bps")),
        still_active_mark_price=_optional_float(row.get("still_active_mark_price")),
        price_quality_status=str(row.get("price_quality_status") or "OK"),
        weak_rank_confirm_days=int(row.get("weak_rank_confirm_days") or 0),
        source_run_id=row.get("source_run_id"),
        evidence_json=dict(row.get("evidence_json") or {}),
        created_at=row.get("created_at") or _utcnow(),
        updated_at=row.get("updated_at") or _utcnow(),
    )


def _episode_from_payload(payload: Mapping[str, Any]) -> AdvisoryEpisode:
    return AdvisoryEpisode(
        episode_id=str(payload["episode_id"]),
        program_id=str(payload["program_id"]),
        program_version=int(payload["program_version"]),
        symbol=str(payload["symbol"]),
        status=str(payload["status"]),
        signal_date=_parse_date(payload["signal_date"]) or date.today(),
        effective_entry_date=_parse_date(payload["effective_entry_date"]) or date.today(),
        entry_price=float(payload["entry_price"]),
        entry_price_basis=str(payload["entry_price_basis"]),
        entry_rank=int(payload["entry_rank"]),
        entry_score=_optional_float(payload.get("entry_score")),
        current_rank=_optional_int(payload.get("current_rank")),
        current_score=_optional_float(payload.get("current_score")),
        exit_signal_date=_parse_date(payload.get("exit_signal_date")),
        effective_exit_date=_parse_date(payload.get("effective_exit_date")),
        exit_price=_optional_float(payload.get("exit_price")),
        exit_price_basis=payload.get("exit_price_basis"),
        exit_reason=payload.get("exit_reason"),
        holding_trading_days=int(payload.get("holding_trading_days") or 0),
        return_bps=_optional_float(payload.get("return_bps")),
        is_win=payload.get("is_win"),
        win_rate_inclusion_status=str(payload.get("win_rate_inclusion_status") or WIN_INCLUDED),
        max_runup_bps=_optional_float(payload.get("max_runup_bps")),
        max_drawdown_bps=_optional_float(payload.get("max_drawdown_bps")),
        still_active_mark_price=_optional_float(payload.get("still_active_mark_price")),
        price_quality_status=str(payload.get("price_quality_status") or "OK"),
        weak_rank_confirm_days=int(payload.get("weak_rank_confirm_days") or 0),
        source_run_id=payload.get("source_run_id"),
        evidence_json=dict(payload.get("evidence_json") or {}),
        created_at=_parse_datetime(payload.get("created_at")) or _utcnow(),
        updated_at=_parse_datetime(payload.get("updated_at")) or _utcnow(),
    )


def _decision_from_row(row: Mapping[str, Any]) -> AdvisoryReviewDecision:
    payload = dict(row.get("decision_input_json") or {})
    if payload.get("program_id") and payload.get("symbol"):
        return AdvisoryReviewDecision(
            program_id=str(payload["program_id"]),
            program_version=int(payload["program_version"]),
            trade_date=_parse_date(payload["trade_date"]) or row["trade_date"],
            symbol=str(payload["symbol"]),
            action=str(payload["action"]),
            reason_code=str(payload["reason_code"]),
            review_status=str(payload["review_status"]),
            episode_id=payload.get("episode_id"),
            rank=_optional_int(payload.get("rank")),
            score=_optional_float(payload.get("score")),
            entry_price=_optional_float(payload.get("entry_price")),
            exit_price=_optional_float(payload.get("exit_price")),
            return_bps=_optional_float(payload.get("return_bps")),
            evidence_json=dict(payload.get("evidence_json") or {}),
            created_at=_parse_datetime(payload.get("created_at")) or _utcnow(),
        )
    return AdvisoryReviewDecision(
        program_id=str(row["program_id"]),
        program_version=int(row.get("program_version") or 1),
        trade_date=row["trade_date"],
        symbol=str(row["code"]),
        action=str(row["action"]),
        reason_code=str(row["reason_code"]),
        review_status=str(row.get("review_status") or REVIEW_STATUS_SUCCEEDED),
        episode_id=row.get("episode_id"),
        rank=_optional_int(row.get("rank")),
        score=_optional_float(row.get("score")),
        exit_price=_optional_float(row.get("current_price")),
        evidence_json=dict(row.get("fusion_evidence_json") or {}),
        created_at=row.get("created_at") or _utcnow(),
    )


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_ready(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_json_ready(child) for child in value]
    return value


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _optional_float(value: Any) -> float | None:
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


def _coalesce_float(value: float | None, fallback: float) -> float:
    return float(value) if value is not None else float(fallback)


def _sort_number(value: Any) -> float:
    parsed = _optional_float(value)
    return parsed if parsed is not None else -10**18


def _sort_none_last(value: Any) -> Any:
    if value is None:
        return (1, "")
    return (0, str(value))
