"""Read-only Paper v2 control plane for canonical PIT migration and activation.

The control plane never rewrites an existing runtime profile, activates the
singleton authority pointer, or submits trading work.  It inventories durable
Paper identities, builds explicit version-migration plans, compares bounded
v1/v2 membership samples, and reports activation blockers.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Callable, Mapping

import psycopg2.extras as pgx

from backend.db.pg_pool import get_conn
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SCOPE,
    CANONICAL_PIT_UNIVERSE_KEY,
    LEGACY_PIT_RULE_VERSION,
    LEGACY_PIT_UNIVERSE_KEY,
    canonical_rule_parameters_digest,
    legacy_rule_parameters_digest,
)
from backend.services.selection_center.canonical_pit_runtime import (
    CANONICAL_PIT_POINTER_PROFILE_SCHEMA,
    CANONICAL_PIT_RUNTIME_LEASE_KEY,
    migrate_runtime_config_to_canonical_pointer,
    validate_canonical_pit_runtime_profile,
)
from backend.services.simulation_runtime.models import SimulationBindingApprovalState
from backend.services.simulation_runtime.repository import SimulationRuntimeRepository
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError

from .models import (
    PaperRuntimeProfileVersion,
    RuntimeConfigActivationStatus,
    RuntimeProfileStatus,
    compute_runtime_config_sha256,
)
from .repository import PaperTradingV2Repository


PAPER_PIT_INVENTORY_SCHEMA = "paper_canonical_pit_inventory_v1"
PAPER_PIT_MIGRATION_PLAN_SCHEMA = "paper_canonical_pit_profile_migration_plan_v1"
PAPER_PIT_SHADOW_SCHEMA = "paper_canonical_pit_shadow_v1"
PAPER_PIT_ACTIVATION_READINESS_SCHEMA = "paper_canonical_pit_activation_readiness_v1"

PIT_PROFILE_CANONICAL = "canonical_pointer"
PIT_PROFILE_LEGACY = "legacy_explicit_v1"
PIT_PROFILE_INVALID = "invalid"

_TERMINAL_SESSION_STATUSES = frozenset({"SUCCEEDED", "FAILED", "STOPPED"})
_TERMINAL_RUN_STATUSES = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})
_INVENTORY_ROW_LIMIT = 10_000


class PaperCanonicalPitControlError(RuntimeConfigInvalidError):
    """Raised when Paper PIT control evidence cannot be proven."""

    error_code = "PAPER_CANONICAL_PIT_CONTROL_INVALID"


def classify_paper_runtime_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Classify one durable config without changing it or guessing defaults."""

    try:
        mode = validate_canonical_pit_runtime_profile(config)
        if mode == CANONICAL_PIT_POINTER_PROFILE_SCHEMA:
            return {"classification": PIT_PROFILE_CANONICAL, "profile_schema": mode, "error": None}
        if CANONICAL_PIT_RUNTIME_LEASE_KEY in config:
            raise PaperCanonicalPitControlError("legacy Paper runtime config cannot carry a canonical PIT lease")
        return {"classification": PIT_PROFILE_LEGACY, "profile_schema": mode, "error": None}
    except Exception as exc:
        return {
            "classification": PIT_PROFILE_INVALID,
            "profile_schema": None,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }


def plan_paper_runtime_profile_migration(version: PaperRuntimeProfileVersion) -> dict[str, Any]:
    """Build a deterministic new-version plan; the source version stays immutable."""

    source = dict(version.config_json)
    classification = classify_paper_runtime_config(source)
    if classification["classification"] == PIT_PROFILE_INVALID:
        raise PaperCanonicalPitControlError(
            "invalid Paper runtime profile cannot be migrated",
            context={
                "profile_version_id": version.profile_version_id,
                "classification": classification,
            },
        )
    if classification["classification"] == PIT_PROFILE_CANONICAL:
        target = source
        action = "NO_OP_ALREADY_CANONICAL"
    else:
        target = migrate_runtime_config_to_canonical_pointer(source)
        action = "CREATE_NEW_CANONICAL_VERSION"
    source_sha = compute_runtime_config_sha256(source)
    target_sha = compute_runtime_config_sha256(target)
    return {
        "schema_version": PAPER_PIT_MIGRATION_PLAN_SCHEMA,
        "action": action,
        "source_profile_version_id": version.profile_version_id,
        "source_version_no": version.version_no,
        "source_config_sha256": source_sha,
        "target_config_sha256": target_sha,
        "target_config_json": target,
        "source_immutable": True,
        "in_place_update_allowed": False,
    }


class PaperCanonicalPitControlService:
    """Inventory, shadow, and activation-readiness service with no writes."""

    def __init__(
        self,
        *,
        repository: PaperTradingV2Repository | Any | None = None,
        simulation_repository: SimulationRuntimeRepository | Any | None = None,
        connection_factory: Callable[[], Any] = get_conn,
    ) -> None:
        self.repository = repository or PaperTradingV2Repository()
        self.simulation_repository = simulation_repository or SimulationRuntimeRepository()
        self.connection_factory = connection_factory

    def inventory(self, *, portfolio_id: str | None = None) -> dict[str, Any]:
        portfolios = (
            [self.repository.get_portfolio(portfolio_id)]
            if portfolio_id is not None
            else _bounded_inventory_rows(
                lambda limit: self.repository.list_portfolios(limit=limit),
                current_count=0,
                scope="portfolios",
            )
        )
        portfolio_ids = {item.portfolio_id for item in portfolios}
        profile_rows: list[dict[str, Any]] = []
        activation_rows: list[dict[str, Any]] = []
        session_rows: list[dict[str, Any]] = []
        run_rows: list[dict[str, Any]] = []
        profile_scanned_count = 0
        session_scanned_count = 0
        run_scanned_count = 0
        version_classification: dict[str, str] = {}

        for portfolio in portfolios:
            profiles = _bounded_inventory_rows(
                lambda limit: self.repository.list_runtime_profiles(portfolio.portfolio_id, limit=limit),
                current_count=profile_scanned_count,
                scope="runtime_profiles",
            )
            profile_scanned_count += len(profiles)
            for profile in profiles:
                versions = _bounded_inventory_rows(
                    lambda limit: self.repository.list_runtime_profile_versions(profile.profile_id, limit=limit),
                    current_count=len(profile_rows),
                    scope="runtime_profile_versions",
                )
                for version in versions:
                    classified = classify_paper_runtime_config(version.config_json)
                    version_classification[version.profile_version_id] = classified["classification"]
                    profile_rows.append(
                        {
                            "portfolio_id": portfolio.portfolio_id,
                            "profile_id": profile.profile_id,
                            "profile_status": profile.status.value,
                            "profile_version_id": version.profile_version_id,
                            "version_no": version.version_no,
                            "config_sha256": version.config_sha256,
                            "is_current_version": version.profile_version_id == profile.current_version_id,
                            **classified,
                        }
                    )
            activations = _bounded_inventory_rows(
                lambda limit: self.repository.list_runtime_config_activations(
                    portfolio.portfolio_id,
                    limit=limit,
                ),
                current_count=len(activation_rows),
                scope="runtime_config_activations",
            )
            for activation in activations:
                activation_rows.append(
                    {
                        "portfolio_id": portfolio.portfolio_id,
                        "activation_id": activation.activation_id,
                        "trade_date": activation.trade_date.isoformat(),
                        "profile_version_id": activation.profile_version_id,
                        "status": activation.status.value,
                        "classification": version_classification.get(
                            activation.profile_version_id,
                            PIT_PROFILE_INVALID,
                        ),
                    }
                )
            sessions = _bounded_inventory_rows(
                lambda limit: self.repository.list_sessions(portfolio.portfolio_id, limit=limit),
                current_count=session_scanned_count,
                scope="sessions",
            )
            session_scanned_count += len(sessions)
            for session in sessions:
                if session.status.value in _TERMINAL_SESSION_STATUSES:
                    continue
                classified = classify_paper_runtime_config(session.runtime_config)
                session_rows.append(
                    {
                        "portfolio_id": portfolio.portfolio_id,
                        "session_id": session.session_id,
                        "mode": session.mode.value,
                        "status": session.status.value,
                        "phase": session.phase.value,
                        "classification": classified["classification"],
                        "has_frozen_lease": CANONICAL_PIT_RUNTIME_LEASE_KEY in session.runtime_config,
                    }
                )
            runs = _bounded_inventory_rows(
                lambda limit: self.repository.list_runs(portfolio.portfolio_id, limit=limit),
                current_count=run_scanned_count,
                scope="runs",
            )
            run_scanned_count += len(runs)
            for run in runs:
                raw_status = run.get("status")
                status = str(getattr(raw_status, "value", raw_status) or "").upper()
                if status in _TERMINAL_RUN_STATUSES:
                    continue
                classified = classify_paper_runtime_config(run.get("runtime_config") or {})
                run_rows.append(
                    {
                        "portfolio_id": portfolio.portfolio_id,
                        "run_id": str(run.get("run_id") or ""),
                        "trade_date": _date_text(run.get("trade_date")),
                        "status": status,
                        "classification": classified["classification"],
                        "has_frozen_lease": CANONICAL_PIT_RUNTIME_LEASE_KEY
                        in (run.get("runtime_config") or {}),
                    }
                )

        release_rows = self._runtime_release_inventory(
            portfolio_ids=portfolio_ids,
            version_classification=version_classification,
        )
        counts = {
            "portfolio_count": len(portfolios),
            "profile_version_count": len(profile_rows),
            "canonical_profile_version_count": sum(
                item["classification"] == PIT_PROFILE_CANONICAL for item in profile_rows
            ),
            "legacy_profile_version_count": sum(
                item["classification"] == PIT_PROFILE_LEGACY for item in profile_rows
            ),
            "invalid_profile_version_count": sum(
                item["classification"] == PIT_PROFILE_INVALID for item in profile_rows
            ),
            "active_config_activation_count": sum(
                item["status"] == RuntimeConfigActivationStatus.ACTIVE.value for item in activation_rows
            ),
            "unfinished_session_count": len(session_rows),
            "unfinished_run_count": len(run_rows),
            "active_runtime_release_count": len(release_rows),
        }
        return {
            "schema_version": PAPER_PIT_INVENTORY_SCHEMA,
            "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "portfolio_filter": portfolio_id,
            "counts": counts,
            "profile_versions": profile_rows,
            "runtime_activations": activation_rows,
            "unfinished_sessions": session_rows,
            "unfinished_runs": run_rows,
            "active_runtime_releases": release_rows,
        }

    def activation_readiness(
        self,
        *,
        portfolio_id: str | None = None,
    ) -> dict[str, Any]:
        inventory = self.inventory(portfolio_id=portfolio_id)
        blockers: list[dict[str, Any]] = []
        for row in inventory["profile_versions"]:
            if row["profile_status"] == RuntimeProfileStatus.ACTIVE.value and row["is_current_version"]:
                if row["classification"] != PIT_PROFILE_CANONICAL:
                    blockers.append(
                        {
                            "reason_code": "PAPER_ACTIVE_PROFILE_NOT_CANONICAL",
                            "portfolio_id": row["portfolio_id"],
                            "profile_id": row["profile_id"],
                            "profile_version_id": row["profile_version_id"],
                            "classification": row["classification"],
                        }
                    )
        for row in inventory["runtime_activations"]:
            if row["status"] == RuntimeConfigActivationStatus.ACTIVE.value and row[
                "classification"
            ] != PIT_PROFILE_CANONICAL:
                blockers.append(
                    {
                        "reason_code": "PAPER_ACTIVE_CONFIG_ACTIVATION_NOT_CANONICAL",
                        **row,
                    }
                )
        for row in inventory["unfinished_sessions"]:
            blockers.append({"reason_code": "PAPER_SIDE_EFFECTING_SESSION_MUST_DRAIN", **row})
        for row in inventory["unfinished_runs"]:
            blockers.append({"reason_code": "PAPER_SIDE_EFFECTING_RUN_MUST_DRAIN", **row})
        for row in inventory["active_runtime_releases"]:
            if row["classification"] != PIT_PROFILE_CANONICAL:
                blockers.append({"reason_code": "PAPER_RUNTIME_RELEASE_NOT_CANONICAL", **row})
        return {
            "schema_version": PAPER_PIT_ACTIVATION_READINESS_SCHEMA,
            "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "ready": not blockers,
            "blocker_count": len(blockers),
            "blockers": blockers,
            "inventory_counts": inventory["counts"],
            "required_operator_sequence": [
                "close_legacy_new_admission",
                "migrate_active_profile_versions_without_in_place_rewrite",
                "drain_side_effecting_paper_sessions_and_runs",
                "recheck_active_runtime_releases",
                "perform_separately_authorized_authority_cas",
            ],
            "production_activation_performed": False,
        }

    def shadow_compare(self, *, trade_date: date, symbols: list[str]) -> dict[str, Any]:
        normalized = _normalize_symbols(symbols)
        if not normalized:
            raise PaperCanonicalPitControlError("Paper PIT shadow requires at least one symbol")
        if len(normalized) > 500:
            raise PaperCanonicalPitControlError(
                "Paper PIT shadow is sample-bounded to 500 symbols",
                context={"symbol_count": len(normalized), "max_symbol_count": 500},
            )
        legacy = self._load_membership(
            universe_key=LEGACY_PIT_UNIVERSE_KEY,
            rule_version=LEGACY_PIT_RULE_VERSION,
            expected_scope="st_only_active",
            trade_date=trade_date,
            symbols=normalized,
        )
        canonical = self._load_membership(
            universe_key=CANONICAL_PIT_UNIVERSE_KEY,
            rule_version=CANONICAL_PIT_RULE_VERSION,
            expected_scope=CANONICAL_PIT_SCOPE,
            trade_date=trade_date,
            symbols=normalized,
        )
        rows = []
        for symbol in normalized:
            in_v1 = symbol in legacy
            in_v2 = symbol in canonical
            state = "both" if in_v1 and in_v2 else "v1_only" if in_v1 else "v2_only" if in_v2 else "neither"
            difference_reason_code = {
                "v1_only": "MEMBER_ONLY_UNDER_LEGACY_RULE",
                "v2_only": "MEMBER_ONLY_UNDER_CANONICAL_RULE",
            }.get(state)
            rows.append(
                {
                    "symbol": symbol,
                    "v1_eligible": in_v1,
                    "v2_eligible": in_v2,
                    "state": state,
                    "difference_reason_code": difference_reason_code,
                    "v1_span": legacy.get(symbol),
                    "v2_span": canonical.get(symbol),
                }
            )
        counts = {state: sum(item["state"] == state for item in rows) for state in ("both", "v1_only", "v2_only", "neither")}
        return {
            "schema_version": PAPER_PIT_SHADOW_SCHEMA,
            "trade_date": trade_date.isoformat(),
            "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "v1": {
                "universe_key": LEGACY_PIT_UNIVERSE_KEY,
                "rule_version": LEGACY_PIT_RULE_VERSION,
                "rule_parameters_digest": legacy_rule_parameters_digest(),
            },
            "v2": {
                "universe_key": CANONICAL_PIT_UNIVERSE_KEY,
                "rule_version": CANONICAL_PIT_RULE_VERSION,
                "rule_parameters_digest": canonical_rule_parameters_digest(),
            },
            "symbol_count": len(normalized),
            "counts": counts,
            "differences": [item for item in rows if item["state"] in {"v1_only", "v2_only"}],
            "rows": rows,
            "read_only": True,
            "orders_submitted": False,
            "official_selection_written": False,
        }

    def _runtime_release_inventory(
        self,
        *,
        portfolio_ids: set[str],
        version_classification: dict[str, str],
    ) -> list[dict[str, Any]]:
        if not portfolio_ids:
            return []
        try:
            bindings = _bounded_inventory_rows(
                lambda limit: self.simulation_repository.list_latest_simulation_release_bindings(limit=limit),
                current_count=0,
                scope="simulation_release_bindings",
            )
        except PaperCanonicalPitControlError:
            raise
        except Exception as exc:
            raise DataUnavailableError(
                "Paper canonical PIT inventory cannot read active runtime bindings",
                context={"portfolio_count": len(portfolio_ids)},
            ) from exc
        rows: list[dict[str, Any]] = []
        for binding in bindings:
            if binding.strategy_id not in portfolio_ids:
                continue
            if binding.approval_state == SimulationBindingApprovalState.RETIRED:
                continue
            try:
                release = self.simulation_repository.get_strategy_runtime_release(binding.release_id)
            except Exception as exc:
                raise DataUnavailableError(
                    "Paper runtime binding references a missing runtime release",
                    context={"binding_id": binding.binding_id, "release_id": binding.release_id},
                ) from exc
            rows.append(
                {
                    "portfolio_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "binding_hash": binding.binding_hash,
                    "approval_state": binding.approval_state.value,
                    "release_id": release.release_id,
                    "release_hash": release.release_hash,
                    "profile_version_id": release.runtime_profile_version_id,
                    "classification": version_classification.get(
                        release.runtime_profile_version_id,
                        PIT_PROFILE_INVALID,
                    ),
                }
            )
        return rows

    def _load_membership(
        self,
        *,
        universe_key: str,
        rule_version: str,
        expected_scope: str,
        trade_date: date,
        symbols: list[str],
    ) -> dict[str, dict[str, Any]]:
        try:
            with self.connection_factory() as conn:
                with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT universe_key, rule_version, scope, status, dirty, start_date, end_date
                          FROM market.stock_universe_pit_state
                         WHERE universe_key = %s
                        """,
                        (universe_key,),
                    )
                    state = cur.fetchone()
                    if not state:
                        raise PaperCanonicalPitControlError(
                            "Paper PIT shadow state is missing",
                            context={"universe_key": universe_key},
                        )
                    state = dict(state)
                    if (
                        state.get("rule_version") != rule_version
                        or state.get("scope") != expected_scope
                        or state.get("status") != "ready"
                        or bool(state.get("dirty"))
                        or not isinstance(state.get("start_date"), date)
                        or not isinstance(state.get("end_date"), date)
                        or trade_date < state["start_date"]
                        or trade_date > state["end_date"]
                    ):
                        raise PaperCanonicalPitControlError(
                            "Paper PIT shadow state is not ready for the requested identity/date",
                            context={
                                "universe_key": universe_key,
                                "rule_version": rule_version,
                                "trade_date": trade_date.isoformat(),
                            },
                        )
                    cur.execute(
                        """
                        SELECT ts_code, eligible_start, eligible_end, entry_reason, exit_reason, terminal_exit
                          FROM market.stock_universe_pit_spans
                         WHERE universe_key = %s
                           AND rule_version = %s
                           AND ts_code = ANY(%s)
                           AND eligible_start <= %s
                           AND eligible_end >= %s
                        """,
                        (universe_key, rule_version, symbols, trade_date, trade_date),
                    )
                    return {
                        str(row["ts_code"]): _span_evidence(row)
                        for row in cur.fetchall()
                    }
        except PaperCanonicalPitControlError:
            raise
        except Exception as exc:
            raise DataUnavailableError(
                "Paper PIT shadow membership lookup failed",
                context={
                    "universe_key": universe_key,
                    "rule_version": rule_version,
                    "trade_date": trade_date.isoformat(),
                    "symbol_count": len(symbols),
                },
            ) from exc


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized = []
    seen = set()
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        normalized.append(symbol)
        seen.add(symbol)
    return normalized


def _date_text(value: Any) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip()
    return text or None


def _span_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "eligible_start": _date_text(row.get("eligible_start")),
        "eligible_end": _date_text(row.get("eligible_end")),
        "entry_reason": row.get("entry_reason"),
        "exit_reason": row.get("exit_reason"),
        "terminal_exit": bool(row.get("terminal_exit")),
    }


def _bounded_inventory_rows(
    fetch: Callable[[int], list[Any]],
    *,
    current_count: int,
    scope: str,
) -> list[Any]:
    remaining = _INVENTORY_ROW_LIMIT - current_count
    if remaining < 0:
        remaining = 0
    rows = list(fetch(remaining + 1))
    if len(rows) > remaining:
        raise PaperCanonicalPitControlError(
            "Paper PIT inventory exceeded its bounded metadata limit",
            context={
                "scope": scope,
                "current_count": current_count,
                "row_limit": _INVENTORY_ROW_LIMIT,
            },
        )
    return rows


__all__ = [
    "PAPER_PIT_ACTIVATION_READINESS_SCHEMA",
    "PAPER_PIT_INVENTORY_SCHEMA",
    "PAPER_PIT_MIGRATION_PLAN_SCHEMA",
    "PAPER_PIT_SHADOW_SCHEMA",
    "PIT_PROFILE_CANONICAL",
    "PIT_PROFILE_INVALID",
    "PIT_PROFILE_LEGACY",
    "PaperCanonicalPitControlError",
    "PaperCanonicalPitControlService",
    "classify_paper_runtime_config",
    "plan_paper_runtime_profile_migration",
]
