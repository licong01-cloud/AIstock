"""Dry-run and gated apply helper for StrategyPackage binding refreeze.

Default mode is read-only. Apply mode performs production DML only when the
operator flag and environment confirmation token are both present.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterator
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.simulation_runtime.models import (  # noqa: E402
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    canonical_json_sha256,
)
from backend.services.simulation_runtime.repository import SimulationRuntimeRepository  # noqa: E402
from backend.services.strategy_package.manifest import compute_manifest_sha256  # noqa: E402
from backend.services.strategy_package.models import PackageStatus, StrategyPackageManifest  # noqa: E402
from backend.services.trading_core.errors import (  # noqa: E402
    DataUnavailableError,
    InvalidStateTransitionError,
    RuntimeConfigInvalidError,
    StrategyPackageValidationError,
)

APPLY_CONFIRM_ENV = "STRATEGY_PACKAGE_BINDING_REFREEZE_APPLY"
APPLY_CONFIRM_VALUE = "I_UNDERSTAND_PRODUCTION_DML"
DEFAULT_OPERATOR = "strategy_package_binding_refreeze"
REFREEZE_REASON = "strategy_package_binding_refreeze_after_asset_backfill"
TARGET_PROD = "prod"
TARGET_DEV = "dev"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

SIM_APPROVAL_STATES = {
    SimulationBindingApprovalState.DRAFT,
    SimulationBindingApprovalState.SIM_VALIDATING,
    SimulationBindingApprovalState.SIM_PASSED,
}
LIVE_APPROVAL_STATES = {
    SimulationBindingApprovalState.LIVE_APPROVAL_PENDING,
    SimulationBindingApprovalState.LIVE_APPROVED,
}


class BindingRefreezeScriptError(RuntimeError):
    """Raised when the refreeze script cannot safely continue."""

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = context or {}


@dataclass(frozen=True)
class PackageSnapshot:
    package_id: str
    manifest_sha256: str
    manifest_json: dict[str, Any]
    package_status: str
    manifest: StrategyPackageManifest

    def current_manifest_json(self) -> dict[str, Any]:
        return self.manifest.model_copy(
            update={
                "manifest_sha256": self.manifest_sha256,
                "package_status": PackageStatus(self.package_status),
            }
        ).model_dump(mode="json")


@dataclass(frozen=True)
class PortfolioRefreezeCandidate:
    portfolio_id: str
    portfolio_name: str | None
    status: str | None
    auto_run_enabled: bool
    old_manifest_sha256: str

    def to_report(self) -> dict[str, Any]:
        return {
            "portfolio_id": self.portfolio_id,
            "portfolio_name": self.portfolio_name,
            "status": self.status,
            "auto_run_enabled": self.auto_run_enabled,
            "old_manifest_sha256": self.old_manifest_sha256,
        }


@dataclass(frozen=True)
class RefreezePlanItem:
    action: str
    reason_code: str
    binding_id: str
    broker_backend: str
    strategy_id: str
    strategy_slot_id: str | None
    account_group_id: str | None
    package_id: str
    old_manifest_sha256: str
    current_manifest_sha256: str
    old_release_id: str | None = None
    old_release_hash: str | None = None
    new_release: StrategyRuntimeRelease | None = None
    new_binding: SimulationReleaseBinding | None = None
    effective_from: date | None = None
    effective_to: date | None = None
    old_binding_effective_to_after: date | None = None
    portfolios: tuple[PortfolioRefreezeCandidate, ...] = ()
    existing_new_release_id: str | None = None
    existing_new_binding_id: str | None = None
    details: dict[str, Any] | None = None

    def to_report(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "reason_code": self.reason_code,
            "binding_id": self.binding_id,
            "broker_backend": self.broker_backend,
            "strategy_id": self.strategy_id,
            "strategy_slot_id": self.strategy_slot_id,
            "account_group_id": self.account_group_id,
            "package_id": self.package_id,
            "old_manifest_sha256": self.old_manifest_sha256,
            "current_manifest_sha256": self.current_manifest_sha256,
            "old_release_id": self.old_release_id,
            "old_release_hash": self.old_release_hash,
            "new_release_id": self.new_release.release_id if self.new_release else None,
            "new_release_hash": self.new_release.release_hash if self.new_release else None,
            "new_binding_id": self.new_binding.binding_id if self.new_binding else None,
            "new_binding_hash": self.new_binding.binding_hash if self.new_binding else None,
            "existing_new_release_id": self.existing_new_release_id,
            "existing_new_binding_id": self.existing_new_binding_id,
            "effective_from": self.effective_from.isoformat() if self.effective_from else None,
            "effective_to": self.effective_to.isoformat() if self.effective_to else None,
            "old_binding_effective_to_after": (
                self.old_binding_effective_to_after.isoformat()
                if self.old_binding_effective_to_after
                else None
            ),
            "portfolio_updates": [item.to_report() for item in self.portfolios],
            "details": self.details or {},
        }


PackageLoader = Callable[[str], PackageSnapshot]
PortfolioLoader = Callable[[str, str, str], list[PortfolioRefreezeCandidate]]


def _load_env_file(path: Path | None) -> None:
    if path is None or not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _db_config(*, target_db: str) -> dict[str, Any]:
    if target_db == TARGET_DEV:
        required = [
            "TDX_DB_DEV_HOST",
            "TDX_DB_DEV_PORT",
            "TDX_DB_DEV_NAME",
            "TDX_DB_DEV_USER",
            "TDX_DB_DEV_PASSWORD",
        ]
        missing = [key for key in required if not os.environ.get(key)]
        if missing:
            raise BindingRefreezeScriptError(
                "missing dev database environment keys",
                context={"reason_code": "BINDING_REFREEZE_DB_ENV_MISSING", "missing_keys": missing},
            )
        cfg = {
            "host": os.environ["TDX_DB_DEV_HOST"],
            "port": int(os.environ["TDX_DB_DEV_PORT"]),
            "dbname": os.environ["TDX_DB_DEV_NAME"],
            "user": os.environ["TDX_DB_DEV_USER"],
            "password": os.environ["TDX_DB_DEV_PASSWORD"],
        }
        host = str(cfg["host"]).lower()
        dbname = str(cfg["dbname"]).lower()
        if host not in {"127.0.0.1", "localhost"} or not any(
            marker in dbname for marker in ("dev", "scratch", "test")
        ):
            raise BindingRefreezeScriptError(
                "refusing dev target because it does not look like a local scratch/dev DB",
                context={
                    "reason_code": "BINDING_REFREEZE_UNSAFE_DEV_TARGET",
                    "host": cfg["host"],
                    "dbname": cfg["dbname"],
                },
            )
        return cfg

    required = ["TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise BindingRefreezeScriptError(
            "missing database environment keys",
            context={"reason_code": "BINDING_REFREEZE_DB_ENV_MISSING", "missing_keys": missing},
        )
    return {
        "host": os.environ["TDX_DB_HOST"],
        "port": int(os.environ["TDX_DB_PORT"]),
        "dbname": os.environ["TDX_DB_NAME"],
        "user": os.environ["TDX_DB_USER"],
        "password": os.environ["TDX_DB_PASSWORD"],
    }


def _target_metadata(cfg: dict[str, Any], *, target_db: str) -> dict[str, Any]:
    return {
        "target_db": target_db,
        "host": cfg["host"],
        "port": cfg["port"],
        "dbname": cfg["dbname"],
        "user": cfg["user"],
        "password_configured": bool(cfg.get("password")),
    }


@contextmanager
def _env_conn_factory(*, env_file: Path | None, target_db: str, readonly: bool) -> Iterator[Any]:
    _load_env_file(env_file)
    conn = psycopg2.connect(**_db_config(target_db=target_db))
    if readonly:
        conn.set_session(readonly=True, autocommit=False)
    else:
        conn.autocommit = False
    try:
        yield conn
        if readonly:
            conn.rollback()
        else:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _repo_from_env(*, env_file: Path | None, target_db: str, readonly: bool) -> SimulationRuntimeRepository:
    def factory() -> Iterator[Any]:
        return _env_conn_factory(env_file=env_file, target_db=target_db, readonly=readonly)

    return SimulationRuntimeRepository(conn_factory=factory)


def _normalize_sha(value: str | None) -> str:
    return str(value or "").strip().lower()


def _ensure_package_self_consistent(snapshot: PackageSnapshot) -> None:
    computed = compute_manifest_sha256(snapshot.manifest)
    stored = _normalize_sha(snapshot.manifest_sha256)
    if stored != computed:
        raise BindingRefreezeScriptError(
            "current package manifest_sha256 does not match manifest_json; refusing to freeze binding",
            context={
                "reason_code": "PACKAGE_MANIFEST_SELF_INCONSISTENT",
                "package_id": snapshot.package_id,
                "stored_manifest_sha256": snapshot.manifest_sha256,
                "computed_manifest_sha256": computed,
            },
        )


def _binding_active_on(binding: SimulationReleaseBinding, active_on: date) -> bool:
    return (binding.effective_from is None or binding.effective_from <= active_on) and (
        binding.effective_to is None or binding.effective_to >= active_on
    )


def _binding_effective_to_on_or_after(binding: SimulationReleaseBinding, active_on: date) -> bool:
    return binding.effective_to is None or binding.effective_to >= active_on


def _derive_refrozen_release(
    source: StrategyRuntimeRelease,
    *,
    current_manifest_sha256: str,
    effective_from: date | None,
    effective_to: date | None,
    operator: str,
    now: datetime,
) -> StrategyRuntimeRelease:
    config = deepcopy(source.release_config_json or {})
    config["schema_version"] = config.get("schema_version") or "strategy_runtime_release_v1"
    config["package_id"] = source.package_id
    config["manifest_sha256"] = current_manifest_sha256
    config["base_release_id"] = source.release_id
    release_hash = canonical_json_sha256(config)
    return StrategyRuntimeRelease(
        release_id=f"srr_{release_hash[:16]}",
        package_id=source.package_id,
        manifest_sha256=current_manifest_sha256,
        base_release_id=source.release_id,
        runtime_profile_id=source.runtime_profile_id,
        runtime_profile_version_id=source.runtime_profile_version_id,
        runtime_profile_sha256=source.runtime_profile_sha256,
        daily_strategy_profile_version_id=source.daily_strategy_profile_version_id,
        execution_policy_version_id=source.execution_policy_version_id,
        execution_policy_sha256=source.execution_policy_sha256,
        tail_policy_version_id=source.tail_policy_version_id,
        tail_policy_sha256=source.tail_policy_sha256,
        release_config_json=config,
        release_hash=release_hash,
        validation_state=source.validation_state,
        validation_evidence=deepcopy(source.validation_evidence or {}),
        effective_from=effective_from,
        effective_to=effective_to,
        created_by=operator,
        created_reason=REFREEZE_REASON,
        created_at=now,
        updated_at=now,
    )


def _derive_refrozen_binding(
    source: SimulationReleaseBinding,
    *,
    new_release: StrategyRuntimeRelease,
    effective_from: date,
    effective_to: date | None,
    operator: str,
    now: datetime,
) -> SimulationReleaseBinding:
    config = deepcopy(source.binding_config_json or {})
    config.update(
        {
            "schema_version": config.get("schema_version") or "simulation_release_binding_v1",
            "strategy_id": source.strategy_id,
            "release_id": new_release.release_id,
            "release_hash": new_release.release_hash,
            "package_id": new_release.package_id,
            "manifest_sha256": new_release.manifest_sha256,
            "broker_backend": source.broker_backend.value,
            "broker_account_id": source.broker_account_id,
            "capital_allocation": float(source.capital_allocation),
            "strategy_name": source.strategy_name,
            "order_remark_prefix": source.order_remark_prefix,
            "approval_state": source.approval_state.value,
            "metadata": deepcopy(config.get("metadata") if isinstance(config.get("metadata"), dict) else {}),
        }
    )
    if source.account_group_id is not None:
        config["account_group_id"] = source.account_group_id
    else:
        config.pop("account_group_id", None)
    if source.strategy_slot_id is not None:
        config["strategy_slot_id"] = source.strategy_slot_id
    else:
        config.pop("strategy_slot_id", None)
    binding_hash = canonical_json_sha256(config)
    return SimulationReleaseBinding(
        binding_id=f"simbind_{binding_hash[:16]}",
        strategy_id=source.strategy_id,
        release_id=new_release.release_id,
        release_hash=new_release.release_hash or "",
        package_id=new_release.package_id,
        manifest_sha256=new_release.manifest_sha256,
        broker_backend=source.broker_backend,
        broker_account_id=source.broker_account_id,
        account_group_id=source.account_group_id,
        strategy_slot_id=source.strategy_slot_id,
        capital_allocation=float(source.capital_allocation),
        strategy_name=source.strategy_name,
        order_remark_prefix=source.order_remark_prefix,
        effective_from=effective_from,
        effective_to=effective_to,
        approval_state=source.approval_state,
        binding_config_json=config,
        binding_hash=binding_hash,
        created_by=operator,
        created_reason=REFREEZE_REASON,
        created_at=now,
        updated_at=now,
    )


def _refreeze_effective_window(
    source: SimulationReleaseBinding,
    active_on: date,
) -> tuple[date, date | None, date | None, dict[str, Any]]:
    """Choose a non-overlapping replacement window without mutating history."""

    source_from = source.effective_from
    source_to = source.effective_to
    if source_from is not None and source_from >= active_on:
        replacement_from = (source_to or source_from) + timedelta(days=1)
        replacement_to = replacement_from if source_to is not None else None
        return (
            replacement_from,
            replacement_to,
            source_to,
            {
                "window_policy": "future_replacement_preserves_same_day_source_window",
                "source_effective_from": source_from.isoformat(),
                "source_effective_to": source_to.isoformat() if source_to else None,
            },
        )

    replacement_from = active_on
    replacement_to = source_to if source_to is None or source_to >= replacement_from else replacement_from
    old_effective_to = replacement_from - timedelta(days=1)
    if source_from is not None and old_effective_to < source_from:
        raise BindingRefreezeScriptError(
            "cannot supersede source binding without creating an invalid effective window",
            context={
                "reason_code": "BINDING_REFREEZE_UNSAFE_EFFECTIVE_WINDOW",
                "binding_id": source.binding_id,
                "source_effective_from": source_from.isoformat(),
                "new_effective_from": replacement_from.isoformat(),
                "requested_old_effective_to": old_effective_to.isoformat(),
            },
        )
    return (
        replacement_from,
        replacement_to,
        old_effective_to,
        {
            "window_policy": "supersede_source_before_replacement",
            "source_effective_from": source_from.isoformat() if source_from else None,
            "source_effective_to": source_to.isoformat() if source_to else None,
        },
    )


def _candidate_bindings(
    repo: SimulationRuntimeRepository,
    *,
    binding_ids: list[str],
    backends: list[str],
    active_on: date,
    limit: int,
) -> list[SimulationReleaseBinding]:
    if binding_ids:
        bindings = [repo.get_simulation_release_binding(binding_id) for binding_id in binding_ids]
    else:
        backend_values = backends or [backend.value for backend in SimulationBrokerBackend]
        bindings = []
        for backend in backend_values:
            bindings.extend(
                repo.list_simulation_release_bindings(
                    broker_backend=backend,
                    approval_states=[SimulationBindingApprovalState.SIM_VALIDATING],
                    active_on=None,
                    limit=limit,
                )
            )
    selected: list[SimulationReleaseBinding] = []
    seen: set[str] = set()
    backend_filter = {SimulationBrokerBackend(value) for value in backends} if backends else None
    for binding in bindings:
        if binding.binding_id in seen:
            continue
        seen.add(binding.binding_id)
        if backend_filter is not None and binding.broker_backend not in backend_filter:
            continue
        if not binding_ids and not _binding_effective_to_on_or_after(binding, active_on):
            continue
        selected.append(binding)
        if len(selected) >= limit:
            break
    return selected


def _skip_item(
    binding: SimulationReleaseBinding,
    *,
    reason_code: str,
    current_manifest_sha256: str | None = None,
    old_release: StrategyRuntimeRelease | None = None,
    details: dict[str, Any] | None = None,
) -> RefreezePlanItem:
    return RefreezePlanItem(
        action="skip",
        reason_code=reason_code,
        binding_id=binding.binding_id,
        broker_backend=binding.broker_backend.value,
        strategy_id=binding.strategy_id,
        strategy_slot_id=binding.strategy_slot_id,
        account_group_id=binding.account_group_id,
        package_id=binding.package_id,
        old_manifest_sha256=binding.manifest_sha256,
        current_manifest_sha256=current_manifest_sha256 or binding.manifest_sha256,
        old_release_id=old_release.release_id if old_release else binding.release_id,
        old_release_hash=old_release.release_hash if old_release else binding.release_hash,
        details=details,
    )


def build_refreeze_plan(
    repo: SimulationRuntimeRepository,
    *,
    package_loader: PackageLoader,
    portfolio_loader: PortfolioLoader,
    binding_ids: list[str] | None = None,
    backends: list[str] | None = None,
    active_on: date,
    limit: int,
    operator: str,
    target: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise BindingRefreezeScriptError(
            "--limit must be positive",
            context={"reason_code": "BINDING_REFREEZE_BAD_LIMIT", "limit": limit},
        )
    now = datetime.now(UTC)
    items: list[RefreezePlanItem] = []
    planned_portfolio_ids: set[str] = set()
    for binding in _candidate_bindings(
        repo,
        binding_ids=binding_ids or [],
        backends=backends or [],
        active_on=active_on,
        limit=limit,
    ):
        if binding.approval_state in LIVE_APPROVAL_STATES:
            raise BindingRefreezeScriptError(
                "refusing to process LIVE binding",
                context={
                    "reason_code": "BINDING_REFREEZE_LIVE_BINDING_REJECTED",
                    "binding_id": binding.binding_id,
                    "approval_state": binding.approval_state.value,
                },
            )
        if binding.approval_state not in SIM_APPROVAL_STATES:
            items.append(_skip_item(binding, reason_code="BINDING_NOT_SIM_APPROVED"))
            continue
        if binding.approval_state != SimulationBindingApprovalState.SIM_VALIDATING:
            items.append(_skip_item(binding, reason_code="BINDING_NOT_SIM_VALIDATING"))
            continue
        if not _binding_active_on(binding, active_on):
            items.append(
                _skip_item(
                    binding,
                    reason_code="BINDING_NOT_ACTIVE_ON_TARGET_DATE",
                    details={"active_on": active_on.isoformat()},
                )
            )
            continue

        release = repo.get_strategy_runtime_release(binding.release_id)
        package = package_loader(binding.package_id)
        _ensure_package_self_consistent(package)
        current_sha = _normalize_sha(package.manifest_sha256)
        binding_sha = _normalize_sha(binding.manifest_sha256)
        release_sha = _normalize_sha(release.manifest_sha256)
        if binding_sha == current_sha and release_sha == current_sha:
            items.append(
                _skip_item(
                    binding,
                    reason_code="BINDING_ALREADY_CURRENT",
                    current_manifest_sha256=current_sha,
                    old_release=release,
                )
            )
            continue
        if binding_sha != release_sha:
            raise BindingRefreezeScriptError(
                "binding and runtime release manifest sha already disagree; manual review required",
                context={
                    "reason_code": "BINDING_RELEASE_MANIFEST_MISMATCH",
                    "binding_id": binding.binding_id,
                    "release_id": release.release_id,
                    "binding_manifest_sha256": binding.manifest_sha256,
                    "release_manifest_sha256": release.manifest_sha256,
                    "current_manifest_sha256": current_sha,
                },
            )

        new_effective_from, new_effective_to, old_effective_to_after, window_details = _refreeze_effective_window(
            binding,
            active_on,
        )
        new_release = _derive_refrozen_release(
            release,
            current_manifest_sha256=current_sha,
            effective_from=new_effective_from,
            effective_to=new_effective_to,
            operator=operator,
            now=now,
        )
        new_binding = _derive_refrozen_binding(
            binding,
            new_release=new_release,
            effective_from=new_effective_from,
            effective_to=new_effective_to,
            operator=operator,
            now=now,
        )
        existing_release = repo.get_strategy_runtime_release_by_hash(new_release.release_hash or "")
        existing_binding = repo.get_simulation_release_binding_by_hash(new_binding.binding_hash or "")
        portfolios = tuple(
            portfolio
            for portfolio in portfolio_loader(binding.package_id, binding.manifest_sha256, binding.broker_backend.value)
            if portfolio.portfolio_id not in planned_portfolio_ids
        )
        planned_portfolio_ids.update(portfolio.portfolio_id for portfolio in portfolios)
        items.append(
            RefreezePlanItem(
                action="planned_refreeze",
                reason_code="BINDING_MANIFEST_SHA_MISMATCH_AFTER_ASSET_BACKFILL",
                binding_id=binding.binding_id,
                broker_backend=binding.broker_backend.value,
                strategy_id=binding.strategy_id,
                strategy_slot_id=binding.strategy_slot_id,
                account_group_id=binding.account_group_id,
                package_id=binding.package_id,
                old_manifest_sha256=binding.manifest_sha256,
                current_manifest_sha256=current_sha,
                old_release_id=release.release_id,
                old_release_hash=release.release_hash,
                new_release=new_release,
                new_binding=new_binding,
                effective_from=new_effective_from,
                effective_to=new_effective_to,
                old_binding_effective_to_after=old_effective_to_after,
                portfolios=portfolios,
                existing_new_release_id=existing_release.release_id if existing_release else None,
                existing_new_binding_id=existing_binding.binding_id if existing_binding else None,
                details=window_details,
            )
        )

    report_items = [item.to_report() for item in items]
    counts = {
        "scanned_bindings": len(items),
        "planned_refreeze": sum(1 for item in items if item.action == "planned_refreeze"),
        "skipped": sum(1 for item in items if item.action == "skip"),
        "portfolio_updates": sum(len(item.portfolios) for item in items),
    }
    return {
        "schema_version": "strategy_package_binding_refreeze_plan_v1",
        "mode": "dry_run_plan",
        "status": "passed",
        "target": target or {},
        "operator": operator,
        "reason": REFREEZE_REASON,
        "active_on": active_on.isoformat(),
        "filter": {
            "binding_ids": binding_ids or [],
            "backends": backends or [],
            "approval_state": "SIM_VALIDATING",
            "active_on_or_after": active_on.isoformat(),
        },
        "counts": counts,
        "db_writes_executed": False,
        "items": report_items,
    }


def _load_package_snapshot(conn_factory: Callable[[], Iterator[Any]], package_id: str) -> PackageSnapshot:
    with conn_factory() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT package_id, manifest_json, manifest_sha256, package_status
                FROM strategy_pkg.package
                WHERE package_id = %s
                """,
                (package_id,),
            )
            row = cur.fetchone()
    if not row:
        raise DataUnavailableError("strategy package does not exist", context={"package_id": package_id})
    manifest_json = dict(row["manifest_json"] or {})
    manifest = StrategyPackageManifest.model_validate(manifest_json).model_copy(
        update={
            "manifest_sha256": _normalize_sha(row["manifest_sha256"]),
            "package_status": PackageStatus(row["package_status"]),
        }
    )
    return PackageSnapshot(
        package_id=row["package_id"],
        manifest_sha256=_normalize_sha(row["manifest_sha256"]),
        manifest_json=manifest_json,
        package_status=row["package_status"],
        manifest=manifest,
    )


def _list_active_auto_run_portfolios(
    conn_factory: Callable[[], Iterator[Any]],
    package_id: str,
    old_manifest_sha256: str,
    broker_backend: str,
) -> list[PortfolioRefreezeCandidate]:
    with conn_factory() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT portfolio_id, portfolio_name, status, auto_run_enabled, manifest_sha256
                FROM paper_v2.portfolio
                WHERE package_id = %s
                  AND manifest_sha256 = %s
                  AND broker_backend = %s
                  AND COALESCE(auto_run_enabled, FALSE) IS TRUE
                  AND UPPER(COALESCE(status, '')) NOT IN ('RETIRED', 'DELETED', 'ARCHIVED')
                ORDER BY updated_at DESC, portfolio_id ASC
                """,
                (package_id, old_manifest_sha256, broker_backend),
            )
            rows = cur.fetchall()
    return [
        PortfolioRefreezeCandidate(
            portfolio_id=row["portfolio_id"],
            portfolio_name=row.get("portfolio_name"),
            status=row.get("status"),
            auto_run_enabled=bool(row.get("auto_run_enabled")),
            old_manifest_sha256=row["manifest_sha256"],
        )
        for row in rows
    ]


def _insert_package_status_event(
    cur: Any,
    *,
    package: PackageSnapshot,
    item: RefreezePlanItem,
    operator: str,
) -> None:
    cur.execute(
        """
        INSERT INTO strategy_pkg.package_status_event (
            package_id, from_status, to_status, reason, context
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (
            package.package_id,
            package.package_status,
            package.package_status,
            REFREEZE_REASON,
            psycopg2.extras.Json(
                {
                    "operator": operator,
                    "old_release_id": item.old_release_id,
                    "new_release_id": item.new_release.release_id if item.new_release else None,
                    "old_binding_id": item.binding_id,
                    "new_binding_id": item.new_binding.binding_id if item.new_binding else None,
                    "old_manifest_sha256": item.old_manifest_sha256,
                    "new_manifest_sha256": item.current_manifest_sha256,
                    "broker_backend": item.broker_backend,
                    "strategy_slot_id": item.strategy_slot_id,
                    "account_group_id": item.account_group_id,
                    "portfolio_ids": [portfolio.portfolio_id for portfolio in item.portfolios],
                    "reason_code": item.reason_code,
                    "rollback_restore": {
                        "old_binding_effective_to": None,
                        "portfolio_manifest_sha256": item.old_manifest_sha256,
                    },
                }
            ),
        ),
    )


def apply_refreeze_plan(
    *,
    conn: Any,
    plan: dict[str, Any],
    repo: SimulationRuntimeRepository,
    package_loader: PackageLoader,
    operator: str,
) -> dict[str, Any]:
    items = [item for item in plan.get("items", []) if item.get("action") == "planned_refreeze"]
    applied: list[dict[str, Any]] = []
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        for item_payload in items:
            source_binding = repo.get_simulation_release_binding(item_payload["binding_id"])
            source_release = repo.get_strategy_runtime_release(source_binding.release_id)
            package = package_loader(source_binding.package_id)
            _ensure_package_self_consistent(package)
            new_release = _derive_refrozen_release(
                source_release,
                current_manifest_sha256=package.manifest_sha256,
                effective_from=date.fromisoformat(item_payload["effective_from"]),
                effective_to=date.fromisoformat(item_payload["effective_to"]) if item_payload.get("effective_to") else None,
                operator=operator,
                now=datetime.now(UTC),
            )
            new_binding = _derive_refrozen_binding(
                source_binding,
                new_release=new_release,
                effective_from=date.fromisoformat(item_payload["effective_from"]),
                effective_to=date.fromisoformat(item_payload["effective_to"]) if item_payload.get("effective_to") else None,
                operator=operator,
                now=datetime.now(UTC),
            )
            _new_from, _new_to, old_effective_to_after, _window_details = _refreeze_effective_window(
                source_binding,
                date.fromisoformat(item_payload["effective_from"]),
            )
            saved_release = repo.save_strategy_runtime_release(new_release)
            saved_binding = repo.save_simulation_release_binding(new_binding)
            if old_effective_to_after != source_binding.effective_to:
                cur.execute(
                    """
                    UPDATE paper_v2.simulation_release_binding
                    SET effective_to = %s,
                        updated_at = NOW()
                    WHERE binding_id = %s
                      AND manifest_sha256 = %s
                      AND approval_state = 'SIM_VALIDATING'
                      AND (effective_to IS NULL OR effective_to >= %s)
                    """,
                    (
                        old_effective_to_after,
                        source_binding.binding_id,
                        source_binding.manifest_sha256,
                        date.fromisoformat(item_payload["effective_from"]),
                    ),
                )
                if cur.rowcount != 1:
                    raise BindingRefreezeScriptError(
                        "source binding supersede compare-and-set failed",
                        context={
                            "reason_code": "BINDING_REFREEZE_BINDING_CAS_MISMATCH",
                            "binding_id": source_binding.binding_id,
                        },
                    )
            manifest_json = package.current_manifest_json()
            portfolio_ids = [row["portfolio_id"] for row in item_payload.get("portfolio_updates") or []]
            if portfolio_ids:
                cur.execute(
                    """
                    UPDATE paper_v2.portfolio
                    SET manifest_sha256 = %s,
                        frozen_manifest_json = %s,
                        updated_at = NOW()
                    WHERE portfolio_id = ANY(%s)
                      AND package_id = %s
                      AND manifest_sha256 = %s
                      AND COALESCE(auto_run_enabled, FALSE) IS TRUE
                      AND UPPER(COALESCE(status, '')) NOT IN ('RETIRED', 'DELETED', 'ARCHIVED')
                    """,
                    (
                        package.manifest_sha256,
                        psycopg2.extras.Json(manifest_json),
                        portfolio_ids,
                        package.package_id,
                        source_binding.manifest_sha256,
                    ),
                )
                if cur.rowcount != len(portfolio_ids):
                    raise BindingRefreezeScriptError(
                        "portfolio refreeze compare-and-set failed",
                        context={
                            "reason_code": "BINDING_REFREEZE_PORTFOLIO_CAS_MISMATCH",
                            "binding_id": source_binding.binding_id,
                            "expected_portfolio_count": len(portfolio_ids),
                            "updated_portfolio_count": cur.rowcount,
                        },
                    )
            item = RefreezePlanItem(
                action="planned_refreeze",
                reason_code=item_payload["reason_code"],
                binding_id=source_binding.binding_id,
                broker_backend=source_binding.broker_backend.value,
                strategy_id=source_binding.strategy_id,
                strategy_slot_id=source_binding.strategy_slot_id,
                account_group_id=source_binding.account_group_id,
                package_id=source_binding.package_id,
                old_manifest_sha256=source_binding.manifest_sha256,
                current_manifest_sha256=package.manifest_sha256,
                old_release_id=source_release.release_id,
                old_release_hash=source_release.release_hash,
                new_release=saved_release,
                new_binding=saved_binding,
                effective_from=date.fromisoformat(item_payload["effective_from"]),
                effective_to=date.fromisoformat(item_payload["effective_to"]) if item_payload.get("effective_to") else None,
                old_binding_effective_to_after=old_effective_to_after,
                portfolios=tuple(
                    PortfolioRefreezeCandidate(
                        portfolio_id=row["portfolio_id"],
                        portfolio_name=row.get("portfolio_name"),
                        status=row.get("status"),
                        auto_run_enabled=bool(row.get("auto_run_enabled")),
                        old_manifest_sha256=source_binding.manifest_sha256,
                    )
                    for row in item_payload.get("portfolio_updates") or []
                ),
            )
            _insert_package_status_event(cur, package=package, item=item, operator=operator)
            applied.append(item.to_report())
    return {
        "schema_version": "strategy_package_binding_refreeze_apply_v1",
        "mode": "apply",
        "status": "applied",
        "operator": operator,
        "reason": REFREEZE_REASON,
        "applied_count": len(applied),
        "db_writes_executed": bool(applied),
        "items": applied,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refreeze active StrategyPackage simulation bindings safely.")
    parser.add_argument("--env-file", type=Path, default=Path(os.environ.get("AISTOCK_ENV_FILE", ".env")))
    parser.add_argument("--target-db", choices=(TARGET_PROD, TARGET_DEV), default=TARGET_PROD)
    parser.add_argument("--active-on", type=date.fromisoformat, default=datetime.now(SHANGHAI_TZ).date())
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--binding-id", action="append", default=[], help="specific binding_id to inspect; repeatable")
    parser.add_argument("--backend", action="append", choices=[backend.value for backend in SimulationBrokerBackend], default=[])
    parser.add_argument("--operator", default=DEFAULT_OPERATOR)
    parser.add_argument("--dry-run", action="store_true", help="explicit no-op alias; dry-run is the default")
    parser.add_argument("--apply", action="store_true", help="perform DML; dry-run is the default")
    parser.add_argument(
        "--confirm-production-dml",
        action="store_true",
        help="required with --apply --target-db prod; environment token is also required",
    )
    parser.add_argument(
        "--confirm-scratch-dml",
        action="store_true",
        help="required with --apply --target-db dev",
    )
    return parser.parse_args(argv)


def _validate_apply_gate(args: argparse.Namespace) -> None:
    if not args.apply:
        return
    if args.target_db == TARGET_PROD:
        if not args.confirm_production_dml:
            raise BindingRefreezeScriptError(
                "--apply on prod requires --confirm-production-dml",
                context={"reason_code": "BINDING_REFREEZE_PROD_CONFIRM_FLAG_MISSING"},
            )
        if os.environ.get(APPLY_CONFIRM_ENV) != APPLY_CONFIRM_VALUE:
            raise BindingRefreezeScriptError(
                f"--apply on prod requires {APPLY_CONFIRM_ENV}={APPLY_CONFIRM_VALUE}",
                context={"reason_code": "BINDING_REFREEZE_PROD_CONFIRM_ENV_MISSING"},
            )
    elif not args.confirm_scratch_dml:
        raise BindingRefreezeScriptError(
            "--apply --target-db dev requires --confirm-scratch-dml",
            context={"reason_code": "BINDING_REFREEZE_SCRATCH_CONFIRM_FLAG_MISSING"},
        )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.limit <= 0:
        raise BindingRefreezeScriptError(
            "--limit must be positive",
            context={"reason_code": "BINDING_REFREEZE_BAD_LIMIT", "limit": args.limit},
        )
    _load_env_file(args.env_file)
    _validate_apply_gate(args)
    cfg = _db_config(target_db=args.target_db)
    target = _target_metadata(cfg, target_db=args.target_db)

    def readonly_conn_factory() -> Iterator[Any]:
        return _env_conn_factory(env_file=args.env_file, target_db=args.target_db, readonly=True)

    repo = _repo_from_env(env_file=args.env_file, target_db=args.target_db, readonly=True)
    plan = build_refreeze_plan(
        repo,
        package_loader=lambda package_id: _load_package_snapshot(readonly_conn_factory, package_id),
        portfolio_loader=lambda package_id, old_sha, backend: _list_active_auto_run_portfolios(
            readonly_conn_factory,
            package_id,
            old_sha,
            backend,
        ),
        binding_ids=args.binding_id,
        backends=args.backend,
        active_on=args.active_on,
        limit=args.limit,
        operator=args.operator,
        target=target,
    )
    result = plan
    if args.apply:
        with _env_conn_factory(env_file=args.env_file, target_db=args.target_db, readonly=False) as conn:
            repo = SimulationRuntimeRepository(conn_factory=lambda: nullcontext(conn))
            result = apply_refreeze_plan(
                conn=conn,
                plan=plan,
                repo=repo,
                package_loader=lambda package_id: _load_package_snapshot(lambda: nullcontext(conn), package_id),
                operator=args.operator,
            )
            result["target"] = target
    text = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        BindingRefreezeScriptError,
        DataUnavailableError,
        InvalidStateTransitionError,
        RuntimeConfigInvalidError,
        StrategyPackageValidationError,
    ) as exc:
        print(
            json.dumps(
                {"ok": False, "error": str(exc), "context": getattr(exc, "context", {})},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        raise SystemExit(1)
