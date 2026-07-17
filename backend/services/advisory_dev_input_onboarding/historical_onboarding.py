"""Exact-DEV orchestration for Advisory historical onboarding (O3)."""

from __future__ import annotations

import hashlib
import logging
import os
import subprocess
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any, Callable, Iterator
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

from backend.services.advisory_phase0a.historical_research import (
    HISTORICAL_RESEARCH_DATA_SOURCE,
    REASON_FORBIDDEN_EXECUTION_DEPENDENCY,
    REASON_HISTORICAL_DATE_REQUIRED,
    HistoricalAdvisoryResearchRunner,
    HistoricalResearchBatchRequest,
    HistoricalResearchInputUnavailable,
    HistoricalResearchRunStatus,
)
from backend.services.advisory_phase0a.historical_research_postgres import (
    PersistedHistoricalSelectionEvidenceAdapter,
    PostgresHistoricalResearchProgramResolver,
    PostgresHistoricalResearchRepository,
    PostgresHistoricalResearchTradingDateResolver,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonical_json_text
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    DatabaseConnectionConfig,
    ReleaseSchemaVerificationError,
    resolve_database_connection,
)
from backend.services.advisory_phase1.stage_trace import Phase1TraceCaptureService
from backend.services.advisory_program import (
    BINDING_STATUS_ACTIVE,
    PACKAGE_MODE_SINGLE,
    PROGRAM_STATUS_ARCHIVED,
    PROGRAM_STATUS_DRAFT,
    AdvisoryProgram,
    AdvisoryProgramPGRepository,
    AdvisoryProgramService,
    AdvisoryStrategyBindingVersion,
    _binding_from_program,
    _binding_payload,
)
from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.selection_center.industry_provider import DbSwIndustryLookupProvider
from backend.services.selection_center.package_health import SelectionPackageHealthService
from backend.services.selection_center.prospective_evidence import (
    CandidateStageName,
    DecisionClockEvidenceV2,
    EvidenceCaptureMode,
    EvidenceCaptureStatus,
    EffectiveConfigChainV2,
    ProspectiveExecutionOrigin,
    ProspectiveSelectionContext,
    SourceReadReceipt,
    SelectionStageTrace,
    StageReceiptStatus,
    UniverseEvidenceV2,
    build_stage_receipt,
    canonical_evidence_json_sha256,
)
from backend.services.selection_center.prospective_evidence_assembler import ProspectiveSelectionEvidenceAssembler
from backend.services.selection_center.repository import SelectionCenterRepository
from backend.services.selection_center.result_enrichment import SelectionResultEnrichmentService
from backend.services.selection_center.risk_policy import (
    AnnouncementRiskDecisionProvider,
    RiskDecision,
    StockRiskPolicyService,
)
from backend.services.selection_center.runtime_profile import (
    is_non_trading_runtime_config,
    mark_non_trading_preview_runtime_config,
    normalize_selection_runtime_config,
    parse_selection_runtime_profile,
    refresh_generated_runtime_profile_binding,
    runtime_profile_config_sha256,
    validate_runtime_profile_binding,
)
from backend.services.selection_center.service import SelectionCenterService
from backend.services.selection_center.tradability import DbSuspendLookupProvider, TradabilityFilter
from backend.services.simulation_runtime.repository import SimulationRuntimeRepository
from backend.services.simulation_runtime.selection import DailySelectionSignalService, StrategyPackageSelectionService
from backend.services.stock_universe_pit_service import require_live_st_pit_universe_key
from backend.services.strategy_package.live_inference import (
    QEExperimentRuntimeAssetResolver,
    WslStrategyPackageInferenceProvider,
)
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    StrategyPackageSelectionArtifactRepository,
    StrategyPackageSelectionArtifactService,
)
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError, TradingCoreError

from .contracts import (
    HistoricalProgramResult,
    HistoricalProgramSpec,
    HistoricalProgramStatus,
    RealDevHistoricalRunReceipt,
    RealDevHistoricalRunRequest,
    RealDevOnboardingError,
    RealDevOnboardingRequest,
    REASON_DSE_INVALID,
    REASON_ENV_INVALID,
    REASON_HISTORICAL_INPUT_PENDING,
    REASON_HISTORICAL_RUN_FAILED,
    REASON_PROGRAM_BINDING_INVALID,
    database_identity_hash,
)
from .production_projection import FixedReadOnlyProjection, readonly_onboarding_connection
from .store import (
    RealDevOnboardingEvidenceStore,
    _assert_contained,
    _assert_no_reparse_path,
    _publish_no_replace,
    _read_exact,
)


LOGGER = logging.getLogger(__name__)
Connector = Callable[..., Any]
ConnFactory = Callable[[], Iterator[Any]]
HISTORICAL_STORE_POLICY = "advisory_real_dev_historical_store_v1"
HISTORICAL_STORE_POLICY_HASH = canonical_json_sha256(
    {
        "policy": HISTORICAL_STORE_POLICY,
        "layout": {
            "request": "historical-requests/<prefix>/<hash>.json",
            "receipt": "historical-receipts/<prefix>/<hash>.json",
        },
        "atomic_no_replace": True,
        "latest_pointer": False,
    }
)
HISTORICAL_DECISION_TIMEZONE = ZoneInfo("Asia/Shanghai")
HISTORICAL_TARGET_ENTRY_CUTOFF = time(9, 25)


def target_package_asset_root_hash(path: Path) -> str:
    resolved = path.expanduser().resolve(strict=True)
    if not resolved.is_dir():
        raise ValueError("target package asset root must be an existing directory")
    return canonical_json_sha256({"resolved_root": os.path.normcase(str(resolved))})


def repository_code_release(repository_root: Path) -> tuple[str, str]:
    resolved = repository_root.expanduser().resolve(strict=True)
    completed = subprocess.run(
        ["git", "-C", str(resolved), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        check=False,
    )
    commit = completed.stdout.strip().lower()
    if completed.returncode != 0 or len(commit) != 40 or any(char not in "0123456789abcdef" for char in commit):
        raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "unable to resolve exact repository code release")
    status = subprocess.run(
        ["git", "-C", str(resolved), "status", "--porcelain=v1", "--untracked-files=all"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        check=False,
    )
    if status.returncode != 0:
        raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "unable to verify repository worktree state")
    if status.stdout.strip():
        raise RealDevOnboardingError(
            REASON_PROGRAM_BINDING_INVALID,
            "historical onboarding requires a clean repository worktree for exact code release evidence",
        )
    return commit, canonical_json_sha256({"git_commit": commit})


@dataclass(frozen=True)
class HistoricalStoredArtifact:
    kind: str
    store_policy_hash: str
    semantic_hash: str
    file_sha256: str
    relative_path: str
    idempotent: bool


class HistoricalOnboardingEvidenceStore:
    """Small O3-only CAS that does not change the already-published O1/O2 store policy."""

    def __init__(self, *, root: Path) -> None:
        self._root = RealDevOnboardingEvidenceStore(root=root).root

    def publish(self, model: RealDevHistoricalRunRequest | RealDevHistoricalRunReceipt) -> HistoricalStoredArtifact:
        if isinstance(model, RealDevHistoricalRunRequest):
            kind = "request"
            folder = "historical-requests"
            identity = str(model.historical_request_hash)
        elif isinstance(model, RealDevHistoricalRunReceipt):
            kind = "receipt"
            folder = "historical-receipts"
            identity = str(model.receipt_hash)
        else:  # pragma: no cover - type boundary.
            raise TypeError("unsupported historical onboarding artifact")
        payload = (canonical_json_text(model.model_dump(mode="json")) + "\n").encode("utf-8")
        relative = Path(folder) / identity[:2] / f"{identity}.json"
        destination = (self._root / relative).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        _assert_contained(path=destination, root=self._root)
        _assert_no_reparse_path(path=destination.parent, root=self._root)
        if destination.exists():
            existing = _read_exact(path=destination, root=self._root)
            if existing != payload:
                raise RealDevOnboardingError(REASON_DSE_INVALID, "historical evidence identity collision")
            return HistoricalStoredArtifact(
                kind,
                HISTORICAL_STORE_POLICY_HASH,
                identity,
                hashlib.sha256(existing).hexdigest(),
                relative.as_posix(),
                True,
            )
        descriptor, name = tempfile.mkstemp(prefix=f".{identity}.", suffix=".tmp", dir=destination.parent)
        temp = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            if not _publish_no_replace(source=temp, target=destination):
                existing = _read_exact(path=destination, root=self._root)
                if existing != payload:
                    raise RealDevOnboardingError(REASON_DSE_INVALID, "historical evidence identity collision")
                return HistoricalStoredArtifact(
                    kind,
                    HISTORICAL_STORE_POLICY_HASH,
                    identity,
                    hashlib.sha256(existing).hexdigest(),
                    relative.as_posix(),
                    True,
                )
        finally:
            temp.unlink(missing_ok=True)
        readback = _read_exact(path=destination, root=self._root)
        if readback != payload:
            raise RealDevOnboardingError(REASON_DSE_INVALID, "historical evidence readback differs")
        return HistoricalStoredArtifact(
            kind,
            HISTORICAL_STORE_POLICY_HASH,
            identity,
            hashlib.sha256(readback).hexdigest(),
            relative.as_posix(),
            False,
        )


class ExactDevConnectionFactory:
    def __init__(
        self,
        config: DatabaseConnectionConfig,
        *,
        expected_database_identity_hash: str,
        connector: Connector = psycopg2.connect,
    ) -> None:
        self.config = config
        self.expected_database_identity_hash = expected_database_identity_hash
        self.connector = connector

    @contextmanager
    def __call__(self) -> Iterator[Any]:
        connection = self.connector(**self.config.connect_kwargs())
        try:
            connection.set_session(readonly=False, autocommit=False, isolation_level="READ COMMITTED")
            actual_identity = _database_identity(connection=connection, config=self.config)
            if database_identity_hash(actual_identity) != self.expected_database_identity_hash:
                raise RealDevOnboardingError(
                    REASON_ENV_INVALID,
                    "writable DEV connection identity differs from the historical request target",
                )
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def _database_identity(*, connection: Any, config: DatabaseConnectionConfig) -> DatabaseIdentity:
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT current_database() AS current_database,
                   host(inet_server_addr()) AS server_address,
                   inet_server_port() AS server_port,
                   current_setting('server_version_num')::integer AS server_version_num,
                   current_user AS current_user
            """
        )
        row = cursor.fetchone()
    if row is None:
        raise RealDevOnboardingError(REASON_ENV_INVALID, "writable DEV connection identity query returned no row")
    return DatabaseIdentity(
        target_label=config.target_label,
        current_database=str(row["current_database"]),
        server_address=str(row["server_address"]) if row["server_address"] is not None else None,
        server_port=int(row["server_port"]),
        server_version_num=int(row["server_version_num"]),
        current_user_hash=hashlib.sha256(str(row["current_user"]).encode("utf-8")).hexdigest(),
        environment_contract_hash=config.environment_contract_hash,
    )


class ExactDevWslInferenceProvider(WslStrategyPackageInferenceProvider):
    """Inject DEV DB values into the WSL child without mutating process-global env."""

    def __init__(self, *, database: DatabaseConnectionConfig, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._database = database

    def _build_env_exports(self) -> str:
        values = {
            "TDX_DB_HOST": self._database.host,
            "TDX_DB_PORT": str(self._database.port),
            "TDX_DB_NAME": self._database.database,
            "TDX_DB_USER": self._database.user,
            "TDX_DB_PASSWORD": self._database.password,
        }
        exports = ["PYTHONIOENCODING=utf-8", "PYTHONDONTWRITEBYTECODE=1", "AISTOCK_STRICT_INFERENCE=1"]
        timeout = os.getenv("AISTOCK_PG_STATEMENT_TIMEOUT_MS")
        if timeout:
            values["AISTOCK_PG_STATEMENT_TIMEOUT_MS"] = timeout
        exports.extend(f"{key}={self._quote(value)}" for key, value in values.items())
        return " ".join(exports)


class ExactDevHMMSnapshotProvider:
    def __init__(self, conn_factory: ConnFactory) -> None:
        self._conn_factory = conn_factory

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        rows = self._query("WHERE s.snapshot_id = %s", (snapshot_id,))
        return rows[0] if rows else None

    def list_snapshots(self, config_id: str) -> list[dict[str, Any]]:
        return self._query("WHERE s.config_id = %s ORDER BY s.trained_at DESC, s.snapshot_id DESC", (config_id,))

    def _query(self, suffix: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    f"""
                    SELECT s.snapshot_id, s.config_id, s.trained_at, s.model_path,
                           s.sector_count, s.status, s.metrics_json,
                           c.display_name AS config_display_name
                    FROM model_train_snapshots AS s
                    JOIN model_train_configs AS c ON c.config_id = s.config_id
                    {suffix}
                    """,
                    params,
                )
                return [dict(row) for row in cursor.fetchall()]


class ExactDevSymbolNameResolver:
    """Display-only name lookup on the exact DEV connection."""

    def __init__(self, conn_factory: ConnFactory) -> None:
        self._conn_factory = conn_factory

    def resolve(self, symbols: Any) -> dict[str, str]:
        normalized = sorted({str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()})
        if not normalized:
            return {}
        resolved = self._query("market.stock_basic", normalized)
        missing = [symbol for symbol in normalized if symbol not in resolved]
        if missing:
            resolved.update(self._query("market.symbol_dim", missing))
        return resolved

    def _query(self, relation: str, symbols: list[str]) -> dict[str, str]:
        if relation not in {"market.stock_basic", "market.symbol_dim"}:
            raise ValueError("unsupported symbol-name relation")
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT ts_code, name FROM {relation} WHERE ts_code = ANY(%s)", (symbols,))
                    rows = cursor.fetchall()
        except Exception as exc:
            LOGGER.warning(
                "advisory_historical_symbol_name_lookup_failed source=%s error_type=%s",
                relation,
                type(exc).__name__,
            )
            return {}
        return {
            str(symbol).strip(): str(name).strip()
            for symbol, name in rows
            if str(symbol or "").strip() and str(name or "").strip()
        }


class ExactDevStPitRiskDecisionProvider:
    source_name = "market.stock_universe_pit_spans"

    def __init__(self, conn_factory: ConnFactory) -> None:
        self._conn_factory = conn_factory

    def evaluate(self, *, symbols: list[str], trade_date: date, profile: Any, current_positions: dict[str, Any] | None = None) -> dict[str, RiskDecision]:
        normalized = sorted({str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()})
        if not normalized:
            return {}
        universe_key = require_live_st_pit_universe_key(profile.st_universe_key)
        if profile.strict_data_ready:
            self._require_ready(universe_key=universe_key, trade_date=trade_date)
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT ts_code, eligible_start, eligible_end, entry_reason,
                               exit_reason, rule_version, metadata
                        FROM market.stock_universe_pit_spans
                        WHERE universe_key = %s
                          AND ts_code = ANY(%s)
                          AND eligible_start <= %s
                          AND eligible_end >= %s
                        """,
                        (universe_key, normalized, trade_date, trade_date),
                    )
                    rows = cursor.fetchall()
        except Exception as exc:
            raise DataUnavailableError(
                "ST PIT risk policy lookup failed",
                context={
                    "trade_date": trade_date.isoformat(),
                    "symbol_count": len(normalized),
                    "universe_key": universe_key,
                },
            ) from exc
        eligible = {str(row[0]) for row in rows}
        row_by_symbol = {str(row[0]): row for row in rows}
        holdings = set(current_positions or {})
        hard_actions = set(profile.hard_actions)
        decisions: dict[str, RiskDecision] = {}
        for symbol in normalized:
            if symbol in eligible:
                row = row_by_symbol[symbol]
                decisions[symbol] = RiskDecision(
                    symbol=symbol,
                    source_events=[
                        {
                            "source_table": self.source_name,
                            "universe_key": universe_key,
                            "visible_trade_date": trade_date.isoformat(),
                            "eligible_start": row[1].isoformat() if row[1] else None,
                            "eligible_end": row[2].isoformat() if row[2] else None,
                            "entry_reason": row[3],
                            "exit_reason": row[4],
                            "rule_version": row[5],
                            "metadata": row[6] or {},
                        }
                    ],
                )
                continue
            force_exit = symbol in holdings and "force_exit" in hard_actions
            decisions[symbol] = RiskDecision(
                symbol=symbol,
                can_buy="block_buy" not in hard_actions,
                force_exit=force_exit,
                sell_only=force_exit,
                position_target_override=0 if force_exit else None,
                reason_codes=["st_pit_not_eligible"],
                source_events=[
                    {
                        "source_table": self.source_name,
                        "universe_key": universe_key,
                        "visible_trade_date": trade_date.isoformat(),
                        "event_type": "st_pit_not_eligible",
                        "risk_level": "P0_BLOCK",
                        "action": sorted(hard_actions),
                        "rule_version": profile.policy_version,
                    }
                ],
            )
        return decisions

    def _require_ready(self, *, universe_key: str, trade_date: date) -> None:
        try:
            with self._conn_factory() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT status, dirty, start_date, end_date, last_error
                        FROM market.stock_universe_pit_state
                        WHERE universe_key = %s
                        """,
                        (universe_key,),
                    )
                    row = cursor.fetchone()
        except Exception as exc:
            raise DataUnavailableError(
                "ST PIT risk policy readiness check failed",
                context={"trade_date": trade_date.isoformat(), "universe_key": universe_key},
            ) from exc
        if not row:
            raise DataUnavailableError(
                "ST PIT risk policy universe state is missing",
                context={"trade_date": trade_date.isoformat(), "universe_key": universe_key},
            )
        status, dirty, start_date, end_date, last_error = row
        if str(status or "").lower() != "ready" or bool(dirty) or start_date is None or end_date is None:
            raise DataUnavailableError(
                "ST PIT risk policy universe is not ready",
                context={
                    "trade_date": trade_date.isoformat(),
                    "universe_key": universe_key,
                    "status": status,
                    "dirty": bool(dirty),
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                    "last_error": last_error,
                },
            )
        if start_date > trade_date or end_date < trade_date:
            raise DataUnavailableError(
                "ST PIT risk policy universe does not cover trade_date",
                context={
                    "trade_date": trade_date.isoformat(),
                    "universe_key": universe_key,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )


class HistoricalResearchExecutionProhibitedPortfolioService:
    """Typed boundary that prevents historical onboarding from entering Paper portfolio flows."""

    @staticmethod
    def create_portfolio(**_kwargs: Any) -> Any:
        raise RuntimeConfigInvalidError(
            "historical advisory onboarding cannot create a Paper portfolio",
            context={"reason_code": REASON_FORBIDDEN_EXECUTION_DEPENDENCY},
        )


@dataclass(frozen=True)
class HistoricalOnboardingComponents:
    conn_factory: ExactDevConnectionFactory
    program_repository: AdvisoryProgramPGRepository
    program_service: AdvisoryProgramService
    selection_center: SelectionCenterService
    selection_service: StrategyPackageSelectionService
    artifact_service: StrategyPackageSelectionArtifactService
    artifact_repository: StrategyPackageSelectionArtifactRepository
    historical_runner: HistoricalAdvisoryResearchRunner
    trading_date_resolver: PostgresHistoricalResearchTradingDateResolver
    program_resolver: PostgresHistoricalResearchProgramResolver
    evidence_adapter: PersistedHistoricalSelectionEvidenceAdapter
    calendar_service: TradingCalendarStatusService


class RealDevHistoricalOnboardingService:
    def __init__(self, *, connector: Connector = psycopg2.connect, now_provider: Callable[[], datetime] | None = None) -> None:
        self._connector = connector
        self._now_provider = now_provider or (lambda: datetime.now(UTC))

    def run(
        self,
        *,
        request: RealDevHistoricalRunRequest,
        env_file: Path,
        evidence_root: Path,
        target_package_asset_root: Path,
        repository_root: Path,
    ) -> tuple[RealDevHistoricalRunReceipt, HistoricalStoredArtifact]:
        started_at = self._now_provider()
        if started_at.tzinfo is None:
            raise RuntimeConfigInvalidError("historical onboarding clock must be timezone-aware")
        base_store = RealDevOnboardingEvidenceStore(root=evidence_root)
        historical_store = HistoricalOnboardingEvidenceStore(root=evidence_root)
        stored_request = historical_store.publish(request)
        onboarding = base_store.load(request.onboarding_request_ref)
        if not isinstance(onboarding, RealDevOnboardingRequest) or onboarding.request_hash != request.onboarding_request_hash:
            raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "historical request references the wrong onboarding request")
        self._validate_request_parity(request=request, onboarding=onboarding)
        try:
            config = resolve_database_connection(target_label=TargetLabel.DEV, env_file=env_file)
        except ReleaseSchemaVerificationError as exc:
            raise RealDevOnboardingError(REASON_ENV_INVALID, "unable to resolve exact DEV connection") from exc
        with readonly_onboarding_connection(config, connector=self._connector) as connection:
            identity = FixedReadOnlyProjection(connection, config).identity()
        actual_identity_hash = database_identity_hash(identity)
        if actual_identity_hash != request.target_database_identity_hash:
            raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "historical request DEV identity differs from exact target")
        actual_root_hash = target_package_asset_root_hash(target_package_asset_root)
        if actual_root_hash != request.target_package_asset_root_hash:
            raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "historical request package asset root differs from exact target")
        release_id, release_hash = repository_code_release(repository_root)
        if request.code_release_id != release_id or request.code_release_hash != release_hash:
            raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "historical request code release differs from the executing repository")
        components = self._build_components(
            config=config,
            expected_database_identity_hash=actual_identity_hash,
            target_package_asset_root=target_package_asset_root,
            repository_root=repository_root,
        )
        components.calendar_service.ensure_trading_day(request.binding_effective_from_trade_date)
        provisioning: dict[str, dict[str, Any]] = {}
        for spec in request.program_specs:
            try:
                program, binding = self._ensure_program(
                    spec=spec,
                    effective_from=request.binding_effective_from_trade_date,
                    components=components,
                )
                provisioning[spec.program_id] = {
                    "program": program,
                    "binding": binding,
                    "reason_codes": (),
                }
            except (TradingCoreError, RealDevOnboardingError) as exc:
                reason = self._reason_code(exc, fallback=REASON_HISTORICAL_RUN_FAILED)
                LOGGER.error(
                    "advisory_historical_program_failed program_id=%s package_id=%s reason_code=%s error_type=%s",
                    spec.program_id,
                    spec.package_id,
                    reason,
                    type(exc).__name__,
                )
                provisioning[spec.program_id] = {"reason_codes": (reason,), "failed": True}
            except Exception:  # pragma: no cover - environment boundary.
                LOGGER.exception(
                    "advisory_historical_program_unexpected program_id=%s package_id=%s",
                    spec.program_id,
                    spec.package_id,
                )
                provisioning[spec.program_id] = {"reason_codes": (REASON_HISTORICAL_RUN_FAILED,), "failed": True}

        try:
            components.trading_date_resolver.require_completed_historical_trading_date(
                decision_trade_date=request.decision_trade_date,
                requested_at=started_at,
            )
        except TradingCoreError as exc:
            if self._reason_code(exc, fallback="") == REASON_HISTORICAL_DATE_REQUIRED:
                raise RealDevOnboardingError(
                    REASON_HISTORICAL_INPUT_PENDING,
                    "historical Programs are provisioned; decision_trade_date is not completed yet",
                    context={
                        "decision_trade_date": request.decision_trade_date.isoformat(),
                        "provisioned_program_ids": sorted(
                            program_id for program_id, state in provisioning.items() if state.get("program") is not None
                        ),
                    },
                ) from exc
            raise

        for spec in request.program_specs:
            state = provisioning[spec.program_id]
            if state.get("program") is None or state.get("binding") is None:
                continue
            try:
                evidence, selection_run_id = self._ensure_prospective_evidence(
                    request=request,
                    spec=spec,
                    program=state["program"],
                    binding=state["binding"],
                    components=components,
                )
                state["evidence"] = evidence
                state["selection_run_id"] = selection_run_id
            except HistoricalResearchInputUnavailable as exc:
                LOGGER.info(
                    "advisory_historical_program_waiting program_id=%s package_id=%s reason_code=%s",
                    spec.program_id,
                    spec.package_id,
                    exc.reason_code,
                )
                state["reason_codes"] = (exc.reason_code,)
                state["waiting"] = True
            except (TradingCoreError, RealDevOnboardingError) as exc:
                reason = self._reason_code(exc, fallback=REASON_HISTORICAL_RUN_FAILED)
                LOGGER.error(
                    "advisory_historical_program_failed program_id=%s package_id=%s reason_code=%s error_type=%s",
                    spec.program_id,
                    spec.package_id,
                    reason,
                    type(exc).__name__,
                )
                state["reason_codes"] = (reason,)
                state["failed"] = True
            except Exception:  # pragma: no cover - environment boundary.
                LOGGER.exception(
                    "advisory_historical_program_unexpected program_id=%s package_id=%s",
                    spec.program_id,
                    spec.package_id,
                )
                state["reason_codes"] = (REASON_HISTORICAL_RUN_FAILED,)
                state["failed"] = True

        batch_request = HistoricalResearchBatchRequest(
            decision_trade_date=request.decision_trade_date,
            program_ids=[item.program_id for item in request.program_specs],
        )
        formal_receipt = components.historical_runner.run(batch_request)
        run_by_program = {item.program_id: item for item in formal_receipt.program_runs}
        expected_program_ids = {item.program_id for item in request.program_specs}
        if set(run_by_program) != expected_program_ids or len(run_by_program) != len(formal_receipt.program_runs):
            raise RealDevOnboardingError(
                REASON_HISTORICAL_RUN_FAILED,
                "formal historical receipt Program identities differ from the exact request",
                context={
                    "expected_program_ids": sorted(expected_program_ids),
                    "actual_program_ids": sorted(run_by_program),
                },
            )
        results: list[HistoricalProgramResult] = []
        for spec in request.program_specs:
            run = run_by_program[spec.program_id]
            state = provisioning.get(spec.program_id, {})
            state_reasons = tuple(state.get("reason_codes") or ())
            run_reasons = tuple(run.reason_codes or ())
            if run.status is HistoricalResearchRunStatus.FAILED or state.get("failed"):
                status = HistoricalProgramStatus.FAILED
                reasons = tuple(sorted({*state_reasons, *run_reasons} or {REASON_HISTORICAL_RUN_FAILED}))
            elif run.status is HistoricalResearchRunStatus.COMPLETE:
                status = HistoricalProgramStatus.COMPLETE
                reasons = ()
            elif run.status is HistoricalResearchRunStatus.WAITING_INPUT or state.get("waiting"):
                status = HistoricalProgramStatus.WAITING_INPUT
                reasons = tuple(sorted({*state_reasons, *run_reasons} or {REASON_HISTORICAL_INPUT_PENDING}))
            else:
                status = HistoricalProgramStatus.FAILED
                reasons = tuple(sorted({*state_reasons, *run_reasons} or {REASON_HISTORICAL_RUN_FAILED}))
            results.append(
                HistoricalProgramResult(
                    program_id=spec.program_id,
                    package_id=spec.package_id,
                    alpha_mode=spec.alpha_mode,
                    status=status,
                    program_payload_sha256=run.program_payload_sha256,
                    binding_version_id=run.binding_version_id,
                    binding_payload_hash=run.binding_payload_hash,
                    selection_run_id=state.get("selection_run_id"),
                    evidence_id=run.evidence_id,
                    evidence_hash=run.evidence_hash,
                    artifact_id=run.artifact_id,
                    artifact_payload_hash=run.artifact_payload_hash,
                    historical_program_run_id=run.program_run_id,
                    reason_codes=reasons,
                )
            )
        batch_status = self._aggregate_program_statuses(results)
        receipt = RealDevHistoricalRunReceipt(
            historical_request_hash=str(request.historical_request_hash),
            target_database_identity_hash=actual_identity_hash,
            target_package_asset_root_hash=actual_root_hash,
            batch_id=formal_receipt.batch_id,
            batch_key=formal_receipt.batch_key,
            batch_status=batch_status,
            formal_batch_receipt_hash=formal_receipt.receipt_hash,
            program_results=tuple(results),
            started_at=started_at,
            finished_at=self._now_provider(),
        )
        stored_receipt = historical_store.publish(receipt)
        LOGGER.info(
            "advisory_historical_onboarding_complete request_hash=%s receipt_hash=%s batch_status=%s request_idempotent=%s receipt_idempotent=%s",
            request.historical_request_hash,
            receipt.receipt_hash,
            receipt.batch_status,
            stored_request.idempotent,
            stored_receipt.idempotent,
        )
        return receipt, stored_receipt

    @staticmethod
    def _aggregate_program_statuses(results: list[HistoricalProgramResult]) -> str:
        statuses = {item.status for item in results}
        if HistoricalProgramStatus.FAILED in statuses:
            return HistoricalProgramStatus.FAILED.value
        if HistoricalProgramStatus.WAITING_INPUT in statuses:
            return HistoricalProgramStatus.WAITING_INPUT.value
        return HistoricalProgramStatus.COMPLETE.value

    @staticmethod
    def _validate_request_parity(*, request: RealDevHistoricalRunRequest, onboarding: RealDevOnboardingRequest) -> None:
        if (
            request.binding_effective_from_trade_date != onboarding.binding_effective_from_trade_date
            or request.decision_trade_date != onboarding.decision_trade_date
            or request.policy_registry_id != onboarding.policy_registry_id
            or request.policy_registry_version != onboarding.policy_registry_version
            or request.policy_registry_hash != onboarding.policy_registry_hash
        ):
            raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "historical request differs from onboarding date/policy identity")
        expected = {
            item.program_id: (item.package_id, item.alpha_mode, item.style, item.target_count, item.review_policy)
            for item in onboarding.target_dev_program_specs
        }
        actual = {
            item.program_id: (item.package_id, item.alpha_mode, item.style, item.target_count, item.review_policy)
            for item in request.program_specs
        }
        if actual != expected:
            raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "historical Program specs differ from onboarding request")

    def _build_components(
        self,
        *,
        config: DatabaseConnectionConfig,
        expected_database_identity_hash: str,
        target_package_asset_root: Path,
        repository_root: Path,
    ) -> HistoricalOnboardingComponents:
        conn_factory = ExactDevConnectionFactory(
            config,
            expected_database_identity_hash=expected_database_identity_hash,
            connector=self._connector,
        )
        package_repository = StrategyPackageRepository(conn_factory=conn_factory)
        artifact_repository = StrategyPackageSelectionArtifactRepository(conn_factory=conn_factory)
        snapshot_provider = ExactDevHMMSnapshotProvider(conn_factory)
        hmm_runtime = SectorHMMRuntime(snapshot_provider=snapshot_provider)
        runtime = StrategyPackageRuntime(hmm_runtime=hmm_runtime, artifact_repository=artifact_repository)
        asset_resolver = QEExperimentRuntimeAssetResolver(
            conn_factory=conn_factory,
            asset_store=LocalPackageAssetStore(root=target_package_asset_root),
        )
        artifact_service = StrategyPackageSelectionArtifactService(
            package_repository=package_repository,
            artifact_repository=artifact_repository,
            runtime_asset_resolver=asset_resolver,
            live_inference_provider=ExactDevWslInferenceProvider(database=config, repo_root=repository_root),
            conn_factory=conn_factory,
        )
        tradability = TradabilityFilter(
            suspend_provider=DbSuspendLookupProvider(conn_factory),
            industry_provider=DbSwIndustryLookupProvider(conn_factory),
        )
        risk_policy = StockRiskPolicyService(
            providers={
                "st_pit": ExactDevStPitRiskDecisionProvider(conn_factory),
                "announcement_risk": AnnouncementRiskDecisionProvider(),
            }
        )
        calendar_service = TradingCalendarStatusService(conn_factory=conn_factory)
        health = SelectionPackageHealthService(
            artifact_repository=artifact_repository,
            runtime_source_resolver=asset_resolver,
            hmm_runtime=hmm_runtime,
        )
        selection_repository = SimulationRuntimeRepository(conn_factory=conn_factory)
        signal_service = DailySelectionSignalService(runtime=runtime, selection_artifact_service=artifact_service)
        strategy_selection = StrategyPackageSelectionService(
            package_repository=package_repository,
            runtime=runtime,
            tradability_filter=tradability,
            refresh_audit=DataRefreshAuditRepository(conn_factory=conn_factory),
            selection_artifact_service=artifact_service,
            calendar_provider=calendar_service,
            risk_policy_service=risk_policy,
            package_health_service=health,
            repository=selection_repository,
            signal_service=signal_service,
            phase1_trace_capture_service=Phase1TraceCaptureService(),
        )
        selection_center = SelectionCenterService(
            package_repository=package_repository,
            repository=SelectionCenterRepository(conn_factory=conn_factory),
            runtime=runtime,
            tradability_filter=tradability,
            refresh_audit=DataRefreshAuditRepository(conn_factory=conn_factory),
            paper_portfolio_service=HistoricalResearchExecutionProhibitedPortfolioService(),
            selection_artifact_service=artifact_service,
            calendar_provider=calendar_service,
            risk_policy_service=risk_policy,
            package_health_service=health,
            strategy_selection_service=strategy_selection,
            result_enrichment_service=SelectionResultEnrichmentService(
                conn_factory=conn_factory,
                symbol_name_resolver=ExactDevSymbolNameResolver(conn_factory),
                quote_fetcher=lambda _symbol: None,
                today_provider=lambda: date.max,
            ),
        )
        program_repository = AdvisoryProgramPGRepository(conn_factory=conn_factory)
        program_service = AdvisoryProgramService(
            repository=program_repository,
            selection_service=selection_center,
            calendar_provider=calendar_service,
            symbol_name_resolver=ExactDevSymbolNameResolver(conn_factory),
            now_provider=self._now_provider,
        )
        historical_repository = PostgresHistoricalResearchRepository(conn_factory=conn_factory)
        program_resolver = PostgresHistoricalResearchProgramResolver(conn_factory=conn_factory)
        evidence_adapter = PersistedHistoricalSelectionEvidenceAdapter(conn_factory=conn_factory)
        trading_date_resolver = PostgresHistoricalResearchTradingDateResolver(conn_factory=conn_factory)
        historical_runner = HistoricalAdvisoryResearchRunner(
            repository=historical_repository,
            trading_date_resolver=trading_date_resolver,
            program_resolver=program_resolver,
            evidence_adapter=evidence_adapter,
            now_provider=self._now_provider,
        )
        return HistoricalOnboardingComponents(
            conn_factory=conn_factory,
            program_repository=program_repository,
            program_service=program_service,
            selection_center=selection_center,
            selection_service=strategy_selection,
            artifact_service=artifact_service,
            artifact_repository=artifact_repository,
            historical_runner=historical_runner,
            trading_date_resolver=trading_date_resolver,
            program_resolver=program_resolver,
            evidence_adapter=evidence_adapter,
            calendar_service=calendar_service,
        )

    def _ensure_program(
        self,
        *,
        spec: HistoricalProgramSpec,
        effective_from: date,
        components: HistoricalOnboardingComponents,
    ) -> tuple[AdvisoryProgram, AdvisoryStrategyBindingVersion]:
        try:
            existing = components.program_repository.get_program(spec.program_id)
        except DataUnavailableError:
            existing = None
        if existing is not None:
            binding = components.program_repository.get_active_binding_version(spec.program_id)
            if binding is None:
                raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "existing historical Program has no active binding")
            self._assert_program_exact(spec=spec, program=existing, binding=binding, effective_from=effective_from, components=components)
            return existing, binding

        config = components.program_service._validated_config(
            program_name=spec.program_name,
            package_mode=PACKAGE_MODE_SINGLE,
            package_ids=[spec.package_id],
            target_count=spec.target_count,
            package_weights=None,
            review_policy=spec.review_policy,
            entry_price_basis=spec.entry_price_basis,
            exit_price_basis=spec.exit_price_basis,
            review_schedule=spec.review_schedule,
        )
        effective = components.program_service._resolve_successor_effective_date(
            program_id=spec.program_id,
            active_binding=None,
            requested_date=effective_from,
        )
        now = self._now_provider()
        program = AdvisoryProgram(
            program_id=spec.program_id,
            status=PROGRAM_STATUS_DRAFT,
            created_by=spec.created_by,
            created_at=now,
            updated_at=now,
            **config,
        )
        binding = _binding_from_program(
            program,
            activation_status=BINDING_STATUS_ACTIVE,
            activation_reason="real DEV historical onboarding",
            created_by=spec.created_by,
            effective_from_trade_date=effective,
            runtime_config_json=spec.runtime_config,
        )
        binding = replace(binding, created_at=now, activated_at=now)
        try:
            components.program_repository.create_program_with_binding(program, binding)
        except Exception:
            try:
                concurrent = components.program_repository.get_program(spec.program_id)
                concurrent_binding = components.program_repository.get_active_binding_version(spec.program_id)
            except Exception:
                raise
            if concurrent_binding is None:
                raise
            self._assert_program_exact(
                spec=spec,
                program=concurrent,
                binding=concurrent_binding,
                effective_from=effective_from,
                components=components,
            )
            return concurrent, concurrent_binding
        return program, binding

    @staticmethod
    def _assert_program_exact(
        *,
        spec: HistoricalProgramSpec,
        program: AdvisoryProgram,
        binding: AdvisoryStrategyBindingVersion,
        effective_from: date,
        components: HistoricalOnboardingComponents,
    ) -> None:
        normalized = components.program_service._validated_config(
            program_name=spec.program_name,
            package_mode=PACKAGE_MODE_SINGLE,
            package_ids=[spec.package_id],
            target_count=spec.target_count,
            package_weights=None,
            review_policy=spec.review_policy,
            entry_price_basis=spec.entry_price_basis,
            exit_price_basis=spec.exit_price_basis,
            review_schedule=spec.review_schedule,
        )
        actual = {
            "program_name": program.program_name,
            "package_mode": program.package_mode,
            "package_ids": program.package_ids,
            "target_count": program.target_count,
            "package_weights": program.package_weights,
            "review_policy": program.review_policy,
            "entry_price_basis": program.entry_price_basis,
            "exit_price_basis": program.exit_price_basis,
            "review_schedule": program.review_schedule,
        }
        expected = {key: normalized[key] for key in actual}
        if actual != expected or program.status == PROGRAM_STATUS_ARCHIVED:
            raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "existing historical Program payload conflicts with request")
        if (
            binding.program_id != program.program_id
            or binding.package_mode != PACKAGE_MODE_SINGLE
            or binding.package_ids != [spec.package_id]
            or binding.effective_from_trade_date != effective_from
            or binding.effective_to_trade_date is not None
            or binding.activation_status != BINDING_STATUS_ACTIVE
            or binding.runtime_config_json != spec.runtime_config
        ):
            raise RealDevOnboardingError(REASON_PROGRAM_BINDING_INVALID, "existing historical binding payload conflicts with request")

    def _ensure_prospective_evidence(
        self,
        *,
        request: RealDevHistoricalRunRequest,
        spec: HistoricalProgramSpec,
        program: AdvisoryProgram,
        binding: AdvisoryStrategyBindingVersion,
        components: HistoricalOnboardingComponents,
    ) -> tuple[Any, str | None]:
        context = components.program_resolver.resolve(program_id=spec.program_id, decision_trade_date=request.decision_trade_date)
        try:
            existing = components.evidence_adapter.load(context=context, decision_trade_date=request.decision_trade_date)
            self._assert_evidence_code_release(
                evidence_id=existing.evidence_id,
                request=request,
                conn_factory=components.conn_factory,
            )
            return existing, None
        except HistoricalResearchInputUnavailable:
            pass
        target_trade_date = components.calendar_service.next_trading_day(request.decision_trade_date, inclusive=False)
        raw_config = components.program_service._review_runtime_config(program, binding.runtime_config_json)
        raw_config = components.program_service._with_advisory_date_context(
            raw_config,
            target_trade_date=target_trade_date,
            selection_as_of_trade_date=request.decision_trade_date,
        )
        package_config = self._prepare_package_config(
            package_id=spec.package_id,
            target_trade_date=target_trade_date,
            raw_config=raw_config,
            selection_service=components.selection_service,
        )
        artifact = components.artifact_service.generate_from_live_inference(
            package_id=spec.package_id,
            trade_date=target_trade_date,
            data_source=HISTORICAL_RESEARCH_DATA_SOURCE,
            runtime_config=package_config,
            cutoff_date=request.decision_trade_date,
        )
        preflight = self._preflight_stages(
            package_id=spec.package_id,
            target_trade_date=target_trade_date,
            package_config=package_config,
            selection_service=components.selection_service,
        )
        prospective = self._prospective_context(
            request=request,
            binding=binding,
            package_config=package_config,
            artifact=artifact,
            preflight=preflight,
            target_trade_date=target_trade_date,
            components=components,
        )
        self._validate_prospective_assembly(
            context=prospective,
            package_id=spec.package_id,
            artifact=artifact,
            preflight=preflight,
            selection_service=components.selection_service,
        )
        run = components.selection_center.run_single_package(
            package_id=spec.package_id,
            trade_date=target_trade_date,
            data_source=HISTORICAL_RESEARCH_DATA_SOURCE,
            runtime_config=raw_config,
            prospective_context=prospective,
        )
        refs = dict(run.runtime_config.get("daily_selection_evidence") or {})
        if refs.get("evidence_capture_status") != EvidenceCaptureStatus.COMPLETE.value:
            raise RealDevOnboardingError(
                REASON_DSE_INVALID,
                "prospective Selection did not produce complete DSE v2",
                context={"program_id": spec.program_id, "reason_codes": refs.get("evidence_reason_codes") or []},
            )
        schema = dict(refs.get("evidence_schema_version_by_package") or {}).get(spec.package_id)
        if schema != "daily_selection_evidence_v2":
            raise RealDevOnboardingError(REASON_DSE_INVALID, "prospective Selection produced a non-v2 DSE")
        evidence = components.evidence_adapter.load(context=context, decision_trade_date=request.decision_trade_date)
        self._assert_evidence_code_release(
            evidence_id=evidence.evidence_id,
            request=request,
            conn_factory=components.conn_factory,
        )
        return evidence, run.run_id

    @staticmethod
    def _assert_evidence_code_release(
        *,
        evidence_id: str,
        request: RealDevHistoricalRunRequest,
        conn_factory: ConnFactory,
    ) -> None:
        with conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT evidence_payload_json #>> '{evidence_contract,producer_code_release_id}'
                               AS producer_code_release_id,
                           evidence_payload_json #>> '{evidence_contract,producer_code_release_hash}'
                               AS producer_code_release_hash,
                           evidence_payload_json #>> '{phase0a_effective_config_chain,code_release_id}'
                               AS config_code_release_id,
                           evidence_payload_json #>> '{phase0a_effective_config_chain,code_release_hash}'
                               AS config_code_release_hash
                    FROM selection.daily_selection_evidence
                    WHERE evidence_id = %s
                    """,
                    (evidence_id,),
                )
                row = cursor.fetchone()
        if row is None:
            raise RealDevOnboardingError(REASON_DSE_INVALID, "historical DSE disappeared during code release validation")
        actual = {
            str(row["producer_code_release_id"] or ""),
            str(row["config_code_release_id"] or ""),
        }
        actual_hashes = {
            str(row["producer_code_release_hash"] or ""),
            str(row["config_code_release_hash"] or ""),
        }
        if actual != {request.code_release_id} or actual_hashes != {request.code_release_hash}:
            raise RealDevOnboardingError(
                REASON_DSE_INVALID,
                "historical DSE code release differs from the exact run request",
                context={"evidence_id": evidence_id},
            )

    @staticmethod
    def _validate_prospective_assembly(
        *,
        context: ProspectiveSelectionContext,
        package_id: str,
        artifact: Any,
        preflight: dict[str, Any],
        selection_service: StrategyPackageSelectionService,
    ) -> None:
        signal = preflight["signal"]
        risk = preflight["risk"]
        tradability = preflight["tradability"]
        manifest = selection_service.package_repository.get(package_id).current_manifest()
        alpha_raw = build_stage_receipt(
            stage=CandidateStageName.ALPHA_RAW,
            status=StageReceiptStatus.COMPLETE,
            input_count=len(signal.alpha_raw_candidates),
            candidates=signal.alpha_raw_candidates,
            semantic_payload={
                "package_id": manifest.package_id,
                "manifest_sha256": signal.snapshot.manifest_sha256,
                "artifact_id": artifact.artifact_id,
                "artifact_sha256": artifact.artifact_sha256,
                "artifact_payload_sha256": artifact.artifact_payload_sha256,
                "artifact_contract_version": artifact.artifact_contract_version,
            },
        )
        trace = SelectionStageTrace(
            alpha_raw=alpha_raw,
            hmm_adjusted=signal.hmm_result.receipt,
            risk_policy_adjusted=risk.receipt,
            selection_effective=tradability.receipt,
            hmm_metadata=signal.hmm_result.hmm_metadata,
            risk_metadata=risk.risk_metadata,
            universe_metadata=tradability.universe_metadata,
        )
        selected = list(tradability.candidates)
        candidate_outcome = "CANDIDATES_PRESENT" if selected else "VALID_NO_CANDIDATE"
        preflight_run_id = f"o3_preflight_{str(context.binding_ref.get('binding_id') or '')}"
        ProspectiveSelectionEvidenceAssembler().assemble(
            context=context.model_copy(update={"selection_run_id": preflight_run_id}),
            manifest=manifest,
            selection_run_id=preflight_run_id,
            artifact=artifact,
            stage_trace=trace,
            runtime_config=preflight["package_config"],
            selected=selected,
            excluded=[*risk.exclusions, *tradability.exclusions],
            created_by="advisory_real_dev_onboarding_preflight",
            candidate_outcome=candidate_outcome,
        )

    @staticmethod
    def _prepare_package_config(
        *,
        package_id: str,
        target_trade_date: date,
        raw_config: dict[str, Any],
        selection_service: StrategyPackageSelectionService,
    ) -> dict[str, Any]:
        config = mark_non_trading_preview_runtime_config(raw_config, reason="historical advisory research evidence")
        config = normalize_selection_runtime_config(config)
        validate_runtime_profile_binding(
            config,
            context={"path": "advisory_real_dev_historical_onboarding.preflight"},
            require_trade_enabled=not is_non_trading_runtime_config(config),
        )
        config = selection_service._apply_point_in_time_selection_config(config, trade_date=target_trade_date)
        config = refresh_generated_runtime_profile_binding(config)
        _records, package_configs, _health = selection_service._prepare_package_runtime_configs(
            package_ids=[package_id],
            config=config,
            trade_date=target_trade_date,
            data_source=HISTORICAL_RESEARCH_DATA_SOURCE,
        )
        return package_configs[package_id]

    @staticmethod
    def _preflight_stages(
        *,
        package_id: str,
        target_trade_date: date,
        package_config: dict[str, Any],
        selection_service: StrategyPackageSelectionService,
    ) -> dict[str, Any]:
        record = selection_service.package_repository.get(package_id)
        manifest = record.current_manifest()
        decision_date = date.fromisoformat(str(package_config["selection_artifact_config"]["cutoff_date"]))
        signal = selection_service.signal_service.build_signal_snapshot_with_trace(
            record=record,
            trade_date=target_trade_date,
            data_source=HISTORICAL_RESEARCH_DATA_SOURCE,
            runtime_config=package_config,
            require_frozen_hmm_snapshot=True,
            hmm_effective_trade_date=target_trade_date,
        )
        profile = parse_selection_runtime_profile(package_config)
        top_k = selection_service._top_k_for_package(manifest, profile, profile, package_config)
        decisions = selection_service.risk_policy_service.evaluate(
            symbols=[item.symbol for item in signal.snapshot.candidates],
            trade_date=target_trade_date,
            profile=profile.risk_policy,
        )
        risk = selection_service.risk_policy_service.apply_to_candidates_with_receipt(
            candidates=signal.snapshot.candidates,
            decisions=decisions,
            trade_date=target_trade_date,
            top_k=top_k,
            package_id=manifest.package_id,
            manifest_sha256=signal.snapshot.manifest_sha256,
            profile=profile.risk_policy,
            allow_empty=True,
        )
        if not (profile.tradability.exclude_suspended or profile.industry_blacklist):
            tradability = selection_service.tradability_filter.select_top_k_with_receipt(
                candidates=risk.candidates,
                top_k=top_k,
                trade_date=target_trade_date,
                package_id=manifest.package_id,
                manifest_sha256=signal.snapshot.manifest_sha256,
            )
        else:
            tradability = selection_service.tradability_filter.filter_candidates_with_receipt(
                candidates=risk.candidates,
                trade_date=target_trade_date,
                top_k=top_k,
                package_id=manifest.package_id,
                manifest_sha256=signal.snapshot.manifest_sha256,
                enabled=profile.tradability.exclude_suspended,
                industry_blacklist=profile.industry_blacklist,
                allow_empty=True,
            )
        return {
            "signal": signal,
            "risk": risk,
            "tradability": tradability,
            "decision_date": decision_date,
            "package_config": package_config,
        }

    def _prospective_context(
        self,
        *,
        request: RealDevHistoricalRunRequest,
        binding: AdvisoryStrategyBindingVersion,
        package_config: dict[str, Any],
        artifact: Any,
        preflight: dict[str, Any],
        target_trade_date: date,
        components: HistoricalOnboardingComponents,
    ) -> ProspectiveSelectionContext:
        source_receipts = [SourceReadReceipt.model_validate(item) for item in (artifact.metadata or {}).get("source_read_receipts") or []]
        observed = [item.available_at or item.first_observed_at for item in source_receipts]
        if not observed or any(item is None for item in observed):
            raise RealDevOnboardingError(REASON_DSE_INVALID, "score artifact source receipts are incomplete")
        data_available_at = max(item for item in observed if item is not None)
        generated_at = self._now_provider()
        if generated_at.tzinfo is None:
            raise RuntimeConfigInvalidError("historical onboarding clock must be timezone-aware")
        latest_legal_cutoff = datetime.combine(
            target_trade_date,
            HISTORICAL_TARGET_ENTRY_CUTOFF,
            tzinfo=HISTORICAL_DECISION_TIMEZONE,
        )
        decision_cutoff = min(generated_at, latest_legal_cutoff)
        if data_available_at > decision_cutoff:
            raise HistoricalResearchInputUnavailable(
                "historical source receipts were first available after the frozen decision cutoff",
                reason_code=REASON_HISTORICAL_INPUT_PENDING,
                context={
                    "decision_trade_date": request.decision_trade_date.isoformat(),
                    "target_trade_date": target_trade_date.isoformat(),
                    "decision_cutoff_ts": decision_cutoff.isoformat(),
                    "data_available_at": data_available_at.isoformat(),
                },
            )
        calendar_payload = self._calendar_payload(
            decision_date=request.decision_trade_date,
            target_date=target_trade_date,
            conn_factory=components.conn_factory,
        )
        binding_payload = _binding_payload(binding)
        binding_hash = canonical_json_sha256(binding_payload)
        runtime_profile = dict(package_config.get("runtime_profile") or {})
        point_in_time = dict(package_config.get("point_in_time_context") or {})
        base = dict(binding.runtime_config_json)
        empty_override: dict[str, Any] = {}
        code_release_hash = request.code_release_hash
        effective_chain = {
            "binding_base_config": base,
            "binding_base_config_hash": canonical_evidence_json_sha256(base),
            "binding_base_source_id": binding.binding_version_id,
            "binding_base_source_version": str(binding.program_version),
            "binding_base_source_hash": binding_hash,
            "binding_base_available_at": binding.activated_at or binding.created_at,
            "binding_base_effective_from_trade_date": binding.effective_from_trade_date,
            "binding_base_effective_to_trade_date": binding.effective_to_trade_date,
            "request_override_config": empty_override,
            "request_override_hash": canonical_evidence_json_sha256(empty_override),
            "date_enforced_config": point_in_time,
            "date_enforced_version": "selection_point_in_time_context_v1",
            "date_enforced_hash": canonical_evidence_json_sha256(point_in_time),
            "selection_normalized_config": runtime_profile,
            "selection_normalized_config_hash": canonical_evidence_json_sha256(runtime_profile),
            "package_effective_config": package_config,
            "package_effective_config_hash": canonical_evidence_json_sha256(package_config),
            "runtime_variant_id": package_config.get("runtime_variant_id"),
            "runtime_profile_version_id": str((package_config.get("runtime_profile_binding") or {}).get("profile_version_id") or "generated_runtime_profile_v1"),
            "runtime_profile_hash": runtime_profile_config_sha256(package_config),
            "selection_adapter_version": "strategy_package_selection_service_v1",
            "query_template_version": "strategy_package_live_inference_v2",
            "provider_version": "exact_dev_wsl_inference_v1",
            "code_release_id": request.code_release_id,
            "code_release_hash": code_release_hash,
            "overridden_field_paths_by_layer": {"request_override": []},
            "final_effective_config_hash": canonical_evidence_json_sha256(package_config),
        }
        universe = self._universe_evidence(
            request=request,
            artifact=artifact,
            preflight=preflight,
            available_at=data_available_at,
            policy_available_at=binding.activated_at or binding.created_at,
        )
        decision_clock = DecisionClockEvidenceV2.model_validate(
            {
                "decision_as_of_trade_date": request.decision_trade_date,
                "selection_as_of_trade_date": request.decision_trade_date,
                "target_trade_date": target_trade_date,
                "effective_entry_trade_date": target_trade_date,
                "score_trade_date": request.decision_trade_date,
                "reference_price_trade_date": request.decision_trade_date,
                "requested_selection_as_of_trade_date": request.decision_trade_date,
                "requested_cutoff_date": request.decision_trade_date,
                "effective_cutoff_date": request.decision_trade_date,
                "decision_cutoff_ts": decision_cutoff,
                "data_available_at": data_available_at,
                "decision_generated_at": generated_at,
                "timezone": "Asia/Shanghai",
                "calendar_version": "market.trading_calendar.v1",
                "calendar_hash": canonical_evidence_json_sha256(calendar_payload),
                "calendar_source": "market.trading_calendar",
                "is_immediately_previous_trade_date": True,
                "immediate_after_data_refresh": generated_at == data_available_at,
            }
        )
        config_chain = EffectiveConfigChainV2.model_validate(effective_chain)
        universe_evidence = UniverseEvidenceV2.model_validate(universe)
        return ProspectiveSelectionContext(
            capture_mode=EvidenceCaptureMode.PROSPECTIVE,
            execution_origin=ProspectiveExecutionOrigin.ADVISORY_RUN,
            decision_clock_seed=decision_clock.model_dump(mode="python"),
            effective_config_seed=config_chain.model_dump(mode="python"),
            policy_registry_ref={
                "policy_registry_id": request.policy_registry_id,
                "policy_registry_version": request.policy_registry_version,
                "registry_hash": request.policy_registry_hash,
            },
            binding_ref={"binding_id": binding.binding_version_id, "binding_hash": binding_hash},
            source_watermark_seed={"universe_evidence": universe_evidence.model_dump(mode="python")},
            created_by="advisory_real_dev_onboarding",
        )

    @staticmethod
    def _calendar_payload(*, decision_date: date, target_date: date, conn_factory: ConnFactory) -> list[dict[str, Any]]:
        with conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT cal_date, is_trading
                    FROM market.trading_calendar
                    WHERE cal_date = ANY(%s)
                    ORDER BY cal_date
                    """,
                    ([decision_date, target_date],),
                )
                rows = [dict(row) for row in cursor.fetchall()]
        if len(rows) != 2 or not all(bool(row.get("is_trading")) for row in rows):
            raise DataUnavailableError(
                "historical onboarding requires exact decision and target trading calendar rows",
                context={"decision_trade_date": decision_date.isoformat(), "target_trade_date": target_date.isoformat()},
            )
        return rows

    @staticmethod
    def _universe_evidence(
        *,
        request: RealDevHistoricalRunRequest,
        artifact: Any,
        preflight: dict[str, Any],
        available_at: datetime,
        policy_available_at: datetime,
    ) -> dict[str, Any]:
        input_context = dict((artifact.metadata or {}).get("artifact_input_context") or {})
        package_hash = str(input_context.get("universe_input_hash") or "")
        if len(package_hash) != 64:
            raise RealDevOnboardingError(REASON_DSE_INVALID, "artifact universe input hash is missing")
        source_refs = list((artifact.metadata or {}).get("source_read_receipts") or [])
        source_hash = str(artifact.source_revision_set_hash or "")
        signal = preflight["signal"]
        risk = preflight["risk"]
        tradability = preflight["tradability"]

        def symbol_hash(rows: list[Any]) -> str:
            return canonical_evidence_json_sha256(sorted(str(item.symbol) for item in rows))

        raw_rows = list(signal.snapshot.candidates)
        risk_rows = list(risk.candidates)
        selected_rows = list(tradability.candidates)
        def layer(
            name: str,
            *,
            status: str,
            input_count: int,
            output_count: int,
            input_hash: str | None = None,
            output_hash: str | None = None,
            policy_id: str | None = None,
            policy_version: str | None = None,
            policy_hash: str | None = None,
            reason_counts: dict[str, int] | None = None,
            reasons: list[str] | None = None,
            revision_refs: list[dict[str, Any]] | None = None,
            revision_hash: str | None = None,
        ) -> dict[str, Any]:
            reason_counts = reason_counts or {}
            excluded = input_count - output_count
            if sum(reason_counts.values()) != excluded:
                raise RealDevOnboardingError(
                    REASON_DSE_INVALID,
                    "historical universe exclusion reasons do not reconcile",
                    context={"layer": name, "excluded_count": excluded, "reason_counts": reason_counts},
                )
            payload: dict[str, Any] = {
                "layer": name,
                "status": status,
                "input_count": input_count,
                "output_count": output_count,
                "excluded_count": excluded,
                "exclusion_reason_counts": reason_counts,
                "input_symbol_set_hash": input_hash,
                "output_symbol_set_hash": output_hash,
                "reason_codes": reasons or [],
            }
            if status != "NOT_APPLICABLE":
                refs = list(revision_refs or [])
                if not refs:
                    raise RealDevOnboardingError(
                        REASON_DSE_INVALID,
                        "executed historical universe layer has no authoritative source receipts",
                        context={"layer": name},
                    )
                payload.update(
                    {
                        "policy_id": policy_id,
                        "policy_version": policy_version,
                        "policy_hash": policy_hash,
                        "policy_available_at": policy_available_at,
                        "policy_effective_from_trade_date": request.binding_effective_from_trade_date,
                        "available_at": available_at,
                        "source_revision_refs": refs,
                        "source_revision_set_hash": revision_hash or canonical_evidence_json_sha256(refs),
                    }
                )
            return payload

        delegated_reason = ["ADVISORY_O3_UNIVERSE_LAYER_DELEGATED_TO_AUTHORITATIVE_RUNTIME"]
        layers = [
            layer(
                name,
                status="NOT_APPLICABLE",
                input_count=0,
                output_count=0,
                reasons=delegated_reason,
            )
            for name in ("listed_universe", "seasoned_universe", "pit_st_delist_risk_universe")
        ]
        layers.append(
            layer(
                "package_eligible_universe",
                status="RESEARCH_ONLY",
                input_count=int(artifact.universe_count),
                output_count=int(artifact.universe_count),
                input_hash=package_hash,
                output_hash=package_hash,
                policy_id=f"strategy_package:{artifact.package_id}",
                policy_version=str(artifact.artifact_contract_version),
                policy_hash=str(artifact.manifest_sha256),
                revision_refs=source_refs,
                revision_hash=source_hash,
            )
        )
        risk_reason_counts: dict[str, int] = {}
        for item in risk.exclusions:
            risk_reason_counts[item.reason] = risk_reason_counts.get(item.reason, 0) + 1
        layers.append(
            layer(
                "risk_can_buy_universe",
                status="FORMAL_READY",
                input_count=risk.receipt.input_count,
                output_count=risk.receipt.output_count,
                input_hash=symbol_hash(raw_rows),
                output_hash=symbol_hash(risk_rows),
                policy_id="selection:risk_policy_adjusted",
                policy_version="selection_stage_receipt_v1",
                policy_hash=risk.receipt.receipt_hash,
                reason_counts=risk_reason_counts,
                revision_refs=[{"stage": "risk_policy_adjusted", "stage_receipt_hash": risk.receipt.receipt_hash}],
            )
        )
        tradability_reason_counts: dict[str, int] = {}
        for item in tradability.exclusions:
            tradability_reason_counts[item.reason] = tradability_reason_counts.get(item.reason, 0) + 1
        layers.append(
            layer(
                "tradability_industry_universe",
                status="FORMAL_READY",
                input_count=tradability.receipt.input_count,
                output_count=tradability.receipt.output_count,
                input_hash=symbol_hash(risk_rows),
                output_hash=symbol_hash(selected_rows),
                policy_id="selection:selection_effective",
                policy_version="selection_stage_receipt_v1",
                policy_hash=tradability.receipt.receipt_hash,
                reason_counts=tradability_reason_counts,
                revision_refs=[
                    {"stage": "selection_effective", "stage_receipt_hash": tradability.receipt.receipt_hash}
                ],
            )
        )
        return {
            "layers": layers,
            "package_cohort": {
                "status": "RESEARCH_ONLY",
                "package_id": artifact.package_id,
                "manifest_sha256": artifact.manifest_sha256,
                "artifact_id": artifact.artifact_id,
                "artifact_payload_sha256": artifact.artifact_payload_sha256,
                "reason_codes": [],
            },
        }

    @staticmethod
    def _reason_code(exc: Exception, *, fallback: str) -> str:
        context = getattr(exc, "context", None)
        if isinstance(context, dict) and context.get("reason_code"):
            return str(context["reason_code"])
        return str(getattr(exc, "reason_code", None) or fallback)
