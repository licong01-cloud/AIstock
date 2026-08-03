"""Materialize the Advisory SHORT_REBOUND Batch B dataset and training files."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import UTC, date, datetime
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Protocol

import psycopg2
from dotenv import dotenv_values

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.composition import (
    _R5_CODE_RELEASE_CLOSURE,
    _R5_SELECTION_CLOSURE,
    _historical_range_code_set_hash,
    build_environment_historical_range_r5_application_service,
)
from backend.services.advisory_historical_range.code_release import (
    HistoricalRangeCodeReleaseResolver,
)
from backend.services.advisory_historical_range.api_models import (
    ExistingProgramInput,
    HistoricalRangeCommandRequest,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    ExistingProgramSpecV1,
    HistoricalRangeAlphaMode,
    HistoricalRangeContractError,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeResearchBatchRequestV1,
    REASON_IDEMPOTENCY_CONFLICT,
)
from backend.services.advisory_historical_range.request_resolver import (
    HistoricalRangeAdmittedPackageResolver,
    HistoricalRangeProgramResolver,
)
from backend.services.advisory_historical_range.runtime_factories import (
    _calendar,
    historical_range_store_identity,
)
from backend.services.advisory_historical_range.semantics import (
    canonical_list_semantics_v2,
)
from backend.services.advisory_modeling.base_snapshot import (
    HistoricalCandidateArtifactResolver,
    RerankerBaseSnapshotReader,
)
from backend.services.advisory_modeling.batch_b import (
    BatchBDatasetMaterializationRequestV1,
    BatchBHistoricalRangeDriver,
    BatchBMaterializationService,
)
from backend.services.advisory_modeling.errors import (
    AdvisoryModelingError,
    REASON_DATASET_SNAPSHOT_NOT_SEALED,
)
from backend.services.advisory_modeling.feature_builder import frozen_formula_registry_v1
from backend.services.advisory_modeling.feature_schema import frozen_feature_schema_v1
from backend.services.advisory_modeling.feature_sources import (
    PostgresFeatureSourceReader,
    frozen_feature_query_registry_v1,
)
from backend.services.advisory_modeling.label_policy import RankingLabelPolicyV1
from backend.services.advisory_modeling.market_regime import MarketRegimePolicyTemplateV1
from backend.services.advisory_modeling.style_profile import (
    SHORT_REBOUND_TARGET_PACKAGE_ID,
    StrategyStyleProfileV1,
)
from backend.services.advisory_modeling.training_view import DatasetBuildIntentV1
from backend.services.advisory_phase0b.snapshot_reader import (
    Phase0BClientDatabaseTargetV1,
    PostgresPhase0BSnapshotCatalog,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from backend.services.advisory_phase1.label_policy import TradingCalendar
from backend.services.advisory_program import (
    BINDING_STATUS_ACTIVE,
    AdvisoryProgram,
    AdvisoryProgramPGRepository,
    AdvisoryStrategyBindingVersion,
)
from backend.services.strategy_package.historical_selection_providers import (
    historical_read_only_connection_factory,
)
from backend.services.strategy_package.repository import StrategyPackageRepository


DB_KEYS = (
    "TDX_DB_HOST",
    "TDX_DB_PORT",
    "TDX_DB_NAME",
    "TDX_DB_USER",
    "TDX_DB_PASSWORD",
)
REQUIRED_RUNTIME_KEYS = (
    "AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT",
    "AISTOCK_ADVISORY_HISTORICAL_RANGE_TASK_RUNTIME_ROOT",
    "AISTOCK_PACKAGE_ASSET_STORE_ROOT",
    "AISTOCK_REPOSITORY_ROOT",
    "AISTOCK_ADVISORY_HISTORICAL_RANGE_POLICY_COMPONENT_ROOT",
    "AISTOCK_ADVISORY_CALCULATION_EVIDENCE_ROOT",
    "AISTOCK_ADVISORY_DATASET_STORE_ROOT",
)


class AdvisoryProgramIdentityReader(Protocol):
    def get_program(self, program_id: str) -> AdvisoryProgram: ...

    def list_binding_versions(self, program_id: str) -> list[AdvisoryStrategyBindingVersion]: ...


FrozenProgramProvider = Callable[
    [ExistingProgramSpecV1, date, date], HistoricalRangeFrozenProgramV1
]
PackageCreatedAtProvider = Callable[[str], datetime]
CalendarProvider = Callable[[date, date], TradingCalendar]


class RestartSafeHistoricalRangeService:
    """Recover an exact Batch B request without weakening planning identity checks."""

    def __init__(self, *, service: Any) -> None:
        if service is None:
            raise ValueError("restart-safe Historical Range service requires a delegate")
        self._service = service

    def with_candidate_prefetch_per_program(self, value: int) -> "RestartSafeHistoricalRangeService":
        return RestartSafeHistoricalRangeService(
            service=self._service.with_candidate_prefetch_per_program(value)
        )

    def create_batch(
        self,
        request: Any,
        *,
        idempotency_key: str,
        background_tasks: Any,
        requested_by: str = "local-user",
    ) -> dict[str, Any]:
        try:
            return self._service.create_batch(
                request,
                idempotency_key=idempotency_key,
                background_tasks=background_tasks,
                requested_by=requested_by,
            )
        except HistoricalRangeContractError as exc:
            if exc.reason_code != REASON_IDEMPOTENCY_CONFLICT:
                raise
            return self._recover_existing_batch(
                error=exc,
                request=request,
                idempotency_key=idempotency_key,
                requested_by=requested_by,
                background_tasks=background_tasks,
            )

    def _recover_existing_batch(
        self,
        *,
        error: HistoricalRangeContractError,
        request: Any,
        idempotency_key: str,
        requested_by: str,
        background_tasks: Any,
    ) -> dict[str, Any]:
        existing_batch_id = str(error.context.get("existing_batch_id") or "").strip()
        if not existing_batch_id:
            raise error
        if len(request.program_specs) != 1:
            raise error
        try:
            expected_spec = ExistingProgramSpecV1.model_validate(
                request.program_specs[0].model_dump(mode="json")
            )
            expected_request = HistoricalRangeResearchBatchRequestV1(
                client_idempotency_key=idempotency_key,
                program_specs=(expected_spec,),
                start_trade_date=request.start_trade_date,
                end_trade_date=request.end_trade_date,
                requested_by=requested_by,
            )
            batch = self._service.get_batch(existing_batch_id)
            payload = batch.get("request_payload_json")
            stored_request_payload = payload.get("request") if isinstance(payload, dict) else None
            stored_request = HistoricalRangeResearchBatchRequestV1.model_validate(
                stored_request_payload
            )
        except (KeyError, TypeError, ValueError):
            raise error from None

        expected = {
            "batch_id": existing_batch_id,
            "client_idempotency_key": idempotency_key,
            "user_request_semantic_hash": expected_request.user_request_semantic_hash,
            "start_trade_date": request.start_trade_date,
            "end_trade_date": request.end_trade_date,
            "request_id": stored_request.request_id,
            "requested_by": requested_by,
        }
        actual = {
            "batch_id": batch.get("batch_id"),
            "client_idempotency_key": batch.get("client_idempotency_key"),
            "user_request_semantic_hash": batch.get("user_request_semantic_hash"),
            "start_trade_date": batch.get("start_trade_date"),
            "end_trade_date": batch.get("end_trade_date"),
            "request_id": batch.get("request_id"),
            "requested_by": stored_request.requested_by,
        }
        if (
            actual != expected
            or stored_request.client_idempotency_key != idempotency_key
            or stored_request.user_request_semantic_hash
            != expected_request.user_request_semantic_hash
        ):
            raise error

        if (
            str(batch.get("status")) == "PLANNING"
            and str(batch.get("catalog_operation_status")) == "RUNNING"
            and not bool(batch.get("catalog_lease_expired"))
        ):
            raise AdvisoryModelingError(
                REASON_DATASET_SNAPSHOT_NOT_SEALED,
                "Existing Batch B source catalog is still owned by an active worker",
                context={
                    "batch_id": existing_batch_id,
                    "catalog_operation_id": batch.get("catalog_operation_id"),
                    "catalog_lease_expires_at": batch.get("catalog_lease_expires_at"),
                },
            )

        if str(batch.get("status")) == "PLANNING":
            return self._service.resume_batch(
                existing_batch_id,
                HistoricalRangeCommandRequest(
                    operation_idempotency_key=(
                        f"{idempotency_key}-catalog-recovery-{int(batch['row_version'])}"
                    ),
                    expected_row_version=int(batch["row_version"]),
                ),
                background_tasks=background_tasks,
            )
        return {
            "ok": True,
            "data": {
                "batch": batch,
                "exact_retry": True,
                "dispatch_state": "NOT_SCHEDULED",
            },
        }

    def __getattr__(self, name: str) -> Any:
        return getattr(self._service, name)


class BatchBRequestBuilder:
    """Resolve one exact existing Program into the frozen Batch B request."""

    def __init__(
        self,
        *,
        program_reader: AdvisoryProgramIdentityReader,
        frozen_program_provider: FrozenProgramProvider,
        package_created_at_provider: PackageCreatedAtProvider,
        calendar_provider: CalendarProvider,
        repository_commit: str,
    ) -> None:
        if any(
            dependency is None
            for dependency in (
                program_reader,
                frozen_program_provider,
                package_created_at_provider,
                calendar_provider,
            )
        ):
            raise ValueError("Batch B request builder requires explicit authority readers")
        normalized_commit = str(repository_commit or "").strip().lower()
        if len(normalized_commit) != 40 or any(
            char not in "0123456789abcdef" for char in normalized_commit
        ):
            raise ValueError("repository_commit must be one full Git SHA")
        self._program_reader = program_reader
        self._frozen_program_provider = frozen_program_provider
        self._package_created_at_provider = package_created_at_provider
        self._calendar_provider = calendar_provider
        self._repository_commit = normalized_commit

    def build(
        self,
        *,
        program_id: str,
        decision_date_start: date,
        decision_date_end: date,
        final_fit_as_of: datetime,
    ) -> BatchBDatasetMaterializationRequestV1:
        if decision_date_start > decision_date_end:
            raise ValueError("decision_date_start must not exceed decision_date_end")
        if final_fit_as_of.tzinfo is None or final_fit_as_of.utcoffset() is None:
            raise ValueError("final_fit_as_of must be timezone-aware")
        normalized_fit_as_of = final_fit_as_of.astimezone(UTC)
        if normalized_fit_as_of.date() < decision_date_end:
            raise ValueError("final_fit_as_of cannot precede decision_date_end")

        program = self._program_reader.get_program(program_id)
        if program.program_id != program_id:
            raise ValueError("Program identity reader returned a different program_id")
        bindings = [
            item
            for item in self._program_reader.list_binding_versions(program.program_id)
            if item.program_version == program.version
            and item.activation_status == BINDING_STATUS_ACTIVE
        ]
        if len(bindings) != 1:
            raise ValueError(
                "Batch B request preparation requires exactly one active binding for the current Program version"
            )
        binding = bindings[0]
        spec = ExistingProgramSpecV1(
            program_id=program.program_id,
            expected_program_version=program.version,
            expected_binding_version_id=binding.binding_version_id,
        )
        frozen = self._frozen_program_provider(spec, decision_date_start, decision_date_end)
        self._verify_target(frozen=frozen, program=program, binding=binding)

        calendar = self._calendar_provider(decision_date_start, decision_date_end)
        if decision_date_start not in calendar.trading_dates:
            raise ValueError("calendar authority does not contain decision_date_start")
        if decision_date_end not in calendar.trading_dates:
            raise ValueError("calendar authority does not contain decision_date_end")
        if normalized_fit_as_of.date() not in calendar.trading_dates:
            raise ValueError("calendar authority does not contain final_fit_as_of trade date")

        cutoff_dates = [
            _aware_utc_date(
                self._package_created_at_provider(frozen.package_id),
                field_name="package.created_at",
            )
        ]
        if program.enabled_since is not None:
            cutoff_dates.append(
                _aware_utc_date(program.enabled_since, field_name="program.enabled_since")
            )
        if binding.effective_from_trade_date is not None:
            cutoff_dates.append(binding.effective_from_trade_date)
        if binding.activated_at is not None:
            cutoff_dates.append(
                _aware_utc_date(binding.activated_at, field_name="binding.activated_at")
            )

        profile = StrategyStyleProfileV1(
            profile_id="short-rebound-target-package-v1",
            profile_version="1",
            package_id=frozen.package_id,
            package_manifest_sha256=frozen.manifest_sha256,
            package_asset_closure_hash=frozen.target_package_asset_root_hash,
            selection_runtime_semantics_hash=frozen.selection_semantics_hash,
            effective_package_oos_cutoff=max(cutoff_dates),
        )
        feature_schema = frozen_feature_schema_v1()
        formula_registry = frozen_formula_registry_v1()
        query_registry = frozen_feature_query_registry_v1(
            repository_commit=self._repository_commit
        )
        regime_policy = MarketRegimePolicyTemplateV1()
        label_policy = RankingLabelPolicyV1()
        components = tuple(
            item.model_dump(mode="json")
            for item in sorted(
                frozen.admitted_package_projection.components,
                key=lambda item: item.component_id,
            )
        )
        intent = DatasetBuildIntentV1(
            style_profile_id=profile.profile_id,
            style_profile_hash=str(profile.profile_payload_sha256),
            package_id=profile.package_id,
            package_manifest_sha256=profile.package_manifest_sha256,
            package_asset_closure_hash=profile.package_asset_closure_hash,
            selection_runtime_semantics_hash=profile.selection_runtime_semantics_hash,
            multi_alpha_parent_contract_version=(
                "advisory_historical_range_candidate_component_lineage_v1"
            ),
            multi_alpha_component_identity_set_hash=canonical_json_sha256(components),
            decision_date_start=decision_date_start,
            decision_date_end=decision_date_end,
            feature_schema_id=feature_schema.feature_schema_id,
            feature_schema_hash=str(feature_schema.feature_schema_hash),
            feature_formula_registry_hash=str(formula_registry.registry_hash),
            feature_query_registry_hash=str(query_registry.registry_hash),
            market_regime_policy_template_id=regime_policy.policy_template_id,
            market_regime_policy_template_hash=str(regime_policy.policy_template_hash),
            label_policy_id=label_policy.label_policy_id,
            label_policy_hash=str(label_policy.label_policy_hash),
            calendar_version=calendar.calendar_version,
            calendar_hash=str(calendar.calendar_hash),
            repository_commit=self._repository_commit,
            final_fit_as_of=normalized_fit_as_of,
        )
        return BatchBDatasetMaterializationRequestV1(
            dataset_intent=intent,
            style_profile=profile,
            existing_program=ExistingProgramInput.model_validate(spec.model_dump(mode="json")),
        )

    @staticmethod
    def _verify_target(
        *,
        frozen: HistoricalRangeFrozenProgramV1,
        program: AdvisoryProgram,
        binding: AdvisoryStrategyBindingVersion,
    ) -> None:
        if frozen.source_program_id != program.program_id:
            raise ValueError("frozen Program differs from requested Program")
        if frozen.source_program_version != program.version:
            raise ValueError("frozen Program version differs from current Program version")
        if frozen.source_binding_version_id != binding.binding_version_id:
            raise ValueError("frozen binding differs from the active binding")
        if frozen.package_id != SHORT_REBOUND_TARGET_PACKAGE_ID:
            raise ValueError("Program does not bind the approved SHORT_REBOUND target package")
        if frozen.alpha_mode is not HistoricalRangeAlphaMode.MULTI_ALPHA:
            raise ValueError("SHORT_REBOUND Batch B requires one native multi-alpha parent")


def publish_batch_b_request(
    *,
    request: BatchBDatasetMaterializationRequestV1,
    artifact_root: Path,
    repository_root: Path,
) -> Path:
    artifact = artifact_root.resolve(strict=True)
    repository = repository_root.resolve(strict=True)
    try:
        artifact.relative_to(repository)
    except ValueError:
        pass
    else:
        raise ValueError("Batch B artifact_root must be outside repository_root")
    request_root = (artifact / "requests").resolve()
    request_root.relative_to(artifact)
    request_root.mkdir(parents=True, exist_ok=True)
    destination = request_root / f"{request.request_hash}.json"
    payload = json.dumps(
        request.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8") + b"\n"
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=request_root,
            prefix=".batch-b-request-",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, destination)
        except FileExistsError:
            if destination.read_bytes() != payload:
                raise ValueError("existing Batch B request file differs from canonical request")
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return destination.resolve(strict=True)


def _aware_utc_date(value: datetime, *, field_name: str) -> date:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} authority must be timezone-aware")
    return value.astimezone(UTC).date()


def _existing_directory(path: Path, *, field_name: str) -> Path:
    if not path.expanduser().is_absolute():
        raise ValueError(f"{field_name} must be an explicit absolute path")
    resolved = path.expanduser().resolve(strict=True)
    if not resolved.is_dir():
        raise ValueError(f"{field_name} must be an existing directory")
    return resolved


def _load_environment(
    path: Path,
    *,
    require_runtime: bool = True,
) -> tuple[dict[str, str], Path]:
    env_path = path.expanduser().resolve(strict=True)
    if not env_path.is_file():
        raise ValueError("env_file must be an existing file")
    values = {
        str(key): str(value)
        for key, value in dotenv_values(env_path, interpolate=False).items()
        if key and value is not None
    }
    required_keys = (*DB_KEYS, *REQUIRED_RUNTIME_KEYS) if require_runtime else DB_KEYS
    missing = tuple(key for key in required_keys if not values.get(key))
    if missing:
        raise ValueError(f"env_file is missing required Batch B configuration keys: {missing}")
    for key in required_keys:
        os.environ[key] = values[key]
    return values, env_path


def _database_config(values: dict[str, str]) -> dict[str, Any]:
    return {
        "host": values["TDX_DB_HOST"],
        "port": int(values["TDX_DB_PORT"]),
        "dbname": values["TDX_DB_NAME"],
        "user": values["TDX_DB_USER"],
        "password": values["TDX_DB_PASSWORD"],
    }


def _request(path: Path) -> BatchBDatasetMaterializationRequestV1:
    payload = json.loads(path.expanduser().resolve(strict=True).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("request file must contain one JSON object")
    return BatchBDatasetMaterializationRequestV1.model_validate(payload)


def _verify_repository(
    repository_root: Path,
    request: BatchBDatasetMaterializationRequestV1 | None = None,
) -> str:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if request is not None and head != request.dataset_intent.repository_commit:
        raise ValueError("repository HEAD differs from the frozen dataset intent commit")
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise ValueError("repository must be clean before durable Batch B materialization")
    return head


def _date(value: str) -> date:
    return date.fromisoformat(value)


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--request", type=Path)
    mode.add_argument("--prepare-program-id")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--spool-root", type=Path)
    parser.add_argument("--decision-date-start", type=_date)
    parser.add_argument("--decision-date-end", type=_date)
    parser.add_argument("--final-fit-as-of", type=_datetime)
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        values, env_file = _load_environment(
            args.env_file,
            require_runtime=args.prepare_program_id is None,
        )
        repository_root = _existing_directory(args.repository_root, field_name="repository_root")
        artifact_root = _existing_directory(args.artifact_root, field_name="artifact_root")
        config = _database_config(values)

        def conn_factory() -> Any:
            return psycopg2.connect(**config)

        if args.prepare_program_id is not None:
            missing = tuple(
                name
                for name, value in (
                    ("decision_date_start", args.decision_date_start),
                    ("decision_date_end", args.decision_date_end),
                    ("final_fit_as_of", args.final_fit_as_of),
                )
                if value is None
            )
            if missing:
                raise ValueError(
                    f"Batch B request preparation is missing required arguments: {missing}"
                )
            repository_commit = _verify_repository(repository_root)
            read_only_factory = historical_read_only_connection_factory(conn_factory)
            package_resolver = HistoricalRangeAdmittedPackageResolver(
                package_reader=StrategyPackageRepository(conn_factory=read_only_factory)
            )
            program_repository = AdvisoryProgramPGRepository(conn_factory=read_only_factory)
            program_resolver = HistoricalRangeProgramResolver(
                package_resolver=package_resolver,
                program_reader=program_repository,
            )
            code_release_resolver = HistoricalRangeCodeReleaseResolver(
                repository_root=repository_root,
                closure_paths=_R5_CODE_RELEASE_CLOSURE,
            )
            selection_hash = _historical_range_code_set_hash(
                repository_root,
                _R5_SELECTION_CLOSURE,
            )
            list_semantics = canonical_list_semantics_v2()

            def freeze_program(
                spec: ExistingProgramSpecV1,
                start_trade_date: date,
                end_trade_date: date,
            ) -> Any:
                release = code_release_resolver.resolve()
                request = HistoricalRangeResearchBatchRequestV1(
                    client_idempotency_key=(
                        f"adv-reranker-batch-b-prepare-{spec.program_id}-{repository_commit[:16]}"
                    ),
                    program_specs=(spec,),
                    start_trade_date=start_trade_date,
                    end_trade_date=end_trade_date,
                    requested_by="advisory-modeling-batch-b-request-builder",
                )
                programs = program_resolver.freeze_programs(
                    request=request,
                    code_release_id=release.code_release_id,
                    code_release_hash=release.code_release_hash,
                    selection_semantics_version="strategy_package_selection_semantics_v1",
                    selection_semantics_hash=selection_hash,
                    list_semantics_version=list_semantics.schema_version,
                    list_semantics_hash=str(list_semantics.semantics_hash),
                )
                if len(programs) != 1:
                    raise ValueError("Batch B request preparation did not freeze exactly one Program")
                return programs[0]

            builder = BatchBRequestBuilder(
                program_reader=program_repository,
                frozen_program_provider=freeze_program,
                package_created_at_provider=lambda package_id: package_resolver.resolve(
                    package_id
                ).record.created_at,
                calendar_provider=lambda start, end: _calendar(
                    conn_factory=read_only_factory,
                    start_trade_date=start,
                    end_trade_date=end,
                ),
                repository_commit=repository_commit,
            )
            prepared = builder.build(
                program_id=args.prepare_program_id,
                decision_date_start=args.decision_date_start,
                decision_date_end=args.decision_date_end,
                final_fit_as_of=args.final_fit_as_of,
            )
            request_path = publish_batch_b_request(
                request=prepared,
                artifact_root=artifact_root,
                repository_root=repository_root,
            )
            print(
                json.dumps(
                    {
                        "status": "PREPARED",
                        "request_hash": prepared.request_hash,
                        "request_path": str(request_path),
                        "program_id": prepared.existing_program.program_id,
                        "program_version": prepared.existing_program.expected_program_version,
                        "binding_version_id": (
                            prepared.existing_program.expected_binding_version_id
                        ),
                        "package_id": prepared.dataset_intent.package_id,
                        "decision_date_start": (
                            prepared.dataset_intent.decision_date_start.isoformat()
                        ),
                        "decision_date_end": prepared.dataset_intent.decision_date_end.isoformat(),
                        "final_fit_as_of": prepared.dataset_intent.final_fit_as_of.isoformat(),
                    },
                    sort_keys=True,
                )
            )
            return 0

        request = _request(args.request)
        if args.spool_root is None:
            raise ValueError("spool_root is required for Batch B materialization")
        spool_root = _existing_directory(args.spool_root, field_name="spool_root")
        dataset_root = _existing_directory(
            Path(values["AISTOCK_ADVISORY_DATASET_STORE_ROOT"]),
            field_name="dataset_store_root",
        )
        _verify_repository(repository_root, request)

        client_target = Phase0BClientDatabaseTargetV1(
            env_file_path_hash=hashlib.sha256(str(env_file).encode("utf-8")).hexdigest(),
            configured_host_hash=hashlib.sha256(config["host"].encode("utf-8")).hexdigest(),
            configured_port=int(config["port"]),
            configured_database_hash=hashlib.sha256(config["dbname"].encode("utf-8")).hexdigest(),
            configured_user_hash=hashlib.sha256(config["user"].encode("utf-8")).hexdigest(),
        )
        dataset_store = LocalContentAddressedStore(
            root=dataset_root,
            repository_root=repository_root,
            store_identity=historical_range_store_identity(),
        )
        historical_artifacts = HistoricalRangeArtifactStore.from_environment()
        service = BatchBMaterializationService(
            historical_driver=BatchBHistoricalRangeDriver(
                service=RestartSafeHistoricalRangeService(
                    service=build_environment_historical_range_r5_application_service()
                )
            ),
            base_reader=RerankerBaseSnapshotReader(
                catalog=PostgresPhase0BSnapshotCatalog(
                    conn_factory=conn_factory,
                    client_target=client_target,
                ),
                dataset_store=dataset_store,
                candidate_artifacts=HistoricalCandidateArtifactResolver(
                    artifact_store=historical_artifacts
                ),
            ),
            feature_source_reader=PostgresFeatureSourceReader(
                conn_factory=conn_factory,
                configured_host_hash=client_target.configured_host_hash,
                configured_port=client_target.configured_port,
                configured_database_hash=client_target.configured_database_hash,
                configured_user_hash=client_target.configured_user_hash,
                statement_timeout_ms=args.statement_timeout_ms,
            ),
        )
        result = service.execute(
            request=request,
            repository_root=repository_root,
            artifact_root=artifact_root,
            spool_root=spool_root,
        )
        print(json.dumps(result.model_dump(mode="json"), sort_keys=True))
        return 0
    except AdvisoryModelingError as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "reason_code": exc.reason_code,
                    "message": str(exc),
                    "context": exc.context,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "reason_code": "MODEL_BATCH_B_EXECUTION_FAILED",
                    "message": str(exc),
                    "error_type": type(exc).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
