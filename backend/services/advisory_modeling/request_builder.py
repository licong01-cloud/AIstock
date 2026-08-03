from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime
import json
import os
from pathlib import Path
import tempfile
from typing import Protocol

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.api_models import ExistingProgramInput
from backend.services.advisory_historical_range.models import (
    ExistingProgramSpecV1,
    HistoricalRangeAlphaMode,
    HistoricalRangeFrozenProgramV1,
)
from backend.services.advisory_phase1.label_policy import TradingCalendar
from backend.services.advisory_program import (
    BINDING_STATUS_ACTIVE,
    AdvisoryProgram,
    AdvisoryStrategyBindingVersion,
)

from .batch_b import BatchBDatasetMaterializationRequestV1
from .feature_builder import frozen_formula_registry_v1
from .feature_schema import frozen_feature_schema_v1
from .feature_sources import frozen_feature_query_registry_v1
from .label_policy import RankingLabelPolicyV1
from .market_regime import MarketRegimePolicyTemplateV1
from .style_profile import SHORT_REBOUND_TARGET_PACKAGE_ID, StrategyStyleProfileV1
from .training_view import DatasetBuildIntentV1


class AdvisoryProgramIdentityReader(Protocol):
    def get_program(self, program_id: str) -> AdvisoryProgram: ...

    def list_binding_versions(self, program_id: str) -> list[AdvisoryStrategyBindingVersion]: ...


FrozenProgramProvider = Callable[
    [ExistingProgramSpecV1, date, date], HistoricalRangeFrozenProgramV1
]
PackageCreatedAtProvider = Callable[[str], datetime]
CalendarProvider = Callable[[date, date], TradingCalendar]


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
        frozen = self._frozen_program_provider(
            spec,
            decision_date_start,
            decision_date_end,
        )
        self._verify_target(frozen=frozen, program=program, binding=binding)

        calendar = self._calendar_provider(decision_date_start, decision_date_end)
        if decision_date_start not in calendar.trading_dates:
            raise ValueError("calendar authority does not contain decision_date_start")
        if decision_date_end not in calendar.trading_dates:
            raise ValueError("calendar authority does not contain decision_date_end")
        if normalized_fit_as_of.date() not in calendar.trading_dates:
            raise ValueError("calendar authority does not contain final_fit_as_of trade date")

        package_created_at = self._package_created_at_provider(frozen.package_id)
        if package_created_at.tzinfo is None or package_created_at.utcoffset() is None:
            raise ValueError("package created_at authority must be timezone-aware")
        cutoff_dates = [package_created_at.astimezone(UTC).date()]
        if program.enabled_since is not None:
            cutoff_dates.append(_aware_utc_date(program.enabled_since, field_name="program.enabled_since"))
        if binding.effective_from_trade_date is not None:
            cutoff_dates.append(binding.effective_from_trade_date)
        if binding.activated_at is not None:
            cutoff_dates.append(_aware_utc_date(binding.activated_at, field_name="binding.activated_at"))

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
