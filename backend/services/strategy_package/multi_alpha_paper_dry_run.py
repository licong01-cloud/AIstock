"""LocalSim dry-run admission for MULTI_ALPHA StrategyPackages."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field

from backend.services.paper_trading_v2.models import BrokerBackendId
from backend.services.strategy_package.models import AlphaMode
from backend.services.strategy_package.multi_alpha_paper_admission import (
    MultiAlphaPaperAdmissionRecord,
    MultiAlphaPaperAdmissionRepository,
    admission_id_from_payload,
    canonical_json_sha256,
)
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.runtime import (
    RebalanceEngine,
    StrategyPackageRuntime,
    TargetPositionEngine,
)
from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactService
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    RuntimeConfigInvalidError,
    StrategyPackageValidationError,
)

MULTI_ALPHA_PAPER_DRY_RUN_CONFIRMATION = "MULTI_ALPHA_LOCALSIM_DRY_RUN"

REASON_MULTI_ALPHA_DRY_RUN_CONFIRMATION_REQUIRED = "multi_alpha_paper_dry_run_confirmation_required"
REASON_MULTI_ALPHA_DRY_RUN_NOT_APPLICABLE = "multi_alpha_paper_dry_run_not_applicable"
REASON_MULTI_ALPHA_DRY_RUN_UNSUPPORTED_BROKER = "multi_alpha_paper_dry_run_unsupported_broker_backend"
REASON_MULTI_ALPHA_DRY_RUN_INVALID_VARIANT = "multi_alpha_paper_dry_run_invalid_runtime_variant"
REASON_MULTI_ALPHA_DRY_RUN_INVALID_CONFIG = "multi_alpha_paper_dry_run_invalid_runtime_config"
REASON_MULTI_ALPHA_DRY_RUN_DETERMINISM_MISMATCH = "multi_alpha_paper_dry_run_determinism_mismatch"
REASON_MULTI_ALPHA_DRY_RUN_NO_ORDER_PREVIEW = "multi_alpha_paper_dry_run_no_order_preview"


class MultiAlphaPaperDryRunResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dry_run_run_id: str
    package_id: str
    manifest_sha256: str
    broker_backend: BrokerBackendId
    runtime_variant: str
    trade_date: date
    selection_artifact_id: str
    selection_artifact_sha256: str
    target_count: int
    order_intent_count: int
    deterministic_replay: bool
    artifact_shas: dict[str, Any] = Field(default_factory=dict)
    evidence_json: dict[str, Any] = Field(default_factory=dict)
    admission: MultiAlphaPaperAdmissionRecord

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class MultiAlphaPaperDryRunValidator:
    """Validate MULTI_ALPHA LocalSim signal readiness without placing orders."""

    def __init__(
        self,
        *,
        package_repository: Any | None = None,
        selection_artifact_service: StrategyPackageSelectionArtifactService | Any | None = None,
        runtime: StrategyPackageRuntime | None = None,
        target_engine: TargetPositionEngine | None = None,
        rebalance_engine: RebalanceEngine | None = None,
        admission_repository: Any | None = None,
        clock: Any | None = None,
    ) -> None:
        self.package_repository = package_repository or StrategyPackageRepository()
        self.selection_artifact_service = selection_artifact_service or StrategyPackageSelectionArtifactService(
            package_repository=self.package_repository
        )
        artifact_repository = getattr(self.selection_artifact_service, "artifact_repository", None)
        self.runtime = runtime or StrategyPackageRuntime(artifact_repository=artifact_repository)
        self.target_engine = target_engine or TargetPositionEngine()
        self.rebalance_engine = rebalance_engine or RebalanceEngine()
        self.admission_repository = admission_repository or MultiAlphaPaperAdmissionRepository()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def run(
        self,
        *,
        package_id: str,
        broker_backend: BrokerBackendId,
        trade_date: date,
        runtime_variant: str,
        confirmation: str,
        validated_by: str = "aistock_api",
        runtime_config: dict[str, Any] | None = None,
        initial_cash: float = 1_000_000.0,
    ) -> MultiAlphaPaperDryRunResult:
        if confirmation != MULTI_ALPHA_PAPER_DRY_RUN_CONFIRMATION:
            raise StrategyPackageValidationError(
                "MULTI_ALPHA paper dry-run requires explicit confirmation",
                context={
                    "reason_code": REASON_MULTI_ALPHA_DRY_RUN_CONFIRMATION_REQUIRED,
                    "package_id": package_id,
                },
            )
        if broker_backend != "local_sim":
            raise StrategyPackageValidationError(
                "MULTI_ALPHA paper dry-run currently admits LocalSim only",
                context={
                    "reason_code": REASON_MULTI_ALPHA_DRY_RUN_UNSUPPORTED_BROKER,
                    "package_id": package_id,
                    "broker_backend": broker_backend,
                    "allowed": ["local_sim"],
                },
            )
        top_k = runtime_variant_top_k(runtime_variant)
        if initial_cash <= 0:
            raise RuntimeConfigInvalidError(
                "initial_cash must be positive for MULTI_ALPHA dry-run",
                context={
                    "reason_code": REASON_MULTI_ALPHA_DRY_RUN_INVALID_CONFIG,
                    "package_id": package_id,
                    "initial_cash": initial_cash,
                },
            )
        if not str(validated_by or "").strip():
            raise RuntimeConfigInvalidError(
                "validated_by is required for MULTI_ALPHA dry-run admission",
                context={
                    "reason_code": REASON_MULTI_ALPHA_DRY_RUN_INVALID_CONFIG,
                    "package_id": package_id,
                },
            )

        record = self.package_repository.get(package_id)
        manifest = record.current_manifest()
        if manifest.alpha_mode != AlphaMode.MULTI_ALPHA:
            raise StrategyPackageValidationError(
                "paper-runtime-dry-run only applies to MULTI_ALPHA packages",
                context={
                    "reason_code": REASON_MULTI_ALPHA_DRY_RUN_NOT_APPLICABLE,
                    "package_id": package_id,
                    "alpha_mode": manifest.alpha_mode.value,
                },
            )
        if not manifest.manifest_sha256:
            raise StrategyPackageValidationError(
                "MULTI_ALPHA dry-run requires frozen manifest_sha256",
                context={"reason_code": "multi_alpha_manifest_incomplete", "package_id": package_id},
            )

        config = _runtime_config_for_variant(runtime_config or {}, top_k=top_k)
        artifact = self.selection_artifact_service.generate_from_live_inference(
            package_id=package_id,
            trade_date=trade_date,
            data_source="DB_HISTORICAL",
            runtime_config=config,
            include_reference_price=True,
        )
        snapshot = self.runtime.build_signal_snapshot(
            manifest=manifest,
            trade_date=trade_date,
            data_source="DB_HISTORICAL",
            runtime_config=config,
        )
        target_manifest = _manifest_for_topk(manifest, top_k=top_k)
        targets = self.target_engine.build_targets(
            snapshot=snapshot,
            total_equity=float(initial_cash),
            top_k=top_k,
            manifest=target_manifest,
            current_positions={},
            current_prices={},
        )
        intents = self.rebalance_engine.build_order_intents(
            package_id=manifest.package_id,
            portfolio_id=f"dryrun_{manifest.package_id}",
            trade_date=trade_date,
            current_positions={},
            target_positions=targets,
        )
        if not intents:
            raise ArtifactGenerationFailedError(
                "MULTI_ALPHA LocalSim dry-run produced no order preview",
                context={
                    "reason_code": REASON_MULTI_ALPHA_DRY_RUN_NO_ORDER_PREVIEW,
                    "package_id": package_id,
                    "trade_date": trade_date.isoformat(),
                    "runtime_variant": runtime_variant,
                },
            )

        replay = self.selection_artifact_service.generate_from_live_inference(
            package_id=package_id,
            trade_date=trade_date,
            data_source="DB_HISTORICAL",
            runtime_config=config,
            include_reference_price=True,
        )
        if _determinism_rows(artifact.scores_json) != _determinism_rows(replay.scores_json):
            raise ArtifactGenerationFailedError(
                "MULTI_ALPHA LocalSim dry-run deterministic replay mismatch",
                context={
                    "reason_code": REASON_MULTI_ALPHA_DRY_RUN_DETERMINISM_MISMATCH,
                    "package_id": package_id,
                    "trade_date": trade_date.isoformat(),
                    "runtime_variant": runtime_variant,
                    "first_artifact_id": artifact.artifact_id,
                    "second_artifact_id": replay.artifact_id,
                    "first_artifact_sha256": artifact.artifact_sha256,
                    "second_artifact_sha256": replay.artifact_sha256,
                },
            )

        targets_preview = [_target_payload(target) for target in targets]
        intents_preview = [_intent_payload(intent) for intent in intents]
        target_sha = canonical_json_sha256(targets_preview)
        order_sha = canonical_json_sha256(intents_preview)
        component_shas = dict(artifact.metadata.get("component_score_artifact_sha256") or {})
        combined_sha = _required_metadata(
            artifact.metadata,
            "combined_score_artifact_sha256",
            package_id=package_id,
            runtime_variant=runtime_variant,
        )
        weight_sha = _required_metadata(
            artifact.metadata,
            "weight_artifact_sha256",
            package_id=package_id,
            runtime_variant=runtime_variant,
        )
        artifact_shas = {
            "schema_version": "multi_alpha_paper_admission_artifacts_v1",
            "selection_artifact_sha256": artifact.artifact_sha256,
            "deterministic_replay_artifact_sha256": replay.artifact_sha256,
            "combined_score_artifact_sha256": combined_sha,
            "weight_artifact_sha256": weight_sha,
            "component_score_artifact_sha256": component_shas,
            "targets_sha256": target_sha,
            "order_intents_sha256": order_sha,
        }
        evidence_json = {
            "schema_version": "multi_alpha_paper_admission_evidence_v1",
            "package_id": package_id,
            "manifest_sha256": manifest.manifest_sha256,
            "broker_backend": broker_backend,
            "runtime_variant": runtime_variant,
            "trade_date": trade_date.isoformat(),
            "runtime_config": config,
            "runtime_config_hash": artifact.runtime_config_hash,
            "selection_artifact_id": artifact.artifact_id,
            "selection_artifact_metadata": artifact.metadata,
            "target_count": len(targets_preview),
            "order_intent_count": len(intents_preview),
            "targets_preview": targets_preview,
            "order_intents_preview": intents_preview,
            "deterministic_replay": True,
        }
        stable_payload = _stable_dry_run_payload(
            package_id=package_id,
            manifest_sha256=manifest.manifest_sha256,
            broker_backend=broker_backend,
            runtime_variant=runtime_variant,
            trade_date=trade_date,
            runtime_config_hash=artifact.runtime_config_hash,
            artifact_shas=artifact_shas,
            targets_preview=targets_preview,
            intents_preview=intents_preview,
        )
        dry_run_run_id = f"mapdry_{canonical_json_sha256(stable_payload)[:24]}"
        admission_payload = {
            "package_id": package_id,
            "manifest_sha256": manifest.manifest_sha256,
            "broker_backend": broker_backend,
            "runtime_variant": runtime_variant,
            "dry_run_run_id": dry_run_run_id,
            "artifact_shas": artifact_shas,
            "stable_evidence": stable_payload,
        }
        admission = MultiAlphaPaperAdmissionRecord(
            admission_id=admission_id_from_payload(admission_payload),
            package_id=package_id,
            manifest_sha256=manifest.manifest_sha256,
            broker_backend=broker_backend,
            runtime_variant=runtime_variant,
            eligible=True,
            dry_run_run_id=dry_run_run_id,
            artifact_shas=artifact_shas,
            evidence_json=evidence_json,
            validated_at=self.clock(),
            validated_by=validated_by,
        )
        saved = self.admission_repository.upsert_success(admission)
        return MultiAlphaPaperDryRunResult(
            dry_run_run_id=dry_run_run_id,
            package_id=package_id,
            manifest_sha256=manifest.manifest_sha256,
            broker_backend=broker_backend,
            runtime_variant=runtime_variant,
            trade_date=trade_date,
            selection_artifact_id=artifact.artifact_id,
            selection_artifact_sha256=str(artifact.artifact_sha256 or ""),
            target_count=len(targets_preview),
            order_intent_count=len(intents_preview),
            deterministic_replay=True,
            artifact_shas=artifact_shas,
            evidence_json=evidence_json,
            admission=saved,
        )


def runtime_variant_top_k(runtime_variant: str) -> int:
    if runtime_variant not in {"top_k=25", "top_k=50"}:
        raise RuntimeConfigInvalidError(
            "MULTI_ALPHA dry-run runtime_variant must be top_k=25 or top_k=50",
            context={
                "reason_code": REASON_MULTI_ALPHA_DRY_RUN_INVALID_VARIANT,
                "runtime_variant": runtime_variant,
                "allowed": ["top_k=25", "top_k=50"],
            },
        )
    return int(runtime_variant.split("=", 1)[1])


def runtime_variant_from_topk(top_k: int | None) -> str | None:
    if top_k is None:
        return None
    return f"top_k={int(top_k)}"


def _runtime_config_for_variant(config: dict[str, Any], *, top_k: int) -> dict[str, Any]:
    merged = deepcopy(config)
    profile = merged.setdefault("runtime_profile", {})
    if not isinstance(profile, dict):
        raise RuntimeConfigInvalidError(
            "runtime_profile must be an object",
            context={
                "reason_code": REASON_MULTI_ALPHA_DRY_RUN_INVALID_CONFIG,
                "runtime_profile_type": type(profile).__name__,
            },
        )
    selection = profile.setdefault("selection", {})
    if not isinstance(selection, dict):
        raise RuntimeConfigInvalidError(
            "runtime_profile.selection must be an object",
            context={
                "reason_code": REASON_MULTI_ALPHA_DRY_RUN_INVALID_CONFIG,
                "runtime_profile_selection_type": type(selection).__name__,
            },
        )
    selection["top_k"] = top_k
    return merged


def _manifest_for_topk(manifest: Any, *, top_k: int) -> Any:
    backtest_context = deepcopy(manifest.backtest_context or {})
    daily_strategy = backtest_context.setdefault("daily_strategy", {})
    if isinstance(daily_strategy, dict):
        daily_strategy["topk"] = top_k
    return manifest.model_copy(update={"backtest_context": backtest_context})


def _determinism_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = ("rank", "symbol", "score", "target_weight", "reference_price", "component_scores")
    return [{key: row.get(key) for key in keys} for row in rows]


def _target_payload(target: Any) -> dict[str, Any]:
    return {
        "symbol": target.symbol,
        "target_quantity": int(target.target_quantity),
        "target_weight": target.target_weight,
        "reference_price": target.reference_price,
        "score": target.score,
        "rank": target.rank,
        "reason": target.reason,
        "metadata": _stable_preview_payload(target.metadata),
    }


def _intent_payload(intent: Any) -> dict[str, Any]:
    return {
        "preview_intent_key": canonical_json_sha256(
            {
                "package_id": intent.package_id,
                "portfolio_id": intent.portfolio_id,
                "symbol": intent.symbol,
                "side": intent.side.value,
                "quantity": int(intent.quantity),
                "target_trade_date": intent.target_trade_date.isoformat(),
            }
        )[:24],
        "symbol": intent.symbol,
        "side": intent.side.value,
        "quantity": int(intent.quantity),
        "target_trade_date": intent.target_trade_date.isoformat(),
        "metadata": _stable_preview_payload(intent.metadata),
    }


def _stable_preview_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _stable_preview_payload(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in {"snapshot_id"}
        }
    if isinstance(value, list):
        return [_stable_preview_payload(item) for item in value]
    return value


def _required_metadata(metadata: Mapping[str, Any], key: str, *, package_id: str, runtime_variant: str) -> str:
    value = metadata.get(key)
    if not value:
        raise ArtifactGenerationFailedError(
            "MULTI_ALPHA LocalSim dry-run selection artifact metadata is incomplete",
            context={
                "reason_code": REASON_MULTI_ALPHA_DRY_RUN_INVALID_CONFIG,
                "package_id": package_id,
                "runtime_variant": runtime_variant,
                "missing_metadata_key": key,
            },
        )
    return str(value)


def _stable_dry_run_payload(
    *,
    package_id: str,
    manifest_sha256: str,
    broker_backend: BrokerBackendId,
    runtime_variant: str,
    trade_date: date,
    runtime_config_hash: str,
    artifact_shas: Mapping[str, Any],
    targets_preview: list[dict[str, Any]],
    intents_preview: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": "multi_alpha_paper_admission_stable_evidence_v1",
        "package_id": package_id,
        "manifest_sha256": manifest_sha256,
        "broker_backend": broker_backend,
        "runtime_variant": runtime_variant,
        "trade_date": trade_date.isoformat(),
        "runtime_config_hash": runtime_config_hash,
        "artifact_shas": dict(artifact_shas),
        "targets_preview": targets_preview,
        "order_intents_preview": intents_preview,
    }
