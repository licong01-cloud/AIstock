"""Read-only StrategyPackage delivery compatibility preflight for Advisory."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Callable

from backend.services.advisory_model_first.model_binding_resolution import AdvisoryModelBindingResolver
from backend.services.advisory_program import AdvisoryProgramService
from backend.services.advisory_universe import (
    AdvisoryUniverseContractError,
    normalize_advisory_universe_selection,
)
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.service import StrategyPackageService


PREFLIGHT_SCHEMA_VERSION = "advisory_delivery_preflight_v1"

UNIVERSE_EXACT = "EXACT_UNIVERSE_MATCHED"
UNIVERSE_FILTER_ONLY = "FILTER_ONLY_COMPATIBLE"
UNIVERSE_LEGACY = "LEGACY_UNIVERSE_UNSPECIFIED"
UNIVERSE_MISMATCH = "PACKAGE_IDENTITY_MISMATCH"
UNIVERSE_INCOMPLETE = "DELIVERY_CONTRACT_INCOMPLETE"

MODEL_PRESENT = "DESCRIPTOR_FILE_PRESENT"
MODEL_UNAVAILABLE = "MODEL_DESCRIPTOR_UNAVAILABLE"
MODEL_REQUIRED_AFTER_BINDING = "REQUIRED_AFTER_BINDING"
MODEL_NOT_APPLICABLE = "NOT_APPLICABLE"


class AdvisoryDeliveryPreflightService:
    """Classify one package/binding proposal without mutating any subsystem."""

    def __init__(
        self,
        *,
        package_service: StrategyPackageService | Any | None = None,
        program_service: AdvisoryProgramService | Any | None = None,
        model_resolver: AdvisoryModelBindingResolver | Any | None = None,
        model_root_provider: Callable[[], str] | None = None,
    ) -> None:
        self._package_service = package_service or StrategyPackageService()
        self._program_service = program_service or AdvisoryProgramService()
        self._model_resolver = model_resolver or AdvisoryModelBindingResolver()
        self._model_root_provider = model_root_provider or (
            lambda: os.getenv("AISTOCK_ADVISORY_MODEL_ROOT", "").strip()
        )

    def preflight(
        self,
        *,
        package_id: str,
        universe_selection: Mapping[str, Any] | None,
        target_count: int,
        program_id: str | None = None,
    ) -> dict[str, Any]:
        requested_package_id = str(package_id or "").strip()
        if not requested_package_id:
            raise AdvisoryUniverseContractError(
                "ADVISORY_DELIVERY_PACKAGE_ID_REQUIRED",
                "package_id is required for Advisory delivery preflight",
            )
        requested_universe = normalize_advisory_universe_selection(universe_selection)
        if target_count <= 0 or target_count > 100:
            raise AdvisoryUniverseContractError(
                "ADVISORY_DELIVERY_TARGET_COUNT_INVALID",
                "target_count must be between 1 and 100",
                context={"target_count": target_count},
            )
        record = self._package_service.get_package(requested_package_id)
        manifest = record.current_manifest()
        normalized_program_id = str(program_id or "").strip() or None
        program = self._program_service.get_program(normalized_program_id) if normalized_program_id else None
        active_binding = self._program_service.active_binding(normalized_program_id) if normalized_program_id else None

        blockers: list[str] = []
        warnings: list[str] = []
        package_status = _enum_value(getattr(record, "package_status", None))
        if package_status == PackageStatus.RETIRED.value:
            blockers.append("PACKAGE_RETIRED")

        eligibility = self._package_service.asset_eligibility.summarize(record)
        eligibility_blockers = [str(value) for value in getattr(eligibility, "blockers", ()) if str(value)]
        eligibility_warnings = [str(value) for value in getattr(eligibility, "warnings", ()) if str(value)]
        if not bool(getattr(eligibility, "eligible", False)):
            blockers.extend(f"PACKAGE_ASSET_INELIGIBLE:{value}" for value in eligibility_blockers)
            if not eligibility_blockers:
                blockers.append("PACKAGE_ASSET_INELIGIBLE")
        warnings.extend(f"PACKAGE_ASSET_WARNING:{value}" for value in eligibility_warnings)

        universe_result = _classify_universe(manifest, requested_universe)
        blockers.extend(universe_result.pop("blockers"))
        warnings.extend(universe_result.pop("warnings"))

        policy_result = _policy_compatibility(
            manifest=manifest,
            target_count=target_count,
            program=program,
        )
        model_result = self._model_compatibility(
            package_id=requested_package_id,
            requested_universe=requested_universe,
            target_count=target_count,
            program_id=normalized_program_id,
            program=program,
            active=active_binding,
            blocked=bool(blockers),
        )
        warnings.extend(model_result.pop("warnings"))

        blockers = _dedupe(blockers)
        warnings = _dedupe(warnings)
        if blockers:
            overall_status = "BLOCKED"
        elif model_result["status"] == MODEL_PRESENT:
            overall_status = "READY_WITH_MODEL"
        else:
            overall_status = "READY_BASELINE_ONLY"

        source = getattr(manifest, "source", None)
        return {
            "schema_version": PREFLIGHT_SCHEMA_VERSION,
            "overall_status": overall_status,
            "package": {
                "package_id": requested_package_id,
                "manifest_sha256": str(getattr(record, "manifest_sha256", "") or ""),
                "package_status": package_status,
                "source_type": _enum_value(getattr(source, "source_type", None)),
                "source_id": str(getattr(source, "source_id", "") or ""),
                "asset_eligible": bool(getattr(eligibility, "eligible", False)),
                "asset_blockers": eligibility_blockers,
            },
            "universe_compatibility": universe_result,
            "policy_compatibility": policy_result,
            "model_compatibility": model_result,
            "blockers": blockers,
            "warnings": warnings,
        }

    def _model_compatibility(
        self,
        *,
        package_id: str,
        requested_universe: dict[str, Any],
        target_count: int,
        program_id: str | None,
        program: Any | None,
        active: Mapping[str, Any] | None,
        blocked: bool,
    ) -> dict[str, Any]:
        if blocked:
            return {
                "status": MODEL_NOT_APPLICABLE,
                "validation_stage": "NOT_APPLICABLE",
                "binding_version_id": None,
                "warnings": [],
            }
        if not program_id:
            return {
                "status": MODEL_REQUIRED_AFTER_BINDING,
                "validation_stage": "PUBLICATION_FULL_RESOLUTION",
                "binding_version_id": None,
                "warnings": ["MODEL_DESCRIPTOR_REQUIRED_AFTER_BINDING"],
            }

        if active is None or program is None:
            raise AdvisoryUniverseContractError(
                "ADVISORY_DELIVERY_PROGRAM_IDENTITY_UNAVAILABLE",
                "active Advisory Program binding is unavailable",
                context={"program_id": program_id},
            )
        active_package_ids = [str(value).strip() for value in active.get("package_ids") or []]
        active_universe = normalize_advisory_universe_selection(active.get("universe_selection"))
        binding_version_id = str(active.get("binding_version_id") or "").strip()
        same_binding_identity = (
            active.get("package_mode") == "single_package"
            and active_package_ids == [package_id]
            and active_universe == requested_universe
            and int(getattr(program, "target_count", 0)) == target_count
        )
        if not same_binding_identity:
            return {
                "status": MODEL_REQUIRED_AFTER_BINDING,
                "validation_stage": "PUBLICATION_FULL_RESOLUTION",
                "binding_version_id": None,
                "warnings": ["MODEL_DESCRIPTOR_REQUIRED_AFTER_BINDING"],
            }

        model_root = str(self._model_root_provider() or "").strip()
        if not model_root:
            return {
                "status": MODEL_UNAVAILABLE,
                "validation_stage": "PUBLICATION_FULL_RESOLUTION",
                "binding_version_id": binding_version_id,
                "warnings": ["ADVISORY_MODEL_ROOT_NOT_CONFIGURED"],
            }
        configured = self._model_resolver.is_configured(
            model_root=model_root,
            program_id=program_id,
            binding_version_id=binding_version_id,
        )
        return {
            "status": MODEL_PRESENT if configured else MODEL_UNAVAILABLE,
            "validation_stage": "PRESENCE_ONLY",
            "binding_version_id": binding_version_id,
            "warnings": [] if configured else ["ADVISORY_MODEL_DESCRIPTOR_NOT_FOUND"],
        }


def _classify_universe(manifest: Any, requested: dict[str, Any]) -> dict[str, Any]:
    evidence: list[tuple[str, Any]] = []
    backtest_context = getattr(manifest, "backtest_context", {})
    if isinstance(backtest_context, Mapping):
        daily_strategy = backtest_context.get("daily_strategy")
        if isinstance(daily_strategy, Mapping):
            custom_params = daily_strategy.get("custom_params")
            if isinstance(custom_params, Mapping) and "universe_selection" in custom_params:
                evidence.append(
                    (
                        "backtest_context.daily_strategy.custom_params.universe_selection",
                        custom_params.get("universe_selection"),
                    )
                )
    source_evidence = getattr(manifest, "source_evidence", {})
    if isinstance(source_evidence, Mapping):
        custom_params = source_evidence.get("custom_params")
        if isinstance(custom_params, Mapping) and "universe_selection" in custom_params:
            evidence.append(("source_evidence.custom_params.universe_selection", custom_params.get("universe_selection")))

    normalized_evidence: list[tuple[str, dict[str, Any]]] = []
    evidence_errors: list[dict[str, Any]] = []
    for path, raw in evidence:
        try:
            normalized_evidence.append((path, normalize_advisory_universe_selection(raw)))
        except AdvisoryUniverseContractError as exc:
            evidence_errors.append({"path": path, "reason_code": exc.reason_code})

    evidence_paths = [path for path, _value in evidence]
    if evidence_errors:
        return {
            "status": UNIVERSE_INCOMPLETE,
            "requested": requested,
            "source_declared": None,
            "evidence_paths": evidence_paths,
            "evidence_errors": evidence_errors,
            "blockers": ["DELIVERY_UNIVERSE_CONTRACT_INVALID"],
            "warnings": [],
        }
    primary_evidence_present = any(
        path == "backtest_context.daily_strategy.custom_params.universe_selection"
        for path, _value in normalized_evidence
    )
    if normalized_evidence and not primary_evidence_present:
        return {
            "status": UNIVERSE_INCOMPLETE,
            "requested": requested,
            "source_declared": None,
            "evidence_paths": [path for path, _value in normalized_evidence],
            "evidence_errors": [],
            "blockers": ["DELIVERY_UNIVERSE_PRIMARY_EVIDENCE_MISSING"],
            "warnings": [],
        }
    if not normalized_evidence:
        return {
            "status": UNIVERSE_LEGACY,
            "requested": requested,
            "source_declared": None,
            "evidence_paths": [],
            "evidence_errors": [],
            "blockers": [],
            "warnings": ["PACKAGE_UNIVERSE_IDENTITY_UNSPECIFIED"],
        }

    declared = normalized_evidence[0][1]
    if any(value != declared for _path, value in normalized_evidence[1:]):
        return {
            "status": UNIVERSE_INCOMPLETE,
            "requested": requested,
            "source_declared": None,
            "evidence_paths": [path for path, _value in normalized_evidence],
            "evidence_errors": [],
            "blockers": ["DELIVERY_UNIVERSE_EVIDENCE_CONFLICT"],
            "warnings": [],
        }

    if declared == requested:
        status = UNIVERSE_EXACT
        blockers: list[str] = []
        warnings: list[str] = []
    elif _is_filter_only_compatible(declared, requested):
        status = UNIVERSE_FILTER_ONLY
        blockers = []
        warnings = ["ADVISORY_POST_SELECTION_FILTER_NOT_QE_EXACT_UNIVERSE"]
    else:
        status = UNIVERSE_MISMATCH
        blockers = ["PACKAGE_UNIVERSE_IDENTITY_MISMATCH"]
        warnings = []
    return {
        "status": status,
        "requested": requested,
        "source_declared": declared,
        "evidence_paths": [path for path, _value in normalized_evidence],
        "evidence_errors": [],
        "blockers": blockers,
        "warnings": warnings,
    }


def _policy_compatibility(*, manifest: Any, target_count: int, program: Any | None) -> dict[str, Any]:
    backtest_topk: int | None = None
    backtest_context = getattr(manifest, "backtest_context", {})
    if isinstance(backtest_context, Mapping):
        daily_strategy = backtest_context.get("daily_strategy")
        if isinstance(daily_strategy, Mapping):
            raw_topk = daily_strategy.get("topk")
            if isinstance(raw_topk, int) and not isinstance(raw_topk, bool) and raw_topk > 0:
                backtest_topk = raw_topk
    active_target_count = int(getattr(program, "target_count", 0)) if program is not None else None
    return {
        "status": (
            "ACTIVE_POLICY_MATCH"
            if active_target_count == target_count
            else "NEW_POLICY_BINDING_REQUIRED"
        ),
        "requested_target_count": target_count,
        "active_target_count": active_target_count,
        "package_backtest_topk": backtest_topk,
        "package_policy_authority": "DIAGNOSTIC_ONLY_NOT_ADVISORY_RUNTIME_AUTHORITY",
    }


def _is_filter_only_compatible(source: dict[str, Any], target: dict[str, Any]) -> bool:
    if source["mode"] == "stock_universe":
        return target["mode"] != "stock_universe"
    if target["mode"] == "stock_universe":
        return False
    source_pools = set(source["pool_ids"])
    target_pools = set(target["pool_ids"])
    return bool(target_pools) and target_pools <= source_pools


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "")


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


__all__ = [
    "AdvisoryDeliveryPreflightService",
    "PREFLIGHT_SCHEMA_VERSION",
]
