"""Read-only StrategyPackage health checks for Selection Center.

The health gate is intentionally conservative: it reports what is known before
an operator starts live inference, and it blocks only product modes that require
ST PIT authoritative behavior.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.strategy_package.backtest_contract import build_backtest_runtime_contract
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.selection_artifact import selection_artifact_runtime_hash
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError, TradingCoreError

from .runtime_profile import parse_selection_runtime_profile


PASS = "PASS"
WARN = "WARN"
BLOCKED = "BLOCKED"
UNKNOWN = "UNKNOWN"


class SelectionPackageHealthService:
    """Build operator-facing package health summaries without mutating assets."""

    def __init__(
        self,
        *,
        artifact_repository: Any | None = None,
        runtime_source_resolver: Any | None = None,
        hmm_runtime: Any | None = None,
    ) -> None:
        self.artifact_repository = artifact_repository
        self.runtime_source_resolver = runtime_source_resolver
        self.hmm_runtime = hmm_runtime

    def summarize(
        self,
        record: Any,
        *,
        runtime_config: dict[str, Any] | None = None,
        trade_date: date | None = None,
        data_source: str | None = None,
    ) -> dict[str, Any]:
        manifest = record.current_manifest()
        config = runtime_config or {}
        checks: list[dict[str, Any]] = []

        contract = self._contract_check(manifest)
        checks.append(contract)
        st_pit_contract_status = contract["status"]
        legacy_non_st_pit = bool(contract["context"].get("legacy_non_st_pit"))

        checks.append(self._latest_artifact_check(manifest))
        if trade_date is not None and data_source:
            checks.append(self._requested_artifact_check(manifest, trade_date, data_source, config))
        if self._auto_generate(config):
            checks.append(self._source_resolution_check(record))
        else:
            checks.append(
                {
                    "name": "source_resolves",
                    "status": UNKNOWN,
                    "message": "not checked until live artifact auto-generation is requested",
                    "context": {},
                }
            )

        checks.extend(self._deferred_runtime_checks(self._hmm_artifact_check(manifest, config, trade_date)))
        blocked = [item for item in checks if item["status"] == BLOCKED]
        extra_checks: list[dict[str, Any]] = []
        st_pit_authoritative = bool(config.get("st_pit_authoritative") or config.get("enforce_st_pit_contract"))
        if st_pit_authoritative and legacy_non_st_pit:
            extra_checks.append(
                self._blocked_check(
                    "st_pit_contract_required",
                    "ST PIT authoritative selection requires a StrategyPackage created from a ST PIT QE backtest",
                    {"package_id": manifest.package_id},
                )
            )
            blocked.extend(extra_checks)

        if blocked:
            status = BLOCKED
        elif legacy_non_st_pit:
            status = "LEGACY_NON_ST_PIT"
        elif any(item["status"] == WARN for item in checks):
            status = "WARN"
        else:
            status = "RUNNABLE"

        return {
            "status": status,
            "runnable": not blocked and not legacy_non_st_pit,
            "st_pit_contract_status": st_pit_contract_status,
            "legacy_non_st_pit": legacy_non_st_pit,
            "checks": [*checks, *extra_checks],
        }

    def require_runnable(
        self,
        record: Any,
        *,
        runtime_config: dict[str, Any],
        trade_date: date,
        data_source: str,
    ) -> dict[str, Any]:
        health = self.summarize(
            record,
            runtime_config=runtime_config,
            trade_date=trade_date,
            data_source=data_source,
        )
        if bool(runtime_config.get("st_pit_authoritative") or runtime_config.get("enforce_st_pit_contract")):
            if not health["runnable"]:
                raise StrategyPackageValidationError(
                    "strategy package is blocked by Selection Center health preflight",
                    context={
                        "package_id": record.package_id,
                        "health_status": health["status"],
                        "checks": health["checks"],
                    },
                )
        return health

    @staticmethod
    def _blocked_check(name: str, message: str, context: dict[str, Any]) -> dict[str, Any]:
        return {"name": name, "status": BLOCKED, "message": message, "context": context}

    @staticmethod
    def _auto_generate(runtime_config: dict[str, Any]) -> bool:
        artifact_config = runtime_config.get("selection_artifact_config")
        if artifact_config is None:
            artifact_config = runtime_config.get("selection_artifact")
        return isinstance(artifact_config, dict) and bool(artifact_config.get("auto_generate"))

    @staticmethod
    def _contract_check(manifest: Any) -> dict[str, Any]:
        try:
            contract = build_backtest_runtime_contract(manifest)
            risk_contract = contract["runtime_features"]["risk_policy"]
        except TradingCoreError as exc:
            return {
                "name": "st_pit_contract_status",
                "status": BLOCKED,
                "message": exc.message,
                "context": exc.context,
            }
        except Exception as exc:
            return {
                "name": "st_pit_contract_status",
                "status": BLOCKED,
                "message": str(exc),
                "context": {"package_id": manifest.package_id},
            }

        policy = risk_contract.get("policy") or {}
        providers = set(policy.get("providers") or [])
        hard_actions = set(policy.get("hard_actions") or [])
        if not risk_contract.get("enabled"):
            return {
                "name": "st_pit_contract_status",
                "status": WARN,
                "message": "legacy package: frozen QE backtest contract did not enable ST PIT risk policy",
                "context": {
                    "package_id": manifest.package_id,
                    "legacy_non_st_pit": True,
                    "contract": contract,
                },
            }
        missing = []
        if "st_pit" not in providers:
            missing.append("providers.st_pit")
        for action in ("block_buy", "force_exit"):
            if action not in hard_actions:
                missing.append(f"hard_actions.{action}")
        if missing:
            return {
                "name": "st_pit_contract_status",
                "status": BLOCKED,
                "message": "risk_policy contract is enabled but incomplete",
                "context": {"package_id": manifest.package_id, "missing": missing, "contract": contract},
            }
        return {
            "name": "st_pit_contract_status",
            "status": PASS,
            "message": "frozen QE backtest contract contains ST PIT risk policy",
            "context": {"package_id": manifest.package_id, "contract": contract},
        }

    def _latest_artifact_check(self, manifest: Any) -> dict[str, Any]:
        if self.artifact_repository is None or not hasattr(self.artifact_repository, "list"):
            return {
                "name": "runtime_assets_ready",
                "status": UNKNOWN,
                "message": "selection artifact repository is not available to the health service",
                "context": {"package_id": manifest.package_id},
            }
        try:
            artifacts = self.artifact_repository.list(
                package_id=manifest.package_id,
                manifest_sha256=manifest.manifest_sha256,
                limit=1,
            )
        except TradingCoreError as exc:
            return {"name": "runtime_assets_ready", "status": WARN, "message": exc.message, "context": exc.context}
        if not artifacts:
            return {
                "name": "runtime_assets_ready",
                "status": WARN,
                "message": "no authoritative selection artifact is cached yet",
                "context": {"package_id": manifest.package_id},
            }
        artifact = artifacts[0]
        metadata = artifact.metadata or {}
        status = PASS if metadata.get("source_type") == AUTHORITATIVE_SELECTION_SOURCE_TYPE else WARN
        return {
            "name": "runtime_assets_ready",
            "status": status,
            "message": "latest cached selection artifact inspected",
            "context": {
                "artifact_id": artifact.artifact_id,
                "trade_date": artifact.trade_date.isoformat(),
                "score_count": artifact.score_count,
                "source_type": metadata.get("source_type"),
            },
        }

    def _requested_artifact_check(
        self,
        manifest: Any,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        if self._auto_generate(runtime_config):
            return {
                "name": "requested_selection_artifact",
                "status": PASS,
                "message": "live artifact auto-generation is enabled for this request",
                "context": {"trade_date": trade_date.isoformat(), "data_source": data_source},
            }
        if self.artifact_repository is None or not hasattr(self.artifact_repository, "get"):
            return {
                "name": "requested_selection_artifact",
                "status": UNKNOWN,
                "message": "selection artifact repository is not available to the health service",
                "context": {"trade_date": trade_date.isoformat(), "data_source": data_source},
            }
        try:
            artifact = self.artifact_repository.get(
                package_id=manifest.package_id,
                manifest_sha256=manifest.manifest_sha256,
                trade_date=trade_date,
                data_source=data_source,
                runtime_config_hash=selection_artifact_runtime_hash(runtime_config),
            )
        except DataUnavailableError as exc:
            return {"name": "requested_selection_artifact", "status": BLOCKED, "message": exc.message, "context": exc.context}
        metadata = artifact.metadata or {}
        if metadata.get("source_type") != AUTHORITATIVE_SELECTION_SOURCE_TYPE:
            return {
                "name": "requested_selection_artifact",
                "status": BLOCKED,
                "message": "requested selection artifact is not authoritative live inference",
                "context": {"artifact_id": artifact.artifact_id, "source_type": metadata.get("source_type")},
            }
        return {
            "name": "requested_selection_artifact",
            "status": PASS,
            "message": "requested authoritative selection artifact exists",
            "context": {"artifact_id": artifact.artifact_id, "score_count": artifact.score_count},
        }

    def _source_resolution_check(self, record: Any) -> dict[str, Any]:
        if self.runtime_source_resolver is None:
            return {
                "name": "source_resolves",
                "status": UNKNOWN,
                "message": "runtime source resolver is not available to the health service",
                "context": {"package_id": record.package_id},
            }
        try:
            source_loader = getattr(self.runtime_source_resolver, "load_source_for_strategy_package", None)
            if callable(source_loader):
                source_loader(
                    source_type=record.source_type,
                    source_id=record.source_id,
                    loop_id=record.loop_id,
                    run_id=record.run_id,
                )
            else:
                self.runtime_source_resolver.load_source(record.source_id)
        except TradingCoreError as exc:
            return {"name": "source_resolves", "status": BLOCKED, "message": exc.message, "context": exc.context}
        return {
            "name": "source_resolves",
            "status": PASS,
            "message": "StrategyPackage QE source resolves for live inference",
            "context": {"source_type": record.source_type, "source_id": record.source_id, "loop_id": record.loop_id},
        }

    def _hmm_artifact_check(
        self,
        manifest: Any,
        runtime_config: dict[str, Any],
        trade_date: date | None,
    ) -> dict[str, Any]:
        try:
            profile = parse_selection_runtime_profile(runtime_config)
        except TradingCoreError as exc:
            return {"name": "hmm_artifact_status", "status": BLOCKED, "message": exc.message, "context": exc.context}
        if not profile.hmm.enabled:
            return {
                "name": "hmm_artifact_status",
                "status": UNKNOWN,
                "message": "not requested because runtime_profile.hmm.enabled is false",
                "context": {},
            }
        if trade_date is None:
            return {
                "name": "hmm_artifact_status",
                "status": BLOCKED,
                "message": "HMM artifact health requires trade_date",
                "context": {"package_id": manifest.package_id},
            }
        if self.hmm_runtime is None or not hasattr(self.hmm_runtime, "preflight_coefficients"):
            return {
                "name": "hmm_artifact_status",
                "status": BLOCKED,
                "message": "HMM runtime is not available for artifact preflight",
                "context": {"package_id": manifest.package_id},
            }
        try:
            context = self.hmm_runtime.preflight_coefficients(
                trade_date=trade_date,
                profile=profile.hmm,
                package_id=manifest.package_id,
            )
        except TradingCoreError as exc:
            return {"name": "hmm_artifact_status", "status": BLOCKED, "message": exc.message, "context": exc.context}
        except Exception as exc:
            return {
                "name": "hmm_artifact_status",
                "status": BLOCKED,
                "message": str(exc),
                "context": {"package_id": manifest.package_id},
            }
        return {
            "name": "hmm_artifact_status",
            "status": PASS,
            "message": "HMM coefficient artifact preflight passed",
            "context": context,
        }

    @staticmethod
    def _deferred_runtime_checks(hmm_artifact_check: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "name": "model_schema_matches",
                "status": UNKNOWN,
                "message": "checked by strict live inference preflight; never padded or truncated by Selection Center",
                "context": {},
            },
            {
                "name": "strict_feature_kept_rows",
                "status": UNKNOWN,
                "message": "checked by strict live inference after feature preparation",
                "context": {},
            },
            hmm_artifact_check,
            {
                "name": "cold_cache_safe",
                "status": UNKNOWN,
                "message": "requires explicit cold-cache materialization regression",
                "context": {},
            },
        ]
