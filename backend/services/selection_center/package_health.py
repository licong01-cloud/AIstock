"""Read-only StrategyPackage health diagnostics for Selection Center.

Package health is diagnostic only in Selection/Paper/MiniQMT simulation paths:
it may report runtime risks, but it must not decide StrategyPackage admission.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.strategy_package.backtest_contract import build_backtest_runtime_contract
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.package_asset_freeze import manifest_has_frozen_runtime_assets
from backend.services.strategy_package.runtime import _candidate_selection_artifact_runtime_hashes
from backend.services.trading_core.errors import DataUnavailableError, TradingCoreError

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
        legacy_non_st_pit = False

        checks.append(self._latest_artifact_check(manifest))
        requested_artifact: dict[str, Any] | None = None
        if trade_date is not None and data_source:
            requested_artifact = self._requested_artifact_check(manifest, trade_date, data_source, config)
            checks.append(requested_artifact)
        if requested_artifact and requested_artifact.get("status") == PASS and requested_artifact.get("context", {}).get("artifact_id"):
            checks.append(
                {
                    "name": "source_resolves",
                    "status": PASS,
                    "message": "frozen authoritative selection artifact exists; live QE source resolution is not required for this daily run",
                    "context": {
                        "artifact_id": requested_artifact["context"].get("artifact_id"),
                        "runtime_config_hash": requested_artifact["context"].get("runtime_config_hash"),
                        "asset_authority": "frozen_selection_score_artifact",
                    },
                }
            )
        elif self._auto_generate(config) or self._has_package_owned_runtime_assets(manifest):
            checks.append(self._source_resolution_check(record, runtime_config=config))
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
        extra_checks: list[dict[str, Any]] = []
        st_pit_authoritative = bool(config.get("st_pit_authoritative") or config.get("enforce_st_pit_contract"))
        if st_pit_authoritative:
            extra_checks.append(self._st_pit_runtime_profile_check(manifest, config))

        all_checks = [*checks, *extra_checks]
        runtime_blocked = any(item["status"] == BLOCKED for item in all_checks)
        if legacy_non_st_pit:
            status = "LEGACY_NON_ST_PIT"
        elif runtime_blocked:
            status = "BLOCKED"
        elif any(item["status"] == WARN for item in all_checks):
            status = "WARN"
        else:
            status = "RUNNABLE"

        return {
            "status": status,
            "runnable": not runtime_blocked,
            "diagnostic_only": True,
            "st_pit_contract_status": st_pit_contract_status,
            "legacy_non_st_pit": legacy_non_st_pit,
            "checks": all_checks,
        }

    def require_runnable(
        self,
        record: Any,
        *,
        runtime_config: dict[str, Any],
        trade_date: date,
        data_source: str,
    ) -> dict[str, Any]:
        """Compatibility wrapper: return diagnostics, never gate package use."""

        return self.summarize(
            record,
            runtime_config=runtime_config,
            trade_date=trade_date,
            data_source=data_source,
        )

    @staticmethod
    def _blocked_check(name: str, message: str, context: dict[str, Any]) -> dict[str, Any]:
        return {"name": name, "status": WARN, "severity": "runtime_warning", "message": message, "context": context}

    @staticmethod
    def _auto_generate(runtime_config: dict[str, Any]) -> bool:
        artifact_config = runtime_config.get("selection_artifact_config")
        if artifact_config is None:
            artifact_config = runtime_config.get("selection_artifact")
        return isinstance(artifact_config, dict) and bool(artifact_config.get("auto_generate"))

    @staticmethod
    def _has_package_owned_runtime_assets(manifest: Any) -> bool:
        try:
            return manifest_has_frozen_runtime_assets(manifest)
        except Exception:
            return False

    @staticmethod
    def _contract_check(manifest: Any) -> dict[str, Any]:
        try:
            contract = build_backtest_runtime_contract(manifest)
            contract["runtime_features"]["risk_policy"]
        except TradingCoreError as exc:
            return {
                "name": "st_pit_contract_status",
                "status": WARN,
                "severity": "runtime_warning",
                "message": exc.message,
                "context": exc.context,
            }
        except Exception as exc:
            return {
                "name": "st_pit_contract_status",
                "status": WARN,
                "severity": "runtime_warning",
                "message": str(exc),
                "context": {"package_id": manifest.package_id},
            }

        return {
            "name": "strategy_runtime_contract_status",
            "status": PASS,
            "message": "StrategyPackage contract is limited to strategy semantics; platform ST PIT/HMM checks use runtime profile",
            "context": {
                "package_id": manifest.package_id,
                "legacy_non_st_pit": False,
                "platform_features": contract.get("runtime_features", {}),
                "contract": contract,
            },
        }

    @staticmethod
    def _st_pit_runtime_profile_check(manifest: Any, runtime_config: dict[str, Any]) -> dict[str, Any]:
        try:
            profile = parse_selection_runtime_profile(runtime_config)
        except TradingCoreError as exc:
            return {"name": "st_pit_runtime_profile", "status": WARN, "severity": "runtime_warning", "message": exc.message, "context": exc.context}
        if not profile.risk_policy.enabled:
            return {
                "name": "st_pit_runtime_profile",
                "status": WARN,
                "severity": "runtime_warning",
                "message": "ST PIT authoritative selection requires runtime_profile.risk_policy.enabled=true",
                "context": {"package_id": manifest.package_id},
            }
        providers = set(profile.risk_policy.providers or [])
        hard_actions = set(profile.risk_policy.hard_actions or [])
        missing = []
        if "st_pit" not in providers:
            missing.append("providers.st_pit")
        for action in ("block_buy", "force_exit"):
            if action not in hard_actions:
                missing.append(f"hard_actions.{action}")
        if missing:
            return {
                "name": "st_pit_runtime_profile",
                "status": WARN,
                "severity": "runtime_warning",
                "message": "ST PIT runtime risk policy profile is incomplete",
                "context": {"package_id": manifest.package_id, "missing": missing},
            }
        return {
            "name": "st_pit_runtime_profile",
            "status": PASS,
            "message": "platform ST PIT runtime profile is enabled",
            "context": {
                "package_id": manifest.package_id,
                "policy_version": profile.risk_policy.policy_version,
                "providers": profile.risk_policy.providers,
                "hard_actions": profile.risk_policy.hard_actions,
            },
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
            return {"name": "runtime_assets_ready", "status": WARN, "severity": "runtime_warning", "message": exc.message, "context": exc.context}
        if not artifacts:
            return {
                "name": "runtime_assets_ready",
                "status": WARN,
                "severity": "runtime_warning",
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
        if self.artifact_repository is None or not hasattr(self.artifact_repository, "get"):
            if self._auto_generate(runtime_config):
                return {
                    "name": "requested_selection_artifact",
                    "status": PASS,
                    "message": "live artifact auto-generation is enabled for this request",
                    "context": {"trade_date": trade_date.isoformat(), "data_source": data_source},
                }
            return {
                "name": "requested_selection_artifact",
                "status": UNKNOWN,
                "message": "selection artifact repository is not available to the health service",
                "context": {"trade_date": trade_date.isoformat(), "data_source": data_source},
            }
        artifact = None
        last_error: DataUnavailableError | None = None
        for runtime_hash in _candidate_selection_artifact_runtime_hashes(runtime_config):
            try:
                artifact = self.artifact_repository.get(
                    package_id=manifest.package_id,
                    manifest_sha256=manifest.manifest_sha256,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config_hash=runtime_hash,
                )
                break
            except DataUnavailableError as exc:
                last_error = exc
        if artifact is None:
            if self._auto_generate(runtime_config):
                context = dict(last_error.context if last_error else {})
                context["auto_generate"] = True
                return {
                    "name": "requested_selection_artifact",
                    "status": PASS,
                    "message": "requested artifact is missing; live artifact auto-generation is enabled",
                    "context": context,
                }
            message = last_error.message if last_error else "requested selection artifact does not exist"
            context = last_error.context if last_error else {}
            return {"name": "requested_selection_artifact", "status": WARN, "severity": "runtime_warning", "message": message, "context": context}
        metadata = artifact.metadata or {}
        if metadata.get("source_type") != AUTHORITATIVE_SELECTION_SOURCE_TYPE:
            return {
                "name": "requested_selection_artifact",
                "status": WARN,
                "severity": "runtime_warning",
                "message": "requested selection artifact is not authoritative live inference",
                "context": {"artifact_id": artifact.artifact_id, "source_type": metadata.get("source_type")},
            }
        return {
            "name": "requested_selection_artifact",
            "status": PASS,
            "message": "requested authoritative selection artifact exists",
            "context": {
                "artifact_id": artifact.artifact_id,
                "artifact_sha256": artifact.artifact_sha256,
                "score_count": artifact.score_count,
                "source_type": metadata.get("source_type"),
                "authority_scope": metadata.get("authority_scope"),
                "runtime_config_hash": artifact.runtime_config_hash,
                "trade_date": artifact.trade_date.isoformat(),
                "data_source": artifact.data_source,
            },
        }

    def _source_resolution_check(self, record: Any, *, runtime_config: dict[str, Any] | None = None) -> dict[str, Any]:
        if self.runtime_source_resolver is None:
            return {
                "name": "source_resolves",
                "status": UNKNOWN,
                "message": "runtime source resolver is not available to the health service",
                "context": {"package_id": record.package_id},
            }
        try:
            preflight = getattr(self.runtime_source_resolver, "preflight_for_strategy_package", None)
            if callable(preflight):
                result = preflight(
                    source_type=record.source_type,
                    source_id=record.source_id,
                    loop_id=record.loop_id,
                    run_id=record.run_id,
                    runtime_config=runtime_config or {},
                    manifest=record.current_manifest(),
                    package_id=record.package_id,
                )
                if not result.passed:
                    blocked = result.blocked_check
                    context: dict[str, Any] = {
                        "package_id": record.package_id,
                        "source_type": record.source_type,
                        "source_id": record.source_id,
                        "loop_id": record.loop_id,
                        "run_id": record.run_id,
                        "preflight": result.to_dict(),
                    }
                    if blocked is not None:
                        context["blocked_check"] = blocked.name
                        context.update(blocked.context or {})
                    return {
                        "name": "live_inference_preflight",
                        "status": BLOCKED,
                        "severity": "runtime_blocker",
                        "message": blocked.message if blocked is not None else "live inference preflight failed",
                        "context": context,
                    }
                return {
                    "name": "live_inference_preflight",
                    "status": PASS,
                    "message": "StrategyPackage live inference preflight passed",
                    "context": {
                        "package_id": record.package_id,
                        "source_type": record.source_type,
                        "source_id": record.source_id,
                        "loop_id": record.loop_id,
                        "preflight": result.to_dict(),
                    },
                }

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
            return {"name": "source_resolves", "status": WARN, "severity": "runtime_warning", "message": exc.message, "context": exc.context}
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
            return {"name": "hmm_artifact_status", "status": WARN, "severity": "runtime_warning", "message": exc.message, "context": exc.context}
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
                "status": WARN,
                "severity": "runtime_warning",
                "message": "HMM artifact health requires trade_date",
                "context": {"package_id": manifest.package_id},
            }
        if self.hmm_runtime is None or not hasattr(self.hmm_runtime, "preflight_coefficients"):
            return {
                "name": "hmm_artifact_status",
                "status": WARN,
                "severity": "runtime_warning",
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
            return {"name": "hmm_artifact_status", "status": WARN, "severity": "runtime_warning", "message": exc.message, "context": exc.context}
        except Exception as exc:
            return {
                "name": "hmm_artifact_status",
                "status": WARN,
                "severity": "runtime_warning",
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
