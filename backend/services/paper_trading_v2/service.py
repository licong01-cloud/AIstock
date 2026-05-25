"""Paper Trading v2 portfolio lifecycle service."""

from __future__ import annotations

import os
from datetime import date
from math import sqrt
from pathlib import Path
from statistics import mean, stdev
from typing import Any

from backend.services.strategy_package.execution_policy import (
    ValidatedExecutionPolicy,
    compute_execution_policy_sha256,
    ensure_policy_can_enter_paper,
    normalize_execution_policy_json,
)
from backend.services.strategy_package.models import StrategyPackageLiveApproval
from backend.services.strategy_package.model_asset_resolver import DEFAULT_MODEL_CACHE_ROOT
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.runtime import apply_runtime_variant_config
from backend.services.strategy_package.runtime_variant import RuntimeVariantValidationStatus, derive_locked_core_hash
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    StrategyRuntimeReleaseService,
)
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError
from backend.services.trading_core.ledger import FeeModel
from backend.services.selection_center.runtime_profile import (
    attach_activation_runtime_profile_binding,
    attach_default_runtime_profile_binding,
    ensure_runtime_config_version_boundary,
    normalize_selection_runtime_config,
    runtime_behavior_keys,
    validate_runtime_profile_binding,
)
from backend.services.strategy_package.backtest_contract import (
    normalize_runtime_config_with_backtest_contract,
)

from .market_data import (
    ALLOWED_MARKET_SOURCES,
    MinuteDataSource,
    assert_broker_market_source_match,
)
from .models import (
    BrokerBackendId,
    ConfigChangeType,
    PaperConfigChangeAudit,
    PaperExecutionPolicyActivation,
    PaperPortfolio,
    PaperRuntimeConfigActivation,
    PaperRuntimeProfile,
    PaperRuntimeProfileVersion,
    PortfolioStatus,
    RuntimeConfigActivationStatus,
    RuntimeProfileStatus,
    RuntimeProfileValidationStatus,
    compute_runtime_config_sha256,
)
from .repository import PaperTradingV2Repository

# Allowed broker_backend values at the Paper v2 portfolio creation API.
# minqmt_live is intentionally excluded - live admission goes through main
# design section 11 flow, not the Paper v2 router.
PAPER_V2_CREATABLE_BROKER_BACKENDS: frozenset[str] = frozenset({"local_sim", "minqmt_sim"})


RUNTIME_PROFILE_INPUT_KEYS = {
    "runtime_profile",
    "runtime_variant_id",
    "top_k",
    "exclude_suspended",
    "industry_blacklist",
    "sector_blacklist",
    "hmm",
    "enable_sector_hmm",
    "hmm_model_snapshot_id",
    "hmm_model_version_id",
    "hmm_signal_preset",
    "hmm_coefficients_path",
    "hmm_coefficients_file",
    "risk_policy",
    "daily_strategy_id",
    "daily_strategy_params",
    "selection_artifact_config",
    "selection_artifact",
    "model",
    "metadata",
}
RUNTIME_PROFILE_VERSION_ALLOWED_KEYS = {
    "runtime_profile",
    "runtime_variant",
    "runtime_variant_id",
    "selection_artifact_config",
    "selection_artifact",
    "model",
    "metadata",
}


PORTFOLIO_STATUS_TRANSITIONS: dict[PortfolioStatus, set[PortfolioStatus]] = {
    PortfolioStatus.READY: {PortfolioStatus.DRAFT, PortfolioStatus.PAUSED, PortfolioStatus.FAILED},
    PortfolioStatus.PAUSED: {PortfolioStatus.READY},
    PortfolioStatus.COMPLETED: {PortfolioStatus.READY, PortfolioStatus.PAUSED},
    PortfolioStatus.RETIRED: {
        PortfolioStatus.DRAFT,
        PortfolioStatus.READY,
        PortfolioStatus.PAUSED,
        PortfolioStatus.FAILED,
        PortfolioStatus.COMPLETED,
    },
}


class PaperTradingV2PortfolioService:
    def __init__(
        self,
        *,
        package_repository: StrategyPackageRepository | Any | None = None,
        repository: PaperTradingV2Repository | Any | None = None,
        validator: StrategyPackageValidator | None = None,
        runtime_release_service: StrategyRuntimeReleaseService | Any | None = None,
    ) -> None:
        self.package_repository = package_repository or StrategyPackageRepository()
        self.repository = repository or PaperTradingV2Repository()
        self.validator = validator or StrategyPackageValidator()
        self.runtime_release_service = runtime_release_service or StrategyRuntimeReleaseService()

    def create_portfolio(
        self,
        *,
        package_id: str,
        portfolio_name: str,
        initial_cash: float,
        start_date: date,
        data_source: MinuteDataSource,
        broker_backend: BrokerBackendId = "local_sim",
        fee_policy: dict[str, Any] | None = None,
        risk_policy: dict[str, Any] | None = None,
        execution_policy: dict[str, Any] | None = None,
    ) -> PaperPortfolio:
        record = self.package_repository.get(package_id)
        manifest = record.current_manifest()
        self.validator.validate_for_paper_trading(manifest)
        if not manifest.manifest_sha256:
            raise StrategyPackageValidationError(
                "paper portfolio requires frozen strategy package manifest",
                context={"package_id": package_id},
            )
        # R-Q9 D1/D3: validate broker_backend up-front (typed error, fail-fast).
        # Engine section 3.6.4 strong binding is re-checked inside PaperPortfolio model
        # validator; this layer additionally restricts to Paper-v2-creatable
        # backends (minqmt_live is admission-flow only, see main design section 11).
        if broker_backend not in PAPER_V2_CREATABLE_BROKER_BACKENDS:
            raise StrategyPackageValidationError(
                "broker_backend not allowed for paper v2 portfolio creation",
                context={
                    "broker_backend": broker_backend,
                    "allowed": sorted(PAPER_V2_CREATABLE_BROKER_BACKENDS),
                },
            )
        assert_broker_market_source_match(broker_backend, data_source)
        # OPEN-EXT-3 stub - broker_compatibility manifest field not yet
        # implemented in Codex schema. Once available, this gate will block
        # portfolios whose backend is incompatible with the package.
        self._validate_broker_compatibility(manifest=manifest, broker_backend=broker_backend)
        manifest_execution_policy = (
            manifest.minute_execution_policy.model_dump(mode="json")
            if manifest.minute_execution_policy is not None
            else None
        )
        validated_policy = self._resolve_validated_execution_policy(
            package_id=package_id,
            manifest_sha256=manifest.manifest_sha256,
            manifest_execution_policy=manifest_execution_policy,
            manifest=manifest,
            requested_policy=execution_policy,
        )
        portfolio = PaperPortfolio(
            portfolio_name=portfolio_name,
            package_id=package_id,
            manifest_sha256=manifest.manifest_sha256,
            frozen_manifest=manifest,
            initial_cash=initial_cash,
            start_date=start_date,
            data_source=data_source,
            broker_backend=broker_backend,
            fee_policy=fee_policy or self._default_fee_policy(),
            risk_policy=risk_policy or {},
            execution_policy=self._paper_execution_policy_payload(validated_policy),
            status=PortfolioStatus.READY,
        )
        saved = self.repository.create_portfolio(portfolio)
        if hasattr(self.package_repository, "mark_paper_portfolio_created"):
            self.package_repository.mark_paper_portfolio_created(package_id, saved.portfolio_id)
        return saved

    @staticmethod
    def _validate_broker_compatibility(*, manifest: Any, broker_backend: BrokerBackendId) -> None:
        """Stub for OPEN-EXT-3 broker_compatibility manifest gate.

        Strategy Engine design section 3.6.5 (R-Q9 D4) defines a manifest field
        ``broker_compatible`` Literal["LocalSim_only", "MiniQMTSim_only", "both"].
        The schema upgrade is part of the Codex main design double-PR flow
        (OPEN-EXT-3) and is NOT yet landed; treating absence as "both" today
        would silently grant cross-broker compatibility. This stub is wired but
        currently a no-op so the call site is in place for the follow-up PR.

        Once OPEN-EXT-3 ships, replace the body with the real check:
            spec = getattr(manifest, "broker_compatible", "LocalSim_only")
            allowed = COMPATIBILITY_MATRIX[spec]
            if broker_backend not in allowed:
                raise BrokerCompatibilityMismatchError(...)
        """

        # OPEN-EXT-3 stub - intentionally a no-op. Do not silently approve in
        # the future schema; replace with the typed check above when landed.
        return None

    def list_portfolios(self, *, limit: int = 100) -> list[PaperPortfolio]:
        return self.repository.list_portfolios(limit=limit)

    def running_summary(
        self,
        *,
        limit: int = 100,
        snapshot_limit: int = 30,
        position_limit: int = 8,
    ) -> list[dict[str, Any]]:
        return self.repository.list_running_summaries(
            limit=limit,
            snapshot_limit=snapshot_limit,
            position_limit=position_limit,
        )

    def running_summary_page(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        snapshot_limit: int = 30,
        position_limit: int = 8,
        statuses: list[str] | None = None,
        sort_by: str = "latest_run_time",
        sort_dir: str = "desc",
        search: str | None = None,
        search_fields: list[str] | None = None,
        min_initial_cash: float | None = None,
        max_initial_cash: float | None = None,
    ) -> dict[str, Any]:
        return self.repository.list_running_summaries_page(
            page=page,
            page_size=page_size,
            snapshot_limit=snapshot_limit,
            position_limit=position_limit,
            statuses=statuses,
            sort_by=sort_by,
            sort_dir=sort_dir,
            search=search,
            search_fields=search_fields,
            min_initial_cash=min_initial_cash,
            max_initial_cash=max_initial_cash,
        )

    def get_portfolio(self, portfolio_id: str) -> PaperPortfolio:
        return self.repository.get_portfolio(portfolio_id)

    def transition_portfolio_status(self, portfolio_id: str, to_status: PortfolioStatus) -> PaperPortfolio:
        allowed = PORTFOLIO_STATUS_TRANSITIONS.get(to_status)
        if not allowed:
            raise InvalidStateTransitionError(
                "unsupported paper v2 portfolio target status",
                context={"portfolio_id": portfolio_id, "to_status": to_status.value},
            )
        current = self.repository.get_portfolio(portfolio_id)
        if current.status not in allowed:
            raise InvalidStateTransitionError(
                "invalid paper v2 portfolio status transition",
                context={
                    "portfolio_id": portfolio_id,
                    "from_status": current.status.value,
                    "to_status": to_status.value,
                    "allowed_from": sorted(item.value for item in allowed),
                },
            )
        return self.repository.update_portfolio_status(portfolio_id, to_status)

    def pause_portfolio(self, portfolio_id: str) -> PaperPortfolio:
        return self.transition_portfolio_status(portfolio_id, PortfolioStatus.PAUSED)

    def resume_portfolio(self, portfolio_id: str) -> PaperPortfolio:
        return self.transition_portfolio_status(portfolio_id, PortfolioStatus.READY)

    def complete_portfolio(self, portfolio_id: str) -> PaperPortfolio:
        return self.transition_portfolio_status(portfolio_id, PortfolioStatus.COMPLETED)

    def retire_portfolio(self, portfolio_id: str) -> PaperPortfolio:
        return self.transition_portfolio_status(portfolio_id, PortfolioStatus.RETIRED)

    def performance_report(self, portfolio_id: str) -> dict[str, Any]:
        portfolio = self.repository.get_portfolio(portfolio_id)
        rows = self.repository.list_daily_snapshots(portfolio_id, limit=10_000)
        if not rows:
            raise DataUnavailableError(
                "paper v2 performance report requires at least one daily snapshot",
                context={"portfolio_id": portfolio_id},
            )
        rows.sort(key=lambda item: item["trade_date"])
        initial_nav = float(portfolio.initial_cash)
        peak_nav = initial_nav
        max_drawdown = 0.0
        previous_nav = initial_nav
        daily_returns: list[dict[str, Any]] = []
        for row in rows:
            nav = float(row["nav"])
            if nav <= 0:
                raise DataUnavailableError(
                    "paper v2 daily snapshot nav must be positive",
                    context={"portfolio_id": portfolio_id, "snapshot_id": row.get("snapshot_id")},
                )
            day_return = (nav / previous_nav) - 1.0
            daily_returns.append(
                {
                    "trade_date": row["trade_date"],
                    "nav": nav,
                    "daily_return": day_return,
                }
            )
            peak_nav = max(peak_nav, nav)
            drawdown = (nav / peak_nav) - 1.0
            max_drawdown = min(max_drawdown, drawdown)
            previous_nav = nav
        final_nav = float(rows[-1]["nav"])
        insufficient_data_reasons: list[str] = []
        annualized_return: float | None = None
        annualized_volatility: float | None = None
        sharpe: float | None = None
        avg_daily_return: float | None = None
        win_day_ratio: float | None = None
        return_values = [float(item["daily_return"]) for item in daily_returns]
        if return_values:
            avg_daily_return = mean(return_values)
            win_day_ratio = sum(1 for value in return_values if value > 0) / len(return_values)
        if len(rows) >= 2:
            period_days = max((rows[-1]["trade_date"] - rows[0]["trade_date"]).days, 1)
            annualized_return = (final_nav / initial_nav) ** (365.0 / period_days) - 1.0
            if len(return_values) >= 2:
                daily_vol = stdev(return_values)
                annualized_volatility = daily_vol * sqrt(252.0)
                if daily_vol > 0:
                    sharpe = (avg_daily_return or 0.0) / daily_vol * sqrt(252.0)
                else:
                    insufficient_data_reasons.append("sharpe requires non-zero daily return volatility")
            else:
                insufficient_data_reasons.append("volatility and sharpe require at least two daily returns")
        else:
            insufficient_data_reasons.append("annualized return, volatility, and sharpe require at least two daily snapshots")
        return {
            "portfolio_id": portfolio_id,
            "initial_nav": initial_nav,
            "final_nav": final_nav,
            "total_return": (final_nav / initial_nav) - 1.0,
            "max_drawdown": max_drawdown,
            "annualized_return": annualized_return,
            "annualized_volatility": annualized_volatility,
            "sharpe": sharpe,
            "avg_daily_return": avg_daily_return,
            "win_day_ratio": win_day_ratio,
            "insufficient_data_reasons": insufficient_data_reasons,
            "snapshot_count": len(rows),
            "start_date": rows[0]["trade_date"],
            "end_date": rows[-1]["trade_date"],
            "daily_returns": daily_returns,
        }

    def list_execution_policies(self, portfolio_id: str) -> list[dict[str, Any]]:
        portfolio = self.repository.get_portfolio(portfolio_id)
        default_policy_id = str(portfolio.execution_policy.get("validated_execution_policy_id") or "")
        rows: list[dict[str, Any]] = []
        for policy in self.package_repository.list_execution_policies(portfolio.package_id):
            payload = self._paper_execution_policy_payload(policy)
            payload["is_portfolio_default"] = policy.policy_id == default_policy_id
            payload["matches_portfolio_manifest"] = policy.manifest_sha256 == portfolio.manifest_sha256
            payload["can_enter_paper"] = False
            payload["paper_check_error"] = None
            try:
                if policy.manifest_sha256 != portfolio.manifest_sha256:
                    raise StrategyPackageValidationError(
                        "policy manifest hash does not match portfolio manifest",
                        context={
                            "policy_id": policy.policy_id,
                            "policy_manifest_sha256": policy.manifest_sha256,
                            "portfolio_manifest_sha256": portfolio.manifest_sha256,
                        },
                    )
                ensure_policy_can_enter_paper(policy)
                self.validator.validate_execution_policy_for_paper(
                    package_id=portfolio.package_id,
                    policy_json=policy.policy_json,
                    instantiate_runtime=False,
                    require_runtime_assets=False,
                )
                if not policy.paper_enabled:
                    raise StrategyPackageValidationError(
                        "validated execution policy is not enabled for paper trading",
                        context={"policy_id": policy.policy_id},
                    )
                payload["can_enter_paper"] = True
            except Exception as exc:  # display-only diagnostics, activation still fail-fasts
                payload["paper_check_error"] = exc.to_dict() if hasattr(exc, "to_dict") else {"message": str(exc)}
            rows.append(payload)
        return rows

    def activate_execution_policy(
        self,
        *,
        portfolio_id: str,
        trade_date: date,
        policy_id: str,
        activated_by: str | None = None,
        reason: str | None = None,
        replace_existing: bool = False,
    ) -> PaperExecutionPolicyActivation:
        portfolio = self.repository.get_portfolio(portfolio_id)
        if trade_date < portfolio.start_date:
            raise StrategyPackageValidationError(
                "execution policy activation trade_date cannot be before portfolio start_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "start_date": portfolio.start_date.isoformat(),
                },
            )
        existing_run = self.repository.get_run_by_portfolio_date(portfolio_id, trade_date)
        if existing_run is not None:
            raise StrategyPackageValidationError(
                "cannot activate execution policy after a paper run exists for the trade_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "existing_run_id": existing_run.run_id,
                    "existing_status": existing_run.status.value,
                },
            )
        existing_activation = self.repository.get_active_execution_policy_activation(portfolio_id, trade_date)
        if existing_activation is not None:
            if not replace_existing:
                raise InvalidStateTransitionError(
                    "active execution policy activation already exists for portfolio trade_date",
                    context={
                        "portfolio_id": portfolio_id,
                        "trade_date": trade_date.isoformat(),
                        "existing_activation_id": existing_activation.activation_id,
                    },
                )
            if not reason:
                raise StrategyPackageValidationError(
                    "replacing an execution policy activation requires a reason",
                    context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat()},
                )
            self.repository.supersede_execution_policy_activation(
                portfolio_id=portfolio_id,
                trade_date=trade_date,
            )
            self._save_config_audit(
                portfolio_id=portfolio_id,
                package_id=portfolio.package_id,
                object_type="execution_policy_activation",
                object_id=existing_activation.activation_id,
                change_type=ConfigChangeType.SUPERSEDE,
                before_json=existing_activation.model_dump(mode="json"),
                before_sha256=existing_activation.policy_sha256,
                reason=reason,
                created_by=activated_by,
            )

        policy = self.package_repository.get_execution_policy(portfolio.package_id, policy_id)
        if policy.manifest_sha256 != portfolio.manifest_sha256:
            raise StrategyPackageValidationError(
                "validated execution policy manifest hash does not match paper portfolio manifest",
                context={
                    "portfolio_id": portfolio_id,
                    "package_id": portfolio.package_id,
                    "policy_id": policy.policy_id,
                    "policy_manifest_sha256": policy.manifest_sha256,
                    "portfolio_manifest_sha256": portfolio.manifest_sha256,
                },
            )
        if not policy.paper_enabled:
            raise StrategyPackageValidationError(
                "validated execution policy is not enabled for paper trading",
                context={"portfolio_id": portfolio_id, "policy_id": policy.policy_id},
            )
        ensure_policy_can_enter_paper(policy)
        self.validator.validate_execution_policy_for_paper(
            package_id=portfolio.package_id,
            policy_json=policy.policy_json,
            instantiate_runtime=False,
            require_runtime_assets=False,
        )
        activation = PaperExecutionPolicyActivation(
            portfolio_id=portfolio_id,
            trade_date=trade_date,
            policy_id=policy.policy_id,
            policy_sha256=policy.policy_sha256 or "",
            policy_name=policy.policy_name,
            policy_json=policy.policy_json,
            activated_by=activated_by,
            reason=reason,
            context={
                "package_id": portfolio.package_id,
                "manifest_sha256": portfolio.manifest_sha256,
                "source_backtest_id": policy.source_backtest_id,
                "source_backtest_status": policy.source_backtest_status,
                "replace_existing": replace_existing,
            },
        )
        saved = self.repository.save_execution_policy_activation(activation)
        self._save_config_audit(
            portfolio_id=portfolio_id,
            package_id=portfolio.package_id,
            object_type="execution_policy_activation",
            object_id=saved.activation_id,
            change_type=ConfigChangeType.ACTIVATE,
            after_json=saved.model_dump(mode="json"),
            after_sha256=saved.policy_sha256,
            reason=reason,
            created_by=activated_by,
        )
        return saved

    def list_execution_policy_activations(
        self,
        portfolio_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperExecutionPolicyActivation]:
        return self.repository.list_execution_policy_activations(portfolio_id, limit=limit)

    def create_runtime_profile(
        self,
        *,
        portfolio_id: str,
        profile_name: str,
        config_json: dict[str, Any],
        created_by: str | None = None,
        reason: str | None = None,
    ) -> tuple[PaperRuntimeProfile, PaperRuntimeProfileVersion]:
        portfolio = self.repository.get_portfolio(portfolio_id)
        config = self._normalize_runtime_profile_config(config_json, manifest=portfolio.frozen_manifest)
        profile = PaperRuntimeProfile(
            portfolio_id=portfolio_id,
            package_id=portfolio.package_id,
            profile_name=profile_name,
            status=RuntimeProfileStatus.ACTIVE,
            created_by=created_by,
        )
        saved_profile = self.repository.save_runtime_profile(profile)
        version = PaperRuntimeProfileVersion(
            profile_id=saved_profile.profile_id,
            version_no=1,
            config_json=config,
            validation_status=RuntimeProfileValidationStatus.VALIDATED,
            created_by=created_by,
            reason=reason,
        )
        saved_version = self.repository.save_runtime_profile_version(version)
        saved_profile = self.repository.update_runtime_profile_current_version(
            profile_id=saved_profile.profile_id,
            current_version_id=saved_version.profile_version_id,
        )
        self._save_config_audit(
            portfolio_id=portfolio_id,
            package_id=portfolio.package_id,
            object_type="runtime_profile",
            object_id=saved_profile.profile_id,
            change_type=ConfigChangeType.CREATE,
            after_json={
                "profile": saved_profile.model_dump(mode="json"),
                "version": saved_version.model_dump(mode="json"),
            },
            after_sha256=saved_version.config_sha256,
            reason=reason,
            created_by=created_by,
        )
        return saved_profile, saved_version

    def list_runtime_profiles(
        self,
        portfolio_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperRuntimeProfile]:
        return self.repository.list_runtime_profiles(portfolio_id, limit=limit)

    def create_runtime_profile_version(
        self,
        *,
        portfolio_id: str,
        profile_id: str,
        config_json: dict[str, Any],
        created_by: str | None = None,
        reason: str | None = None,
    ) -> PaperRuntimeProfileVersion:
        portfolio = self.repository.get_portfolio(portfolio_id)
        profile = self.repository.get_runtime_profile(profile_id)
        if profile.portfolio_id != portfolio_id:
            raise StrategyPackageValidationError(
                "runtime profile does not belong to portfolio",
                context={"portfolio_id": portfolio_id, "profile_id": profile_id},
            )
        if profile.status == RuntimeProfileStatus.RETIRED:
            raise InvalidStateTransitionError(
                "retired runtime profile cannot receive new versions",
                context={"portfolio_id": portfolio_id, "profile_id": profile_id},
            )
        config = self._normalize_runtime_profile_config(config_json, manifest=portfolio.frozen_manifest)
        versions = self.repository.list_runtime_profile_versions(profile_id, limit=10_000)
        next_no = (max((item.version_no for item in versions), default=0) + 1)
        current = next((item for item in versions if item.profile_version_id == profile.current_version_id), None)
        version = PaperRuntimeProfileVersion(
            profile_id=profile_id,
            version_no=next_no,
            config_json=config,
            validation_status=RuntimeProfileValidationStatus.VALIDATED,
            created_by=created_by,
            reason=reason,
            supersedes_version_id=profile.current_version_id,
        )
        saved = self.repository.save_runtime_profile_version(version)
        self.repository.update_runtime_profile_current_version(
            profile_id=profile_id,
            current_version_id=saved.profile_version_id,
        )
        self._save_config_audit(
            portfolio_id=portfolio_id,
            package_id=portfolio.package_id,
            object_type="runtime_profile_version",
            object_id=saved.profile_version_id,
            change_type=ConfigChangeType.UPDATE,
            before_json=current.model_dump(mode="json") if current else None,
            after_json=saved.model_dump(mode="json"),
            before_sha256=current.config_sha256 if current else None,
            after_sha256=saved.config_sha256,
            reason=reason,
            created_by=created_by,
        )
        return saved

    def list_runtime_profile_versions(
        self,
        profile_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperRuntimeProfileVersion]:
        return self.repository.list_runtime_profile_versions(profile_id, limit=limit)

    def activate_runtime_config(
        self,
        *,
        portfolio_id: str,
        trade_date: date,
        profile_version_id: str,
        activated_by: str | None = None,
        reason: str | None = None,
        replace_existing: bool = False,
    ) -> PaperRuntimeConfigActivation:
        portfolio = self.repository.get_portfolio(portfolio_id)
        if trade_date < portfolio.start_date:
            raise StrategyPackageValidationError(
                "runtime config activation trade_date cannot be before portfolio start_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "start_date": portfolio.start_date.isoformat(),
                },
            )
        existing_run = self.repository.get_run_by_portfolio_date(portfolio_id, trade_date)
        if existing_run is not None:
            raise StrategyPackageValidationError(
                "cannot activate runtime config after a paper run exists for the trade_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "existing_run_id": existing_run.run_id,
                    "existing_status": existing_run.status.value,
                },
            )
        version = self.repository.get_runtime_profile_version(profile_version_id)
        profile = self.repository.get_runtime_profile(version.profile_id)
        if profile.portfolio_id != portfolio_id:
            raise StrategyPackageValidationError(
                "runtime profile version does not belong to portfolio",
                context={
                    "portfolio_id": portfolio_id,
                    "profile_id": profile.profile_id,
                    "profile_version_id": profile_version_id,
                },
            )
        if profile.status != RuntimeProfileStatus.ACTIVE:
            raise InvalidStateTransitionError(
                "only ACTIVE runtime profiles can be activated",
                context={"profile_id": profile.profile_id, "status": profile.status.value},
            )
        if version.validation_status != RuntimeProfileValidationStatus.VALIDATED:
            raise StrategyPackageValidationError(
                "runtime profile version must be validated before activation",
                context={
                    "profile_version_id": profile_version_id,
                    "validation_status": version.validation_status.value,
                    "validation_errors": version.validation_errors,
                },
            )
        normalize_runtime_config_with_backtest_contract(
            portfolio.frozen_manifest,
            version.config_json,
            context={
                "portfolio_id": portfolio_id,
                "profile_version_id": profile_version_id,
                "trade_date": trade_date.isoformat(),
                "check": "runtime_config_activation",
            },
        )
        existing = self.repository.get_active_runtime_config_activation(portfolio_id, trade_date)
        if existing is not None:
            if not replace_existing:
                raise InvalidStateTransitionError(
                    "active runtime config activation already exists for portfolio trade_date",
                    context={
                        "portfolio_id": portfolio_id,
                        "trade_date": trade_date.isoformat(),
                        "existing_activation_id": existing.activation_id,
                    },
                )
            if not reason:
                raise StrategyPackageValidationError(
                    "replacing a runtime config activation requires a reason",
                    context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat()},
                )
            self.repository.supersede_runtime_config_activation(
                portfolio_id=portfolio_id,
                trade_date=trade_date,
            )
            self._save_config_audit(
                portfolio_id=portfolio_id,
                package_id=portfolio.package_id,
                object_type="runtime_config_activation",
                object_id=existing.activation_id,
                change_type=ConfigChangeType.SUPERSEDE,
                before_json=existing.model_dump(mode="json"),
                before_sha256=version.config_sha256,
                reason=reason,
                created_by=activated_by,
            )
        activation = PaperRuntimeConfigActivation(
            portfolio_id=portfolio_id,
            trade_date=trade_date,
            profile_version_id=profile_version_id,
            status=RuntimeConfigActivationStatus.ACTIVE,
            activated_by=activated_by,
            reason=reason,
            context={
                "package_id": portfolio.package_id,
                "manifest_sha256": portfolio.manifest_sha256,
                "profile_id": profile.profile_id,
                "profile_name": profile.profile_name,
                "version_no": version.version_no,
                "config_sha256": version.config_sha256,
                "replace_existing": replace_existing,
            },
        )
        saved = self.repository.save_runtime_config_activation(activation)
        self._save_config_audit(
            portfolio_id=portfolio_id,
            package_id=portfolio.package_id,
            object_type="runtime_config_activation",
            object_id=saved.activation_id,
            change_type=ConfigChangeType.ACTIVATE,
            after_json=saved.model_dump(mode="json"),
            after_sha256=version.config_sha256,
            reason=reason,
            created_by=activated_by,
        )
        return saved

    def list_runtime_config_activations(
        self,
        portfolio_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperRuntimeConfigActivation]:
        return self.repository.list_runtime_config_activations(portfolio_id, limit=limit)

    def list_config_change_audit(
        self,
        portfolio_id: str,
        *,
        limit: int = 200,
    ) -> list[PaperConfigChangeAudit]:
        return self.repository.list_config_change_audit(portfolio_id, limit=limit)

    def create_live_approval_candidate(
        self,
        *,
        portfolio_id: str,
        trade_date: date,
        target_broker_backend: str,
        sim_validation_evidence: dict[str, Any],
        broker_compatibility: dict[str, Any],
        broker_account_id: str | None = None,
        requested_by: str | None = None,
        risk_note: str | None = None,
        rollback_plan: str | None = None,
    ) -> StrategyPackageLiveApproval:
        portfolio = self.repository.get_portfolio(portfolio_id)
        runtime_activation = self.repository.get_active_runtime_config_activation(portfolio_id, trade_date)
        if runtime_activation is None:
            raise StrategyPackageValidationError(
                "live approval candidate requires an active runtime profile activation for the trade_date",
                context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat()},
            )
        runtime_version = self.repository.get_runtime_profile_version(runtime_activation.profile_version_id)
        runtime_profile = self.repository.get_runtime_profile(runtime_version.profile_id)
        execution_activation = self.repository.get_active_execution_policy_activation(portfolio_id, trade_date)
        if execution_activation is None:
            raise StrategyPackageValidationError(
                "live approval candidate requires an active execution policy activation for the trade_date",
                context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat()},
            )
        tail_policy = self._tail_policy_snapshot(execution_activation.policy_json)
        tail_policy_sha256 = compute_runtime_config_sha256(tail_policy)
        daily_strategy_profile_version_id = self._daily_strategy_profile_version_id(runtime_version.config_json)
        runtime_release = self.runtime_release_service.create_release(
            package_id=portfolio.package_id,
            manifest_sha256=portfolio.manifest_sha256,
            runtime_profile_id=runtime_profile.profile_id,
            runtime_profile_version_id=runtime_version.profile_version_id,
            runtime_profile_sha256=runtime_version.config_sha256 or "",
            daily_strategy_profile_version_id=daily_strategy_profile_version_id,
            execution_policy_version_id=execution_activation.policy_id,
            execution_policy_sha256=execution_activation.policy_sha256,
            tail_policy_version_id=tail_policy["tail_policy_id"],
            tail_policy_sha256=tail_policy_sha256,
            validation_evidence={
                "sim_validation_evidence": sim_validation_evidence,
            },
            release_metadata={
                "source": "paper_v2_live_approval_candidate",
                "portfolio_id": portfolio_id,
                "trade_date": trade_date.isoformat(),
                "runtime_config_activation_id": runtime_activation.activation_id,
                "execution_policy_activation_id": execution_activation.activation_id,
            },
            effective_from=trade_date,
            created_by=requested_by,
            created_reason="live approval candidate runtime release",
        )
        runtime_release_payload = {
            **runtime_release.release_config_json,
            "release_id": runtime_release.release_id,
            "release_hash": runtime_release.release_hash,
        }
        self._save_config_audit(
            portfolio_id=portfolio_id,
            package_id=portfolio.package_id,
            object_type="strategy_runtime_release",
            object_id=runtime_release.release_id,
            change_type=ConfigChangeType.CREATE,
            after_json=runtime_release.model_dump(mode="json"),
            after_sha256=runtime_release.release_hash,
            reason="create immutable runtime release for live approval candidate",
            created_by=requested_by,
        )
        binding_broker_backend = self._simulation_binding_broker_backend(target_broker_backend)
        broker_binding = self.runtime_release_service.create_binding(
            strategy_id=portfolio_id,
            release=runtime_release,
            broker_backend=binding_broker_backend,
            broker_account_id=broker_account_id,
            capital_allocation=portfolio.initial_cash,
            strategy_name=portfolio.portfolio_name,
            order_remark_prefix=portfolio_id,
            binding_metadata={
                "source": "paper_v2_live_approval_candidate",
                "target_broker_backend": target_broker_backend,
                "trade_date": trade_date.isoformat(),
            },
            effective_from=trade_date,
            created_by=requested_by,
            created_reason="bind runtime release to portfolio and broker target",
        )
        binding_payload = broker_binding.model_dump(mode="json")
        compatibility = {
            "target_broker_backend": target_broker_backend,
            "simulation_binding_id": broker_binding.binding_id,
            "simulation_binding_hash": broker_binding.binding_hash,
            **dict(broker_compatibility or {}),
        }
        return StrategyPackageService(repository=self.package_repository, validator=self.validator).create_live_approval_candidate(
            package_id=portfolio.package_id,
            manifest_sha256=portfolio.manifest_sha256,
            alpha_core_sha256=derive_locked_core_hash(portfolio.frozen_manifest),
            portfolio_id=portfolio_id,
            runtime_release_id=runtime_release.release_id,
            runtime_release_sha256=runtime_release.release_hash or "",
            runtime_profile_id=runtime_profile.profile_id,
            runtime_profile_version_id=runtime_version.profile_version_id,
            runtime_profile_sha256=runtime_version.config_sha256 or "",
            execution_policy_id=execution_activation.policy_id,
            execution_policy_sha256=execution_activation.policy_sha256,
            tail_policy_id=tail_policy["tail_policy_id"],
            tail_policy_sha256=tail_policy_sha256,
            target_broker_backend=target_broker_backend,
            broker_account_id=broker_account_id,
            sim_validation_evidence=sim_validation_evidence,
            broker_compatibility=compatibility,
            requested_by=requested_by,
            risk_note=risk_note,
            rollback_plan=rollback_plan,
            audit_json={
                "source": "paper_v2_live_approval_candidate",
                "runtime_release": runtime_release_payload,
                "simulation_release_binding": binding_payload,
            },
        )

    def submit_live_approval(
        self,
        *,
        package_id: str,
        approval_id: str,
        requested_by: str,
        risk_note: str,
        rollback_plan: str,
    ) -> StrategyPackageLiveApproval:
        return StrategyPackageService(repository=self.package_repository, validator=self.validator).submit_live_approval(
            package_id=package_id,
            approval_id=approval_id,
            requested_by=requested_by,
            risk_note=risk_note,
            rollback_plan=rollback_plan,
        )

    def approve_live_approval(
        self,
        *,
        package_id: str,
        approval_id: str,
        approved_by: str,
        risk_note: str | None = None,
        rollback_plan: str | None = None,
    ) -> StrategyPackageLiveApproval:
        return StrategyPackageService(repository=self.package_repository, validator=self.validator).approve_live_approval(
            package_id=package_id,
            approval_id=approval_id,
            approved_by=approved_by,
            risk_note=risk_note,
            rollback_plan=rollback_plan,
        )

    def reject_live_approval(
        self,
        *,
        package_id: str,
        approval_id: str,
        rejected_by: str,
        rejection_reason: str,
    ) -> StrategyPackageLiveApproval:
        return StrategyPackageService(repository=self.package_repository, validator=self.validator).reject_live_approval(
            package_id=package_id,
            approval_id=approval_id,
            rejected_by=rejected_by,
            rejection_reason=rejection_reason,
        )

    def retire_live_approval(
        self,
        *,
        package_id: str,
        approval_id: str,
        retired_by: str,
        retirement_reason: str,
    ) -> StrategyPackageLiveApproval:
        return StrategyPackageService(repository=self.package_repository, validator=self.validator).retire_live_approval(
            package_id=package_id,
            approval_id=approval_id,
            retired_by=retired_by,
            retirement_reason=retirement_reason,
        )

    def list_live_approvals(
        self,
        *,
        package_id: str | None = None,
        portfolio_id: str | None = None,
        limit: int = 100,
    ) -> list[StrategyPackageLiveApproval]:
        return StrategyPackageService(repository=self.package_repository, validator=self.validator).list_live_approvals(
            package_id=package_id,
            portfolio_id=portfolio_id,
            limit=limit,
        )

    @staticmethod
    def _config_without_runtime_variant_selector(config: dict[str, Any]) -> dict[str, Any]:
        cleaned = dict(config)
        cleaned.pop("runtime_variant_id", None)
        return cleaned

    def resolve_runtime_config_for_date(
        self,
        *,
        portfolio: PaperPortfolio,
        trade_date: date,
        runtime_config: dict[str, Any] | None,
    ) -> dict[str, Any]:
        config = dict(runtime_config or {})
        runtime_variant = self._resolve_runtime_variant_for_portfolio(portfolio, config)
        config = self._config_without_runtime_variant_selector(apply_runtime_variant_config(config, runtime_variant))
        ensure_runtime_config_version_boundary(
            config,
            context={
                "portfolio_id": portfolio.portfolio_id,
                "trade_date": trade_date.isoformat(),
                "broker_backend": portfolio.broker_backend,
                "path": "paper_v2.resolve_runtime_config_for_date",
            },
        )
        if config.get("runtime_profile_activation"):
            config = attach_activation_runtime_profile_binding(
                config,
                activation=dict(config["runtime_profile_activation"]),
            )
            normalized = normalize_runtime_config_with_backtest_contract(
                portfolio.frozen_manifest,
                config,
                context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat()},
            )
            validate_runtime_profile_binding(
                normalized,
                context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat()},
            )
            return normalized
        session_opts = config.get("paper_v2_session") if isinstance(config.get("paper_v2_session"), dict) else {}
        if session_opts.get("freeze_runtime_profile"):
            normalized = normalize_runtime_config_with_backtest_contract(
                portfolio.frozen_manifest,
                config,
                context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat()},
            )
            validate_runtime_profile_binding(
                normalized,
                context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat()},
            )
            return normalized
        activation = self.repository.get_active_runtime_config_activation(portfolio.portfolio_id, trade_date)
        if activation is None:
            behavior_keys = runtime_behavior_keys(config)
            if behavior_keys:
                raise StrategyPackageValidationError(
                    "runtime_config changes trading behavior without an active runtime profile activation",
                    context={
                        "portfolio_id": portfolio.portfolio_id,
                        "trade_date": trade_date.isoformat(),
                        "broker_backend": portfolio.broker_backend,
                        "behavior_keys": behavior_keys,
                    },
                )
            normalized = normalize_runtime_config_with_backtest_contract(
                portfolio.frozen_manifest,
                config,
                context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat()},
            )
            normalized = attach_default_runtime_profile_binding(normalized)
            validate_runtime_profile_binding(
                normalized,
                context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat()},
            )
            return normalized
        conflicting = sorted(key for key in config if key in RUNTIME_PROFILE_INPUT_KEYS)
        if conflicting:
            raise StrategyPackageValidationError(
                "runtime_config conflicts with active runtime profile activation",
                context={
                    "portfolio_id": portfolio.portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "activation_id": activation.activation_id,
                    "conflicting_keys": conflicting,
                },
            )
        version = self.repository.get_runtime_profile_version(activation.profile_version_id)
        profile = self.repository.get_runtime_profile(version.profile_id)
        if profile.portfolio_id != portfolio.portfolio_id:
            raise StrategyPackageValidationError(
                "active runtime config activation references another portfolio",
                context={
                    "portfolio_id": portfolio.portfolio_id,
                    "activation_id": activation.activation_id,
                    "profile_id": profile.profile_id,
                },
            )
        effective = dict(version.config_json)
        effective.update(config)
        effective = self._config_without_runtime_variant_selector(effective)
        effective["runtime_profile_activation"] = {
            "activation_id": activation.activation_id,
            "profile_id": profile.profile_id,
            "profile_name": profile.profile_name,
            "profile_version_id": version.profile_version_id,
            "version_no": version.version_no,
            "config_sha256": version.config_sha256,
            "activated_at": activation.activated_at.isoformat(),
            "activated_by": activation.activated_by,
            "reason": activation.reason,
        }
        effective = attach_activation_runtime_profile_binding(
            effective,
            activation=effective["runtime_profile_activation"],
        )
        normalized = normalize_runtime_config_with_backtest_contract(
            portfolio.frozen_manifest,
            effective,
            context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat()},
        )
        validate_runtime_profile_binding(
            normalized,
            context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat()},
        )
        return normalized

    def _resolve_runtime_variant_for_portfolio(self, portfolio: PaperPortfolio, config: dict[str, Any]) -> Any | None:
        raw_id = config.get("runtime_variant_id")
        if raw_id is None:
            return None
        variant_id = str(raw_id).strip()
        if not variant_id:
            raise StrategyPackageValidationError(
                "runtime_variant_id cannot be empty",
                context={"portfolio_id": portfolio.portfolio_id, "package_id": portfolio.package_id},
            )
        variant = self.package_repository.get_runtime_variant(portfolio.package_id, variant_id)
        if variant.validation_status != RuntimeVariantValidationStatus.VALIDATION_PASSED or not variant.paper_candidate:
            raise StrategyPackageValidationError(
                "runtime variant must be a validated paper candidate before Paper v2 use",
                context={
                    "portfolio_id": portfolio.portfolio_id,
                    "package_id": portfolio.package_id,
                    "runtime_variant_id": variant.variant_id,
                    "validation_status": variant.validation_status.value,
                    "paper_candidate": variant.paper_candidate,
                },
            )
        if variant.manifest_sha256 != portfolio.manifest_sha256:
            raise StrategyPackageValidationError(
                "runtime variant manifest hash does not match paper portfolio manifest",
                context={
                    "portfolio_id": portfolio.portfolio_id,
                    "package_id": portfolio.package_id,
                    "runtime_variant_id": variant.variant_id,
                    "variant_manifest_sha256": variant.manifest_sha256,
                    "portfolio_manifest_sha256": portfolio.manifest_sha256,
                },
            )
        return variant

    def fee_model_from_policy(self, fee_policy: dict[str, Any]) -> FeeModel:
        return FeeModel(
            open_cost=float(fee_policy.get("open_cost", FeeModel.open_cost)),
            close_cost=float(fee_policy.get("close_cost", FeeModel.close_cost)),
            min_cost=float(fee_policy.get("min_cost", FeeModel.min_cost)),
        )

    def _normalize_runtime_profile_config(
        self,
        config_json: dict[str, Any],
        *,
        manifest: Any | None = None,
    ) -> dict[str, Any]:
        if not isinstance(config_json, dict) or not config_json:
            raise StrategyPackageValidationError("runtime profile config_json must be a non-empty object")
        self._reject_runtime_profile_execution_overrides(config_json)
        unknown = sorted(set(config_json).difference(RUNTIME_PROFILE_INPUT_KEYS))
        if unknown:
            raise StrategyPackageValidationError(
                "runtime profile config contains unsupported top-level keys",
                context={
                    "unknown_fields": unknown,
                    "allowed_fields": sorted(RUNTIME_PROFILE_INPUT_KEYS),
                },
            )
        if manifest is not None:
            normalized = normalize_runtime_config_with_backtest_contract(
                manifest,
                config_json,
                context={"package_id": getattr(manifest, "package_id", None), "check": "runtime_profile_config"},
                inherit_source_defaults=False,
            )
        else:
            normalized = normalize_selection_runtime_config(config_json)
        for legacy_key in RUNTIME_PROFILE_INPUT_KEYS.difference(RUNTIME_PROFILE_VERSION_ALLOWED_KEYS):
            normalized.pop(legacy_key, None)
        self._drop_unset_daily_strategy_defaults(normalized, config_json)
        unknown_after = sorted(set(normalized).difference(RUNTIME_PROFILE_VERSION_ALLOWED_KEYS))
        if unknown_after:
            raise StrategyPackageValidationError(
                "normalized runtime profile config contains unsupported top-level keys",
                context={
                    "unknown_fields": unknown_after,
                    "allowed_fields": sorted(RUNTIME_PROFILE_VERSION_ALLOWED_KEYS),
                },
            )
        compute_runtime_config_sha256(normalized)
        return normalized

    @staticmethod
    def _drop_unset_daily_strategy_defaults(
        normalized: dict[str, Any],
        original_config: dict[str, Any],
    ) -> None:
        """Do not persist implicit daily-strategy placeholders on profile versions."""

        profile = normalized.get("runtime_profile")
        if not isinstance(profile, dict):
            return
        selection = profile.get("selection")
        if not isinstance(selection, dict):
            return
        original_profile = original_config.get("runtime_profile")
        original_selection = (
            original_profile.get("selection")
            if isinstance(original_profile, dict) and isinstance(original_profile.get("selection"), dict)
            else {}
        )
        daily_id_explicit = (
            "daily_strategy_id" in original_config or "daily_strategy_id" in original_selection
        )
        daily_params_explicit = (
            "daily_strategy_params" in original_config or "daily_strategy_params" in original_selection
        )
        if not daily_id_explicit and selection.get("daily_strategy_id") is None:
            selection.pop("daily_strategy_id", None)
        if not daily_params_explicit and not selection.get("daily_strategy_params"):
            selection.pop("daily_strategy_params", None)

    @staticmethod
    def _reject_runtime_profile_execution_overrides(config_json: dict[str, Any]) -> None:
        forbidden = {
            "algo_code",
            "algo_config",
            "execution_policy",
            "unfilled_handler",
            "unfilled_handler_params",
            "unfilled_policy",
            "validated_execution_policy",
            "paper_v2_session",
            "paper_v2_replay",
        }
        present = sorted(key for key in forbidden if key in config_json)
        if present:
            raise StrategyPackageValidationError(
                "runtime profile cannot contain execution/session overrides",
                context={"forbidden_keys": present},
            )

    def _save_config_audit(
        self,
        *,
        portfolio_id: str | None,
        package_id: str | None,
        object_type: str,
        object_id: str,
        change_type: ConfigChangeType,
        before_json: dict[str, Any] | None = None,
        after_json: dict[str, Any] | None = None,
        before_sha256: str | None = None,
        after_sha256: str | None = None,
        reason: str | None = None,
        created_by: str | None = None,
    ) -> None:
        self.repository.save_config_change_audit(
            PaperConfigChangeAudit(
                portfolio_id=portfolio_id,
                package_id=package_id,
                object_type=object_type,
                object_id=object_id,
                change_type=change_type,
                before_json=before_json,
                after_json=after_json,
                before_sha256=before_sha256,
                after_sha256=after_sha256,
                reason=reason,
                created_by=created_by,
            )
        )

    @staticmethod
    def _default_fee_policy() -> dict[str, float]:
        return {
            "open_cost": FeeModel.open_cost,
            "close_cost": FeeModel.close_cost,
            "min_cost": FeeModel.min_cost,
        }

    def _resolve_validated_execution_policy(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        manifest_execution_policy: dict[str, Any] | None,
        manifest: Any,
        requested_policy: dict[str, Any] | None,
    ) -> ValidatedExecutionPolicy:
        if requested_policy:
            allowed_keys = {"validated_execution_policy_id", "policy_id"}
            unknown = sorted(set(requested_policy).difference(allowed_keys))
            if unknown:
                raise StrategyPackageValidationError(
                    "paper v2 execution_policy must reference a backtest-validated policy id",
                    context={"unknown_fields": unknown, "allowed_fields": sorted(allowed_keys)},
                )
            policy_id = requested_policy.get("validated_execution_policy_id") or requested_policy.get("policy_id")
            if not policy_id:
                raise StrategyPackageValidationError("validated_execution_policy_id is required")
            policy = self.package_repository.get_execution_policy(package_id, str(policy_id))
        else:
            if manifest_execution_policy is None:
                manifest_execution_policy = self._derive_platform_execution_policy_from_backtest_context(
                    package_id=package_id,
                    manifest_sha256=manifest_sha256,
                    manifest=manifest,
                )
            policy = self._select_default_manifest_execution_policy(
                package_id=package_id,
                manifest_sha256=manifest_sha256,
                manifest_execution_policy=manifest_execution_policy,
            )
        if policy.manifest_sha256 != manifest_sha256:
            raise StrategyPackageValidationError(
                "validated execution policy manifest hash does not match paper portfolio manifest",
                context={
                    "package_id": package_id,
                    "policy_id": policy.policy_id,
                    "policy_manifest_sha256": policy.manifest_sha256,
                    "portfolio_manifest_sha256": manifest_sha256,
                },
            )
        if not policy.paper_enabled:
            raise StrategyPackageValidationError(
                "validated execution policy is not enabled for paper trading",
                context={"package_id": package_id, "policy_id": policy.policy_id},
            )
        ensure_policy_can_enter_paper(policy)
        self.validator.validate_execution_policy_for_paper(
            package_id=package_id,
            policy_json=policy.policy_json,
            instantiate_runtime=False,
            require_runtime_assets=False,
        )
        return policy

    def _derive_platform_execution_policy_from_backtest_context(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        manifest: Any,
    ) -> dict[str, Any]:
        execution = (getattr(manifest, "backtest_context", None) or {}).get("execution")
        if not isinstance(execution, dict):
            raise StrategyPackageValidationError(
                "Paper v2 requires a platform execution policy or QE backtest execution evidence",
                context={"package_id": package_id, "manifest_sha256": manifest_sha256},
            )
        algo_code = str(execution.get("execution_algo") or "").strip().upper()
        if not algo_code:
            raise StrategyPackageValidationError(
                "Paper v2 requires execution_algo evidence before deriving a platform execution policy",
                context={"package_id": package_id, "manifest_sha256": manifest_sha256},
            )
        backtest_freq = str(execution.get("backtest_freq") or "").strip().lower()
        if backtest_freq in {"5min", "5m"}:
            bar_freq = "5m"
        elif backtest_freq in {"1min", "1m", "minute", ""}:
            bar_freq = "1m"
        else:
            raise StrategyPackageValidationError(
                "Paper v2 can only derive minute execution policy from minute backtest evidence",
                context={
                    "package_id": package_id,
                    "manifest_sha256": manifest_sha256,
                    "backtest_freq": execution.get("backtest_freq"),
                    "algo_code": algo_code,
                },
            )
        algo_config = dict(execution.get("execution_algo_params") or {})
        algo_config = self._complete_platform_algo_config(algo_code, algo_config)
        return {
            "execution_level": "minute",
            "bar_freq": bar_freq,
            "algo_code": algo_code,
            "algo_config": algo_config,
            "fallback_algo_code": None,
            "data_requirements": {
                "requires_minute_bar": True,
                "requires_limit_price": True,
                "requires_trade_calendar": True,
                "requires_suspend_status": True,
            },
            "fallback_policy": {"on_missing_minute_bar": "fail", "on_algo_error": "fail"},
            "quality_report": {
                "record_slippage": True,
                "record_participation_rate": True,
                "record_unfilled_reason": True,
            },
            "unfilled_handler": execution.get("unfilled_handler"),
            "unfilled_handler_params": dict(execution.get("unfilled_handler_params") or {}),
        }

    @staticmethod
    def _complete_platform_algo_config(algo_code: str, algo_config: dict[str, Any]) -> dict[str, Any]:
        config = dict(algo_config)
        if algo_code in {"V25_TWO_STAGE", "V25_1_SMALL_CAP"}:
            cache_root = _platform_model_cache_root()
            config.setdefault("early_model_path", str(cache_root / algo_code / "v25_early_net_joint_fixed.pt"))
            config.setdefault("late_model_path", str(cache_root / algo_code / "v25_late_net_joint_fixed.pt"))
        return config

    def _select_default_manifest_execution_policy(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        manifest_execution_policy: dict[str, Any] | None,
    ) -> ValidatedExecutionPolicy:
        normalized_policy = normalize_execution_policy_json(manifest_execution_policy)
        digest = compute_execution_policy_sha256(normalized_policy)
        for policy in self.package_repository.list_execution_policies(package_id):
            if policy.policy_sha256 == digest:
                return policy
        # Paper simulation may use the immutable manifest minute policy as a
        # platform runtime default. It is not a StrategyPackage governance gate;
        # algorithm/data availability still fail fast during runtime preflight.
        return ValidatedExecutionPolicy(
            policy_id=f"platform_manifest_{digest[:16]}",
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            policy_name="Platform runtime default from frozen manifest",
            policy_json=normalized_policy,
            policy_sha256=digest,
            source_backtest_id=f"strategy_package_manifest:{manifest_sha256}",
            source_backtest_status="BACKTEST_VALIDATED",
            paper_enabled=True,
        )

    @staticmethod
    def _tail_policy_snapshot(policy_json: dict[str, Any]) -> dict[str, Any]:
        algo_code = str(policy_json.get("algo_code") or "").strip().upper() or "UNKNOWN"
        return {
            "tail_policy_id": f"tail_policy:{algo_code}",
            "unfilled_handler": policy_json.get("unfilled_handler") or "default_fail_fast",
            "unfilled_handler_params": dict(policy_json.get("unfilled_handler_params") or {}),
            "fallback_policy": dict(policy_json.get("fallback_policy") or {}),
        }

    @staticmethod
    def _daily_strategy_profile_version_id(runtime_config: dict[str, Any]) -> str:
        profile = runtime_config.get("runtime_profile") or {}
        selection = profile.get("selection") if isinstance(profile, dict) else {}
        daily_strategy_id = selection.get("daily_strategy_id") if isinstance(selection, dict) else None
        return str(daily_strategy_id or DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID)

    @staticmethod
    def _simulation_binding_broker_backend(target_broker_backend: str) -> str:
        normalized = str(target_broker_backend or "").strip().lower()
        if normalized == "minqmt_live":
            return "minqmt_sim"
        if normalized in {"local_sim", "minqmt_sim"}:
            return normalized
        raise StrategyPackageValidationError(
            "live approval candidate target broker is not supported by simulation binding",
            context={
                "target_broker_backend": target_broker_backend,
                "allowed_target_broker_backends": ["local_sim", "minqmt_sim", "minqmt_live"],
            },
        )

    @staticmethod
    def _paper_execution_policy_payload(policy: ValidatedExecutionPolicy) -> dict[str, Any]:
        return {
            "validated_execution_policy_id": policy.policy_id,
            "policy_sha256": policy.policy_sha256,
            "policy_name": policy.policy_name,
            "policy_json": policy.policy_json,
            "algo_code": policy.algo_code,
            "source_backtest_id": policy.source_backtest_id,
            "source_backtest_status": policy.source_backtest_status,
            "validation_status": policy.validation_status.value,
            "paper_enabled": policy.paper_enabled,
        }


def _platform_model_cache_root() -> Path:
    return Path(os.getenv("AISTOCK_MODEL_CACHE_DIR") or DEFAULT_MODEL_CACHE_ROOT)
