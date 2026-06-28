"""Paper Trading v2 portfolio lifecycle service."""

from __future__ import annotations

import os
from datetime import date
from math import sqrt
from pathlib import Path
from statistics import mean, stdev
from typing import Any

from backend.execution_algos.vnpy_style import VNPY_STYLE_ASSETS, get_vnpy_style_asset, is_vnpy_style_algo
from backend.services.strategy_package.execution_policy import (
    ValidatedExecutionPolicy,
    compute_execution_policy_sha256,
    normalize_execution_policy_json,
)
from backend.services.strategy_package.asset_eligibility import StrategyPackageAssetEligibilityService
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
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    LiveApprovalRequiredError,
    PackageAssetInvalidError,
    RuntimeConfigInvalidError,
    TradingCoreError,
)
from backend.services.trading_core.ledger import FeeModel
from backend.services.selection_center.runtime_profile import (
    attach_activation_runtime_profile_binding,
    attach_default_runtime_profile_binding,
    normalize_selection_runtime_config,
    validate_runtime_profile_binding,
)
from backend.services.strategy_package.backtest_contract import (
    normalize_runtime_config_with_backtest_contract,
)

from .market_data import (
    MinuteDataSource,
    assert_broker_market_source_match,
)
from .models import (
    BrokerAccountBindingStatus,
    BrokerBackendId,
    ConfigChangeType,
    PaperBrokerAccountBinding,
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
from .auto_run import (
    MINIQMT_ACCOUNT_GROUP_BINDING_MODE,
    auto_run_live_source_for_broker,
    compute_auto_run_config_sha256,
    miniqmt_account_group_id,
    miniqmt_strategy_slot_id,
    normalize_auto_run_config,
)

# Allowed broker_backend values at the Paper v2 portfolio creation API.
# minqmt_live is intentionally excluded - live admission goes through main
# design section 11 flow, not the Paper v2 router.
PAPER_V2_CREATABLE_BROKER_BACKENDS: frozenset[str] = frozenset({"local_sim", "minqmt_sim"})
VNPY_STYLE_TEMPLATE_POLICY_PREFIX = "vnpy_asset:"


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
        asset_eligibility_service: StrategyPackageAssetEligibilityService | Any | None = None,
    ) -> None:
        self.package_repository = package_repository or StrategyPackageRepository()
        self.repository = repository or PaperTradingV2Repository()
        self.validator = validator or StrategyPackageValidator()
        self.runtime_release_service = runtime_release_service or StrategyRuntimeReleaseService()
        self.asset_eligibility_service = asset_eligibility_service or StrategyPackageAssetEligibilityService(
            validator=self.validator
        )

    def _assert_minqmt_account_accepts_group_slot(
        self,
        *,
        broker_account_id: str,
        broker_mode: str,
        candidate_portfolio_id: str | None,
        account_group_id: str | None = None,
        strategy_slot_id: str | None = None,
    ) -> None:
        active_bindings = self.repository.list_active_broker_account_bindings_for_account(
            broker_backend="minqmt_sim",
            broker_mode=broker_mode,
            broker_account_id=broker_account_id,
        )
        for binding in active_bindings:
            if candidate_portfolio_id and binding.portfolio_id == candidate_portfolio_id:
                continue
            if binding.allocation_mode != MINIQMT_ACCOUNT_GROUP_BINDING_MODE:
                raise InvalidStateTransitionError(
                    "MiniQMT account has a legacy exclusive Paper v2 auto-run binding",
                    context={
                        "broker_backend": "minqmt_sim",
                        "broker_mode": broker_mode,
                        "broker_account_id": broker_account_id,
                        "existing_portfolio_id": binding.portfolio_id,
                        "existing_allocation_mode": binding.allocation_mode,
                        "candidate_portfolio_id": candidate_portfolio_id,
                    },
                )
            if not binding.account_group_id or not binding.strategy_slot_id:
                raise InvalidStateTransitionError(
                    "MiniQMT account group binding is missing strategy slot attribution",
                    context={
                        "broker_backend": "minqmt_sim",
                        "broker_mode": broker_mode,
                        "broker_account_id": broker_account_id,
                        "existing_portfolio_id": binding.portfolio_id,
                        "candidate_portfolio_id": candidate_portfolio_id,
                    },
                )
            if not candidate_portfolio_id:
                continue
            if binding.account_group_id == account_group_id and binding.strategy_slot_id == strategy_slot_id:
                raise InvalidStateTransitionError(
                    "MiniQMT account group already has this Paper v2 strategy slot active",
                    context={
                        "broker_backend": "minqmt_sim",
                        "broker_mode": broker_mode,
                        "broker_account_id": broker_account_id,
                        "account_group_id": account_group_id,
                        "strategy_slot_id": strategy_slot_id,
                        "existing_portfolio_id": binding.portfolio_id,
                        "candidate_portfolio_id": candidate_portfolio_id,
                    },
                )

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
        # R-Q9 D1/D3: validate broker_backend up-front (typed error, fail-fast).
        # Engine section 3.6.4 strong binding is re-checked inside PaperPortfolio model
        # validator; this layer additionally restricts to Paper-v2-creatable
        # backends (minqmt_live is admission-flow only, see main design section 11).
        if broker_backend not in PAPER_V2_CREATABLE_BROKER_BACKENDS:
            raise RuntimeConfigInvalidError(
                "broker_backend not allowed for paper v2 portfolio creation",
                context={
                    "broker_backend": broker_backend,
                    "allowed": sorted(PAPER_V2_CREATABLE_BROKER_BACKENDS),
                },
            )
        self.asset_eligibility_service.require_eligible(record, broker_backend=broker_backend)
        if not manifest.manifest_sha256:
            raise PackageAssetInvalidError(
                "paper portfolio requires frozen strategy package manifest",
                context={"package_id": package_id},
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

    def list_portfolios_page(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        statuses: list[str] | None = None,
        search: str | None = None,
        sort_by: str = "created_at",
        sort_dir: str = "desc",
    ) -> dict[str, Any]:
        if page <= 0 or page_size <= 0:
            raise DataUnavailableError(
                "paper v2 portfolio pagination requires positive limits",
                context={"page": page, "page_size": page_size},
            )
        rows = self.repository.list_portfolios(limit=10_000)
        status_filter = {str(item).strip().upper() for item in (statuses or []) if str(item).strip()}
        if status_filter:
            rows = [item for item in rows if item.status.value.upper() in status_filter]
        if search and search.strip():
            needle = search.strip().lower()
            rows = [
                item
                for item in rows
                if needle in item.portfolio_name.lower()
                or needle in item.portfolio_id.lower()
                or needle in item.package_id.lower()
            ]
        sort_keys = {
            "portfolio_name": lambda item: item.portfolio_name.lower(),
            "status": lambda item: item.status.value,
            "initial_cash": lambda item: item.initial_cash,
            "start_date": lambda item: item.start_date.isoformat(),
            "created_at": lambda item: item.created_at,
            "updated_at": lambda item: item.updated_at,
        }
        rows = sorted(rows, key=sort_keys.get(sort_by, sort_keys["created_at"]), reverse=str(sort_dir).lower() != "asc")
        total = len(rows)
        start = (page - 1) * page_size
        page_rows = rows[start : start + page_size]
        return {
            "portfolios": [item.model_dump(mode="json") for item in page_rows],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": max(1, (total + page_size - 1) // page_size),
                "statuses": sorted(status_filter),
                "search": search,
                "sort_by": sort_by,
                "sort_dir": "asc" if str(sort_dir).lower() == "asc" else "desc",
            },
        }

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

    def create_minqmt_auto_run_portfolio(
        self,
        *,
        package_id: str,
        portfolio_name: str,
        initial_cash: float,
        start_date: date,
        broker_account_id: str,
        top_k: int | None = None,
        hmm: dict[str, Any] | None = None,
        industry_blacklist: list[str] | None = None,
        fee_policy: dict[str, Any] | None = None,
        risk_policy: dict[str, Any] | None = None,
        execution_policy: dict[str, Any] | None = None,
        trade_window_policy: dict[str, Any] | None = None,
        auto_run_config: dict[str, Any] | None = None,
        created_by: str | None = None,
        create_session: bool = True,
    ) -> dict[str, Any]:
        account_id = str(broker_account_id or "").strip()
        if not account_id:
            raise RuntimeConfigInvalidError(
                "MiniQMT auto-run portfolio requires broker_account_id",
                context={"package_id": package_id, "broker_backend": "minqmt_sim"},
            )
        self._assert_minqmt_account_accepts_group_slot(
            broker_account_id=account_id,
            broker_mode="SIM",
            candidate_portfolio_id=None,
        )
        config = normalize_auto_run_config(
            auto_run_config,
            package_id=package_id,
            broker_account_id=account_id,
            broker_backend="minqmt_sim",
            broker_mode="SIM",
            initial_cash=initial_cash,
            top_k=top_k,
            hmm=hmm,
            industry_blacklist=industry_blacklist,
            fee_policy=fee_policy,
            trade_window_policy=trade_window_policy,
        )
        portfolio = self.create_portfolio(
            package_id=package_id,
            portfolio_name=portfolio_name,
            initial_cash=initial_cash,
            start_date=start_date,
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            broker_backend="minqmt_sim",
            fee_policy=fee_policy,
            risk_policy=risk_policy,
            execution_policy=execution_policy,
        )
        account_group_id = miniqmt_account_group_id(account_id)
        strategy_slot_id = miniqmt_strategy_slot_id(portfolio.portfolio_id)
        config["broker"]["account_group_id"] = account_group_id
        config["broker"]["strategy_slot_id"] = strategy_slot_id
        config_sha256 = compute_auto_run_config_sha256(config)
        binding = self.repository.create_broker_account_binding(
            PaperBrokerAccountBinding(
                broker_backend="minqmt_sim",
                broker_mode="SIM",
                broker_account_id=account_id,
                account_group_id=account_group_id,
                strategy_slot_id=strategy_slot_id,
                portfolio_id=portfolio.portfolio_id,
                binding_status=BrokerAccountBindingStatus.ACTIVE,
                allocation_mode=MINIQMT_ACCOUNT_GROUP_BINDING_MODE,
                initial_cash=initial_cash,
                created_by=created_by,
            )
        )
        portfolio = self.repository.update_portfolio_auto_run(
            portfolio.portfolio_id,
            enabled=True,
            config=config,
            config_sha256=config_sha256,
            updated_by=created_by,
        )
        session = None
        if create_session:
            from .session import PaperTradingSessionService
            from .models import PaperSessionMode

            session_config = dict(config)
            session_config["auto_run_config"] = dict(config)
            session_config.setdefault("paper_v2_session", {})["manual_tick_only"] = False
            session = PaperTradingSessionService(
                repository=self.repository,
                package_repository=self.package_repository,
            ).create_session(
                portfolio_id=portfolio.portfolio_id,
                mode=PaperSessionMode.LIVE_ONLY,
                start_date=start_date,
                live_data_source=MinuteDataSource.MINIQMT_REALTIME,
                runtime_config=session_config,
                created_by=created_by or "auto_run_create",
            )
        return {
            "portfolio": portfolio,
            "binding": binding,
            "session": session,
            "auto_run": {
                "enabled": True,
                "config_sha256": config_sha256,
                "config": config,
                "next_plan": self._auto_run_next_plan(config, start_date=start_date),
            },
        }

    def enable_auto_run(
        self,
        portfolio_id: str,
        *,
        broker_account_id: str,
        config: dict[str, Any] | None = None,
        updated_by: str | None = None,
        create_session: bool = True,
    ) -> dict[str, Any]:
        portfolio = self.repository.get_portfolio(portfolio_id)
        broker_backend: BrokerBackendId = portfolio.broker_backend
        account_id = str(broker_account_id or "").strip()
        if not account_id:
            account_id = f"{broker_backend}:{portfolio_id}"
        normalized = normalize_auto_run_config(
            config or portfolio.auto_run_config,
            package_id=portfolio.package_id,
            broker_account_id=account_id,
            broker_backend=broker_backend,
            initial_cash=portfolio.initial_cash,
        )
        broker_mode = str((normalized.get("broker") or {}).get("broker_mode") or "SIM").upper()
        account_group_id = miniqmt_account_group_id(account_id) if broker_backend == "minqmt_sim" else None
        strategy_slot_id = miniqmt_strategy_slot_id(portfolio_id) if broker_backend == "minqmt_sim" else None
        if broker_backend == "minqmt_sim":
            normalized["broker"]["account_group_id"] = account_group_id
            normalized["broker"]["strategy_slot_id"] = strategy_slot_id
        active_portfolio_bindings = self.repository.list_active_broker_account_bindings(portfolio_id)
        existing = next(
            (
                item
                for item in active_portfolio_bindings
                if item.broker_backend == broker_backend
                and item.broker_mode == broker_mode
                and item.broker_account_id == account_id
            ),
            None,
        )
        existing = existing or self.repository.get_active_broker_account_binding(
            broker_backend=broker_backend,
            broker_mode=broker_mode,
            broker_account_id=account_id,
            account_group_id=account_group_id,
            strategy_slot_id=strategy_slot_id,
        )
        if broker_backend == "minqmt_sim":
            self._assert_minqmt_account_accepts_group_slot(
                broker_account_id=account_id,
                broker_mode=broker_mode,
                candidate_portfolio_id=portfolio_id,
                account_group_id=account_group_id,
                strategy_slot_id=strategy_slot_id,
            )
        else:
            account_existing = self.repository.get_active_broker_account_binding(
                broker_backend=broker_backend,
                broker_mode=broker_mode,
                broker_account_id=account_id,
            )
            if account_existing is not None and account_existing.portfolio_id != portfolio_id:
                raise InvalidStateTransitionError(
                    "Paper v2 auto-run account already has an active binding",
                    context={
                        "broker_backend": broker_backend,
                        "broker_mode": broker_mode,
                        "broker_account_id": account_id,
                        "existing_portfolio_id": account_existing.portfolio_id,
                        "portfolio_id": portfolio_id,
                    },
                )
        if existing is None:
            conflicting_portfolio_binding = next(iter(active_portfolio_bindings), None)
            if conflicting_portfolio_binding is not None:
                raise InvalidStateTransitionError(
                    "Paper v2 portfolio already has an active auto-run broker binding; disable auto-run before switching accounts",
                    context={
                        "portfolio_id": portfolio_id,
                        "existing_broker_backend": conflicting_portfolio_binding.broker_backend,
                        "existing_broker_mode": conflicting_portfolio_binding.broker_mode,
                        "existing_broker_account_id": conflicting_portfolio_binding.broker_account_id,
                        "broker_backend": broker_backend,
                        "broker_mode": broker_mode,
                        "broker_account_id": account_id,
                    },
                )
            allocation_mode = MINIQMT_ACCOUNT_GROUP_BINDING_MODE if broker_backend == "minqmt_sim" else "virtual_portfolio"
            binding = self.repository.create_broker_account_binding(
                PaperBrokerAccountBinding(
                    broker_backend=broker_backend,
                    broker_mode=broker_mode,
                    broker_account_id=account_id,
                    account_group_id=account_group_id,
                    strategy_slot_id=strategy_slot_id,
                    portfolio_id=portfolio_id,
                    binding_status=BrokerAccountBindingStatus.ACTIVE,
                    allocation_mode=allocation_mode,
                    initial_cash=portfolio.initial_cash,
                    created_by=updated_by,
                )
            )
        else:
            binding = existing
        config_sha256 = compute_auto_run_config_sha256(normalized)
        portfolio = self.repository.update_portfolio_auto_run(
            portfolio_id,
            enabled=True,
            config=normalized,
            config_sha256=config_sha256,
            updated_by=updated_by,
        )
        session = None
        if create_session and not self.repository.list_active_sessions(portfolio_id):
            from .session import PaperTradingSessionService
            from .models import PaperSessionMode

            session_config = dict(normalized)
            session_config["auto_run_config"] = dict(normalized)
            session = PaperTradingSessionService(
                repository=self.repository,
                package_repository=self.package_repository,
            ).create_session(
                portfolio_id=portfolio_id,
                mode=PaperSessionMode.LIVE_ONLY,
                start_date=portfolio.start_date,
                live_data_source=auto_run_live_source_for_broker(broker_backend),
                runtime_config=session_config,
                created_by=updated_by or "auto_run_enable",
            )
        return {
            "portfolio": portfolio,
            "binding": binding,
            "session": session,
            "auto_run": {
                "enabled": True,
                "config_sha256": config_sha256,
                "config": normalized,
                "next_plan": self._auto_run_next_plan(normalized, start_date=portfolio.start_date),
            },
        }

    def disable_auto_run(self, portfolio_id: str, *, updated_by: str | None = None) -> dict[str, Any]:
        portfolio = self.repository.get_portfolio(portfolio_id)
        config = dict(portfolio.auto_run_config or {})
        config["enabled"] = False
        config_sha256 = compute_auto_run_config_sha256(config)
        portfolio = self.repository.update_portfolio_auto_run(
            portfolio_id,
            enabled=False,
            config=config,
            config_sha256=config_sha256,
            updated_by=updated_by,
        )
        bindings = self.repository.list_active_broker_account_bindings(portfolio_id)
        retired = [
            self.repository.update_broker_account_binding_status(
                item.binding_id,
                BrokerAccountBindingStatus.RETIRED,
            )
            for item in bindings
        ]
        return {
            "portfolio": portfolio,
            "retired_bindings": retired,
            "auto_run": {"enabled": False, "config_sha256": config_sha256, "config": config},
        }

    def update_auto_run_config(
        self,
        portfolio_id: str,
        *,
        patch: dict[str, Any],
        updated_by: str | None = None,
    ) -> dict[str, Any]:
        portfolio = self.repository.get_portfolio(portfolio_id)
        merged = self._deep_merge(dict(portfolio.auto_run_config or {}), patch)
        normalized = normalize_auto_run_config(merged, package_id=portfolio.package_id, broker_backend=portfolio.broker_backend)
        config_sha256 = compute_auto_run_config_sha256(normalized)
        portfolio = self.repository.update_portfolio_auto_run(
            portfolio_id,
            enabled=bool(normalized.get("enabled", portfolio.auto_run_enabled)),
            config=normalized,
            config_sha256=config_sha256,
            updated_by=updated_by,
        )
        return {
            "portfolio": portfolio,
            "auto_run": {
                "enabled": portfolio.auto_run_enabled,
                "config_sha256": config_sha256,
                "config": normalized,
                "next_plan": self._auto_run_next_plan(normalized, start_date=portfolio.start_date),
            },
        }

    def auto_run_status(self, portfolio_id: str) -> dict[str, Any]:
        portfolio = self.repository.get_portfolio(portfolio_id)
        sessions = self.repository.list_active_sessions(portfolio_id)
        bindings = self.repository.list_active_broker_account_bindings(portfolio_id)
        config = normalize_auto_run_config(portfolio.auto_run_config, package_id=portfolio.package_id, broker_backend=portfolio.broker_backend)
        return {
            "portfolio_id": portfolio_id,
            "enabled": portfolio.auto_run_enabled,
            "config_sha256": portfolio.auto_run_config_sha256,
            "config": config,
            "bindings": [item.model_dump(mode="json") for item in bindings],
            "active_sessions": [item.model_dump(mode="json") for item in sessions],
            "next_plan": self._auto_run_next_plan(config, start_date=portfolio.start_date),
        }

    @staticmethod
    def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
        result = dict(base)
        for key, value in patch.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = PaperTradingV2PortfolioService._deep_merge(dict(result[key]), value)
            else:
                result[key] = value
        return result

    @staticmethod
    def _auto_run_next_plan(config: dict[str, Any], *, start_date: date) -> str:
        policy = config.get("trade_window_policy") or {}
        windows = policy.get("submit_windows") if isinstance(policy.get("submit_windows"), list) else []
        first_window = windows[0] if windows else {"start": "09:25"}
        start = str(first_window.get("start") or "09:25")
        timezone = str((config.get("calendar_policy") or {}).get("timezone") or "Asia/Shanghai")
        return f"{start_date.isoformat()} {start} {timezone}"

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

    def delete_portfolio(self, portfolio_id: str) -> dict[str, int]:
        portfolio = self.repository.get_portfolio(portfolio_id)
        blockers: list[str] = []
        if portfolio.status == PortfolioStatus.RUNNING:
            blockers.append("portfolio_status_RUNNING")
        active_sessions = self.repository.list_active_sessions(portfolio_id)
        if active_sessions:
            blockers.append("active_sessions")
        active_orders = [
            row
            for row in self.repository.list_orders(portfolio_id, limit=10_000)
            if str(row.get("status") or "").upper() in {"PENDING", "SUBMITTED", "PARTIALLY_FILLED"}
        ]
        if active_orders:
            blockers.append("unfinished_orders")
        if blockers:
            raise InvalidStateTransitionError(
                "paper v2 portfolio delete blocked by active runtime dependencies",
                context={
                    "portfolio_id": portfolio_id,
                    "blockers": blockers,
                    "active_sessions": [
                        {"session_id": item.session_id, "status": item.status.value, "mode": item.mode.value}
                        for item in active_sessions
                    ],
                    "unfinished_orders": [
                        {"order_id": row.get("order_id"), "run_id": row.get("run_id"), "status": row.get("status")}
                        for row in active_orders[:20]
                    ],
                },
            )
        return self.repository.delete_portfolio(portfolio_id)

    def bulk_lifecycle(self, portfolio_ids: list[str], action: str) -> dict[str, Any]:
        action_map = {
            "pause": self.pause_portfolio,
            "resume": self.resume_portfolio,
            "complete": self.complete_portfolio,
            "retire": self.retire_portfolio,
        }
        handler = action_map.get(str(action).lower())
        if handler is None:
            raise InvalidStateTransitionError(
                "unsupported paper v2 bulk lifecycle action",
                context={"action": action, "supported_actions": sorted(action_map)},
            )
        results: list[dict[str, Any]] = []
        for portfolio_id in portfolio_ids:
            try:
                portfolio = handler(portfolio_id)
                results.append({"portfolio_id": portfolio_id, "ok": True, "portfolio": portfolio.model_dump(mode="json")})
            except TradingCoreError as exc:
                results.append({
                    "portfolio_id": portfolio_id,
                    "ok": False,
                    "error_code": exc.error_code,
                    "message": str(exc),
                    "context": getattr(exc, "context", {}),
                })
        return {"action": action, "results": results}

    def bulk_delete_portfolios(self, portfolio_ids: list[str]) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for portfolio_id in portfolio_ids:
            try:
                results.append({"portfolio_id": portfolio_id, "ok": True, "deleted_counts": self.delete_portfolio(portfolio_id)})
            except TradingCoreError as exc:
                results.append({
                    "portfolio_id": portfolio_id,
                    "ok": False,
                    "error_code": exc.error_code,
                    "message": str(exc),
                    "context": getattr(exc, "context", {}),
                })
        return {"results": results}

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
        seen_policy_ids: set[str] = set()
        for policy in self.package_repository.list_execution_policies(portfolio.package_id):
            payload = self._paper_execution_policy_payload(policy)
            payload["is_portfolio_default"] = policy.policy_id == default_policy_id
            payload["matches_portfolio_manifest"] = policy.manifest_sha256 == portfolio.manifest_sha256
            payload["runtime_selectable"] = policy.manifest_sha256 == portfolio.manifest_sha256
            payload["runtime_diagnostics"] = [
                "execution policy is runtime configuration; platform checks happen when the run starts"
            ]
            rows.append(payload)
            seen_policy_ids.add(policy.policy_id)
        for policy in self._vnpy_style_template_policies_for_portfolio(portfolio):
            if policy.policy_id in seen_policy_ids:
                continue
            payload = self._paper_execution_policy_payload(policy)
            spec = get_vnpy_style_asset(policy.algo_code)
            payload.update(
                {
                    "is_portfolio_default": policy.policy_id == default_policy_id,
                    "matches_portfolio_manifest": policy.manifest_sha256 == portfolio.manifest_sha256,
                    "runtime_selectable": True,
                    "activation_policy_source": "vnpy_style_asset_template",
                    "source_attribution": spec.metadata()["source_attribution"],
                    "asset_version": spec.version,
                    "runtime_diagnostics": [
                        "vn.py-style MiniQMT asset template; broker quote/order/trade state is authoritative",
                        "no TWAP/default fallback is allowed when the selected asset cannot run",
                    ],
                }
            )
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
            raise InvalidStateTransitionError(
                "execution policy activation trade_date cannot be before portfolio start_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "start_date": portfolio.start_date.isoformat(),
                },
            )
        existing_run = self.repository.get_run_by_portfolio_date(portfolio_id, trade_date)
        if existing_run is not None:
            raise InvalidStateTransitionError(
                "cannot activate execution policy after a paper run exists for the trade_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "existing_run_id": existing_run.run_id,
                    "existing_status": existing_run.status.value,
                },
            )
        policy = self._resolve_activation_execution_policy(portfolio=portfolio, policy_id=policy_id)
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
                raise RuntimeConfigInvalidError(
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

        activation_context = self._execution_policy_activation_context(
            portfolio=portfolio,
            policy=policy,
            requested_policy_id=policy_id,
            replace_existing=replace_existing,
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
            context=activation_context,
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

    @staticmethod
    def _vnpy_style_policy_id(algo_code: str) -> str:
        return f"{VNPY_STYLE_TEMPLATE_POLICY_PREFIX}{str(algo_code or '').strip().upper()}"

    @staticmethod
    def _vnpy_style_algo_code_from_policy_id(policy_id: str) -> str | None:
        normalized = str(policy_id or "").strip()
        if not normalized:
            return None
        if normalized.lower().startswith(VNPY_STYLE_TEMPLATE_POLICY_PREFIX):
            return normalized.split(":", 1)[1].strip().upper() or None
        upper = normalized.upper()
        return upper if is_vnpy_style_algo(upper) else None

    def _vnpy_style_template_policies_for_portfolio(self, portfolio: PaperPortfolio) -> list[ValidatedExecutionPolicy]:
        if str(portfolio.broker_backend).strip().lower() != "minqmt_sim":
            return []
        return [self._vnpy_style_template_policy(portfolio, algo_code) for algo_code in sorted(VNPY_STYLE_ASSETS)]

    def _vnpy_style_template_policy(self, portfolio: PaperPortfolio, algo_code: str) -> ValidatedExecutionPolicy:
        spec = get_vnpy_style_asset(algo_code)
        policy_json = normalize_execution_policy_json(spec.execution_policy_json())
        digest = compute_execution_policy_sha256(policy_json)
        return ValidatedExecutionPolicy(
            policy_id=self._vnpy_style_policy_id(spec.algo_code),
            package_id=portfolio.package_id,
            manifest_sha256=portfolio.manifest_sha256,
            policy_name=f"{spec.display_name} (vn.py-style MiniQMT asset)",
            policy_json=policy_json,
            policy_sha256=digest,
            source_backtest_id=f"vnpy_style_asset:{spec.algo_code}:{spec.version}",
            source_backtest_status="BACKTEST_VALIDATED",
            paper_enabled=False,
        )

    def _resolve_activation_execution_policy(self, *, portfolio: PaperPortfolio, policy_id: str) -> ValidatedExecutionPolicy:
        vnpy_algo_code = self._vnpy_style_algo_code_from_policy_id(policy_id)
        if vnpy_algo_code:
            if str(portfolio.broker_backend).strip().lower() != "minqmt_sim":
                raise RuntimeConfigInvalidError(
                    "vn.py-style execution asset templates require a MiniQMT simulated broker portfolio",
                    context={
                        "portfolio_id": portfolio.portfolio_id,
                        "package_id": portfolio.package_id,
                        "policy_id": policy_id,
                        "broker_backend": portfolio.broker_backend,
                        "required_broker_backend": "minqmt_sim",
                    },
                )
            return self._vnpy_style_template_policy(portfolio, vnpy_algo_code)
        policy = self.package_repository.get_execution_policy(portfolio.package_id, policy_id)
        if policy.manifest_sha256 != portfolio.manifest_sha256:
            raise PackageAssetInvalidError(
                "validated execution policy manifest hash does not match paper portfolio manifest",
                context={
                    "portfolio_id": portfolio.portfolio_id,
                    "package_id": portfolio.package_id,
                    "policy_id": policy.policy_id,
                    "policy_manifest_sha256": policy.manifest_sha256,
                    "portfolio_manifest_sha256": portfolio.manifest_sha256,
                },
            )
        return policy

    def _execution_policy_activation_context(
        self,
        *,
        portfolio: PaperPortfolio,
        policy: ValidatedExecutionPolicy,
        requested_policy_id: str,
        replace_existing: bool,
    ) -> dict[str, Any]:
        context: dict[str, Any] = {
            "package_id": portfolio.package_id,
            "manifest_sha256": portfolio.manifest_sha256,
            "source_backtest_id": policy.source_backtest_id,
            "source_backtest_status": policy.source_backtest_status,
            "replace_existing": replace_existing,
            "requested_policy_id": requested_policy_id,
        }
        algo_code = str(policy.algo_code or "").strip().upper()
        if is_vnpy_style_algo(algo_code):
            spec = get_vnpy_style_asset(algo_code)
            context.update(
                {
                    "activation_policy_source": "vnpy_style_asset_template",
                    "asset_version": spec.version,
                    "broker_backend_supported": list(spec.broker_backend_supported),
                    "source_attribution": spec.metadata()["source_attribution"],
                }
            )
        return context

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
            raise RuntimeConfigInvalidError(
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
            raise InvalidStateTransitionError(
                "runtime config activation trade_date cannot be before portfolio start_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "start_date": portfolio.start_date.isoformat(),
                },
            )
        existing_run = self.repository.get_run_by_portfolio_date(portfolio_id, trade_date)
        if existing_run is not None:
            raise InvalidStateTransitionError(
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
            raise RuntimeConfigInvalidError(
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
            raise RuntimeConfigInvalidError(
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
                raise RuntimeConfigInvalidError(
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
            raise LiveApprovalRequiredError(
                "live approval candidate requires an active runtime profile activation for the trade_date",
                context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat()},
            )
        runtime_version = self.repository.get_runtime_profile_version(runtime_activation.profile_version_id)
        runtime_profile = self.repository.get_runtime_profile(runtime_version.profile_id)
        execution_activation = self.repository.get_active_execution_policy_activation(portfolio_id, trade_date)
        if execution_activation is None:
            raise LiveApprovalRequiredError(
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
            execution_policy_json=execution_activation.policy_json,
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

    @staticmethod
    def _with_platform_runtime_defaults(portfolio: PaperPortfolio, config: dict[str, Any]) -> dict[str, Any]:
        updated = dict(config)
        if portfolio.data_source != MinuteDataSource.DB_HISTORICAL:
            artifact_config = updated.get("selection_artifact_config")
            if artifact_config is None:
                artifact_config = updated.get("selection_artifact")
            if artifact_config is None:
                updated["selection_artifact_config"] = {
                    "auto_generate": True,
                    "inference_backend": "wsl",
                    "signal_data_source": MinuteDataSource.DB_HISTORICAL.value,
                }
            session_config = dict(updated.get("paper_v2_session") or {})
            session_config.setdefault("signal_data_source", MinuteDataSource.DB_HISTORICAL.value)
            updated["paper_v2_session"] = session_config
        return updated

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
        session_opts = config.get("paper_v2_session") if isinstance(config.get("paper_v2_session"), dict) else {}
        if session_opts.get("freeze_runtime_profile"):
            config = self._with_platform_runtime_defaults(portfolio, config)
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
        if config.get("runtime_profile_activation"):
            config = self._with_platform_runtime_defaults(portfolio, config)
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
        activation = self.repository.get_active_runtime_config_activation(portfolio.portfolio_id, trade_date)
        if activation is None:
            config = self._with_platform_runtime_defaults(portfolio, config)
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
        version = self.repository.get_runtime_profile_version(activation.profile_version_id)
        profile = self.repository.get_runtime_profile(version.profile_id)
        if profile.portfolio_id != portfolio.portfolio_id:
            raise RuntimeConfigInvalidError(
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
        effective = self._with_platform_runtime_defaults(portfolio, effective)
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
            raise RuntimeConfigInvalidError(
                "runtime_variant_id cannot be empty",
                context={"portfolio_id": portfolio.portfolio_id, "package_id": portfolio.package_id},
            )
        variant = self.package_repository.get_runtime_variant(portfolio.package_id, variant_id)
        if variant.validation_status != RuntimeVariantValidationStatus.VALIDATION_PASSED:
            raise RuntimeConfigInvalidError(
                "runtime variant must be validated before Paper v2 use",
                context={
                    "portfolio_id": portfolio.portfolio_id,
                    "package_id": portfolio.package_id,
                    "runtime_variant_id": variant.variant_id,
                    "validation_status": variant.validation_status.value,
                },
            )
        if variant.manifest_sha256 != portfolio.manifest_sha256:
            raise RuntimeConfigInvalidError(
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
            raise RuntimeConfigInvalidError("runtime profile config_json must be a non-empty object")
        self._reject_runtime_profile_execution_overrides(config_json)
        unknown = sorted(set(config_json).difference(RUNTIME_PROFILE_INPUT_KEYS))
        if unknown:
            raise RuntimeConfigInvalidError(
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
            raise RuntimeConfigInvalidError(
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
            raise RuntimeConfigInvalidError(
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
                raise RuntimeConfigInvalidError(
                    "paper v2 execution_policy must reference a backtest-validated policy id",
                    context={"unknown_fields": unknown, "allowed_fields": sorted(allowed_keys)},
                )
            policy_id = requested_policy.get("validated_execution_policy_id") or requested_policy.get("policy_id")
            if not policy_id:
                raise RuntimeConfigInvalidError("validated_execution_policy_id is required")
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
            raise PackageAssetInvalidError(
                "validated execution policy manifest hash does not match paper portfolio manifest",
                context={
                    "package_id": package_id,
                    "policy_id": policy.policy_id,
                    "policy_manifest_sha256": policy.manifest_sha256,
                    "portfolio_manifest_sha256": manifest_sha256,
                },
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
            raise RuntimeConfigInvalidError(
                "Paper v2 requires a platform execution policy or QE backtest execution evidence",
                context={"package_id": package_id, "manifest_sha256": manifest_sha256},
            )
        algo_code = str(execution.get("execution_algo") or "").strip().upper()
        if not algo_code:
            raise RuntimeConfigInvalidError(
                "Paper v2 requires execution_algo evidence before deriving a platform execution policy",
                context={"package_id": package_id, "manifest_sha256": manifest_sha256},
            )
        backtest_freq = str(execution.get("backtest_freq") or "").strip().lower()
        if backtest_freq in {"5min", "5m"}:
            bar_freq = "5m"
        elif backtest_freq in {"1min", "1m", "minute", ""}:
            bar_freq = "1m"
        else:
            raise RuntimeConfigInvalidError(
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
            paper_enabled=False,
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
        raise RuntimeConfigInvalidError(
            "live approval candidate target broker is not supported by simulation binding",
            context={
                "target_broker_backend": target_broker_backend,
                "allowed_target_broker_backends": ["local_sim", "minqmt_sim", "minqmt_live"],
            },
        )

    @staticmethod
    def _paper_execution_policy_payload(policy: ValidatedExecutionPolicy) -> dict[str, Any]:
        return {
            "policy_id": policy.policy_id,
            "validated_execution_policy_id": policy.policy_id,
            "policy_sha256": policy.policy_sha256,
            "policy_name": policy.policy_name,
            "policy_json": policy.policy_json,
            "algo_code": policy.algo_code,
            "source_backtest_id": policy.source_backtest_id,
            "source_backtest_status": policy.source_backtest_status,
            "validation_status": policy.validation_status.value,
        }


def _platform_model_cache_root() -> Path:
    return Path(os.getenv("AISTOCK_MODEL_CACHE_DIR") or DEFAULT_MODEL_CACHE_ROOT)
