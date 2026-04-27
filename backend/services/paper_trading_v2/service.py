"""Paper Trading v2 portfolio lifecycle service."""

from __future__ import annotations

from datetime import date
from math import sqrt
from statistics import mean, stdev
from typing import Any

from backend.services.strategy_package.execution_policy import (
    ValidatedExecutionPolicy,
    compute_execution_policy_sha256,
    ensure_policy_can_enter_paper,
    normalize_execution_policy_json,
)
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError
from backend.services.trading_core.ledger import FeeModel

from .market_data import MinuteDataSource
from .models import PaperExecutionPolicyActivation, PaperPortfolio, PortfolioStatus
from .repository import PaperTradingV2Repository


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
    ) -> None:
        self.package_repository = package_repository or StrategyPackageRepository()
        self.repository = repository or PaperTradingV2Repository()
        self.validator = validator or StrategyPackageValidator()

    def create_portfolio(
        self,
        *,
        package_id: str,
        portfolio_name: str,
        initial_cash: float,
        start_date: date,
        data_source: MinuteDataSource,
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
        validated_policy = self._resolve_validated_execution_policy(
            package_id=package_id,
            manifest_sha256=manifest.manifest_sha256,
            manifest_execution_policy=manifest.minute_execution_policy.model_dump(mode="json"),
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
            fee_policy=fee_policy or self._default_fee_policy(),
            risk_policy=risk_policy or manifest.risk_policy.model_dump(mode="json"),
            execution_policy=self._paper_execution_policy_payload(validated_policy),
            status=PortfolioStatus.READY,
        )
        saved = self.repository.create_portfolio(portfolio)
        if hasattr(self.package_repository, "mark_paper_portfolio_created"):
            self.package_repository.mark_paper_portfolio_created(package_id, saved.portfolio_id)
        return saved

    def list_portfolios(self, *, limit: int = 100) -> list[PaperPortfolio]:
        return self.repository.list_portfolios(limit=limit)

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
        return self.repository.save_execution_policy_activation(activation)

    def list_execution_policy_activations(
        self,
        portfolio_id: str,
        *,
        limit: int = 100,
    ) -> list[PaperExecutionPolicyActivation]:
        return self.repository.list_execution_policy_activations(portfolio_id, limit=limit)

    def fee_model_from_policy(self, fee_policy: dict[str, Any]) -> FeeModel:
        return FeeModel(
            open_cost=float(fee_policy.get("open_cost", FeeModel.open_cost)),
            close_cost=float(fee_policy.get("close_cost", FeeModel.close_cost)),
            min_cost=float(fee_policy.get("min_cost", FeeModel.min_cost)),
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
        manifest_execution_policy: dict[str, Any],
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
            policy = self._ensure_default_manifest_execution_policy(
                package_id=package_id,
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
        )
        return policy

    def _ensure_default_manifest_execution_policy(
        self,
        *,
        package_id: str,
        manifest_execution_policy: dict[str, Any],
    ) -> ValidatedExecutionPolicy:
        normalized_policy = normalize_execution_policy_json(manifest_execution_policy)
        digest = compute_execution_policy_sha256(normalized_policy)
        for policy in self.package_repository.list_execution_policies(package_id):
            if policy.policy_sha256 == digest:
                if not policy.paper_enabled:
                    policy = StrategyPackageService(repository=self.package_repository).enable_execution_policy_for_paper(
                        package_id,
                        policy.policy_id,
                    )
                return policy
        record = self.package_repository.get(package_id)
        source_backtest_id = record.run_id or record.source_id
        return StrategyPackageService(repository=self.package_repository).create_execution_policy(
            package_id=package_id,
            policy_name="manifest_default_execution_policy",
            policy_json=normalized_policy,
            source_backtest_id=source_backtest_id,
            source_backtest_status="BACKTEST_VALIDATED",
            paper_enabled=True,
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
