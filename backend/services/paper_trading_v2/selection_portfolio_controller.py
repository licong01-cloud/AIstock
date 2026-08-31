"""Temporary Paper product orchestration for Selection Center.

SIM-LR-A keeps the existing route and side effects, but the selection service
no longer imports or invokes Paper portfolio code.  SIM-LR-C will replace this
controller with the unified simulation control plane.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.selection_center.models import SelectionSimulationAccountLink
from backend.services.selection_center.service import SelectionCenterService
from backend.services.simulation_data.contracts import MinuteDataSource

from .service import PaperTradingV2PortfolioService


class SelectionPaperPortfolioController:
    def __init__(
        self,
        *,
        selection_service: SelectionCenterService,
        portfolio_service: PaperTradingV2PortfolioService | Any | None = None,
    ) -> None:
        self.selection_service = selection_service
        self.portfolio_service = portfolio_service

    def create_from_run(
        self,
        *,
        run_id: str,
        portfolio_name: str,
        initial_cash: float,
        start_date: date,
        data_source: MinuteDataSource,
        fee_policy: dict[str, Any] | None = None,
        risk_policy: dict[str, Any] | None = None,
        execution_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        prepared = self.selection_service.prepare_localsim_account_creation(run_id=run_id)
        run = prepared["run"]
        package_id = str(prepared["package_id"])
        manifest_sha256 = str(prepared["manifest_sha256"])
        binding = self.selection_service.runtime_profile_binding_for_selection_trace(run)
        paper_runtime_config = {
            "selection_source": {
                "run_id": run.run_id,
                "mode": run.mode.value,
                "trade_date": run.trade_date.isoformat(),
                "data_source": run.data_source,
                "package_ids": run.package_ids,
                "manifest_sha256_by_package": run.manifest_sha256_by_package,
                "candidate_count": len(run.aggregate_results),
                "note": "Paper v2 must generate authoritative live selection artifacts for each trading day; "
                "selection run scores are trace-only and are not reused as signal input.",
            },
            "selection_runtime_profile_binding": dict(binding),
        }
        portfolio_service = self.portfolio_service or PaperTradingV2PortfolioService(
            package_repository=self.selection_service.package_repository
        )
        portfolio = portfolio_service.create_portfolio(
            package_id=package_id,
            portfolio_name=portfolio_name,
            initial_cash=initial_cash,
            start_date=start_date,
            data_source=data_source,
            fee_policy=fee_policy,
            risk_policy=risk_policy,
            execution_policy=execution_policy,
        )
        link = self.selection_service.repository.create_simulation_account_link(
            SelectionSimulationAccountLink(
                run_id=run.run_id,
                simulation_account_id=portfolio.portfolio_id,
                package_id=package_id,
                manifest_sha256=manifest_sha256,
                trade_date=run.trade_date,
                data_source=run.data_source,
                start_date=start_date,
                initial_cash=initial_cash,
                runtime_config=paper_runtime_config,
            )
        )
        return {"portfolio": portfolio, "link": link, "paper_runtime_config": paper_runtime_config}

    def list_links(self, run_id: str) -> list[SelectionSimulationAccountLink]:
        self.selection_service.repository.get_run(run_id)
        return self.selection_service.repository.list_simulation_account_links(run_id)
