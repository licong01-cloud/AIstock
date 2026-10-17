"""Simulation-runtime owned Selection-to-LocalSIM product seam."""

from __future__ import annotations

from backend.services.selection_center.models import SelectionSimulationAccountLink
from backend.services.selection_center.service import SelectionCenterService
from backend.services.trading_core.errors import InvalidStateTransitionError

from .localsim_product_control import (
    LocalSimAccountCreateRequestV1,
    LocalSimControlResponseV1,
    LocalSimProductControlPlaneService,
    LocalSimSelectionLinkContextV1,
)


class SelectionLocalSimAccountController:
    def __init__(
        self,
        *,
        selection_service: SelectionCenterService,
        product_service: LocalSimProductControlPlaneService,
    ) -> None:
        self.selection_service = selection_service
        self.product_service = product_service

    def create_from_run(
        self,
        *,
        run_id: str,
        request: LocalSimAccountCreateRequestV1,
        created_by: str,
    ) -> tuple[LocalSimControlResponseV1, SelectionSimulationAccountLink]:
        prepared = self.selection_service.prepare_localsim_account_creation(run_id=run_id)
        run = prepared["run"]
        if request.package_id != prepared["package_id"]:
            raise InvalidStateTransitionError(
                "Selection run package does not match the LocalSIM request",
                context={"reason_code": "LOCALSIM_SELECTION_PACKAGE_MISMATCH", "run_id": run_id},
            )
        provenance = {
            "schema_version": "selection_simulation_account_link_v1",
            "selection_source": {
                "run_id": run.run_id,
                "mode": run.mode.value,
                "trade_date": run.trade_date.isoformat(),
                "data_source": run.data_source,
                "package_ids": list(run.package_ids),
                "manifest_sha256_by_package": dict(run.manifest_sha256_by_package),
                "candidate_count": len(run.aggregate_results),
            },
            "selection_runtime_profile_binding": self.selection_service.runtime_profile_binding_for_selection_trace(run),
        }
        response, raw_link = self.product_service.create_account_from_selection(
            request,
            link_context=LocalSimSelectionLinkContextV1(
                run_id=run.run_id,
                trade_date=run.trade_date,
                data_source=run.data_source,
                runtime_config=provenance,
            ),
            created_by=created_by,
        )
        link = SelectionSimulationAccountLink.model_validate(raw_link)
        if response.account is None or link.simulation_account_id != response.account.account_id:
            raise InvalidStateTransitionError(
                "Selection LocalSIM account transaction readback is incomplete",
                context={"reason_code": "LOCALSIM_SELECTION_ACCOUNT_READBACK_MISMATCH"},
            )
        return response, link

    def list_links(self, run_id: str) -> list[SelectionSimulationAccountLink]:
        self.selection_service.repository.get_run(run_id)
        return self.selection_service.repository.list_simulation_account_links(run_id)
