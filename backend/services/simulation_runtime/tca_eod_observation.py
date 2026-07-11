"""Default-off EOD TCA observation hook, isolated from execution outcomes."""

from __future__ import annotations

import logging
import os
from datetime import UTC, date, datetime
from typing import Any, Callable, Mapping

from backend.services.qmt_strategy_ledger.tca_rebuild import (
    ExecutionTcaRebuildService,
    TcaRebuildRequest,
)
from backend.services.qmt_strategy_ledger.tca_repository import ExecutionTcaRebuildScope
from backend.services.qmt_strategy_ledger.tca_read_service import TcaReadError, TcaReadRuntimeConfig


logger = logging.getLogger(__name__)


class TcaEodObservationHook:
    """Observe terminal MiniQMT SIM runs after reconciliation without touching them.

    This hook is intentionally best-effort from the scheduler's perspective.
    It may materialize immutable TCA evidence when explicitly enabled, but it
    never writes a simulation run, calls the broker, or changes reconciliation
    success/failure.  Every enabled-path failure is returned and logged loudly.
    """

    def __init__(
        self,
        *,
        rebuild_service_factory: Callable[[], ExecutionTcaRebuildService] = ExecutionTcaRebuildService,
        config_provider: Callable[[], TcaReadRuntimeConfig] = TcaReadRuntimeConfig.from_environ,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self._rebuild_service_factory = rebuild_service_factory
        self._config_provider = config_provider
        self._environ = os.environ if environ is None else environ

    def observe_post_reconciliation(
        self,
        *,
        lifecycle_scheduler: Any,
        terminalized_runs: tuple[Mapping[str, Any], ...],
        trade_date: date,
        as_of_time: datetime,
    ) -> tuple[dict[str, Any], ...]:
        """Run the observation sidecar for post-close terminalization receipts."""

        try:
            config = self._config_provider()
        except Exception as exc:  # noqa: BLE001 - preserve scheduler outcome while surfacing config failure.
            return (self._failure("ADAPTIVE_IS_TCA_EOD_CONFIG_LOAD_FAILED", "TCA_EOD_CONFIG", exc),)
        if not config.eod_observation_enabled:
            return (
                {
                    "status": "DISABLED",
                    "reason_code": "ADAPTIVE_IS_TCA_EOD_OBSERVATION_DISABLED",
                    "stage": "TCA_EOD_CONFIG",
                },
            )
        try:
            pseudonymizer = config.require_pseudonymizer()
            code_commit = _required_env(self._environ, "MINIQMT_TCA_EOD_CODE_COMMIT")
            operator_pseudonym = _required_env(self._environ, "MINIQMT_TCA_EOD_OPERATOR_PSEUDONYM")
        except Exception as exc:  # noqa: BLE001 - same fail-isolated observation boundary.
            return (self._failure("ADAPTIVE_IS_TCA_EOD_AUDIT_CONFIG_INVALID", "TCA_EOD_CONFIG", exc),)

        eligible = [item for item in terminalized_runs if bool(item.get("post_close_terminalization"))]
        if not eligible:
            return (
                {
                    "status": "SKIPPED",
                    "reason_code": "ADAPTIVE_IS_TCA_EOD_NO_POST_CLOSE_TERMINAL_RUN",
                    "stage": "TCA_EOD_ELIGIBILITY",
                },
            )
        outcomes: list[dict[str, Any]] = []
        for terminalized in eligible:
            outcomes.append(
                self._observe_run(
                    lifecycle_scheduler=lifecycle_scheduler,
                    run_id=str(terminalized.get("run_id") or ""),
                    trade_date=trade_date,
                    as_of_time=as_of_time,
                    pseudonymizer=pseudonymizer,
                    code_commit=code_commit,
                    operator_pseudonym=operator_pseudonym,
                )
            )
        return tuple(outcomes)

    def _observe_run(
        self,
        *,
        lifecycle_scheduler: Any,
        run_id: str,
        trade_date: date,
        as_of_time: datetime,
        pseudonymizer: Any,
        code_commit: str,
        operator_pseudonym: str,
    ) -> dict[str, Any]:
        if not run_id:
            return self._failure(
                "ADAPTIVE_IS_TCA_EOD_RUN_ID_MISSING",
                "TCA_EOD_ELIGIBILITY",
                ValueError("post-close terminalization receipt omitted run_id"),
            )
        try:
            run = lifecycle_scheduler.repository.get_simulation_daily_run(run_id)
            if run is None:
                raise TcaReadError(
                    "ADAPTIVE_IS_TCA_EOD_RUN_NOT_FOUND",
                    "post-close terminalization run no longer exists",
                    http_status=404,
                    stage="TCA_EOD_ELIGIBILITY",
                    context={"run_id": run_id},
                )
            binding = lifecycle_scheduler.repository.get_simulation_release_binding(run.binding_id)
            if _backend_value(getattr(binding, "broker_backend", None)) != "miniqmt_sim":
                return {
                    "status": "SKIPPED",
                    "reason_code": "ADAPTIVE_IS_TCA_EOD_NON_MINIQMT_SCOPE",
                    "stage": "TCA_EOD_ELIGIBILITY",
                    "run_id": run_id,
                    "binding_id": str(getattr(binding, "binding_id", "") or ""),
                }
            if run.trade_date != trade_date:
                return {
                    "status": "SKIPPED",
                    "reason_code": "ADAPTIVE_IS_TCA_EOD_TRADE_DATE_MISMATCH",
                    "stage": "TCA_EOD_ELIGIBILITY",
                    "run_id": run_id,
                    "binding_id": str(getattr(binding, "binding_id", "") or ""),
                }
            if not _has_completed_reconciliation(run):
                return {
                    "status": "SKIPPED",
                    "reason_code": "ADAPTIVE_IS_TCA_EOD_RECONCILIATION_NOT_TERMINAL",
                    "stage": "TCA_EOD_ELIGIBILITY",
                    "run_id": run_id,
                    "binding_id": str(getattr(binding, "binding_id", "") or ""),
                }
            account_id = str(getattr(binding, "broker_account_id", "") or "").strip()
            if not account_id:
                raise TcaReadError(
                    "ADAPTIVE_IS_TCA_EOD_ACCOUNT_MISSING",
                    "MiniQMT SIM binding lacks broker account identity for TCA scope",
                    http_status=503,
                    stage="TCA_EOD_ELIGIBILITY",
                    context={"run_id": run_id, "binding_id": str(getattr(binding, "binding_id", "") or "")},
                )
            request = TcaRebuildRequest(
                scope=ExecutionTcaRebuildScope(
                    binding_ids=(str(binding.binding_id),),
                    trade_date_from=trade_date,
                    trade_date_to=trade_date,
                    account_ids=(account_id,),
                    environment="SIM",
                ),
                snapshot_kind="RECONCILED_FINAL",
                as_of_time=_utc(as_of_time),
                account_pseudonyms={account_id: pseudonymizer.pseudonymize(account_id)},
                account_pseudonym_key_version=str(pseudonymizer.key_version),
                operator_pseudonym=operator_pseudonym,
                code_commit=code_commit,
            )
            rebuild = self._rebuild_service_factory().rebuild(request)
            if rebuild.receipt_status != "COMPLETED":
                return self._failure(
                    rebuild.reason_code or "ADAPTIVE_IS_TCA_EOD_REBUILD_FAILED",
                    rebuild.stage or "TCA_EOD_REBUILD",
                    RuntimeError("TCA rebuild returned non-completed receipt"),
                    run_id=run_id,
                    binding_id=str(binding.binding_id),
                )
            return {
                "status": "REBUILT",
                "reason_code": None,
                "stage": "TCA_EOD_REBUILD",
                "run_id": run_id,
                "binding_id": str(binding.binding_id),
                "receipt_id": rebuild.receipt_id,
                "receipt_generation": rebuild.receipt_generation,
                "result_ids": list(rebuild.result_ids),
                "reused": rebuild.reused,
            }
        except Exception as exc:  # noqa: BLE001 - observation failure must never mutate scheduler outcome.
            reason_code = getattr(exc, "reason_code", None) or "ADAPTIVE_IS_TCA_EOD_REBUILD_EXCEPTION"
            stage = getattr(exc, "stage", None) or "TCA_EOD_REBUILD"
            return self._failure(reason_code, stage, exc, run_id=run_id)

    @staticmethod
    def _failure(
        reason_code: str,
        stage: str,
        exc: Exception,
        *,
        run_id: str | None = None,
        binding_id: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "status": "FAILED",
            "reason_code": reason_code,
            "stage": stage,
            "run_id": run_id,
            "binding_id": binding_id,
            "error_type": type(exc).__name__,
        }
        logger.error("MiniQMT TCA EOD observation failed: %s", payload)
        return payload


def _required_env(environ: Mapping[str, str], name: str) -> str:
    value = str(environ.get(name) or "").strip()
    if value:
        return value
    raise TcaReadError(
        "ADAPTIVE_IS_TCA_EOD_AUDIT_CONFIG_MISSING",
        f"{name} must be configured when MINIQMT_TCA_EOD_OBSERVATION_ENABLED=true",
        http_status=503,
        stage="TCA_EOD_CONFIG",
        context={"missing_field": name},
    )


def _backend_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def _has_completed_reconciliation(run: Any) -> bool:
    if _backend_value(getattr(run, "status", None)) != "succeeded":
        return False
    payload = getattr(run, "run_payload_json", None)
    if not isinstance(payload, Mapping):
        return False
    reconciliation = payload.get("reconcile_after_submit")
    if not isinstance(reconciliation, Mapping):
        return False
    run_summary = reconciliation.get("run")
    return isinstance(run_summary, Mapping) and str(run_summary.get("status") or "") == "SUCCEEDED"


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
