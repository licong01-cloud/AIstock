"""MiniQMT runtime public API with side-effect-free submodule imports."""

from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORT_MODULES = {
    "MINIQMT_EXECUTION_RUNTIME_ENV": ".config",
    "MiniQMTExecutionRuntimeKind": ".config",
    "get_miniqmt_execution_runtime_kind": ".config",
    "MiniQMTRuntimeManagedBatchSubmitResult": ".client",
    "MiniQMTRuntimeEvidence": ".client",
    "MiniQMTPlanPreviewResult": ".client",
    "MiniQMTExecutionRuntimeClient": ".client",
    "CatchUpPolicyV1": ".kernel_clock",
    "ExchangeSessionClockV1": ".kernel_clock",
    "ExchangeSessionProjectionV1": ".kernel_clock",
    "KernelClockError": ".kernel_clock",
    "KernelClockWakeReceiptV1": ".kernel_clock",
    "GatewayReconciliationSnapshotV1": ".kernel_outbox",
    "KernelGatewayPreCallError": ".kernel_outbox",
    "KernelOutboxDispatchError": ".kernel_outbox",
    "KernelOutboxDispatcherV1": ".kernel_outbox",
    "KernelOutboxRecoveryV1": ".kernel_outbox",
    "KernelOutboxReconcilerV1": ".kernel_outbox",
    "MiniQMTKernelGatewayAdapterV1": ".kernel_outbox",
    "KernelDiagnosticsReadServiceV1": ".kernel_diagnostics",
    "KernelDiagnosticsProjectionV1": ".kernel_diagnostics",
    "project_kernel_diagnostics_v1": ".kernel_diagnostics",
    "build_vnpy_facade_characterization_authority_fresh_process_v2": (".vnpy_facade_characterization_runner"),
    "run_vnpy_facade_source_execution_sets_v1": ".vnpy_facade_characterization_runner",
    "validate_vnpy_facade_characterization_authority_fresh_process_v2": (".vnpy_facade_characterization_runner"),
    "validate_vnpy_facade_conformance_set_fresh_process_v2": (".vnpy_facade_characterization_runner"),
    "validate_vnpy_facade_k3_expected_trace_materials_v1": (".vnpy_facade_characterization_runner"),
    "VnpyFacadeDiagnosticsSnapshotV1": ".vnpy_facade_diagnostics",
    "VnpyFacadeFailureSummaryV1": ".vnpy_facade_diagnostics",
    "VnpyFacadeMetricV1": ".vnpy_facade_diagnostics",
    "read_vnpy_facade_diagnostics_v1": ".vnpy_facade_diagnostics",
    "CurrentThreeLegacyInventorySetV1": ".kernel_current_three_contracts",
    "CurrentThreeLegacyStateInventoryV1": ".kernel_current_three_contracts",
    "CurrentThreeDependentBuyInventoryV1": ".kernel_current_three_contracts",
    "CurrentThreeParityInputV1": ".kernel_current_three_contracts",
    "CurrentThreeParityReceiptV1": ".kernel_current_three_contracts",
    "CurrentThreeShadowSourceSnapshotV1": ".kernel_current_three_contracts",
    "CurrentThreeShadowCommandAssociationV1": ".kernel_current_three_contracts",
    "build_current_three_legacy_inventory_set_v1": ".kernel_current_three_inventory",
    "build_current_three_parity_input_from_shadow_v1": ".kernel_current_three_shadow_runner",
    "build_current_three_shadow_event_v1": ".kernel_current_three_shadow_runner",
    "run_current_three_committed_parity_v1": ".kernel_current_three_shadow_runner",
    "build_current_three_shadow_creation_request_v1": ".kernel_current_three_shadow_orchestration",
    "build_current_three_shadow_delivery_input_v1": ".kernel_current_three_shadow_orchestration",
    "MiniQMTGatewayContract": ".contracts",
    "MiniQMTGatewayEventSourceContract": ".contracts",
    "MiniQMTStrategyLedgerOmsContract": ".contracts",
    "MiniQMTVnpyAlgoCoreContract": ".contracts",
    "FakeMiniQMTGateway": ".gateway",
    "MiniQMTGateway": ".gateway",
    "ClosingAuctionCapabilityProbe": ".quote_auction",
    "MarkoutAnchor": ".quote_evidence",
    "QuoteEvidenceCoordinator": ".quote_evidence",
    "QuoteEvidenceHealth": ".quote_evidence",
    "QuoteIngressHealthV1": ".quote_evidence",
    "MiniQMTGatewayCancelAck": ".gateway",
    "MiniQMTGatewayEventSink": ".gateway",
    "MiniQMTGatewayEventSource": ".gateway",
    "MiniQMTGatewayEventSourceError": ".gateway",
    "MiniQMTGatewayOrderAck": ".gateway",
    "QmtClientMiniQMTEventLoopGateway": ".gateway",
    "QmtClientMiniQMTGateway": ".gateway",
    "MiniQMTAlgoInstanceStatus": ".models",
    "MiniQMTChildOrder": ".models",
    "MiniQMTChildOrderStatus": ".models",
    "MiniQMTExecutionAlgoInstance": ".models",
    "MiniQMTExecutionEvent": ".models",
    "MiniQMTExecutionEventType": ".models",
    "MiniQMTExecutionRuntimeConfig": ".models",
    "MiniQMTExecutionRuntimeMode": ".models",
    "MiniQMTExecutionRuntimeRecord": ".models",
    "MiniQMTExecutionRuntimeState": ".models",
    "MiniQMTGatewayState": ".models",
    "MiniQMTOmsState": ".models",
    "MiniQMTOperatorCommandResult": ".models",
    "MiniQMTOperatorCommandStatus": ".models",
    "MiniQMTRuntimeRecoverySnapshot": ".models",
    "MiniQMTOmsLedger": ".oms",
    "MiniQMTOmsProjection": ".oms",
    "DEFAULT_MINIQMT_EXECUTION_RUNTIME_REPOSITORY": ".repository",
    "MINIQMT_EXECUTION_RUNTIME_JSONFILE_TEST_ONLY_ENV": ".repository",
    "MINIQMT_EXECUTION_RUNTIME_PRUNE_EVERY_WRITES_ENV": ".repository",
    "MINIQMT_EXECUTION_RUNTIME_REPOSITORY_ENV": ".repository",
    "InMemoryMiniQMTExecutionRuntimeRepository": ".repository",
    "JsonFileMiniQMTExecutionRuntimeRepository": ".repository",
    "MiniQMTExecutionRuntimeRepository": ".repository",
    "PostgresMiniQMTExecutionRuntimeRepository": ".repository",
    "default_miniqmt_execution_runtime_repository": ".repository",
    "ConfigurableMiniQMTRiskEngine": ".risk",
    "MiniQMTRiskDecision": ".risk",
    "MiniQMTRiskDecisionAction": ".risk",
    "MiniQMTRiskEngine": ".risk",
    "MiniQMTRiskPriceBand": ".risk",
    "MiniQMTRiskRuleSet": ".risk",
    "NoopMiniQMTRiskEngine": ".risk",
    "MiniQMTExecutionEventLoop": ".runtime",
    "MiniQMTExecutionRuntime": ".runtime",
}

__all__ = list(_EXPORT_MODULES)


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted((*globals(), *_EXPORT_MODULES))
