"""Read-only Phase 1F.2 receipt and catalog guard for Phase 1G."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from backend.services.advisory_phase1.phase1g_contract import (
    REASON_SCHEMA_NOT_READY,
    REASON_SCHEMA_RECEIPT_INVALID,
    REASON_UNEXPECTED_ERROR,
    Phase1GContractError,
)
from backend.services.advisory_phase1.release_schema_contract import (
    RECEIPT_SCHEMA_VERSION_V2,
    ManagedSchemaStatus,
    DatabaseIdentity,
    OperationStatus,
    PrerequisiteStatus,
    RequestedOperation,
    ReleaseSchemaContract,
    ReleaseSchemaReceipt,
    TargetLabel,
    load_release_schema_contract,
    plan_month_partitions_for_contracts,
)
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    CatalogVerification,
    DatabaseConnectionConfig,
    ReleaseSchemaVerificationError,
    observed_managed_catalog_evidence,
    resolve_database_connection,
    verify_database_catalog,
)


class Phase1GSchemaGuardError(Phase1GContractError):
    pass


@dataclass(frozen=True)
class Phase1GSchemaGuardEvidence:
    release_receipt_hash: str
    catalog_fingerprint: str
    database_identity: DatabaseIdentity


ConnectionResolver = Callable[..., DatabaseConnectionConfig]
CatalogVerifier = Callable[..., CatalogVerification]


class Phase1GExactTargetConnectionResolver:
    """Resolve only the explicitly requested target from one explicit env file."""

    def __init__(self, *, env_file: Path, resolver: ConnectionResolver = resolve_database_connection) -> None:
        self._env_file = env_file.expanduser().resolve()
        self._resolver = resolver

    def resolve(self, *, target_label: TargetLabel) -> DatabaseConnectionConfig:
        try:
            config = self._resolver(target_label=target_label, env_file=self._env_file)
        except ReleaseSchemaVerificationError as exc:
            raise Phase1GSchemaGuardError(
                REASON_SCHEMA_RECEIPT_INVALID,
                "exact Phase 1G target connection configuration is invalid",
                context={"target_label": target_label.value, "cause_reason_code": exc.reason_code},
            ) from exc
        except Exception as exc:
            raise Phase1GSchemaGuardError(
                REASON_UNEXPECTED_ERROR,
                "unexpected exact Phase 1G target connection resolution failure",
                context={"target_label": target_label.value, "error_type": type(exc).__name__},
            ) from exc
        if config.target_label is not target_label:
            raise Phase1GSchemaGuardError(
                REASON_SCHEMA_RECEIPT_INVALID,
                "connection resolver returned a different target label",
            )
        return config


class Phase1GReleaseSchemaGuard:
    """Verify one immutable receipt against a fresh read-only catalog snapshot."""

    def __init__(
        self,
        *,
        contract: ReleaseSchemaContract | None = None,
        verifier: CatalogVerifier = verify_database_catalog,
    ) -> None:
        self._contract = contract or load_release_schema_contract()
        self._verifier = verifier

    def verify(
        self,
        *,
        receipt: ReleaseSchemaReceipt,
        target_label: TargetLabel,
        connection_config: DatabaseConnectionConfig,
    ) -> Phase1GSchemaGuardEvidence:
        self._validate_receipt(receipt=receipt, target_label=target_label, connection_config=connection_config)
        assert receipt.legacy_inventory is not None
        expected_partitions = plan_month_partitions_for_contracts(
            partition_contracts=self._contract.partition_contracts,
            target_months=receipt.legacy_inventory.target_months,
        )
        try:
            verification = self._verifier(
                config=connection_config,
                contract=self._contract,
                expected_partitions=expected_partitions,
            )
        except ReleaseSchemaVerificationError as exc:
            raise Phase1GSchemaGuardError(
                REASON_SCHEMA_NOT_READY,
                "fresh read-only Phase 1G catalog verification failed",
                context={"target_label": target_label.value, "cause_reason_code": exc.reason_code},
            ) from exc
        except Exception as exc:
            raise Phase1GSchemaGuardError(
                REASON_UNEXPECTED_ERROR,
                "unexpected fresh Phase 1G catalog verification failure",
                context={"target_label": target_label.value, "error_type": type(exc).__name__},
            ) from exc
        if (
            not verification.downstream_ready
            or verification.managed_differences
            or verification.prerequisite_differences
        ):
            raise Phase1GSchemaGuardError(
                REASON_SCHEMA_NOT_READY,
                "fresh target catalog is not fully compatible with the Phase 1F.2 contract",
                context={
                    "target_label": target_label.value,
                    "managed_status": verification.managed_schema_status.value,
                    "prerequisite_status": verification.prerequisite_status.value,
                    "managed_difference_count": len(verification.managed_differences),
                    "prerequisite_difference_count": len(verification.prerequisite_differences),
                },
            )
        live_identity = verification.projection.database_identity
        if live_identity != receipt.database_identity:
            raise Phase1GSchemaGuardError(
                REASON_SCHEMA_RECEIPT_INVALID,
                "release receipt database identity is stale or belongs to a different target",
                context={"target_label": target_label.value},
            )
        try:
            evidence = observed_managed_catalog_evidence(
                projection=verification.projection,
                contract=self._contract,
                expected_partitions=expected_partitions,
            )
        except Exception as exc:
            raise Phase1GSchemaGuardError(
                REASON_UNEXPECTED_ERROR,
                "unexpected Phase 1G catalog fingerprint projection failure",
                context={"target_label": target_label.value, "error_type": type(exc).__name__},
            ) from exc
        if evidence.total_sha256 != receipt.post_catalog_fingerprint:
            raise Phase1GSchemaGuardError(
                REASON_SCHEMA_RECEIPT_INVALID,
                "release receipt catalog fingerprint is stale",
                context={
                    "target_label": target_label.value,
                    "receipt_catalog_fingerprint": receipt.post_catalog_fingerprint,
                    "observed_catalog_fingerprint": evidence.total_sha256,
                },
            )
        return Phase1GSchemaGuardEvidence(
            release_receipt_hash=receipt.receipt_content_hash,
            catalog_fingerprint=evidence.total_sha256,
            database_identity=live_identity,
        )

    def _validate_receipt(
        self,
        *,
        receipt: ReleaseSchemaReceipt,
        target_label: TargetLabel,
        connection_config: DatabaseConnectionConfig,
    ) -> None:
        compatible = (
            receipt.schema_version == RECEIPT_SCHEMA_VERSION_V2
            and receipt.operation in {RequestedOperation.APPLY, RequestedOperation.VERIFY}
            and receipt.operation_status is OperationStatus.SUCCESS
            and receipt.managed_schema_status is ManagedSchemaStatus.COMPATIBLE
            and receipt.prerequisite_status is PrerequisiteStatus.COMPATIBLE
            and receipt.downstream_ready
            and not receipt.dml_executed
            and not receipt.runtime_activated
            and not receipt.managed_differences
            and not receipt.prerequisite_differences
            and receipt.legacy_inventory is not None
            and receipt.post_catalog_fingerprint is not None
            and receipt.post_catalog_evidence is not None
        )
        if not compatible:
            raise Phase1GSchemaGuardError(
                REASON_SCHEMA_RECEIPT_INVALID,
                "Phase 1F.2 release receipt is not a complete downstream-ready v2 receipt",
                context={"target_label": target_label.value},
            )
        if receipt.contract_content_hash != self._contract.contract_content_hash:
            raise Phase1GSchemaGuardError(
                REASON_SCHEMA_RECEIPT_INVALID,
                "release receipt contract hash does not match the active Phase 1F.2 registry",
            )
        if (
            receipt.database_identity.target_label is not target_label
            or connection_config.target_label is not target_label
        ):
            raise Phase1GSchemaGuardError(
                REASON_SCHEMA_RECEIPT_INVALID,
                "release receipt or connection target label does not match the Phase 1G request",
            )
        if receipt.database_identity.environment_contract_hash != connection_config.environment_contract_hash:
            raise Phase1GSchemaGuardError(
                REASON_SCHEMA_RECEIPT_INVALID,
                "release receipt environment contract does not match the exact target connection",
            )
