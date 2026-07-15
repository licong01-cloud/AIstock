from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from backend.services.advisory_phase1 import phase1g_schema_guard as guard_module
from backend.services.advisory_phase1.phase1g_contract import (
    REASON_SCHEMA_NOT_READY,
    REASON_SCHEMA_RECEIPT_INVALID,
    REASON_UNEXPECTED_ERROR,
)
from backend.services.advisory_phase1.phase1g_schema_guard import (
    Phase1GExactTargetConnectionResolver,
    Phase1GReleaseSchemaGuard,
    Phase1GSchemaGuardError,
)
from backend.services.advisory_phase1.release_schema_contract import (
    ManagedSchemaStatus,
    OperationStatus,
    PrerequisiteStatus,
    TargetLabel,
)
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    CatalogProjection,
    CatalogVerification,
    DatabaseConnectionConfig,
    ReleaseSchemaVerificationError,
)
from backend.tests.advisory_phase1.phase1g_test_support import (
    catalog_evidence,
    database_identity,
    h,
    release_receipt,
)


def _config(*, environment_contract_hash: str = h("b")) -> DatabaseConnectionConfig:
    return DatabaseConnectionConfig(
        target_label=TargetLabel.DEV,
        host="127.0.0.1",
        port=5432,
        database="aistock_dev",
        user="aistock",
        password="secret",
        environment_contract_hash=environment_contract_hash,
    )


def _verification(*, identity=None, ready: bool = True) -> CatalogVerification:  # type: ignore[no-untyped-def]
    projection = CatalogProjection(
        database_identity=identity or database_identity(),
        relations=(),
        columns=(),
        constraints=(),
        indexes=(),
        functions=(),
        triggers=(),
        comments=(),
        partitions=(),
    )
    return CatalogVerification(
        projection=projection,
        managed_schema_status=ManagedSchemaStatus.COMPATIBLE if ready else ManagedSchemaStatus.PARTIAL_ADDITIVE,
        prerequisite_status=PrerequisiteStatus.COMPATIBLE,
        managed_differences=(),
        prerequisite_differences=(),
    )


def test_exact_target_resolver_reads_only_explicit_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / "phase1g.env"
    env_file.write_text(
        "\n".join(
            (
                "TDX_DB_DEV_HOST=127.0.0.1",
                "TDX_DB_DEV_PORT=5433",
                "TDX_DB_DEV_NAME=aistock_dev",
                "TDX_DB_DEV_USER=dev_user",
                "TDX_DB_DEV_PASSWORD=dev_password",
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TDX_DB_DEV_HOST", "shell-fallback-must-not-be-used")
    config = Phase1GExactTargetConnectionResolver(env_file=env_file).resolve(target_label=TargetLabel.DEV)

    assert config.host == "127.0.0.1"
    assert config.port == 5433
    assert config.database == "aistock_dev"

    incomplete = tmp_path / "incomplete.env"
    incomplete.write_text("TDX_DB_DEV_HOST=127.0.0.1\n", encoding="utf-8")
    with pytest.raises(Phase1GSchemaGuardError) as error:
        Phase1GExactTargetConnectionResolver(env_file=incomplete).resolve(target_label=TargetLabel.DEV)
    assert error.value.reason_code == REASON_SCHEMA_RECEIPT_INVALID

    wrong_target = replace(_config(), target_label=TargetLabel.PRODUCTION)
    with pytest.raises(Phase1GSchemaGuardError, match="different target"):
        Phase1GExactTargetConnectionResolver(
            env_file=env_file,
            resolver=lambda **_: wrong_target,
        ).resolve(target_label=TargetLabel.DEV)


def test_schema_guard_accepts_complete_receipt_and_matching_fresh_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = release_receipt()
    verification = _verification(identity=receipt.database_identity)
    monkeypatch.setattr(
        guard_module,
        "observed_managed_catalog_evidence",
        lambda **_: catalog_evidence(fingerprint=str(receipt.post_catalog_fingerprint)),
    )
    guard = Phase1GReleaseSchemaGuard(verifier=lambda **_: verification)

    evidence = guard.verify(
        receipt=receipt,
        target_label=TargetLabel.DEV,
        connection_config=_config(environment_contract_hash=receipt.database_identity.environment_contract_hash),
    )

    assert evidence.release_receipt_hash == receipt.receipt_content_hash
    assert evidence.catalog_fingerprint == receipt.post_catalog_fingerprint
    assert evidence.database_identity == receipt.database_identity


def test_schema_guard_rejects_stale_database_identity_and_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = release_receipt()
    changed_identity = database_identity(
        environment_contract_hash=receipt.database_identity.environment_contract_hash
    ).model_copy(update={"server_version_num": receipt.database_identity.server_version_num + 1})
    guard = Phase1GReleaseSchemaGuard(verifier=lambda **_: _verification(identity=changed_identity))
    with pytest.raises(Phase1GSchemaGuardError) as identity_error:
        guard.verify(
            receipt=receipt,
            target_label=TargetLabel.DEV,
            connection_config=_config(environment_contract_hash=receipt.database_identity.environment_contract_hash),
        )
    assert identity_error.value.reason_code == REASON_SCHEMA_RECEIPT_INVALID

    monkeypatch.setattr(
        guard_module, "observed_managed_catalog_evidence", lambda **_: catalog_evidence(fingerprint=h("f"))
    )
    guard = Phase1GReleaseSchemaGuard(verifier=lambda **_: _verification(identity=receipt.database_identity))
    with pytest.raises(Phase1GSchemaGuardError, match="fingerprint") as catalog_error:
        guard.verify(
            receipt=receipt,
            target_label=TargetLabel.DEV,
            connection_config=_config(environment_contract_hash=receipt.database_identity.environment_contract_hash),
        )
    assert catalog_error.value.reason_code == REASON_SCHEMA_RECEIPT_INVALID


def test_schema_guard_rejects_not_ready_or_failed_receipt_without_repair() -> None:
    receipt = release_receipt()
    guard = Phase1GReleaseSchemaGuard(
        verifier=lambda **_: _verification(identity=receipt.database_identity, ready=False)
    )
    with pytest.raises(Phase1GSchemaGuardError) as not_ready:
        guard.verify(
            receipt=receipt,
            target_label=TargetLabel.DEV,
            connection_config=_config(environment_contract_hash=receipt.database_identity.environment_contract_hash),
        )
    assert not_ready.value.reason_code == REASON_SCHEMA_NOT_READY

    failed = receipt.model_copy(update={"operation_status": OperationStatus.FAILED})
    with pytest.raises(Phase1GSchemaGuardError) as invalid:
        guard.verify(
            receipt=failed,
            target_label=TargetLabel.DEV,
            connection_config=_config(environment_contract_hash=receipt.database_identity.environment_contract_hash),
        )
    assert invalid.value.reason_code == REASON_SCHEMA_RECEIPT_INVALID


def test_schema_guard_maps_connection_failure_and_environment_drift_to_explicit_reasons() -> None:
    receipt = release_receipt()

    def failed_verifier(**_):  # type: ignore[no-untyped-def]
        raise ReleaseSchemaVerificationError("ADVISORY_PHASE1F_DATABASE_CONNECTION_FAILED", "offline")

    guard = Phase1GReleaseSchemaGuard(verifier=failed_verifier)
    with pytest.raises(Phase1GSchemaGuardError) as unavailable:
        guard.verify(
            receipt=receipt,
            target_label=TargetLabel.DEV,
            connection_config=_config(environment_contract_hash=receipt.database_identity.environment_contract_hash),
        )
    assert unavailable.value.reason_code == REASON_SCHEMA_NOT_READY

    guard = Phase1GReleaseSchemaGuard(verifier=lambda **_: _verification(identity=receipt.database_identity))
    with pytest.raises(Phase1GSchemaGuardError, match="environment contract") as drift:
        guard.verify(
            receipt=receipt,
            target_label=TargetLabel.DEV,
            connection_config=_config(environment_contract_hash=h("f")),
        )
    assert drift.value.reason_code == REASON_SCHEMA_RECEIPT_INVALID


def test_schema_guard_wraps_unexpected_resolver_and_verifier_failures_without_leaking_messages(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "phase1g.env"
    env_file.write_text("unused=true\n", encoding="utf-8")

    def unexpected(**_):  # type: ignore[no-untyped-def]
        raise RuntimeError("postgresql://user:password@host/database")

    with pytest.raises(Phase1GSchemaGuardError) as resolver_error:
        Phase1GExactTargetConnectionResolver(env_file=env_file, resolver=unexpected).resolve(
            target_label=TargetLabel.DEV
        )
    assert resolver_error.value.reason_code == REASON_UNEXPECTED_ERROR
    assert resolver_error.value.context == {"target_label": "DEV", "error_type": "RuntimeError"}
    assert "postgresql" not in str(resolver_error.value)
    assert isinstance(resolver_error.value.__cause__, RuntimeError)

    receipt = release_receipt()
    guard = Phase1GReleaseSchemaGuard(verifier=unexpected)
    with pytest.raises(Phase1GSchemaGuardError) as verifier_error:
        guard.verify(
            receipt=receipt,
            target_label=TargetLabel.DEV,
            connection_config=_config(environment_contract_hash=receipt.database_identity.environment_contract_hash),
        )
    assert verifier_error.value.reason_code == REASON_UNEXPECTED_ERROR
    assert verifier_error.value.context == {"target_label": "DEV", "error_type": "RuntimeError"}
    assert "postgresql" not in str(verifier_error.value)
    assert isinstance(verifier_error.value.__cause__, RuntimeError)
