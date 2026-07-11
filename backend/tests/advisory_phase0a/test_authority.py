from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from backend.services.advisory_phase0a.authority import (
    ApprovalDecisionRequest,
    AuthorizationEventType,
    DecisionEventType,
    DecisionKind,
    EvidenceScope,
    OperationAuthorizationRequest,
    Phase0AAuthorityError,
    build_approval_bundle,
    build_approval_decision,
    build_handoff_bundle,
    build_operation_authorization_event,
    validate_approval_bundle_active,
    validate_decision_chains,
    validate_operation_authorization_chain,
)
from backend.services.advisory_phase0a.approval_repository import PostgresApprovalAuthorityRepository
from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonical_json_text
from scripts import advisory_phase0a_finalize as finalizer


finalizer_main = finalizer.main


def _write_receipt(tmp_path) -> None:
    audit_id = "audit_authority"
    request_hash = "1" * 64
    handoff_hashes = {
        "source_availability_matrix_hash": "2" * 64,
        "universe_survivorship_hash": "3" * 64,
        "asset_runtime_hmm_ledger_hash": "4" * 64,
        "oos_interval_report_hash": "5" * 64,
        "candidate_authority_stage_capability_hash": "6" * 64,
        "metric_label_policy_hash": "7" * 64,
        "prior_registry_hash": "8" * 64,
        "multiple_testing_registry_hash": "9" * 64,
        "policy_registry_hash": "a" * 64,
    }
    manifest_base = {
        "schema_version": "advisory_phase0a_audit_receipt_v1",
        "audit_id": audit_id,
        "request_hash": request_hash,
        "audit_policy_version": "policy_v1",
        "policy_hash": "b" * 64,
        "serializer_version": "advisory_phase0a_canonical_v1",
        "result_hash": "c" * 64,
        "phase1_handoff_hashes": handoff_hashes,
        "read_only": True,
        "raw_intermediate_location": "tmp/advisory_phase0a/audit_authority",
        "raw_intermediate_cleanup_status": "NOT_CREATED",
    }
    manifest = {**manifest_base, "audit_manifest_hash": canonical_json_sha256(manifest_base)}
    payloads = {
        "audit_manifest.json": manifest,
        "approval_receipt.json": {
            "schema_version": "advisory_phase0a_approval_receipt_v1",
            "audit_id": audit_id,
            "approval_status": "NOT_APPROVED",
            "approved_request_reference": "request-1",
            "automatic_approval": False,
            "phase1_exit_gate_status": "BLOCKED_PENDING_MANUAL_APPROVAL",
        },
        "target_scope_registry.json": {
            "schema_version": "advisory_phase0a_target_scope_registry_v1",
            "audit_id": audit_id,
            "request_hash": request_hash,
            "targets": [
                {
                    "audit_target_id": "target-1",
                    "program_id": "program-1",
                    "package_id": "package-1",
                    "manifest_sha256": "d" * 64,
                    "expected_alpha_mode": "single_alpha",
                    "decision_date_range": {"start_date": "2026-02-05", "end_date": "2026-02-05"},
                    "decision_dates": ["2026-02-05"],
                    "style_family": "SHORT_REBOUND",
                    "requested_capabilities": ["candidate_authority"],
                }
            ],
        },
        "runtime_semantics_ledger.json": {
            "schema_version": "advisory_phase0a_runtime_semantics_ledger_v1",
            "audit_id": audit_id,
            "entries": [
                {
                    "decision_date": "2026-02-05",
                    "package_id": "package-1",
                    "selection_runtime_semantics_id": "selection-v1",
                    "effective_config_hashes": {"runtime": "e" * 64, "package": "f" * 64},
                }
            ],
        },
        "candidate_authority_stage_capability_report.json": {
            "schema_version": "advisory_phase0a_candidate_authority_stage_capability_v1",
            "audit_id": audit_id,
            "entries": [
                {
                    "audit_target_id": "target-1",
                    "decision_date": "2026-02-05",
                    "signal_context_hash": "0" * 64,
                    "decision_clock": {"calendar_hash": "1" * 64},
                }
            ],
            "canonical_observation_groups": [
                {
                    "signal_context_hash": "0" * 64,
                    "lineage": [
                        {
                            "audit_target_id": "target-1",
                            "program_id": "program-1",
                            "package_id": "package-1",
                            "decision_date": "2026-02-05",
                            "binding_version_id": "binding-1",
                            "canonical_signal_observation_id": "signal-1",
                        }
                    ],
                }
            ],
        },
        "oos_interval_report.json": {
            "schema_version": "advisory_phase0a_oos_interval_report_v1",
            "audit_id": audit_id,
            "classifications": [
                {
                    "audit_target_id": "target-1",
                    "decision_date": "2026-02-05",
                    "signal_context_hash": "0" * 64,
                    "signal_capability": "candidate_signal",
                    "formal_oos_status": "FORMAL_OOS",
                }
            ],
            "intervals": [
                {
                    "audit_target_id": "target-1",
                    "interval_id": "interval-1",
                    "start_date": "2026-02-05",
                    "end_date": "2026-02-05",
                    "signal_context_hash": "0" * 64,
                    "signal_capability": "candidate_signal",
                    "formal_oos_status": "FORMAL_OOS",
                }
            ],
        },
    }
    for name, payload in payloads.items():
        (tmp_path / name).write_text(canonical_json_text(payload) + "\n", encoding="utf-8")


def _handoff(tmp_path):
    _write_receipt(tmp_path)
    return build_handoff_bundle(
        receipt_dir=tmp_path,
        created_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )


def test_handoff_derives_stable_scope_and_excludes_created_at(tmp_path) -> None:
    handoff = _handoff(tmp_path)

    assert handoff.audit_id == "audit_authority"
    assert handoff.phase1_handoff_bundle_hash
    assert len(handoff.sorted_target_handoffs) == 1
    scope = handoff.sorted_target_handoffs[0].admission_scopes[0]
    assert scope.allowed_evidence_scope == EvidenceScope.FORMAL_OOS
    assert scope.stable_signal_semantics_payload_v1["package_id"] == "package-1"
    assert scope.target_handoff_hash == handoff.sorted_target_handoffs[0].target_handoff_hash


def test_decision_chain_and_scope_revoke_only_excludes_scope(tmp_path) -> None:
    handoff = _handoff(tmp_path)
    scope = handoff.sorted_target_handoffs[0].admission_scopes[0]
    global_approval = build_approval_decision(
        handoff=handoff,
        request=ApprovalDecisionRequest(
            decision_kind=DecisionKind.GLOBAL,
            event_type=DecisionEventType.APPROVE,
            approval_reference="global-approval",
        ),
        existing_events=[],
        actor_principal="approver",
        authority_backend_id="local-test",
        decision_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    scope_approval = build_approval_decision(
        handoff=handoff,
        request=ApprovalDecisionRequest(
            decision_kind=DecisionKind.ADMISSION_SCOPE,
            event_type=DecisionEventType.APPROVE,
            admission_scope_id=scope.admission_scope_id,
            approval_reference="scope-approval",
        ),
        existing_events=[global_approval],
        actor_principal="approver",
        authority_backend_id="local-test",
        decision_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    bundle = build_approval_bundle(
        handoff=handoff,
        events=[global_approval, scope_approval],
        created_by="finalizer",
        authority_backend_id="local-test",
        created_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    assert bundle.scope_member_count == 1

    revoked = build_approval_decision(
        handoff=handoff,
        request=ApprovalDecisionRequest(
            decision_kind=DecisionKind.ADMISSION_SCOPE,
            event_type=DecisionEventType.REVOKE,
            admission_scope_id=scope.admission_scope_id,
            revokes_decision_hash=scope_approval.decision_hash,
            approval_reference="scope-revoke",
        ),
        existing_events=[global_approval, scope_approval],
        actor_principal="approver",
        authority_backend_id="local-test",
        decision_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    replacement = build_approval_bundle(
        handoff=handoff,
        events=[global_approval, scope_approval, revoked],
        created_by="finalizer",
        authority_backend_id="local-test",
        created_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    assert replacement.scope_member_count == 0


def test_decision_chain_rejects_fork(tmp_path) -> None:
    handoff = _handoff(tmp_path)
    approved = build_approval_decision(
        handoff=handoff,
        request=ApprovalDecisionRequest(
            decision_kind=DecisionKind.GLOBAL,
            event_type=DecisionEventType.APPROVE,
            approval_reference="approve",
        ),
        existing_events=[],
        actor_principal="approver",
        authority_backend_id="local-test",
        decision_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    revoke = build_approval_decision(
        handoff=handoff,
        request=ApprovalDecisionRequest(
            decision_kind=DecisionKind.GLOBAL,
            event_type=DecisionEventType.REVOKE,
            revokes_decision_hash=approved.decision_hash,
            approval_reference="revoke",
        ),
        existing_events=[approved],
        actor_principal="approver",
        authority_backend_id="local-test",
        decision_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    fork = revoke.model_copy(update={"decision_id": "fork"})
    with pytest.raises(Phase0AAuthorityError, match="FORKED"):
        validate_decision_chains(handoff=handoff, events=[approved, revoke, fork])


def test_operation_authorization_enforces_business_and_governance_matrix() -> None:
    now = datetime(2026, 7, 11, tzinfo=timezone.utc)
    business = OperationAuthorizationRequest(
        authorization_id="auth-1",
        event_type=AuthorizationEventType.AUTHORIZE,
        operation_type="CAPTURE_DML",
        environment="dev",
        approval_bundle_hash="a" * 64,
        admission_scope_set_hash="b" * 64,
        operation_payload_hash="c" * 64,
        valid_from=now,
        expires_at=datetime(2026, 7, 12, tzinfo=timezone.utc),
        approval_reference="request-1",
    )
    event = build_operation_authorization_event(
        request=business,
        existing_events=[],
        actor_principal="authorizer",
        event_at=now,
    )
    assert event.authorization_event_hash

    invalid = business.model_copy(update={"authorization_id": "auth-2", "governance_scope_hash": "d" * 64})
    with pytest.raises(Phase0AAuthorityError, match="business matrix"):
        build_operation_authorization_event(
            request=invalid,
            existing_events=[],
            actor_principal="authorizer",
            event_at=now,
        )


def test_operation_authorization_revoke_and_reauthorize_chain() -> None:
    now = datetime(2026, 7, 11, tzinfo=timezone.utc)
    request = OperationAuthorizationRequest(
        authorization_id="auth-chain",
        event_type=AuthorizationEventType.AUTHORIZE,
        operation_type="CAPTURE_DML",
        environment="dev",
        approval_bundle_hash="a" * 64,
        admission_scope_set_hash="b" * 64,
        operation_payload_hash="c" * 64,
        valid_from=now,
        expires_at=datetime(2026, 7, 12, tzinfo=timezone.utc),
        approval_reference="authorize",
    )
    authorized = build_operation_authorization_event(
        request=request,
        existing_events=[],
        actor_principal="authorizer",
        event_at=now,
    )
    revoked = build_operation_authorization_event(
        request=request.model_copy(
            update={
                "event_type": AuthorizationEventType.REVOKE,
                "revokes_event_hash": authorized.authorization_event_hash,
                "approval_reference": "revoke",
            }
        ),
        existing_events=[authorized],
        actor_principal="authorizer",
        event_at=datetime(2026, 7, 11, 1, tzinfo=timezone.utc),
    )
    reauthorized = build_operation_authorization_event(
        request=request.model_copy(
            update={
                "previous_event_hash": revoked.authorization_event_hash,
                "approval_reference": "reauthorize",
            }
        ),
        existing_events=[authorized, revoked],
        actor_principal="authorizer",
        event_at=datetime(2026, 7, 11, 2, tzinfo=timezone.utc),
    )
    validate_operation_authorization_chain(
        authorization_id=request.authorization_id,
        events=[authorized, revoked, reauthorized],
    )


def test_approval_bundle_becomes_inactive_after_scope_revoke(tmp_path) -> None:
    handoff = _handoff(tmp_path)
    scope = handoff.sorted_target_handoffs[0].admission_scopes[0]
    global_approval = build_approval_decision(
        handoff=handoff,
        request=ApprovalDecisionRequest(
            decision_kind=DecisionKind.GLOBAL,
            event_type=DecisionEventType.APPROVE,
            approval_reference="global",
        ),
        existing_events=[],
        actor_principal="approver",
        authority_backend_id="local-test",
        decision_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    scope_approval = build_approval_decision(
        handoff=handoff,
        request=ApprovalDecisionRequest(
            decision_kind=DecisionKind.ADMISSION_SCOPE,
            event_type=DecisionEventType.APPROVE,
            admission_scope_id=scope.admission_scope_id,
            approval_reference="scope",
        ),
        existing_events=[global_approval],
        actor_principal="approver",
        authority_backend_id="local-test",
        decision_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    bundle = build_approval_bundle(
        handoff=handoff,
        events=[global_approval, scope_approval],
        created_by="finalizer",
        authority_backend_id="local-test",
        created_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    revoked = build_approval_decision(
        handoff=handoff,
        request=ApprovalDecisionRequest(
            decision_kind=DecisionKind.ADMISSION_SCOPE,
            event_type=DecisionEventType.REVOKE,
            admission_scope_id=scope.admission_scope_id,
            revokes_decision_hash=scope_approval.decision_hash,
            approval_reference="scope-revoke",
        ),
        existing_events=[global_approval, scope_approval],
        actor_principal="approver",
        authority_backend_id="local-test",
        decision_at=datetime(2026, 7, 11, 1, tzinfo=timezone.utc),
    )
    with pytest.raises(Phase0AAuthorityError, match="admission scope is inactive"):
        validate_approval_bundle_active(
            bundle=bundle,
            events=[global_approval, scope_approval, revoked],
        )


def test_authority_migration_declares_append_only_control_plane() -> None:
    migration = (
        Path(__file__).parents[2]
        / "db"
        / "migrations"
        / "add_advisory_phase0a1_authority_20260711.sql"
    ).read_text(encoding="utf-8")
    for required in (
        "advisory_phase0a_approval_event",
        "advisory_phase0a_approval_bundle",
        "advisory_phase0a_approval_bundle_scope",
        "advisory_phase1_operation_authorization_event",
        "reject_advisory_phase0a_authority_mutation",
        "aistock_advisory_phase0a_approver",
        "aistock_advisory_phase1_operation_authorizer",
    ):
        assert required in migration


def test_finalizer_validate_and_build_handoff_are_database_free(tmp_path, capsys) -> None:
    _write_receipt(tmp_path)
    assert finalizer_main(["validate-handoff", "--receipt-dir", str(tmp_path)]) == 0
    validated = json.loads(capsys.readouterr().out)
    assert validated["mode"] == "validated_only"

    output = tmp_path / "phase1_handoff_bundle.json"
    assert finalizer_main(
        ["build-handoff-bundle", "--receipt-dir", str(tmp_path), "--output", str(output)]
    ) == 0
    bundle = json.loads(output.read_text(encoding="utf-8"))
    assert bundle["phase1_handoff_bundle_hash"] == validated["phase1_handoff_bundle_hash"]


class _FakeCursor:
    def __init__(self, *, rows_by_marker: dict[str, list[tuple[Any, ...]]] | None = None) -> None:
        self.rows_by_marker = rows_by_marker or {}
        self.executed: list[tuple[str, Any]] = []
        self._rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, query: str, params: Any = None) -> None:
        self.executed.append((query, params))
        self._rows = next((rows for marker, rows in self.rows_by_marker.items() if marker in query), [])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self.cursor_value = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return self.cursor_value

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_postgres_authority_repository_serializes_append_only_records(tmp_path) -> None:
    handoff = _handoff(tmp_path)
    decision = build_approval_decision(
        handoff=handoff,
        request=ApprovalDecisionRequest(
            decision_kind=DecisionKind.GLOBAL,
            event_type=DecisionEventType.APPROVE,
            approval_reference="approve",
        ),
        existing_events=[],
        actor_principal="approver",
        authority_backend_id="local-test",
        decision_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    bundle = build_approval_bundle(
        handoff=handoff,
        events=[decision],
        created_by="finalizer",
        authority_backend_id="local-test",
        created_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    authorization = build_operation_authorization_event(
        request=OperationAuthorizationRequest(
            authorization_id="repo-auth",
            event_type=AuthorizationEventType.AUTHORIZE,
            operation_type="CAPTURE_DML",
            environment="dev",
            approval_bundle_hash=bundle.approval_bundle_content_hash,
            admission_scope_set_hash=bundle.admission_scope_set_hash,
            operation_payload_hash="c" * 64,
            valid_from=datetime(2026, 7, 11, tzinfo=timezone.utc),
            expires_at=datetime(2026, 7, 12, tzinfo=timezone.utc),
            approval_reference="authorize",
        ),
        existing_events=[],
        actor_principal="authorizer",
        event_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
    )
    cursor = _FakeCursor(
        rows_by_marker={
            "SELECT current_user": [("db_approver",)],
            "FROM app.advisory_phase0a_approval_event": [(json.dumps(decision.model_dump(mode="json")),)],
            "FROM app.advisory_phase0a_approval_bundle": [(json.dumps(bundle.model_dump(mode="json")),)],
            "FROM app.advisory_phase1_operation_authorization_event": [
                (json.dumps(authorization.model_dump(mode="json")),)
            ],
        }
    )
    connection = _FakeConnection(cursor)

    @contextmanager
    def factory():
        yield connection

    repository = PostgresApprovalAuthorityRepository(conn_factory=factory)
    assert repository.current_actor() == "db_approver"
    assert repository.list_decisions(handoff=handoff) == [decision]
    assert repository.list_decisions_for_bundle(bundle=bundle) == [decision]
    assert repository.get_approval_bundle(approval_bundle_content_hash=bundle.approval_bundle_content_hash) == bundle
    assert repository.list_operation_authorizations(authorization_id="repo-auth") == [authorization]
    assert repository.append_decision(decision) == decision
    assert repository.append_bundle(bundle) == bundle
    assert repository.append_operation_authorization(authorization) == authorization
    assert connection.commits == 3


class _MemoryAuthorityRepository:
    def __init__(self) -> None:
        self.decisions = []
        self.bundles: dict[str, Any] = {}
        self.authorizations = []

    def current_actor(self) -> str:
        return "test_db_actor"

    def list_decisions(self, *, handoff):
        return list(self.decisions)

    def list_decisions_for_bundle(self, *, bundle):
        return list(self.decisions)

    def append_decision(self, event):
        self.decisions.append(event)
        return event

    def append_bundle(self, bundle):
        self.bundles[bundle.approval_bundle_content_hash] = bundle
        return bundle

    def get_approval_bundle(self, *, approval_bundle_content_hash: str):
        return self.bundles.get(approval_bundle_content_hash)

    def list_operation_authorizations(self, *, authorization_id: str):
        return [event for event in self.authorizations if event.authorization_id == authorization_id]

    def append_operation_authorization(self, event):
        self.authorizations.append(event)
        return event


def test_finalizer_mutation_commands_use_repository_and_registered_bundle(tmp_path, monkeypatch, capsys) -> None:
    _write_receipt(tmp_path)
    handoff_path = tmp_path / "handoff.json"
    assert finalizer_main(["build-handoff-bundle", "--receipt-dir", str(tmp_path), "--output", str(handoff_path)]) == 0
    capsys.readouterr()
    repository = _MemoryAuthorityRepository()
    monkeypatch.setattr(finalizer, "_repository", lambda _args: repository)

    decision_path = tmp_path / "global_approve.json"
    decision_path.write_text(
        canonical_json_text(
            {
                "decision_kind": "GLOBAL",
                "event_type": "APPROVE",
                "approval_reference": "test-approve",
            }
        ),
        encoding="utf-8",
    )
    command_prefix = ["register-decision", "--handoff", str(handoff_path), "--decision", str(decision_path), "--authority-backend-id", "test"]
    assert finalizer_main(command_prefix) == 2
    assert finalizer_main([*command_prefix, "--execute-finalize"]) == 0

    decision_path.write_text(
        canonical_json_text(
            {
                "decision_kind": "GLOBAL",
                "event_type": "REVOKE",
                "revokes_decision_hash": "a" * 64,
                "approval_reference": "wrong-command",
            }
        ),
        encoding="utf-8",
    )
    assert finalizer_main([*command_prefix, "--execute-finalize"]) == 2
    decision_path.write_text(
        canonical_json_text(
            {
                "decision_kind": "GLOBAL",
                "event_type": "APPROVE",
                "approval_reference": "test-approve",
            }
        ),
        encoding="utf-8",
    )

    bundle_command = ["build-approval-bundle", "--handoff", str(handoff_path), "--authority-backend-id", "test", "--execute-bundle"]
    assert finalizer_main(bundle_command) == 0
    assert finalizer_main(["verify-decision-chain", "--handoff", str(handoff_path)]) == 0
    bundle = next(iter(repository.bundles.values()))

    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text(
        canonical_json_text(
            {
                "authorization_id": "cli-auth",
                "event_type": "AUTHORIZE",
                "operation_type": "CAPTURE_DML",
                "environment": "dev",
                "approval_bundle_hash": bundle.approval_bundle_content_hash,
                "admission_scope_set_hash": bundle.admission_scope_set_hash,
                "operation_payload_hash": "d" * 64,
                "valid_from": "2026-07-11T00:00:00Z",
                "expires_at": "2026-07-12T00:00:00Z",
                "approval_reference": "test-authorize",
            }
        ),
        encoding="utf-8",
    )
    authorization_revoke = json.loads(authorization_path.read_text(encoding="utf-8"))
    authorization_revoke["event_type"] = "REVOKE"
    authorization_path.write_text(canonical_json_text(authorization_revoke), encoding="utf-8")
    assert finalizer_main(["authorize-operation", "--authorization", str(authorization_path), "--execute-authorize"]) == 2
    authorization_revoke["event_type"] = "AUTHORIZE"
    authorization_path.write_text(canonical_json_text(authorization_revoke), encoding="utf-8")
    assert finalizer_main(["authorize-operation", "--authorization", str(authorization_path), "--execute-authorize"]) == 0
    authorization_revoke["event_type"] = "REVOKE"
    authorization_revoke["revokes_event_hash"] = repository.authorizations[0].authorization_event_hash
    repository.bundles.clear()
    authorization_path.write_text(canonical_json_text(authorization_revoke), encoding="utf-8")
    assert finalizer_main(
        ["revoke-authorization", "--authorization", str(authorization_path), "--execute-auth-revoke"]
    ) == 0
    assert finalizer_main(["verify-authorization", "--authorization-id", "cli-auth"]) == 0
    assert '"ok": true' in capsys.readouterr().out


def test_finalizer_rejects_unconfigured_or_unauthorized_database_targets(monkeypatch) -> None:
    for key in (
        "TDX_DB_DEV_HOST",
        "TDX_DB_DEV_PORT",
        "TDX_DB_DEV_NAME",
        "TDX_DB_DEV_USER",
        "TDX_DB_DEV_PASSWORD",
    ):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(finalizer.AdvisoryPhase0AFinalizerCommandError, match="missing database"):
        finalizer._db_config(target_db="dev")

    monkeypatch.setenv("TDX_DB_DEV_HOST", "database.example")
    monkeypatch.setenv("TDX_DB_DEV_PORT", "5432")
    monkeypatch.setenv("TDX_DB_DEV_NAME", "production_like")
    monkeypatch.setenv("TDX_DB_DEV_USER", "user")
    monkeypatch.setenv("TDX_DB_DEV_PASSWORD", "password")
    with pytest.raises(finalizer.AdvisoryPhase0AFinalizerCommandError, match="refusing dev target"):
        finalizer._db_config(target_db="dev")
    with pytest.raises(finalizer.AdvisoryPhase0AFinalizerCommandError, match="production authority mutation"):
        with finalizer._write_conn_factory(
            env_file=None,
            target_db="prod",
            allow_production_authority=False,
        ):
            pass
