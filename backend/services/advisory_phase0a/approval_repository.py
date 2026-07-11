"""Append-only PostgreSQL persistence for Phase 0A.1 authority evidence."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from psycopg2.extras import Json

from .authority import (
    ApprovalBundle,
    ApprovalDecisionEvent,
    HandoffBundle,
    OperationAuthorizationEvent,
    Phase0AAuthorityError,
)


class PostgresApprovalAuthorityRepository:
    """Persist immutable authority records using the authenticated DB principal."""

    def __init__(self, *, conn_factory: Callable[[], Iterator[Any]]) -> None:
        self._conn_factory = conn_factory

    def current_actor(self) -> str:
        with self._connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT current_user")
                row = cursor.fetchone()
        actor = str(row[0] if row else "").strip()
        if not actor:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_MISSING: authenticated actor")
        return actor

    def list_decisions(self, *, handoff: HandoffBundle) -> list[ApprovalDecisionEvent]:
        return self._list_decisions(
            audit_id=handoff.audit_id,
            phase1_handoff_bundle_hash=handoff.phase1_handoff_bundle_hash,
        )

    def list_decisions_for_bundle(self, *, bundle: ApprovalBundle) -> list[ApprovalDecisionEvent]:
        return self._list_decisions(
            audit_id=bundle.audit_id,
            phase1_handoff_bundle_hash=bundle.phase1_handoff_bundle_hash,
        )

    def _list_decisions(
        self,
        *,
        audit_id: str,
        phase1_handoff_bundle_hash: str,
    ) -> list[ApprovalDecisionEvent]:
        query = """
            SELECT payload_json
            FROM app.advisory_phase0a_approval_event
            WHERE audit_id = %s AND phase1_handoff_bundle_hash = %s
            ORDER BY decision_at, decision_hash
        """
        return [
            ApprovalDecisionEvent.model_validate(_json_value(row[0]))
            for row in self._fetchall(query, (audit_id, phase1_handoff_bundle_hash))
        ]

    def append_decision(self, event: ApprovalDecisionEvent) -> ApprovalDecisionEvent:
        query = """
            INSERT INTO app.advisory_phase0a_approval_event (
                decision_id, decision_kind, event_type, audit_id, audit_manifest_hash,
                request_hash, initial_approval_receipt_hash, phase1_handoff_bundle_hash,
                audit_target_id, target_handoff_hash, admission_scope_id, admission_scope_hash,
                previous_terminal_decision_hash, revokes_decision_hash, decision_hash,
                actor_principal, authority_backend_id, decision_at, payload_json
            ) VALUES (
                %(decision_id)s, %(decision_kind)s, %(event_type)s, %(audit_id)s, %(audit_manifest_hash)s,
                %(request_hash)s, %(initial_approval_receipt_hash)s, %(phase1_handoff_bundle_hash)s,
                %(audit_target_id)s, %(target_handoff_hash)s, %(admission_scope_id)s, %(admission_scope_hash)s,
                %(previous_terminal_decision_hash)s, %(revokes_decision_hash)s, %(decision_hash)s,
                %(actor_principal)s, %(authority_backend_id)s, %(decision_at)s, %(payload_json)s
            )
            ON CONFLICT (decision_hash) DO NOTHING
        """
        payload = event.model_dump(mode="json")
        params = {
            **payload,
            "decision_kind": event.decision_kind.value,
            "event_type": event.event_type.value,
            "payload_json": Json(payload),
        }
        self._execute_write(query, params)
        return event

    def append_bundle(self, bundle: ApprovalBundle) -> ApprovalBundle:
        bundle_query = """
            INSERT INTO app.advisory_phase0a_approval_bundle (
                approval_bundle_id, schema_version, audit_id, audit_manifest_hash, request_hash,
                initial_approval_receipt_hash, phase1_handoff_bundle_hash, global_terminal_decision_hash,
                admission_scope_set_hash, scope_member_count, authority_backend_id, authority_backend_hash,
                approval_bundle_content_hash, created_by, created_at, payload_json
            ) VALUES (
                %(approval_bundle_id)s, %(schema_version)s, %(audit_id)s, %(audit_manifest_hash)s, %(request_hash)s,
                %(initial_approval_receipt_hash)s, %(phase1_handoff_bundle_hash)s, %(global_terminal_decision_hash)s,
                %(admission_scope_set_hash)s, %(scope_member_count)s, %(authority_backend_id)s, %(authority_backend_hash)s,
                %(approval_bundle_content_hash)s, %(created_by)s, %(created_at)s, %(payload_json)s
            ) ON CONFLICT (approval_bundle_content_hash) DO NOTHING
        """
        scope_query = """
            INSERT INTO app.advisory_phase0a_approval_bundle_scope (
                approval_bundle_id, admission_scope_id, admission_scope_hash, terminal_decision_hash,
                allowed_evidence_scope, scope_member_content_hash
            ) VALUES (%(approval_bundle_id)s, %(admission_scope_id)s, %(admission_scope_hash)s,
                %(terminal_decision_hash)s, %(allowed_evidence_scope)s, %(scope_member_content_hash)s)
            ON CONFLICT (approval_bundle_id, admission_scope_id) DO NOTHING
        """
        with self._connection() as conn:
            with conn.cursor() as cursor:
                payload = bundle.model_dump(mode="json")
                cursor.execute(bundle_query, {**payload, "payload_json": Json(payload)})
                for scope in bundle.scopes:
                    scope_payload = scope.model_dump(mode="json")
                    cursor.execute(
                        scope_query,
                        {
                            "approval_bundle_id": bundle.approval_bundle_id,
                            **scope_payload,
                            "allowed_evidence_scope": scope.allowed_evidence_scope.value,
                        },
                    )
            conn.commit()
        return bundle

    def get_approval_bundle(self, *, approval_bundle_content_hash: str) -> ApprovalBundle | None:
        query = """
            SELECT payload_json
            FROM app.advisory_phase0a_approval_bundle
            WHERE approval_bundle_content_hash = %s
        """
        rows = self._fetchall(query, (approval_bundle_content_hash,))
        if not rows:
            return None
        if len(rows) != 1:
            raise Phase0AAuthorityError("ADVISORY_PHASE1_OPERATION_AUTHORIZATION_SCOPE_MISMATCH: duplicate approval bundle")
        return ApprovalBundle.model_validate(_json_value(rows[0][0]))

    def list_operation_authorizations(self, *, authorization_id: str) -> list[OperationAuthorizationEvent]:
        query = """
            SELECT payload_json
            FROM app.advisory_phase1_operation_authorization_event
            WHERE authorization_id = %s
            ORDER BY event_at, authorization_event_hash
        """
        return [
            OperationAuthorizationEvent.model_validate(_json_value(row[0]))
            for row in self._fetchall(query, (authorization_id,))
        ]

    def append_operation_authorization(self, event: OperationAuthorizationEvent) -> OperationAuthorizationEvent:
        query = """
            INSERT INTO app.advisory_phase1_operation_authorization_event (
                authorization_id, schema_version, event_type, operation_type, environment,
                approval_bundle_hash, admission_scope_set_hash, governance_scope_hash,
                operation_payload_hash, max_rows, max_bytes, valid_from, expires_at,
                previous_event_hash, revokes_event_hash, actor_principal, event_at,
                approval_reference, authorization_event_hash, payload_json
            ) VALUES (
                %(authorization_id)s, %(schema_version)s, %(event_type)s, %(operation_type)s, %(environment)s,
                %(approval_bundle_hash)s, %(admission_scope_set_hash)s, %(governance_scope_hash)s,
                %(operation_payload_hash)s, %(max_rows)s, %(max_bytes)s, %(valid_from)s, %(expires_at)s,
                %(previous_event_hash)s, %(revokes_event_hash)s, %(actor_principal)s, %(event_at)s,
                %(approval_reference)s, %(authorization_event_hash)s, %(payload_json)s
            ) ON CONFLICT (authorization_event_hash) DO NOTHING
        """
        payload = event.model_dump(mode="json")
        self._execute_write(
            query,
            {**payload, "event_type": event.event_type.value, "payload_json": Json(payload)},
        )
        return event

    def _fetchall(self, query: str, params: tuple[Any, ...]) -> list[Any]:
        with self._connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return list(cursor.fetchall())

    def _execute_write(self, query: str, params: dict[str, Any]) -> None:
        with self._connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
            conn.commit()

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        with self._conn_factory() as conn:
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise


def _json_value(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, dict):
            return decoded
    raise Phase0AAuthorityError("ADVISORY_PHASE0A_APPROVAL_CHAIN_INVALID: malformed database payload")


__all__ = ["PostgresApprovalAuthorityRepository"]
