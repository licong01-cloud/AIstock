"""Validation-only single-transaction coordinator for Phase 1G G5 L3."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable, Literal

import sqlparse
from sqlparse.sql import Statement
from sqlparse.tokens import DML, Keyword, Name, String, Whitespace

from backend.services.advisory_phase0a.policy import canonical_json_sha256

from .phase1g_dev_evidence_contract import (
    Phase1GDevEvidenceError,
    Phase1GDevQueryEvidence,
    REASON_L3_COORDINATOR_INVALID,
    REASON_L3_FORBIDDEN_SQL,
    REASON_L3_ROLLBACK_FAILED,
)


APPROVED_WRITE_RELATIONS = frozenset(
    {
        "app.advisory_phase1_control_binding_event",
        "app.advisory_capture_batch",
        "app.advisory_capture_plan",
        "app.advisory_capture_batch_evidence_membership",
        "app.advisory_source_revision_set",
        "app.advisory_source_revision_member",
        "app.advisory_selection_stage_trace_outbox",
        "app.advisory_selection_stage_trace_delivery_event",
        "app.advisory_signal_observation",
        "app.advisory_signal_observation_version",
        "app.advisory_signal_observation_lineage_identity",
        "app.advisory_signal_observation_lineage_payload",
        "app.advisory_signal_stage_evidence",
        "app.advisory_signal_stage_candidate_identity",
        "app.advisory_signal_stage_candidate_payload",
    }
)

_EXACT_TRANSACTION_SETUP = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
_EXACT_READ_TRANSACTION_SETUP = (
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
)
_SAFE_CONNECTION_ATTRIBUTES = frozenset(
    {"encoding", "server_version", "status", "info", "notices", "protocol_version"}
)


@dataclass(frozen=True)
class Phase1GDevQuerySummary:
    evidence: tuple[Phase1GDevQueryEvidence, ...]
    read_query_count: int
    write_query_count: int
    normalized_query_set_hash: str
    write_relation_set: tuple[str, ...]
    observed_transactional_dml: bool


class Phase1GDevQueryRecorder:
    def __init__(self) -> None:
        self._evidence: list[Phase1GDevQueryEvidence] = []

    def record(
        self,
        *,
        statement_type: str,
        relations: Iterable[str],
        normalized_sql: str,
        facade_mode: Literal["read", "write", "owner"],
    ) -> None:
        self._evidence.append(
            Phase1GDevQueryEvidence(
                statement_type=statement_type,
                relation_names=tuple(sorted(set(relations))),
                normalized_sql_hash=canonical_json_sha256(normalized_sql),
                facade_mode=facade_mode,
            )
        )

    def summary(self) -> Phase1GDevQuerySummary:
        evidence = tuple(self._evidence)
        writes = tuple(
            item
            for item in evidence
            if item.statement_type in {"INSERT", "UPDATE"}
        )
        reads = tuple(item for item in evidence if item not in writes)
        relations = tuple(
            sorted(
                {
                    relation
                    for item in writes
                    for relation in item.relation_names
                }
            )
        )
        return Phase1GDevQuerySummary(
            evidence=evidence,
            read_query_count=len(reads),
            write_query_count=len(writes),
            normalized_query_set_hash=canonical_json_sha256(
                [item.model_dump(mode="json") for item in evidence]
            ),
            write_relation_set=relations,
            observed_transactional_dml=bool(writes),
        )


class Phase1GDevRollbackCoordinator:
    """Own exactly one physical transaction and expose non-finalizing facades."""

    def __init__(
        self,
        *,
        connection_factory,  # type: ignore[no-untyped-def]
        application_name: str,
        statement_timeout_ms: int,
        lock_timeout_ms: int,
    ) -> None:
        if statement_timeout_ms <= 0 or lock_timeout_ms <= 0:
            raise Phase1GDevEvidenceError(
                REASON_L3_COORDINATOR_INVALID,
                "rollback coordinator timeouts must be positive",
            )
        self._connection_factory = connection_factory
        self._application_name = _application_name(application_name)
        self._statement_timeout_ms = statement_timeout_ms
        self._lock_timeout_ms = lock_timeout_ms
        self._connection: Any | None = None
        self._recorder = Phase1GDevQueryRecorder()
        self._physical_rollback_count = 0
        self._facade_finalize_counts = {"commit": 0, "rollback": 0, "close": 0}
        self._entered = False

    @property
    def recorder(self) -> Phase1GDevQueryRecorder:
        return self._recorder

    @property
    def physical_rollback_count(self) -> int:
        return self._physical_rollback_count

    @property
    def physical_commit_count(self) -> int:
        return 0

    @property
    def facade_finalize_counts(self) -> dict[str, int]:
        return dict(self._facade_finalize_counts)

    def __enter__(self) -> "Phase1GDevRollbackCoordinator":
        if self._entered:
            raise Phase1GDevEvidenceError(
                REASON_L3_COORDINATOR_INVALID,
                "rollback coordinator cannot be re-entered",
            )
        connection = self._connection_factory()
        if bool(getattr(connection, "autocommit", False)):
            try:
                connection.close()
            finally:
                raise Phase1GDevEvidenceError(
                    REASON_L3_COORDINATOR_INVALID,
                    "rollback coordinator connection must disable autocommit",
                )
        self._connection = connection
        self._entered = True
        try:
            with connection.cursor() as cur:
                cur.execute(_EXACT_TRANSACTION_SETUP)
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (str(self._statement_timeout_ms),),
                )
                cur.execute(
                    "SELECT set_config('lock_timeout', %s, true)",
                    (str(self._lock_timeout_ms),),
                )
                cur.execute(
                    "SELECT set_config('application_name', %s, true)",
                    (self._application_name,),
                )
            for sql in (
                _EXACT_TRANSACTION_SETUP,
                "SELECT set_config('statement_timeout', ?, true)",
                "SELECT set_config('lock_timeout', ?, true)",
                "SELECT set_config('application_name', ?, true)",
            ):
                self._recorder.record(
                    statement_type="SETUP",
                    relations=(),
                    normalized_sql=sql,
                    facade_mode="owner",
                )
        except Exception:
            self._rollback_and_close()
            raise
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        del exc_type, exc, traceback
        self._rollback_and_close()

    def transaction_connection_factory(self) -> "_ConnectionFacade":
        return self._facade(mode="write")

    def readonly_connection_factory(self) -> "_ConnectionFacade":
        return self._facade(mode="read")

    def owner_cursor(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        if self._connection is None:
            raise Phase1GDevEvidenceError(
                REASON_L3_COORDINATOR_INVALID,
                "rollback coordinator is not active",
            )
        return self._connection.cursor(*args, **kwargs)

    def _facade(self, *, mode: Literal["read", "write"]) -> "_ConnectionFacade":
        if self._connection is None:
            raise Phase1GDevEvidenceError(
                REASON_L3_COORDINATOR_INVALID,
                "rollback coordinator is not active",
            )
        return _ConnectionFacade(coordinator=self, mode=mode)

    def _finalize_requested(self, operation: Literal["commit", "rollback", "close"]) -> None:
        self._facade_finalize_counts[operation] += 1

    def _rollback_and_close(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        rollback_error: Exception | None = None
        try:
            connection.rollback()
            self._physical_rollback_count += 1
        except Exception as exc:  # noqa: BLE001
            rollback_error = exc
        finally:
            try:
                connection.close()
            except Exception as exc:  # noqa: BLE001
                rollback_error = rollback_error or exc
        if rollback_error is not None:
            raise Phase1GDevEvidenceError(
                REASON_L3_ROLLBACK_FAILED,
                "physical rollback or close failed",
                context={"exception_type": type(rollback_error).__name__},
            ) from rollback_error


class _ConnectionFacade:
    def __init__(
        self,
        *,
        coordinator: Phase1GDevRollbackCoordinator,
        mode: Literal["read", "write"],
    ) -> None:
        self._coordinator = coordinator
        self._mode = mode

    @property
    def autocommit(self) -> bool:
        return False

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        raise Phase1GDevEvidenceError(
            REASON_L3_COORDINATOR_INVALID,
            "facade cannot change autocommit",
            context={"requested_value": bool(value)},
        )

    @property
    def closed(self) -> int:
        return 0

    def cursor(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        connection = self._physical()
        return _CursorFacade(
            cursor=connection.cursor(*args, **kwargs),
            recorder=self._coordinator.recorder,
            mode=self._mode,
        )

    def set_session(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        if args:
            raise Phase1GDevEvidenceError(
                REASON_L3_COORDINATOR_INVALID,
                "facade set_session only accepts exact keyword arguments",
            )
        exact = {
            "readonly": True,
            "autocommit": False,
            "isolation_level": "REPEATABLE READ",
        }
        if self._mode != "read" or kwargs != exact:
            raise Phase1GDevEvidenceError(
                REASON_L3_COORDINATOR_INVALID,
                "facade rejected transaction characteristic change",
            )
        self._coordinator.recorder.record(
            statement_type="SETUP",
            relations=(),
            normalized_sql="READ_SCOPE_SET_SESSION_VALIDATED_NOOP",
            facade_mode="read",
        )

    def commit(self) -> None:
        self._coordinator._finalize_requested("commit")

    def rollback(self) -> None:
        self._coordinator._finalize_requested("rollback")

    def close(self) -> None:
        self._coordinator._finalize_requested("close")

    def __enter__(self) -> "_ConnectionFacade":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        del exc_type, exc, traceback

    def __getattr__(self, name: str) -> Any:
        if name in _SAFE_CONNECTION_ATTRIBUTES:
            return getattr(self._physical(), name)
        raise Phase1GDevEvidenceError(
            REASON_L3_COORDINATOR_INVALID,
            "facade rejected unsupported connection operation",
            context={"operation": name},
        )

    def _physical(self) -> Any:
        connection = self._coordinator._connection
        if connection is None:
            raise Phase1GDevEvidenceError(
                REASON_L3_COORDINATOR_INVALID,
                "facade used after outer transaction finished",
            )
        return connection


class _CursorFacade:
    def __init__(
        self,
        *,
        cursor: Any,
        recorder: Phase1GDevQueryRecorder,
        mode: Literal["read", "write"],
    ) -> None:
        self._cursor = cursor
        self._recorder = recorder
        self._mode = mode

    def execute(self, sql: Any, params: Any = None) -> Any:
        action = _classify_sql(sql=str(sql), mode=self._mode)
        self._recorder.record(
            statement_type=action.statement_type,
            relations=action.relations,
            normalized_sql=action.normalized_sql,
            facade_mode=self._mode,
        )
        if action.noop:
            return None
        return self._cursor.execute(sql, params)

    def executemany(self, sql: Any, params_seq: Any) -> Any:
        action = _classify_sql(sql=str(sql), mode=self._mode)
        if action.noop:
            raise Phase1GDevEvidenceError(
                REASON_L3_FORBIDDEN_SQL,
                "transaction setup cannot use executemany",
            )
        self._recorder.record(
            statement_type=action.statement_type,
            relations=action.relations,
            normalized_sql=action.normalized_sql,
            facade_mode=self._mode,
        )
        return self._cursor.executemany(sql, params_seq)

    def __enter__(self) -> "_CursorFacade":
        self._cursor.__enter__()
        return self

    def __exit__(self, exc_type, exc, traceback) -> Any:  # type: ignore[no-untyped-def]
        return self._cursor.__exit__(exc_type, exc, traceback)

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self._cursor)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


@dataclass(frozen=True)
class _SqlAction:
    statement_type: str
    relations: tuple[str, ...]
    normalized_sql: str
    noop: bool = False


def _classify_sql(
    *,
    sql: str,
    mode: Literal["read", "write"],
) -> _SqlAction:
    statements = tuple(item for item in sqlparse.parse(sql) if str(item).strip())
    if len(statements) != 1:
        raise _forbidden("exactly one SQL statement is required")
    statement = statements[0]
    normalized = _normalize_sql(statement)
    if normalized == _EXACT_TRANSACTION_SETUP:
        if mode != "write":
            raise _forbidden("transaction setup is only valid on the write facade")
        return _SqlAction("SETUP", (), normalized, noop=True)
    if normalized == _EXACT_READ_TRANSACTION_SETUP:
        if mode != "read":
            raise _forbidden("read transaction setup is only valid on the read facade")
        return _SqlAction("SETUP", (), normalized, noop=True)
    statement_type = statement.get_type().upper()
    leading = _leading_keyword(statement)
    forbidden_commands = {
        "ALTER",
        "CALL",
        "COPY",
        "CREATE",
        "DELETE",
        "DO",
        "DROP",
        "GRANT",
        "MERGE",
        "REVOKE",
        "TRUNCATE",
    }
    if leading in forbidden_commands:
        raise _forbidden("SQL statement type is forbidden", types=[leading])
    relations: tuple[str, ...] = ()
    if _starts_with(statement, {"WITH"}):
        actions = _write_actions(statement)
        forbidden = [action for action, _relation in actions if action in {"DELETE", "MERGE"}]
        if forbidden:
            raise _forbidden("CTE write type is forbidden", types=forbidden)
        if len(actions) > 1:
            raise _forbidden(
                "CTE must contain exactly one database write action",
                types=[action for action, _relation in actions],
                relations=[relation for _action, relation in actions if relation],
            )
        effective_type = actions[0][0] if actions else None
        relations = (actions[0][1],) if actions and actions[0][1] else ()
    else:
        effective_type = (
            statement_type if statement_type in {"INSERT", "UPDATE"} else None
        )
    if effective_type is None and statement_type in {"SELECT", "UNKNOWN"} and (
        _starts_with(statement, {"SELECT", "SHOW", "WITH"})
    ):
        return _SqlAction("SELECT", (), normalized)
    if mode == "read":
        raise _forbidden("read facade rejected non-read SQL", types=[statement_type])
    if effective_type not in {"INSERT", "UPDATE"}:
        raise _forbidden("write facade rejected unsupported SQL", types=[statement_type])
    if not relations:
        relations = _write_relations(
            statement=statement,
            statement_type=effective_type,
        )
    if len(relations) != 1 or relations[0] not in APPROVED_WRITE_RELATIONS:
        raise _forbidden("write relation is outside the G5 Advisory allowlist", relations=relations)
    return _SqlAction(effective_type, relations, normalized)


def _normalize_sql(statement: Statement) -> str:
    value = sqlparse.format(
        str(statement),
        keyword_case="upper",
        identifier_case="lower",
        strip_comments=True,
        reindent=False,
    )
    return re.sub(r"\s+", " ", value).strip().rstrip(";")


def _starts_with(statement: Statement, accepted: set[str]) -> bool:
    for token in statement.flatten():
        if token.ttype in Whitespace or token.ttype in String:
            continue
        return str(token.normalized).upper() in accepted
    return False


def _leading_keyword(statement: Statement) -> str:
    for token in statement.flatten():
        if token.ttype in Whitespace or token.ttype in String:
            continue
        return str(token.normalized).upper()
    return ""


def _write_actions(statement: Statement) -> tuple[tuple[str, str], ...]:
    flattened = tuple(statement.flatten())
    actions: list[tuple[str, str]] = []
    for index, token in enumerate(flattened):
        action = str(token.normalized).upper()
        if token.ttype not in DML or action not in {
            "INSERT",
            "UPDATE",
            "DELETE",
            "MERGE",
        }:
            continue
        previous = _previous_significant_token(flattened, index)
        if action == "UPDATE" and previous in {"DO", "FOR"}:
            continue
        relation = ""
        if action == "INSERT":
            anchor_index = next(
                (
                    candidate
                    for candidate in range(index + 1, len(flattened))
                    if str(flattened[candidate].normalized).upper() == "INTO"
                ),
                None,
            )
            if anchor_index is not None:
                relation = _relation_after(flattened, anchor_index)
        elif action == "UPDATE":
            relation = _relation_after(flattened, index)
        actions.append((action, relation))
    return tuple(actions)


def _previous_significant_token(tokens: tuple[Any, ...], index: int) -> str:
    for token in reversed(tokens[:index]):
        if token.ttype in Whitespace:
            continue
        return str(token.normalized).upper()
    return ""


def _relation_after(tokens: tuple[Any, ...], anchor_index: int) -> str:
    parts: list[str] = []
    started = False
    for token in tokens[anchor_index + 1 :]:
        text = str(token.value).strip()
        if not text:
            if started:
                break
            continue
        if token.ttype in Name or text in {".", '"'}:
            started = True
            parts.append(text.strip('"').lower())
            continue
        if started:
            break
    return "".join(parts)


def _write_relations(*, statement: Statement, statement_type: str) -> tuple[str, ...]:
    flattened = tuple(statement.flatten())
    anchor = "INTO" if statement_type == "INSERT" else "UPDATE"
    anchor_index = next(
        (
            index
            for index, token in enumerate(flattened)
            if str(token.normalized).upper() == anchor
            and token.ttype in {Keyword, DML}
        ),
        None,
    )
    if anchor_index is None:
        return ()
    relation = _relation_after(flattened, anchor_index)
    return (relation,) if relation else ()


def _forbidden(
    message: str,
    *,
    types: list[str] | None = None,
    relations: Iterable[str] = (),
) -> Phase1GDevEvidenceError:
    return Phase1GDevEvidenceError(
        REASON_L3_FORBIDDEN_SQL,
        message,
        context={
            "statement_types": types or [],
            "relations": sorted(set(relations)),
        },
    )


def _application_name(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 63 or not re.fullmatch(r"[A-Za-z0-9_.:-]+", normalized):
        raise Phase1GDevEvidenceError(
            REASON_L3_COORDINATOR_INVALID,
            "rollback application_name is invalid",
        )
    return normalized
