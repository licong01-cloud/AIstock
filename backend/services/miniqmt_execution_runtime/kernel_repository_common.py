"""Shared exceptions, connection ownership, and generic DB codecs for K2-A repository."""

from __future__ import annotations

from contextlib import contextmanager
import inspect
import json
from typing import Any, Callable, Iterator

import psycopg2
import psycopg2.extras

from backend.db.pg_pool import get_conn


class KernelRepositoryConflict(RuntimeError):
    """A durable identity or CAS version conflicts with persisted facts."""


class KernelRepositorySchemaError(RuntimeError):
    """The K2 schema is absent or only partially installed."""


class KernelRepositoryCommitUnknown(RuntimeError):
    """The database may have committed, but the transaction return was not observed."""


def _accepts_keyword(factory: Any, keyword: str) -> bool:
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        return False
    return keyword in signature.parameters or any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()
    )


def _json(value: Any) -> psycopg2.extras.Json:
    return psycopg2.extras.Json(value)


def _row_json(row: Any, key: str) -> dict[str, Any]:
    value = row[key]
    if not isinstance(value, dict):
        raise KernelRepositoryConflict(f"durable {key} is not a JSON object")
    return value


def _model_from_json(model: Any, value: dict[str, Any]) -> Any:
    return model.model_validate_json(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _bounded_limit(limit: int) -> int:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("limit must be a strict integer in [1, 1000]")
    return limit


class KernelRepositoryBase:
    """Own the only connection factory and transaction boundary for repository mixins."""

    def __init__(self, conn_factory: Callable[..., Any] = get_conn) -> None:
        self._conn_factory = conn_factory
        self._accepts_autocommit = _accepts_keyword(conn_factory, "autocommit")
        self._accepts_manage_transaction = _accepts_keyword(conn_factory, "manage_transaction")

    @contextmanager
    def _connection(self, *, transaction: bool) -> Iterator[Any]:
        kwargs: dict[str, Any] = {}
        if self._accepts_autocommit:
            kwargs["autocommit"] = not transaction
        if self._accepts_manage_transaction:
            kwargs["manage_transaction"] = transaction
        with self._conn_factory(**kwargs) as conn:
            yield conn

    def _verify_lease_owner(self, lease_owner: str) -> None:
        worker_id, separator, process_incarnation_id = lease_owner.partition(":")
        if not separator or not worker_id or not process_incarnation_id:
            raise ValueError("lease_owner must be worker_id:process_incarnation_id")
        with self._connection(transaction=False) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT 1 FROM qmt_strategy.execution_kernel_worker_incarnation
                        WHERE worker_id=%s AND process_incarnation_id=%s
                        """,
                    (worker_id, process_incarnation_id),
                )
                exists = cur.fetchone() is not None
        if not exists:
            raise KernelRepositoryConflict("lease owner references unknown worker incarnation")
