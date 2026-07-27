"""Read-only, keyset-paginated projections for historical-range research."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from typing import Any

import psycopg2.extras

from .api_models import json_ready
from .canonical import canonical_json_sha256


class HistoricalRangeQueryError(ValueError):
    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class HistoricalRangeNotFoundError(HistoricalRangeQueryError):
    pass


class HistoricalRangeCursorCodec:
    schema_version = "advisory_historical_range_cursor_v1"

    @classmethod
    def encode(cls, *, order_key: Sequence[Any], filter_payload: Mapping[str, Any]) -> str:
        payload = {
            "schema_version": cls.schema_version,
            "order_key": json_ready(list(order_key)),
            "filter_hash": canonical_json_sha256(dict(filter_payload)),
        }
        raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @classmethod
    def decode(
        cls,
        cursor: str | None,
        *,
        filter_payload: Mapping[str, Any],
        key_size: int,
    ) -> tuple[Any, ...] | None:
        if cursor is None:
            return None
        try:
            if not cursor or len(cursor) > 4096:
                raise ValueError("cursor length is invalid")
            raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
            payload = json.loads(raw.decode("ascii"))
            if not isinstance(payload, dict):
                raise ValueError("cursor payload is not an object")
            if payload.get("schema_version") != cls.schema_version:
                raise ValueError("cursor schema version differs")
            expected_hash = canonical_json_sha256(dict(filter_payload))
            if payload.get("filter_hash") != expected_hash:
                raise ValueError("cursor filters differ from this request")
            order_key = payload.get("order_key")
            if not isinstance(order_key, list) or len(order_key) != key_size:
                raise ValueError("cursor order key is invalid")
            if any(isinstance(item, (dict, list)) for item in order_key):
                raise ValueError("cursor order key contains a composite value")
            return tuple(order_key)
        except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise HistoricalRangeQueryError(
                "ADVISORY_HR_CURSOR_INVALID",
                "historical-range cursor is invalid for this request",
            ) from exc


class PostgresHistoricalRangeQueryRepository:
    """Every method owns one short REPEATABLE READ, READ ONLY transaction."""

    def __init__(self, *, conn_factory: Callable[[], Any]) -> None:
        if conn_factory is None:
            raise ValueError("historical-range query repository requires conn_factory")
        self._conn_factory = conn_factory

    def list_batches(
        self,
        *,
        statuses: Sequence[str] = (),
        program_id: str | None = None,
        created_before: datetime | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        limit = _limit(limit)
        filters = {
            "statuses": sorted(set(statuses)),
            "program_id": program_id,
            "created_before": created_before.isoformat() if created_before else None,
        }
        key = HistoricalRangeCursorCodec.decode(cursor, filter_payload=filters, key_size=2)
        where = ["TRUE"]
        params: list[Any] = []
        if statuses:
            where.append("batch.status = ANY(%s)")
            params.append(list(statuses))
        if program_id:
            where.append(
                "EXISTS (SELECT 1 FROM app.advisory_historical_range_run r "
                "WHERE r.batch_id = batch.batch_id AND r.research_program_id = %s)"
            )
            params.append(program_id)
        if created_before:
            where.append("batch.created_at < %s")
            params.append(created_before)
        if key:
            where.append("(batch.created_at, batch.batch_id) < (%s::timestamptz, %s)")
            params.extend(key)
        rows = self._rows(
            f"""
            SELECT batch.*,
                   EXISTS (
                       SELECT 1 FROM app.advisory_historical_range_operation operation
                       WHERE operation.batch_id = batch.batch_id
                         AND operation.operation_type = 'BUILD_SOURCE_CATALOG'
                         AND operation.status IN ('QUEUED', 'WAITING_INPUT')
                   ) OR EXISTS (
                       SELECT 1 FROM app.advisory_historical_range_operation operation
                       WHERE operation.batch_id = batch.batch_id
                         AND operation.operation_type = 'BUILD_SOURCE_CATALOG'
                         AND operation.status = 'RUNNING'
                         AND operation.lease_expires_at <= clock_timestamp()
                   ) AS planning_recoverable,
                   current_operation.operation_id AS current_operation_id,
                   current_operation.operation_type AS current_operation_type,
                   current_operation.status AS current_operation_status
            FROM app.advisory_historical_range_batch batch
            LEFT JOIN LATERAL (
                SELECT operation_id, operation_type, status
                FROM app.advisory_historical_range_operation operation
                WHERE operation.batch_id = batch.batch_id
                ORDER BY operation.created_at DESC, operation.operation_id DESC
                LIMIT 1
            ) current_operation ON TRUE
            WHERE {' AND '.join(where)}
            ORDER BY batch.created_at DESC, batch.batch_id DESC
            LIMIT %s
            """,
            (*params, limit + 1),
        )
        return _page(rows, limit, filters, lambda row: (row["created_at"], row["batch_id"]))

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        rows = self._rows(
            """
            SELECT batch.*,
                   catalog.operation_id AS catalog_operation_id,
                   catalog.status AS catalog_operation_status,
                   catalog.lease_expires_at AS catalog_lease_expires_at,
                   (catalog.status = 'RUNNING' AND catalog.lease_expires_at <= clock_timestamp()) AS catalog_lease_expired,
                   (batch.status = 'PLANNING' AND catalog.status IN ('QUEUED', 'WAITING_INPUT')) OR
                   (batch.status = 'PLANNING' AND catalog.status = 'RUNNING' AND catalog.lease_expires_at <= clock_timestamp())
                       AS planning_recoverable
            FROM app.advisory_historical_range_batch batch
            LEFT JOIN LATERAL (
                SELECT * FROM app.advisory_historical_range_operation operation
                WHERE operation.batch_id = batch.batch_id
                  AND operation.operation_type = 'BUILD_SOURCE_CATALOG'
                ORDER BY operation.created_at DESC LIMIT 1
            ) catalog ON TRUE
            WHERE batch.batch_id = %s
            """,
            (batch_id,),
        )
        return _one(rows, "batch", batch_id)

    def list_runs(self, *, batch_id: str, cursor: str | None = None, limit: int = 50) -> dict[str, Any]:
        filters = {"batch_id": batch_id}
        key = HistoricalRangeCursorCodec.decode(cursor, filter_payload=filters, key_size=2)
        where = ["run.batch_id = %s"]
        params: list[Any] = [batch_id]
        if key:
            where.append("(run.research_program_id, run.range_run_id) > (%s, %s)")
            params.extend(key)
        rows = self._rows(
            f"""
            SELECT run.*, batch.trade_date_count AS total_day_count,
                   latest_day.decision_trade_date AS latest_successful_trade_date,
                   summary.summary_id AS latest_summary_id,
                   summary.summary_version AS latest_summary_version
            FROM app.advisory_historical_range_run run
            JOIN app.advisory_historical_range_batch batch ON batch.batch_id = run.batch_id
            LEFT JOIN LATERAL (
                SELECT decision_trade_date FROM app.advisory_historical_range_day_run day_run
                WHERE day_run.range_run_id = run.range_run_id
                  AND day_run.status IN ('COMPLETE', 'VALID_NO_CANDIDATE')
                ORDER BY day_run.ordinal DESC LIMIT 1
            ) latest_day ON TRUE
            LEFT JOIN LATERAL (
                SELECT summary_id, summary_version FROM app.advisory_historical_range_summary summary
                WHERE summary.range_run_id = run.range_run_id
                ORDER BY summary.summary_version DESC, summary.summary_id DESC LIMIT 1
            ) summary ON TRUE
            WHERE {' AND '.join(where)}
            ORDER BY run.research_program_id, run.range_run_id
            LIMIT %s
            """,
            (*params, _limit(limit) + 1),
        )
        return _page(rows, _limit(limit), filters, lambda row: (row["research_program_id"], row["range_run_id"]))

    def get_run(self, range_run_id: str) -> dict[str, Any]:
        rows = self._rows(
            """
            SELECT run.*, batch.trade_date_count AS total_day_count,
                   summary.summary_id AS latest_summary_id,
                   summary.summary_version AS latest_summary_version,
                   summary.summary_artifact_ref AS latest_summary_ref
            FROM app.advisory_historical_range_run run
            JOIN app.advisory_historical_range_batch batch ON batch.batch_id = run.batch_id
            LEFT JOIN LATERAL (
                SELECT * FROM app.advisory_historical_range_summary summary
                WHERE summary.range_run_id = run.range_run_id
                ORDER BY summary.summary_version DESC, summary.summary_id DESC LIMIT 1
            ) summary ON TRUE
            WHERE run.range_run_id = %s
            """,
            (range_run_id,),
        )
        return _one(rows, "run", range_run_id)

    def list_operations(
        self,
        *,
        batch_id: str,
        operation_types: Sequence[str] = (),
        statuses: Sequence[str] = (),
        cursor: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        filters = {
            "batch_id": batch_id,
            "operation_types": sorted(set(operation_types)),
            "statuses": sorted(set(statuses)),
        }
        key = HistoricalRangeCursorCodec.decode(cursor, filter_payload=filters, key_size=2)
        where = ["operation.batch_id = %s"]
        params: list[Any] = [batch_id]
        if operation_types:
            where.append("operation.operation_type = ANY(%s)")
            params.append(list(operation_types))
        if statuses:
            where.append("operation.status = ANY(%s)")
            params.append(list(statuses))
        if key:
            where.append("(operation.created_at, operation.operation_id) < (%s::timestamptz, %s)")
            params.extend(key)
        rows = self._rows(
            f"""
            SELECT operation.*,
                   (operation.status = 'RUNNING' AND operation.lease_expires_at <= clock_timestamp()) AS lease_expired
            FROM app.advisory_historical_range_operation operation
            WHERE {' AND '.join(where)}
            ORDER BY operation.created_at DESC, operation.operation_id DESC
            LIMIT %s
            """,
            (*params, _limit(limit) + 1),
        )
        return _page(rows, _limit(limit), filters, lambda row: (row["created_at"], row["operation_id"]))

    def get_operation(self, operation_id: str) -> dict[str, Any]:
        rows = self._rows(
            """
            SELECT operation.*,
                   (operation.status = 'RUNNING' AND operation.lease_expires_at <= clock_timestamp()) AS lease_expired
            FROM app.advisory_historical_range_operation operation
            WHERE operation.operation_id = %s
            """,
            (operation_id,),
        )
        return _one(rows, "operation", operation_id)

    def get_operation_internal(self, operation_id: str) -> dict[str, Any]:
        """Return durable lease identity for a background worker, never for HTTP projection."""
        rows = self._rows(
            """
            SELECT operation.*,
                   (operation.status = 'RUNNING' AND operation.lease_expires_at <= clock_timestamp()) AS lease_expired
            FROM app.advisory_historical_range_operation operation
            WHERE operation.operation_id = %s
            """,
            (operation_id,),
            public=False,
        )
        return _one(rows, "operation", operation_id)

    def list_days(
        self,
        *,
        range_run_id: str,
        statuses: Sequence[str] = (),
        cursor: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        filters = {"range_run_id": range_run_id, "statuses": sorted(set(statuses))}
        key = HistoricalRangeCursorCodec.decode(cursor, filter_payload=filters, key_size=2)
        where = ["day_run.range_run_id = %s"]
        params: list[Any] = [range_run_id]
        if statuses:
            where.append("day_run.status = ANY(%s)")
            params.append(list(statuses))
        if key:
            where.append("(day_run.ordinal, day_run.day_run_id) > (%s::integer, %s)")
            params.extend(key)
        rows = self._rows(
            f"""
            SELECT day_run.*,
                   candidate.count AS candidate_count,
                   list_version.list_version_id,
                   list_version.active_count,
                   list_version.enter_count,
                   list_version.hold_count,
                   list_version.exit_count,
                   list_version.watch_count
            FROM app.advisory_historical_range_day_run day_run
            LEFT JOIN LATERAL (
                SELECT count(*)::integer AS count
                FROM app.advisory_historical_range_candidate candidate
                WHERE candidate.day_run_id = day_run.day_run_id
            ) candidate ON TRUE
            LEFT JOIN app.advisory_historical_range_list_version list_version
              ON list_version.day_run_id = day_run.day_run_id
            WHERE {' AND '.join(where)}
            ORDER BY day_run.ordinal, day_run.day_run_id
            LIMIT %s
            """,
            (*params, _limit(limit) + 1),
        )
        return _page(rows, _limit(limit), filters, lambda row: (row["ordinal"], row["day_run_id"]))

    def get_day(
        self,
        *,
        range_run_id: str,
        trade_date: date,
        candidate_cursor: str | None = None,
        candidate_limit: int = 50,
    ) -> dict[str, Any]:
        day = _one(
            self._rows(
                "SELECT * FROM app.advisory_historical_range_day_run "
                "WHERE range_run_id = %s AND decision_trade_date = %s",
                (range_run_id, trade_date),
            ),
            "day",
            f"{range_run_id}:{trade_date.isoformat()}",
        )
        filters = {"day_run_id": day["day_run_id"]}
        key = HistoricalRangeCursorCodec.decode(candidate_cursor, filter_payload=filters, key_size=2)
        where = ["candidate.day_run_id = %s"]
        params: list[Any] = [day["day_run_id"]]
        if key:
            where.append(
                "(COALESCE(candidate.selection_effective_rank, 2147483647), candidate.symbol) "
                "> (%s::integer, %s)"
            )
            params.extend((2147483647 if key[0] is None else key[0], key[1]))
        rows = self._rows(
            f"""
            SELECT candidate.* FROM app.advisory_historical_range_candidate candidate
            WHERE {' AND '.join(where)}
            ORDER BY candidate.selection_effective_rank NULLS LAST, candidate.symbol
            LIMIT %s
            """,
            (*params, _limit(candidate_limit) + 1),
        )
        candidates = _page(
            rows,
            _limit(candidate_limit),
            filters,
            lambda row: (row["selection_effective_rank"], row["symbol"]),
        )
        return {"day": day, "candidates": candidates["items"], "candidate_page": candidates["page"]}

    def get_list(
        self,
        *,
        range_run_id: str,
        trade_date: date,
        item_cursor: str | None = None,
        item_limit: int = 50,
    ) -> dict[str, Any]:
        list_version = _one(
            self._rows(
                """
                SELECT list_version.*
                FROM app.advisory_historical_range_list_version list_version
                JOIN app.advisory_historical_range_day_run day_run ON day_run.day_run_id = list_version.day_run_id
                WHERE list_version.range_run_id = %s AND day_run.decision_trade_date = %s
                """,
                (range_run_id, trade_date),
            ),
            "list",
            f"{range_run_id}:{trade_date.isoformat()}",
        )
        filters = {"list_version_id": list_version["list_version_id"]}
        key = HistoricalRangeCursorCodec.decode(item_cursor, filter_payload=filters, key_size=2)
        where = ["list_item.list_version_id = %s"]
        params: list[Any] = [list_version["list_version_id"]]
        if key:
            where.append("(COALESCE(list_item.rank, 2147483647), list_item.symbol) > (%s::integer, %s)")
            params.extend((2147483647 if key[0] is None else key[0], key[1]))
        rows = self._rows(
            f"""
            SELECT list_item.*, episode.mark_json AS episode_mark,
                   episode.recommendation_state, episode.enter_decision_trade_date,
                   episode.exit_decision_trade_date
            FROM app.advisory_historical_range_list_item list_item
            LEFT JOIN app.advisory_historical_range_episode_snapshot episode
              ON episode.list_version_id = list_item.list_version_id AND episode.symbol = list_item.symbol
            WHERE {' AND '.join(where)}
            ORDER BY list_item.rank NULLS LAST, list_item.symbol
            LIMIT %s
            """,
            (*params, _limit(item_limit) + 1),
        )
        items = _page(rows, _limit(item_limit), filters, lambda row: (row["rank"], row["symbol"]))
        return {"list": list_version, "items": items["items"], "item_page": items["page"]}

    def list_outcomes(
        self,
        *,
        range_run_id: str,
        subject_type: str | None = None,
        projection: str | None = None,
        maturity_status: str | None = None,
        horizon: int | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        filters = {
            "range_run_id": range_run_id,
            "subject_type": subject_type,
            "projection": projection,
            "maturity_status": maturity_status,
            "horizon": horizon,
        }
        key = HistoricalRangeCursorCodec.decode(cursor, filter_payload=filters, key_size=5)
        where = ["scope.range_run_id = %s"]
        params: list[Any] = [range_run_id]
        for column, value in (
            ("outcome.subject_type", subject_type),
            ("outcome.projection", projection),
            ("outcome.maturity_status", maturity_status),
            ("outcome.horizon_trade_days", horizon),
        ):
            if value is not None:
                where.append(f"{column} = %s")
                params.append(value)
        if key:
            where.append(
                "(outcome.subject_type, outcome.subject_id, outcome.projection, outcome.horizon_trade_days, outcome.outcome_version) "
                "> (%s, %s, %s, %s::integer, %s::integer)"
            )
            params.extend(key)
        rows = self._rows(
            f"""
            WITH scope AS (
                SELECT candidate.candidate_id AS subject_id, 'CANDIDATE'::text AS subject_type, day_run.range_run_id
                FROM app.advisory_historical_range_candidate candidate
                JOIN app.advisory_historical_range_day_run day_run ON day_run.day_run_id = candidate.day_run_id
                UNION ALL
                SELECT episode.episode_id, 'EPISODE', episode.range_run_id
                FROM app.advisory_historical_range_episode_snapshot episode
                UNION ALL
                SELECT list_version.list_version_id, 'LIST_VERSION', list_version.range_run_id
                FROM app.advisory_historical_range_list_version list_version
                UNION ALL
                SELECT run.range_run_id, 'RANGE', run.range_run_id
                FROM app.advisory_historical_range_run run
            )
            SELECT outcome.* FROM app.advisory_historical_range_outcome outcome
            JOIN scope ON scope.subject_type = outcome.subject_type AND scope.subject_id = outcome.subject_id
            WHERE {' AND '.join(where)}
            ORDER BY outcome.subject_type, outcome.subject_id, outcome.projection,
                     outcome.horizon_trade_days, outcome.outcome_version
            LIMIT %s
            """,
            (*params, _limit(limit) + 1),
        )
        return _page(
            rows,
            _limit(limit),
            filters,
            lambda row: (
                row["subject_type"], row["subject_id"], row["projection"],
                row["horizon_trade_days"], row["outcome_version"],
            ),
        )

    def list_summaries(
        self, *, range_run_id: str, cursor: str | None = None, limit: int = 50
    ) -> dict[str, Any]:
        filters = {"range_run_id": range_run_id}
        key = HistoricalRangeCursorCodec.decode(cursor, filter_payload=filters, key_size=2)
        where = ["summary.range_run_id = %s"]
        params: list[Any] = [range_run_id]
        if key:
            where.append("(summary.summary_version, summary.summary_id) < (%s::integer, %s)")
            params.extend(key)
        rows = self._rows(
            f"""
            SELECT summary.* FROM app.advisory_historical_range_summary summary
            WHERE {' AND '.join(where)}
            ORDER BY summary.summary_version DESC, summary.summary_id DESC
            LIMIT %s
            """,
            (*params, _limit(limit) + 1),
        )
        return _page(rows, _limit(limit), filters, lambda row: (row["summary_version"], row["summary_id"]))

    def resolved_request_hash(self, batch_id: str) -> str:
        batch = self.get_batch(batch_id)
        value = batch.get("request_payload_sha256")
        if not value:
            raise HistoricalRangeQueryError(
                "ADVISORY_HR_BATCH_NOT_SEALED",
                "historical-range batch has not been sealed",
                context={"batch_id": batch_id},
            )
        return str(value)

    def _rows(
        self, statement: str, params: Sequence[Any], *, public: bool = True
    ) -> list[dict[str, Any]]:
        with self._conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(statement, tuple(params))
                    rows = [dict(row) for row in cur.fetchall()]
            finally:
                conn.rollback()
        return [_public_row(row) for row in rows] if public else [json_ready(row) for row in rows]


def _limit(limit: int) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > 500:
        raise HistoricalRangeQueryError(
            "ADVISORY_HR_LIMIT_INVALID",
            "limit must be between 1 and 500",
            context={"limit": limit},
        )
    return limit


def _page(
    rows: list[dict[str, Any]],
    limit: int,
    filters: Mapping[str, Any],
    key: Callable[[dict[str, Any]], Sequence[Any]],
) -> dict[str, Any]:
    has_more = len(rows) > limit
    items = rows[:limit]
    next_cursor = (
        HistoricalRangeCursorCodec.encode(order_key=key(items[-1]), filter_payload=filters)
        if has_more and items
        else None
    )
    return {
        "items": items,
        "page": {"limit": limit, "next_cursor": next_cursor, "has_more": has_more},
    }


def _one(rows: list[dict[str, Any]], resource: str, identity: str) -> dict[str, Any]:
    if not rows:
        raise HistoricalRangeNotFoundError(
            "ADVISORY_HR_RESOURCE_NOT_FOUND",
            f"historical-range {resource} does not exist",
            context={f"{resource}_id": identity},
        )
    return rows[0]


_SENSITIVE_KEYS = frozenset(
    {
        "request_payload_json",
        "worker_id",
        "lease_token",
        "component_lineage_json",
        "entry_execution_evidence_json",
        "exit_execution_evidence_json",
    }
)


def _public_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result = {key: json_ready(value) for key, value in row.items() if key not in _SENSITIVE_KEYS}
    if result.get("lease_expires_at") and "lease_expired" not in result:
        result["lease_expired"] = False
    return result
