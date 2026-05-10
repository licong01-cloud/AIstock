"""FactorValueArchiveHandler — handles factor.recompute.completed events.

Per D5 Q4 design + Codex review fix round 2:

Idempotency key (per Q4.c): (factor_name, code_text_hash, data_start, data_end,
snapshot_date). Source data is conventionally a parquet file at
single/{factor_name}.parquet but for the dev-DB integration test we also accept
an in-payload sample list (for synthetic Batch C events).

Partition routing relies on the DEFAULT partition (factor_value_default) added
in P1.5 so any trade_date outside the example y2026m05 still lands.

Boundary:
  - Writes only to qe_archive.factor_value
  - Never modifies single/{factor_name}.parquet on disk
  - factor.recompute payload only carries identifiers; handler is the
    one responsible for fetching the actual rows (per Q2.b)
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any, Callable, ClassVar, Sequence

from psycopg2.extras import RealDictCursor, execute_batch

from ..models import ArchiveJobRecord, ClaimedOutboxEvent
from .contract import (
    ArchiveHandler,
    ArchiveResult,
    HandlerStatus,
    PayloadValidationError,
)

ConnectionProvider = Callable[[], Any]


def _default_get_conn() -> Any:
    from backend.db.pg_pool import get_conn
    return get_conn()


class FactorValueArchiveHandler(ArchiveHandler):
    """Bulk-load factor_value rows for a single recompute event."""

    event_type: ClassVar[str] = "factor.recompute.completed"
    supported_schema_versions: ClassVar[tuple[int, ...]] = (1,)
    batch_size: ClassVar[int] = 5000  # qe_archive.factor_value can be large; batch UPSERT
    coalesce_window_seconds: ClassVar[int] = 60

    SUPPORTED_EVENT_TYPES: ClassVar[tuple[str, ...]] = ("factor.recompute.completed",)

    def __init__(
        self,
        connection_provider: ConnectionProvider | None = None,
        source_loader: Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]] | None = None,
    ) -> None:
        self._connection_provider = connection_provider or _default_get_conn
        # source_loader is injectable for tests. Default reads from
        # rdagent_assets/factor_values/single/{factor_name}.parquet via pandas
        # (lazy import).
        self._source_loader = source_loader or self._default_source_loader

    def __init_subclass__(cls, **kwargs: Any) -> None:  # pragma: no cover
        super(ArchiveHandler, cls).__init_subclass__(**kwargs)

    def can_handle(self, event: ClaimedOutboxEvent) -> bool:
        if event.event_type != self.event_type:
            return False
        return (event.payload or {}).get("routing_class") == "archive"

    def validate_payload(self, payload: Mapping[str, Any]) -> None:
        if not isinstance(payload, Mapping):
            raise PayloadValidationError(
                f"payload must be a mapping, got {type(payload).__name__}"
            )
        version = payload.get("schema_version")
        if not version:
            raise PayloadValidationError("payload missing required 'schema_version'")
        if version not in self.supported_schema_versions:
            raise PayloadValidationError(
                f"unsupported schema_version={version!r}; "
                f"supported={self.supported_schema_versions}"
            )
        if payload.get("routing_class") != "archive":
            raise PayloadValidationError(
                f"payload routing_class={payload.get('routing_class')!r} != 'archive'"
            )
        for required in ("factor_name", "code_text_hash"):
            if not payload.get(required):
                raise PayloadValidationError(f"payload missing required {required!r}")

    def handle(
        self,
        event: ClaimedOutboxEvent,
        archive_job: ArchiveJobRecord,
    ) -> ArchiveResult:
        self.validate_payload(event.payload or {})
        payload = dict(event.payload or {})
        try:
            rows = list(self._source_loader(payload))
        except FileNotFoundError as e:
            return ArchiveResult(
                status=HandlerStatus.NOOP,
                stats={"reason": f"source data missing: {e}"},
            )

        if not rows:
            return ArchiveResult(
                status=HandlerStatus.NOOP,
                stats={
                    "reason": "no rows produced by source loader",
                    "factor_name": payload.get("factor_name"),
                },
            )

        try:
            with self._connection_provider() as conn:
                conn.autocommit = False
                try:
                    inserted, upserted = self._bulk_upsert(conn, payload, rows)
                    conn.commit()
                    return ArchiveResult(
                        status=HandlerStatus.SUCCESS,
                        rows_inserted=inserted,
                        rows_upserted=upserted,
                        stats={
                            "factor_name": payload.get("factor_name"),
                            "code_text_hash": payload.get("code_text_hash"),
                            "row_count": len(rows),
                        },
                    )
                except Exception:
                    conn.rollback()
                    raise
        except PayloadValidationError:
            raise
        except Exception as e:
            return ArchiveResult(
                status=HandlerStatus.FAILED,
                error_message=f"{type(e).__name__}: {str(e)[:500]}",
            )

    # ------------------------------------------------------------------
    def _bulk_upsert(
        self,
        conn: Any,
        payload: Mapping[str, Any],
        rows: Sequence[Mapping[str, Any]],
    ) -> tuple[int, int]:
        factor_name = payload["factor_name"]
        code_text_hash = payload["code_text_hash"]
        snapshot_date = payload.get("snapshot_date") or _today()

        # Pre-count via a temp marker: query existing rows with same (factor_name,
        # code_text_hash) to compute upserted vs inserted accurately. For
        # idempotency the unique key is (factor_name, code_text_hash, trade_date,
        # code), so re-running the same payload should produce 0 new inserts.
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) FROM qe_archive.factor_value
                   WHERE factor_name = %s AND code_text_hash = %s""",
                (factor_name, code_text_hash),
            )
            pre_count = cur.fetchone()[0]

            params = []
            for r in rows:
                if "trade_date" not in r or "code" not in r:
                    continue
                params.append((
                    factor_name, code_text_hash, r["trade_date"], r["code"],
                    r.get("value"), snapshot_date,
                ))

            if not params:
                return 0, 0

            # Batched UPSERT via execute_batch — DO NOTHING gives clean idempotency
            execute_batch(
                cur,
                """
                INSERT INTO qe_archive.factor_value (
                    factor_name, code_text_hash, trade_date, code,
                    value, snapshot_date, captured_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (factor_name, code_text_hash, trade_date, code) DO NOTHING
                """,
                params,
                page_size=min(self.batch_size, len(params)),
            )

            cur.execute(
                """SELECT COUNT(*) FROM qe_archive.factor_value
                   WHERE factor_name = %s AND code_text_hash = %s""",
                (factor_name, code_text_hash),
            )
            post_count = cur.fetchone()[0]

        new_inserts = post_count - pre_count
        upserted = len(params) - new_inserts  # rows skipped via ON CONFLICT
        return new_inserts, upserted

    # ------------------------------------------------------------------
    @staticmethod
    def _default_source_loader(payload: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
        """Default: read rdagent_assets/factor_values/single/{factor_name}.parquet.

        This is the production path. Tests inject a list-returning loader instead
        so they don't need parquet files on disk.
        """
        factor_name = payload["factor_name"]
        # Path is project-relative; assumes standard layout under repo root.
        # Importing pandas lazily keeps the handler module light when tests
        # stub the loader.
        repo_root = Path(__file__).resolve().parents[4]
        parquet_path = repo_root / "rdagent_assets" / "factor_values" / "single" / f"{factor_name}.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(parquet_path)
        import pandas as pd  # type: ignore[import-untyped]
        df = pd.read_parquet(parquet_path)
        # Expect columns: trade_date, code, value
        if not {"trade_date", "code", "value"}.issubset(df.columns):
            raise ValueError(
                f"parquet at {parquet_path} missing required columns "
                f"trade_date / code / value; have {list(df.columns)}"
            )
        return df[["trade_date", "code", "value"]].to_dict(orient="records")


def _today() -> date:
    return date.today()
