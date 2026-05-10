"""PaperV2ArchiveHandler — pulls paper_v2.* source rows by run_id and
mirrors them into qe_archive.paper_v2_*.

Implements 3 event types per D5 Q1 design + T13 routing_class gate:

  paper.portfolio_run.completed   — flagship handler, touches 16 archive tables
  paper.daily_snapshot.captured   — narrow: paper_v2_daily_snapshot + position_snapshot
  paper.config.changed            — narrow: paper_v2_config_change_audit

Schema synthesize per BUG-006..008 (source has no equivalent column):
  - paper_v2_cash_ledger.entry_type  derived from (side, notional, fee, cash_delta)
  - paper_v2_reset_audit.reset_type  derived from (rerun_policy, deleted_counts)
  - paper_v2_session_day.data_quality derived from (expected_bar_count, derived actual)

Idempotency strategy (per D5 Q3.b):
  - paper_v2_run                INSERT ... ON CONFLICT (run_id) DO NOTHING
  - dim_paper_v2_portfolio      SCD2: lookup current row by natural key,
                                  insert new version if absent or differs
  - all fact / event tables     INSERT ... ON CONFLICT (natural_key) DO NOTHING

Boundary:
  - Reads from paper_v2.* schema in the SAME connection (single-DB design)
  - Writes to qe_archive.* schema in the SAME transaction
  - Failures roll back the whole event's writes (one event = one transaction)
  - Source rows are never modified
  - Worker default disabled (handler not registered until ops authorize)

Extension scope: deeper schema reconciliation for paper_v2.cash_ledger /
reset_audit / session_day deferred to BUG-009..011 follow-up; this handler
synthesizes around the gaps via _synthesize.py helpers.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Callable, ClassVar

import psycopg2
from psycopg2.extras import Json, RealDictCursor

from ..models import ArchiveJobRecord, ClaimedOutboxEvent
from . import _synthesize as synth
from .contract import (
    ArchiveHandler,
    ArchiveResult,
    HandlerStatus,
    PayloadValidationError,
)

# Connection provider injection mirrors backend.services.qe_archive.repository
# pattern. Default uses backend.db.pg_pool.get_conn (TDX_DB_* env). Tests
# inject a dev-pointing provider.
ConnectionProvider = Callable[[], Any]


# Source -> archive enum normalization tables (P1.4 alignment)
PAPER_V2_RUN_STATUS_ALLOWED = (
    "PENDING", "RUNNING", "SUCCEEDED", "FAILED", "INTERRUPTED",
)
ORDER_EVENT_TYPE_ALLOWED = (
    "SUBMITTED", "PARTIALLY_FILLED", "FILLED", "NO_FILL", "CANCELLED", "REJECTED",
)
ORDER_STATUS_ALLOWED = (
    "SUBMITTED", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED", "EXPIRED",
)
CONFIG_CHANGE_ACTION_ALLOWED = ("CREATE", "ACTIVATE", "DEACTIVATE", "MODIFY")


def _default_get_conn() -> Any:
    """Lazy import of the project pool so this module can be imported without
    initializing pg_pool side effects in tests."""
    from backend.db.pg_pool import get_conn
    return get_conn()


class PaperV2ArchiveHandler(ArchiveHandler):
    """Mirror paper_v2 simulation runtime into qe_archive.

    Connection is provided per-handle() rather than per-instance so tests can
    inject a dev-DB-bound connection while production uses the global pool.
    """

    # event_type is set to the flagship "paper.portfolio_run.completed" so
    # ArchiveHandler.__init_subclass__ accepts the class definition (it requires
    # a non-empty event_type at class-definition time). The handler actually
    # services 3 event types — we override can_handle() below to accept the
    # full SUPPORTED_EVENT_TYPES tuple while still enforcing routing_class.
    event_type: ClassVar[str] = "paper.portfolio_run.completed"
    supported_schema_versions: ClassVar[tuple[int, ...]] = (1,)
    batch_size: ClassVar[int] = 100
    coalesce_window_seconds: ClassVar[int] = 30

    SUPPORTED_EVENT_TYPES: ClassVar[tuple[str, ...]] = (
        "paper.portfolio_run.completed",
        "paper.daily_snapshot.captured",
        "paper.config.changed",
    )

    def __init__(self, connection_provider: ConnectionProvider | None = None) -> None:
        self._connection_provider = connection_provider or _default_get_conn

    def can_handle(self, event: ClaimedOutboxEvent) -> bool:
        if event.event_type not in self.SUPPORTED_EVENT_TYPES:
            return False
        payload = event.payload or {}
        return payload.get("routing_class") == "archive"

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

    # ------------------------------------------------------------------
    # Top-level dispatch
    # ------------------------------------------------------------------
    def handle(
        self,
        event: ClaimedOutboxEvent,
        archive_job: ArchiveJobRecord,
    ) -> ArchiveResult:
        self.validate_payload(event.payload or {})
        et = event.event_type

        try:
            with self._connection_provider() as conn:
                conn.autocommit = False
                try:
                    if et == "paper.portfolio_run.completed":
                        result = self._handle_run_completed(conn, event)
                    elif et == "paper.daily_snapshot.captured":
                        result = self._handle_daily_snapshot(conn, event)
                    elif et == "paper.config.changed":
                        result = self._handle_config_changed(conn, event)
                    else:
                        # can_handle should have rejected; defensive
                        raise PayloadValidationError(
                            f"unsupported event_type {et!r}"
                        )
                    conn.commit()
                    return result
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

    # ==================================================================
    # Event 1: paper.portfolio_run.completed
    # ==================================================================
    def _handle_run_completed(
        self, conn: Any, event: ClaimedOutboxEvent,
    ) -> ArchiveResult:
        run_id = (event.payload or {}).get("run_id") or event.source_sub_id or event.source_id
        if not run_id:
            raise PayloadValidationError("paper.portfolio_run.completed needs run_id in payload")

        inserted = 0
        upserted = 0
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1) fetch source run row (NOOP if it disappeared from source)
            cur.execute(
                "SELECT * FROM paper_v2.run WHERE run_id = %s", (run_id,),
            )
            run_row = cur.fetchone()
            if not run_row:
                return ArchiveResult(
                    status=HandlerStatus.NOOP,
                    stats={"reason": f"source run {run_id} not found"},
                )

            # 2) upsert dim_paper_v2_portfolio (SCD2) and capture portfolio_version_id
            portfolio_version_id, scd_inserted = self._upsert_portfolio_dim(
                cur, run_row["portfolio_id"],
            )
            inserted += scd_inserted

            # 3) INSERT paper_v2_run (idempotent)
            run_inserted = self._insert_paper_v2_run(cur, run_row, portfolio_version_id)
            inserted += run_inserted

            # 4) trade_session + session_day + session_events for this run
            inserted += self._mirror_sessions_for_run(cur, run_id, portfolio_version_id)

            # 5) run_events
            inserted += self._mirror_run_events(cur, run_id)

            # 6) orders + order_events + order_execution_state
            inserted += self._mirror_orders_for_run(cur, run_id, portfolio_version_id)

            # 7) fills (partition table — idempotent on (fill_id, trade_date))
            inserted += self._mirror_fills_for_run(cur, run_id, portfolio_version_id)

            # 8) positions -> paper_v2_position_snapshot
            inserted += self._mirror_positions_for_run(cur, run_id, portfolio_version_id)

            # 9) daily_snapshots
            inserted += self._mirror_daily_snapshots_for_run(cur, run_id, portfolio_version_id)

            # 10) intraday_snapshots
            inserted += self._mirror_intraday_snapshots_for_run(cur, run_id)

            # 11) cash_ledger (synthesize entry_type)
            inserted += self._mirror_cash_ledger_for_run(cur, run_id, portfolio_version_id)

            # 12) errors -> paper_v2_error
            inserted += self._mirror_errors_for_run(cur, run_id)

        return ArchiveResult(
            status=HandlerStatus.SUCCESS,
            rows_inserted=inserted,
            rows_upserted=upserted,
            stats={"run_id": run_id, "portfolio_version_id": portfolio_version_id},
        )

    # ------------------------------------------------------------------
    # SCD2 upsert: dim_paper_v2_portfolio
    # ------------------------------------------------------------------
    def _upsert_portfolio_dim(self, cur: Any, portfolio_id: str) -> tuple[int | None, int]:
        cur.execute(
            "SELECT * FROM paper_v2.portfolio WHERE portfolio_id = %s",
            (portfolio_id,),
        )
        portfolio = cur.fetchone()
        if not portfolio:
            return None, 0

        manifest_sha256 = portfolio.get("manifest_sha256") or ""
        # broker_backend not on source — synthesize from data_source / default
        broker_backend = self._derive_broker_backend(
            portfolio.get("data_source"), portfolio.get("frozen_manifest_json"),
        )
        data_source = portfolio.get("data_source") or "DB_HISTORICAL"
        package_id = portfolio.get("package_id")

        # SCD2 lookup: do we have a current row matching natural key?
        cur.execute(
            """
            SELECT portfolio_version_id, manifest_sha256, broker_backend, data_source
            FROM qe_archive.dim_paper_v2_portfolio
            WHERE portfolio_id = %s AND is_current = TRUE
            """,
            (portfolio_id,),
        )
        existing = cur.fetchone()

        if existing and (
            existing["manifest_sha256"] == manifest_sha256
            and existing["broker_backend"] == broker_backend
            and existing["data_source"] == data_source
        ):
            return existing["portfolio_version_id"], 0

        # close out previous current row, insert new
        if existing:
            cur.execute(
                """
                UPDATE qe_archive.dim_paper_v2_portfolio
                SET valid_to = NOW(), is_current = FALSE
                WHERE portfolio_version_id = %s
                """,
                (existing["portfolio_version_id"],),
            )

        cur.execute(
            """
            INSERT INTO qe_archive.dim_paper_v2_portfolio (
                portfolio_id, manifest_sha256, broker_backend, data_source,
                package_id, initial_cash, fee_policy_json, risk_policy_json,
                valid_from, valid_to, is_current, captured_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s::jsonb, %s::jsonb,
                NOW(), NULL, TRUE, NOW()
            )
            ON CONFLICT (portfolio_id, manifest_sha256, broker_backend, valid_from) DO NOTHING
            RETURNING portfolio_version_id
            """,
            (
                portfolio_id, manifest_sha256, broker_backend, data_source,
                package_id, portfolio.get("initial_cash"),
                json.dumps(portfolio.get("fee_policy") or {}),
                json.dumps(portfolio.get("risk_policy") or {}),
            ),
        )
        row = cur.fetchone()
        if row:
            return row["portfolio_version_id"], 1

        # ON CONFLICT path — re-fetch
        cur.execute(
            """
            SELECT portfolio_version_id FROM qe_archive.dim_paper_v2_portfolio
            WHERE portfolio_id = %s AND is_current = TRUE
            """,
            (portfolio_id,),
        )
        row = cur.fetchone()
        return (row["portfolio_version_id"] if row else None), 0

    # ------------------------------------------------------------------
    # paper_v2_run insert (idempotent on PK)
    # ------------------------------------------------------------------
    def _insert_paper_v2_run(
        self, cur: Any, run_row: Mapping[str, Any], portfolio_version_id: int | None,
    ) -> int:
        status = synth.normalize_status(run_row.get("status"), PAPER_V2_RUN_STATUS_ALLOWED)
        # broker_backend not on source.run; pull from portfolio-level synth (already done)
        # but paper_v2_run requires it NOT NULL — re-derive via portfolio lookup
        broker_backend = self._derive_broker_backend_for_run(cur, run_row["portfolio_id"])
        data_source = run_row.get("data_source") or "DB_HISTORICAL"
        runtime_cfg = run_row.get("runtime_config") or {}
        if isinstance(runtime_cfg, str):
            try:
                runtime_cfg = json.loads(runtime_cfg)
            except Exception:
                runtime_cfg = {}
        node_id = (runtime_cfg or {}).get("node_id")
        manifest_sha256 = self._lookup_portfolio_manifest_sha(cur, run_row["portfolio_id"])
        package_id = self._lookup_portfolio_package_id(cur, run_row["portfolio_id"])
        model_params_origin = run_row.get("model_params_origin") or "node"
        if model_params_origin not in ("node", "cache", "unavailable"):
            model_params_origin = "node"

        cur.execute(
            """
            INSERT INTO qe_archive.paper_v2_run (
                run_id, portfolio_id, trade_date, portfolio_version_id,
                package_id, manifest_sha256, broker_backend, data_source,
                node_id, model_params_origin, status, started_at, completed_at,
                captured_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                NOW()
            )
            ON CONFLICT (run_id) DO NOTHING
            """,
            (
                run_row["run_id"], run_row["portfolio_id"], run_row["trade_date"],
                portfolio_version_id, package_id, manifest_sha256,
                broker_backend, data_source, node_id, model_params_origin,
                status, run_row.get("started_at"), run_row.get("completed_at"),
            ),
        )
        return cur.rowcount

    # ------------------------------------------------------------------
    # Sessions for a run
    # ------------------------------------------------------------------
    def _mirror_sessions_for_run(
        self, cur: Any, run_id: str, portfolio_version_id: int | None,
    ) -> int:
        # Find all session_ids tied to this run via session_day
        cur.execute(
            """
            SELECT DISTINCT session_id FROM paper_v2.session_day
            WHERE run_id = %s AND session_id IS NOT NULL
            """,
            (run_id,),
        )
        session_ids = [r["session_id"] for r in cur.fetchall()]
        if not session_ids:
            return 0

        n = 0
        # Insert paper_v2_session rows (one per session_id × run_id pair)
        cur.execute(
            "SELECT * FROM paper_v2.trade_session WHERE session_id = ANY(%s)",
            (session_ids,),
        )
        for sess in cur.fetchall():
            mode = sess.get("mode")
            if mode and mode not in ("REPLAY_ONLY", "LIVE_ONLY", "CATCHUP_THEN_LIVE"):
                mode = None  # CHECK constraint allows NULL or one of these
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_session (
                    run_id, portfolio_version_id, trade_session_id, trade_date,
                    mode, validated_execution_policy_json,
                    started_at, ended_at, captured_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s::jsonb,
                    %s, %s, NOW()
                )
                ON CONFLICT (trade_session_id) DO NOTHING
                """,
                (
                    run_id, portfolio_version_id, sess["session_id"],
                    sess.get("start_date") or sess.get("end_date"),
                    mode,
                    json.dumps(sess.get("validated_execution_policy_json") or {}),
                    sess.get("started_at"), sess.get("completed_at"),
                ),
            )
            n += cur.rowcount

        # session_day rows for this run (synthesize data_quality)
        cur.execute(
            "SELECT * FROM paper_v2.session_day WHERE run_id = %s",
            (run_id,),
        )
        for sd in cur.fetchall():
            # actual_bar_count not on source; derive from intraday_snapshots count
            actual = self._count_intraday_snapshots(cur, run_id, sd.get("trade_date"))
            quality = synth.synthesize_session_day_data_quality(
                sd.get("expected_bar_count"), actual,
            )
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_session_day (
                    run_id, trade_session_id, trade_date,
                    expected_bar_count, actual_bar_count,
                    latest_available_bar_time, data_quality, captured_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (trade_session_id, trade_date) DO NOTHING
                """,
                (
                    run_id, sd.get("session_id"), sd.get("trade_date"),
                    sd.get("expected_bar_count"), actual,
                    sd.get("latest_available_bar_time"), quality,
                ),
            )
            n += cur.rowcount

        # session_events
        cur.execute(
            """
            SELECT event_id, session_id, run_id, event_type, message, context, created_at
            FROM paper_v2.session_events WHERE session_id = ANY(%s)
            """,
            (session_ids,),
        )
        for ev in cur.fetchall():
            # source event_id is bigint; cast to TEXT
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_session_event (
                    event_id, run_id, trade_session_id, trade_date,
                    event_type, event_payload, occurred_at, captured_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s::jsonb,
                    %s, NOW()
                )
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    str(ev["event_id"]), run_id, ev.get("session_id"),
                    ev.get("created_at").date() if ev.get("created_at") else None,
                    ev.get("event_type"),
                    json.dumps(ev.get("context") or {"message": ev.get("message")}),
                    ev.get("created_at"),
                ),
            )
            n += cur.rowcount
        return n

    def _mirror_run_events(self, cur: Any, run_id: str) -> int:
        cur.execute(
            "SELECT * FROM paper_v2.run_events WHERE run_id = %s ORDER BY event_seq",
            (run_id,),
        )
        n = 0
        for ev in cur.fetchall():
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_run_event (
                    event_id, run_id, trade_date, event_type,
                    event_payload, occurred_at, captured_at
                ) VALUES (
                    %s, %s, %s, %s, %s::jsonb, %s, NOW()
                )
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    f"{run_id}_{ev['event_seq']}", run_id,
                    ev.get("created_at").date() if ev.get("created_at") else None,
                    ev.get("event_type"),
                    json.dumps(ev.get("context") or {"message": ev.get("message")}),
                    ev.get("created_at"),
                ),
            )
            n += cur.rowcount
        return n

    # ------------------------------------------------------------------
    # Orders + order_events + order_execution_state
    # ------------------------------------------------------------------
    def _mirror_orders_for_run(
        self, cur: Any, run_id: str, portfolio_version_id: int | None,
    ) -> int:
        cur.execute("SELECT * FROM paper_v2.orders WHERE run_id = %s", (run_id,))
        orders = cur.fetchall()
        n = 0
        for o in orders:
            status = synth.normalize_status(o.get("status"), ORDER_STATUS_ALLOWED)
            order_type = o.get("order_type") or "MARKET"
            if order_type not in ("LIMIT", "MARKET", "STOP", "STOP_LIMIT"):
                order_type = "MARKET"
            side = o.get("side")
            if side not in ("BUY", "SELL"):
                continue  # source CHECK upstream should prevent this; defensive skip
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_order (
                    run_id, portfolio_version_id, order_id, trade_session_id,
                    trade_date, symbol, side, order_type, quantity, price,
                    status, placed_at, captured_at
                ) VALUES (
                    %s, %s, %s, NULL,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, NOW()
                )
                ON CONFLICT (order_id) DO NOTHING
                """,
                (
                    run_id, portfolio_version_id, o["order_id"],
                    o.get("created_at").date() if o.get("created_at") else None,
                    o["symbol"], side, order_type,
                    o.get("quantity"), o.get("limit_price"),
                    status, o.get("created_at"),
                ),
            )
            n += cur.rowcount

        # order_events for those orders
        if orders:
            order_ids = [o["order_id"] for o in orders]
            cur.execute(
                "SELECT * FROM paper_v2.order_events WHERE order_id = ANY(%s)",
                (order_ids,),
            )
            for ev in cur.fetchall():
                event_type = ev.get("event_type")
                if event_type and event_type not in ORDER_EVENT_TYPE_ALLOWED:
                    # Source enum drift: skip with note rather than crashing whole event
                    continue
                cur.execute(
                    """
                    INSERT INTO qe_archive.paper_v2_order_event (
                        event_id, order_id, run_id, trade_date,
                        event_type, event_payload, occurred_at, captured_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s::jsonb, %s, NOW()
                    )
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        ev["event_id"], ev["order_id"], run_id,
                        ev.get("event_time").date() if ev.get("event_time") else None,
                        event_type,
                        json.dumps({
                            "metadata": ev.get("metadata"),
                            "fill_json": ev.get("fill_json"),
                            "reason": ev.get("reason"),
                        }, default=str),
                        ev.get("event_time"),
                    ),
                )
                n += cur.rowcount

            # order_execution_state
            cur.execute(
                "SELECT * FROM paper_v2.order_execution_state WHERE order_id = ANY(%s)",
                (order_ids,),
            )
            for st in cur.fetchall():
                cur.execute(
                    """
                    INSERT INTO qe_archive.paper_v2_order_execution_state (
                        order_id, run_id, trade_date, algo_code,
                        final_algo_state_json, filled_quantity, captured_at
                    ) VALUES (
                        %s, %s, %s, %s, %s::jsonb, %s, NOW()
                    )
                    ON CONFLICT (order_id) DO NOTHING
                    """,
                    (
                        st["order_id"], run_id, st.get("trade_date"),
                        st.get("algo_code"),
                        json.dumps(st.get("algo_state_json") or {}),
                        st.get("filled_quantity"),
                    ),
                )
                n += cur.rowcount
        return n

    # ------------------------------------------------------------------
    # Fills (partitioned table)
    # ------------------------------------------------------------------
    def _mirror_fills_for_run(
        self, cur: Any, run_id: str, portfolio_version_id: int | None,
    ) -> int:
        cur.execute("SELECT * FROM paper_v2.fills WHERE run_id = %s", (run_id,))
        n = 0
        for f in cur.fetchall():
            side = f.get("side") if f.get("side") in (None, "BUY", "SELL") else None
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_fill (
                    fill_id, run_id, portfolio_version_id, order_id, trade_date,
                    symbol, side, filled_quantity, fill_price, fill_value,
                    fees, slippage_bps, broker_backend, algo_code,
                    intended_price, fill_market_context, filled_at, captured_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    NULL, NULL, NULL, NULL,
                    %s, %s::jsonb, %s, NOW()
                )
                ON CONFLICT (fill_id, trade_date) DO NOTHING
                """,
                (
                    f["fill_id"], run_id, portfolio_version_id, f["order_id"],
                    f.get("trade_time").date() if f.get("trade_time") else None,
                    f["symbol"], side, f.get("quantity"), f.get("price"),
                    None,  # fill_value not on source — derive at downstream query time
                    f.get("intended_price"),
                    json.dumps(f.get("fill_market_context") or {}, default=str)
                    if f.get("fill_market_context") else None,
                    f.get("trade_time"),
                ),
            )
            n += cur.rowcount
        return n

    # ------------------------------------------------------------------
    # Positions, daily_snapshot, intraday_snapshot, cash_ledger, errors
    # ------------------------------------------------------------------
    def _mirror_positions_for_run(
        self, cur: Any, run_id: str, portfolio_version_id: int | None,
    ) -> int:
        cur.execute("SELECT * FROM paper_v2.positions WHERE run_id = %s", (run_id,))
        n = 0
        for p in cur.fetchall():
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_position_snapshot (
                    run_id, portfolio_version_id, trade_date, symbol,
                    quantity, cost_basis, market_value, unrealized_pnl,
                    captured_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (run_id, trade_date, symbol) DO NOTHING
                """,
                (
                    run_id, portfolio_version_id, p["trade_date"], p["symbol"],
                    p.get("quantity"), p.get("avg_cost"),
                    p.get("market_value"),
                    self._derive_unrealized_pnl(p),
                ),
            )
            n += cur.rowcount
        return n

    def _mirror_daily_snapshots_for_run(
        self, cur: Any, run_id: str, portfolio_version_id: int | None,
    ) -> int:
        cur.execute("SELECT * FROM paper_v2.daily_snapshots WHERE run_id = %s", (run_id,))
        n = 0
        for s in cur.fetchall():
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_daily_snapshot (
                    run_id, portfolio_version_id, trade_date, total_value,
                    cash, positions_value, realized_pnl, unrealized_pnl,
                    benchmark_csi300, benchmark_csi500, benchmark_csi1000,
                    relative_to_csi300, regime, captured_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NOW()
                )
                ON CONFLICT (run_id, trade_date) DO NOTHING
                """,
                (
                    run_id, portfolio_version_id, s["trade_date"], s.get("nav"),
                    s.get("cash"), s.get("market_value"),
                ),
            )
            n += cur.rowcount
        return n

    def _mirror_intraday_snapshots_for_run(self, cur: Any, run_id: str) -> int:
        cur.execute("SELECT * FROM paper_v2.intraday_snapshots WHERE run_id = %s", (run_id,))
        n = 0
        for s in cur.fetchall():
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_intraday_snapshot (
                    snapshot_id, run_id, trade_date, snapshot_time,
                    total_value, cash, positions_json, captured_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s::jsonb, NOW()
                )
                ON CONFLICT (snapshot_id) DO NOTHING
                """,
                (
                    s["snapshot_id"], run_id, s["trade_date"], s["snapshot_time"],
                    s.get("nav"), s.get("cash"),
                    json.dumps(s.get("positions_json") or {}, default=str),
                ),
            )
            n += cur.rowcount
        return n

    def _mirror_cash_ledger_for_run(
        self, cur: Any, run_id: str, portfolio_version_id: int | None,
    ) -> int:
        cur.execute("SELECT * FROM paper_v2.cash_ledger WHERE run_id = %s", (run_id,))
        n = 0
        for c in cur.fetchall():
            entry_type = synth.synthesize_cash_ledger_entry_type(
                c.get("side"), c.get("notional"), c.get("fee"), c.get("cash_delta"),
            )
            # source has no balance_after — use cash_after
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_cash_ledger (
                    ledger_entry_id, run_id, portfolio_version_id, trade_date,
                    entry_type, amount, balance_after, related_order_id,
                    occurred_at, captured_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (ledger_entry_id) DO NOTHING
                """,
                (
                    str(c["cash_id"]), run_id, portfolio_version_id, c.get("trade_date"),
                    entry_type, c.get("cash_delta") or c.get("notional") or 0,
                    c.get("cash_after"),
                    c.get("fill_id"),  # proxy for related_order_id
                    c.get("created_at"),
                ),
            )
            n += cur.rowcount
        return n

    def _mirror_errors_for_run(self, cur: Any, run_id: str) -> int:
        cur.execute("SELECT * FROM paper_v2.errors WHERE run_id = %s", (run_id,))
        n = 0
        for e in cur.fetchall():
            error_class = synth.derive_error_class(e.get("error_code"), e.get("message"))
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_error (
                    error_id, run_id, trade_date, trade_session_id,
                    error_class, error_code, error_message, stack_trace,
                    related_order_id, occurred_at, captured_at
                ) VALUES (
                    %s, %s, %s, NULL,
                    %s, %s, %s, NULL,
                    NULL, %s, NOW()
                )
                ON CONFLICT (error_id) DO NOTHING
                """,
                (
                    str(e["error_id"]), run_id,
                    e.get("created_at").date() if e.get("created_at") else None,
                    error_class, e.get("error_code"), e.get("message"),
                    e.get("created_at"),
                ),
            )
            n += cur.rowcount
        return n

    # ==================================================================
    # Event 2: paper.daily_snapshot.captured
    # ==================================================================
    def _handle_daily_snapshot(
        self, conn: Any, event: ClaimedOutboxEvent,
    ) -> ArchiveResult:
        payload = event.payload or {}
        run_id = payload.get("run_id") or event.source_id
        trade_date = payload.get("trade_date")
        if not run_id or not trade_date:
            raise PayloadValidationError(
                "paper.daily_snapshot.captured needs run_id + trade_date"
            )
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM paper_v2.run WHERE run_id = %s", (run_id,))
            run_row = cur.fetchone()
            if not run_row:
                return ArchiveResult(status=HandlerStatus.NOOP,
                                     stats={"reason": "source run missing"})
            pvid, _ = self._upsert_portfolio_dim(cur, run_row["portfolio_id"])

            # daily_snapshot.captured can fire BEFORE portfolio_run.completed in
            # the natural event order. Ensure parent paper_v2_run exists (idempotent
            # ON CONFLICT DO NOTHING) so the FK from paper_v2_daily_snapshot resolves.
            self._insert_paper_v2_run(cur, run_row, pvid)

            # narrow: write only this trade_date's daily + position snapshots
            cur.execute(
                """SELECT * FROM paper_v2.daily_snapshots
                   WHERE run_id = %s AND trade_date = %s""",
                (run_id, trade_date),
            )
            inserted = 0
            for s in cur.fetchall():
                cur.execute(
                    """
                    INSERT INTO qe_archive.paper_v2_daily_snapshot (
                        run_id, portfolio_version_id, trade_date, total_value,
                        cash, positions_value, realized_pnl, unrealized_pnl,
                        captured_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, NOW())
                    ON CONFLICT (run_id, trade_date) DO NOTHING
                    """,
                    (run_id, pvid, s["trade_date"], s.get("nav"),
                     s.get("cash"), s.get("market_value")),
                )
                inserted += cur.rowcount

            cur.execute(
                """SELECT * FROM paper_v2.positions
                   WHERE run_id = %s AND trade_date = %s""",
                (run_id, trade_date),
            )
            for p in cur.fetchall():
                cur.execute(
                    """
                    INSERT INTO qe_archive.paper_v2_position_snapshot (
                        run_id, portfolio_version_id, trade_date, symbol,
                        quantity, cost_basis, market_value, unrealized_pnl,
                        captured_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (run_id, trade_date, symbol) DO NOTHING
                    """,
                    (run_id, pvid, p["trade_date"], p["symbol"],
                     p.get("quantity"), p.get("avg_cost"), p.get("market_value"),
                     self._derive_unrealized_pnl(p)),
                )
                inserted += cur.rowcount

            return ArchiveResult(
                status=HandlerStatus.SUCCESS,
                rows_inserted=inserted,
                stats={"run_id": run_id, "trade_date": str(trade_date)},
            )

    # ==================================================================
    # Event 3: paper.config.changed
    # ==================================================================
    def _handle_config_changed(
        self, conn: Any, event: ClaimedOutboxEvent,
    ) -> ArchiveResult:
        payload = event.payload or {}
        audit_id = payload.get("audit_id")
        if not audit_id:
            raise PayloadValidationError("paper.config.changed needs audit_id in payload")
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM paper_v2.config_change_audit WHERE audit_id = %s",
                (int(audit_id) if str(audit_id).isdigit() else audit_id,),
            )
            row = cur.fetchone()
            if not row:
                return ArchiveResult(status=HandlerStatus.NOOP,
                                     stats={"reason": f"audit_id {audit_id} not found"})
            change_type = row.get("change_type") or "MODIFY"
            if change_type not in CONFIG_CHANGE_ACTION_ALLOWED:
                # Source enum drift — fail-fast (no silent fallback)
                raise PayloadValidationError(
                    f"unknown change_type {change_type!r} not in {CONFIG_CHANGE_ACTION_ALLOWED}"
                )
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_config_change_audit (
                    audit_id, portfolio_id, change_type, old_value_json,
                    new_value_json, changed_by, changed_at, captured_at
                ) VALUES (
                    %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, NOW()
                )
                ON CONFLICT (audit_id) DO NOTHING
                """,
                (
                    str(row["audit_id"]), row.get("portfolio_id"), change_type,
                    json.dumps(row.get("before_json") or {}, default=str),
                    json.dumps(row.get("after_json") or {}, default=str),
                    row.get("created_by"), row.get("created_at"),
                ),
            )
            return ArchiveResult(
                status=HandlerStatus.SUCCESS,
                rows_inserted=cur.rowcount,
                stats={"audit_id": str(audit_id), "change_type": change_type},
            )

    # ==================================================================
    # Helpers (synthesize broker_backend, count intraday, etc.)
    # ==================================================================
    def _derive_broker_backend(
        self, data_source: str | None, frozen_manifest_json: Any,
    ) -> str:
        # Try to extract from frozen_manifest first
        if isinstance(frozen_manifest_json, dict):
            bb = frozen_manifest_json.get("broker_backend")
            if bb:
                return str(bb)
        # Fall back: heuristic by data_source
        if data_source and "MINIQMT" in data_source:
            return "miniqmtsim"
        return "localsim"

    def _derive_broker_backend_for_run(self, cur: Any, portfolio_id: str) -> str:
        cur.execute(
            "SELECT data_source, frozen_manifest_json FROM paper_v2.portfolio "
            "WHERE portfolio_id = %s",
            (portfolio_id,),
        )
        row = cur.fetchone()
        if not row:
            return "localsim"
        return self._derive_broker_backend(row.get("data_source"), row.get("frozen_manifest_json"))

    def _lookup_portfolio_manifest_sha(self, cur: Any, portfolio_id: str) -> str | None:
        cur.execute(
            "SELECT manifest_sha256 FROM paper_v2.portfolio WHERE portfolio_id = %s",
            (portfolio_id,),
        )
        row = cur.fetchone()
        return row.get("manifest_sha256") if row else None

    def _lookup_portfolio_package_id(self, cur: Any, portfolio_id: str) -> str | None:
        cur.execute(
            "SELECT package_id FROM paper_v2.portfolio WHERE portfolio_id = %s",
            (portfolio_id,),
        )
        row = cur.fetchone()
        return row.get("package_id") if row else None

    def _count_intraday_snapshots(self, cur: Any, run_id: str, trade_date: Any) -> int | None:
        if trade_date is None:
            return None
        cur.execute(
            """SELECT COUNT(*) AS n FROM paper_v2.intraday_snapshots
               WHERE run_id = %s AND trade_date = %s""",
            (run_id, trade_date),
        )
        row = cur.fetchone()
        return int(row["n"]) if row else None

    def _derive_unrealized_pnl(self, position_row: Mapping[str, Any]) -> float | None:
        qty = position_row.get("quantity")
        avg_cost = position_row.get("avg_cost")
        market_value = position_row.get("market_value")
        if qty is None or avg_cost is None or market_value is None:
            return None
        try:
            return float(market_value) - float(qty) * float(avg_cost)
        except (TypeError, ValueError):
            return None
