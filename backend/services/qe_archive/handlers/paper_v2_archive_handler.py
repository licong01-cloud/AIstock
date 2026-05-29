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

ID canonicalization (BUG-009):
  - source BIGINT event/audit ids are archived as raw decimal TEXT strings
  - no table prefixes, no float conversion, no locale formatting

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

from psycopg2.extras import RealDictCursor

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


def _archive_text_id(value: Any) -> str:
    """Canonical archive TEXT natural key for source integer/text ids."""
    return str(value)


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
        archive_job: ArchiveJobRecord | None = None,
    ) -> ArchiveResult:
        """P1.2 (Codex round 2): exceptions PROPAGATE to caller; no silent
        FAILED conversion. The worker's archive_handler_adapter (worker.py)
        catches and converts to ArchiveWorkerEventResult(success=False).
        Inner try/except remains for transactional ROLLBACK before re-raise.
        """
        self.validate_payload(event.payload or {})
        et = event.event_type

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
                    raise PayloadValidationError(f"unsupported event_type {et!r}")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

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
            # P1.1 (Codex round 3 BLOCKED) — short-circuit on completion marker,
            # NOT mere row existence. If a previous attempt committed the
            # paper_v2_run row but failed mid-way through child mirrors, that
            # row would have archive_complete=false and we MUST retry the full
            # 17-step mirror to recover. archive_complete flips to TRUE only
            # after every child mirror commits in the same transaction.
            #
            # Replaces the round-2 P1.5 existence-only short-circuit: the prior
            # design protected SCD2 dim from version-bumping on replay but also
            # masked partial archives forever. The completion marker keeps the
            # SCD2 protection (we still skip when archive_complete=true) AND
            # restores worker retry semantics for partial failures.
            cur.execute(
                """SELECT archive_complete FROM qe_archive.paper_v2_run
                   WHERE run_id = %s""",
                (run_id,),
            )
            existing = cur.fetchone()
            if existing and existing["archive_complete"]:
                return ArchiveResult(
                    status=HandlerStatus.SUCCESS,
                    rows_inserted=0,
                    rows_upserted=0,
                    stats={"run_id": run_id, "replay_skipped": True,
                           "archive_complete": True},
                )

            # 1) fetch source run row (NOOP if it disappeared from source)
            cur.execute("SELECT * FROM paper_v2.run WHERE run_id = %s", (run_id,))
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
            inserted += self._insert_paper_v2_run(cur, run_row, portfolio_version_id)

            # 4) trade_session + session_day + session_events for this run
            inserted += self._mirror_sessions_for_run(cur, run_id, portfolio_version_id)

            # 5) run_events
            inserted += self._mirror_run_events(cur, run_id)

            # 6) orders + order_events + order_execution_state (P1.3: enum drift raises)
            inserted += self._mirror_orders_for_run(cur, run_id, portfolio_version_id)

            # 7) fills (partition table)
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

            # P1.4 (Codex round 2) — 5 previously-unfilled archive tables:
            # 13) dim_paper_v2_runtime_profile (SCD2) for this portfolio
            inserted += self._upsert_runtime_profile_dim(cur, run_row["portfolio_id"])

            # 14) dim_paper_v2_runtime_profile_version (immutable version log)
            inserted += self._mirror_runtime_profile_versions(cur, run_row["portfolio_id"])

            # 15) paper_v2_runtime_config_activation
            inserted += self._mirror_runtime_config_activation(cur, run_row["portfolio_id"], run_row["trade_date"])

            # 16) paper_v2_execution_policy_activation
            inserted += self._mirror_execution_policy_activation(cur, run_row["portfolio_id"], run_row["trade_date"])

            # 17) paper_v2_reset_audit (synthesize reset_type per BUG-007)
            inserted += self._mirror_reset_audit(cur, run_row["portfolio_id"])

            # P1.1 (Codex round 3) — flip completion marker AFTER all 17 child
            # mirrors landed. If any step above raised, the whole transaction
            # rolls back including this UPDATE — next event delivery sees
            # archive_complete=false and re-runs the full mirror.
            cur.execute(
                """UPDATE qe_archive.paper_v2_run
                   SET archive_complete = TRUE,
                       archive_completed_at = NOW()
                   WHERE run_id = %s""",
                (run_id,),
            )

        return ArchiveResult(
            status=HandlerStatus.SUCCESS,
            rows_inserted=inserted,
            rows_upserted=upserted,
            stats={"run_id": run_id, "portfolio_version_id": portfolio_version_id,
                   "archive_complete": True},
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
                    _archive_text_id(ev["event_id"]), run_id, ev.get("session_id"),
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
            # P1.3 (Codex round 2): no silent skip on enum drift. Raise so the
            # whole event transaction rolls back; operator must fix source data
            # or extend allowed enum before re-archiving.
            if side not in ("BUY", "SELL"):
                raise ValueError(
                    f"unknown order side {side!r} for run_id={run_id} "
                    f"order_id={o.get('order_id')}; expected one of {('BUY','SELL')}"
                )
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
                # P1.3 (Codex round 2): no silent skip; raise on enum drift.
                if event_type and event_type not in ORDER_EVENT_TYPE_ALLOWED:
                    raise ValueError(
                        f"unknown order_event event_type {event_type!r} for "
                        f"order_id={ev.get('order_id')} event_id={ev.get('event_id')}; "
                        f"expected one of {ORDER_EVENT_TYPE_ALLOWED}"
                    )
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
                        _archive_text_id(ev["event_id"]), ev["order_id"], run_id,
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
        snapshots = cur.fetchall()
        n = 0
        for s in snapshots:
            # P2.3 (Codex round 3) — ETL-join market.index_daily for benchmarks
            # and market.regime_label for regime. NULL fallback if either source
            # is missing data for this trade_date (e.g., regime_label table not
            # yet populated). No raise on missing — design §5.9 explicitly says
            # "NULL 表示该日尚未生成标签".
            enrichment = self._fetch_benchmark_and_regime(cur, s["trade_date"])
            relative = self._compute_relative_to_csi300(s, enrichment)
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
                    %s, %s, %s,
                    %s, %s, NOW()
                )
                ON CONFLICT (run_id, trade_date) DO NOTHING
                """,
                (
                    run_id, portfolio_version_id, s["trade_date"], s.get("nav"),
                    s.get("cash"), s.get("market_value"),
                    enrichment.get("benchmark_csi300"),
                    enrichment.get("benchmark_csi500"),
                    enrichment.get("benchmark_csi1000"),
                    relative,
                    enrichment.get("regime"),
                ),
            )
            n += cur.rowcount
        return n

    # P2.3 helper — pre-fetch benchmarks + regime for a given trade_date.
    # Index codes per design §5.9 + Batch A's INDEX_CODES tuple:
    #   CSI300  = '000300.SH'
    #   CSI500  = '000905.SH'
    #   CSI1000 = '000852.SH'
    # source_method default = 'simple_quadrant' (per design §8.6).
    _BENCHMARK_INDEX_CODES = {
        "benchmark_csi300":  "000300.SH",
        "benchmark_csi500":  "000905.SH",
        "benchmark_csi1000": "000852.SH",
    }
    _REGIME_DEFAULT_SOURCE_METHOD = "simple_quadrant"

    def _fetch_benchmark_and_regime(
        self, cur: Any, trade_date: Any,
    ) -> dict[str, Any]:
        """LEFT-join semantics: returns dict with all 4 keys; missing rows -> None.

        Single round-trip for benchmarks via IN (3) + 1 round-trip for regime.
        """
        if trade_date is None:
            return {k: None for k in
                    list(self._BENCHMARK_INDEX_CODES) + ["regime"]}

        out: dict[str, Any] = {k: None for k in self._BENCHMARK_INDEX_CODES}
        # Benchmarks: pull all 3 close prices in one query
        codes = list(self._BENCHMARK_INDEX_CODES.values())
        cur.execute(
            """SELECT ts_code, close FROM market.index_daily
               WHERE trade_date = %s AND ts_code = ANY(%s)""",
            (trade_date, codes),
        )
        code_to_close = {r["ts_code"]: r["close"] for r in cur.fetchall()}
        for col, code in self._BENCHMARK_INDEX_CODES.items():
            out[col] = code_to_close.get(code)

        # Regime: NULL fallback if regime_label not yet computed for this date
        cur.execute(
            """SELECT regime FROM market.regime_label
               WHERE trade_date = %s AND source_method = %s
               LIMIT 1""",
            (trade_date, self._REGIME_DEFAULT_SOURCE_METHOD),
        )
        row = cur.fetchone()
        out["regime"] = row["regime"] if row else None
        return out

    def _compute_relative_to_csi300(
        self, snapshot: Mapping[str, Any], enrichment: Mapping[str, Any],
    ) -> float | None:
        """relative_to_csi300 = (NAV / prior_close) - benchmark_pct_change is
        complex (needs prior NAV). For round 3 we leave the cross-day return
        derivation to a downstream view and only populate this column when
        the snapshot has a directly-comparable single-day return field.
        Currently source paper_v2.daily_snapshots has no ret_today column, so
        we return None — design §5.9 calls this 'snapshot return - benchmark
        return' which is a multi-row computation better suited for a SQL view
        than a per-INSERT lookup.
        """
        return None

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
            # P1.5 (Codex round 2) policy interaction: daily_snapshot.captured
            # MUST NOT pre-create paper_v2_run, otherwise the subsequent
            # paper.portfolio_run.completed event would short-circuit and skip
            # the full mirror. If paper_v2_run is not yet archived, return NOOP
            # — the run.completed handler will pull all daily snapshots when it
            # eventually fires (via _mirror_daily_snapshots_for_run).
            cur.execute(
                "SELECT portfolio_version_id FROM qe_archive.paper_v2_run WHERE run_id = %s",
                (run_id,),
            )
            pvid_row = cur.fetchone()
            if not pvid_row:
                return ArchiveResult(
                    status=HandlerStatus.NOOP,
                    stats={
                        "reason": "paper_v2_run not yet archived; "
                                  "deferred to portfolio_run.completed",
                        "run_id": run_id,
                        "trade_date": str(trade_date),
                    },
                )
            pvid = pvid_row["portfolio_version_id"]

            # narrow: write only this trade_date's daily + position snapshots
            cur.execute(
                """SELECT * FROM paper_v2.daily_snapshots
                   WHERE run_id = %s AND trade_date = %s""",
                (run_id, trade_date),
            )
            inserted = 0
            for s in cur.fetchall():
                # P2.3: same ETL-join treatment as the run-completed path.
                enrichment = self._fetch_benchmark_and_regime(cur, s["trade_date"])
                relative = self._compute_relative_to_csi300(s, enrichment)
                cur.execute(
                    """
                    INSERT INTO qe_archive.paper_v2_daily_snapshot (
                        run_id, portfolio_version_id, trade_date, total_value,
                        cash, positions_value, realized_pnl, unrealized_pnl,
                        benchmark_csi300, benchmark_csi500, benchmark_csi1000,
                        relative_to_csi300, regime, captured_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, NULL, NULL,
                        %s, %s, %s, %s, %s, NOW()
                    )
                    ON CONFLICT (run_id, trade_date) DO NOTHING
                    """,
                    (run_id, pvid, s["trade_date"], s.get("nav"),
                     s.get("cash"), s.get("market_value"),
                     enrichment.get("benchmark_csi300"),
                     enrichment.get("benchmark_csi500"),
                     enrichment.get("benchmark_csi1000"),
                     relative,
                     enrichment.get("regime")),
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
                    _archive_text_id(row["audit_id"]), row.get("portfolio_id"), change_type,
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

    # ==================================================================
    # P1.4 (Codex round 2): 5 previously-unfilled archive table mirrors
    # ==================================================================

    def _upsert_runtime_profile_dim(self, cur: Any, portfolio_id: str) -> int:
        """SCD2 mirror of paper_v2.runtime_profile -> dim_paper_v2_runtime_profile.

        Natural key: (profile_id, valid_from). valid_from = source created_at so
        replays are idempotent on ON CONFLICT (profile_id, valid_from) DO NOTHING.

        P2.2 (Codex round 3) — close-current: when inserting a new SCD2 version
        for an existing profile_id, FIRST close the previous current row
        (UPDATE is_current=false, valid_to=new row's valid_from). Without this,
        multiple is_current=true rows can coexist for the same profile_id —
        not a true SCD2.

        The lookup-then-write pattern uses ON CONFLICT DO NOTHING semantics:
        if (profile_id, valid_from) already exists we skip the insert entirely
        and DO NOT close the current row (it might already be the same row).
        """
        cur.execute(
            """SELECT profile_id, profile_name, status, current_version_id,
                      package_id, created_by, created_at
               FROM paper_v2.runtime_profile WHERE portfolio_id = %s""",
            (portfolio_id,),
        )
        n = 0
        for p in cur.fetchall():
            profile_json = {
                "profile_name": p.get("profile_name"),
                "status": p.get("status"),
                "current_version_id": p.get("current_version_id"),
                "package_id": p.get("package_id"),
                "created_by": p.get("created_by"),
            }
            new_valid_from = p.get("created_at")

            # Check if we already have a row at exactly this valid_from
            cur.execute(
                """SELECT 1 FROM qe_archive.dim_paper_v2_runtime_profile
                   WHERE profile_id = %s AND valid_from = %s""",
                (p["profile_id"], new_valid_from),
            )
            if cur.fetchone():
                # Idempotent: same row, no SCD2 transition needed
                continue

            # P2.2 close-current: any prior is_current=TRUE row for this
            # profile_id must be closed so we don't have multiple current rows.
            cur.execute(
                """UPDATE qe_archive.dim_paper_v2_runtime_profile
                   SET is_current = FALSE, valid_to = %s
                   WHERE profile_id = %s AND is_current = TRUE
                     AND valid_from < %s""",
                (new_valid_from, p["profile_id"], new_valid_from),
            )

            cur.execute(
                """
                INSERT INTO qe_archive.dim_paper_v2_runtime_profile (
                    profile_id, profile_name, profile_json,
                    valid_from, valid_to, is_current, captured_at
                ) VALUES (
                    %s, %s, %s::jsonb,
                    %s, NULL, TRUE, NOW()
                )
                ON CONFLICT (profile_id, valid_from) DO NOTHING
                """,
                (
                    p["profile_id"], p.get("profile_name"),
                    json.dumps(profile_json, default=str),
                    new_valid_from,
                ),
            )
            n += cur.rowcount
        return n

    def _mirror_runtime_profile_versions(self, cur: Any, portfolio_id: str) -> int:
        """Append-only mirror of paper_v2.runtime_profile_version. Idempotent
        on (version_id) UNIQUE."""
        cur.execute(
            """SELECT v.profile_version_id, v.profile_id, v.version_no,
                      v.config_json, v.config_sha256, v.created_by, v.created_at,
                      v.supersedes_version_id, v.reason
               FROM paper_v2.runtime_profile_version v
               JOIN paper_v2.runtime_profile p ON p.profile_id = v.profile_id
               WHERE p.portfolio_id = %s""",
            (portfolio_id,),
        )
        n = 0
        for v in cur.fetchall():
            # config_diff_json: when supersedes_version_id is set we'd compute a
            # delta vs that version; for now we record the full new config_json
            # plus a marker pointing at the predecessor.
            diff = {
                "supersedes_version_id": v.get("supersedes_version_id"),
                "config_sha256": v.get("config_sha256"),
                "reason": v.get("reason"),
                "full_config": v.get("config_json"),
            }
            cur.execute(
                """
                INSERT INTO qe_archive.dim_paper_v2_runtime_profile_version (
                    version_id, profile_id, version_number,
                    config_diff_json, created_by, created_at, captured_at
                ) VALUES (
                    %s, %s, %s,
                    %s::jsonb, %s, %s, NOW()
                )
                ON CONFLICT (version_id) DO NOTHING
                """,
                (
                    v["profile_version_id"], v["profile_id"], v.get("version_no"),
                    json.dumps(diff, default=str),
                    v.get("created_by"), v.get("created_at"),
                ),
            )
            n += cur.rowcount
        return n

    def _mirror_runtime_config_activation(
        self, cur: Any, portfolio_id: str, trade_date: Any,
    ) -> int:
        """Mirror paper_v2.runtime_config_activation rows. Source has trade_date
        column so we restrict by (portfolio_id, trade_date) to avoid pulling
        unrelated history every replay."""
        cur.execute(
            """SELECT activation_id, portfolio_id, trade_date, profile_version_id,
                      status, activated_at, activated_by
               FROM paper_v2.runtime_config_activation
               WHERE portfolio_id = %s AND trade_date = %s""",
            (portfolio_id, trade_date),
        )
        n = 0
        for a in cur.fetchall():
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_runtime_config_activation (
                    activation_id, portfolio_id, profile_version_id,
                    activated_at, activated_by, captured_at
                ) VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (activation_id) DO NOTHING
                """,
                (
                    a["activation_id"], a["portfolio_id"],
                    a.get("profile_version_id"),
                    a.get("activated_at"), a.get("activated_by"),
                ),
            )
            n += cur.rowcount
        return n

    def _mirror_execution_policy_activation(
        self, cur: Any, portfolio_id: str, trade_date: Any,
    ) -> int:
        cur.execute(
            """SELECT activation_id, portfolio_id, trade_date, policy_id,
                      policy_sha256, policy_name, policy_json, status,
                      activated_at, activated_by
               FROM paper_v2.execution_policy_activation
               WHERE portfolio_id = %s AND trade_date = %s""",
            (portfolio_id, trade_date),
        )
        n = 0
        for a in cur.fetchall():
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_execution_policy_activation (
                    activation_id, portfolio_id, policy_sha256, policy_json,
                    activated_at, captured_at
                ) VALUES (%s, %s, %s, %s::jsonb, %s, NOW())
                ON CONFLICT (activation_id) DO NOTHING
                """,
                (
                    a["activation_id"], a["portfolio_id"],
                    a.get("policy_sha256"),
                    json.dumps(a.get("policy_json") or {}, default=str),
                    a.get("activated_at"),
                ),
            )
            n += cur.rowcount
        return n

    def _mirror_reset_audit(self, cur: Any, portfolio_id: str) -> int:
        """Mirror paper_v2.reset_audit. Source lacks reset_type / reset_reason /
        snapshot_before_json — synthesize from (rerun_policy, deleted_counts)
        per BUG-007. Source audit_id is bigint, cast to TEXT.
        """
        cur.execute(
            """SELECT audit_id, portfolio_id, rerun_policy, start_date, end_date,
                      confirm_text, deleted_counts, status, context, created_at
               FROM paper_v2.reset_audit WHERE portfolio_id = %s""",
            (portfolio_id,),
        )
        n = 0
        for r in cur.fetchall():
            reset_type = synth.synthesize_reset_audit_reset_type(
                r.get("rerun_policy"), r.get("deleted_counts"),
            )
            reset_reason = r.get("rerun_policy") or "synthesized_from_source"
            snapshot_before = {
                "rerun_policy": r.get("rerun_policy"),
                "start_date": str(r.get("start_date")) if r.get("start_date") else None,
                "end_date": str(r.get("end_date")) if r.get("end_date") else None,
                "deleted_counts": r.get("deleted_counts"),
                "context": r.get("context"),
                "status": r.get("status"),
            }
            cur.execute(
                """
                INSERT INTO qe_archive.paper_v2_reset_audit (
                    audit_id, portfolio_id, reset_type, reset_reason,
                    snapshot_before_json, reset_at, captured_at
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, NOW())
                ON CONFLICT (audit_id) DO NOTHING
                """,
                (
                    _archive_text_id(r["audit_id"]), r["portfolio_id"], reset_type, reset_reason,
                    json.dumps(snapshot_before, default=str),
                    r.get("created_at"),
                ),
            )
            n += cur.rowcount
        return n
