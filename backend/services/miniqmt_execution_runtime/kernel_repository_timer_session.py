"""Worker, timer, session, and bounded recovery operations for K2-A."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

from .plugin_canonical import canonical_utc_datetime_v1, thaw_json_v1
from .kernel_clock import ExchangeSessionProjectionV1, project_exchange_session_v1
from .kernel_repository_common import (
    KernelRepositoryConflict,
    _bounded_limit,
    _json,
    _model_from_json,
    _row_json,
)
from .kernel_repository_projection import (
    _assert_scalar_columns,
    _exchange_session_scalar_projection,
    _timer_occurrence_scalar_projection,
    _timer_schedule_scalar_projection,
    _worker_startup_scalar_projection,
)
from .plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    DeliveryStatusV1,
    ExecutionAlgoTimerOccurrenceStatusV1,
    ExecutionAlgoTimerOccurrenceV1,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoTimerScheduleV1,
    ExchangeSessionAuthorityV1,
    KernelWorkerStartupReceiptV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    kernel_lease_fence_token_v1,
    transaction_commit_identity_v1,
)


class KernelRepositoryTimerSessionMixin:
    """Own worker startup, timer/session facts, and deterministic recovery queries."""

    def start_worker_incarnation(
        self,
        *,
        worker_id: str,
        process_role: str,
        source_revision: str,
        started_at_utc: Any,
    ) -> KernelWorkerStartupReceiptV1:
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_kernel_worker_epoch(worker_id, process_role)
                    VALUES (%s, %s)
                    ON CONFLICT (worker_id, process_role) DO NOTHING
                    """,
                    (worker_id, process_role),
                )
                cur.execute(
                    """
                    SELECT incarnation_sequence
                    FROM qmt_strategy.execution_kernel_worker_epoch
                    WHERE worker_id = %s AND process_role = %s
                    FOR UPDATE
                    """,
                    (worker_id, process_role),
                )
                row = cur.fetchone()
                if row is None:
                    raise KernelRepositoryConflict("worker epoch row disappeared while locked")
                sequence = int(row["incarnation_sequence"]) + 1
                provisional = KernelWorkerStartupReceiptV1.create(
                    worker_id=worker_id,
                    process_role=process_role,
                    incarnation_sequence=sequence,
                    source_revision=source_revision,
                    started_at_utc=started_at_utc,
                    startup_transaction_commit_identity="mqtx_pending_worker_startup",
                )
                transaction_id = transaction_commit_identity_v1(
                    operation="START_WORKER_INCARNATION",
                    owner_identities=(worker_id, process_role),
                    input_hashes=(),
                    output_identities=(provisional.process_incarnation_id,),
                )
                receipt = KernelWorkerStartupReceiptV1.create(
                    worker_id=worker_id,
                    process_role=process_role,
                    incarnation_sequence=sequence,
                    source_revision=source_revision,
                    started_at_utc=started_at_utc,
                    startup_transaction_commit_identity=transaction_id,
                )
                cur.execute(
                    """
                    UPDATE qmt_strategy.execution_kernel_worker_epoch
                    SET incarnation_sequence = %s, updated_at_utc = %s
                    WHERE worker_id = %s AND process_role = %s AND incarnation_sequence = %s
                    """,
                    (sequence, receipt.started_at_utc, worker_id, process_role, sequence - 1),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("worker epoch CAS failed")
                receipt_projection = _worker_startup_scalar_projection(receipt)
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_kernel_worker_incarnation(
                        worker_id, process_role, incarnation_sequence, source_revision,
                        process_incarnation_id, started_at_utc, startup_transaction_commit_identity,
                        receipt_sha256, startup_receipt_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        receipt_projection["worker_id"],
                        receipt_projection["process_role"],
                        receipt_projection["incarnation_sequence"],
                        receipt_projection["source_revision"],
                        receipt_projection["process_incarnation_id"],
                        receipt_projection["started_at_utc"],
                        receipt_projection["startup_transaction_commit_identity"],
                        receipt_projection["receipt_sha256"],
                        _json(receipt.model_dump(mode="json")),
                    ),
                )
        return self.read_worker_startup_receipt(receipt.process_incarnation_id)

    def read_worker_startup_receipt(self, process_incarnation_id: str) -> KernelWorkerStartupReceiptV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT worker_id,process_role,incarnation_sequence,source_revision,
                           process_incarnation_id,started_at_utc,startup_transaction_commit_identity,
                           receipt_sha256,startup_receipt_json
                    FROM qmt_strategy.execution_kernel_worker_incarnation
                    WHERE process_incarnation_id = %s
                    """,
                    (process_incarnation_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(process_incarnation_id)
        receipt = _model_from_json(KernelWorkerStartupReceiptV1, _row_json(row, "startup_receipt_json"))
        _assert_scalar_columns(
            row,
            _worker_startup_scalar_projection(receipt),
            carrier_name="worker startup receipt",
        )
        return receipt

    def write_timer_schedule(self, schedule: ExecutionAlgoTimerScheduleV1) -> ExecutionAlgoTimerScheduleV1:
        if not isinstance(schedule, ExecutionAlgoTimerScheduleV1):
            raise TypeError("schedule must be ExecutionAlgoTimerScheduleV1")
        if schedule.lease_owner is not None:
            self._verify_lease_owner(schedule.lease_owner)
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._write_timer_schedule_with_cursor(cur, schedule)
        readback = self.read_timer_schedule(schedule.schedule_id)
        if readback != schedule:
            raise KernelRepositoryConflict("timer schedule post-commit readback differs from writer payload")
        return readback

    def _write_timer_schedule_with_cursor(self, cur: Any, schedule: ExecutionAlgoTimerScheduleV1) -> None:
        cur.execute(
            "SELECT carrier_json FROM qmt_strategy.execution_algo_timer_schedule WHERE schedule_id=%s FOR UPDATE",
            (schedule.schedule_id,),
        )
        row = cur.fetchone()
        previous = None
        if row is not None:
            previous = _model_from_json(ExecutionAlgoTimerScheduleV1, _row_json(row, "carrier_json"))
            if previous != schedule:
                schedule.validate_successor_v1(previous)
        else:
            try:
                schedule.validate_initial_v1()
            except ValueError as exc:
                raise KernelRepositoryConflict("timer schedule first write requires exact initial state") from exc
        projection = _timer_schedule_scalar_projection(schedule)
        if row is None:
            cur.execute(
                """
                INSERT INTO qmt_strategy.execution_algo_timer_schedule(
                    schedule_id,runtime_id,algo_instance_id,timer_name,schedule_epoch,due_at_exchange_utc,
                    catch_up_policy,payload_json,payload_sha256,status,timer_occurrence_id,emitted_event_id,
                    lease_owner,lease_worker_id,lease_process_incarnation_id,lease_epoch,lease_fence_token,
                    lease_expires_at_utc,row_version,created_at_utc,updated_at_utc,closed_at_utc,
                    schedule_receipt_sha256,carrier_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (
                    projection["schedule_id"],
                    projection["runtime_id"],
                    projection["algo_instance_id"],
                    projection["timer_name"],
                    projection["schedule_epoch"],
                    projection["due_at_exchange_utc"],
                    projection["catch_up_policy"],
                    _json(projection["payload_json"]),
                    projection["payload_sha256"],
                    projection["status"],
                    projection["timer_occurrence_id"],
                    projection["emitted_event_id"],
                    projection["lease_owner"],
                    projection["lease_worker_id"],
                    projection["lease_process_incarnation_id"],
                    projection["lease_epoch"],
                    projection["lease_fence_token"],
                    projection["lease_expires_at_utc"],
                    projection["row_version"],
                    projection["created_at_utc"],
                    projection["updated_at_utc"],
                    projection["closed_at_utc"],
                    projection["schedule_receipt_sha256"],
                    _json(schedule.model_dump(mode="json")),
                ),
            )
        elif previous != schedule:
            cur.execute(
                """
                UPDATE qmt_strategy.execution_algo_timer_schedule
                SET status=%s,emitted_event_id=%s,lease_owner=%s,lease_worker_id=%s,
                    lease_process_incarnation_id=%s,lease_epoch=%s,lease_fence_token=%s,
                    lease_expires_at_utc=%s,row_version=%s,updated_at_utc=%s,closed_at_utc=%s,
                    schedule_receipt_sha256=%s,carrier_json=%s
                WHERE schedule_id=%s AND row_version=%s AND lease_owner IS NOT DISTINCT FROM %s
                  AND lease_epoch=%s AND lease_fence_token IS NOT DISTINCT FROM %s
                """,
                (
                    projection["status"],
                    projection["emitted_event_id"],
                    projection["lease_owner"],
                    projection["lease_worker_id"],
                    projection["lease_process_incarnation_id"],
                    projection["lease_epoch"],
                    projection["lease_fence_token"],
                    projection["lease_expires_at_utc"],
                    projection["row_version"],
                    projection["updated_at_utc"],
                    projection["closed_at_utc"],
                    projection["schedule_receipt_sha256"],
                    _json(schedule.model_dump(mode="json")),
                    projection["schedule_id"],
                    previous.row_version,
                    previous.lease_owner,
                    previous.lease_epoch,
                    previous.lease_fence_token,
                ),
            )
            if cur.rowcount != 1:
                raise KernelRepositoryConflict("timer schedule CAS failed")
        cur.execute(
            "SELECT carrier_json FROM qmt_strategy.execution_algo_timer_schedule WHERE schedule_id=%s",
            (schedule.schedule_id,),
        )
        persisted_row = cur.fetchone()
        if persisted_row is None:
            raise KernelRepositoryConflict("timer schedule write did not persist its identity")
        persisted = _model_from_json(ExecutionAlgoTimerScheduleV1, _row_json(persisted_row, "carrier_json"))
        if persisted != schedule:
            raise KernelRepositoryConflict("timer schedule identity exists with different immutable payload")

    def write_timer_occurrence(self, occurrence: ExecutionAlgoTimerOccurrenceV1) -> ExecutionAlgoTimerOccurrenceV1:
        if not isinstance(occurrence, ExecutionAlgoTimerOccurrenceV1):
            raise TypeError("occurrence must be ExecutionAlgoTimerOccurrenceV1")
        if occurrence.lease_owner is not None:
            self._verify_lease_owner(occurrence.lease_owner)
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._write_timer_occurrence_with_cursor(cur, occurrence)
        readback = self.read_timer_occurrence(occurrence.timer_occurrence_id)
        if readback != occurrence:
            raise KernelRepositoryConflict("timer occurrence post-commit readback differs from writer payload")
        return readback

    def _write_timer_occurrence_with_cursor(self, cur: Any, occurrence: ExecutionAlgoTimerOccurrenceV1) -> None:
        cur.execute(
            "SELECT carrier_json FROM qmt_strategy.execution_algo_timer_occurrence "
            "WHERE timer_occurrence_id=%s FOR UPDATE",
            (occurrence.timer_occurrence_id,),
        )
        row = cur.fetchone()
        previous = None
        if row is not None:
            previous = _model_from_json(ExecutionAlgoTimerOccurrenceV1, _row_json(row, "carrier_json"))
            if previous != occurrence:
                occurrence.validate_successor_v1(previous)
        else:
            try:
                occurrence.validate_initial_v1()
            except ValueError as exc:
                raise KernelRepositoryConflict("timer occurrence first write requires exact initial state") from exc
        projection = _timer_occurrence_scalar_projection(occurrence)
        if row is None:
            cur.execute(
                """
                INSERT INTO qmt_strategy.execution_algo_timer_occurrence(
                    timer_occurrence_id,schedule_id,runtime_id,algo_instance_id,due_at_exchange_utc,
                    exchange_session_authority_sha256,status,emitted_event_id,catch_up_receipt_sha256,
                    lease_owner,lease_worker_id,lease_process_incarnation_id,lease_epoch,lease_fence_token,
                    lease_expires_at_utc,row_version,created_at_utc,closed_at_utc,
                    occurrence_receipt_sha256,carrier_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (
                    projection["timer_occurrence_id"],
                    projection["schedule_id"],
                    projection["runtime_id"],
                    projection["algo_instance_id"],
                    projection["due_at_exchange_utc"],
                    projection["exchange_session_authority_sha256"],
                    projection["status"],
                    projection["emitted_event_id"],
                    projection["catch_up_receipt_sha256"],
                    projection["lease_owner"],
                    projection["lease_worker_id"],
                    projection["lease_process_incarnation_id"],
                    projection["lease_epoch"],
                    projection["lease_fence_token"],
                    projection["lease_expires_at_utc"],
                    projection["row_version"],
                    projection["created_at_utc"],
                    projection["closed_at_utc"],
                    projection["occurrence_receipt_sha256"],
                    _json(occurrence.model_dump(mode="json")),
                ),
            )
        elif previous != occurrence:
            cur.execute(
                """
                UPDATE qmt_strategy.execution_algo_timer_occurrence
                SET status=%s,emitted_event_id=%s,catch_up_receipt_sha256=%s,
                    lease_owner=%s,lease_worker_id=%s,lease_process_incarnation_id=%s,
                    lease_epoch=%s,lease_fence_token=%s,lease_expires_at_utc=%s,row_version=%s,
                    closed_at_utc=%s,occurrence_receipt_sha256=%s,carrier_json=%s
                WHERE timer_occurrence_id=%s AND row_version=%s
                  AND lease_owner IS NOT DISTINCT FROM %s AND lease_epoch=%s
                  AND lease_fence_token IS NOT DISTINCT FROM %s
                """,
                (
                    projection["status"],
                    projection["emitted_event_id"],
                    projection["catch_up_receipt_sha256"],
                    projection["lease_owner"],
                    projection["lease_worker_id"],
                    projection["lease_process_incarnation_id"],
                    projection["lease_epoch"],
                    projection["lease_fence_token"],
                    projection["lease_expires_at_utc"],
                    projection["row_version"],
                    projection["closed_at_utc"],
                    projection["occurrence_receipt_sha256"],
                    _json(occurrence.model_dump(mode="json")),
                    projection["timer_occurrence_id"],
                    previous.row_version,
                    previous.lease_owner,
                    previous.lease_epoch,
                    previous.lease_fence_token,
                ),
            )
            if cur.rowcount != 1:
                raise KernelRepositoryConflict("timer occurrence CAS failed")
        cur.execute(
            "SELECT carrier_json FROM qmt_strategy.execution_algo_timer_occurrence WHERE timer_occurrence_id=%s",
            (occurrence.timer_occurrence_id,),
        )
        persisted_row = cur.fetchone()
        if persisted_row is None:
            raise KernelRepositoryConflict("timer occurrence write did not persist its identity")
        persisted = _model_from_json(ExecutionAlgoTimerOccurrenceV1, _row_json(persisted_row, "carrier_json"))
        if persisted != occurrence:
            raise KernelRepositoryConflict("timer occurrence identity exists with different immutable payload")

    def read_timer_schedule(self, schedule_id: str) -> ExecutionAlgoTimerScheduleV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT schedule_id,runtime_id,algo_instance_id,timer_name,schedule_epoch,
                           due_at_exchange_utc,catch_up_policy,payload_json,payload_sha256,
                           status,timer_occurrence_id,emitted_event_id,
                           lease_owner,lease_worker_id,lease_process_incarnation_id,lease_epoch,
                           lease_fence_token,lease_expires_at_utc,row_version,created_at_utc,
                           updated_at_utc,closed_at_utc,schedule_receipt_sha256,carrier_json
                    FROM qmt_strategy.execution_algo_timer_schedule WHERE schedule_id=%s
                    """,
                    (schedule_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(schedule_id)
        schedule = _model_from_json(ExecutionAlgoTimerScheduleV1, _row_json(row, "carrier_json"))
        _assert_scalar_columns(
            row,
            _timer_schedule_scalar_projection(schedule),
            carrier_name="timer schedule",
        )
        return schedule

    def read_timer_occurrence(self, timer_occurrence_id: str) -> ExecutionAlgoTimerOccurrenceV1:
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT timer_occurrence_id,schedule_id,runtime_id,algo_instance_id,due_at_exchange_utc,
                           exchange_session_authority_sha256,status,emitted_event_id,
                           catch_up_receipt_sha256,lease_owner,
                           lease_worker_id,lease_process_incarnation_id,lease_epoch,lease_fence_token,
                           lease_expires_at_utc,row_version,created_at_utc,closed_at_utc,
                           occurrence_receipt_sha256,carrier_json
                    FROM qmt_strategy.execution_algo_timer_occurrence WHERE timer_occurrence_id=%s
                    """,
                    (timer_occurrence_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(timer_occurrence_id)
        occurrence = _model_from_json(ExecutionAlgoTimerOccurrenceV1, _row_json(row, "carrier_json"))
        _assert_scalar_columns(
            row,
            _timer_occurrence_scalar_projection(occurrence),
            carrier_name="timer occurrence",
        )
        return occurrence

    def write_exchange_session_authority(self, authority: ExchangeSessionAuthorityV1) -> ExchangeSessionAuthorityV1:
        if not isinstance(authority, ExchangeSessionAuthorityV1):
            raise TypeError("authority must be ExchangeSessionAuthorityV1")
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT trade_date FROM qmt_strategy.execution_runtime WHERE runtime_id=%s FOR SHARE",
                    (authority.runtime_id,),
                )
                runtime_row = cur.fetchone()
                if runtime_row is None:
                    raise KeyError(authority.runtime_id)
                if runtime_row["trade_date"] != date.fromisoformat(authority.exchange_trade_date):
                    raise KernelRepositoryConflict("exchange-session trade date conflicts with runtime owner")
                authority_projection = _exchange_session_scalar_projection(authority)
                cur.execute(
                    """
                    INSERT INTO qmt_strategy.execution_exchange_session_authority(
                        runtime_id,exchange_trade_date,calendar_snapshot_set_id,calendar_snapshot_set_sha256,
                        session_definition_version,authority_sha256,authority_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (runtime_id,exchange_trade_date) DO NOTHING
                    """,
                    (
                        authority_projection["runtime_id"],
                        authority_projection["exchange_trade_date"],
                        authority_projection["calendar_snapshot_set_id"],
                        authority_projection["calendar_snapshot_set_sha256"],
                        authority_projection["session_definition_version"],
                        authority_projection["authority_sha256"],
                        _json(authority.model_dump(mode="json")),
                    ),
                )
                cur.execute(
                    """
                    SELECT authority_json FROM qmt_strategy.execution_exchange_session_authority
                    WHERE runtime_id=%s AND exchange_trade_date=%s
                    """,
                    (authority.runtime_id, authority.exchange_trade_date),
                )
                persisted = _model_from_json(ExchangeSessionAuthorityV1, _row_json(cur.fetchone(), "authority_json"))
                if persisted != authority:
                    raise KernelRepositoryConflict("exchange-session authority drift for runtime/trade date")
        readback = self.read_exchange_session_authority(
            runtime_id=authority.runtime_id,
            exchange_trade_date=date.fromisoformat(authority.exchange_trade_date),
        )
        if readback != authority:
            raise KernelRepositoryConflict("exchange-session post-commit readback differs from writer payload")
        return readback

    def read_exchange_session_authority(
        self, *, runtime_id: str, exchange_trade_date: date
    ) -> ExchangeSessionAuthorityV1:
        if type(exchange_trade_date) is not date:
            raise TypeError("exchange_trade_date must be a date")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._read_exchange_session_authority_with_cursor(
                    cur,
                    runtime_id=runtime_id,
                    exchange_trade_date=exchange_trade_date,
                    lock=False,
                )

    @staticmethod
    def _read_exchange_session_authority_with_cursor(
        cur: Any,
        *,
        runtime_id: str,
        exchange_trade_date: date,
        lock: bool,
    ) -> ExchangeSessionAuthorityV1:
        """Read the exact session authority on an existing product-route transaction."""

        if type(runtime_id) is not str or not runtime_id or runtime_id != runtime_id.strip():
            raise TypeError("runtime_id must be a non-empty canonical string")
        if type(exchange_trade_date) is not date:
            raise TypeError("exchange_trade_date must be a date")
        cur.execute(
            """
            SELECT authority.runtime_id,authority.exchange_trade_date,
                   authority.calendar_snapshot_set_id,authority.calendar_snapshot_set_sha256,
                   authority.session_definition_version,authority.authority_sha256,
                   authority.authority_json,runtime.trade_date AS runtime_trade_date
            FROM qmt_strategy.execution_exchange_session_authority AS authority
            JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=authority.runtime_id
            WHERE authority.runtime_id=%s AND authority.exchange_trade_date=%s
            """
            + (" FOR SHARE OF authority,runtime" if lock else ""),
            (runtime_id, exchange_trade_date),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError((runtime_id, exchange_trade_date))
        authority = _model_from_json(ExchangeSessionAuthorityV1, _row_json(row, "authority_json"))
        _assert_scalar_columns(
            row,
            _exchange_session_scalar_projection(authority),
            carrier_name="exchange-session authority",
        )
        if row["exchange_trade_date"] != row["runtime_trade_date"]:
            raise KernelRepositoryConflict("exchange-session trade date drifts from runtime owner")
        return authority

    def _read_exchange_session_projection(
        self,
        *,
        runtime_id: str,
        observed_at_utc: Any,
    ) -> ExchangeSessionProjectionV1:
        """Internal exact projection for events emitted outside the clock worker."""

        observed_text = canonical_utc_datetime_v1(observed_at_utc, field_name="observed_at_utc")
        observed = datetime.fromisoformat(observed_text.replace("Z", "+00:00"))
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT trade_date FROM qmt_strategy.execution_runtime WHERE runtime_id=%s",
                    (runtime_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(runtime_id)
        trade_date = row["trade_date"]
        if type(trade_date) is not date:
            raise KernelRepositoryConflict("runtime trade date readback is invalid")
        authority = self.read_exchange_session_authority(
            runtime_id=runtime_id,
            exchange_trade_date=trade_date,
        )
        local_date = observed.astimezone(ZoneInfo("Asia/Shanghai")).date()
        if local_date < trade_date:
            raise KernelRepositoryConflict("outcome event time precedes its runtime exchange trade date")
        if local_date == trade_date:
            return project_exchange_session_v1(authority, observed)
        close_anchor = datetime.combine(
            trade_date,
            time(23, 59, 59, 999999),
            tzinfo=ZoneInfo("Asia/Shanghai"),
        )
        closed = project_exchange_session_v1(authority, close_anchor)
        return replace(closed, observed_at_utc=observed_text)

    def read_runtime_last_event_sequence(self, runtime_id: str) -> int:
        if type(runtime_id) is not str or not runtime_id or runtime_id != runtime_id.strip():
            raise ValueError("runtime_id must be a non-empty trim-stable strict string")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT last_event_sequence,archived_at FROM qmt_strategy.execution_runtime WHERE runtime_id=%s",
                    (runtime_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(runtime_id)
        if row["archived_at"] is not None:
            raise KernelRepositoryConflict("cannot allocate a clock event for an archived runtime")
        sequence = row["last_event_sequence"]
        if type(sequence) is not int or sequence < 0:
            raise KernelRepositoryConflict("runtime last_event_sequence is not a non-negative strict integer")
        return sequence

    def read_runtime_trade_date(self, runtime_id: str) -> date:
        if type(runtime_id) is not str or not runtime_id or runtime_id != runtime_id.strip():
            raise ValueError("runtime_id must be a canonical identity")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT trade_date FROM qmt_strategy.execution_runtime WHERE runtime_id=%s",
                    (runtime_id,),
                )
                row = cur.fetchone()
        if row is None:
            raise KeyError(runtime_id)
        trade_date_value = row["trade_date"]
        if type(trade_date_value) is not date:
            raise KernelRepositoryConflict("runtime trade-date scalar readback is invalid")
        return trade_date_value

    def claim_due_timer_schedules_atomic(
        self,
        *,
        runtime_id: str,
        exchange_trade_date: date,
        exchange_session_authority_sha256: str,
        due_cutoff_at_utc: Any,
        observed_at_utc: Any,
        lease_owner: str,
        lease_expires_at_utc: Any,
        eligible_algo_statuses: Sequence[str],
        limit: int,
    ) -> tuple[tuple[ExecutionAlgoTimerScheduleV1, ExecutionAlgoTimerOccurrenceV1], ...]:
        """Atomically claim/reclaim bounded due schedule+occurrence pairs."""

        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("timer claim limit must be a strict integer in [1, 200]")
        if type(exchange_trade_date) is not date:
            raise TypeError("exchange_trade_date must be an exact date")
        observed = canonical_utc_datetime_v1(observed_at_utc, field_name="observed_at_utc")
        cutoff = canonical_utc_datetime_v1(due_cutoff_at_utc, field_name="due_cutoff_at_utc")
        lease_expiry = canonical_utc_datetime_v1(lease_expires_at_utc, field_name="lease_expires_at_utc")
        if cutoff > observed:
            raise ValueError("timer due cutoff cannot be later than observed_at_utc")
        if lease_expiry <= observed:
            raise ValueError("timer claim lease expiry must be later than observed_at_utc")
        exact_algo_statuses = tuple(eligible_algo_statuses)
        if (
            not exact_algo_statuses
            or len(set(exact_algo_statuses)) != len(exact_algo_statuses)
            or any(status not in {"ACTIVE", "PAUSED"} for status in exact_algo_statuses)
        ):
            raise ValueError("eligible_algo_statuses must be a non-empty unique ACTIVE/PAUSED subset")
        worker_id, separator, process_incarnation_id = lease_owner.partition(":")
        if not separator or not worker_id or not process_incarnation_id:
            raise ValueError("lease_owner must be worker_id:process_incarnation_id")
        claimed: list[tuple[ExecutionAlgoTimerScheduleV1, ExecutionAlgoTimerOccurrenceV1]] = []
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 1 FROM qmt_strategy.execution_kernel_worker_incarnation
                    WHERE worker_id=%s AND process_incarnation_id=%s
                    """,
                    (worker_id, process_incarnation_id),
                )
                if cur.fetchone() is None:
                    raise KernelRepositoryConflict("timer claim references unknown worker incarnation")
                cur.execute(
                    """
                    SELECT authority_json FROM qmt_strategy.execution_exchange_session_authority
                    WHERE runtime_id=%s AND exchange_trade_date=%s FOR SHARE
                    """,
                    (runtime_id, exchange_trade_date),
                )
                authority_row = cur.fetchone()
                if authority_row is None:
                    raise KernelRepositoryConflict("timer claim has no durable exchange-session authority")
                authority = _model_from_json(
                    ExchangeSessionAuthorityV1,
                    _row_json(authority_row, "authority_json"),
                )
                if authority.authority_sha256 != exchange_session_authority_sha256:
                    raise KernelRepositoryConflict("timer claim exchange-session authority hash drift")
                cur.execute(
                    """
                    SELECT schedule.carrier_json
                    FROM qmt_strategy.execution_algo_timer_schedule AS schedule
                    JOIN qmt_strategy.execution_algo_instance AS algo
                      ON algo.runtime_id=schedule.runtime_id
                     AND algo.algo_instance_id=schedule.algo_instance_id
                    WHERE schedule.runtime_id=%s AND schedule.due_at_exchange_utc<=%s
                      AND algo.status=ANY(%s::text[])
                      AND (
                        schedule.status='SCHEDULED'
                        OR (schedule.status='EMITTING' AND schedule.lease_expires_at_utc<=%s)
                      )
                    ORDER BY schedule.due_at_exchange_utc,schedule.schedule_id
                    LIMIT %s
                    FOR UPDATE OF schedule SKIP LOCKED
                    """,
                    (runtime_id, cutoff, list(exact_algo_statuses), observed, limit),
                )
                schedules = tuple(
                    _model_from_json(ExecutionAlgoTimerScheduleV1, _row_json(row, "carrier_json"))
                    for row in cur.fetchall()
                )
                for previous_schedule in schedules:
                    cur.execute(
                        "SELECT carrier_json FROM qmt_strategy.execution_algo_timer_occurrence "
                        "WHERE timer_occurrence_id=%s FOR UPDATE",
                        (previous_schedule.timer_occurrence_id,),
                    )
                    occurrence_row = cur.fetchone()
                    previous_occurrence = (
                        None
                        if occurrence_row is None
                        else _model_from_json(
                            ExecutionAlgoTimerOccurrenceV1,
                            _row_json(occurrence_row, "carrier_json"),
                        )
                    )
                    if previous_schedule.status is ExecutionAlgoTimerScheduleStatusV1.SCHEDULED:
                        if previous_occurrence is not None:
                            raise KernelRepositoryConflict(
                                "SCHEDULED timer already has an occurrence and cannot be claimed"
                            )
                    elif (
                        previous_occurrence is None
                        or previous_occurrence.status is not ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED
                        or previous_occurrence.lease_expires_at_utc is None
                        or previous_occurrence.lease_expires_at_utc > observed
                        or previous_occurrence.lease_epoch != previous_schedule.lease_epoch
                        or previous_occurrence.lease_owner != previous_schedule.lease_owner
                    ):
                        raise KernelRepositoryConflict(
                            "stale EMITTING timer does not have one matching expired CLAIMED occurrence"
                        )
                    lease_epoch = previous_schedule.lease_epoch + 1
                    mutation = TimerMutationV1.create(
                        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
                        algo_instance_id=previous_schedule.algo_instance_id,
                        transition_id=f"mqtimerclaim_{lease_epoch}",
                        ordinal=0,
                        timer_name=previous_schedule.timer_name,
                        schedule_epoch=previous_schedule.schedule_epoch,
                        due_at_exchange_utc=previous_schedule.due_at_exchange_utc,
                        catch_up_policy=previous_schedule.catch_up_policy,
                        payload=thaw_json_v1(previous_schedule.payload),
                    )
                    schedule = ExecutionAlgoTimerScheduleV1.create(
                        runtime_id=previous_schedule.runtime_id,
                        mutation=mutation,
                        status=ExecutionAlgoTimerScheduleStatusV1.EMITTING,
                        emitted_event_id=None,
                        lease_owner=lease_owner,
                        lease_epoch=lease_epoch,
                        lease_fence_token=kernel_lease_fence_token_v1(
                            owner_type="TIMER_SCHEDULE",
                            owner_id=previous_schedule.schedule_id,
                            lease_epoch=lease_epoch,
                            lease_owner=lease_owner,
                        ),
                        lease_expires_at_utc=lease_expiry,
                        row_version=previous_schedule.row_version + 1,
                        created_at_utc=previous_schedule.created_at_utc,
                        updated_at_utc=observed,
                        closed_at_utc=None,
                    )
                    occurrence = ExecutionAlgoTimerOccurrenceV1.create(
                        schedule=schedule,
                        exchange_session_authority_sha256=exchange_session_authority_sha256,
                        status=ExecutionAlgoTimerOccurrenceStatusV1.CLAIMED,
                        emitted_event_id=None,
                        catch_up_receipt_sha256=None,
                        lease_owner=lease_owner,
                        lease_epoch=lease_epoch,
                        lease_fence_token=kernel_lease_fence_token_v1(
                            owner_type="TIMER_OCCURRENCE",
                            owner_id=schedule.timer_occurrence_id,
                            lease_epoch=lease_epoch,
                            lease_owner=lease_owner,
                        ),
                        lease_expires_at_utc=lease_expiry,
                        row_version=1 if previous_occurrence is None else previous_occurrence.row_version + 1,
                        created_at_utc=observed if previous_occurrence is None else previous_occurrence.created_at_utc,
                        closed_at_utc=None,
                    )
                    schedule.validate_successor_v1(previous_schedule)
                    if previous_occurrence is not None:
                        occurrence.validate_successor_v1(previous_occurrence)
                    self._write_timer_schedule_with_cursor(cur, schedule)
                    self._write_timer_occurrence_with_cursor(cur, occurrence)
                    claimed.append((schedule, occurrence))
        readback: list[tuple[ExecutionAlgoTimerScheduleV1, ExecutionAlgoTimerOccurrenceV1]] = []
        for schedule, occurrence in claimed:
            persisted = (
                self.read_timer_schedule(schedule.schedule_id),
                self.read_timer_occurrence(occurrence.timer_occurrence_id),
            )
            if persisted != (schedule, occurrence):
                raise KernelRepositoryConflict("timer claim post-commit readback differs from atomic writer payload")
            readback.append(persisted)
        return tuple(readback)

    def finalize_timer_claim_atomic(
        self,
        *,
        schedule: ExecutionAlgoTimerScheduleV1,
        occurrence: ExecutionAlgoTimerOccurrenceV1,
    ) -> tuple[ExecutionAlgoTimerScheduleV1, ExecutionAlgoTimerOccurrenceV1]:
        if not isinstance(schedule, ExecutionAlgoTimerScheduleV1) or not isinstance(
            occurrence, ExecutionAlgoTimerOccurrenceV1
        ):
            raise TypeError("timer finalization requires strict schedule and occurrence carriers")
        allowed_pairs = {
            (
                ExecutionAlgoTimerScheduleStatusV1.EMITTED,
                ExecutionAlgoTimerOccurrenceStatusV1.EVENT_COMMITTED,
            ),
            (ExecutionAlgoTimerScheduleStatusV1.EXPIRED, ExecutionAlgoTimerOccurrenceStatusV1.SKIPPED),
            (ExecutionAlgoTimerScheduleStatusV1.EXPIRED, ExecutionAlgoTimerOccurrenceStatusV1.EXPIRED),
        }
        if (schedule.status, occurrence.status) not in allowed_pairs:
            raise ValueError("timer finalization status pair is not registered")
        if (
            schedule.runtime_id != occurrence.runtime_id
            or schedule.schedule_id != occurrence.schedule_id
            or schedule.timer_occurrence_id != occurrence.timer_occurrence_id
            or schedule.algo_instance_id != occurrence.algo_instance_id
            or schedule.lease_epoch != occurrence.lease_epoch
            or schedule.emitted_event_id != occurrence.emitted_event_id
        ):
            raise ValueError("timer finalization carriers do not form one exact occurrence closure")
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                self._write_timer_occurrence_with_cursor(cur, occurrence)
                self._write_timer_schedule_with_cursor(cur, schedule)
        readback = (
            self.read_timer_schedule(schedule.schedule_id),
            self.read_timer_occurrence(occurrence.timer_occurrence_id),
        )
        if readback != (schedule, occurrence):
            raise KernelRepositoryConflict("timer finalization post-commit readback differs from atomic writer payload")
        return readback

    def list_recovery_deliveries(
        self, *, runtime_id: str, trade_date: date, statuses: Sequence[str], limit: int
    ) -> tuple[AlgoDeliveryPersistenceV1, ...]:
        delivery_ids = self._recovery_identities(
            table="execution_algo_event_delivery",
            runtime_id=runtime_id,
            trade_date=trade_date,
            statuses=statuses,
            limit=limit,
        )
        return tuple(self.read_delivery(delivery_id) for delivery_id in delivery_ids)

    def list_recovery_outbox_commands(
        self, *, runtime_id: str, trade_date: date, statuses: Sequence[str], limit: int
    ) -> tuple[BrokerCommandOutboxV1, ...]:
        command_ids = self._recovery_identities(
            table="execution_algo_command_outbox",
            runtime_id=runtime_id,
            trade_date=trade_date,
            statuses=statuses,
            limit=limit,
        )
        return tuple(self.read_outbox_command(command_id) for command_id in command_ids)

    def list_recovery_timer_occurrences(
        self, *, runtime_id: str, trade_date: date, statuses: Sequence[str], limit: int
    ) -> tuple[ExecutionAlgoTimerOccurrenceV1, ...]:
        occurrence_ids = self._recovery_identities(
            table="execution_algo_timer_occurrence",
            runtime_id=runtime_id,
            trade_date=trade_date,
            statuses=statuses,
            limit=limit,
        )
        return tuple(self.read_timer_occurrence(occurrence_id) for occurrence_id in occurrence_ids)

    def _recovery_identities(
        self,
        *,
        table: str,
        runtime_id: str,
        trade_date: date,
        statuses: Sequence[str],
        limit: int,
    ) -> tuple[str, ...]:
        limit = _bounded_limit(limit)
        if type(runtime_id) is not str or not runtime_id or runtime_id != runtime_id.strip():
            raise ValueError("runtime_id must be a non-empty trim-stable strict string")
        if type(trade_date) is not date:
            raise TypeError("trade_date must be an exact date")
        exact_statuses = tuple(statuses)
        if not exact_statuses or any(type(item) is not str or not item.strip() for item in exact_statuses):
            raise ValueError("recovery statuses must be a non-empty strict string sequence")
        if len(set(exact_statuses)) != len(exact_statuses):
            raise ValueError("recovery statuses must not contain duplicates")
        table_authority = {
            "execution_algo_event_delivery": (
                {status.value for status in DeliveryStatusV1},
                "delivery_id",
                "target.created_at_utc, target.algo_delivery_sequence, target.delivery_id",
            ),
            "execution_algo_command_outbox": (
                {status.value for status in BrokerCommandOutboxStatusV1},
                "command_id",
                "target.next_attempt_at_utc NULLS FIRST, target.created_at_utc, target.command_id",
            ),
            "execution_algo_timer_occurrence": (
                {status.value for status in ExecutionAlgoTimerOccurrenceStatusV1},
                "timer_occurrence_id",
                "target.due_at_exchange_utc, target.created_at_utc, target.timer_occurrence_id",
            ),
        }
        if table not in table_authority:
            raise ValueError("unsupported recovery table")
        allowed_statuses, identity_column, order_by = table_authority[table]
        invalid = tuple(status for status in exact_statuses if status not in allowed_statuses)
        if invalid:
            raise ValueError(f"unsupported recovery statuses for {table}: {invalid}")
        query = f"""
            SELECT target.{identity_column} AS recovery_identity
            FROM qmt_strategy.{table} AS target
            JOIN qmt_strategy.execution_runtime AS runtime ON runtime.runtime_id=target.runtime_id
            WHERE target.runtime_id=%s AND runtime.trade_date=%s AND target.status=ANY(%s::text[])
            ORDER BY {order_by}
            LIMIT %s
        """
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (runtime_id, trade_date, list(exact_statuses), limit))
                rows = cur.fetchall()
        return tuple(str(row["recovery_identity"]) for row in rows)
