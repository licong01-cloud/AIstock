"""Resume admission keeps the original recoverable operation identity."""

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Lock
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.models import (
    REASON_IDEMPOTENCY_CONFLICT,
    HistoricalRangeContractError,
    HistoricalRangeOperationRequestV1,
    HistoricalRangeOperationType,
)
from backend.services.advisory_historical_range.repository import PostgresHistoricalRangeRepository


def _request(key="old", *, batch="batch", kind=HistoricalRangeOperationType.RESUME, version=7):
    return HistoricalRangeOperationRequestV1(
        operation_id=f"operation-{batch}-{kind.value}-{key}",
        batch_id=batch,
        operation_type=kind,
        operation_idempotency_key=key,
        request_payload_sha256="a" * 64,
        expected_row_version=version,
    )


class _Database:
    """Small SQL boundary double; production policy stays in the repository."""

    def __init__(self):
        self.rows = {}
        self.lock = Lock()

    def seed(self, request, *, status="RUNNING", expired=True):
        row = request.model_dump(mode="json")
        row.update(status=status, lease_expires_at=datetime.now(UTC) + timedelta(hours=-1 if expired else 1))
        self.rows[request.operation_id] = row
        return row

    @contextmanager
    def connection(self):
        cursor = _Cursor(self)
        try:
            yield SimpleNamespace(cursor=cursor.open)
        finally:
            if cursor.locked:
                self.lock.release()

    def repository(self):
        return PostgresHistoricalRangeRepository(conn_factory=self.connection, artifact_store=object())


class _Cursor:
    def __init__(self, db):
        self.db = db
        self.one = None
        self.rowcount = 0
        self.locked = False

    @contextmanager
    def open(self, **_kwargs):
        yield self

    def execute(self, query, params):
        sql = " ".join(query.split())
        self.one = None
        self.rowcount = 0
        if "pg_advisory_xact_lock" in sql:
            self.db.lock.acquire()
            self.locked = True
            return
        if sql.startswith("SELECT * FROM app.advisory_historical_range_operation"):
            batch, kind, key = params
            self.one = next(
                (
                    dict(row)
                    for row in self.db.rows.values()
                    if (row["batch_id"], row["operation_type"], row["operation_idempotency_key"]) == (batch, kind, key)
                ),
                None,
            )
            return
        if "AS lease_expired" in sql:
            batch, kind = params
            self.one = next(
                (
                    dict(
                        row,
                        lease_expired=(
                            row["status"] == "RUNNING"
                            and row["lease_expires_at"] is not None
                            and row["lease_expires_at"] <= datetime.now(UTC)
                        ),
                    )
                    for row in sorted(self.db.rows.values(), key=lambda row: row["status"] != "RUNNING")
                    if row["batch_id"] == batch
                    and row["operation_type"] == kind
                    and row["status"] in {"QUEUED", "RUNNING"}
                ),
                None,
            )
            return
        if sql.startswith("INSERT INTO app.advisory_historical_range_operation"):
            names = (
                "operation_id",
                "batch_id",
                "operation_type",
                "operation_idempotency_key",
                "request_payload_sha256",
                "planning_identity_hash",
                "expected_row_version",
            )
            row = dict(zip(names, params[:7], strict=True), status="QUEUED", lease_expires_at=None)
            self.db.rows[row["operation_id"]] = row
            self.rowcount = 1
            return
        raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self):
        return self.one


@pytest.mark.parametrize(("status", "expired"), [("RUNNING", True), ("RUNNING", False), ("QUEUED", False)])
def test_new_resume_key_does_not_strand_a_second_operation(status, expired):
    db = _Database()
    original = _request()
    original_row = db.seed(original, status=status, expired=expired)

    with pytest.raises(HistoricalRangeContractError) as caught:
        db.repository().get_or_create_operation(_request("new", version=9))

    assert caught.value.reason_code == "ADVISORY_HISTORICAL_RANGE_OPERATION_INFLIGHT_CONFLICT"
    context = caught.value.context
    assert context["existing_operation_id"] == original.operation_id
    assert context["operation_idempotency_key"] == original.operation_idempotency_key
    assert context["expected_row_version"] == 7
    assert context["recovery_action"] == ("EXACT_RETRY" if status == "QUEUED" or expired else "WAIT")
    assert context["lease_expired"] is (status == "RUNNING" and expired)
    assert list(db.rows.values()) == [original_row]


def test_original_resume_exact_retry_preserves_identity_but_rejects_changed_payload():
    db = _Database()
    original = _request()
    row = db.seed(original)
    returned, exact = db.repository().get_or_create_operation(original)
    assert exact is True
    assert returned == row
    with pytest.raises(HistoricalRangeContractError) as caught:
        db.repository().get_or_create_operation(_request(version=9))
    assert caught.value.reason_code == REASON_IDEMPOTENCY_CONFLICT
    assert len(db.rows) == 1


def test_legacy_stranded_queue_points_to_running_identity_but_does_not_block_its_exact_retry():
    db = _Database()
    original = _request()
    original_row = db.seed(original)
    stranded = _request("stranded", version=9)
    db.seed(stranded, status="QUEUED", expired=False)

    with pytest.raises(HistoricalRangeContractError) as caught:
        db.repository().get_or_create_operation(stranded)
    assert caught.value.context["existing_operation_id"] == original.operation_id
    assert caught.value.context["recovery_action"] == "EXACT_RETRY"
    row, exact = db.repository().get_or_create_operation(original)
    assert exact is True
    assert row == original_row
    assert len(db.rows) == 2


def test_simultaneous_new_resume_keys_leave_one_recoverable_operation():
    db = _Database()
    repository = db.repository()

    def admit(key):
        try:
            return repository.get_or_create_operation(_request(key))[0]["operation_id"]
        except HistoricalRangeContractError as exc:
            assert exc.reason_code == "ADVISORY_HISTORICAL_RANGE_OPERATION_INFLIGHT_CONFLICT"
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(admit, ("first", "second")))
    assert sum(result is not None for result in results) == 1
    assert len(db.rows) == 1
    winner = next(request for request in (_request("first"), _request("second")) if request.operation_id in db.rows)
    _, exact = repository.get_or_create_operation(winner)
    assert exact is True


@pytest.mark.parametrize("status", ["COMPLETED", "FAILED", "RETRYABLE_FAILED", "WAITING_INPUT"])
def test_terminal_or_blocked_old_resume_does_not_block_new_identity(status):
    db = _Database()
    db.seed(_request(), status=status)
    new = _request("new", version=9)
    row, exact = db.repository().get_or_create_operation(new)
    assert exact is False
    assert row["operation_id"] == new.operation_id
    assert len(db.rows) == 2


@pytest.mark.parametrize(
    "other",
    [
        _request("new", batch="other"),
        _request("new", kind=HistoricalRangeOperationType.CANCEL),
    ],
)
def test_other_batch_and_operation_type_remain_independent(other):
    db = _Database()
    db.seed(_request())
    row, exact = db.repository().get_or_create_operation(other)
    assert exact is False
    assert row["operation_id"] == other.operation_id
