"""Read-only, repeatable-read access to immutable MiniQMT TCA evidence.

The repository is deliberately the authority for result-series selection.  API
adapters must not reconstruct a "latest" result from timestamps or receipt
generations: a result series is ordered only by ``result_generation`` and must
retain a complete supersession chain with COMPLETED-receipt membership.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator, Mapping, Sequence

import psycopg2.extras

from backend.db.pg_pool import get_conn

from .tca_models import canonical_json_sha256
from .tca_read_service import TcaActiveReadVersion, TcaReadError


_SNAPSHOT_KINDS = frozenset({"DEADLINE", "RECONCILED_FINAL"})
_TERMINAL_STATES = frozenset({"NO_ELIGIBLE", "WORKING", "COMPLETED_BY_DEADLINE", "DEADLINE_RESIDUAL", "INVALID"})
_VERSION_FIELDS = (
    "calculator_version",
    "formula_version",
    "schema_version",
    "query_version",
    "benchmark_policy_version",
    "mark_policy_version",
    "fee_policy_version",
    "trade_provenance_policy_version",
)


@dataclass(frozen=True, slots=True)
class ExecutionTcaParentPage:
    """One stable, keyset-addressable page of SIM parent benchmarks."""

    parents: tuple[Mapping[str, Any], ...]
    next_key: tuple[date, str, int] | None


@dataclass(frozen=True, slots=True)
class ExecutionTcaSelection:
    """Selected result plus the immutable series that proved its authority."""

    result: Mapping[str, Any]
    result_series: tuple[Mapping[str, Any], ...]
    selection_mode: str


@dataclass(frozen=True, slots=True)
class ExecutionTcaDetail:
    """One selected result with exact mark and observation memberships."""

    selection: ExecutionTcaSelection
    marks: tuple[Mapping[str, Any], ...]
    trade_observations: tuple[Mapping[str, Any], ...]


class ExecutionTcaReadRepository:
    """SIM-only source reader for parent/list/TCA operations.

    Every public operation can take a caller-owned cursor.  Otherwise it opens
    one explicit read-only ``REPEATABLE READ`` snapshot.  This lets the next
    service layer compose a parent and its TCA without observing two database
    snapshots, while keeping individual repository calls safe by default.
    """

    def __init__(self, conn_factory: Any = None) -> None:
        self._conn_factory = conn_factory or get_conn

    @contextmanager
    def read_snapshot(self) -> Iterator[Any]:
        """Yield one read-only repeatable-read cursor for all composed queries."""

        with self._conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                yield cursor

    def get_parent(
        self,
        *,
        parent_intent_id: str,
        parent_revision: int | None = None,
        cursor: Any | None = None,
    ) -> Mapping[str, Any] | None:
        """Return a SIM parent benchmark, including legacy-invalid parents.

        An omitted revision resolves to the unique highest immutable revision;
        it never relies on audit timestamps.
        """

        parent_id = _required_text(parent_intent_id, "parent_intent_id")
        revision = _positive_revision(parent_revision) if parent_revision is not None else None
        if cursor is None:
            with self.read_snapshot() as snapshot_cursor:
                return self.get_parent(
                    parent_intent_id=parent_id,
                    parent_revision=revision,
                    cursor=snapshot_cursor,
                )

        clauses = ["b.parent_intent_id = %s", "b.environment = 'SIM'"]
        params: list[Any] = [parent_id]
        if revision is not None:
            clauses.append("b.parent_revision = %s")
            params.append(revision)
        cursor.execute(
            f"""
            SELECT b.*
            FROM qmt_strategy.execution_parent_benchmark b
            WHERE {" AND ".join(clauses)}
            ORDER BY b.parent_revision DESC
            """,
            tuple(params),
        )
        rows = _rows(cursor)
        return rows[0] if rows else None

    def list_parents(
        self,
        *,
        binding_id: str,
        trade_date: date,
        limit: int = 100,
        after_key: tuple[date, str, int] | None = None,
        terminal_state: str | None = None,
        active_version: TcaActiveReadVersion | None = None,
        snapshot_kind: str | None = None,
        cursor: Any | None = None,
    ) -> ExecutionTcaParentPage:
        """List SIM parents in the documented keyset order.

        Terminal-state filtering uses typed benchmark/TCA fields only.  A
        version tuple is required for that filter so it cannot silently compare
        results from different result series.
        """

        binding = _required_text(binding_id, "binding_id")
        page_limit = _page_limit(limit)
        normalized_after = _normalize_after_key(after_key, trade_date)
        normalized_terminal = _terminal_state_filter(terminal_state)
        snapshot_kinds = _list_snapshot_kinds(snapshot_kind)
        if normalized_terminal is not None and active_version is None:
            raise _read_error(
                "ADAPTIVE_IS_TCA_ACTIVE_READ_VERSION_MISSING",
                "terminal-state filtering requires the explicit active TCA read version",
                http_status=503,
                context={"operation": "list_execution_parents"},
            )

        if cursor is None:
            with self.read_snapshot() as snapshot_cursor:
                return self.list_parents(
                    binding_id=binding,
                    trade_date=trade_date,
                    limit=page_limit,
                    after_key=normalized_after,
                    terminal_state=normalized_terminal,
                    active_version=active_version,
                    snapshot_kind=snapshot_kind,
                    cursor=snapshot_cursor,
                )

        clauses = ["b.binding_id = %s", "b.trade_date = %s", "b.environment = 'SIM'"]
        params: list[Any] = [binding, trade_date]
        if normalized_after is not None:
            clauses.append("(b.trade_date, b.parent_intent_id, b.parent_revision) > (%s, %s, %s)")
            params.extend(normalized_after)
        cursor.execute(
            f"""
            SELECT b.*
            FROM qmt_strategy.execution_parent_benchmark b
            WHERE {" AND ".join(clauses)}
            ORDER BY b.trade_date ASC, b.parent_intent_id ASC, b.parent_revision ASC
            """,
            tuple(params),
        )
        source_rows = _rows(cursor)

        records: list[Mapping[str, Any]] = []
        for parent in source_rows:
            selected_result: Mapping[str, Any] | None = None
            if active_version is not None:
                for selected_snapshot_kind in snapshot_kinds:
                    try:
                        selection = self.get_tca(
                            parent_intent_id=str(parent["parent_intent_id"]),
                            parent_revision=int(parent["parent_revision"]),
                            snapshot_kind=selected_snapshot_kind,
                            active_version=active_version,
                            cursor=cursor,
                        )
                        selected_result = selection.result
                        break
                    except TcaReadError as exc:
                        if exc.reason_code != "ADAPTIVE_IS_TCA_RESULT_NOT_FOUND":
                            raise
            state = _derive_terminal_state(parent, selected_result) if active_version is not None else None
            if normalized_terminal is not None and state != normalized_terminal:
                continue
            record = dict(parent)
            if state is not None:
                record["terminal_state"] = state
                record["latest_tca_result_id"] = (
                    str(selected_result["tca_result_id"]) if selected_result is not None else None
                )
                record["latest_tca_snapshot_kind"] = (
                    str(selected_result["snapshot_kind"]) if selected_result is not None else None
                )
            records.append(record)

        page_rows = tuple(records[:page_limit])
        has_more = len(records) > page_limit
        next_key = _parent_key(page_rows[-1]) if has_more and page_rows else None
        return ExecutionTcaParentPage(parents=page_rows, next_key=next_key)

    def get_tca(
        self,
        *,
        parent_intent_id: str,
        parent_revision: int,
        snapshot_kind: str,
        active_version: TcaActiveReadVersion | None = None,
        tca_version: str | None = None,
        receipt_id: str | None = None,
        as_of: datetime | None = None,
        cursor: Any | None = None,
    ) -> ExecutionTcaSelection:
        """Resolve one TCA result by immutable series, receipt, or evidence time.

        ``tca_version`` is the content-addressed result ID.  Without it, the
        active version tuple identifies exactly one result series; an ambiguous
        series, supersession fork, generation gap, or missing completed receipt
        is a loud error rather than a fallback to a timestamp-based "latest".
        """

        parent_id = _required_text(parent_intent_id, "parent_intent_id")
        revision = _positive_revision(parent_revision)
        kind = _snapshot_kind(snapshot_kind)
        explicit_id = _optional_text(tca_version, "tca_version")
        requested_receipt = _optional_text(receipt_id, "receipt_id")
        normalized_as_of = _as_of_time(as_of)
        _validate_selection_request(
            tca_version=explicit_id,
            receipt_id=requested_receipt,
            as_of=normalized_as_of,
        )

        if cursor is None:
            with self.read_snapshot() as snapshot_cursor:
                return self.get_tca(
                    parent_intent_id=parent_id,
                    parent_revision=revision,
                    snapshot_kind=kind,
                    active_version=active_version,
                    tca_version=explicit_id,
                    receipt_id=requested_receipt,
                    as_of=normalized_as_of,
                    cursor=snapshot_cursor,
                )

        if explicit_id is not None:
            direct_rows = self._read_explicit_result(
                cursor=cursor,
                parent_intent_id=parent_id,
                parent_revision=revision,
                snapshot_kind=kind,
                tca_result_id=explicit_id,
            )
            if len(direct_rows) != 1:
                raise _result_not_found(parent_id, revision, kind, {"tca_version": explicit_id})
            direct = direct_rows[0]
            series = self._read_result_series(
                cursor=cursor,
                parent_intent_id=parent_id,
                parent_revision=revision,
                snapshot_kind=kind,
                result_series_key=_required_text(direct.get("result_series_key"), "result_series_key"),
            )
            return _select_result(
                rows=series,
                parent_intent_id=parent_id,
                parent_revision=revision,
                snapshot_kind=kind,
                explicit_result_id=explicit_id,
                receipt_id=requested_receipt,
                as_of=None,
                expected_series_key=None,
            )

        if active_version is None:
            raise _read_error(
                "ADAPTIVE_IS_TCA_ACTIVE_READ_VERSION_MISSING",
                "implicit TCA reads require MINIQMT_TCA_ACTIVE_READ_VERSION",
                http_status=503,
                context={"operation": "get_execution_tca"},
            )
        series = self._read_active_results(
            cursor=cursor,
            parent_intent_id=parent_id,
            parent_revision=revision,
            snapshot_kind=kind,
            active_version=active_version,
        )
        return _select_result(
            rows=series,
            parent_intent_id=parent_id,
            parent_revision=revision,
            snapshot_kind=kind,
            explicit_result_id=None,
            receipt_id=requested_receipt,
            as_of=normalized_as_of,
            expected_series_key=_expected_result_series_key(
                parent_intent_id=parent_id,
                parent_revision=revision,
                snapshot_kind=kind,
                active_version=active_version,
            ),
        )

    def get_tca_detail(
        self,
        *,
        parent_intent_id: str,
        parent_revision: int,
        snapshot_kind: str,
        active_version: TcaActiveReadVersion | None = None,
        tca_version: str | None = None,
        receipt_id: str | None = None,
        as_of: datetime | None = None,
        cursor: Any | None = None,
    ) -> ExecutionTcaDetail:
        """Return a selected TCA result with exact immutable lineage rows."""

        if cursor is None:
            with self.read_snapshot() as snapshot_cursor:
                return self.get_tca_detail(
                    parent_intent_id=parent_intent_id,
                    parent_revision=parent_revision,
                    snapshot_kind=snapshot_kind,
                    active_version=active_version,
                    tca_version=tca_version,
                    receipt_id=receipt_id,
                    as_of=as_of,
                    cursor=snapshot_cursor,
                )
        selection = self.get_tca(
            parent_intent_id=parent_intent_id,
            parent_revision=parent_revision,
            snapshot_kind=snapshot_kind,
            active_version=active_version,
            tca_version=tca_version,
            receipt_id=receipt_id,
            as_of=as_of,
            cursor=cursor,
        )
        result_id = _required_text(selection.result.get("tca_result_id"), "tca_result_id")
        return ExecutionTcaDetail(
            selection=selection,
            marks=self._read_result_marks(cursor=cursor, tca_result_id=result_id),
            trade_observations=self._read_result_trade_observations(cursor=cursor, tca_result_id=result_id),
        )

    def _read_active_results(
        self,
        *,
        cursor: Any,
        parent_intent_id: str,
        parent_revision: int,
        snapshot_kind: str,
        active_version: TcaActiveReadVersion,
    ) -> tuple[Mapping[str, Any], ...]:
        version_values = active_version.as_mapping()
        cursor.execute(
            _RESULT_SELECT
            + """
            AND r.parent_intent_id = %s
            AND r.parent_revision = %s
            AND r.snapshot_kind = %s
            AND r.calculator_version = %s
            AND r.formula_version = %s
            AND r.schema_version = %s
            AND r.query_version = %s
            AND r.benchmark_policy_version = %s
            AND r.mark_policy_version = %s
            AND r.fee_policy_version = %s
            AND r.trade_provenance_policy_version = %s
            ORDER BY r.result_series_key ASC, r.result_generation ASC
            """,
            (
                parent_intent_id,
                parent_revision,
                snapshot_kind,
                *(version_values[field] for field in _VERSION_FIELDS),
            ),
        )
        return _rows(cursor)

    def _read_explicit_result(
        self,
        *,
        cursor: Any,
        parent_intent_id: str,
        parent_revision: int,
        snapshot_kind: str,
        tca_result_id: str,
    ) -> tuple[Mapping[str, Any], ...]:
        cursor.execute(
            _RESULT_SELECT
            + """
            AND r.parent_intent_id = %s
            AND r.parent_revision = %s
            AND r.snapshot_kind = %s
            AND r.tca_result_id = %s
            ORDER BY r.result_generation ASC
            """,
            (parent_intent_id, parent_revision, snapshot_kind, tca_result_id),
        )
        return _rows(cursor)

    def _read_result_series(
        self,
        *,
        cursor: Any,
        parent_intent_id: str,
        parent_revision: int,
        snapshot_kind: str,
        result_series_key: str,
    ) -> tuple[Mapping[str, Any], ...]:
        cursor.execute(
            _RESULT_SELECT
            + """
            AND r.parent_intent_id = %s
            AND r.parent_revision = %s
            AND r.snapshot_kind = %s
            AND r.result_series_key = %s
            ORDER BY r.result_generation ASC
            """,
            (parent_intent_id, parent_revision, snapshot_kind, result_series_key),
        )
        return _rows(cursor)

    def _read_result_marks(self, *, cursor: Any, tca_result_id: str) -> tuple[Mapping[str, Any], ...]:
        cursor.execute(
            """
            SELECT
                membership.tca_result_id,
                membership.mark_id,
                membership.mark_role,
                membership.membership_hash,
                mark.parent_intent_id,
                mark.parent_revision,
                mark.mark_type,
                mark.trade_account_id,
                mark.trade_date,
                mark.trade_id,
                mark.child_order_id,
                mark.horizon_ms,
                mark.target_time,
                mark.market_time,
                mark.received_at,
                mark.bid_price_1,
                mark.ask_price_1,
                mark.mid_price,
                mark.last_price,
                mark.quote_source,
                mark.age_or_lag_ms,
                mark.quality,
                mark.market_phase,
                mark.stock_status,
                mark.raw_quote_sha256,
                mark.market_data_id,
                mark.mark_policy_version,
                mark.source_input_sha256,
                mark.evidence_sha256
            FROM qmt_strategy.execution_tca_result_mark membership
            INNER JOIN qmt_strategy.execution_tca_mark mark
                ON mark.mark_id = membership.mark_id
                AND mark.parent_intent_id = membership.parent_intent_id
                AND mark.parent_revision = membership.parent_revision
                AND mark.mark_type = membership.mark_role
            WHERE membership.tca_result_id = %s
            ORDER BY membership.mark_role ASC, membership.mark_id ASC
            """,
            (tca_result_id,),
        )
        return _rows(cursor)

    def _read_result_trade_observations(self, *, cursor: Any, tca_result_id: str) -> tuple[Mapping[str, Any], ...]:
        cursor.execute(
            """
            SELECT
                membership.tca_result_id,
                membership.trade_observation_id,
                membership.parent_intent_id,
                membership.parent_revision,
                membership.trade_account_id,
                membership.trade_date,
                membership.trade_id,
                membership.observation_role,
                membership.selected_content_sha256,
                membership.membership_hash,
                observation.intent_id,
                observation.qmt_order_id,
                observation.child_order_id,
                observation.symbol,
                observation.side,
                observation.ingest_source,
                observation.observed_at,
                observation.broker_trade_time,
                observation.price,
                observation.quantity,
                observation.amount,
                observation.commission,
                observation.fee_evidence_level,
                observation.canonical_trade_fact_sha256,
                observation.timing_observation_sha256,
                observation.attribution_sha256,
                observation.fee_observation_sha256,
                observation.raw_observation_sha256,
                observation.reconciliation_run_id,
                observation.normalization_version,
                observation.broker_time_parser_version
            FROM qmt_strategy.execution_tca_result_trade_observation membership
            INNER JOIN qmt_strategy.execution_tca_trade_observation observation
                ON observation.trade_observation_id = membership.trade_observation_id
                AND observation.account_id = membership.trade_account_id
                AND observation.trade_date = membership.trade_date
                AND observation.trade_id = membership.trade_id
            WHERE membership.tca_result_id = %s
            ORDER BY membership.trade_date ASC, membership.trade_id ASC, membership.observation_role ASC
            """,
            (tca_result_id,),
        )
        return _rows(cursor)


_RESULT_SELECT = """
SELECT
    r.*,
    ARRAY(
        SELECT rr.receipt_id
        FROM qmt_strategy.execution_tca_receipt_result rr
        INNER JOIN qmt_strategy.execution_tca_rebuild_receipt receipt
            ON receipt.receipt_id = rr.receipt_id
            AND receipt.receipt_status = rr.receipt_status
        WHERE rr.tca_result_id = r.tca_result_id
            AND rr.receipt_status = 'COMPLETED'
            AND receipt.receipt_status = 'COMPLETED'
        ORDER BY rr.receipt_id ASC
    ) AS completed_receipt_ids
FROM qmt_strategy.execution_parent_tca r
INNER JOIN qmt_strategy.execution_parent_benchmark b
    ON b.parent_intent_id = r.parent_intent_id
    AND b.parent_revision = r.parent_revision
WHERE b.environment = 'SIM'
"""


def _select_result(
    *,
    rows: Sequence[Mapping[str, Any]],
    parent_intent_id: str,
    parent_revision: int,
    snapshot_kind: str,
    explicit_result_id: str | None,
    receipt_id: str | None,
    as_of: datetime | None,
    expected_series_key: str | None,
) -> ExecutionTcaSelection:
    if not rows:
        raise _result_not_found(parent_intent_id, parent_revision, snapshot_kind, {})
    series = _validate_result_series(rows, expected_series_key=expected_series_key)
    if explicit_result_id is not None:
        matches = [row for row in series if str(row.get("tca_result_id")) == explicit_result_id]
        if len(matches) != 1:
            raise _chain_fork("explicit result was absent from its result series")
        selected = matches[0]
        mode = "EXPLICIT"
        if receipt_id is not None and receipt_id not in _completed_receipt_ids(selected):
            raise _result_not_found(
                parent_intent_id,
                parent_revision,
                snapshot_kind,
                {"tca_version": explicit_result_id, "receipt_id": receipt_id},
            )
    elif receipt_id is not None:
        matches = [row for row in series if receipt_id in _completed_receipt_ids(row)]
        if not matches:
            raise _result_not_found(
                parent_intent_id,
                parent_revision,
                snapshot_kind,
                {"receipt_id": receipt_id},
            )
        if len(matches) != 1:
            raise _chain_fork("one completed receipt selected multiple results for the same parent snapshot")
        selected = matches[0]
        mode = "RECEIPT"
    elif as_of is not None:
        matches = [row for row in series if _completed_receipt_ids(row) and _result_source_started_at(row) <= as_of]
        if not matches:
            raise _result_not_found(
                parent_intent_id,
                parent_revision,
                snapshot_kind,
                {"as_of": as_of.isoformat()},
            )
        selected = max(matches, key=lambda row: int(row["result_generation"]))
        mode = "AS_OF"
    else:
        selected = series[-1]
        mode = "ACTIVE_HEAD"

    if not _completed_receipt_ids(selected):
        raise _read_error(
            "ADAPTIVE_IS_TCA_BATCH_EVIDENCE_MISSING",
            "the selected TCA result is not a member of any COMPLETED receipt",
            http_status=409,
            context={
                "parent_intent_id": parent_intent_id,
                "parent_revision": parent_revision,
                "snapshot_kind": snapshot_kind,
                "tca_result_id": str(selected.get("tca_result_id") or ""),
            },
        )
    return ExecutionTcaSelection(result=selected, result_series=series, selection_mode=mode)


def _validate_result_series(
    rows: Sequence[Mapping[str, Any]], *, expected_series_key: str | None
) -> tuple[Mapping[str, Any], ...]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        key = _required_text(row.get("result_series_key"), "result_series_key")
        grouped.setdefault(key, []).append(dict(row))
    if len(grouped) != 1:
        raise _chain_fork("active version tuple resolved more than one result series")
    series_key, series_rows = next(iter(grouped.items()))
    if expected_series_key is not None and series_key != expected_series_key:
        raise _chain_fork("active version tuple did not resolve to its content-addressed result series")
    series = tuple(sorted(series_rows, key=lambda row: int(row["result_generation"])))
    expected_generation = 1
    predecessor_id: str | None = None
    seen_ids: set[str] = set()
    version_manifest = _result_version_manifest(series[0])
    for row in series:
        generation = _positive_generation(row.get("result_generation"))
        result_id = _required_text(row.get("tca_result_id"), "tca_result_id")
        supersedes = _optional_text(row.get("supersedes_tca_result_id"), "supersedes_tca_result_id")
        if (
            generation != expected_generation
            or supersedes != predecessor_id
            or result_id in seen_ids
            or _result_version_manifest(row) != version_manifest
        ):
            raise _chain_fork("result generation or supersedes chain is not contiguous")
        expected_generation += 1
        predecessor_id = result_id
        seen_ids.add(result_id)
    return series


def _expected_result_series_key(
    *,
    parent_intent_id: str,
    parent_revision: int,
    snapshot_kind: str,
    active_version: TcaActiveReadVersion,
) -> str:
    return canonical_json_sha256(
        {
            "parent_intent_id": parent_intent_id,
            "parent_revision": parent_revision,
            "snapshot_kind": snapshot_kind,
            **active_version.as_mapping(),
        }
    )


def _result_version_manifest(row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(_required_text(row.get(field), field) for field in _VERSION_FIELDS)


def _derive_terminal_state(parent: Mapping[str, Any], result: Mapping[str, Any] | None) -> str:
    if str(parent.get("eligibility_class") or "") == "NO_ELIGIBLE_QUANTITY":
        return "NO_ELIGIBLE"
    eligible_quantity = _integer_or_none(parent.get("eligible_quantity"))
    if eligible_quantity == 0:
        return "NO_ELIGIBLE"
    if result is None:
        return "WORKING"
    if str(result.get("result_status") or "") == "INVALID":
        return "INVALID"
    deadline_residual = _integer_or_none(result.get("deadline_residual_quantity"))
    completion = _decimal_or_none(result.get("completion_by_deadline_quantity"))
    if deadline_residual == 0 or (completion is not None and completion >= Decimal("1")):
        return "COMPLETED_BY_DEADLINE"
    if deadline_residual is not None and deadline_residual > 0:
        return "DEADLINE_RESIDUAL"
    if str(result.get("snapshot_kind") or "") == "RECONCILED_FINAL":
        return "DEADLINE_RESIDUAL"
    return "WORKING"


def _validate_selection_request(*, tca_version: str | None, receipt_id: str | None, as_of: datetime | None) -> None:
    if receipt_id is not None and as_of is not None:
        raise _read_error(
            "ADAPTIVE_IS_TCA_SELECTION_CONFLICT",
            "receipt_id and as_of are mutually exclusive",
            http_status=400,
            context={"receipt_id": receipt_id},
        )
    if tca_version is not None and as_of is not None:
        raise _read_error(
            "ADAPTIVE_IS_TCA_SELECTION_CONFLICT",
            "tca_version and as_of are mutually exclusive",
            http_status=400,
            context={"tca_version": tca_version},
        )


def _result_not_found(
    parent_intent_id: str,
    parent_revision: int,
    snapshot_kind: str,
    extra_context: Mapping[str, Any],
) -> TcaReadError:
    return _read_error(
        "ADAPTIVE_IS_TCA_RESULT_NOT_FOUND",
        "no readable TCA result matched the requested parent and selection",
        http_status=404,
        context={
            "parent_intent_id": parent_intent_id,
            "parent_revision": parent_revision,
            "snapshot_kind": snapshot_kind,
            **dict(extra_context),
        },
    )


def _chain_fork(message: str) -> TcaReadError:
    return _read_error(
        "ADAPTIVE_IS_TCA_CHAIN_FORK",
        message,
        http_status=409,
    )


def _read_error(
    reason_code: str,
    message: str,
    *,
    http_status: int,
    context: Mapping[str, Any] | None = None,
) -> TcaReadError:
    return TcaReadError(
        reason_code,
        message,
        http_status=http_status,
        stage="TCA_READ_REPOSITORY",
        context=context,
    )


def _required_text(value: Any, name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise _read_error(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            f"{name} must not be empty",
            http_status=400,
            context={"field": name},
        )
    return normalized


def _optional_text(value: Any, name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, name)


def _positive_revision(value: Any) -> int:
    try:
        revision = int(value)
    except (TypeError, ValueError) as exc:
        raise _read_error(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            "parent_revision must be a positive integer",
            http_status=400,
            context={"field": "parent_revision"},
        ) from exc
    if revision <= 0:
        raise _read_error(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            "parent_revision must be a positive integer",
            http_status=400,
            context={"field": "parent_revision"},
        )
    return revision


def _positive_generation(value: Any) -> int:
    try:
        generation = int(value)
    except (TypeError, ValueError) as exc:
        raise _chain_fork("result generation is invalid") from exc
    if generation <= 0:
        raise _chain_fork("result generation is invalid")
    return generation


def _snapshot_kind(value: str) -> str:
    normalized = _required_text(value, "snapshot_kind")
    if normalized not in _SNAPSHOT_KINDS:
        raise _read_error(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            "snapshot_kind is unsupported",
            http_status=400,
            context={"snapshot_kind": normalized},
        )
    return normalized


def _list_snapshot_kinds(value: str | None) -> tuple[str, ...]:
    """Prefer final evidence, then deadline evidence, for parent-list state."""

    if value is None:
        return "RECONCILED_FINAL", "DEADLINE"
    return (_snapshot_kind(value),)


def _page_limit(value: Any) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise _read_error(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            "limit must be an integer from 1 through 200",
            http_status=400,
            context={"field": "limit"},
        ) from exc
    if not 1 <= limit <= 200:
        raise _read_error(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            "limit must be an integer from 1 through 200",
            http_status=400,
            context={"field": "limit"},
        )
    return limit


def _terminal_state_filter(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _required_text(value, "terminal_state")
    if normalized not in _TERMINAL_STATES:
        raise _read_error(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            "terminal_state is unsupported",
            http_status=400,
            context={"terminal_state": normalized},
        )
    return normalized


def _normalize_after_key(
    value: tuple[date, str, int] | None, expected_trade_date: date
) -> tuple[date, str, int] | None:
    if value is None:
        return None
    if len(value) != 3:
        raise _read_error(
            "ADAPTIVE_IS_TCA_CURSOR_INVALID",
            "keyset cursor last key is invalid",
            http_status=400,
        )
    trade_date, parent_intent_id, parent_revision = value
    if trade_date != expected_trade_date:
        raise _read_error(
            "ADAPTIVE_IS_TCA_CURSOR_INVALID",
            "keyset cursor trade date does not match the requested filter",
            http_status=400,
        )
    return trade_date, _required_text(parent_intent_id, "cursor.parent_intent_id"), _positive_revision(parent_revision)


def _parent_key(parent: Mapping[str, Any]) -> tuple[date, str, int]:
    raw_trade_date = parent.get("trade_date")
    parsed_trade_date = raw_trade_date if isinstance(raw_trade_date, date) else date.fromisoformat(str(raw_trade_date))
    return (
        parsed_trade_date,
        _required_text(parent.get("parent_intent_id"), "parent_intent_id"),
        _positive_revision(parent.get("parent_revision")),
    )


def _completed_receipt_ids(row: Mapping[str, Any]) -> tuple[str, ...]:
    raw = row.get("completed_receipt_ids") or ()
    if isinstance(raw, str):
        raw = (raw,)
    return tuple(sorted({_required_text(value, "completed_receipt_id") for value in raw}))


def _result_source_started_at(row: Mapping[str, Any]) -> datetime:
    value = row.get("source_snapshot_started_at")
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise _chain_fork("result source snapshot timestamp is invalid") from exc
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _as_of_time(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise _read_error(
            "ADAPTIVE_IS_TCA_REQUEST_INVALID",
            "as_of must include a UTC offset",
            http_status=400,
            context={"field": "as_of"},
        )
    return value.astimezone(UTC)


def _integer_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _row_mapping(cursor: Any, row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    names = [description[0] for description in cursor.description]
    return dict(zip(names, row, strict=True))


def _rows(cursor: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(_row_mapping(cursor, row) for row in cursor.fetchall())
